"""判据 2 的端到端验证：真实子进程 + 真实 SIGKILL。

慢用例（CI 默认不跑），本地用 `pytest -m slow` 执行。
"""

from __future__ import annotations

import os
import signal
import time

import pytest

from evalconsole import db, fleet


@pytest.mark.slow
def test_sigkill_worker_task_redispatched_within_10s(tmp_path):
    path = str(tmp_path / "kill.db")
    db.init_db(path)
    conn = db.connect(path)
    ts = db.now()
    for i in range(6):
        conn.execute(
            "INSERT INTO tasks(task_id, payload, status, created_at, updated_at) VALUES(?,?, 'pending', ?, ?)",
            (
                f"k-{i}",
                '{"handler": "sleep", "args": {"seconds": 3.0}, "timeout": 20}',
                ts + i * 1e-5,
                ts,
            ),
        )

    scheduler_proc = fleet.spawn_scheduler(path, interval=0.5)
    workers = [fleet.spawn_worker(path, f"w{i + 1}", task_timeout=20.0) for i in range(2)]
    try:
        # 等一个正在跑的任务
        deadline = time.time() + 20
        picked = None
        while time.time() < deadline and picked is None:
            for row in db.rows(conn, "SELECT task_id, lease_owner FROM tasks WHERE status='running'"):
                owner = row["lease_owner"]
                pid_row = conn.execute("SELECT pid FROM workers WHERE worker_id=?", (owner,)).fetchone()
                pid = int(pid_row["pid"]) if pid_row and pid_row["pid"] else 0
                if pid and os.path.exists(f"/proc/{pid}"):
                    picked = (str(row["task_id"]), str(owner), pid)
                    break
            time.sleep(0.02)
        assert picked is not None, "没有抓到在跑的任务"

        task_id, owner, pid = picked
        kill_at = time.time()
        os.kill(pid, signal.SIGKILL)

        reassigned_ms = None
        while time.time() - kill_at < 15:
            row = conn.execute("SELECT lease_owner, status FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is not None and row["lease_owner"] and row["lease_owner"] != owner:
                new_pid_row = conn.execute(
                    "SELECT pid FROM workers WHERE worker_id=?", (row["lease_owner"],)
                ).fetchone()
                new_pid = int(new_pid_row["pid"]) if new_pid_row and new_pid_row["pid"] else 0
                if new_pid and os.path.exists(f"/proc/{new_pid}"):
                    reassigned_ms = (time.time() - kill_at) * 1000.0
                    break
            time.sleep(0.02)
        assert reassigned_ms is not None, "任务没有被重派"
        assert reassigned_ms <= 10_000, f"重派耗时 {reassigned_ms:.0f}ms 超过 10s"

        rec = conn.execute(
            "SELECT from_worker, silence_ms FROM redispatches WHERE task_id=? ORDER BY id DESC LIMIT 1", (task_id,)
        ).fetchone()
        assert rec is not None and rec["from_worker"] == owner
    finally:
        for proc in [scheduler_proc, *workers]:
            fleet.stop(proc, timeout=5)
        fleet.close_handles([scheduler_proc, *workers])
        conn.close()

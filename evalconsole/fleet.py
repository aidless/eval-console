"""进程编排：把 scheduler / worker 作为真实子进程拉起（这样 SIGKILL 才有意义）。"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from . import db

ROOT = Path(__file__).resolve().parent.parent


def _env() -> dict[str, str]:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + existing if existing else "")
    return env


def _spawn(args: list[str], log_path: str | None = None) -> subprocess.Popen:
    cmd = [sys.executable, "-m", "evalconsole", *args]
    stdout = stderr = subprocess.DEVNULL
    handle = None
    if log_path:
        parent = os.path.dirname(os.path.abspath(log_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        handle = open(log_path, "a", encoding="utf-8")
        stdout = stderr = handle
    proc = subprocess.Popen(cmd, cwd=str(ROOT), env=_env(), stdout=stdout, stderr=stderr)
    if handle is not None:
        proc._log_handle = handle  # type: ignore[attr-defined]  # 保持句柄，关闭时统一释放
    return proc


def spawn_worker(
    db_path: str,
    worker_id: str,
    *,
    heartbeat: float = 1.0,
    lease_ttl: float = 5.0,
    task_timeout: float = 10.0,
    log_path: str | None = None,
) -> subprocess.Popen:
    return _spawn(
        [
            "worker",
            "--db", db_path,
            "--id", worker_id,
            "--heartbeat", str(heartbeat),
            "--lease-ttl", str(lease_ttl),
            "--task-timeout", str(task_timeout),
        ],
        log_path=log_path,
    )


def spawn_scheduler(
    db_path: str,
    *,
    interval: float = 0.5,
    alerts_path: str | None = None,
    stale_after: float = 15.0,
    failure_rate: float = 0.20,
    log_path: str | None = None,
) -> subprocess.Popen:
    args = [
        "scheduler",
        "--db", db_path,
        "--interval", str(interval),
        "--stale-after", str(stale_after),
        "--failure-rate", str(failure_rate),
    ]
    if alerts_path:
        args += ["--alerts-file", alerts_path]
    return _spawn(args, log_path=log_path)


def stop(proc: subprocess.Popen, timeout: float = 5.0) -> None:
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def close_handles(procs: list[subprocess.Popen]) -> None:
    for proc in procs:
        handle = getattr(proc, "_log_handle", None)
        if handle is not None:
            try:
                handle.close()
            except OSError:
                pass


def pid_of(db_path: str, worker_id: str) -> int | None:
    conn = db.connect(db_path)
    try:
        row = conn.execute("SELECT pid FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
        return int(row["pid"]) if row and row["pid"] else None
    finally:
        conn.close()


def task_owner(db_path: str, task_id: str) -> tuple[str | None, int | None, str | None]:
    conn = db.connect(db_path)
    try:
        row = conn.execute("SELECT lease_owner, status FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            return None, None, None
        owner = row["lease_owner"]
        pid_row = None
        if owner:
            pid_row = conn.execute("SELECT pid FROM workers WHERE worker_id=?", (owner,)).fetchone()
        return owner, int(pid_row["pid"]) if pid_row and pid_row["pid"] else None, str(row["status"])
    finally:
        conn.close()


def wait_terminal(db_path: str, timeout: float = 120.0) -> dict[str, int]:
    conn = db.connect(db_path)
    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            pending = int(db.scalar(
                conn, "SELECT COUNT(*) FROM tasks WHERE status NOT IN ('done','failed')"
            ) or 0)
            if pending == 0:
                break
            time.sleep(0.2)
        counts: dict[str, int] = {}
        for row in db.rows(conn, "SELECT status, COUNT(*) AS n FROM tasks GROUP BY status"):
            counts[str(row["status"])] = int(row["n"])
        return counts
    finally:
        conn.close()


def seed_tasks(db_path: str, n: int, *, fail_every: int = 10, handler: str = "sha") -> None:
    conn = db.connect(db_path)
    try:
        ts = db.now()
        for i in range(n):
            payload: dict[str, Any]
            if fail_every and i % fail_every == 0 and i > 0:
                payload = {"handler": "fail", "args": {"message": f"seed failure {i}"}}
            elif handler == "sha":
                payload = {"handler": "sha", "args": {"seed": str(i), "rounds": 60_000}, "timeout": 10}
            else:
                payload = {"handler": handler, "args": {}}
            conn.execute(
                "INSERT OR IGNORE INTO tasks(task_id, payload, status, created_at, updated_at)"
                " VALUES(?,?, 'pending', ?, ?)",
                (f"t-{i:04d}", json.dumps(payload), ts + i * 1e-4, ts),
            )
    finally:
        conn.close()

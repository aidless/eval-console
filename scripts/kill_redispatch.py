#!/usr/bin/env python3
"""故障注入：SIGKILL 掉正在跑任务的 worker，实测 kill → 重新派发的毫秒数。

默认跑 3 次，输出 evidence/kill_redispatch.json。全部为真实进程、真实 SIGKILL。
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from evalconsole import db, fleet  # noqa: E402


def loadavg() -> list[float]:
    try:
        return [round(x, 2) for x in os.getloadavg()]
    except OSError:
        return []


def find_running_task(conn, exclude_workers: set[str]) -> tuple[str, str, int] | None:
    rows = db.rows(
        conn,
        "SELECT task_id, lease_owner FROM tasks WHERE status='running' AND lease_owner IS NOT NULL"
        " ORDER BY updated_at",
    )
    for row in rows:
        owner = str(row["lease_owner"])
        if owner in exclude_workers:
            continue
        pid_row = conn.execute("SELECT pid FROM workers WHERE worker_id=?", (owner,)).fetchone()
        pid = int(pid_row["pid"]) if pid_row and pid_row["pid"] else None
        if pid and os.path.exists(f"/proc/{pid}"):
            return str(row["task_id"]), owner, pid
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/tmp/evalconsole-kill.db")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--tasks", type=int, default=9)
    ap.add_argument("--sleep", type=float, default=8.0)
    ap.add_argument("--out", default="evidence/kill_redispatch.json")
    args = ap.parse_args()

    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(args.db + suffix)
        except FileNotFoundError:
            pass
    db.init_db(args.db)
    conn = db.connect(args.db)
    start_load = loadavg()
    ts = db.now()
    for i in range(args.tasks):
        conn.execute(
            "INSERT INTO tasks(task_id, payload, status, created_at, updated_at)"
            " VALUES(?,?, 'pending', ?, ?)",
            (
                f"k-{i:03d}",
                json.dumps({"handler": "sleep", "args": {"seconds": args.sleep}, "timeout": args.sleep + 12}),
                ts + i * 1e-4,
                ts,
            ),
        )

    procs = [fleet.spawn_scheduler(args.db, interval=0.5, alerts_path=None, stale_after=15.0)]
    workers = {f"w{i + 1}": fleet.spawn_worker(args.db, f"w{i + 1}", task_timeout=30.0) for i in range(3)}
    reps: list[dict] = []
    killed_workers: set[str] = set()
    started = time.time()
    try:
        time.sleep(3.0)  # 等所有 worker 注册并领到任务
        for rep in range(args.reps):
            picked = None
            for _ in range(300):  # 等待至少一个可杀的任务
                picked = find_running_task(conn, killed_workers)
                if picked:
                    break
                time.sleep(0.1)
            if not picked:
                break
            task_id, owner, pid = picked
            kill_at = time.time()
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                continue
            killed_workers.add(owner)

            # 轮询 DB：等任务换到另一个存活的 worker
            reassigned_ms = None
            new_owner = None
            reclaim_ts = None
            deadline = kill_at + 30.0
            while time.time() < deadline:
                now = time.time()
                row = conn.execute(
                    "SELECT lease_owner, status FROM tasks WHERE task_id=?", (task_id,)
                ).fetchone()
                if row is not None and row["status"] in ("running", "retried"):
                    if row["lease_owner"] and row["lease_owner"] != owner:
                        pid_row = conn.execute(
                            "SELECT pid FROM workers WHERE worker_id=?", (row["lease_owner"],)
                        ).fetchone()
                        newpid = int(pid_row["pid"]) if pid_row and pid_row["pid"] else 0
                        if newpid and os.path.exists(f"/proc/{newpid}"):
                            reassigned_ms = (now - kill_at) * 1000.0
                            new_owner = str(row["lease_owner"])
                            break
                time.sleep(0.02)
            rec = conn.execute(
                "SELECT ts, silence_ms, reclaim_ms FROM redispatches WHERE task_id=? ORDER BY id DESC LIMIT 1",
                (task_id,),
            ).fetchone()
            if rec is not None:
                reclaim_ts = float(rec["ts"])

            # 等该任务在新 worker 上跑完，记录完成耗时
            done_ms = None
            while time.time() < kill_at + 60:
                row = conn.execute("SELECT status FROM tasks WHERE task_id=?", (task_id,)).fetchone()
                if row is not None and row["status"] in ("done", "failed"):
                    done_ms = (time.time() - kill_at) * 1000.0
                    break
                time.sleep(0.05)

            # 恢复被杀的 worker（新 pid，restart_count+1）
            fleet.stop(workers[owner], timeout=1)
            workers[owner] = fleet.spawn_worker(args.db, owner, task_timeout=30.0)
            killed_workers.discard(owner)

            reps.append(
                {
                    "rep": rep + 1,
                    "task_id": task_id,
                    "killed_worker": owner,
                    "killed_pid": pid,
                    "new_worker": new_owner,
                    "reassign_ms": None if reassigned_ms is None else round(reassigned_ms, 1),
                    "reclaim_after_kill_ms": None if reclaim_ts is None else round((reclaim_ts - kill_at) * 1000.0, 1),
                    "silence_ms_at_reclaim": None if rec is None else round(float(rec["silence_ms"]), 1),
                    "done_after_kill_ms": None if done_ms is None else round(done_ms, 1),
                }
            )
            time.sleep(1.0)
    finally:
        for proc in procs + list(workers.values()):
            fleet.stop(proc)
        fleet.close_handles(procs + list(workers.values()))
        conn.close()

    latencies = [r["reassign_ms"] for r in reps if r["reassign_ms"] is not None]
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "host": os.uname().nodename,
        "cpu_count": os.cpu_count(),
        "loadavg_start": start_load,
        "loadavg_end": loadavg(),
        "wall_seconds": round(time.time() - started, 1),
        "reps": reps,
        "summary": {
            "reps_measured": len(latencies),
            "reassign_ms_min": min(latencies) if latencies else None,
            "reassign_ms_max": max(latencies) if latencies else None,
            "reassign_ms_mean": round(sum(latencies) / len(latencies), 1) if latencies else None,
            "all_within_10s": all(v <= 10_000 for v in latencies) and len(latencies) == args.reps,
        },
    }
    out = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    for r in reps:
        print(json.dumps(r, ensure_ascii=False))
    print(f"evidence: {out}")
    return 0 if payload["summary"]["all_within_10s"] else 1


if __name__ == "__main__":
    sys.exit(main())

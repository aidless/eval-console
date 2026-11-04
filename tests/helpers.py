"""测试辅助：不连真实外部依赖，全部基于临时 SQLite。"""

from __future__ import annotations

import json
import os
import threading
import time

from evalconsole import db
from evalconsole.worker import Worker


def add_task(
    conn,
    task_id: str,
    *,
    handler: str = "sha",
    args: dict | None = None,
    timeout: float = 5.0,
    max_attempts: int = 3,
    status: str = "pending",
    attempt: int = 0,
    lease_owner: str | None = None,
    lease_expires_at: float | None = None,
    created_at: float | None = None,
    redispatches: int = 0,
):
    ts = db.now() if created_at is None else created_at
    payload = json.dumps({"handler": handler, "args": args or {"seed": task_id, "rounds": 20_000}, "timeout": timeout})
    conn.execute(
        "INSERT INTO tasks(task_id, payload, status, attempt, max_attempts, lease_owner, lease_expires_at,"
        " created_at, updated_at, redispatches)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)",
        (task_id, payload, status, attempt, max_attempts, lease_owner, lease_expires_at, ts, ts, redispatches),
    )


def add_worker(conn, worker_id: str, *, heartbeat_age: float = 0.0, stopped: bool = False, pid: int | None = None):
    ts = db.now() - heartbeat_age
    conn.execute(
        "INSERT INTO workers(worker_id, pid, host, started_at, heartbeat_at, stopped_at)"
        " VALUES(?,?,?,?,?,?)",
        (worker_id, pid, "test-host", ts, ts, db.now() if stopped else None),
    )


def wait_until(conn, sql: str, params: tuple = (), timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = db.scalar(conn, sql, params)
        if value:
            return True
        time.sleep(0.02)
    return False


class RunningWorker:
    """把 Worker 放在线程里跑；进程模型与真实 worker 相同，只是不另起解释器。"""

    def __init__(self, db_path: str, worker_id: str, **kwargs):
        defaults = dict(heartbeat_interval=0.2, lease_ttl=1.0, task_timeout=5.0, poll_interval=0.05)
        defaults.update(kwargs)
        self.worker = Worker(db_path, worker_id, **defaults)
        self.thread = threading.Thread(target=self.worker.run, name=f"worker-{worker_id}", daemon=True)
        self.thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self.worker.stop()
        self.thread.join(timeout)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()


def zombie_children(ppid: int | None = None) -> list[str]:
    """扫描 /proc，找出当前进程下状态为 Z 的子进程。"""
    ppid = os.getpid() if ppid is None else ppid
    found = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/status", "r", encoding="utf-8") as fh:
                data = fh.read()
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        if f"PPid:\t{ppid}" in data and "State:\tZ" in data:
            found.append(entry)
    return found

"""worker 进程：领租约、写心跳、执行任务、落状态。

心跳线程与主线程分离，因此可以复现"进程还活着但心跳停了"这种真实故障
（任务租约到期后由 scheduler 重派，worker 即便后来算完也不能写回结果）。
"""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import socket
import sqlite3
import threading
from typing import Any

from . import db, tasks


class Worker:
    def __init__(
        self,
        db_path: str,
        worker_id: str,
        *,
        heartbeat_interval: float = 1.0,
        lease_ttl: float = 5.0,
        task_timeout: float = 10.0,
        poll_interval: float = 0.2,
    ) -> None:
        self.db_path = db_path
        self.worker_id = worker_id
        self.heartbeat_interval = heartbeat_interval
        self.lease_ttl = lease_ttl
        self.task_timeout = task_timeout
        self.poll_interval = poll_interval

        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._current_task: str | None = None
        self._heartbeat_thread: threading.Thread | None = None

    # ---------- 注册与心跳 ----------

    def register(self, conn: sqlite3.Connection) -> None:
        ts = db.now()
        row = conn.execute(
            "SELECT worker_id, restart_count FROM workers WHERE worker_id=?", (self.worker_id,)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO workers(worker_id, pid, host, started_at, heartbeat_at, stopped_at, restart_count)"
                " VALUES(?,?,?,?,?,NULL,0)",
                (self.worker_id, None, None, ts, ts),
            )
        else:
            conn.execute(
                "UPDATE workers SET pid=?, started_at=?, heartbeat_at=?, stopped_at=NULL, restart_count=restart_count+1"
                " WHERE worker_id=?",
                (None, ts, ts, self.worker_id),
            )
            db.log_event(
                conn,
                "worker_restart",
                worker_id=self.worker_id,
                detail=f"restart_count={int(row['restart_count']) + 1}",
            )
        conn.execute(
            "UPDATE workers SET pid=?, host=? WHERE worker_id=?",
            (os.getpid(), socket.gethostname(), self.worker_id),
        )
        db.log_event(conn, "worker_start", worker_id=self.worker_id, detail=f"pid={os.getpid()}")

    def _heartbeat_loop(self) -> None:
        conn = db.connect(self.db_path)
        try:
            while not self._stop.is_set():
                ts = db.now()
                with self._lock:
                    task_id = self._current_task
                try:
                    conn.execute(
                        "UPDATE workers SET heartbeat_at=?, last_task_id=? WHERE worker_id=?",
                        (ts, task_id, self.worker_id),
                    )
                    conn.execute(
                        "INSERT INTO heartbeat_log(worker_id, ts) VALUES(?,?)", (self.worker_id, ts)
                    )
                    if task_id is not None:
                        # 续租：只有仍然持有该任务时才生效，任务被重派后这里不会复活旧租约。
                        conn.execute(
                            "UPDATE tasks SET lease_expires_at=? WHERE task_id=? AND lease_owner=?"
                            " AND status='running'",
                            (ts + self.lease_ttl, task_id, self.worker_id),
                        )
                except sqlite3.OperationalError:
                    # 心跳写失败不终止进程：正好复现"进程活着但租约不再续"的故障。
                    pass
                self._stop.wait(self.heartbeat_interval)
        finally:
            conn.close()

    # ---------- 领取与执行 ----------

    def claim(self, conn: sqlite3.Connection) -> dict[str, Any] | None:
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            return None
        try:
            row = conn.execute(
                "SELECT task_id, payload, attempt, max_attempts FROM tasks"
                " WHERE status IN ('pending','retried')"
                " ORDER BY created_at, task_id LIMIT 1"
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            ts = db.now()
            cur = conn.execute(
                "UPDATE tasks SET status='running', lease_owner=?, lease_expires_at=?, updated_at=?,"
                " started_at=?, first_lease_at=COALESCE(first_lease_at, ?),"
                " attempt=attempt+1, error=NULL, finished_at=NULL"
                " WHERE task_id=? AND status IN ('pending','retried')",
                (self.worker_id, ts + self.lease_ttl, ts, ts, ts, row["task_id"]),
            )
            if cur.rowcount != 1:
                conn.execute("ROLLBACK")
                return None
            conn.execute("COMMIT")
            return {
                "task_id": row["task_id"],
                "payload": row["payload"],
                "attempt": int(row["attempt"]) + 1,
                "max_attempts": int(row["max_attempts"]),
            }
        except sqlite3.OperationalError:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            return None

    def _execute(self, payload: str) -> tuple[str, str | None]:
        """返回 (状态, 结果/错误)。超时用独立子进程强制回收，避免 worker 主进程卡死。"""
        import json

        data = json.loads(payload or "{}")
        name = data.get("handler", "sha")
        args = data.get("args", {})
        timeout = float(data.get("timeout", self.task_timeout))

        ctx = mp.get_context("spawn")
        parent_conn, child_conn = ctx.Pipe(duplex=False)
        proc = ctx.Process(target=tasks.run_handler, args=(name, args, child_conn), daemon=True)
        proc.start()
        child_conn.close()
        proc.join(timeout)

        timed_out = proc.is_alive()
        if timed_out:
            proc.terminate()
            proc.join(5)
            if proc.is_alive():
                proc.kill()
                proc.join(5)
        message: dict[str, Any] | None = None
        if not timed_out:
            if parent_conn.poll(1.0):
                try:
                    message = parent_conn.recv()
                except EOFError:
                    message = None
        parent_conn.close()

        if timed_out:
            return "timeout", f"task exceeded {timeout}s; child exitcode={proc.exitcode}"
        if message is None:
            return "error", f"child produced no result; exitcode={proc.exitcode}"
        if not message.get("ok"):
            return "error", str(message.get("error"))
        return "ok", str(message.get("result"))

    def _settle(
        self,
        conn: sqlite3.Connection,
        task: dict[str, Any],
        outcome: str,
        detail: str | None,
    ) -> bool:
        """带租约守卫的落库：租约被抢走后写入会被拒绝。"""
        ts = db.now()
        if outcome == "ok":
            status, error, result = "done", None, detail
        else:
            terminal = task["attempt"] >= task["max_attempts"]
            status = "failed" if terminal else "retried"
            error, result = f"{outcome}: {detail}", None
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "UPDATE tasks SET status=?, result=?, error=?, updated_at=?,"
                " finished_at=CASE WHEN ? IN ('done','failed') THEN ? ELSE NULL END,"
                " started_at=CASE WHEN ?='retried' THEN NULL ELSE started_at END,"
                " lease_owner=CASE WHEN ?='retried' THEN NULL ELSE lease_owner END,"
                " lease_expires_at=CASE WHEN ?='retried' THEN NULL ELSE lease_expires_at END"
                " WHERE task_id=? AND lease_owner=? AND status='running'",
                (status, result, error, ts, status, ts, status, status, status, task["task_id"], self.worker_id),
            )
            if cur.rowcount == 0:
                conn.execute("COMMIT")
                db.log_event(
                    conn,
                    "stale_write_rejected",
                    worker_id=self.worker_id,
                    task_id=task["task_id"],
                    detail=f"lease lost; outcome={outcome}",
                )
                return False
            conn.execute("COMMIT")
            db.log_event(
                conn,
                "task_" + status,
                worker_id=self.worker_id,
                task_id=task["task_id"],
                detail=error or "ok",
            )
            return True
        except sqlite3.OperationalError as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            db.log_event(
                conn,
                "settle_db_error",
                worker_id=self.worker_id,
                task_id=task["task_id"],
                detail=str(exc),
            )
            return False

    # ---------- 主循环 ----------

    def run(self) -> None:
        conn = db.connect(self.db_path)
        try:
            self.register(conn)
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, name=f"hb-{self.worker_id}", daemon=True
            )
            self._heartbeat_thread.start()
            while not self._stop.is_set():
                task = self.claim(conn)
                if task is None:
                    self._stop.wait(self.poll_interval)
                    continue
                with self._lock:
                    self._current_task = task["task_id"]
                try:
                    outcome, detail = self._execute(task["payload"])
                except Exception as exc:  # noqa: BLE001 - 执行层异常不能带崩 worker
                    outcome, detail = "error", f"{type(exc).__name__}: {exc}"
                with self._lock:
                    self._current_task = None
                self._settle(conn, task, outcome, detail)
        finally:
            self._stop.set()
            if self._heartbeat_thread is not None:
                self._heartbeat_thread.join(timeout=3)
            try:
                conn.execute("UPDATE workers SET stopped_at=? WHERE worker_id=?", (db.now(), self.worker_id))
                db.log_event(conn, "worker_stop", worker_id=self.worker_id)
            finally:
                conn.close()

    def stop(self) -> None:
        self._stop.set()


def install_signal_handlers(worker: Worker) -> None:
    def _handler(signum, frame):  # noqa: ARG001
        worker.stop()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)

"""判据 2：租约过期 → 重派；旧 worker 的写入必须被拒绝。"""

from __future__ import annotations

from evalconsole import db, scheduler
from evalconsole.worker import Worker

from helpers import add_task, add_worker, wait_until, RunningWorker


def test_reclaim_expired_lease_marks_retried(db_path, conn):
    add_worker(conn, "w-dead", heartbeat_age=30.0, pid=0)
    add_task(
        conn,
        "t-1",
        status="running",
        attempt=1,
        max_attempts=3,
        lease_owner="w-dead",
        lease_expires_at=db.now() - 2.0,
    )
    reclaimed = scheduler.reclaim_expired(conn)
    assert len(reclaimed) == 1 and reclaimed[0]["status"] == "retried"
    row = conn.execute("SELECT status, lease_owner, redispatches FROM tasks WHERE task_id='t-1'").fetchone()
    assert row["status"] == "retried" and row["lease_owner"] is None and row["redispatches"] == 1
    rec = conn.execute("SELECT from_worker, silence_ms, reclaim_ms FROM redispatches WHERE task_id='t-1'").fetchone()
    assert rec["from_worker"] == "w-dead"
    assert rec["silence_ms"] >= 25_000 and rec["reclaim_ms"] > 1_000


def test_reclaim_skips_healthy_lease(db_path, conn):
    add_worker(conn, "w-alive", heartbeat_age=0.0)
    add_task(
        conn,
        "t-2",
        status="running",
        attempt=1,
        lease_owner="w-alive",
        lease_expires_at=db.now() + 30.0,
    )
    assert scheduler.reclaim_expired(conn) == []
    row = conn.execute("SELECT status, lease_owner FROM tasks WHERE task_id='t-2'").fetchone()
    assert row["status"] == "running" and row["lease_owner"] == "w-alive"


def test_reclaim_with_exhausted_attempts_goes_failed(db_path, conn):
    add_worker(conn, "w-dead2", heartbeat_age=30.0, pid=0)
    add_task(
        conn,
        "t-3",
        status="running",
        attempt=3,
        max_attempts=3,
        lease_owner="w-dead2",
        lease_expires_at=db.now() - 1.0,
    )
    scheduler.reclaim_expired(conn)
    row = conn.execute("SELECT status, finished_at FROM tasks WHERE task_id='t-3'").fetchone()
    assert row["status"] == "failed" and row["finished_at"] is not None


def test_stale_lease_write_is_rejected(db_path, conn):
    add_task(conn, "t-4")
    w = Worker(db_path, "w-old", heartbeat_interval=0.2, lease_ttl=1.0)
    task = w.claim(conn)
    conn.execute("UPDATE tasks SET lease_expires_at=? WHERE task_id='t-4'", (db.now() - 5.0,))
    add_worker(conn, "w-old", heartbeat_age=20.0)
    scheduler.reclaim_expired(conn)
    assert conn.execute("SELECT status FROM tasks WHERE task_id='t-4'").fetchone()["status"] == "retried"
    # 旧 worker 算完了想写回：租约已丢，必须被拒绝
    assert w._settle(conn, task, "ok", "late-result") is False
    row = conn.execute("SELECT status, result FROM tasks WHERE task_id='t-4'").fetchone()
    assert row["status"] == "retried" and row["result"] is None
    assert conn.execute("SELECT COUNT(*) FROM events WHERE kind='stale_write_rejected'").fetchone()[0] == 1


def test_reclaimed_task_is_picked_up_by_another_worker(db_path, conn):
    add_worker(conn, "w-dead3", heartbeat_age=30.0, pid=0)
    add_task(
        conn,
        "t-5",
        status="running",
        attempt=1,
        max_attempts=3,
        lease_owner="w-dead3",
        lease_expires_at=db.now() - 1.0,
        args={"seed": "pickup", "rounds": 10_000},
    )
    scheduler.reclaim_expired(conn)
    with RunningWorker(db_path, "w-new"):
        assert wait_until(conn, "SELECT status='done' FROM tasks WHERE task_id='t-5'", timeout=15)
    row = conn.execute("SELECT status, lease_owner, attempt FROM tasks WHERE task_id='t-5'").fetchone()
    assert row["status"] == "done" and row["lease_owner"] == "w-new" and row["attempt"] == 2

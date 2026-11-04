"""判据 3：进程重启不丢状态、不重跑已完成任务；崩溃中的任务可恢复。"""

from __future__ import annotations

import time

from evalconsole import db, scheduler

from helpers import add_task, add_worker, wait_until, RunningWorker


def test_done_tasks_are_not_rerun_after_restart(db_path, conn):
    for i in range(3):
        add_task(conn, f"t-{i}", args={"seed": str(i), "rounds": 10_000})
    with RunningWorker(db_path, "w-first"):
        assert wait_until(conn, "SELECT COUNT(*) = 3 FROM tasks WHERE status='done'", timeout=20)

    snapshot = [dict(r) for r in db.rows(
        conn, "SELECT task_id, attempt, updated_at, result FROM tasks ORDER BY task_id"
    )]
    done_events = int(db.scalar(conn, "SELECT COUNT(*) FROM events WHERE kind='task_done'") or 0)

    with RunningWorker(db_path, "w-second"):
        time.sleep(1.5)

    after = [dict(r) for r in db.rows(
        conn, "SELECT task_id, attempt, updated_at, result FROM tasks ORDER BY task_id"
    )]
    assert after == snapshot, "重启后已完成任务被改动/重跑了"
    assert int(db.scalar(conn, "SELECT COUNT(*) FROM events WHERE kind='task_done'") or 0) == done_events
    # 第二个 worker 注册了，但没领到任何任务
    assert int(db.scalar(conn, "SELECT COUNT(*) FROM heartbeat_log WHERE worker_id='w-second'") or 0) > 0


def test_crashed_running_task_is_recovered_after_restart(db_path, conn):
    add_worker(conn, "w-crashed", heartbeat_age=30.0, pid=0)
    add_task(
        conn,
        "t-crash",
        status="running",
        attempt=1,
        max_attempts=3,
        lease_owner="w-crashed",
        lease_expires_at=db.now() - 3.0,
    )
    reclaimed = scheduler.reclaim_expired(conn)
    assert len(reclaimed) == 1
    with RunningWorker(db_path, "w-recover"):
        assert wait_until(conn, "SELECT status='done' FROM tasks WHERE task_id='t-crash'", timeout=20)
    row = conn.execute("SELECT attempt, lease_owner FROM tasks WHERE task_id='t-crash'").fetchone()
    assert row["attempt"] == 2 and row["lease_owner"] == "w-recover"


def test_schema_init_is_idempotent(db_path, conn):
    db.init_db(db_path)
    db.init_db(db_path)
    version = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]
    assert version == str(db.SCHEMA_VERSION)
    assert int(db.scalar(conn, "SELECT COUNT(*) FROM sqlite_master WHERE type='table'")) >= 8

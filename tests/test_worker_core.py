"""判据 1/3：worker 领取、心跳、状态落库、控制台可读。"""

from __future__ import annotations

import time

from evalconsole import console, db
from evalconsole.worker import Worker

from helpers import add_task, add_worker, wait_until, RunningWorker


def test_claim_sets_running_lease_and_attempt(db_path, conn):
    add_task(conn, "t-1")
    w = Worker(db_path, "w1", heartbeat_interval=0.2, lease_ttl=2.0, task_timeout=5.0)
    task = w.claim(conn)
    assert task is not None and task["task_id"] == "t-1"
    row = conn.execute("SELECT status, lease_owner, lease_expires_at, attempt FROM tasks WHERE task_id='t-1'").fetchone()
    assert row["status"] == "running"
    assert row["lease_owner"] == "w1"
    assert row["attempt"] == 1
    assert float(row["lease_expires_at"]) > db.now()


def test_settle_done_is_terminal(db_path, conn):
    add_task(conn, "t-2")
    w = Worker(db_path, "w1", heartbeat_interval=0.2, lease_ttl=2.0)
    task = w.claim(conn)
    assert w._settle(conn, task, "ok", "hash-result") is True
    row = conn.execute("SELECT status, result, finished_at FROM tasks WHERE task_id='t-2'").fetchone()
    assert row["status"] == "done" and row["result"] == "hash-result" and row["finished_at"] is not None


def test_retry_then_terminal_failed(db_path, conn):
    add_task(conn, "t-3", max_attempts=2, handler="fail", args={"message": "always"})
    w = Worker(db_path, "w1", heartbeat_interval=0.2, lease_ttl=2.0)
    first = w.claim(conn)
    assert w._settle(conn, first, "error", "boom") is True
    row = conn.execute("SELECT status, attempt, lease_owner FROM tasks WHERE task_id='t-3'").fetchone()
    assert row["status"] == "retried"
    assert row["lease_owner"] is None

    second = w.claim(conn)
    assert second["attempt"] == 2
    assert w._settle(conn, second, "error", "boom") is True
    row = conn.execute("SELECT status FROM tasks WHERE task_id='t-3'").fetchone()
    assert row["status"] == "failed"


def test_heartbeat_writes_row_and_log(db_path, conn):
    add_worker(conn, "seed-worker")
    conn.execute("DELETE FROM workers")
    with RunningWorker(db_path, "w-hb"):
        assert wait_until(conn, "SELECT COUNT(*) >= 3 FROM heartbeat_log WHERE worker_id='w-hb'", timeout=5)
        row = conn.execute("SELECT heartbeat_at, pid FROM workers WHERE worker_id='w-hb'").fetchone()
        assert row["pid"] is not None
        assert db.now() - float(row["heartbeat_at"]) < 2.0
        live_state = {w["worker_id"]: w["state"] for w in console.worker_status(conn)}["w-hb"]
        assert live_state == "alive"
    # 退出后必须落成 stopped，而不是永远显示 alive
    stopped_state = {w["worker_id"]: w["state"] for w in console.worker_status(conn)}["w-hb"]
    assert stopped_state == "stopped"


def test_worker_runs_tasks_end_to_end(db_path, conn):
    add_task(conn, "t-4", args={"seed": "a", "rounds": 10_000})
    add_task(conn, "t-5", args={"seed": "b", "rounds": 10_000})
    with RunningWorker(db_path, "w-run"):
        assert wait_until(
            conn, "SELECT COUNT(*) = 2 FROM tasks WHERE status='done'", timeout=15
        )
    rows = db.rows(conn, "SELECT task_id, result, attempt FROM tasks ORDER BY task_id")
    assert [r["task_id"] for r in rows] == ["t-4", "t-5"]
    assert all(r["attempt"] == 1 and r["result"] for r in rows)


def test_console_marks_stale_and_stopped(db_path, conn):
    add_worker(conn, "w-old", heartbeat_age=40.0)
    add_worker(conn, "w-off", heartbeat_age=40.0, stopped=True)
    add_worker(conn, "w-live", heartbeat_age=0.5)
    states = {w["worker_id"]: w["state"] for w in console.worker_status(conn)}
    assert states == {"w-old": "stale", "w-off": "stopped", "w-live": "alive"}
    text = console.render_text(conn)
    assert "w-old" in text and "stale" in text
    assert "pending=" in text and "done=" in text


def test_console_html_contains_kpis(db_path, conn):
    add_task(conn, "t-6", status="done")
    add_task(conn, "t-7", status="failed")
    add_worker(conn, "w1")
    html = console.render_html(conn, stale_after=1.0)
    assert "eval-console" in html
    assert html.count("<table") == 2
    assert "成功率" in html

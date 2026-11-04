"""失效模式用例：每一条都是一个真实会发生、且真的能跑起来的坏情况。

1. 进程还活着但心跳线程停了            -> 租约过期后被重派
2. 任务卡死（超过超时）                -> 记 retried，子进程被终止并回收，无僵尸
3. SQLite 并发写                      -> 多进程/多线程写不报 database is locked
4. 重复派发                           -> 同一个任务只会有一个 owner
5. 重试次数耗尽                        -> 终态 failed，不再被领取
6. 进程重启                           -> 已完成任务不重跑
7. 迟到的结果写回                      -> 租约丢失后被拒绝
8. worker 心跳丢失告警                 -> 规则触发且不重复刷屏
9. 空队列 / 空库                       -> 调度周期不崩、不产生假任务
10. 重复 init_db                       -> schema 幂等，不丢数据
"""

from __future__ import annotations

import sqlite3
import threading
import time

from evalconsole import alerts, db, scheduler
from evalconsole.worker import Worker

from helpers import add_task, add_worker, wait_until, RunningWorker, zombie_children


def test_stalled_heartbeat_worker_is_reclaimed_while_alive(db_path, conn):
    add_task(conn, "t-stall", handler="sleep", args={"seconds": 3.0}, timeout=10.0)
    # 心跳间隔 30s >> 租约 1s：进程活着，但租约必然过期
    with RunningWorker(db_path, "w-stall", heartbeat_interval=30.0, lease_ttl=1.0, task_timeout=10.0):
        assert wait_until(conn, "SELECT status='running' FROM tasks WHERE task_id='t-stall'", timeout=10)
        time.sleep(1.6)
        reclaimed = scheduler.reclaim_expired(conn)
    assert len(reclaimed) == 1
    row = conn.execute("SELECT status, redispatches FROM tasks WHERE task_id='t-stall'").fetchone()
    assert row["status"] in ("retried", "failed")
    assert row["redispatches"] == 1


def test_task_timeout_retried_and_child_reaped(db_path, conn):
    add_task(conn, "t-slow", handler="sleep", args={"seconds": 5.0}, timeout=0.3, max_attempts=1)
    with RunningWorker(db_path, "w-to"):
        assert wait_until(
            conn,
            "SELECT status = 'failed' FROM tasks WHERE task_id='t-slow'",
            timeout=10,
        )
    row = conn.execute("SELECT status, error FROM tasks WHERE task_id='t-slow'").fetchone()
    assert row["status"] == "failed"
    assert "timeout" in (row["error"] or "")
    assert zombie_children() == []


def test_concurrent_sqlite_writers_do_not_fail(db_path, conn):
    add_worker(conn, "w-shared")
    errors: list[str] = []

    def writer(n: int) -> None:
        c = db.connect(db_path)
        try:
            for i in range(30):
                c.execute(
                    "INSERT INTO events(ts, kind, worker_id, detail) VALUES(?,?,?,?)",
                    (db.now(), f"stress-{n}", "w-shared", str(i)),
                )
                c.execute("UPDATE workers SET heartbeat_at=? WHERE worker_id='w-shared'", (db.now(),))
        except sqlite3.OperationalError as exc:  # 只有真的锁死才会走到这里
            errors.append(str(exc))
        finally:
            c.close()

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert int(db.scalar(conn, "SELECT COUNT(*) FROM events WHERE kind LIKE 'stress-%'")) == 120


def test_duplicate_dispatch_only_one_owner(db_path, conn):
    for i in range(40):
        add_task(conn, f"t-c{i}")
    claimed: list[str] = []
    lock = threading.Lock()

    def consumer(n: int) -> None:
        c = db.connect(db_path)
        w = Worker(db_path, f"w-{n}", heartbeat_interval=1.0, lease_ttl=5.0)
        try:
            while True:
                task = w.claim(c)
                if task is None:
                    break
                with lock:
                    claimed.append(task["task_id"])
        finally:
            c.close()

    threads = [threading.Thread(target=consumer, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(claimed) == 40
    assert len(set(claimed)) == 40, "同一个任务被派发给了两个 owner"
    owners = conn.execute("SELECT COUNT(DISTINCT lease_owner) FROM tasks").fetchone()[0]
    assert 1 <= owners <= 4


def test_attempts_exhausted_goes_failed_and_not_reclaimed(db_path, conn):
    add_task(conn, "t-done-attempts", handler="fail", args={"message": "x"}, max_attempts=1)
    with RunningWorker(db_path, "w-fail"):
        assert wait_until(conn, "SELECT status='failed' FROM tasks WHERE task_id='t-done-attempts'", timeout=10)
    assert conn.execute("SELECT status FROM tasks WHERE task_id='t-done-attempts'").fetchone()["status"] == "failed"
    # 失败任务不会被再次领取
    w = Worker(db_path, "w-late")
    assert w.claim(conn) is None


def test_restart_does_not_rerun_finished_task(db_path, conn):
    add_task(conn, "t-once", args={"seed": "once", "rounds": 5_000})
    with RunningWorker(db_path, "w-a"):
        assert wait_until(conn, "SELECT status='done' FROM tasks WHERE task_id='t-once'", timeout=10)
    before = conn.execute("SELECT attempt, updated_at FROM tasks WHERE task_id='t-once'").fetchone()
    with RunningWorker(db_path, "w-b"):
        time.sleep(1.0)
    after = conn.execute("SELECT attempt, updated_at FROM tasks WHERE task_id='t-once'").fetchone()
    assert (before["attempt"], before["updated_at"]) == (after["attempt"], after["updated_at"])


def test_late_result_from_stale_lease_is_rejected(db_path, conn):
    add_task(conn, "t-late")
    w = Worker(db_path, "w-slow", heartbeat_interval=0.2, lease_ttl=1.0)
    task = w.claim(conn)
    add_worker(conn, "w-slow", heartbeat_age=20.0)
    conn.execute("UPDATE tasks SET lease_expires_at=? WHERE task_id='t-late'", (db.now() - 5.0,))
    scheduler.reclaim_expired(conn)
    assert w._settle(conn, task, "ok", "late") is False
    assert conn.execute("SELECT status FROM tasks WHERE task_id='t-late'").fetchone()["status"] == "retried"


def test_stale_worker_alert_once(db_path, conn):
    add_worker(conn, "w-vanish", heartbeat_age=30.0)
    first = alerts.evaluate(conn, {"stale_after_s": 10.0, "cooldown_s": 3600.0})
    second = alerts.evaluate(conn, {"stale_after_s": 10.0, "cooldown_s": 3600.0})
    assert len(first) == 1 and second == []


def test_empty_database_cycle_is_safe(db_path, conn):
    assert scheduler.reclaim_expired(conn) == []
    assert alerts.evaluate(conn) == []
    assert int(db.scalar(conn, "SELECT COUNT(*) FROM tasks")) == 0


def test_reopen_db_keeps_state_and_schema_idempotent(db_path, conn):
    add_task(conn, "t-persist", status="done")
    conn.close()
    db.init_db(db_path)
    c2 = db.connect(db_path)
    try:
        row = c2.execute("SELECT status FROM tasks WHERE task_id='t-persist'").fetchone()
        assert row["status"] == "done"
        assert c2.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0] >= 8
    finally:
        c2.close()

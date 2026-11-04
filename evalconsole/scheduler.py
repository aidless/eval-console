"""调度器：回收过期租约、把任务放回可领取队列、触发告警。

自愈语义：
  1. worker 每 1s 续租，租约 5s；
  2. worker 被 SIGKILL / 心跳线程死亡后不再续租；
  3. 租约到期，调度器把任务从 running 改回 retried（attempt 已用尽则 failed）；
  4. 其他 worker 在 ≤ poll_interval 内领走任务。
"""

from __future__ import annotations

import sqlite3
from typing import Any

from . import alerts, db


def reclaim_expired(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """把租约过期的 running 任务收回。返回本次回收的明细。"""
    ts = db.now()
    reclaimed: list[dict[str, Any]] = []
    rows = db.rows(
        conn,
        "SELECT task_id, attempt, max_attempts, lease_owner, lease_expires_at FROM tasks"
        " WHERE status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?",
        (ts,),
    )
    for row in rows:
        owner = row["lease_owner"]
        silence_ms = None
        if owner:
            hb = conn.execute(
                "SELECT heartbeat_at FROM workers WHERE worker_id=?", (owner,)
            ).fetchone()
            if hb is not None:
                silence_ms = (ts - float(hb["heartbeat_at"])) * 1000.0
        terminal = int(row["attempt"]) >= int(row["max_attempts"])
        status = "failed" if terminal else "retried"
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "UPDATE tasks SET status=?, updated_at=?,"
                " lease_owner=NULL, lease_expires_at=NULL,"
                " redispatches=redispatches+1,"
                " error=?, finished_at=CASE WHEN ?='failed' THEN ? ELSE NULL END,"
                " started_at=CASE WHEN ?='retried' THEN NULL ELSE started_at END"
                " WHERE task_id=? AND status='running' AND lease_owner IS ?"
                " AND lease_expires_at IS NOT NULL AND lease_expires_at < ?",
                (
                    status,
                    ts,
                    "lease_expired: worker heartbeat stopped",
                    status,
                    ts,
                    status,
                    row["task_id"],
                    owner,
                    ts,
                ),
            )
            if cur.rowcount != 1:
                conn.execute("COMMIT")
                continue
            conn.execute(
                "INSERT INTO redispatches(ts, task_id, from_worker, to_worker, silence_ms, reclaim_ms)"
                " VALUES(?,?,?,?,?,?)",
                (ts, row["task_id"], owner, None, silence_ms, (ts - float(row["lease_expires_at"])) * 1000.0),
            )
            conn.execute("COMMIT")
        except sqlite3.OperationalError:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            continue
        db.log_event(
            conn,
            "task_reclaimed",
            worker_id=owner,
            task_id=row["task_id"],
            detail=f"status={status} silence_ms={silence_ms}",
        )
        reclaimed.append(
            {
                "task_id": row["task_id"],
                "from_worker": owner,
                "status": status,
                "silence_ms": silence_ms,
                "ts": ts,
            }
        )
    return reclaimed


def run_once(conn: sqlite3.Connection, alert_cfg: dict | None = None, alerts_path: str | None = None) -> None:
    reclaimed = reclaim_expired(conn)
    fired = alerts.evaluate(conn, alert_cfg or {})
    if alerts_path:
        alerts.append_jsonl(alerts_path, fired)
    if reclaimed:
        db.log_event(conn, "scheduler_cycle", detail=f"reclaimed={len(reclaimed)}")
    if fired:
        db.log_event(conn, "scheduler_cycle", detail=f"alerts={len(fired)}")

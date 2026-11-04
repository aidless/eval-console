"""两条告警规则：worker 心跳丢失、失败率超阈值。

规则不写死在调度循环里，纯函数 evaluate() 便于单测。
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from . import db

DEFAULTS = {
    "stale_after_s": 15.0,
    "failure_rate_threshold": 0.20,
    "min_terminal_tasks": 20,
    "cooldown_s": 60.0,
}


def cfg_with_defaults(cfg: dict | None) -> dict[str, Any]:
    merged = dict(DEFAULTS)
    if cfg:
        merged.update(cfg)
    return merged


def _emit(
    conn: sqlite3.Connection,
    *,
    rule: str,
    entity: str,
    message: str,
    value: float,
    threshold: float,
    cd: float,
) -> dict[str, Any] | None:
    key = f"{rule}|{entity}"
    ts = db.now()
    row = conn.execute("SELECT last_fired FROM alert_state WHERE key=?", (key,)).fetchone()
    if row is not None and (ts - float(row["last_fired"])) < cd:
        return None
    conn.execute(
        "INSERT INTO alerts(ts, rule, entity, message, value, threshold) VALUES(?,?,?,?,?,?)",
        (ts, rule, entity, message, value, threshold),
    )
    conn.execute(
        "INSERT INTO alert_state(key, last_fired, last_value) VALUES(?,?,?)"
        " ON CONFLICT(key) DO UPDATE SET last_fired=excluded.last_fired, last_value=excluded.last_value",
        (key, ts, value),
    )
    return {
        "ts": ts,
        "rule": rule,
        "entity": entity,
        "message": message,
        "value": value,
        "threshold": threshold,
    }


def evaluate(conn: sqlite3.Connection, cfg: dict | None = None) -> list[dict[str, Any]]:
    c = cfg_with_defaults(cfg)
    fired: list[dict[str, Any]] = []
    ts = db.now()

    # 规则 1：worker 心跳超过 stale_after_s 未更新（且未正常退出）
    for w in db.rows(conn, "SELECT worker_id, heartbeat_at, stopped_at FROM workers"):
        if w["stopped_at"] is not None:
            continue
        age = ts - float(w["heartbeat_at"])
        if age > float(c["stale_after_s"]):
            item = _emit(
                conn,
                rule="stale_worker",
                entity=str(w["worker_id"]),
                message=f"worker {w['worker_id']} 心跳已停 {age:.1f}s（阈值 {c['stale_after_s']}s）",
                value=age,
                threshold=float(c["stale_after_s"]),
                cd=float(c["cooldown_s"]),
            )
            if item:
                fired.append(item)

    # 规则 2：失败率超阈值（分母只算终态任务，且必须有足够样本）
    total = int(db.scalar(conn, "SELECT COUNT(*) FROM tasks WHERE status IN ('done','failed')") or 0)
    failed = int(db.scalar(conn, "SELECT COUNT(*) FROM tasks WHERE status='failed'") or 0)
    if total >= int(c["min_terminal_tasks"]):
        rate = failed / total
        if rate > float(c["failure_rate_threshold"]):
            item = _emit(
                conn,
                rule="failure_rate",
                entity="global",
                message=f"失败率 {rate:.1%}（{failed}/{total}，阈值 {c['failure_rate_threshold']:.0%}）",
                value=rate,
                threshold=float(c["failure_rate_threshold"]),
                cd=float(c["cooldown_s"]),
            )
            if item:
                fired.append(item)

    return fired


def append_jsonl(path: str | None, items: list[dict[str, Any]]) -> None:
    if not path or not items:
        return
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        for item in items:
            fh.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")

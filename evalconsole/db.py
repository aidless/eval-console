"""SQLite 存取层：schema、连接、通用查询。

只使用标准库 sqlite3。所有进程各自持有独立连接（WAL 模式下可多进程读写）。
"""

from __future__ import annotations

import os
import sqlite3
import time
from typing import Any

SCHEMA_VERSION = 1

# 任务状态机：
#   pending -> running -> done
#                      -> failed（可重试时先转 retried）
#   running（租约过期）-> retried -> running -> ...
#   retried 是"可立即被领取"的中间态，不会长时间停留。
TASK_STATES = ("pending", "running", "done", "failed", "retried")
TERMINAL_STATES = ("done", "failed")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id          TEXT PRIMARY KEY,
    payload          TEXT NOT NULL DEFAULT '{}',
    status           TEXT NOT NULL DEFAULT 'pending',
    attempt          INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 3,
    lease_owner      TEXT,
    lease_expires_at REAL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    started_at       REAL,
    finished_at      REAL,
    first_lease_at   REAL,
    result           TEXT,
    error            TEXT,
    redispatches     INTEGER NOT NULL DEFAULT 0,
    CHECK (status IN ('pending','running','done','failed','retried'))
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id    TEXT PRIMARY KEY,
    pid          INTEGER,
    host         TEXT,
    started_at   REAL NOT NULL,
    heartbeat_at REAL NOT NULL,
    stopped_at   REAL,
    restart_count INTEGER NOT NULL DEFAULT 0,
    last_task_id TEXT
);

-- 心跳流水，用于计算"心跳是否中断"以及最大间隔。每个心跳一行。
CREATE TABLE IF NOT EXISTS heartbeat_log (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id TEXT NOT NULL,
    ts        REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        REAL NOT NULL,
    kind      TEXT NOT NULL,
    worker_id TEXT,
    task_id   TEXT,
    detail    TEXT
);

-- 每次"租约过期后重派"都记一行，可用于核对自愈次数与静默时长。
CREATE TABLE IF NOT EXISTS redispatches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    task_id     TEXT NOT NULL,
    from_worker TEXT,
    to_worker   TEXT,
    silence_ms  REAL,
    reclaim_ms  REAL
);

CREATE TABLE IF NOT EXISTS alerts (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        REAL NOT NULL,
    rule      TEXT NOT NULL,
    entity    TEXT,
    message   TEXT NOT NULL,
    value     REAL,
    threshold REAL
);

-- 告警去重：同一规则 + 同一实体在冷却期内只发一次。
CREATE TABLE IF NOT EXISTS alert_state (
    key        TEXT PRIMARY KEY,
    last_fired REAL NOT NULL,
    last_value REAL
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_created ON tasks(status, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_lease ON tasks(lease_owner);
CREATE INDEX IF NOT EXISTS idx_hb_worker_ts ON heartbeat_log(worker_id, ts);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
"""


def now() -> float:
    """统一的时间戳（epoch 秒，浮点）。跨进程比对时必须用同一时钟。"""
    return time.time()


def connect(path: str, timeout: float = 5.0) -> sqlite3.Connection:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path, timeout=timeout, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(path: str) -> None:
    """建库/补表，可重复执行（幂等）。"""
    conn = connect(path)
    try:
        for stmt in [s.strip() for s in SCHEMA_SQL.split(";") if s.strip()]:
            conn.execute(stmt)
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )
    finally:
        conn.close()


def log_event(
    conn: sqlite3.Connection,
    kind: str,
    *,
    worker_id: str | None = None,
    task_id: str | None = None,
    detail: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO events(ts, kind, worker_id, task_id, detail) VALUES(?,?,?,?,?)",
        (now(), kind, worker_id, task_id, detail),
    )


def scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    return None if row is None else row[0]


def rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    return list(conn.execute(sql, params).fetchall())

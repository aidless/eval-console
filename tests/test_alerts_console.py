"""告警规则：心跳丢失、失败率超阈值，以及去重/冷却。"""

from __future__ import annotations

import json

from evalconsole import alerts, console, db

from helpers import add_task, add_worker


def test_stale_worker_alert_fires_once_per_cooldown(db_path, conn):
    add_worker(conn, "w-stale", heartbeat_age=40.0)
    fired = alerts.evaluate(conn, {"stale_after_s": 15.0, "cooldown_s": 60.0})
    assert len(fired) == 1
    assert fired[0]["rule"] == "stale_worker" and fired[0]["entity"] == "w-stale"
    assert fired[0]["value"] >= 39.0
    assert alerts.evaluate(conn, {"stale_after_s": 15.0, "cooldown_s": 60.0}) == []


def test_stale_worker_alert_skips_gracefully_stopped(db_path, conn):
    add_worker(conn, "w-gone", heartbeat_age=40.0, stopped=True)
    assert alerts.evaluate(conn) == []


def test_failure_rate_alert_requires_min_sample(db_path, conn):
    for i in range(3):
        add_task(conn, f"t-{i}", status="failed")
    for i in range(2):
        add_task(conn, f"t-s{i}", status="done")
    assert alerts.evaluate(conn, {"min_terminal_tasks": 20}) == []


def test_failure_rate_alert_fires_above_threshold(db_path, conn):
    for i in range(18):
        add_task(conn, f"t-d{i}", status="done")
    for i in range(7):
        add_task(conn, f"t-f{i}", status="failed")
    fired = alerts.evaluate(conn, {"failure_rate_threshold": 0.20, "min_terminal_tasks": 20})
    assert len(fired) == 1 and fired[0]["rule"] == "failure_rate"
    assert fired[0]["value"] > 0.20


def test_healthy_fleet_produces_no_alert(db_path, conn):
    add_worker(conn, "w-ok", heartbeat_age=0.5)
    add_task(conn, "t-ok", status="done")
    assert alerts.evaluate(conn) == []


def test_alerts_jsonl_is_appended(db_path, conn, tmp_path):
    add_worker(conn, "w-stale2", heartbeat_age=40.0)
    fired = alerts.evaluate(conn)
    path = str(tmp_path / "alerts.jsonl")
    alerts.append_jsonl(path, fired)
    lines = open(path, encoding="utf-8").read().strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["rule"] == "stale_worker"


def test_dashboard_reflects_counts(db_path, conn):
    for i in range(5):
        add_task(conn, f"t-{i}", status="done")
    add_task(conn, "t-bad", status="failed")
    add_worker(conn, "w1")
    summary = console.task_summary(conn)
    assert summary["counts"]["done"] == 5 and summary["counts"]["failed"] == 1
    assert abs(summary["success_rate"] - 5 / 6) < 1e-9

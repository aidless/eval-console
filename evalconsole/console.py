"""控制台：把 SQLite 里的 worker 心跳与任务状态读成「谁活着、多久没动」的表，
并可导出单文件静态 HTML 看板（无外部资源、无第三方依赖）。
"""

from __future__ import annotations

import html
import os
import sqlite3
import time
from typing import Any

from . import db


def worker_status(conn: sqlite3.Connection, stale_after: float = 15.0) -> list[dict[str, Any]]:
    ts = db.now()
    out: list[dict[str, Any]] = []
    for w in db.rows(
        conn, "SELECT worker_id, pid, host, started_at, heartbeat_at, stopped_at, restart_count, last_task_id"
        " FROM workers ORDER BY worker_id"
    ):
        age = ts - float(w["heartbeat_at"])
        if w["stopped_at"] is not None:
            state = "stopped"
        elif age > stale_after:
            state = "stale"
        else:
            state = "alive"
        out.append(
            {
                "worker_id": w["worker_id"],
                "pid": w["pid"],
                "host": w["host"],
                "state": state,
                "heartbeat_age_s": round(age, 2),
                "last_task_id": w["last_task_id"],
                "restart_count": int(w["restart_count"]),
                "started_at": float(w["started_at"]),
            }
        )
    return out


def task_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    counts = {s: 0 for s in db.TASK_STATES}
    for row in db.rows(conn, "SELECT status, COUNT(*) AS n FROM tasks GROUP BY status"):
        counts[str(row["status"])] = int(row["n"])
    terminal = counts["done"] + counts["failed"]
    durations = [
        float(r["d"])
        for r in db.rows(
            conn, "SELECT finished_at - started_at AS d FROM tasks"
            " WHERE status='done' AND finished_at IS NOT NULL AND started_at IS NOT NULL"
        )
    ]
    p95 = 0.0
    if durations:
        durations.sort()
        p95 = durations[min(len(durations) - 1, int(round(0.95 * (len(durations) - 1))))]
    return {
        "counts": counts,
        "terminal": terminal,
        "success_rate": (counts["done"] / terminal) if terminal else None,
        "p95_latency_s": round(p95, 3),
        "redispatches": int(db.scalar(conn, "SELECT COALESCE(SUM(redispatches),0) FROM tasks") or 0),
        "alerts": int(db.scalar(conn, "SELECT COUNT(*) FROM alerts") or 0),
    }


def heartbeat_gap(conn: sqlite3.Connection, worker_id: str) -> float:
    """worker 心跳流水的最大间隔（秒）。用于检查心跳是否"断过"。"""
    ts_rows = [float(r["ts"]) for r in db.rows(
        conn, "SELECT ts FROM heartbeat_log WHERE worker_id=? ORDER BY ts", (worker_id,)
    )]
    if len(ts_rows) < 2:
        return 0.0
    return max(b - a for a, b in zip(ts_rows, ts_rows[1:]))


def heartbeat_gap_between(conn: sqlite3.Connection, worker_id: str, t0: float, t1: float) -> float:
    """只统计 [t0, t1] 窗口内的心跳间隔，用于排除"被 SIGKILL 期间"的心跳中断。"""
    ts_rows = [
        float(r["ts"])
        for r in db.rows(
            conn,
            "SELECT ts FROM heartbeat_log WHERE worker_id=? AND ts BETWEEN ? AND ? ORDER BY ts",
            (worker_id, t0, t1),
        )
    ]
    if len(ts_rows) < 2:
        return 0.0
    return max(b - a for a, b in zip(ts_rows, ts_rows[1:]))


def render_text(conn: sqlite3.Connection, stale_after: float = 15.0) -> str:
    summary = task_summary(conn)
    lines = [
        f"== worker 心跳（{db.now():.0f}，> {stale_after:.0f}s 未更新判定为 stale）==",
        f"{'worker':<12}{'pid':>8}  {'state':<9}{'hb_age(s)':>11}{'restarts':>9}  {'last_task':<16}",
    ]
    for w in worker_status(conn, stale_after):
        lines.append(
            f"{w['worker_id']:<12}{str(w['pid']):>8}  {w['state']:<9}{w['heartbeat_age_s']:>11.2f}"
            f"{w['restart_count']:>9}  {str(w['last_task_id'] or '-'):<16}"
        )
    c = summary["counts"]
    lines += [
        "",
        "== 任务状态 ==",
        f"pending={c['pending']} running={c['running']} retried={c['retried']} "
        f"done={c['done']} failed={c['failed']}",
        f"success_rate={summary['success_rate']} p95_latency={summary['p95_latency_s']}s "
        f"redispatches={summary['redispatches']} alerts={summary['alerts']}",
    ]
    return "\n".join(lines)


def render_html(conn: sqlite3.Connection, stale_after: float = 15.0) -> str:
    workers = worker_status(conn, stale_after)
    s = task_summary(conn)
    c = s["counts"]
    alerts = db.rows(conn, "SELECT ts, rule, entity, message FROM alerts ORDER BY ts DESC LIMIT 20")
    wrows = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(w['worker_id']))}</td><td>{w['pid']}</td>"
        f"<td class='{w['state']}'>{w['state']}</td>"
        f"<td>{w['heartbeat_age_s']:.2f}</td><td>{w['restart_count']}</td>"
        f"<td>{html.escape(str(w['last_task_id'] or '-'))}</td>"
        "</tr>"
        for w in workers
    )
    arows = "\n".join(
        "<tr>"
        f"<td>{time.strftime('%H:%M:%S', time.localtime(float(a['ts'])))}</td>"
        f"<td>{html.escape(str(a['rule']))}</td><td>{html.escape(str(a['entity']))}</td>"
        f"<td>{html.escape(str(a['message']))}</td>"
        "</tr>"
        for a in alerts
    ) or "<tr><td colspan='4'>无告警</td></tr>"
    rate = "-" if s["success_rate"] is None else f"{s['success_rate']:.1%}"
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>eval-console · fleet 看板</title>
<style>
 body{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:24px;background:#fafafa;color:#1f2328}}
 h1{{font-size:20px;margin:0 0 4px}} .sub{{color:#6b7280;font-size:12px;margin-bottom:18px}}
 .kpis{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px}}
 .kpi{{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:12px 16px;min-width:110px}}
 .kpi b{{display:block;font-size:22px}} .kpi span{{color:#6b7280;font-size:12px}}
 table{{border-collapse:collapse;background:#fff;border:1px solid #e5e7eb;margin-bottom:18px;width:100%}}
 th,td{{padding:7px 10px;border-bottom:1px solid #eef0f2;text-align:left;font-size:13px}}
 th{{background:#f6f7f9}} .alive{{color:#15803d;font-weight:600}} .stale{{color:#b45309;font-weight:600}}
 .stopped{{color:#6b7280}}
</style></head><body>
<h1>eval-console · 评测 fleet 看板</h1>
<div class="sub">生成于 {time.strftime('%Y-%m-%d %H:%M:%S')} ｜ 数据源: SQLite ｜ stale 阈值 {stale_after:.0f}s</div>
<div class="kpis">
 <div class="kpi"><b>{sum(c.values())}</b><span>任务总数</span></div>
 <div class="kpi"><b>{c['done']}</b><span>done</span></div>
 <div class="kpi"><b>{c['failed']}</b><span>failed</span></div>
 <div class="kpi"><b>{c['running']}</b><span>running</span></div>
 <div class="kpi"><b>{rate}</b><span>成功率</span></div>
 <div class="kpi"><b>{s['p95_latency_s']}s</b><span>P95 时延</span></div>
 <div class="kpi"><b>{s['redispatches']}</b><span>重派次数</span></div>
</div>
<table><thead><tr><th>worker</th><th>pid</th><th>状态</th><th>心跳龄(s)</th><th>重启</th><th>当前/最近任务</th></tr></thead>
<tbody>{wrows}</tbody></table>
<table><thead><tr><th>时间</th><th>规则</th><th>对象</th><th>消息</th></tr></thead>
<tbody>{arows}</tbody></table>
</body></html>"""


def write_html(conn: sqlite3.Connection, path: str, stale_after: float = 15.0) -> str:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(render_html(conn, stale_after))
    return path

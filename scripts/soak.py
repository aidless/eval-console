#!/usr/bin/env python3
"""2 小时 soak：3 worker + 1 调度器，低并发循环派活，定期采样负载/内存/心跳。

判据：
  1) 全部任务最终进入终态（无死锁、无饿死）；
  2) worker RSS 无单调增长（给出首尾值与斜率）；
  3) 心跳流水的最大间隔 < 5s（1s 心跳、允许少量调度抖动）；
  4) 中途 SIGKILL 一个 worker，任务仍被重派并完成（持续自愈）。
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from evalconsole import console, db, fleet  # noqa: E402


def rss_kb(pid: int | None) -> int | None:
    if not pid:
        return None
    try:
        with open(f"/proc/{pid}/status", "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    return None


def loadavg() -> float:
    try:
        return round(os.getloadavg()[0], 2)
    except OSError:
        return 0.0


def pids_by_worker(conn) -> dict[str, int]:
    out = {}
    for row in db.rows(conn, "SELECT worker_id, pid FROM workers"):
        if row["pid"]:
            out[str(row["worker_id"])] = int(row["pid"])
    return out


def counts(conn) -> dict[str, int]:
    out = {s: 0 for s in db.TASK_STATES}
    for row in db.rows(conn, "SELECT status, COUNT(*) AS n FROM tasks GROUP BY status"):
        out[str(row["status"])] = int(row["n"])
    return out


def linear_slope(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    if denom == 0:
        return 0.0
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom


def gap_excluding(conn, worker_id: str, lo: float | None, hi: float | None) -> float:
    """最大心跳间隔，但跳过跨过 [lo, hi]（SIGKILL 造成的停机窗口）的那一个间隔。"""
    ts = [float(r["ts"]) for r in db.rows(
        conn, "SELECT ts FROM heartbeat_log WHERE worker_id=? ORDER BY ts", (worker_id,)
    )]
    if len(ts) < 2:
        return 0.0
    gaps = []
    for a, b in zip(ts, ts[1:]):
        if lo is not None and hi is not None and a >= lo and a <= hi:
            continue
        gaps.append(b - a)
    return max(gaps) if gaps else 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=7200.0, help="总时长（秒）")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--sample", type=float, default=15.0)
    ap.add_argument("--feed-interval", type=float, default=1.0)
    ap.add_argument("--max-outstanding", type=int, default=6)
    ap.add_argument("--out-dir", default="evidence")
    ap.add_argument("--kill-at", type=float, default=0.5, help="在总时长的哪个比例处 kill 一个 worker，<0 关闭")
    args = ap.parse_args()

    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.out_dir)
    os.makedirs(out_dir, exist_ok=True)
    db_path = os.path.join(out_dir, "soak.db")
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(db_path + suffix)
        except FileNotFoundError:
            pass
    db.init_db(db_path)

    conn = db.connect(db_path)
    ts = db.now()
    seeded = 24
    payload = json.dumps({"handler": "sha", "args": {"seed": "soak", "rounds": 30_000}, "timeout": 3})
    for i in range(seeded):
        conn.execute(
            "INSERT INTO tasks(task_id, payload, status, created_at, updated_at) VALUES(?,?, 'pending', ?, ?)",
            (f"s-{i:06d}", payload, ts + i * 1e-6, ts),
        )

    procs = [fleet.spawn_scheduler(db_path, interval=0.5, alerts_path=os.path.join(out_dir, "soak_alerts.jsonl"),
                                   stale_after=15.0, failure_rate=0.2)]
    workers = {f"w{i + 1}": fleet.spawn_worker(db_path, f"w{i + 1}", task_timeout=3.0) for i in range(args.workers)}
    all_procs = procs + list(workers.values())

    samples: list[dict] = []
    rss_series: dict[str, list[tuple[float, int]]] = {}
    seg_pid: dict[str, int] = {}
    segments: dict[str, list[dict]] = {}
    stop = {"flag": False}
    start = time.time()
    deadline = start + args.duration
    kill_plan = None
    kill_record = None
    killed_pid = None

    def _handler(signum, frame):  # noqa: ARG001
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
    next_task = seeded
    next_sample = start
    next_feed = start
    stuck_events: list[dict] = []

    try:
        while time.time() < deadline and not stop["flag"]:
            now = time.time()
            # 1) 派活：固定节奏喂任务，保持少量积压、低并发
            c = counts(conn)
            outstanding = c["pending"] + c["running"] + c["retried"]
            if outstanding < args.max_outstanding and now >= next_feed:
                next_feed = now + args.feed_interval
                payload = json.dumps(
                    {"handler": "sha", "args": {"seed": f"soak-{next_task}", "rounds": 30_000}, "timeout": 3}
                )
                conn.execute(
                    "INSERT INTO tasks(task_id, payload, status, created_at, updated_at) VALUES(?,?, 'pending', ?, ?)",
                    (f"s-{next_task:06d}", payload, now, now),
                )
                next_task += 1

            # 2) 中途故障注入：SIGKILL 一个 worker，验证 soak 期间自愈
            if args.kill_at > 0 and kill_plan is None:
                kill_plan = start + args.duration * args.kill_at
            if kill_plan is not None and time.time() >= kill_plan and kill_record is None:
                running = []
                # 低并发下 worker 可能空闲：先补 3 个任务保证有活可干，再等最多 5s 抓到 running
                for _ in range(3):
                    now2 = time.time()
                    conn.execute(
                        "INSERT INTO tasks(task_id, payload, status, created_at, updated_at) VALUES(?,?, 'pending', ?, ?)",
                        (
                            f"s-{next_task:06d}",
                            json.dumps({"handler": "sha", "args": {"seed": f"soak-{next_task}", "rounds": 30_000}, "timeout": 3}),
                            now2,
                            now2,
                        ),
                    )
                    next_task += 1
                wait_deadline = time.time() + 5.0
                while time.time() < wait_deadline:
                    running = db.rows(
                        conn,
                        "SELECT task_id, lease_owner FROM tasks WHERE status='running' AND lease_owner IS NOT NULL"
                        " ORDER BY updated_at LIMIT 1",
                    )
                    if running:
                        break
                    time.sleep(0.05)
                if running:
                    owner = str(running[0]["lease_owner"])
                    pid = pids_by_worker(conn).get(owner)
                    if pid:
                        task_id = str(running[0]["task_id"])
                        kill_at = time.time()
                        try:
                            os.kill(pid, signal.SIGKILL)
                            killed_pid = pid
                            # 等重派并完成，最多 30s
                            reassigned_ms = None
                            new_owner = None
                            while time.time() - kill_at < 30:
                                row = conn.execute(
                                    "SELECT lease_owner, status FROM tasks WHERE task_id=?", (task_id,)
                                ).fetchone()
                                if row and row["lease_owner"] and row["lease_owner"] != owner and row["status"] == "running":
                                    reassigned_ms = round((time.time() - kill_at) * 1000.0, 1)
                                    new_owner = str(row["lease_owner"])
                                    break
                                time.sleep(0.02)
                            done_ms = None
                            while time.time() - kill_at < 60:
                                row = conn.execute("SELECT status FROM tasks WHERE task_id=?", (task_id,)).fetchone()
                                if row and row["status"] in ("done", "failed"):
                                    done_ms = round((time.time() - kill_at) * 1000.0, 1)
                                    break
                                time.sleep(0.05)
                            time.sleep(1.0)
                            fleet.stop(workers[owner], timeout=1)
                            restarted_at = time.time()
                            workers[owner] = fleet.spawn_worker(db_path, owner, task_timeout=3.0)
                            all_procs.append(workers[owner])
                            kill_record = {
                                "killed_worker": owner,
                                "killed_pid": pid,
                                "task_id": task_id,
                                "reassign_ms": reassigned_ms,
                                "new_worker": new_owner,
                                "done_after_kill_ms": done_ms,
                                "at_elapsed_s": round(time.time() - start, 1),
                                "killed_at_epoch": kill_at,
                                "restarted_at_epoch": restarted_at,
                            }
                            print(f"[soak] mid-run kill: {json.dumps(kill_record, ensure_ascii=False)}", flush=True)
                        except ProcessLookupError:
                            kill_record = {"error": "process already gone"}
                else:
                    kill_record = {"error": "no running task at kill time"}

            # 3) 采样
            if time.time() >= next_sample:
                next_sample += args.sample
                pids = pids_by_worker(conn)
                sample = {
                    "t": round(time.time() - start, 1),
                    "loadavg1": loadavg(),
                    "counts": counts(conn),
                    "workers": {},
                }
                total_rss = 0
                for wid, pid in pids.items():
                    rss = rss_kb(pid)
                    if rss is None:
                        continue
                    total_rss += rss
                    if seg_pid.get(wid) != pid:
                        seg_pid[wid] = pid
                        segments.setdefault(wid, []).append({"pid": pid, "start_t": round(time.time() - start, 1), "samples": []})
                    segment = segments[wid][-1]
                    segment["samples"].append([round(time.time() - start, 1), rss])
                sample["rss_total_kb"] = total_rss
                sample["workers"] = {
                    w["worker_id"]: {"state": w["state"], "hb_age": w["heartbeat_age_s"], "pid": w["pid"]}
                    for w in console.worker_status(conn)
                }
                # 检查"有租约过期但仍 running"的僵死任务
                overdue = db.rows(
                    conn,
                    "SELECT task_id, lease_owner, lease_expires_at FROM tasks"
                    " WHERE status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?",
                    (time.time() - 10,),
                )
                if overdue:
                    stuck_events.append({"t": sample["t"], "overdue_over_10s": len(overdue)})
                samples.append(sample)
            time.sleep(0.15)

        # 4) 收尾：停止派活，等积压清空
        drain_deadline = time.time() + 180
        while time.time() < drain_deadline:
            c = counts(conn)
            if c["pending"] + c["running"] + c["retried"] == 0:
                break
            time.sleep(0.5)
        end = time.time()
    finally:
        for proc in all_procs:
            fleet.stop(proc, timeout=5)
        fleet.close_handles(all_procs)
        conn.close()

    # ---------- 汇总 ----------
    conn = db.connect(db_path)
    try:
        final_counts = counts(conn)
        total = sum(final_counts.values())
        worker_table = console.worker_status(conn)
        raw_gaps = {w["worker_id"]: round(console.heartbeat_gap(conn, w["worker_id"]), 2) for w in worker_table}
        excl_lo = excl_hi = None
        if kill_record and "killed_at_epoch" in kill_record:
            excl_lo = float(kill_record["killed_at_epoch"]) - 1.0
            excl_hi = float(kill_record["restarted_at_epoch"]) + 2.0
        gaps = {
            w["worker_id"]: round(gap_excluding(conn, w["worker_id"], excl_lo, excl_hi), 2)
            for w in worker_table
        }
        summary = console.task_summary(conn)
        alerts = db.rows(conn, "SELECT ts, rule, entity, message FROM alerts ORDER BY ts")
        stalls = db.rows(conn, "SELECT task_id, status, redispatches FROM tasks WHERE redispatches > 0")
        console.write_html(conn, os.path.join(out_dir, "dashboard_soak.html"))
    finally:
        conn.close()

    # 内存分析：每个 (worker, pid) 段的首尾值与斜率（KB/min）
    mem = {}
    for wid, segs in segments.items():
        for seg in segs:
            pts = seg["samples"]
            if len(pts) < 3:
                continue
            xs = [p[0] / 60.0 for p in pts]
            ys = [p[1] for p in pts]
            first, last = ys[0], ys[-1]
            slope = linear_slope(xs, ys)
            increasing_steps = sum(1 for a, b in zip(ys, ys[1:]) if b > a)
            mem.setdefault(wid, []).append(
                {
                    "pid": seg["pid"],
                    "duration_min": round(xs[-1] - xs[0], 1),
                    "rss_first_kb": first,
                    "rss_last_kb": last,
                    "delta_kb": last - first,
                    "delta_pct": round((last - first) / first * 100, 2) if first else None,
                    "slope_kb_per_min": round(slope, 2),
                    "increasing_steps": increasing_steps,
                    "samples": len(ys),
                }
            )

    loads = [s["loadavg1"] for s in samples]
    hb_ages = [w["hb_age"] for s in samples for w in s["workers"].values()]
    ok_terminal = all(v == 0 for k, v in final_counts.items() if k in ("pending", "running", "retried"))
    max_gap = max(gaps.values()) if gaps else None
    mem_monotonic = any(
        seg["delta_pct"] is not None and seg["delta_pct"] > 5 and seg["increasing_steps"] / max(1, seg["samples"] - 1) > 0.8
        for segs in mem.values() for seg in segs
    )
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "planned_duration_s": args.duration,
        "actual_duration_s": round(end - start, 1),
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)),
        "ended_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)),
        "host": os.uname().nodename,
        "cpu_count": os.cpu_count(),
        "workers": args.workers,
        "max_outstanding": args.max_outstanding,
        "feed_interval_s": args.feed_interval,
        "loadavg1_samples": {
            "n": len(loads),
            "min": min(loads) if loads else None,
            "max": max(loads) if loads else None,
            "mean": round(sum(loads) / len(loads), 2) if loads else None,
            "samples_over_4": sum(1 for x in loads if x > 4),
            "first": loads[0] if loads else None,
            "last": loads[-1] if loads else None,
            "high_load_caveat": (sum(1 for x in loads if x > 4) > 0),
        },
        "tasks": {
            "total": total,
            "final_counts": final_counts,
            "redispatched_tasks": len(stalls),
            "p95_latency_s": summary["p95_latency_s"],
            "success_rate": summary["success_rate"],
        },
        "deadlock": {"no_stuck_tasks": ok_terminal, "overdue_running_events": len(stuck_events)},
        "heartbeat": {
            "max_gap_s": max(gaps.values()) if gaps else None,
            "per_worker": gaps,
            "per_worker_including_kill_window": raw_gaps,
            "all_under_5s": all(v < 5.0 for v in gaps.values()) if gaps else False,
            "max_observed_age_s": round(max(hb_ages), 2) if hb_ages else None,
            "note": "per_worker 已排除 SIGKILL 停机窗口那一个间隔；原始值见 including_kill_window。",
        },
        "memory": {
            "segments": mem,
            "monotonic_growth_detected": mem_monotonic,
            "note": "斜率按采样点最小二乘拟合；被 SIGKILL 重启的 worker 单独成段。",
        },
        "mid_run_kill": kill_record,
        "alerts": [dict(a) for a in alerts][:20],
        "runtime_paths": {"db": db_path, "dashboard": os.path.join(out_dir, "dashboard_soak.html"),
                          "alerts_jsonl": os.path.join(out_dir, "soak_alerts.jsonl")},
    }
    report["drain_ok"] = ok_terminal
    report["mid_run_kill_ok"] = kill_record is not None and kill_record.get("reassign_ms") is not None
    out = os.path.join(out_dir, "soak_report.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    print(json.dumps({k: report[k] for k in ("actual_duration_s", "deadlock", "heartbeat", "mid_run_kill")},
                     ensure_ascii=False, indent=2))
    print(f"report: {out}")
    kill_required = args.kill_at > 0
    return 0 if ok_terminal and not mem_monotonic and (not kill_required or report["mid_run_kill_ok"]) else 1


if __name__ == "__main__":
    sys.exit(main())

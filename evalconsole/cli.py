"""命令行入口。零依赖，仅标准库。"""

from __future__ import annotations

import argparse
import signal
import sys
import time

from . import alerts, console, db, fleet, scheduler, worker as worker_mod


def _add_db(p: argparse.ArgumentParser) -> None:
    p.add_argument("--db", default="./evalconsole.db", help="SQLite 文件路径")


def cmd_init(args: argparse.Namespace) -> int:
    db.init_db(args.db)
    print(f"initialized: {args.db}")
    return 0


def cmd_seed(args: argparse.Namespace) -> int:
    db.init_db(args.db)
    fleet.seed_tasks(args.db, args.n, fail_every=args.fail_every)
    print(f"seeded {args.n} tasks (fail_every={args.fail_every}) into {args.db}")
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    db.init_db(args.db)
    w = worker_mod.Worker(
        args.db,
        args.id,
        heartbeat_interval=args.heartbeat,
        lease_ttl=args.lease_ttl,
        task_timeout=args.task_timeout,
    )
    worker_mod.install_signal_handlers(w)
    w.run()
    return 0


def cmd_scheduler(args: argparse.Namespace) -> int:
    db.init_db(args.db)
    conn = db.connect(args.db)
    cfg = {"stale_after_s": args.stale_after, "failure_rate_threshold": args.failure_rate}
    stop = {"flag": False}

    def _handler(signum, frame):  # noqa: ARG001
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
    try:
        while not stop["flag"]:
            scheduler.run_once(conn, cfg, args.alerts_file)
            time.sleep(args.interval)
    finally:
        conn.close()
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    conn = db.connect(args.db)
    try:
        print(console.render_text(conn, args.stale_after))
        if args.html:
            console.write_html(conn, args.html, args.stale_after)
            print(f"dashboard: {args.html}")
    finally:
        conn.close()
    return 0


def cmd_dashboard(args: argparse.Namespace) -> int:
    conn = db.connect(args.db)
    try:
        console.write_html(conn, args.out, args.stale_after)
        print(f"dashboard: {args.out}")
    finally:
        conn.close()
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    """最小可跑：建库 → 灌任务 → 1 调度器 + N worker → 跑完 → 打印看板。"""
    db.init_db(args.db)
    fleet.seed_tasks(args.db, args.tasks, fail_every=args.fail_every)
    procs = [fleet.spawn_scheduler(args.db, alerts_path=args.alerts_file)]
    procs += [fleet.spawn_worker(args.db, f"w{i + 1}", task_timeout=args.task_timeout) for i in range(args.workers)]
    try:
        counts = fleet.wait_terminal(args.db, timeout=args.timeout)
        print(f"terminal states: {counts}")
        conn = db.connect(args.db)
        try:
            print(console.render_text(conn))
            out = args.html or "dashboard.html"
            console.write_html(conn, out)
            print(f"dashboard: {out}")
        finally:
            conn.close()
    finally:
        for proc in procs:
            fleet.stop(proc)
        fleet.close_handles(procs)
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    """常驻模式（人工观察/soak 用）：调度器 + N worker，直到收到 SIGINT 或超时。"""
    db.init_db(args.db)
    procs = [fleet.spawn_scheduler(args.db, alerts_path=args.alerts_file)]
    procs += [fleet.spawn_worker(args.db, f"w{i + 1}", task_timeout=args.task_timeout) for i in range(args.workers)]
    deadline = time.time() + args.duration
    try:
        while time.time() < deadline:
            time.sleep(1)
    finally:
        for proc in procs:
            fleet.stop(proc)
        fleet.close_handles(procs)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="evalconsole", description="评测 fleet 持续运行与自愈控制台")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("init", help="初始化 SQLite schema")
    _add_db(s)
    s.set_defaults(func=cmd_init)

    s = sub.add_parser("seed", help="灌入演示任务")
    _add_db(s)
    s.add_argument("--n", type=int, default=30)
    s.add_argument("--fail-every", type=int, default=10, help="每 N 个任务放一个失败任务，0 表示不放")
    s.set_defaults(func=cmd_seed)

    s = sub.add_parser("worker", help="启动单个 worker")
    _add_db(s)
    s.add_argument("--id", required=True)
    s.add_argument("--heartbeat", type=float, default=1.0)
    s.add_argument("--lease-ttl", type=float, default=5.0)
    s.add_argument("--task-timeout", type=float, default=10.0)
    s.set_defaults(func=cmd_worker)

    s = sub.add_parser("scheduler", help="启动调度器（回收过期租约 + 告警）")
    _add_db(s)
    s.add_argument("--interval", type=float, default=0.5)
    s.add_argument("--alerts-file", default=None)
    s.add_argument("--stale-after", type=float, default=15.0)
    s.add_argument("--failure-rate", type=float, default=0.20)
    s.set_defaults(func=cmd_scheduler)

    s = sub.add_parser("status", help="打印 worker 心跳表与任务状态")
    _add_db(s)
    s.add_argument("--stale-after", type=float, default=15.0)
    s.add_argument("--html", default=None, help="同时导出静态 HTML 看板")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("dashboard", help="导出静态 HTML 看板")
    _add_db(s)
    s.add_argument("--out", default="dashboard.html")
    s.add_argument("--stale-after", type=float, default=15.0)
    s.set_defaults(func=cmd_dashboard)

    s = sub.add_parser("demo", help="最小可跑：灌任务 → 3 worker + 调度器 → 跑完出看板")
    _add_db(s)
    s.add_argument("--tasks", type=int, default=30)
    s.add_argument("--workers", type=int, default=3)
    s.add_argument("--fail-every", type=int, default=10)
    s.add_argument("--task-timeout", type=float, default=10.0)
    s.add_argument("--timeout", type=float, default=120.0, help="等待全部任务终态的上限（秒）")
    s.add_argument("--html", default="dashboard.html")
    s.add_argument("--alerts-file", default="alerts.jsonl")
    s.set_defaults(func=cmd_demo)

    s = sub.add_parser("serve", help="常驻：调度器 + N worker 跑指定时长")
    _add_db(s)
    s.add_argument("--workers", type=int, default=3)
    s.add_argument("--duration", type=float, default=600.0)
    s.add_argument("--task-timeout", type=float, default=30.0)
    s.add_argument("--alerts-file", default=None)
    s.set_defaults(func=cmd_serve)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())

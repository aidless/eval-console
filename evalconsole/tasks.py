"""任务负载：真实可运行的小计算/睡眠/失败任务。

handler 必须是模块级函数（multiprocessing.spawn 需要可 pickle 的引用）。
"""

from __future__ import annotations

import hashlib
import time


def sha_work(seed: str = "0", rounds: int = 220_000, sleep_s: float = 0.0) -> str:
    """固定次数的 SHA-256 链，CPU 开销小、结果确定。"""
    digest = seed.encode("utf-8")
    for i in range(rounds):
        digest = hashlib.sha256(digest + b"|" + str(i).encode()).digest()
    if sleep_s:
        time.sleep(sleep_s)
    return digest.hex()[:32]


def sleep_task(seconds: float = 3.0) -> str:
    time.sleep(seconds)
    return f"slept:{seconds}"


def fail_task(message: str = "boom") -> str:
    raise RuntimeError(message)


HANDLERS = {
    "sha": sha_work,
    "sleep": sleep_task,
    "fail": fail_task,
}


def run_handler(name: str, payload: dict | None, conn) -> None:
    """在子进程里执行 handler，把结果/异常通过 Pipe 写回父进程。"""
    payload = payload or {}
    try:
        fn = HANDLERS[name]
        result = fn(**payload)
        conn.send({"ok": True, "result": result})
    except BaseException as exc:  # noqa: BLE001 - 需要把任何失败都回传给父进程
        conn.send({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
    finally:
        conn.close()

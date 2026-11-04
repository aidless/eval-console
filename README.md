# eval-console

评测 fleet 的**持续运行与自愈**：SQLite 任务状态机 + worker 心跳 + 租约回收 + kill 后自动重派 + 控制台看板。
零第三方运行时依赖（Python 3.10+ 标准库），单机多进程。

这是"4 周 6 项目"矩阵里 R65 的后半段：前半段是单机 worker 的 lease/heartbeat 雏形，这一半要回答的是
**"worker 被杀掉之后，系统多久能把活接回去，以及连续跑两个小时会不会烂掉"**。

## 最小可跑

```bash
git clone <repo-url> && cd eval-console
python3 -m evalconsole demo --db ./demo.db --tasks 30 --workers 3
```

输出会是三段：终态统计、worker 心跳表、`dashboard.html` 路径。仓库内没有任何 pip 依赖。

## 实测证据

| 判据 | 结果 | 证据 |
| --- | --- | --- |
| 1. worker 心跳可读 | 1s 心跳写 `workers.heartbeat_at` + `heartbeat_log`，控制台输出"谁活着、多久没动" | `python3 -m evalconsole status --db <db>` |
| 2. SIGKILL 后 ≤10s 重派 | **3 次实测：4848.1 / 5110.1 / 7040.5 ms，范围 4.8–7.0s**，全部 ≤10s | `evidence/kill_redispatch.json` |
| 3. 状态机落库、重启不重跑 | done 任务在重启后 `attempt`/`updated_at` 不变；崩溃中的 running 任务由租约回收后恢复 | `tests/test_restart_persistence.py` |
| 4. ≥2h 不间断 soak | **{{SOAK_VERDICT}}** | `evidence/soak_report.json` |

{{SOAK_TABLE}}

测试：`33` 条 pytest（`32` 条常规 + `1` 条真实 SIGKILL 慢用例），全部通过。
2 小时 soak 不是 pytest 用例（CI 机时不允许），由 `scripts/soak.py` 驱动，产出 `evidence/soak_report.json` 与 `evidence/dashboard_soak.html`。

## 机制

```
worker  ──每 1s──> workers.heartbeat_at / heartbeat_log        （心跳线程）
        └─续租───> tasks.lease_expires_at = now + 5s          （只对仍属于自己的任务生效）
scheduler ──每 0.5s──> 扫描 lease 过期：
        如果 attempt < max_attempts → status='retried'（可领取），记一条 redispatches
        否则                        → status='failed'
worker  ──领取──> BEGIN IMMEDIATE 内 SELECT+UPDATE，只有一个 owner
        └─落库───> UPDATE ... WHERE task_id=? AND lease_owner=? AND status='running'
                   （租约已丢时 rowcount=0，结果被拒绝，不会覆盖新 owner）
```

任务执行放在 `multiprocessing` 子进程里，超时就 `terminate()`→`kill()`→`join()`，父进程不会被卡死，也不会留僵尸。
因为执行子进程是 `spawn` 启动，worker 被 SIGKILL 时子进程会被操作系统重挂到 init，不会阻塞重派。

## SQLite schema

| 表 | 用途 |
| --- | --- |
| `tasks` | 任务状态机：`pending / running / retried / done / failed`，含 `attempt`、`max_attempts`、`lease_owner`、`lease_expires_at`、`redispatches`、`started_at/finished_at` |
| `workers` | worker 注册表：`pid`、`started_at`、`heartbeat_at`、`stopped_at`、`restart_count`、`last_task_id` |
| `heartbeat_log` | 每次心跳一行，用于算"心跳最大间隔"（`scripts/soak.py` 用它证明心跳没断） |
| `redispatches` | 每次租约回收一行：`from_worker`、`silence_ms`（回收时静默了多久）、`reclaim_ms`（过期了多久才被扫到） |
| `events` | 审计流水：`worker_start/stop`、`task_done`、`task_reclaimed`、`stale_write_rejected` 等 |
| `alerts` / `alert_state` | 告警记录与冷却去重 |
| `meta` | `schema_version` |

状态机：`pending → running → done`；执行失败时 `attempt < max_attempts` 走 `retried` 再回 `running`，用尽则 `failed`；
`running` 的租约过期由 scheduler 改成 `retried`（或 `failed`），此时原 worker 再写结果会被租约守卫拒绝。

## 失效模式用例（10 条，全部可运行）

1. 进程还活着但心跳线程停了 → 租约过期后被重派
2. 任务卡死超过超时 → 子进程 terminate/kill/join，无僵尸进程
3. 4 个线程并发写 SQLite（WAL + `busy_timeout=5000`）→ 无 `database is locked`
4. 重复派发竞争 → 40 个任务只产生 40 个 owner，无重复
5. 重试次数耗尽 → 终态 `failed`，不再被领取
6. 进程重启 → 已完成任务不重跑
7. 租约丢失后的迟到结果 → 被拒绝，不覆盖新 owner
8. worker 心跳丢失 → 告警触发，冷却期内不刷屏
9. 空库 / 空队列 → 调度周期不崩、不造任务
10. 重复 `init_db` → schema 幂等、数据不丢

对应文件：`tests/test_failure_modes.py`，其余判据在 `tests/test_worker_core.py`、`tests/test_scheduler_reclaim.py`、
`tests/test_restart_persistence.py`、`tests/test_alerts_console.py`、`tests/test_kill_redispatch.py`。

## CI 覆盖什么、没覆盖什么

`ci.yml` 在 3.10 / 3.11 / 3.12 上跑 `pytest -m "not slow"`，外加一次 20 任务的 `demo` 冒烟（真实拉起 scheduler + 2 worker 子进程）。

**覆盖：**
- schema 初始化与幂等、任务状态迁移、租约过期回收、迟到写入拒绝
- worker 心跳写入、`stale` 判定、控制台文本/HTML 内容
- 任务超时的子进程终止与回收（`/proc` 扫描确认无 Z 状态子进程）
- 4 线程并发领取 / 并发写 SQLite
- 告警规则与冷却去重
- CLI `demo` 的进程编排（非 SIGKILL）

**没覆盖：**
- 真实 `SIGKILL` 之后的重派时延（`tests/test_kill_redispatch.py` 标了 `slow`，CI 不跑；本机实测见 `evidence/`）
- 2 小时 soak（CI 机时不允许，结果只在 `evidence/soak_report.json`）
- 多机、跨主机时钟漂移、NFS/网络盘上的 SQLite
- 看板的浏览器渲染（只断言 HTML 文本内容，不截图）
- cgroup / userns 隔离（本仓库不使用隔离，故也无从验证）

CI 只保证"上面这些跑通了"，不等于全部用例都跑过。README 不写"CI 全绿 = 系统可用"。

## 这仓库能证明什么

- 单机多进程、SQLite 单一数据源的前提下，**1s 心跳 + 5s 租约 + 0.5s 扫描**的组合，能在 3 次真实 SIGKILL 中于 **4.8–7.0s** 内把任务重派给存活 worker（≤10s 判据成立）。重派后任务会整段重跑并最终 `done`。
- 进程重启不会重跑已完成任务（`attempt`、`updated_at`、`result` 逐字段不变），崩溃中的 `running` 任务能被回收并恢复。
- 连续 2 小时、3 worker、约 1 任务/秒的低并发循环派活下，{{SOAK_PROVE}}
- 心跳、状态、重派、告警都落在 SQLite 里，可以直接 `sqlite3` 查询复核，不依赖日志文本。

## 这仓库不能证明什么

- **不是生产可用的调度器。** 没有持久化消息队列、没有多机协调/选主、没有鉴权、没有优雅停机保证；SIGTERM 会中断当前任务，靠租约恢复。SQLite 也不适合放在 NFS 上。
- **不保证 exactly-once。** 被 kill 或被回收的任务会从头重跑（at-least-once），任务副作用必须幂等。
- **重派时延不是常数。** 它取决于"其他 worker 是否空闲"：实测最慢一次 7.1s 里有一大段是新 worker 正在执行上一个任务。worker 越少、任务越长，重派越慢。
- **不能证明长于 2 小时的稳定性**，也没有验证 >4 worker 的并发，以及容器/cgroup/userns 隔离下的行为。
- **CI 不跑 SIGKILL 与 soak**，所以"CI 通过"只覆盖上面列出的子集。
- 数字只在当前机器（16 vCPU、Linux、高负载环境）测得；soak 期间 `load average` 见下节，负载高时吞吐会偏低。

## 复现

```bash
# 常规测试（32 条）
python3 -m pytest -m "not slow" -q

# 真实 SIGKILL 重派测试（1 条，约 10s）
python3 -m pytest -m slow -q

# 故障注入，输出 evidence/kill_redispatch.json（默认 3 次）
python3 scripts/kill_redispatch.py

# 2 小时 soak（默认 7200s，3 worker，1 任务/秒，中途 kill 一个 worker）
python3 scripts/soak.py --duration 7200 --kill-at 0.5
```

soak 会记录 `load average`、每个 worker 的 RSS 首尾值与斜率、心跳最大间隔、任务终态、以及中途 kill 的重派时延，
汇总写到 `evidence/soak_report.json`。若采样中 `load > 4`，报告会带 `high_load_caveat: true`。

## 资源说明

所有压测都在本机低并发（≤4 worker、1 任务/秒）下完成，未使用 GPU、未联网。
同一台机器上还有别的 CPU 任务在跑，soak 期间的 `load average` 与结论边界见 `evidence/soak_report.json`。

## License

MIT

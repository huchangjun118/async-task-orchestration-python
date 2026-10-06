# Async Task Orchestration Service

一个带**状态机**的异步任务编排服务。任务之间可以声明先后依赖，服务提供并发上限、
优先级、互斥、重试退避、超时、取消与进程重启恢复；所有状态迁移都是原子的
compare-and-set，保证重复触发、失败重试、并发取消和崩溃恢复都不会让任务状态错乱。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload          # http://127.0.0.1:8000
uv run pytest                              # 55 个自动化测试
```

默认把任务持久化到 `./data/tasks.db`（SQLite + WAL）。设置 `TASK_DB_PATH=:memory:`
可使用不跨进程的内存存储（主要用于测试）。

### 内置示例 handler

| 名称    | 行为                                                        |
|---------|-------------------------------------------------------------|
| `noop`  | 立即成功，返回 `{payload, attempt}`                         |
| `fail`  | 永远抛 `RuntimeError`（用来演示重试/跳过/取消）             |
| `flaky` | 前 `payload.fail_times` 次失败，之后成功                    |
| `sleep` | 睡眠 `payload.seconds` 秒，可被取消立即唤醒                 |

自定义 handler 通过 `Orchestrator.register_handler(name, fn)` 注册，签名为
`async def handler(ctx)`，其中 `ctx` 提供：

* `ctx.task_id` / `ctx.attempt` —— 任务与第几次尝试（从 1 开始）
* `ctx.payload` —— 触发时提交的参数
* `ctx.results` —— 全部前置任务的成功结果 `{dep_id: result}`
* `await ctx.cancelled()` —— 是否已被请求取消（协作式取消时轮询）
* `ctx.attempt_token` —— 本次尝试唯一的幂等令牌 `task_id:attempt:run_token`，
  可作为对外部系统的去重键

## HTTP 接口

| 方法 & 路径                    | 作用                                   |
|--------------------------------|----------------------------------------|
| `POST /tasks`                  | 触发单个任务（可带依赖、重试等配置）   |
| `POST /workflows`              | 批量触发一个任务图（依赖可互相引用）   |
| `GET  /tasks?limit&offset`     | 按创建顺序分页查询任务                 |
| `GET  /tasks/{id}`             | 查询单个任务当前状态（一致快照）       |
| `GET  /tasks/{id}/history`     | 查询完整状态流转历史                   |
| `POST /tasks/{id}/cancel`      | 取消任务                               |

触发示例：

```bash
curl -X POST localhost:8000/workflows -H 'content-type: application/json' -d '{
  "tasks": [
    {"id": "download", "handler": "sleep", "payload": {"seconds": 1},
     "max_attempts": 3, "timeout": 30,
     "backoff": {"base_delay": 1, "multiplier": 2, "max_delay": 60}},
    {"id": "transform", "handler": "flaky", "deps": ["download"],
     "dep_fail_policy": "SKIP", "priority": 5},
    {"id": "report", "handler": "noop", "deps": ["transform"]}
  ]
}'
curl localhost:8000/tasks/report/history
curl -X POST localhost:8000/tasks/report/cancel -d '{"reason":"obsolete"}'
```

## 状态机语义

```
PENDING ──全部依赖成功──────────────► READY
PENDING ──任一依赖终态非成功(策略)──► SKIPPED 或 CANCELLED
READY ─────────────────────────────► RUNNING
RUNNING ──成功──────────────────────► SUCCEEDED          (终态)
RUNNING ──失败且仍有重试额度────────► RETRY_WAIT ──退避到期──► READY
RUNNING ──失败且重试耗尽────────────► FAILED             (终态)
RUNNING ──超时且仍有重试额度────────► RETRY_WAIT
RUNNING ──超时且重试耗尽────────────► TIMED_OUT          (终态，区别于 FAILED)
任意非终态 ──用户取消───────────────► CANCELLED          (终态)
```

* **PENDING**：至少有一个前置任务还没结束。
* **READY**：前置全部成功，等待调度槽位（受并发上限/优先级/互斥约束）。
* **RUNNING**：某一次尝试正在执行。
* **RETRY_WAIT**：上一次尝试失败或超时，按指数退避等待下一次尝试；
  `delay(n) = min(max_delay, base_delay * multiplier^(n-2))`。
* **SUCCEEDED / FAILED / SKIPPED / CANCELLED / TIMED_OUT**：五个终态，互不混淆。
  * 超时是**独立终态 `TIMED_OUT`**，历史事件也是独立的 `TIMED_OUT`，不会被记成普通失败。
  * `SKIPPED` 一定带 `skip_reason`（如 `predecessor 'x' ended as FAILED`）；
    `CANCELLED` 一定带 `cancel_reason`。
* 后继任务**仅在全部前置 `SUCCEEDED` 时启动**；任一前置为 `FAILED / TIMED_OUT /
  SKIPPED / CANCELLED` 时，按该任务的 `dep_fail_policy` 处理：
  `SKIP`（默认，标记 SKIPPED 并记录原因）或 `CANCEL`（标记 CANCELLED 并记录原因）。
  策略沿依赖边传播（菱形依赖也会逐层跳过/取消）。

### 取消语义

* 对终态任务取消返回 `false`/409，状态不变。
* 对 READY/PENDING 任务：直接原子迁移到 CANCELLED，handler 永不执行。
* 对 RUNNING 任务：**先把状态原子翻转为 CANCELLED，再取消执行协程**。
  handler 在被取消后即使有残留逻辑也无法写入结果——结果写入是带
  `(state=RUNNING, run_token)` 前置条件的 CAS，取消后必然失败。handler 应通过
  `await ctx.cancelled()` 或可取消的等待（内置 `sleep` 已示范）协作式尽快停下。

## 调度保证

* **并发度上限**：`TASK_MAX_CONCURRENCY`（默认 4）个信号量槽位。
* **优先级**：`priority` 越大越先调度。
* **稳定顺序**：同优先级按全局创建序号 `seq` 先进先出；`seq` 持久化在存储中，
  **进程重启后排队顺序仍可复现**。
* **互斥**：相同 `mutex_group` 的任务不会同时执行；互斥只会阻塞同组任务，
  不会阻塞排在后面但不互斥的任务。

## 重试、超时与幂等

* `max_attempts`（含首次，默认 1）、`backoff`（指数退避）、`timeout`（秒）均可按任务配置。
* **重复触发**：
  * 相同 `idempotency_key` 的触发返回同一个任务（无论成功前后），不会产生第二个任务，
    已完成任务也不会被重新执行。
  * 相同显式 `task_id` 同理返回已有任务。并发重复触发由存储层唯一约束保证只建一条。
* **不产生重复副作用**：同一时刻一个任务最多只有一个尝试在执行；每次尝试有单调递增的
  `run_token` 与唯一 `attempt_token`，结果写入必须带匹配的 token，取消/重试产生的
  旧尝试写入会被 CAS 拒绝。注意：若进程在 handler 对外部系统产生副作用**之后、写回结果
  之前**崩溃，重试会再次调用 handler——handler 对外部系统应使用 `ctx.attempt_token`
  做下游去重（at-least-once 执行 + 幂等下游）。
* 被重启打断的尝试计入重试预算：`max_attempts=1` 的任务在 RUNNING 时崩溃，恢复后为
  FAILED（`error_type=InterruptedByRestart`）；仍有额度时进入 RETRY_WAIT 继续。

## 进程重启恢复

服务启动时 `Orchestrator.start()` 先启动调度循环再执行恢复扫描，恢复与新触发**并发安全**：
恢复只使用 CAS 迁移，已终态的任务不会被触碰，正在迁移的任务会被前置条件挡住。

* `RUNNING`：最后一次尝试被标记为失败（`InterruptedByRestart`），有额度则按退避重试，
  无额度则 FAILED，并追加 `RECOVERED` 事件。
* `RETRY_WAIT`：若 `not_before` 已过立即 READY，否则继续等待（宕机时间也计入退避）。
* `READY`：重新进入调度队列，按持久化的 `seq` 保持原有相对顺序。
* `PENDING`：重新评估依赖；此前失败的前置会让其按策略 SKIP/CANCELLED。
* 已完成任务保持原结果，不重跑、不遗漏；恢复期间到达的触发与普通触发走完全相同的
  原子路径，最终结果确定。

## 历史与一致性

* 每次状态迁移都向只追加的事件表写一条事件（`CREATED / READY / STARTED / SUCCEEDED /
  FAILED / RETRY_SCHEDULED / TIMED_OUT / SKIPPED / CANCEL_REQUESTED / CANCELLED /
  RECOVERED`），每个任务事件序号连续无缺口，并记录对应的 `attempt` 与原因明细。
* 查询返回的是**快照**：记录与其完整事件列表在同一个临界区（内存锁 / SQLite 事务）读出，
  任意时刻看到的状态与历史都互相一致。

## 配置项

| 配置                | 位置                         | 默认值        | 说明                         |
|---------------------|------------------------------|---------------|------------------------------|
| `TASK_DB_PATH`      | 环境变量                     | `data/tasks.db` | SQLite 路径；`:memory:` 内存 |
| `TASK_MAX_CONCURRENCY` | 环境变量                  | `4`           | 全局最大并发执行任务数       |
| `max_attempts`      | 任务配置                     | `1`           | 最大尝试次数（含首次）       |
| `backoff.base_delay`| 任务配置                     | `1.0`         | 首次重试基础退避（秒）       |
| `backoff.multiplier`| 任务配置                     | `2.0`         | 退避指数倍率                 |
| `backoff.max_delay` | 任务配置                     | `60.0`        | 退避上限（秒）               |
| `timeout`           | 任务配置                     | 无            | 单次尝试超时（秒）           |
| `priority`          | 任务配置                     | `0`           | 调度优先级，越大越先         |
| `mutex_group`       | 任务配置                     | 无            | 互斥组，同组串行             |
| `dep_fail_policy`   | 任务配置                     | `SKIP`        | 前置失败时 `SKIP`/`CANCEL`   |
| `idempotency_key`   | 任务配置                     | 无            | 重复触发去重键               |

## 代码结构

```
app/
  models.py        # 状态机枚举、TaskSpec/TaskRecord/TaskEvent、异常
  store.py         # 存储协议 + 内存实现（锁 + CAS + 深拷贝快照）
  store_sqlite.py  # SQLite 持久化实现（单事务 CAS，崩溃恢复基础）
  scheduler.py     # 稳定优先级队列、并发信号量、互斥组过滤
  engine.py        # 调度循环、依赖评估、尝试执行（超时/重试/取消/恢复）
  orchestrator.py  # 对外门面：触发/工作流/DAG 校验/查询/取消
  handlers.py      # 内置示例 handler
  schemas.py       # HTTP 请求/响应模型
  api.py / main.py # FastAPI 装配与入口
tests/             # 依赖、调度、重试超时、幂等、取消、恢复、API、压力、存储
```

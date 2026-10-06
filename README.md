# async-task-orchestration

基于 asyncio + SQLite 的异步任务编排服务：任务之间可声明依赖，带完整状态机、
并发控制、失败重试、取消/超时、崩溃恢复与流转历史。对外提供 HTTP 接口。

## 运行

```bash
uv sync
uv run python main.py          # 监听 127.0.0.1:8000
uv run pytest                  # 运行测试
```

环境变量：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `ORCHESTRATOR_DB` | `orchestrator.db` | SQLite 存储路径（`:memory:` 表示纯内存） |
| `ORCHESTRATOR_MAX_CONCURRENCY` | `4` | 全局并发度上限 |

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks/{task_name}/trigger` | 触发任务（body: `{"payload": ..., "idempotency_key": ...}`） |
| GET | `/runs/{run_id}` | 查询一次运行的一致快照（含所有任务实例） |
| GET | `/instances/{instance_id}` | 查询单个任务实例快照 |
| GET | `/instances/{instance_id}/history` | 查询实例从创建到结束的完整流转历史 |
| POST | `/runs/{run_id}/cancel?task_name=` | 取消整个 run 或其中某个任务 |

实例 id 形如 `{run_id}:{task_name}`。一次触发会为该任务及其全部传递依赖各创建
一个实例（依赖先创建，顺序与 DAG 拓扑序一致）。

## 状态机语义

```
PENDING --(全部依赖成功)--> READY --(调度)--> RUNNING --+--> SUCCEEDED (终态)
   |                        |                          +--> FAILED     (终态, 重试耗尽)
   |                        |                          +--> TIMED_OUT  (终态, 超时且重试耗尽)
   |                        |                          +--> CANCELLED  (终态, 运行中被取消)
   |                        |                          +--> READY      (重试退避后重新入队)
   |                        +--> CANCELLED (终态, 排队时被取消)
   +--> SKIPPED   (终态, 依赖失败且策略为 skip)
   +--> CANCELLED (终态, 依赖失败且策略为 cancel / 显式取消)
```

- **终态**：`SUCCEEDED / FAILED / TIMED_OUT / CANCELLED / SKIPPED`，终态不可逆。
- **TIMED_OUT 与 FAILED 严格区分**：超时由每次尝试的 `timeout` 触发，与处理器
  抛异常的普通失败走不同的终态与错误信息。
- **依赖失败传播**：任一前置任务进入失败类终态（FAILED/TIMED_OUT/CANCELLED/
  SKIPPED）时，后继任务按 `on_dependency_failure` 策略被置为 `SKIPPED` 或
  `CANCELLED`，并沿依赖链继续向下传播；`skip_reason`/`error` 字段记录具体原因
  （哪个依赖、以什么状态结束）。
- **一致性保证**：所有状态变更都是对 SQLite 的 compare-and-swap 单事务写入
  （校验当前状态 ∈ expected → 更新 → 追加历史），因此并发触发、重试、取消与
  恢复之间的竞争是确定的：第一个成功的 CAS 生效，其余为无操作。任意时刻读到
  的状态都是某一一致快照。

## 任务定义与配置项

```python
TaskDefinition(
    name="transform",
    handler=async_fn,                 # async (instance, token) -> result
    dependencies=("extract",),        # 前置任务名，全部成功才会开始
    priority=1,                       # 数值越大越先调度
    mutex_group="db-writers",         # 同组任务绝不并发执行
    timeout=10.0,                     # 每次尝试的超时秒数，None 为不限
    retry_policy=RetryPolicy(
        max_retries=2,                # 首次失败后的最大重试次数
        backoff=BackoffStrategy.EXPONENTIAL,  # fixed | exponential
        base_delay=0.1,               # 退避基数（秒）
        max_delay=30.0,               # 退避上限（秒）
        retry_on_timeout=True,        # 超时是否也重试
    ),
    on_dependency_failure=DependencyFailurePolicy.SKIP,  # skip | cancel
)
```

引擎级参数：`OrchestrationEngine(store, registry, max_concurrency=4,
cancel_grace=0.5)`。`cancel_grace` 是取消运行中任务时，先发出协作式取消信号
（`CancellationToken`）、再强制取消 asyncio 任务之间的宽限期。

## 语义保证

- **排队顺序**：优先级高者先调度；同优先级按提交顺序严格 FIFO（单调序号打破
  平局），顺序稳定可复现。互斥组被占用时，组内任务留在队列中，不阻塞其他组。
- **幂等触发**：携带相同 `idempotency_key` 的触发（包括并发、跨进程重启）只
  创建一次运行，后续触发返回原 `run_id` 且 `created=false`，处理器不会重复
  执行、不产生重复副作用。未提供 key 时按 `任务名+payload` 派生确定性 key。
- **取消**：PENDING/READY 直接置 CANCELLED；RUNNING 先 CAS 置 CANCELLED 再
  通知处理器停下。由于结果写入要求 CAS 自 RUNNING 成功，**取消之后绝不会再
  写入最终结果**。后继任务按策略被 SKIPPED/CANCELLED。
- **崩溃恢复**：所有状态持久化于 SQLite。重启时：RUNNING（被中断的尝试）重置
  为 READY 重新执行（处理器需幂等）；READY 重新入队；PENDING 按依赖现状重新
  评估；终态实例既不重复执行也不遗漏。恢复与正常触发共用同一套 CAS 与调度
  路径，恢复期间到达的新触发与恢复并发进行且结果确定。
- **历史**：每次状态迁移追加一条不可变记录（from/to/attempt/原因/时间戳），
  可完整还原每个任务从创建到结束的链路，包括每次重试与取消。

## 代码结构

```
orchestrator/
  models.py     # 状态枚举、任务定义/实例、重试策略、历史记录等纯类型
  registry.py   # 任务定义注册与依赖图校验（存在性、无环）
  store.py      # SQLite 持久化：实例、幂等键、流转历史，CAS 状态迁移
  scheduler.py  # 优先级队列（同优先级 FIFO）、并发上限、互斥组
  executor.py   # 单次尝试执行：超时、协作式/强制取消、异常归类
  engine.py     # 编排引擎：触发/取消/查询/恢复、依赖评估、重试退避
  service.py    # FastAPI HTTP 层
tests/
  test_engine.py    # 依赖失败、并发、重复触发、取消、超时、重启恢复等场景
  crash_child.py    # 重启恢复测试用的崩溃子进程
```

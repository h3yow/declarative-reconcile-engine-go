# 本地声明式资源协调引擎（Go）

一个**完全在内存中**模拟"目标环境"的声明式协调引擎（declarative reconciliation
engine）。客户端只提交**期望状态**，引擎持续把**实际状态**收敛到期望状态，并在
并发提交、版本冲突、依赖排序、故障注入、进程停止/重启等情况下给出**一致、可解释**
的结果。不依赖任何外部服务，仅用 Go 标准库。

## 架构

```
客户端提交                期望状态(Store)              实际状态(Target, 内存模拟)
 Apply/Delete  ──准入──▶  记录 + 单调版本 + tombstone
   │                        │                            ▲
   │ 冲突即拒绝(可区分原因)   │  enqueue(去重/延迟重排)      │ create/update/ready/delete
   ▼                        ▼                            │
Decision 审计日志 ◀──── Engine（worker 池 + 周期 resync）─┘
```

| 文件 | 职责 |
| --- | --- |
| `types.go` | `Desired` / `Observed` / `Status` / `Decision` 等核心类型 |
| `errors.go` | 可区分的冲突原因（`ConflictError`/`ConflictReason`）、可重试的 `TargetError` |
| `defaults.go` | 准入时的**确定性缺省**（`kind=generic`、`replicas=1`） |
| `store.go` | 期望状态存储：乐观并发、版本、依赖环检测、tombstone |
| `target.go` | 目标环境内存模拟 + 故障注入（瞬时/永久、次数、延迟）与操作计数 |
| `reconciler.go` | 单个资源的一次协调（diff 实际 vs 期望，幂等收敛） |
| `queue.go` | 去重工作队列：in-flight dirty 合并、延迟重试、优雅排空 |
| `engine.go` | worker 池、resync 巡检、退避、准入、代际通行证（ticket）、审计日志 |

## 协调协议（reconciliation loop）

每个资源在工作队列中**串行去重**处理，多资源由 worker 池**并行**处理。一次协调：

1. 读取**最新**期望记录（进行中的提交不影响本次，下一通行证自然追上）。
2. **代际通行证校验**：仅当 `(version, tombstone)` 仍是最新准入代际时才允许产生目标
   侧副作用；排队中的陈旧通行证（延迟重试 / 周期 resync）直接跳过，从根上杜绝
   "删除后被僵尸通行证重建"之类的重复处理。
3. 按记录类型分支：
   - **Upsert**
     1. 依赖门禁：所有 `DependsOn` 必须存在、未删除且目标侧 Ready，否则 `Pending`
        并延迟重试；
     2. 实际不存在 → `create`（新建时非 Ready）；
     3. spec 不一致 → `update`（同时复活处于 Terminating 的对象）；
     4. 未 Ready → `ready` 信号操作；
     5. 全部一致 → `Healthy/InSync`，**零写入**。
   - **Delete（tombstone）**
     1. 目标已不存在 → 从期望存储 `purge`（终态）；
     2. 未标记终止 → `markDeletion`；
     3. 已标记 → `delete` → `purge`。删除路径立即再入队，终态化不依赖巡检时机。
4. 另有固定间隔的 **resync** 全量重入队，负责修复外部漂移（spec 被改 / 对象被外部
   删除）并重新驱动永久失败或依赖阻塞的资源——这保证了"短暂故障恢复后继续推进，
   任务不丢，资源不会永久卡住"。

所有目标操作都是幂等语义：删除不存在的对象算成功；重复收敛 `InSync` 不产生写入。

## 声明、更新、删除与版本（乐观并发）

`Desired.ExpectedVersion` 是强制前置条件，准入按存储互斥串行线性化：

| 提交 | 含义 |
| --- | --- |
| `-1` | 必须**不存在**（create） |
| 正数 `v` | 必须存在且当前版本恰好为 `v`（update / delete） |
| `0` 或 `< -1` | `InvalidVersion` |

被拒绝的提交**绝不修改任何状态**，并返回带 `ConflictReason` 的 `ConflictError`：

- `StaleVersion`：版本不匹配（含实际版本号，客户端可据此重试）；
- `MustNotExist`：`-1` 创建但已存在；
- `MustExist`：对未声明资源做更新/删除；
- `InvalidVersion`：版本号非法；
- `DependencyCycle`：引入依赖环；
- `DependencyBlocksDelete`：仍被其他存活资源依赖时删除；
- `Deleting`：对**删除终态中**（tombstone）的资源再做 Apply/Delete。删除是终态的，
  客户端必须等待删除完成（状态 `Deleted`、记录 purge）后再重新创建。

**重复提交相同期望状态不产生任何变更**：在版本校验前先做确定性缺省，并比较
`kind/spec/dependencies`（依赖列表排序去重），内容相同则返回**原版本号**、不递增、
不入队、不触发协调，记一条 `NoopApply/Identical` 决策。

**缺省语义确定**：缺省在准入时按固定规则物化（只补缺、不覆盖显式值），因此部分字段
的重复提交也会收敛到同一份字节。

## 依赖关系与生命周期

- 创建/更新按依赖拓扑推进：依赖未 Ready 时保持 `Pending`，不会提前创建（无孤儿）。
- 删除必须**自依赖者向被依赖者**反向进行；仍被存活资源依赖时以
  `DependencyBlocksDelete` 明确拒绝。
- 删除与并发变更叠加：准入串行 + tombstone 终态 + 代际通行证 + 队列撤销
  （`RemovePending`）共同保证只有一个明确结局，不出现孤儿、重复删除或僵尸重建。
- 从未物化的 Pending 资源可走 `hardDelete`（不产生任何目标调用）。

## 故障与重试规则

- 目标故障分两类：`Transient=true` 的 `TargetError` 可重试；永久错误进入
  `Progressing` 并记录 `LastError`，由周期 resync 限速重试（故障清除即恢复）。
- 瞬时错误采用**指数退避**（`BackoffBase` 起、`BackoffMax` 封顶）；单资源操作有
  `OpTimeout`，超时按瞬时错误处理。
- 单个资源失败/超时只影响它自己（通行证 + 独立退避），不阻塞其他资源；测试用操作
  计数证明故障期间健康邻居零影响、且故障资源不忙等（attempts 有上界）。
- 故障注入通过 `target.InjectFailure(FailureRule{...})`：可按资源 ID、操作
  （create/update/delete/ready）、瞬时性、触发次数、人工延迟配置；`Drift/DriftDelete`
  模拟外部漂移。

## 停止与重启

`Shutdown` 先取消所有进行中的协调（`context` 取消，含超时操作），再等待 worker
退出。语义保证：

- 进行中被打断的协调不会留下"半资源"——未完成的 create 不落库，目标侧只保留
  完整完成的工作；
- 期望记录是持久的（存在 Store 中），停止不丢任务；
- 重新 `Start` 会把所有现存期望记录重新播种（durable replay），打断的资源随后收敛。

## 可解释性

引擎为每个资源维护 `Status`（阶段、版本、尝试次数、`LastError`、最近 15 条
`Condition` 判定轨迹），并记录结构化 `Decision` 审计流：`AdmitApply` /
`NoopApply` / `RejectApply` / `AdmitDelete` / `RejectDelete` / `Reconcile` 等，
每条带 action、id、version、reason、detail、时间。测试日志会打印**输入与判定依据**。

## 验证方法

```bash
go test ./...                 # 全部 17 个用例
go test -race ./...           # 竞态检测
go test -v -run TestConflictReasons ./...   # 查看输入/判定日志示例
go test -count=30 ./...       # 重复稳定性（CI 建议搭配 -race）
```

用例覆盖：

- **准入/版本**：六种冲突原因可区分、拒绝不改状态、相同提交幂等零写入、缺省确定、
  依赖环拒绝；
- **并发**：40 goroutine 竞争创建（恰 1 个胜出，其余 `MustNotExist`）、CAS 风格的
  竞争更新串行化、删除/更新竞争两种结局都明确（测试用准入栅栏确定性地制造两种排序）；
- **依赖与生命周期**：依赖顺序创建（就绪时刻严格有序）、反向删除无孤儿、删除与并发
  变更叠加、终止中重声明复活、停止中断 + 重启回放、Pending 硬删除；
- **故障与恢复**：限定次数瞬时故障退避恢复且不拖累邻居、永久故障不忙等且清除后自愈、
  spec 漂移被纠正、对象被外部删除后重建、操作超时隔离。

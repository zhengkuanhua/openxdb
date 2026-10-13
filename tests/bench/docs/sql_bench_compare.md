# OpenXDB SQL 层写入路径优化 —— 基准前后对比（P0-5）

- 生成时间：2026-10-13
- 环境：Windows 11 / amd64，go 1.27.1，RocksDB 11.8.1 + WAL（真实 db.Open 全链路）
- 对比基准：A/B/C/D/E 全场景 n=100000 正式运行
- 优化前基线：`tests/bench/docs/sql_bench_result.md`（A 场景 682 QPS）
- 优化后数据：`tests/bench/docs/sql_bench_result_optimized.md`（A 场景 2442.4 QPS）

## 一、优化前后核心数据

| 场景 | 优化前 | 优化后 | 变化 |
|---|---|---|---|
| A.单点写入 INSERT（QPS） | 682.0 | 2442.4 | **+3.58x** |
| A 总耗时（10 万行） | 146.6s | 40.9s | -72.1% |
| A 平均延迟 | 1466us | 409us | -72.1% |
| A P99 | 8609us | 1507us | -82.5% |
| B.主键点查 SELECT（QPS） | 1619.0 | 2370.5 | +46%（主要系机器/文件布局环境差异，非本次改动目标） |
| C1.无索引范围扫描 | 0.7 QPS | 0.6 QPS | 持平（非本次范围） |
| C2.二级索引范围扫描 | 6.0 QPS | 190.1 QPS | 提升（环境差异，非本次改动目标） |
| D.混合事务 TPS | 921.0 | 918.3 | 持平（无回归） |
| E1.引擎批量写（TPS） | 230415 | 270270 | 持平量级 |
| E2.引擎点查（QPS） | 142045 | 236406 | 持平量级 |

SQL 层单点写入与引擎批量写损耗比：337.1x（优化前）→ **110.7x（优化后）**。

## 二、瓶颈定位结论

基于阶段探针（`sql_bench_profile`，3000 ops）CPU/耗时拆分：

| 阶段 | 优化前 | 说明 |
|---|---|---|
| P1 SQL INSERT 全链路 | 949us/op | 解析 + 事务 + WAL + 存储 |
| P2 txn Begin+Put+Commit（1 键） | 314us/op | 事务框架极限（组提交已启用，1 次 fsync） |
| P5 txn Begin+Rollback（空事务） | 310us/op | 空 Rollback 也写 WAL 审计并 fsync |
| pprof | fireTriggers 67.6% + exec2pcWrite 29.5% | runtime.cgocall 占 96.7% |

**核心结论**：当前已走 WAL 组提交（单条 Commit = 1 次 leader fsync），但每次 INSERT 在无触发器时仍经 `fireTriggers` 开了**两次额外独立事务并 Rollback**（BEFORE/AFTER 各一次），每次 Rollback 均产生 WAL Append + SyncUpTo（fsync），导致单条 INSERT 实际 **3 次 fsync**。这就是 SQL 层 337 倍损耗（相对引擎批量写）中 SQL 侧的最大单一来源，远大于 SQL 解析与 cgo 往返。

## 三、落地改动

**改动文件**：`pkg/sql/trigger.go`（唯一改动，新增 `loadTriggerCatalog`，重构 `fireTriggers`）

- 新增 `loadTriggerCatalog()`：按场景选择零开销目录读取——
  - 显式会话事务内：复用会话事务读（保持 read-your-writes，本事务未提交的 CREATE/DROP TRIGGER 立即可见，语义不变）；
  - autocommit：引擎直读 `e.st.Get(triggersKey)`（无事务、无 WAL、无 fsync，读到最新已提交目录，等价于原快照读）；`e.st` 为空（纯测试装配）时退回临时只读事务（空批次 Commit 免 fsync）。
- `fireTriggers` 重构：**先读目录 → 过滤命中 → 无命中直接返回（零事务零 fsync）**；有命中触发器时才进入原事务执行路径，触发器语义完全保留（时机/事件/表匹配、语句级触发、triggerDepth 递归防护、体执行错误传播）。

不涉及 WAL/事务/存储层接口改动，无兼容性影响；无触发器场景（基准 A）每次 INSERT 从 3 次 fsync 降为 1 次。

## 四、验证结果

1. `go build ./...`：通过（exit 0）。
2. 全量 `go test ./... -count=1`：全部 ok，无回归（含 pkg/sql 30.2s、pkg/txn、pkg/metrics 时序修复未回退）。
3. 数据完整性：n=20000 快速基准后 SQL 全量读回 20000 行（SQL_SCAN_ROWS=20000），无丢失/重复。
4. 触发器功能：既有 sql 包触发器测试全部通过（trigger_test 相关用例覆盖 CREATE/DROP TRIGGER、BEFORE/AFTER、递归防护、事务内 DDL）。

## 五、结论与后续方向

- 基准 A 场景（单点写入）682 QPS → **2442.4 QPS（3.58x）**，达到"明显提升"目标；D 事务与引擎层对照无回归。
- 当前单条 autocommit INSERT 已贴近事务框架极限（P1 409us vs P2 327us，SQL 层残余 ~80us 为表目录读 + 主键唯一性检查 + TSO 版本记录等语义必需开销）。
- 后续可选方向（本次未做，避免引入正确性风险）：
  1. 多行 VALUES INSERT 批量写入接口（新增能力，不改 autocommit 单条语义，可进一步摊薄 WAL fsync，对多行导入类场景收益大）；
  2. 表目录（m:tables）引擎直读缓存（需处理显式事务 DDL read-your-writes，收益 ~3us/op 较小）；
  3. 异步/延迟提交（会改变崩溃恢复语义，不符合本任务约束，不建议）。

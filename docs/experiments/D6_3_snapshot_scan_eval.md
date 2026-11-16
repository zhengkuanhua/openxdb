# D6.3 Snapshot 批量 Scan 复用评估

- 版本：v1.18.0 D6 读路径优化第 3 项（DEV_PLAN_v1.18.0 Phase D）
- 结论：**无新增代码点**——快照号复用、多区间单次合并扫描、批量版本过滤三项能力均已存在；
  剩余可优化点属跨 region 远端聚合，建议并入 v1.19 写路径/2PC 一并处理。

## 1. 评估目标

DEV_PLAN D6.3 要求评估「`scanMergedVisible` 快照复用」，即在 Snapshot 读路径上削减
重复取号与逐键过滤开销。评估按三处关键点核对现状。

## 2. 现状核对

| 关注点 | 位置 | 现状 | 是否需改 |
| --- | --- | --- | --- |
| 快照号复用 | `pkg/sql/executor.go:1288 snapshotTS` | 显式事务首条读语句取号并写入 `sess.curBeginTS`，后续读复用；autocommit 按语句取号写入 `sess.stmtBeginTS`（D3.3/M18：取号前 `ts.Reset()` 丢弃残余预取批次，保证跨节点强一致下拿到权威新鲜号） | 否，已复用 |
| 多区间一次合并扫描 | `pkg/sql/sharding.go:184 scanMerged(tx, ranges)` | `ranges` 以切片传入，单次调用内做内层键序合并，等价单机全表序；未按区间反复重建迭代器 | 否 |
| 版本可见性过滤 | `pkg/sql/multinode.go:213 scanMergedVisible` | 先收集全部 key，再一次 `cluster.BatchVersionVisible(tx.Scan, keys, beginTS)` 批量判定，非逐键往返；`beginTS==0`（单机/未开集群）直接短路 | 否，已批量化 |
| 远端 region 扫描 | `pkg/sql/multinode.go:243 scanMergedCluster` | 按 `ranges` 逐 region 展开：本地 region 走 `scanMergedVisible`，远端 region 每次一条 `QUERY_REQ`（`ScanRemote`，携带 beginTS 由归属节点过滤） | 是，但不在 D6 范围 |

## 3. 判断依据

1. **快照号不是瓶颈**：单条语句只取一次号，事务内复用同一 `curBeginTS`；此处的
   `ts.Reset()` 是跨节点可见性正确性要求（残留批号滞后于 2PC commitTS 会导致远端提交
   本地不可见），不能以性能为由去掉。
2. **批量 Scan 已批量化**：版本过滤从"逐键 scan 判定"变为"先收集后一次批量判定"，
   与 D6.2 的点查合并同型收益（削减的是调用次数，而非单次调用成本）。
3. **剩余收益在远端聚合**：`scanMergedCluster` 对每个远端 region 单独发一次
   `QUERY_REQ`，多分区表跨 N 个 region 即 N 次网络 RTT。可做「按归属节点聚合区间」的
   批量转发（一次带上多个 range），但：
   - 收益取决于分区数与 RTT，单机/单 region 场景无收益；
   - 需改 `ScanRemote` 协议与"归属节点按 beginTS 过滤"的回传路径，与 v1.19.0 M8
     写路径强制路由 + 2PC 参与共用同一套跨节点请求骨架，分开改会重复返工；
   - 归入 v1.19.0 一并处理，避免在 v1.18.0 引入协议变更风险。

## 4. D6 三项收口状态

| 子项 | 结论 | 证据 |
| --- | --- | --- |
| D6.1 块缓存调优 | 已完成，结论为**负收益不采纳**：块缓存调至 256MB 实测 -1.6%，已回退；瓶颈口径为 cgo 往返（`runtime.cgocall` flat 63.75%），非引擎缓存容量 | `docs/experiments/storage_engine_selection.md` §5–§6 |
| D6.2 批量点查合并深化 | 已完成：`routedJoin` / `evalInSubqueryRouted` 由 N 次点查压成 1 次 `mgetKVCluster`；N=64 基准 33.3ms → 0.52ms；用例断言 `Get=0`、`MGet=1` | `docs/experiments/D6_2_batch_pointget.md`、`pkg/sql/v1180_d6_batch_pointget_internal_test.go` |
| D6.3 Snapshot 批量 Scan | 已评估：现状已具备快照复用 + 批量过滤，无新增代码；远端区间聚合转 v1.19.0 | 本文 |

D6 验收口径（「至少一项落地 + 基准对比 + 无回归」）由 D6.2 满足并附基准；D6.1 以负收益
结论留档（同样属"有基准对比"）；D6.3 以评估结论留档。

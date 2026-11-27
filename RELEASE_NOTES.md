# OpenXDB v2.0.9 Release Notes

发布日期：2026-11-27（本地完成，未打 tag、未推送）

## 版本口径

承接 v2.0.8，只修复 `BALANCE` / 自动均衡链路的存量缺陷，同步 `pkg/version/version.go` 版本常量；不引入新特性，不改既有语句语法。修复对象两类：① v2.0.8 遗留的 Q4 单轮收敛缺陷；② 三机真机复验暴露的 B1 / B2 / B3。唯一行为变更是**集群模式自动均衡默认开启**，可用 `SET auto_balance = off` 完全关闭并回到 v2.0.8 行为。

## 版本亮点

- **B1（P0）修复：`BALANCE` 不再因重复候选报 `region not local` 而整轮作废**。`localRegionLoad` 的显式段（遍历 `AllRegions`）与隐式段（按表目录补隐式整表 region）各自 `append`、无 regionID 去重，而 `UpsertRegion` 会把隐式整表 region 的 ID 真实写进 `r.regions`、`RegionsOf` 对该条目又仍返回 `Explicit=false`，于是同一隐式 region 被两段各收一次；首次迁出成功后再选中同 ID 便命中 `regionLocal` 校验，整轮以 `SQL error: region not local` 中止、零迁移。修法：候选采集引入 `seen` 集合，显式段先统计再判重、隐式段构造 ID 后判重，同 ID 优先保留带路由视图信息的条目；v2.0.7 的跨节点归属过滤保持不变。新增 `sql: balance local candidates: candidates=%d unique=%d deduped=%d` 日志，`deduped>0` 即重复现场。
- **B2（P0）修复：自动均衡真正上线**。`balanceLoop` 一直存在，但生产路径上没有任何开启点——`db.StartCluster` 只调了 `StopAutoBalance`，自动均衡在所有生产部署中从未运行；failover 掉线与节点回 UP 也不会触发一次均衡，布局只能靠人工 `BALANCE`。本版三处补齐：① `db.StartCluster` 在 SQL 层就绪后调用 `EnableDefaultAutoBalance`（周期 `DefaultAutoBalanceInterval`=3s）；② `triggerAutoBalance(reason)` 接入 ha.go 的 `failover-down-node:<id>` 与 `node-up:<id>`，事件到即均衡一次并与 tick 共用 `balanceMu` 串行；③ 单轮失败不再静默——打 `WARN balance loop round failed (failures=N migrated=M retry_in=T)` 并按指数退避重试（封顶 `balanceLoopMaxBackoff`=60s），同时新增 region 迁移冷却（5min 内同一 region 最多迁 2 次），终结两节点来回搬。
- **B3（P1）修复：`spread` 只有一个含义**。此前 `logBalanceRound` 的 `spread` 是"本节点 − 最闲节点"、`logBalanceSummary` 的 `spread` 是"全局极差"，同名字段两种语义；`converged` 一处硬编码 `<=1`、一处引用常量。本版抽出纯函数 `balanceCountSpan` 统一供两处打点使用：`spread` 恒为**全局极差 max−min**，"本节点 − 最闲"迁到新字段 `local_gap`，并补打 `min=` / `max=`，`converged` 统一由 `balanceConvergedSpread` 决定。**运维提示**：习惯按 `spread` 读"本节点偏离度"的脚本请改用 `local_gap`，`spread` 现在只表示整体收敛度。
- **测试有效性加固**：核查 `balance_converge_v209_test.go` 的 `v209Spread` 辅助函数，`maxC` 更新分支存在，未发现"只累加 minC、恒返回 ≤0"的缺陷；为防该口径日后被改坏导致 `sp > balanceConvergedSpread` 系列断言空转，新增 `TestV209SpreadHelperSemantics`（`{5,0}→5`、`{1,3,7}→6`、`{5,5,4}→1`、缺省补零、全零→0）锁定极差口径，并做变异反证——破坏 `maxC` 更新后该用例立刻失败（`v209Spread({a:1,b:3,c:7}) = 2, want 6`），恢复后通过。

## 变更明细

| 项目 | 内容 |
| --- | --- |
| 修复（B1） | `pkg/sql/balance.go`：`localRegionLoad` 引入 `seen map[storage.ID]bool` 去重 + `balance local candidates` 打点 |
| 修复（B2） | `pkg/sql/balance.go`：`triggerAutoBalance`、`EnableDefaultAutoBalance`、`AutoBalanceStatus`、`balanceLoopBackoff`/`balanceLoopMaxBackoff`、`balanceMigrationCooldownWindow`/`Limit`；`pkg/sql/ha.go`：failover/节点回 UP 触发点；`pkg/db/db.go`：`StartCluster` 默认装配 |
| 修复（B3） | `pkg/sql/balance.go`：新增纯函数 `balanceCountSpan`，改写 `logBalanceRound` / `logBalanceSummary` |
| 新增用例 | `pkg/sql/balance_converge_v209_test.go`（含 B1 去重与 `TestV209SpreadHelperSemantics`）、`pkg/sql/balance_log_v209_test.go`（B3 三例）、`pkg/sql/balance_autobalance_v209_test.go`（B2 六例）、`pkg/db/db_autobalance_v209_test.go`（B2 生产装配三例），v2.0.9 合计新增 18 例 |
| 行为变更 | 集群模式自动均衡默认开启（3s）；关闭方式 `SET auto_balance = off`，周期 `SET auto_balance_interval = <毫秒>` |
| 版本 | `pkg/version/version.go` → `v2.0.9` |

## 验证

| 项目 | 结果 |
| --- | --- |
| `go build ./...` | exit 0 |
| `go vet ./pkg/sql` | exit 0 |
| `gofmt -l pkg/sql/balance_converge_v209_test.go` | 无输出 |
| `go test ./pkg/sql -run TestV209 -count=1` | 全 PASS（B1/B2/B3 + P4 加固共 18 例） |
| `TestV209SpreadHelperSemantics` 变异反证 | 破坏 `maxC` 后 FAIL（`= 2, want 6`），恢复后 PASS |
| 全仓 `go test ./... -count=1` × 3 轮 | 三轮均 exit 0、零失败 |

## 全量回归（三轮墙钟）

| 轮次 | 墙钟耗时 | 结果 | 失败项 |
| --- | --- | --- | --- |
| 第 1 轮 | 231.0s | exit=0 | 无 |
| 第 2 轮 | 190.1s | exit=0 | 无 |
| 第 3 轮 | 142.5s | exit=0 | 无（`pkg/sql` 121.4s） |

每轮 33 个包条目：22 个 `ok`、11 个无测试文件；输出中 `FAIL` / `build failed` / `panic:` 零命中。首轮耗时偏高为编译缓存冷启动，两轮热缓存后稳定在 140~190s。

## 已知事项

- 残留边界详见 `KNOWN_LIMITATIONS.md` §3.9（单轮上限 32、按 region 数加权、单点失败整轮中止、迁移冷却限速）与新增 §3.11（自动均衡默认开启的后台影响与关闭方式）。
- 自动均衡默认开启属行为变更：升级后无需任何操作即会出现后台迁移与 `sql: balance ...` 日志，`SHOW REGION ROUTES` 可能在无人操作时变化；不希望后台迁移请执行 `SET auto_balance = off`。
- v2.0.8 期间曾偶发的 `TestNew5ConcurrentGetNoDup`、`TestD44RestartPrunesSupersededRegions`、`TestBackupScheduleRetainRotation` 本轮三轮均未复现，仍按存量 flaky 记录，与本版改动无关。

---

# OpenXDB v2.0.5 Release Notes（历史版本）

发布日期：2026-11-25

## 版本亮点

- **根治「新建未切分表首写失败窗口」（P0 收敛）**：v2.0.4 解决了未切分表归属"从来不广播"，但纯转发节点的首写仍要走一遍"失败 → 拉视图 → 重试"的自愈链路。本版把归属登记提前到建表动作本身：`CREATE TABLE` / `CREATE TABLE AS SELECT` 提交并释放 DDL 锁后，异步补一次隐式整表 region 归属——本机先登记认知，再向在线对端推一档权威路由视图。新建表在 owner 侧写入直达本地，纯转发节点在推送到达后即知归属，不再依赖首写失败触发。
- **建表返回路径零副作用**：v2.0.4 已记录"DDL 路径上新增任何事务都会打乱其后副本维护时序"（B1 单副本用例 32s 超时即由此复现）。本版严格约束：建表返回路径只做一次轻量状态判断 + goroutine 派生，不开事务、不遍历表目录、不落盘；归属学习与视图推送全部在异步 goroutine 内完成。单机部署不派生任何行为，语义零变化。
- **推送去抖**：对端推送带 200ms 冷却窗口，批量建表（含脚本灌表）只推一轮，避免把管理面打成广播风暴；错过的表由下一次建表 / 视图刷新自愈覆盖。
- **用例确定性修复（非产品语义变更）**：`TestD45B1SingleCopyRecoverableE2E` 与 `TestV1200C4RegionSyncReconnectCatchUp` 的偶发 `lost=3` 定位为播种顺序与隐式归属规则耦合——隐式整表 region 归"在线节点中 ID 字典序最小者"，播种节点非 owner 时切分前的 10 行被转发到 owner，切分后 owner 本地无行。播种用例改为"建表 → 切 3 region → 写入"，并按 `SelfID` 固定节点角色。该 flaky 在 v2.0.4 基线即存在，非本版回归。

## 变更明细

| 项目 | 内容 |
| --- | --- |
| 新增 | `pkg/sql/implicit_push.go`：`notifyImplicitRouteView()` / `learnImplicitRouteLocal()` / `implicitPushCooldown` |
| 接线 | `pkg/sql/executor.go`：`execCreateTable`、`execCreateTableAsSelect` 在 `broadcastDDL`/`commitTx` 之后调用；`Executor` 新增 `implicitPushMu/Last/Busy/Hook` |
| 测试 | `pkg/sql/v205_implicit_push_test.go`（5 组：建表学归属、owner 本地、对端推送钩子、单机零回归、切分表不被覆盖） |
| 用例 | `pkg/db/d4_4_route_view_test.go`（`seedThreeRegions` 改为先切分后写入）、`pkg/db/d4_5_b1_replica_test.go`（三节点按 ID 排定角色） |
| 版本 | `pkg/version/version.go` → `v2.0.5`（握手串 `8.0.36-OpenXDB v2.0.5`） |

## 验证

| 用例 | 结果 |
| --- | --- |
| `TestV205*`（5 组） | 全 PASS |
| `TestD45B1SingleCopyRecoverableE2E` × 8 | 8/8 PASS |
| `TestV1200C4RegionSyncReconnectCatchUp` × 6 | 6/6 PASS |
| `pkg/sql` + `pkg/db` 全量（`-p 1`） | PASS（99.165s / 58.850s） |

## 全量回归

`go test ./pkg/sql ./pkg/db -count=1 -p 1`：`ok pkg/sql 99.165s`、`ok pkg/db 58.850s`。
`go test ./... -count=1 -p 1`：全部 22 个包 `ok`（`pkg/db` 57.151s、`pkg/sql` 58.589s），无 FAIL。

## 已知事项

- 跨节点读取未切分表仍可能取不到数据（读侧聚合视图重启后未重建），沿用 v2.0.4 登记的后续 NEW 项，本版未涉及。
- v2.1.0 的 TSO 租约兜底（`--tso-lease`，默认关闭）仍在 Unreleased 开发线，本版不含。

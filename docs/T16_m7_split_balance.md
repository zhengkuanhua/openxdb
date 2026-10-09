# T16 M7 Region 自动分裂与负载均衡（Split & Balance）
- 状态：已落地（v5.0-P5-M7，v0.7.0-alpha）
- 前置：M3 分片、M4 集群、M5 2PC 写路径、M6 高可用/故障转移
- 目标：写路径自动检测 region 规模超阈值并沿数据中点分裂；按节点 region 数/数据量负载均衡（手动 BALANCE 或周期自动触发）；迁移期间读写不中断（在线迁移，REGION_PUSH 装载目标）；单机模式 BALANCE 明确报 "cluster not enabled"（M4/M5/M6 行为零回归）
- 设计取舍：全部挂接在既有 sharding Router / 集群路由 / 2PC / M6 心跳机制之上；单节点部署自动分裂仍生效（本地 region 可分裂），但 BALANCE/迁移只在集群启用时执行

## 1. 背景与问题
M3 已提供 RegionInfo 元数据、Router（`[StartKey, EndKey)` 半开边界）、region 前缀物理键与手动 SPLIT REGION；M4/M5 提供节点注册表、集群路由表（region→node）、跨节点读写与 2PC；M6 提供 down 检测与 region 重指派。不足：region 只能手动分裂，规模增长后单个 region 无限膨胀（单 region 数据倾斜）；多节点下 region 分布不均时没有自动调度；且单机模式执行 `BALANCE` 时因 `db.Open` 默认创建 cluster.Manager 导致 `e.mgr == nil` 判断失效、静默返回 nil 而非报错。M7 补齐这三个闭环。

## 2. 总体设计
M7 全部落在 `pkg/sql/balance.go`（约 518 行，含 `pkg/sql/sql.go` 引擎代理），复用 M3 Router 边界语义与 M4 REGION_PUSH 装载协议：

| 子功能 | 入口 | 关键函数 | 集群依赖 |
|---|---|---|---|
| 自动分裂 | 写路径提交后 | `maybeAutoSplit` / `checkAutoSplit` / `countRegionRows` / `splitRegion` | 无（单节点即可用） |
| 手动分裂 | `SPLIT REGION` 语句 | `execSplitRegion` / `splitRegionByID` | 无 |
| 手动均衡 | `BALANCE` 语句 | `execBalance` / `balanceOnce` | 需 `HasRemoteNodes` |
| 自动均衡 | 周期 goroutine | `balanceLoop` / `setAutoBalance` / `stopAutoBalance` | 需 `HasRemoteNodes` |
| 在线迁移 | 均衡调度内部 | `migrateRegion` / `regionCountOnNode` / `localRegionLoad` | 需 `HasRemoteNodes` |

### 2.1 自动分裂（写路径检测）
- 写路径（execInsert/execUpdate/execDelete）提交后累计表级写计数；水位达到阈值一半时触发 `checkAutoSplit` 做**真实行数检查**（`countRegionRows` 扫描本地 region 物理键空间），避免只按写计数误判。
- 本地 region 行数超过阈值则沿**数据中点**一分为二：`splitRegion` 以 `innerOf` 提取含 `s` 前缀的内层键，取中点作为新 region 边界（`StartKey` 含 / `EndKey` 排他，与 `EncodeRegionKey`/`EncodeTableKey` 语义一致，即 `'s'+tableID:8B+pk`）。
- 分裂在同一事务内完成「存量数据重路由 + 路由表替换 + 元数据落盘」，路由与副本同步更新；与 M3 手动 `SplitTable` 兼容——`SPLIT REGION` 语句按数据中点分裂，显式边界仍走 `SplitTable`。
- 配置接口：`Engine.SetAutoSplit(enabled bool, rowThreshold int64)`；主动触发：`Engine.SplitRegionNow(regionID uint64)`。

### 2.2 负载均衡（均衡调度）
- 手动：`BALANCE` 语句 → `execBalance` → `balanceOnce`；自动：`Engine.SetAutoBalance(enabled, interval)` 启动 `balanceLoop` 周期 goroutine，`StopAutoBalance()` 停止（`balanceStop` channel）。
- 调度策略：统计本节点本地 region 行数负载（`localRegionLoad` / `regionCountOnNode`），当本节点 region 数**显著多于最闲存活节点**时，把最大 region 在线迁移过去；down 节点不计入候选（复用 M6 `IsDown`）。
- `BALANCE` 返回受影响行数（迁移的 region 数）：均衡后再次 BALANCE 返回 0。

### 2.3 在线迁移（读写不中断）
- 迁移过程源节点**路由不变**、读写不中断（存量流量继续打向源节点，2PC/转发照常工作）；
- 步骤：扫描源 region 物理行（含 KV + 索引）→ `REGION_PUSH` 装载目标节点（数据与权威路由视图原子落盘，目标节点 `applyPush` 本地写入 + 学路由 + 落盘 `m:regions`）→ 迁移完成才切换本节点路由归属并落盘；其余存活节点经下一次心跳路由同步学习新归属（M4 `getClusterRoutes` 机制）。
- 迁移期间对目标节点而言数据已在本地，切换后源节点转发/本地读均指向新 owner，无窗口期错误。

### 2.4 集群启用判据（单机报错修复）
- 缺陷根因：`db.Open` 默认创建 `cluster.Manager` 并注入 executor（`Engine.SetCluster`），单机模式 `e.mgr != nil`，原 `if e.mgr == nil` 判断失效，`Execute("BALANCE")` 走不到报错分支直接返回 nil。
- 修正：集群启用判据统一改为 **`e.mgr == nil || !e.mgr.HasRemoteNodes()`**（`HasRemoteNodes()` 返回是否存在非本节点注册节点）：
  - `execBalance`：单机 → `&SQLError{Msg: "cluster not enabled"}`；集群 → 正常调度；
  - `migrateRegion`：同判据兜底（单机/无远端绝不迁移）；
  - `balanceLoop`：单机同样不启动迁移（周期循环无候选自然跳过）。
- 单机模式自动分裂不受影响（无集群依赖）；M4/M5/M6 行为零回归（`TestM7BalanceClusterDisabled` 验证）。

## 3. SQL / 接口
- 语句：`BALANCE`、`SPLIT REGION`（lexer 含 BALANCE/SPLIT 关键字；parser `parseStmt` 分派 `case "BALANCE"` / `case "SPLIT"` → `parseSplitRegion`；ast 定义 `BalanceStmt` / `SplitRegionStmt`）。
- 引擎接口（`pkg/sql/sql.go`）：`SetAutoSplit`、`SetAutoBalance`、`StopAutoBalance`、`SplitRegionNow`、`BalanceNow`、`ExecutorRouter`（复用 M3）。

## 4. 测试
`pkg/sql/balance_test.go`：
- `TestM7AutoSplitTriggersOnWriteThreshold`：写计数水位 → 真实行数检查 → 数据中点分裂，边界断言使用 `m7TKey`（`'s'+tableID(1)+8B pk`）验证 StartKey/EndKey 落盘格式与 EncodeTableKey 一致
- `TestM7SplitRegionManualStmtBoundary`：手动 SPLIT REGION 按数据中点分裂，`[4,6)/[6,8)` 边界正确
- `TestM7SplitRegionNowFunc`：`SplitRegionNow(regionID)` 主动触发
- `TestM7SplitKeepsSecondaryIndexConsistent`：分裂后索引一致
- `TestM7BalanceClusterDisabled`：单机 `BALANCE` 报 `cluster not enabled`（本次缺陷修复的回归测试）

`pkg/db/db_m7_test.go`：
- `TestM7ManualBalanceMigratesHotRegion`：4 region 拓扑（A 3 / B 1），手动 BALANCE 迁移最大 region 到 B，路由切换、A/B 读写一致、迁移后点查/写可用、二次 BALANCE 返回 0
- `TestM7AutoBalanceTriggersMigration`：150ms 周期自动触发迁移
- `TestM7BalanceSkipsDownNode`：B down 后 BALANCE 不迁移（无存活候选），数据经 M6 failover 保持完整

## 5. 验收
- `go build ./...` 全绿；`go test ./... -count=1` 全包通过（本机 CGO 工具链：go1.27.1 + winlibs gcc16.2.0，CGO_CFLAGS/CGO_CXXFLAGS 指向 RocksDB include，CGO_LDFLAGS 指向 `rocksdb-build -lrocksdb`，仓库 #cgo 附加 `-static-libstdc++ -static-libgcc -lwinpthread -lshlwapi -lrpcrt4 -lws2_32`）。

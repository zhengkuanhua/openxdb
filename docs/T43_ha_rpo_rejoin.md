# T43 v1.6.0 P2-4：HA 后续——async 丢 RPO 补偿 / 回迁自动化设计草案

> 版本：v1.6.0-P2-4（阶段一、阶段二均已实现）
> 日期：2026-10-24
> 状态：阶段一（RPO 补偿）落地；阶段二（回迁自动化）落地
> 范围：
> - `pkg/replication/follower.go`（FollowerStatus/Follower 扩展 + PullCatchup 一次性追平）
> - `pkg/replication/replication.go`（RoleCompensate 角色 + msgCompensate 帧常量）
> - `pkg/replication/protocol.go`（encodeCompensation/decodeCompensation）
> - `pkg/replication/master.go`（补偿帧接收：SetCompensationHandler/handleCompensation）
> - `pkg/replication/compensation.go`（PushCompensation 一次性补偿连接）
> - `pkg/db/ha.go`（promoteInfo/LastPromoteInfo/追平窗口 + 回迁入口 StaleMasterReady/TryRejoinAsFollower/FullAlignViaRegionPush）
> - `pkg/db/db.go`（FailoverCatchupTimeout 可配置字段 + ReceiveCompensation/compMu）
> - `pkg/db/db_ha_repl_test.go`（阶段一三测试）、`pkg/db/db_rejoin_test.go`（阶段二回迁测试）
> - 文档：本设计草案（docs/T43_ha_rpo_rejoin.md）
> 闭环：`go build ./...` 无输出；`go test ./pkg/replication/...`、`go test ./pkg/sql/...` 全绿；
> `go test ./pkg/db/...` 仅基线失败 TestM6DownNodeReadFailover（排期外不修改）

## 背景

v1.6.0 第 4 项「HA 后续」承接 M6 主从切换（docs/T15_m6_ha.md）与半同步复制（docs/T33_m20_sync_semi_sync_replication.md）：

- 复制为 **async**：主端 binlog append-only 落盘后异步推送从端，从端严格顺序应用并回 msgAck；
  AppliedLSN 仅内存，主端只记录从节点确认位点（master.go Ack）。
- **async 丢 RPO**：主端故障后，从端只持有已应用位点；主端故障前已落盘但尚未送达从端的
  binlog 段（“RPO 窗口”）随旧主一起消失，从端升主后这批写永久丢失且无审计记录。
- **回迁缺失**：旧主恢复后无自动路径识别新主、补回未复制段、重置复制基线重新从属。

阶段一解决「丢多少」——位点通告 + RPO 审计 + promote 前追平窗口（旧主短暂可达时尽力补偿）；
阶段二解决「丢的怎么回来 + 角色回迁」——新增补偿帧回灌 + 复制基线重置（本实现）。

## 阶段一：RPO 补偿（本实现）

### 1. 主端位点通告

| 组件 | 改动 | 说明 |
|---|---|---|
| helloOK 载荷 | 既有实现（T33）已含 | `master.go` 握手响应 `encodeHelloOK(m.store.LastLSN(), m.ReplicationMode())`：`lastLSN u64 LE + mode u8 LE` 尾随在旧 payload 前缀之后；`decodeHelloOK` 兼容旧 10B 帧（旧主无位点字段=0） |
| Follower 握手 | `follower.go` | `decodeHelloOK(payload)` 取 last 记录 `lastMasterLSN = max(已知, last)` |
| msgBinlog 路径 | `follower.go` | 每条 binlog 应用前 `recordMasterLSN(e.LSN)`（max 语义，主端推送位点单调递增） |

向后兼容：编码保持旧 payload 前缀不变、仅尾部追加字段；旧主（无 lastLSN 字段）返回 0，
从端按 `max` 语义退化为无位点通告模式，不影响既有复制。

### 2. RPO 审计

- `FollowerStatus` 新增 `LastMasterLSN uint64`（Status() 填充）；新增 `Follower.LastMasterLSN() uint64`。
- 新增 `Follower.RpoGap() uint64`：`= LastMasterLSN - AppliedLSN`，小于 0 记 0。
- `DB` 新增 `promoteInfo{At time.Time, RpoLost uint64}` 与 `LastPromoteInfo() promoteInfo`、
  `recordPromoteInfo`（`promoteMu` 保护）；**不改动现有返回签名**。
- `PromoteToMaster` 停止 follower 后记录 `RpoGap()` 到 `lastPromote`（追平窗口补偿后剩余缺口）。

### 3. Promote 前追平窗口

- `DB.FailoverCatchupTimeout time.Duration`：默认 `2 * time.Second`（Open 构造）；
  **0 = 跳过追平，保持旧行为**（回归开关）。
- `failoverLoop` 检测到 offline 后、`PromoteToMaster` 前调用 `catchupBeforePromote`：
  - 旧主地址可达（`PullCatchup` 内 `catchupDialTimeout=500ms` 短探测）→ 以续传方式重连旧主；
  - 追平目标 = 主端通告位点 `lastMasterLSN`（握手 + 每个 msgBinlog max 更新）；
  - 追平到目标或窗口超时（ctx deadline）即停；旧主不可达/窗口超时直接带剩余 RPO 提升。
- 并发安全：`failoverLoop` 单 goroutine 顺序执行，追平期间**不会重复 promote**；
  `PullCatchup` 用**独立一次性连接**（不写 `f.conn`），复用最近一次 `StartTLS` 的 `savedTLS`
  （nil=明文），`ApplyBinlog` 幂等（锁内写存储），与主复制循环可安全并发；
  同步执行、`defer conn.Close()`，**不泄漏 goroutine**；不修改 Follower 主复制状态。

### 4. 阶段一测试（pkg/db/db_ha_repl_test.go）

| 用例 | 场景 | 断言 |
|---|---|---|
| TestM6PromoteRpoLostRecorded | 主端先写 300 条，从端握手获知位点后仅应用少量即断开 | `LastPromoteInfo().RpoLost` 精确等于 `主端位点 - 断开时 AppliedLSN`（`wantLost = total - applied`，非 flaky） |
| TestM6FailoverCatchupRpoZero | follower 落线但旧主端口仍可达，failover 自动追平 | `RpoLost = 0`；旧主在窗口内新写的键已补齐（k0..k14 均存在） |
| TestM6FailoverCatchupDisabledRegression | `FailoverCatchupTimeout = 0` | 跳过追平照常 promote；窗口内旧主新键 k9 缺失、k0 既有数据存在（旧行为回归） |

## 阶段二：回迁自动化（本实现）

### 目标

旧主（M_old）故障后，新主（M_new，原从端）接管写路径并推进 binlog（LSN 从 1 重新计数）。
M_old 恢复后自动完成：识别新主 → 回灌崩溃前未复制段 → 重置复制基线 → 作为全新 follower 从属，
且数据最终一致；binlog 损坏时走 SQL 层 region push 全量对齐兜底（接口骨架 + 文档说明，联动为后续排期）。

### 1. 旧主恢复后识别新主

- 复用 P0 HA 既有能力：`msgCapability` 版本协商（epoch/fencing 位）、`NoteEpoch`、
  `ReconcileAfterPartition`（epoch 大者胜收敛）。
- `DB.StaleMasterReady() (bool, uint64, error)` 判定可回迁陈旧主：本地 binlog 文件存在且
  非空（`BinlogLSN()>0`，崩溃前曾是主节点）+ 已降级只读（`ReadOnly()`，NoteEpoch 已收到
  更高 clusterEpoch，写路径被 fencing 拒绝）→ 就绪，返回 binlog 最大位点；
  binlog 为空（从未主/已重置）→ 非陈旧主跳过；binlog 损坏 → 返回错误（走全量对齐兜底）。

### 2. 崩溃前未复制 binlog 段经补偿帧回灌

- 新帧类型 `msgCompensate = 32`（replication.go 帧表新增）；新角色 `RoleCompensate = 3`。
- **帧格式（本实现）**：`payload = startLSN u64（段起始位点）| count u32（条目数）| records`；
  records 逐条复用既有 `encodeBinRecord/decodeBinRecord`（每条含 CommitLSN/SchemaVer 与
  OPENXDB_ROWS_EVENT 语义 kind/table_id/pk/raw_value），不新增字段格式。
- **发送侧**：`replication.PushCompensation(ctx, masterAddr, entries, tlsCfg)`——
  独立一次性连接（不占用从端 dump 流、不注册从端）：`RoleCompensate` 握手 → 收 helloOK
  （取主端位点）→ capability 帧 → 按 `maxCompensateBatch=512` 分批发送 msgCompensate、
  每批等 msgAck 确认；`defer conn.Close()` 用完即释放，不泄漏句柄；30s 连接超时兜底。
- **接收侧**：`master.go` 新增 `compensateFn` 与 `SetCompensationHandler(fn)`；`handleSlave`
  中 role==RoleCompensate 分支走 `handleCompensation`（回 HelloOK+Capability，循环读
  msgCompensate → 解码 → 调补偿回调 apply → 回 msgAck；EOF 结束）。
- **DB 层入口**：`DB.ReceiveCompensation(entries []*replication.BinlogEntry) error`——
  `compMu` 串行化批次：逐条 `Storage.Write(batch)` 幂等 apply（PUT/DELETE 覆盖语义）后
  `Replicator.Append` 追加进自身 binlog（LSN 续增、CommitLSN 保留源值、SchemaVer 透传），
  使其他从端经正常复制流同步到补偿数据；补偿写入不经过 Txn/commit hook，无竞争窗口。
- **冲突语义：新写为准**——补偿先 apply、promote 后新写后 apply 时键冲突由新写覆盖
  （均走同一存储覆盖语义；binlog 顺序即最终顺序，追平幂等）。
- 幂等：PUT/DELETE 覆盖语义天然幂等；断线重发不重复生效；从端已确认段也重放（更稳，无害）。

### 3. 旧主重置复制基线，作为全新 follower 从 LSN=1 追平

- `DB.TryRejoinAsFollower(newMasterAddr) error` 回迁入口：
  1. `StaleMasterReady` 前置校验（binlog 非空 + 已降级只读），不满足返回 `ErrNotStaleMaster`；
  2. 停止旧复制器（若有）→ `readLocalBinlogRange(1, lastLSN)` 读全段 → `PushCompensation`
     回灌新主（30s ctx 超时）；失败返回 `ErrCompensationFailed`（调用方走全量对齐兜底）；
  3. 补偿成功 → `archiveBinlog`（binlog 改名为 `.rejoin-bak-<unixnano>`，防重启重复回迁）
     → 停止残留 Follower → `StartFollower(newMasterAddr)` 以全新 follower 身份从 LSN=1
     追平新主完整 binlog（新写覆盖补偿段，幂等；追平后进入正常 streaming 增量同步）。
- 基线重置即“旧身份作废”：旧 binlog 归档、旧 Replicator 关闭、epoch 只读态保留（写路径
  持续 fencing，不回写身份）。
- 风险与兜底：追平期间 M_new 继续写（binlog 追加），M_old 追平到当前尾后进入正常
  streaming，由后续 msgBinlog 增量同步，无窗口丢失（async 语义下 RPO=0 至多等于在途段）。

### 4. binlog 损坏兜底：SQL 层 region push 全量对齐

- 触发：M_old 崩溃后本地 binlog 损坏/缺失（`BinlogLSN`/段读取失败），无法按段补偿。
- 实现（v1.7.0 已接通）：`TryRejoinAsFollower` 补偿失败自动降级调用
  `DB.FullAlignViaRegionPush(newMasterNodeID, newMasterAddr)`：定位新主节点后经集群
  `RequestFullAlign`（`cluster.FullAlignReq`/`handleFullAlign`，`SetFullAlignHandler`
  装配）请求新主导出全部 region 当前行——SQL 层 `PushAllRegionsToNode` 遍历
  `AllRegions` + 隐式单 region 扫描物理键，`e.mgr.PushRegion` 逐 region 推送，
  旧主侧 `applyPush` 覆盖落本地；对齐成功后归档旧 binlog、停旧链路，以全新
  follower 从 LSN=1 追平新主（复用 §3 重置基线）。失败统一返回 `ErrFullAlignFailed`。
- 代价：全量对齐吞吐低于增量补偿，仅作兜底；与 SQL 层既有 region 同步机制复用，不新造复制协议。

### 5. 明确不做（排期外）

- **region 多副本**：数据页级多副本/raft 组复制（超出复制轨，属存储层改造）；
- **TSO 高可用**：时间戳服务单点 HA/双活（独立 TSO 组件，排期外）；
- **跨机房多活**：多活写入/冲突 CRDT 语义（区域级拓扑，排期外）。

## 文件清单

| 文件 | 改动 | 状态 |
|---|---|---|
| pkg/replication/follower.go | FollowerStatus/Follower 字段扩展、LastMasterLSN/RpoGap/recordMasterLSN、握手与 msgBinlog 记录位点、savedTLS、PullCatchup、catchupDialTimeout | 阶段一已实现 |
| pkg/replication/replication.go | 帧表新增 msgCompensate（阶段二）、RoleCompensate 角色 | 阶段二已实现 |
| pkg/replication/protocol.go | encodeCompensation/decodeCompensation（复用 encodeBinRecord/decodeBinRecord） | 阶段二已实现 |
| pkg/replication/master.go | compensateFn/SetCompensationHandler、handleSlave RoleCompensate 分支、handleCompensation 接收循环 | 阶段二已实现 |
| pkg/replication/compensation.go | PushCompensation 一次性补偿连接（分批发送 + ACK） | 阶段二已实现 |
| pkg/db/ha.go | promoteInfo/LastPromoteInfo、catchupBeforePromote、failoverLoop 追平窗口（阶段一）；StaleMasterReady/TryRejoinAsFollower/readLocalBinlogRange/archiveBinlog/FullAlignViaRegionPush（阶段二） | 均已实现 |
| pkg/db/db.go | FailoverCatchupTimeout 字段与默认值；ReceiveCompensation/compMu、StartReplicationTLS 装配补偿回调（阶段二） | 均已实现 |
| pkg/db/db_ha_repl_test.go | 阶段一三测试 | 阶段一已实现 |
| pkg/db/db_rejoin_test.go | 阶段二回迁测试（回灌/识别重置/兜底判定） | 阶段二已实现 |
| docs/T43_ha_rpo_rejoin.md | 本文档 | 本提交 |

## 测试矩阵

| 阶段 | 用例 | 覆盖点 | 状态 |
|---|---|---|---|
| 一 | TestM6PromoteRpoLostRecorded | RPO 审计精确性（RpoGap → promoteInfo.RpoLost） | ✅ 通过 |
| 一 | TestM6FailoverCatchupRpoZero | 追平窗口生效：可达旧主补段、RpoLost=0 | ✅ 通过 |
| 一 | TestM6FailoverCatchupDisabledRegression | 回归开关：Timeout=0 跳过追平、旧行为不变 | ✅ 通过 |
| 一 | TestM6PromoteFollowerWriteContinues / TestM6AutoPromoteFollowerOnMasterDown | 既有 HA 回归 | ✅ 通过 |
| 一 | pkg/replication 全量 / pkg/sql 全量 | 编解码/复制协议/上层回归 | ✅ 通过（TestM6DownNodeReadFailover 基线失败排期外不修改） |
| 二 | TestRejoinCompensationBackfill | 补偿帧回灌：M_old 段 → M_new apply+追加 binlog；新主数据与旧主崩溃前一致；其他从端同步到补偿段 | ✅ 通过 |
| 二 | TestRejoinStaleMasterResetsAndTracksNewMaster | 旧主识别新主 epoch（fencing 降级只读）→ 补偿回灌 → 归档 binlog 重置基线 → 全新 follower 追平新写 | ✅ 通过 |
| 二 | TestRejoinFallbackPathDecision | 兜底判定：binlog 为空跳过（ErrNotStaleMaster）/ binlog 损坏走补偿失败（ErrCompensationFailed）/ FullAlignViaRegionPush 未就绪（ErrFullAlignNotReady） | ✅ 通过 |


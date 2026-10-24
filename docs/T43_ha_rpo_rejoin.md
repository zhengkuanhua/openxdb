# T43 v1.6.0 P2-4：HA 后续——async 丢 RPO 补偿 / 回迁自动化设计草案

> 版本：v1.6.0-P2-4（阶段一已实现，阶段二设计草案待立项）
> 日期：2026-10-24
> 状态：阶段一（RPO 补偿）落地；阶段二（回迁自动化）设计草案评审后立项
> 范围：
> - `pkg/replication/follower.go`（FollowerStatus/Follower 扩展 + PullCatchup 一次性追平）
> - `pkg/db/ha.go`（promoteInfo/LastPromoteInfo/追平窗口 catchupBeforePromote）
> - `pkg/db/db.go`（FailoverCatchupTimeout 可配置字段 + 默认 2s）
> - `pkg/db/db_ha_repl_test.go`（阶段一三测试）
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
阶段二解决「丢的怎么回来 + 角色回迁」——新增补偿帧回灌 + 复制基线重置（设计草案，待立项）。

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

## 阶段二：回迁自动化（设计草案，待立项）

### 目标

旧主（M_old）故障后，新主（M_new，原从端）接管写路径并推进 binlog（LSN 从 1 重新计数）。
M_old 恢复后自动完成：识别新主 → 回灌崩溃前未复制段 → 重置复制基线 → 作为全新 follower 从属，
且数据最终一致；binlog 损坏时走 SQL 层 region push 全量对齐兜底。

### 1. 旧主恢复后识别新主

- 复用 P0 HA 既有能力：`msgCapability` 版本协商（epoch/fencing 位）、`NoteEpoch`、
  `ReconcileAfterPartition`（epoch 大者胜收敛）。
- M_old 恢复启动后不自动重开复制；先探测集群对端（SQL 层 region 目录/心跳），
  比较 `clusterEpoch`：对端 epoch 更大 → 对端是新主，M_old 进入回迁流程；
  M_old epoch 更大（无对端存活）→ M_old 维持主身份（split-brain 由 quorum/epoch 仲裁已有语义兜底）。

### 2. 崩溃前未复制 binlog 段经补偿帧回灌

- 新帧类型 `msgCompensate`（replication.go 帧表新增）：
  `payload = lastLSN u64（主端当前位点）| seq u64 | schema_ver u64 | kind u8 | table_id u64 | pk lenenc | raw_value lenenc`，
  语义对齐现有 OPENXDB_ROWS_EVENT（PUT/DELETE）。
- 触发：M_old 恢复后读取**崩溃前本地 binlog**（自 LSN=1 起，含崩溃瞬间未推送段）；
  M_old 记录的确认位点（master.go Ack 侧从端确认）< 崩溃时 binlog 尾 → 存在未复制段。
- 回灌路径：M_old 以补偿连接向 M_new 发送未复制段（逐条 msgCompensate）；
  **M_new 侧 apply 语义**：按 table_id+pk 写入新主存储，并**追加进自己 binlog**（新 LSN 续计数），
  使其他从端可同步；**冲突语义：新写为准**——补偿帧仅填充不存在或旧版本键
  （新主已在新写路径覆盖的键不覆盖，保证写冲突不丢新写）。
- 幂等：补偿帧携带 seq/位点，M_new 按主键幂等应用；断线重发不重复生效。

### 3. 旧主重置复制基线，作为全新 follower 从 LSN=1 追平

- 回灌完成后 M_old 丢弃本地旧 binlog（或归档），`StartFollowerTLS(M_new)`：
  hello `from=1`，从 M_new 的 binlog 头开始全新追平——M_new binlog 已含
  「M_old 未复制段 + 新写段」，追平后 M_old 与 M_new 数据一致。
- 复制基线重置即“旧身份作废”：M_old 的旧 epoch/旧 master 身份下线，
  M_new 的 Ack 记录从 LSN=1 重新计数。
- 风险与兜底：追平期间 M_new 继续写（binlog 追加），M_old 追平到当前尾后进入正常
  streaming，由后续 msgBinlog 增量同步，无窗口丢失（async 语义下 RPO=0 至多等于在途段）。

### 4. binlog 损坏兜底：SQL 层 region push 全量对齐

- 触发：M_old 崩溃后本地 binlog 损坏/缺失（无法定位未复制段），无法按段补偿。
- 方案：复用 SQL 层 region 数据通道（引擎 region push / scan + apply 全量）：
  M_new 导出全量 region 快照（含数据与表目录 schema_ver），M_old 清空后全量 apply，
  再走 §3 重置基线从 LSN=1 追平。
- 代价：全量对齐吞吐低于增量补偿，仅作兜底；与 SQL 层既有 region 同步机制复用，
  不新造复制协议。

### 5. 明确不做（排期外）

- **region 多副本**：数据页级多副本/raft 组复制（超出复制轨，属存储层改造）；
- **TSO 高可用**：时间戳服务单点 HA/双活（独立 TSO 组件，排期外）；
- **跨机房多活**：多活写入/冲突 CRDT 语义（区域级拓扑，排期外）。

## 文件清单

| 文件 | 改动 | 状态 |
|---|---|---|
| pkg/replication/follower.go | FollowerStatus/Follower 字段扩展、LastMasterLSN/RpoGap/recordMasterLSN、握手与 msgBinlog 记录位点、savedTLS、PullCatchup、catchupDialTimeout | 阶段一已实现 |
| pkg/replication/replication.go | 帧表新增 msgCompensate 与编解码（阶段二） | 草案 |
| pkg/replication/master.go | 追平连接从端身份/确认位点处理、补偿帧发送侧（阶段二） | 草案 |
| pkg/db/ha.go | promoteInfo/LastPromoteInfo、catchupBeforePromote、failoverLoop 追平窗口 | 阶段一已实现 |
| pkg/db/db.go | FailoverCatchupTimeout 字段与默认值；回迁后 StartFollowerTLS 复用（阶段二） | 阶段一已实现 / 草案 |
| pkg/db/db_ha_repl_test.go | 阶段一三测试 | 阶段一已实现 |
| docs/T43_ha_rpo_rejoin.md | 本文档 | 本提交 |

## 测试矩阵

| 阶段 | 用例 | 覆盖点 | 状态 |
|---|---|---|---|
| 一 | TestM6PromoteRpoLostRecorded | RPO 审计精确性（RpoGap → promoteInfo.RpoLost） | ✅ 通过 |
| 一 | TestM6FailoverCatchupRpoZero | 追平窗口生效：可达旧主补段、RpoLost=0 | ✅ 通过 |
| 一 | TestM6FailoverCatchupDisabledRegression | 回归开关：Timeout=0 跳过追平、旧行为不变 | ✅ 通过 |
| 一 | TestM6PromoteFollowerWriteContinues / TestM6AutoPromoteFollowerOnMasterDown | 既有 HA 回归 | ✅ 通过 |
| 一 | pkg/replication 全量 / pkg/sql 全量 | 编解码/复制协议/上层回归 | ✅ 通过（TestM6DownNodeReadFailover 基线失败排期外不修改） |
| 二 | （草案）补偿帧回灌：M_old 段 → M_new apply+追加 binlog、新写为准冲突、断线幂等 | 待立项后实现 |
| 二 | （草案）重置基线：M_old 从 LSN=1 全新追平后数据一致 | 待立项后实现 |
| 二 | （草案）binlog 损坏兜底：region push 全量对齐 | 待立项后实现 |

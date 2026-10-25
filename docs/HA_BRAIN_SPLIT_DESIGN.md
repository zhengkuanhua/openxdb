---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_34b40438ce9b11f1ab89525400393706
    ReservedCode1: CstTcMLx8oZ6Le1q3Y7wwkRorzEMkASx2rKHa6fihS2f32HEW5whCGXmzd2drQLfpPV6ZLe8I0bwCFUU9PNqfdf63kS9pn0Q/0MMvEGkpaaQ6kxx/m+QMECz8QOEO92wnfAsFRZT7JFmg0snE0Jj+4Ltbgx7aSIVTKjTmduMXQ2DrCC8sbU+orHGWZ4=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_34b40438ce9b11f1ab89525400393706
    ReservedCode2: CstTcMLx8oZ6Le1q3Y7wwkRorzEMkASx2rKHa6fihS2f32HEW5whCGXmzd2drQLfpPV6ZLe8I0bwCFUU9PNqfdf63kS9pn0Q/0MMvEGkpaaQ6kxx/m+QMECz8QOEO92wnfAsFRZT7JFmg0snE0Jj+4Ltbgx7aSIVTKjTmduMXQ2DrCC8sbU+orHGWZ4=
---

# OpenXDB 高可用脑裂防护设计（已落地）

- 状态：已落地（Implemented，v1.10.0）
- 版本：v1.0
- 日期：2026-10-25（初稿 2026-10-23）
- 范围：基于现有 HA 代码（集群分片轨 + 主从复制轨）设计并实现脑裂防护机制（epoch / fencing / quorumGuard / ReconcileAfterPartition / 回迁）；本文档描述设计语义与 v1.10.0 落地状态。

---

## 1. 问题分析

### 1.1 现有 HA 模型回顾

OpenXDB 现有 HA 由两条相对独立的链路构成，均**不包含 quorum / epoch / fencing**：

| 链路 | 关键代码 | 现有防护 | 缺口 |
|---|---|---|---|
| 集群分片轨 | `pkg/cluster/heartbeat.go`、`pkg/cluster/cluster.go`：全连接心跳（默认 interval 2s / timeout 6s，见 `pkg/db/db.go` L275 调用 `StartHeartbeat(2*time.Second, 6*time.Second)`）；DOWN 后经 `SetOnDown` 回调 `pkg/sql/ha.go FailoverDownNode` 重指派 region | `EverUp` 防启动竞态后才允许 `MarkDown`；`failoverDone` 幂等防单节点重复重指派 | Down 判定是**单节点本地结论**；重指派无全局纪元，无目标持有校验 |
| 主从复制轨 | `pkg/replication`（async / semi-sync / sync，T33）；`pkg/db/ha.go PromoteToMaster / EnableAutoFailover / failoverLoop`（200ms tick，Follower 读主超时即自升主） | sync/semi-sync 只保证 RPO（主单方面等待从确认） | 任意 follower 可自行升主，**无多数派确认、无 fencing**，多 follower 可同时成为双主 |
| TSO | `pkg/cluster/tso.go` + `db.SetTSONode`，基于 `pkg/tso.Source{ Get() (uint64, error) }` 单调发号（批量取号），`--tso-node` 指定 | 单调性（`TestTSOMonotonicGet` 已验证） | 中心化**单点**，无备份/切换；可作为 epoch 源但自身无 HA |

核心特征：节点间通过注册表 + 心跳维护视图，视图一致性完全依赖网络假设；任何节点对"对端 DOWN"的判断都是本地超时结论，缺少可全局仲裁的单调纪元来区分"新旧"。

### 1.2 脑裂场景

**场景 A：多 follower 同时升主（复制轨）**

网络分区将主库与全部/部分从库断开，但从库之间仍互通（或两个 follower 各自观测主超时）。每个 follower 的 `failoverLoop` 独立检测到主读超时后调用 `PromoteToMaster`：

- 现有代码对升主**无任何竞争条件防护**，两个 follower 均自封 master 并开始接受写；
- 各自 binlog 分叉，客户端重连任意"新主"即产生双主写分裂；
- 无 fencing，旧主若在分区另一端存活并恢复连接，仍可继续写。

**场景 B：网络分区双端各自重指派 region（集群轨）**

分区两端各自包含部分存活节点，每端对另一端节点的心跳均超时（6s）后判定 DOWN：

- 两端各自触发 `FailoverDownNode → failoverRegion`，把同一 region 重指派到各自端内的存活副本；
- `resolveRegionOwner` 在两端读到不同的 failover 路由视图，同一 region 出现两个 owner；
- 写请求按路由发往不同 owner，数据分叉且互相不可见；`failoverDone` 幂等只保护**单个节点内**不重复执行，无法阻止**双端各自**执行。

**场景 C：分区恢复合并**

分区愈合后，双主/双 owner 同时存在：

- 两个节点都自认 master（复制轨）或同一 region 都自认 owner（集群轨）；
- 现有代码无冲突检测与仲裁协议，无版本/纪元信息可判断"哪份数据是新的"；
- 表现为路由震荡、日志刷屏、写错乱，需人工介入且难以安全收敛。

### 1.3 根因归纳

1. **无全局单调 epoch**：无法区分新旧 master、新旧路由、新旧拓扑；陈旧节点无从被识别和拒绝。
2. **无 quorum 仲裁**：DOWN 判定为单节点本地结论（`EverUp` 只防启动竞态，不防分区两难）。
3. **无 fencing**：被取代的节点不会被"踢出"，仍可继续写 / 推送路由。
4. **region 重指派无持有校验**：`regionHoldsLocal`（`pkg/sql/multinode.go` L517，判断 region 是否本节点物理持有）已存在，但未用于重指派准入，候选可能并不真正持有键空间。

---

## 2. 设计目标与范围

### 2.1 设计目标

| 编号 | 目标 | 度量 |
|---|---|---|
| G1 | 防双主：任意时刻集群内至多一个接受写的 master（复制轨） | 并发升主请求至多一个获得授权 |
| G2 | 防路由分裂：每个 region 至多一个 owner（集群轨） | 分区双端重指派至多一端成功 |
| G3 | 防陈旧写入：被取代节点通过 fencing 拒绝后续写 / 重指派 | 旧 epoch 操作被拒且记录日志 |
| G4 | 可恢复：分区愈合后可安全收敛，不依赖人工 | 恢复后路由与主从状态一致 |

### 2.2 范围界定

**做（本草案覆盖）**：

- 单调 epoch 机制（clusterEpoch + regionEpoch，来源与持久化）；
- leader 仲裁：follower 升主前多数派确认旧主不可达；
- region 重指派增加 epoch 校验 + `regionHoldsLocal` 持有校验；
- TSO 作为 epoch 发号源的可行性评估；
- 兼容开关与滚动升级路径；
- P0–P2 分阶段实现计划与测试矩阵。

**不做（后续另行规划，本草案不覆盖）**：

- region 多副本 / 存储层共识（Raft 等）——保留现有复制机制；
- TSO 高可用（备份、自动切换）——仅复用其单调性作为 epoch 源；
- 跨机房多活 / 异地容灾；
- 分区期间完整可用性降级策略（本草案给出原则，细节后续细化）。

---

## 3. 方案设计

### 3.1 单调 epoch 机制

**定义两个纪元**：

- `clusterEpoch`：全局拓扑纪元。任何 master 变更、region 归属变更、成员增删均 +1，由 epoch 服务统一签发；
- `regionEpoch`：per-region 归属纪元。每次该 region owner 变更 +1，随 region 路由一并广播。

**来源**：优先复用现有 TSO 单调发号能力（`pkg/tso.Source.Get()`），扩展 `pkg/cluster/tso.go` 提供 `AcquireEpoch()` 授权接口：

- 节点切换 master / 重指派 region 前，先向 epoch 服务申请"新纪元"并持久化；
- 当 `--tso-node` 不可用时，降级由**注册表 master 节点**兜底发号（需保证注册表主节点唯一，见 3.4）。

**携带与校验**：所有跨节点控制消息（路由推送、PushRegion、Promote 广播）携带 epoch：

- 接收方拒绝 `epoch < 本地已知` 的陈旧消息；
- 写路径在 master 侧校验自身仍持有当前 clusterEpoch 的 master 授权（fencing token），不持有则拒绝写入。

**持久化**：epoch 随注册表 / 路由表持久化，节点重启从持久化值恢复，杜绝重启回退。

### 3.2 leader 仲裁与 quorum 判定

**复制轨升主流程（改造 `PromoteToMaster` / `failoverLoop`）**：

1. follower 检测主读超时（保留现有触发条件）；
2. 进入仲裁：向多数派节点发起"旧主不可达"确认。多数派 = `N/2+1`（N 为配置节点数）；`N < 3` 时默认仅允许人工升主（可通过开关显式放行）；
3. 获得多数派确认 **且** `AcquireEpoch()` 成功 → 升主并广播带新 epoch 的消息；
4. 旧主若在分区另一端存活，将收到更大 epoch 的 fencing 消息，其后续写入与路由推送被拒。

**集群轨重指派**：`FailoverDownNode` 执行前同样需多数派确认（或等 clusterEpoch 授权成功）；重指派结果写入注册表时携带 regionEpoch。

**quorum 不足时**：不执行自动升主 / 重指派，进入"降级只读"状态（保留自动重试，但不再自封 master / owner）。

### 3.3 region 重指派 epoch + 持有校验

在 `failoverRegion` 选择副本后、真正推送路由前，强制两步校验：

1. **持有校验**：调用 `regionHoldsLocal(rg)`（`pkg/sql/multinode.go` L517 现有实现，验证 region 物理键空间是否由本节点持有）验证目标副本真实持有该 region 数据（行数 / 元数据一致）；不通过则跳过该候选，尝试下一候选；
2. **epoch 校验**：重指派消息携带 `clusterEpoch + regionEpoch`，接收方校验通过才更新路由。

配套约束：

- `resolveRegionOwner` 读取 failover 路由时校验 epoch 新鲜度，陈旧 failover 条目视为无效；
- 分区双端各自重指派时，两个候选均向 epoch 服务申请，**只有一个**获得递增后的 regionEpoch；另一端推送因 epoch 落后被拒，路由不分裂。

### 3.4 TSO 结合可行性

- **可行**：TSO 单调性已验证（`pkg/tso` `TestTSOMonotonicGet`），`Get()` 为本地单调递增发号器；扩展为 epoch 发号源复用现有批量取号连接，新增组件少。
- **风险**：TSO 为单点（`--tso-node`）。若 TSO 节点处于分区对端，epoch 服务不可用 → 冻结一切变更（保守策略）或降级由注册表 master 发号（推荐，需保证注册表主节点唯一且携带 epoch 校验）。
- **结论**：先以 TSO 为 epoch 源、注册表 master 兜底；TSO 自身 HA（备份 / 切换）列入后续规划，不阻塞本方案。

---

## 4. 兼容与迁移

### 4.1 兼容策略

- 新增配置开关：`ha.epochEnabled`、`ha.quorumEnabled`，**v1.10.0 落地结论：`ha.epochEnabled` 默认开启、`ha.quorumEnabled` 默认关闭**。评估与理由见 §5.5；关闭时行为与现有一致，零回归；
- 复制模式 async / semi-sync / sync 协议不变：quorum 确认叠加在现有升主路径之上，不影响正常复制协议与 RPO 语义；
- `EnableAutoFailover`：开启 quorum 后自动升主需多数派确认；单 follower / 两节点集群无多数派时，自动升主降级为"等待人工确认"，可通过 `ha.allowMinorityAutoPromote` 显式放行（保持向后兼容）；
- `FailoverDownNode / failoverRegion`：重指派增加 epoch + 持有校验；单节点 down 的现有场景（TestM6*/TestM7* 等）在 epoch 授权成功后行为不变；
- 写路径 fencing：仅在 epoch 开启后生效，关闭开关时跳过，不影响旧版本客户端。

### 4.2 滚动升级

- 旧节点不识别 epoch 字段 → **版本协商**：混合集群中对旧节点消息跳过 epoch 校验（防呆优先：宁可保守拒绝新消息，不破坏旧节点）；
- 升级顺序建议：先升级 epoch 服务（TSO / 注册表），再升级数据节点，最后开启开关；
- 迁移步骤：灰度开启 → 观察仲裁与 epoch 拒绝日志 → 全量开启 → 关闭无 epoch 的降级路径。

### 4.3 对现有测试的影响

- v1.10.0 实测：`ha.epochEnabled` 默认开启后，`pkg/db`、`pkg/replication`、`pkg/sql` 三包全量回归全绿（详见 §5.5 回归记录）；既有基线（`TestM6DownNodeReadFailover`、`TestM6AutoReassignRegionAfterDown`、`TestDBSyncRepl*` 等）无破坏；
- `TestM6AutoReassignRegionAfterDown` 按 P1 持有校验（`regionHoldsLocalTarget`，ASSIGN 禁止重复指派）适配：候选节点需先真实持有 region 副本再重指派，用例已同步更新；
- 新增用例（脑裂演练、backup_schedule 单测）仅针对开启开关的路径（见第 6 节测试矩阵）。

---

## 5. 实现计划

### P0 — 最小防双主（复制轨）

- **文件**：`pkg/db/ha.go`（`PromoteToMaster` / `EnableAutoFailover` / `failoverLoop`）、`pkg/replication`（master / follower 状态机）、`pkg/cluster/tso.go`（epoch 授权接口）、`pkg/cluster/cluster.go`（多数派确认辅助）
- **关键函数**：`PromoteToMaster`、`failoverLoop`、`AcquireEpoch`、`quorumProbe`
- **产出**：多 follower 并发升主至多一个成功；fencing 后旧主写入被拒
- **预估**：5–8 人日

### P1 — 防路由分裂（集群轨）

- **文件**：`pkg/sql/ha.go`（`FailoverDownNode` / `failoverRegion` / `resolveRegionOwner`）、`pkg/sql/multinode.go`（`regionHoldsLocal` 接入重指派准入）、`pkg/cluster/cluster.go`（路由广播携带 epoch）
- **关键函数**：`failoverRegion`、`resolveRegionOwner`、`regionHoldsLocal`
- **产出**：分区双端重指派至多一端成功；陈旧 failover 条目失效
- **预估**：5–8 人日

### P2 — 分区恢复与测试矩阵

- **文件**：`pkg/cluster`（分区检测 / 视图 version）、`pkg/db/ha.go`（恢复收敛）、新增 `*_test.go`
- **关键产出**：恢复合并协议（epoch 大者胜 + 数据冲突标记）、全矩阵自动化测试
- **预估**：4–6 人日

**合计：约 14–22 人日。**

### 5.5 落地状态（v1.10.0）

**已实现语义清单**（v1.10.0 前代码主体已入库，本版完成开关评估、演练与文档对齐）：

| 语义 | 实现位置 | 关键行为 | 覆盖测试 |
|---|---|---|---|
| clusterEpoch 单调发号 | `pkg/db/epoch.go`（clusterEpoch / acquireEpoch / NoteEpoch） | master 变更、region 归属变更单调递增；重启从持久化恢复 | `epoch_test.go` |
| epoch fencing（写路径） | `pkg/db/epoch.go` AllowWrite / StaleMasterReady | 陈旧 master 写入被拒；binlog 非空 + 已降级只读才允许回迁 | `db_ha_repl_test.go`、`brain_split_drill_test.go` |
| quorumGuard 升主多数派仲裁 | `pkg/db/epoch.go` quorumGuard / QuorumOK | 升主前向多数派确认旧主不可达；不足多数派拒绝 | `epoch_test.go`、`db_ha_test.go` |
| 分区视图 / MajorityOK | `pkg/cluster/partition.go` | 注入 prober 模拟网络分区视图；MajorityOK 翻转驱动仲裁 | `partition_test.go`、`brain_split_drill_test.go` |
| ReconcileAfterPartition 回迁闭环 | `pkg/db/ha.go` | 分区恢复后旧主经 NoteEpoch 降级只读、fencing 旧写、StaleMasterReady 回迁就绪 | `db_rejoin_test.go`、`brain_split_drill_test.go` |
| 组网级脑裂演练 | `pkg/db/brain_split_drill_test.go` | 三节点：A 主、B/C follower；注入分区触发 MajorityOK 翻转 → B 升主 → A 降级只读 + fencing → 回迁就绪 | `TestP1BrainSplitDrill` |
| 备份调度测试补齐 | `pkg/sql/backup_schedule_test.go` | 调度触发 / 跳过 / 失败恢复 / 保留轮转四场景 | `TestBackupSchedule*` |

**开关默认开启评估结论**（P0）：

- 评估对象：`ha.epochEnabled`（epoch 发号 + fencing 写路径）、`ha.quorumEnabled`（升主多数派仲裁）；
- 结论：**`ha.epochEnabled` 可直接默认开启**——三包全量回归（pkg/db 37.0s / pkg/replication 11.9s / pkg/sql 51.9s）全绿，既有基线（TestM6DownNodeReadFailover、TestM6AutoReassignRegionAfterDown、TestDBSyncRepl* 等）无破坏；`TestM6AutoReassignRegionAfterDown` 按 P1 持有校验适配后 PASS。`ha.quorumEnabled` 保持默认关闭：quorum 仲裁改变升主竞争语义（单/两节点无多数派时自动升主行为变化），需在组网环境灰度后再全量开启；
- 落地动作：`pkg/db/db.go` DB.Open 默认 `epochEnabled = true`（quorum 仍默认 false）；`ha_config.go` 配置项保持可覆盖；
- 建议：v1.11.0 在真实组网灰度 quorum 开关后评估全量开启。
- **v1.11.0 全量开启评估结论**（单机/测试框架）：对 `quorumGuard`（pkg/db/epoch.go）做代码级影响面核对——开启后 ① `Cluster == nil`（单机未组网）直接返回 ErrQuorumNotMet；② 注册节点数 n<2 拒绝升主（`cluster size N, need >= 2`）；③ 两节点主从场景 quorum=N/2+1=2，从节点升主时旧主已探活失败，`up=1<2` 拒绝升主。**结论：全量开启将破坏单节点自动升主与两节点主从自动升主语义，保持默认关闭**；三节点及以上真实组网灰度后再评估开启（结论同步至 KNOWN_LIMITATIONS §3.4）。

**演练覆盖矩阵**（组网级脑裂演练 `TestP1BrainSplitDrill` 对第 6 节 T 编号的覆盖映射）：

| T 编号 | 场景 | 覆盖方式 |
|---|---|---|
| T1 | 主从断开、从端升主 | 注入 A 分区视图使 B 端 MajorityOK 翻转 → B 升主（演步骤 2） |
| T2 | 并发升主竞争 | epoch 冲突裁决：B 升主后 C 重复尝试被 quorum/fencing 拒绝（演步骤 3） |
| T3 | 分区恢复合并（复制轨） | ReconcileAfterPartition 使 A 降级只读 + fencing 旧写（演步骤 4） |
| T8 | 开关关闭回归 | epoch_test.go TestP0EpochDefaultOnZeroRegression / ha_p2_test.go 显式关闭开关验证零回归 |
| T9 | 单点 down 正常 failover | db_ha_test.go / db_ha_repl_test.go 既有基线覆盖 |

**回归记录（v1.10.0）**：`go build ./...` 通过；`pkg/db`、`pkg/replication`、`pkg/sql` 全量 `go test` 全绿（含 backup_schedule 新增 4 用例，sql 包 46.3s）。

---

## 6. 测试矩阵

| 编号 | 场景 | 步骤 | 期望结果 |
|---|---|---|---|
| T1 | 网络分区（主从断开，从端升主） | 3 节点，切断主与两从连接；从端触发升主 | 从端多数派确认失败 → 保持只读，不升主 |
| T2 | 并发双 down（两 follower 同时检测主超时） | 3 节点，两 follower 同时观测主超时并发 `PromoteToMaster` | 至多一个获得 epoch 授权升主，另一个被拒 |
| T3 | 分区恢复合并（复制轨） | 双主形成后恢复网络 | epoch 大者胜；陈旧侧被 fencing 停止写入并转为从 |
| T4 | 分区恢复合并（集群轨） | 双 owner 形成后恢复网络 | regionEpoch 大者胜，路由收敛为单一 owner |
| T5 | 多副本仲裁 | 5 节点集群，存活 3 个（多数派）可升主 / 重指派；存活 2 个不可 | 符合 quorum 判定 |
| T6 | epoch 拒绝陈旧消息 | 手工构造旧 epoch 的 Promote / PushRegion 消息 | 被接收方拒绝并记录日志 |
| T7 | 重指派持有校验 | 候选副本不持有目标 region 键空间 | 跳过该候选，选择下一候选或放弃 |
| T8 | 开关关闭回归 | 关闭 `ha.epochEnabled / ha.quorumEnabled` 运行全部现有 HA 用例 | TestM6*/TestM7*/TestHeartbeatDown 全绿，行为与现状一致 |
| T9 | 单点 down 正常 failover（兼容性） | 单节点 down，其余节点多数派存活 | 升主 / 重指派流程在 epoch 校验下正常完成 |

---

## 7. 附录：代码位置索引

| 组件 | 位置 | 说明 |
|---|---|---|
| 心跳 | `pkg/cluster/heartbeat.go` | tick / 超时判 DOWN，`EverUp` 防护 |
| 集群管理 | `pkg/cluster/cluster.go` | `StartHeartbeat`（L401，默认 2s/6s）、`SetOnDown`、`notifyDown`、`MarkDown` |
| 分片 failover | `pkg/sql/ha.go` | `FailoverDownNode`、`failoverRegion`、`resolveRegionOwner`、`failoverDone` |
| region 持有判断 | `pkg/sql/multinode.go` L517 | `regionHoldsLocal(rg)` |
| 复制轨 | `pkg/replication` | async / semi-sync / sync，T33 |
| 复制 HA | `pkg/db/ha.go` | `PromoteToMaster`、`EnableAutoFailover`、`failoverLoop` |
| 装配 | `pkg/db/db.go` | L273-275 `SetOnDown` 回调 + `StartHeartbeat`；`SetTSONode` |
| TSO | `pkg/cluster/tso.go`、`pkg/tso/tso.go` | 批量取号客户端；`Source.Get()` 单调发号 |
*（内容由AI生成，仅供参考）*

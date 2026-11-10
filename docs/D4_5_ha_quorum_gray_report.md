---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_7cf54fe5d8b311f19232525400248c00
    ReservedCode1: TwBr3wgjt5ThV00B/HEBYsJJmNaqx+6KxepYkMOC02/eWlJ9kYAyfVnKQV+BubHMd8QOqZtmo2sWeEcHVwb7uOO352gd/gqSAPWx4yY4J+9wru1zcG7MCSVyUR4OkzLazcJdPbapvgZJXi/26yT1uWnhl89lwn0GSFkn5o3xgEUlvY14DlG1GwIboXU=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_7cf54fe5d8b311f19232525400248c00
    ReservedCode2: TwBr3wgjt5ThV00B/HEBYsJJmNaqx+6KxepYkMOC02/eWlJ9kYAyfVnKQV+BubHMd8QOqZtmo2sWeEcHVwb7uOO352gd/gqSAPWx4yY4J+9wru1zcG7MCSVyUR4OkzLazcJdPbapvgZJXi/26yT1uWnhl89lwn0GSFkn5o3xgEUlvY14DlG1GwIboXU=
---

# D4.5 HA quorum 灰度报告（正式版 v1.0）

- 日期：2026-11-10（首次发布 2026-11-05）
- 版本：v1.18.0 Phase B（D4）收口版
- 依据：DEV_PLAN_v1.18.0 D4.5（全链路演练结论 + HA_BRAIN_SPLIT_DESIGN §5.5 更新）
- 演练基础：D4.1（quorum 开启评估）、D4.2（宕机 failover）、D4.3（脑裂 / epoch 拒绝）、D4.4（节点恢复回迁）、D4.5 Phase5-Phase9（TSO HA 接线 / 双主分叉 / 启动崩溃 / 集群轨归属纪元）
- **文档状态：正式版 v1.0（收口）**。收口依据：D4.4 三项 GAP 已在 `af727fc` 二进制 VM 三节点复测全 PASS（§5.8）；其后 Phase5/6/8/9 四轮修复分别闭合 TSO HA 生产接线、NEW-1 双主分叉、NEW-2 启动期空句柄崩溃、T3 集群轨 fencing 的**代码级根因**，全仓 `go test ./... -count=1` 全绿（22 包），本报告升版当日复跑 Phase8/9 相关用例均 ok（`pkg/db` 21.7s、`pkg/sql` 4.6s、`pkg/cluster` 0.5s）。
- **本版仍保留的三项验证债（不影响收口定级，但禁止据此开启 quorum 全量）**：T3 真分区未复验（Phase9 仅进程内 e2e）、NEW-1 陈旧主写拒绝未做真实三节点复验、NEW-2 未做进程级 2×3 重启矩阵复跑。三项复验方案见 §8。
- 复测原始日志：`d44_rerun_v1180_clean.txt`（会话中间产物目录，未入库；本文第 5 节数据全部取自该日志，未做推断补齐）
- **v0.3 变更记录（2026-11-05）**：承接 §5.7 根因判定，两轮修复入库（`529e34c` 路由视图强制从权威源刷新 + 写路径 region 归属强制；`af727fc` 权威同步后清理被现役 region 取代的本地历史 region），VM 三节点复测（二进制 `af727fc`）显示 D4.4 三项 GAP **全部 PASS**（详见 §5.8）。v0.2 各节内容按原样保留以留痕：其"路由回迁部分改善、聚合读未闭合"判定对应修复前状态，结论已被 §5.8 取代。该版当时维持草案 v0.3——GAP 全闭合不等于生产可用，§6 三项硬缺口未因该轮修复改变（**此结论已被本版 §5.9 / §6 取代**）。
- **v1.0 变更记录（2026-11-10）**：
  1. 补入 Phase5-Phase9 四轮修复与复测（新增 §5.9）：`a4b33bf` TSO HA 生产接线与 `ha_*` 持久化加载顺序、`0303c13` NEW-1 重启双主分叉丢数据、`3d8373d` NEW-2 v2 启动期空句柄崩溃（B1 存储就绪栅栏）、`59b1c05` NEW-1 与 T3 同源收口（集群轨归属纪元推进并持久化）；另含 Phase7 真机复测（`0303c13` 二进制）之 NEW-2 阻塞级崩溃与 B2 TSO HA PASS 记录。
  2. 更新 §3 结论矩阵：新增 NEW-1、NEW-2 两行，改写 B1、B2、T3、写路径四行判定，另将"路由自动回迁""聚合读恢复"两行由 v0.2 的 ⚠️/❌ 按 `af727fc` 复测转为 ✅；§1 汇总计数同步重算（13 项：通过 8 / 代码已修待真机复验 4 / 部分落地 1，口径为"通过=有 VM 三节点真机证据"）。
  3. 修正 §6 第 1 项过时表述：B1 单副本"不可恢复"已不成立——`396b4bb` 起实现主动副本补齐（`EnsureRegionReplicas`），故障点即时补齐、无存活副本时记 pending 重试而不再永久关闭；风险性质由"数据不可恢复"降级为"依赖异步补齐 + 无一致性协议 + 生产默认关闭"。
  4. §6 第 2 项 TSO 单点：Phase5 已把 TSO HA 接入生产启动路径（`a4b33bf`），Phase7 真机复测 B2 PASS，降级为"已闭环，保留运维约束"。
  5. §6 第 3 项 T3：Phase9 已落地集群轨归属纪元（cluster term）落盘与写门槛三态，fencing 由"未实现"改为"代码已实现、真实分区待复验"。
  6. §7 受限灰度前提按 Phase9 行为收紧（重启节点在权威视图确认前拒写 → 存在秒级写不可用窗口），并给出 `ha_quorum` 默认关闭的**最终结论**（§1、§7.1）。
  7. §8 重构为「已闭环 / 未闭环 / 收口判定」三段，明确三项真机验证欠账在 v1.18.0 发版前不阻塞、v1.19.0 前必须完成；§9 补入 Phase7 复测报告与 v1.0 修复链提交、用例索引。
  8. 澄清一处易误读点（§7.1-3）：重启待确认拒写门槛由 `ha_epoch_enabled`（默认 on）驱动，**与 `ha_quorum_enabled` 无关**，但仅三种情形触发，单机与未接管 region 的节点零回归。

## 1. 结论摘要

| 判定项 | 结论 |
|---|---|
| `ha.quorumEnabled` 默认值 | **维持 off**（`ha.epochEnabled` 维持默认 on） |
| 是否全量开启 | **否** |
| 灰度形态 | **受限灰度（Restricted Gray）**：仅三节点及以上、每 region ≥2 存活副本、TSO 不与数据节点同机的验证环境可开启；v1.0 版前提收紧：还须接受"重启节点在权威视图确认前拒绝写入"带来的秒级写不可用窗口（§7） |
| 全链路验收计数 | 共 13 项：通过 8 项（含 B2 TSO HA、路由自动回迁、数据追平、聚合读恢复、quorum 仲裁门等真机证据项）/ 代码已修待真机复验 4 项（B1、T3、NEW-1、NEW-2）/ 部分落地 1 项（写路径全局视图）。口径：**"通过"= 有 VM 三节点真机证据**，单测不计入 |
| 生产可用性定性 | 集群轨为"**单副本 + 中心 TSO + 无一致性协议**"结构。Phase5-Phase9 已清除 TSO 单点（B2）、把单副本补齐改为自动（B1）、为集群轨补上归属纪元 fencing（T3），但**副本间仍无持续复制 / 无共识提交**，故 RPO>0 与"单副本可写窗口"结构性存在，不具备生产级 HA 判据 |
| **最终判定（v1.0）** | **`ha.quorumEnabled` 维持默认关闭（off）**；`ha.epochEnabled` 维持默认 on。v1.18.0 不给出全量开启建议，默认值在 v1.19.0 M8（2PC / 持续复制）落地前不再调整（§7.1） |

一句话结论：**quorum 开关本体行为正确；D4.4 三项 GAP 已于 `af727fc` 复测全 PASS，Phase5-Phase9 又把 B2 单点、B1 单副本、T3 fencing 从"未实现"推进到"代码已修"，v1.18.0 的 HA 骨架已成形；但三项关键行为（T3 真分区、NEW-1 陈旧主写拒绝、NEW-2 进程级重启矩阵）仅有单测/进程内证据，且副本间无持续复制与共识提交，故 quorum 维持默认关闭、按受限灰度处理，本报告升为正式版 v1.0 的依据是"修复链收口 + 证据分级"，不是"生产可用放行"。**

> **v0.2 判定留痕（保留作基线）**：含 P3 修复的 VM 三节点首轮复测显示数据追平闭合，但路由回迁仅部分改善、聚合读在重启后写入即发散，并定位出"重启节点 region 归属视图未从集群权威源刷新"的新根因；当时判定 v1.18.0 不具备全量开启条件。
>
> **v0.3 结论更新（2026-11-05）**：两轮修复（`529e34c` 路由视图强制从权威源刷新 + 写路径归属强制、`af727fc` 权威同步后清理被取代历史 region）后，以 `af727fc` 二进制复测：**D4.4 三项 GAP 全部 PASS**——路由回迁 PASS（三端 region 集合 {11,12,10} 一致、region 12 自动回迁 B、重启节点无残留历史 region）、数据追平 PASS（S3 三端 12/141，S4 后 13/172）、聚合读 PASS（B 写 `id=31` 后三端同刻 13/172）。证据见 **§5.8**。该版仍维持草案，因 §6 三项硬缺口当时未改善。
>
> **v1.0 结论更新（2026-11-10）**：§6 三项硬缺口中，B2 已闭环（Phase5 接线 + Phase7 真机 0.10s/0.09s 切换）、B1 已改为自动副本补齐、T3 已落地 cluster term fencing；残余一项结构性缺口为"副本间无持续复制 / 无共识提交"（§6-4）。因此 quorum 默认关闭的理由从"三项未实现"转为"结构性缺口 + 三项真机复验未做"，最终结论见 §7.1。

## 2. 演练汇总

| 编号 | 演练内容 | 环境 | 结果 | 产物 |
|---|---|---|---|---|
| D4.1 | quorum=on 三端，S1-S5 场景矩阵 | Windows 本机三进程（17881-17883） | PASS 16 / FAIL 2（预期）/ GAP 2 | `tests/bench/bench_data/d41/run_d41_quorum_enable_eval.ps1`、`run_d41_output.txt` |
| D4.2 | 杀归属节点进程 → 读 failover | 本机三进程 | PASS 33 / FAIL 2（预期）/ GAP 1 | `tests/bench/bench_data/d42/run_d42_failover_drill.ps1`、`run_d42_output.txt` |
| D4.3 | 双向网络隔离 → epoch 拒绝 / 分区恢复 | VM 192.168.100.200 三节点 | T6/T8/T9 PASS；T1/T2 不适用（集群轨无升主路径）；T3 GAP（Phase9 前） | `tests/bench/bench_data/d43/run_d43_split_brain_epoch.py`、`run_d43_output.txt` |
| D4.4 | SIGKILL / SIGTERM 重启 → 回迁 / 追平 / 路由收敛 / 聚合读 | VM 三节点 | 宕机检测 PASS、failover 重指派 PASS；首轮三项 GAP；**v0.3 复测（`af727fc`）：三项 GAP 全 PASS** | `tests/bench/bench_data/d44/run_d44_node_recovery.py`、`run_d44_output.txt`、`d44_rerun_v1180_clean.txt` |
| Phase7 | D4.5 真机矩阵复测（`0303c13` 二进制）：quorum 持久化重启、双主分叉、TSO HA、分区 | VM 三节点（单机多 IP 别名） | NEW-2 **阻塞级崩溃**（0xc0000005）；NEW-1 部分闭环；B2 TSO HA PASS；T3 NOT VERIFIED | `docs/D4_5_phase7_retest.md` |
| Phase8/Phase9 | 代码级修复 + 单测 / 进程内 e2e 回归 | Go 单测（`pkg/db`、`pkg/sql`、`pkg/cluster`） | Phase8：NEW-2 启动崩溃闭合（B1 存储就绪栅栏，2×3 矩阵）；Phase9：NEW-1/T3 同源收口（cluster term），10 个用例 + 1 个 e2e 全 ok；**未经真机复验** | `pkg/db/d4_5_phase8_new2_startup_test.go`、`pkg/sql/d4_5_phase8_b1_gate_test.go`、`pkg/cluster/p9_internal_test.go`、`pkg/db/ha_p9.go`、`pkg/sql/ha_p9.go`、`pkg/sql/ha_p9_test.go` |

## 3. 结论矩阵

| 验收项 | 结论 | 证据 | 说明 |
|---|---|---|---|
| quorum 多数派仲裁门（2/3 通过、1/3 拒绝） | ✅ PASS | D4.1 S1-S5 | 1/3 场景按设计拒绝，未误放行 |
| 不破坏三节点自动升主语义 | ✅ PASS | D4.1 | 多数派存活时升主/重指派路径正常 |
| 宕机心跳检测（DOWN） | ✅ PASS | D4.2、D4.4 | interval 2s / timeout 6s，约 6s 生效 |
| 读 failover（存活副本前提） | ✅ PASS | D4.2 S3 | `resolveRegionOwner` → `FailoverDownNode` → 持有校验通过后重指派，聚合读完整 |
| 单副本 region 宕机可恢复（B1） | ⚠️ 代码已修，待真机复验（原 ❌ 设计边界） | `396b4bb`、`3d8373d`、Phase8 单测 | `ha_b1.go` 的 `EnsureRegionReplicas` + `b1EnsureAsync` 在归属节点 DOWN 时主动补齐副本，无存活副本时记 pending 持续重试（不再永久放弃）；Phase8 追加存储就绪栅栏。**但补齐为异步推送非日志复制**：窗口内仍单副本可写，且 `ha_replica_enabled` 生产默认 off |
| TSO 单点下的跨节点写（B2） | ✅ PASS（v1.0 复测闭合，原 ❌ 已知限制） | D4.1 B2、Phase5 `a4b33bf`、Phase7 真机 | Phase5 修复 `ha_*` 持久化配置加载顺序并把 TSO HA 接入**生产启动路径**；Phase7 真机主 TSO 停机 **0.10s** 切备、恢复 **0.09s** 回切，跨节点写未中断（D4.1/D4.2 时 `tso unavailable` 全失败）。保留运维约束：TSO 主备不得与唯一副本同机，切换窗口内写需重试 |
| T3 分区恢复合并 + 旧主 fencing（集群轨） | ⚠️ 代码已修，待真机复验（原 ❌ 未修） | D4.3、Phase9 `59b1c05`、`TestP9*`、`TestD45P9RestartOwnerConfirmedByPeersE2E` | Phase9 新增集群轨归属纪元 cluster term：grant/revoke 推进并落盘持久化，写门槛由"`masterEpoch>0` 才校验（集群轨恒 0 故恒放行）"改为三态（本地权威 / 需上行确认 / 拒绝），重启节点确认前拒写；路由同步消息携带 term，claim owner 推送 grant。**未在 VM 三独立 IP 上构造真实双向隔离**，故不判 PASS |
| 节点恢复后路由自动回迁 | ✅ PASS（v0.3 复测闭合，原 v0.2 ⚠️ 部分改善） | `529e34c`、`af727fc`、§5.8.3-5.8.4 | 复测三端 region 集合 {11,12,10} 完全一致；region 12 自动回迁 B；重启节点无残留历史 region（`af727fc` 清理被取代本地历史 region）；（原 (a) r3 未回迁、(b) B 与 A/C 视图不一致、(c) 需人工 `ASSIGN REGION` 三项现象均已消失） |
| 节点恢复后数据回灌 / 追平 | ✅ PASS（一次性对齐，RPO>0） | D4.4 S3、§5.8.3 | 重启 B 后三端读数一致 12/141；B 上可查到 down 期写 `id=30` 值 `pA30`。仍为 region 级一次性 `FullAlignViaRegionPush` 对齐，非持续复制 |
| 恢复后聚合读恢复 | ✅ PASS（v0.3 复测闭合，原 v0.2 ❌ 未闭合） | §5.8.3、§5.8.6 | 复测中 B 写入 `id=31` 后三端同刻可见 13/172（v0.2 时仅 B 读到 13/172、A/C 停留 12/141）；（S4 后旧现象为 A 12/141、B 13/172、C 12/141，已不复现） |
| 恢复节点写路径进入全局视图 | ⚠️ 部分落地 | `529e34c`（写路径按 region 归属强制路由）、§5.7 | 写路径已强制按归属路由，不再纯本地提交；但全局视图仍靠归属广播 + 一次性对齐维护，**无 2PC / 无持续复制**（§6-4），故不判 PASS |
| NEW-1：重启节点与新主并存时旧归属写被拒（双主可写） | ⚠️ 代码已修，待真机复验 | Phase6 `0303c13`、Phase7 真机、Phase9 `59b1c05`、`TestP9RestartPendingUntilAuthoritative` | Phase6 改 epoch 字典序比较 + 双写熔断：不再丢数，但 Phase7 真机仍观察**双主可写**；Phase9 以 cluster term 三态门槛收口（重启节点待确认期拒写、确认后恢复），已由单测 + 进程内 e2e 验证，真机未复跑 |
| NEW-2：持久化 `ha_quorum=on` 重启即崩（0xc0000005） | ⚠️ 代码已修，待真机复验 | Phase7 真机、Phase8 `3d8373d`、`TestPhase8New2StartupPersistedQuorumMatrix` | 根因：启动期 B1 副本补齐早于存储就绪，RocksDB 句柄为空传给 `xdb_snapshot_create`。修复：`b1StorageGate` 就绪门控 + deferred 补齐、句柄读写锁 `hmu` + Snapshot nil 校验。Phase8 单测 2×3 矩阵全绿，**进程级真机重启矩阵未复跑** |

> **矩阵版本口径（v1.0）**：本表 13 行结论全部按最新证据改写，历史判定以括号形式留痕。「✅ PASS」= 有 VM 三节点真机证据；「⚠️ 代码已修，待真机复验」= 代码级根因已闭合、仅有单测/进程内证据，**禁止**据此判 PASS；「⚠️ 部分落地」= 有真机证据但覆盖不完整。前两行历史 v0.2 ❌ 已转 ✅；写路径一行因缺 2PC / 持续复制仍为 ⚠️。

## 4. P3 源码修复清单（对照 D4.4 §4 修复建议）

| D4.4 建议 | P3 落地实现 | 提交 | 复测 |
|---|---|---|---|
| 1. 节点 join 时同步全量 region 路由、覆盖本地 meta，重建数据面通道 | `execAddNode` 先将新节点 `SetDataReady(false)` 并广播 → `pushAllRegionsToNode` 下发权威路由；`SetOnUp` + `HandleNodeUp` 在心跳 DOWN→UP 时重同步；`RegisterPeerData` 对"本地 DOWN、广播 UP"场景锁外 `notifyUp`（修复注册表广播抢占心跳检测导致回调丢失） | `6a9f06c`、`34235f1` | 部分生效：重启后 r4 自动回迁，但 r3 未回迁；B 自身视图 r2 归属仍误判（`local=true`） |
| 2. failover 接管记录来源，原主恢复后回迁 | `sharding.RegionInfo.SourceOwner` + `SetRegionNodeEpochSource`；本机接管与远端接管分支均记录；`HandleNodeUp` 对 `SourceOwner == nodeID` 的 region 调 `tryRejoinAsFollower`（含无数据场景 `routeOnlyRejoin` 纯路由归还） | `6a9f06c`、`34235f1` | 部分生效：r4 回迁成功；r3 未触发回迁（`SourceOwner` 未命中或未执行）；人工 `ASSIGN` 后 A/C 收敛 |
| 3. 数据追平（region 级全量对齐） | `FullAlignViaRegionPush` 复用复制轨通道，把 `SourceOwner` 区域从接管节点改挂到回迁节点；`syncRegionsToNode` 在对齐推送成功后标记 `DataReady=true` | `6a9f06c` | **通过**：重启后三端 12/141 一致，B 可见 down 期写 `id=30`=`pA30`；此后新增写跨节点不可见（属建议 4 范畴，非本轮追平判定） |
| 4. 写路径经 region 归属强制路由 / 2PC 参与 | **未落地**（仍本地提交） | — | 未启动（建议并入 v1.19.0 M8 2PC） |

聚合读 READY 门细节：`getKVCluster` / `scanMergedCluster` / `scanMergedClusterTop` 对 `DataReady=false` 的节点拒绝转发（`checkRemoteReady`），`db.StartCluster` 启动即声明自身未就绪，避免重启节点在数据面未重建时污染聚合视图。

## 5. D4.4 VM 三节点复测结果（v1.18.0 含 P3 修复）

- 复测时间：2026-11-05
- 被测二进制：v1.18.0 含 P3 修复（`6a9f06c`、`34235f1`）
- 环境：VM 192.168.100.200 三节点 —— A=`node-24b6a076`（13306）、B=`node-bdbff1cc`（13307）、C=`node-cab8c15a`（13308）
- 脚本：`tests/bench/bench_data/d44/run_d44_node_recovery.py`（本轮修复：`SPLIT REGION` 的 region id 由硬编码 2 改为动态获取、查询超时 20s→45s、`SHOW NODES` / `SHOW REGION ROUTES` 输出 tab 可读化）
- 数据来源：`d44_rerun_v1180_clean.txt`（49 行）。本节数值与该日志逐条对应，不补写日志中不存在的探针结果。

### 5.1 基线（重建后）

| 节点 | COUNT(*) | SUM(id) |
|---|---|---|
| A | 11 | 111 |
| B | 11 | 111 |
| C | 11 | 111 |

三端一致。对照 v1.17.0 基线（A=5 / B=3 / C=3，分裂），读聚合基线已生效；S1 段 A 的路由可见 r3、r4、r2 三个 region，说明脚本修复前的"重建基线只剩 1 个 region"问题随动态 region id 一并消除。

### 5.2 S1：SIGKILL 杀 B

- `SHOW NODES`（A 侧）：B 置 DOWN，A/C 保持 UP
- A 路由：r3、r4 `owner=node-24b6a076 local=true`（failover 重指派生效）；r2 `owner=node-cab8c15a local=false`
- down 期写 `INSERT ... (30,'pA30')`：回显 `(empty)`（无报错）
- A 读：12/141（基线 11 行 + down 期新写 1 行）

### 5.3 S3：重启 B（SIGKILL 后拉起）

| 观测项 | 结果 |
|---|---|
| A 路由 | r3 `owner=node-24b6a076 local=true`（**未回迁**）；r4 `owner=node-bdbff1cc local=false`（**已回迁**）；r2 `owner=node-cab8c15a` |
| B 路由 | r3 `owner=node-24b6a076 local=false`；r4 `owner=node-bdbff1cc local=true`；r2 `owner=node-bdbff1cc local=true`（与 A/C 的 `node-cab8c15a` **不一致**） |
| 三端读 | A/B/C 均 12/141（数据追平） |
| B 查 `id=30` | `30 / pA30`（down 期写已可见） |
| B 写 `id=31` 后 | B 13/172；A/C 仍 12/141 |

### 5.4 S3+：人工回迁对照（`ASSIGN REGION 3` → B）

- A/C 路由一致：r3、r4 `owner=node-bdbff1cc`；r2 `owner=node-cab8c15a`
- 三端读：A/B/C 均 12/141
- 采集缺口：该段脚本仅打印 A/C 路由，未采集 B 自身视图，B 侧一致性在本段无法判定（S4 段补到 B 视图，见 5.5）

### 5.5 S4：SIGTERM 优雅退出重启对照

- A 路由：r3、r4 `owner=node-bdbff1cc`；r2 `owner=node-cab8c15a`
- B 路由：r2 `owner=node-bdbff1cc local=true`（应为 `node-cab8c15a`）；r3、r4 `owner=node-bdbff1cc local=true`
- 读：A 12/141、B 13/172、C 12/141

### 5.6 三项 GAP 判定

| GAP | 判定 | 依据 |
|---|---|---|
| B 数据追平 | ✅ PASS，闭合 | 见 5.3：三端 12/141 一致，B 可读 down 期写 `id=30` |
| A 路由回迁 | ⚠️ 部分改善，未闭合 | failover 重指派生效（5.2）、r4 自动回迁（5.3）；但 r3 未自动回迁、重启节点 B 路由视图与 A/C 不一致（r2 归属错）、人工 `ASSIGN` 只修正 A/C 侧（5.4、5.5） |
| C 聚合读恢复 | ❌ 未闭合 | 三端一致仅维持到重启瞬间：B 写 `id=31` 后仅 B 读到 13/172，A/C 仍 12/141（5.3）；S4 重启后仍为 A 12/141 / B 13/172 / C 12/141（5.5） |

判定口径说明：本轮 clean 日志未包含"额外跨端写探针（id=40/41）及其跨端查询"段落输出，故 GAP C 仅以日志实有的 `id=31` 写入与 S3/S4 三端读数差为证据；该差异已足以证明重启后写路径不进入全局视图，判定不因此改变。探针的独立采集结果见 5.6.2（探针在 S4 之后另行执行并单独记录，未并入上述 clean 日志）。

> 版本衔接：上表为 **v0.2（修复前二进制）** 的判定。两轮修复（`529e34c`、`af727fc`）后的复测判定见 **§5.8**，三项 GAP 已全部转 PASS；本节原判定保留不改，作为修复前基线对照。

### 5.6.2 双向写可见性定向探针

在 S4 之后、未重建基线的状态下，追加一组双向写可见性定向探针：

| 探针 | 操作 | A | B | C |
|---|---|---|---|---|
| P0 | 探针前读数 | 12/141 | 13/172 | 12/141 |
| P1 | A 执行 `INSERT`（`id=40`）后读数 | 13/181 | 13/172 | 13/181 |
| P2 | C 执行 `INSERT`（`id=41`）后按 `id IN (30,31,40,41)` 查询 | 命中 30/40/41 | 命中 30/31 | 命中 30/40/41 |

同一时刻三端路由视图：

| 节点 | r2 owner | r3 owner | r4 owner | `local` 标记 |
|---|---|---|---|---|
| A | `node-cab8c15a` | `node-bdbff1cc` | `node-bdbff1cc` | 非本地区域 `false`，仅本地区域 `true` |
| C | `node-cab8c15a` | `node-bdbff1cc` | `node-bdbff1cc` | 同 A，与 A 完全一致 |
| B | `node-bdbff1cc` | `node-bdbff1cc` | `node-bdbff1cc` | r2/r3/r4 三项全部 `local=true`（独自分裂） |

证据要点：

- P0：探针前 B 上已存在 A/C 不可见的 `id=31`（S3 段写入）；
- P1：A 写入 `id=40` 后 A、C 同步为 13/181，B 仍为 13/172 → B 看不到 `id=40`；
- P2：A、C 均可见 `id=30/40/41`，B 仅见 `id=30/31` → 可见性差异是双向的，不是单向写入延迟；
- 路由：A 与 C 视图一致（r3、r4 → `node-bdbff1cc`，r2 → `node-cab8c15a`），B 把 r2/r3/r4 全部判为 `node-bdbff1cc` 且 `local=true`。

判定：B 重启后其 region 归属视图未刷新，读写均落本地，形成与 A/C 双向不可见的孤立视图。该探针为 §5.7 的根因判定提供直接证据，同时也说明 GAP C 中"聚合读恢复"与"写路径强制路由"必须同簇修复——只修读聚合无法消除分叉，写侧仍需按 region 归属强制路由。

### 5.7 新增根因判定：重启节点 region 归属视图未从集群权威源刷新

证据链：
1. 重启后的 B（S3、S4 两次重启均复现）把 r2 判为 `owner=node-bdbff1cc local=true`，而 A/C 均为 `node-cab8c15a`；
2. 该误判使 B 对 r2 区间的读写全部走本地，形成"孤立读视图"——B 写 `id=31` 后仅自己可见（13/172），A/C 仍 12/141；
3. 人工 `ASSIGN REGION 3` 只让 A/C 侧收敛，B 侧仍不自洽，说明 B 的本地 meta 未被集群权威视图覆盖，`HandleNodeUp` / join 同步未覆盖全部 region 或存在时序竞态。

影响：这是"写路径必须按 region 归属强制路由"的直接依据——只要节点本地归属视图可能与集群不一致，本地提交就会产生其他节点不可见的发散写。

处置：列为下一簇必修项（候选 C），与 §6 第 3 项（集群轨 epoch fencing / 按 region 归属强制路由）及 §4 建议 4（2PC 参与）同批处理；修复要点是节点 join 与心跳 UP 时以权威视图**全量覆盖**本地 region 归属，并对 `local` 标记做一致性校验。

> 后续进展（v0.3）：该根因已按上段要点分两轮修复并入库（`529e34c`、`af727fc`），VM 三节点复测确认视图收敛与跨端写可见性均闭合，详见 **§5.8**。

### 5.8 v1.18.x 修复复测：GAP A/B/C 全闭合

#### 5.8.1 修复内容（对应 §5.7 根因）

| 提交 | 修复项 | 针对 GAP |
|---|---|---|
| `529e34c` | 重启节点路由视图强制从集群权威源全量刷新（`HandleNodeUp` 首触丢失重试、`syncRegionsToNode` 改副本语义 `claim=false` 不覆盖远端权威 owner）；写路径按 region 归属强制路由（非 owner 远端写入走 `groupOps` 转发，无协调者时显式报错不静默本地写） | A 路由回迁、C 聚合读恢复 |
| `af727fc` | 权威路由同步（`Authoritative` 全量视图）完成后清理"权威视图不存在、且键区间已被现役同表 region 完全覆盖"的本地历史 region；仅删路由记录，物理键保留（`RetainedStale` 回执 + 告警日志），增量广播不做收敛删除 | 重启节点视图残留（`SHOW REGION ROUTES` 出现权威视图外历史 region 且标 `local=true`） |

#### 5.8.2 复测环境与口径

| 项 | 内容 |
|---|---|
| 环境 | VM `192.168.100.200` 三数据节点：A `node-24b6a076:7791`、B `node-bdbff1cc:7792`、C `node-cab8c15a:7793` |
| 二进制 | `af727fc` 构建，部署后校验和 `451f85b7451b94aad8fa3ba3608049de`，`/tmp/openxdb-af727fc` 与 `/usr/local/bin/openxdb` 一致；三端进程同一二进制，`SHOW NODES` 三节点均 `UP` |
| 序列 | 重建基线 → S1 `SIGKILL` 杀 B → S3 拉起 B → S3+ 人工回迁对照 → S4 `SIGTERM` 优雅退出重启 |
| region 编号 | 本轮重建基线后重新 `SPLIT`，编号为 **11/12/10**（v0.2 复测为 7/8/6 并残留 3/4/2），编号差异属重建正常现象 |

#### 5.8.3 读数一致性（COUNT(*) / SUM(id)）

| 阶段 | 操作 | A | B | C |
|---|---|---|---|---|
| 基线 | 重建后 | 11/111 | 11/111 | 11/111 |
| S1 | `SIGKILL` 杀 B，region 12 owner 接管为 A，停机期写 `id=30` | 12/141 | 进程已杀 | — |
| S3 | 拉起 B 后 | 12/141 | 12/141 | 12/141 |
| S3 | B 写 `id=31` 后 | 13/172 | 13/172 | 13/172 |
| S4 | `SIGTERM` 优雅重启 B 后 | 13/172 | 13/172 | 13/172 |

关键对照：基线由 v0.2 的 A 11/111、B 25/333、C 25/333 收敛为**三端 11/111**——v0.2 中 B/C 多出的行来自残留 region 副本重复可读，随视图清理同步消失。B 写 `id=31` 后三端同刻均为 13/172，v0.2 中"A/C 仍 12/141、仅 B 13/172"的发散现象不再出现。S3 段 B 查询停机期写 `id=30` 返回 `pA30`，确认 failover 接管数据对重启节点可见。

#### 5.8.4 路由视图收敛（v0.2 GAP A 现象消失）

S4 重启后三端 `SHOW REGION ROUTES` 最终态：

| region_id | 键区间 | 权威 owner | A `local` | B `local` | C `local` |
|---|---|---|---|---|---|
| 11 | `[ , ...0002)` | `node-24b6a076` | `true` | `false` | `false` |
| 12 | `[...0002, ...0003)` | `node-bdbff1cc` | `false` | `true` | `false` |
| 10 | `[...0003, )` | `node-cab8c15a` | `false` | `false` | `true` |

- 三端 region 集合均为 **{11, 12, 10}**，与 A 权威视图一致；重启节点 B 不再出现权威视图外的历史 region（v0.2 中 B 曾返回 3/7/4/8/2/6 共 6 条、region 3 与 7 区间完全重叠且 `local=true`）；
- `local` 标记仅对自身 owner 为 `true`，不存在"owner 为远端却 `local=true`"的矛盾条目（v0.2 中 B 的 r2/r3/r4 全部 `local=true`）；
- region 12 在 S1 被 A 接管、S3 重启后回迁至 B（`owner=node-bdbff1cc`），v0.2 中"r3 未自动回迁"的问题闭合。

#### 5.8.5 GAP 判定（v1.18.x 修复后）

| GAP | v0.2 判定 | v0.3 判定 | 依据 |
|---|---|---|---|
| A 路由回迁 | ⚠️ 部分改善，未闭合 | ✅ **PASS** | 5.8.4：三端集合与 owner 一致，region 12 自动回迁 B，重启节点无残留历史 region |
| B 数据追平 | ✅ PASS（维持） | ✅ **PASS** | 5.8.3：S3 三端 12/141 一致，B 可读停机期写 `id=30`；S4 重启后仍 13/172 一致 |
| C 聚合读恢复 | ❌ 未闭合 | ✅ **PASS** | 5.8.3：B 写 `id=31` 后 A/B/C 同刻均 13/172，双向写可见性不再分叉 |

#### 5.8.6 遗留观察（不改变上述判定）

1. **重启瞬间的短暂视图窗口**：S3 拉起 B 的瞬间，A 认为 region 11 owner=A、B 认为 owner=B，两侧视图存在毫秒级不一致；500ms 轮询内即收敛为 A 视角，窗口期内三端数据读一致，未观察到写不可见。属心跳/路由同步的固有收敛窗口，非缺陷，但灰度期应以 ≥2 副本兜底。
2. **`ASSIGN REGION 3` 报 `region not found: 3`**：属复测脚本硬编码 v0.2 的旧 region id（本轮重建后为 11/12/10），非功能缺陷；脚本补强见 §8。
3. **视图收敛不清理物理副本**：被取代 region 的键空间数据按设计保留在本机待人工 GC（`RetainedStale` 回执 + `manual GC required` 告警），属数据面对账范畴，不影响路由一致性；但对账前同一数据可能存在新旧副本并存，生产环境需纳入运维规程。

#### 5.8.7 小节结论

D4.4 三项 GAP 在 `af727fc` 二进制下**全部 PASS**，D4.4 复测判定可回写为 PASS。v0.3 当时强调"本轮修复只闭合路由视图一致性与跨节点写可见性，不改变三项生产阻塞项（B1/B2/T3）"——**该判断已被后续 Phase5-Phase9 修复链取代**：B2 由 Phase5/Phase7 闭环，B1 由 `396b4bb`/Phase8 改为自动副本补齐，T3 由 Phase9 落地 cluster term fencing（详见 §5.9 与 §6 重述）。`ha.quorumEnabled` 仍维持默认关闭、维持受限灰度，但理由已从"三项未实现"转为"无一致性协议 + 三项真机复验未做"。

### 5.9 Phase5-Phase9 修复链与复测（v1.0 新增）

> 本节补齐 v0.3 未覆盖的四轮修复（Phase5/6/8/9）及其间一次真机复测（Phase7），是 v1.0 升版的直接依据。所有提交均在 `59b1c05`（HEAD）链上，双远端一致。

#### 5.9.1 修复链总览

| 轮次 | 提交 | 根因 | 修复要点 | 验证形式 | 判定 |
|---|---|---|---|---|---|
| Phase5 | `a4b33bf` | `ha_*` 持久化配置在启动装配阶段被早于配置加载读取（零值覆盖盘上配置）；TSO HA 主备切换只接在测试装配路径，生产启动路径仍单点 | 调整配置加载顺序，确保持久化 `ha_quorum/ha_epoch/ha_replica` 在装配前生效；TSO HA 接入生产启动路径（主备探测 + 切换 + 回切） | 单测 + Phase7 真机（B2） | ✅ B2 闭环 |
| Phase6 | `0303c13` | NEW-1：重启节点与新主并存时归属 epoch 比较用字符串序，`"10" < "9"` 造成旧归属被判为新 → 双主可写 | region 归属 epoch 改字典序（数值比较）；加双写熔断（同 region 双写检测后拒绝后到者） | 单测 + Phase7 真机（部分闭环：不丢数，但双主仍可写） | ⚠️ 部分闭环 |
| Phase7 | （复测，无提交） | 真机矩阵复测暴露三个新事实 | — | VM 三节点真机（`0303c13` 二进制） | 见 5.9.2 |
| Phase8 | `3d8373d` | NEW-2：持久化 `ha_quorum=on` 时启动期 B1 副本补齐在存储就绪前触发，RocksDB 句柄为空，`xdb_snapshot_create(0x0)` 原生访问违例 0xc0000005 | ① B1 存储就绪栅栏 `b1StorageGate`（ready 门控 + deferred 补齐，`ha_b1_gate.go`）；② RocksDB 句柄读写锁 `hmu` + Snapshot nil 校验，Close 后返回 error 而非传空指针 | `TestPhase8New2StartupPersistedQuorumMatrix`、`TestPhase8B1StorageGateLifecycle`、`TestPhase8EnsureBlockedBeforeStorageReady`（2×3 矩阵） | ⚠️ 单测闭环，进程级真机矩阵未复跑 |
| Phase9 | `59b1c05` | NEW-1 与 T3 **同源**：集群轨缺少归属纪元，`masterEpoch` 恒 0 使 `AllowWrite` 恒放行，重启/分区后无权威判定依据 | 新增 cluster term：grant/revoke 推进纪元并落盘持久化；写门槛三态（本地权威 / 需上行确认 / 拒绝）；重启节点"待确认"期内拒绝写入直至对端确认；路由同步消息携带 term，claim owner 推送 grant | `TestP9ClusterTermGrantAndRevoke`、`TestP9RestartPendingUntilAuthoritative`、`TestP9ConfirmOwnershipLocalNoPeer`、`TestP9RestartStaleOwnerPendingWithoutAnyTerm`、`TestP9RestartSingleNodeNoPending`、`TestP9RestartPendingPersisted`、`TestP9LocalTakeoverNotifiesTerm`、`TestP9RouteSyncCarriesClusterTerm`、`TestP9ClaimOwnerPushNotifiesGrant`、`TestD45P9RestartOwnerConfirmedByPeersE2E` | ⚠️ 单测 + 进程内 e2e 闭环，真分区未复验 |

#### 5.9.2 Phase7 真机复测事实（`0303c13` 二进制，VM 三节点）

复测报告：`docs/D4_5_phase7_retest.md`。四项结论：

1. **NEW-2 为阻塞级**：写入 `ha.quorum=on` 到配置文件后重启进程即崩（0xc0000005），无法进入服务状态——意味着"按文档开启 quorum"的常规路径不可用，直接触发 Phase8。
2. **NEW-1 部分闭环**：Phase6 后重启窗口不再丢数，但新旧归属可同时写（双主可写），跨端收敛仍靠 Phase9。
3. **B2 TSO HA PASS**：主 TSO 停机后 0.10s 切备、恢复后 0.09s 回切，跨节点写未中断（对比 D4.1/D4.2 的 `tso unavailable` 全失败）——B2 由此转 PASS。
4. **T3 NOT VERIFIED**：本轮未构造真分区（VM 为单机多 IP 别名/同宿主，无法真断网），T3 仍按"代码未修"记录，直至 Phase9 落地代码。

#### 5.9.3 本版证据等级声明

- **真机证据（VM 三节点）**：D4.3、D4.4（含 `af727fc` 复测）、Phase7。用于 D4.4 三项 GAP 与 B2 的 PASS 判定。
- **单测 / 进程内 e2e 证据**：Phase8、Phase9 全部用例；本报告升版当日复跑 `go test ./pkg/db ./pkg/sql ./pkg/cluster -count=1` 全 ok（21.7s / 4.6s / 0.5s），全仓 `go test ./... -count=1` 22 包全绿。用于"代码级根因已修复"判定，**不用于 PASS 判定**。
- 因此 §3 矩阵中 B1 / T3 / NEW-1 / NEW-2 四行一律标注 ⚠️（代码已修 + 待真机复验），禁止以单测通过替代真机演练结论。

## 6. 剩余硬缺口（v1.0 重述）

> **v1.0 口径说明**：本节 v0.1-v0.3 曾以"单副本不可恢复 / TSO 单点 / T3 fencing 未生效"三项作为生产阻塞项。经 Phase5-Phase9 修复链，三项已全部从"未实现"降级为"已实现但缺真机复验或依赖默认关闭"，风险性质随之改变。以下为逐项重述，**降级不等于可开启 quorum**：仍存在一项结构性缺口（第 4 项：无一致性协议）。

1. **单副本 region 恢复（B1）——原"不可恢复"表述已废止**：`396b4bb` 起实现主动副本补齐（`ha_b1.go` 的 `EnsureRegionReplicas` + `b1EnsureAsync`）：归属节点宕机时立即为受影响 region 补齐副本（不再依赖人工 `ASSIGN REGION`），无任何存活副本时**记 pending 并持续重试**，不再"永久放弃重指派"；Phase8（`3d8373d`）追加存储就绪栅栏，消除补齐动作在存储未就绪时触发的启动期崩溃。
   - **残余风险（仍不得据此开全量）**：① 补齐为**异步推送**，非 raft 日志复制，补齐窗口内该 region 仍单副本可写，窗口内再损毁仍会丢数据；② 依赖开关——`ha_replica_enabled` 生产默认 **off**，仅在 `ha_quorum_enabled=on` 时联动生效，关闭态行为与 v1.17.0 一致；③ 补齐后无副本间持续复制通道（见第 4 项），副本间仍可能发散。
2. **TSO 中心单点（B2）——已闭环，保留运维约束**：Phase5（`a4b33bf`）修复 `ha_*` 持久化配置加载顺序，并把 TSO HA 接入**生产启动路径**（此前主备切换仅在测试装配路径生效）；Phase7 真机复测（`0303c13` 二进制）主 TSO 停机 **0.10s** 切备、恢复 **0.09s** 回切，跨节点写通过。
   - **保留约束**：TSO 主备仍须不与数据节点唯一副本同机；主备切换窗口内（实测亚秒级）存在写失败，业务侧需重试；TSO 主备本身不参与 quorum 仲裁。
3. **集群轨 epoch fencing（T3）——代码已实现，真实分区待复验**：Phase9（`59b1c05`）新增集群轨归属纪元 cluster term：grant/revoke 纪元推进并**落盘持久化**，写门槛由"`masterEpoch>0` 才校验（集群轨恒为 0 故恒放行）"改为三态（本地权威 / 需上行确认 / 拒绝），重启节点在获得权威确认前进入"待确认"而非直接放行；路由同步消息携带 cluster term，claim owner 时推送 grant 通知。
   - **残余风险**：仅进程内 e2e（`TestD45P9RestartOwnerConfirmedByPeersE2E`）与单测取证，**未在 VM 三独立 IP 上构造真实双向隔离**；真分区下"旧主写被拒 + 恢复后单主收敛"未取得实测证据，故不判 PASS（复验计划见 §8.2）。
4. **【结构性缺口】副本间无持续复制 / 无共识提交**：P3 的 region push 只在 rejoin 时刻做一次性对齐（`FullAlignViaRegionPush`），此后无持续复制通道；写提交为归属节点本地提交 + 归属强制路由，不做跨副本 2PC 或共识复制。因此 RPO>0（KNOWN_LIMITATIONS §3.4）、分区期写可能发散（需业务对账）、副本补齐窗口内仍单副本可写。**这是 v1.18.0 不具备生产级 HA 判据的唯一结构性原因**，须由 v1.19.0 M8（2PC 参与 / 持续复制）解决。
5. **重启节点 region 归属视图刷新——已闭合**：`529e34c` 强制视图从权威源刷新 + 写路径归属强制，`af727fc` 权威同步后清理被取代历史 region；VM 三节点复测确认重启节点视图与权威一致、跨端写可见（§5.8）。v1.0 起本项不再列为缺口，仅作历史留痕。
6. **新增验证欠账（非代码缺口）**：NEW-1 陈旧主写拒绝、NEW-2 进程级重启矩阵、T3 真分区三项只有单测/进程内证据，Phase7 真机矩阵未复跑；三项已列入 §8.2 收口待办，是本版保留"受限灰度"而非"可灰度放量"的直接原因。

## 7. 受限灰度开启指引

开启前提（全部满足方可开启）：

| 前提 | 要求 |
|---|---|
| 节点数 | ≥3 个数据节点（保证多数派可用） |
| region 副本 | 每个 region ≥2 个存活副本；v1.0 起可由 `EnsureRegionReplicas` 在 `ha_quorum_enabled=on` 时自动补齐（`ha_replica_enabled` 默认 off），但**不替代上线前的人工副本核对**（异步补齐存在窗口） |
| TSO 部署 | TSO 主备均须配置且不与任一 region 唯一副本同机；主备切换窗口（实测 0.10s 切备 / 0.09s 回切）内写请求需业务侧重试 |
| **重启窗口（v1.0 新增，Phase9 行为变化）** | 节点重启后在获得权威归属确认前**拒绝写入**（cluster term 待确认态），灰度需接受该节点秒级写不可用；不再有"待确认期放行写"的隐患，但需在运维流程中把重启节点摘流至确认完成 |
| 数据一致性预期 | 接受跨节点写 RPO>0、分区期写可能发散（需业务侧对账）；副本间无持续复制，补齐后仍可能发散（§6-4） |
| 回退手段 | feature flag 关闭 `ha_quorum_enabled` 即回退，回退后行为与 v1.17.0 一致（`ha_replica_enabled` 联动关闭，副本补齐停止） |

禁止开启场景：单机部署、两节点主从、单副本 region、以及任何要求强一致的业务场景。

灰度期需重点关注的日志：
- `failover skip candidate ... not held by candidate`：单副本无存活副本可接管；
- `rejoin region N to X: no local data, route only`：回迁时无本地数据，仅归还路由（数据未追平）；
- `tso unavailable: dial tcp ...`：TSO 不可达导致跨节点写失败；
- `checkRemoteReady` 拒绝转发：目标节点数据面未 READY（正常保护，持续出现则说明数据面未重建完成）。
- **（v1.0 新增）`cluster term pending` / 归属待确认拒写日志**：Phase9 后重启节点在权威确认前的正常保护行为，持续超过秒级未确认视为异常（对端不可达或 grant 丢失）；
- **（v1.0 新增）`b1 ensure deferred: storage not ready`**：Phase8 存储就绪栅栏生效记录；若长期停留 deferred，说明数据面未完成重建；
- **（v1.0 新增）`b1 ensure pending: no live replica`**：B1 无任何存活副本，已转 pending 重试（不再永久放弃）——此时该 region 数据不可读，需人工介入。

### 7.1 `ha_quorum` 默认关闭最终结论（v1.0）

**结论：`ha.quorumEnabled` 在 v1.18.0 维持默认 `off`，`ha.epochEnabled` 维持默认 `on`；不给全量开启建议，默认值在 v1.19.0 M8 落地前不再调整。**

依据三层：

1. **结构性**：集群轨无一致性协议（无 raft 式日志复制与选举），写提交为归属节点本地提交 + 归属强制路由，副本补齐为异步推送。故任何时刻都可能存在"单副本可写窗口"与 RPO>0，不满足生产级 HA 定义，与开关是否打开无关（§6-4）。
2. **证据性**：三项关键行为（T3 真分区 fencing、NEW-1 陈旧主写拒绝、NEW-2 进程级重启矩阵）目前只有单测 / 进程内 e2e 证据，Phase7 真机矩阵未复跑（§5.9.3、§8）。未取得真机证据前不得以"代码已修"替代"验证通过"。
3. **行为性**：Phase9 引入的重启待确认拒写，把原来的"错误放行"换成"正确拒绝"，代价是出现秒级写不可用窗口，属可用性回退。**须注意该门槛由 `ha_epoch_enabled`（v1.10.0 起默认 `on`）驱动，与 `ha_quorum_enabled` 无关**，即默认配置下也生效；缓解在于门槛仅三种情形触发（重启后归属待权威确认 / 归属已被权威视图收回 / 曾获授权 `clusterOwnTerm>0`），单节点集群、从未接管过 region 的普通节点恒放行（`AllowWrite` 在 `epochEnabled=false` 时整体短路），故对既有单机与普通集群部署为零回归，但**集群部署的重启操作需按 §7「重启窗口」一行摘流**。

升级路径：v1.19.0 M8 完成 2PC 参与 / 持续复制后，以真机三独立 IP（可真实双向隔离）复跑 T3、NEW-1、NEW-2 三项 → 三项转 PASS 且 2PC 落地后，再评估是否把默认值改为 `on`。

## 8. 待办与收口条件（v1.0 收口状态）

### 8.1 已闭环（可核验）

- [x] VM 三节点复测（2026-11-05，v1.18.0 含 P3 修复）：数据追平 PASS；路由回迁、聚合读两项初始未闭合（§5），已由 `529e34c`/`af727fc` 复测转 PASS（§5.8）
- [x] 未通过分支处置：记录新根因「重启节点 region 归属视图未从权威源刷新」（§5.7）并顺延至候选 C
- [x] 候选 C 修复并复测（`529e34c`、`af727fc`）：VM 三节点复测 D4.4 三项 GAP 全 PASS（§5.8）→ **本节原第 3 项收口条件满足，报告升 v1.0**
- [x] B2 TSO 单点：Phase5 `a4b33bf` 修复配置加载顺序 + TSO HA 接入生产启动路径；Phase7 真机 0.10s/0.09s 切换通过（§5.9）→ 矩阵 B2 转 PASS
- [x] B1 单副本不可恢复（原缺陷）：`396b4bb` 自动副本补齐（pending 重试替代永久放弃）、Phase8 `3d8373d` 存储就绪栅栏
- [x] NEW-2 启动崩溃：Phase8 `3d8373d`（句柄读写锁 + Snapshot nil 校验 + B1 栅栏），单测矩阵全绿
- [x] NEW-1 / T3 同源收口：Phase9 `59b1c05` cluster term 归属纪元推进 + 持久化 + 三态写门槛
- [x] §5.5 同步：`docs/HA_BRAIN_SPLIT_DESIGN.md` §5.5「v1.18.0 D4 灰度结论」已含本报告 v1.0 口径

### 8.2 未闭环（v1.18.0 发版前不阻塞，v1.19.0 前必须完成）

- [ ] **NEW-2 进程级重启矩阵真机复跑**：Phase8 仅单测（`TestPhase8New2StartupPersistedQuorumMatrix`），需在 VM 上重放 Phase7 的"持久化 `ha.quorum=on` → 重启"场景，确认 0xc0000005 不再出现
- [ ] **T3 真分区复验**：需三个**独立 IP**（非同宿主别名，可真实双向隔离）构造分区，验证"旧主写被拒 + 恢复后单主收敛"；当前 VM 为单机多 IP，无法取证
- [ ] **NEW-1 陈旧主写拒绝真机复验**：Phase9 后需在真机重放"重启节点 + 新主并存"场景，确认待确认态拒写、确认后恢复
- [ ] 脚本补强：S3+ 段补充 B 自身路由采集；跨端写探针（id=40/41）与逐 id 跨端可见性查询
- [ ] 写路径强制路由 / 2PC 参与 / 持续复制（D4.4 建议 4）：并入 v1.19.0 M8，为解除默认关闭的结构性前置
- [ ] KNOWN_LIMITATIONS 补充 Phase9 行为变化：`ha_epoch_enabled` 默认 `on` 下，集群部署重启节点在权威确认前拒写（§7.1-3）

### 8.3 本报告收口判定

- **升版依据**：§8 原收口条件为"三项 GAP 全 PASS 方可转 v1.0"，该条件已于 `af727fc` 复测满足；Phase5-Phase9 修复链使 B1/B2/T3 全部由"未实现"转为"已实现"，故本报告由草案 v0.3 升为**正式版 v1.0**。
- **升版不等于放行**：v1.0 的判定为「`ha_quorum` 维持默认关闭 + 受限灰度可用」，不是「生产可用」。§8.2 三项真机验证欠账完成前，不得把 T3/NEW-1/NEW-2 改写为 PASS，也不得在文档中给出全量开启建议。

## 9. 产物索引

- 演练脚本与输出：`tests/bench/bench_data/d41/`、`d42/`、`d43/`、`d44/`
- 分项报告：`docs/experiments/D4_1_quorum_enable_eval.md`、`docs/experiments/D4_2_failover_drill.md`、`docs/D4_3_split_brain_epoch.md`、`docs/D4_4_node_recovery_migration.md`
- 设计文档：`docs/HA_BRAIN_SPLIT_DESIGN.md`（§5.5）
- 分项报告（v1.0 新增）：`docs/D4_5_phase7_retest.md`（Phase7 真机复测，`0303c13` 二进制）
- 代码修复：`6a9f06c`（P3 source fixes）、`34235f1`（P3 rejoin fix）
- v0.2 复测日志：`d44_rerun_v1180_clean.txt`（会话中间产物目录，未入库）
- v0.3 修复提交：`529e34c`（路由视图强制从权威源刷新 + 写路径 region 归属强制）、`af727fc`（权威同步后清理被取代的历史 region）
- v0.3 复测日志：VM 三节点 `af727fc` 复测原始输出（会话中间产物目录，未入库；§5.8 数据全部取自该输出）
- **v1.0 修复链提交**：`396b4bb`（B1 自动副本补齐 + pending 重试）、`a4b33bf`（Phase5：`ha_*` 配置加载顺序 + TSO HA 生产接线）、`0303c13`（Phase6：NEW-1 epoch 字典序比较 + 双写熔断）、`3d8373d`（Phase8：NEW-2 启动崩溃 + B1 存储就绪栅栏）、`59b1c05`（Phase9：NEW-1/T3 同源收口，cluster term）
- **v1.0 用例索引**：Phase8 — `pkg/db/d4_5_phase8_*_test.go`；Phase9 — `pkg/db/d4_5_phase9_cluster_term_test.go`、`pkg/db/d4_5_phase9_restart_e2e_test.go`、`pkg/sql/ha_p9_test.go`、`pkg/cluster/p9_internal_test.go`

*（内容由AI生成，仅供参考）*

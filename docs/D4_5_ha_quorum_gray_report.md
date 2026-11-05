---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 00000000000000000000000000000000_0000000000000000
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 00000000000000000000000000000000_0000000000000000
---

# D4.5 HA quorum 灰度报告（草案 v0.1）

- 日期：2026-11-05
- 版本：v1.18.0 Phase B（D4）
- 依据：DEV_PLAN_v1.18.0 D4.5（全链路演练结论 + HA_BRAIN_SPLIT_DESIGN §5.5 更新）
- 演练基础：D4.1（quorum 开启评估）、D4.2（宕机 failover）、D4.3（脑裂 / epoch 拒绝）、D4.4（节点恢复回迁）
- **文档状态：草案**。P3 源码修复已入库（`6a9f06c`、`34235f1`），VM 三节点复测**尚未执行**；复测通过后本文档转 v1.0，并同步改写 D4.4 结论矩阵。

## 1. 结论摘要

| 判定项 | 结论 |
|---|---|
| `ha.quorumEnabled` 默认值 | **维持 off**（`ha.epochEnabled` 维持默认 on） |
| 是否全量开启 | **否** |
| 灰度形态 | **受限灰度（Restricted Gray）**：仅三节点及以上、每 region ≥2 存活副本、TSO 不与数据节点同机的验证环境可开启 |
| 全链路验收计数 | 通过 3 项 / 源码已修待复测 3 项 / 设计缺口未修 1 项 |
| 生产可用性定性 | 集群轨当前为"**单副本 + 中心 TSO**"结构，不具备生产级 HA 能力；quorum 开启只解决多数据节点下的仲裁与路由一致性，不解决数据面持久性与 TSO 单点 |

一句话结论：**quorum 开关本体行为正确（未破坏多数派语义），但集群轨的 failover 可用性受限于单副本、数据一致性与 TSO 单点三项硬缺口，v1.18.0 不具备全量开启条件，按受限灰度处理。**

## 2. 演练汇总

| 编号 | 演练内容 | 环境 | 结果 | 产物 |
|---|---|---|---|---|
| D4.1 | quorum=on 三端，S1-S5 场景矩阵 | Windows 本机三进程（17881-17883） | PASS 16 / FAIL 2（预期）/ GAP 2 | `tests/bench/bench_data/d41/run_d41_quorum_enable_eval.ps1`、`run_d41_output.txt` |
| D4.2 | 杀归属节点进程 → 读 failover | 本机三进程 | PASS 33 / FAIL 2（预期）/ GAP 1 | `tests/bench/bench_data/d42/run_d42_failover_drill.ps1`、`run_d42_output.txt` |
| D4.3 | 双向网络隔离 → epoch 拒绝 / 分区恢复 | VM 192.168.100.200 三节点 | T6/T8/T9 PASS；T1/T2 不适用（集群轨无升主路径）；**T3 GAP** | `tests/bench/bench_data/d43/run_d43_split_brain_epoch.py`、`run_d43_output.txt` |
| D4.4 | SIGKILL / SIGTERM 重启 → 回迁 / 追平 / 路由收敛 / 聚合读 | VM 三节点 | 宕机检测 PASS、failover 重指派 PASS；**路由回迁 / 数据回灌 / 聚合读恢复 三项 GAP** | `tests/bench/bench_data/d44/run_d44_node_recovery.py`、`run_d44_output.txt` |

## 3. 结论矩阵

| 验收项 | 结论 | 证据 | 说明 |
|---|---|---|---|
| quorum 多数派仲裁门（2/3 通过、1/3 拒绝） | ✅ PASS | D4.1 S1-S5 | 1/3 场景按设计拒绝，未误放行 |
| 不破坏三节点自动升主语义 | ✅ PASS | D4.1 | 多数派存活时升主/重指派路径正常 |
| 宕机心跳检测（DOWN） | ✅ PASS | D4.2、D4.4 | interval 2s / timeout 6s，约 6s 生效 |
| 读 failover（存活副本前提） | ✅ PASS | D4.2 S3 | `resolveRegionOwner` → `FailoverDownNode` → 持有校验通过后重指派，聚合读完整 |
| 单副本 region 宕机可恢复（B1） | ❌ FAIL（设计边界） | D4.1 B1、D4.2 S2 | 候选节点均不持有副本 → `failover skip candidate` → 放弃重指派 → 聚合读丢行 |
| TSO 单点下的跨节点写（B2） | ❌ FAIL（已知限制） | D4.1 B2、D4.2 S2、D4.4 | 中心 TSO down 时 `tso unavailable`，跨节点写全失败；读路径不受影响 |
| T3 分区恢复合并 + 旧主 fencing（集群轨） | ❌ GAP（未修） | D4.3 | 集群轨 `masterEpoch=0` → `AllowWrite` 恒放行；分区期旧主写成功，恢复后不自动收敛 |
| 节点恢复后路由自动回迁 | ⚠️ 源码已修，待复测 | D4.4 S3 + P3 | 原 `failover` 接管不回迁、B 本地 meta 与集群视图分裂；P3 引入 `SourceOwner` + `HandleNodeUp` 自动回迁 |
| 节点恢复后数据回灌 / 追平 | ⚠️ 源码已修（一次性对齐），待复测 | D4.4 S3 + P3 | P3 以 region 级 `FullAlignViaRegionPush` 一次性对齐代替持续复制；非持续追平，RPO>0 |
| 恢复后聚合读恢复 | ⚠️ 源码已修，待复测 | D4.4 S3/S4 + P3 | P3 加 `DataReady` READY 门 + join 路由同步；D4.4 观测三端仅见本地行（A=6/B=4/C=3） |
| 恢复节点写路径进入全局视图 | ❌ 未修 | D4.3、D4.4 | 集群轨写仍为本地提交，不按 region 归属强制路由，写出发散不可避免 |

## 4. P3 源码修复清单（对照 D4.4 §4 修复建议）

| D4.4 建议 | P3 落地实现 | 提交 | 复测 |
|---|---|---|---|
| 1. 节点 join 时同步全量 region 路由、覆盖本地 meta，重建数据面通道 | `execAddNode` 先将新节点 `SetDataReady(false)` 并广播 → `pushAllRegionsToNode` 下发权威路由；`SetOnUp` + `HandleNodeUp` 在心跳 DOWN→UP 时重同步；`RegisterPeerData` 对"本地 DOWN、广播 UP"场景锁外 `notifyUp`（修复注册表广播抢占心跳检测导致回调丢失） | `6a9f06c`、`34235f1` | 待执行 |
| 2. failover 接管记录来源，原主恢复后回迁 | `sharding.RegionInfo.SourceOwner` + `SetRegionNodeEpochSource`；本机接管与远端接管分支均记录；`HandleNodeUp` 对 `SourceOwner == nodeID` 的 region 调 `tryRejoinAsFollower`（含无数据场景 `routeOnlyRejoin` 纯路由归还） | `6a9f06c`、`34235f1` | 待执行 |
| 3. 数据追平（region 级全量对齐） | `FullAlignViaRegionPush` 复用复制轨通道，把 `SourceOwner` 区域从接管节点改挂到回迁节点；`syncRegionsToNode` 在对齐推送成功后标记 `DataReady=true` | `6a9f06c` | 待执行 |
| 4. 写路径经 region 归属强制路由 / 2PC 参与 | **未落地**（仍本地提交） | — | 未启动（建议并入 v1.19.0 M8 2PC） |

聚合读 READY 门细节：`getKVCluster` / `scanMergedCluster` / `scanMergedClusterTop` 对 `DataReady=false` 的节点拒绝转发（`checkRemoteReady`），`db.StartCluster` 启动即声明自身未就绪，避免重启节点在数据面未重建时污染聚合视图。

## 5. 剩余硬缺口（生产阻塞项）

1. **单副本 region 不可恢复（B1）**：shared-nothing 单副本分片的固有边界。当前仅能靠 `ASSIGN REGION` 手工建立第二副本，缺少自动副本复制 / raft 组。生产部署必须保证每 region ≥2 副本，否则任一归属节点宕机即该 region 数据不可读。
2. **TSO 中心单点（B2）**：TSO 挂在单一节点，该节点宕机则全集群跨节点写失败（读不受影响）。需 TSO HA（主备 + epoch 选主）。
3. **集群轨 epoch fencing 未生效（T3）**：`AllowWrite` 在 `masterEpoch>0` 才生效，集群轨 `masterEpoch` 恒为 0，分区期旧主写被放行。需将集群轨写入纳入 epoch 授权，或经 region 归属强制路由。
4. **追平为一次性对齐而非持续复制**：P3 的 region push 只在 rejoin 时刻对齐一次，此后无持续复制通道，恢复节点与归属节点之间仍可能发散；RPO>0 属既定语义（KNOWN_LIMITATIONS §3.4）。

## 6. 受限灰度开启指引

开启前提（全部满足方可开启）：

| 前提 | 要求 |
|---|---|
| 节点数 | ≥3 个数据节点（保证多数派可用） |
| region 副本 | 每个 region ≥2 个存活副本（`ASSIGN REGION` 手工建副本） |
| TSO 部署 | TSO 节点不得与唯一副本所在节点同机；接受 TSO 单点写风险 |
| 数据一致性预期 | 接受跨节点写 RPO>0、分区期写可能发散（需业务侧对账） |
| 回退手段 | feature flag 关闭 `ha_quorum_enabled` 即回退，回退后行为与 v1.17.0 一致 |

禁止开启场景：单机部署、两节点主从、单副本 region、以及任何要求强一致的业务场景。

灰度期需重点关注的日志：
- `failover skip candidate ... not held by candidate`：单副本无存活副本可接管；
- `rejoin region N to X: no local data, route only`：回迁时无本地数据，仅归还路由（数据未追平）；
- `tso unavailable: dial tcp ...`：TSO 不可达导致跨节点写失败；
- `checkRemoteReady` 拒绝转发：目标节点数据面未 READY（正常保护，持续出现则说明数据面未重建完成）。

## 7. 待办与收口条件

- [ ] VM 三节点复测：`tests/bench/bench_data/d44/run_d44_node_recovery.py` 复跑 S1-S4，验证路由自动回迁、数据对齐、聚合读恢复三项是否转 PASS；
- [ ] 复测通过后：本文档转 v1.0，D4.4 结论矩阵三项 GAP 改写为 PASS 并注明修复提交；未通过则记录新根因并顺延；
- [ ] 写路径强制路由 / 2PC 参与（对应 D4.4 建议 4）：并入 v1.19.0 M8 排期；
- [ ] T3 集群轨 fencing：随写路径改造一并闭环；
- [ ] §5.5 更新：本报告结论已同步至 `docs/HA_BRAIN_SPLIT_DESIGN.md` §5.5「v1.18.0 D4 灰度结论」。

## 8. 产物索引

- 演练脚本与输出：`tests/bench/bench_data/d41/`、`d42/`、`d43/`、`d44/`
- 分项报告：`docs/experiments/D4_1_quorum_enable_eval.md`、`docs/experiments/D4_2_failover_drill.md`、`docs/D4_3_split_brain_epoch.md`、`docs/D4_4_node_recovery_migration.md`
- 设计文档：`docs/HA_BRAIN_SPLIT_DESIGN.md`（§5.5）
- 代码修复：`6a9f06c`（P3 source fixes）、`34235f1`（P3 rejoin fix）

*（内容由AI生成，仅供参考）*

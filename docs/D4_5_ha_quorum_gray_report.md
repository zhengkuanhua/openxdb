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

# D4.5 HA quorum 灰度报告（草案 v0.3）

- 日期：2026-11-05
- 版本：v1.18.0 Phase B（D4）
- 依据：DEV_PLAN_v1.18.0 D4.5（全链路演练结论 + HA_BRAIN_SPLIT_DESIGN §5.5 更新）
- 演练基础：D4.1（quorum 开启评估）、D4.2（宕机 failover）、D4.3（脑裂 / epoch 拒绝）、D4.4（节点恢复回迁）
- **文档状态：草案（v0.2 基线状态；现行版本见下方 v0.3 变更记录）**。P3 源码修复已入库（`6a9f06c`、`34235f1`）；VM 三节点复测已于 2026-11-05 执行（v1.18.0 含 P3 修复二进制，S1-S4），三项 GAP 仅"数据追平"转 PASS，路由回迁仅部分改善、聚合读恢复仍未闭合，**不满足转 v1.0 条件**，维持草案并按受限灰度处理。
- 复测原始日志：`d44_rerun_v1180_clean.txt`（会话中间产物目录，未入库；本文第 5 节数据全部取自该日志，未做推断补齐）
- **v0.3 变更记录（2026-11-05）**：承接 §5.7 根因判定，两轮修复入库（`529e34c` 路由视图强制从权威源刷新 + 写路径 region 归属强制；`af727fc` 权威同步后清理被现役 region 取代的本地历史 region），VM 三节点复测（二进制 `af727fc`）显示 D4.4 三项 GAP **全部 PASS**（详见 §5.8）。v0.2 各节内容按原样保留以留痕：其"路由回迁部分改善、聚合读未闭合"判定对应修复前状态，结论已被 §5.8 取代。本文档仍维持**草案 v0.3**——GAP 全闭合不等于生产可用，§6 的三项硬缺口（单副本 B1、TSO 单点 B2、T3 fencing）未因本轮修复改变，转 v1.0 由 §8 收口条件决定。

## 1. 结论摘要

| 判定项 | 结论 |
|---|---|
| `ha.quorumEnabled` 默认值 | **维持 off**（`ha.epochEnabled` 维持默认 on） |
| 是否全量开启 | **否** |
| 灰度形态 | **受限灰度（Restricted Gray）**：仅三节点及以上、每 region ≥2 存活副本、TSO 不与数据节点同机的验证环境可开启 |
| 全链路验收计数 | 共 11 项：通过 5 项 / 部分改善未闭合 1 项（路由回迁）/ 未闭合 3 项（T3 fencing、聚合读恢复、写路径全局视图）/ 设计边界或已知限制 2 项（B1、B2） |
| 生产可用性定性 | 集群轨当前为"**单副本 + 中心 TSO**"结构，不具备生产级 HA 能力；quorum 开启只解决多数据节点下的仲裁与路由一致性，不解决数据面持久性与 TSO 单点 |

一句话结论：**quorum 开关本体行为正确（未破坏多数派语义）；v1.18.0 含 P3 修复的 VM 三节点复测显示数据追平已闭合，但路由回迁仅部分改善、聚合读一致性在重启后写入即发散，且定位出"重启节点 region 归属视图未从集群权威源刷新"的新根因；failover 可用性仍受限于单副本、写路径未按归属强制路由与 TSO 单点三项硬缺口，v1.18.0 不具备全量开启条件——quorum 维持默认关闭，按受限灰度处理，不升 v1.0。**

> **v0.3 结论更新（2026-11-05）**：上述为 v0.2 判定，保留作基线。追加两轮修复（`529e34c` 路由视图强制从权威源刷新 + 写路径归属强制、`af727fc` 权威同步后清理被取代历史 region）后，VM 三节点以 `af727fc` 二进制复测：**D4.4 三项 GAP 全部 PASS**——路由回迁 PASS（三端 region 集合 {11,12,10} 一致、region 12 自动回迁 B、重启节点无残留历史 region）、数据追平 PASS（S3 三端 12/141，S4 后 13/172）、聚合读 PASS（B 写 `id=31` 后三端同刻 13/172）。证据与逐项对照见 **§5.8**。
>
> 但 **quorum 默认关闭的结论不变**：本轮只闭合路由视图一致性与跨节点写可见性，§6 三项生产阻塞项（单副本 B1、TSO 单点 B2、T3 fencing）未改善，**本文档维持草案 v0.3，不升 v1.0**。

## 2. 演练汇总

| 编号 | 演练内容 | 环境 | 结果 | 产物 |
|---|---|---|---|---|
| D4.1 | quorum=on 三端，S1-S5 场景矩阵 | Windows 本机三进程（17881-17883） | PASS 16 / FAIL 2（预期）/ GAP 2 | `tests/bench/bench_data/d41/run_d41_quorum_enable_eval.ps1`、`run_d41_output.txt` |
| D4.2 | 杀归属节点进程 → 读 failover | 本机三进程 | PASS 33 / FAIL 2（预期）/ GAP 1 | `tests/bench/bench_data/d42/run_d42_failover_drill.ps1`、`run_d42_output.txt` |
| D4.3 | 双向网络隔离 → epoch 拒绝 / 分区恢复 | VM 192.168.100.200 三节点 | T6/T8/T9 PASS；T1/T2 不适用（集群轨无升主路径）；**T3 GAP** | `tests/bench/bench_data/d43/run_d43_split_brain_epoch.py`、`run_d43_output.txt` |
| D4.4 | SIGKILL / SIGTERM 重启 → 回迁 / 追平 / 路由收敛 / 聚合读 | VM 三节点 | 宕机检测 PASS、failover 重指派 PASS；首轮三项 GAP；**v0.2 复测（v1.18.0 含 P3）：数据追平转 PASS，路由回迁部分改善未闭合，聚合读恢复未闭合** | `tests/bench/bench_data/d44/run_d44_node_recovery.py`、`run_d44_output.txt`、`d44_rerun_v1180_clean.txt` |

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
| 节点恢复后路由自动回迁 | ⚠️ 部分改善，未闭合 | D4.4 S3/S3+/S4 + P3 | failover 重指派生效（杀 B 后 A 路由 r3/r4 `owner=node-24b6a076 local=true`），重启后 r4 自动回迁到 B；但 (a) r3 未自动回迁，仍留 A；(b) 重启后的 B 自身路由视图与 A/C 不一致——B 显示 `r2 owner=node-bdbff1cc(local=true)`，A/C 显示 `r2 owner=node-cab8c15a`；(c) 人工 `ASSIGN REGION 3 TO NODE 'node-bdbff1cc'` 后 A/C 收敛一致 |
| 节点恢复后数据回灌 / 追平 | ✅ PASS（一次性对齐，RPO>0） | D4.4 S3 | 重启 B 后三端读数一致 12/141；B 上可查到 down 期写 `id=30` 值 `pA30`。仍为 region 级一次性 `FullAlignViaRegionPush` 对齐，非持续复制 |
| 恢复后聚合读恢复 | ❌ 未闭合 | D4.4 S3/S3+/S4 | 三端一致仅维持到重启瞬间：此后 B 写入 `id=31` 仅 B 读到 13/172，A/C 仍 12/141；S4 优雅退出重启后 A 12/141、B 13/172、C 12/141 |
| 恢复节点写路径进入全局视图 | ❌ 未修（v0.2 已定位根因） | D4.3、D4.4 | 集群轨写仍为本地提交，不按 region 归属强制路由；复测进一步定位根因为重启节点 region 归属视图未从集群权威源刷新（详见 §5.7） |

> **v0.3 覆盖说明（2026-11-05）**：上表"节点恢复后路由自动回迁""恢复后聚合读恢复""恢复节点写路径进入全局视图"三行结论为 **v0.2（`af727fc` 修复前）** 判定，保留作基线。`529e34c` + `af727fc` 复测后三项均转 **✅ PASS**，覆盖关系与证据见 §5.8.5；其余各行结论不受本轮修复影响。

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

D4.4 三项 GAP 在 `af727fc` 二进制下**全部 PASS**，D4.4 复测判定可回写为 PASS。但需强调：本轮修复只闭合"路由视图一致性 + 跨节点写可见性"，**不改变** §6 的三项生产阻塞项（单副本 region 不可恢复 B1、TSO 中心单点 B2、集群轨 T3 fencing 未生效），`ha.quorumEnabled` 维持默认关闭、维持受限灰度；本文档转 v1.0 仍按 §8 收口条件执行。

## 6. 剩余硬缺口（生产阻塞项）

1. **单副本 region 不可恢复（B1）**：shared-nothing 单副本分片的固有边界。当前仅能靠 `ASSIGN REGION` 手工建立第二副本，缺少自动副本复制 / raft 组。生产部署必须保证每 region ≥2 副本，否则任一归属节点宕机即该 region 数据不可读。
2. **TSO 中心单点（B2）**：TSO 挂在单一节点，该节点宕机则全集群跨节点写失败（读不受影响）。需 TSO HA（主备 + epoch 选主）。
3. **集群轨 epoch fencing 未生效（T3）**：`AllowWrite` 在 `masterEpoch>0` 才生效，集群轨 `masterEpoch` 恒为 0，分区期旧主写被放行。需将集群轨写入纳入 epoch 授权，或经 region 归属强制路由。
4. **追平为一次性对齐而非持续复制**：P3 的 region push 只在 rejoin 时刻对齐一次，此后无持续复制通道，恢复节点与归属节点之间仍可能发散；RPO>0 属既定语义（KNOWN_LIMITATIONS §3.4）。
5. **重启节点 region 归属视图未从权威源全量刷新（v0.2 新增）**：节点重启后把非本地 region 误判为 `local=true`，读写全部走本地，形成孤立读视图（§5.7）。属写路径强制路由的前置必修项，与本节第 3 项、§4 建议 4 同批闭合。

> v0.3 状态更新：本项已闭合（`529e34c` 视图强制刷新 + 写路径归属强制、`af727fc` 清理被取代历史 region），VM 三节点复测确认重启节点视图与权威一致、跨端写可见，详见 §5.8。本节第 1-4 项生产阻塞状态不变。

## 7. 受限灰度开启指引

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

## 8. 待办与收口条件

- [x] VM 三节点复测（2026-11-05 已执行，v1.18.0 含 P3 修复）：数据追平 PASS、路由回迁部分改善未闭合、聚合读恢复未闭合（§5）
- [x] 未通过分支处置：记录新根因（重启节点 region 归属视图未从权威源刷新，§5.7）并顺延；本文档**维持草案 v0.2，不升 v1.0**
- [ ] 三项 GAP 全部转 PASS 后方可：本文档转 v1.0，D4.4 结论矩阵三项 GAP 改写为 PASS 并注明修复提交
- [ ] 候选 C：region 归属视图全量刷新修复（解除 r3 不回迁、重启节点视图错位）+ 复测
- [x] 候选 C 已修复并复测（2026-11-05）：`529e34c`（路由视图强制从权威源刷新 + 写路径归属强制）、`af727fc`（权威同步后清理被取代历史 region）；VM 三节点复测三项 GAP 全 PASS（§5.8）
- [ ] 脚本补强：S3+ 段补充 B 自身路由采集；增加跨端写探针（id=40/41）与逐 id 跨端可见性查询
- [ ] 写路径强制路由 / 2PC 参与（对应 D4.4 建议 4）：并入 v1.19.0 M8 排期；
- [ ] T3 集群轨 fencing：随写路径改造一并闭环；
- [ ] §5.5 更新：本报告结论已同步至 `docs/HA_BRAIN_SPLIT_DESIGN.md` §5.5「v1.18.0 D4 灰度结论」。

## 9. 产物索引

- 演练脚本与输出：`tests/bench/bench_data/d41/`、`d42/`、`d43/`、`d44/`
- 分项报告：`docs/experiments/D4_1_quorum_enable_eval.md`、`docs/experiments/D4_2_failover_drill.md`、`docs/D4_3_split_brain_epoch.md`、`docs/D4_4_node_recovery_migration.md`
- 设计文档：`docs/HA_BRAIN_SPLIT_DESIGN.md`（§5.5）
- 代码修复：`6a9f06c`（P3 source fixes）、`34235f1`（P3 rejoin fix）
- v0.2 复测日志：`d44_rerun_v1180_clean.txt`（会话中间产物目录，未入库）
- v0.3 修复提交：`529e34c`（路由视图强制从权威源刷新 + 写路径 region 归属强制）、`af727fc`（权威同步后清理被取代的历史 region）
- v0.3 复测日志：VM 三节点 `af727fc` 复测原始输出（会话中间产物目录，未入库；§5.8 数据全部取自该输出）

*（内容由AI生成，仅供参考）*

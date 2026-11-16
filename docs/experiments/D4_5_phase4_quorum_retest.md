---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_4ae2595dd9bd11f19232525400248c00
    ReservedCode1: fYDAxJN1g1Kd3pU9gycb7rtnGHiwYp8SMP61LusGmr78+KRliaSmg2Ez61zYO/HdJzjHPeC28gnSksCX+qQCMD5kgMFGCCxyrrvb1LdQjBd18LtMex9vDV6FNWoXuZtjdD8iXBjJeFTrtmsXzdFIRJQ/bO7zKm3wBOPUQpWgQBj8zXw/jhZqRzcCR4A=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_4ae2595dd9bd11f19232525400248c00
    ReservedCode2: fYDAxJN1g1Kd3pU9gycb7rtnGHiwYp8SMP61LusGmr78+KRliaSmg2Ez61zYO/HdJzjHPeC28gnSksCX+qQCMD5kgMFGCCxyrrvb1LdQjBd18LtMex9vDV6FNWoXuZtjdD8iXBjJeFTrtmsXzdFIRJQ/bO7zKm3wBOPUQpWgQBj8zXw/jhZqRzcCR4A=
---

# D4.5 Phase 4：quorum 开启态 VM 三节点复测报告（B1 / B2 / T3）

- 复测日期：2026-11-06
- 基线：`396b4bb`（B1 单副本不可恢复修复 + B2 TSO HA + T3 集群轨 fencing 三者同源）
- 二进制：`/usr/local/bin/openxdb-396b4bb`，`OpenXDB v1.17.0 (M2 master-slave replication) dev build`
- 环境：VM 192.168.100.200，节点 A(node-24b6a076, 集群 7791 / SQL 13306)、B(node-bdbff1cc, 7792 / 13307)、C(node-cab8c15a, 7793 / 13308)
- 复测脚本：`tests/bench/bench_data/d45/run_d45_quorum_ha.py`（分段可重放，各段输出 JSON）
- 复测姿态：三节点均 `SET ha_epoch_enabled=on`、`SET ha_quorum_enabled=on`（session 级热更新），全程 quorum 开启态
- 本轮约束：只做验证，未修改 quorum 默认值，未改动 T3/B2/B1 已入库逻辑

> 说明：三节点部署在同一台 VM、同一 IP 的不同端口上，`iptables` 为**主机级**规则，无法按进程隔离入/出向流量。这一环境限制直接决定了 T3 真分区场景的可达性，详见 §2。

---

## 1. 结论速览

| 项 | 场景 | 结果 | 判定 |
|---|---|---|---|
| T3-a | quorum 开启态下孤立节点拒绝升主（防脑裂） | 三节点日志均出现 `refuse to promote: reachable 1/3, need 2` | **PASS** |
| T3-b | 分区期间旧主写被拒 | 旧主写入返回 `TXN_PREPARE_FAILED ... participant node-cab8c15a: cluster: node is down`，未落盘 | **PASS（拒绝生效，但拒绝原因非 fencing 报错）** |
| T3-c | 真分区下"多数派侧仍可写" | 单 IP 环境无法构造 A/C 多数派 + B 少数派的真分区 | **NOT VERIFIED** |
| B2 | TSO 主节点宕机后跨节点写自动恢复 | A/B 写入 0.0s 立即失败 `tso unavailable: dial tcp 192.168.100.200:7793: connection refused`，8s 后重试仍失败 | **FAIL** |
| B2′ | TSO HA 生产接线 | `EnableTSOHA` 仅被 `pkg/db/tso_ha_test.go` 调用，启动路径无开关/无装配 | **FAIL（根因）** |
| B1-a | owner 下线后数据可读（持有副本节点） | A 停机后 C 读 `d45_t2` 返回 3 行、`d45_t1` 返回 12 行 | **PASS** |
| B1-b | owner 下线后数据可读（无副本节点） | B 读 `d45_t2`/`d45_t1` 全部报 `dial tcp ...:7791: connection refused` | **FAIL** |
| B1-c | owner 下线后自动 failover/回迁 | 停机窗口内未见 region 接管动作，B1 补齐日志为空 | **NOT VERIFIED / 可观测性缺失** |
| NEW-1 | 重启节点 region 归属视图与权威端收敛 | A 重启后 r2 归属为 A，B/C 为 C；A 写 70 与 B/C 写 71/72 均成功，形成**双主数据分叉**，二次重启收敛后 A 侧 70 **静默丢失** | **FAIL（新阻塞）** |
| NEW-2 | `SET PERSIST ha_*` 后的节点重启 | `persisted.json` 含 `ha_epoch_enabled="on"` 时进程启动即退出：`requires cluster HA configuration` | **FAIL（新阻塞）** |

**总体判定：`ha.quorumEnabled` 尚不满足默认开启条件，维持 off；`docs/D4_5_ha_quorum_gray_report.md` 维持草案 v0.2，不升 v1.0。**

---

## 2. 环境与方法

### 2.1 基线布置

| 对象 | 内容 |
|---|---|
| 表 `d45_t1` | SPLIT 出 r3 / r4 / r2，r4→B、r2→C、r3 留 A；三端写入后 11 行、SUM(id)=111 |
| 表 `d45_t2` | 组网后创建并 SPLIT 出 r5 / r6，均归 A（单副本，用于 B1 探针），3 行 |
| 三端一致性 | 两表三端 `COUNT(*)`/`SUM(id)` 完全一致，路由视图五 region 一致 |
| quorum | 三端 `SET ha_epoch_enabled=on`、`SET ha_quorum_enabled=on` 返回空（成功），`b1Enabled()` 因 quorum=on 自动置真 |

### 2.2 分区手段与限制

T3 场景使用端口级隔离：

```
iptables -A INPUT  -p tcp --dport 7792 -j DROP
iptables -A OUTPUT -p tcp --dport 7791 -j DROP
iptables -A OUTPUT -p tcp --dport 7793 -j DROP
```

实测结果：由于 A/B/C 共享同一 IP，`OUTPUT` 规则作用于**整机**所有进程，A→C、B→C 流量同时被切断，实际形成"三向全断"而非"B 少数派 + A/C 多数派"的可控分区。

- A 上 `SHOW NODES`：A UP，B/C DOWN
- C 上 `SHOW NODES`：C UP，A/B DOWN
- B 上同理（三端日志均为 `reachable 1/3`）

因此本轮 T3 仅验证了"孤立节点不升主 + 孤立节点写入被拒"，**未能**验证真分区下多数派侧可用性。要闭环需为三节点分配独立 IP（`ip addr add` 别名 + 重新组网），属于环境改造，未在本轮执行。

---

## 3. 场景数据

### 3.1 T3 —— 分区与 fencing

分区 22s 后采集（原始输出摘录）：

```
# A 节点日志
23:49:54 sql: failover down node node-bdbff1cc skipped: quorum not met: db: quorum not met, refuse to promote: reachable 1/3, need 2
23:50:00 sql: failover down node node-cab8c15a skipped: quorum not met: db: quorum not met, refuse to promote: reachable 1/3, need 2
23:50:47 [openxdb-mysql] query failed: SQL error: TXN_PREPARE_FAILED: 2pc ...-1 participant node-cab8c15a: cluster: node is down; sql="INSERT INTO d45_t1 VALUES (40,'a-part')"

# B 节点（旧 r4 owner）写入：3 次全部被拒
ERROR 1045: TXN_PREPARE_FAILED: 2pc ...-1 participant node-cab8c15a: cluster: node is down
ERROR 1045: TXN_PREPARE_FAILED: 2pc ...-2 participant node-cab8c15a: cluster: node is down
ERROR 1045: TXN_PREPARE_FAILED: 2pc ...-3 participant node-cab8c15a: cluster: node is down

# C 节点（r2 owner）写入成功
(empty)  →  INSERT INTO d45_t1 VALUES (41,'c-part') 成功

# 分区期间 A 侧读取（仅本地 region3）
COUNT(*)  SUM(id) = 5 / 15
```

分区撤销后（25s 心跳恢复，`node up ... route synced`）：

| 节点 | COUNT(*) | SUM(id) |
|---|---|---|
| A | 12 | 152 |
| B | 12 | 152 |
| C | 12 | 152 |

→ 隔离期被拒的写（id 30/31/32/40）全部未落盘，恢复后三端一致、无重复、无脏写。

**结论**：quorum 开启态有效阻止了孤立节点升主（`reachable 1/3, need 2`），符合 T3 防脑裂设计意图；但拒绝旧主写入走的是 2PC 参与者不可达路径（`cluster: node is down`），不是 `ErrFenced`，说明 fencing 校验在本场景中并非首道拦截点。

### 3.2 B2 —— TSO 主节点（C）宕机

| 探针 | 结果 | 耗时 |
|---|---|---|
| 停机前 A/B 读 | 12 行 / 152 | — |
| `kill -TERM` C | 正常退出 | — |
| A 写入 id=60 | `TXN_PREPARE_FAILED: tso unavailable: dial tcp 192.168.100.200:7793: connect: connection refused` | 0.0s |
| B 写入 id=61 | 同上 | 0.0s |
| A 8s 后重试 id=62 | 同上（无任何重试/切换迹象） | — |
| A 读取 | 12 行 / 152（读不受影响） | — |
| 重启 C 后 | 三端 12 行 / 152 | — |

源码核验（`findstr EnableTSOHA *.go`）：

```
pkg\cluster\tso_ha.go:95:  func (s *Server) EnableTSOHA(...)          // 定义
pkg\db\tso_ha.go:42:       func (d *DB) EnableTSOHA(...)                // 定义
pkg\db\tso_ha_test.go:120: d.EnableTSOHA(group, 8)                      // 仅测试调用
pkg\db\tso_ha_test.go:255: dA.EnableTSOHA(group, 8)                     // 仅测试调用
```

启动日志亦印证：`TSO source -> node node-cab8c15a (batch 64)` 为**固定指向**，无选主/接管痕迹。

**结论**：B2 的 HA 状态机（epoch/last/reserved 落盘、TSO_ELECT/TSO_FENCE、同 epoch 仲裁）已在源码层实现且单测 PASS，但**生产二进制未接线**，TSO 仍是单点。本项 GAP 在真实环境未闭环。

### 3.3 B1 —— 单副本 owner（A）下线

owner 关系：r3/r5/r6 → A，r4 → B，r2 → C。

| 探针（A 停机 6s 后） | B | C |
|---|---|---|
| `SELECT id,v FROM d45_t2` | `dial tcp 192.168.100.200:7791: connection refused` | `1 t1 / 2 t2 / 3 t3`（3 行全返） |
| `SELECT COUNT(*),SUM(id) FROM d45_t1` | 同上错误 | `12 / 152` |

- C 侧在 r5/r6 owner（A）已 DOWN 的情况下仍能完整读出 `d45_t2` 全部 3 行，证明 B1 的影子副本（`claim=false`，副本数 2，选源 push）在 C 上已存在且可读。
- B 侧两表查询均直接失败，说明 B 未获得副本，且**读路径未对"owner DOWN"做转发/降级**，直接向 7791 发起连接。
- 全程未见 `B1` 相关日志（`b1_log_before` 为空），副本补齐过程无任何可观测输出。

**结论**：B1 修复使"数据在存活副本节点上可读"成立（PASS），但未做到"集群内任一节点在 owner 下线后均可读"（FAIL）；补齐与接管缺乏日志，无法从日志层面确认触发时序（可观测性缺口）。

### 3.4 NEW-1 —— 重启节点的 region 归属视图发散（本轮最重要的新发现）

A 在 B1 场景被 kill 后重新拉起，15s 后采集：

| 节点 | r2 归属 | local |
|---|---|---|
| A | **node-24b6a076（A）** | true |
| B | node-cab8c15a（C） | false |
| C | node-cab8c15a（C） | true |

A 日志给出直接原因：

```
00:23:34 sql: node up node-cab8c15a rejoin region 2 failed: cluster: push failed: sql: stale region epoch 1 < known 2 (rid=2)
00:23:34 sql: node up node-cab8c15a route synced
```

即：`ASSIGN r2 -> C` 在 A/B 侧把 known epoch 抬到 2，而 C 本地仍为 epoch 1；A 重启后向 C 拉取 r2 视图时被"epoch 1 < known 2"判为 stale 而拒绝应用，A 保留本地 r2→A 的旧归属，形成**同一 region 双主**。

跨端写探针（id 70/71/72 均落在 r2 键范围）：

| 节点 | 写入 | 写入响应 | 该端 COUNT/SUM | 该端可见的探针 id |
|---|---|---|---|---|
| A | id=70 | 成功 | 13 / 222 | **70** |
| B | id=71 | 成功（转发 r2 owner=C） | 14 / 295 | **71, 72** |
| C | id=72 | 成功 | 14 / 295 | **71, 72** |

→ A 看不到 71/72，B/C 看不到 70：**同一 region 出现两份互不可见的数据，写入均返回成功**。quorum=on 未能拦截，因为 quorum 只统计可达节点数（3/3 可达），不校验 region 归属 epoch。

二次重启 A 后视图收敛（r2→C，local=false），三端均为 14 行 / 295，但 **A 侧 id=70 静默消失**（既未合并、也未报错）：

```
after_recover: A 14/295  B 14/295  C 14/295
probe_ids:     A 71,72   B 71,72   C 71,72
```

**结论**：这是 D4.5 尚未闭环的第 4 类根因——**重启节点 rejoin 时，权威端 epoch 低于本地 known epoch 即拒绝收敛，导致视图发散 → 双主 → 收敛期静默丢数据**。候选 C 的修复（`529e34c` 强制从权威源刷新）未覆盖该分支。

### 3.5 NEW-2 —— `persisted.json` 中的 ha_* 键导致节点无法启动

复测初期对 A 执行 `SET PERSIST ha_epoch_enabled=on` 后重启，进程直接退出：

进程退出前输出（摘录关键行）：

```
... ha_epoch_enabled requires cluster HA configuration ...
```

`/data/d43-node1/persisted.json` 内容为 `{"ha_epoch_enabled":"on","ha_quorum_enabled":"on"}`。剔除 ha_* 键后可正常启动（脚本已内置 `_sanitize_persisted()` 规避）。

**结论**：持久化配置的 apply 时机早于集群 HA 装配，一旦 ha_* 被 PERSIST，节点重启即不可用。这是 quorum 默认开启的硬前置缺陷（若默认 on 且被 PERSIST，故障重启会放大为启动失败）。

---

## 4. 对照 §8 收口条件

`docs/D4_5_ha_quorum_gray_report.md` §8 规定：三项 GAP 全部转 PASS 后方可转 v1.0、并把 D4.4 矩阵三项改写为 PASS。

| §8 要求 | 本轮复测状态 | 是否满足 |
|---|---|---|
| T3 集群轨 fencing 闭环 | 孤立节点不升主 PASS；真分区多数派可写未验证；fencing 未成为首道拦截 | ✗ |
| B2 TSO 单点消除 | 源码已实现，生产未接线，实测 TSO 主宕机写全失败 | ✗ |
| B1 单副本可恢复 | 存活副本节点可读 PASS；无副本节点读不可用、无自动接管、无日志 | ✗ |
| 附加前提：故障重启可用 | `SET PERSIST ha_*` 后重启启动失败 | ✗ |
| 附加前提：路由视图收敛 | 重启节点双主 + 收敛丢数据 | ✗ |

**因此：三项 GAP 均未全部转 PASS，`ha.quorumEnabled` 不满足默认开启条件，维持默认 off；本文档须继续维持 v0.2 草案状态。**

---

## 5. 建议（本轮不实施，仅提出）

1. **rejoin 收敛策略**：权威端 epoch < 本地 known epoch 时，不应简单判 stale 拒绝，需区分"本地视图陈旧"与"权威视图陈旧"；可引入集群级单调 region epoch 计数器，彻底消除两侧 epoch 不可比的问题。
2. **写路径按权威路由校验**：`WriteFence` 增加 region 归属 epoch 校验（以权威路由为准而非本地路由），避免本地视图发散期间仍接受写。
3. **TSO HA 接线**：新增启动参数（如 `--tso-ha`）并在 `server.go` 装配期调用 `db.EnableTSOHA`，把 group/load/save 与 `tso-node` 打通。
4. **持久化顺序**：把 ha_* 等依赖集群装配的键推迟到集群就绪后 apply，或在加载阶段降级为"忽略并告警"而非启动失败。
5. **B1 可观测性**：补齐/接管动作补结构化日志；读路径在 owner DOWN 时对"无本地副本"节点做转发或明确降级报错。
6. **T3 真分区验证**：为三节点分配独立 IP（`ip addr add` + 重新 ADD NODE 组网），方可构造"A/C 多数派可写 + B 少数派被 fence"的真实分区拓扑。

---

## 6. 复现方式

```bash
python3 tests/bench/bench_data/d45/run_d45_quorum_ha.py --stage preflight
python3 tests/bench/bench_data/d45/run_d45_quorum_ha.py --stage s2-t3-partition
python3 tests/bench/bench_data/d45/run_d45_quorum_ha.py --stage cleanup-partition
python3 tests/bench/bench_data/d45/run_d45_quorum_ha.py --stage s3-b2-tso
python3 tests/bench/bench_data/d45/run_d45_quorum_ha.py --stage s4-b1-owner
python3 tests/bench/bench_data/d45/run_d45_quorum_ha.py --stage s5-route-split
python3 tests/bench/bench_data/d45/run_d45_quorum_ha.py --stage s5-recover
python3 tests/bench/bench_data/d45/run_d45_quorum_ha.py --stage postcheck
```

各 stage 独立输出 JSON，可直接作为后续报告的数据源。
*（内容由AI生成，仅供参考）*

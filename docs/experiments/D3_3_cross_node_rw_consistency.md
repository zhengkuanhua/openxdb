---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_a3d33c00ffee00112233445566778899
    ReservedCode1: ZRgTb7zW0fUlcjh61ilsI5y3dORzkqSP7YCUEAH05g5mXNFHtg3wmn5x95r5HNAZ56cW+6D7Yd5MigaJw3DbYLTpfuLTpeyp+T6i/0qIwxUWE+uu7uGhn/62Lyqv71mgaaC5hrnOefGyjMgaiRl5urCUY11mGU6K3VGDlRYZhLqBu3yy23sy7g3nA7U=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_a3d33c00ffee00112233445566778899
    ReservedCode2: ZRgTb7zW0fUlcjh61ilsI5y3dORzkqSP7YCUEAH05g5mXNFHtg3wmn5x95r5HNAZ56cW+6D7Yd5MigaJw3DbYLTpfuLTpeyp+T6i/0qIwxUWE+uu7uGhn/62Lyqv71mgaaC5hrnOefGyjMgaiRl5urCUY11mGU6K3VGDlRYZhLqBu3yy23sy7g3nA7U=
---



# D3.3 跨节点读写一致性专项验证（v1.18.0 D3 Phase A）

- 实验编号：D3.3（跨节点读写一致性专项验证，v1.18.0 D3 Phase A）
- 日期：2026-11-02
- 状态：**验证通过（41 PASS / 0 FAIL / 0 GAP）**；commit A3 收口
- 定位：基于 D3.2 三节点分布成果，验证 node 间点查/COUNT/scan 互读、COUNT/SUM 聚合不丢不重、2PC 远端写落归属节点；针对 D3.2 缺口 G1（COUNT 快速路径仅本地计数 executor.go:4383）与 G2（节点注册表不互广播 cluster.go:143 / put.go:112）评估并落地修复；验证暴露的跨节点版本可见性边界（tsoClient 批量缓存滞后）同步修复
- 依据：`DEV_PLAN_v1.18.0.md` §3 D3.3、§4 风险与回退；`D3_1_data_placement_strategy.md`（选型）、`D3_2_implicit_region_assign.md`（三节点分布基线，含 G1-G6 缺口记录）；`T13_m4_multinode.md`（M4 跨节点读）、`T14_m5_2pc.md`（M5 2PC）、`T17_m8_dist_txn.md`（M8 TSO + 分布式快照）；风格对齐 `docs/experiments/` 系列
- 脚本/数据归档：`tests/bench/bench_data/d33/run_d33_cross_node_rw_consistency.ps1`（可重跑）、`run_d33_output.txt`（本次完整输出）；节点数据目录位于 `%TEMP%/openxdb_d33_<stamp>/`（RocksDB 不支持非 ASCII 路径，脚本以 ASCII 临时目录承载数据，结果归档到 d33/）

## 1. 验证目标与验收基准

D3.3 子项（DEV_PLAN §3）：基于 D3.2 三节点分布成果，验证 node 间点查/COUNT/scan 互读、COUNT/SUM 聚合不丢不重、2PC 远端写落归属节点；针对 D3.2 记录的缺口（G1 COUNT 快速路径仅本地计数、G2 注册表不互广播）评估并落地修复使跨节点 COUNT/聚合一致，或明确记录不可修复边界。

| 验收基准 | 断言 | 结果 |
|---|---|---|
| 注册表互广播（G2 修复） | A 上 ADD NODE B/C 后，B/C 上 SHOW NODES count=3（B/C 互相可见） | PASS |
| node 间点查互读（G1/G3/G5 修复回归） | B 点查 id=1（rA 归属 A）、C 点查 id=4（rB 归属 B）、B 点查 id=9（rC 归属 C）均可见 | PASS |
| node 间 COUNT 互读（G1 修复） | A/B/C 上 COUNT(*) t3 均 =9；COUNT-where =9 | PASS |
| node 间 scan 互读 | B/C 上 scan 1..9 合并有序 | PASS |
| COUNT/SUM 聚合不丢不重 | 27 行（A 插 1..9、B 插 10..18、C 插 19..27）：A/B/C COUNT=27、SUM=378、BETWEEN 4..6=3；B/C rC-range(id>=19)=9 | PASS |
| 2PC 远端写落归属节点 | A UPDATE id=5（rC 归属 C）走 2PC：A/B/C 点查均 v5x（C 本地无版本 gap）；C 本地 rC range(id>=7) count=21（物理落盘证据） | PASS |
| 多协调者 2PC + DELETE | B 协调 UPDATE id=8 → A/C 可见 v8x；A 协调 DELETE id=9 → A/B/C COUNT=26 | PASS |
| 跨节点版本可见性（G4 边界消除） | S6 矩阵：A 转发读 id=8、B 转发读 id=8、C 本地读 id=8/id=5 全部命中 | PASS |

## 2. 验证场拓扑与执行步骤

三进程组网（同机不同端口模拟三节点，A 为 M8 中心 TSO）：

| 节点 | 内部端口 | SQL 端口 | TSO 角色 | 数据目录 |
|---|---|---|---|---|
| node-a（A） | 17881 | 13316 | 权威发号（--tso-node 自指） | `%TEMP%/openxdb_d33_*/node-a` |
| node-b（B） | 17882 | 13317 | 远端 TSO 客户端（--tso-node A） | `%TEMP%/openxdb_d33_*/node-b` |
| node-c（C） | 17883 | 13318 | 远端 TSO 客户端（--tso-node A） | `%TEMP%/openxdb_d33_*/node-c` |

执行步骤（脚本内注释 S1/S2/S3/S4/S5/S6 分段）：

1. **组网**：三节点 init + 启动；A 先启并解析 node id 作为 B/C 的 `--tso-node`；A 上 `ADD NODE` 注册 B/C；`SHOW NODES` 在 A/B/C 三节点均确认 3 节点（G2 修复验证：注册表经 BroadcastRegistry 广播，B/C 互见）。
2. **建表 + SPLIT + ASSIGN**：A 建 `t3(id INT PK, v VARCHAR(64))` 并插 9 行；B/C 建同 schema（DDL 广播，already exists 断言）；两次 `SPLIT REGION` 切出 rA/rB/rC；`ASSIGN REGION rB TO 'B'`、`ASSIGN REGION rC TO 'C'`；三节点路由表均学习 3 行。
3. **S3 node 间互读**：B 点查 id=1（远端 rA）、C 点查 id=4（远端 rB，G3 修复回归）、B 点查 id=9（远端 rC，G5 修复回归）、C 点查 id=5（本地 rC）；A/B/C 上 `COUNT(*)` 与 `COUNT(*) WHERE` 均 =9（G1 修复：tryCountFastPath 集群感知，回退 scanMergedCluster 跨节点合并计数）；B/C scan 1..9 合并有序。
4. **S4 聚合不丢不重**：B 插 10..18、C 插 19..27（共 27 行）；A/B/C 上 `COUNT(*)=27`、`SUM(id)=378`、`COUNT(*) WHERE id BETWEEN 4 AND 6 =3`；B/C 上 `COUNT(*) WHERE id>=19`（rC 段）=9。
5. **S5 2PC 远端写**：A 上 `UPDATE id=5`（rC 远端，M5 2PC）→ A/B/C 点查均 v5x；C 本地 rC range(id>=7) count=21（2PC 写入真实落 C 盘）；B 协调 `UPDATE id=8` → A/C 可见 v8x；A 协调 `DELETE id=9` → A/B/C COUNT=26。
6. **S6 G4 可见性矩阵**：A 转发读 id=8、B 转发读 id=8、C 本地读 id=8 与 id=5 全部命中（此前 C 本地读 0 row / 计数少 2 的版本可见性缺口已消除）。

## 3. 结果汇总

```
SUMMARY pass=41 fail=0 gap=0
```

修复前基线（脚本修正后、D3.3 修复落地前）：37 PASS / 2 FAIL / 2 GAP。
- FAIL：B 点查 id=5 0 row、B COUNT=25（应为 26）
- GAP：C 点查 id=5 0 row、C COUNT=25

修复落地后重跑：上述 FAIL/GAP 全部转为 PASS，其余 37 项保持 PASS，合计 41 PASS / 0 FAIL / 0 GAP。

关键证据（S5/S6）：

```
C point id=5 = v5x (local read, no version gap)   [PASS]
C local rC range count=21 (2PC write landed on owner) [PASS]
A/B COUNT=26 after remote delete (no-loss-no-dup) [PASS]
C COUNT=26 after 2PC delete                       [PASS]
G4 matrix: A forward id=8=v8x / B forward id=8=v8x / C local id=8=v8x / C local id=5=v5x
```

## 4. 修复落地（本 commit 落码）

### 4.1 D3.2 缺口 G1：COUNT 快速路径仅本地计数（executor.go:4383）

**定位**：`tryCountFastPath` 对无 WHERE/GROUP/HAVING 的 `COUNT(*)/COUNT(1)` 走本地 `CountRange` 直读，未并入跨节点合并，B/C 上 COUNT 只数本地 region（D3.2 实测 B=2、C=5）。

**修复**：`tryCountFastPath` 增加 `clusterEnabled()` 集群感知——非本地 region 存在时快速路径返回 `(nil,false,nil)`，回退 `scanMergedCluster` 跨节点合并计数。单机（clusterEnabled=false）行为零变化，保留快速路径。

### 4.2 D3.2 缺口 G2：节点注册表不互广播（cluster.go:143 / put.go:112）

**定位**：`AddNode` 仅向发起节点注册；`BroadcastRegions` 只广播路由不携带注册表，B/C 未互相注册，物化扫描/转发报 `node not registered`（D3.2 实测 G3/G5 同源）。

**修复**（三文件）：
- `pkg/cluster/cluster.go`：新增 `BroadcastRegistry`（遍历 `ListNodes` 跳过 SelfID，向每个 peer 调 `syncRegistry` 推送全量节点注册表）、`RegisterPeers`（批量注册 peer 节点）。
- `pkg/cluster/server.go`：新增 `handleRegistrySync` 处理注册表同步帧（调 `RegisterPeers` + `persistNodes` 持久化），`handleConn` 增 `msgRegistrySync` 分发。
- `pkg/cluster/put.go`：帧号 32/33（REGISTRY_SYNC/OK），避开 2PC(22-27)/TSO(28-29)/DDL(30-31) 占用；`syncRegistry` 短连接发送。
- `pkg/sql/multinode.go`：`execAddNode` 注册成功后调用 `BroadcastRegistry` 广播。
- 效果：A 上 ADD NODE 后 B/C 立即互见完整拓扑（S1 断言 PASS）。

### 4.3 D3.3 验证暴露缺口 G4 衍生：tsoClient 批量缓存致快照号滞后

**定位**：B/C 挂远端 TSO（`pkg/cluster/tso.go` `tsoClient`），`GetBatch(batch=64)` 缓存充足时消费本地批、不向 A 拉新号；A 上 2PC commitTS 取自同一发号器更高号 → B/C 下一次读快照 `beginTS` 滞后于 commitTS，`VersionVisible` 过滤后 0 row（FAIL/GAP 根因）。`snapshotTS` autocommit 分支（executor.go:1264）此前直接 `e.ts.Get()` 消费缓存号。

**修复**（`pkg/sql/executor.go`，两处）：
1. **autocommit 取号前 Reset**：`snapshotTS` autocommit 分支在 `e.ts.Get()` 前调用 `e.ts.Reset()`，丢弃残余预取批次后向权威 TSO 重新拉批——与显式事务 M18 既有模式一致。每条 autocommit 读语句真正取号时都拿到权威序列当前新鲜号，保证 ≥ 此前任何 2PC commitTS，跨节点可见。
2. **集群模式禁用 C6 共享快照窗口**：`stmtROShareableLocked` 增加 `clusterEnabled()` 判断（false），并同步在 `resetStmtSnapshot` 入口条件加 `!e.clusterEnabled()`——共享窗口过期检测依赖本地 `WriteSeq`（storage.WriteSeqProvider），无法感知远端节点 2PC 提交，跨节点复用窗口快照会导致远端写入对本地不可见。集群下恢复"逐语句独立快照"原语义（正确性优先，C6 优化仅限单机）。

**回归**：`go build ./pkg/sql/... ./pkg/cluster/...` 通过；`go test ./pkg/sql/...`（84.6s）与 `./pkg/cluster/...` 全部 ok。

## 5. 结论与边界

- **结论**：D3.3 专项验证全部通过（41 PASS / 0 FAIL / 0 GAP）。node 间点查/COUNT/scan 互读一致；COUNT/SUM 聚合跨三节点不丢不重（27 行 SUM=378）；2PC 远端写真实落归属节点（C 本地物理行数证据）且 A/B/C 三视角可见；A/B 双协调者 2PC 与远端 DELETE 后 COUNT 一致。
- **D3.2 缺口闭环**：G1（COUNT 跨节点）、G2（注册表广播，连带 G3/G5）修复落地并验证；G6（源节点保留副本的本地 COUNT 语义）为设计语义，文档化边界，维持 D3.1 §4 回退约定。
- **跨节点版本可见性（G4）已消除**：根因为 TSO 客户端批量缓存滞后而非 2PC 本身，修复后 C 本地读与 A/B 转发读完全一致，不再属于不可修复边界。
- **已知边界/开销**：
  - 每条 autocommit 跨节点读语句取号前 Reset 触发一次 TSO RTT（batch=64 拉批仅消费 1 号），读路径额外一次内部往返；单机（本地 TSO）Reset 后 Get 从权威序列续号，行为一致、无额外网络开销。优化方向（后续簇）：远端 TSO 客户端按需拉小批或引入"缓存批有效期/权威水位"校验，减少 RTT 次数。
  - 集群模式禁用 C6 快照共享窗口：跨节点连续只读语句不再共享 RocksDB 快照，每条语句独立取号 + 独立临时事务，牺牲该优化换取跨节点强一致读语义。
- **回退路径**（DEV_PLAN §4）：修复全部可逆——G1/G2/D3.3 修复均在 executor/cluster 路径内，单机行为（clusterEnabled=false）零变化；如后续发现跨节点一致性问题，仍可保留源节点副本，ASSIGN 路由逐 region 撤销重指派回源。

*（内容由AI生成，仅供参考）*

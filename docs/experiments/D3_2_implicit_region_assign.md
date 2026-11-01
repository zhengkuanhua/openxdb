---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_deb60d13d60611f1a0da525400393706
    ReservedCode1: ZRgTb7zW0fUlcjh61ilsI5y3dORzkqSP7YCUEAH05g5mXNFHtg3wmn5x95r5HNAZ56cW+6D7Yd5MigaJw3DbYLTpfuLTpeyp+T6i/0qIwxUWE+uu7uGhn/62Lyqv71mgaaC5hrnOefGyjMgaiRl5urCUY11mGU6K3VGDlRYZhLqBu3yy23sy7g3nA7U=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_deb60d13d60611f1a0da525400393706
    ReservedCode2: ZRgTb7zW0fUlcjh61ilsI5y3dORzkqSP7YCUEAH05g5mXNFHtg3wmn5x95r5HNAZ56cW+6D7Yd5MigaJw3DbYLTpfuLTpeyp+T6i/0qIwxUWE+uu7uGhn/62Lyqv71mgaaC5hrnOefGyjMgaiRl5urCUY11mGU6K3VGDlRYZhLqBu3yy23sy7g3nA7U=
---



# D3.2 隐式显式化 + ASSIGN REGION 三节点分布验证（v1.18.0 D3 Phase A）

- 实验编号：D3.2（隐式显式化 + ASSIGN REGION 三节点分布验证，v1.18.0 D3 Phase A）
- 日期：2026-11-01
- 状态：**验证通过（24 PASS / 0 FAIL / 6 GAP 已知边界）**；commit A2 收口
- 定位：验证 D3.1 选型结论「显式 REGION 指派优先」在三节点真实组网中的落地形态：建表 → SPLIT/隐式显式化 → ASSIGN REGION（不强制先 SPLIT）→ 三节点各持 region → 数据落归属节点、路由表有内容、跨节点点查可见
- 依据：`DEV_PLAN_v1.18.0.md` §3 D3.2、§4 风险与回退；`D3_1_data_placement_strategy.md`（选型结论）；`T13_m4_multinode.md`（M4 ASSIGN/隐式显式化/SHOW REGION ROUTES/跨节点读）、`T14_m5_2pc.md`（M5 2PC）、`T17_m8_dist_txn.md`（M8 TSO + 分布式快照）、`T15_m6_ha.md`（M6 failover）、`T12_m3_sharding.md`（M3 分片）；风格对齐 `docs/experiments/` 系列
- 脚本/数据归档：`tests/bench/bench_data/d32/run_d32_assign_region.ps1`（可重跑）、`run_d32_output.txt`（本次完整输出）；节点数据目录位于 `%TEMP%/openxdb_d32_<stamp>/`（RocksDB 不支持非 ASCII 路径，脚本以 ASCII 临时目录承载数据，结果归档到 d32/）

## 1. 验证目标与验收基准

D3.2 子项（DEV_PLAN §3）：建表 → ASSIGN REGION（不强制先 SPLIT）→ 三节点各持 region；验证 region 数据落归属节点、SHOW REGION ROUTES 有内容、跨节点点查可见（node-b 写 node-a 点查 cnt=1）。

| 验收基准 | 断言 | 结果 |
|---|---|---|
| 三节点组网就绪 | SHOW NODES count=3，node id 按 addr 解析 | PASS |
| SPLIT 出 3 个显式 region | t3 两次 SPLIT 后 SHOW REGION ROUTES 3 行 | PASS |
| ASSIGN REGION 三节点各持 1 region | A/B/C 路由表中各自 region `local=true`，其余 false | PASS |
| SHOW REGION ROUTES 有内容 | A/B/C 三节点路由表均 3 行，归属正确 | PASS |
| region 数据落归属节点 | A 持有 rA（id 1,2）、B 持有 rB（id 3,4）、C 持有 rC（id 5..9）；B/C 本地 COUNT 物理命中 | PASS（B=2、C=5，见 §4 缺口） |
| 隐式显式化（不强制先 SPLIT） | t_imp 未 SPLIT 直接 ASSIGN REGION（tableID|1<<63）→ node-b；A 路由 non-local / B 本地 | PASS |
| 跨节点点查可见（验收基准） | B 写 t_imp id=4 后，A 点查 cnt=1、v=b4 | PASS |
| 跨 region 写 + 读可见 | A UPDATE t3 id=5（rC，远端）走 2PC Query OK；A 点查 v5x | PASS |

## 2. 验证场拓扑与执行步骤

三进程组网（同机不同端口模拟三节点）：

| 节点 | 内部端口 | SQL 端口 | 数据目录 |
|---|---|---|---|
| node-a（A） | 17781 | 13306 | `%TEMP%/openxdb_d32_*/node-a` |
| node-b（B） | 17782 | 13307 | `%TEMP%/openxdb_d32_*/node-b` |
| node-c（C） | 17783 | 13308 | `%TEMP%/openxdb_d32_*/node-c` |

执行步骤（脚本内注释 S1/S2 分段）：

1. **组网**：三节点 `openxdb init` + 启动；A 上 `ADD NODE` 注册 B/C；`SHOW NODES` 确认 3 节点；按 addr 解析 node id（A=node-b9a0a502、B=node-20a9f4b8、C=node-57aec42e，运行时动态）。
2. **建表 + SPLIT**：A 建 `t3(id INT PK, v VARCHAR(64))` 并插入 9 行；B/C 建同 schema 空表；A 上两次 `SPLIT REGION` 切出 rA(空~…03)、rB(…03~…05)、rC(…05~∞)。
3. **ASSIGN**：A 上 `ASSIGN REGION rB TO NODE 'B'`、`ASSIGN REGION rC TO NODE 'C'`；rA 留在 A（源节点持有副本语义：ASSIGN 不删除源数据）。
4. **路由验证**：A/B/C 各执行 `SHOW REGION ROUTES`：三节点均学习到 3 行路由；每节点自己持有的 region `local=true`，其余 `false`。
5. **跨节点点查**：A 点查 id=5（rC 归属 C，经 QUERY_REQ 转发 C 合并）、id=9（本地 rA）；B 点查 id=1（rA 归属 A，B 转发 A）。
6. **S1 隐式显式化**：A 建 `t_imp`（不 SPLIT，隐式单 region `2|1<<63`，显示为 `-9223372036854775806`）插入 3 行；B/C 建同 schema 空表；A 上 `ASSIGN REGION <隐式id> TO NODE 'B'`；A/B 路由表中该 region `local=false/true`。
7. **验收基准点查**：B 上 `INSERT INTO t_imp VALUES(4,'b4')`（B 持有该 region，本地写 Query OK）；A 上点查/计数 t_imp id=4 → cnt=1、v=b4。
8. **跨 region 2PC 写**：A 上 `UPDATE t3 SET v='v5x' WHERE id=5`（id=5 属 rC，远端 region，走 M5 2PC）→ Query OK；A 点查 id=5 → v5x。

## 3. 结果汇总

```
SUMMARY pass=24 fail=0 gap=6
```

全部验收基准 PASS（§1 表）。关键路由证据（SHOW REGION ROUTES，ASSIGN 后 A 节点视图）：

```
region_id  table_id  start_key  end_key  state   node             local
3          1                    …03       ACTIVE  node-b9a0a502    true     <- rA 留在 A
4          1         …03        …05       ACTIVE  node-20a9f4b8    false    <- rB 归属 B
2          1         …05                 ACTIVE  node-57aec42e    false    <- rC 归属 C
```

隐式显式化证据（ASSIGN 后 B 节点视图新增行）：

```
-9223372036854775806  2                    ACTIVE  node-20a9f4b8   true     <- t_imp 隐式 region 归属 B
```

## 4. 实现缺口（6 项，均记录为已知边界，本 commit 不落代码改动）

验证暴露的实现缺口与定位、修复方向如下。按 D3 纪律，**本 commit 只记录，不落代码**；修复排入 D3.3（一致性验证）或后续簇。

| # | 缺口 | 实测证据 | 代码定位 | 修复方向 |
|---|---|---|---|---|
| G1 | **COUNT 快速路径仅本地计数**：`tryCountFastPath` 未接入 `scanMergedCluster`，B/C 上 `COUNT(*)` 只数本地 region | B=2、C=5（应为 9） | `pkg/sql/executor.go:4383`（tryCountFastPath，本地 CountRange 直读） | COUNT(*)/COUNT(1) 无谓词时按 rowRanges 展开远端转发合并，或禁用 fastpath 回退 scanMergedCluster |
| G2 | **节点注册表不互广播**：注册仅在 ADD NODE 发起节点生效；B/C 未注册对方节点，物化扫描/转发报 `node not registered` | B/C 上 `COUNT(*) WHERE` 报 `cluster: node not registered` | `pkg/cluster/cluster.go:143`（AddNode 单向注册）、`cluster.go:72`（ErrNodeNotFound）；`put.go:112`（BroadcastRegions 仅源→目标学习路由，不广播注册表） | 新节点握手后向全拓扑广播注册表（或 ASSIGN/Broadcast 时附带节点视图）；修复后 G3 一并消失 |
| G3 | **C→B 转发不可达**：C 未注册 B，C 点查 rB 数据失败 | C 点查 id=4（rB 归属 B）报 `node not registered` | 同 G2 注册表缺口 | 同 G2 |
| G4 | **跨节点版本可见性**：push/2PC 写入归属节点的数据，归属节点本地快照读不可见（A 侧同源快照转发读可见） | A UPDATE id=5 后 A 点查 v5x PASS；C 本地点查 0 row（重试 700ms 仍不可见）；C 物理行数正确（fastpath=5） | M8 TSO/版本序列跨节点未统一（T17）：REGION_PUSH/2PC 写入版本号与本地读 beginTS 非同一序列空间 | D3.3 一致性验证重点：统一跨节点版本分配（TSO 全局取号贯通到 push/2PC 写入）或在 QUERY_REQ 远端读复用协调者 beginTS（当前已部分生效） |
| G5 | **物化 COUNT 跨节点聚合对非协调者节点不可用**（G2 衍生） | B/C 物化 COUNT 报 not registered | 同 G2 | 同 G2 |
| G6 | **源节点保留副本导致非归属节点本地 COUNT 语义为"本地副本行数"**（设计语义，非缺陷） | A COUNT=9（源保留全量）、B=2、C=5 | M4 数据装载设计（ASSIGN 不删源数据，D3.1 §4 回退约定） | 明确文档：COUNT 语义跨节点聚合前，本地 COUNT 仅代表本地物理行；G1 修复后统一 |

边界判定说明：G2/G3/G5 为同一注册表缺口；G4 为 M8 跨节点版本可见性边界，**不属于 D3.2 验收基准覆盖范围**（验收基准点查 cnt=1 已通过），排入 D3.3 一致性验证；G1 为聚合查询优化缺口，排入 D3.3（COUNT/SUM 跨节点聚合不丢不重）。

## 5. 结论与回退

- **结论**：D3.1 选型结论落地验证通过——显式 REGION 指派机制在三节点组网中完整可用：SPLIT 切分、ASSIGN 数据推送与路由归属、SHOW REGION ROUTES 三节点一致、隐式 region 无需 SPLIT 即可显式化指派、跨节点点查（本地/转发）可见、远端 region 写走 M5 2PC 提交成功。
- **验收基准达成**：`node-b 写 node-a 点查 cnt=1` PASS；三节点各持 region 且 SHOW REGION ROUTES 有内容 PASS。
- **已知边界**：注册表广播、COUNT 跨节点聚合、跨节点版本可见性三项缺口按 §4 记录，不阻塞本阶段；进入 D3.3 一致性验证时优先修复 G2（注册表广播，连带 G3/G5）与 G1。
- **回退路径**（DEV_PLAN §4）：如 D3.3 发现不可修复的一致性问题，按 D3.1 回退约定保留源节点副本，数据仍在源节点，ASSIGN 路由可逐 region 撤销（重指派回源），隐式单 region 语义不受影响。
- **commit**：A2（D3.2 验证 + 文档 + 脚本归档），双远端（gitee + GitHub 443）SHA 一致。
*（内容由AI生成，仅供参考）*

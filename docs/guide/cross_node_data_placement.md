---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_a4d33c00ffee00112233445566778899
    ReservedCode1: ZRgTb7zW0fUlcjh61ilsI5y3dORzkqSP7YCUEAH05g5mXNFHtg3wmn5x95r5HNAZ56cW+6D7Yd5MigaJw3DbYLTpfuLTpeyp+T6i/0qIwxUWE+uu7uGhn/62Lyqv71mgaaC5hrnOefGyjMgaiRl5urCUY11mGU6K3VGDlRYZhLqBu3yy23sy7g3nA7U=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_a4d33c00ffee00112233445566778899
    ReservedCode2: ZRgTb7zW0fUlcjh61ilsI5y3dORzkqSP7YCUEAH05g5mXNFHtg3wmn5x95r5HNAZ56cW+6D7Yd5MigaJw3DbYLTpfuLTpeyp+T6i/0qIwxUWE+uu7uGhn/62Lyqv71mgaaC5hrnOefGyjMgaiRl5urCUY11mGU6K3VGDlRYZhLqBu3yy23sy7g3nA7U=
---



# v1.18.0 跨节点数据分布用户指南（Cross-Node Data Placement Guide）

- 文档类型：用户操作指南（面向使用方 / HA 灰度复用）
- 版本：v1.18.0（D3.4）
- 日期：2026-11-02
- 状态：**正式指南**，基于 D3.1 选型结论 + D3.2 三节点分布验证（24P/0F/6G）+ D3.3 跨节点一致性验证（41P/0F/0G）编写
- 定位：指导用户在多节点组网下使用**显式 REGION 指派**完成跨节点数据分布，并正确理解 node 间读写 / 聚合 / 2PC 远端写行为与已知边界
- 依据：`../experiments/D3_1_data_placement_strategy.md`（选型）、`../experiments/D3_2_implicit_region_assign.md`（分布操作实测）、`../experiments/D3_3_cross_node_rw_consistency.md`（一致性验证与修复）；`DEV_PLAN_v1.18.0.md` §3 D3、§4 风险与回退

---

## 1. 概述

跨节点数据分布是指把一张表的数据按 **REGION（区域）** 粒度分布到多个数据库节点上，使不同节点各自持有部分数据，从而获得容量线性扩展与 region 粒度的故障隔离能力。

v1.18.0 采用**显式 REGION 指派优先**的选型结论（见 `../experiments/D3_1_data_placement_strategy.md`）：

- 数据按 **REGION** 逻辑切分（`SPLIT REGION`），每个 region 有明确的行键范围（start_key ~ end_key）；
- 通过 **`ASSIGN REGION ... TO NODE`** 把 region 显式指派给指定节点，region 数据推送到归属节点；
- 各节点通过 **`SHOW REGION ROUTES`** 学习全局路由表，跨节点读写自动转发/合并；
- 广播表（全 region 全节点副本）列为后续版本增强，本版本不提供。

本指南覆盖：组网前置条件（§2）、显式 REGION 指派完整流程（§3）、node 间读写与聚合一致性用法（§4）、2PC 远端写行为（§5）、已知边界与限制（§6）、回退路径（§7）。

---

## 2. 前置条件：三节点组网

跨节点数据分布要求多节点组网就绪。以三节点（A/B/C）为例：

| 节点 | 角色 | 说明 |
|---|---|---|
| node-a（A） | TSO 权威发号节点 | 启动参数 `--tso-node` 指向自身 |
| node-b（B） | 远端 TSO 客户端 | 启动参数 `--tso-node A` |
| node-c（C） | 远端 TSO 客户端 | 启动参数 `--tso-node A` |

部署步骤：

1. 各节点 `openxdb init` 初始化数据目录后启动（节点数据目录请使用 ASCII 路径，RocksDB 不支持非 ASCII 路径）；
2. 在 A 上执行 `ADD NODE` 注册 B/C 节点；注册后注册表会经广播同步到全部节点；
3. 在任一节点执行 `SHOW NODES` 确认拓扑，应看到 count=3 且 B/C 互相可见（D3.3 G2 修复后注册表互广播）。

```sql
SHOW NODES;
-- 预期：3 节点，node id 形如 node-b9a0a502 / node-20a9f4b8 / node-57aec42e
```

> 提示：后续 `ASSIGN REGION ... TO NODE` 中的节点名使用 `SHOW NODES` 解析出的 node id。

---

## 3. 显式 REGION 指派（核心操作）

完整流程：**建表 → SPLIT REGION → ASSIGN REGION TO NODE → SHOW REGION ROUTES 验证**。

### 3.1 建表

在任意节点建表，并在各节点建立同 schema 空表（DDL 会广播）：

```sql
CREATE TABLE t3(id INT PRIMARY KEY, v VARCHAR(64));
```

### 3.2 SPLIT REGION 切分

在持有源数据的节点上执行两次 `SPLIT REGION`，把表切为 3 个显式 region（rA / rB / rC）：

```sql
SPLIT REGION;            -- 按行键范围切出第 1 个边界
SPLIT REGION;            -- 再切出第 2 个边界，得到 rA(空~…03)、rB(…03~…05)、rC(…05~∞)
```

### 3.3 ASSIGN REGION 指派

把 region 显式指派给目标节点（**ASSIGN 不删除源节点副本**，源节点保留数据，见 §6.1）：

```sql
ASSIGN REGION rB TO NODE 'node-20a9f4b8';  -- rB 归属 B
ASSIGN REGION rC TO NODE 'node-57aec42e';  -- rC 归属 C
-- rA 留在 A
```

### 3.4 SHOW REGION ROUTES 验证

在各节点执行 `SHOW REGION ROUTES`，三节点应学习到一致的 3 行路由；每节点自己持有的 region `local=true`，其余为 `false`：

```
region_id  table_id  start_key  end_key  state   node             local
3          1                    …03       ACTIVE  node-b9a0a502    true     <- rA 留在 A
4          1         …03        …05       ACTIVE  node-20a9f4b8    false    <- rB 归属 B
2          1         …05                 ACTIVE  node-57aec42e    false    <- rC 归属 C
```

### 3.5 隐式显式化（不强制先 SPLIT）

未 SPLIT 的表只有 1 个隐式 region（显示为 `-9223372036854775806`），可直接 `ASSIGN` 指派，无需先切分：

```sql
ASSIGN REGION <隐式region id> TO NODE 'node-20a9f4b8';  -- t_imp 隐式 region 归属 B
```

指派后 B 节点 `SHOW REGION ROUTES` 出现新行：该 region `local=true`，A 节点视图为 `local=false`。指派后在该 region 上写入的数据落归属节点（B 本地写 Query OK），其他节点可跨节点读取可见。

---

## 4. node 间读写与聚合一致性用法

以下用法均已通过 D3.3 专项验证（41 PASS / 0 FAIL / 0 GAP）。

### 4.1 点查互读（自动转发）

任意节点点查任意 region 的数据，本地无该 region 时自动转发到归属节点合并返回：

```sql
-- A 点查 id=5（rC 归属 C，转发 C 合并）-> 可见
-- B 点查 id=1（rA 归属 A，转发 A）-> 可见
-- C 点查 id=4（rB 归属 B）-> 可见
SELECT * FROM t3 WHERE id=5;
```

### 4.2 聚合查询（COUNT / SUM，跨节点不丢不重）

`COUNT(*)` / `COUNT(1)` 无谓词查询在集群模式下自动回退跨节点合并计数，三节点结果一致；`SUM`、带 `WHERE` 的 COUNT 同理：

```sql
-- 三节点插入共 27 行后：
SELECT COUNT(*) FROM t3;   -- A/B/C 均 = 27
SELECT SUM(id)  FROM t3;   -- A/B/C 均 = 378
SELECT COUNT(*) FROM t3 WHERE id BETWEEN 4 AND 6;  -- = 3
SELECT COUNT(*) FROM t3 WHERE id >= 19;            -- rC 段 = 9
```

### 4.3 范围扫描（跨节点合并有序）

```sql
SELECT * FROM t3 WHERE id BETWEEN 1 AND 9;  -- B/C 上执行，结果跨节点合并且有序
```

### 4.4 多节点写入

各节点均可向本表写入，数据按行键落入对应 region 的归属节点：

```sql
-- A 插入 1..9、B 插入 10..18、C 插入 19..27
-- 最终任意节点 COUNT(*)=27、SUM(id)=378
```

---

## 5. 2PC 远端写行为

当写操作（`UPDATE` / `DELETE` / `INSERT`）命中的行键属于**远端 region** 时，自动走两阶段提交（2PC，M5）：

- **协调者可任意节点**：A 上 `UPDATE id=5`（rC 归属 C）、B 上 `UPDATE id=8`、A 上 `DELETE id=9` 均提交成功；
- **写入真实落归属节点**：2PC 提交后数据物理写入归属节点（D3.3 实测 C 本地 rC 段物理行数=21）；
- **三视角可见**：提交后 A/B/C 任意节点点查均能看到最新值，无版本缺口（D3.3 S6 可见性矩阵全部命中）。

示例：

```sql
UPDATE t3 SET v='v5x' WHERE id=5;   -- id=5 属 rC（远端），走 2PC，Query OK
SELECT v FROM t3 WHERE id=5;        -- A/B/C 三视角均 = v5x
DELETE FROM t3 WHERE id=9;          -- A/B/C COUNT 均 = 26
```

快照可见性语义：集群使用全局 TSO 取号，autocommit 读语句取号前会丢弃本地预取缓存批、向权威 TSO 拉取新鲜号，保证读快照 ≥ 此前任意 2PC 提交版本，跨节点读写强一致。

---

## 6. 已知边界与限制

| # | 边界 | 说明 | 影响与建议 |
|---|---|---|---|
| 6.1 | **ASSIGN 不删源节点副本** | region 指派后源节点仍保留物理数据副本（D3.1 §4 回退约定，D3.2 G6 设计语义） | 源节点磁盘占用包含冗余副本；聚合查询语义已统一（§4.2），本地物理行数不等于表总行数，勿以本地存储行数判断表规模 |
| 6.2 | **跨节点读多一次 TSO RTT** | 集群模式下每条 autocommit 读语句取号前 Reset 预取缓存并重新拉批，跨节点读路径额外一次内部往返 | 跨节点高频只读场景存在固定延迟开销；单机（本地 TSO）无此开销。后续版本优化远端 TSO 拉批策略 |
| 6.3 | **集群模式禁用 C6 快照共享窗口** | 跨节点连续只读语句不再共享 RocksDB 快照，每条语句独立取号 + 独立临时事务 | 正确性优先，换取跨节点强一致读；单机仍保留快照共享优化 |
| 6.4 | **广播表为后续版本** | 全 region 全节点副本的广播表（广播写协议）本版本不提供 | 需要"每节点都有全量数据"的场景暂不可用，可先用显式指派 + 源副本满足 |
| 6.5 | **节点数据目录需 ASCII 路径** | RocksDB 不支持非 ASCII 路径 | 部署时节点数据目录使用 ASCII 绝对路径，避免中文等路径导致启动/读写异常 |
| 6.6 | **多协调者写冲突保护沿用事务语义** | 2PC 跨节点写以全局 TSO 版本判定可见性，未提供跨节点行级锁等待语义 | 高并发写同一远端行时以快照隔离语义为准，应用侧按常规事务冲突处理 |

---

## 7. 回退路径

显式指派是**可回退**的管理操作：

- ASSIGN 可逐 region 撤销并重指派回源节点，撤销后恢复为跨节点不可见的旧行为；
- 由于 ASSIGN 不删源数据，撤销/回退全程**数据不丢**；
- 如后续发现跨节点一致性问题，可按 D3.1 §4 回退约定保留源节点副本、撤除 region 路由即可回退到隐式单 region 语义。

---

## 8. 参考文档

- `../experiments/D3_1_data_placement_strategy.md` —— 数据分布策略选型与回退约定
- `../experiments/D3_2_implicit_region_assign.md` —— 三节点 ASSIGN REGION 分布验证（含 G1-G6 缺口记录）
- `../experiments/D3_3_cross_node_rw_consistency.md` —— 跨节点读写一致性验证与修复（41P/0F/0G）
- `DEV_PLAN_v1.18.0.md` —— 版本排期、风险与回退（§3 D3、§4）
- 测试脚本归档：`tests/bench/bench_data/d32/`、`tests/bench/bench_data/d33/`（可重跑验证）

*（内容由AI生成，仅供参考）*

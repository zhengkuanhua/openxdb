---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_875935c4c53a11f197b3525400248c00
    ReservedCode1: KPc+Z77cHY8Dn7Bals1Rw2E3wLXaUiH6cFznxbVvkiVQCFtENkY323H01qWjTn0Tu96xx9u3ngkjhBW4ekJ4kndDtUUBn7/SskFGgOHC757FCT7CBWxySG0hta1/W3hQSImFTEVBV8Hh3Zu7mh020ZJT2m5MB3YJTpbdSX7fX8yrvMSXfwVWqN95EYo=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_875935c4c53a11f197b3525400248c00
    ReservedCode2: KPc+Z77cHY8Dn7Bals1Rw2E3wLXaUiH6cFznxbVvkiVQCFtENkY323H01qWjTn0Tu96xx9u3ngkjhBW4ekJ4kndDtUUBn7/SskFGgOHC757FCT7CBWxySG0hta1/W3hQSImFTEVBV8Hh3Zu7mh020ZJT2m5MB3YJTpbdSX7fX8yrvMSXfwVWqN95EYo=
---

# T25 M12 功能簇（1/3）：分布式 JOIN（跨分片路由 + 合并）

> 版本：v5.0-P13-M12, v0.14.0-alpha
> 日期：2026-10-11
> 范围：`pkg/sql`（distjoin.go / executor.go + 测试 `m12_join_test.go`）、`pkg/db`（跨节点集成测试 `db_m12_join_test.go`）
> 闭环：`go test ./pkg/sharding/ ./pkg/sql/ ./pkg/server/ -count=1` 全绿，不 push（README 勾选由 M12 整簇收尾提交统一更新）
> 提交：`M12: distributed join`（本地，未 push）

## 背景
OpenXDB 在 M1-M8 已具备单机 SQL（含普通 JOIN：`joinRels` 全量内存拼接）与分布式存储/分片/集群能力（`pkg/sharding` region 路由、`pkg/cluster` 跨节点转发合并、行键带 region 前缀）。但既有 JOIN 只做**驱动表全量扫描 + 右表全量扫描 + 内存拼接**：即使右表可按分区键/主键定位到单个 region，也会退化为全表扫描右表，跨节点时放大网络传输且无法利用分片局部性。本子功能补齐**分布式 SQL 路由合并层**：右表主键等值 JOIN 时，驱动行按分区键**路由点查**右表（本地直读 / 远端转发），仅合并各分片命中行；单分区时自然退化为本地点查，行为与单机 JOIN 一致。

## 设计
### 路由判定（planJoinRoute）
满足以下条件时走分布式路由路径，否则回退 `joinRels` 全量内存 JOIN（行为与 M1 单机一致）：
- 右表为**物理表**（非子查询派生表）；
- ON 条件全部为**等值**（`=`，多列 AND 等值支持）；
- 至少一条等值条件引用**右表主键列**，且驱动侧对应列可在左 Rel 中解析；两种书写方向均可：
  - `a.x = b.pk`（右主键在右操作数侧）；
  - `b.pk = a.x`（右主键在左操作数侧）；
- 驱动侧列类型与右表主键编码兼容（见「回退兜底」）。

### 执行路径（routedJoin）
```
驱动表（左）仍由 scanMergedCluster 跨 region / 跨节点全量扫描合并得到左行
  → 对每个左行：
      1. 驱动值 pkBytes 编码右表主键；
      2. e.locateRow(meta, pk) 按分区键定位所属 region；
      3. e.rowKeyAt 编码【带 region 前缀】的物理行键（与 INSERT 写入键一致）；
      4. e.getKVCluster 点查：本地 region 直读（快照版本过滤）/ 远端 region 经
         cluster 转发（携带 begin_ts），未命中返回 storage.ErrNotFound；
      5. 命中 → decodeRow 还原右行 → matchJoinOn 按完整 ON（含多列 AND 等值）
         校验；未命中或条件不满足 → INNER 丢弃该左行，LEFT 以空值补右列。
```
- 右表主键唯一，每左行至多命中 1 行，输出语义与 `joinRels` 全量拼接一致；
- 空分区天然正确：驱动扫描展开到每个 region，右表某 region 无数据即点查未命中；
- 快照一致性：点查与驱动扫描共用 `snapshotTS()` 的 begin_ts，本地/远端均按 M8 快照版本过滤，无幻读差异。

### 退化与兜底
- **单分区**：未 SplitTable 时 router 隐式单 region，`locateRow` 定位默认 region，点查即本地点查，行为与单机 JOIN 完全一致（测试显式覆盖）；
- **键类型不兼容**：驱动列 Kind 与右表主键类型编码不兼容（如 INT 主键对 TEXT 驱动值）时，路由点查必然 miss 而全量 JOIN 可能按字符串比较匹配，因此 `routedJoin` 返回 `errJoinUnroutable`，由 `joinRel` 捕获后**回退 `joinRels`**，保证与单机语义零偏差；
- **不可路由 JOIN**（子查询右表、非等值 ON、无右主键等值条件）直接走 `joinRels`。

### 代码落点
- `pkg/sql/distjoin.go`（新增）：`joinRoutePlan` / `planJoinRoute` / `joinRel`（统一入口）/ `routedJoin` / `leftJoinNullRow` / `joinKeyCompatible` / `errJoinUnroutable`；
- `pkg/sql/executor.go`：`execSelectRel` JOIN 循环由 `joinRels` 改为 `e.joinRel`（一行改动接入）。

## 测试
### pkg/sql/m12_join_test.go（单机 + 分片路由）
| 测试 | 覆盖 |
|---|---|
| `TestM12JoinRoutedInner` | users/orders 各拆 3 region，`u.id = o.id`（右主键等值）INNER JOIN 路由合并，4 行结果正确 |
| `TestM12JoinRoutedLeft` | LEFT JOIN 路由路径：10 行，未匹配左行右列补空 |
| `TestM12JoinMultiColEq` | 多列 AND 等值（右主键 + 一般列）：路由命中后按完整 ON 校验，名不匹配行被过滤 |
| `TestM12JoinReverseDirection` | 方向二 `o.id = u.id` 同样走路由路径 |
| `TestM12JoinAliasRoute` | 表别名限定列路由 |
| `TestM12JoinFallbackNonPK` | 关联列非右主键 → 回退 joinRels，分片后结果正确 |
| `TestM12JoinSinglePartitionDegrade` | 单分区（未分片）路由 JOIN 退化为本地点查，行为正确 |
| `TestM12JoinEmptyPartition` | 右表某 region 为空时 LEFT JOIN 仍正确（空分区） |
| `TestM12JoinSplitConsistency` | 未分片 vs 分片：INNER/LEFT/反向全量结果 DeepEqual 一致 |
| `TestM12JoinFallbackTypeMismatch` | 驱动 TEXT 值对 INT 主键：路由不兼容 → 回退 joinRels，按字符串比较匹配（与单机一致） |

### pkg/db/db_m12_join_test.go（双节点真分布式）
| 测试 | 覆盖 |
|---|---|
| `TestM12JoinCrossNodeInner` | 双节点集群 + ASSIGN 中间 region 给 B：驱动扫描跨节点合并 + 右表点查命中远端 region 转发，INNER JOIN 4 行正确 |
| `TestM12JoinCrossNodeLeft` | 跨节点 LEFT JOIN：10 行，匹配行金额正确、未匹配补空 |
| `TestM12JoinCrossNodeReverse` | 跨节点方向二写法 `o.id = u.id` |

## 取舍说明
- **驱动侧全量扫描 + 右表路由点查**：分片下 JOIN 代价 = O(左表) 点查，相比 joinRels 的 O(左×右) 全量拼接显著降低跨节点传输；未做 Hash Join / Broadcast Join 优化（数据分布未知、右表大时后续可加），本子功能聚焦正确性路由层；
- **物理行键带 region 前缀**：routedJoin 必须经 `locateRow + rowKeyAt` 构造与写入一致的键（早期实现直接用 `EncodeTableKey` 无前缀导致点查全部 miss，已修正并纳入测试）；
- **不兼容即回退**：宁可回退全量 JOIN 也不冒险用不匹配的编码路由，正确性优先于性能；
- **非右主键等值不回退也正确**：回退 joinRels 只是性能取舍，非主键关联列目前不支持哈希/索引路由（后续可在二级索引之上扩展）。

## README 勾选

由 M12 整簇收尾提交统一执行：
- Status 行更新为 M12 分布式 SQL 纵深簇；
- T 表格追加 `T25 M12 distributed SQL cluster: distributed join + cross-node ORDER BY/paging + distributed subquery`；
- 文档列表追加 `T25_m12_join.md`（随本提交加入）。

*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

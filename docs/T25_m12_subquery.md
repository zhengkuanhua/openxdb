# T25 M12 功能簇（3/3）：分布式子查询（IN / EXISTS / 标量）

> 版本：v5.0-P13-M12, v0.14.0-alpha
> 日期：2026-10-11
> 范围：`pkg/sql`（distributed_subquery.go / parser.go / executor.go + 测试 `m12_subquery_test.go`）、`pkg/db`（测试 `db_m12_subquery_test.go`）
> 闭环：`go test ./pkg/sharding/ ./pkg/sql/ ./pkg/server/ ./pkg/db/ -count=1` 全绿，不 push（README 勾选由 M12 整簇收尾提交统一更新）
> 提交：`M12: distributed subquery`（本地，未 push）

## 背景

M12 分布式 SQL 纵深簇至此收尾：子功能一分布式 JOIN（distjoin.go，提交 238ed0e）、子功能二跨节点 ORDER BY/分页（orderlimit.go，提交 0fbb80d）已完成，本子功能补齐 **WHERE 子查询的跨分片/跨节点支持**。此前子查询（IN / EXISTS / 标量）仅在单机逐行执行（`inSubquery` 本地扫描），分片表场景下要么无法感知分片边界、要么反复全表扫描，网络与计算开销不可控。本子功能在复用单机子查询语义的基础上，引入「预计算缓存 + 主键路由点查（分片裁剪）」执行模型，并在子查询与主查询同为分片表时按主查询列值裁剪分片，单分区/未分片场景退化本地执行，行为与单机完全一致。

## 设计

### 支持的子查询形态

```sql
-- 1) IN 子查询：WHERE col IN (SELECT ...)
SELECT u.id FROM users u WHERE u.id IN (SELECT o.id FROM orders o WHERE o.amount > 100);

-- 2) EXISTS 子查询（非关联）：WHERE EXISTS (SELECT ...)
SELECT u.id FROM users u WHERE EXISTS (SELECT o.id FROM orders o WHERE o.amount >= 500);

-- 3) 标量子查询（单行单列）：WHERE col = (SELECT ...)
SELECT u.id FROM users u WHERE u.id = (SELECT o.id FROM orders o WHERE o.amount = 500);
```

> 关联（correlated）子查询（子查询 WHERE 引用主查询列，如 `o.id = u.id`）不在本次支持范围，与单机行为一致（执行时报 unknown column）。解析层已支持 `EXISTS (SELECT ...)` 与右值 `(SELECT ...)` 标量两种形态。

### 执行模型

- **默认模式（归并）**：子查询作为独立语句执行（`execSelect` 走 `scanMergedCluster` 跨 region/跨节点归并），结果在 coordinator 一次性预计算缓存（`precomputeSubqueries`，按 WHERE 条件指针索引）；主查询逐行过滤时 O(1) 复用缓存，不再重复执行子查询。EXISTS 在无 ORDER BY/LIMIT 时克隆子查询设 `LIMIT 1` 尽早截断，不改变存在性；标量多行报错、零行视为 NULL（比较条件不成立）。
- **IN 路由模式（分片裁剪）**：满足静态条件时，对主查询左列每个不同值按子查询表主键路由点查归属 region（`locateRow` → `rowKeyAt` 构造带 region 前缀行键 → `getKVCluster` 本地直读/远端转发），命中后按子查询 WHERE 本地校验。只访问该值归属的分片，网络传输从「子查询全表跨分片归并」降为「按值点查」。
- **行级接入**：`executor.go` 的 `execSelectRel` 在 WHERE 过滤前先 `precomputeSubqueries` 再 `matchWhereRelCached`；`matchWhereRel`/`matchCondRel` 保留为对 `*Cached(nil)` 的包装，CASE 分支内子查询走动态执行兜底，兼容旧调用点。

### 路由判定（`inSubqueryRoutable`）

IN 子查询可路由需同时满足：

1. 子查询为单物理表：`From` 非空、无 FROM 子查询、无 JOIN / GROUP BY / ORDER BY / LIMIT / OFFSET；
2. SELECT 恰为单列裸列（无 Ref / Alias / Agg / Window），且列名等于子查询表主键列（可点查）；
3. 子查询 WHERE 无嵌套子查询（路由命中后行级校验走 `matchWhere`）；
4. 子查询表存在多个 region（`regionsOf(metaS) > 1`），单分区/未分片直接回退默认模式。

### 退化与兜底

- **单分区/未分片**：子查询表 region ≤ 1 → 跳过路由，子查询本地全量归并，行为与单机一致（测试 `TestM12SubquerySinglePartitionDegrade`）；
- **类型不兼容回退**：驱动值类型与子查询表主键编码不兼容（如 INT 主键对 TEXT 驱动值）→ `evalInSubqueryRouted` 返回 `errSubqueryUnroutable`，`buildInSubquerySet` 捕获后回退默认归并模式，保证与单机语义零偏差（测试 `TestM12SubqueryTypeMismatchFallback`）；
- **一致性保障**：路由命中使用 `compareVal` 精比较构造集合，与单机 `inSubquery` 逐值语义一致（允许跨 Kind 宽松相等）。

## 测试

### pkg/sql/m12_subquery_test.go（分片层）

| 测试 | 覆盖 |
|---|---|
| `TestM12SubqueryInSharded` | 三 region 分片 IN 子查询：带 WHERE 路由模式命中集、无 WHERE 全量集合、空集合；驱动值逐值点查 |
| `TestM12SubqueryExistsSharded` | EXISTS 跨分片判定真/假，配合主查询条件过滤；无条件 EXISTS 全量 |
| `TestM12SubqueryScalarSharded` | 标量子查询取数命中、零行 NULL、多行报错 |
| `TestM12SubquerySinglePartitionDegrade` | 子查询表单分区/未分片退化本地，结果与分片一致 |
| `TestM12SubqueryPlainVsSharded` | 同一批语句在未分片与分片引擎上结果逐条一致（回归防偏差） |
| `TestM12SubqueryTypeMismatchFallback` | TEXT 驱动 vs INT 主键 → 路由回退默认归并，结果与单机一致 |

### pkg/db/db_m12_subquery_test.go（跨节点）

| 测试 | 覆盖 |
|---|---|
| `TestM12SubqueryCrossNodeIn` | 中间 region 指派节点 B：带 WHERE 命中 B 远端行；全表集合跨节点归并 |
| `TestM12SubqueryCrossNodeExists` | EXISTS 判定依赖 B 远端分片数据，配合主查询条件返回 |
| `TestM12SubqueryCrossNodeScalar` | 标量值位于 B 远端分片取回参与比较；零行 NULL；多行报错 |

## 取舍说明

- **预计算一次 + 缓存**：子查询结果对主查询所有行复用，避免 N 次重复执行；缓存按 WHERE 条件指针索引，条件相同只算一次；
- **路由点查而非广播**：IN 子查询可路由时只访问驱动值归属分片，避免全表跨分片归并；代价是路由路径为逐值点查（值去重后执行），适合主查询列基数小的场景，基数大时回退归并更优（本次未做基数估算，留待后续优化）；
- **非关联子查询范围**：关联子查询（列引用）与单机一致不支持，避免引入逐行代入的执行复杂度；后续如需可在 `matchCondRelCached` 动态路径扩展；
- **EXISTS 截断**：无 ORDER BY/LIMIT 时克隆子查询设 `LIMIT 1`，跨分片归并仍发生但每分片尽早截断，不改变存在性语义；
- **标量严格性**：多行直接报错（`scalar subquery returned more than one row`）而非取首行，与标准 SQL 一致；
- **解析层形态**：`parseCond` 增加 `EXISTS (SELECT...)` 独立分支（`Op="EXISTS"`）与右值 `(SELECT...)` 标量分支（`c.Sub` 承载），`NOT IN`/`NOT EXISTS` 仍不支持（parser 原限制，不在本次范围）。

## README 勾选

由 M12 整簇收尾提交统一执行：
- Status 行更新为 M12 分布式 SQL 纵深簇；
- T 表格追加 `T25 M12 distributed SQL cluster: distributed JOIN + cross-node ORDER BY/limit + distributed subquery`；
- 文档列表追加 `T25_m12_subquery.md`（随本提交加入）。

*（内容由AI生成，仅供参考）*

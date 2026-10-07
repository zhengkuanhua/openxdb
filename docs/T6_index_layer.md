# T6 二级索引层（Secondary Index）

## 目标

为 SQL 层提供二级索引能力：支持 `CREATE INDEX` / `DROP INDEX` DDL、DML 自动维护、查询优化器利用索引完成等值/范围查询与排序，为 T4 ACID 一致性（C）与后续性能基准（E1）提供存储侧支撑。

## 语法

```
CREATE INDEX [name] ON <table> (<column>)
DROP INDEX [name] ON <table>
```

- `name` 可选：缺省自动生成 `idx_<column>`。
- 索引列支持 INT / TEXT（类型与表列一致）。
- 表必须存在、列必须存在；索引名表内唯一。

## 存储格式

索引键编码（`pkg/sql/meta.go`）：

```
i{tableID:8B}{indexID:8B}{idxVal}p{pk}
```

| 段 | 说明 |
|----|------|
| `i` | 索引命名空间前缀（`'i'` < `'s'`，索引键整体位于行键区间之前） |
| `{tableID:8B}` | 大端表 ID |
| `{indexID:8B}` | 大端索引 ID（表内自增） |
| `{idxVal}` | 排序友好编码：INT 翻转符号位（`v^0x8000000000000000`，8B BE），TEXT 原字节；保证字节序 = 值语义序（负数 < 0 < 正数） |
| `p` | 分隔符，用于定位 pk 后缀 |
| `{pk}` | 主键字节（INT 定长 / TEXT 原字节），保证同一索引值内按主键有序 |

索引键值为单字节存在标记 `{1}`。

范围扫描辅助（`meta.go`）：

- `IndexRange(tid, iid, lo, hi)`：`[i{tid}{iid}{lo}, 上界)`；`hi` 非 nil 时上界排他（`idxVal < hi`），`hi` 为 nil 时上界为整表索引边界（`i{tid+1}`，避免 `0xff` 前缀截断）。
- 单索引完整区间：`[EncodeIndexKey2(tid, iid, nil, nil), EncodeIndexKey2(tid, iid+1, nil, nil))`。

## 执行器行为

### DDL

- `CREATE INDEX`：校验表/列/索引名 → 登记 `IndexMeta`（`ID/Name/Col`）到表元数据（`m:tables` 单键 JSON，经 Txn+WAL 原子持久化）→ **先登记再回填**：全表扫描逐行写索引键（避免新索引漏写）。
- `DROP INDEX`：从元数据摘除 → 扫描单索引区间逐键删除 → 返回删除键数。
- `DROP TABLE`：先删除该表全部索引键（整表索引区间），再删行键与元数据。

### DML 维护（ACID-C）

- `INSERT`：写行后为每行写入全部索引键（`putIndexKeys`）。
- `UPDATE`：删除旧索引键 → 改行值 → 写新索引键（改索引列时旧值不可查、新值可查）。
- `DELETE`：删除行前清理全部索引键。

所有 DML 均在事务内完成，任一步失败整体回滚，索引与行数据不会出现不一致。

### 查询优化（`execSelect`）

优化顺序：

1. 主键点查（WHERE 单条件 `pk = v`）→ `Get`；
2. 二级索引（`indexLookup`）：取 WHERE 中第一个命中索引列且非 `!=` 的条件；
   - `=`：`[prefix(v), prefix(v)+0xff)`
   - `>=`：`[prefix(v), +inf)`；`>`：`[prefix(v)+0xff, +inf)`
   - `<`：`[-inf, prefix(v))`；`<=`：`[-inf, prefix(v)+0xff)`
   - 扫描索引键 → 提取 pk（`indexPKSuffix`）→ 点查行 → 行级 `matchWhere` 兜底（等值扫描可能因 TEXT 前缀误包含而多取，由行级过滤保证正确）
3. 全表扫描 + 过滤（无可用索引时）。

排序优化：索引扫描结果天然按索引列升序，若 `ORDER BY` 恰为该索引列且非 DESC，跳过重复排序（`finishSelect` 的 `ordered` 标记）。

## 测试

`pkg/sql/sql_test.go` 新增 9 个用例（T6 段）：

| 用例 | 覆盖 |
|------|------|
| TestCreateIndexLookup | 等值查询、缺失值、重复名/未知列/未知表报错 |
| TestCreateIndexBackfill | 存量数据建索引回填 |
| TestIndexRangeAndOrderBy | 范围+排序、索引序 LIMIT、DESC 重排 |
| TestIndexNegativeIntOrder | 负数 INT 翻转符号位排序正确 |
| TestIndexMaintenance | INSERT/UPDATE/DELETE 索引维护 |
| TestDropIndex | 删索引、重建同名索引 |
| TestDropTableWithIndex | 删表清索引、重建同名表无残留 |
| TestMultiColumnIndexes | 多索引并存互不影响 |
| TestIndexPersistenceAcrossReopen | 索引元数据与键持久化 |

## 验证

- `go test ./pkg/sql/`：22/22 PASS
- `go test ./...`：7 包全绿
- TCP 冒烟（`cmd/openxdb start`）：建表 → 插入 → 建索引（回填 4 行）→ 等值/范围/排序查询 → UPDATE 改索引列 → DELETE → DROP INDEX（3 键）→ 全链路通过

## 后续演进

- 复合索引（多列编码进 idxVal）；
- 索引选择器（多个索引条件择优）；
- 覆盖索引（索引键携带列值，免回表）；
- 显式 `WHERE` 索引提示语法。

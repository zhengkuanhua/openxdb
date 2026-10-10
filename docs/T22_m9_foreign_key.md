# T22 M9 功能簇（4/5）：外键约束

> 版本：v5.0-P11-M9FK, v0.13.0-alpha
> 日期：2026-10-10
> 范围：`pkg/sql`（新增 `foreign_key.go` + ast / lexer / parser / executor / meta / csv）+ 单元测试（`m9_foreign_key_test.go`）+ README 勾选
> 闭环：`go build ./...` 与 `go test ./... -count=1` 全绿，不 push
> 提交：`M9: foreign key (REFERENCES / ON DELETE CASCADE, meta persistence, DROP TABLE cleanup)`（本地，未 push）

## 背景

M1 建表仅支持主键与索引，缺少引用完整性：子表可写入任意孤儿行、父表行可被
随意删除导致悬空引用。M9 外键约束为 SQL 层补上**声明式引用完整性**：
`REFERENCES`（列级）与 `FOREIGN KEY ... REFERENCES`（表级）定义父子关系，
支持 `ON DELETE CASCADE / RESTRICT`；外键定义随表元数据持久化，
`DROP TABLE` 父表时联动清理子表外键定义，杜绝悬空引用与残留约束。

## 设计

### 语法

```sql
-- 列级（单列引用）
CREATE TABLE child (
  id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) [ON DELETE CASCADE|RESTRICT]
);
-- 表级（单列 / 多列复合引用）
CREATE TABLE child (
  id INT PRIMARY KEY, pid INT,
  FOREIGN KEY (pid) REFERENCES parent (id) [ON DELETE CASCADE|RESTRICT]
);
```

### 词法 / 语法层

- `lexer.go`：关键字表补充 `FOREIGN` / `REFERENCES` / `CASCADE` / `RESTRICT`；
- `ast.go`：新增 `ForeignKey{Columns, RefTable, RefCols, OnDelete}`
  （OnDelete ∈ `""` / `"CASCADE"` / `"RESTRICT"`，缺省即 RESTRICT 语义）；
  `TableMeta` 增加 `ForeignKeys []ForeignKey` 字段；
- `parser.go`：`parseCreate` 的列定义解析列级 `REFERENCES`（含可选的
  `ON DELETE ...` 子句）；表级 `FOREIGN KEY (...)` 约束在列清单解析后
  收集进 `ForeignKeys`；
- `meta.go`：表元数据序列化 / 反序列化接入 `ForeignKeys`（随 DDL 持久化）；
- `csv.go`：表结构导出 / 导入同步携带外键定义。

### 执行层（executor + foreign_key.go）

- `execCreateTable` 建表校验（`validateForeignKeys`）：
  - 引用表必须存在（`referenced table not exists`）；
  - 引用列必须是父表**主键**（`must be primary key`）；
  - 子列与父主键类型一致（`column type mismatch`）；
- 写入防护（`checkFKOnRow`）：`INSERT` / `UPDATE` 子表行时，逐条外键校验
  父行存在（跨 region 读取父表，`fkRowMatches` 比对主键编码），违反即
  `foreign key constraint violated`，事务回滚；
- 父行更新防护（`fkParentReferenced`）：`UPDATE` 父表时**基于旧行**判断
  该行是否被子表引用，被引用则拒绝（`ON UPDATE` 未支持，RESTRICT 语义）；
- 父行删除联动（`collectFKDeleteOps`）：`DELETE` 父表行时扫描引用它的子表：
  - RESTRICT：存在引用行则整体拒绝删除（`is referenced by parent`）；
  - CASCADE：级联删除子表引用行，同时收集子表索引键删除 ops（2PC 原子落盘），
    保证主表、子表、索引三方一致；
- `execDropTable` 联动清理：`DROP TABLE` 父表时，遍历全部子表元数据移除指向
  该父表的外键定义并持久化，**子表数据保留**，不再校验该约束。

## 测试（pkg/sql/m9_foreign_key_test.go）

| 测试 | 覆盖 |
|---|---|
| `TestForeignKeyTableLevelConstraint` | 表级 FOREIGN KEY：合法插入通过、孤儿插入报错 |
| `TestForeignKeyColumnLevelReferences` | 列级 REFERENCES 行为 |
| `TestForeignKeyCreateValidation` | 引用表不存在 / 非主键列 / 类型不匹配均拒绝建表 |
| `TestForeignKeyOnDeleteRestrict` | 默认 RESTRICT：被引用父行禁止删除、未引用可删 |
| `TestForeignKeyOnDeleteCascade` | CASCADE：父行删除级联清空全部子行 |
| `TestForeignKeyCascadeIndexCleanup` | CASCADE 后子表二级索引同步清理（索引查不到残留） |
| `TestForeignKeyUpdateParentProtected` | 被引用父行禁止 UPDATE（RESTRICT 语义），未引用可改 |
| `TestForeignKeyDropTableCleansRefs` | DROP 父表：子表数据保留、外键解除、不再校验 |
| `TestForeignKeyDropParentWithSiblingRefs` | 多子表指向同一父表：DROP 后全部联动清理 |
| `TestForeignKeyShardingCrossRegion` | 分片引擎下跨 region 外键校验仍生效 |
| `TestForeignKeyOrderByScan` | 外键表 ORDER BY 查询正常 |

## 取舍说明

- `ON DELETE` 支持 `CASCADE` / `RESTRICT` 两种动作，缺省 RESTRICT；
  `ON UPDATE` 未实现（父表被引用行更新一律拒绝，安全优先）；
- 外键列仅支持引用父表**主键**（单列 / 复合主键），不支持引用唯一索引列；
- DROP 父表采用**引用解除**而非级联删除子表（与主流 RDBMS 的
  `DROP ... RESTRICT` 不同，避免误删业务数据；文档 / 测试明确此语义）；
- 级联删除通过 2PC ops 与主删除同事务原子提交，分片下跨 region 同样生效。

## README 勾选

- Status 行更新为 `M9 foreign key: REFERENCES / FOREIGN KEY + ON DELETE CASCADE|RESTRICT, meta persistence, DROP TABLE cleanup (v5.0-P11-M9FK), v0.13.0-alpha.`
- T 列表追加 `+ T22 M9: foreign key (REFERENCES / ON DELETE CASCADE, meta persistence, DROP TABLE cleanup)`

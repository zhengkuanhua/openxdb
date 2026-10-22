---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_f8c27df6c4b211f19063525400393706
    ReservedCode1: LIvUvcdg7awnhvzp7hCzdMRvbXdJso2rMbvcwISxbwtG0xMrHuLQNZn48PhfjzPFssZ0L8grIc0mOGWXN478XNLAweby9GeIQ8ieNidALC6RGUCmPu4zmtSrwUZTG6/YSd0zJdSFb50/iukUfCe8Jy73KUX/G94BneG6jqUEh3rOTjz57u67OAL4Jc0=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_f8c27df6c4b211f19063525400393706
    ReservedCode2: LIvUvcdg7awnhvzp7hCzdMRvbXdJso2rMbvcwISxbwtG0xMrHuLQNZn48PhfjzPFssZ0L8grIc0mOGWXN478XNLAweby9GeIQ8ieNidALC6RGUCmPu4zmtSrwUZTG6/YSd0zJdSFb50/iukUfCe8Jy73KUX/G94BneG6jqUEh3rOTjz57u67OAL4Jc0=
---

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
支持 `ON DELETE CASCADE / RESTRICT` 与 `ON UPDATE CASCADE / RESTRICT / SET NULL`（后者为 P0-3 补齐）；外键定义随表元数据持久化，
`DROP TABLE` 父表时联动清理子表外键定义，杜绝悬空引用与残留约束。

## 设计

### 语法

```sql
-- 列级（单列引用）
CREATE TABLE child (
  id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) [ON DELETE CASCADE|RESTRICT] [ON UPDATE CASCADE|RESTRICT|SET NULL]
);
-- 表级（单列 / 多列复合引用）
CREATE TABLE child (
  id INT PRIMARY KEY, pid INT,
  FOREIGN KEY (pid) REFERENCES parent (id) [ON DELETE CASCADE|RESTRICT] [ON UPDATE CASCADE|RESTRICT|SET NULL]
);
```

### 词法 / 语法层

- `lexer.go`：关键字表补充 `FOREIGN` / `REFERENCES` / `CASCADE` / `RESTRICT` / `NULL`（P0-3，`SET NULL` 动作）； 
- `ast.go`：新增 `ForeignKey{Columns, RefTable, RefCols, OnDelete, OnUpdate}`
  （OnDelete ∈ `""` / `"CASCADE"` / `"RESTRICT"`，缺省即 RESTRICT 语义；
  OnUpdate ∈ `""` / `"CASCADE"` / `"RESTRICT"` / `"SET NULL"`，缺省 RESTRICT）；
  `TableMeta` 增加 `ForeignKeys []ForeignKey` 字段；
- `parser.go`：`parseCreate` 的列定义解析列级 `REFERENCES`（含可选的
  `ON DELETE ...` / `ON UPDATE ...` 子句，任意顺序、可重复出现）；表级
  `FOREIGN KEY (...)` 约束在列清单解析后收集进 `ForeignKeys`；
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
- 父行更新联动（`collectFKUpdateOps`，P0-3）：`UPDATE` 父表行且**主键列值
  实际变化**时，扫描引用它的子表行匹配旧父键值，按 `ON UPDATE` 动作处理：
  - RESTRICT（缺省）：存在引用行则整体拒绝更新（`references parent`），
    与 ON DELETE RESTRICT 同语义；被引用父行更新非主键列不受影响；
  - CASCADE：子表外键列同步更新为新父键值（含旧索引键删除、行重写、新
    索引键写入；子表主键被级联改写时递归联动更深层引用，多级外键链同样
    生效），主键变化时行键按新主键重新路由（跨 region 同样正确）；
  - SET NULL：子表外键列置 NULL（外键列为主键 / 不可空在建表校验拒绝：
    `requires nullable column`）；
  全部 ops 与父行更新同事务原子提交（2PC）；
- 父行删除联动（`collectFKDeleteOps`）：`DELETE` 父表行时扫描引用它的子表：
  - RESTRICT：存在引用行则整体拒绝删除（`is referenced by parent`）；
  - CASCADE：级联删除子表引用行，同时收集子表索引键删除 ops（2PC 原子落盘），
    保证主表、子表、索引三方一致；
  - SET NULL（P0-3）：子表外键列置 NULL（与 ON UPDATE SET NULL 同语义，
    不可空列建表校验拒绝）；
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
| `TestForeignKeyUpdateParentProtected` | 被引用父行 UPDATE 主键拒绝（RESTRICT 语义），未引用可改、非主键列可改 | 
| `TestForeignKeyOnUpdateRestrict` | ON UPDATE RESTRICT：多行引用时整体拒绝、未引用可改、非主键列可改 |
| `TestForeignKeyOnUpdateCascade` | ON UPDATE CASCADE：多行级联、二级索引同步更新、新父键可引用 |
| `TestForeignKeyOnUpdateCascadeIndexCleanup` | CASCADE 后子表二级索引同步更新（索引查得到新值、查不到旧值） |
| `TestForeignKeyOnUpdateSetNull` | ON UPDATE SET NULL：引用行外键置 NULL、未引用行保持原值 |
| `TestForeignKeyOnUpdateSetNullNonNullable` | 外键列为主键（不可空）时 SET NULL 建表拒绝 |
| `TestForeignKeyOnUpdateMultiLevelChain` | 多级外键链 A←B←C：A 主键更新逐级级联改写 B 主键与 C 外键 |
| `TestForeignKeyOnUpdateWithDelete` | ON UPDATE CASCADE 与 ON DELETE CASCADE 组合：更新与删除动作互不干扰 | 
| `TestForeignKeyDropTableCleansRefs` | DROP 父表：子表数据保留、外键解除、不再校验 |
| `TestForeignKeyDropParentWithSiblingRefs` | 多子表指向同一父表：DROP 后全部联动清理 |
| `TestForeignKeyShardingCrossRegion` | 分片引擎下跨 region 外键校验仍生效 |
| `TestForeignKeyOrderByScan` | 外键表 ORDER BY 查询正常 |

## 取舍说明

- `ON DELETE` 支持 `CASCADE` / `RESTRICT` / `SET NULL`（P0-3 补 SET NULL），缺省 RESTRICT；
  `ON UPDATE` 支持 `CASCADE` / `RESTRICT` / `SET NULL`（P0-3），缺省 RESTRICT，
  与 ON DELETE 错误处理（整体拒绝 + 事务回滚）与文档语义保持一致；
- 外键列仅支持引用父表**主键**（单列 / 复合主键），不支持引用唯一索引列；
- DROP 父表采用**引用解除**而非级联删除子表（与主流 RDBMS 的
  `DROP ... RESTRICT` 不同，避免误删业务数据；文档 / 测试明确此语义）；
- 级联删除通过 2PC ops 与主删除同事务原子提交，分片下跨 region 同样生效。

## README 勾选

- Status 行更新为 `M9 foreign key: REFERENCES / FOREIGN KEY + ON DELETE CASCADE|RESTRICT|SET NULL, ON UPDATE CASCADE|RESTRICT|SET NULL (P0-3), meta persistence, DROP TABLE cleanup (v5.0-P11-M9FK), v0.13.0-alpha.`
- T 列表追加 `+ T22 M9: foreign key (REFERENCES / ON DELETE CASCADE, ON UPDATE CASCADE|RESTRICT|SET NULL (P0-3), meta persistence, DROP TABLE cleanup)`
*（内容由AI生成，仅供参考）*

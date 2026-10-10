# T22 M9 功能簇（1/5）：在线 DDL（ALTER TABLE）

> 版本：v5.0-P11-M9DDL, v0.13.0-alpha
> 日期：2026-10-10
> 范围：`pkg/sql`（lexer / ast / parser / executor）+ 单元测试（`m9_ddl_test.go`）+ README 勾选
> 闭环：`go build ./...` 与 `go test ./... -count=1` 全绿，不 push
> 提交：`M9: online DDL (ALTER TABLE)`（本地，未 push）

## 背景

M1–M8 已支持建表 / 删表 / 建索引 / 删索引 / DML / 分布式写与备份恢复，但表结构
一旦创建便不可变：新增列、改名、增删索引都只能 `DROP TABLE` 后重建，数据全部丢失。
M9 在线 DDL 为目标表提供**在线结构变更**能力：`ALTER TABLE` 六种常用动作，
对已有数据行正确迁移，DDL 以事务方式执行并保证崩溃安全（WAL 落盘），
元数据（SHOW TABLES / SHOW INDEX）与数据始终一致。

## 设计

### 语法

```sql
ALTER TABLE t ADD COLUMN c TYPE       -- 新增列（已有行回填默认值）
ALTER TABLE t DROP COLUMN c           -- 删除列（同步移除引用该列的索引）
ALTER TABLE t RENAME COLUMN c TO c2   -- 列改名（同步更新主键 / 索引列引用）
ALTER TABLE t RENAME TO t2            -- 表改名
ALTER TABLE t ADD INDEX i (c)         -- 建索引（已有行全量回填索引键）
ALTER TABLE t DROP INDEX i            -- 删索引（删除索引键 + 元数据）
```

不支持动作（如 `MODIFY COLUMN`、多动作组合）返回明确语法错误。

### 词法 / 语法层

- `lexer.go` 关键字表新增 `ALTER` / `COLUMN` / `RENAME`（`INDEX` / `TO` / `ADD` / `DROP` 已有）；
- `ast.go` 新增 `AlterTableStmt{Table, Action, Column, Col, NewName, Name}`，
  Action 取值 `ADD_COLUMN / DROP_COLUMN / RENAME_COLUMN / RENAME_TABLE / ADD_INDEX / DROP_INDEX`；
- `parser.go` `parseStmt` 新增 `ALTER` 分支，`parseAlter` 按动作分流解析。

### 执行层（executor）

`execAlterTable` 统一骨架：`getTx()` 取事务 → `loadTables` 取表目录 →
按动作执行 → `ddlCommit` 提交；autocommit 时 `defer tx.Rollback()` 兜底。

**数据行迁移**（ADD/DROP COLUMN）：
- 全表 `scanMerged(rowRanges(meta))` 展开所有 region；
- 用**旧列布局** `decodeRow`、按**新列布局** `encodeRow`，重写行键值；
- 新增列回填默认值（`zeroValue`：INT=0 / TEXT='' / DATE=1970-01-01 /
  DECIMAL=0.0000 / BLOB=''）；删除列直接从行值中移除。

**索引联动**：
- `DROP COLUMN` 先扫描并删除引用该列的索引键，再从元数据移除索引，
  避免索引键悬空（脏索引）；
- `ADD INDEX` 建索引元数据后全表回填索引键（复用 `putIndexKeys`，
  pk 取行键内层主键）；`RENAME COLUMN` 同步改写主键列名与索引列名。

**事务与崩溃安全**：
- 元数据键 `metaKey` 是全局键（不含 region 前缀），直接 `tx.Put` 写入事务缓冲；
- 数据重写 ops 走既有 2PC 路径 `exec2pcWrite`（单机零触发 = applyOps + Commit），
  行键含 region 前缀可正常分组；空 ops（仅改名）直接 `tx.Commit()`；
- 元数据与数据同事务提交，Commit 经 WAL 落盘：崩溃后 WAL 重放恢复一致状态，
  不会出现"目录有新列但数据未回填"或反之；
- 显式事务（BEGIN…COMMIT）内 DDL 加入会话事务，由用户 COMMIT / ROLLBACK
  决定生效（分布式写仍按 M5 约定：显式事务内远端写报错）。

**约束**：
- `ADD COLUMN` 列重名报错；`DROP COLUMN` 不存在列 / 主键列报错；
- `RENAME COLUMN` 旧列不存在 / 新列重名报错；`RENAME TABLE` 目标重名报错；
- `ADD INDEX` 索引重名 / 列不存在报错；`DROP INDEX` 索引不存在报错。

## 测试（pkg/sql/m9_ddl_test.go）

| 测试 | 覆盖 |
|---|---|
| `TestAlterAddColumnBackfill` | 3 行已有数据回填 0，新行按新列写入 |
| `TestAlterAddColumnTypeDefaults` | TEXT/DATE/DECIMAL/BLOB 默认值；重复加列报错 |
| `TestAlterDropColumn` | 删列 + 索引同步移除；删主键 / 不存在列报错 |
| `TestAlterRenameColumn` | 列改名后查询；索引列引用同步；重名报错 |
| `TestAlterRenamePrimaryKeyColumn` | 主键列改名后按新主键点查 |
| `TestAlterRenameTable` | 表改名后查询 / SHOW TABLES 一致；旧表名不可用 |
| `TestAlterAddDropIndex` | ADD INDEX 回填可见；DROP INDEX 清理；非法列 / 索引报错 |
| `TestAlterDDLTransactionRollback` | 显式事务内 DDL 回滚后元数据不变 |
| `TestAlterParseLevel` | 六种动作语法解析 + 非法动作报错 |

## 取舍说明

- 不实现 `MODIFY COLUMN` 类型变更（类型兼容矩阵复杂，后续版本按需扩展）；
- 不实现一条 ALTER 内多动作组合（当前一次一个动作，保持单事务原子性简单可证）；
- `RENAME TABLE` 不搬移物理键（表 ID 不变），仅改目录中显示名，行 / 索引键无需重写。

## README 勾选

- Status 行更新为 `M9 online DDL: ALTER TABLE ADD/DROP/RENAME COLUMN + RENAME TABLE + ADD/DROP INDEX (transactional, crash-safe) (v5.0-P11-M9DDL), v0.13.0-alpha.`
- T 列表追加 `+ T22 M9: online DDL (ALTER TABLE ADD/DROP/RENAME COLUMN + RENAME TABLE + ADD/DROP INDEX)`

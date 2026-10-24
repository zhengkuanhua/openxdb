# T43 SQL 能力落地：ALTER MODIFY COLUMN / CREATE DATABASE 库级管理 / 多列窗口

- 版本: v1.6.0 阶段一（2026-10-24 落地）
- 范围: `pkg/sql`（lexer / parser / ast / executor / meta / session）
- 结论：P1 三项核心 SQL 硬缺口全部落地并通过 `go build ./...` 与 `go test ./pkg/sql`（全量 ok，23s）。
- 测试证据：`pkg/sql/t39_sql_subset_test.go`（17 个专项测试函数，覆盖三项功能与回归边界）。

---

## 1. ALTER TABLE 列级操作补全（MODIFY COLUMN）

### 1.1 语法与语义

- `ALTER TABLE <t> MODIFY COLUMN <col> <newtype> [<params>]`：修改列类型（INT/TEXT/DATE/DECIMAL/BLOB）及类型精度参数。
- 存量行重写：按 `convertColValue` 类型转换规则将已有行值转换为新类型后回写（DECIMAL 保留精度、INT/TEXT/DATE/BLOB 互转规则与 INSERT 路径一致）。
- 边界语义：
  - 空表 / 同类型幂等修改直接通过（`TestT39AlterModifyEmptyTable` / `TestT39AlterModifySameTypeIdempotent`）；
  - 索引列 / 主键列类型变更校验：变更会破坏索引键语义时报错拒绝（`TestT39AlterModifyRejections`）；
  - 文本→整数按零填充、DATE/TEXT 往返保持格式（`TestT39AlterModifyTextToIntZeroFill` / `TestT39AlterModifyDateTextRoundtrip`）。

### 1.2 涉及代码

- `executor.go` `execAlterTable`：新增 `MODIFY_COLUMN` 分支，复用既有 ADD/DROP/RENAME/INDEX 动作框架；
- `executor.go` `convertColValue`：列值类型转换（MODIFY 存量行重写共用）；
- parser/ast/lexer：`MODIFY` 动作解析与 `AlterTableStmt` 动作枚举。

## 2. CREATE DATABASE / 库级管理

### 2.1 语法与语义

- `CREATE DATABASE [IF NOT EXISTS] <db>`：新建库目录；重复创建报错，`IF NOT EXISTS` 幂等（`TestT39CreateDatabaseDuplicate`）。
- `DROP DATABASE <db>`：级联删除库内全部表及其数据/索引元数据（`TestT39DropDatabase`）。
- `SHOW DATABASES`：列出全部库（`TestT39CreateDatabaseShow`）。
- `USE <db>`：设置会话级默认库，`Session.CurrentDB` 持久于会话（`TestT39UseDatabase`）。
- 默认库：未限定库前缀的表归 `default`，存量数据零迁移兼容（旧 `TableMeta.DB` 为空按 default 解析）。
- 表名解析：`db.tbl` 前缀全链路归一（`resolveTableDB`），覆盖 CREATE TABLE / CTAS / DROP / ALTER / INSERT / UPDATE / DELETE / SELECT；库不存在报 `database not exists: <db>`（`TestT39QualifiedTableDML`）。
- `SHOW TABLES` 按当前会话库过滤；`information_schema` 保留虚拟库语义，`default.information_schema.tables` 等带库前缀写法兼容剥除（`TestT39ShowTablesPerDB`）。

### 2.2 涉及代码

- `meta.go`：库目录元数据 `m:databases` 键（序列化与 saveTables 同风格）；`TableMeta.DB` 归属库字段。
- `ast.go`：新增 `CreateDatabaseStmt` / `DropDatabaseStmt` / `UseStmt` / `ShowDatabasesStmt`。
- `parser.go`：`parseCreate` 增加 DATABASE 分支；`parseDrop` / `parseUse` / `parseShow` 分支；lexer 补 `DATABASE`/`USE`/`IF`/`EXISTS` 关键字。
- `executor.go`：`execCreateDatabase` / `execDropDatabase` / `execUse` / `execShowDatabases`；`resolveTableDB` / `tableDB` 库前缀归一；DML/DDL 全链路接入；`execShowTables` 按 `CurrentDB` 过滤。
- `session.go`：`Session.CurrentDB`（默认 `default`）。

## 3. 多列窗口扩展

### 3.1 语法与语义

- `WindowSpec.OrderBy` 由单列 `*OrderBy` 扩展为多列 `[]*OrderBy`；`ORDER BY col [ASC|DESC], ...` 逗号分隔多列。
- `computeWindowCol` 排序按多列键依次比较（`TestT39WindowOrderByMultiCol` / `TestT39WindowOrderByMultiColMixed`）；全部键相等视为同序。
- RANK / DENSE_RANK 多列同键处理：并列行共享同一名次（`TestT39WindowOrderByMultiColRank`）。
- RANGE 帧在多列 ORDER BY 时按首列键域计算（与单列行为一致）；多列 RANGE 帧在解析层明确报错限制并保持单列行为（`TestT39WindowRangeFrameRejectsMultiOrder`）。
- 多列 PARTITION BY + 多列 ORDER BY 组合测试（`TestT39WindowAggMultiOrder`）。

### 3.2 涉及代码

- `ast.go`：`WindowSpec.OrderBy []*OrderBy`。
- `parser.go`：`parseWindowSpec` 支持多列 ORDER BY / PARTITION BY。
- `executor.go`：`computeWindowCol` 多列键比较与 RANK/DENSE_RANK 分组逻辑；`aggWindowSpan` 帧内聚合消费多列排序结果。

---

## 4. 参考

- 测试：`pkg/sql/t39_sql_subset_test.go`（TestT39AlterModify* ×6、TestT39CreateDatabase* ×2、TestT39Window* ×5、TestT39QualifiedTableDML、TestT39ShowTablesPerDB、TestT39UseDatabase、TestT39DropDatabase）。
- README：版本状态段 `v1.6.0 阶段一`、`SQL subset` 能力清单、M10 窗口条目。
*（内容由AI生成，仅供参考）*

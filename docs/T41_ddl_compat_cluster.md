---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_c001636fc8f711f196f9525400248c00
    ReservedCode1: yF4z59etsYBMKbHMbB/u3xc2RnFjd4VV3eTQrl9Zy0WpGy9LSIPmnZzi4QO+1ZFTNviqP+A4PgEFIUZ5rxHNBEPMuEN6xUlwzZcWpqrO7+7ypIEflW3PqrZdy6DYu0sxDtEC3uSg+ur0ogfIqyIxgUlnpVqb0LFkvyzAb9ELZg3gXOufA/BtX2drrII=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_c001636fc8f711f196f9525400248c00
    ReservedCode2: yF4z59etsYBMKbHMbB/u3xc2RnFjd4VV3eTQrl9Zy0WpGy9LSIPmnZzi4QO+1ZFTNviqP+A4PgEFIUZ5rxHNBEPMuEN6xUlwzZcWpqrO7+7ypIEflW3PqrZdy6DYu0sxDtEC3uSg+ur0ogfIqyIxgUlnpVqb0LFkvyzAb9ELZg3gXOufA/BtX2drrII=
---

# T41 DDL 兼容补齐：CREATE 系列 IF NOT EXISTS + CREATE TABLE AS SELECT（CTAS）

- 版本：v0.29.0-alpha
- 范围：pkg/sql（parser / ast / executor / matview / rbac / trigger / procedure）
- 目标：补齐两类 DDL 语法缺口——① CREATE 系列统一支持 `IF NOT EXISTS`；② 支持 `CREATE TABLE [IF NOT EXISTS] <name> AS SELECT ...`（CTAS）。

---

## 第一节：CREATE 系列 IF NOT EXISTS

### 1.1 现状

- `DROP` 系列（TABLE / USER / VIEW / MATERIALIZED VIEW / TRIGGER / PROCEDURE / ROLE）自 T21 起已支持 `IF EXISTS`：对象不存在时静默成功。
- `CREATE` 系列（TABLE / VIEW / MATERIALIZED VIEW / INDEX / USER / ROLE / TRIGGER / PROCEDURE）**均不支持** `IF NOT EXISTS`：
  - 解析层：`parseCreate` 分发各子函数后直接解析对象名，遇到 `IF NOT EXISTS` 会报 unexpected token。
  - 执行层：各 `execCreate*` 在对象已存在时一律返回 `xxx already exists` 错误，无静默跳过路径。
- lexer 关键字：`IF`、`EXISTS`、`AS` 均已登记（lexer.go keywords 表）；`NOT` 未登记为关键字，但按既有约定（列级 `NOT NULL` 同此处理）可按 token 文本匹配接受。

### 1.2 设计

**语法**（与 DROP 系列 `IF EXISTS` 对称）：

```
CREATE TABLE             [IF NOT EXISTS] <name> (...)
CREATE VIEW              [IF NOT EXISTS] <name> AS <SELECT>
CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name> AS <SELECT>
CREATE INDEX             [IF NOT EXISTS] <name> ON <table> (<col>)
CREATE UNIQUE INDEX      [IF NOT EXISTS] <name> ON <table> (<col>)
CREATE USER              [IF NOT EXISTS] '<name>' IDENTIFIED BY '<pwd>'
CREATE ROLE              [IF NOT EXISTS] '<role>'
CREATE TRIGGER           [IF NOT EXISTS] <name> BEFORE|AFTER <event> ON <table> ...
CREATE PROCEDURE         [IF NOT EXISTS] <name> [(...)] BEGIN ... END
```

**AST**：各 CREATE 系列 Stmt 结构体新增 `IfNotExists bool` 字段（CreateTableStmt / CreateViewStmt / CreateMatViewStmt / CreateIndexStmt / CreateUserStmt / CreateRoleStmt / CreateTriggerStmt / CreateProcedureStmt）。

**解析**：新增统一 helper `parseIfNotExists() (bool, error)`，模式与 `DROP ... IF EXISTS` 一致，仅中间多一个 `NOT`：

```
IF → NOT（按文本匹配） → EXISTS → 置 true
```

各 `parseCreate*` 在消费对象类型关键字之后、解析对象名之前调用该 helper。

**执行语义**：对象**已存在时静默跳过**（返回空 Result，不覆盖、不报错）；不存在时走正常创建。边界：
- `CREATE TABLE IF NOT EXISTS`：同名表存在 → 跳过（不覆盖）。
- `CREATE VIEW IF NOT EXISTS`：同名视图存在 → 跳过；同名**表**存在 → 仍报命名冲突（与 MySQL 语义一致：名字被非视图对象占用不算"视图已存在"）。
- `CREATE MATERIALIZED VIEW IF NOT EXISTS`：同名物化视图存在 → 跳过；同名表/视图存在 → 仍报冲突。
- INDEX / USER / ROLE / TRIGGER / PROCEDURE：对应对象存在 → 跳过。
- 前置校验（如引用表不存在、外键校验等）先于存在性检查执行，`IF NOT EXISTS` 仅豁免"对象已存在"错误。

### 1.3 测试方案

- CREATE TABLE：不存在创建成功；已存在第二次执行静默成功（不报错、不覆盖原表，原数据仍在）；与 DROP TABLE IF EXISTS 组成幂等脚本（重复执行 CREATE IF NOT EXISTS + DROP IF EXISTS 均不报错）。
- CREATE VIEW：不存在创建成功；已存在跳过；同名表冲突仍报错。
- CREATE INDEX / USER / ROLE：已存在跳过。
- parser AST：`CREATE TABLE IF NOT EXISTS t(id INT)` 解析后 `IfNotExists == true`；不带 IF 时 `false`。

---

## 第二节：CREATE TABLE AS SELECT（CTAS）

### 2.1 现状

- 解析层：`parseCreate` 的 TABLE 分支在对象名后直接 `expect(tokLParen)`，遇到 `AS SELECT` 报 unexpected token；无任何 CTAS 分支。
- 执行层：无 SELECT 结果建表的执行路径。
- 可复用基建（M15 物化视图已验证）：
  - `execCreateMatView`：单事务内 `execSelect` → `matViewColumnNames` 列名归一化 → `inferMatViewSchema` 类型推断 → 注册表元数据 → `writeMatViewRows` 落盘行的完整闭环。
  - `inferMatViewColType`：单列类型推断（全 INT→INT；INT/DECIMAL→DECIMAL；全 DATE→DATE；全 BLOB→BLOB；其余→TEXT；混合不支持的组合报错）。
  - `normalizeMatViewValue` / `coerceValue`：值归一化为目标列类型。

### 2.2 设计

**语法**：

```
CREATE TABLE [IF NOT EXISTS] <name> AS <SELECT>
```

- `<SELECT>` 复用 `parseSelect`，天然兼容 `SELECT *`、表达式投影（T40 算术表达式）、聚合（MAX/MIN/COUNT/SUM/AVG）、`DISTINCT`、`GROUP BY`、`WHERE`、`ORDER BY`、`LIMIT`、别名列。
- 与 `IF NOT EXISTS` 组合：`CREATE TABLE IF NOT EXISTS t AS SELECT ...`，表已存在 → 静默跳过（不执行 SELECT、不建表）。

**执行流程**（`execCreateTableAsSelect`，参照 `execCreateMatView` 的单事务模式）：

1. 存在性检查：`information_schema` 拒绝；同名表存在时按 `IfNotExists` 跳过或报错。
2. 事务包装：非显式会话事务时 `beginTx` 包一层，保证"建表 + 插数 + 元数据持久化"原子；失败回滚不残留表。
3. `execSelect` 执行 SELECT 取结果集（列名 + 行）。
4. `matViewColumnNames` 归一化列名（去限定前缀；重名保留完整限定名；空列名报错）。
5. `inferMatViewSchema` 由结果值推断列类型（空结果集 → 全部 TEXT，但列定义仍按 SELECT 投影列生成，建表成功）。
6. 构造 `TableMeta`：列定义 = 推断结果；无显式主键按项目惯例 `PK = 首列`（与普通 `CREATE TABLE` 一致）。
7. `saveTables` 注册表元数据（同事务 Put）。
8. `writeCTASRows` 逐行编码写入（`encodeRow` + `pkBytes(首列)` + `locateRow` + `rowKeyAt` + `tx.Put`），与 `writeMatViewRows` 同机制但不附加隐藏主键列。
9. 事务提交（非显式事务）后 `broadcastDDL(DDLOpCreate)` 广播集群元数据变更（与普通 `CREATE TABLE` 一致；显式事务中由用户 COMMIT 后不广播，与现有 DDL 行为对齐）。

**类型推断规则**（与物化视图一致）：

| 结果值组合 | 推断列类型 |
|---|---|
| 全 INT | INT |
| INT/DECIMAL 混合或全 DECIMAL | DECIMAL |
| 全 DATE | DATE |
| 全 BLOB | BLOB |
| 其余（含 TEXT、混合标量） | TEXT（字符串化） |
| DATE+INT、DECIMAL+TEXT 等不可混合组合 | 报错（不建表，事务回滚） |

**失败回滚**：SELECT 引用不存在的表、类型推断失败、写行失败（编码/路由错误）时整个事务回滚，目标表不残留。

### 2.3 测试方案

- 基础投影：`CREATE TABLE t2 AS SELECT id, name FROM t1`，数据与列类型正确。
- `SELECT *`：全列复制。
- 表达式投影：`SELECT id, price * qty AS total`（T40 算术表达式），total 推断 DECIMAL。
- 聚合 + GROUP BY：`SELECT age, COUNT(*) AS c FROM t GROUP BY age`，c 推断 INT。
- WHERE 过滤：仅满足条件的行进入新表。
- 空结果：`SELECT ... WHERE 1=0` 建表成功、0 行、列定义存在。
- IF NOT EXISTS 组合：目标表已存在时跳过（原表数据不变）。
- 类型推断断言：INT 列 → INT；字符串列 → TEXT；含小数列 → DECIMAL。
- 失败回滚：`CREATE TABLE t AS SELECT * FROM no_such_table` 报错且 `t` 不存在。
- parser AST：`CREATE TABLE t AS SELECT id FROM u` 解析后 `Select != nil`、`Columns` 为空。

---

## 关联改动文件

| 文件 | 改动 |
|---|---|
| `pkg/sql/ast.go` | CREATE 系列 Stmt 加 `IfNotExists`；CreateTableStmt 加 `Select` |
| `pkg/sql/parser.go` | 新增 `parseIfNotExists`；各 `parseCreate*` 接入；TABLE 分支加 CTAS 解析 |
| `pkg/sql/executor.go` | `execCreateTable` 加 IfNotExists 跳过与 CTAS 分发；新增 `execCreateTableAsSelect`、`writeCTASRows`；`execCreateUser`/`execCreateIndex`/`execCreateView` 加 IfNotExists |
| `pkg/sql/matview.go` | `execCreateMatView` 加 IfNotExists 跳过 |
| `pkg/sql/rbac.go` | `execCreateRole` 加 IfNotExists 跳过 |
| `pkg/sql/trigger.go` | `execCreateTrigger` 加 IfNotExists 跳过 |
| `pkg/sql/procedure.go` | `execCreateProcedure` 加 IfNotExists 跳过 |
| `pkg/sql/t41_ddl_test.go` | 新增 T41 单元与集成测试 |
| `README.md` | Status 追加 T41 描述；Roadmap 加条目；版本 v0.29.0-alpha |
*（内容由AI生成，仅供参考）*

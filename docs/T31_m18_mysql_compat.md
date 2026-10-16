# T31 M18 MySQL 兼容续集：Prepared Statement + Information Schema

## 1. 背景与目标

M18 是 MySQL 兼容性的第二个里程碑。M18-A（显式事务 ID 跨节点传播）已在既有会话结构中落地（`Session.curTxID`），本轮补齐剩余两个能力：

- **M18-B Prepared Statement**：`PREPARE` / `EXECUTE` / `DEALLOCATE PREPARE` 三语句闭环，支持 `?` 占位符与 `@var` 用户变量，为高频 SQL 复用与安全参数化提供 SQL 层基础。
- **M18-C Information Schema**：只读虚拟库 `information_schema`，暴露 `tables` / `columns` / `schemata` / `statistics` 四张视图，支持基础列投影与等值过滤，便于生态工具（mysqldump、监控探针）发现元数据。

## 2. 设计原则

1. **沿用四层架构**：lexer → ast → parser → executor 逐层新增，不绕过任何一层。
2. **协议中立**：prepared statement 为纯 SQL 层能力，文本行协议与嵌入式 `Execute` 均可直接使用。
3. **零破坏**：既有语句、CSV 导入导出、事务行为完全不变；新语法为增量扩展。
4. **无第三方依赖**：离线环境，全部实现基于标准库。

## 3. M18-B Prepared Statement

### 3.1 语法

```
PREPARE <name> FROM '<sql-text>'          -- 编译并缓存
EXECUTE <name> [USING @a [, @b ...]]      -- 绑定参数并执行
DEALLOCATE PREPARE <name>                 -- 释放缓存
SET @var = <literal>                      -- 用户变量赋值（扩展）
SELECT @var ...                           -- 用户变量读取（扩展）
```

- `<name>` 为会话级标识符，同名重新 `PREPARE` 覆盖旧缓存；`EXECUTE` / `DEALLOCATE` 引用不存在的名称报错。
- SQL 文本内可用 `?` 作为参数占位符；占位符个数与 `USING` 参数个数必须一致，否则报错。
- 用户变量 `@var` 通过扩展后的 `SET` 赋值，`EXECUTE ... USING @var` 读取；变量值与会话绑定。

### 3.2 词法（lexer）

- 新增关键字：`PREPARE`、`EXECUTE`、`DEALLOCATE`、`USING`（进入 keywords 表，大小写不敏感）。
- 新增 token：`tokQuestion`（`?`）——作为参数占位符，仅允许出现在 PREPARE 的 SQL 文本解析结果中。
- 用户变量 `@var`：复用既有 `tokVar`（M15-3 存储过程变量引入，文本保留 `@` 前缀）。

### 3.3 语法树（ast）

```
type PrepareStmt struct { Name string; SQL  string }
type ExecuteStmt struct { Name string; Args []string }  // Args = 用户变量名（含 @）或字面量
type DeallocateStmt struct { Name string }
```

### 3.4 解析（parser）

- `parseStmt` 增加 `PREPARE` / `EXECUTE` / `DEALLOCATE` 分发。
- `PREPARE name FROM '<sql>'`：name 取 tokIdent，SQL 文本取 tokString。
- `EXECUTE name [USING arg [, arg ...]]`：arg 接受 tokVar（`@x`）或字面量（数字/字符串）。
- `DEALLOCATE PREPARE name`：`DEALLOCATE` 后强制要求 `PREPARE` 关键字。
- `parseValue` 增加 `tokQuestion` 分支，返回参数占位节点。

### 3.5 执行（executor）

- `Session` 增加字段：

```
prepared map[string]*PreparedPlan   // 会话级 prepared 缓存
userVars map[string]Value           // 会话级用户变量 @var
type PreparedPlan struct {
    Stmt  Stmt      // 已解析 AST（含 ? 占位）
    Slots []*Value  // 占位符槽位（按出现顺序）
}
```

- `execPrepare`：对 SQL 文本调 `Parse` 得 AST；深度遍历 AST 收集 `?` 占位符（替换为槽位引用）；按名称缓存。
- `execExecute`：取缓存 plan；解析 `USING` 参数（`@var` 查 userVars，字面量直接转换）；按顺序写入 Slots；执行 plan.Stmt；返回结果。
- `execDeallocate`：删除缓存；不存在报错。
- `execSet` 扩展：变量名以 `@` 开头 → 写入 `userVars`（字面量转 Value）；以 `@` 开头的变量查询（SELECT 列表）→ 读取 `userVars`。

### 3.6 安全与边界

- PREPARE 只接受完整单条 SQL（解析失败即报错，不落缓存）。
- 占位符仅存在于 PREPARE 文本解析产物；普通 SQL 中出现裸 `?` 报语法错误。
- 用户变量不参与 binlog 复制（会话状态，非表数据）。

## 4. M18-C Information Schema

### 4.1 虚拟视图

| 视图 | 列 | 说明 |
|---|---|---|
| `information_schema.schemata` | SCHEMA_NAME | 数据库名（固定 `default`） |
| `information_schema.tables` | TABLE_NAME, TABLE_TYPE, TABLE_ROWS | TABLE_TYPE=`BASE TABLE`；TABLE_ROWS=估算行数（实时 Count） |
| `information_schema.columns` | TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE | 列序、类型（INT/TEXT/DATE/DECIMAL/BLOB）、可空性 |
| `information_schema.statistics` | TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE | 主键与二级索引（含唯一索引） |

### 4.2 查询语法

```
SELECT <cols|*> FROM information_schema.<view> [WHERE <col> = <literal> [AND ...]]
```

- 支持列投影（`*` 或列名列表）；不支持表达式、JOIN、GROUP BY（虚拟表路径拒绝高级子句并报错）。
- WHERE 仅支持 `col = literal` 等值条件（AND 连接），用于按 TABLE_NAME 等过滤。

### 4.3 解析扩展

- FROM 表名支持限定名 `db.table`：`parseSelect` 表名处遇 `tokDot` 拼接为 `db.table` 全名（普通表名不含点，零冲突）。

### 4.4 执行扩展

- `execSelect` 在 CTE/视图展开后、高级路径判断前检测：`s.From` 小写后以 `information_schema.` 开头 → 走 `execInfoSchema(sess, s)`。
- `execInfoSchema`：
  1. `loadTables` 读取全部表元数据；
  2. 按视图构造虚拟 `Result{Columns, Rows}`（类型映射：TABLE_ROWS 为 INT，其余为 TEXT）；
  3. 应用 WHERE 等值过滤（列名小写匹配）；
  4. 应用列投影（`*` 原样，列名按不区分大小写匹配视图列，未知列报错）。

### 4.5 边界

- information_schema 为只读虚拟库，`CREATE/DROP/INSERT/UPDATE` 等落到该库直接报错。
- 查询始终反映当前快照（读元数据事务），与表数据一致性由底层存储保证。

## 5. 测试计划

- `pkg/sql/m18_prepared_test.go`：
  - PREPARE/EXECUTE 基础（SELECT 常量、INSERT 参数化多行）；
  - USING 参数个数不匹配报错；未 PREPARE 先 EXECUTE 报错；同名覆盖；DEALLOCATE 后再 EXECUTE 报错；
  - 用户变量 SET/读取/跨语句传递；字符串参数含引号转义；
  - 普通 SQL 裸 `?` 报语法错误。
- `pkg/sql/m18_infoschema_test.go`：
  - schemata/tables/columns/statistics 四视图全量列与投影；
  - WHERE TABLE_NAME 过滤；未知视图/未知列报错；限定名 FROM 解析；
  - 对 information_schema 写操作报错。
- 全仓 `go test ./...` 回归全绿。

## 6. 文档与版本

- 本设计对应 README Status 更新为 M18 cluster，版本号按既有 alpha 语义递增（见 README 更新记录）。

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_15c52666c57711f19063525400393707
    ReservedCode1: jgZEH9JNVXNTUwsgxsVY9hFvVNeBKy+nEyqJFGFDzcdbJcsC78SJkgzGW0pNvQRaW66nFW9HILRMAmAHoExTYNzV1xl7+wUn5evtiGh9NtVFWr+KnDH039sul74VtYecPRC0XlFz5o84EkX3OjqLuu1Rf+oB6MUEqnDQJ9DmwzECB2yC7OlmS1NEkVY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_15c52666c57711f19063525400393707
    ReservedCode2: jgZEH9JNVXNTUwsgxsVY9hFvVNeBKy+nEyqJFGFDzcdbJcsC78SJkgzGW0pNvQRaW66nFW9HILRMAmAHoExTYNzV1xl7+wUn5evtiGh9NtVFWr+KnDH039sul74VtYecPRC0XlFz5o84EkX3OjqLuu1Rf+oB6MUEqnDQJ9DmwzECB2yC7OlmS1NEkVY=
---

# T28 M15 SQL 功能增强簇（3/3）：存储过程

> 版本：v5.0-P16-M15, v0.18.0-alpha（P1-3 补齐：v1.6.0 P1-3 存储过程受限子集，2026-10-24）
> 日期：2026-10-12
> 范围：`pkg/sql`（ast.go 新增 ParamMeta/ProcedureMeta/CreateProcedureStmt/DropProcedureStmt/CallStmt/ShowProceduresStmt，parser.go 新增 parseCreateProcedure/parseDropProcedure/parseCall 与 CREATE/DROP/SHOW/CALL 分发，lexer.go 新增 PROCEDURE/PROCEDURES/CALL 关键字与 tokVar 词法（`@name`），executor.go 新增四个执行器分发与 Executor.procDepth 递归防护字段，新增 procedure.go 与 procedure_test.go）、`docs/T28_m15_procedure.md`（本文档）。
> P1-3 补齐范围：变量声明 DECLARE、表达式赋值、IF/ELSIF/ELSE、WHILE/LOOP/FOR/REPEAT、游标（DECLARE CURSOR/OPEN/FETCH/CLOSE）、OUT/INOUT 参数；ast.go ParamMeta 增加 Mode 字段，executor.go execCall 拆 execCallOut 支持参数回写，procedure.go 新增游标节点与 procCursorState，新增 proc_subset_test.go（10 用例）。
> 闭环：CGO 环境下 `go build ./...` + `go test -count=1 ./pkg/sql/...` 全绿（含 12 个 M15 存储过程用例 + 10 个 P1-3 受限子集用例）
> 提交：`M15: stored procedure`（本地，未 push）；P1-3：`feat(sql): v1.6.0 P1-3 存储过程受限子集补齐`（双远端）

## 背景
M15 SQL 功能增强簇按 Roadmap 依次落地三个子功能：物化视图（1/3，已闭环）、触发器（2/3，已闭环）、存储过程（3/3，本次）。存储过程在触发器已提交的 8a16cb4 之上增量开发，复用同一套 AST/parser/executor 分文件结构与 metaKey + JSON 元数据持久化模式，与既有虚拟视图、物化视图、触发器共存不冲突。

## 设计

### 存储层：独立目录 `m:procedures`
- **元数据** `ProcedureMeta{Name, Params []ParamMeta, Body}`：`ParamMeta{Name, Type}`，以 JSON 序列化持久化于独立目录键 `proceduresKey = "m:procedures"`（新增），**不写入** `m:tables` / `m:views` / `m:matviews` / `m:triggers`，各对象命名空间互相独立、互不干扰；
- **Body 原文持久化**：过程体以原始 SQL 文本保存，CALL 时切分为语句序列后逐条执行。

### 执行器
| 语句 | 行为 |
|---|---|
| `CREATE PROCEDURE <name> [( [IN|OUT|INOUT] <param> <TYPE> [, ...] )] BEGIN <stmt>; [<stmt>; ...] END` | 校验名称非空、参数个数/类型合法、体非空、体可切分为一条或多条受支持语句（INSERT/UPDATE/DELETE/SELECT/局部变量赋值/控制流/游标/CALL 嵌套受限于递归防护）、体语句可解析、名称唯一 → 注册 `m:procedures` 持久化，单事务提交 |
| `DROP PROCEDURE [IF EXISTS] <name>` | 仅移除元数据注册，不动任何表数据；IF EXISTS 对不存在名称静默成功 |
| `SHOW PROCEDURES` | 按名称排序返回 `procedure / params / statement` 三列，`params` 为 `name type` 逗号拼接，`statement` 回显体原文 |
| `CALL <name> [( <arg> [, <arg> ...] )]` | 实参按位置一一对应代入参数（个数严格匹配，类型不强制、按字面量代入后由体语句自身类型规则校验）；IN 实参可为字面量或 `@var`（取当前值）；OUT/INOUT 实参必须为 `@var`，调用结束后回写到调用方作用域/会话用户变量；无参过程可写 `CALL name` 或 `CALL name()` |

### 参数与局部变量（P1-3 补齐后全量支持）
- **IN 参数**：`(IN id INT)` 或 `(id INT)` 声明，过程体内以 `@id` 引用；CALL 实参为字面量（INT/DECIMAL/TEXT）或 `@var`（取当前会话/调用方值）；
- **OUT / INOUT 参数**：`(OUT b INT)` / `(INOUT c INT)` 声明，`ParamMeta.Mode` 记录（空 = IN）；过程体内以 `@b` / `@c` 读写；CALL 实参必须为 `@var`（非变量实参报 `requires a @variable argument`），调用结束后 OUT 按默认值初始化、INOUT 带调用方现值进入，结束后回写到调用方作用域（嵌套 CALL 回写父 env）或会话 `userVars`（顶层 CALL，可用 `SELECT @var` 读取）；
- **局部变量**：`DECLARE @x [TYPE] [DEFAULT expr];` 声明（类型可选，DEFAULT 缺省为零值），`SET @x = <expr>;` 更新本次调用局部作用域；SET 支持字面量、`@var` 引用与算术表达式（`@sum + @i`、`@a * 2` 等，复用 evalArith 求值）；每次 CALL 独立作用域、不跨调用保持；
- **变量引用**：`@name` 在普通 SQL 语句文本中按名匹配替换为当前值后重新 Parse 走标准执行路径；控制流/赋值语句内 `@name` 走 procRunner 解释求值；未定义的 `@name` 在执行期报 `unknown procedure variable`，创建期预校验仅验证语句可解析。

### 多语句体与递归防护
- **多语句体**：`splitBodyStatements` 按顶层分号切分（括号深度与 `CASE ... END` 内的分号不计入切分），逐条顺序执行；任一条失败即终止并返回 `procedure <name> failed: ...`；
- **递归防护**：`Executor.procDepth` 计数器，过程体再次 CALL 时不再展开，报 `procedure recursion not supported`，避免无限递归（文档化限制；嵌套 CALL 亦受同一防护）。

### 过程体支持范围（P1-3 补齐后）
| 支持 | 说明 |
|---|---|
| `INSERT INTO ... VALUES (...)` | 值限字面量 / `@var` 引用；行数不限 |
| `UPDATE ... SET col = <字面量> [WHERE ...]` | SET 值限字面量 / `@var` 引用 |
| `DELETE FROM ... [WHERE ...]` | 条件删除 |
| `SELECT ...` | 可解析并执行，但 **CALL 不返回 SELECT 结果集**（CALL 返回固定 `AffectedRows=1`，文档化限制）；游标查询必须为 `SELECT` |
| `SET @x = <expr>` | 局部变量赋值（字面量 / `@var` / 算术表达式） |
| `DECLARE @x [TYPE] [DEFAULT expr];` | 局部变量声明 |
| `IF <cond> THEN <stmt>; [ELSIF <cond> THEN ...] [ELSE <stmt>;] END IF;` | 条件分支；cond 支持比较与 `@var` |
| `WHILE <cond> DO ... END WHILE;` / `WHILE <cond> LOOP ... END LOOP;` | 前置判断循环 |
| `REPEAT <body> UNTIL <cond> END REPEAT;` | 后置判断循环 |
| `[label:] LOOP ... END LOOP [label];` / `[label:] FOR @i IN <from> .. <to> LOOP ... END LOOP [label];` | 无限循环 / 计数循环 |
| `ITERATE;` / `LEAVE;` | 循环内跳过 / 退出（须在循环体内） |
| `DECLARE c CURSOR FOR SELECT ...;` `OPEN c;` `FETCH c INTO @a [, @b ...];` `CLOSE c;` | 游标：OPEN 执行查询收集行集，FETCH 顺序消费写入变量，行集耗尽报 `fetch out of rows`；创建期静态校验（未声明引用 / 非 SELECT / FETCH 缺变量报错），重复声明拒绝 |
| `CALL ...` | 受 procDepth 递归防护限制；支持 OUT/INOUT 实参回写 |
| 不受支持 | 事务控制（BEGIN/COMMIT/ROLLBACK）、动态 SQL、异常处理器（DECLARE HANDLER）仍在创建期拒绝 |

### 与物化视图 / 触发器 / 虚拟视图共存
- 命名空间隔离：存储过程名称与表 / 虚拟视图 / 物化视图 / 触发器名称互不冲突，独立注册；
- 过程体引用虚拟视图名走 `expandViewsInner` 正常展开；物化视图保持快照语义不受 CALL 影响；触发器不受 CALL 影响、CALL 内 DML 正常触发既有触发器（共用同一 DML 执行路径）。

## 语法示例
```sql
-- 无参多语句过程：插入用户 + 更新审计计数
CREATE PROCEDURE p_ins BEGIN
  INSERT INTO users (id, name, age) VALUES (4, 'dave', 40);
  UPDATE audit SET cnt = 100;
END;

-- IN 参数过程：按 id 插入
CREATE PROCEDURE p_add (IN id INT) BEGIN
  INSERT INTO users (id, name, age) VALUES (@id, 'new', 1);
END;

-- 局部变量：赋值后引用
CREATE PROCEDURE p_var BEGIN
  SET @x = 5;
  INSERT INTO t (v) VALUES (@x);
END;

-- P1-3：变量声明 + WHILE 累加 + IF/ELSE（算术表达式赋值）
CREATE PROCEDURE p_calc (IN n INT) BEGIN
  DECLARE @i INT DEFAULT 0;
  DECLARE @sum INT DEFAULT 0;
  WHILE @i < @n DO
    SET @sum = @sum + @i;
    SET @i = @i + 1;
  END WHILE;
  IF @sum > 5 THEN
    INSERT INTO results (v) VALUES (@sum);
  ELSE
    INSERT INTO results (v) VALUES (-1);
  END IF;
END;

-- P1-3：游标遍历（DECLARE CURSOR + OPEN + FETCH + CLOSE）
CREATE PROCEDURE p_cur BEGIN
  DECLARE c CURSOR FOR SELECT id, name FROM users ORDER BY id;
  DECLARE @id INT;
  DECLARE @nm TEXT;
  OPEN c;
  FETCH c INTO @id, @nm;
  INSERT INTO picked (id, name) VALUES (@id, @nm);
  CLOSE c;
END;

-- P1-3：OUT/INOUT 参数（CALL 后回写 @var）
CREATE PROCEDURE p_io (IN a INT, OUT b INT, INOUT c INT) BEGIN
  SET @b = @a * 2;
  SET @c = @c + @a;
END;
SET @inout = 5;
CALL p_io(10, @out, @inout);  -- @out=20, @inout=15，可用 SELECT @out 读取

CALL p_ins;
CALL p_ins();
CALL p_add(10);
SHOW PROCEDURES;
DROP PROCEDURE p_ins;
DROP PROCEDURE IF EXISTS p_ins;
```

## 测试清单（procedure_test.go 12 用例 + proc_subset_test.go 10 用例，全绿）
| 用例 | 覆盖点 |
|---|---|
| TestM15CreateCallShowDropProcedure | 创建 / SHOW / CALL / DROP / DROP 后不可调用 |
| TestM15CallWithInParams | IN 参数代入、实参个数校验 |
| TestM15ProcedureMultiStatementBody | 多语句顺序执行、失败终止 |
| TestM15ProcedureLocalVariable | SET 局部变量赋值与读取 |
| TestM15ProcedureDropIfExistsAndDuplicate | IF EXISTS / 重复定义拒绝 |
| TestM15ProcedureSyntaxError | 语法错误（缺 END / 非法参数类型等） |
| TestM15ProcedureUnknownVariable | 未定义变量执行期报错 |
| TestM15ProcedureRecursionGuard | 递归 CALL 防护 |
| TestM15ProcedureCoexistWithOthers | 与视图 / 物化视图 / 触发器共存 |
| TestM15ProcedurePersistAcrossCalls | 元数据持久化、跨调用存在 |
| TestM15ProcedureCallRepeatedly | CALL 可重复执行 |
| TestM15ProcedureLiteralAndCaseEnd | 字符串字面量 / CASE 表达式内分号切分正确 |
| TestP13ProcDeclareIfWhile | DECLARE + WHILE 累加 + IF/ELSE 分支 |
| TestP13ProcWhileIterateNestedIf | WHILE + ITERATE + IF 嵌套回归 |
| TestP13ProcCursorFetch | 游标 OPEN/FETCH/CLOSE 顺序消费 |
| TestP13ProcCursorWithVarQuery | 游标查询引用 IN 参数 |
| TestP13ProcOutInoutParams | OUT/INOUT 参数回写 |
| TestP13ProcInParamTakesVarValue | IN 实参为 @var 取当前值 |
| TestP13ProcRepeatIterateRegression | REPEAT + ITERATE 回归共存 |
| TestP13ProcCursorSyntaxErrors | 未声明游标 / 非 SELECT / FETCH 缺变量 / 实参非 @var 拒绝 |
| TestP13ProcCursorFetchOutOfRows | FETCH 越界运行期报错 |
| TestP13ProcCursorDuplicateDecl | 游标重复声明拒绝 |
| TestP13ProcValueKind | FETCH 值落库保持数值类型 |

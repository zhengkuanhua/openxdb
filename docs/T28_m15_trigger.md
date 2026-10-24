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

# T28 M15 SQL 功能增强簇（2/3）：触发器

> 版本：v5.0-P16-M15, v0.18.0-alpha, v1.8.0（行级语义增强）
> 日期：2026-10-12（v1.8.0 更新：2026-10-24）
> 范围：`pkg/sql`（ast.go 新增 CreateTriggerStmt/DropTriggerStmt/ShowTriggersStmt 与 TriggerMeta.RowLevel，parser.go 新增 parseCreateTrigger/parseDropTrigger 与 CREATE/DROP/SHOW 分发、FOR EACH ROW/STATEMENT 粒度解析、NEW./OLD. 行引用（TRIGREF）识别（parseValue 与 parseExprPrimary），lexer.go 新增 TRIGGER/TRIGGERS/BEFORE/AFTER/EACH/ROW/FOR/STATEMENT 关键字，executor.go 新增三个执行器分发、Executor.triggerMaxDepth 配置与 Session.triggerStack 递归栈、execInsert/execUpdate/execDelete 的 BEFORE/AFTER 触发钩子与行快照收集、autocommit 下触发器存在时会话事务提升（体写同事务原子性），新增 trigger.go / trigger_row.go / trigger_test.go / trigger_row_test.go）、`docs/T28_m15_trigger.md`（本文档）。
> 闭环：CGO 环境下 `go build ./...` + `go test -count=1 ./pkg/sql/...` 全绿（含 13 个 M15 触发器用例 + 8 个 v1.8.0 行级用例）；pkg/db、pkg/sql、pkg/replication 三包回归全绿
> 提交：`M15: trigger`（本地，未 push）；v1.8.0 行级语义增强（`feat(sql): v1.8.0 触发器行级语义（FOR EACH ROW/NEW OLD 引用/体回滚递归策略）`，SHA 43f8e5e，gitee + GitHub(443 origin) 双端一致）
## 背景
M15 SQL 功能增强簇按 Roadmap 依次落地三个子功能：物化视图（1/3，已闭环）、触发器（本次 2/3）、存储过程（3/3）。触发器在物化视图已 push 的 1399bbc 之上增量开发，复用同一套 AST/parser/executor 分文件结构与 metaKey + JSON 元数据持久化模式，与既有虚拟视图、物化视图共存不冲突。

## 设计

### 存储层：独立目录 `m:triggers`
- **元数据** `TriggerMeta{Name, Timing, Event, Table, Body}`：以 JSON 序列化持久化于独立目录键 `triggersKey = "m:triggers"`（新增），**不写入** `m:tables` / `m:views` / `m:matviews`，三类对象命名空间互相独立、互不干扰；
- **Body 原文持久化**：触发器体以原始 SQL 文本保存，DML 触发时重新 `Parse` + `execStmt`，与普通语句共用同一执行路径，天然复用物化视图/虚拟视图展开逻辑。

### 执行器
| 语句 | 行为 |
|---|---|
| `CREATE TRIGGER <name> BEFORE\|AFTER INSERT\|UPDATE\|DELETE ON <table> [FOR EACH ROW\|FOR EACH STATEMENT] <body>` | 校验名称非空、时机（BEFORE/AFTER）、事件（INSERT/UPDATE/DELETE）、源表存在、体非空且可解析、体为受支持语句（UPDATE 条件更新 / INSERT 单行）、体引用表存在、UPDATE 体 SET 列存在、名称唯一、行引用事件语义与列存在性（v1.8.0）→ 注册 `m:triggers` 持久化（含 RowLevel 粒度），单事务提交。粒度缺省（无子句）按 FOR EACH STATEMENT（语句级） |
| `DROP TRIGGER [IF EXISTS] <name>` | 仅移除元数据注册，不动任何表数据；IF EXISTS 对不存在名称静默成功 |
| `SHOW TRIGGERS [FROM <table>]` | 按名称排序返回 `trigger / event / table / timing / statement` 五列，`statement` 回显体原文，`FROM <table>` 过滤源表 |

### DML 自动触发（`fireTriggers`）
- **匹配规则**：时机 + 事件 + 源表名三者全部一致；BEFORE 在写入提交前触发、AFTER 在写入提交后触发，`execInsert` / `execUpdate` / `execDelete` 三处钩子对称；
- **触发粒度（v1.8.0）**：`FOR EACH ROW` 触发器按受影响行**逐行触发**，每行携带行上下文（行快照 `triggerRow`：INSERT 仅 NEW 行、DELETE 仅 OLD 行、UPDATE NEW+OLD 双行）；`FOR EACH STATEMENT`（缺省/显式）保持语句级——受影响行数 > 0 时触发一次，兼容 M15 旧语义；
- **行上下文（v1.8.0）**：行快照由 `buildRow` 按表列序物化；触发器体解析后先经 `replaceTrigRefs` 将 `NEW.col` / `OLD.col` 替换为快照值再执行，支持表达式/赋值/条件位置（SET、VALUES、WHERE、CASE 等）；引用未定义列、越权事件（INSERT 体用 OLD、DELETE 体用 NEW）在**创建期**即校验报错；
- **体写同事务原子性（v1.8.0）**：触发器体与主 DML 在**同一事务**内执行（autocommit 下检测到行级触发器时提升会话事务），体写失败主 DML 一并回滚，不再出现"体已提交、主语句回滚"的不一致；主 DML 失败时体写入同步回滚；
- **递归策略（v1.8.0）**：`Session.triggerStack` 同键递归栈抑制直接递归（同一触发器在自身触发链上再次触发直接跳过，防无限递归）；`Executor.triggerMaxDepth`（默认 1）允许**链式触发**——不同触发器的嵌套触发深度不超过上限（默认允许 1 层链式嵌套，多级链式按配置放开）；M15 旧 `triggerDepth` 全局抑制移除；
- **失败语义**：BEFORE 体执行失败返回 `trigger <name> failed: ...` 并阻断主 DML（主语句不提交）；AFTER 体失败同样报错，且因同事务语义主 DML 一并回滚。

### 触发器体支持范围（文档化限制）
| 支持 | 说明 |
|---|---|
| `UPDATE <table> SET col = <字面量/行引用> [WHERE ...]` | 条件更新，SET 值限字面量与 `NEW.col` / `OLD.col` 行引用（v1.8.0）；WHERE 支持既有比较表达式及行引用 |
| `INSERT INTO <table> (cols) VALUES (<字面量/行引用>)` | 单行插入，值限字面量与行引用（v1.8.0） |
| 行级触发 | `FOR EACH ROW` 逐行触发，行上下文 NEW/OLD 按事件可用（v1.8.0） |
| 不受支持 | 算术表达式（如 `cnt = cnt + 1`）、SELECT/多语句体、NEW/OLD 行引用越权事件与未定义列（创建期拒绝） |

### 与物化视图 / 虚拟视图共存
- 命名空间隔离：触发器名称与表 / 虚拟视图 / 物化视图名称互不冲突，独立注册；
- 触发器体引用虚拟视图名走 `expandViewsInner` 正常展开；物化视图保持快照语义不受源表 DML 触发影响，`REFRESH MATERIALIZED VIEW` 后触发器仍正常；
- 源表 DROP 后其上触发器元数据保留（可执行 DROP TRIGGER 清理，文档化限制）。

## 语法示例
```sql
-- 审计计数：users 每被插入一次（语句级），audit.cnt 置 100
CREATE TRIGGER tr_ins AFTER INSERT ON users FOR EACH ROW
  UPDATE audit SET cnt = 100;

-- BEFORE 时机：users 插入前先置位标志
CREATE TRIGGER tr_pre BEFORE INSERT ON users FOR EACH ROW
  UPDATE pre SET flag = 1;

-- INSERT 体：写审计日志
CREATE TRIGGER tr_log AFTER INSERT ON users FOR EACH ROW
  INSERT INTO log (msg) VALUES ('row inserted');

-- v1.8.0 行级触发 + NEW 引用：每插入一行，audit.cnt 置为该行的 id
CREATE TRIGGER tr_row AFTER INSERT ON users FOR EACH ROW
  UPDATE audit SET cnt = NEW.id;

-- v1.8.0 UPDATE 双引用：audit 记录新老年龄
CREATE TRIGGER tr_upd AFTER UPDATE ON users FOR EACH ROW
  INSERT INTO audit (old_age, new_age) VALUES (OLD.age, NEW.age);

-- v1.8.0 显式语句级（等价缺省）：整语句触发一次
CREATE TRIGGER tr_stmt AFTER INSERT ON users FOR EACH STATEMENT
  UPDATE audit SET cnt = 100;

SHOW TRIGGERS;
SHOW TRIGGERS FROM users;
DROP TRIGGER tr_ins;
DROP TRIGGER IF EXISTS tr_ins;
```

## 测试清单

| 用例 | 覆盖点 |
|---|---|
| `TestM15CreateShowDropTrigger` | 创建 / SHOW TRIGGERS 五列回显 / DROP 后 SHOW 为空且不再触发 |
| `TestM15AfterInsertTriggerFires` | AFTER INSERT 自动触发；批量插入语句级触发一次 |
| `TestM15BeforeInsertTriggerRunsBeforeDML` | BEFORE 时机在主 DML 提交前生效 |
| `TestM15BeforeTriggerFailureBlocksDML` | BEFORE 体执行失败阻断主 DML（主语句未生效） |
| `TestM15TriggerOnUpdate` / `TestM15TriggerOnDelete` | AFTER UPDATE / AFTER DELETE 触发；0 行受影响不触发 |
| `TestM15MultipleTriggersSameEvent` | 同一事件多个触发器按序全部执行 |
| `TestM15TriggerInsertBody` | INSERT 体写入审计日志 |
| `TestM15TriggerCoexistsWithMatViewAndView` | 虚拟视图即时可见 / 物化视图保持快照 / 触发器同时生效；REFRESH 后仍正常 |
| `TestM15TriggerRecursionGuard` | 触发器体写同一源表时递归防护，仅执行一次不死循环 |
| `TestM15ShowTriggersFromTable` | `SHOW TRIGGERS FROM <table>` 按源表过滤 |
| `TestM15CreateTriggerErrors` | 缺时机 / 非法事件 / 缺 FOR EACH ROW / 空体 / SELECT 体拒绝 / 源表不存在 / 体表不存在 / 重名 / SET 未知列 / DROP 不存在 / DROP IF EXISTS |
| `TestM15TriggerMetaPersisted` | 元数据持久化：SHOW 的 statement 列回显体原文 |

## v1.8.0 行级语义测试矩阵（trigger_row_test.go）

| 用例 | 覆盖点 |
|---|---|
| `TestRowLevelInsertFiresPerRow` | FOR EACH ROW 逐行触发：INSERT 3 行触发 3 次（审计计数验证） |
| `TestRowLevelInsertNewRefs` | INSERT 行上下文仅 NEW 可用；SET/VALUES/WHERE 位置引用 NEW.col 物化正确 |
| `TestRowLevelUpdateNewOldRefs` | UPDATE 行上下文 NEW+OLD 双引用；审计表记录新老值差异 |
| `TestRowLevelDeleteOldRef` | DELETE 行上下文仅 OLD；删除多行逐行触发 |
| `TestRowLevelCreateTimeValidation` | 创建期校验：INSERT 体用 OLD、DELETE 体用 NEW、引用未定义列均报错 |
| `TestRowLevelBodyRollbackWithMainDML` | 体写同事务原子性：主 DML 失败时体写入同步回滚（计数为 0） |
| `TestRowLevelChainAndRecursionLimit` | 递归/链式：直接递归被同键栈抑制；不同触发器链式触发可达（默认 1 层嵌套） |
| `TestRowLevelStatementLevelExplicit` | FOR EACH STATEMENT 显式语句级回归：整语句触发一次，与缺省语义一致 |

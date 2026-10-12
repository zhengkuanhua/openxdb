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

> 版本：v5.0-P16-M15, v0.18.0-alpha
> 日期：2026-10-12
> 范围：`pkg/sql`（ast.go 新增 CreateTriggerStmt/DropTriggerStmt/ShowTriggersStmt，parser.go 新增 parseCreateTrigger/parseDropTrigger 与 CREATE/DROP/SHOW 分发，lexer.go 新增 TRIGGER/TRIGGERS/BEFORE/AFTER/EACH/ROW/FOR 关键字，executor.go 新增三个执行器分发、Executor.triggerDepth 递归防护字段与 execInsert/execUpdate/execDelete 的 BEFORE/AFTER 触发钩子，新增 trigger.go 与 trigger_test.go）、`docs/T28_m15_trigger.md`（本文档）。
> 闭环：CGO 环境下 `go build ./...` + `go test -count=1 ./pkg/sql/...` 全绿（含 13 个 M15 触发器用例），未 push
> 提交：`M15: trigger`（本地，未 push）
## 背景
M15 SQL 功能增强簇按 Roadmap 依次落地三个子功能：物化视图（1/3，已闭环）、触发器（本次 2/3）、存储过程（3/3）。触发器在物化视图已 push 的 1399bbc 之上增量开发，复用同一套 AST/parser/executor 分文件结构与 metaKey + JSON 元数据持久化模式，与既有虚拟视图、物化视图共存不冲突。

## 设计

### 存储层：独立目录 `m:triggers`
- **元数据** `TriggerMeta{Name, Timing, Event, Table, Body}`：以 JSON 序列化持久化于独立目录键 `triggersKey = "m:triggers"`（新增），**不写入** `m:tables` / `m:views` / `m:matviews`，三类对象命名空间互相独立、互不干扰；
- **Body 原文持久化**：触发器体以原始 SQL 文本保存，DML 触发时重新 `Parse` + `execStmt`，与普通语句共用同一执行路径，天然复用物化视图/虚拟视图展开逻辑。

### 执行器
| 语句 | 行为 |
|---|---|
| `CREATE TRIGGER <name> BEFORE\|AFTER INSERT\|UPDATE\|DELETE ON <table> FOR EACH ROW <body>` | 校验名称非空、时机（BEFORE/AFTER）、事件（INSERT/UPDATE/DELETE）、源表存在、体非空且可解析、体为受支持语句（UPDATE 条件更新 / INSERT 单行）、体引用表存在、UPDATE 体 SET 列存在、名称唯一 → 注册 `m:triggers` 持久化，单事务提交 |
| `DROP TRIGGER [IF EXISTS] <name>` | 仅移除元数据注册，不动任何表数据；IF EXISTS 对不存在名称静默成功 |
| `SHOW TRIGGERS [FROM <table>]` | 按名称排序返回 `trigger / event / table / timing / statement` 五列，`statement` 回显体原文，`FROM <table>` 过滤源表 |

### DML 自动触发（`fireTriggers`）
- **匹配规则**：时机 + 事件 + 源表名三者全部一致；BEFORE 在写入提交前触发、AFTER 在写入提交后触发，`execInsert` / `execUpdate` / `execDelete` 三处钩子对称；
- **触发粒度**：语句级——源语句受影响行数 > 0 时触发一次，不按行触发；
- **递归防护**：`Executor.triggerDepth` 计数器，触发器体再次写任何表时不再触发，避免无限递归；
- **失败语义**：BEFORE 体执行失败返回 `trigger <name> failed: ...` 并阻断主 DML（主语句不提交）；AFTER 体失败同样报错。

### 触发器体支持范围（文档化限制）
| 支持 | 说明 |
|---|---|
| `UPDATE <table> SET col = <字面量> [WHERE ...]` | 条件更新，SET 值限字面量（INT/DECIMAL/字符串）；WHERE 支持既有比较表达式 |
| `INSERT INTO <table> (cols) VALUES (<字面量>)` | 单行插入，值限字面量 |
| 不受支持 | 算术表达式（如 `cnt = cnt + 1`）、SELECT/多语句体、NEW/OLD 行引用、行级（FOR EACH ROW 仅语法声明，语义为语句级）均不支持并在创建期拒绝或文档化 |

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

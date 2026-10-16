---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_21dc3a3bc90611f18b7f525400393706
    ReservedCode1: 8mPVZDVTrB/piZ91abgDbUuUbsIEMqP1M0jEby8ZfMtg5ZNGX6bOVQcPxh1CtHHP4MKf69B0jeWsVnBSvJAXkwVcqf+GVz19uMRL6Judejtf77unjYb11r6ua322xFRknAp5/rMZ6Fv1b+JoTvnj9KY5TNjSiiWrsfr7md7Ig6Q4/4/B+ARdsBjHZys=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_21dc3a3bc90611f18b7f525400393706
    ReservedCode2: 8mPVZDVTrB/piZ91abgDbUuUbsIEMqP1M0jEby8ZfMtg5ZNGX6bOVQcPxh1CtHHP4MKf69B0jeWsVnBSvJAXkwVcqf+GVz19uMRL6Judejtf77unjYb11r6ua322xFRknAp5/rMZ6Fv1b+JoTvnj9KY5TNjSiiWrsfr7md7Ig6Q4/4/B+ARdsBjHZys=
---



# T42 INSERT INTO ... SELECT 语法支持

- 版本：v0.30.0-alpha
- 范围：pkg/sql（parser / ast / executor）
- 目标：补齐 `INSERT [INTO] <table> [(cols)] SELECT ...` 与 `REPLACE INTO ... SELECT` 语法：解析层识别 SELECT 源，执行层在同一事务内先执行 SELECT 取快照、再逐行灌入目标表，复用 VALUES 路径全部单行写入语义（类型 coerced / 主键冲突 / UPSERT / 索引 / 外键 / 触发器 / 自动分裂 / 2PC）。

---

## 第一节：现状

- 解析层：`parseInsertInto`（parser.go L1661 起）仅支持 `INSERT [INTO] <name> [(col,...)] VALUES (...)` 与 `REPLACE INTO`、`ON DUPLICATE KEY UPDATE` 子句；在可选列清单之后直接 `expectKeyword("VALUES")`，遇到 `SELECT` 关键字报 `expected keyword VALUES`，**无 SELECT 源分支**。
- AST：`InsertStmt`（ast.go L227）仅含 `Table / Columns / Rows / OnDup / Replace`，**无 Select 字段**。
- 执行层：`execInsert`（executor.go L1575）仅遍历 `s.Rows` 逐行 `insertRowUpsertOps`，**无 SELECT 结果灌入路径**。
- 可复用基建（T41 CTAS 已验证）：
  - `execSelect`：完整 SELECT 能力（SELECT * / 表达式投影 / 聚合 / DISTINCT / 别名 / WHERE / GROUP BY / ORDER BY / LIMIT / JOIN / 子查询 / CTE / 视图），返回 `*Result{Rows: [][]Value}`。
  - `insertRowUpsertOps`：单行写入的完整语义（buildRow 列数/类型校验 → coerceRow → 外键检查 → 主键定位 → 主键/唯一索引冲突判定 → 普通插入 / ON DUPLICATE 更新 / REPLACE 删旧插新 → 索引键 Put），供 VALUES 与 SELECT 两路径共用。
  - `buildRow`：列清单长度与值长度不匹配时报 `column count mismatch`，省略列清单时按目标表全列校验。

## 第二节：设计

### 2.1 语法

```
INSERT [INTO] <table> [(col, ...)] SELECT <select_statement>
INSERT [INTO] <table> [(col, ...)] SELECT ... [ON DUPLICATE KEY UPDATE col=val, ...]
REPLACE [INTO] <table> [(col, ...)] SELECT ...
```

- 列清单可省略（按目标表全列顺序）；列清单与 SELECT 结果列数不匹配时报错（复用 `buildRow` 的 `column count mismatch`）。
- `REPLACE INTO ... SELECT` 支持（与 REPLACE INTO ... VALUES 语义一致：冲突删旧插新，无 ON DUPLICATE 子句）。
- `INSERT INTO t SELECT ... ON DUPLICATE KEY UPDATE` 支持（MySQL 兼容；冲突行更新而非报错）。

### 2.2 AST

`InsertStmt` 新增字段：

```go
Select *SelectStmt // T42：INSERT INTO ... SELECT 源（非空时忽略 Rows，按 SELECT 结果逐行灌入）
```

### 2.3 解析

`parseInsertInto` 在可选列清单解析之后改为双分支：

- 当前 token 为关键字 `SELECT` → 调用 `parseSelect()` 解析完整 SELECT 语句（兼容 CTE / DISTINCT / 表达式 / 聚合 / JOIN / WHERE / GROUP BY / ORDER BY / LIMIT 等全部既有 SELECT 能力），类型断言为 `*SelectStmt` 存入 `stmt.Select`；
- 否则 → 原有 `VALUES` 分支不变。

两分支汇合后统一处理 `ON DUPLICATE KEY UPDATE`（仅非 REPLACE；SELECT 源同样适用）。REPLACE 分支同样识别 SELECT 关键字。

### 2.4 执行

`execInsert` 增加 SELECT 源灌入路径，事务语义与 VALUES 路径对齐：

1. **同一事务原子**：SELECT 源且 autocommit 时，先 `beginTx(sess)` 将会话提升为显式事务，`getTx` 返回同一 `curTx`，`execSelect` 复用它（autocommit=false 不再 defer Rollback）；写入与读取同事务，提交仍走 `exec2pcWrite`（分布式写 / TSO 版本记录路径不变）。显式 `BEGIN` 事务内无需提升（`sess.inTx` 已为 true）。提升后无论成败 `defer` 复位会话事务状态（`sess.inTx/curTx/curBeginTS/curTxID` 清空 + TSO 缓存 Reset），避免提交后残留导致后续语句误入已结束事务。
2. **SELECT 快照先于写入**：先 `execSelect` 取完整结果集 `res.Rows`，再逐行写入——目标表与源表相同（`INSERT INTO t SELECT * FROM t` 自插入）时天然满足快照语义（先读后写，不读取自己新写入的行）。
3. **逐行灌入复用 VALUES 语义**：每行调用 `insertRowUpsertOps(sess, tx, tabs, meta, s.Columns, row, s.OnDup, s.Replace, &pending)`，与 VALUES 路径完全一致（类型 coerced、主键/唯一索引冲突、ON DUPLICATE 更新、REPLACE 删旧插新、索引维护、外键检查、pending 语句内重复判定）；部分行失败返回错误 → 整个语句回滚（autocommit 时 defer Rollback / 2PC 未提交；显式事务内 applyOps 失败即返回）。
4. **触发器 / 自动分裂**：BEFORE/AFTER INSERT 触发器与 `maybeAutoSplit` 沿用 execInsert 原有统一逻辑，与 VALUES 路径一致。

### 2.5 兼容与边界

- SELECT 源能力与 T40/T41 对齐：`SELECT *`、表达式投影、聚合（COUNT/SUM/AVG/MAX/MIN）、DISTINCT、别名、WHERE（含 BETWEEN/IN/子查询/表达式）、GROUP BY、ORDER BY、LIMIT/OFFSET、JOIN、FROM 子查询、CTE、视图、`SELECT @var` 常量投影均可作为 INSERT 源。
- 列数不匹配：列清单长度 != SELECT 输出列数 → `column count mismatch`（buildRow 统一报错）。
- 类型不匹配：`coerceRow` 在单行写入时按列类型强制，无法强制时报类型错误。
- 主键冲突：普通 INSERT 报 `duplicate primary key`；`ON DUPLICATE KEY UPDATE` 更新冲突行；`REPLACE INTO` 删旧插新。
- information_schema 写拒绝：`INSERT INTO information_schema.x SELECT ...` 与 VALUES 一致在 execInsert 入口拒绝。

## 第三节：测试方案

落 `pkg/sql/t42_insert_select_test.go`（package sql_test），复用既有 helper（newEngine / mustExec / mustRows / mustErr）：

1. 基础全列：`INSERT INTO dst SELECT * FROM src`，行数与值全等校验。
2. 指定列：`INSERT INTO dst (a, b) SELECT x, y FROM src` 列映射正确。
3. WHERE 过滤：仅灌入满足条件的行。
4. 表达式投影：`SELECT id*10, val+1` 计算后灌入。
5. 聚合：`INSERT INTO dst SELECT grp, SUM(val) FROM src GROUP BY grp`。
6. DISTINCT：去重后灌入。
7. 空结果：`SELECT ... WHERE 1=0` 不写任何行，AffectedRows=0。
8. 列数不匹配报错：`INSERT INTO dst (a, b, c) SELECT x, y FROM src` → column count mismatch。
9. 类型不匹配报错：目标 INT 列灌入无法强制的值。
10. 主键冲突报错：普通 INSERT 灌入与已有行冲突的主键。
11. REPLACE INTO ... SELECT：冲突删旧插新（affected = 删数+1）。
12. ON DUPLICATE KEY UPDATE + SELECT：冲突更新。
13. 自插入：`INSERT INTO t SELECT * FROM t` 快照语义（行数翻倍且不递归）。
14. 失败回滚：第 N 行主键冲突导致整条语句回滚，前 N-1 行不可见。
15. 显式事务内：`BEGIN; INSERT INTO dst SELECT ...; COMMIT` 生效；回滚后不可见。
16. parser AST 校验：`INSERT INTO t SELECT ...` 解析后 `InsertStmt.Select != nil`、Rows 为空；VALUES 语句 `Select == nil`。

实际落盘测试（pkg/sql/t42_insert_select_test.go 18 个功能 + t42_insert_select_internal_test.go 4 个 parser AST = 22 个 TestT42*）在以上基础上补充：省略列清单不匹配（全列数校验报错）、省略列清单 OK、LIMIT/OFFSET 源、AffectedRows 计数、显式事务 read-your-writes（事务内立即可见、回滚后不可见、提交后生效）。
*（内容由AI生成，仅供参考）*

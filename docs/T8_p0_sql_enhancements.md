---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_dc059125c2e011f197eb525400393706
    ReservedCode1: czTxqCShs5TO4bQRO8MkmoIOF0iPXlFyOW/YyM/3Vo/hR5/CTsXQzqTtoTE/tlquZM4eZiI6bAQvaB0C3tlPCCWANXmBm5MlJQWMjcW4ZVBH1ES7ESmBr8XspiQWFVQdFv822Za1DN1bpdpuVSoVfmFqTJi1GyBorjIMzacx8OYda4rEcUEm7YdWTbQ=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_dc059125c2e011f197eb525400393706
    ReservedCode2: czTxqCShs5TO4bQRO8MkmoIOF0iPXlFyOW/YyM/3Vo/hR5/CTsXQzqTtoTE/tlquZM4eZiI6bAQvaB0C3tlPCCWANXmBm5MlJQWMjcW4ZVBH1ES7ESmBr8XspiQWFVQdFv822Za1DN1bpdpuVSoVfmFqTJi1GyBorjIMzacx8OYda4rEcUEm7YdWTbQ=
---



# T8 SQL 增强：JOIN / GROUP BY / 子查询 / WHERE 扩展 / 会话事务

## 目标

在 T3 SQL 子集（单表 SELECT + 基础 WHERE + 聚合）之上补齐 P0 能力：

1. JOIN：INNER JOIN / LEFT JOIN，ON 支持 `= != < > <= >=` 及其 AND 组合；
2. GROUP BY + 聚合（COUNT / SUM / AVG），保留无 GROUP BY 的全局聚合；
3. FROM 子查询（派生表）与 IN 子查询；
4. WHERE 增强：BETWEEN、IN（列表 / 子查询）；
5. 会话级显式事务 SQL：BEGIN / COMMIT / ROLLBACK 透传 txn 层。

## 新增 / 修改文件

| 文件 | 变更 |
|------|------|
| `pkg/sql/lexer.go` | 新增 `tokDot` token；keywords 扩充：`JOIN INNER LEFT OUTER GROUP BETWEEN IN AS BEGIN COMMIT ROLLBACK` |
| `pkg/sql/ast.go` | `Cond` 增加 `Ref / Val2 / RightRef / InList / Sub`；`SelectColumn` 增加 `Ref / Alias`；`SelectStmt` 增加 `FromAlias / Subquery / SubAlias / Joins / GroupBy`；`OrderBy` 增加 `Ref`；新增 `JoinClause`、`BeginStmt / CommitStmt / RollbackStmt` |
| `pkg/sql/parser.go` | `parseStmt` 增加事务语句分支；`parseSelect` 重写支持别名 / 子查询 / JOIN / GROUP BY / 限定列；`parseSelectColumn` 支持 `t.col` 与 `AS` 别名；`parseWhere` 重写为 `parseCond`（BETWEEN / IN / 列引用右值）；新增 `parseJoin / parseColumnRef / parseOptAlias / parseSelectAlias / qualifiedName` |
| `pkg/sql/executor.go` | `Executor` 增加会话事务状态（`mu / curTx / inTx`）与 `beginTx / commitTx / rollbackTx / getTx / autocommit`；全部 DDL/DML 改为会话事务感知（BEGIN 后不自动提交）；新增 Rel 关系代数执行引擎（`execSelectRel` 等）承载 JOIN / 子查询 / GROUP BY / 增强 WHERE |
| `pkg/sql/sql.go` | `IsSQL` 关键字列表增加 `BEGIN / COMMIT / ROLLBACK` 供协议层路由 |
| `pkg/sql/sql_test.go` | 新增 17 个 P0 测试用例（见下文） |
| `docs/T8_p0_sql_enhancements.md` | 本文档 |
| `README.md` | SQL subset 说明与 Roadmap 勾选更新 |

## 支持语法

### JOIN

```
SELECT cols FROM t1 [AS a] [INNER|LEFT [OUTER]] JOIN t2 [AS b] [ON conds]
```

- 默认 `JOIN` 等价 `INNER JOIN`；`LEFT [OUTER] JOIN` 等价 LEFT JOIN；
- ON 条件支持 `= != < > <= >=` 的 AND 组合，左右操作数可为任意一侧表的列或字面量（如 `ON a.id = b.uid AND b.amount >= 100`）；
- JOIN 右侧可为子查询：`... JOIN (SELECT ...) AS sub ON ...`（子查询必须带别名）；
- SELECT 列 / WHERE / ORDER BY / GROUP BY 均支持 `t.col` 限定列（引用别名或表名）；裸列名在无歧义时可用，歧义时报错。

### GROUP BY

```
SELECT cols, agg(...) FROM ... [WHERE ...] GROUP BY col[, ...]
```

- 支持 `COUNT(*)` / `COUNT(col)` / `SUM(col)` / `AVG(col)`；
- 无 GROUP BY 时保留原有全局聚合（单行结果）；
- 有 GROUP BY 时按组输出，每组一行：SELECT 中非聚合列必须出现在 GROUP BY 中，否则报错（见设计取舍）；
- GROUP BY 列可为限定列（`t.col`）。

### 子查询

- FROM 子查询：`SELECT ... FROM (SELECT ...) AS alias ...`，外层可引用其列（含 `alias.col` 限定引用）；子查询内部支持现有全部 SELECT 能力（WHERE / ORDER BY / LIMIT / 聚合）；
- IN 子查询：`WHERE col IN (SELECT col FROM t ...)`。

### WHERE 增强

- `col BETWEEN a AND b`：闭区间 `a <= col <= b`（INT / TEXT 均可）；
- `col IN (v1, v2, ...)`：值列表等值匹配；
- `col IN (SELECT ...)`：子查询结果集合匹配（子查询返回单列，行值取该列）。

### 事务 SQL

```
BEGIN;  ...  COMMIT;
BEGIN;  ...  ROLLBACK;
```

- `BEGIN` 开启会话级显式事务：此后所有 DDL / DML 在同一事务内执行、**不自动提交**；
- `COMMIT` 提交当前事务并结束；`ROLLBACK` 回滚并结束；
- 未 BEGIN 时保持原有单语句隐式事务行为（写即提交）；
- `BEGIN` 时已有未提交事务、`COMMIT / ROLLBACK` 时无活动事务 → 返回 SQL 错误（`transaction already active` / `no active transaction`）；
- 事务内出错（如类型错误、主键冲突）不会自动回滚，由用户决定 COMMIT / ROLLBACK（与大多数数据库一致，文档说明）。

## 执行语义

### 关系代数执行（execSelectRel）

- 触发条件：语句包含 JOIN / 子查询 / GROUP BY / BETWEEN / IN / 限定列 任一增强特征时走 `execSelectRel`，否则沿用原单表快速路径（点查 / 索引 / 全表扫描）；
- 执行流水线：FROM 关系构建（表扫描或子查询物化）→ 逐级 JOIN（内存嵌套循环）→ WHERE 过滤 → GROUP BY / 聚合 → ORDER BY / LIMIT → 投影；
- JOIN 输出行 = 左表行拼接右表行；LEFT JOIN 无匹配时右列以空串 TEXT 补齐（INT 列语义按空值处理，当前实现统一为 TEXT 空串）；
- 子查询（FROM / IN）先物化为内存关系再参与外层求值。

### 输出列名策略

- **多表 / 增强路径下 `SELECT *` 输出限定列名（`t.col`）**，避免两表同名列冲突；单表路径仍输出裸列名；
- 显式 `AS` 别名覆盖输出列名；未加别名时限定列输出 `t.col` 原名；
- 子查询派生表的列名归一化为裸列名（去掉内部限定前缀），外层 `alias.col` 引用到该列。

### 聚合与分组语义

- 全局聚合（无 GROUP BY）：对 WHERE 过滤后的全部行计算，输出单行；
- 分组聚合：按 GROUP BY 列的值分组（组序 = 首现顺序，即底层扫描序），每组输出一行；`COUNT(*)` 为该组行数；
- **非聚合列引用规则**：有 GROUP BY 时，SELECT 中的非聚合列若不在 GROUP BY 列表中则报错 `column X must appear in GROUP BY`（SQL 标准一致），避免"每组取哪一行"的歧义；`ORDER BY` 中引用列同样受此约束（聚合结果上排序仅允许引用分组列或聚合列）。

### 事务透传

- `sql.Engine` 内部持有会话事务状态（`curTx` + `inTx` 标志），`getTx()` 返回当前事务（显式事务中复用；否则新建），`autocommit()` 在非显式事务时于语句末尾提交；
- BEGIN 后 DDL 也在事务内执行（建表 / 删表 / 建索引均受 ROLLBACK 保护），与 txn 层快照隔离语义一致；
- 同一会话串行执行，事务状态用互斥锁保护（为将来多会话预留）。

## 设计取舍

1. **内存嵌套循环 JOIN**：数据量级（单机学习内核）下实现最简单、语义最清晰；ON 多条件与 LEFT 补空在行级直接求值。未做 hash join / 索引 join（Roadmap 优化项）。
2. **LEFT JOIN 空值用 TEXT 空串**：Value 模型无 NULL 概念，空串作为"缺失"表示；文档明确该语义，后续引入 NULL 时统一替换。
3. **分组列校验**：严格拒绝非分组非聚合列，避免隐式取首行的不确定性；同时降低实现复杂度。
4. **子查询仅支持派生表 + IN 标量子集**：不支持相关子查询 / EXISTS / 标量子查询（SELECT 列表内），保持 P0 范围可控。
5. **会话事务不阻塞**：显式事务持有 txn.Txn 句柄，事务内每条语句复用同一快照与写缓冲，COMMIT 一次落 WAL + 存储；中间出错不自动回滚（由用户显式 ROLLBACK）。

## 测试

`pkg/sql/sql_test.go` 新增用例（外部包，真实 db.Init/Open 装配）：

| 用例 | 覆盖点 |
|------|--------|
| `TestP0JoinInner` | INNER JOIN 等值匹配行数 / 列值 |
| `TestP0JoinLeft` | LEFT JOIN 无匹配补空行 |
| `TestP0JoinMultiCond` | ON 多条件（AND 组合，右表列作左操作数） |
| `TestP0JoinQualifiedRefs` | 限定列引用 + AS 别名输出列名 |
| `TestP0GroupBy` | 多组 GROUP BY + COUNT/SUM 组值 |
| `TestP0GroupByJoin` | JOIN + GROUP BY 组合 |
| `TestP0GroupByNonGroupColError` | 非分组列引用报错 |
| `TestP0FromSubquery` | FROM 子查询派生表 + 外层限定引用 |
| `TestP0SubqueryNested` | 子查询内 WHERE/ORDER BY/聚合 |
| `TestP0WhereBetween` | BETWEEN 闭区间 |
| `TestP0WhereInList` | IN 值列表 |
| `TestP0WhereInSubquery` | IN 子查询 |
| `TestP0TxnBeginCommit` | BEGIN→INSERT→COMMIT 持久化 |
| `TestP0TxnBeginRollback` | BEGIN→INSERT→ROLLBACK 不落盘 |
| `TestP0TxnRollbackDDL` | BEGIN→CREATE TABLE→ROLLBACK 撤销 |
| `TestP0TxnStateErrors` | 状态约束错误（重复 BEGIN / 无事务 COMMIT） |
| `TestP0TxnAutocommitDefault` | 未 BEGIN 时隐式提交行为不变 |

## 验证

- 全仓 `go build ./...` 通过；`go test ./... -count=1` 全绿（db / server / sql / storage / btree / rocksdb / txn / wal）；
- 本地构建环境：CGO 指向本机 RocksDB（OpenXDBTools 静态库），Windows msys2 工具链，与 CI 同参数（见 README Build 与 .github/workflows/ci.yml）。
*（内容由AI生成，仅供参考）*

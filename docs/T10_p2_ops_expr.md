---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_d7b137a2c30711f197eb525400393706
    ReservedCode1: PgfrWydNAoe7VbYN/PqyVvDXi52+9G/I8etcLF3TvgJGf8dNdQ+Ll65qlI2eXpoGzg56euLwq/6uwNyhP6BSc0gmsoPYCYblM+8MUZoO3Xu4ecLYwji20ayOASZqkPKaRoJg+hR8M8WsS5h1n11FgVJTO30TpCO3CAP6obbmnyp4QxKNuB9A7IWOSq4=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_d7b137a2c30711f197eb525400393706
    ReservedCode2: PgfrWydNAoe7VbYN/PqyVvDXi52+9G/I8etcLF3TvgJGf8dNdQ+Ll65qlI2eXpoGzg56euLwq/6uwNyhP6BSc0gmsoPYCYblM+8MUZoO3Xu4ecLYwji20ayOASZqkPKaRoJg+hR8M8WsS5h1n11FgVJTO30TpCO3CAP6obbmnyp4QxKNuB9A7IWOSq4=
---



# T10 P2 运维语句与表达式增强实现记录

## 目标

在 P0（JOIN/GROUP BY/子查询/BETWEEN/IN/事务 SQL）与 P1（DATE/DECIMAL/BLOB、EXPORT/IMPORT CSV）之上，补齐 P2 功能簇：

1. 运维语句：`SHOW TABLES` / `SHOW INDEX FROM t` / `EXPLAIN <SELECT>` / 慢查询（阈值配置 + `SHOW SLOWQUERIES`）。
2. 表达式增强：`LIKE`（`%` / `_` 模式匹配）与 `CASE WHEN`（SELECT 投影与 WHERE 条件）。
3. 联动：解析器 / AST / 执行器 / Result 输出扩展；EXPLAIN 与慢查询输出走 Result 结构，供协议层 / REPL 统一展示；LIKE / CASE 与现有 JOIN / GROUP BY / 子查询组合不回归。

## 模块改动

| 文件 | 改动 |
|------|------|
| `pkg/sql/lexer.go` | keywords 追加 SHOW / EXPLAIN / SLOWQUERIES / LIKE / CASE / WHEN / THEN / ELSE / END / SET / SLOW |
| `pkg/sql/ast.go` | Cond 增 `Op="LIKE"`、`LeftCase`；SelectColumn 增 `Case`；新增 ShowTablesStmt / ShowIndexStmt / ShowSlowQueriesStmt / ExplainStmt / CaseWhenClause / SlowQueryRecord |
| `pkg/sql/parser.go` | parseStmt 增 SHOW / EXPLAIN / SET SLOW 分发；parseShow / parseExplain / parseCaseWhen；parseCond 支持 LIKE 与左值 CASE；parseSelectColumn 支持 CASE 投影与别名 |
| `pkg/sql/executor.go` | Execute 增运维语句分发；新增 slowThreshold / slowQueries 与 SetSlowThreshold / SlowQueries / recordSlow / execTimed；execShowTables / execShowIndex / execShowSlowQueries / execExplain / planOf / accessOf；likeMatch / evalCase（单表）/ evalCaseRel（Rel） |
| `pkg/sql/sql.go` | Engine 增 SetSlowThreshold / SlowQueries 透传；IsSQL 支持 SHOW / EXPLAIN / SET SLOW |
| `pkg/sql/sql_test.go` | 新增 P2 测试（见下） |

## 运维语句语法与输出

### SHOW TABLES

```
SHOW TABLES;
```

列出全部表，按表名升序排序。输出列：

| 列 | 类型 | 说明 |
|----|------|------|
| `table` | TEXT | 表名 |
| `columns` | TEXT | 列定义摘要 `name:TYPE`，逗号分隔，按定义序 |
| `pk` | TEXT | 主键列名 |
| `indexes` | TEXT | 二级索引摘要 `idxName(col)`，逗号分隔；无索引为空串 |

示例：
```
table    columns                  pk   indexes
orders   id:INT, item:TEXT, qty:INT  id
users    id:INT, name:TEXT, age:INT  id   idx_age(age)
```

### SHOW INDEX FROM t

```
SHOW INDEX FROM users;
```

列出指定表全部二级索引（无索引时返回空结果集）。输出列：

| 列 | 类型 | 说明 |
|----|------|------|
| `table` | TEXT | 表名 |
| `index` | TEXT | 索引名 |
| `column` | TEXT | 索引列 |
| `id` | INT | 表内索引 ID（递增分配） |
| `unique` | TEXT | 恒为 `NO`（当前仅支持非唯一单列索引） |

### EXPLAIN <SELECT>

```
EXPLAIN SELECT name FROM users WHERE age >= 25 ORDER BY age LIMIT 5;
```

不实际执行查询，仅输出执行计划文本（不返回数据行）。输出列 `plan`（TEXT），每行一条算子：

- `access:` 访问路径：
  - 单表：`point lookup on PK (pk)`（主键等值单条件）/ `index scan on idx(col)`（命中二级索引列）/ `table scan (full)`（全表扫描）
  - 多表：`nested-loop join (t + N table(s))`
  - FROM 子查询：`subquery scan (FROM (alias))`
- `filter:` WHERE 条件摘要（含 BETWEEN / IN / LIKE / CASE，AND 组合）
- `joinN on:` JOIN 的 ON 条件摘要
- `group:` GROUP BY 列
- `aggregate: yes`（存在聚合）
- `sort:` ORDER BY 列 + 方向（`asc` / `desc`）
- `limit:` LIMIT 值
- `projection:` SELECT 投影列摘要（`*` / 列名 / `COUNT(...)` / `CASE(...)`）

说明：`EXPLAIN` 只分析 `FROM` 主表的访问路径；JOIN / 子查询形态给出算子提示而非逐表细化，后续可按需扩展。

### 慢查询

配置形态（内存记录）：

- 阈值：`SET SLOW <ms>`（毫秒；`<=0` 关闭记录，默认 `1000ms`）。由 `Executor.SetSlowThreshold` 提供，Engine 透传。
- 记录范围：至少覆盖 SELECT 路径（ExecuteText 对所有语句统一计时，超阈值即记录，因此 DML/DDL 同样可被记录）。
- 记录容量：内存环形保留最近 `slowQueryMax = 64` 条；超限丢弃最旧。
- 查看：`SHOW SLOWQUERIES;`，按新→旧排序。输出列：

| 列 | 类型 | 说明 |
|----|------|------|
| `time` | TEXT | 本地时间 `YYYY-MM-DD HH:MM:SS` |
| `duration_ms` | INT | 执行耗时（毫秒） |
| `sql` | TEXT | 原始 SQL 文本（Engine 透传原文；直接调 Execute 时退化为 `%T` 类型名） |

```sql
SET SLOW 0;                 -- 关闭记录
SET SLOW 100;               -- 记录耗时 >= 100ms 的语句
SHOW SLOWQUERIES;           -- 查看最近慢查询
```

## LIKE 匹配规则

语法：`WHERE col LIKE 'pattern'`，可与其他条件以 `AND` 组合。

- 模式字符：
  - `%`：匹配任意多字符（含零个）。
  - `_`：匹配恰好一个字符。
  - 其余字符按字面匹配。
- 大小写敏感：按 TEXT 字节序（字典序）精确匹配，`LIKE` 与 `= / < / >` 等一致采用大小写敏感语义；INT 列先转文本再匹配。
- 空模式：`''` 仅匹配空串。
- 求值：`likeMatch` 基于双指针 / 回溯的简单匹配器（模式长度较小，无需编译为自动机）；`%` 采用贪婪回退，保证正确性优先于最坏情形性能。

示例：
```sql
SELECT name FROM users WHERE name LIKE 'a%';   -- alice
SELECT name FROM users WHERE name LIKE '_ob';  -- bob
SELECT name FROM users WHERE name LIKE '%a%' AND age > 20;
```

## CASE WHEN 语法与类型规则

SELECT 投影：

```sql
SELECT id, CASE WHEN age >= 30 THEN 'old' ELSE 'young' END AS grp FROM users;
SELECT CASE WHEN age < 20 THEN 'teen' WHEN age < 30 THEN 'young' ELSE 'old' END FROM users;
```

WHERE 条件（左值为 CASE 表达式）：

```sql
SELECT id FROM users WHERE CASE WHEN age >= 30 THEN 'A' ELSE 'B' END = 'A';
```

- 多分支：`WHEN <cond> THEN <val>` 可重复；按书写顺序短路求值，命中首个为真的分支。
- `ELSE` 可选：无命中且无 ELSE 时返回空串 `''`。
- `THEN / ELSE` 值允许 INT 或 TEXT 字面量；CASE 列输出类型按命中分支的值而定（宽松，不做跨分支类型合并）。
- 条件 `<cond>` 复用现有 Cond 结构：支持 `= != < > <= >= BETWEEN IN LIKE` 及 CASE 嵌套（`LeftCase`），以 AND 组合；行级求值走 `matchWhere`（单表）或 `matchWhereRel`（Rel/JOIN/子查询）。
- 别名：`AS alias` 支持；无别名时输出名取 CASE 表达式文本摘要 `CASE(...)`（由 parser 生成）。
- 组合：CASE 投影可与 JOIN / GROUP BY 组合——GROUP BY 输出循环对 CASE 列按组内首行求值，不再要求 CASE 列必须出现在 GROUP BY 列表。

## 设计取舍

1. 慢查询采用内存记录而非落盘：P2 目标是可观测与演示，`SHOW SLOWQUERIES` 通过 Result 结构复用既有协议层展示；文件持久化与轮转留给后续里程碑。
2. `EXPLAIN` 不真正执行：避免统计信息引入额外状态；访问路径判定规则与执行器 `indexLookup` / 点查优化保持一致（`accessOf` 与 `execSelect` 共用同一判定逻辑）。
3. LIKE 大小写敏感：与 `compareVal` 的 TEXT 字节序语义一致，避免引入大小写折叠后的索引排序错配；后续如需不敏感匹配可新增 `ILIKE` 或函数形态。
4. `SHOW TABLES` 一次输出列定义 + 主键 + 索引摘要：单条语句即可支撑 REPL/协议层表格渲染，无需多次内省。
5. CASE 条件复用 Cond：避免单独表达式树导致 WHERE / JOIN ON / GROUP BY 三处求值逻辑分叉，降低回归风险。

## 测试用例（sql_test.go 新增 TestP2*）

| 测试 | 覆盖点 |
|------|--------|
| `TestP2ShowTables` | SHOW TABLES 行数、列摘要 `name:TYPE`、主键、索引摘要 |
| `TestP2ShowIndex` | SHOW INDEX FROM 列（table/index/column/id/unique）、不存在表报错 |
| `TestP2Explain` | EXPLAIN 点查 / 索引扫描 / 全表扫描 / JOIN / GROUP BY / LIMIT 计划形态 |
| `TestP2SlowQueries` | 阈值触发与未触发、SHOW SLOWQUERIES 记录内容、SET SLOW 开关 |
| `TestP2Like` | `%` 前后缀 / `_` 单字符 / 空模式 / 大小写敏感 / 与 AND 组合 |
| `TestP2CaseSelect` | CASE 多分支 / ELSE / 别名 / 无 ELSE 空串 |
| `TestP2CaseWhere` | WHERE 内 CASE 等值过滤 |
| `TestP2CaseJoinGroup` | CASE 与 JOIN / GROUP BY 组合不回归 |

## 验证

- 本机真实工具链（MinGW + RocksDB v11.8.1）`go build ./...` 与 `go test ./... -count=1` 全绿。
- README SQL subset / Highlights / Development log / Roadmap 同步更新并勾选 P2。
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_4d056ddec89b11f18b7f525400393706
    ReservedCode1: z9/AXlXp64KXCmIRP3lzH9QnR6+rJBfkkzNd8jUMAM7Wy9H/ctcYHjucA+j3fQGN9JnqTpDO1bX5xy3XFlhunyHXuWsdBnRJ8KYKDRE8kLAT4EgJHIa6wN7rEdo14GXLs9QX6QbvMwNcWT+y7upg5WgNSUbbAZZYd9FlRaJDocSARXzn1WMe1Uv3DIo=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_4d056ddec89b11f18b7f525400393706
    ReservedCode2: z9/AXlXp64KXCmIRP3lzH9QnR6+rJBfkkzNd8jUMAM7Wy9H/ctcYHjucA+j3fQGN9JnqTpDO1bX5xy3XFlhunyHXuWsdBnRJ8KYKDRE8kLAT4EgJHIa6wN7rEdo14GXLs9QX6QbvMwNcWT+y7upg5WgNSUbbAZZYd9FlRaJDocSARXzn1WMe1Uv3DIo=
---

# T40 SQL 能力补齐：MAX/MIN 聚合、算术表达式、SELECT DISTINCT

版本：v0.28.0-alpha
日期：2026-10-15
前置：T39 能力缺口评估（docs/T39_sql_capability_gap_assessment.md）
后续状态：本文档三类缺口（MAX/MIN、算术表达式、SELECT DISTINCT）均已实现并合入 v1.1.0 前版本（测试见 pkg/sql/t40_sql_test.go 等）。

## 总览

根据 T39 评估结论，OpenXDB 存在三类 SQL 兼容性缺口：

| 缺口 | 现状 | 等级 |
|---|---|---|
| MAX/MIN 聚合函数 | 仅 COUNT/SUM/AVG 支持，MAX/MIN 在解析白名单与执行 switch 缺失 | P1 |
| 算术表达式（+ - * / %） | 词法层即不支持（`k+1` 报 unexpected character '+'） | P0 |
| SELECT DISTINCT | 关键字未登记、无 parser 分支、无 AST 字段、无执行去重 | P2 |

本文档分三节，各含现状 / 设计 / 测试方案。

---

## 一、MAX/MIN 聚合函数

### 1.1 现状

- `pkg/sql/parser.go` `parseSelectColumn` 聚合函数白名单仅 `COUNT / SUM / AVG / ROW_NUMBER / RANK / DENSE_RANK`，`MAX( / MIN(` 直接报 "function not supported"。
- `pkg/sql/executor.go`：
  - `aggColumn`（单表路径，约 L2358）：仅处理 COUNT/SUM/AVG。
  - `aggValues`（Rel 路径，约 L2712）：仅处理 COUNT/SUM/AVG。
  - `computeWindowCol`（窗口路径）：聚合类窗口仅 COUNT/SUM/AVG。
- `pkg/sql/ast.go` `SelectColumn.Agg` 注释枚举未含 MAX/MIN。

### 1.2 设计

1. **解析**：`parseSelectColumn` 白名单增加 `MAX`、`MIN`（与 SUM/AVG 同一分支），支持全局聚合、GROUP BY 聚合与窗口函数（`MAX(col) OVER (...)`）。
2. **执行（单表路径 `aggColumn`）**：新增 `MAX/MIN` 分支——遍历行取目标列值，用 `compareVal` 比较维护最大/最小值；空表返回 `zeroValue(列类型)`（与 AVG 空表返回 0 的既有风格一致；OpenXDB 无 NULL 概念）。
3. **执行（Rel 路径 `aggValues`）**：同构实现，取值走 `relValue`。
4. **执行（窗口路径 `computeWindowCol`）**：新增 `MAX/MIN` 累计分支，随行数累积更新当前窗口极值。
5. **聚合参数表达式**：MAX/MIN（以及 SUM/AVG/COUNT）参数支持算术表达式（如 `SUM(k+1)`），见第二节设计。

### 1.3 测试方案

- 全局聚合：`SELECT MAX(k), MIN(k) FROM t`。
- GROUP BY：按分组取各组的 MAX/MIN。
- 窗口：`MAX(k) OVER (PARTITION BY ... ORDER BY ...)`。
- 边界：空表返回零值；多行乱序验证正确性；DECIMAL 列极值；TEXT 列极值（按字典序）。

---

## 二、算术表达式（+ - * / %）

### 2.1 现状

- `pkg/sql/lexer.go`：无 `+ - * / %` 的 token（`*` 为 `tokStar` 仅用于 SELECT * / COUNT(*)）；lexer default 分支报 "unexpected character"。
- `pkg/sql/parser.go`：SELECT 列只支持裸列/限定列/聚合/窗口/CASE；WHERE 左操作数仅列引用或 CASE；右操作数仅字面量/列引用/子查询。无优先级解析。
- `pkg/sql/executor.go`：`finishSelect` / `finishRel` 投影无表达式求值；`matchWhere` / `matchCondRelCached` 无表达式比较。

### 2.2 设计

**词法（lexer.go）**：
- 新增 token：`tokPlus(+)`、`tokMinus(-)`、`tokSlash(/)`、`tokMod(%)`；`*` 复用 `tokStar` 兼作乘法运算符（SELECT * / COUNT(*) 语义由 parser 上下文区分）。
- `-` 消歧：新增 `prevEndsValue()` 判断上一 token 是否为可结束操作数的值类 token（ident/number/string/rparen/star/var/question）。上一 token 非值类且 `-` 后跟数字时按**负数字面量**处理（保持既有 `-3`、`WHERE age > -3` 兼容）；否则按**减法运算符**（`k-1`、`5-2`）。

**AST（ast.go）**：
- 新增表达式节点：

```go
type Expr struct {
    Op    string // "" | "+" | "-" | "*" | "/" | "%" | "NEG"
    Left  *Expr  // 二元左操作数
    Right *Expr  // 二元右操作数 / NEG 子表达式
    Ref   string // 叶子：限定表名/别名
    Col   string // 叶子：裸列名（Val 为 nil 时有效）
    Val   *Value // 叶子：字面量
    Raw   string // 原始文本（用于输出列名）
}
```

- `SelectColumn` 增加 `Expr *Expr`：非空表示投影列或聚合参数为表达式。
- `Cond` 增加 `LeftExpr *Expr`（左操作数表达式，忽略 Ref/Col/LeftCase）与 `RightExpr *Expr`（右操作数表达式，忽略 Val/RightRef）。

**解析（parser.go）**：
- 递归下降优先级：`parseExpr` → `parseAddSub`（+ -）→ `parseMulDiv`（* / %）→ `parseUnary`（一元负 NEG / 一元正忽略）→ `parsePrimary`（数字/字符串/括号表达式/限定列/裸列）。
- SELECT 列：ident 分支后接运算符时进入表达式解析；数字、`(`、`+`、`-` 开头亦进入表达式；表达式叶子为列引用时仍产出普通列（保持既有路径与索引优化），复杂表达式填 `Expr`。输出列名取表达式原始文本（如 `k+1`）。
- 聚合/窗口函数参数：`parseColumnRef` 升级为 `parseExpr`，叶子列保持 `Ref/Col` 语义，复杂表达式填 `Expr`（支持 `SUM(k+1)`、`MAX(k*2)`）。
- WHERE：左操作数 CASE 分支保留，其余先 `parseExpr`——叶子列走既有 Ref/Col（保持索引优化），复杂表达式填 `LeftExpr`；比较运算符右值统一 `parseExpr`——叶子列 → RightRef、字面量 → Val、复杂表达式 → RightExpr。BETWEEN/IN/LIKE 右端保持字面量语义。

**执行（executor.go）**：
- 新增 `evalExpr(meta, row, x)`（单表路径）与 `evalExprRel(rel, row, x)`（Rel 路径）：叶子取字面量/列值（复用 `rowValue` / `relValue`），运算走公共 `arithEval(a, b, op, raw)`。
- `arithEval` 数值规则：
  - 任一侧 DECIMAL → 定点缩放运算：`+`/`-` 直接缩放值加减；`*`/`/` 用 `math/big` 防溢出（乘 `decUnit` 缩放、除法先乘后除）；`%` 对 DECIMAL 报不支持。
  - 两侧 INT → int64 四则；`/` 整数除法；`%` 取模；除/模零报 "division by zero"。
  - 非数值操作数（TEXT/DATE/BLOB）报 "non-numeric operand"。
- 投影：`finishSelect` / `finishSelectWindow` / `finishRel` 列循环增加 `c.Expr != nil` 分支。
- WHERE：`matchWhere` 与 `matchCondRelCached` 左/右操作数增加表达式求值（子查询/IN/BETWEEN/LIKE 逻辑保持基于求值后的左值）。
- 聚合：`aggColumn` / `aggValues` / `computeWindowCol` 取值处支持 `c.Expr`（窗口路径经调用点注入 eval 闭包）。

### 2.3 测试方案

- 四则：`k+1`、`k-1`、`k*2`、`k/2`、`k%3` 投影与比较。
- 优先级：`k+2*3`、`(k+2)*3`、`k*2-4/2`。
- 负数：字面量负数 `-5`、一元负 `-k`、`5 - -3`、`WHERE k > -3`。
- 列与常量：列列运算 `a+b`、列常量运算。
- WHERE 中运算：`WHERE k+1 > 3`、`WHERE k*2 = 10`。
- 除法/取模：整除、余数、除零报错、模零报错。
- DECIMAL：`price*1.05`、`price/2`、DECIMAL 与 INT 混合。
- 聚合参数表达式：`SUM(k+1)`、`AVG(k*2)`。
- 表达式列别名与输出列名。

---

## 三、SELECT DISTINCT

### 3.1 现状

- `lexer.go` keywords 未登记 DISTINCT。
- `parser.go` parseSelect 无 DISTINCT 分支。
- `ast.go` SelectStmt 无 Distinct 字段。
- 执行层无去重逻辑（既有规避手段为 GROUP BY 等价）。

### 3.2 设计

1. **词法**：keywords 注册 `DISTINCT`。
2. **解析**：`parseSelect` 在 `SELECT` 关键字后消费可选 `DISTINCT`，写入 `SelectStmt.Distinct`。
3. **执行**：DISTINCT 语义为"投影行去重，去重先于 ORDER BY/LIMIT"：
   - 单表路径 `finishSelect`：排序/分页前按投影键去重（普通列、`*` 展开列、CASE 列、表达式列参与键；窗口列不参与——去重后再计算窗口，近似 MySQL 语义）。
   - Rel 路径 `finishRel`：ORDER BY/LIMIT 前同构去重（`distinctRowsRel`，CASE 列经 `evalCaseRel` 求值）。
   - GROUP BY 路径：`groupRel` 输出后按结果行去重。
4. **与其它子句组合**：`SELECT DISTINCT ... WHERE ...`（过滤先去重后）、`ORDER BY`（去重后排序，排序列须为 SELECT 列）、`LIMIT/OFFSET`（去重后分页）、聚合（聚合结果去重，单行无影响但合法）。

### 3.3 测试方案

- 单列：`SELECT DISTINCT k FROM t`。
- 多列：`SELECT DISTINCT a, b FROM t`。
- 与 WHERE：`SELECT DISTINCT k FROM t WHERE k > 1`。
- 与 ORDER BY：`SELECT DISTINCT k FROM t ORDER BY k`（含 DESC）。
- 与 LIMIT：`SELECT DISTINCT k FROM t LIMIT 2`。
- 与聚合：`SELECT DISTINCT g, SUM(k) FROM t GROUP BY g`。
- 含重复行全去重、空结果集。

---

## 四、GROUP BY/聚合 LIMIT/OFFSET 截断

### 4.1 现状

- `executor.go` 的 `execSelectRel` 遇到 GROUP BY 或全局聚合时直接返回 `groupRel` / `aggRel`。
- `groupRel` 与 `aggRel` 尾部均直接 `return res`，未应用 `s.Offset` / `s.Limit`，导致 `SELECT dept, COUNT(*) FROM t GROUP BY dept LIMIT 5`、`SELECT MAX(k) FROM t LIMIT 1` 等查询 LIMIT/OFFSET 被静默忽略。
- 仅有非聚合路径 `finishRel`（约 L3270-3279）在尾部做了 OFFSET/LIMIT 截断。

### 4.2 设计

1. **统一截断语义**：抽取 `applyOffsetLimit(rows, offset, limit)`，与 `finishRel` 截断逻辑一致——先 OFFSET 跳过、再 LIMIT 截断；LIMIT<=0 不截断；OFFSET 越界返回空。
2. **接入点**：`groupRel` 与 `aggRel` 在分组/聚合完成后、返回前对 `res.Rows` 应用 `applyOffsetLimit`。GROUP BY 场景 LIMIT 作用于分组结果（每组一行），全局聚合单行结果按同一规则截断。
3. **顺序说明**：GROUP BY 在聚合计算完成后截断，不参与分组键构造；`groupRel` 分组顺序为行首见顺序（与既有行为一致，未引入排序依赖）。

### 4.3 测试方案

- `GROUP BY + LIMIT`：`SELECT grp, COUNT(*) FROM t40 GROUP BY grp LIMIT 1`。
- `GROUP BY + OFFSET + LIMIT`：`LIMIT 1 OFFSET 1`（parser 仅支持 `LIMIT n OFFSET m` 形式）。
- 全局聚合 + LIMIT：`SELECT MAX(val) FROM t40 LIMIT 1`。
- LIMIT 大于分组数不截断：`LIMIT 100`；LIMIT=0 不截断。
- OFFSET 越界返回空：`LIMIT 100 OFFSET 5`（GROUP BY）与 `LIMIT 100 OFFSET 1`（全局聚合）。
- 测试落于 `pkg/sql/t40_sql_test.go`（`TestT40GroupByLimit` / `TestT40GroupByOffset` / `TestT40GroupByLimitLarge` / `TestT40AggLimitOffset`）。

---

## 五、验证与收尾

- 新增集成测试 `pkg/sql/m40_sql_capability_test.go`，覆盖各节全部测试方案。
- `go test ./... -count=1 -p 1` 全绿。
- README Status 行推进 `v0.28.0-alpha`，Roadmap 勾选 T40 并链接本文档。
*（内容由AI生成，仅供参考）*

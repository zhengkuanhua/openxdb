---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_3aec17fbc89011f196f9525400248c00
    ReservedCode1: 3MkZoLMSLFYq9l6TDSxg2X1pJrBMQCzqfwONTujeTNu7u7q4xSVaU/gSZlxt/q9qfumTcjEECSBMdnV1u6BlhKhYqreh96EcLd7t2LS0Y7JJrAEOvTImkpCysw/3q6zMgLpqtISRdZVTPhaKJO0Y6FlmOG2t6SSx4LBL4qzr5GTI78RfTBFoHGTk3ws=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_3aec17fbc89011f196f9525400248c00
    ReservedCode2: 3MkZoLMSLFYq9l6TDSxg2X1pJrBMQCzqfwONTujeTNu7u7q4xSVaU/gSZlxt/q9qfumTcjEECSBMdnV1u6BlhKhYqreh96EcLd7t2LS0Y7JJrAEOvTImkpCysw/3q6zMgLpqtISRdZVTPhaKJO0Y6FlmOG2t6SSx4LBL4qzr5GTI78RfTBFoHGTk3ws=
---

# T39 SQL 层能力缺口评估：聚合函数 / 算术表达式 / DISTINCT

- 版本: v0.27.0-alpha
- 范围: `pkg/sql`（lexer / parser / ast / executor / value）
- 结论先行：**聚合函数部分支持（COUNT/SUM/AVG），MAX/MIN 未实现；算术表达式完全不支持（词法层缺失）；SELECT DISTINCT 完全不支持（全链路无实现）**。三类缺失均可修复，算术表达式工作量最大，DISTINCT 与 MAX/MIN 工作量最小。
- 依据：源码走查（`pkg/sql/*.go`）+ 既有测试（`sql_test.go` / `m10_window_test.go` / `matview_test.go`）+ E5 基准压测实测（`docs/experiments/E5_standard_bench_alignment.md`）。本报告只做评估，不实施代码修改。
- 后续状态：本文档为 v0.27.0-alpha 时点评估快照；所列缺口已由后续版本实现——MAX/MIN、算术表达式、SELECT DISTINCT 均随 v1.1.0 前 T40/P1 系列落地（测试见 pkg/sql/t40_sql_test.go、p1_10_edge_test.go）。

---

## 1. 聚合函数（SUM / COUNT / MAX / MIN / AVG）

### 1.1 现状证据

**已支持的聚合（COUNT / SUM / AVG，覆盖三种执行上下文）**

| 上下文 | 代码位置 | 说明 |
|---|---|---|
| 解析白名单 | `parser.go` L2101-L2132 `parseSelectColumn` | 仅接受 `COUNT/SUM/AVG/ROW_NUMBER/RANK/DENSE_RANK`，其余函数报 `function %q not supported` |
| 无 GROUP BY 全局聚合 | `executor.go` L2342 `doAgg` → L2357 `aggColumn`（旧路径）；L2884 `aggRel` → L2711 `aggValues`（增强路径） | `SELECT SUM(k) FROM t` 可运行 |
| GROUP BY 分组聚合 | `executor.go` L2900 `groupRel` | `SELECT uid, COUNT(*), SUM(amount) FROM orders GROUP BY uid` 可运行；非聚合列必须属于分组键（L2942 报 `column not in GROUP BY`） |
| 窗口聚合（OVER） | `executor.go` L2756 `computeWindows` / `computeWindowCol` | `SUM(salary) OVER (ORDER BY id)` 等可运行（M10） |

**测试证据**

- `sql_test.go` L174-180：`SELECT COUNT(*)` / `SUM(age)` / `AVG(age) WHERE ...`（全局聚合）
- `sql_test.go` L537-541 `TestP0GroupBy`：`GROUP BY uid` + `COUNT(*)` / `SUM(amount)`
- `m10_window_test.go` L84 / L98：窗口 `SUM/AVG/COUNT OVER`
- `matview_test.go` L161：物化视图内 `GROUP BY age` + `COUNT(*)`
- 全量测试扫描：**无任何 `MAX(` / `MIN(` 用例**，证实二者从未实现

**缺失与限制（逐条代码定位）**

1. **MAX / MIN 未实现**
   - `parser.go` L2103 白名单不含 `MAX`/`MIN` → 解析即报 `function "MAX" not supported`
   - `ast.go` `SelectColumn.Agg` 枚举仅 COUNT/SUM/AVG/ROW_NUMBER/RANK/DENSE_RANK
   - `executor.go` L2364-2406 `aggColumn`、L2713-2750 `aggValues` switch 均无 MAX/MIN → `unknown aggregate` / `unsupported aggregate`
2. **聚合函数只允许出现在 SELECT 列表**
   - `parser.go` L2036-2044 `parseColumnRef`：`COUNT/SUM/AVG/ROW_NUMBER/RANK/DENSE_RANK` 后跟 `(` 时在 WHERE/GROUP BY/ORDER BY 等位置显式报 `function %s() only allowed in SELECT list`（无 HAVING 子句，无法按分组条件过滤聚合结果）
3. **聚合参数只能是裸列名 / t.col 或 COUNT(*)**
   - `parser.go` L2125-2137：参数走 `parseColumnRef`，不接受常量（`SUM(1)` 失败）、不接受表达式（`SUM(k+1)` 在词法层即失败）、无 `COUNT(DISTINCT col)` 形式
4. **聚合别名必须显式 AS**
   - `parser.go` L2171-2184 `parseSelectAlias` 只处理 `AS + ident`；裸别名（`SUM(k) s`）不会被识别为别名，`s` 会被当作下一个 SELECT 列解析，最终报列不存在类错误——E5 实测现象的直接根因
5. **GROUP BY 后 ORDER BY 别名 / LIMIT 被忽略**
   - `executor.go` L2652-2657 `execSelectRel`：`len(s.GroupBy) > 0` 时 `return e.groupRel(...)` **直接返回**，后续排序/分页逻辑完全不执行；全局聚合 `L2661-2663 return e.aggRel(s, rel)` 同理
6. **聚合与窗口函数混用受限**
   - `executor.go` L2248 / L2659：无 GROUP BY 时 `window function cannot be mixed with aggregate`；有 GROUP BY 时窗口直接报 `window function not supported with GROUP BY`

### 1.2 缺失根因

- MAX/MIN 是**聚合框架未扩展**：解析白名单、AST 枚举、两处执行 switch 三处都没加，属"半成品聚合"而非架构不支持。
- 其余限制（别名、位置、参数、排序）是**聚合只围绕 GROUP BY/窗口场景设计**，未按标准 SQL 语义做完整表达式/子句体系。

### 1.3 E5 现象解释

- `SELECT SUM(k) AS s` 可运行：无 GROUP BY 全局聚合路径（`aggRel`/`doAgg`）存在，且写了 AS。
- 用户感知"不支持聚合"的真实来源：E5/TPC-C 脚本中一旦使用 **MAX/MIN**（解析即失败）、**裸别名**（被当作列）、**聚合带算术参数**（词法失败）或 **GROUP BY 后排序**（静默忽略）即表现为不可用；加上 E5 观察到的聚合别名须 AS 现象，整体给人"聚合不可用"的观感。

### 1.4 修复方案与工作量

**必做（MAX/MIN 补齐）——工作量：小（约 0.5~1 人日）**

| 步骤 | 文件 | 改动 |
|---|---|---|
| 解析白名单 | `parser.go` L2103 及 `parseColumnRef` L2037 | 白名单加入 `"MAX","MIN"` |
| AST 枚举 | `ast.go` | `SelectColumn.Agg` 注释/枚举补充 MAX/MIN |
| 执行 | `executor.go` `aggColumn` L2364 与 `aggValues` L2713 | 新增 `case "MAX","MIN"`：遍历行取值，用 `compareVal` 取最大/最小（INT/DECIMAL/字符串均可比较），空行返回 NULL/零值语义需与 COUNT 对齐 |
| 测试 | `sql_test.go` / `m10_window_test.go` | 全局 `SELECT MAX(age)`、`GROUP BY` 组内 MAX/MIN、窗口 `MAX OVER`；含 DECIMAL、空表边界 |

**可选增强（建议单独排期，不阻塞 T39）——工作量：中**

| 增强 | 说明 | 前置 |
|---|---|---|
| 裸别名支持 | `parseSelectAlias` 允许 `ident` 无 AS（需与后续列解析消歧） | 无 |
| `COUNT(DISTINCT col)` | 依赖 DISTINCT 修复（见 §3） | DISTINCT |
| 聚合参数表达式 | `SUM(k+1)`、`SUM(amount*2)` | 算术表达式（§2） |
| HAVING 子句 | parser 新子句 + 对分组结果过滤 | 聚合表达式 |
| GROUP BY 后 ORDER BY/LIMIT 生效 | `execSelectRel` L2656 改为"先分组聚合 → 再 applyOrderLimit" | 无 |

---

## 2. 算术表达式（+ - * / %）

### 2.1 现状证据

**词法层（直接根因）**

- `lexer.go` L10-31 token 枚举：**不存在任何算术运算符 token**（仅有 `tokStar` 用于 `SELECT *` / `COUNT(*)`，`=` `!=` `<` `<=` `>` `>=` 为比较符）
- `lexer.go` L75-142 `lex()` switch：`+` `/` `%` 无匹配分支 → 落入 `default` 报 `unexpected character %q at %d`；`-` 仅在 `isDigit(peek(1))` 时作为负数数字字面量（L103、L156-159），非数字前导的 `-` 同样报错
- 因此 E5 中 `k+1` 报 `unexpected character '+'` 是**词法层拒绝**，语句根本到不了 parser

**语法层 / 执行层**

- `ast.go`：无 BinaryExpr / 表达式节点；表达式能力仅有比较 Cond（`=` `<>` `<` `>` `<=` `>=`）、`BETWEEN`、`IN`、`CASE`
- `parser.go`：无运算符优先级解析逻辑；`parseSelectColumn`（L2058）只接受 `*` 投影 / `@var` / `CASE` / 列名 / 聚合，**`SELECT k*2` 中 `*` 词法通过但语法层无算术语义**（`*` 只会被当作投影星号），`SELECT 1` 这类常量投影也不支持（tokNumber 未处理）
- `value.go`：无任何算术求值函数（Add/Sub/Mul/Div/Mod 不存在）
- `executor.go`：WHERE 匹配（`matchCondRel`）、列投影、ORDER BY key 计算均无表达式求值入口

**测试证据**：全量测试扫描未发现任何算术表达式用例（`+ - * / %` 仅出现在字符串/注释中），与该能力缺失一致。

### 2.2 缺失根因

- 三层全缺：**词法层未定义算术 token**（第一道闸门，导致 `unexpected character`）；**语法层无表达式 AST 与优先级解析**；**执行层无求值函数与接入点**。属于从零补齐一条"表达式链路"，而非局部修补。

### 2.3 修复方案与工作量

**工作量：中~大（约 2~4 人日）**

| 层 | 文件 | 改动 |
|---|---|---|
| 词法 | `lexer.go` | 新增 `tokPlus/tokMinus/tokSlash/tokPercent`；`lex()` 增加 `+` `/` `%` 分支；`-` 先判数字字面量（负数）再判 `tokMinus`（注意与 L103 现有负数分支的优先级） |
| 语法 | `ast.go` | 新增表达式节点：`Expr`（列引用 / 数字常量 / `BinaryExpr{Op,Left,Right}`），支持括号嵌套 |
| 语法 | `parser.go` | 表达式优先级解析（`*` `/` `%` 高于 `+` `-`，支持 `()`），接入：SELECT 列（`parseSelectColumn` 检测二元表达式起始 token）、WHERE 条件（现有 Cond 扩展左/右操作数为表达式）、ORDER BY、GROUP BY |
| 执行 | `value.go` | 算术求值：INT 走 int64、DECIMAL 保持精度（复用现有 scaled int64 表示），除零/模零报错，类型不符报错 |
| 执行 | `executor.go` | 投影（SELECT 列计算）、`matchCondRel`（如 `WHERE k+1 > 5`）、ORDER BY key 计算三处接入表达式求值 |
| 测试 | 新增 | lexer（各运算符/负号歧义）、parser（优先级/括号）、端到端（`k+1`、`k*2`、`k/0` 报错、DECIMAL 精度、`WHERE k+1>5`、`ORDER BY k+1`、空表） |

> 提示：`pkg/sql` 已有 Cond 比较与 CASE 求值框架，表达式求值可复用 `rowValue` / `relValue` 取列、`compareVal` 比较，降低接入成本；但多子句接入点分散，是本项工作量高于其余两项的主因。

---

## 3. SELECT DISTINCT

### 3.1 现状证据

- `lexer.go` keywords 表（L40-105）：**无 `DISTINCT` 关键字**（会被当作普通 ident）
- `parser.go` L1796-1809 `parseSelect`：`expectKeyword("SELECT")` 后直接进入 `parseSelectColumn` 循环，**无 DISTINCT 分支**
- `ast.go` `SelectStmt`：**无 `Distinct` 字段**
- `executor.go`：**无任何去重逻辑**（全文件扫描 `distinct` 零命中）
- 现状行为：`SELECT DISTINCT k FROM t` → `DISTINCT` 被当作列名解析（tokIdent），`k` 被当作第二列 → 报 `unknown column: DISTINCT` 一类错误（若表恰有该列则静默错语义）；`SELECT DISTINCT *` 同理失败
- E5 处理：以 `SELECT k FROM t GROUP BY k` 等价替代（复用 `groupRel` 分组能力）

**测试证据**：全量测试扫描无任何 DISTINCT 用例。

### 3.2 缺失根因

- 全链路缺失：关键字未登记 → parser 无语法分支 → AST 无字段 → 执行无去重算子。属"未设计"而非"设计受限"。

### 3.3 修复方案与工作量

**工作量：小~中（约 1~2 人日）**

| 层 | 文件 | 改动 |
|---|---|---|
| 词法 | `lexer.go` keywords | 登记 `"DISTINCT": true` |
| 语法 | `parser.go` `parseSelect` | `SELECT` 后检测 `DISTINCT` 关键字并跳过；`ast.go` `SelectStmt` 加 `Distinct bool` |
| 执行 | `executor.go` | 建议复用 `groupRel` 的按键分桶思路：把"全 SELECT 列"作为分组键做全行去重（注意 NULL 语义与行序稳定性，键拼接用现有 `String()` + 分隔符方案）；在 `execSelectRel` / 旧路径投影前插入去重 |
| 测试 | 新增 | 单列/多列 DISTINCT、含 NULL、`DISTINCT + WHERE`、`DISTINCT + ORDER BY/LIMIT`、空表；若与聚合增强合排，补 `COUNT(DISTINCT col)` |

> 简化提示：DISTINCT 本质是"全列分组"，与 `groupRel` 高度同构，可先复用分组框架再抽公共去重算子，避免新写一套 map 逻辑。

---

## 4. 修复优先级建议（按对 E5 基准 / T38 真实验证影响排序）

| 优先级 | 项目 | 依据（E5/T38 影响） | 工作量 | 建议 |
|---|---|---|---|---|
| **P0** | 算术表达式 | E5 中 TPC-C 累加被迫改为客户端读-改-写（每笔多 2 次往返，TPS 失真）；sysbench 部分 SQL 无法直译；且是聚合表达式、HAVING 等后续能力的前置 | 中~大（2~4 人日） | 立即启动，先词法→语法→执行逐层落地，配套端到端测试 |
| **P1** | MAX/MIN 聚合 | 真实业务/报表聚合查询高频使用；E5 聚合脚本无法覆盖最大值最小值语义；实现成本最低、复用现有 agg 框架 | 小（0.5~1 人日） | 与 P0 并行，交付快、风险低 |
| **P2** | SELECT DISTINCT | 对 E5 影响小（GROUP BY 等价替代已验证可用）；但 SQL 兼容盲区明显（基准脚本需主动规避），实现成本低 | 小~中（1~2 人日） | 建议与 `COUNT(DISTINCT col)` 合排，一次补齐聚合+DISTINCT 交叉能力 |
| **P3（可选）** | 裸别名、HAVING、聚合参数表达式、GROUP BY 后 ORDER BY/LIMIT | 提升 SQL 标准兼容度，非 E5/T38 阻塞项 | 中 | 后续迭代，按依赖关系排在 P0/P2 之后 |

**优先级逻辑**：E5 基准的"真实性"最受算术表达式拖累（客户端读-改-写改变事务轮廓）；MAX/MIN 影响 SQL 覆盖面但实现成本最低可快速闭环；DISTINCT 已有等价替代、不阻塞基准，但与聚合增强有交叉收益。三者修复后，E5 压测脚本与 T38 真实验证可移除对应 capability probe 规避逻辑。

---

## 5. 参考

- 代码：`pkg/sql/lexer.go`、`pkg/sql/parser.go`、`pkg/sql/ast.go`、`pkg/sql/executor.go`、`pkg/sql/value.go`、`pkg/sql/orderlimit.go`
- 测试：`pkg/sql/sql_test.go`、`pkg/sql/m10_window_test.go`、`pkg/sql/matview_test.go`
- E5 基准实测：`docs/experiments/E5_standard_bench_alignment.md`
*（内容由AI生成，仅供参考）*

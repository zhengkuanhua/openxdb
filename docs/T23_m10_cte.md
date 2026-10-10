---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_fa0e3e54c4b211f19063525400393706
    ReservedCode1: EeVBOs4FI3T3UtgATGNbdfpg717mBFPA/HFccpMNw11kZEBdKwLbxFPE3UHF8V5oyQZ/PrgcP4EfpbFR2HKSbR4uZ/7WyKd9qUEOm66bat1ZRTF/wcDYeGMbLxFGwKBondkCJE2s9YaQdHquMcNW8mHTwvfQWlmxrl0xRaDu5bp4yhx0o5jWbegwJyE=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_fa0e3e54c4b211f19063525400393706
    ReservedCode2: EeVBOs4FI3T3UtgATGNbdfpg717mBFPA/HFccpMNw11kZEBdKwLbxFPE3UHF8V5oyQZ/PrgcP4EfpbFR2HKSbR4uZ/7WyKd9qUEOm66bat1ZRTF/wcDYeGMbLxFGwKBondkCJE2s9YaQdHquMcNW8mHTwvfQWlmxrl0xRaDu5bp4yhx0o5jWbegwJyE=
---

# T23 M10 功能簇（1/3）：CTE（WITH 子句）

> 版本：v5.0-P12-M10, v0.14.0-alpha
> 日期：2026-10-10
> 范围：`pkg/sql`（lexer / ast / parser / executor + 测试 `m10_cte_test.go`）+ README 勾选
> 闭环：`go build ./...` 与 `go test ./... -count=1` 全绿，不 push
> 提交：`M10: CTE (WITH cte AS (SELECT ...) SELECT ..., multi/chain/cycle guard)`（本地，未 push）

## 背景

查询层此前只能直接引用物理表或视图，复杂查询（多层过滤、重复引用同一中间结果）需反复书写或依赖视图。M10 CTE 将 **语句级命名查询** 引入 SQL 层：`WITH cte AS (SELECT ...) SELECT ...` 允许在一个语句内定义临时关系，支持单/多 CTE、CTE 引用 CTE（兄弟互引）、与视图互引，为窗口函数等后续功能提供可组合的中间结果载体。

## 设计

### 语法

```sql
WITH cte AS (SELECT ...) SELECT ...;            -- 单 CTE
WITH a AS (SELECT ...), b AS (SELECT ...) SELECT ...;  -- 多 CTE
WITH a AS (SELECT ... FROM b), b AS (SELECT ...) SELECT ...; -- 兄弟互引
```

### 词法 / 语法层
- `lexer.go`：关键字表补充 `WITH`；
- `ast.go`：新增 `CTE{Name string, Select *SelectStmt}`，`SelectStmt` 增加 `CTEs []CTE` 字段；
- `parser.go`：`parseSelect` 开头检测 `WITH`，循环解析 `name AS (子查询)` 并断言为 `SelectStmt`，追加到 `stmt.CTEs`；`parseStmt` 分发 `WITH` 到 `parseSelect`。

### 执行层（executor）
- `expandCTEs`：`execSelect` 入口（先于视图展开）调用，语句内作用域展开；
- `expandCTEsInner`：递归处理——①注册本层 CTE（同名覆盖）；②FROM/JOIN 中引用 CTE 名时替换为定义深拷贝子查询（`cloneSelect`，JSON 往返防共享 AST 污染），别名取原表名/显式别名；③递归展开子查询、JOIN 子查询、本层 CTE 定义（stack 推进检测循环）；
- `cyclicCTE`：栈式循环引用防护——CTE 直接/间接引用自身报 `cyclic CTE reference`；
- 未定义 CTE 名不特殊处理，退化为普通表名解析（报 `table not exists`），与 MySQL 行为一致；
- CTE 与视图互引：CTE 定义展开后由 `execSelect` 递归进入视图展开；视图展开为子查询后同样经 `execSelect` 递归执行，两套机制可组合。

## 测试（pkg/sql/m10_cte_test.go）

| 测试 | 覆盖 |
|---|---|
| `TestCTESingle` | 单 CTE 基本查询 |
| `TestCTEMultipleAndChain` | 多 CTE + CTE 引用 CTE（链式） |
| `TestCTEWithView` | CTE 定义引用视图 |
| `TestCTEJoin` | JOIN 中使用 CTE |
| `TestCTEUndefined` | 未定义 CTE 名 → `table not exists` |
| `TestCTECyclic` | 直接循环引用 → `cyclic CTE reference` |
| `TestCTECyclicIndirect` | a→b→a 间接循环引用 → `cyclic CTE reference` |

## 取舍说明

- CTE 采用 **展开式宏** 而非物化：每次执行实时展开底层 SQL，保证数据新鲜、无物化缓存一致性负担；代价是复杂 CTE 每次全量执行；
- 循环防护以 **执行期栈式检测** 为主（CTE 是语句级临时对象，不持久化，无需建期静态检查；视图建期静态检查仍保留）；
- 未定义 CTE 名不报“unknown CTE”而是退化为表名解析：语义简洁且与标准 SQL 兼容（MySQL 同样将其作为表名处理）；
- CTE 同名覆盖语义：语句内后定义覆盖先定义（SQL 标准作用域规则），不展开跨语句持久化。

## README 勾选

- Status 行更新为 `M10 SQL compatibility cluster: CTE + window functions + UPSERT (v5.0-P12-M10), v0.14.0-alpha.`
- T 表格追加 `+ T23 M10 SQL compatibility cluster: CTE + window functions + UPSERT`
- 文档列表追加 `T23_m10_cte.md` / `T23_m10_window.md` / `T23_m10_upsert.md`（三子功能文档随各自提交依次加入）

*（内容由AI生成，仅供参考）*

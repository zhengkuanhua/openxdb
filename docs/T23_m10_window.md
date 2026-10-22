# T23 M10 功能簇（2/3）：窗口函数（OVER 子句）

> 版本：v5.0-P11-M10WIN, v0.13.0-alpha
> 日期：2026-10-11
> 范围：`pkg/sql`（lexer / ast / parser / executor）+ 单元测试（`m10_window_test.go`）+ README 勾选
> 闭环：`go build ./...` 与 `go test ./... -count=1` 全绿，不 push
> 提交：`M10: window functions (OVER)`（本地，未 push）

## 背景
SQL 层此前只有普通列、CASE 与全局聚合（COUNT/SUM/AVG 整表一值）。M10 窗口函数引入 **OVER 子句**：在保留每行明细的同时，按 PARTITION BY 分区、窗口内 ORDER BY 排序，逐行计算排名（ROW_NUMBER/RANK/DENSE_RANK）与累计聚合（SUM/AVG/COUNT）。窗口函数仅允许出现在 SELECT 列表（WHERE/GROUP BY/ORDER BY 中出现即解析层报错），避免破坏查询阶段语义。

## 设计

### 语法
```sql
SELECT ROW_NUMBER()   OVER (PARTITION BY col1, col2 ORDER BY col ASC|DESC) ...
SELECT RANK()         OVER (ORDER BY col) ...
SELECT DENSE_RANK()   OVER () ...
SELECT SUM(col)       OVER (PARTITION BY col1 ORDER BY col2) ...
SELECT AVG(col)       OVER (...) ...
SELECT COUNT(*)       OVER (...) ...
```

### 词法 / 语法层
- `lexer.go`：关键字表已含 `OVER` / `PARTITION`（M10 规划时预埋）；
- `ast.go`：`SelectColumn` 新增 `Window *WindowSpec` 字段，`Agg` 允许值扩展 `ROW_NUMBER` / `RANK` / `DENSE_RANK`（复用既有 `WindowSpec{PartitionBy []string; OrderBy *OrderBy}`）；
- `parser.go`：
  - 排名函数 `ROW_NUMBER()` / `RANK()` / `DENSE_RANK()` 无参解析后**必须**跟随 `OVER`，否则报 `window function %s() requires OVER clause`；
  - `COUNT(*)` 与 `SUM(arg)` / `AVG(arg)` 解析后若遇 `OVER` 关键字则调用 `parseWindowSpec` 填充 `sc.Window`（可选 OVER，缺省为普通聚合）；
  - 新增 `parseWindowSpec(sc)`：解析 `OVER ( PARTITION BY col,... [ORDER BY col ASC|DESC] )`，PARTITION BY 支持多列，ORDER BY 单列（复用 `parseOrderBy` 单列能力）；
  - 非法位置拦截：`parseColumnRef` 在 SELECT 列表之外（WHERE / GROUP BY / ORDER BY 等）检测到 `函数名(` 时返回 `function %s() only allowed in SELECT list`。

### 执行层（executor）
- 窗口计算核心 `computeWindowCol(rows, getter, col)`：
  1. **分区**：按 `WindowSpec.PartitionBy` 各列取值拼键分组，无 PARTITION 时全结果集一组；
  2. **组内排序**：按窗口 `OrderBy` 键稳定排序（支持 DESC），无 ORDER BY 保持输入行序；
  3. **逐行求值**：按 `Agg` 分发——
     - `ROW_NUMBER`：组内序号 1..n；
     - `RANK`：排序键相同时并列同号，后续跳号（gap）；
     - `DENSE_RANK`：排序键并列同号但连续编号（无 gap）；
     - `SUM/AVG/COUNT`：默认框架 `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`，按窗口序逐行累计；SUM/AVG 支持 INT/DECIMAL，对 TEXT 等非数值列报 `window aggregate on non-numeric column`；
  4. `computeWindows` 按 SELECT 列表顺序批量计算全部窗口列，产出 `winVals[i][j]`。
- 投影接入两条路径：
  - 单表路径 `finishSelect`：检测 `hasWindow`——与全局聚合混用报 `window function cannot be mixed with aggregate`，否则 `finishSelectWindow` 先投影普通列、再按行回填窗口值；
  - 关系路径 `finishRel`：收集窗口列后调用 `computeWindows`（getter 用 `relValue` 取限定名 `alias.col`），随后统一输出行。
- 聚合检测修正：`execSelectRel` 的 `hasAgg` 检测**排除**窗口列（窗口列不再误入全局聚合分支）；`GROUP BY` 与窗口函数混用自 P1-10 起合法（窗口在分组聚合结果上计算，见 pkg/sql/p1_10_edge_test.go）。

## 测试（pkg/sql/m10_window_test.go）

| 测试 | 覆盖 |
|---|---|
| `TestWindowRowNumber` | ROW_NUMBER 窗口序 1..n |
| `TestWindowRankDenseRank` | RANK 带 gap 与 DENSE_RANK 连续编号并列值 |
| `TestWindowPartitionSingle` | 单列 PARTITION BY + 窗口内 DESC 编号 |
| `TestWindowPartitionMulti` | 多列 PARTITION BY 联合分区 |
| `TestWindowAggRunning` | SUM/AVG/COUNT 逐行累计（整数除法 AVG） |
| `TestWindowNoPartition` | 无 PARTITION / 无 ORDER BY：全组 RANK=1、SUM 按行序累计 |
| `TestWindowOrderDesc` | 窗口内 ORDER BY DESC |
| `TestWindowAliasAndPlainCol` | 窗口列别名 + 与普通列混合 |
| `TestWindowWithJoin` | JOIN 后窗口函数（rel 路径 finishRel） |
| `TestWindowWithCTE` | CTE 与窗口函数结合 |
| `TestWindowEmpty` | 空表窗口查询返回空 |
| `TestWindowMissingOver` | ROW_NUMBER/RANK/DENSE_RANK 缺 OVER 报错 |
| `TestWindowIllegalPosition` | WHERE / GROUP BY / ORDER BY 中窗口函数报错 |
| `TestWindowMixedAggregate` | 与 GROUP BY 混用、与全局聚合混用报错 |
| `TestWindowNonNumericAgg` | 对 TEXT 列 SUM 报错 |

## 取舍说明
- 排名函数（ROW_NUMBER/RANK/DENSE_RANK）**强制**要求 OVER；聚合函数（SUM/AVG/COUNT）OVER 可选——缺省即普通全局聚合，保持向后兼容；
- 窗口默认框架固定为 `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`（逐行累计），暂不支持显式 `ROWS/RANGE` 框架与 `PRECEDING/FOLLOWING`，后续可扩展；
- 窗口 ORDER BY 当前支持单列；多列窗口排序列为后续扩展点；
- 窗口函数限定 SELECT 列表（解析层拦截）；与 GROUP BY 混用自 P1-10 起支持（窗口在分组聚合结果行上计算），M10 阶段"明确拒绝"的限制已解除；
- PARTITION BY / ORDER BY 支持限定列名（`alias.col`），JOIN 场景可正确取值。

## README 勾选

- Status 行更新为 `M10 SQL compatibility cluster: CTE + window functions + UPSERT (v5.0-P12-M10), v0.14.0-alpha.`
- T 表格追加 `+ T23 M10 SQL compatibility cluster: CTE + window functions + UPSERT`
- 文档列表追加 `T23_m10_cte.md` / `T23_m10_window.md` / `T23_m10_upsert.md`（三子功能文档随各自提交依次加入，本子功能提交暂不修改 README）

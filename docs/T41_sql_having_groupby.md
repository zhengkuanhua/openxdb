---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_4913735ecdd111f1898e525400248c00
    ReservedCode1: If/qtkFoYPRQMoPnEjEU7bKbCAVpfaOOd1Kzs9VMgHQ14QTWSKL+sl/eS7RzMbmLGOFgrdRGqcEL0vaws4ZqLik1EmXLdbzbyQLi2afrRTYlV4qLjx/0lKXmRCugCmLTLCvmyhTB915PVCcGtAC1SlFCrTzQ11FTc/3/YSSbMje+Lsp9S8hhmwoieLM=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_4913735ecdd111f1898e525400248c00
    ReservedCode2: If/qtkFoYPRQMoPnEjEU7bKbCAVpfaOOd1Kzs9VMgHQ14QTWSKL+sl/eS7RzMbmLGOFgrdRGqcEL0vaws4ZqLik1EmXLdbzbyQLi2afrRTYlV4qLjx/0lKXmRCugCmLTLCvmyhTB915PVCcGtAC1SlFCrTzQ11FTc/3/YSSbMje+Lsp9S8hhmwoieLM=
---



# T41 HAVING + GROUP BY 语义补全（v1.2.0 P0-4）

- 版本: v1.2.0
- 范围: `pkg/sql`（lexer / parser / ast / executor）+ 测试 `pkg/sql/p04_having_groupby_test.go`
- 结论先行：**HAVING 子句、聚合表达式参数（`SUM(a+b)` 等）、`COUNT(DISTINCT expr)`、GROUP BY 后 ORDER BY / LIMIT 均已落地并通过测试**；既有测试除排期外已知失败 `TestP1DecimalAgg`（空表 AVG 返回 NULL，禁止越界修改）外全部通过。
- 对应消项：docs/T39_sql_capability_gap_assessment.md §1.1 缺失清单第 2（HAVING 过滤聚合）、第 3（聚合参数放宽 / COUNT(DISTINCT)）、第 5（GROUP BY 后 ORDER BY/LIMIT 被忽略）。

## 1. HAVING 子句

### 1.1 语法

```
SELECT 分组列/聚合... FROM t [WHERE ...] GROUP BY ... HAVING <条件> [ORDER BY ...] [LIMIT ...]
```

- 条件支持：聚合调用（`COUNT(*)` / `SUM(x)` / `AVG(x)` / `MAX(x)` / `MIN(x)`）、分组列引用、表达式左操作数（如 `SUM(x)+1 > 3`），运算符 `= != < > <= >=`，右值为字面量、列引用或表达式。
- 无 GROUP BY 时亦可使用（对全局聚合单行过滤）。
- 限制：HAVING 内子查询在解析期即被拦截（报 `expected expression`）；`EXISTS` 在条件求值时报 `EXISTS not supported in HAVING`；HAVING 引用非分组列/非聚合列报 `unknown column in HAVING: <col>`。

### 1.2 实现

| 位置 | 说明 |
|---|---|
| `lexer.go` | 注册 `HAVING` 关键字 |
| `ast.go` | `Cond` 新增 `LeftAgg *SelectColumn`，支持聚合左操作数 |
| `parser.go` | `parseSelect` 在 GROUP BY 后接入 `parseHaving`；`parseHavingCond` / `parseHavingRight` 实现条件与右值解析 |
| `executor.go` | `aggRel`（全局聚合）与 `groupRel`（分组聚合）在结果行上逐条调用 `havingMatch` 过滤；`finishRel` 支持无聚合 HAVING（聚合调用报错）；`havingMatch` 支持 LeftAgg / LeftExpr / 分组列求值，非法列引用带 `in HAVING` 上下文报错 |

## 2. 聚合参数放宽

| 能力 | 说明 |
|---|---|
| 表达式参数 | `SUM(a+b)` / `AVG(price*2)` 等，参数经 `parseAggSelectColumn` 解析为 `Expr`，单表路径 `aggColumn` 与 Rel 路径 `aggValues` 均通过表达式求值 |
| `COUNT(DISTINCT col)` | 解析填充 `Distinct` 标志，聚合计数按去重后的值集合统计 |
| `COUNT(DISTINCT expr)` | 对表达式结果做去重计数（如 `COUNT(DISTINCT salary%200)`） |

## 3. GROUP BY 后 ORDER BY / LIMIT

- 原实现 `execSelectRel` 在 `len(s.GroupBy) > 0` 时直接返回 `groupRel(...)`，排序/分页完全不执行；现 `groupRel` 尾部接入 `sortGroupResult`（按输出列或分组键稳定排序）与 `applyOffsetLimit`，`ORDER BY 别名/列` 与 `LIMIT/OFFSET` 均生效。
- 全局聚合（`aggRel`）同样保持排序/分页能力。

## 4. 测试

- 新增 `pkg/sql/p04_having_groupby_test.go`：
  - `TestP04HavingBasic` / `TestP04HavingAggOps` / `TestP04HavingGroupCol` / `TestP04HavingNoGroupBy` / `TestP04HavingExprLeft`：HAVING 各形态正确性
  - `TestP04HavingErrors`：非法列引用、EXISTS、子查询、无 FROM 聚合错误路径
  - `TestP04AggExprParams` / `TestP04AggDistinct`：表达式参数与 DISTINCT 计数
  - `TestP04GroupOrderLimit` / `TestP04HavingPagination`：GROUP BY 后排序分页及 HAVING 组合

## 5. 验证

- `go test ./pkg/sql/ -run TestP04 -count=1`：全部通过
- `go test ./pkg/sql/ -count=1`：仅 `TestP1DecimalAgg` 失败（排期外既有失败，未越界修改）
- `go build ./...` 通过；`gofmt -l` 相关文件无输出

## 6. 文档同步

- `KNOWN_LIMITATIONS.md` §1.1：移除"不支持 HAVING"描述，标注 v1.2.0 支持范围
- `docs/product_manual_zh.md` §10 已知限制：同步更新
*（内容由AI生成，仅供参考）*

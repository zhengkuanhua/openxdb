# T22 M9-3 批量导入（Bulk Import）

## 目标

完善 pkg/sql 既有 `ImportStmt`（AST L202 / parser parseImport / csv.go execImport），
补齐三方面能力：

1. 批量 INSERT 多行（`INSERT INTO t (cols) VALUES (...), (...), ...`）一次批量提交；
2. 高效批处理提交与错误行计数：`IMPORT ... BATCH <n>` 逐批落 WAL，`IMPORT ... IGNORE ERRORS`
   跳过坏行并统计 inserted / skipped / errors / batches；
3. 导入过程事务性：默认严格原子（坏行整体回滚），BATCH 模式逐批提交（每批独立事务）。

## 语法

```
IMPORT INTO <table> FROM '<csv-path>' [BATCH <n>] [IGNORE ERRORS]
INSERT INTO <table> [(cols)] VALUES (...), (...) [, ...]
```

- `BATCH <n>`：按每 n 个成功行一批提交（`n > 0`，否则解析报错）。每批 `exec2pcWrite`
  提交（落 WAL），随后开启新事务续读，尾批提交后回滚预开的空事务。
- `IGNORE ERRORS`：坏行（字段类型错误 / 列数不匹配）计数跳过不中断，好行照常插入；
  与表中已存在主键冲突的行按 skipped 计数（沿用既有 dupSkip 语义）。
- 默认（无 BATCH / 无 IGNORE ERRORS）：整个 CSV 单事务原子提交，任一行出错整体回滚。

## 实现要点

- **AST**（`pkg/sql/ast.go`）：`ImportStmt` 新增 `Batch int`、`IgnoreErrors bool` 字段。
- **Parser**（`pkg/sql/parser.go`）：`parseImport` 解析可选 `BATCH <n>`（`tokNumber`）
  与 `IGNORE ERRORS`（`tokIdent` 大小写不敏感）后缀，非法后缀报
  `unexpected token after IMPORT`。
- **Executor**（`pkg/sql/csv.go`）：`execImport` 重写为
  - 收集 `kvOp` 列表，按 `s.Batch` 阈值调用 `flush()`（`exec2pcWrite` 提交 + 新事务续读）；
  - `IGNORE ERRORS` 时坏行 `bad++` 跳过、行级错误 `errors++` 语义合并为 bad 计数；
  - **文件内重复主键防护**：新增 `seen` 集合记录同批已收集行主键
    （事务内未提交写对唯一性检查不可见，否则后行覆盖前行），重复行按 skipped 计数；
  - 返回 `inserted / skipped / errors / batches` 四列结果，`AffectedRows = inserted`。
- **多行 INSERT**：parser 既有 `parseInsert` 已支持多行 VALUES，executor 原样批量执行，
  任一行主键冲突整条语句报错（原子语义）。

## 取舍与说明

- BATCH 与 IGNORE ERRORS 可组合：按成功行分批（坏行不计入批次计数）。
- 逐批提交的非原子性为显式可选策略：用户明确要求分批时，中途失败会留下已提交批次
  （与 MySQL LOAD DATA 分批语义一致）；默认仍是原子导入。
- `seen` 修复同时覆盖"CSV 文件内重复主键"历史盲区：既有 `TestP1CsvImportSemantics`
  保持通过，无回归。

## 测试（pkg/sql/m9_bulk_import_test.go）

- `TestM9MultiRowInsertBatch`：100 行多行 INSERT 一次提交，主键冲突整条回滚。
- `TestM9ImportStrictAtomicRollback`：默认严格模式坏行整体回滚，0 行落库。
- `TestM9ImportIgnoreErrors`：类型坏行 / 列数坏行 / 重复主键混合，inserted=2 skipped=1 errors=2。
- `TestM9ImportBatchCommit`：BATCH 2 导入 5 行 → 3 批（2+2+1）。
- `TestM9ImportBatchWithErrors`：BATCH+IGNORE 组合，坏行跳过按成功行分批。
- `TestM9ImportParseOptions`：语法层 BATCH 0 报错、未知后缀报错、组合选项正常执行。

## 验证

- `go build ./...` 通过；
- `go test ./... -count=1` 全绿（13 包 ok，含新增 6 个 M9 批量导入测试）。

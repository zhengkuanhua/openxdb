---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_1c70c22dc2ec11f18019525400248c00
    ReservedCode1: 7g1DsBfCd6WdIGr44mJ0zk9JZl7+dnAxa1tcmXWJ6AhHO3wRk9F3ivKRhfSz/pewit0gQNhHZjprqj6e/ARV/5+snLxHK04+TAuAMxXJ4AMAANcK7W1kXSmowJzXSCBFflEPTd99Gvz0ff4TH59W6EO1SQ/ThF3ubo/cBkxATyeRvK75mLjRqZvSXVA=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_1c70c22dc2ec11f18019525400248c00
    ReservedCode2: 7g1DsBfCd6WdIGr44mJ0zk9JZl7+dnAxa1tcmXWJ6AhHO3wRk9F3ivKRhfSz/pewit0gQNhHZjprqj6e/ARV/5+snLxHK04+TAuAMxXJ4AMAANcK7W1kXSmowJzXSCBFflEPTd99Gvz0ff4TH59W6EO1SQ/ThF3ubo/cBkxATyeRvK75mLjRqZvSXVA=
---



# T9 P1 设计记录：CSV 导入导出 + DATE/DECIMAL/BLOB 类型扩充

## 目标

在 P0（JOIN/GROUP BY/子查询/BETWEEN/IN/会话事务）基础上推进 P1：

1. `CREATE TABLE` 列类型扩充为 **INT / TEXT / DATE / DECIMAL / BLOB**，既有 INT/TEXT 行为不回归。
2. 提供 **EXPORT / IMPORT** 两个 SQL 语句入口，实现表（或列子集）与 CSV 文件之间的往返导入导出。
3. 类型系统全链路联动：比较（`compareVal`）、排序、索引键编码（`idxBytes`）、主键编码（`pkBytes`）、行编码（`encodeRow/decodeRow`）、WHERE 匹配、聚合（SUM/AVG）对新类型行为完整定义并实现。

## 新增/修改文件

| 文件 | 变更 |
|------|------|
| `pkg/sql/value.go` | 新增：DATE 校验、DECIMAL 定点解析/规范化、BLOB hex 编解码、`coerceValue`、`decVal` |
| `pkg/sql/ast.go` | 修改：`Value.Kind`/`ColumnDef.Type` 注释扩展；新增 `ExportStmt`/`ImportStmt` |
| `pkg/sql/lexer.go` | 修改：关键字表新增 `EXPORT`/`IMPORT`/`TO`；`lexNumber` 支持小数 |
| `pkg/sql/parser.go` | 修改：`parseStmt` 分发 EXPORT/IMPORT；`parseCreate` 类型白名单扩展；`parseValue` 支持 DECIMAL 字面量；新增 `parseExport`/`parseImport` |
| `pkg/sql/executor.go` | 修改：`Execute` 分发；`coerceRow` 改用 `coerceValue`；`execUpdate` SET 类型强制；`doAgg`/`aggColumn` 支持 DECIMAL 聚合；`compareVal` DECIMAL/BLOB 语义；`indexLookup` BLOB 索引列字面量解码 |
| `pkg/sql/meta.go` | 修改：`idxBytes`/`pkBytes` 支持 DECIMAL/BLOB；`encodeRow`/`decodeRow` 按新类型序列化 |
| `pkg/sql/csv.go` | 新增：`execExport`/`execImport`/`parseCell`/`csvValue` |
| `pkg/sql/sql.go` | 修改：`IsSQL` 关键字路由追加 `EXPORT`/`IMPORT` |
| `pkg/sql/sql_test.go` | 修改：新增 9 个 P1 用例（TestP1*） |
| `docs/T9_p1_csv_types.md` | 新增：本文档 |
| `README.md` | 修改：SQL subset、Highlights、Roadmap 勾选 P1 |

## 类型系统

### 总览

| 类型 | 内部形态 | 存储序列化（encodeRow） | 主键/索引键字节 | 排序/比较语义 |
|------|----------|------------------------|----------------|---------------|
| INT | `I` int64 | JSON number | 定长 8B BE（主键原值 / 索引翻转符号位） | 数值序 |
| TEXT | `S` string | JSON string | 原字节 | 字典序 |
| DATE | `S` string（`YYYY-MM-DD`） | JSON string | 原字节 | 字典序 = 时间序 |
| DECIMAL | `I` 缩放 int64（scale=4）+ `S` 规范化字符串 | JSON string（规范化） | 翻转符号位定长 8B | 数值序（跨 INT 亦数值） |
| BLOB | `S` 大写 hex 文本 | JSON string（大写 hex） | hex 解码后的原始字节 | 原始字节序（hex 字符序 = 字节序） |

### DATE

- 文本形态 `YYYY-MM-DD`，插入/更新/导入时经 `validateDate` 做真实日历校验（含闰年）。
- 字典序即时间序，排序、索引键、主键直接使用原字节，无需额外编码。
- 非法值（格式错误、月份 13、2 月 30 日等）在 `coerceValue`/`parseCell` 阶段报错拒绝。

### DECIMAL

- 定点数，固定小数位 **scale=4**（`decUnit=10000`）。
- 内部以 `int64` 缩放表示（`Value.I`），展示与存储用规范化字符串（`Value.S`，如 `123.4500`）。
- 输入接受 `[-+]?digits[.digits]`：整数自动补 4 位小数；小数位超过 4 位报错（不截断不四舍五入）；超出 int64 缩放范围报错。
- 比较语义：DECIMAL 与 DECIMAL / INT 一律数值序（`compareDec`，INT 放大到 scale 后比较，避免溢出采用商/余两段比较）。
- 聚合：`SUM`/`AVG` 支持 DECIMAL 列，累加/平均以缩放整数进行，结果保持 4 位小数精度；空表 `AVG` 返回 `0.0000`。INT 与 DECIMAL 混聚时按 DECIMAL 输出。

### BLOB

- 二进制大对象；SQL 与 CSV 的输入输出形态均为**大写十六进制文本**（如 `0AFF`、`DEADBEEF`）。
- `encodeRow` 存大写 hex 字符串；`decodeRow` 还原为 BLOB Value。
- 主键/索引键编码为 hex 解码后的原始字节；`pkBytes` 在主键列为 BLOB 时，对 WHERE 字面量（TEXT 形态）统一按 hex 解码，保证点查与存储键一致；`indexLookup` 同理对 BLOB 索引列字面量先解码再编码。
- 比较语义：BLOB 与 BLOB 按解码后原始字节比较；BLOB 与 TEXT 比较时，TEXT 侧若为合法 hex 亦解码后比较（宽松匹配，例如 `WHERE data = 'deadbeef'` 命中存储 `DEADBEEF`）。

### 类型强制（coerceValue）

| 列类型 | 接受输入 | 拒绝 |
|--------|----------|------|
| INT | INT | 其余全部 |
| TEXT | TEXT / DATE（归一为 TEXT） | INT / DECIMAL / BLOB |
| DATE | DATE / 合法 `YYYY-MM-DD` 文本 | 其余 |
| DECIMAL | DECIMAL / INT / 合法数字文本 | 其余 |
| BLOB | BLOB / 合法 hex 文本 | 其余 |

## CSV 导入导出

### 语法

```
EXPORT TABLE t [(col, ...)] TO 'path'
IMPORT INTO t FROM 'path'
```

- `EXPORT TABLE t TO 'path'`：导出表全部列。
- `EXPORT TABLE t (a, c) TO 'path'`：导出列子集（按给定列序）。
- `IMPORT INTO t FROM 'path'`：从 CSV 文件导入建行。

### CSV 规范（RFC 4180 风格）

- UTF-8 文本；**首行为表头**（列名，与表定义顺序一致），后续每行为一条记录。
- 字段含逗号/双引号/换行时以双引号包裹，内部双引号翻倍（`""`），由 Go `encoding/csv` 保证。
- 各类型字段文本形态：
  - INT：十进制整数
  - TEXT：原样（含空串）
  - DATE：`YYYY-MM-DD`
  - DECIMAL：十进制数（导出为规范化 4 位小数；导入接受 ≤4 位小数）
  - BLOB：大写十六进制（导入大小写均可）

### 导出行为

- 表内行按主键序导出（存储序）。
- 导出文件为 UTF-8 无 BOM；表头 = 导出的列名。
- 文件写入在事务读快照之后进行，路径不存在时自动创建，覆盖已有文件（同名覆盖由调用方负责确认）。

### 导入行为（原子语义）

- 单事务执行：任一行的列数不匹配 / 类型非法 / 表头不符 → 整体报错回滚，不产生部分写入。
- **主键重复的行跳过**（不覆盖、不报错），返回 `inserted / skipped` 两列计数。
- 表头列名与表定义顺序必须完全一致（顺序敏感，简单明确）。

### 设计取舍

- 选择 `EXPORT/IMPORT` 而非标准 `COPY`：与既有 DDL/DML 关键字风格一致，避免 `COPY` 与复制语义歧义。
- 首行强制表头：导入自描述、可校验列序，避免按位置猜测。
- DECIMAL 采用 int64 缩放（scale=4）：实现简单、比较/索引友好、无浮点误差；代价是范围受限（±9.2e14 缩放值）与固定小数位，文档明确即可。
- BLOB 文本 hex：SQL 子集无二进制字面量，hex 文本与既有字符串 lexer 天然兼容。
- 主键重复跳过而非报错：导入常用语"灌数据"，跳过幂等重放，配合原子性保证不脏写。

## 验证

- 新增 9 个 P1 用例（`pkg/sql/sql_test.go`，`TestP1*`）：
  1. `TestP1CreateInsertNewTypes` — 新类型建表 + 插入 + 查询
  2. `TestP1DateValidation` — DATE 合法/非法边界
  3. `TestP1DecimalCompareOrder` — DECIMAL 比较与排序
  4. `TestP1DecimalAgg` — DECIMAL SUM/AVG 聚合（含空表）
  5. `TestP1BlobRoundtrip` — BLOB 存取/点查/比较往返
  6. `TestP1CsvExportImportRoundtrip` — 导出→导入往返一致性
  7. `TestP1CsvExportColumns` — 列子集导出
  8. `TestP1CsvImportSemantics` — 表头/列数/主键重复/类型错误语义
  9. `TestP1MixedTypesWithIndex` — 混合类型建索引 + 范围查询
- 全仓 `go test ./... -count=1` 全绿（本机真实 CGO 工具链）。
*（内容由AI生成，仅供参考）*

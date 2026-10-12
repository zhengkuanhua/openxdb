---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_13da6eafc57711f197b3525400248c00
    ReservedCode1: s9AjnO1nZtHdNAfExQip6bmNNdMPsv5+oj7mR3BXoGvExOzHrMKaKDwdIMnaOL+wMOnKNLZ+72ZhEJypdKnnTRqAZuNGDcuIPM528/O/Bd0lvXTKuk7mpRvq0gxs5pS4Px2X4QIvD8HvmaJfR4C9cQjzPD/myzz5jKPfV9fX+OxYALRYCmnxjsIBt6M=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_13da6eafc57711f197b3525400248c00
    ReservedCode2: s9AjnO1nZtHdNAfExQip6bmNNdMPsv5+oj7mR3BXoGvExOzHrMKaKDwdIMnaOL+wMOnKNLZ+72ZhEJypdKnnTRqAZuNGDcuIPM528/O/Bd0lvXTKuk7mpRvq0gxs5pS4Px2X4QIvD8HvmaJfR4C9cQjzPD/myzz5jKPfV9fX+OxYALRYCmnxjsIBt6M=
---

---
# T27 M14 功能簇（1/3）：mysqldump 兼容导入

> 版本：v5.0-P16-M14, v0.18.0-alpha
> 日期：2026-10-11
> 范围：`pkg/import`（新增 mysqldump 解析器 mysqldump.go / mysqldump_test.go）、`pkg/sql`（ast.go ImportStmt.Format、parser.go parseImport MYSQLDUMP 分支、csv.go execImport 分派、新增 mysqldump_import.go 执行器与 mysqldump_import_test.go 端到端测试）
> 闭环：`go test ./pkg/sql/ ./pkg/import/ -count=1` 全绿，不 push（README 勾选由 M14 整簇收尾提交统一更新）
> 提交：`M14: mysqldump compatible import`（本地，未 push）

## 背景

M14 生态接入簇的目标是打通 OpenXDB 与外部 MySQL 生态：子功能一实现 **mysqldump 兼容导入**——解析 `mysqldump` 导出的 SQL 文本（`CREATE TABLE` + `INSERT` 语句）并灌入 OpenXDB，实现一键迁移 MySQL 数据库。在 P1 CSV 导入（`IMPORT INTO t FROM 'path'`，T9）基础上新增独立导入通道：复用现有 parser/executor 的 DDL/DML 执行路径，不引入第三方 SQL 解析依赖，纯文本规则解析覆盖 MySQL 常见 dump 语法子集。

## 设计

### 解析器（`pkg/import` 新增包）

`pkg/import/mysqldump.go` 提供纯文本解析器，不依赖 SQL 执行层，输出结构化结果供导入方消费：

```go
type Value struct { Null bool; Raw string }   // NULL 或规范化文本（见类型映射）
type Column struct { Name, Type string; Primary bool }
type Table struct { Name string; Columns []Column; Rows [][]Value }
type Dump struct {
    Tables     []Table   // 按出现顺序：CREATE + 后续 INSERT 累积的行
    Drops      []string  // DROP TABLE IF EXISTS 记录（供导入方重建语义）
    Statements []string  // 其它被跳过的语句（SET/注释等）
    Skipped    []string  // 解析失败被跳过的语句（IgnoreErrors 模式）
}
type Options struct { IgnoreErrors bool }
func Parse(r io.Reader, opts Options) (*Dump, error)
func ParseFile(path string, opts Options) (*Dump, error)
```

**语句切分与识别**：按 `;` 切分语句（跳过 `--` 行注释、`#` 注释、`/* ... */` 与 `/*!...*/` 版本化注释、空行）；识别 `CREATE TABLE` / `INSERT INTO` / `DROP TABLE` / 其余辅助语句（`SET`、`LOCK TABLES`、`UNLOCK TABLES` 等整体跳过）。

**CREATE TABLE 解析**：
- 反引号标识符（含 `` `a``b` `` 转义反引号）与裸标识符；
- 列定义：名称 + 类型 + 可选 `PRIMARY KEY` / `NOT NULL` / `AUTO_INCREMENT` / `DEFAULT ...`；
- 表级 `PRIMARY KEY (...)`；
- 可忽略子句：`ENGINE=...`、`DEFAULT CHARSET=...`、`AUTO_INCREMENT=...`、`COMMENT=...` 等尾部表选项；
- 类型映射：`INT`/`BIGINT`/`SMALLINT`/`TINYINT` → `INT`；`VARCHAR`/`CHAR`/`TEXT`/`LONGTEXT`/`MEDIUMTEXT` → `TEXT`；`DECIMAL`/`NUMERIC` → `DECIMAL`；`DATE`/`DATETIME`/`TIMESTAMP` → `DATE`；`BLOB`/`LONGBLOB`/`MEDIUMBLOB`/`VARBINARY` → `BLOB`；未知类型在严格模式报错、宽松模式跳过该表。

**INSERT 解析**：
- `INSERT INTO t VALUES (...),(...),...` 多值拆分与显式列名列表（`INSERT INTO t (c1,c2) VALUES ...` 按列顺序重排）；
- `INSERT IGNORE` 前缀；
- 值字面量：`NULL`、整数/小数（含负数）、单双引号字符串（反斜杠转义 `\' \n \r \t \0 \b \\` 还原与引号翻倍）、十六进制 `0x...` 与 `X'...'`（BLOB，统一转大写 hex 去前缀）、`TRUE`/`FALSE`；
- 单元格解析失败时：宽松模式丢弃该行并计数，严格模式整次解析报错（返回错误）。

### 服务端 SQL 入口（`pkg/sql`）

复用现有 `IMPORT` 语句家族扩展新语法：

```
IMPORT MYSQLDUMP FROM '<path>' [BATCH n] [IGNORE ERRORS]
```

- `ImportStmt` 新增 `Format` 字段（`""`=CSV，`"MYSQLDUMP"`）；`parseImport` 识别 `MYSQLDUMP` 关键字分支；`execImport` 按 Format 分派到新执行器 `execImportDump`（`pkg/sql/mysqldump_import.go`）。
- **执行语义**：先按顺序处理 `DROP TABLE IF EXISTS`（重建语义：同名表旧数据清除）；`CREATE TABLE` 通过现有 `CreateTableStmt` 路径建表（已存在时报错，宽松模式跳过）；`INSERT` 行经 `dumpRowValues` 类型映射为 OpenXDB 值（`INT`→I、`TEXT`/`DATE`/`DECIMAL`/`BLOB`→S，DECIMAL 复用 `parseDecimal` 规范化固定 4 位小数，NULL 按列类型取零值）后复用 `insertRowOps` 批量写入，`BATCH n` 分批提交，主键冲突跳过计数。
- **结果**：`Result{Columns: [tables, inserted, skipped, errors, batches], Rows: [[...]]}`，与 CSV 导入风格一致。
- **错误容忍**：`IGNORE ERRORS` 宽松模式跳过坏行/坏表继续；严格模式任一错误整体回滚（单事务执行，含 DDL）。

## 测试

### 解析器单元测试（`pkg/import/mysqldump_test.go`）

17 项，覆盖：反引号标识符、类型映射、可忽略子句、内联主键、多值 INSERT、显式列重排、字符串转义、特殊值（TRUE/FALSE/负数）、辅助语句跳过、DROP TABLE、真实 mysqldump 风格样本、IgnoreErrors/严格模式、未定义表 INSERT、未知类型、转义反引号、`X'hex'` 与 `0x` BLOB 字面量。

### 端到端测试（`pkg/sql/mysqldump_import_test.go`）

8 项，覆盖：完整 dump 导入（DROP + 多表 CREATE + 多值 INSERT + SET/注释 + SELECT 验证 DECIMAL 4 位小数/DATE/BLOB hex/NULL 零值）、BATCH 分批、文件内重复主键跳过、IGNORE ERRORS 坏行跳过、严格模式回滚（表不存在）、DROP 重建语义（旧行清除）、字符串转义还原、语法分支（空 dump / 缺 FROM 报错）。

### 验证

```
go test ./pkg/sql/ ./pkg/import/ -count=1   # 全绿（sql 15.6s / import 0.4s）
```

## 边界与后续

- 当前兼容子集：单表 dump 常规输出（含 `SET NAMES`、`LOCK TABLES`、`/*!...*/` 等辅助语句跳过）；不支持存储过程/触发器/视图定义（解析器跳过非 CREATE TABLE/INSERT/DROP 语句）。
- CLI `openxdb import` 命令与 binlog 对外订阅 CDC（子功能二）、工具链收尾与 README 勾选（子功能三）在 M14 后续子功能推进。
*（内容由AI生成，仅供参考）*

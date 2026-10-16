# T32 M19 生态导出：JSON + Parquet

## 1. 背景与目标

M19 为生态互操作补齐结构化数据导出能力。既有 P1 CSV 导出（`EXPORT TABLE t TO 'path'`）面向表格工具，本轮新增两种生态格式：

- **JSON Lines**：流式对象序列，每行一个记录，广泛用于日志管道、数据湖摄取、ETL。
- **Apache Parquet**：列式二进制存储，大数据生态事实标准（Spark/Hive/DuckDB/Polars 直接消费）。

目标：`EXPORT` 语句扩展 `FORMAT` 子句，一行 SQL 完成三种格式导出；Parquet 写入器为项目内自实现最小实现，无第三方依赖。

## 2. 语法扩展

```
EXPORT TABLE t [(col, ...)] TO 'path'                 -- 既有行为，等价 FORMAT CSV
EXPORT TABLE t [(col, ...)] TO 'path' FORMAT CSV
EXPORT TABLE t [(col, ...)] TO 'path' FORMAT JSON
EXPORT TABLE t [(col, ...)] TO 'path' FORMAT PARQUET
```

- `FORMAT` 关键字大小写不敏感，后接 `CSV` / `JSON` / `PARQUET` 之一，缺省 `CSV`（向后兼容）。
- 列子集、全列导出语义与 CSV 完全一致；导出过程同一事务快照。

## 3. JSON 导出（JSON Lines）

### 3.1 格式

- UTF-8 文本，每行一个 JSON 对象，键为列名，值为类型化 JSON 标量：

| OpenXDB 类型 | JSON 值 |
|---|---|
| INT | 数字 |
| DECIMAL | 数字（规范化字符串经 `strconv.ParseFloat` 近似，或保留字符串——见决策） |
| TEXT / DATE / BLOB | 字符串（BLOB 为大写十六进制文本，与 CSV 语义一致） |

- 文件扩展名建议 `.jsonl`；以 `\n` 结尾，无外层数组（流式，可增量消费）。

### 3.2 实现

`execExport` 按 `FORMAT` 分派到 `exportJSON`（`pkg/sql/export_json.go`），复用既有表/列/行读取管线，仅替换序列化后端。

## 4. Parquet 导出（自实现最小写入器）

### 4.1 背景决策

离线环境无法 `go get` 第三方 Parquet 库（proxy.golang.org 不可达），因此实现 `pkg/export/parquet.go` 最小 Parquet v1 写入器。目标不是全量格式覆盖，而是**能被主流读取器直接消费的合规文件**。

### 4.2 格式范围（v1）

- 单 Row Group；每列一个 ColumnChunk。
- 编码：PLAIN（无字典、无压缩，`UNCOMPRESSED` codec）。
- 重复/定义层级：全部列 REQUIRED（导出列无 NULL），不写层级字节。
- 类型映射：

| OpenXDB 类型 | Parquet 物理类型 | 说明 |
|---|---|---|
| INT | INT64 | 8 字节小端 |
| DECIMAL | INT64 | 存储 scaled int64（scale=4 与存储一致） |
| TEXT / DATE / BLOB | BYTE_ARRAY | 4 字节长度 + UTF-8 字节 |

- Schema：扁平 `schema` group + 每列一个 leaf `SchemaElement`；`created_by=openxdb`，`version=1`。

### 4.3 文件布局

```
"PAR1"
  <ColumnChunk 0 DataPage>
  <ColumnChunk 1 DataPage>
  ...
  <FileMetaData (Thrift Compact Protocol)>
  <footer length: 4B little-endian>
"PAR1"
```

- DataPage v1：PageHeader（type=DATA_PAGE, uncompressed/compressed size）+ PLAIN values。
- FileMetaData：`num_rows`、`schema`、`row_groups[0]`（含各 ColumnChunk 的 `file_offset`、`ColumnMetaData`：type/encodings=[PLAIN]/codec=UNCOMPRESSED/path_in_schema/num_values/total_sizes/data_page_offset/num_rows）。

### 4.4 Thrift Compact Protocol 编码

仅实现 FileMetaData 路径所需子集：

- varint / zigzag 整数；
- struct 与 field header（field id delta、类型 nibble）；
- binary（长度 + 字节）；
- list 编码。

对每个写入字段手工构造（不引入通用 thrift 库），全链路由回读验证测试兜底。

### 4.5 正确性验证

无第三方读取器，采用双保险：

1. `pkg/export/parquet_test.go` 内置 **mini reader**：解析 footer（magic/长度/compact 结构）、RowGroup 布局与 DataPage PLAIN 值，逐列逐行还原并与源数据比对（roundtrip）。
2. 关键字节断言：文件以 `PAR1` 开头结尾、footer 长度与实际偏移一致、SchemaElement 顺序与类型正确。

## 5. 模块与测试

| 新增文件 | 职责 |
|---|---|
| `pkg/sql/export_json.go` | JSON Lines 序列化 |
| `pkg/export/parquet.go` | Parquet v1 最小写入器 |
| `pkg/export/parquet_test.go` | mini reader roundtrip 与字节断言 |
| `pkg/sql/m19_export_test.go` | FORMAT 语法、JSON/Parquet 导出端到端 |

- 全仓 `go test ./...` 回归全绿（含既有 544 测试）。
- 性能：JSON/Parquet 走流式写入（bufio），单表导出内存占用 O(列) 级别。

## 6. 文档与版本

- 本设计对应 README Status 更新为 M19 cluster，版本号按既有 alpha 语义递增。

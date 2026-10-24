# T44 v1.6.0 P1-2：MySQL wire 复制协议命令 + mysqldump 全选项补全

> 版本：v1.6.0-P1-2
> 日期：2026-10-24
> 范围：
> - `pkg/mysqlproto`（command.go 复制协议命令分发 + 新增 binlog_dump.go / binlog_dump_test.go）
> - `pkg/sql`（ast.go / parser.go / executor.go SHOW MASTER STATUS + 新增 p1_2_wire.go / p1_2_wire_test.go）
> - `pkg/import`（mysqldump.go CREATE DATABASE / USE / 库前缀表名 + mysqldump_test.go 追加用例）
> - `pkg/sql/mysqldump_import_test.go` 追加端到端用例
> - 文档：DEV_PLAN_2026Q5 §3.3 执行状态、README MySQL 兼容特性清单
> 闭环：`go build ./...` 无输出；`go test ./pkg/... -count=1` 全绿（基线 TestM6DownNodeReadFailover / TestP1DecimalAgg 排期外不修改）

## 背景

v1.6.0 第 2 项「MySQL wire：binlog / mysqldump 全选项」承接第 1 项 SQL 子集闭环，目标是生态兼容增强：
外部 MySQL 生态工具（复制/增量同步管道、mysqldump 迁移）可经标准 wire 协议入口对接 OpenXDB。
现有基础：
- `pkg/replication` binlog append-only 落盘 + `BinlogReader` 只读拉取；`pkg/cdc` Subscriber/Event/OffsetStore 已提供 LSN 位点订阅；
- `pkg/sql` 已有 `SHOW BINLOG EVENTS [FROM lsn] [LIMIT n]` 与 `IMPORT MYSQLDUMP FROM 'path' [BATCH n] [IGNORE ERRORS] [CONCURRENCY k]`；
- `pkg/mysqlproto/command.go` 命令面覆盖 comQuit/Ping/InitDB/Query/FieldList/StmtPrepare/Execute/Close/Reset/SendLongData/SetOption/ChangeUser，**缺复制协议命令**。

## 缺口分析

| # | 缺口 | 现状 | 影响 | 处置 |
|---|---|---|---|---|
| 1 | `COM_REGISTER_SLAVE (0x15)` | serveCommands 落入 default 返回 unknown command | MySQL 复制客户端在 dump 前必须登记，缺命令握手失败 | 实现解析 + OK 应答（无状态登记） |
| 2 | `COM_BINLOG_DUMP (0x12)` | 无 | 外部订阅无法从 wire 层消费 binlog | 实现按 position（LSN）流式推送事件 |
| 3 | `SHOW MASTER STATUS` | 无 | 复制工具取位点标准入口缺失（canal/mysqlbinlog 依赖） | 新增 SQL：File/Position/GTID 五列 |
| 4 | mysqldump `--databases` 模式 | 解析器遇 `CREATE DATABASE` 整体跳过、`USE db` 跳过、表名带 `db.` 前缀按整体标识符处理 | 整库 dump（多库/库前缀）迁移失败 | 支持 CREATE DATABASE/USE 识别跳过 + 表名库前缀剥离 |
| 5 | 导入侧选项 | BATCH / IGNORE ERRORS / CONCURRENCY 已全 | — | 无需改动（OpenXDB 单 default 库模型） |

范围决策：第 2 项以「wire 复制协议命令 + 位点入口 + dump 解析器数据库级语句」为闭环交付；
内部 binlog 事件经 MySQL binlog event 外壳流式推送（OpenXDB 专有事件体，不伪造 MySQL 行事件二进制语义）。

## 设计

### 1. 复制协议命令（pkg/mysqlproto/binlog_dump.go 新增）

#### COM_REGISTER_SLAVE (0x15)
- 负载：`server-id u32 | hostname_len u8 + hostname | user_len u8 + user | password_len u8 + password | port u16 | rank u32 | master-id u32`。
- 语义：解析并校验长度边界，登记到连接（仅记录 server-id，无全局注册表），返回 OK 包。
- 非法负载返回 ERR（malformed COM_REGISTER_SLAVE）。

#### COM_BINLOG_DUMP (0x12)
- 负载：`position u32 | flags u16 | server-id u32 | binlog-filename NUL 字符串`。
- position 映射为内部 LSN 起点：0 或文件名空 → 从头（LSN 0）；否则从该 LSN 起（含）。
- 响应：流式 binlog 事件包（无 OK 前缀），外壳按 MySQL binlog event header（19 字节：
  `timestamp u32 LE | type u8 | server_id u32 LE | event_size u32 LE | log_pos u32 LE | flags u16 LE`），
  log_pos 取该事件 LSN（位点推进，供订阅方续传）。事件体为 OpenXDB 专有格式（下表），
  明确不伪装 MySQL 行事件（列类型元数据体系差异过大，且 OpenXDB 仅 INT/TEXT/DATE/DECIMAL/BLOB 五型）。
- 读到文件尾后轮询等待新事件（每 200ms 重新扫描），连接断开（写失败）即退出。
- binlog 未启用（binlogPath 为空）返回 ERR `binlog not enabled`。

| type | 事件 | 事件体 |
|---|---|---|
| 99 | OPENXDB_FORMAT_EVENT | `magic "OPENXDB" | filename NUL 串 | start_lsn u64 LE | last_lsn u64 LE`（流格式声明，首包） |
| 100 | OPENXDB_ROWS_EVENT | `lsn u64 | commit_lsn u64 | schema_ver u64 | kind u8(1=PUT/2=DELETE) | table_id u64 | is_index u8 | table_name lenenc | pk lenenc | raw_value lenenc(PUT 行值 JSON；DELETE 空)` |
| 101 | OPENXDB_XID_EVENT | `commit_lsn u64`（每条 binlog 记录=一次提交，末尾发送） |

- 表名还原复用 SQL 层表目录（Engine.BinlogWireInfo 注入）；未知表 ID 输出 `#table-<id>`。

#### 位点入口：SHOW MASTER STATUS
- 语法 `SHOW MASTER STATUS`，输出五列 `File / Position / Binlog_Do_DB / Binlog_Ignore_DB / Executed_Gtid_Set`。
- File 恒 `openxdb-binlog`（单文件 append-only）；Position = binlog 最后 LSN + 1（下一写位点，与 COM_BINLOG_DUMP position 语义对齐）；后三列空串。
- binlog 未启用返回 `binlog not enabled`。

### 2. mysqldump 全选项补全（pkg/import/mysqldump.go）

| 语句 | 处理 |
|---|---|
| `CREATE DATABASE [IF NOT EXISTS] db [charset 等]` | 识别并记录到 `Dump.Databases []string`（OpenXDB 单 default 库模型，导入侧不建库） |
| `USE db;` | 识别并跳过（计入 Skipped 计数） |
| `CREATE TABLE db.tbl` / `INSERT INTO db.tbl` / `DROP TABLE db.tbl` | 表名库前缀剥离：仅保留最后一段（`db.tbl` → `tbl`） |

- 剥离规则只对 `a.b` 形式的带点标识符生效（内部表名本身不含点，零回归）。
- 导入执行器无改动：表名已剥离，Databases 仅记录供诊断。

### 3. SQL 层新增
- `ast.go`：`ShowMasterStatusStmt`。
- `parser.go`：SHOW 分支识别 `MASTER STATUS`（MASTER/STATUS 不在关键字表）。
- `executor.go`：分发 `*ShowMasterStatusStmt` → `execShowMasterStatus`（p1_2_wire.go）。
- `p1_2_wire.go`：执行器 + `binlogWireInfo()`（binlogPath + 表目录，供 mysqlproto 装配）。
- `sql.go`：`Engine.BinlogWireInfo() (binlogPath string, tables []TableMeta, err error)`。

## 测试

| 覆盖点 | 用例 |
|---|---|
| COM_REGISTER_SLAVE | 合法负载 OK；截断负载 ERR（mysqlproto） |
| COM_BINLOG_DUMP 全量 | 引擎装配 binlog → 写入多行 → dump from 0 收到 FORMAT + ROWS(每行 1) + XID；校验 LSN/PK/Kind/表名还原 |
| COM_BINLOG_DUMP FROM | 从中间 LSN dump 只收到后续事件；越过末尾仅 FORMAT 后等待 |
| COM_BINLOG_DUMP 轮询 | dump 到文件尾后写入新行 → 200ms 内收到新 ROWS 事件（超时上限 5s） |
| binlog 未启用 | COM_BINLOG_DUMP 返回 ERR binlog not enabled |
| SHOW MASTER STATUS | 位点=最后 LSN+1；空 binlog 位点 1；未启用报错（sql） |
| mysqldump 数据库级 | CREATE DATABASE 记录、USE 跳过、`db.tbl` 表名剥离（import 单元） |
| 端到端导入 | 带 CREATE DATABASE + USE + `db.tbl` 的整库 dump 导入成功、数据正确（sql） |

闭环验证：`go build ./...` 无输出；`go test ./pkg/... -count=1` 全绿（基线两项排期外不修改）。

## 边界与后续
- COM_BINLOG_DUMP 为轮询拉取外壳（OpenXDB 专有事件体），未实现 MySQL 完整 row event 列类型二进制编码；
  后续如需 canal/MySQL 原生 binlog 客户端等价对接，需扩展 TABLE_MAP/ROWS 事件与列元数据映射。
- 单文件 binlog 无轮转；超大 binlog 下 dump 起始为顺序扫描（同 SHOW BINLOG EVENTS 边界）。
- SHOW MASTER STATUS Position 语义为「下一写位点」（最后 LSN+1），与 MySQL「文件头 + 已写偏移」的
  position 表示不同，订阅方应以本设计文档定义的 LSN 语义解析。

# T30 配置文件与结构化日志告警体系

> 版本：v0.20.0-alpha
> 日期：2026-10-13
> 分支：main（本地开发分支 feat/config-log-alert）
> 范围：
> - `pkg/configfile`（新增）：统一配置文件（INI 风格）解析与校验：`File/Section`、`Parse/Load/Validate/ValidateServerValues`、`configfile_test.go`
> - `pkg/logx`（新增）：结构化日志器与告警管理器：`Logger`（JSON 行：time/level/module/event/msg + 动态 kv，级别过滤）、`AlertManager`（内存环形缓冲 + JSON 行文件落盘 + 异步 HTTP webhook，配置 `Enabled/File/Webhook/Keep`）、`logx_test.go` / `alerts_test.go` / `webhook_test.go`
> - `pkg/sql`：`config.go` 注册 `log_level`、`alert_enabled`、`alert_file`、`alert_webhook`、`alert_keep` 五项配置键（SET 即时生效）；`ast.go` / `parser.go` / `executor.go` / `alert.go`（新增）支持 `SHOW ALERTS`、`SHOW ALERT STATUS`；`backup_schedule.go` 备份失败埋点；`ConfigKeys` 导出；`logalert_test.go`
> - `pkg/db/db.go`：存储/WAL/txn 打开与恢复失败告警埋点
> - `pkg/server/server.go`：监听失败与 Accept 失败告警埋点
> - `cmd/openxdb/main.go`：`--config` 加载统一配置文件、命令行参数覆盖文件、`config-check` 子命令、`s.Logf` 切换为结构化日志
> - `docs/openxdb.conf.example`（新增示例配置）、本文档
> 闭环：CGO 环境下 `go build ./...` + `go test ./...` 全绿（含新增 logx/configfile/sql 告警用例）；冒烟：`config-check` 校验通过/拒绝、`start` 结构化日志与告警文件生效
> 提交：`feat(config,log): unified config file & structured log alerting`（本地，未 push）

## 背景

此前 server 启动参数分散在命令行 flag，配置项由 `configSpecs` 注册表经 `SET PERSIST` 热更新与落盘（M16），但缺少"一份配置文件统一管理启动参数与配置项"的入口；日志为 `log.Printf` 非结构化文本，且关键模块（WAL/存储/网络/备份）异常没有可配置的告警出口。本任务补齐两件事：统一配置文件 + 结构化日志告警体系。

## 一、统一配置文件

### 选型

沿用仓库现有 `key = value` + `#` 注释的 INI 风格（与 `db.Init` 生成的 `openxdb.conf` 一致），并支持分段：

```
# 示例：docs/openxdb.conf.example
[server]
port = 7788
data_dir = ./data
log_level = info
alert_enabled = off

[sql]
query_cache = off
slow_query_threshold = 100
backup_interval = 0
```

- 段/键大小写不敏感；值可用单/双引号包裹；重复键后者覆盖；`;` 亦视为注释。
- **未知段 / 未知键 / 非法值一律拒绝**（`config-check` 与启动时校验），不静默忽略。

### 加载与覆盖优先级

`cmd/openxdb/main.go` 新增 `--config <path>`（默认 `openxdb.conf`，不存在则跳过）。加载顺序：

1. `[server]` 段：提供 `data_dir`、`port`、`replica_port`、`metrics_port`、`shutdown_timeout`、`cluster_addr`、`log_level`、`alert_*` 的**默认值**；
2. 命令行 flag（`--port`、`--backup-*`、`--tls-*`、`--metrics-port` 等）**覆盖**文件值；
3. `[sql]` 段：与 `SHOW CONFIG` 键白名单一致的 13 个配置键，经 `SET` 装配到 SQL 引擎（引擎启动后立即生效，含 `log_level` / `alert_*`）。

### config-check 子命令

```
openxdb config-check --config docs/openxdb.conf.example
```

- 校验：文件可解析、无未知段/未知键、`[server]` 值格式合法（端口 1..65535，`replica_port/metrics_port` 允许 0=关闭，秒数/保留数非负整数，布尔 on/off/true/false/1/0，`http(s)://` URL，日志级别 debug/info/warn/error）。
- 通过输出 `config OK` 并回显各段；失败输出具体错误行并以退出码 1 结束。

### [sql] 段键白名单（来自 `pkg/sql.ConfigKeys`，与 SHOW CONFIG 完全一致）

`query_cache` / `slow_query_threshold` / `slow_query_log` / `shutdown_timeout` / `backup_interval` / `backup_keep` / `backup_dir` / `backup_remote_url` / `log_level` / `alert_enabled` / `alert_file` / `alert_webhook` / `alert_keep`（13 项）。

## 二、结构化日志（pkg/logx）

- `Logger`：每行一个 JSON 对象，固定字段 `time`（RFC3339 毫秒）/ `level` / `module` / `event` / `msg`，`msg` 后可追加任意动态 kv。
- 级别：`debug < info < warn < error`；低于当前级别不输出。全局实例 `logx.Default`（stderr），`server.go` 的 `s.Logf` 已切换为 `logx.Default.Info(module=server)`。
- **动态调整**：`SET log_level = 'warn'` 立即生效（内存级），或配置文件 `[server] log_level` 启动时装配；非法级别拒绝。

## 三、告警体系（pkg/logx.AlertManager）

- 告警 = 结构性事件（module/event/msg + 时间戳），写入 Error 级结构化日志。
- 出口（可配置）：
  - **内存环形缓冲**：保留最近 `keep` 条，供 `SHOW ALERTS` 查询；
  - **文件**：`alert_file`，JSON 行追加；
  - **HTTP webhook**：`alert_webhook`，异步 POST 告警 JSON（沿用标准库 `net/http`，零第三方依赖）。
- 开关与配置：`alert_enabled`（on/off）、`alert_keep`（保留条数）；`SET` 即时生效。

### 告警埋点清单（真实源码位置）

| 模块 | event | 位置 |
|---|---|---|
| storage | `storage_open_failed` | `pkg/db/db.go` Open：RocksDB 打开失败 |
| wal | `wal_open_failed` | `pkg/db/db.go` Open：WAL 打开失败 |
| wal | `wal_recover_failed` | `pkg/db/db.go` Open：WAL 恢复失败 |
| txn | `txn_init_failed` | `pkg/db/db.go` Open：事务层初始化失败 |
| network | `listen_failed` | `pkg/server/server.go` ServeTCP 监听失败 |
| network | `accept_failed` | `pkg/server/server.go` Accept 循环失败 |
| backup | `backup_failed` | `pkg/sql/backup_schedule.go` execBackup 失败 |
| backup | `upload_failed` | `pkg/sql/backup_schedule.go` 远程上传失败 |
| backup | `backup_dir_not_configured` / `backup_dir_create_failed` | `pkg/sql/backup_schedule.go` 目录检查/创建失败 |

### 查看告警状态与开关

| 语句 | 行为 |
|---|---|
| `SHOW ALERTS` | 最近告警列表（time / level / module / event / msg） |
| `SHOW ALERT STATUS` | 当前状态（variable / value：level、enabled、file、webhook、keep） |
| `SET log_level = ...` / `SET alert_enabled = ...` / `SET alert_file = ...` / `SET alert_webhook = ...` / `SET alert_keep = ...` | 动态开关与调整 |

## 四、验证

- `go test ./pkg/logx/...`：JSON 行字段/级别过滤/级别解析、告警环形缓冲/落盘/开关/webhook 推送；
- `go test ./pkg/configfile/...`：解析（段/注释/引号/大小写/重复键）、格式错误拒绝、未知段/键拒绝、server 值校验；
- `go test ./pkg/sql/ -run 'TestLogAlert|TestShowAlerts'`：SHOW ALERT STATUS 默认值与动态调整、非法值拒绝、SHOW ALERTS 反映已触发告警；
- 全量 `go build ./...` + `go test ./...` 通过（CGO/RocksDB 环境）。

## 五、新增/修改文件清单

| 文件 | 类型 |
|---|---|
| `pkg/configfile/configfile.go` + `configfile_test.go` | 新增 |
| `pkg/logx/logx.go` / `alerts.go` + 三个测试文件 | 新增 |
| `pkg/sql/alert.go` + `logalert_test.go` | 新增 |
| `pkg/sql/config.go` / `ast.go` / `parser.go` / `executor.go` / `backup_schedule.go` | 修改 |
| `pkg/db/db.go` | 修改 |
| `pkg/server/server.go` | 修改 |
| `cmd/openxdb/main.go` | 修改 |
| `docs/openxdb.conf.example` | 新增示例 |
| `docs/T30_config_log_alert.md` | 本文档 |

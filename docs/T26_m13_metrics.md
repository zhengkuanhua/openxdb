---
# T26 M13 功能簇（2/3）：metrics 导出（Prometheus 文本格式）

> 版本：v5.0-P13-M13, v0.14.0-alpha
> 日期：2026-10-11
> 范围：`pkg/metrics`（新增 registry 包 metrics.go / metrics_test.go）、`pkg/server`（server.go METRICS 命令 + server_metrics_test.go）、`pkg/sql`（executor.go / sql.go 计数接入 + metrics_internal_test.go）、`cmd/openxdb`（main.go /metrics HTTP 端点 + metrics.go CLI 拉取）
> 闭环：`go test ./pkg/metrics/ ./pkg/sql/ ./pkg/server/ -count=1` 全绿，不 push（README 勾选由 M13 整簇收尾提交统一更新）
> 提交：`M13: metrics export`（本地，未 push）

## 背景

M13 可观测性簇在 T20 ops stats（`SHOW STATS`：连接数/表数/region 数/慢查询计数/binlog 位点等）与 M13 子功能一慢查询日志（ed955f8，`execTimed` 统一计时 + `SlowQueryRecord` + `recordSlow`）基础上，新增**标准 Prometheus 文本格式导出**：既面向运维拉取（`/metrics` HTTP），也面向人工排查（`METRICS` 命令），为后续监控状态查询（子功能三）提供统一数据底座。

## 设计

### 指标清单（Prometheus 文本格式，`# HELP` / `# TYPE` / 指标名+标签）

| 指标 | 类型 | 含义 |
|---|---|---|
| `openxdb_queries_total{type="sql"\|"kv"}` | counter | SQL / KV 累计执行数 |
| `openxdb_qps{type="sql"\|"kv"}` | gauge | 当前 1 秒窗口 QPS |
| `openxdb_connections_active` | gauge | 当前活跃 TCP 连接数（注入 `Engine.SetStatsSources` 同源） |
| `openxdb_slow_query_total` | counter | 慢查询累计数（复用 `recordSlow` 命中） |
| `openxdb_slow_query_threshold_ms` | gauge | 慢查询阈值（注入慢查询配置源，默认 100） |
| `openxdb_storage_bytes` | gauge | 数据目录落盘总字节（`dirSize` 5 秒缓存） |
| `openxdb_region_count` | gauge | 路由表 region 总数 |
| `openxdb_region_state{state="..."}` | gauge | 按 region 状态（ACTIVE/LEAVING 等）分布 |
| `openxdb_region_local` / `openxdb_region_remote` | gauge | 本地 / 远端 region 数 |
| `openxdb_txn_commit_total` / `openxdb_txn_rollback_total` | counter | 事务成功提交 / 回滚累计数 |
| `openxdb_query_local_total` / `openxdb_query_remote_total` | counter | 数据访问本地直读 / 远端转发累计数 |

### 注册表（`pkg/metrics` 新增）

- **线程安全与低开销**：计数器用 `atomic.Int64`；QPS 用环形秒窗数组（`[8]atomic.Int64`，写入取 `now%8` 槽位，渲染时取最近 4 秒窗口，无锁）；渲染拼装一次性快照，无锁遍历。
- **注入源**：连接数、慢查询阈值、存储字节、region 分布通过 `SetConnSource / SetSlowThresholdSource / SetStorageBytesSource / SetRegionStatsSource` 注入回调（T20 `SetStatsSources` 同风格），metrics 包零依赖 server/sql 实现；未注入源渲染为 0。
- **标签转义**：`labelValue` 按 Prometheus 规范转义 `\` 与 `"`（region 状态含特殊字符安全）。
- **HTTP handler**：`Handler()` 返回 `http.Handler`，`Content-Type: text/plain; version=0.0.4`，供内置端点复用。

### 接入点（计数挂载）

| 挂载点 | 计数 |
|---|---|
| `executor.execTimed` | SQL 累计 +1、QPS 秒窗 +1（`ExecuteTextAs → execTimed` 主路径） |
| `server.go` KV 分支 GET/SET/DEL | KV 累计 +1、QPS 秒窗 +1（`s.incKV()`） |
| `commitTx` / `rollbackTx` | 成功提交 / 回滚 +1（失败不计数） |
| `recordSlow` | 慢查询累计 +1（`m` 非 nil 时计数，零开销） |
| `getKVCluster` / `scanMergedCluster` | 本地直读 `incLocal`，远端转发 `incRemote` |

nil 安全：所有 `incXxx` / `recordSlow` 对 `m == nil` 直接跳过（未装配指标不影响原有行为）。

### 暴露入口

- **HTTP**：`openxdb server --metrics-port <n>`（默认 7789，与主端口 7788 不冲突；`0` 禁用），`http://<addr>:<port>/metrics` 返回 Prometheus 文本。
- **CLI**：`openxdb metrics [--addr host:port]` 对本地/远端（默认 127.0.0.1:7788）发起 `METRICS` 命令并打印文本指标，等价 HTTP 拉取内容。
- **命令**：`METRICS` 返回注册表渲染文本；未装配返回 `ERR metrics not enabled`（cmdStart 装配：`s.SetMetrics(m)` + `d.SQL.SetMetrics(m)` 共享同一注册表）。

## 测试

### pkg/metrics/metrics_test.go

| 测试 | 覆盖 |
|---|---|
| `TestM13MetricsRenderFormat` | 空注册表渲染含 `# HELP`/`# TYPE`/零值指标行，符合 Prometheus 文本结构 |
| `TestM13MetricsCounters` | SQL/KV 累计计数与 `{type=}` 标签 |
| `TestM13MetricsQPSWindow` | 秒窗 QPS 聚合：4 秒窗口内计数、窗口外过期不参与 |
| `TestM13MetricsInjectedSources` | 注入连接/阈值/存储/region 源后渲染对应数值；未注入渲染 0 |
| `TestM13MetricsLabelEscape` | region 状态标签含引号/反斜杠按规范转义 |
| `TestM13MetricsConcurrentIncrement` | 20 goroutine 并发自增，最终计数精确无丢失 |
| `TestM13MetricsHandler` | HTTP handler 状态 200、`Content-Type: version=0.0.4`、正文含指标 |

### pkg/sql/metrics_internal_test.go

| 测试 | 覆盖 |
|---|---|
| `TestM13MetricsSQLAndSlow` | `execTimed` 每执行计 SQL；`recordSlow` 命中计慢查询 |
| `TestM13MetricsTxnCommitRollback` | 成功提交/回滚计数；失败不计数 |
| `TestM13MetricsRegionStats` | 路由表聚合 region 总数/状态分布/本地与远端 |
| `TestM13MetricsNilSafety` | 未注入指标时全部计数路径 nil 安全不 panic |

### pkg/server/server_metrics_test.go

| 测试 | 覆盖 |
|---|---|
| `TestM13MetricsCommandNotEnabled` | 未装配注册表 `METRICS` 报 `ERR metrics not enabled` |
| `TestM13MetricsCommandRender` | 真实命令（KV×4、SQL×7、事务成功×2）驱动计数，`METRICS` 输出各项指标正确 |
| `TestM13MetricsHTTPHandler` | `/metrics` HTTP 端点返回 Prometheus 文本 |

## README 勾选

由 M13 整簇收尾提交统一执行：
- Status 行更新为 M13 可观测性簇；
- T 表格追加 `T26 M13 observability cluster: slow query log + metrics export + monitoring status query`；
- 文档列表追加 `T26_m13_metrics.md`（随本提交加入）。

*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

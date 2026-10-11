# T26 M13 功能簇（3/3）：监控状态查询（aggregated health/status view）

> 版本：v5.0-P13-M13, v0.14.0-alpha
> 日期：2026-10-11
> 范围：`pkg/server`（status.go / server.go STATUS 命令 + 测试 `server_status_test.go`）、`pkg/metrics`（metrics.go Snapshot 快照 + 测试 `metrics_test.go` 增补）、`pkg/sql`（sql.go ClusterNodes/ClusterSelfNode 转发）、`cmd/openxdb`（status.go CLI + main.go 注册与装配）
> 闭环：`go test ./pkg/server/ ./pkg/metrics/ -count=1` 全绿，不 push；README 勾选由 M13 整簇收尾提交统一更新
> 提交：`M13: status view`（本地，未 push）

## 背景

M13 可观测性簇至此收尾：子功能一慢查询日志（ed955f8，`execTimed` 统一计时 + `SlowQueryRecord` + `recordSlow`）、子功能二 metrics 导出（1ac9e92，`pkg/metrics` Registry + METRICS 命令 + `/metrics` HTTP + `openxdb metrics` CLI）已完成。运维侧已有 T20 doctor 健康检查（`pkg/server/doctor`）与 `SHOW STATS` 运行指标，但缺少一个**统一入口**：把节点/region/存储/慢查询/事务/连接/metrics 要点聚合成一张健康/状态快照，同时服务端命令与 CLI 双入口可查，供人工排障与脚本拉取。

本子功能在复用 M13 子功能二注册表（`metrics.Snapshot` 与 `Render` 同源）与子功能一慢查询记录（`SlowQueries`）基础上，新增 **STATUS 聚合快照**：只读、不阻塞、单项降级不整体失败。

## 设计

### 聚合快照模型（`statusReport`）

| 字段 | 内容 | 来源 |
|---|---|---|
| `server` / `status` / `time` | 服务名 / 整体状态（`ok` \| `degraded`）/ 快照时间 | 固定 + `time.Now()` |
| `nodes` | 节点存活/角色（ID、Addr、Role、State） | `SQL.ClusterNodes()`；空注册表回退 `ClusterSelfNode()` 单机兜底 |
| `regions` | region 分布：总数 / 各状态 / 本地 / 远端 | `SQL.MetricsRegionStats()`（与 SHOW STATS、metrics 同口径）；SQL 不可用回退 metrics 注入源 |
| `storage_bytes` | 存储总数据量字节 | metrics 注册表 `StorageBytes`（`dirSize` 5 秒缓存源） |
| `slow_queries` | 慢查询摘要：`enabled` / `threshold_ms` / `recent_count` / 最近 ≤5 条明细 | `SQL.SlowQueryThreshold()` + `SQL.SlowQueries()`；累计数回退 `SlowQueryTotal` |
| `txn_commit_total` / `txn_rollback_total` | 事务提交/回滚累计 | metrics 注册表计数器 |
| `active_connections` | 活跃 TCP 连接数 | metrics 注入源 `ActiveConns` |
| `qps` | 当前秒窗 SQL/KV QPS | metrics 注册表 `SQLQPS` / `KVQPS` |
| `warnings` | 降级警告列表 | 各数据源采集失败/未装配时追加 |

### 双入口

- **服务端命令**：`STATUS` 输出人类可读表格（`key | value` 行式 + `nodes:`/`regions:`/`slow_queries:` 分节）；`STATUS JSON` 输出 `json.MarshalIndent` 结构化快照（字段与上表 json tag 对齐）。`handleLocked` 已加 `STATUS` 分支，HELP 文本同步追加。
- **CLI**：`openxdb status [--addr <host:port>] [--json]`，默认 `127.0.0.1:7788`，向服务端发起 `STATUS` / `STATUS JSON` 并打印；连接失败或服务端拒绝（`ERR` 前缀）退出码 2，成功退出码 0。`main.go` 已注册子命令并补充 usage。

### 只读与降级语义

- **只读不阻塞**：全部数据源为内存原子读 / 轻量注入源调用（`ClusterNodes`、`MetricsRegionStats`、`SlowQueries` 均为内存副本），不发起网络请求、不加锁等待，不阻塞正常请求。
- **单项降级不整体失败**：任一数据源未装配或采集失败，仅记入 `Warnings` 并将整体 `status` 置为 `degraded`，其余数据照常输出；SQL 引擎整体缺失时节点/慢查询省略、region 回退 metrics 源；metrics 注册表缺失时计数归零并警告。`STATUS JSON` 即使降级也始终可解析。

### 装配（cmdStart，与 METRICS 共享注册表）

`metrics.New()` 注入连接数/慢查询阈值/存储大小/region 分布采集源 → `s.SetMetrics(m)` + `d.SQL.SetMetrics(m)` → 可选 `--metrics-port` 启动 `/metrics` HTTP。STATUS 与 METRICS、HTTP 三个入口共用同一注册表，口径一致。

## 测试

### pkg/server/server_status_test.go（协议层）
| 测试 | 覆盖 |
|---|---|
| `TestM13StatusCommandText` | STATUS 文本快照：ok 状态、连接数、存储、事务、regions/slow_queries 分节、单机回退节点行（DATA/UP） |
| `TestM13StatusCommandJSON` | STATUS JSON 可解析且字段正确：server/status/connections/storage/nodes/regions（regions 与 SQL 源口径逐字段一致） |
| `TestM13StatusCommandDegraded` | 未装配 metrics 时 STATUS 不整体失败：`degraded` 状态 + metrics registry 警告 + 计数归零，JSON 仍可解析 |
| `TestM13StatusSlowSummary` | 慢查询摘要：threshold 启用、最近条数与明细（SELECT 原文 + 耗时）出现在文本快照 |

### pkg/metrics/metrics_test.go（快照层增补）
| 测试 | 覆盖 |
|---|---|
| `TestM13MetricsSnapshot` | Snapshot 与 Render 同源：计数器直读 + 注入源实时值（连接/阈值/存储/region 分布） |
| `TestM13MetricsSnapshotNilSources` | 未装配注入源输出 0（STATUS 降级路径），计数器仍正常读取 |

闭环命令：`go test ./pkg/server/ ./pkg/metrics/ -count=1` 全绿；`go build ./...` 通过（CGO：PATH 前置 mingw64/bin，CGO_LDFLAGS 仅 `-lrocksdb -lshlwapi`，严禁 `-lstdc++`）。

## 取舍说明

- **SQL 优先、metrics 兜底**：region 分布优先走 SQL 路由表（与 SHOW STATS 口径一致），仅 SQL 不可用时回退 metrics 注入源，保证数字来源单一可信；
- **普通表单 region 隐式不落盘**：`MetricsRegionStats` 仅统计显式路由表 region，单机普通表 Total 可为 0，测试断言与 SQL 源逐字段一致而非臆断最小数；
- **慢查询明细限 5 条**：快照保持轻量，完整记录仍走 `SHOW SLOWQUERIES` / slow.log，避免 STATUS 输出膨胀；
- **降级置 degraded 而非报错**：排障场景"有数据看"优于"全失败"，Warnings 保留失败原因供脚本与人工判断；
- **CLI 默认 127.0.0.1:7788**：与 `openxdb stats` / `openxdb metrics` 默认地址一致，`--addr` 支持远端拉取。

## README 勾选
由 M13 整簇收尾提交统一执行：
- Status 行更新为 M13 可观测性簇（慢查询日志 + metrics 导出 + 监控状态查询）；
- T 表格追加 `T26 M13 observability cluster: slow query log + metrics export + status view`；
- 文档列表追加 `T26_m13_status.md`（随本提交加入）。

*（内容由AI生成，仅供参考）*

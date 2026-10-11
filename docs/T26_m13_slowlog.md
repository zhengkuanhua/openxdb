# T26 M13 功能簇（1/3）：慢查询日志

> 版本：v5.0-P15-M13, v0.16.0-alpha
> 日期：2026-10-11
> 范围：`pkg/sql`（slowlog.go / executor.go / sql.go / ast.go + 测试 `slowlog_internal_test.go` / `sql_test.go`）、`pkg/server`（server.go + 测试 `server_slowlog_test.go`）、`cmd/openxdb`（main.go 启动装配）
> 闭环：`go test ./pkg/server/ ./pkg/sql/ -count=1` 全绿，不 push（README 勾选由 M13 整簇收尾提交统一更新）
> 提交：`M13: slow query log`（本地，未 push）

## 背景
M13 可观测性簇子功能一：慢查询日志。此前 P2 已埋下慢查询基础设施（`execTimed` / `SlowQueryRecord` / `SHOW SLOWQUERIES`），但存在两处致命脱节：`Execute` 分发未接入计时包装（记录永远为空）、未透传原始 SQL 文本（记录无语句）。本子功能补齐 M13 完整规格：engine/server 层记录执行耗时超过阈值的 SQL（含用户、开始时间、耗时、SQL 文本、结果行数/错误信息），阈值可配置（默认 100ms，支持运行时 `SET` 与配置项），日志可落盘（默认 `data/slow.log` 可配置）并在内存保留最近 N 条（`SHOW SLOWQUERIES` 查询），写入线程安全、低开销（阈值判断先于格式化），配置变更即时生效。

## 设计

### 执行链路（统一计时挂回主路径）

- `Engine.Execute(stmt)` → `ExecuteTextAs(stmt, sqlText, user)`：解析文本在 Engine 层保留，透传原始 SQL 供记录展示；
- `ExecuteTextAs` 统一走 `execTimed(user, sqlText, fn)` 计时包装，`fn` 内为原 `execStmt` 语句分发（SELECT/DML/DDL/运维语句全部覆盖，含查询缓存读写与失效逻辑）；
- `execTimed`：先取开始时间戳，执行结束取结束时间戳，**仅当 `slowThreshold > 0 && ms >= slowThreshold` 才构造 `SlowQueryRecord` 并记录**——正常语句零格式化开销（只多两次时间读取）；
- 执行失败同样记录：`Err` 字段承载错误信息，`Rows=0`；DML 记录影响行数（`AffectedRows`），SELECT 记录返回行数。

### 记录结构（`SlowQueryRecord`）

| 字段 | 含义 |
|---|---|
| `SQL` | 原始 SQL 文本（Engine 透传，多行转义为 `\n`/`\r`） |
| `User` | 执行用户（root / AUTH 会话用户；guest 为空串） |
| `StartAt` | 开始时间 `YYYY-MM-DD HH:MM:SS` |
| `Duration` | 耗时（毫秒） |
| `Rows` | 结果行数（SELECT 返回行数 / DML 影响行数；失败为 0） |
| `Err` | 错误信息（成功为空串；慢且失败同样记录） |

### 内存保留（最近 N 条，可查询）

- `slowQueries` 环形裁剪：追加前若 `len >= slowQueryMax`（默认 64）丢弃最旧一条；`SetSlowQueryMax(n)` 运行时调整并即时裁剪；
- `SlowQueries()` 加锁返回副本，供 `SHOW SLOWQUERIES` 与协议层安全并发读取；
- 查询入口 `SHOW SLOWQUERIES`（新→旧），列：`time | user | duration_ms | rows | error | sql`。

### 落盘（线程安全、低开销）

- `SlowLog`：互斥锁串行化追加写，`Configure(path, enabled)` 即时生效（空路径或 `enabled=false` 关闭）；
- 落盘仅慢查询触发后执行（低频），以追加模式打开文件写一行后关闭（避免长句柄）；写入失败静默忽略，不阻塞 SQL 执行；
- 默认装配：`cmd openxdb start` 将落盘路径设为 `<data目录>/slow.log` 并开启（`SetSlowLog(slowLogPath, true)`），启动打印路径与阈值；
- 内存记录与落盘解耦：`SET slow_query_log = off` 停止落盘，内存记录仍可查。

### 配置（即时生效）

| 配置 | 方式 |
|---|---|
| 阈值（毫秒，<=0 关闭记录） | `SET slow_query_threshold = <ms>`；`Engine.SetSlowThreshold(ms)`；默认 100ms |
| 落盘开关 | `SET slow_query_log = on/off`；`Engine.SetSlowLog(path, enabled)` |
| 落盘路径 | `Engine.SetSlowLog(path, true)`（启动默认 `data/slow.log`） |
| 内存保留条数 | `Engine.SetSlowQueryMax(n)`（默认 64） |

### server 协议兼容（SET 路由）

`SET` 行在 server 层优先路由 SQL 引擎：`isSQLSetAssign` 判定 `SET <name> = <value>` / `SET <name>=<value>` 形式走 `doSQL`（即 SQL 变量赋值），其余 `SET <key> <value...>` 保持 KV 语义（向后兼容既有驱动）。

## 测试

### pkg/sql/slowlog_internal_test.go（白盒）
| 测试 | 覆盖 |
|---|---|
| `TestM13SlowThresholdDefault` | 默认阈值 100ms、内存上限 64 |
| `TestM13ExecTimedErr` | 慢且失败语句同样记录：Err 字段 + 落盘 `err=` |
| `TestM13SlowQueryMax` | 内存保留条数上限即时生效（环形裁剪） |
| `TestM13SlowLogFormat` | 落盘行格式化：单行、换行转义、字段齐全 |
| `TestM13SlowLogConfigure` | 默认关闭；空路径不开启；开启后追加、关闭后不再追加 |
| `TestM13ExecTimedLowOverhead` | 未超阈值不产生记录（阈值判断先于格式化） |

### pkg/sql/sql_test.go（黑盒 / 行协议语义）
| 测试 | 覆盖 |
|---|---|
| `TestM13SlowQueryPublic` | `SET slow_query_threshold` 即时生效、按用户执行记录 user/rows、落盘内容、阈值置 0 停止记录 |
| `TestM13SlowQuerySetError` | SET 非法阈值/非法 log 值报错且不改配置 |
| `TestP2SlowQueries`（扩展） | SHOW SLOWQUERIES 列由 3 列扩展为 6 列（time/user/duration_ms/rows/error/sql） |

### pkg/server/server_slowlog_test.go（行协议）
| 测试 | 覆盖 |
|---|---|
| `TestM13ServerSlowQueryView` | 经 server 行协议：SET 阈值即时生效、SHOW SLOWQUERIES 查看内存记录、slow.log 落盘内容 |
| `TestM13ServerSlowQueryDisable` | `SET slow_query_log = off` 停止落盘（内存仍可查） |

## 取舍说明

- **统一计时 wrapper（踩坑规避）**：P2 教训是计时方法与分发入口脱节（`execTimed` 未接入 `Execute`）导致记录为空、`Execute` 未透传原文导致无语句。本实现将 `execStmt` 提取为纯分发，`ExecuteTextAs` 统一包 `execTimed` 并保留原文，SELECT 等所有语句真正进入计时；
- **阈值判断先于格式化**：`execTimed` 只在 `ms >= threshold` 后构造记录与落盘行，正常语句路径零格式化/零文件操作，热路径仅增加两次时间读取；
- **落盘低频重复打开**：慢查询天然低频，每次追加打开/关闭文件避免长句柄与跨语句文件状态，代价（系统调用）可忽略；写入失败静默忽略，慢查询记录不阻塞 SQL 执行；
- **内存与落盘解耦**：`slow_query_log = off` 只停落盘，内存最近 N 条仍可 `SHOW SLOWQUERIES` 查询，符合运维可观测预期；
- **SET 语义兼容**：server 层按 token 形态区分 SQL 变量赋值与 KV `SET`，不破坏 T19 驱动既有 `SET <key> <value>` 用法；
- **锁粒度**：内存记录锁（`e.mu`）与落盘锁（`SlowLog.mu`）分离，落盘在内存锁外执行，慢查询低频下互不阻塞热路径。

*（内容由AI生成，仅供参考）*

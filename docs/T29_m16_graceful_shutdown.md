---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_832d10fec60711f19063525400393706
    ReservedCode1: wJ4kwp7sogdo0Bp0/2cOyDYmIWtJ88P7WbHPt5UrfDL9rKQGJ5/NQt0eREvw/+m2jodalXo/iJ1qNu3uU98/VwDKPCqKKfYIX7iSTIjDOn/Nj4WUkc5iU19D7BgA3j9NBKcsqMyc19XuKwwUH2N/QcJSqV3Muv+M//ClIAD7KGbBWXIqJ7E/K3fCIPo=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_832d10fec60711f19063525400393706
    ReservedCode2: wJ4kwp7sogdo0Bp0/2cOyDYmIWtJ88P7WbHPt5UrfDL9rKQGJ5/NQt0eREvw/+m2jodalXo/iJ1qNu3uU98/VwDKPCqKKfYIX7iSTIjDOn/Nj4WUkc5iU19D7BgA3j9NBKcsqMyc19XuKwwUH2N/QcJSqV3Muv+M//ClIAD7KGbBWXIqJ7E/K3fCIPo=
---





# T29 M16 运维增强簇（2/3）：优雅停机

> 版本：v5.0-P16-M16, v0.20.0-alpha
> 日期：2026-10-12
> 范围：`pkg/server/server.go`（停机状态机、SHUTDOWN 命令、GracefulShutdown/WaitShutdown/ForceExited、监听器关闭、drain 逻辑）、`cmd/openxdb/main.go`（停机钩子装配、`--shutdown-timeout` 参数、SIGINT/SIGTERM 信号处理）、`pkg/sql`（executor.go 停机超时字段与方法、config.go 新增 `shutdown_timeout` 配置项、sql.go 透传 ShutdownTimeout）、新增 `pkg/server/server_shutdown_test.go`、`cmd/openxdb/main_shutdown_test.go`、`docs/T29_m16_graceful_shutdown.md`（本文档）。
> 闭环：CGO 环境下 `go build ./...` + `go test ./... -count=1` 全绿（含 8 个 M16-2 停机用例），未 push
> 提交：`M16: graceful shutdown`（本地，未 push）

## 背景
M16 运维增强簇按 Roadmap 依次落地三个子功能：在线配置热更新（1/3，已完成，提交 60cd6ce）、优雅停机（2/3，本次）、灰度升级辅助（3/3）。优雅停机在 60cd6ce 之上增量开发，目标：数据库停止服务时拒绝新连接/新事务，等待进行中事务在超时窗口内完成（drain），随后 flush/关闭存储（WAL、RocksDB、持久化配置）并正常退出（退出码 0）；drain 超时则强制退出（退出码非 0）并明确告警，但存储仍完成关闭，保证重启后数据完整可重新打开。SIGINT/SIGTERM 与 SQL 命令 `SHUTDOWN [GRACEFUL]` 走同一停机流程。

## 设计

### 触发途径
| 途径 | 行为 | 退出码 |
|---|---|---|
| `SHUTDOWN [GRACEFUL]` 命令（SQL 会话/CLI/TCP） | 进入优雅停机流程，返回 `OK: graceful shutdown started` | 0（正常）/ 1（超时强制） |
| `SIGINT`（Ctrl-C）/ `SIGTERM` | `cmd/openxdb` 捕获信号并触发同一流程 | 0（正常）/ 1（超时强制） |
| `--shutdown-timeout <sec>` 启动参数 | 覆盖停机超时（优先级高于配置项） | — |
| `SET [PERSIST] shutdown_timeout = <sec>` | 热更新停机超时（M16-1 配置体系扩展） | — |

### 停机流程（状态机）
```
state 0 (运行中)
  │  触发 SHUTDOWN / 信号
  ▼
state 1 (draining)
  ├─ 关闭监听器 → 拒绝新 TCP 连接（已建连会话在命令入口拒绝新命令/新事务）
  ├─ 放行：COMMIT / ROLLBACK / QUIT / PING / HELP / SHUTDOWN（事务终态收尾）
  ├─ drain 条件：无正在执行命令（active==0 且命令锁空闲）且无未收尾显式事务（cur==nil）
  ├─ 超时未排空 → forced=1，记录 TIMEOUT 告警日志 → state 2
  ▼
state 2 (closed)
  ├─ 调用关闭钩子（db.Close：按序停止 txn/WAL/storage，flush RocksDB 与持久化配置）
  ├─ 关闭完成 → 关闭 shutdownDone 通道
  └─ 返回：forced=0 → nil（退出码 0）；forced=1 → ErrShutdownTimeout（退出码 1）
```

### 超时语义
- 超时来源优先级：命令行 `--shutdown-timeout` > SQL 引擎 `shutdown_timeout`（内存 `SET` > 持久化 `SET PERSIST` > 默认 30s）；
- drain 从监听器关闭时刻开始计时，以 20ms 周期轮询；
- 超时强制路径**不跳过存储关闭**：数据完整可 reopen，仅退出码为 1 并记录告警；
- 未提交事务在强制路径回滚（由事务层保证），已提交数据不丢失。

### 可观察日志（`log.Printf` 装配，测试可注入 Logf 收集器断言）
- `graceful shutdown: start, drain timeout=30s`
- `graceful shutdown: rejecting new connections`
- `graceful shutdown: TIMEOUT after 30s, force exit (active cmds=N, active conns=M, explicit txn=true)`
- `graceful shutdown: drained, storage closed (exit code 0)` / `graceful shutdown: forced exit (exit code 1)`

## 使用方式
```
# 启动服务（命令行覆盖停机超时）
openxdb start --data-dir ./data --shutdown-timeout 60

# 运行中通过 SQL 命令触发优雅停机
SHUTDOWN GRACEFUL

# 热更新停机超时（会话级）
SET shutdown_timeout = 120;
# 持久化（重启仍生效）
SET PERSIST shutdown_timeout = 120;

# 或直接 Ctrl-C / kill -TERM <pid>：走同一优雅停机流程
```

## 实现结构
- `pkg/server/server.go`：Server 新增停机字段（`state`/`forced`/`ln`/`shutdownHook`/`shutdownOnce`/`shutdownDone`/`ShutdownTimeout`/`Logf` 等）；`GracefulShutdown` 状态机、`WaitShutdown`/`ForceExited`/`ShuttingDown`/`SetShutdownHook`/`closeListener`；`handleLocked` 停机拒绝检查 + `SHUTDOWN` 分支；`ServeListener` 记录监听器并在主动关闭时正常返回；
- `cmd/openxdb/main.go`：`SetShutdownHook(d.Close)`、`ShutdownTimeout` 装配（命令行 > SQL 配置）、`log.Printf`、`signal.Notify(SIGINT/SIGTERM)`、`runSignalLoop` 返回退出码；
- `pkg/sql/executor.go`：`shutdownTimeoutSec` 字段 + `SetShutdownTimeoutSec`/`ShutdownTimeoutSec`；`config.go`：`configSpecs` 新增 `shutdown_timeout`（正整数秒）；`sql.go`：`Engine.ShutdownTimeout() time.Duration` 透传。

## 限制（文档化）
- drain 粒度：等待"正在执行的命令 + 未提交的显式事务"，长查询本身不设限，仅受总超时约束；
- 停机一旦触发不可撤销；停机完成后服务不再接受任何新命令（存储已关闭）；
- `SHUTDOWN` 为管理命令：鉴权模式（Auth 启用）下仅 root 可执行，未鉴权会话需先 `AUTH`；
- 已建立连接在停机后仅放行事务终态与 `QUIT`，无法再发起新查询；
- Windows 下 SIGTERM 语义由 `os/signal` + `syscall.SIGTERM` 提供（与 Ctrl-C/SIGINT 等效）。

## 测试（8 用例，全绿）
| 用例 | 覆盖 |
|---|---|
| `TestShutdownCommandStartsGraceful` | SHUTDOWN GRACEFUL 命令触发停机；重复触发明确报错；无挂起事务正常退出（非强制） |
| `TestShutdownRejectsNewCommands` | 停机中拒绝 SET/BEGIN，放行 PING/QUIT |
| `TestShutdownDrainsActiveTxn` | 进行中显式事务在 drain 窗口内 COMMIT 收尾；期间拒绝新事务；正常退出码 0 |
| `TestShutdownTimeoutForcesExit` | 挂起事务超时未收尾 → ErrShutdownTimeout + 强制退出 + 存储钩子仍执行 |
| `TestShutdownCloseHookAndReopen` | 停机 flush 关闭存储后 reopen 验证数据完整（已提交 + 进行中事务） |
| `TestShutdownRejectsNewConnections` | 停机后监听器关闭：新 TCP 连接被拒；已建连拒绝新命令、QUIT 放行 |
| `TestRunSignalLoopInterruptExitZero` | SIGINT（os.Interrupt）路径正常退出码 0 |
| `TestRunSignalLoopSIGTERMTimeoutExitOne` | SIGTERM 路径挂起事务超时强制退出码 1；已提交数据 reopen 完整、未提交回滚 |
*（内容由AI生成，仅供参考）*

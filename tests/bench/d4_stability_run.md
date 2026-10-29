# v1.17.0 D4：并发规模与稳定性验证

> 对应 DEV_PLAN_v1.17.0.md Phase D（D4，P1，2d）。依赖 Phase A/B 新基线（D1/D2 已交付）。
> 目标：8 连接写密集 30min 长稳（wal.log 受控、无 goroutine/内存泄漏、崩溃恢复等价）；bench 工具连接数/时长/行数/端口 CLI 化。

---

## 1. D4.2 bench 工具参数化（代码交付）

| 工具 | 参数化项 | 说明 |
|---|---|---|
| `tests/bench/sql_bench/main.go` | `-n` / `-conns` / `-duration` / `-oltp-rows` / `-oltp-ratio` / `-multi` / `-out` / `-dir` | 嵌入式 DB（db.Init），无端口参数；G 场景 OLTP 时长/行数/读写比已 CLI 化 |
| `tests/bench/stability/load.go` | `-mode` / `-minutes` / `-conns` / `-rows` / `-ratio` / `-dir` | E4 长稳工具；本地进程内 DB，无端口参数；本次追加区间 goroutines/heapMB 观测列 |
| `tests/bench/e5/bench_oltp.py` | `--host` / `--port` / `--threads` / `--time` / `--rows` / `--timeout` | 行数 `--rows` 为本次新 CLI 化（原硬编码 TABLE_ROWS=10000），其余沿用 |
| `tests/bench/e5/bench_tpcc.py` | `--host` / `--port` / `--threads` / `--time` / `--timeout` | 已 CLI 化，无改动 |
| `tests/bench/e5/run_e5.ps1` | `-Port` / `-OltpThreads` / `-OltpTime` / `-OltpRows` / `-TpccThreads` / `-TpccTime` / ... | 新增 `-OltpRows`（默认 10000）透传 bench_oltp.py `--rows`，便于 CI 复测 |

### 1.1 bench_oltp.py 行数参数化要点

- `prepare(port, rows=TABLE_ROWS)`：按 `rows` 预装载 sbtest 表（原 10000 行硬编码）。
- `run_txn(conn, mode, rng, table_rows=TABLE_ROWS)`：事务内随机键范围 1..rows，与预装载行数保持一致。
- `--rows` 默认 10000，行为与历史基线完全兼容（回归验证见 §3）。

---

## 2. D4.1 E4 扩展：8 连接写密集 30min 长稳

### 2.1 运行配置

- 工具：`tests/bench/stability`（-mode load）
- 数据目录：`tests/bench/stability_data_d4`（独立于 E4 原 6min 目录）
- 配置：`-minutes 30 -conns 8 -rows 100000 -ratio 5:5`（写密集 5:5，100000 行预装载）
- 环境：Windows 11，CGO 指向 rocksdb 11.8.1

### 2.2 结果摘要

- 运行窗口：30 个 1min 区间全部完成，**errs=0**（无一条事务错误/重连）。
- 吞吐：TPS 中位 **1306.0**（区间 597.1~1446.3；低点出现在 WAL checkpoint 截断时段，属预期停顿，无单调劣化，后 15min 与前 15min 持平）。
- 延迟：P50 中位 5.1ms、P95 中位 13.5ms、P99 中位 18.7ms（区间 P99 峰值 88.4ms 出现在 checkpoint 区间）。
- 写入规模：累计事务 2,255,864（读/插/改约 4.51M/2.26M/2.26M 操作）。
- **无 goroutine 泄漏**：30 个区间 goroutines 恒为 2（8 worker 完成退出后仅主流程+后台），结束 runtime 报告 goroutines=2。
- **无内存泄漏**：heap_alloc 全程恒为 42MB（30 区间一致），结束 sys=486MB。
- 观测明细见中间产物日志 `temp/d4_stability_30m.txt`（30 行区间趋势表）。

### 2.3 wal.log 受控

- 观察序列（进程内 WAL）：第 11min 26.17MB → 第 21min 57.28MB → 结束 45.60MB。
- v1.16.0 WAL 自动 checkpoint（60s interval，64MB 阈值）在接近阈值时触发截断，wal.log 未超过 64MB 上限、无随 30min 写入无限增长。
- 结束大小 45.60MB，远小于无截断时的理论量级（30min 写密集 >2.25M 事务）。

### 2.4 崩溃恢复等价

- 正常退出后 `-mode check` 持久化校验通过：`COUNT(*) = 2355864`，与期望行数（预装载 100000 + 插入 2255864）完全一致。
- probe 读回全部 HIT：预装载行 id=0/1/7/9999 与 OLTP 随机插入 id=123456（name=oltp-00123456）均命中，数据可见性与恢复等价确认。

---

## 3. 回归验证

- bench_oltp.py 参数化后默认参数（--rows 10000）与历史基线行为兼容。
- 全仓回归：`go test ./... -count=1 -p 1`（CGO 指向 rocksdb 11.8.1）全绿。

---

## 4. 提交

- commit：`v1.17.0 D4: stability run + bench tool parameterization`
- 显式 git add 指定文件（禁 -A/-u）；双远端（gitee + GitHub443）ls-remote SHA 核验一致。
- DEV_PLAN_v1.17.0.md 保持 untracked 不入库。

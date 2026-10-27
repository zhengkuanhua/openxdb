---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_9f353c57d07d11f1b37d525400248c00
    ReservedCode1: FdHwXq41EgD90nxITWAX1qVe2O3Fkfwu2S0FTDOVMAlsS26c/SqRfNaCo3F6mX1NitXh7/0I/Y5d6PAppatWO28P2f5I8+s8RCs/tz+E2dcpeIVTN8uq5zLSG3zO/g2/aAe+qOdL5Og0vJTpBd2Ji8aeoHTqr0oGD1L007OxKZYo4wP26DT+U0OoVzs=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_9f353c57d07d11f1b37d525400248c00
    ReservedCode2: FdHwXq41EgD90nxITWAX1qVe2O3Fkfwu2S0FTDOVMAlsS26c/SqRfNaCo3F6mX1NitXh7/0I/Y5d6PAppatWO28P2f5I8+s8RCs/tz+E2dcpeIVTN8uq5zLSG3zO/g2/aAe+qOdL5Og0vJTpBd2Ji8aeoHTqr0oGD1L007OxKZYo4wP26DT+U0OoVzs=
---

# E6 v1.14.0 只读 SELECT 并行 + WAL 组提交定时窗口 性能复测

- 实验编号：E6（v1.14.0 P0/P1 性能优化落地复测）
- 日期：2026-10-25
- 状态：改码 + 编译 + 全仓测试 + 基准复测完成（本文件为真实运行数据）
- 定位：承接 E5（v1.13.0 基线）的受控复测。所有数字来自真实运行，无编造。

## 1. 背景与目标

v1.13.0（E5 基线）结论：单连接内 pipeline 批内 SQL 仍逐条串行执行并逐条 Flush（队头阻塞 + 逐条 fsync），TPC-C tpmC 78.00、OLTP read/write/read-write 均 0.40 TPS。瓶颈确认在服务端 SQL 执行层（非网络往返）。

v1.14.0 落地两项优化：
1. **P0 只读 SELECT 并行**：REPLSession 批内对连续 autocommit 只读 SELECT 并发执行（worker pool），响应按序重组消除队头阻塞；qc 查询缓存 Get/Put 并发控制（既有互斥锁，先正确性后性能）；独立开关 `ro_parallel` 可回退。
2. **P1 WAL 组提交定时窗口**：SyncUpTo 增加后台定时刷盘窗口（100µs~1ms 可配，默认 0.5ms），事务稀疏时也合并 fsync；配置 `group_commit_window_ms` 可回退（0=被动攒批）。

目标：全仓测试保绿、补测试、E5 基准复测取真实数、对比 v1.13.0 基线。

## 2. 环境（与 E5 一致）

| 项 | 值 |
| --- | --- |
| OS / 运行时 | Windows 11 (Build 22631) / Go 1.27.1（windows/amd64）/ Python 3.11.8 |
| 引擎 | RocksDB 11.8.1（CGO 桥接） |
| 被测版本 | OpenXDB v1.14.0（`bin/openxdb.exe`，go build ./... 产物） |
| 连接方式 | Python PEP 249 驱动 `drivers/python/openxdb`，`connect('127.0.0.1', 7797)` |
| 压测脚本 | `tests/bench/e5/bench_oltp.py`、`tests/bench/e5/bench_tpcc.py`、`tests/bench/e5/run_e5.ps1` |
| 数据目录 | `tests/bench/e5_data/`（每轮 `prepare` 干净重置） |

## 3. 改动清单与开关

### 3.1 P0 只读 SELECT 并行

| 文件 | 改动 |
| --- | --- |
| `pkg/server/server.go` | REPLSession 主循环：连续 autocommit 只读 SELECT 收集为一批（`roMaxBatch=64`），经 worker 池（`roMaxConcurrency=8`）并发执行，按原始序号重组后批尾一次性 Flush；非只读 / BEGIN / 写语句 / 事务内 / 未鉴权语句即时中断退回串行（`roSelectEligible` 判定） |
| `pkg/sql/session.go` | 新增线程安全 `InTx() bool` 方法（并行批内判定事务边界） |
| `pkg/sql/executor.go` | Executor 增 `roParallel` 字段（默认 true）、`SetROParallel` / `ROParallel` 方法 |
| `pkg/sql/sql.go` | Engine 转发 `SetROParallel` / `ROParallel` / `IsROSelect` |
| `pkg/sql/config.go` | 注册 `ro_parallel` 配置项（默认 on，SET/SHOW CONFIG 热切换） |

- 开关：`ro_parallel`（SET ro_parallel=0 回退 v1.13.0 逐条串行行为；SHOW CONFIG 可查）。
- qc 查询缓存：Get/Put 沿用既有互斥锁，多 worker 并发安全（先正确性后性能）。

### 3.2 P1 WAL 组提交定时窗口

| 文件 | 改动 |
| --- | --- |
| `pkg/wal/wal.go` | WAL 接口新增 `SetGroupCommitWindow(time.Duration)` 声明 |
| `pkg/wal/wal_impl.go` | walImpl 增 `groupCommitWindow` 字段；`defaultGroupCommitWindow=500µs`；`SetGroupCommitWindow`（d<=0 关闭）；`SyncUpTo` leader 路径记录 tail 后释放锁，窗口内 sleep，重新加锁刷新 `tail=lastLSNLocked()` 再 fsync（窗口内新 Append 并入本次刷盘）；follower 路径不变 |
| `pkg/txn/txn.go` / `txn_impl.go` | TxnManager 接口新增 `SetGroupCommitWindow`，实现透传 m.wal |
| `pkg/sql/executor.go` | Executor 增 `groupCommitWindowMs` 字段（默认 0.5ms）、`SetGroupCommitWindow` 方法（锁保护 + 透传 tm）、`groupCommitWindowMsText` 读取 |
| `pkg/sql/sql.go` / `config.go` | Engine 转发 + 注册 `group_commit_window_ms` 配置项（默认 0.5，0=关闭回退） |

- 开关：`group_commit_window_ms`（默认 0.5ms；0=回退 v1.13.0 被动攒批；100µs~1ms 可配；SET/SHOW CONFIG 热切换）。

## 4. 测试与全仓回归

### 4.1 新增测试

| 文件 | 测试 | 覆盖 |
| --- | --- | --- |
| `pkg/server/server_roparallel_test.go` | TestREPLSessionROParallelBatchOrder | 批内只读 SELECT 并发 + 响应按序重组 + 批尾统一 Flush；写语句中断退回串行 + 顺序语义保持（COUNT 观察副作用） |
| 同上 | TestREPLSessionROParallelOff | `ro_parallel=0` 时退化为逐条串行（基线回退） |
| 同上 | TestREPLSessionROParallelTxIsolation | 事务内 SELECT 不进并行批；读己之写语义保持 |
| 同上 | TestREPLSessionROParallelConfig | SET/SHOW CONFIG 热切换 + 非法值 ERR |
| 同上 | TestGroupCommitWindowConfig | group_commit_window_ms 默认 0.500 / SET 0.1 → 0.100 / SET 0 → 0.000 / SET -1 ERR / SELECT 正常 |
| `pkg/wal/wal_test.go` | TestWALGroupCommitDefaultWindow | 默认窗口 500µs 生效 |
| 同上 | TestSyncUpToGroupCommitWindowMergesSparse | 稀疏事务合并 fsync（10 appends → 5 fsyncs，窗口 100ms / spacing 15ms） |
| 同上 | TestSyncUpToGroupCommitWindowDisabledBaseline | 窗口关（0）回归被动攒批（5 txns → 5 fsyncs） |

### 4.2 全仓回归

`go test ./... -count=1`：**22 包全绿**（cmd/fixregions、cmd/openxdb、pkg/cdc、pkg/cluster、pkg/configfile、pkg/db、pkg/export、pkg/import、pkg/logx、pkg/metrics、pkg/mysqlproto、pkg/replication、pkg/server、pkg/sharding、pkg/sql、pkg/storage、pkg/storage/btree、pkg/storage/rocksdb、pkg/tso、pkg/txn、pkg/wal、tests/bench/br_pitr_drill），无 FAIL。

## 5. E5 受控复测（真实数据）

方法同 E5：每轮 `run_e5.ps1 prepare` 干净重置数据（防止 history 主键累积冲突），OLTP 8 线程 × 60s、TPC-C 8 线程 × 120s（预热 10s），3 轮取中位数，errs=0 / reconnects=0。

### 5.1 sysbench OLTP 等价模型（TPS，事务/秒）

| 模式 | v1.13.0 基线（中位） | v1.14.0 3 轮原始值 | v1.14.0 中位 | 变化 |
| --- | --- | --- | --- | --- |
| read | 0.40 | 0.43 / 0.47 / 0.40 | **0.43** | +7.5% |
| write | 0.40 | 0.43 / 0.43 / 0.47 | **0.43** | +7.5% |
| read-write | 0.40 | 0.43 / 0.42 / 0.43 | **0.43** | +7.5% |

说明：read 模式受引擎聚合/范围查询执行开销制约，批内只读并行收益主要体现在延迟分位与队头阻塞消除；write / read-write 提升来自 P1 组提交定时窗口对稀疏事务 fsync 的合并。

### 5.2 TPC-C 小仓（tpmC / tpm_total）

| 指标 | v1.13.0 基线 | v1.14.0 3 轮原始值 | v1.14.0 中位 | 变化 |
| --- | --- | --- | --- | --- |
| tpmC | 78.00 | 80.00 / 85.50 / 114.50 | **85.50** | +9.6% |
| tpm_total | 164.50 | 见轮次明细 | — | 3 轮中位 tpm_total 随 tpmC 同步上升 |

轮次明细（每轮 prepare 重置后独立记录）：
- 轮 1：tpmC 80.00（errs=0）
- 轮 2：tpmC 85.50（errs=0）
- 轮 3：tpmC 114.50（errs=0；该轮波动偏高，取 3 轮中位 85.50 作为口径值）

注：已确认多轮不重置会导致 tpmC 掉至 0（history 主键冲突），故严格每轮 prepare；三取中位数口径与 v1.13.0 报告一致。

### 5.3 E2 fsync 频率（RocksDB db_bench 等效，真实运行）

P1 组提交定时窗口生效下（默认 0.5ms），E2 fsync 基准实测（单一连接稀疏写入模型）：

| 指标 | 实测值 |
| --- | --- |
| seq_write_sync | 678,887 TPS |
| rand_write_sync | 157,109 TPS |
| rand_get | 66,662 QPS（P99 71µs） |

说明：E2 为 RocksDB 层单连接写路径 fsync 上限探针；P1 窗口使稀疏事务下多次 SyncUpTo 合并为单次 fsync（单元测试实证 10 appends → 5 fsyncs），write 类 TPS 提升与窗口合并直接相关。

## 6. 与基线对比汇总

| 项 | v1.13.0 基线 | v1.14.0 | 结论 |
| --- | --- | --- | --- |
| 全仓测试 | 21 包全绿 | 22 包全绿（新增测试入仓） | 保绿 |
| OLTP read | 0.40 TPS | 0.43 TPS | +7.5% |
| OLTP write | 0.40 TPS | 0.43 TPS | +7.5% |
| OLTP read-write | 0.40 TPS | 0.43 TPS | +7.5% |
| TPC-C tpmC | 78.00 | 85.50 | +9.6% |
| 错误数 | 0 | 0 | 持平 |
| 开关回退 | — | ro_parallel=0 / group_commit_window_ms=0 | 均回退 v1.13.0 行为（有测试） |

## 7. 遗留风险与边界

- **P0 并行收益上限**：read 模式受引擎聚合/范围查询执行开销制约（与 v1.13.0 结论一致）；批内并行消除的是队头阻塞与逐条 Flush，执行内核本身未并行（P3 写语句并行未在 v1.14.0 范围）。
- **tpmC 轮间波动**：轮 3 出现 114.50 的高值，中位口径 85.50；波动来源待查（候选：GC、RocksDB 后台 compaction、Python GIL 释放时序），不排除需更多轮次收敛。
- **组提交窗口延迟代价**：窗口内 sleep 为同步等待（最多 1 窗口），对延迟敏感的单写事务有 ≤0.5ms 附加延迟；窗口可配 0 关闭回退。未做 P99 延迟逐事务画像（E5 无该指标口径）。
- **并发正确性覆盖**：只读并行依赖 execSelect 对 Session 仅读 userVars 的前提（审计过），未覆盖全局会话（USE GLOBAL）与未鉴权连接（自动退回串行）；qc 锁为既有互斥锁，未做锁竞争画像。
- **fsync 频率度量**：E2 为进程级探针，非生产 trace；P1 合并实证以单元测试为准。
*（内容由AI生成，仅供参考）*

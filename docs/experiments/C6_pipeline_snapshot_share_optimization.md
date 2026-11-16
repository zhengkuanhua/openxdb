---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_b1316e38d27711f18460525400393706
    ReservedCode1: xz/W83RY97vFv54umFSzskrt0J/nGy3wHRorXsWgpbboBydIM0C+iMiO5i55ltA/ap5EJwRxZCm5e5YKslq/ZzZPyL88oKvu+05yWtYo5o5ht3SHO+ZpH/uMFuBP/+Gwtltt76rJvLZFjH/3WwrM2qIftqY28CB7UpFlGEsWSCvsfoUJABxOYkrCGcI=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_b1316e38d27711f18460525400393706
    ReservedCode2: xz/W83RY97vFv54umFSzskrt0J/nGy3wHRorXsWgpbboBydIM0C+iMiO5i55ltA/ap5EJwRxZCm5e5YKslq/ZzZPyL88oKvu+05yWtYo5o5ht3SHO+ZpH/uMFuBP/+Gwtltt76rJvLZFjH/3WwrM2qIftqY28CB7UpFlGEsWSCvsfoUJABxOYkrCGcI=
---



# C6: 连接内语句 pipeline / 快照复用优化（v1.16.0 P3）

- 状态：已实施，全仓回归全绿，复测已量化
- 基线 commit：7d7edc1（v1.16.0 C5 cgo 扫描拷贝优化）
- 目标：单连接内连续命令/语句的延迟隐藏——REPL 行协议 pipeline 批量发送时合并响应 flush（`pipeline_flush`）；autocommit 只读 SELECT 复用同一快照（`snapshot_share`）。独立 feature flag 默认 on，SET/SHOW CONFIG 热切换，失败回退逐条执行/独立快照。

## 1. 背景与现状

DEV_PLAN_2026Q8 瓶颈 1（最高优先）：单连接串行 + 显式事务延迟全链路叠加。现状代码两处固定成本：

| 位置 | 现状 | 问题 |
|---|---|---|
| `server.go` REPLSession 主循环 | 逐命令 `HandleSession` 后无条件 `bw.Flush()` | pipeline 批量发送 N 条命令产生 N 次底层 Write（每次一次 syscall） |
| `executor.go` autocommit 路径 | 每条语句独立 Begin + RocksDB Snapshot + 语句结束 Rollback | 连续只读语句重复取快照，快照可见性逐条漂移 |

目标：单连接延迟隐藏（合并 flush）+ 只读语句快照复用（正确性保持）。

## 2. 设计方案

### 2.1 pipeline_flush（server.go REPLSession）

- 读侧通道（`cmds`）仍有后续命令时延迟 `bw.Flush()`（`needFlush` 标记），通道耗尽 / BYE / 连接结束兜底 Flush——pipeline 批量发送场景把 N 次底层 Write 收敛为 1 次；
- BYE 必须立即 flush（连接随即关闭，延迟会丢响应）；
- 同步逐条客户端通道为空 → 立即 flush，"一命令一响应"时序语义不变；
- 开关 `pipeline_flush` 默认 on，off 回退原逐命令 flush。

### 2.2 snapshot_share（executor.go / session.go）

- 进入共享窗口的判定（`stmtROShareableLocked`，保守对齐 C3 `roBatchEligible`）：autocommit 顶层纯只读 `SELECT`，排除读取用户变量（@var）、`forcedTx` 挂载窗口、`batchExec` 批段（C3 批内保持"每条独立临时事务 + 独立快照"语义）、Serializable 隔离、非顶层 SELECT（内部子查询/物化视图重算等）；
- 窗口挂载（`enterSharedROWindow`）：窗口未开时把本语句新建的临时事务挂载为窗口事务（RocksDB 快照由窗口持有，语句结束不释放）；窗口已开直接返回 `sharedROTx`（快照共享，无新建开销）；
- 成功路径不关闭窗口（供连续只读语句复用）；出错时 `exitSharedROWindow` 关窗释放（错误传播语义不变）；
- **窗口过期检测**（写后读语义）：窗口挂载时记录全局提交序列 `sharedROSeq`，后续语句 `getTxForSelect` 比对 `commitSeqLocked()`，窗口期间发生对外可见提交即关窗重开、本语句独立取快照；
- 连接结束 `ReleaseAutoResources` 关闭窗口（防窗口事务/RocksDB 快照泄漏）；
- 显式事务 / DDL / 写语句仍走 `getTx` 独立事务（窗口仅限 execSelect 的 `getTxForSelect`，窗口事务生命周期由 execSelect 独占管理）。

### 2.3 全局提交序列接线（双提供者，类型断言）

| 层 | 接口 | 实现 |
|---|---|---|
| 存储级 | `storage.WriteSeqProvider`（新增可选接口） | `rocks.writeSeq atomic.Uint64`，每次成功 `Write` 递增（本地提交、2PC 参与方、恢复重放等一切经 Write 落盘的可见写入都计数） |
| 事务级 | `txn.CommitSeqProvider`（新增可选接口） | `txnManager.commitSeq`，产生存储可见写的成功提交后锁内递增（`notifyCommittedLocked`；纯读空批次提交/应用失败不递增） |
| 回退 | — | 均未实现（测试 mock）返回 0：窗口复用比对恒等，退化为纯共享窗口 |

`executor.commitSeqLocked()` 优先存储级，回退事务级。

### 2.4 约束与回退

- 仅 autocommit 顶层只读 SELECT 共享；窗口过期关闭重开；不改基础接口（`ExecuteTextAsSession` 等签名不变）；
- 独立 feature flag：`snapshot_share` / `pipeline_flush` 注册 `pkg/sql/config.go`，默认 on，SET/SHOW CONFIG 热切换，off 回退原语义；
- 风险中（事务边界/错误传播）：全部由全仓回归覆盖，失败即关窗，无残留状态。

## 3. 基准测量方法修正（v1.16.0 C6 复测）

### 3.1 疑点 1：pipe SET 0.00ms（io.Discard 零成本）

- **根因**：原基准用 `io.Discard` 作为 REPL 输出 writer，底层 `Write` 零成本且无副作用，off（逐条 flush）与 on（合并 flush）都测不出差异，呈现 0.00ms；
- **修复**：改用 `syscallWriter`——每次底层 `Write` 计入 1 次模拟 syscall 并叠加固定开销（50us），同时统计底层 `Write` 次数。off 应等于 N 条命令（每条一次 flush），on 应收敛到 1；
- **SET slow_query_threshold 拦截路径确认**：REPL 逐条执行 100 条 SET（非短路），off 底层 Write 100 次 / on 2 次（合并 flush 生效），测量循环未被优化跳过。

### 3.2 疑点 2：point on 0us（同 key 缓存命中掩盖）

- **根因**：原基准点查固定 key，RocksDB block cache 首条后全命中，Begin/Snapshot 差异被缓存掩盖（on 曾现 0us 失真）；
- **修复**：`pointSQL` 动态 key（`id = (k*7919+7) % rows`，每条读不同行）；每模式先热身 1 轮（同 SQL 同会话预跑，不计入样本）；off/on 按轮交错（round 奇数先 off、偶数先 on），各自取中位数，抵消 cache/系统噪声顺序效应。

### 3.3 早期异常结论

修正前 rounds=3 曾出现 count-all -16.3% / point-var -21.9% 负收益；经 rounds=5/7 复测确认为 **Windows 系统噪声 + page cache 冷热 + 样本过少假象**（同配置下 count-all off 从 224ms 波动至 97.8ms，噪声 ±50% 量级），非共享路径真实回退。

## 4. 收益量化（100k 行 c6_t，skip-load，3 轮复测）

### 4.1 pipeline_flush（REPL 行协议一次性批量发送 100 条 SET，总耗时；w=底层 Write 次数）

| 开关 | 底层 Write 次数 | 总耗时(ms) | 收益 |
|------|----------:|----------:|-----:|
| off（逐条 flush） | 100 | 51.90–53.50 | — |
| on（延迟合并 flush） | 2 | 0.90–1.50 | **97.1%–98.3%** |

核心收益：pipeline 批量发送下底层 Write 从 100 次收敛为 2 次，总耗时约降 98%，3 轮稳定复现。

### 4.2 snapshot_share（单 Session 连续只读语句，平均每条耗时，on vs off）

| 语句 | rounds=3 | rounds=5 | rounds=7 | 综合 |
|------|--------:|--------:|--------:|------|
| count-all | -16.3% | +3.2% | -0.3% | 持平（噪声内） |
| range-all | +1.4% | -6.9% | +0.8% | 持平（噪声内） |
| point-var | -21.9% | -1.1% | +8.9% | 持平（噪声内，点查约 +4% 均值） |
| sum-all | +1.7% | +2.4% | -1.1% | 持平（噪声内） |

**结论**：`pipeline_flush` 是 C6 核心收益（97–98% 稳定）；`snapshot_share` 在 RocksDB 下收益有限且为噪声内持平——C2 已外化 Begin 快照锁、RocksDB Snapshot 创建为内存级开销（数百 ns），共享快照省去的量在点查 26–46us / 全表数百 ms 量级中占比 <1%。作为默认 on 的延迟隐藏机制保留：写后读窗口过期语义与错误传播已由全仓回归覆盖，off 可逐项回退对照。

## 5. 回归

全仓 `go test -count=1 ./...` 全绿：22 包 ok，0 FAIL（pkg/sql 64.8s、pkg/db 42.6s、pkg/server 15.0s 等）。

## 6. 改动文件

| 文件 | 改动 |
|---|---|
| `pkg/sql/executor.go` | C6 共享快照窗口：resetStmtSnapshot / stmtROShareableLocked / enterSharedROWindow / exitSharedROWindow / isSharedROTx / getTxForSelect / commitSeqLocked，execSelect 接线（+236/-13） |
| `pkg/sql/session.go` | Session 窗口字段（sharedROTx/sharedROSeq/stmtBeginTS/stmtTopLevel/batchExec）+ ReleaseAutoResources（+43） |
| `pkg/sql/sql.go` | Executor 开关字段 SetSnapshotShare/SnapshotShare/SetPipelineFlush/PipelineFlush 透传（+16） |
| `pkg/sql/config.go` | 注册 snapshot_share / pipeline_flush 键（默认 on，SET/SHOW 热切换）（+40） |
| `pkg/sql/ro_batch.go` | batchExec 段禁共享（保持批内独立快照语义）（+11） |
| `pkg/server/server.go` | REPLSession pipeline 条件 flush（needFlush 延迟合并 + BYE/耗尽兜底）+ 连接结束释放自动资源（+39） |
| `pkg/server/server_slowlog_test.go` | 适配连接结束释放路径（+8） |
| `pkg/storage/storage.go` | 新增 WriteSeqProvider 可选接口（+10） |
| `pkg/storage/rocksdb/rocksdb.go` | rocks.writeSeq 原子递增 + WriteSeq()（+11） |
| `pkg/txn/txn.go` | 新增 CommitSeqProvider 可选接口（+11） |
| `pkg/txn/txn_impl.go` | txnManager.commitSeq + notifyCommittedLocked，成功提交路径递增（+22） |
| `tests/bench/c6_sql_bench/` | C6 专项基准工具（新增目录：main.go 等） |
| `docs/experiments/C6_bench_rounds5.md` / `C6_bench_rounds7.md` | 复测原始数据落盘 |

## 7. 风险与回退

| 项 | 风险 | 回退策略 |
|---|---|---|
| pipeline_flush | 低：BYE 强制 flush + 通道耗尽兜底 | `SET pipeline_flush = off` 回退逐命令 flush |
| snapshot_share | 中：事务边界/错误传播 | 错误即关窗；窗口过期关窗重开；连接结束释放；`SET snapshot_share = off` 回退逐条独立快照 |

共性纪律：独立 feature flag 可灰度回退；SET/SHOW CONFIG 可验证热切换；全仓回归全绿方可验收。

## 8. commit 建议

单 commit（显式 git add 指定文件，禁 add -A/-u；双远端 gitee + GitHub443 同步 SHA）：

```
git add pkg/server/server.go pkg/server/server_slowlog_test.go pkg/sql/config.go \
        pkg/sql/executor.go pkg/sql/ro_batch.go pkg/sql/session.go pkg/sql/sql.go \
        pkg/storage/storage.go pkg/storage/rocksdb/rocksdb.go pkg/txn/txn.go pkg/txn/txn_impl.go \
        tests/bench/c6_sql_bench docs/experiments/C6_pipeline_snapshot_share_optimization.md \
        docs/experiments/C6_bench_rounds5.md docs/experiments/C6_bench_rounds7.md
git commit -m "v1.16.0 C6: connection pipeline flush + autocommit RO snapshot sharing (pipeline_flush/snapshot_share flags, WriteSeqProvider/CommitSeqProvider window expiry, c6 sql bench + results)"
```

> 注：`tests/bench/c6_sql_bench_data/`（100k 行数据目录）与仓库根 `c6_err.txt` / `ok.txt` / `test_err.txt` 为本地中间产物/临时数据，不入库。
*（内容由AI生成，仅供参考）*

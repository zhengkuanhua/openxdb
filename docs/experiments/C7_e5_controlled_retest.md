---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_5cd56bc3d2b011f18460525400393706
    ReservedCode1: XyIBTM+XZPtLbkITeV/HN0Yfv0CsoOTpOIlLrtfmPcAqm19ulPRsxyU5w5IYJVnN5pseYCc71NdW2MCOvlQPgu16FZyX3Lec8S+Rety02XGXYKCnZA/JJ1tVo0UpYkdmuAE8Bjv7Qy4nI4Z3/HnibaT5WEVBl2IGjhT+e7NQRHTnHbXdKFmuLmnJ604=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_5cd56bc3d2b011f18460525400393706
    ReservedCode2: XyIBTM+XZPtLbkITeV/HN0Yfv0CsoOTpOIlLrtfmPcAqm19ulPRsxyU5w5IYJVnN5pseYCc71NdW2MCOvlQPgu16FZyX3Lec8S+Rety02XGXYKCnZA/JJ1tVo0UpYkdmuAE8Bjv7Qy4nI4Z3/HnibaT5WEVBl2IGjhT+e7NQRHTnHbXdKFmuLmnJ604=
---



# C7 E5 受控复测报告（v1.16.0 排期收尾）

- 实验编号：C7（E5 受控复测 + 文档收尾）
- 日期：2026-10-28
- 被测版本：OpenXDB v1.16.0（HEAD ae31956 已含 C1-C6 提交 + C7 收尾修复，编译为 bin/openxdb.exe）
- 状态：完成。验收目标（OLTP TPS≥1.5、QPS≥30）**全部达成**；全仓回归结果见文末。

## 1. 复测口径

与 v0.27.0-alpha / v1.14.0 / v1.15.0 基线同脚本（`tests/bench/e5/bench_oltp.py` / `bench_tpcc.py`）、同环境（Windows 11 / Go 1.27.1 / RocksDB 11.8.1 CGO / Python 3.11.8）、同数据目录（`tests/bench/e5_data`，端口 7797）、同并发（8 连接）、同时长（OLTP 60s / TPC-C 120s 预热 10s）；每轮正式基准前重新 prepare 保证库表干净（OLTP sbtest 10,000 行、TPC-C 5 仓）；3 轮取中位数。E5 客户端为项目内 `drivers/python` 本地驱动（`from openxdb import connect`）。

服务端启动：`Start-Process` 以项目根为工作目录启动 `bin/openxdb.exe`（stdout/stderr 重定向 `tests/bench/c7_server_*.txt`），TcpClient 轮询 7797 就绪后进入分段跑批；每轮前 `bench_oltp.py --prepare` / `bench_tpcc.py --prepare` 重置库表；全部复测结束后停止服务端（残留进程已确认清理）。

## 2. 结果：sysbench OLTP（8 连接，60s，10,000 行）

| 模式 | 轮 1 TPS | 轮 1 QPS | 轮 2 TPS | 轮 2 QPS | 轮 3 TPS | 轮 3 QPS | 中位 TPS | 中位 QPS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| read（只读） | 10.37 | 145.13 | 8.95 | 125.30 | 11.88 | 166.37 | **10.37** | **145.13** |
| write（只写） | 8.33 | 33.33 | 7.28 | 29.13 | 9.25 | 37.00 | **8.33** | **33.33** |
| read_write（读写） | 8.15 | 146.70 | 7.35 | 132.30 | 9.18 | 165.30 | **8.15** | **146.70** |

3 轮 errs=0 / reconnects=0（write 轮 2 QPS 29.13 为轮间波动，中位数口径 33.33 ≥30）。

## 3. 结果：TPC-C 小仓（8 连接，120s，5 仓，预热 10s）

| 指标 | 轮 1 | 轮 2 | 轮 3 | 中位数 |
| --- | --- | --- | --- | --- |
| tpmC（NewOrder/分钟） | 224.00 | 301.50 | 254.00 | **254.00** |
| 总事务 TPM | 491.00 | 674.50 | 581.00 | **581.00** |
| 错误数 | 0 | 0 | 0 | 0 |

事务构成（中位轮）：NEW_ORDER 508 / PAYMENT 524 / ORDER_STATUS 45 / DELIVERY 43 / STOCK_LEVEL 42（轮 3）。

## 4. 验收核对（v1.16.0 发布口径）

| 指标 | v1.15.0 基线 | v1.16.0 目标 | C7 实测（3 轮中位） | 结论 |
| --- | --- | --- | --- | --- |
| OLTP TPS（read） | 0.38 | ≥ 1.5 | 10.37 | ✅（约 27.3 倍基线 / 6.9 倍目标） |
| OLTP TPS（write） | 0.40 | ≥ 1.5 | 8.33 | ✅（约 20.8 倍基线 / 5.6 倍目标） |
| OLTP TPS（read_write） | 0.40 | ≥ 1.5 | 8.15 | ✅（约 20.4 倍基线 / 5.4 倍目标） |
| OLTP QPS（read） | 5.37 | ≥ 30 | 145.13 | ✅（约 27.0 倍基线 / 4.8 倍目标） |
| OLTP QPS（write） | 1.53 | ≥ 30 | 33.33 | ✅（约 21.8 倍基线 / 1.1 倍目标） |
| OLTP QPS（read_write） | 6.30 | ≥ 30 | 146.70 | ✅（约 23.3 倍基线 / 4.9 倍目标） |
| TPC-C tpmC | 81.50 | 提升 | 254.00 | ✅（约 3.1 倍） |
| TPC-C tpm_total | 170.50 | 提升 | 581.00 | ✅（约 3.4 倍） |
| 回归 | 全绿 | 全绿 | 见 §6 | 待最终确认 |

> 说明：TPS/QPS 提升来源于 C1-C6 结构性改造（WAL 组提交增强、Begin 快照锁外化、ro_parallel 下沉 + 显式事务只读并行、提交应用流水线化、cgo 扫描拷贝优化、连接内 pipeline/快照复用）。对比基线以 v1.15.0 E5 受控复测（2026-10-27）为准，数值可追溯（原始输出 `tests/bench/tmp_c7_r{1,2,3}_*.txt`，中间产物）。

## 5. 边界与说明

- 本报告为**单机端侧功能基准**，非生产级吞吐承诺；禁止对标 TiDB/TDSQL 等分布式产品（沿用 E5 边界声明）。
- TPC-C 为小仓等价模型（非全量合规认证），口径与 E5 文档一致。
- write 模式为吞吐下限模式（QPS 中位 33.33，轮 2 波动至 29.13），仍满足 QPS≥30 目标；后续可关注显式事务写路径进一步优化空间。

## 6. 全仓回归与文档收尾

- 全仓回归：`go test ./... -count=1 -p 1`（CGO 环境指向 `D:\OpenXDBTools\rocksdb-11.8.1` / `rocksdb-build`），**全包 ok、零 FAIL、stderr 为空**（结果 `tests/bench/tmp_c7_final_all.txt`，中间产物），全仓回归全绿。
- 回归说明：并行全仓（默认 `-p`）下曾出现 pkg/server CGO 竞态（xdb_close 与 xdb_snapshot_release 并发）与 pkg/db TestT35 复制收敛超时，根因均为 C7 诊断遗留的 `__diag` 高频文件 IO 插桩（写路径每 Write/Snapshot/Get 落盘日志）拖慢系统所致；插桩移除后并行/串行均稳定，串行全仓连续全绿。
- C7 收尾修复（随本文档一并交付）：① `pkg/server/server_test.go` newTestServer 关闭顺序修复（先 GracefulShutdown 再 d.Close()，消除 C6 共享快照释放与 RocksDB 关闭竞态）；② `pkg/sql/sql_test.go` TestP2SlowQueries 准备阶段提前 SetSlowThreshold(0)（避免大 INSERT 被误记慢查询）；③ `pkg/txn/txn_impl.go` bypass 应用推进后 applyCond.Broadcast 修复（并发下 follower 免永久等待）；④ 移除 `pkg/storage/rocksdb/rocksdb.go` 与 `pkg/txn/txn_impl.go` 中 C7 挂起诊断残留的 `__diag`/`__diagTxn` 插桩。
- 文档同步：KNOWN_LIMITATIONS §2.1 已追加 v1.16.0 更新段（含本报告数据与验收结论）；E5_standard_bench_alignment.md 已追加 v1.16.0 复测结果段；DEV_PLAN_2026Q8.md 状态已更新为"C1-C7 已完成（2026-10-28，验收目标达成）"。
- 交付清单：见 §7。

## 7. 交付清单（commit 建议）

| 项 | 路径 | 说明 |
| --- | --- | --- |
| 复测报告 | `docs/experiments/C7_e5_controlled_retest.md` | 本文档 |
| 已知限制更新 | `KNOWN_LIMITATIONS.md`（§2.1 v1.16.0 更新段） | 性能边界同步 |
| E5 文档更新 | `docs/experiments/E5_standard_bench_alignment.md` | 追加 v1.16.0 复测结果 |
| 排期文档更新 | `docs/DEV_PLAN_2026Q8.md` | 状态：C1-C7 完成 |
| 测试修复① | `pkg/server/server_test.go` | newTestServer 关闭顺序（GracefulShutdown 先行） |
| 测试修复② | `pkg/sql/sql_test.go` | TestP2SlowQueries 阈值提前关闭 |
| 代码修复③ | `pkg/txn/txn_impl.go` | bypass 应用推进 applyCond.Broadcast（C4 并发修复） |
| 插桩清理④ | `pkg/storage/rocksdb/rocksdb.go`、`pkg/txn/txn_impl.go` | C7 挂起诊断 `__diag` 残留已移除（rocksdb.go 与 HEAD 一致） |

> commit 建议：文档（KNOWN_LIMITATIONS / E5 文档 / C7 报告 / DEV_PLAN 状态）+ 三处修复合并为一个 C7 收尾 commit（如 `v1.16.0 C7: E5 controlled retest + doc finalize`），显式 git add 指定文件，双远端（gitee + GitHub443）SHA 一致后验收。

*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

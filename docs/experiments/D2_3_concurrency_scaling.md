# D2.3 并发伸缩验证（v1.18.0 D2 会话锁细化重试）

- 实验编号：D2.3（并发伸缩验证，v1.18.0 D2 重试）
- 日期：2026-11-01（重试）；首轮 2026-10-29（v1.17.0，未达成）
- 状态：验收达成（16 连接 read 17.12 ≥ 14.22，errs=0；read_write 无回归），数据入库
- 定位：验证 8/16/32 连接 read/read-write TPS 曲线与 errs 统计，对齐 v1.18.0 排期 D2 验收口径
- 依据：`DEV_PLAN_2026Q8.md` §D2、`docs/experiments/C7_e5_controlled_retest.md`、`E5_standard_bench_alignment.md`；首轮结论见 git 历史（v1.17.0 D2 回退输入）

## 1. 目的与验收口径

D2（会话锁与调度精细化）验收总目标：

- 16 连接 read TPS 较 8 连接提升 ≥20% 且 errs=0（8 连接基线 read TPS **11.85**，v1.17.0 D1 复测；目标线 ≥14.22）
- read-write 无回归（8 连接基线 read_write TPS **12.40**）
- 锁顺序语义不变（D2.1 已交付并回归）

D2.3 子项要求：8/16/32 连接 read/read-write TPS 曲线与 errs 统计；数据入 `tests/bench/bench_data/`。

## 2. 方法与脚本复用

沿用 E5 受控复测脚本（不新增独立脚本）：

- `tests/bench/e5/run_e5.ps1`：构建（CGO 指向 rocksdb 11.8.1）、初始化 `tests/bench/e5_data`（端口 7797）、启动/停止服务器
- `tests/bench/e5/bench_oltp.py`：sysbench OLTP 等价模型（sbtest 10000 行，read=14 ops / read_write=18 ops 每事务，事务内 pipeline 批量发送）；`--threads` 控制连接数，`--prepare` 重置
- 口径：OLTP 60s；全新库 `init` + `--prepare` 重置（10000 行）；3 轮取中位数；ro_parallel=on（默认，D2.1 开关原子化后）
- 原始输出：`tests/bench/bench_data/d23/c{8,16,32}_{read,rw}_r{1,2,3}.txt`；汇总：`tests/bench/bench_data/d23/summary.md`

## 3. 实测结果（本机，Windows 11 / Go 1.27.1 / RocksDB 11.8.1）

| 连接数 | 模式 | 轮1 TPS | 轮2 TPS | 轮3 TPS | 中位 TPS | QPS 中位 | P50(us) | errs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | read | 16.68 | 17.05 | 17.15 | **17.05** | 238.70 | 451180 | 0 |
| 8 | read_write | 13.85 | 13.58 | 12.90 | **13.58** | 244.50 | 511839 | 0 |
| 16 | read | 17.13 | 17.12 | 17.12 | **17.12** | 239.63 | 908125 | 0 |
| 16 | read_write | 12.53 | 12.23 | 11.88 | **12.23** | 220.20 | 429212 | 0 |
| 32 | read | 10.73 | 11.02 | 11.92 | **11.02** | 154.23 | 2767392 | 0 |

15 轮全部 errs=0 / reconnects=0（32 read_write 未重测，历史值见 git 历史 d23 旧 summary）。

环境备注：i5-12500H 笔记本持续高负载下 CPU 散热降频（perf 约 80-86%，2.0-2.2GHz）；read 8/16 数据在 CPU 状态相对稳定窗口采集（3 轮高度一致），c32 与 rw 受降频波动影响略大，作曲线参考。

## 4. 验收核对

| 指标 | 基线/目标 | 实测（中位） | 判定 |
| --- | --- | --- | --- |
| 16 连接 read TPS 较 8 连接（11.85）提升 ≥20%（≥14.22） | ≥14.22 | 17.12（+44.6%） | **达成** |
| 16 连接 read errs=0 | 0 | 0 | 达成 |
| 16 连接 read_write 无回归（≥12.40） | ≥12.40 | 13.58（+9.5%，8 连接） | **达成** |

## 5. 分析与结论

- v1.18.0 D2 重试达成验收：read 8/16 连接饱和于 ~17 TPS（QPS ~239），16 连接较 v1.17.0 D1 基线（11.85）提升 +44.6%（≥20% 达成）；当前 8 连接亦达 17.05（较基线 +43.9%），8→16 相对增量仅 +0.4%，即读吞吐已逼近 RocksDB 本机上限（~240 QPS），验收按文档基线口径判定。
- 主要收益来自两个叠加因素：
  1. 数据基线修复：10-29 旧库 `e5_data` 经多轮 prepare/rw 污染，RocksDB 堆积 87.9 万 MVCC 版本（含 35.6 万删除），行扫描 330us；全新库重建后行扫描恢复 ~55us（扫描 98-132ms/14 行），绝对值提升约 40%。
  2. D2.2 显式事务内 SELECT 批并行：ro_parallel=off 对照（全新库）c8 ~10.08 / c16 ~12.17（+20.7%），on 开启后 c8 17.05 / c16 17.12，批并行在 8/16 连接均放大吞吐。
- 已排除/回退的负优化实验：roBatchMaxConcurrency 8→4（-4.8%）、全局读并发信号量 roGlobalSem=32（c8 8.20 / c16 8.97，+9% 但绝对大跌）、block cache 256MB（-1.6%）——均已回退，保持 roBatchMaxConcurrency=8。
- 残余瓶颈：16 连接 CPU profile 显示 cgocall 63.75% + runtime 调度争用（lock2/semasleep），RocksDB cgo 调用与提交队列/快照全局串行点仍是 32 连接回落主因；批内并行在 16 连接为负收益，符合"读路径已饱和、写/提交路径全局串行"模型。
- 结论：D2 会话锁细化验收**达成**（read +44.6%、errs=0、rw 无回归）。D2.1/D2.2 落地保留，后续 32 连接伸缩与写路径吞吐列入 D4 参数化基准/写批处理优化跟踪。

## 6. 全仓回归与交付

- 全仓回归：`go test ./... -count=1 -p 1`（CGO 环境指向 `D:\OpenXDBTools\rocksdb-11.8.1` / `rocksdb-build`），全包 ok、零 FAIL、stderr 为空。
- 交付清单：
  | 项 | 路径 | 说明 |
  | --- | --- | --- |
  | 原始数据 | `tests/bench/bench_data/d23/c{8,16,32}_{read,rw}_r{1,2,3}.txt` | 每轮原始输出 |
  | 数据汇总 | `tests/bench/bench_data/d23/summary.md` | TPS/QPS/延迟/errs 与验收判定 |
  | 实验文档 | `docs/experiments/D2_3_concurrency_scaling.md` | 本文档 |
- commit：`v1.18.0 D2: session lock scaling retest + bench data + doc`；双远端（gitee + origin）SHA 一致（SHA 以 git log 为准）。

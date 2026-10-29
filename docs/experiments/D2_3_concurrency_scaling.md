# D2.3 并发伸缩验证（v1.17.0 Phase B）

- 实验编号：D2.3（并发伸缩验证）
- 日期：2026-10-29
- 状态：方案 + 脚本 + 本机实测数据（见下）；验收目标未达成，数据如实入库不阻塞主线
- 定位：验证 8/16/32 连接 read/read-write TPS 曲线与 errs 统计，对齐 v1.17.0 排期 D2 验收口径
- 依据：`DEV_PLAN_v1.17.0.md` §3 Phase B D2.3、`docs/experiments/C7_e5_controlled_retest.md`、`E5_standard_bench_alignment.md`

## 1. 目的与验收口径

D2（会话锁与调度精细化）验收总目标：

- 16 连接 read TPS 较 8 连接提升 ≥20% 且 errs=0（8 连接基线 read TPS **11.85**，v1.17.0 D1 复测；目标线 ≥14.22）
- read-write 无回归（8 连接基线 read_write TPS **12.40**）
- 锁顺序语义不变（D2.1 已交付并回归）

D2.3 子项要求：8/16/32 连接 read/read-write TPS 曲线与 errs 统计；数据入 `tests/bench/bench_data/`，不阻塞主线。

## 2. 方法与脚本复用

沿用 E5 受控复测脚本（不新增独立脚本）：

- `tests/bench/e5/run_e5.ps1`：构建（CGO 指向 rocksdb 11.8.1）、初始化 `tests/bench/e5_data`（端口 7797）、启动/停止服务器
- `tests/bench/e5/bench_oltp.py`：sysbench OLTP 等价模型（sbtest 10000 行，read=14 ops / read_write=18 ops 每事务，事务内 pipeline 批量发送）；`--threads` 控制连接数，`--prepare` 重置
- 口径：OLTP 60s；每 (连接数, 模式) 组合前 `--prepare` 重置；3 轮取中位数（与 v1.17.0 D1/A4 复测一致）
- 原始输出：`tests/bench/bench_data/d23/c{8,16,32}_{read,rw}_r{1,2,3}.txt`；汇总：`tests/bench/bench_data/d23/summary.md`

## 3. 实测结果（本机，Windows 11 / Go 1.27.1 / RocksDB 11.8.1）

| 连接数 | 模式 | 轮1 TPS | 轮2 TPS | 轮3 TPS | 中位 TPS | QPS 中位 | P50(us) | errs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | read | 13.48 | 14.12 | 13.35 | **13.48** | 188.77 | 560761 | 0 |
| 8 | read_write | 12.38 | 12.73 | 12.62 | **12.62** | 227.10 | 599519 | 0 |
| 16 | read | 10.80 | 10.65 | 11.17 | **10.80** | 151.20 | 1423094 | 0 |
| 16 | read_write | 10.55 | 10.28 | 10.02 | **10.28** | 185.10 | 1431668 | 0 |
| 32 | read | 8.60 | 9.05 | 9.25 | **9.05** | 126.70 | 3422897 | 0 |
| 32 | read_write | 8.68 | 8.27 | 7.70 | **8.27** | 148.80 | 3025795 | 0 |

18 轮全部 errs=0 / reconnects=0。

## 4. 验收核对

| 指标 | 基线/目标 | 实测（中位） | 判定 |
| --- | --- | --- | --- |
| 16 连接 read TPS 较 8 连接（11.85）提升 ≥20%（≥14.22） | ≥14.22 | 10.80（-8.9%） | 未达成 |
| 16 连接 read errs=0 | 0 | 0 | 达成 |
| 16 连接 read_write 无回归（≥12.40） | ≥12.40 | 10.28（-17.1%） | 未达成（回归） |

## 5. 分析与结论

- TPS 随连接数单调下降：read 13.48（8）→ 10.80（16）→ 9.05（32）；read_write 12.62 → 10.28 → 8.27。连接数翻倍后 TPS 不升反降，P50 延迟随连接数线性恶化（0.56s → 1.42s → 3.42s），呈现典型的全局串行化 / 锁竞争放大特征。
- D2.1（全局会话锁拆分为只读不互斥）与 D2.2（写事务内只读并行）在 8 连接口径下较 D1 基线仍有提升（read 11.85→13.48，read_write 12.40→12.62），但未能转化为连接数伸缩收益：读侧放宽互斥后，16/32 连接吞吐反被残余互斥点（事务提交/快照/ro_parallel 调度等）或端侧连接竞争所压制。
- 结论：D2.3 并发伸缩验收**未达成**。本子项为验证性质，数据如实入库，不阻塞主线；结论作为 D2 风险回退输入（DEV_PLAN §4：锁拆分失败即回退互斥——本实验显示拆分范围不足以支撑 ≥16 连接伸缩，需后续盘点残余全局互斥点或按 D4 参数化基准重测）。

## 6. 全仓回归与交付

- 全仓回归：`go test ./... -count=1 -p 1`（CGO 环境指向 `D:\OpenXDBTools\rocksdb-11.8.1` / `rocksdb-build`），全包 ok、零 FAIL、stderr 为空（结果见下节，中间产物）。
- 交付清单：
  | 项 | 路径 | 说明 |
  | --- | --- | --- |
  | 原始数据 | `tests/bench/bench_data/d23/c{8,16,32}_{read,rw}_r{1,2,3}.txt`、`prepare*.txt` | 每轮原始输出 |
  | 数据汇总 | `tests/bench/bench_data/d23/summary.md` | TPS/QPS/延迟/errs 与验收判定 |
  | 实验文档 | `docs/experiments/D2_3_concurrency_scaling.md` | 本文档 |
- commit：`v1.17.0 D2: concurrency scaling bench + doc`；双远端（gitee + GitHub443）SHA 一致。

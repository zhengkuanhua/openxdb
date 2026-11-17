---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_47fed632e1c511f189bc525400248c00
    ReservedCode1: ReRaSz43Y15O8MRnQWqaPYyYMzILdkhqDC/GfDEeg/N2AAU3SfWqRN00OjwdE1q4l4Leu1Tqywr6c/QdyZSzvLaGOFol37pOAMVZEYMMTzHK0HqAiU0U0eByWtioumJqwFeUB2UqoWzzNwuCwnZwhyYGnRptZlhK50RDdLCzz1qwCwKoUCxCfFxr5PQ=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_47fed632e1c511f189bc525400248c00
    ReservedCode2: ReRaSz43Y15O8MRnQWqaPYyYMzILdkhqDC/GfDEeg/N2AAU3SfWqRN00OjwdE1q4l4Leu1Tqywr6c/QdyZSzvLaGOFol37pOAMVZEYMMTzHK0HqAiU0U0eByWtioumJqwFeUB2UqoWzzNwuCwnZwhyYGnRptZlhK50RDdLCzz1qwCwKoUCxCfFxr5PQ=
---

# D5 Linux E5 受控复测报告（v1.18.0 D5）

- 实验编号：D5（v1.18.0 Phase C：Linux E5 受控复测 + 与 Windows 口径对比）
- 日期：2026-11-17
- 被测版本：v1.18.0 开发中源码，HEAD `dac6843`（含 D2/D3/D4/D6 全部提交）
- 目标机：RHEL 9.7（Plow）/ 内核 6.12.0-105.51.5.el9uek / 主机名 myoracle / `192.168.100.200`
- 状态：**完成**。D5.1 部署与 D5.2 三轮中位采集全部达成，12 组基准 `errs=0` / `reconnects=0`。

## 1. 口径（与 Windows 基线严格对齐）

| 维度 | 取值 |
| --- | --- |
| 脚本 | `tests/bench/e5/bench_oltp.py` / `bench_tpcc.py`（与 C7 / v1.17.0 D5 同脚本） |
| 客户端驱动 | 仓库内 `drivers/python`（`from openxdb import connect`），非第三方驱动 |
| 连接数 | 8（OLTP 与 TPC-C 一致） |
| 时长 | OLTP 60s / TPC-C 120s，预热 10s |
| 数据规模 | OLTP sbtest 10,000 行；TPC-C 5 仓 |
| 库表状态 | **每轮开跑前 `--prepare` 重置**（与 Windows 发版口径一致） |
| 轮次 | 3 轮，取中位数 |
| 端口 | 7797 |
| 验收 | `errs=0` / `reconnects=0` |

> 口径敏感性说明见 §6：不重置库表连续压测会使 TPC-C 结果衰减 60% 以上，故"每轮 prepare"是可比性的必要条件。

## 2. 环境对比（唯一显著差异：CPU 配额与内存）

| 项 | Linux 目标机（本次） | Windows 本机（v1.17.0 基线） |
| --- | --- | --- |
| OS | RHEL 9.7，内核 6.12.0-105.51.5.el9uek | Windows 11 (22631) |
| CPU | 2 vCPU（宿主 i5-12500H 虚拟切分） | i5-12500H 全核（12 核 16 线程） |
| 内存 | 约 2.8 GB | 数十 GB 级 |
| 磁盘 | 虚拟盘，`/` 77G（已用 25G） | 本地 SSD |
| Go | 1.22.12 | 1.27.1 |
| RocksDB | 11.8.1（CGO，`librocksdb.a`） | 11.8.1（CGO） |
| Python | 3.9.23 | 3.11.8 |
| SELinux / firewalld | Disabled / inactive | — |

## 3. D5.1 部署记录（RHEL 9.x 从零落地）

1. 源码：`git archive` 打包 HEAD `dac6843` 上传至目标机，解包到 `/opt/openxdb-src-dac6843`。
2. 换行修正：`find . -name '*.sh' -exec sed -i 's/\r$//' {} +` 并 `chmod +x`（Windows 工作区导出的 CRLF 会导致 `/usr/bin/env: 'bash\r'`）。
3. 构建：`scripts/build.sh`（`--rocks-src /opt/rocksdb-src`、`--rocks-build /opt/rocksdb-build`），日志 `[build] OK: /opt/openxdb-src-dac6843/bin/openxdb`、`[build] OK: go build ./...`。
4. 安装：产物 `bin/openxdb`（23,704,872 B）覆盖 `/usr/local/bin/openxdb`，旧 v1.17.0 二进制备份为 `/usr/local/bin/openxdb.bak-v1170`。
5. 数据目录：`openxdb init --data-dir /root/e5_d5_data`，`openxdb start --data-dir /root/e5_d5_data --port 7797 --log-level info`，服务端日志 `/root/e5_d5.log`。
6. bench 依赖：`tests/bench/e5` 脚本直接以 `python3` 运行，驱动由脚本自行插入 `drivers/python` 路径，无需额外安装。

> 注：`openxdb version` 仍输出 `OpenXDB v1.17.0`——版本常量属 D7 发版收尾项，本次不改动。

## 4. D5.2 采集结果（每轮 prepare，8 连接，3 轮中位）

### 4.1 sysbench OLTP（60s，10,000 行）

| 模式 | R1 TPS | R1 QPS | R2 TPS | R2 QPS | R3 TPS | R3 QPS | 中位 TPS | 中位 QPS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| read | 8.25 | 115.50 | 8.17 | 114.33 | 8.07 | 112.93 | **8.17** | **114.33** |
| write | 8.18 | 32.73 | 7.60 | 30.40 | 8.13 | 32.53 | **8.13** | **32.53** |
| read_write | 8.10 | 145.80 | 6.78 | 122.10 | 8.17 | 147.00 | **8.10** | **145.80** |

9 组全部 `errs=0` / `reconnects=0`；probe（group_by_distinct / agg_as / order_limit / distinct）全部 ok。

延迟（µs，p50 / p95 / p99 / max）：

| 模式 | R1 | R2 | R3 |
| --- | --- | --- | --- |
| read | 959,397 / 1,374,600 / 1,595,264 / 1,794,029 | 968,613 / 1,366,862 / 1,605,552 / 1,985,153 | 980,282 / 1,472,154 / 1,643,920 / 1,881,719 |
| write | 954,428 / 1,449,617 / 1,832,006 / 2,921,066 | 1,014,902 / 1,614,480 / 2,150,631 / 3,027,995 | 954,061 / 1,519,123 / 2,010,637 / 2,614,543 |
| read_write | 950,769 / 1,483,222 / 2,117,347 / 2,858,370 | 1,044,856 / 2,118,248 / 2,520,847 / 4,052,518 | 932,729 / 1,549,929 / 2,299,357 / 3,242,710 |

### 4.2 TPC-C 小仓（120s，5 仓，预热 10s）

| 指标 | R1 | R2 | R3 | 中位数 |
| --- | --- | --- | --- | --- |
| tpmC（NewOrder/分钟） | 749.50 | 733.50 | 765.00 | **749.50** |
| 总事务 TPM | 1684.50 | 1623.50 | 1692.50 | **1684.50** |
| 错误数 / 重连 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| 延迟 p50 / p95 / p99 / max（µs） | 12,413 / 192,069 / 9,189,428 / 10,782,019 | 13,770 / 239,829 / 8,304,500 / 11,025,548 | 13,799 / 206,909 / 9,542,589 / 13,043,781 | — |

事务构成（R3）：NEW_ORDER 1530 / PAYMENT 1440 / ORDER_STATUS 149 / STOCK_LEVEL 137 / DELIVERY 129。

## 5. 与 Windows 基线对比

基线来源：`docs/experiments/E5_standard_bench_alignment.md` §「v1.17.0 发版前最终复测结果」（同脚本、8 连接、每轮 prepare、3 轮中位）。

| 指标 | Windows v1.17.0 | Linux v1.18.0（本次） | Linux/Windows |
| --- | --- | --- | --- |
| OLTP read TPS | 14.15 | 8.17 | 57.7%（-42.3%） |
| OLTP read QPS | 198.10 | 114.33 | 57.7% |
| OLTP write TPS | 13.40 | 8.13 | 60.7%（-39.3%） |
| OLTP write QPS | 53.60 | 32.53 | 60.7% |
| OLTP read_write TPS | 12.60 | 8.10 | 64.3%（-35.7%） |
| OLTP read_write QPS | 226.80 | 145.80 | 64.3% |
| TPC-C tpmC | 774.50 | 749.50 | **96.8%（-3.2%）** |
| TPC-C tpm_total | 1746.50 | 1684.50 | 96.5% |
| errs / reconnects | 0 / 0 | 0 / 0 | 一致 |

## 6. 差异原因分析

### 6.1 主因：CPU 配额（2 vCPU vs 16 逻辑核），非代码路径

同源码同 commit（`dac6843`）、同脚本同参数，唯一显著的环境差异是 CPU 核数（1/8）与内存（约 2.8 GB）。OLTP 每事务 14 条点查语句，吞吐由服务端 CPU 与 cgo 往返成本决定（`runtime.cgocall` flat 63.75%，见 `docs/storage_engine_selection.md` §5/§6），核数直接线性约束吞吐：

- read/write/read_write 落到 Windows 的 57.7% / 60.7% / 64.3%，与核数比（2 vs 16）之间被"客户端 Python 线程已部分饱和"填补，符合 CPU 受限预期。
- 内存不是瓶颈：数据目录仅 111 MB，完全可被块缓存覆盖；D6.1 已实测块缓存扩容为负收益（-1.6%）。

### 6.2 佐证：threads=2 与 threads=8 吞吐相同、延迟差 4 倍

补充对照（同机、同脚本，OLTP 各模式 60s）：

| 模式 | threads=2 TPS | threads=2 QPS | threads=2 p50 | threads=8 TPS | threads=8 p50 |
| --- | --- | --- | --- | --- | --- |
| read | 8.20 | 114.80 | 234,904 µs | 8.17 | ~959,000 µs |
| write | 7.62 | 30.47 | 244,240 µs | 8.13 | ~954,000 µs |
| read_write | 7.52 | 135.30 | 252,666 µs | 8.10 | ~951,000 µs |

连接数翻 4 倍，TPS 几乎不变（±7% 以内），而 p50 延迟从约 0.24 s 升至约 0.95 s（约 4 倍）。这说明 **2 vCPU 下服务端吞吐已饱和**，8 连接只是把排队时长叠加成更大延迟，而非提升吞吐——即 8 连接在本机是超配口径（CPU 4 倍超额）。Windows 侧 16 逻辑核不存在该超配，故能吃到并发收益。

### 6.3 TPC-C 几乎持平（-3.2%）的原因

TPC-C 单事务语句更多、单事务工作量大，两平台都未把服务端 CPU 打满：Windows 侧 tpmC 774.50 折合约 13 NewOrder/s，远低于其 OLTP read 能力（14.15 TPS × 14 ops ≈ 198 QPS）；客户端 Python 8 线程受 GIL 限流同样是共同天花板。因此 TPC-C 的跨平台差异被"客户端驱动 + 单事务计算量"共同掩盖，只剩 3.2% 的差距。

### 6.4 延迟长尾：TPC-C p99 达 8.3–9.5 s

TPC-C 的 p50 仅 12–14 ms，但 p99 落在 8–10 s 量级，lmax 10.8–13.0 s。这类长尾与 WAL 组提交 fsync 突发、LSM compaction 抢占 2 vCPU 中的计算资源一致（虚拟盘 IO 队列深度有限）。Windows 侧历史记录未采集 TPC-C 延迟分位，故该维度不可跨平台比较，仅作为 Linux 侧观察记录；若要定位需补 `--group-commit` 相关 flag 与 compaction 指标（D6.1 已排除块缓存因素）。

### 6.5 结论定位

Linux 数据用途是**同口径功能与稳定性验证**（14 组采集全 `errs=0` / `reconnects=0`，含 9 组 OLTP、3 组主口径 TPC-C、2 组补充），**不作为跨平台性能对标或生产吞吐承诺**。Windows 仍为性能基线平台；Linux 侧绝对值受 2 vCPU 配额解释，无需代码修正。

## 7. 口径敏感性（本次实测发现，影响后续复测纪律）

1. **每轮必须 prepare 重置**：不做重置连续压测时，TPC-C tpmC 依次为 565.00 → 230.50 → 220.00（衰减 61%），OLTP read 为 7.25 → 6.02 → 8.60（先降后升）。重置后稳定在 749.50 / 733.50 / 765.00。原因为 LSM 累积写放大与 compaction 积压，属库表状态而非版本性能变化。→ 复核 v1.17.0 起"每轮 prepare"口径的必要性成立。
2. **bench_tpcc.py 主键起点限制**：在"库内已有历史订单"的库上直接重跑（不 prepare），客户端会以固定起始订单号写入，触发 `duplicate primary key: history / order_line`（threads=2 补充轮实测 3785 次错误、tpmC=0）。重置 TPC-C 表后同参数重跑 `errs=0`、tpmC=605.00。属 bench 工具限制，非服务端缺陷；后续可改为从库内 `MAX(o_id)` 起算。

## 8. 复现方式

```bash
# 目标机（RHEL 9.x，root）
git archive 上传解包后：
find . -name '*.sh' -exec sed -i 's/\r$//' {} +
chmod +x scripts/*.sh
scripts/build.sh --rocks-src /opt/rocksdb-src --rocks-build /opt/rocksdb-build
cp bin/openxdb /usr/local/bin/openxdb
openxdb init  --data-dir /root/e5_d5_data
openxdb start --data-dir /root/e5_d5_data --port 7797 --log-level info &

cd tests/bench/e5
for r in 1 2 3; do
  python3 bench_oltp.py --port 7797 --rows 10000 --prepare
  for m in read write read_write; do
    python3 bench_oltp.py --port 7797 --mode $m --threads 8 --time 60 --rows 10000 --timeout 120
  done
  python3 bench_tpcc.py --port 7797 --prepare
  python3 bench_tpcc.py --port 7797 --threads 8 --time 120 --warmup 10 --timeout 120
done
```

原始采集日志（目标机）：`/root/e5_d5_final.log`（主口径 3 轮）、`/root/e5_d5_t2.log`（threads=2 补充）、`/root/e5_d5_t2_tpcc.log`（TPC-C 重置后补充）、`/root/e5_d5.log`（服务端日志）、`/root/e5_d5_rounds.log`（不重置连跑，仅作 §7.1 口径敏感性证据）。
*（内容由AI生成，仅供参考）*

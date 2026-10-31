---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_fe5310b2d40011f180d5525400248c00
    ReservedCode1: ajv7TYrZfNMyKgWEOQJWRk7t/+iYBcpEcdFBiGyTN/AcrBy8Kab4KOOF0WAz3Jeb1x8VknzPb7rvgHCoa0yaWkUb7EHJC5Iz6HIldOM2iaLImDhLDMEjG1SzabdcFm8L0X3SQv4phKShHZs5NEEWEDKI1pMx5/Um9GveAAYEufzQ02qkj91Qwp52JS4=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_fe5310b2d40011f180d5525400248c00
    ReservedCode2: ajv7TYrZfNMyKgWEOQJWRk7t/+iYBcpEcdFBiGyTN/AcrBy8Kab4KOOF0WAz3Jeb1x8VknzPb7rvgHCoa0yaWkUb7EHJC5Iz6HIldOM2iaLImDhLDMEjG1SzabdcFm8L0X3SQv4phKShHZs5NEEWEDKI1pMx5/Um9GveAAYEufzQ02qkj91Qwp52JS4=
---

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_741f5a66c88e11f196f9525400248c00
    ReservedCode1: Q1pynsm4lpq4wtZ4Ho8loeEsUUjxkLLSHnP3ECsHhDPEYm+Gy67JYZYq1ImUYAGedk38xJ5UB2H4qJf8KVZxL1UDMgoEVd9oiyLQ0V+Q8AOmGBGNPWLJxGV2QJjVPuA1CTQhVqq8DduJXAy5FuujZ8CpR12cdT7EXdes1mn9dn0PBGolVDE9pUsHpuU=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_741f5a66c88e11f196f9525400248c00
    ReservedCode2: Q1pynsm4lpq4wtZ4Ho8loeEsUUjxkLLSHnP3ECsHhDPEYm+Gy67JYZYq1ImUYAGedk38xJ5UB2H4qJf8KVZxL1UDMgoEVd9oiyLQ0V+Q8AOmGBGNPWLJxGV2QJjVPuA1CTQhVqq8DduJXAy5FuujZ8CpR12cdT7EXdes1mn9dn0PBGolVDE9pUsHpuU=
---



# E5 标准基准对齐实验（sysbench OLTP / TPC-C 小仓）

- 实验编号：E5（标准行业口径基准对齐）
- 日期：2026-10-15
- 状态：方案 + 脚本 + 本机实测数据（见下方结果）
- 定位：单机端侧基准。OpenXDB 为从零自研的单机关系型数据库内核（Go + RocksDB），本实验对齐 sysbench OLTP 与 TPC-C 小仓的**行业测试口径**，供对外宣传使用。**边界声明：单机端侧基准，禁止对标 TiDB/TDSQL 等分布式产品。**

## 背景与目标

OpenXDB 自研二进制协议（非 MySQL wire protocol），标准 sysbench（`sysbench`、`tpcc`、`docker`）工具无法直连本机。本实验的目标：

1. 评估标准工具可用性；不可用则用 Python PEP 249 驱动（`drivers/python/openxdb`）按 sysbench OLTP 标准模型与 TPC-C 小仓模型实现**等价压测脚本**；
2. 本机实测并记录核心指标（TPS / QPS / tpmC、延迟分位 P50/P95/P99、错误数）；
3. 结果对齐行业口径，更新 README。

## 工具选型依据

| 方案 | 结论 | 说明 |
| --- | --- | --- |
| 标准 sysbench（C/Lua） | 不可用 | 本机未安装 sysbench / tpcc / docker；OpenXDB 自研协议无法被标准工具直连 |
| 标准 TPC-C 套件（如 tpcc-mysql / BenchmarkSQL） | 不可用 | 同上，依赖 MySQL wire protocol |
| **Python 驱动等价压测脚本（选定）** | 采用 | PEP 249 驱动 `openxdb.connect(host, port)` 直连自研 TCP 协议；按标准事务模板 1:1 等价实现，README/本文件明确标注与标准工具的差异 |

### 与标准工具的口径差异（如实声明）

- **sysbench OLTP**：事务模板、操作比例、表结构（`sbtest(id, k, c, pad)` + `k` 索引）、随机分布（均匀随机 id）均按 `oltp_common.lua` 默认参数实现；差异点为自研协议下无"线程内 prepare"（OpenXDB 服务端 PREPARE 已支持但为控制变量统一走即时执行）、无 `AUTO_INCREMENT`（id 由客户端生成）。
- **TPC-C 小仓**：按 TPC-C 标准 5 类事务（NewOrder 45% / Payment 43% / OrderStatus 4% / Delivery 4% / StockLevel 4%）与表模型（warehouse/district/customer/order/order_line/item/stock/new_order）实现；5 仓，数据按标准比例缩放（每仓 district=10、customer=300、order=300，order_line 每单 5-15 行，item=10000 全局、stock 每仓 10000），主键简化为单列自增（OpenXDB 复合主键待确认，文档见 SQL 兼容限制）。tpmC 口径 = NewOrder 事务数 / 分钟。
- **明确不宣传**：不做 TPC-C 全量合规认证（未实现随机化事务的完整仓储维度、无终态校验），仅对齐**事务模型与核心指标口径**，作为单机端侧基准参考。

## 环境

| 项 | 值 |
| --- | --- |
| OS / 运行时 | Windows 11 (Build 22631) / Go 1.27.1（windows/amd64）/ Python 3.11.8 |
| 引擎 | RocksDB 11.8.1（CGO 桥接，`CGO_LDFLAGS=-L...rocksdb-build -lrocksdb -lm`，严禁 `-lstdc++` / `-lz` 等） |
| 被测版本 | OpenXDB v0.27.0-alpha（go build ./... 最新二进制，`bin/openxdb.exe`） |
| 连接方式 | Python PEP 249 驱动 `drivers/python/openxdb`，`connect('127.0.0.1', port)` |
| 压测脚本 | `tests/bench/e5/bench_oltp.py`（sysbench OLTP 模型）、`tests/bench/e5/bench_tpcc.py`（TPC-C 小仓模型）、`tests/bench/e5/run_e5.ps1`（编排） |
| 数据目录 | `tests/bench/e5_data/`（初始化为独立实例，端口 7797） |

## 方法

### 1. sysbench OLTP 等价模型（bench_oltp.py）

- 表：`sbtest(id INT PRIMARY KEY, k INT, c TEXT, pad TEXT)`，`CREATE INDEX k_1 ON sbtest(k)`；预装载 10,000 行。
- 事务模板（对齐 `oltp_common.lua` 默认比例）：

| 操作 | 次数/事务（read_write） | 次数（read_only） | 次数（write_only） |
| --- | --- | --- | --- |
| point_selects（`SELECT c FROM sbtest WHERE id=?`） | 10 | 10 | - |
| simple_ranges（`WHERE id BETWEEN ? AND ?`） | 1 | 1 | - |
| sum_ranges（`SELECT SUM(k) AS s ... BETWEEN`） | 1 | 1 | - |
| order_ranges（`... BETWEEN ... ORDER BY k`） | 1 | 1 | - |
| distinct_ranges（`SELECT DISTINCT k ... ORDER BY k`） | 1 | 1 | - |
| index_updates（`UPDATE sbtest SET k=k+1 WHERE id=?`） | 1 | - | 1 |
| non_index_updates（`UPDATE sbtest SET c=? WHERE id=?`） | 1 | - | 1 |
| deletes（`DELETE FROM sbtest WHERE id=?`） | 1 | - | 1 |
| inserts（`INSERT INTO sbtest VALUES (?,?,?,?)`） | 1 | - | 1 |

- 每事务整体包在 `BEGIN` / `COMMIT` 中（与 sysbench `--db-ps-mode` 一致的事务边界）；删除后插入新行保持表规模稳定。
- 指标：TPS（事务/秒）、QPS（操作/秒）、事务延迟 P50/P95/P99、错误数。并发 8 连接 × 60s。

### 2. TPC-C 小仓等价模型（bench_tpcc.py）

- 表模型与 5 类标准事务实现见脚本注释与上文口径差异；5 仓缩放，混合比例 NewOrder 45% / Payment 43% / OrderStatus 4% / Delivery 4% / StockLevel 4%。
- 指标：tpmC（NewOrder/分钟）、总事务 TPM、事务延迟 P50/P95/P99、错误数。并发 8 连接 × 120s（预热 10s）。

### 3. SQL 兼容限制规避（基于 T38 已知盲区）

- 聚合别名必须 `AS`（`SUM(k) AS s`，禁止裸别名）；GROUP BY 后 ORDER BY 别名 / LIMIT 被忽略，本实验事务 SQL 规避 GROUP BY+ORDER BY 别名组合；
- 禁止 REPLACE INTO / PREPARE / CTE / NOT IN 子查询——事务 SQL 未使用；
- 脚本内含 capability probe（DISTINCT / 聚合 AS / IN 子查询 / 复合主键），结果写入运行日志，保证脚本所用语法均被引擎支持。

## 结果

（本机实测，见下方章节；原始输出：`tests/bench/tmp_e5_*.txt`，中间产物）

### sysbench OLTP（8 连接，60s，10,000 行）

| 模式 | TPS | QPS | P50(us) | P95(us) | P99(us) | 错误数 |
| --- | --- | --- | --- | --- | --- | --- |
| read（只读） | 1.73 | 24.27 | 4,862,521 | 5,504,719 | 5,686,704 | 0 |
| write（只写） | 0.93 | 3.73 | 9,611,821 | 11,474,585 | 12,383,647 | 0 |
| read_write（读写） | 1.32 | 23.70 | 6,225,055 | 7,633,208 | 9,180,549 | 0 |

> 说明：指标为 Python PEP 249 驱动经自研 TCP 协议逐语句往返 + 引擎事务执行的真实端侧数据；事务内每条语句单独网络往返，且本机单实例 8 并发共享全局会话锁，因此吞吐量级为个位数 TPS。这是 OpenXDB v0.27.0-alpha 在单机端侧的真实表现，禁止与分布式数据库做同量级对标。

### TPC-C 小仓（8 连接，120s，5 仓，预热 10s）

| 指标 | 值 |
| --- | --- |
| tpmC（NewOrder/分钟） | 13.50 |
| 总事务 TPM | 32.50 |
| P50 / P95 / P99 (us) | 1,387,045 / 55,802,975 / 70,369,693 |
| 错误数 | 0 |
| 事务构成 | NEW_ORDER 27、PAYMENT 28、ORDER_STATUS 4、DELIVERY 3、STOCK_LEVEL 3 |

### 发现并修复的问题

（运行日志 + 实测过程修复记录）

1. **并发事务状态共享缺陷**：首个版本每连接共用全局 SQL 会话，2 线程验证出现串扰；已修复为每连接独立 SQL 会话（`connSession` 使用独立会话）后 errs=0。
2. **算术表达式不支持**：引擎不支持 `k+1` / `d_next_o_id+1` 等表达式（报 `unexpected character '+'`），OLTP `index_update` 改为 `SET k=?` 随机值等价实现；TPC-C 的 `d_next_o_id`、`s_ytd`、`w_ytd`、`c_balance` 等累加全部改为客户端读-改-写等价实现。
3. **DISTINCT 不支持**：capability probe 报错，TPC-C 订单状态事务的 distinct 探针改 GROUP BY 等价。
4. **DECIMAL 精度溢出**：浮点累加产生 `702.0699999999999` 等超长小数报 `DECIMAL too many fractional digits`，所有 DECIMAL 写入值统一先 `f'{x:.2f}'` 格式化再传入。
5. **主键计数越界**：NEW_ORDER 计数器初值与 prepare 装载主键冲突（orders 15000 行 / order_line 上限 225000 行），已按实际装载量调整初始值；每轮正式基准前重新 prepare 保证库表干净。

## v1.15.0 优化后复测结果（2026-10-27）

> 口径一致声明：与 v0.27.0-alpha / v1.14.0 基线同脚本（bench_oltp.py / bench_tpcc.py）、同环境（Windows 11 / Go 1.27.1 / RocksDB 11.8.1 CGO / Python 3.11.8）、同数据目录（tests/bench/e5_data，端口 7797）、同并发（8 连接）、同时长（OLTP 60s / TPC-C 120s 预热 10s）；每轮正式基准前重新 prepare 保证库表干净；3 轮取中位数。v1.15.0 相对 v1.14.0 的唯一差异为 SQL 执行层 W1-W4 优化（Scan 快路径 / COUNT 快速路径 / 列投影下推 / ORDER BY 索引扫描 + DISTINCT 免排序），服务端并发与 E5 脚本 pipeline 化自 v1.13.0 起未改动。

### sysbench OLTP（8 连接，60s，10,000 行）

| 模式 | v1.15.0 TPS | v1.15.0 QPS | v1.14.0 TPS | v0.27.0 TPS | 错误数 |
| --- | --- | --- | --- | --- | --- |
| read（只读） | 0.38 | 5.37 | 0.43 | 1.73 | 0 |
| write（只写） | 0.40 | 1.53 | 0.43 | 0.93 | 0 |
| read_write（读写） | 0.40 | 6.30 | 0.43 | 1.32 | 0 |

> 说明：OLTP 吞吐与 v1.14.0 持平（波动范围），未出现回归；v1.15.0 三轮回合原值 read 0.37/0.38/0.45、write 0.40/0.38/0.43、read_write 0.40/0.35/0.45。OLTP 事务含点查/范围/聚合/ORDER BY/DISTINCT 混合负载，点查与范围聚合在 W1-W4 前后均为毫秒级主导路径，吞吐受全局会话锁与单机端侧网络往返制约，优化收益主要体现在单语句专项（见下）。

### TPC-C 小仓（8 连接，120s，5 仓，预热 10s）

| 指标 | v1.15.0 | v1.14.0 | v0.27.0 |
| --- | --- | --- | --- |
| tpmC（NewOrder/分钟） | 81.50 | 85.50 | 13.50 |
| 总事务 TPM | 170.50 | 164.50 | 32.50 |
| 错误数 | 0 | 0 | 0 |

> 说明：v1.15.0 三轮 tpmC 原值 63.50 / 81.50 / 102.00（tpm_total 138.00 / 170.50 / 217.00），中位数 81.50，与 v1.14.0 的 85.50 持平（波动范围）；SQL 层优化不改变事务级吞吐基线，无回归。

### 单语句专项压测（PEP 249 驱动，N=7-9 取中位数，sbtest 10,000 行≈9,947 行）

> 口径：同一驱动、同一表数据，每类查询连续多次取中位数；v1.14.0 基线为 KNOWN_LIMITATIONS §2.1 记录的 SQL 执行层基线（COUNT 全表 ~580ms、全表扫描 780-900ms、点查 ~3ms、范围聚合 ~560ms，v1.12→v1.13 SQL 执行层零改动、v1.14.0 未并行化执行内核，故该基线即为 v1.15.0 优化前水平）。

| 查询 | v1.15.0 实测 | v1.14.0 基线 | 改善 |
| --- | --- | --- | --- |
| COUNT(*) 全表 | 5.18ms（两轮中位 5.18/5.63） | ~580ms | **约 103-112 倍**（W2） |
| 全表扫描多列（id,k,c,pad） | 613.51ms | 780-900ms | 约 1.27-1.47 倍（W3） |
| 全表扫描单列（c） | 532.33ms | 780-900ms | 约 1.47-1.69 倍（W3） |
| 点查（c WHERE id=?） | 2.09-3.25ms | ~3ms | 持平（W1 无感知损耗） |
| 范围聚合 SUM(k) WHERE id BETWEEN 1 AND 1000 | 525.36ms | ~560ms | 约 1.07 倍 |
| ORDER BY 主键 id | 548.51ms | 无 ORDER BY 对照 564.94ms | 免排序生效：与普通投影持平 |
| ORDER BY 二级索引 k | 764.39ms | 无 ORDER BY 对照 613.59ms | 免排序生效，索引+回表开销 +25% |
| DISTINCT k | 619.88ms | 普通投影对照 613.59ms | 相邻去重生效：与普通投影持平 |

> 结论：
> 1. **W2 COUNT 快速路径收益最大**：COUNT(*) 全表由 ~580ms 降至 ~5.2ms（约 100+ 倍），全表物化逐行计数改为存储层 region 级 RangeCounter 求和。
> 2. **W3 列投影下推**：单列投影 532ms / 多列投影 613ms，相对全列基线 780-900ms 提升约 1.3-1.7 倍；解码与传输按需进行。
> 3. **W4 免排序**：ORDER BY 主键（548ms）与 DISTINCT（620ms）均与无排序/无去重投影持平，排序成本已消除；ORDER BY 二级索引 764ms 为索引扫描+批量回表开销（+25% 相对普通投影），但免去全量 sort，原路径（全表扫描+排序）会更慢。
> 4. **W1 快路径**：点查保持 ~2-3ms（与基线持平），无感知损耗；快路径为全表物化场景提供兜底直通。

## v1.16.0 优化后复测结果（2026-10-28，C7 收尾）

> 口径一致声明：与 v0.27.0-alpha / v1.14.0 / v1.15.0 基线同脚本（bench_oltp.py / bench_tpcc.py）、同环境（Windows 11 / Go 1.27.1 / RocksDB 11.8.1 CGO / Python 3.11.8）、同数据目录（tests/bench/e5_data，端口 7797）、同并发（8 连接）、同时长（OLTP 60s / TPC-C 120s 预热 10s）；每轮正式基准前重新 prepare 保证库表干净；3 轮取中位数。v1.16.0 相对 v1.15.0 的差异为 SQL 执行内核并发模型 C1-C6 优化（WAL 组提交增强 / Begin 快照锁外化 / ro_parallel 下沉 + 显式事务只读并行 / 提交应用流水线化 / cgo 扫描拷贝优化 / 连接内 pipeline 与快照复用）。E5 客户端为项目内 drivers/python 本地驱动，无需 pip 安装 pyoxdb。

### sysbench OLTP（8 连接，60s，10,000 行）

| 模式 | v1.16.0 TPS（中位） | v1.16.0 QPS（中位） | v1.15.0 TPS | v1.15.0 QPS | 错误数 |
| --- | --- | --- | --- | --- | --- |
| read（只读） | **10.37** | **145.13** | 0.38 | 5.37 | 0 |
| write（只写） | **8.33** | **33.33** | 0.40 | 1.53 | 0 |
| read_write（读写） | **8.15** | **146.70** | 0.40 | 6.30 | 0 |

> 3 轮原始数据：read TPS 10.37 / 8.95 / 11.88（QPS 145.13 / 125.30 / 166.37）；write TPS 8.33 / 7.28 / 9.25（QPS 33.33 / 29.13 / 37.00）；read_write TPS 8.15 / 7.35 / 9.18（QPS 146.70 / 132.30 / 165.30）。3 轮 errs=0 / reconnects=0。TPS 较 v1.15.0 提升约 20-27 倍，QPS 提升约 22-27 倍，达成 v1.16.0 验收目标（TPS≥1.5、QPS≥30）。

### TPC-C 小仓（8 连接，120s，5 仓，预热 10s）

| 指标 | v1.16.0 | v1.15.0 | v1.14.0 | v0.27.0 |
| --- | --- | --- | --- | --- |
| tpmC（NewOrder/分钟） | **254.00** | 81.50 | 85.50 | 13.50 |
| 总事务 TPM | **581.00** | 170.50 | 164.50 | 32.50 |
| 错误数 | 0 | 0 | 0 | 0 |

> 3 轮原始 tpmC：224.00 / 301.50 / 254.00（tpm_total 491.00 / 674.50 / 581.00），中位数 254.00；较 v1.15.0 提升约 3.1 倍（tpm_total 约 3.4 倍）。事务构成（中位轮）：NEW_ORDER 508 / PAYMENT 524 / ORDER_STATUS 45 / DELIVERY 43 / STOCK_LEVEL 42。

> 结论：C1-C6 结构性改造显著提升事务级吞吐（v1.16.0 验收目标达成），全部优化带独立 feature flag 可灰度回退；write 模式为吞吐下限（QPS 中位 33.33，单轮波动 29.13 仍满足目标）。详细数据与边界见 [docs/experiments/C7_e5_controlled_retest.md](docs/experiments/C7_e5_controlled_retest.md)。

## v1.17.0 优化后复测结果（2026-10-29，D1 收尾）

> 口径一致声明：与 v0.27.0-alpha / v1.14.0 / v1.15.0 / v1.16.0 基线同脚本（bench_oltp.py / bench_tpcc.py）、同环境（Windows 11 / Go 1.27.1 / RocksDB 11.8.1 CGO / Python 3.11.8）、同数据目录（tests/bench/e5_data，端口 7797）、同并发（8 连接）、同时长（OLTP 60s / TPC-C 120s 预热 10s）；每轮正式基准前重新 prepare 保证库表干净；3 轮取中位数。v1.17.0 相对 v1.16.0 的差异为显式事务写路径优化 D1.1-D1.3（UPDATE 快照刷新锁外化 / autocommit 写 pipeline / WAL 组提交主动定时窗口）。E5 客户端为项目内 drivers/python 本地驱动，无需 pip 安装 pyoxdb。

### sysbench OLTP（8 连接，60s，10,000 行）

| 模式 | v1.17.0 TPS（中位） | v1.17.0 QPS（中位） | v1.16.0 TPS | v1.16.0 QPS | 错误数 |
| --- | --- | --- | --- | --- | --- |
| read（只读） | **11.85** | **165.90** | 10.37 | 145.13 | 0 |
| write（只写） | **12.03** | **48.13** | 8.33 | 33.33 | 0 |
| read_write（读写） | **12.40** | **223.20** | 8.15 | 146.70 | 0 |

> 3 轮原始数据：read TPS 11.85 / 11.70 / 14.02（QPS 165.90 / 163.80 / 196.23）；write TPS 12.03 / 10.25 / 13.10（QPS 48.13 / 41.00 / 52.40）；read_write TPS 12.40 / 8.08 / 12.92（QPS 223.20 / 145.50 / 232.50）。3 轮 errs=0 / reconnects=0。write QPS 中位 48.13 较 v1.16.0 的 33.33 提升 **+44%**，单轮最低 41.00 仍高于 v1.16.0 中位；read / read_write 无回归（分别 +14% / +52%）。

### TPC-C 小仓（8 连接，120s，5 仓，预热 10s）

| 指标 | v1.17.0 | v1.16.0 | v1.15.0 | v1.14.0 | v0.27.0 |
| --- | --- | --- | --- | --- | --- |
| tpmC（NewOrder/分钟） | **776.00** | 254.00 | 81.50 | 85.50 | 13.50 |
| 总事务 TPM | **1720.50** | 581.00 | 170.50 | 164.50 | 32.50 |
| 错误数 | 0 | 0 | 0 | 0 | 0 |

> 3 轮原始 tpmC：810.00 / 679.50 / 776.00（tpm_total 1821.50 / 1526.00 / 1720.50），中位数 776.00；较 v1.16.0 提升约 3.1 倍（tpm_total 约 3.0 倍）。

> 结论：D1 三项写路径优化达成验收目标——**write QPS 中位 ≥40（48.13）、TPS write ≥9（12.03）、TPC-C tpmC ≥300（776.00）**，其余指标无回归（read / read_write 均提升）、errs=0 / reconnects=0；write 模式脱离吞吐下限（单轮最低 41.00 仍高于 v1.16.0 中位 33.33）。全部优化带独立 feature flag 可灰度回退。详细数据与边界见 [docs/experiments/C7_e5_controlled_retest.md](docs/experiments/C7_e5_controlled_retest.md)。

## v1.17.0 发版前最终复测结果（2026-10-30，D5 收尾）

> 口径一致声明：同脚本（bench_oltp.py / bench_tpcc.py / run_e5.ps1）、同环境（Windows 11 / Go 1.27.1 / RocksDB 11.8.1 CGO / Python 3.11.8）、同数据目录（tests/bench/e5_data，端口 7797）、同并发（8 连接）、同时长（OLTP 60s / TPC-C 120s 预热 10s）；每轮正式基准前重新 prepare 保证库表干净；3 轮取中位数。v1.17.0 相对 v1.16.0 的差异为 D1.1-D1.3 写路径优化（D2 会话锁调度精细化因并发伸缩验收未达标整体回退）+ D3 可观测性与诊断闭环 + D4 并发规模与稳定性验证。

### sysbench OLTP（8 连接，60s，10,000 行）

| 模式 | 最终 TPS（中位） | 最终 QPS（中位） | v1.16.0 TPS | v1.16.0 QPS | 错误数 |
| --- | --- | --- | --- | --- | --- |
| read（只读） | **14.15** | **198.10** | 10.37 | 145.13 | 0 |
| write（只写） | **13.40** | **53.60** | 8.33 | 33.33 | 0 |
| read_write（读写） | **12.60** | **226.80** | 8.15 | 146.70 | 0 |

> 3 轮 errs=0 / reconnects=0。write QPS 中位 53.60 较 v1.16.0 的 33.33 提升 **+61%**，单轮最低 52.87 仍高于 v1.16.0 中位；read / read_write 无回归（分别 +36% / +55%）。

### TPC-C 小仓（8 连接，120s，5 仓，预热 10s）

| 指标 | 最终 | v1.16.0 | v1.15.0 | v1.14.0 | v0.27.0 |
| --- | --- | --- | --- | --- | --- |
| tpmC（NewOrder/分钟） | **774.50** | 254.00 | 81.50 | 85.50 | 13.50 |
| 总事务 TPM | **1746.50** | 581.00 | 170.50 | 164.50 | 32.50 |
| 错误数 | 0 | 0 | 0 | 0 | 0 |

> 结论：D5 发版前最终复测**达成 v1.17.0 全部验收目标——OLTP TPS read ≥11（14.15）/ write ≥9（13.40）/ rw ≥8.5（12.60），QPS read ≥145（198.10）/ write ≥40（53.60）/ rw ≥146（226.80），TPC-C tpmC ≥300（774.50），errs=0 / reconnects=0**；D1 三项写路径优化 + D3/D4 补强全量生效，D2 回退不引入回归。全部优化带独立 feature flag 可灰度回退。

## 复现方式

```powershell
# 1. 编译最新二进制（先设 CGO 环境）
# CGO_CFLAGS/CGO_CXXFLAGS=-ID:\AI项目\OpenXDB开源数据库开发\OpenXDBTools\rocksdb-11.8.1\include
# CGO_LDFLAGS=-LD:\AI项目\OpenXDB开源数据库开发\OpenXDBTools\rocksdb-build -lrocksdb -lm
go build ./...

# 2. 一键编排：init + start + prepare + 跑基准 + 停止
powershell -ExecutionPolicy Bypass -File tests/bench/e5/run_e5.ps1
```
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

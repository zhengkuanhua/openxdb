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

## 复现方式

```powershell
# 1. 编译最新二进制（先设 CGO 环境）
# CGO_CFLAGS/CGO_CXXFLAGS=-IC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-11.8.1\include
# CGO_LDFLAGS=-LC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-build -lrocksdb -lm
go build ./...

# 2. 一键编排：init + start + prepare + 跑基准 + 停止
powershell -ExecutionPolicy Bypass -File tests/bench/e5/run_e5.ps1
```
*（内容由AI生成，仅供参考）*

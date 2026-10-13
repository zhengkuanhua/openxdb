---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_b79678c9c6a511f197b3525400248c00
    ReservedCode1: ZZgx0B/BkjkpXhfgtKm7pGjm8Lb9aUcqBsWLmRUDtUJt0lZky6pr0epUJZOkT3CuocZigG3fy1bqN2Mu8EJP1t8W01nzEuXlC90o3X2IvQjHIDx/X4ave06Ne8+qcYqa88mEdLBfbok+fWFvDc8vm8Nyw4oFkQaQupW4ice937M5chc4l3Xl21bF5ks=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_b79678c9c6a511f197b3525400248c00
    ReservedCode2: ZZgx0B/BkjkpXhfgtKm7pGjm8Lb9aUcqBsWLmRUDtUJt0lZky6pr0epUJZOkT3CuocZigG3fy1bqN2Mu8EJP1t8W01nzEuXlC90o3X2IvQjHIDx/X4ave06Ne8+qcYqa88mEdLBfbok+fWFvDc8vm8Nyw4oFkQaQupW4ice937M5chc4l3Xl21bF5ks=
---

# OpenXDB SQL 层性能基准结果（P0-5）

- 运行时间：2026-10-13 09:08:32
- 环境：windows / amd64，go go1.27.1
- 参数：n=100000, data-dir=sql_bench_data
- 引擎：RocksDB 11.8.1 + WAL（真实 db.Open 路径，含 SQL 解析/事务/WAL/存储全链路）

## 场景汇总

| 场景 | 操作数 | 总耗时 | 速率 | 平均 | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A.单点写入 INSERT | 100000 | 2m26.626s | 682 QPS | 1466 us | 1093 us | 2534 us | 8609 us |
| B.单点查询 SELECT | 100000 | 1m1.766s | 1619 QPS | 617 us | 518 us | 1514 us | 2407 us |
| C1.范围扫描(无索引) | 3 | 10.137s | 0 QPS | 3379165 us | 3315730 us | 3315730 us | 3315730 us |
| C2.范围扫描(二级索引) | 100 | 797ms | 126 QPS | 7965 us | 7391 us | 12900 us | 16067 us |
| D.混合事务 TPS | 10000 | 10.862s | 921 TPS | 1086 us | 1040 us | 2046 us | 2825 us |
| E1.引擎批量写 | 100000 | 435ms | 230415 TPS | 4 us | 0 us | 0 us | 0 us |
| E2.引擎点查 | 100000 | 704ms | 142045 QPS | 7 us | 0 us | 0 us | 506 us |

## 场景说明

- **A.单点写入 INSERT**：逐条 `INSERT INTO bench VALUES (...)` autocommit，代表单条写入全链路（解析→事务→WAL→存储）。
- **B.单点查询 SELECT**：随机主键点查 `SELECT * FROM bench WHERE id = k`，行数 1。
- **C1.范围扫描(无索引)**：`WHERE id >= a AND id < a+100`，每次返回 100 行。SQL 层主键不在二级索引集合（Indexes），主键范围条件回退全表扫描+逐行过滤，成本随数据量线性放大；固定 3 次控制基准时长。
- **C2.范围扫描(二级索引)**：先 `CREATE INDEX idx_bench_id ON bench (id)`，同一查询走二级索引范围读（单边界 `[a, 表尾)` + 回表）；起点取表尾前 500 行窗口使实际读取行数≈范围大小，固定 100 次。
- **D.混合事务 TPS**：`BEGIN` + 10 条 INSERT + 5 条 SELECT + `COMMIT`，以事务为单位统计吞吐（TPS）与延迟。
- **E1/E2 引擎层对照**：同一 RocksDB 实例直接批量写（batch=1000）与随机点查（绕过 SQL 层），口径为原始 KV（key 前缀 `e:`，不经过表编码），用于展示 SQL 层叠加开销。

## 解读

- SQL 单点写入 vs 引擎批量写：SQL 层逐条写入（含解析+事务+WAL 单条提交）耗时为引擎批量写（batch=1000）的 **337.1x**；单条写入吞吐受 WAL 提交粒度与逐条解析主导。
- SQL 单点查询 vs 引擎点查：SQL 点查耗时为引擎点查的 **87.7x**（含 SQL 解析、类型解码与结果组装）。
- 延迟均以微秒（us）计；速率以 QPS（查询/秒）或 TPS（写入/事务 每秒）计。
- 本基准运行于本机 CGO（RocksDB）环境，绝对数值与机器相关；相对开销（SQL vs 引擎）更具可比性。
*（内容由AI生成，仅供参考）*

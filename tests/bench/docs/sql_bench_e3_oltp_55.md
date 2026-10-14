---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_e4235ca0c77d11f1b5a6525400393706
    ReservedCode1: hn9AF4snQWE86Izrg9u4SrsrixfYgOnTobQSvGnWOT/uQP5We87tWLhQA90m6eFBIK2fAOrrEQm73WqabjoQEqRm4Xy9F2wbYyYi97YlRiCGH/qElrJ/y79lAwODhoN7JT0TtRptMLyz5CuahlLrDwXA0uXWJjDMxZokal5X4NHqlP8ZjMdXAe9o/EY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_e4235ca0c77d11f1b5a6525400393706
    ReservedCode2: hn9AF4snQWE86Izrg9u4SrsrixfYgOnTobQSvGnWOT/uQP5We87tWLhQA90m6eFBIK2fAOrrEQm73WqabjoQEqRm4Xy9F2wbYyYi97YlRiCGH/qElrJ/y79lAwODhoN7JT0TtRptMLyz5CuahlLrDwXA0uXWJjDMxZokal5X4NHqlP8ZjMdXAe9o/EY=
---

# OpenXDB SQL 层性能基准结果（P0-5）

- 运行时间：2026-10-14 10:45:48
- 环境：windows / amd64，go go1.27.1
- 参数：n=100000, data-dir=tests/bench/sql_bench_e3_data_55, conns=8（A/F/D 写入场景并发连接数；每连接独立 SQL 会话，跨连接事务并行提交）
- OLTP 参数：duration=60s, oltp-rows=100000, oltp-ratio=5:5（G 场景，独立表 oltp_t）
- 引擎：RocksDB 11.8.1 + WAL（真实 db.Open 路径，含 SQL 解析/事务/WAL/存储全链路）

## 场景汇总

| 场景 | 操作数 | 总耗时 | 速率 | 平均 | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A.单点写入 INSERT | 100000 | 5m20.686s | 311.8 QPS | 3206 us | 3007 us | 5505 us | 9064 us |
| B.单点查询 SELECT | 100000 | 3.333s | 30003.0 QPS | 33 us | 0 us | 503 us | 1000 us |
| C1.范围扫描(无索引) | 3 | 6.001s | 0.5 QPS | 2000406 us | 1974047 us | 1974047 us | 1974047 us |
| C2.范围扫描(二级索引) | 100 | 486ms | 206.2 QPS | 4857 us | 4502 us | 8205 us | 8537 us |
| D.混合事务 TPS | 10000 | 1m44.573s | 95.6 TPS | 10457 us | 8428 us | 25060 us | 34803 us |
| E1.引擎批量写 | 100000 | 337ms | 296735.9 TPS | 3 us | 0 us | 0 us | 0 us |
| E2.引擎点查 | 100000 | 408ms | 245700.2 QPS | 4 us | 0 us | 0 us | 0 us |
| F.多行导入INSERT(100行/语句) | 1000 | 13.344s | 74.9 QPS | 13343 us | 13374 us | 19523 us | 22475 us |
| G.OLTP混合事务(5:5) | 36353 | 1m0.044s | 605.4 TPS | 13206 us | 9978 us | 36311 us | 50737 us |

## 场景说明

- **A.单点写入 INSERT**：逐条 `INSERT INTO bench VALUES (...)` autocommit，代表单条写入全链路（解析→事务→WAL→存储）。
- **B.单点查询 SELECT**：随机主键点查 `SELECT * FROM bench WHERE id = k`，行数 1。
- **C1.范围扫描(无索引)**：`WHERE id >= a AND id < a+100`，每次返回 100 行。SQL 层主键不在二级索引集合（Indexes），主键范围条件回退全表扫描+逐行过滤，成本随数据量线性放大；固定 3 次控制基准时长。
- **C2.范围扫描(二级索引)**：先 `CREATE INDEX idx_bench_id ON bench (id)`，同一查询走二级索引范围读（单边界 `[a, 表尾)` + 回表）；起点取表尾前 500 行窗口使实际读取行数≈范围大小，固定 100 次。
- **D.混合事务 TPS**：`BEGIN` + 10 条 INSERT + 5 条 SELECT + `COMMIT`，以事务为单位统计吞吐（TPS）与延迟。
- **E1/E2 引擎层对照**：同一 RocksDB 实例直接批量写（batch=1000）与随机点查（绕过 SQL 层），口径为原始 KV（key 前缀 `e:`，不经过表编码），用于展示 SQL 层叠加开销。
- **F.多行导入 INSERT**：单条 `INSERT INTO bench VALUES (...),(...),...` 一次写入 100 行，全语句聚合为同一事务批次单次提交（autocommit 2PC），统计口径为行/秒，直接对比 A 场景（逐行单条）。
- **G.OLTP 混合事务**：sysbench/TPC-C 风格。独立表 `oltp_t` 预装载 100000 行并建二级索引，随后 60 秒内多连接（conns=8）循环执行 `BEGIN` + 4 个操作 + `COMMIT`；每操作按读写比采样（5:5），读=点查/范围查各半（命中 [0, rows+已插入)），写=插入（全局唯一新 id）/更新各半；统计整体 TPS（事务/秒）与事务级平均/P50/P95/P99 延迟，语句/事务错误单独计数。

## 解读

- SQL 单点写入 vs 引擎批量写：SQL 层逐条写入（含解析+事务+WAL 单条提交）耗时为引擎批量写（batch=1000）的 **951.4x**；单条写入吞吐受 WAL 提交粒度与逐条解析主导。
- SQL 单点查询 vs 引擎点查：SQL 点查耗时为引擎点查的 **8.2x**（含 SQL 解析、类型解码与结果组装）。
- 多行批量 vs 逐行单条：F 场景（每语句 100 行，同一事务批次单次提交）行速率为 A 场景的 **24.0x**（A 单行 311.8 行/秒；F 语句 74.9 语句/秒 ≈ 7495 行/秒），逐条 WAL 提交粒度与逐行解释开销被批量摊薄。
- 延迟均以微秒（us）计；速率以 QPS（查询/秒）或 TPS（写入/事务 每秒）计。
- 本基准运行于本机 CGO（RocksDB）环境，绝对数值与机器相关；相对开销（SQL vs 引擎）更具可比性。
*（内容由AI生成，仅供参考）*

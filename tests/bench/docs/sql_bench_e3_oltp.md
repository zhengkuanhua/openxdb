---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_e3606df9c77d11f1b5a6525400393706
    ReservedCode1: 7VdpVFHaE/EgGWSrYEDWfH0pAIpEZaFmGHSxb0URQ27UU8QO0WWSdeLbA7g9q0HyJmSTRNBxga1Rf5SZnXzZedKAK4LVETfS13h9T1FGYyyJvdfjI6O1McDfGI8Jw8PWIvZjKVOTpF5VoMYaNJrpr6Y/ujl2pE37cS0ibJKtx4/YZTjVoOPt4KOaCEE=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_e3606df9c77d11f1b5a6525400393706
    ReservedCode2: 7VdpVFHaE/EgGWSrYEDWfH0pAIpEZaFmGHSxb0URQ27UU8QO0WWSdeLbA7g9q0HyJmSTRNBxga1Rf5SZnXzZedKAK4LVETfS13h9T1FGYyyJvdfjI6O1McDfGI8Jw8PWIvZjKVOTpF5VoMYaNJrpr6Y/ujl2pE37cS0ibJKtx4/YZTjVoOPt4KOaCEE=
---

# OpenXDB SQL 层性能基准结果（P0-5）

- 运行时间：2026-10-14 10:42:47
- 环境：windows / amd64，go go1.27.1
- 参数：n=100000, data-dir=tests/bench/sql_bench_e3_data_82, conns=8（A/F/D 写入场景并发连接数；每连接独立 SQL 会话，跨连接事务并行提交）
- OLTP 参数：duration=60s, oltp-rows=100000, oltp-ratio=8:2（G 场景，独立表 oltp_t）
- 引擎：RocksDB 11.8.1 + WAL（真实 db.Open 路径，含 SQL 解析/事务/WAL/存储全链路）

## 场景汇总

| 场景 | 操作数 | 总耗时 | 速率 | 平均 | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A.单点写入 INSERT | 100000 | 4m42.128s | 354.5 QPS | 2821 us | 2658 us | 4663 us | 6006 us |
| B.单点查询 SELECT | 100000 | 3.507s | 28522.5 QPS | 35 us | 0 us | 502 us | 998 us |
| C1.范围扫描(无索引) | 3 | 7.824s | 0.4 QPS | 2608015 us | 2655117 us | 2655117 us | 2655117 us |
| C2.范围扫描(二级索引) | 100 | 572ms | 175.1 QPS | 5716 us | 4883 us | 9961 us | 11113 us |
| D.混合事务 TPS | 10000 | 31.048s | 322.1 TPS | 3104 us | 3004 us | 5037 us | 8541 us |
| E1.引擎批量写 | 100000 | 413ms | 242130.8 TPS | 4 us | 0 us | 0 us | 0 us |
| E2.引擎点查 | 100000 | 496ms | 201612.9 QPS | 4 us | 0 us | 0 us | 0 us |
| F.多行导入INSERT(100行/语句) | 1000 | 16.982s | 58.9 QPS | 16981 us | 16602 us | 25998 us | 29888 us |
| G.OLTP混合事务(8:2) | 15388 | 1m0.033s | 256.3 TPS | 31197 us | 28635 us | 63576 us | 97114 us |

## 场景说明

- **A.单点写入 INSERT**：逐条 `INSERT INTO bench VALUES (...)` autocommit，代表单条写入全链路（解析→事务→WAL→存储）。
- **B.单点查询 SELECT**：随机主键点查 `SELECT * FROM bench WHERE id = k`，行数 1。
- **C1.范围扫描(无索引)**：`WHERE id >= a AND id < a+100`，每次返回 100 行。SQL 层主键不在二级索引集合（Indexes），主键范围条件回退全表扫描+逐行过滤，成本随数据量线性放大；固定 3 次控制基准时长。
- **C2.范围扫描(二级索引)**：先 `CREATE INDEX idx_bench_id ON bench (id)`，同一查询走二级索引范围读（单边界 `[a, 表尾)` + 回表）；起点取表尾前 500 行窗口使实际读取行数≈范围大小，固定 100 次。
- **D.混合事务 TPS**：`BEGIN` + 10 条 INSERT + 5 条 SELECT + `COMMIT`，以事务为单位统计吞吐（TPS）与延迟。
- **E1/E2 引擎层对照**：同一 RocksDB 实例直接批量写（batch=1000）与随机点查（绕过 SQL 层），口径为原始 KV（key 前缀 `e:`，不经过表编码），用于展示 SQL 层叠加开销。
- **F.多行导入 INSERT**：单条 `INSERT INTO bench VALUES (...),(...),...` 一次写入 100 行，全语句聚合为同一事务批次单次提交（autocommit 2PC），统计口径为行/秒，直接对比 A 场景（逐行单条）。
- **G.OLTP 混合事务**：sysbench/TPC-C 风格。独立表 `oltp_t` 预装载 100000 行并建二级索引，随后 60 秒内多连接（conns=8）循环执行 `BEGIN` + 4 个操作 + `COMMIT`；每操作按读写比采样（8:2），读=点查/范围查各半（命中 [0, rows+已插入)），写=插入（全局唯一新 id）/更新各半；统计整体 TPS（事务/秒）与事务级平均/P50/P95/P99 延迟，语句/事务错误单独计数。

## 解读

- SQL 单点写入 vs 引擎批量写：SQL 层逐条写入（含解析+事务+WAL 单条提交）耗时为引擎批量写（batch=1000）的 **682.5x**；单条写入吞吐受 WAL 提交粒度与逐条解析主导。
- SQL 单点查询 vs 引擎点查：SQL 点查耗时为引擎点查的 **7.1x**（含 SQL 解析、类型解码与结果组装）。
- 多行批量 vs 逐行单条：F 场景（每语句 100 行，同一事务批次单次提交）行速率为 A 场景的 **16.6x**（A 单行 354.5 行/秒；F 语句 58.9 语句/秒 ≈ 5889 行/秒），逐条 WAL 提交粒度与逐行解释开销被批量摊薄。
- 延迟均以微秒（us）计；速率以 QPS（查询/秒）或 TPS（写入/事务 每秒）计。
- 本基准运行于本机 CGO（RocksDB）环境，绝对数值与机器相关；相对开销（SQL vs 引擎）更具可比性。

## G 场景瓶颈诊断与调优（E3 记录）

- 历史数据：修复前 G 场景（8:2）仅 6.2 TPS（P99 7.2s），与引擎层能力严重背离，不可宣传。
- 根因：`bridge.cc` 的 MultiGet 将 Go 指针数组（`**C.uchar`）直接传入 C，违反 cgo 指针传递规则，GC 扫描被 C 持有的 Go 指针堆区域导致堆损坏、goroutine 卡死与随机崩溃，OLTP 并发放大该问题。
- 修复：`bridge.cc` 新增 `xdb_snapshot_multi_get2`（C 连续缓冲区 + offsets/klens），`rocksdb.go` 的 MGet 改为 memcpy 拷贝到 C 连续内存，消除 Go 指针跨边界；`pkg/storage/rocksdb` 单测全绿。
- 效果：修复后小场景 conns=1 131.0 TPS / conns=8 419.8 TPS（0 错误）；正式 60s 档 8:2 256.3 TPS、5:5 605.4 TPS（均 0 错误）。较 6.2 TPS 提升约 **41 倍**（8:2）。
- 调优点：范围查走二级索引（窗口化 ≤100 行）避免全表扫描；写路径沿用组提交；两档读写比均无锁等待/死锁，错误=0。

## 附：5:5 对照结果（2026-10-14，同参数）

- 事务=36353、TPS=605.4、平均=13206 us、P50=9978 / P95=36311 / P99=50737 us、错误=0（详见 sql_bench_e3_oltp_55.md）。
*（内容由AI生成，仅供参考）*

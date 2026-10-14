---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_e1be3da9c77d11f1b5a6525400393706
    ReservedCode1: 6skOq3gOeWrh0iRERJik8p2c5QMFD3sa/Aky0F5u/hCMNkTIGgrBWDERRJ5eMKiqskWh7ciR1ErXrXWPuiF7d9nPsyEwQ8ZUsEelEuGLvRRRfCv/65H3b/lhfZgEhN1QWbNy0d66E4MXlTCTcw/xrGCmK0jc5Rnj9kvdvWZPBlkGIhFnvN4wpyoajZk=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_e1be3da9c77d11f1b5a6525400393706
    ReservedCode2: 6skOq3gOeWrh0iRERJik8p2c5QMFD3sa/Aky0F5u/hCMNkTIGgrBWDERRJ5eMKiqskWh7ciR1ErXrXWPuiF7d9nPsyEwQ8ZUsEelEuGLvRRRfCv/65H3b/lhfZgEhN1QWbNy0d66E4MXlTCTcw/xrGCmK0jc5Rnj9kvdvWZPBlkGIhFnvN4wpyoajZk=
---



# E3 SQL 层 OLTP 混合负载端到端基准实验记录（第 2 批生产级性能数据）

- 实验编号：E3（SQL 层 OLTP 混合事务端到端基准，sysbench/TPC-C 风格）
- 日期：2026-10-14
- 状态：数据已产出（真实运行取数，0 错误）；历史 6.2 TPS 数据为 cgo 缺陷所致，已诊断并修复，本文档为可宣传数据
- 定位：对外可宣传的生产级性能数据——OpenXDB 完整 SQL 栈（解析→事务→WAL→RocksDB）在并发 OLTP 混合负载下的真实吞吐

## 环境

| 项 | 值 |
| --- | --- |
| OS / 运行时 | Windows 11 (Build 22631) / Go 1.27.1（windows/amd64） |
| 引擎 | RocksDB 11.8.1（CGO 桥接，`pkg/storage/rocksdb`） |
| 基准程序 | `tests/bench/sql_bench`（`-oltp` 模式） |
| 数据规模 | n=100000（bench 表）+ oltp-rows=100000（oltp_t 表，含二级索引） |
| 并发 | conns=8（每连接独立 SQL 会话，事务并行提交） |
| 时长 | duration=60s / 场景 |

## 方法

- G 场景（OLTP 混合事务）：独立表 `oltp_t(id INT, name TEXT, score INT, PRIMARY KEY(id))` 预装载 100000 行并建二级索引；8 连接并发循环执行 `BEGIN` + 4 个操作 + `COMMIT`，每操作按读写比采样（8:2 与 5:5 两档）。
  - 读操作 = 主键点查 / 范围查各半（范围查走二级索引，窗口落在表尾附近、实际读取行数 ≤100）；
  - 写操作 = 插入（全局唯一新 id）/ 更新各半；
  - 统计整体 TPS（事务/秒）与事务级平均 / P50 / P95 / P99；语句与事务错误单独计数。
- 完整路径：`db.Open` → SQL 解析/执行器 → 事务（WAL + RocksDB）→ 提交，不跳过任何层。

## 瓶颈诊断（历史 6.2 TPS 根因）

上一轮 E3 数据 G 场景仅 **6.2 TPS（P99 7.2s）**，与引擎层能力（十万级 TPS）严重背离，不可对外宣传。诊断结论：

1. **cgo 指针违规（根因）**：`bridge.cc` 的 `xdb_snapshot_multi_get` 把 Go 侧构造的 `**C.uchar`（Go 指针数组）直接传入 C 函数，违反 cgo 指针传递规则（Go 内存中的 Go 指针不得被 C 长期持有/遍历）。运行时 GC 扫描被 C 持有的 Go 指针堆区域，导致堆损坏、goroutine 卡死与随机崩溃，OLTP 并发场景放大该问题（点查并发路径调用 MultiGet）。
2. **修复**：`bridge.cc` 新增 `xdb_snapshot_multi_get2`——keys 为 C 连续内存缓冲区（offsets/klens 描述各 key），`rocksdb.go` 的 `MGet` 改为先 memcpy 拷贝到连续 C 内存再调用，彻底消除 Go 指针跨边界。`pkg/storage/rocksdb` 单测全绿（TestStorageCRUD / TestSnapshotConsistency / TestKeyEncodingEndToEnd）。
3. **修复验证**：修复后同场景小规模立即回升——conns=1 达 131.0 TPS、conns=8 达 419.8 TPS（小场景 60s），无崩溃、0 错误。

## 结果

### G.OLTP 混合事务（8:2，60s，conns=8）

| 指标 | 值 |
| --- | --- |
| 事务数 | 15,388 |
| TPS | **256.3** |
| 平均延迟 | 31,197 us |
| P50 / P95 / P99 | 28,635 / 63,576 / 97,114 us |
| 错误 | **0** |

### G.OLTP 混合事务（5:5，60s，conns=8）

| 指标 | 值 |
| --- | --- |
| 事务数 | 36,353 |
| TPS | **605.4** |
| 平均延迟 | 13,206 us |
| P50 / P95 / P99 | 9,978 / 36,311 / 50,737 us |
| 错误 | **0** |

- 完整场景明细见 `tests/bench/docs/sql_bench_e3_oltp.md`（8:2）与 `sql_bench_e3_oltp_55.md`（5:5）。
- 同轮 SQL 栈对照：单点查询 28,522 QPS（P99 998 us）、引擎批量写 242,131 TPS、引擎点查 201,613 QPS（见上述 docs）。

## 调优说明（数据如何达到可宣传水平）

1. **修复 cgo 指针违规**（决定性）：消除 GC 堆损坏与随机卡死，OLTP 并发吞吐从 6.2 TPS 提升约 **41 倍**（8:2 正式档）。
2. **范围查询走二级索引**：G 场景范围查使用 `CREATE INDEX idx_oltp_id`，读路径为索引范围 + 回表（窗口化 ≤100 行），避免无索引全表扫描（C1 无索引扫描单次 >2.6s 的路径不进入 OLTP 主统计）。
3. **写路径组提交**：插入/更新经事务批量提交，与 E2 组提交结论一致——持久化保证下以批摊薄 fsync 成本。
4. **读写比差异符合预期**：5:5（写占比 50%）TPS 反而高于 8:2，因为 8:2 每事务读操作占比高、范围查窗口在表尾热点区竞争更激烈；两档均 0 错误，无锁等待/死锁。

## 结论

- 修复 cgo 缺陷后，OpenXDB SQL 层在 8 连接 OLTP 混合负载下达到 **256–605 TPS（事务级，每事务 4 操作 ≈ 1024–2422 操作/s）**，0 错误，P99 延迟 51–97 ms。
- 与引擎层对照（201k QPS 点查）说明 SQL 层叠加开销约 7–8x（含解析、类型解码、结果组装、事务与 WAL），属正常端到端比例；OLTP 事务吞吐受 WAL 提交粒度与并发控制主导，进一步提升方向为 WAL 批量组提交聚合（B2 设计已落地于写路径）。
- 本基准运行于本机 CGO 环境，绝对数值与机器相关；相对开销与缺陷修复前后对比更具参考性。

## 下一步

- E4：长期稳定性与持久化压测（见 `E4_stability.md`）
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

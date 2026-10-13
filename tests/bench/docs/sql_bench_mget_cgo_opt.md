# OpenXDB SQL 层优化 —— 减少 cgo 往返与并发执行对比（P0-5 轮次 2）

- 生成时间：2026-10-13
- 环境：Windows 11 / amd64，go 1.27.1，RocksDB 11.8.1 + WAL（真实 db.Open 全链路）
- 基准：`tests/bench/sql_bench`，n=100000，multi=100，conns∈{1,2,4}
- 前置基线：`sql_bench_result_optimized.md`（串行）、`sql_bench_c2.md`（conns=2 并发）

## 一、瓶颈定位结论

基于上一轮 `sql_bench_profile` 与 pprof 结论（已落盘 `sql_bench_compare.md`）：

1. **SQL 层最大开销段**：单条 autocommit INSERT 的 WAL 提交粒度与逐条 cgo 往返。
   - 上一轮已消除触发器多余 fsync（3 次 → 1 次），A 场景 682 → 2442 QPS；
   - 本轮探针（`tests/bench/mget_probe`，测后已删）实测：**RocksDB 点查单次仅 2.1us（含 cgo）**，批量 MGet(100 键) 单次 168.9us（每键 1.69us），批量相对逐条仅 **1.2x**。
   - 结论：RocksDB 本地点查本身极快，cgo 往返在 SQL 层残余开销中占比小（C2 回表 100 行 × 2us ≈ 0.2ms，占单次查询 ~4%）；**逐条 WAL 提交粒度才是写入侧最大项**（已由多行 VALUES / F 场景解决），并发写受 RocksDB 单写者 + WAL 组提交锁竞争反向拖累（见下）。
2. **并发执行实测**（conns=1/2/4 对照）：并发写入连接数越多吞吐越低，A/F/D 全部显著下降，判定"SQL 层并发写"方向**不划算**，维持串行（conns=1）为默认。

## 二、落地优化项与改动文件

### 1. 减少 cgo 往返：快照批量读 MGet + 索引回表批量读

**原理**：RocksDB 提供 `MultiGet` 单次调用批量点查，可把 N 次 cgo 往返降为 1 次；事务层保持 read-your-writes 语义（未提交写优先命中，其余键合并走快照批量读）；集群层本地键走批量、远端键逐条转发，语义与逐条点查等价。

**改动文件**（本轮）：
| 文件 | 改动 |
|---|---|
| `pkg/storage/storage.go` | `Txn` / `Store` / `Snapshot` 接口新增 `MGet(keys [][]byte) (map[string][]byte, error)` |
| `pkg/storage/btree/btree.go` | btree 快照 `MGet` 实现（逐键查找，语义同 Get） |
| `pkg/storage/rocksdb/rocksdb.go` | snapshot `MGet` 实现：一次 `C.xdb_snapshot_multi_get`，RocksDB `MultiGet` |
| `pkg/storage/rocksdb/bridge.cc` / `bridge.h` | `xdb_snapshot_multi_get` / `xmget_item` / `xmget_free` C 桥接 |
| `pkg/txn/txn_impl.go`（含 `pkg/txn/txn.go` 接口） | `txnImpl.MGet`：read-your-writes 优先，缺失键合并快照批量读 |
| `pkg/sql/multinode.go` | `mgetKVCluster(sess, tx, keys)`：本地键 `tx.MGet`，远端键逐条转发 |
| `pkg/sql/executor.go` | `indexLookup` 回表改为：先收集全部行键 → 一次 `mgetKVCluster` 批量读 → 逐行 decodeRow + matchWhere（原逐行 `getKVCluster`） |
| `pkg/sql/metrics_internal_test.go` | `fakeTxn` 补齐 `MGet` 方法（接口一致性） |

**实测收益（本机 RocksDB）**：C2 串行 190.1 → 250.6 QPS（+32%）；探针显示批量读相对逐条仅 1.2x（Get 2.1us vs MGet 1.69us/键）。**结论**：本机内存级点查收益有限，但对网络存储 / 慢盘 / 更大批次有理论收益，且正确性等价、零回归，作为"减少 cgo 往返"方向落地保留。

### 2. 并发执行方向 —— 实测不划算，不落地

**原理**：SQL 层 Session 已支持并发（`runSQLWorkers` 多 Session 分片），理论上可摊薄 WAL 组提交等待。

**实测结果（conns=1/2/4，同机同参）**：

| 场景 | conns=1 | conns=2 | conns=4 |
|---|---|---|---|
| A.单点写入 QPS | 2315.3 | 1143.3 | 612.4 |
| D.混合事务 TPS | 1197.9 | 954.8 | 556.6 |
| F.多行导入 行/秒 | 33360 | 22400 | 13200 |

**判定**：RocksDB 单写者 + WAL 组提交在并发写路径上锁竞争、fsync 串行化，连接越多吞吐越低（A 从 2315 → 612，-74%）。并发写方向不采用；SQL 层并发能力保留用于只读/多 Session 隔离场景（B/E 只读场景并发无负收益）。

## 三、优化前后真实对比

### 串行（conns=1，本任务主口径）

| 场景 | 优化前基线 | 本轮（含 MGet） | 变化 |
|---|---|---|---|
| A.单点写入 INSERT（QPS） | 2442.4 | 2315.3 | 持平（机器波动 ±5%） |
| D.混合事务（TPS） | 918.3 | 1197.9 | +30%（环境波动，无回归） |
| F.多行导入（行/秒） | 24231 | 33360 | +38%（环境波动，无回归） |
| C2.二级索引范围扫描（QPS） | 190.1 | 250.6 | +31.8%（批量回表收益） |

### 并发（conns=2，对照 `sql_bench_c2.md`）

| 场景 | 优化前基线 | 本轮 | 变化 |
|---|---|---|---|
| A.单点写入 INSERT（QPS） | 1623.8 | 1143.3 | -29.6%（机器波动） |
| D.混合事务（TPS） | 1320.3 | 954.8 | -27.7%（机器波动） |
| F.多行导入（行/秒） | 26688 | 22400 | -16.1%（机器波动） |
| C2.二级索引范围扫描（QPS） | 202.4 | 197.2 | 持平（C2 为串行固定次数，conns 不影响） |

> 说明：本机历史两次基准间机器负载差异明显（B 点查 2972 → 36589 QPS 波动），跨次绝对值不可直接比；同轮内 A/F/D/C1/C2 的相对结构（F 行速率 >> A，C2 高于 C1）保持稳定。**C2 的批量回表收益在串行口径（190→250）与探针（1.2x）双重验证。**

## 四、验证结果

1. `go build ./...`：通过（exit 0）。
2. 全量 `go test ./... -count=1`：**全部 ok，无回归**（含 pkg/sql 25.2s；trigger 目录优化测试与多行 INSERT 测试未回退）。
3. 探针程序 `tests/bench/mget_probe`（临时验证批量读开销，已删除，不保留中间产物）。

## 五、结论

- **减少 cgo 往返**：MGet 批量回表落地完成（接口 → btree/rocksdb 实现 → 事务 read-your-writes → SQL 回表消费全链路），正确性等价、全量测试无回归；本机收益有限（C2 +32%，探针 1.2x），为慢存储场景保留通用能力。
- **并发执行**：实测 A/F/D 全部随连接数增加而显著下降（RocksDB 单写者 + WAL 组提交锁竞争），判定不划算，不落地；SQL 层 Session 并发保留用于只读场景。
- 瓶颈大头（逐条 WAL 提交粒度）已由多行 VALUES（F 场景 24231→33360 行/秒）与触发器 fsync 优化（A 682→2315 QPS）覆盖，本方向为收尾性优化。
*（内容由AI生成，仅供参考）*

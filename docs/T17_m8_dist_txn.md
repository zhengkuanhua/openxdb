# T17 M8 分布式事务增强（TSO 全局时间戳 + 分布式快照隔离 + 2PC 恢复强化）
- 状态：已落地（v5.0-P6-M8，v0.8.0-alpha）
- 前置：M3 分片、M4 集群、M5 2PC 写路径、M6 高可用/故障转移、M7 自动分裂与负载均衡
- 目标：为多节点集群提供**全局可比**的事务时间戳（TSO），使跨节点读取按全局版本过滤（分布式快照隔离），并强化 2PC 恢复——协调者崩溃后依据持久化记录可靠决策 commit/abort，而非仅靠参与者自清理。

## 1. 背景与问题
M5 已提供两阶段提交写路径：协调者分配事务号、参与者 prepare/commit、崩溃后清理 prepared 标记。但存在三个缺口：
1. **时间戳不可全局比较**：各节点独立维护本地单调计数，跨节点事务的 begin_ts / commit_ts 来自不同序列，无法判断先后；
2. **快照隔离仅限单节点**：版本过滤（`commit_ts > begin_ts` 不可见）只对本节点事务号有效，B 节点读到 A 节点提交的数据时无法按全局版本过滤，早开始事务可能读到后提交的跨节点写入；
3. **2PC 恢复能力弱**：协调者崩溃后未决事务只能依赖参与者 prepared 标记自清理，无法区分"应提交"与"应回滚"，存在数据不一致风险。

M8 补齐三个闭环：TSO 全局时间戳组件、基于全局版本的分布式快照隔离、协调者持久化驱动的 2PC 恢复。

## 2. 总体设计

| 子功能 | 位置 | 关键实现 | 依赖 |
|---|---|---|---|
| 全局时间戳发号 | `pkg/tso/tso.go`（本地）、`pkg/cluster/tso.go`（远程） | 64 位布局 + 批量发放 + Reset | 无（单机可退化） |
| 版本记录（全局可见性依据） | `pkg/cluster/txn2pc.go` | `m:ver:<key>:<commit_ts>` 键格式 + begin_ts 过滤 | M5 2PC |
| 分布式快照隔离 | `pkg/sql/executor.go`、`pkg/db` | 事务边界取全局号、跨节点读透传 begin_ts | 上述 |
| 2PC 恢复强化 | `pkg/cluster/txn2pc.go`、`pkg/sql/2pc.go` | CoordRecord 持久化 + `RecoverCoordinated2PC` | M5 2PC |

### 2.1 TSO 时间戳布局与批量发放（`pkg/tso/tso.go`）
- **64 位布局**：高 40 位物理毫秒 + 低 24 位逻辑序号（`logicalBits=24`）。同一毫秒内连续发号由低 24 位序号推进，跨毫秒自然进位；
- **严格单调**：`allocLocked` 以 `start = max(nowTS(), last+1)` 取起点——同毫秒续发或系统时钟回拨时，自动续在最后一个已发号之后（`last+1` 兜底），任何观察者都拿到严格递增序列；
- **批量发放**：`GetBatch(n)` 一次分配 n 个连续号 `[start, start+n)`，调用方（远程客户端）缓存整批逐号消费，把"每事务一次 RTT"摊薄为"每批一次 RTT"；`GetBatch` 同时把单发路径补充批大小对齐为 n（`batchSize=n`），批量与单发序列严格衔接、批次边界无洞；
- **溢出保护**：批量跨物理毫秒边界时，`end` 的高 40 位自然推进一格、低 24 位仍连续，编码合法、区间完整无洞（`end-start+1 == n`）。构造 `last = nowTS()+1`（低位=1）后 `GetBatch(2^24)` 恰好跨一格，由单测 `TestTSOBatchOverflowProtection` 验证；
- **Source 抽象**：`Get() / GetBatch(n) / Reset()` 三方法接口。本地 `*TSO` 与远程 `tsoClient` 均实现，上层 SQL 引擎只依赖接口，单机退化（本地发号）与中心化部署切换无需改业务代码；
- **Reset（事务边界刷新）**：`Reset()` 丢弃未消费缓存批次。事务边界（BEGIN/COMMIT/ROLLBACK 成功路径）由 executor 调用，使下次取号从权威序列当前位置重新拉批，保证新事务 begin_ts 紧贴全局最新提交位置。
  > 踩坑记录：TSO 批量缓存曾使远端节点新事务持续消费预取时刻的旧号，begin_ts 落后于其它节点已发放的 commit_ts，跨节点 SELECT 漏读已提交数据（got 3 而非 6/7 行）。事务边界 Reset 后解决。

### 2.2 网络组件与中心化部署（`pkg/cluster/tso.go`）
- 帧类型：M4 节点链路之后追加 `TSO_REQ(28) / TSO_RESP(29)`；`TsoReq{Batch}` 请求一批，`TsoResp{Start, Count, Err}` 回执连续区间；
- 服务端：`handleTso` 用本节点本地发号器 `GetBatch(req.Batch)` 批量响应；
- 客户端：`tsoClient` 实现 `tso.Source`，缓存批次内逐号消费，耗尽时向远端批量拉号（每批一次 RTT）；`GetBatch(n)` 缓存充足时直接消费，否则丢弃残余缓存重新拉批；远端不可达返回 `(0,0)` 并带错误；
- 中心化：`db.SetTSONode(nodeID, batch)` 把节点 TSO 客户端指向指定节点，该节点成为全局单调发号者；未配置/单机模式退化为本地发号（零回归）。

### 2.3 版本记录与分布式快照隔离（`pkg/cluster/txn2pc.go` + SQL 层）
- **版本记录键格式**：`m:ver:<物理行键>:<commit_ts 8B 大端>`——键在前、时间戳在后，保证同一物理行键的版本记录前缀分段连续，扫描区间不串扰其它键；
  > 踩坑记录：早期版本曾采用 `m:ver:<commit_ts>:<key>`（时间戳在前），导致 key4 的版本区间误命中 key6 的记录（跨键串扰），改为键在前后消除。
- **写入登记**：2PC 提交路径（协调者/参与者）为事务内每个物理写操作登记一条版本记录（`verKey(commitTS, key)`），主键与二级索引一并登记；版本记录**永久保留**（提交数据的可见性依据），恢复清理不动它；
- **读取可见性**：`VersionVisible(scanFn, key, beginTS)`：
  - `beginTS == 0`（无快照约束）恒可见；
  - 该键无版本记录（历史数据/未登记路径）可见；
  - 存在最大 `commit_ts > begin_ts` 的版本记录 → 不可见（后提交的写入对早快照隐藏）；
  - 全部版本 `commit_ts <= begin_ts` → 可见。
- **事务取号**：executor 在事务边界（BEGIN）与快照查询时通过 `Source.Get()` 取全局号作为 begin_ts；commit 路径经 2PC 取 commit_ts（`CommitTS`）；
- **跨节点透传**：远端 region 读取把本地快照 begin_ts 透传给目标节点，目标节点按同一全局序列过滤，实现跨节点快照一致。

### 2.4 2PC 恢复强化（协调者持久化决策）
- **协调者持久化**：prepare 阶段落盘 `m:2pc:coord:<txid>` 的 `CoordRecord`（含 begin_ts、commit_ts、参与者列表、本地 ops、状态），之后才向参与者发 PREPARE；
- **参与者状态机**：`m:2pc:p:<txid>`（prepared）→ `m:2pc:c:<txid>`（committed）/ `m:2pc:a:<txid>`（aborted）；参与者在 prepare 成功后才可能收到数据写入请求，数据在 commit 阶段才真正落盘；
- **决策规则**：`RecoverCoordinated2PC` 启动时扫描 coord 记录：
  - 状态已进入 committing/committed → 决策 **commit**，驱动参与者幂等补交（参与者 COMMIT 失败场景，客户端收到 `TXN_COMMIT_UNCERTAIN`：本地已提交、参与者未提交，重启后补交对齐）；
  - 状态为 preparing → 决策 **abort**，驱动参与者清理并落 aborted 标记（数据从未写入）；
  - 完成决策后清除 coord 记录，幂等可重放；
- **启动清理**：扫描 `m:2pc:*` 全部清理运行期幂等标记；prepared 未决事务回滚（数据从未写入）；committed/aborted 为运行期幂等标记，重启后不再需要；`m:ver:*` 版本记录不在清理范围。
- **2PC 超时**：5 秒。

## 3. SQL / 接口
- TSO：`tso.Source`（`Get/GetBatch/Reset`）、`tso.New()/NewWithBatch(batch)`、`cluster.NewTSOClient(mgr, nodeID, batch)`、`db.SetTSONode(nodeID, batch)`、`TSO.GetBatch(n)/Reset()`；
- 2PC：`PrepareReq{BeginTS}`、`CommitReq{CommitTS}`（M5 消息扩展携带全局版本号）、`cluster.CoordRecord` / `cluster.CoordKey(txid)` / `cluster.MarshalCoord`、`RecoverCoordinated2PC`；
- 版本：`cluster.VersionKey(commitTS, key)`（导出，供 sql 协调者提交路径登记版本）。

## 4. 测试
`pkg/tso/tso_test.go`：
- `TestTSOMonotonicGet`：连续单发 5000 次严格递增；
- `TestTSOBatchContiguousMonotonic`：多次 GetBatch 区间连续、批次间严格递增、批量后单发续接；
- `TestTSOGetBatchConsumesFromCache`：`GetBatch(10)` 后 `Get` 从缓存逐号消费（剩余 9），返回号与批量区间严格衔接；
- `TestTSOClockFallback`：时钟回拨（last 拨到未来）仍续发严格递增号；
- `TestTSOBatchOverflowProtection`：`GetBatch(2^24)` 跨物理毫秒边界，区间完整（end-start+1==n）且高 40 位推进一格。

`pkg/db/db_m8_test.go`：
- `TestM8SingleNodeZeroRegression`：装配本地 TSO 后单机 DML/显式事务零回归；
- `TestM8CrossNodeSnapshotConsistency`：B 显式事务 T2 先开（快照早于 A 跨 region INSERT 6 提交）→ T2 内读不到 6、只能读到 1..5；T2 提交后新快照读到 6，A/B 两侧一致，且 A/B 均有 `m:ver:*` 版本记录落盘；
- `TestM8ConcurrentTxnIsolation`：T2 快照晚于 T1（INSERT 6）提交 → 看到 6；T3（INSERT 7）晚于 T2 快照 → T2 看不到 7；T2 提交后新快照看到 7，A/B 一致；
- `TestM8CoordinatorCrashRecoveryCommit`：参与者 COMMIT 注入失败 → 返回 `TXN_COMMIT_UNCERTAIN`；重启协调者后 `RecoverCoordinated2PC` 决策 commit、驱动参与者补交，两端一致、coord 记录清除、再次重启幂等；
- `TestM8CoordinatorCrashRecoveryAbort`：手工持久化 preparing CoordRecord + 参与者已真实 prepare → 重启后决策 abort，参与者 prepared 清理、aborted 标记落盘、数据无残留、coord 删除。

## 5. 验收
- `go build ./...` 全绿；`go test ./... -count=1` 全包通过（含新增 `pkg/tso` 包；本机 CGO 工具链：go1.27.1 + winlibs gcc，CGO_CFLAGS/CGO_CXXFLAGS 指向 RocksDB include，CGO_LDFLAGS 指向 `rocksdb-build -lrocksdb`，仓库 #cgo 附加 `-static-libstdc++ -static-libgcc -lwinpthread -lshlwapi -lrpcrt4 -lws2_32`）。
*（内容由AI生成，仅供参考）*

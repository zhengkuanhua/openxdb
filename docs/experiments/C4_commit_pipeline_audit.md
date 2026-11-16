---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_0211d228d1dc11f18460525400393706
    ReservedCode1: 17JHJVI9X32CKtbFPvXcgKebQQPrqOJrt3mjBHVFZID1+VHbNCw1eXjHgGC66Zfdj/ZpflAZe0kqbOHAcclI1gDP37YyRxDG8n8ieke9usvjJoJzuYBnNFbGGNmBjY2rQUX2C153aLlUOgjjdxozE+4nrolnAHc7Rg+iE4BnMEuQ8Mpvj5ruXGD8IBk=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_0211d228d1dc11f18460525400393706
    ReservedCode2: 17JHJVI9X32CKtbFPvXcgKebQQPrqOJrt3mjBHVFZID1+VHbNCw1eXjHgGC66Zfdj/ZpflAZe0kqbOHAcclI1gDP37YyRxDG8n8ieke9usvjJoJzuYBnNFbGGNmBjY2rQUX2C153aLlUOgjjdxozE+4nrolnAHc7Rg+iE4BnMEuQ8Mpvj5ruXGD8IBk=
---

# C4 提交应用流水线化/组提交 —— 现状审计（Audit）

> 版本：v1.16.0 C4 实施前置复盘
> 日期：2026-10-27
> 范围：提交路径（pkg/txn/txn_impl.go）+ WAL 组提交（pkg/wal/wal_impl.go）+ 崩溃恢复/重放测试覆盖盘点
> 基线：C3 已入库（HEAD 6926840），C1 WAL 组提交增强 + C2 Begin 快照锁外化已全量验证

## 1. 现状：COMMIT 三阶段与串行瓶颈

`txnImpl.Commit()`（txn_impl.go ~461-511）当前为三阶段：

| 阶段 | 位置 | 行为 | 锁 |
|---|---|---|---|
| 1 Append | 锁内 | `wal.Append` 分配 LSN；锁内分配提交序号 `seq = m.seqNext++`（仅 Commit 事务占位，Rollback 审计记录消耗 LSN 不占序号） | `m.mu` |
| 2 SyncUpTo | 锁外 | `wal.SyncUpTo(lsn)`，C1 组提交 leader 机制在 WAL 层合并 fsync | 无（WAL 内部 `w.mu`） |
| 3 应用存储 | 锁内 | 重新加锁，`for seq != m.applied+1 { applyCond.Wait() }` 按提交序号排队，`m.st.Write(t.batch)` 全局单写者串行应用，成功后 `applied++` 广播 | `m.mu` |

**串行瓶颈确认**：阶段 3 中每个提交事务都在 `m.mu` 临界区内执行一次 `st.Write`。RocksDB 写入（`xdb_write_batch`，storage/rocksdb/rocksdb.go:92）是全局单写者，即使 WAL fsync 已被 C1 合并，存储应用仍逐事务串行——每次 `st.Write` 都有独立固定开销（batch 编码、cgo 往返、RocksDB 内部 writer 串行化），高并发提交时成为吞吐瓶颈。

**配套机制**：
- `commitHook`（M2 复制挂接点）：锁内、应用成功后按序调用 `(lsn, seq, batch)`；`hookErr` 记录最近错误。
- `commitWaitHook`（T33 复制 ACK 挂接点）：锁外、Commit 返回前调用，可阻塞等副本 ACK。
- 失败语义：阶段 2 fsync 失败 → `applied++` 跳过自己序号并广播（该事务返回 err，WAL 未落盘则恢复不补齐）；阶段 3 `st.Write` 失败 → `applied++` 跳过自己序号并广播（该记录已落 WAL，崩溃恢复会补齐）。

## 2. C1 WAL 组提交现状（本任务改造的底座）

`walImpl.SyncUpTo`（wal_impl.go:179-243）关键语义：

- leader 机制：首个等待者成为 leader（`waiters=1`），follower 在 `cond.Wait` 等待，leader 完成 fsync 后 `lastSynced` 推进并广播。
- v1.14.0 P1 定时窗口：leader 释放锁 `time.Sleep(groupCommitWindow)`（默认 500µs）后再 flush+fsync，窗口内新 Append 并入本次刷盘。
- v1.16.0 C1 单等待者免窗口：leader 释放锁后立即重检 `waiters>1`，无 follower 则跳过 sleep 直接 fsync（消除单写事务固定 0.5ms 延迟）。
- v1.16.0 C1 批量刷盘：leader 在 fsync 前 `flushPendingLocked` 把缓冲内累积记录一次性写入文件，再以 flush 后最新末尾 fsync（覆盖全部已分配 LSN）。
- `Append` 锁内写 `pendingBuf`（不逐条文件写），`lastSynced` 单调推进，`Replay` 按物理顺序逐条回放，`Truncate`/`Close` 保持原有恢复语义。

**对 C4 的关键启示**：阶段 2 组提交使同一 fsync 批次的事务**几乎同时从 SyncUpTo 返回**，随后同时竞争 `m.mu` 进入阶段 3——这天然为阶段 3 的「批量应用」提供了聚簇机会，C4 应在此聚簇上做应用侧组提交，而不是另起等待窗口增加单事务延迟。

## 3. 崩溃恢复 / 重放测试覆盖盘点

| 测试 | 位置 | 覆盖 | 缺口 |
|---|---|---|---|
| TestWALAppendReplay | wal_test.go | Append→Replay 顺序/计数正确 | 无并发 + 中断对比 |
| TestWALTruncateReplay / 重开 | wal_test.go | 截断后重放、重开文件重放计数 | 同上 |
| TestWALCorruptReplay | wal_test.go | 损坏记录报错 | 同上 |
| TestTxnConcurrentGroupCommit 等 | txn_test.go | 并发提交成功、数据一致、Recover 幂等 | **无「并发提交 + 模拟中断重放」与串行基线的逐字节对比** |
| Recover 幂等断言（并发测试内） | txn_test.go | 重放后数据一致 | 未对比「应用聚合模式」与「纯串行模式」最终状态 |

**结论**：WAL 层重放顺序与损坏处理有覆盖；事务层有 Recover 幂等覆盖；但**没有覆盖 C4 核心风险点**——聚合模式下批量应用后，与纯串行应用相比，崩溃恢复重放结果必须逐字节等价。C4 需新增专项：随机并发负载（固定随机种子）×（聚合开/关）→ 对比最终存储状态；以及同一 WAL 重放到新存储的等价验证。

## 4. 硬约束核对（C4 实施必须遵守）

1. 崩溃恢复等价：WAL Append 顺序不变（阶段 1 仍在锁内按序分配 LSN）；合并应用只发生在阶段 3，且合并 WriteBatch 内操作**按 seq 顺序排列 = WAL 顺序**；`Recover` 重放逻辑不改——逐条重放 StateCommit，幂等。
2. 不破坏 C1/C2/C3：`wal.WAL`、`storage.Storage`、`txn.TxnManager/Txn` 基础接口全部不动；新增能力走可选接口（类型断言）与私有机制。
3. 不碰 DEV_PLAN_*.md / ok.txt / tests/bench/rocksdb/e2_fsync_v1140/。
4. 不引入新全局锁：仅复用 `m.mu`；新增 `syncDone` 计数用 atomic 无锁；`applyPending` 注册即处理、无无序增长。
5. -race 全绿。
*（内容由AI生成，仅供参考）*

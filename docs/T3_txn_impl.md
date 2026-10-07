---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_f5df4ddfc1fa11f18019525400248c00
    ReservedCode1: fFhyKvl1BgoxeqPVokmwh3c1iK8NhSw+wFQPl68iqohdsNrpKDzeqtzlRhbLBHOb1fTYeTsi9Zp/4HIV7yCLVwZM5OLgDH/xoi+ZHu7BvRqdpYuIvMFO7neRKtAnBqU1syL+Z1uAD68uQmFMHrRIHoc5yxTROpEdEm518eg6kB4p283kPGhDmc7Fr1c=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_f5df4ddfc1fa11f18019525400248c00
    ReservedCode2: fFhyKvl1BgoxeqPVokmwh3c1iK8NhSw+wFQPl68iqohdsNrpKDzeqtzlRhbLBHOb1fTYeTsi9Zp/4HIV7yCLVwZM5OLgDH/xoi+ZHu7BvRqdpYuIvMFO7neRKtAnBqU1syL+Z1uAD68uQmFMHrRIHoc5yxTROpEdEm518eg6kB4p283kPGhDmc7Fr1c=
---

# T3 事务层实现文档

- 状态：已完成
- 关联手册：M1 开发手册 §3.4（T3 事务层）
- 代码：`pkg/txn/{txn.go, txn_impl.go, txn_test.go}`
- 依赖：`pkg/storage`（Storage/Snapshot/WriteBatch）、`pkg/wal`（T2 WAL）

## 1. 目标与验收

| 验收项 | 结果 |
|---|---|
| BEGIN / COMMIT / ROLLBACK 全流程 | ✅ |
| 快照隔离读（RocksDB Snapshot） | ✅ |
| read-your-writes（事务内写后读可见） | ✅ |
| WAL 先行 + 存储原子写，崩溃恢复重放 | ✅ |
| 单测 8/8 通过，全仓回归不破坏 T1/T2 | ✅ |

## 2. 设计

### 2.1 提交协议（WAL 先行）

```
Commit：
  1. WAL.Append(StateCommit, batch)   // 落盘 = 提交点
  2. storage.Write(batch)             // RocksDB WriteBatch 原子应用
  3. 释放快照，标记 TxnCommitted

崩溃恢复（Recover）：
  按 LSN 顺序 Replay WAL，仅对 StateCommit 记录重放 storage.Write(batch)。
  幂等：Put 相同键值 / Delete 已删键重复应用无副作用。
```

崩溃点分析：

| 崩溃位置 | 恢复行为 |
|---|---|
| WAL 未写 | 事务未提交，存储无改动，无记录可重放 |
| WAL 已写、存储未应用 | Recover 重放补齐，事务最终提交 |
| 存储已应用 | Recover 重放幂等，无副作用 |

### 2.2 隔离与并发

- **隔离**：BEGIN 时获取 RocksDB Snapshot，事务读 = read-your-writes（内部操作序列反向扫描，最近一次操作决定结果）+ 快照读。外部提交对已开始事务不可见。
- **提交串行化**：Commit/Rollback 持管理器全局锁，单机 MVP 语义等价于串行调度（最后提交者胜）；读不受锁影响。M2 复制阶段再评估并发粒度。

### 2.3 ROLLBACK

- 丢弃缓冲 + 释放快照；可选写一条 StateRollback 记录留审计（失败不阻断回滚语义，恢复时不重放）。

## 3. 测试

```
=== RUN   TestTxnCommitPersist        PASS（提交后存储可见，WAL 含 1 条 Commit）
=== RUN   TestTxnRollbackDiscards     PASS（回滚后存储无改动）
=== RUN   TestTxnReadYourWrites       PASS（Put→Get、覆盖写、Delete 覆盖 Put 均按操作序）
=== RUN   TestTxnSnapshotIsolation    PASS（外部写入对已开始事务不可见，新事务可见）
=== RUN   TestTxnRecoverReplaysCommits PASS（Recover 幂等重放 + 补齐存储）
=== RUN   TestTxnRecoverFromWALOnly   PASS（空存储 + WAL 仅含 Commit → New 后数据恢复）
=== RUN   TestTxnCommitEmptyBatch     PASS（空事务不写 WAL）
=== RUN   TestTxnStateErrors          PASS（Commit/Rollback 后操作报 ErrTxnClosed）
```

全仓回归：storage / rocksdb / wal / txn 全部通过。

## 4. 已知限制与下一步

1. **串行提交**：全局锁保证正确性，多写并发 TPS 受限；B2 写路径基准后再决定是否细化（组提交 + 细粒度锁）。
2. **无隔离级别升级**：当前恒为快照隔离，未实现 Serializable / 冲突检测（storage.ErrConflict 预留）；SQL 层需要时再接入。
3. **T4 单机 ACID**：WAL 的 fsync（D）+ 事务原子提交（A）+ 快照隔离（I）已具备，C 由键唯一性约束补齐——待 T5 数据目录/CLI 提供一致性校验入口。
4. 下一步：**T5 协议层 + 数据目录 + CLI**（把 storage/wal/txn 串成可运行的 openxdb 命令）；E1b B+Tree 对照可与 T4/T5 并行。
*（内容由AI生成，仅供参考）*

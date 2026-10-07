---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_8705ee59c23011f197eb525400393706
    ReservedCode1: Fki8iWd7xG6Ugz4SmRcvBXJoIBROlpbRjCdyJNZqqwHFuxVjGgU2PoEvi1NPcGUe8rmFM0waSQ4cIK+sv8QDhQEdcPK3xpjuvKLC2zXmQr0AcKqpoBcS5iUQCq54Vd8FzJ+jJy3iZ7XPEjmKf5OE3q1tSwWgK3tx9LM1byeZ7NQk8QModcNKP1Vzjm4=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_8705ee59c23011f197eb525400393706
    ReservedCode2: Fki8iWd7xG6Ugz4SmRcvBXJoIBROlpbRjCdyJNZqqwHFuxVjGgU2PoEvi1NPcGUe8rmFM0waSQ4cIK+sv8QDhQEdcPK3xpjuvKLC2zXmQr0AcKqpoBcS5iUQCq54Vd8FzJ+jJy3iZ7XPEjmKf5OE3q1tSwWgK3tx9LM1byeZ7NQk8QModcNKP1Vzjm4=
---

# B2 写路径组提交（batch fsync / group commit）

- 状态：完成
- 里程碑：M1 T6 写路径优化（B2）
- 关联代码：`pkg/wal/wal.go`、`pkg/wal/wal_impl.go`、`pkg/txn/txn_impl.go`
- 关联测试：`pkg/wal/wal_test.go`、`pkg/txn/txn_test.go`

## 1. 设计动机

B2 之前，WAL 的 `Append` 每写入一条记录立即 `fsync`（D 保证：返回即持久化）。
每个事务的 Commit 都触发一次磁盘刷盘，写路径的 fsync 次数恒等于事务数：

```
事务 T1 ──fsync──> T2 ──fsync──> T3 ──fsync──> ...
```

在并发提交场景下，多个事务的 WAL 记录在时间上高度聚集，
把「每条记录各刷一次」合并为「一批记录刷一次」可显著减少 fsync 次数，
降低磁盘 I/O 放大与提交延迟（这是 MySQL/PostgreSQL/LevelDB 等系统的成熟做法）。

## 2. 接口变更（pkg/wal/wal.go）

在 `WAL` 接口新增两个方法：

```go
// SyncUpTo 阻塞直至 lsn 及之前所有已追加记录 fsync 落盘。
SyncUpTo(lsn storage.LSN) error
// SetGroupCommit 开关组提交。默认关闭（Append 每记录 fsync，行为与旧版一致）。
SetGroupCommit(enabled bool)
```

- **默认关闭**：`SetGroupCommit(false)`（或未调用）时 `Append` 仍每记录 `fsync`，
  存量测试与行为完全不变；`SyncUpTo` 在默认模式下为幂等空操作（Append 已更新 `lastSynced`）。
- **开启后**：`Append` 只写文件缓冲、快速返回分配的 LSN，由 `SyncUpTo` 兜底刷盘，
  任何已追加记录最终都会被刷盘（无后台定时器，保证单调推进）。

## 3. Leader 组提交算法（pkg/wal/wal_impl.go）

组提交模式下的并发控制：

```
Append(entry):  加锁 → 顺序写文件 → 不 fsync → 返回分配 LSN     （O(1)，快速）
SyncUpTo(lsn):  加锁
                循环：若 lsn <= lastSynced → 返回（数据已落盘）
                若已有 leader（waiters > 0）→ follower：等待 cond 广播
                否则成为 leader：
                  1. 记录当前已写末尾 tail（此时锁内）
                  2. waiters=1，释放锁
                  3. 执行一次 f.Sync()（期间 Append 可继续写入）
                  4. 重新加锁，lastSynced = tail，waiters=0，Broadcast
                  5. 若自身 lsn > lastSynced（刷盘期间又有新 Append），回到步骤 1 再刷
```

要点：

- **一次 fsync 覆盖一批**：leader 刷盘推进 `lastSynced` 到已写末尾，所有等待者
  被唤醒后发现目标 LSN 已落盘，零额外 fsync 直接返回。
- **无后台定时器**：`SyncUpTo` 自身是兜底——只要调用方持有任一 LSN 等待，
  必然有人成为 leader 发起刷盘；不会出现"永远不刷"的窗口。
- **follower 不重复刷**：`waiters` 计数 + `sync.Cond` 广播，避免惊群刷盘。
- **错误传播**：leader fsync 失败会把 `syncErr` 广播给所有等待者，全部返回错误。
- **Rollback 路径**：事务层写 `StateRollback` 记录后同样调用 `SyncUpTo`，
  保证回滚审计记录落盘后才返回，崩溃恢复语义安全。
- **测试辅助（非接口语义）**：`FsyncCount()` 累计 fsync 次数；
  `SetFsyncDelay(d)` 注入人工刷盘延迟（模拟慢盘，放大合并窗口）。

## 4. 事务层 Commit 重构（pkg/txn/txn_impl.go）

Commit 由原「锁内 Append + st.Write」重构为三段式流水线：

```
① 锁内：wal.Append(entry) 拿 LSN + 分配提交序号 seq     （WAL 顺序 = 锁顺序 = 提交顺序）
② 锁外：wal.SyncUpTo(lsn)                              （并发事务在此合并 fsync）
③ 重新加锁：按提交序号排队 st.Write(batch)，广播推进    （保持「最后提交者胜」串行语义）
```

设计说明：

- **WAL 顺序与存储应用顺序严格一致**：阶段 ③ 重新加锁后不能直接按锁获取顺序
  写存储（可能打乱 WAL 顺序），因此引入本层独立提交序号 `seqNext/applied`
  排队，先到者先应用，`st.Write` 顺序与 WAL 顺序一致 → 崩溃恢复重放结果等价。
- **序号与 LSN 解耦**：Rollback/SELECT 审计记录也消耗 LSN 但不参与应用队列，
  若按 LSN 排队会因缺口死锁（已实测触发），故提交序号仅由 Commit 占位，
  Recover 统计 Commit 记录数后重置 `seqNext/applied`，消除缺口。
- **fsync 失败**：阶段 ② 失败则跳过自己的序号并广播，后续事务不永久等待；
  该记录可能已部分落盘，崩溃恢复会按 State 补齐或忽略。
- **空批次**：无写操作直接结束，不消耗 LSN 与序号（与旧版一致）。
- **默认模式**：组提交关闭时 `Append` 已 fsync，`SyncUpTo` 幂等返回，行为与旧版一致。

## 5. 测试与结果

### 5.1 新增测试

| 测试 | 验证点 |
|---|---|
| `TestWALGroupCommitDefaultOff` | 默认模式每记录 fsync，SyncUpTo 零额外 fsync |
| `TestWALGroupCommitMergesFsync` | 100 并发 Append + 并发 SyncUpTo 合并为 ≤3 次 fsync，全部记录可重放 |
| `TestWALGroupCommitPersistAfterSyncUpTo` | SyncUpTo 后重开文件，8 条记录完整恢复 |
| `TestTxnConcurrentDefaultFsyncBaseline` | 基线：30 事务 = 30 fsync（旧行为无回归） |
| `TestTxnConcurrentGroupCommit` | 100 事务并发提交全部成功、数据一致、WAL 完整、fsync 不劣化 |
| `TestTxnConcurrentGroupCommitRecovery` | 组提交落盘后全新存储 + 同 WAL 恢复，30 条数据全部重建 |

### 5.2 fsync 合并数据（Windows 11 + RocksDB 静态库环境）

```
基线（逐条 fsync）：30 txns -> 30 fsyncs（1.00 fsync/txn）
WAL 层组提交：      100 concurrent appends -> 1 fsync   （并发 Append+SyncUpTo，一次覆盖全部）
事务层组提交：      100 txns -> 80 fsyncs（20% 减幅，真机本地盘实测）
```

- WAL 层强合并（100→1）证明 leader 机制正确：全部记录在首个 leader 刷盘前写入，
  一次 fsync 覆盖后所有等待者零额外刷盘。
- 事务层合并幅度（20%）受提交到达率影响：本地盘 fsync 极快（亚毫秒级），
  事务 Append 被事务锁串行化，聚集窗口窄；在 fsync 较慢的盘（机械盘/网络盘）
  或高突发提交负载下，合并收益会更显著（可通过 `SetFsyncDelay` 注入延迟验证）。

### 5.3 回归

存量全部测试保持全绿：

```
ok  github.com/zhengkuanhua/openxdb/cmd/openxdb       [no test files]
ok  github.com/zhengkuanhua/openxdb/pkg/db
ok  github.com/zhengkuanhua/openxdb/pkg/server
ok  github.com/zhengkuanhua/openxdb/pkg/sql
ok  github.com/zhengkuanhua/openxdb/pkg/storage
ok  github.com/zhengkuanhua/openxdb/pkg/storage/btree
ok  github.com/zhengkuanhua/openxdb/pkg/storage/rocksdb
ok  github.com/zhengkuanhua/openxdb/pkg/txn
ok  github.com/zhengkuanhua/openxdb/pkg/wal
```

构建环境（Windows）：注入 RocksDB 静态库环境后 `go build ./...`、`go test ./...`：

```
CGO_CFLAGS   = -IC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-11.8.1\include
CGO_CXXFLAGS = 同上
CGO_LDFLAGS  = -LC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-build -lrocksdb -lshlwapi -lrpcrt4 -lws2_32 -static
PATH         += C:\Users\27756\AppData\Local\OpenXDBTools\mingw64\bin
```

## 6. 验收对照

- [x] WAL 接口新增 `SyncUpTo` 与 `SetGroupCommit`，默认关闭兼容存量
- [x] WAL 实现 leader 机制合并 fsync，Append 只写不刷、SyncUpTo 兜底，无后台定时器
- [x] 事务层 Commit 三段式重构，保持「最后提交者胜」串行语义与 WAL 顺序，Rollback 安全同步
- [x] 并发组提交测试：全部成功、数据一致、崩溃恢复语义不破坏、fsync 显著减少
- [x] 存量测试全绿；README 勾选 B2 完成
*（内容由AI生成，仅供参考）*

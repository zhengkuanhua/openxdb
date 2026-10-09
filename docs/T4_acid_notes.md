---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_51a3c7a1c36f11f18019525400248c00
    ReservedCode1: c4mVST4FOh4oMIB7wEH5oYnag54B12e8pavTPh6QVY4Cg7GBiYR+J6Zr9P/T2gBoL56efcYbEw6X4y2QX5qpo+YX2cRC3+GF+OJldhYm6GNTwOABtr9MQ2zDP5Ql1W7FEFuRs+SbKO84UE2TcyNx3kXBrVmJlKtolmwV1v7SIK06GcwH0TMTuHwy/x8=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_51a3c7a1c36f11f18019525400248c00
    ReservedCode2: c4mVST4FOh4oMIB7wEH5oYnag54B12e8pavTPh6QVY4Cg7GBiYR+J6Zr9P/T2gBoL56efcYbEw6X4y2QX5qpo+YX2cRC3+GF+OJldhYm6GNTwOABtr9MQ2zDP5Ql1W7FEFuRs+SbKO84UE2TcyNx3kXBrVmJlKtolmwV1v7SIK06GcwH0TMTuHwy/x8=
---



# T4 单机事务 ACID 收尾笔记

- 关联实现：`pkg/txn`（txn.go / txn_impl.go）、`pkg/wal`、`pkg/storage`、`pkg/sql`（会话级显式事务）、`pkg/server`
- 前置文档：`T3_txn_impl.md`（M1 T3 事务层初版）、`T8_p0_sql_enhancements.md`（P0 会话事务透传）、`T11_m2_replication.md`（M2 commit hook 衔接）、`T12_m3_sharding.md`（M3 分片路由）
- 状态：与当前代码实现对齐（M3 落地后复核）

## 1. 事务层架构

事务层位于 `pkg/txn`，向上服务 SQL Executor 与协议层，向下依赖 `pkg/wal`（写前日志）与 `pkg/storage`（KV 引擎抽象）。核心语义：**快照隔离读 + 提交串行化写**（WAL 先行 + 存储原子写 + 崩溃恢复重放）。

### 1.1 关键类型与接口（真实引用）

| 类型 / 接口 | 位置 | 职责 |
| --- | --- | --- |
| `txn.Txn` | `pkg/txn/txn.go` | 单事务句柄：`ID() / Get / Scan / Put / Delete / Commit / Rollback` |
| `txn.TxnManager` | `pkg/txn/txn.go` | 事务管理器：`Begin() / Recover() / SetCommitHook() / HookErr() / Close()` |
| `txn.TxnState` | `pkg/txn/txn.go` | `TxnActive=1 / TxnCommitted=2 / TxnRolledBack=3` |
| `txnManager` | `pkg/txn/txn_impl.go` | 管理器实现：`mu / st / wal / nextID / seqNext / applied / applyCond / commitHook / hookErr` |
| `txnImpl` | `pkg/txn/txn_impl.go` | 事务实现：`id / mgr / batch / ops / snap / state`；`ops` 为 `opEntry{isDelete, key, value}` 缓冲 |
| `storage.Storage` | `pkg/storage/storage.go` | KV 抽象：`Get / Scan / Write(batch) / Snapshot / Close` |
| `storage.WriteBatch` | `pkg/storage/storage.go` | 原子写批次：`Puts []KVPair` + `Deletes [][]byte` |
| `storage.Snapshot` | `pkg/storage/storage.go` | 一致性快照：`Get / Scan(r, limit) / Release` |
| `storage.KeyRange` | `pkg/storage/storage.go` | `[Start, End)` 半开区间 |
| `wal.WAL` | `pkg/wal/wal.go` | 顺序日志：`Append / SyncUpTo / SetGroupCommit / Replay / Truncate / LastLSN / Close` |
| `wal.WalEntry` | `pkg/wal/wal.go` | `LSN / TxnID / Batch *storage.WriteBatch / State`；`StatePrepare=1 / StateCommit=2 / StateRollback=3` |

存储引擎（RocksDB / B+Tree）都实现同一 `storage.Storage` 接口，事务层不感知引擎差异；`WriteBatch` 的 Puts/Deletes 直接映射到引擎的原子写原语。

## 2. 快照隔离（Snapshot Isolation）

- `Begin()`（`txnManager.Begin`）在获取事务 ID 时调用 `m.st.Snapshot()` 取得**一致性快照** `storage.Snapshot`，存入 `txnImpl.snap`。此后事务内所有未在本事务缓冲中修改的读，都以该快照为准（`txn.go` 中 `Get` 注释：*否则走 BEGIN 时刻快照（隔离读）*）。
- 读不阻塞、无读锁；写不直接落存储，先进内存缓冲（`txnImpl.batch` + `ops`）。
- 写写并发采用**提交串行化**：每个事务在 `Commit` 时领取单调递增序号 `seq = m.seqNext++`，通过 `applyCond` 等待 `seq == m.applied+1` 后按序号顺序应用存储（详见 §6），保证提交顺序全局一致。
- 快照在 `finish()`（提交/回滚）时 `snap.Release()` 释放。

## 3. read-your-writes（读写自见）

事务内对自身未提交写入立即可见，合并策略位于 `txnImpl`：

- **`Get(key)`**：逆序遍历 `t.ops`（最新写优先）：命中 `opEntry` 且非删除 → 返回缓冲值；命中且 `isDelete=true` → 返回 `storage.ErrNotFound`（tombstone 可见）；全部未命中 → 回退 `t.snap.Get(key)`。
- **`Scan(r, limit)`**：先取 `base = t.snap.Scan(r, 0)`；用 `dirtyContains` 标记快照中被本事务写过的键并从 `merged` 移除；随后逆序应用 `ops`（Put 覆盖 / Delete 移除）；最后按键升序输出，支持 `limit` 截断。
- **SQL 层会话**：`BEGIN` 后所有语句经 `getTx()` 复用同一 `curTx`（见 §5），天然满足 read-your-writes；autocommit 模式下单语句内部（如 INSERT 后同事务内检查）也走同一事务句柄。

## 4. Tombstone 标记（删除）

- `Delete(key)`：追加 `batch.Deletes` 并记录 `opEntry{isDelete: true}`，不立即触碰存储。
- 读路径：`Get` 命中 isDelete → `ErrNotFound`（对自身表现为"已删除"）；`Scan` 从合并结果 `delete(merged, ks)`。
- 提交后：`m.st.Write(t.batch)` 将 Puts/Deletes 作为同一 `WriteBatch` 原子应用到存储引擎（RocksDB Delete / B+Tree 删除），tombstone 仅存在于引擎层的删除操作，不引入版本链。
- 隔离性：事务内删除对其他未提交事务不可见（快照隔离）；已提交删除对其他事务的后续读不可见。

## 5. 主键唯一性（ACID-C）

行键编码见 `pkg/sql/meta.go`：`EncodeTableKey(tableID, pk) = 's' + tableID:8B + pk`（M3 后物理键外层加 region 前缀 `r{regionID:8B}`，内层不变）。

唯一性约束在 SQL 层 `pkg/sql/executor.go::insertRow` 实现：

1. `buildRow` 类型强制 → `pkOf` 提取主键字节（INT 定长 Big Endian / TEXT 原字节）；
2. `locateRow(meta, pk)` 按 region 路由定位（M3），`rowKeyAt(meta, rid, pk)` 构造物理键；
3. `tx.Get(key)` 探测：
   - 命中（`err == nil`）→ 主键已存在：`dupSkip=false`（普通 INSERT）返回 `&SQLError{Msg: "duplicate primary key: " + meta.Name}`；`dupSkip=true`（CSV 导入）返回 `(false, nil)` 跳过该行；
   - 非 `storage.ErrNotFound` 的其他错误向上传播；
   - `ErrNotFound` → 继续写入：`tx.Put(key, raw)` 并维护二级索引（`putIndexKeys`）。

检测与写入在同一事务内完成（探测走 read-your-writes + 快照），并发插入的唯一性由提交串行化与 WAL 顺序保证，无"检查后写入"的竞态窗口（单机内核按提交序号串行应用）。

CSV 导入语义（`pkg/sql/csv.go`）：主键重复行跳过（不覆盖、不报错）；任一行的列数/类型非法则整体报错回滚（原子导入）。

## 6. 会话事务流程（BEGIN / COMMIT / ROLLBACK）

### 6.1 语句路由

- 协议层 `pkg/server` 将 `BEGIN / COMMIT / ROLLBACK` 识别为 SQL 关键字路由到引擎（T8：`sql.IsSQL` 关键字列表含 BEGIN/COMMIT/ROLLBACK；server 原生支持显式事务命令）。
- SQL AST：`BeginStmt / CommitStmt / RollbackStmt`（`pkg/sql/ast.go`）；`Executor.Execute` 分别转发到 `beginTx() / commitTx() / rollbackTx()`。

### 6.2 Executor 会话事务状态

`pkg/sql/executor.go`：

| 成员 / 函数 | 行为 |
| --- | --- |
| `mu / curTx / inTx` | 会话事务句柄与开启标志 |
| `beginTx()` | `inTx` 已开 → `"transaction already started"`；否则 `e.tm.Begin()` 存入 `curTx`、置 `inTx=true` |
| `commitTx()` | 未开 → `"no active transaction"`；否则取 `curTx`、清状态、`tx.Commit()` |
| `rollbackTx()` | 未开 → `"no active transaction"`；否则取 `curTx`、清状态、`tx.Rollback()` |
| `getTx()` | 显式事务内返回 `curTx`（复用）；否则 `e.tm.Begin()` 新建临时事务 |
| `autocommit()` | `!e.inTx`：仅在非显式事务时为 true |

所有 DDL/DML 统一模式：`tx := e.getTx()` + `auto := e.autocommit()`；`auto` 时 `defer tx.Rollback()`（失败兜底）且语句成功末尾显式 `tx.Commit()`；显式事务内不自动提交，由用户的 `COMMIT / ROLLBACK` 决定。`BEGIN` 后 DDL（建表/删表/建索引）同样在事务内执行，受 ROLLBACK 保护。

### 6.3 事务状态错误

已结束事务上的读写/提交/回滚由 txn 层拦截：`txn.ErrTxnClosed`（"txn: transaction already finished"）。

## 7. 提交 / 回滚路径与 WAL、存储原子写的关系

### 7.1 Commit（`txnImpl.Commit`）完整路径

1. **空事务短路**：`batch` 无 Puts/Deletes → 直接 `finish(TxnCommitted)`，不落 WAL。
2. **WAL 先行**：`wal.Append(&wal.WalEntry{TxnID, Batch, State: StateCommit})`，分配 `LSN`；默认 Append 即 fsync（组提交开启时仅写缓冲）。
3. **领取提交序号**：`seq = m.seqNext++`（全局单调）。
4. **持久化屏障**：`wal.SyncUpTo(lsn)` 阻塞至该 LSN 落盘（leader 机制合并 fsync；组提交下多个事务共享一次刷盘）。
5. **提交串行化**：`for seq != m.applied+1 { m.applyCond.Wait() }`——按序号顺序等待，保证存储应用顺序与提交顺序一致。
6. **存储原子写**：`m.st.Write(t.batch)`——一个 `WriteBatch`（Puts + Deletes）原子应用到引擎，`applied++` 并广播。
7. **commit hook**：`m.commitHook(lsn, seq, batch)`（M2 挂接点，见 §8），hook 错误仅记 `hookErr`。
8. **收尾**：`finish(TxnCommitted)`（释放快照）。

> **ACID 落点**：
> - **原子性**：WAL 记录包含事务全部写；存储端 `WriteBatch` 单次原子应用；任一步失败（WAL Append / SyncUpTo / st.Write）都返回错误且不标记提交成功。
> - **持久性**：`SyncUpTo(lsn)` 保证提交点在 WAL 已 fsync 之后；崩溃后 `Recover` 重放已 Commit 记录。
> - **一致性**：唯一性检查 + 索引维护与行写入同一事务；提交串行化保证并发事务顺序确定。
> - **隔离性**：§2 快照隔离。

### 7.2 Rollback（`txnImpl.Rollback`）

- `wal.Append(&WalEntry{TxnID, Batch, State: StateRollback})` + `SyncUpTo(en.LSN)`（尽力落盘），随后 `finish(TxnRolledBack)`。
- **不应用存储**：回滚事务的写从不进入引擎；WAL 中的 Rollback 记录在 `Recover` 中被跳过（只应用 `StateCommit`），因此崩溃后回滚事务不产生任何效果（已落盘的"回滚"记录仅作审计/完整性标记，不重放）。

### 7.3 崩溃恢复（`txnManager.Recover`）

- `wal.Replay(func(e *WalEntry))` 从文件头顺序重放；**仅对 `State == wal.StateCommit` 的记录**执行 `m.st.Write(e.Batch)`（幂等，可重复调用）；`StatePrepare` 为未来两阶段提交预留，`StateRollback` 不应用。
- 恢复成功后 `m.seqNext = m.applied = commits`，后续新事务序号从恢复点继续，保证提交顺序与 WAL LSN 单调衔接。
- WAL 文件格式（`pkg/wal/wal.go` 常量）：`magic(9B)+version(2B)` 头；每条记录 `crc(4B)|len(4B)|lsn(8B)|txn(8B)|state(1B)|nputs(4B)|ndeletes(4B)|body`；crc 覆盖 len 到记录结尾；损坏时报 `wal.ErrCorrupt` 中止重放。

### 7.4 组提交（B2）

`wal.SetGroupCommit(enabled)`：开启后 `Append` 只写不刷，由 `SyncUpTo` 合并 fsync（首个等待者一次刷盘覆盖全部 pending 记录后广播），无后台定时器。事务提交路径不受影响（`Commit` 始终经 `SyncUpTo` 落盘后才应用存储）。

## 8. M2 commit hook 衔接（Binlog 顺序 = 提交顺序）

- `TxnManager.SetCommitHook(hook func(lsn storage.LSN, seq uint64, batch *storage.WriteBatch) error)` 注册复制回调；`Commit` 在**存储应用成功后、同一提交串行化锁内**按 `seq` 递增顺序调用 `hook(lsn, seq, batch)`。
- `seq` 全局单调（`m.seqNext++`），因此 **hook 调用顺序 = binlog 追加顺序 = 事务提交顺序**；`lsn` 为 WAL LSN，幂等恢复时 binlog 可从任一提交点衔接。
- hook 返回错误仅写入 `m.hookErr`（`HookErr()` 可查），**不影响事务提交成功语义**——复制推送为异步，主库不因备库失败阻塞提交（详见 T11）。

## 9. 验证

- `pkg/txn/txn_test.go`：快照隔离、read-your-writes、tombstone、提交/回滚、恢复重放等用例。
- `pkg/wal/wal_test.go`：追加、崩溃恢复、损坏检测、组提交。
- `pkg/sql`：含会话事务（BEGIN/COMMIT/ROLLBACK）与主键唯一性约束集成用例；T8 记录了 9 例二级索引与事务透传覆盖。
- 全仓 `go test ./... -count=1` 绿色（M3 落地后复核）。
*（内容由AI生成，仅供参考）*

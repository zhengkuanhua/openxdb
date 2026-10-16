---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_482e246ec82b11f1b5a6525400393706
    ReservedCode1: WWrZU69zRqKkwQmNonTCPvtnJR8jphdjwKwatw02JlLq6+xPeKZwBS0nHsGVlK+ekFZvx22DTNKogiPFuP8hsIU+j0hgilYraj+HejBbTJgFnkzhOkuWuk8sV6LLe3REBvl5tw1yXA2FKBwo1R7aoG45uFMUKxnYyxU/ouhPikJNL9qlVmHg+pWJX4Y=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_482e246ec82b11f1b5a6525400393706
    ReservedCode2: WWrZU69zRqKkwQmNonTCPvtnJR8jphdjwKwatw02JlLq6+xPeKZwBS0nHsGVlK+ekFZvx22DTNKogiPFuP8hsIU+j0hgilYraj+HejBbTJgFnkzhOkuWuk8sV6LLe3REBvl5tw1yXA2FKBwo1R7aoG45uFMUKxnYyxU/ouhPikJNL9qlVmHg+pWJX4Y=
---

# T33 M20 同步/半同步复制（Sync / Semi-Sync Replication）

- 批次：M20 分布式纵深（Distributed Depth）第一项
- 依赖：M2 异步复制（T11，`pkg/replication`）、M16 在线配置热更新（`pkg/sql/config.go`）
- 状态：已实现，测试全绿

## 1. 背景与目标

M2 实现了 master-follower 异步复制：主节点在事务提交应用存储后通过 commit hook 将 binlog 落盘并异步推送，从节点幂等应用后回 ACK。异步模型下客户端拿到提交成功时，副本可能尚未收到数据，存在"主故障丢数据"窗口（RPO>0）。

T33 目标：在保持 M2 异步复制完全兼容（默认行为不变）的前提下，为主节点增加**同步复制**与**半同步复制**两种强一致/准强一致模式：

- **同步复制（sync）**：主节点在向客户端返回提交成功**之前**，等待**所有在线**副本确认该位点已落盘应用；任一在线副本超时未确认或没有在线副本时，提交返回超时错误（不静默降级），保证已确认事务不丢。
- **半同步复制（semi-sync）**：主节点等待**至少 N 个**在线副本 ACK 后才向客户端返回成功；ACK 超时或在线副本不足时**降级为异步**继续（记录降级状态，保证可用性），类似 MySQL semi-sync 的 `rpl_semi_sync_master_timeout` 语义。

## 2. 现状分析（M2）

### 2.1 复制链路

```
客户端 ──COMMIT──> txnManager ──(锁内)──> storage.Write ──> commitHook ──> MasterReplicator.Append
                                                    │                              │
                                                    │                 binlogStore.Append（fsync 落盘）
                                                    │                              │
                                                    └── 返回 nil（提交成功）      pushLoop ──> Follower ──> ApplyBinlog ──> ACK
```

关键事实（源码确认）：

- `txn_impl.go` Commit 阶段 3 在 `m.mu` 锁内依次：写存储 → `applied++` → 调用 `commitHook(lsn, seq, batch)` → `t.finish` → 解锁 → 返回 nil。**commitHook 在锁内执行**，错误仅记录 `HookErr`，不影响提交成功语义。
- `BinlogStore.Append` 写入即 `fsync`，binlog 落盘保证（RPO=0 at master）。
- `Follower.ApplyBinlog` 成功应用后立即回 ACK（ACK 帧语义 = "该 LSN 已应用落盘"）。
- `MasterReplicator.readLoop` 收到 ACK 后更新 `sc.info.lastAck`；`SlaveStatus` 暴露 `LastAck`。
- 协议帧：`len(4B)+type(1B)+payload`；HELLO/HELLO_OK/BINLOG/ACK/PING/PONG；`protoVersion=1`。
- 主节点维护 `clients`（在线连接）与 `known`（全部历史从节点）；`heartbeatLoop` 超时剔除离线连接（online=false）。

### 2.2 与 T33 的差距

1. `commitHook` 在锁内执行且不等待 ACK：同步等待会阻塞整个事务管理器锁，不可接受；且 hook 错误不回传 Commit 调用者。
2. `MasterReplicator` 无复制模式、无 ACK 聚合等待、无超时降级状态。
3. 无运行时切换配置入口（SQL / 命令行 / 配置文件）。

## 3. 总体设计

### 3.1 设计原则

1. **协议最小扩展、向后兼容**：同步/半同步完全复用 M2 的 BINLOG/ACK 帧（ACK 即"已应用落盘"确认，语义天然满足要求），不新增帧类型；仅 HELLO_OK 追加 1 字节复制模式（旧从节点按旧长度解析，兼容）。
2. **等待点移出事务锁**：新增"提交后置确认钩子"（`SetCommitWaitHook`）在 `m.mu` 解锁后、Commit 返回前调用，可安全阻塞等待 ACK；binlog 顺序仍由锁内 commitHook 串行保证。
3. **默认异步零开销**：`ModeAsync` 下 WaitAck 立即返回，M2 行为完全不变。
4. **模式切换热生效**：通过 db API / `SET replication_mode` / 配置文件 / 命令行参数多入口，均可运行时切换（`SET PERSIST` 可持久化）。

### 3.2 复制模式

| 模式 | 常量 | 提交语义 | ACK 不足/超时 | 无在线副本 |
|---|---|---|---|---|
| 异步 | `ModeAsync` | 不等待，立即成功（M2 原样） | — | 正常提交 |
| 半同步 | `ModeSemiSync` | 等待 ≥min(N, 在线) 个 ACK | 降级异步，提交成功 | 降级异步 |
| 同步 | `ModeSync` | 等待全部在线 ACK | 返回 `ErrAckTimeout`（提交报错） | 返回 `ErrNoSlave` |

参数：

- `N`（半同步最少确认副本数，默认 1，≥1 生效）
- `timeout`（ACK 等待超时，默认 10s，与 `HeartbeatTimeout` 一致；≤0 用默认值）

### 3.3 协议扩展

保持 `protoVersion=1`，不新增帧类型。仅 `HELLO_OK` payload 由 `version(2B)+lastLSN(8B)` 扩展为 `version(2B)+lastLSN(8B)+mode(1B)`（共 11B）：

- `decodeHelloOK` 兼容旧长度 10B（视为 `ModeAsync`），新长度 11B 读取 mode。
- 从节点 `Follower` 已确认 `lastLSN` 语义不变，ACK 行为不变，无需协议感知模式。
- 好处：从节点/运维工具可感知主端当前模式，便于诊断；协议向后兼容。

> 备注：不引入新帧的原因——"同步"只需主端聚合等待既有 ACK，ACK 帧已表达"已应用"；引入专用帧只会增加握手版本分裂风险，无信息增益。

### 3.4 主端 ACK 聚合等待器（MasterReplicator 扩展）

新增字段（`MasterReplicator`）：

```go
mode       ReplMode        // 复制模式（默认 ModeAsync）
semiAckN   int             // 半同步最少 ACK 数（默认 1）
ackTimeout time.Duration   // ACK 等待超时（默认 10s）
degraded   bool            // 半同步降级标志
degLSN     storage.LSN     // 触发降级的位点
degAt      time.Time       // 降级时间
```

新增方法：

- `SetReplicationMode(mode ReplMode, ackN int, timeout time.Duration) error`：校验参数并热切换；async 忽略 ackN/timeout；返回 `ErrInvalidMode` / `ErrBadAckN` / `ErrBadTimeout`。
- `ReplicationMode() ReplMode`、`SyncStatus() SyncStatus`（模式/N/超时/降级标志/降级位点/降级时间/最近等待耗时/当前是否在等待）。
- `WaitAck(ctx context.Context, lsn storage.LSN) error`：核心等待器。

`WaitAck` 算法：

```
1. mode==ModeAsync → 立即返回 nil（M2 零开销）。
2. 快照在线副本：online = {c in clients | info.online && lastAck < lsn}（只统计未达标的，减少扫描）。
   need = online 总数
   - ModeSync:  need = len(online)（全部在线都要达标）；need==0 → ErrNoSlave
   - ModeSemiSync: need = min(semiAckN, len(online))；need==0 → 立即降级返回 nil
3. 轮询（10ms 间隔）直到 need==0 / ctx 取消 / deadline 到达：
   - readLoop 已把 ACK 写入 sc.info.lastAck（加锁读取）
   - semi-sync 超时 → 置降级标志（degraded=true, degLSN=lsn, degAt=now），返回 nil
   - sync 超时 → 返回 ErrAckTimeout
4. 等待期间新增在线副本不改变 need（已按快照计算），避免等待期间新连接导致提交无限阻塞。
```

实现要点：

- 轮询加锁读取 `clients`/`info.lastAck`，与 `readLoop` 的写天然互斥。
- `ctx` 用于外部取消（DB 关闭时中断等待，避免 Close 阻塞）。
- 降级只记状态不记入历史队列；`SyncStatus` 每次读取返回当前视图；半同步从降级中恢复（后续提交 ACK 达标）时 `degraded=false`（新提交成功即视为恢复，旧降级事件在 `DegradedCount` 累计）。

### 3.5 事务提交路径改造（pkg/txn）

新增可选钩子（不影响 M2）：

```go
// TxnManager 新增
SetCommitWaitHook(hook func(lsn storage.LSN, seq uint64) error)
```

`Commit` 流程改动（`txn_impl.go` 阶段 3 末尾）：

```
m.mu.Unlock()                      // 解锁（原有）
if m.commitWaitHook != nil {       // 新增：锁外、Commit 返回前
    if herr := m.commitWaitHook(lsn, seq); herr != nil {
        return herr                // sync 超时 → 提交返回错误
    }
}
return nil
```

- `commitHook`（锁内）保持原样：Append binlog 落盘 + 唤醒推送，保证 binlog 顺序 = 提交顺序。
- `commitWaitHook`（锁外）执行 `WaitAck`：可阻塞最长 timeout，不阻塞其他事务提交。
- 语义说明：sync 超时返回错误时，本地事务**已提交**（WAL+存储+binlog 均已落盘），但客户端收到失败——这是分布式系统的"提交不确定"窗口（与 2PC 的 COMMIT_UNCERTAIN 同构）。客户端重试幂等（同 key 写入按 binlog LSN 幂等应用）。文档明确此语义。
- semi-sync 降级返回 nil：提交成功，仅记录降级事件。

### 3.6 DB 层接线（pkg/db）

`StartReplicationTLS` 在原有 commitHook 基础上追加：

```go
d.Txn.SetCommitHook(func(lsn, seq, batch) error { return r.Append(...) }) // M2 原样（锁内）
d.Txn.SetCommitWaitHook(func(lsn, seq) error {                            // T33 新增（锁外）
    return r.WaitAck(context.Background(), lsn)
})
```

新增 API：

- `SetReplicationMode(mode replication.ReplMode, ackN int, timeout time.Duration) error`：透传 `r.SetReplicationMode`。
- `ReplicationSyncStatus() replication.SyncStatus`：透传 `r.SyncStatus`。
- 未启用复制时调用 SetReplicationMode 返回明确错误（`ErrReplicationNotStarted`）。

### 3.7 SQL / 配置切换（pkg/sql + cmd）

复用 M16 配置注册表（`configSpecs`）新增三个热更新键：

| 键 | 值语义 | 默认 |
|---|---|---|
| `replication_mode` | `async` / `semi-sync` / `sync` | `async` |
| `replication_ack` | 半同步最少确认副本数（正整数） | `1` |
| `replication_timeout` | ACK 超时毫秒（正整数，≤0 用默认） | `10000` |

- `SET replication_mode = 'sync'` 立即热切换；`SET PERSIST replication_mode = 'sync'` 落盘持久化（重启生效）。
- `SHOW CONFIG` 自动展示；`SHOW REPLICATION STATUS` 保持原有输出（新增模式列由 db 层 ReplicationStatus 增强可选）。
- `sql.Executor` 通过接口 `ReplicationModeSetter { SetMode(mode string, ackN int, timeoutMs int64) error }` 转发，由 `db.Open` 注入（避免包循环）。
- cmd/openxdb：`--replication-mode` / `--replication-ack` / `--replication-timeout` 命令行参数 + `[server]` 配置段 `replication_mode` / `replication_ack` / `replication_timeout`，优先级 持久化 < 配置文件 < 命令行（与现有参数一致）。

### 3.8 副本故障处理

| 场景 | sync | semi-sync |
|---|---|---|
| 副本离线（heartbeat 超时剔除） | 后续提交等待全部**在线**副本；离线副本不参与计数 → 若还有在线副本，提交成功（旧副本回连后从 lastAck 续推补齐） | 在线数 < N → 立即降级异步 |
| 无在线副本 | 提交返回 `ErrNoSlave`（拒绝伪同步） | 立即降级异步，提交成功 |
| 副本慢（ACK 迟到） | 超时 → `ErrAckTimeout`，事务报错（本地已提交，重试幂等） | 超时 → 降级异步，提交成功 |
| 副本重连 | `pushLoop(from=lastAck)` 续推；后续提交的 ACK 等待恢复 | 同左，降级状态在下一次 ACK 达标时自动恢复 |
| 主节点崩溃 | 已提交事务 WAL+binlog 均落盘；恢复后从节点按 lastAck 续推，不丢已确认事务 | 同左 |

## 4. 文件改动清单

| 文件 | 改动 |
|---|---|
| `pkg/replication/master.go` | MasterReplicator 增加模式/降级字段；新增 SetReplicationMode/ReplicationMode/SyncStatus/WaitAck |
| `pkg/replication/protocol.go` | HELLO_OK 编解码兼容扩展 mode 字节 |
| `pkg/replication/sync.go`（新） | ReplMode 类型、SyncStatus、错误定义 |
| `pkg/replication/sync_test.go`（新） | 主端等待器单测（fake slave） |
| `pkg/txn/txn.go` / `txn_impl.go` | TxnManager 接口 + Commit 锁外提交后置确认钩子 |
| `pkg/db/db.go` | StartReplicationTLS 接线 commitWaitHook；SetReplicationMode/ReplicationSyncStatus |
| `pkg/db/db_sync_repl_test.go`（新） | 主从集成测试（sync 提交等待、semi-sync 降级、sync 超时报错） |
| `pkg/sql/config.go` / `sql.go` / `executor.go` | 注册 replication_mode/replication_ack/replication_timeout 配置键与转发 |
| `cmd/openxdb/main.go` | --replication-mode/--replication-ack/--replication-timeout 参数与 [server] 配置段 |
| `README.md` | Status/Roadmap 更新（T33 勾选 + v0.22.0-alpha） |

## 5. 测试计划

- 单元（`pkg/replication/sync_test.go`，fake slave 协议客户端）：
  - async 模式 WaitAck 立即返回（M2 兼容）；
  - sync 模式：1/2 个 slave ACK 后返回；无 slave → `ErrNoSlave`；slave 不 ACK 超时 → `ErrAckTimeout`；
  - semi-sync 模式：N=1 时 1 个 ACK 即返回；无 slave/超时 → 降级返回 nil 且 SyncStatus 可见；
  - HELLO_OK 长度兼容（10B 旧帧可解，11B 新帧带 mode）。
- 集成（`pkg/db/db_sync_repl_test.go`，真实主从 DB + 随机端口）：
  - sync：主提交 INSERT 后从库可查且提交成功（提交返回前副本已应用）；
  - sync 无副本：INSERT 返回超时错误；
  - semi-sync：N=1 + 1 从库 → 提交成功且从库可查；无从库 + 短超时 → 降级提交成功，`SyncStatus.Degraded=true`；
  - 模式热切换：提交中切换 async→sync→async 行为正确；
  - 回归：async 模式提交与 M2 完全一致（不等待）。
- 全仓：`go test ./... -count=1 -p 1`（串行避免端口竞争）。

## 6. 版本与文档

- README Status：推进至 **v0.22.0-alpha**（M20 第一批 T33 同步/半同步复制）。
- Roadmap：勾选 T33，链接本文档。
*（内容由AI生成，仅供参考）*

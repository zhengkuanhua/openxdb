---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_19817006c31311f18019525400248c00
    ReservedCode1: NUVVwRLEHr2U4tLoWkkZ1u5gNl/kAEe2Zbdb96Aa8OKIz9j0MyajwMi2Wldmd5tOQ+Pf2N0i8ZD4kUuT/C01LLlmVWnYu50fclV1hlw4PIQnwktwv3Rlf+l8ZUP/fjd9AbLF6cquDBqs1OMQbkERqXp0qtOlxMSQ/QSYwvZBBukDPkoDkSPDLzHcP4Y=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_19817006c31311f18019525400248c00
    ReservedCode2: NUVVwRLEHr2U4tLoWkkZ1u5gNl/kAEe2Zbdb96Aa8OKIz9j0MyajwMi2Wldmd5tOQ+Pf2N0i8ZD4kUuT/C01LLlmVWnYu50fclV1hlw4PIQnwktwv3Rlf+l8ZUP/fjd9AbLF6cquDBqs1OMQbkERqXp0qtOlxMSQ/QSYwvZBBukDPkoDkSPDLzHcP4Y=
---

# T11: M2 复制落地（Binlog / Replicator / Follower / 心跳）

> 里程碑：M2（多节点）第一阶段 —— 从单机到主从复制的第一步落地。
> 关联：T7 M2/M3 接口预留 → 本实现；README Roadmap M2 勾选。

## 1. 目标与范围

在单机事务（WAL 先行 + 提交串行化）之上增加主从复制：

1. **Binlog 生成与落盘**：事务 Commit 时把写批次（Puts/Deletes）以 BinlogEntry 记录落盘；
2. **Replicator（主端）**：监听复制端口、握手（版本/角色/起始位点）、按位点流式推送、断线续传、多 slave；
3. **Follower（从端）**：连接主节点、握手携带续传位点、接收 binlog 幂等应用、已应用位点可查询；
4. **心跳**：PING/PONG 双向心跳，超时判离线；
5. **集成**：pkg/db 装配复制组件并挂接事务提交；cmd/openxdb 提供 `--replica-port` 与 `replica` 子命令；
6. 不破坏单机事务语义；全仓测试不回归。

## 2. 架构总览

```
                       ┌────────────────────────────┐
                       │  Master (主节点)           │
                       │                            │
   txn.Commit ──hook──▶│  binlog.log  (BinlogStore) │
                       │       │  Append(异步推送)   │
                       │       ▼                    │
                       │  MasterReplicator          │
                       │   acceptLoop / pushLoop /  │
                       │   heartbeatLoop            │
                       └───────┬─────────┬──────────┘
                      TCP :0（随机）      │
                       ┌───────┴─────────▼──────────┐
                       │  Follower (从节点)          │
                       │  Start: 握手(from=applied+1)│
                       │  loop: 收 BINLOG → 幂等应用 │
                       │        → ACK; 收 PING → PONG│
                       │  本地 storage 应用          │
                       └────────────────────────────┘
```

- **Binlog 与 WAL 相互独立**：WAL 负责单机崩溃恢复（txn 层）；Binlog 负责跨节点复制（复制层）。
  二者位点语义不同但均单调递增。
- **事务提交路径不变**：Commit 仍走「WAL Append → SyncUpTo → 按序号串行应用存储」。
  复制通过 **commit hook** 在存储应用成功后同步追加 Binlog（锁内串行，保证 Binlog 顺序 = 提交顺序）。

## 3. 位点语义

| 概念 | 定义 |
|---|---|
| Binlog LSN | BinlogStore.Append 自动分配，从 1 开始严格递增、文件内唯一；**复制位点 = Binlog LSN** |
| CommitLSN | 源事务在 WAL 中的 LSN（关联审计用，不参与复制续传） |
| AppliedLSN（从端） | 已成功应用到本地存储的最大 Binlog LSN；续传起始位点 = AppliedLSN + 1 |
| LastAck（主端） | 各 slave 已确认（ACK 帧携带）的最大位点；断线时保留供重连续传观测 |

幂等标识：BinlogEntry.LSN 即幂等标识 —— 从端按「LSN <= applied 跳过；LSN == applied+1 应用；
LSN > applied+1 报 ErrGap」规则保证同一记录只应用一次。

## 4. 协议消息格式

二进制帧：`[len:4B BE][type:1B][payload:lenB]`，首帧 magic 校验（"OPENXDBRPL"）。
当前协议版本 `protoVersion = 1`。

| type | 名称 | payload | 方向 | 说明 |
|---|---|---|---|---|
| 1 | HELLO | `role:1B, fromLSN:8B BE` | slave→master | 握手：角色（1=slave）+ 请求起始位点 |
| 2 | HELLO_OK | `protoVersion:4B BE, lastLSN:8B BE` | master→slave | 握手应答：主端版本与当前最大位点 |
| 3 | BINLOG | `lsn:8B, commitLsn:8B, schemaVer:4B, state:1B, nPuts:4B, nDel:4B, body` | master→slave | 复制条目（body 为 Put 键值对 + Delete 键列表） |
| 4 | ACK | `lsn:8B BE` | slave→master | 从端确认已应用位点 |
| 5 | PING | `ts:8B BE`（毫秒时间戳） | master→slave | 心跳探测 |
| 6 | PONG | `ts:8B BE`（回显） | slave→master | 心跳应答 |

- 主端每 `HeartbeatInterval` 向在线 slave 发 PING；从端收到立即回 PONG。
- 空闲时主端也按 `HeartbeatInterval` 发送 PING，使从端读超时成为「失联」判定依据。

## 5. 一致性模型（异步复制）

- **本地提交 = 同步**：主节点事务提交语义完全不变（WAL fsync 后返回成功）。
- **复制推送 = 异步**：commit hook 在提交路径内同步落 Binlog（保证 Binlog 完整），
  推送由 Replicator 后台 goroutine 按位点增量下发；主节点不等待 slave ACK。
- **结果**：主从可能短暂不一致（异步窗口），从端数据最终收敛到主端已提交集合；
  **不支持读扩展的一致性保证**（follower 读可能滞后），文档不承诺「强一致读」。
- **取舍**：同步复制（等 ACK 才返回提交成功）会放大提交延迟并引入可用性耦合
  （slave 故障阻塞主节点写入），本阶段不做；后续 M2 阶段可加同步模式开关。

## 6. 幂等应用规则

从端 `ApplyBinlog(e)`：

1. `e.LSN <= applied`：已应用过（重连重复推送），跳过，返回 nil —— **幂等**；
2. `e.LSN == applied+1`：正常顺序，`storage.Write(e.Batch)` 后推进 applied；
3. `e.LSN > applied+1`：流缺口（丢失），返回 `ErrGap` 并置 offline，等待重连续传。

主端 pushLoop 永远从「slave 请求的起始位点」顺序读取 BinlogStore，天然保证无缺口。

## 7. 心跳与故障处理

| 场景 | 行为 |
|---|---|
| 从端无响应超时（主端） | heartbeatLoop 检测 `now-lastRecv > HeartbeatTimeout`，标记 offline 并断开连接 |
| 主端失联（从端读超时） | Follower.ReadTimeout 内未收到任何帧 → Start 返回错误，State=offline |
| 从端断线 | 主端保留其 lastAck；重连握手携带 from=applied+1 续传 |
| 从端重连 | 同一 Follower 实例内存位点续传；重启后 applied=0 全量补（本阶段不持久化 applied，见取舍） |
| 主端 Close | 关闭监听、全部 slave 连接与 binlog 文件 |

超时参数：MasterReplicator.HeartbeatInterval（默认 2s）、HeartbeatTimeout（默认 5s）；
Follower.ReadTimeout（默认 10s）。测试中可调小以快速验证。

## 8. 接口与接线

- `pkg/replication`：`BinlogStore`（OpenBinlog/Append/ReadFrom/LastLSN/Close）、
  `MasterReplicator`（NewMaster/Serve/Append/Pull/Ack/LastLSN/SlaveStatus/Close）、
  `Follower`（NewFollower/ApplyBinlog/AppliedLSN/Status/Start/Stop）、
  `BinlogEntry{ LSN, CommitLSN, Batch, SchemaVer }`、`SlaveStatus{ NodeID, Remote, Online, LastAck, LastSeen }`。
- `pkg/txn`：`TxnManager.SetCommitHook(func(lsn, seq, batch) error)` + `HookErr()`；
  Commit 在存储应用成功后、返回前调用 hook（锁内，串行）。
- `pkg/db`：`DB.StartReplication(addr)` / `DB.StartFollower(masterAddr)` / `DB.ReplicationStatus()`
  / `DB.Close()` 关闭复制组件；`BinlogFile = "binlog.log"`。
- `cmd/openxdb`：`start --replica-port <port>` 启动复制端口；`replica --data-dir <dir> --master <addr>` 启动从端。

## 9. 启动示例

```sh
# 主节点（数据目录 master/data，TCP 7788，复制端口 7790）
openxdb init --data-dir ./master
openxdb start --data-dir ./master --port 7788 --replica-port 7790

# 从节点（数据目录 slave/data，连接主节点复制端口）
openxdb init --data-dir ./slave
openxdb replica --data-dir ./slave --master 127.0.0.1:7790
```

## 10. 设计取舍与后续

- **Binlog 独立文件而非复用 WAL**：职责分离（恢复 vs 复制）；WAL 含 Rollback 审计记录与
  事务结构，复制不需要。代价是双写一份磁盘 I/O（可接受，本阶段正确性优先）。
- **Binlog 顺序 = 提交顺序**：hook 在 txn 锁内调用，保证 Binlog LSN 顺序与 WAL/存储应用顺序一致，
  崩溃后从端与主端数据等价。
- **Follower 不持久化 applied 位点**：重启后从 0 全量补拉（Binlog 全量重放，幂等安全）。
  持久化 applied 留待后续（可在 data dir 落一个小位点文件）。
- **异步复制窗口**：见 §5。读扩展需在 SQL 层标记「允许读滞后」，后续阶段实现。
- **不处理从端写冲突**：本阶段约定从端仅通过复制写入；直接写从端会造成分叉，后续版本拒绝或标记。

## 11. 测试

见 `pkg/replication/replication_test.go`（binlog 落盘/回读/重开/损坏检测、幂等与跳号、
主从全链路、断线重连续传、主端心跳离线、从端心跳超时）与
`pkg/db/db_replication_test.go`（提交后 binlog 落盘、主从真实链路一致性、幂等应用）。
所有端口使用 `:0` 随机端口，超时参数调小，CI 不卡死。
*（内容由AI生成，仅供参考）*

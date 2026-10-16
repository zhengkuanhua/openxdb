---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_492a442ec82b11f1b5a6525400393706
    ReservedCode1: YXenUO9V98lvg9qY/am2ILwoOeX8WucZ4mZaI+yjEFNh5vX1eMd9w9dR4kD20P4w8SkdMVXfmJbHRiCaKnrQR84WgVJZfnOTR/naiq/J7F2tzQdRBk3QhP7Vpqi1Kv9FJUvFp/h0BGP3EAEd39U665nO8H4psskKjPqOg9wEblxm77UG3gL6j2kvrbY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_492a442ec82b11f1b5a6525400393706
    ReservedCode2: YXenUO9V98lvg9qY/am2ILwoOeX8WucZ4mZaI+yjEFNh5vX1eMd9w9dR4kD20P4w8SkdMVXfmJbHRiCaKnrQR84WgVJZfnOTR/naiq/J7F2tzQdRBk3QhP7Vpqi1Kv9FJUvFp/h0BGP3EAEd39U665nO8H4psskKjPqOg9wEblxm77UG3gL6j2kvrbY=
---

# T34 M20 跨节点 DDL（Cross-Node DDL）
- 批次：M20 分布式纵深（Distributed Depth）第二项
- 依赖：M2 复制链路（`pkg/replication`）、M4 节点拓扑/路由（`pkg/cluster`）、M5 分布式 2PC 写（`pkg/cluster/txn2pc.go`）、M8 分布式快照隔离、M9 在线 DDL（`pkg/sql` + `docs/T22_m9_online_ddl.md`）、T33 同步/半同步复制
- 状态：已实现，测试全绿

## 1. 背景与目标

M9 在线 DDL（`execAlterTable` → `ddlCommit`）在**单节点语义**下工作：表目录 `m:tables`（全局键、无 region 前缀）只写协调者本地；`ALTER` 产生的数据行重写 ops 已能跨节点（`groupOps` → 远端 region ops 走 M5/M8 2PC），但**元数据不广播**，导致多节点集群中：

1. 远端数据节点本地 `m:tables` 缺失或滞后该表 → 该节点独立执行 `SHOW TABLES` / `INSERT` / `SELECT`（针对本地持有的 region）时出现 `table not exists` 或按**旧列布局**解析行数据，产生列错位/解码错误；
2. `ALTER ADD COLUMN` 后，协调者数据行已带新列，而远端节点按旧结构读行 → 跨节点查询结果不一致；
3. `CREATE/DROP TABLE` 在任意节点执行后，其他节点目录无感知，集群表视图分裂。

T34 目标：**在保持单节点 DDL 行为完全兼容（默认不触发）的前提下，为集群增加 DDL 元数据广播通道**，使任意节点执行的 DDL 都能收敛到全集群表目录，并明确失败/离线处理策略与复制链路（含 T33）的配合关系。

## 2. 现状分析

### 2.1 单节点 DDL 链路（源码确认）

- `execCreateTable` / `execDropTable` / `execAlterTable` 均以 `loadTables(tx.Get)` 读目录、改内存 `[]*TableMeta`、`saveTables` 序列化、`tx.Put(metaKey, raw)` 写回，autocommit 时 `tx.Commit()`；
- `execAlterTable` 的 `ADD_COLUMN` 走 `ddlCommit`：`tx.Put(metaKey)` 后把数据重写 ops 交给 `exec2pcWrite`（`groupOps` → 本地 `applyOps` + 远端 `mgr.PrepareTxn/CommitTxn`），2PC 提交随事务原子完成；`ADD_INDEX/DROP_INDEX` 不走 `ddlCommit`，直接本地提交；
- `execDropTable` 用 `scanMerged`（本地扫描，因 ASSIGN REGION 约定数据保留源节点副本）删除本地行与索引键，并从 `m:tables` 移除条目、清理 `m:regions` 与权限/角色元数据；
- 表目录只有一个全局键 `m:tables`，无 region 前缀，即**目录天然是集群级状态**，缺的只是传播机制。

### 2.2 既有跨节点能力

- M4 `pkg/cluster`：`Manager`（节点拓扑 `ListNodes/HasRemoteNodes/IsDown`、短连接 `Client.roundTrip`、心跳 `StartHeartbeat` + `OnDown` 回调）、`Server`（`handleConn` 按帧分发 + `SetRouter/SetManager/SetTSO` 注入式装配）；
- M5/M8 2PC：`PrepareTxn/CommitTxn/AbortTxn` + 协调者 `m:2pc:coord:<txid>` 崩溃恢复；帧 22-27；`REGION_PUSH` 帧 20/21（`put.go`）；TSO 帧 28/29；
- T33：`commitHook`（锁内 binlog 全量 batch，含 metaKey Put）→ 从节点 replay 自动获得目录变更；`commitWaitHook`（锁外 ACK 等待）与元数据无关，不感知 DDL；
- SQL 四层架构 lexer/ast/parser/executor：T34 **不新增语法**，只扩展 executor 提交后的行为。

### 2.3 与 T33 的差距 / 设计缺口

1. 无 DDL 元数据广播帧与节点侧 apply 逻辑；
2. 无协调者侧广播目标选择、失败记录（pending）与重放机制；
3. 未定义与 binlog 复制链路的先后/幂等关系。

## 3. 总体设计

### 3.1 设计原则

1. **单机零回归**：`mgr == nil` 或 `!HasRemoteNodes()` 时广播路径零触发，单节点 DDL 完全走原路径；
2. **协议最小扩展**：新增 1 对帧 `DDL_PUSH(30) / DDL_PUSH_OK(31)`，复用既有 `len(4B)+type(1B)+payload(JSON)` 帧格式；载荷**不引用 sql 类型**（`Meta []byte` 携带序列化表条目），避免 `cluster`↔`sql` 循环依赖；
3. **本地提交为权威**：DDL 的原子性/崩溃安全沿用 M9 语义（本地 `tx.Commit` 成功即成功）；广播是**最终一致**的收敛通道，不参与 DDL 事务原子性；
4. **失败不阻塞**：广播失败/节点离线 → 协调者记录 pending（内存 + 落盘），DDL 仍返回成功；节点恢复后重放收敛（类 semi-sync 降级语义，优先可用性）；
5. **幂等收敛**：表 ID 由 `nextTableID` 单调分配不复用，按 `TableID` upsert/delete 天然幂等，重复广播/与 binlog 重放并发安全；
6. **只动目录不动数据**：T34 只广播 `m:tables` 目录变更；数据行 ops 仍走既有 2PC（ALTER 重写）/本地（DROP 本地副本），不新增数据迁移逻辑。

### 3.2 协议扩展（pkg/cluster/ddl.go）

```
30 DDL_PUSH    -> 31 DDL_PUSH_OK
```

```go
type DDLPushReq struct {
    DDLID   string `json:"ddl_id"`            // 协调者生成，审计/测试用
    Op      string `json:"op"`                // "create" | "alter" | "drop"
    TableID uint64 `json:"table_id"`
    Name    string `json:"name"`
    Meta    []byte `json:"meta,omitempty"`    // Op=create/alter：TableMeta 的 JSON 字节
}

type DDLPushResp struct {
    Err  string `json:"err,omitempty"`
    Done bool   `json:"done"`
}
```

- `Manager.PushDDL(nodeID string, req DDLPushReq) error`：短连接发送并读取 `DDL_PUSH_OK`（复用 `ensureRemote` + `clientFor` 模式）；
- `Server.SetDDLApply(fn func(DDLPushReq) error)`：注入节点侧 apply 回调（由 `db.Open` 装配 `sql.Executor` 的方法），`handleConn` 新增 `case msgDDLPush: s.handleDDLPush(conn, payload)`，apply 失败回 `DDLPushResp{Err}`。

### 3.3 节点侧 apply（pkg/sql/ddl_broadcast.go）

`Executor.ApplyRemoteDDL(req DDLPushReq) error`（导出，供 db 装配）：

```
tx := tm.Begin()                      // 独立事务（与复制 replay 通道同构）
tabs := loadTables(tx.Get)
idx := indexByTableID(tabs, req.TableID)
switch req.Op:
  "drop":   idx >= 0 → tabs 移除该条目
  default:  Unmarshal(req.Meta) → TableMeta
            idx >= 0 → tabs[idx] = meta（同 ID 覆盖）
            否则     → tabs = append(tabs, &meta)
raw := saveTables(tabs); tx.Put(metaKey, raw); tx.Commit()
```

要点：
- **幂等**：同 ID 覆盖/删除，重复收到同一 `DDLID` 或并发交错均安全；不需要专用 dedup 键（`m:ddl:done` 省略，注释说明理由）；
- **容忍未知表**：目标节点没有该表（未参与其 region）时直接追加，保证后续 ASSIGN REGION 时目录已就绪；
- **异常保护**：`req.Meta` 损坏/Op 非法返回错误 → 协调者视为广播失败进入 pending。

### 3.4 协调者广播（pkg/sql/ddl_broadcast.go）

`Executor.broadcastDDL(op string, meta *TableMeta, dropID uint64, dropName string)`：

```
if e.mgr == nil || !e.mgr.HasRemoteNodes() { return }   // 单机零回归
e.replayPendingDDL()                                    // 先收敛历史失败
ddlID := newDDLID()
req := DDLPushReq{DDLID: ddlID, Op: op, TableID: ..., Meta: marshal(meta) 或 nil}
for n := range e.mgr.ListNodes():
    if n.ID == e.selfID || e.mgr.IsDown(n.ID) { continue }
    if err := e.mgr.PushDDL(n.ID, req); err != nil:
        e.recordPendingDDL(req)     // 内存 + 落盘 m:ddl:pending
```

- **广播目标**：全部在线注册节点（排除自身），而非仅该表 region 归属节点——任何节点都可能被 ASSIGN 新 region 或作为查询协调者，目录全局一致是"跨节点 DDL"的语义；
- **pending 落盘**：`m:ddl:pending` 键存 JSON 数组（`[]DDLPushReq`），崩溃/重启后可恢复；`recordPendingDDL` 用独立事务写；
- **重放触发**：① 每次广播前先 `replayPendingDDL()`（顺序重试，成功者移出列表）；② 公开 `ReplayPendingDDL() error` 供启动/运维/测试调用（`db.Open` 后由调用方触发）；
- **并发安全**：pending 列表与落盘操作在 `Executor.mu` 保护下执行（DDL 提交后短暂持锁，量小无性能影响）。

### 3.5 执行顺序与一致性保证

```
DDL 语句 →（本地事务：目录 + 本地数据 ops / 2PC 数据 ops 原子提交）→ 提交成功
        → broadcastDDL（逐节点短连接推送，失败记录 pending）→ DDL 返回成功
```

- **原子性**：DDL 数据+目录在协调者本地原子提交（M9/M5/M8 既有保证）；
- **顺序性**：目录变更以协调者提交顺序为序；pending 按记录顺序重放，保证节点按序收敛；
- **最终一致**：在线节点同步推送（一次 RTT）；离线节点在其恢复后的下一次广播/重放收敛；期间该节点目录滞后，但其离线时也不提供查询服务（心跳 DOWN 语义），恢复后先重放后服务，窗口收敛；
- **崩溃恢复**：协调者崩溃 → pending 落盘恢复，重启后 `ReplayPendingDDL` 补齐；节点崩溃 → 2PC 协调者恢复沿用 M8（目录提交已在本地完成，不依赖节点）。

### 3.6 失败/节点离线处理

| 场景 | 处理 |
|---|---|
| 在线节点 push 失败（网络抖动/瞬时） | DDL 成功，请求进 pending，下次广播前重试 |
| 节点离线（心跳 DOWN / 连接拒绝） | 跳过（不尝试），DDL 成功；恢复后经重放收敛 |
| 节点 apply 失败（存储错误/载荷损坏） | 回 `Err` → 进 pending 重试；若持续失败不影响 DDL 成功 |
| 协调者 DDL 本地提交失败 | 按既有 M9 语义整体失败/回滚，不产生广播 |

**不做回滚**：广播失败不触发 DDL 回滚——本地已提交且数据重写（2PC）已完成，回滚目录会造成"数据新/目录旧"更糟状态；收敛语义优于回滚。

### 3.7 与复制链路（M2/T33）的配合

- **主从复制**：`commitHook` 在事务提交锁内把**全量 batch（含 metaKey Put）**写入 binlog → 从节点 `ApplyBinlog` replay 后目录自动同步。T34 不改变该通道；
- **集群数据节点**：非复制从节点的数据节点（集群拓扑）走 `DDL_PUSH` 获得目录；
- **双通道共存**：某节点既是从节点又是集群数据节点时，binlog replay 与 DDL_PUSH 写同一 `m:tables`，按 TableID upsert/delete 幂等，后到者覆盖，无冲突；
- **T33 语义不变**：`WaitAck` 只关心数据 LSN ACK，不感知 DDL 广播；同步/半同步模式照常作用于 binlog 通道，DDL 广播不参与 ACK 计数（它只是管理帧）；
- **时序**：DDL 本地提交 → commitHook 写 binlog（同步通道）与 broadcastDDL（管理通道）**并行收敛**，二者独立、幂等，不要求严格先后。

### 3.8 已知限制（明确不属本项范围）

- **DROP TABLE 的远端 region 数据行清理**：既有实现只删本地副本行（`scanMerged` 本地扫描）；远端节点目录条目经广播删除后，其持有的孤儿行不再可访问（表已不存在），但物理数据仍在。彻底清理属"REGION 数据回收/迁移"后续项；
- **显式事务内的 DDL 不广播**：与 M5 分布式写在显式事务内的限制一致（`ddlCommit` 对显式事务报错/降级路径），T34 仅广播 autocommit 提交成功的 DDL；文档注明，测试覆盖 autocommit 路径；
- **广播无同步确认语义**：不提供"所有节点确认后才成功"的强一致 DDL（如需可后续加 `ALTER TABLE ... WAIT` 语义，超出本项）。

## 4. 实现清单

| 文件 | 动作 | 说明 |
|---|---|---|
| `pkg/cluster/ddl.go` | 新增 | 帧 30/31、`DDLPushReq/Resp`、`Manager.PushDDL`、`Client.pushDDL`、`Server.SetDDLApply/handleDDLPush` |
| `pkg/cluster/server.go` | 修改 | `handleConn` 增加 `msgDDLPush` 分支 |
| `pkg/sql/ddl_broadcast.go` | 新增 | `Executor.ApplyRemoteDDL`（节点 apply）、`broadcastDDL`、`newDDLID`、pending 持久化/重放 |
| `pkg/sql/executor.go` | 修改 | `execCreateTable/execDropTable/execAlterTable` 提交成功后调用 `broadcastDDL` |
| `pkg/db/db.go` | 修改 | `StartCluster` 装配 `srv.SetDDLApply(d.SQL.ApplyRemoteDDL)`；导出集群启动后重放入口 |
| `pkg/cluster/ddl_test.go` | 新增 | 载荷编解码、PushDDL 往返、handleDDLPush 注入回调 |
| `pkg/sql/ddl_broadcast_test.go` | 新增 | 节点 apply 幂等/upsert/drop、单机零触发、pending 落盘与重放 |
| `pkg/db/db_t34_ddl_test.go` | 新增 | 三节点集成：CREATE/ALTER/DROP 广播、离线 pending 恢复、跨节点读一致 |

## 5. 测试计划

1. **单机回归**：`mgr == nil` 时 CREATE/ALTER/DROP 全量既有测试零回归（广播零触发）；
2. **pkg/cluster 单测**：`DDLPushReq` JSON 往返（含 `Meta []byte` base64）、`Server.handleDDLPush` 正确调用注入回调并回 `DDLPushResp`；
3. **pkg/sql 单测**：节点 apply——新表追加、同 ID 覆盖（alter）、drop 移除、重复广播幂等、损坏载荷报错；
4. **pkg/db 集成**（三节点，仿 `db_2pc_test.go`）：
   - A `CREATE TABLE` → B `SHOW TABLES` 可见；
   - A `ALTER TABLE ADD COLUMN`（含远端 region 数据重写）→ B `SHOW COLUMNS` 含新列、B 本地 `INSERT` 新列成功、A 跨节点读回一致；
   - B 离线（停心跳/`MarkDown`）→ A `ALTER` 成功且 pending 落盘 → B 恢复 + `ReplayPendingDDL` → B 目录同步；
   - A `DROP TABLE` → B `SHOW TABLES` 移除；
5. **全仓验证**：`go test ./... -count=1 -p 1`（串行避免端口竞争）全绿。
*（内容由AI生成，仅供参考）*

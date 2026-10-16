---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_49e45e64c82b11f1b5a6525400393706
    ReservedCode1: t4UiwI6iVIHTU4/5r5hBLDlR7s1xj7+ia5XDh2R05HQVs+g5zxbaYtj+KGJmnn8corPJnOhr7a0d4SRxTFmE9ktQzeoDA787OvzbI6noy44rnKpZqDMJ0QFcYJDGYsfz1x2ZkVAkDKEtxBomD2373iuDtjZpeMme1SkLdqFFTtQPSV0CPcXRl/19lnk=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_49e45e64c82b11f1b5a6525400393706
    ReservedCode2: t4UiwI6iVIHTU4/5r5hBLDlR7s1xj7+ia5XDh2R05HQVs+g5zxbaYtj+KGJmnn8corPJnOhr7a0d4SRxTFmE9ktQzeoDA787OvzbI6noy44rnKpZqDMJ0QFcYJDGYsfz1x2ZkVAkDKEtxBomD2373iuDtjZpeMme1SkLdqFFTtQPSV0CPcXRl/19lnk=
---

# T35 M20 分布式备份一致性（Distributed Backup Consistency）
- 批次：M20 分布式纵深（Distributed Depth）第三项
- 依赖：M2 复制链路、M4 节点拓扑/路由、M8 分布式事务 TSO、M8(BR) 单机备份/恢复、T33 同步/半同步复制、T34 跨节点 DDL
- 状态：已实现，测试全绿

## 1. 背景与目标
M8(BR)（docs/T18_backup_restore.md）提供**单机** BACKUP/RESTORE：基于一致快照生成单一备份文件（magic+version+metaJSON+metaCRC+数据段+dataCRC），支持备份点 LSN 与 `TO LSN` PITR 回放。多节点集群（M4 拓扑、M8 TSO、M2/T33 复制、T34 跨节点 DDL）下，**在各节点上各自执行单机 BACKUP 无法得到全局一致的恢复点**：各节点备份时刻不同、LSN 互不对齐，恢复后节点间数据可能互相矛盾（A 含某提交而 B 不含，反之亦然）。

T35 目标：**在保持单机 BACKUP/RESTORE 行为完全兼容（默认不触发）的前提下，为集群增加分布式备份协调通道**，使一次 `BACKUP TO` 在协调者组织下得到：
1. 所有参与节点各自一致的快照文件；
2. 一个**全局一致性标记**（对齐后的全局 LSN / TSO 与各节点元数据清单）；
3. `RESTORE FROM` 能按该标记把各节点恢复到同一全局时间点（多节点恢复对齐），且单机备份文件不受影响（无 manifest 时仍走原单机路径）。

## 2. 现状分析
### 2.1 单机备份/恢复链路（源码确认）
- `execBackup`（pkg/sql/backup.go）：`e.st.Snapshot()` 一致快照 → 读 `m:tables` / `m:regions` 元数据 → `replication.OpenBinlog(e.binlogPath).LastLSN()` 取备份点 LSN → 快照扫描全部 `'s'`（数据行）/`'i'`（索引键）物理键 → 原子写文件（`.tmp` + rename）；
- `execRestore`：`readBackupFile` 校验（magic/version/metaCRC/dataCRC）→ 幂等删旧（同名表先删行+索引）→ 写回元数据与数据行（整体事务，失败回滚）→ 内存路由重建 → PITR 回放 binlog `[BackupLSN+1, ToLSN]`；
- 备份文件格式 version 1：`metaJSON` 为 `backupMeta`（FormatVersion/CreatedAt/BackupLSN/TablesRaw/HasRegions/RegionsRaw）。

### 2.2 既有跨节点能力
- M4 `pkg/cluster`：`Manager.ListNodes/GetNode/IsDown/HasRemoteNodes`、短连接 `Client.roundTrip`、帧协议 `len(4B)+type(1B)+payload(JSON)`；
- M8 TSO：`tso.Source`（`e.ts` 注入），`Server.SetTSO` + `tsoClient`；
- T33：复制链路 + `commitWaitHook` ACK；T34：`DDL_PUSH(30)/31` 帧 + `ApplyRemoteDDL` + `SetDDLApply` 装配模式（本项直接复用该注入模式）。

### 2.3 与 T18/T34 的差距 / 设计缺口
1. 无跨节点备份协调帧与节点侧备份回调；
2. 备份文件 meta 无节点身份/全局一致性标记（无法判断一份备份属于哪个节点、对齐到哪个全局点）；
3. 无全局 manifest（各节点备份文件清单 + 一致性标记）；
4. `RESTORE FROM` 只能恢复本节点，无"按 manifest 驱动各节点恢复"路径。

## 3. 总体设计
### 3.1 设计原则
1. **单机零回归**：`mgr == nil` 或 `!HasRemoteNodes()` 时 BACKUP/RESTORE 完全走 T18 原路径（不产生 manifest）；`RESTORE FROM <path>` 仅在 `<path>.manifest` 存在时才走分布式恢复；
2. **协议最小扩展**：新增 3 对帧 `BACKUP_PREPARE(40)/41`、`BACKUP_DO(42)/43`、`RESTORE_DO(44)/45`，载荷不引用 sql 类型，避免 `cluster`↔`sql` 循环依赖；
3. **备份一致性是强一致**：与 T34 的"最终一致 pending 收敛"不同，备份必须覆盖**全部注册节点且全部在线**——离线/失败节点直接导致整体失败（可重试），不做降级；理由：备份产物是可归档恢复点，节点缺失会造成恢复后数据分裂；
4. **两阶段 LSN/TSO 对齐**：PREPARE 阶段收集各节点 `LastLSN/TSO` → `G = max(all LSN)` 为全局一致性点 → DO 阶段各节点以 `G` 写入备份 meta（`global_lsn`）并二次校验（备份时刻超过 G 则 `retry` 提升 G 重新对齐，最多 3 轮）→ 恢复时各节点 `RESTORE ... TO LSN G` 补齐到同一全局点；
5. **产物格式向后兼容**：备份文件结构（version 1）不变，`backupMeta` 仅新增可选字段（旧 reader 忽略未知字段）；多节点场景新增独立 manifest 文件（协调者本地 `<path>.manifest`），单机无 manifest；
6. **恢复全有或全无**：manifest 节点集合必须被当前在线节点集合覆盖；任一节点恢复失败整体报错（节点侧恢复本身是事务性、可重入，重试收敛）。

### 3.2 协议扩展（pkg/cluster/backup.go）
```
40 BACKUP_PREPARE -> 41 BACKUP_PREPARE_OK
42 BACKUP_DO      -> 43 BACKUP_DO_OK
44 RESTORE_DO     -> 45 RESTORE_DO_OK
```

```go
type BackupPrepareReq struct {
    BackupID string `json:"backup_id"`           // 协调者生成，审计/测试用
}
type BackupPrepareResp struct {
    NodeID  string `json:"node_id"`
    LastLSN uint64 `json:"last_lsn"`             // 本地 binlog 最大位点
    TSO     uint64 `json:"tso"`                  // 本地当前全局时间戳（快照版本参考）
    Err     string `json:"err,omitempty"`
}

type BackupDoReq struct {
    BackupID  string `json:"backup_id"`
    Path      string `json:"path"`               // 节点本地备份文件路径
    GlobalLSN uint64 `json:"global_lsn"`         // 对齐目标（全局一致性点）
}
type BackupDoResp struct {
    BackupLSN uint64 `json:"backup_lsn"`         // 实际备份点
    Tables    int    `json:"tables"`
    Rows      int    `json:"rows"`
    Retry     bool   `json:"retry,omitempty"`    // 备份点超过对齐目标 → 需提升 G 重对齐
    LastLSN   uint64 `json:"last_lsn,omitempty"`
    Err       string `json:"err,omitempty"`
}

type RestoreDoReq struct {
    BackupID  string `json:"backup_id"`
    Path      string `json:"path"`
    GlobalLSN uint64 `json:"global_lsn"`
}
type RestoreDoResp struct {
    Tables int    `json:"tables"`
    Rows   int    `json:"rows"`
    LSN    uint64 `json:"lsn"`                   // 恢复后最终位点（= GlobalLSN）
    Err    string `json:"err,omitempty"`
}
```

- `Manager.PrepareBackup/DoBackup/DoRestore(nodeID, req)`：短连接发送并读取对应 OK 帧（复用 `ensureRemote` + `clientFor` 模式）；
- `Server.SetBackupPrepare(fn)/SetBackupDo(fn)/SetRestoreDo(fn)`：注入节点侧回调（由 `db.StartCluster` 装配 `sql.Executor` 方法），`handleConn` 新增 3 个 case。

### 3.3 备份产物格式扩展
**备份文件（保持 version 1，字段向后兼容）**：
```go
type backupMeta struct {
    FormatVersion int    `json:"format_version"`
    CreatedAt     string `json:"created_at"`
    BackupLSN     uint64 `json:"backup_lsn"`
    TablesRaw     []byte `json:"tables_raw"`
    HasRegions    bool   `json:"has_regions"`
    RegionsRaw    []byte `json:"regions_raw"`
    // T35 新增（单机均为零值，不影响旧 reader）
    NodeID    string `json:"node_id,omitempty"`     // 产生该备份的节点
    BackupID  string `json:"backup_id,omitempty"`   // 分布式备份批次 ID
    GlobalLSN uint64 `json:"global_lsn,omitempty"`  // 全局一致性点（0=单机）
    GlobalTSO uint64 `json:"global_tso,omitempty"`  // 全局一致性参考时间戳
}
```
**manifest（协调者本地 `<path>.manifest`，JSON）**：
```json
{
  "format_version": 1,
  "backup_id": "bak-...",
  "created_at": "2026-10-14 08:00:00",
  "global_lsn": 42,
  "global_tso": 7,
  "nodes": [
    {"node_id": "node-a", "addr": "127.0.0.1:9001", "backup_file": "D:/bak/b.bak", "backup_lsn": 42, "tables": 2, "rows": 10},
    {"node_id": "node-b", "addr": "127.0.0.1:9002", "backup_file": "D:/bak/b.bak.node-node-b", "backup_lsn": 40, "tables": 2, "rows": 10}
  ]
}
```
- 协调者本地备份文件 = `path`；远端节点文件 = `path + ".node-" + nodeID`；
- manifest 是**一致性标记**：记录全局对齐点、参与节点、各节点备份文件路径与各自备份点；恢复时据此驱动。

### 3.4 协调者流程（pkg/sql/dist_backup.go）
`execBackup` 扩展：`e.mgr != nil && e.mgr.HasRemoteNodes()` 时进入 `distributedBackup(path)`：

```
1. 生成 backupID；收集节点清单（selfID + ListNodes 全部远端，排除自身）。
2. PREPARE：本地取 (selfID, LastLSN, TSO)；远端 mgr.PrepareBackup 逐个获取。
   - 任一节点离线（IsDown / 连接失败 / 回执 Err）→ 整体失败报错（不产生半成品）。
3. G = max(所有 LastLSN)。
4. DO 轮询（最多 3 轮）：
   - 本地：backupCore(path, nodeID=selfID, backupID, G)；
     若 meta.BackupLSN > G → 提升 G 并标记 retry。
   - 远端：mgr.DoBackup(n, {Path: path+".node-"+n.ID, GlobalLSN: G})；
     resp.Retry=true → 提升 G（=max(G, resp.LastLSN)）并标记 retry。
   - 本轮 retry → 进入下一轮（全部节点重新 DO，原子覆盖旧文件）；
     否则 → 成功收敛，跳出。
5. 3 轮仍不收敛 → 整体失败（写负载过高，建议降低写入后重试）。
6. 写 manifest（原子替换），返回 Result{path(manifest), nodes, tables, rows, lsn}。
```

`execRestore` 扩展：先检查 `<path>.manifest`：
- 存在 → `distributedRestore(path, manifest)`：
```
1. 解析校验 manifest（format_version/global_lsn/节点清单）。
2. 校验节点覆盖：manifest 每个节点必须在线且存在于当前注册表；否则报错。
3. 本地：restoreCore(自节点 backup_file, GlobalLSN, hasToLSN=true)（PITR 对齐）。
4. 远端：mgr.DoRestore(n, {Path: 节点 backup_file, GlobalLSN})。
5. 任一失败 → 整体报错（已成功节点可重入，重试收敛）。
6. 返回 Result{tables, rows, lsn=GlobalLSN}。
```
- 不存在 → 原单机路径（兼容旧备份文件与单机部署）。

### 3.5 节点侧回调（pkg/sql/dist_backup.go，由 db.StartCluster 装配）
```go
func (e *Executor) RemoteBackupPrepare(req) (resp, error)
    // 返回 (selfID, binlog LastLSN, tso)
func (e *Executor) RemoteBackupDo(req) (resp, error)
    // 先取当前 LastLSN：> req.GlobalLSN → 直接 Retry（不写文件）；
    // 否则 backupCore(req.Path, selfID, req.BackupID, req.GlobalLSN)；
    // 备份后 BackupLSN > GlobalLSN → Retry。
func (e *Executor) RemoteRestoreDo(req) (resp, error)
    // restoreCore(req.Path, req.GlobalLSN, true)（PITR 对齐到全局点）
```

### 3.6 失败/节点离线处理

| 场景 | 处理 |
|---|---|
| PREPARE 阶段节点离线/连接失败 | 整体失败，报错列出失败节点；不产生 manifest（无半成品） |
| DO 阶段节点备份失败（存储/IO） | 整体失败；已写出的节点文件保留，重试时原子覆盖 |
| DO 阶段节点备份点超过 G（备份期间有写入） | `retry` → 提升 G，全部节点重新 DO（最多 3 轮） |
| 3 轮仍不收敛 | 整体失败，提示写负载过高 |
| RESTORE 阶段节点离线 | 整体失败（恢复是全有或全无）；节点侧恢复事务性可重入 |
| manifest 缺失（单机/旧备份） | 原单机 RESTORE 路径，零回归 |
| 协调者自身节点离线/崩溃 | 协调者即备份发起方，无远端可依赖；本节点恢复后重试即可 |

**不做部分成功恢复**：分布式恢复不返回"部分节点已恢复"的成功——任一失败即报错，用户可修复后重试（幂等，重复执行安全）。

### 3.7 一致性论证
- **对齐点**：各节点备份文件内 `global_lsn = G = max(各节点 PREPARE 时 LastLSN)`；G 单调不减（retry 只提升）；
- **恢复收敛**：节点 i 备份点 `backup_lsn_i ≤ G`（DO 校验保证），恢复执行 `restoreCore(path_i, G, true)`：先恢复备份点快照，再 PITR 回放本地 binlog `(backup_lsn_i, G]`；由于备份期间该节点无超过 G 的提交（否则 retry），回放到 G 后该节点状态 = 全局时间 G 时的本地状态；
- **全局一致性**：各节点恢复到同一 G 的各自本地状态；配合复制链路（T33/M2）或分片写（M5/M8），同一事务的数据/目录在各节点同一 LSN 可见，恢复后集群状态与"在 G 时刻停服"等价；
- **TSO 辅助**：`global_tso` 记录对齐时刻的全局时间戳，用于审计与未来跨快照比对（本项不做 TSO 级恢复，LSN 即权威）。

### 3.8 与复制/DDL 通道的配合
- **复制链路（M2/T33）**：数据节点间一致性由复制/分片保证；备份只读快照 + binlog，不参与复制 ACK；从节点若同时是集群数据节点，其备份文件与主节点独立生成，靠 G 对齐；
- **跨节点 DDL（T34）**：备份覆盖 `m:tables` 目录原值，恢复时目录与数据一并回退到 G；T34 的 pending DDL 重放不受影响（恢复后目录可能回退到旧版本，后续 DDL 广播以协调者为准收敛）；
- **单机路径**：不产生 manifest、不写全局字段、不触发任何帧——T18 全部测试语义不变。

### 3.9 已知限制（明确不属本项范围）
- **无增量/并行备份**：每节点备份为全量逻辑备份，文件随数据量线性增长；并行传输/压缩/增量属后续项；
- **不做跨节点事务级恢复**：以"各节点恢复到 G 的本地状态"为准，不额外做跨节点 2PC 状态回放（备份不落 prepare 中间态，与 T18 一致）；
- **恢复不自动重建离线节点**：manifest 中全部节点必须在线才能恢复；"从单一节点恢复整个集群"（节点级灾备重构）属后续项；
- **协调者即备份发起节点**：无独立 backup 协调服务；协调者离线期间集群备份不可发起（备份是管理面操作，非数据面依赖）。

## 4. 实现清单

| 文件 | 动作 | 说明 |
|---|---|---|
| `pkg/cluster/backup.go` | 新增 | 帧 40-45、`BackupPrepareReq/Resp`、`BackupDoReq/Resp`、`RestoreDoReq/Resp`、`Manager.PrepareBackup/DoBackup/DoRestore`、`Client.*`、`Server.SetBackupPrepare/SetBackupDo/SetRestoreDo/handle*` |
| `pkg/cluster/server.go` | 修改 | `Server` 增加 3 个回调字段；`handleConn` 增加 3 个 case |
| `pkg/sql/backup.go` | 修改 | `backupMeta` 扩展字段；`execBackup/execRestore` 重构为 `backupCore/restoreCore` + 分布式分支 |
| `pkg/sql/dist_backup.go` | 新增 | `distributedBackup/distributedRestore`、manifest 读写、`newBackupID`、节点回调 `RemoteBackupPrepare/RemoteBackupDo/RemoteRestoreDo` |
| `pkg/sql/sql.go` | 修改 | `Engine` 透传 3 个节点回调方法 |
| `pkg/db/db.go` | 修改 | `StartCluster` 装配 `srv.SetBackupPrepare/SetBackupDo/SetRestoreDo` |
| `pkg/cluster/backup_test.go` | 新增 | 载荷编解码、回调往返、retry 语义、未注册节点失败 |
| `pkg/db/db_t35_backup_test.go` | 新增 | 双节点集成：备份/恢复往返、离线失败路径、单机兼容回归 |

## 5. 测试计划
1. **单机回归**：无远端节点时 BACKUP/RESTORE 全量既有测试零回归（不生成 manifest）；
2. **pkg/cluster 单测**：`BackupDoReq` 等 JSON 往返、`Server.handleBackupDo` 正确调用注入回调并回 `BackupDoResp`（含 retry）、未注册节点失败；
3. **pkg/db 集成**（双节点，仿 db_t34_ddl_test.go）：
   - A `ADD NODE B` → 建表/写入 → A `BACKUP TO path` → 校验 manifest 与两个节点备份文件存在 → 备份后继续写入 → `RESTORE FROM path` → A/B 数据均回到备份点且一致；
   - B 离线（关闭 Server）→ A `BACKUP` 报错（离线失败路径）；备份成功后关闭 B → `RESTORE` 报错（恢复离线失败路径）；
   - 单机（无 ADD NODE）`BACKUP/RESTORE` 不生成 manifest（兼容回归）；
4. **全仓验证**：`go test ./... -count=1 -p 1`（串行避免端口竞争）全绿。
*（内容由AI生成，仅供参考）*

# T18 备份与恢复（BACKUP / RESTORE + PITR 基础）

实现于 M8 分布式事务增强之后（BR 功能簇）。在 openxdb 内核上新增逻辑备份（`BACKUP TO`）、恢复（`RESTORE FROM`）与基于 binlog LSN 的时间点恢复基础链路（`RESTORE FROM ... TO LSN <n>`）。单节点语义保持零回归：既有建表/写入/查询路径不变。

## 1. 目标与范围

- **备份**：基于一致快照生成**单一文件**的完整逻辑备份，便于传输与归档；内容覆盖全部表元数据、全部数据行、region 路由与分片元数据、备份点 LSN。
- **恢复**：校验备份文件版本与完整性后，按备份记录顺序重建表结构、导入数据行、重建索引与 region 路由；已存在的同名表采用"先 DROP 再建"的幂等语义。
- **时间点恢复基础**：`RESTORE FROM <path> TO LSN <n>` 在恢复备份点之后，将 binlog 中备份点之后、`<= TO LSN` 的记录按 LSN 升序回放，实现"备份 + binlog 回放"的 PITR 基础链路（时间点语义以 LSN 为锚）。
- **不备份**：2PC 未决中间态（`m:2pc:*`）、未提交事务数据（快照天然仅含已提交数据）。

## 2. 语法

```sql
BACKUP TO '<path>'                 -- 生成完整逻辑备份快照文件
RESTORE FROM '<path>'              -- 校验并恢复备份快照
RESTORE FROM '<path>' TO LSN <n>   -- 恢复备份点后回放 binlog 到指定 LSN（含）
```

实现位置：`pkg/sql`（lexer/ast/parser 新增 `BACKUP`/`RESTORE`/`LSN` 关键字与语句类型；`pkg/sql/backup.go` 实现执行逻辑）。

## 3. 备份设计

### 3.1 一致快照

`execBackup` 调用引擎级 `storage.Snapshot()` 获取**一致性快照**，元数据与数据行全部在该快照上读取，与并发写入隔离：

- 表目录 `m:tables`、region 路由 `m:regions` 通过 `snap.Get` 读取；
- 全部物理键通过 `snap.Scan` 读取；
- 备份点 LSN 通过打开 binlog 读取 `LastLSN()`，作为 TO LSN 回放的起点。

因此备份期间并发提交的事务不会进入备份（快照固定于创建时刻），也不会产生半提交行；备份结果是"某一位点之后不会再变化"的一致视图。

### 3.2 备份内容

| 内容 | 来源 | 说明 |
|------|------|------|
| 表元数据（DDL 记录） | `m:tables` 原值 | CREATE TABLE / CREATE INDEX 等价记录，恢复时重建表结构与索引 |
| 数据行（INSERT 等价记录） | 快照扫描 innerKey 前缀 `'s'` 的物理键 | 含 region 前缀，恢复后键空间一致 |
| 索引键 | 快照扫描 innerKey 前缀 `'i'` 的物理键 | 恢复后二级索引立即可用 |
| region 路由 / 分片元数据 | `m:regions` 原值 | router/region 表；无显式分片时为无（HasRegions=false） |
| 备份点 LSN | binlog `LastLSN()` | PITR 回放起点；binlog 未启用时为 0 |

### 3.3 备份文件格式（version 1）

```
┌─────────────┬──────────────────────────────┬──────────────────────────────┐
│ 头部         │ magic "OPENXDBBAK"(10B)      │ version(2B BE)=1             │
│             │ metaLen(4B BE)               │                              │
├─────────────┼──────────────────────────────┼──────────────────────────────┤
│ meta 段      │ metaJSON (backupMeta)        │ metaCRC(4B BE, crc32)        │
├─────────────┼──────────────────────────────┼──────────────────────────────┤
│ 数据段       │ 循环记录：klen(4B BE) vlen(4B BE) key value                │
│             │ 以 klen=0 vlen=0 结束标记     │                              │
├─────────────┼──────────────────────────────┼──────────────────────────────┤
│ 尾部         │ dataCRC(4B BE, crc32(数据段全部字节含结束标记))             │
└─────────────┴──────────────────────────────┴──────────────────────────────┘
```

- **版本**：`backupVersion = 1`，`backupMeta.FormatVersion` 与头部 version 双重校验；
- **校验**：meta 段 crc32 校验、数据段 crc32 校验；任一不匹配即报 `backup ... checksum mismatch (file corrupted)`；
- **原子写入**：先写 `<path>.tmp`，`fsync` 后原子 `rename` 覆盖旧备份，避免写入中途崩溃产生半文件。

`backupMeta` 字段：`format_version` / `created_at` / `backup_lsn` / `tables_raw` / `has_regions` / `regions_raw`。

## 4. 恢复设计

### 4.1 校验流程

1. 打开文件，校验 magic 与 version；
2. 读取 meta 段并校验 metaCRC，解析 `backupMeta`，校验 `FormatVersion`；
3. 按 `klen/vlen` 循环读取数据段直至结束标记，全程累计字节并校验尾部 dataCRC；
4. 任一步失败即返回明确错误（bad magic / unsupported version / checksum mismatch / corrupt record），不触碰任何数据。

### 4.2 幂等语义（先 DROP 再建）

`RESTORE` 对**目标库中与备份同名的表**采用"先 DROP 再建"语义：

- 读取当前表目录，对每个备份表，若目标库存在同名表，则删除其**数据行物理键**与**索引物理键**（备份包含的键除外，由后续 Put 覆盖，避免同批 WriteBatch 中 Delete 后于 Put 应用导致同键丢失）；
- 写回备份表目录 `m:tables`（覆盖旧目录，旧表的多余列/多余索引随目录覆盖而消失）；
- 写回全部备份数据行与索引键；
- 写回 region 路由 `m:regions`（备份无路由时清除目标路由；单节点读路径回退隐式 region）；
- 内存 Router 重建：有备份路由则 `SetRegions(seq, regions)`，否则 `SetRegions(0, nil)`。

整体恢复在一个事务中执行，任一步失败整体回滚，不留下半恢复状态。

## 5. 时间点恢复（PITR）基础

`RESTORE FROM <path> TO LSN <n>`：

1. 校验 `TO LSN >= 备份点 LSN`，早于备份点直接报错；
2. 恢复备份快照（第 4 节全流程）；
3. 若 `TO LSN > 备份点 LSN`：打开 binlog，从 `BackupLSN+1` 起按 LSN 升序读取记录，回放所有 `LSN <= TO LSN` 的记录（引擎级物理写），读到 `> TO LSN` 即停止；
4. 返回结果 `lsn` 列为最终回放位点（未回放时为备份点 LSN）。

实现说明：

- 回放以 binlog 记录中的 `storage.WriteBatch` 为最小单位，直接落到引擎级 Storage（不经过 SQL 事务），保持与崩溃恢复重放路径一致；
- 范围限定为**数据变更回放**：备份点之后同表的 INSERT/UPDATE/DELETE 记录可精确回放；DDL（建/删表）记录不在本版回放范围内（见 §8 限制）；
- `TO LSN` 精确到 binlog 记录粒度：回放到恰好包含 `LSN = TO LSN` 的记录。

## 6. 测试清单

位于 `pkg/db/db_br_test.go`（5 个用例，本机 CGO 工具链全绿）：

| 用例 | 覆盖点 |
|------|--------|
| `TestBRBackupRestoreRoundTrip` | 备份后写新数据再恢复，恢复后数据回到备份点；含显式 region 分片与二级索引全等 |
| `TestBRBackupIntegrityTamper` | 篡改备份文件（meta/数据段）后恢复报校验错误 |
| `TestBRRestoreIdempotent` | 同名表已存在（不同结构/数据/多余索引）时恢复：旧行清除、旧索引元数据被备份目录覆盖、可重新建同名索引、无残留 |
| `TestBRPITRToLSN` | 备份点 + 若干条 binlog 记录：回放到备份点=快照、备份点+1 条、备份点+2 条均精确对应；TO LSN 早于备份点报错 |
| `TestBRConcurrentBackupConsistency` | 写入 goroutine 持续提交期间备份，恢复后行集 = 备份点快照行集（无半提交行、首尾主键连续） |

验收：`go build ./...` 与 `go test ./... -count=1` 全绿（含既有 M2–M8 全部回归）。

## 7. 语法与执行接线

- `lexer.go`：keywords 新增 `BACKUP` / `RESTORE` / `LSN`；
- `ast.go`：新增 `BackupStmt{Path}`、`RestoreStmt{Path, ToLSN, HasToLSN}`；
- `parser.go`：`parseStmt` 新增分支与 `parseBackup` / `parseRestore`（`BACKUP TO '<path>'`；`RESTORE FROM '<path>' [TO LSN <n>]`）；
- `executor.go`：`Executor` 新增 `st storage.Storage` / `binlogPath string` 字段与 `SetBackupEnv` 注入；`execStmt` 新增两个分支；
- `sql.go`：`Engine.SetBackupEnv` 透传；`IsSQL` 关键字表补充；
- `db.go`：`Open` 装配时注入引擎存储与 binlog 路径（`eng.SetBackupEnv(st, filepath.Join(dir, BinlogFile))`）。

## 8. 限制与后续

- PITR 回放限定为数据变更（binlog 中的物理批次）；DDL 记录的精确回放、按时间戳（而非 LSN）的恢复界面留待后续；
- 备份文件为单机格式：跨节点恢复（把备份恢复到另一拓扑的集群）需要额外的节点/路由映射逻辑，当前不做；
- 备份为逻辑快照（物理键记录），未压缩；超大库可后续引入压缩段与增量备份；
- 恢复期间节点不接受写入（整体事务 + 引擎级回放），操作类语句在 REPL/单连接下串行执行。

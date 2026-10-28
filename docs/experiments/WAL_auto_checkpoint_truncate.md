---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_2d09afd1d2be11f196c5525400248c00
    ReservedCode1: neUiwo7vzYKYNuuPdMKEfpZn+1oRfMGtOm1PmBIwpKnjaYnyrCL5axxIkmw5FmuDWYAJ7ReXVc8yTEi0huVid/P9cradJxZwweq53y8QGQNzJnGeIXqvCkFX584X8RGOTCZW0+6QNZAdcelxN6qzc/FSJXVmVY+4BFE7smirkRcHYO+FQOxDkWWUfgg=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_2d09afd1d2be11f196c5525400248c00
    ReservedCode2: neUiwo7vzYKYNuuPdMKEfpZn+1oRfMGtOm1PmBIwpKnjaYnyrCL5axxIkmw5FmuDWYAJ7ReXVc8yTEi0huVid/P9cradJxZwweq53y8QGQNzJnGeIXqvCkFX584X8RGOTCZW0+6QNZAdcelxN6qzc/FSJXVmVY+4BFE7smirkRcHYO+FQOxDkWWUfgg=
---

# WAL 自动 checkpoint / truncate —— 设计与实验报告

> 版本：v1.16.0（WAL 持久化增强）
> 状态：已实现并通过全仓回归（2026-10-28）
> 依据：`docs/DEV_PLAN_2026Q8.md` C1（组提交增强）后置的持久化收敛项；长期记忆沉淀的「WAL 只增长不自动截断」领域事实
> 约定：本文档按项目约定不入库；正式内容落盘后同步 KNOWN_LIMITATIONS / README

---

## 1. 背景与根因

### 1.1 现象

- 数据目录 `wal.log` 只增长、不自动截断：每次运行（尤其超时被杀）都在同一目录 WAL 上持续追加，多次运行累计可达数十～数百 MB；装载场景（10 万 INSERT+D/F 事务）wal.log 达 54MB 属正常累积而非泄漏，但**长期运行磁盘占用单调增长**问题真实存在。
- 生产路径无自动 checkpoint；`Truncate` 仅在 `wal_test.go` 测试调用，进程正常退出也不收敛。

### 1.2 根因定位（代码证据）

| 根因 | 证据 | 说明 |
|---|---|---|
| WAL 打开方式为追加写、无轮转 | `wal_impl.go` `OpenFile` 使用 `O_CREATE\|O_RDWR` 并 `SeekEnd` 追加 | 文件句柄始终指向日志末尾，只增不减 |
| 无后台 checkpoint 控制器 | 全仓检索仅 `wal_test.go` 调用 `Truncate`；`pkg/db` 无周期性截断装配 | 生产路径从未触发截断 |
| 截断前置条件缺失 | 存储层无 `Flush` 接口、事务层无 `AppliedLSN` 位点、WAL 无 `LastSyncedLSN` 位点 | 即使手动 Truncate 也无法在「安全位点」保证不丢数据 |

> 结论：根因是**缺少周期性的自动 checkpoint 机制**以及支撑安全截断的「已应用位点 / 已落盘位点」基础设施。

---

## 2. 设计

### 2.1 目标

1. 长期运行 wal.log 大小受控（不随累计提交单调增长）；
2. 截断不丢已提交数据；崩溃恢复（WAL 重放）结果与截断前严格一致；
3. 与既有 group_commit（C1）/ C1-C6 机制兼容、零回归、可回退。

### 2.2 安全位点模型（核心）

截断只能删除「既已落盘、又已应用到存储」的记录，推导双安全位点：

```
synced  = WAL.LastSyncedLSN()    // 已 fsync 落盘的最大 LSN
applied = txn.AppliedLSN()       // 已成功应用到存储的最大提交 LSN
safe    = min(synced, applied)   // 双重约束取小
truncAt = safe + 1               // 保留起点（safe 及之前可全部删除）
```

- 只删 `<= synced`：避免删除尚未 fsync（崩溃即丢）的记录；
- 只删 `<= applied`：避免删除已落盘但尚未应用、崩溃恢复仍需重放的记录；
- 截断前对实现 `storage.Flusher` 的存储（RocksDB）执行一次 `Flush()`，把已应用数据落 SST，双保险保证截断位点数据不依赖被截记录。

### 2.3 触发条件 / 频率 / 配置项

| 配置键 | 默认 | 语义 |
|---|---|---|
| `wal_auto_checkpoint` | `on` | 总开关（`off` 完全关闭后台截断，零回归回退） |
| `wal_checkpoint_interval_ms` | `60000` | 后台检查周期（毫秒），热更新经 wake 通道立即生效 |
| `wal_checkpoint_max_mb` | `64` | 大小阈值：WAL 超过该值（MB）才截断；`<=0` 表示每次检查都截断（周期强制收敛） |

触发逻辑：`interval` 到期 → 若 enabled → 计算 safe 位点 → 若 `maxMB>0` 且 WAL 未超阈值则跳过 → `Flush()` → `WAL.Truncate(safe+1)`。

### 2.4 崩溃恢复安全

- Truncate 位点受双安全位点约束，删除的记录要么已应用（SST 已含数据）、要么未 fsync（本就不保证）；
- 截断位点数据的 SST 持久化由截断前 `Flush()` 保障；
- 恢复路径不变：`db.Open` 仍执行 WAL 重放；重放起点为截断后的保留记录，结果与截断前严格等价（已由 CrashRecoveryConsistency / LongRunBoundedWAL 测试覆盖）。

### 2.5 并发安全

- checkpoint 与提交（`group_commit` / 三阶段提交）并发：`LastSyncedLSN / AppliedLSN` 位点读取为原子快照语义，Truncate 只删安全位点之前的记录，与提交锁无交集（提交锁内 Append 的写路径不受影响）；
- 后台协程 stop 由 `DB.Close` 最先调用（`cc.Stop()` 在关闭 WAL/Txn/Storage 之前），保证关闭期无并发访问；
- 配置热更新经 `wakeCh` 非阻塞唤醒，丢信号无害（下一迭代重读配置）。

### 2.6 与 C1-C6 / group_commit 兼容

- C1 组提交窗口（`group_commit_window_ms` 开关）与 fsync 语义不变：本特性只**追加**周期截断，不触碰 Append/fsync 路径；
- C4 提交流水线的 WAL 重放等价性以「safe 位点只删已应用记录」为前提，不改变重放语义；
- 独立 feature flag（`wal_auto_checkpoint`）可灰度回退；默认参数保守（60s/64MB）低负载下几乎不触发。

---

## 3. 实现改动清单

| 文件 | 改动 |
|---|---|
| `pkg/wal/wal.go` | 新增 `LastSyncedLSN()` 接口 |
| `pkg/wal/wal_impl.go` | 修复 `Truncate` 的 nextLSN 计算；实现 `LastSyncedLSN()` |
| `pkg/storage/storage.go` | 新增 `Flusher` 接口（`Flush() error`） |
| `pkg/storage/rocksdb/bridge.h` / `bridge.cc` | 新增 `xdb_flush` cgo 桥 |
| `pkg/storage/rocksdb/rocksdb.go` | 实现 `Flush()`（RocksDB memtable flush） |
| `pkg/txn/txn.go` | 新增 `AppliedLSNProvider` 接口 |
| `pkg/txn/txn_impl.go` | 新增 `appliedLSN` 字段，Recover/Commit 各路径推进，实现 `AppliedLSN()` |
| `pkg/db/checkpoint.go` | 新增 `checkpointController`（周期检查/安全位点/Flush+Truncate/Stats/热更新 wake） |
| `pkg/db/db.go` | Open 装配 checkpoint（LoadPersistedConfig 之前），Close 停止 |
| `pkg/sql/wal_checkpoint_config.go` | 新增 `CheckpointSetter` 接口 + 三个 `wal_*` 配置键与 apply |
| `pkg/sql/executor.go` | 新增 `chkCtl` 字段与 SET/SHOW 转发 |
| `pkg/sql/sql.go` | 新增 `SetCheckpointController` |
| `pkg/db/checkpoint_test.go` | 6 项功能测试 |
| `pkg/sql/wal_checkpoint_config_test.go` | 5 项 SQL 配置层测试 |

---

## 4. 测试与验证结果

### 4.1 功能测试（`go test ./pkg/db/ -run TestCheckpoint`，全绿）

| 测试 | 验证点 |
|---|---|
| `TestCheckpointTruncatesWAL` | 手动 checkpoint：WAL 体积下降、数据不丢；默认 64MB 阈值下不误截断 |
| `TestCheckpointAutoLoop` | 后台循环自动截断：小周期 + 每次截断，WAL 收敛 <2KB |
| `TestCheckpointDisabled` | 总开关关闭后不截断（可回退） |
| `TestCheckpointCrashRecoveryConsistency` | 截断后重开（Recover 重放）数据严格一致；截断后继续写 + 再截断仍一致 |
| `TestCheckpointLongRunBoundedWAL` | 长期运行：40 轮 × 5 提交持续写入期间 WAL 始终 <256KB，200 条数据不丢，重开一致 |
| `TestCheckpointConcurrentWrites` | checkpoint 与并发提交并行安全，数据完整，截断后重开一致 |

### 4.2 SQL 配置层测试（`go test ./pkg/sql/ -run TestWalCheckpoint`，全绿）

| 测试 | 验证点 |
|---|---|
| `TestWalCheckpointConfigDefaults` | SHOW CONFIG 默认值 on/60000/64，来源 default |
| `TestWalCheckpointConfigHotSet` | SET 热更新即时生效；非法值拒绝 |
| `TestWalCheckpointConfigPersist` | SET PERSIST 重启后仍生效（persisted.json 闭环） |
| `TestWalCheckpointHotDrivesTruncate` | SET 热更新真实驱动后台自动截断，数据不丢 |
| `TestWalCheckpointConfigUnwired` | 未装配控制器时 SET wal_* 返回明确错误（零回归） |

### 4.3 全仓回归

```
go test ./... -count=1 → 全绿
（cmd/openxdb, pkg/cdc, cluster, configfile, db, export, import, logx, metrics,
 mysqlproto, replication, server, sharding, sql, storage, storage/btree,
 storage/rocksdb, tso, txn, wal, br_pitr_drill 等全部 ok）
```

---

## 5. 使用说明

```sql
SHOW CONFIG LIKE 'wal_%';                          -- 查看当前配置与来源
SET wal_auto_checkpoint = off;                     -- 关闭自动截断（回退）
SET wal_checkpoint_interval_ms = 30000;            -- 每 30s 检查一次
SET wal_checkpoint_max_mb = 128;                   -- WAL 超 128MB 才截断
SET PERSIST wal_checkpoint_max_mb = 128;           -- 重启后仍生效
```

观测：`cc.Stats()`（Runs/Truncations/LastTruncated/LastError）随控制器暴露；可通过测试与监控调用。

---

## 6. 风险与回退

| 项 | 风险 | 回退策略 |
|---|---|---|
| 截断安全 | 低：双安全位点（已 fsync + 已应用）+ 截断前 Flush | `wal_auto_checkpoint=off` 完全关闭；默认 64MB 阈值保守 |
| 与 group_commit/C1-C6 兼容 | 低：只追加周期截断，不触碰 Append/fsync 路径 | 独立开关灰度；全仓回归 + WAL 重放专项兜底 |
| 配置持久化 | 低：非法值明确报错，不静默忽略 | 删除 persisted.json 中 wal_* 键即回默认 |

---

## 7. 交付清单与 commit 建议

### 建议 commit 拆分（可合并为 1 个）

1. **支撑位点基础设施**：wal/storage/txn 的 `LastSyncedLSN` / `Flusher` / `AppliedLSN`（含 Truncate 修复）
2. **checkpoint 控制器与 DB 装配**：`pkg/db/checkpoint.go`、`db.go`
3. **SQL 配置层**：`pkg/sql/wal_checkpoint_config.go` + executor/sql 装配
4. **测试与文档**：`checkpoint_test.go`、`wal_checkpoint_config_test.go`、本报告

### 变更文件（显式 add，禁 add -A）

```
pkg/wal/wal.go
pkg/wal/wal_impl.go
pkg/storage/storage.go
pkg/storage/rocksdb/bridge.h
pkg/storage/rocksdb/bridge.cc
pkg/storage/rocksdb/rocksdb.go
pkg/txn/txn.go
pkg/txn/txn_impl.go
pkg/db/checkpoint.go
pkg/db/checkpoint_test.go
pkg/db/db.go
pkg/sql/wal_checkpoint_config.go
pkg/sql/wal_checkpoint_config_test.go
pkg/sql/executor.go
pkg/sql/sql.go
docs/experiments/WAL_auto_checkpoint_truncate.md
```

### 提交前校验

- 双远端（gitee + GitHub443）SHA 一致；
- 逐文件 diff 复核；
- 全仓回归全绿（本次已执行通过）。

---

*（内容由 AI 生成，实测数据基于本次实现与回归，供评审后入库）*
*（内容由AI生成，仅供参考）*

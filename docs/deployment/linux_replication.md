---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_7893f45dd41911f180d5525400248c00
    ReservedCode1: ZzLqY7mb/qAwggmbMu2dZsIQyokNrgo4jrvvWC9zgAOMj1L/CwTVSOYeB89Asr+Fleech5Z7k5K1fww9JEKt5r0ol0Zy69G8YIezPM5hRweBtN9Z7d24/j3ChO56LkL7Gg+kGL+JgUvOMqDjy3aRct5YQ8BAJW0TxcMEecr3U2C0bbtEa3WSlch1vso=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_7893f45dd41911f180d5525400248c00
    ReservedCode2: ZzLqY7mb/qAwggmbMu2dZsIQyokNrgo4jrvvWC9zgAOMj1L/CwTVSOYeB89Asr+Fleech5Z7k5K1fww9JEKt5r0ol0Zy69G8YIezPM5hRweBtN9Z7d24/j3ChO56LkL7Gg+kGL+JgUvOMqDjy3aRct5YQ8BAJW0TxcMEecr3U2C0bbtEa3WSlch1vso=
---

# OpenXDB 主从复制搭建文档（Linux 7.x）

> 说明：本文档适用于 Linux 7.x（CentOS/RHEL 7 系）。
> 适用版本：OpenXDB v1.17.0
> 适用系统：CentOS 7.x / RHEL 7.x（x86_64）
> 面向读者：有 Linux 基础的 DBA / 运维工程师
> 来源：本文能力与命令均取自仓库（gitee：zhengkh/openxdb，github：zhengkuanhua/openxdb）的 `cmd/openxdb/main.go`（`start`/`replica` 子命令与参数）、`pkg/replication/*`、`docs/T33_m20_sync_semi_sync_replication.md`、`docs/product_manual_zh.md`（第 8.1 节）、`KNOWN_LIMITATIONS.md`；凡仓库未明确定义之处均标注"以仓库实际为准"。

---

## 目录

- [1. 复制能力现状（v1.17.0）](#1-复制能力现状v1170)
- [2. 拓扑规划与前置检查清单](#2-拓扑规划与前置检查清单)
- [3. 主库配置](#3-主库配置)
- [4. 从库配置](#4-从库配置)
- [5. 复制模式与热切换](#5-复制模式与热切换)
- [6. 复制状态验证](#6-复制状态验证)
- [7. 故障处理与切换](#7-故障处理与切换)
- [8. 边界与已知限制](#8-边界与已知限制)
- [9. 替代方案](#9-替代方案)
- [10. 常见问题（FAQ）](#10-常见问题faq)
- [11. 参考资料](#11-参考资料)

---

## 1. 复制能力现状（v1.17.0）

OpenXDB 主从复制为 **master-follower 异步复制**（M2），v1.17.0 在 M2 之上叠加 **semi-sync（半同步）与 sync（全同步）**（T33）三种模式，支持运行期热切换：

| 模式 | 语义（仓库 T33 文档） | 默认 |
|------|----------------------|------|
| `async` | 主端 binlog 落盘后异步推送，不等 ACK（M2 原样） | ✅ |
| `semi-sync` | 等待 ≥ min(N, 在线副本数) 的 ACK；超时/无在线副本 → 降级异步（`Degraded=true`，累计 `DegradedCount`） | - |
| `sync` | 等待全部**在线**副本 ACK；无在线副本 → `ErrNoSlave`（拒绝伪同步）；超时 → `ErrAckTimeout`（本地已提交，客户端报错） | - |

**核心机制（仓库事实）**：

- 主端：`start --replica-port <n>` 启用复制监听，事务提交时 append binlog（独立追加式文件、单调递增 LSN，binlog 顺序 = 提交顺序），按位点推送给从库，心跳 PING/PONG（默认 2s/6s）。
- 从端：`replica` 子命令以从节点身份连接主节点，严格按序幂等应用 binlog 并回 `msgAck`（AppliedLSN 仅内存，主端记录从节点确认位点）；断点续传（`pushLoop` 从 `lastAck` 续推）。
- 提交确认：`WaitAck` 在事务锁外执行，不阻塞其他事务提交。

> **如实说明**：当前源码未检索到 `SHOW REPLICATION STATUS` SQL 语句的实现（T33 设计文档提到该语句"保持原有输出"，但 `pkg/sql` 中无对应 AST/executor）。复制状态查看以 `openxdb status` / `openxdb stats` / `SHOW BINLOG EVENTS` 为准（见 §6）。

---

## 2. 拓扑规划与前置检查清单

### 2.1 建议拓扑

| 节点 | 角色 | 数据目录 | 服务端口 | 复制端口 |
|------|------|----------|----------|----------|
| master | 主库 | /data/openxdb-master | 7788 | 7789 |
| slave-1 | 从库 | /data/openxdb-slave1 | 7788 | -（仅连接主库） |
| slave-2 | 从库（可选，semi-sync N=2 场景） | /data/openxdb-slave2 | 7788 | -（仅连接主库） |

- 主库开启复制监听（`--replica-port 7789`）；从库以 `replica --master <master_host>:<replica_port>` 连接。
- 一主多从：多个从库可同时连接同一主库；主库 `SlaveStatus` 记录全部已知从节点（在线+离线）。
- 复制与集群（M4）可共存，本文只讲复制拓扑。

### 2.2 前置检查清单

| # | 检查项 | 要求 | 说明 |
|---|--------|------|------|
| 1 | 操作系统/glibc | CentOS/RHEL 7.x、glibc ≥ 2.17 | 与单机文档一致 |
| 2 | 节点间网络 | master 复制端口（默认 7789）对从库可达；SQL 端口（7788）对客户端可达 | 逐对 `telnet` 验证 |
| 3 | 时钟同步 | NTP 同步 | 心跳超时判定与排障依赖 |
| 4 | 二进制版本 | 主从使用相同版本（建议同为 v1.17.0 及以上） | 协议向后兼容性以仓库实际为准 |
| 5 | 数据一致性基线 | 从库数据目录应来自主库一致快照（或空目录首连全量追平） | 复制基于 binlog 增量，不会自行全量拷贝历史数据（见 §4.2 初始化基线） |
| 6 | 防火墙/SELinux | 放行复制端口 | - |
| 7 | 证书（可选） | 启用 TLS 时准备 CA/证书 | 见 §3.3 / §4.3 |

---

## 3. 主库配置

### 3.1 启动主库（启用复制监听）

```bash
openxdb init --data-dir /data/openxdb-master
openxdb start --data-dir /data/openxdb-master --port 7788 --replica-port 7789
```

- `--replica-port > 0` 即启动复制监听并生成 binlog（`start` 装配逻辑）。
- 也可写入 `openxdb.conf`：

```ini
[server]
port = 7788
replica_port = 7789
```

### 3.2 复制模式启动参数（可选）

```bash
openxdb start --data-dir /data/openxdb-master --port 7788 --replica-port 7789 \
    --replication-mode semi-sync --replication-ack 1 --replication-timeout 10000
```

| 参数 | 值 | 默认 |
|------|-----|------|
| `--replication-mode` | `async` / `semi-sync` / `sync` | `async` |
| `--replication-ack` | 半同步最少确认副本数（正整数） | `1` |
| `--replication-timeout` | ACK 超时毫秒（正整数，≤0 用默认） | `10000` |

对应 `[server]` 配置段：`replication_mode` / `replication_ack` / `replication_timeout`。优先级：持久化 < 配置文件 < 命令行（与现有参数一致）。

### 3.3 主库 TLS（可选）

主库启用 TLS 后复制端口同样加密：

```bash
openxdb start --data-dir /data/openxdb-master --port 7788 --replica-port 7789 \
    --tls-cert /etc/openxdb/tls/server.crt --tls-key /etc/openxdb/tls/server.key
```

证书生成：`openxdb gen-cert [--cert <path>] [--key <path>] [--out-dir <dir>] [--days <n>] [--cn <name>] [--force]`。

---

## 4. 从库配置

### 4.1 初始化基线

**方案 A（推荐）**：从库数据目录由主库一致快照恢复而来，保证基线一致：

```bash
# 主库执行逻辑备份
openxdb repl --data-dir /data/openxdb-master <<< "BACKUP TO '/data/backup/master.bak'"

# 从库机器
openxdb init --data-dir /data/openxdb-slave1
openxdb repl --data-dir /data/openxdb-slave1 <<< "RESTORE FROM '/data/backup/master.bak'"
```

**方案 B**：从库空数据目录直接启动复制——从库将从握手点开始应用**之后**的 binlog；若主库已有历史数据且从库为空，历史数据不会自动同步（复制为增量机制，仓库未提供自动全量回填；具体行为以仓库实际为准）。

### 4.2 启动从库复制

```bash
openxdb replica --data-dir /data/openxdb-slave1 --master 10.0.0.1:7789
```

- `--master <host:port>`：主库复制监听地址（必填）。
- 该命令以从节点身份连接主库，**后台持续复制并阻塞**（前台运行；生产建议配合 systemd/nohup）。
- TLS 场景：`--replica-tls`（不校验服务器名）/ `--replica-tls-ca <path>`（校验 CA，verify-full）。

### 4.3 从库 systemd 建议

```ini
[Unit]
Description=OpenXDB Follower (slave1)
After=network.target

[Service]
Type=simple
User=openxdb
ExecStart=/usr/local/bin/openxdb replica --data-dir /data/openxdb-slave1 --master 10.0.0.1:7789
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

> 从库自身一般不对外提供写服务（写应走主库）；从库可作只读查询与灾备。是否具备只读会话控制以仓库实际为准。

---

## 5. 复制模式与热切换

复制模式可在主库运行期热切换（无需重启、无需重建从库）：

```sql
-- 查看当前模式（SHOW CONFIG 自动展示 replication_mode / replication_ack / replication_timeout）
SHOW CONFIG;

-- 热切换（立即生效）
SET replication_mode = 'semi-sync';
SET replication_ack = 1;
SET replication_timeout = 10000;

-- 持久化（落盘，重启生效）
SET PERSIST replication_mode = 'sync';
SET PERSIST replication_ack = 2;
```

**各模式提交行为速查（T33 §3.8）**：

| 场景 | async | semi-sync | sync |
|------|-------|-----------|------|
| 副本离线（心跳剔除） | 照常提交 | 在线数 < N → 立即降级异步 | 等待全部在线副本；若仍有在线副本则提交成功（离线副本回连后从 lastAck 续推补齐） |
| 无在线副本 | 照常提交 | 降级异步，提交成功 | 提交返回 `ErrNoSlave`（拒绝伪同步） |
| 副本慢（ACK 迟到） | 照常提交 | 超时 → 降级异步，提交成功 | 超时 → `ErrAckTimeout`，事务报错（本地已提交，重试幂等） |
| 副本重连 | - | 降级状态在 ACK 达标时自动恢复 | 续推 + ACK 等待恢复 |
| 主节点崩溃 | 已提交事务 WAL+binlog 均落盘；恢复后从节点按 lastAck 续推 | 同左 | 同左 |

> 注意：`sync` 超时返回错误时，本地事务**已提交**（WAL+存储+binlog 均已落盘），客户端收到失败——分布式系统"提交不确定"窗口；客户端重试幂等（同 key 写入按 binlog LSN 幂等应用）。

---

## 6. 复制状态验证

### 6.1 openxdb status（聚合状态快照）

```bash
openxdb status --addr 127.0.0.1:7788 [--json]
```

输出聚合状态快照：节点存活/角色、region 分布、存储用量、慢查询摘要、事务计数、连接数与 QPS。主库视角的从节点信息来自 `db.ReplicationStatus()` → `SlaveStatus` 列表（字段：`NodeID` / `Remote` / `Online` / `LastAck` / `LastSeen`）。status 是否直接展示各从库明细以仓库实际输出为准。

### 6.2 openxdb stats（binlog LSN 增长）

```bash
# 主库：--interval 两次采样计算 binlog LSN 增长
openxdb stats --addr 127.0.0.1:7788 --interval 5
```

主库 LSN 随写入单调增长即证明 binlog 链路正常。

### 6.3 SHOW BINLOG EVENTS（CDC 事件查看）

```sql
SHOW BINLOG EVENTS;                    -- 查看全部 binlog 事件（M14 CDC）
SHOW BINLOG EVENTS FROM <lsn> LIMIT 10; -- 从指定 LSN 续读
```

### 6.4 端到端验证步骤

| # | 步骤 | 命令/语句 | 预期 |
|---|------|-----------|------|
| 1 | 主库写入 | 主库 `INSERT INTO t1 VALUES (1,'a');` | 成功 |
| 2 | 从库可见 | 从库机器 `openxdb repl --data-dir /data/openxdb-slave1` 内 `SELECT * FROM t1;` | 查到主库数据（async 有延迟窗口） |
| 3 | 主库状态 | `openxdb status --addr 127.0.0.1:7788` | 从库 Online，LastAck 随写入推进 |
| 4 | binlog 增长 | `openxdb stats --addr 127.0.0.1:7788 --interval 5` | LSN 增长 |
| 5 | sync 模式验证 | 主库 `SET replication_mode='sync'` 后插入 | 提交成功且从库立即可查 |

---

## 7. 故障处理与切换

### 7.1 从库故障

- 心跳超时（默认 2s/6s）→ 主库标记从库 offline；按 §5 表格各模式行为继续提交。
- 从库恢复重连 → `pushLoop` 从 `lastAck` 续推补齐，不丢已应用位点之后的数据；semi-sync 降级状态在下一次 ACK 达标时自动恢复。

### 7.2 主库故障（切换）

- 仓库提供复制主从切换 Go API：`PromoteToMaster` / `EnableAutoFailover` / `StopAutoFailover`（`pkg/db/ha.go`，M6/T43）；`FailoverCatchupTimeout` 默认 2s，promote 前可对旧主做短窗口追平（RPO 补追，`PullCatchup`），并记录 `LastPromoteInfo().RpoLost`。
- **如实说明**：这些为 Go API（`pkg/db`），CLI/SQL 层未检索到对应命令封装；生产故障切换当前需要依赖应用层/脚本调用该能力，或人工按 §7.3 流程执行。以仓库实际为准。

### 7.3 人工切换流程（推荐先演练）

1. 确认旧主已停止写入（停机或 `SHUTDOWN`）。
2. 在数据最全的从库上执行 promote（Go API 或运维脚本）。
3. 新主开启复制监听（如原从库以 `start --replica-port` 重启为独立主库）。
4. 其余从库改指新主：`replica --master <新主>:7789`。
5. 客户端连接切换至新主；旧主修复后作为新从库回连（T43 回连/补偿以仓库实际为准）。

### 7.4 切换演练建议

- 定期演练：kill 主库 → 从库 promote → 恢复旧主 → 旧主回连补追。
- 记录每次 promote 的 `RpoLost`（RPO 审计）：async 模式下旧主已落盘未推送的 binlog 段可能随旧主一起丢失，评估 RPO 需求后决定是否使用 semi-sync/sync。

---

## 8. 边界与已知限制

- **async 丢写窗口**：异步复制下主库崩溃时，已落盘未推送/未 ACK 的 binlog 可能丢失（RPO 窗口）；要求低 RPO 请使用 semi-sync/sync 或评估 HA 机制。
- **sync 提交不确定**：sync 超时后本地已提交但客户端报错，需幂等重试；无在线副本时提交被拒绝（`ErrNoSlave`）。
- **复制为增量机制**：从库空目录不会自动全量回填历史数据，需先做基线快照（§4.1 方案 A）；具体首连行为以仓库实际为准。
- **`SHOW REPLICATION STATUS` 未实现**：T33 设计文档提及，但当前源码未检索到实现，复制状态请用 `status` / `stats` / `SHOW BINLOG EVENTS`（以仓库实际为准）。
- **复制协议向后兼容**：HELLO_OK 帧尾部追加模式/位点字节，旧帧可解（10B/11B 兼容）；但生产建议主从同版本。
- **HA quorum 默认关闭**、epoch 默认开启（两节点主从不防脑裂，生产建议 ≥3 节点或启用 quorum，前提以仓库实际为准）。
- **性能数值为单机端侧基准**：复制吞吐/延迟需自测。

---

## 9. 替代方案

若业务不需要实时主从复制，或需要更强一致性，可组合以下仓库真实能力：

| 方案 | 说明 | 适用 |
|------|------|------|
| **备份恢复** | `BACKUP TO '<path>'` 一致快照 + `RESTORE FROM '<path>'` / `RESTORE ... TO LSN <n>`（PITR）+ 定时调度 `--backup-interval` | 定期灾备、误删恢复、低频率数据保护 |
| **2PC 集群内冗余** | M4 集群 + M5 2PC 跨节点写：数据分布多节点，节点故障由 HA region 重指派/读 failover 兜底 | 要求集群内高可用，接受跨节点写走 2PC |
| **复制 + 集群共存** | `--replica-port` + `--cluster-addr` 同时启用，复制提供异步灾备、集群提供路由/HA | 最完整但复杂度最高 |

---

## 10. 常见问题（FAQ）

| 问题 | 排查/处理 |
|------|-----------|
| 从库连接失败 | 检查 `--master host:port` 是否为主库**复制端口**（7789）而非服务端口（7788）；检查防火墙；主库是否带 `--replica-port` 启动 |
| 从库看不到新数据 | async 有延迟窗口；查主库 `stats --interval` 看 LSN 是否增长；从库 `status` 看 Online/LastAck |
| sync 模式下写入报错 | 无在线副本 → `ErrNoSlave`；副本慢 → `ErrAckTimeout`（本地已提交，重试幂等）；确认至少一个从库在线且 ACK 正常 |
| semi-sync 是否生效 | `SHOW CONFIG` 看 `replication_mode`；`SET replication_mode='semi-sync'` 后观察是否降级（`Degraded`/`DegradedCount` 以 status/stats 输出为准） |
| 从库历史数据缺失 | 复制为增量机制，先按 §4.1 用主库 BACKUP/RESTORE 建立基线 |
| 主从版本不一致 | 先统一版本再建复制，避免协议差异（以仓库实际为准） |

---

## 11. 参考资料

- `docs/T33_m20_sync_semi_sync_replication.md`（semi-sync/sync 设计与提交语义）
- `docs/product_manual_zh.md` 第 8.1 节（复制 M2）
- `docs/T43_ha_rpo_rejoin.md`（RPO 补追/回连）、`docs/T15_m6_ha.md`（M6 HA 复制主从切换）
- `cmd/openxdb/main.go`（`start` / `replica` 子命令与参数）
- `KNOWN_LIMITATIONS.md`、`CHANGELOG.md`
*（内容由AI生成，仅供参考）*

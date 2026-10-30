# OpenXDB 分布式安装部署文档（Linux 7.x）

> 说明：本文档适用于 Linux 7.x（CentOS/RHEL 7 系）。
> 适用版本：OpenXDB v1.17.0
> 适用系统：CentOS 7.x / RHEL 7.x（x86_64）
> 面向读者：有 Linux 基础的 DBA / 运维工程师
> 来源：本文能力现状与命令均取自仓库（gitee：zhengkh/openxdb，github：zhengkuanhua/openxdb）的 `docs/T13_m4_multinode.md`、`docs/T14_m5_2pc.md`、`docs/product_manual_zh.md`（第 8 章）、`cmd/openxdb/main.go`、`pkg/cluster/*`、`KNOWN_LIMITATIONS.md`；凡仓库未明确定义之处均标注"以仓库实际为准"。

---

## 目录

- [1. 分布式能力现状（v1.17.0）](#1-分布式能力现状v1170)
- [2. 拓扑规划与前置检查清单](#2-拓扑规划与前置检查清单)
- [3. 每节点安装](#3-每节点安装)
- [4. 集群模式启动与节点注册](#4-集群模式启动与节点注册)
- [5. 路由与 region 管理](#5-路由与-region-管理)
- [6. 跨节点写与 2PC](#6-跨节点写与-2pc)
- [7. 分布式事务与 TSO](#7-分布式事务与-tso)
- [8. HA 与故障处理](#8-ha-与故障处理)
- [9. 分布式备份](#9-分布式备份)
- [10. 验证清单（多节点写入）](#10-验证清单多节点写入)
- [11. 边界与已知限制](#11-边界与已知限制)
- [12. 参考资料](#12-参考资料)

---

## 1. 分布式能力现状（v1.17.0）

| 能力 | 对应里程碑 | 状态（仓库事实） | 说明 |
|------|-----------|-----------------|------|
| 多节点集群拓扑 | M4（T13） | 已交付 | 节点注册表、心跳、集群路由表（region→节点）、跨节点读转发+合并 |
| 分布式写/2PC | M5（T14） | 已交付 | 跨节点写两阶段提交，幂等 COMMIT/ABORT |
| 分片 | M3 | 已交付 | region 分裂、`SPLIT REGION`、路由表 |
| 负载均衡/在线迁移 | M7 | 已交付 | `BALANCE`、自动均衡、在线迁移（源路由不变） |
| 中心化 TSO | M8 | 已交付 | `--tso-node` 中心化发号、分布式快照隔离 |
| HA 故障切换 | M6（T15） | 已交付 | 心跳 down 检测、region 自动重指派、读 failover、复制主从切换 |
| 跨节点 DDL | T34 | 已交付 | DDL 广播（以仓库实际为准） |
| 分布式备份 | T35 | 已交付 | 多节点一致快照（以仓库实际为准） |

> **重要边界**：M4 之后跨节点**读**已打通（SELECT 转发+合并）；跨节点**写**依赖 2PC，一条写语句同时命中本地与远端 region 时自动触发两阶段提交；**单节点部署时 2PC 零触发、零回归**。所有性能数值均为单机端侧功能基准（KNOWN_LIMITATIONS）。

---

## 2. 拓扑规划与前置检查清单

### 2.1 建议拓扑

| 节点 | 角色 | 服务端口 | 集群链路 | 复制端口（可选） | TSO |
|------|------|----------|----------|------------------|-----|
| node-a | 数据节点（首节点） | 7788 | 10.0.0.1:7791 | 7789 | 可选中心化发号 |
| node-b | 数据节点 | 7788 | 10.0.0.2:7791 | 7789 | - |
| node-c | 数据节点（三节点起建议） | 7788 | 10.0.0.3:7791 | 7789 | - |

- 集群节点链路地址 `--cluster-addr` 与 SQL 服务端口 `--port` 是两个不同监听：前者走节点协议（握手/查询转发/数据装载/心跳），后者对外服务。
- 节点 ID：默认按数据目录路径 crc32 自动生成（`clusterNodeID`）；推荐在数据目录 `openxdb.conf` 中显式写 `node_id = node-a` 便于管理。
- 复制（M2 主从）与集群（M4）可同时启用（`--replica-port` + `--cluster-addr`），本文档聚焦集群；复制单独见《openxdb_linux_replication.md》（`docs/deployment/linux_replication.md`）。

### 2.2 前置检查清单

| # | 检查项 | 要求 | 说明 |
|---|--------|------|------|
| 1 | 操作系统 | CentOS/RHEL 7.x（x86_64） | 与单机文档一致 |
| 2 | glibc | ≥ 2.17 | 同上 |
| 3 | 节点间网络 | 全互连，开放 SQL 端口（7788）、集群链路端口（如 7791）、复制端口（7789）、指标端口 | 用 `telnet <ip> <port>` 逐对验证 |
| 4 | 时钟同步 | NTP/chrony，各节点时钟偏差建议 < 1s | TSO/心跳/2PC 强依赖 |
| 5 | 每节点磁盘 | 独立数据目录所在分区充足 | 各节点独立存储 |
| 6 | 防火墙/SELinux | 放行上述端口；SELinux 可先 `setenforce 0` 排查 | 以生产安全策略为准 |
| 7 | 节点 ID 规划 | 提前规划 node-a/node-b/... | 写在 `openxdb.conf` 的 `node_id` |
| 8 | 构建/二进制 | 每节点可共享同一份 openxdb 二进制 | 构建方式见单机文档 §3 |

---

## 3. 每节点安装

构建二进制方式与单机文档完全一致（`scripts/build.sh` 或手动 CGO 或 Dockerfile），不再重复。**部署阶段**：

```bash
# 每节点重复执行（以 node-a 为例）
cp openxdb /usr/local/bin/openxdb
useradd -r -s /sbin/nologin openxdb
mkdir -p /data/openxdb && chown openxdb:openxdb /data/openxdb
openxdb init --data-dir /data/openxdb
```

**注意**：节点 ID 在 `init` 后写入数据目录 `openxdb.conf`：

```ini
[server]
port = 7788
cluster_addr = 10.0.0.1:7791
node_id = node-a
```

> `node_id` 也可不写（自动按数据目录生成）；显式指定便于 `SHOW NODES` / `ASSIGN REGION ... TO NODE '<nodeID>'` 管理。节点 ID 生成逻辑（`pkg/db/db.go clusterNodeID`）：优先读数据目录 `openxdb.conf` 中 `node_id = <id>`，否则取数据目录路径 crc32。

---

## 4. 集群模式启动与节点注册

### 4.1 启动集群模式

```bash
# 每节点：--cluster-addr 非空即启用 M4 集群
openxdb start --data-dir /data/openxdb --port 7788 --cluster-addr 10.0.0.1:7791
```

启动后该节点自我注册（`RegisterSelf`），节点注册表落盘 `m:nodes`，Open 时自动恢复，无需手动发现。

### 4.2 节点注册（ADD NODE）

在**任一已启动的节点**上执行：

```bash
openxdb repl --data-dir /data/openxdb
```

```sql
-- 向集群注册远端节点（与远端握手并写入节点注册表；握手成功自动双向注册 RegisterPeer）
ADD NODE '10.0.0.2:7791';
ADD NODE '10.0.0.3:7791';

-- 查看节点注册表（ID / Addr / Role / State / LastSeen）
SHOW NODES;
```

预期输出三行：`node-a`（自身，Role=Data、State=Up）+ `node-b` / `node-c`（握手注册）。若节点已在心跳中互相发现（HELLO 自动互学），`SHOW NODES` 也会展示。

### 4.3 集群链路协议与心跳

- 帧类型：`NODE_HELLO` / `NODE_HELLO_OK`（双向身份确认）、`QUERY_REQ` / `QUERY_RESP`（查询转发，物理键区间直扫）、`REGION_PUSH`（数据装载）、`PING/PONG`（复用 M2 心跳协议）。
- 心跳周期：默认 PING 2s / PONG 超时 6s，超时标记节点 down（`State=down`），触发 OnDown 回调（region 重指派/读 failover）。
- 节点身份：集群模式下节点 ID 由 `openxdb.conf` 的 `node_id` 指定或按数据目录/地址自动生成（`db.StartCluster` 装配）。

---

## 5. 路由与 region 管理

### 5.1 集群路由表

- 路由表为 **region → 节点** 映射（落盘 `m:regions`），`SHOW REGION ROUTES` 一表查看：region / table / start / end / node / local。
- **Local 语义**：`node` 为空或等于本节点 selfID 的 region 在本节点本地执行；单节点部署所有 region 均满足 → 2PC/转发零触发。
- 隐式单 region（未 `SPLIT` 的表）：`ASSIGN` 时先显式化注册再指派。

### 5.2 region 管理与均衡

```sql
-- 查看 region 路由
SHOW REGION ROUTES;

-- 手动将某 region 指派到某节点（更新归属并推送数据）
ASSIGN REGION <regionID> TO NODE 'node-b';

-- 手动沿数据中点分裂 region
SPLIT REGION <regionID>;

-- 手动触发一次负载均衡（迁移过载本地 region 到最闲存活节点；返回迁移 region 数，再 BALANCE 返回 0）
BALANCE;
```

- 自动分裂：写路径提交后累计写计数，水位达阈值一半触发真实行数检查，超阈值沿数据中点一分为二（同一事务完成重路由+路由表替换+元数据落盘）。
- 自动均衡：`Engine.SetAutoBalance(enabled, interval)`（Go API，周期触发；是否暴露 SQL/CLI 以仓库实际为准）。
- 在线迁移：迁移期间源路由不变、读写不中断；扫描源 region 物理行 → `REGION_PUSH` 装载目标节点 → 切换本节点路由并落盘；其余存活节点经下一次心跳路由同步。

> 上述 SQL 语句（ADD NODE / SPLIT REGION / ASSIGN REGION / BALANCE 等）仅 root 可执行（`pkg/sql/authz.go`）。

---

## 6. 跨节点写与 2PC

### 6.1 触发条件

一条写语句（`INSERT` / `UPDATE` / `DELETE` / CSV `IMPORT`）的物理键操作同时命中**本地 region 与远端 region** 时，发起节点作为协调者自动执行两阶段提交：

```
协调者（发起节点）：
  本地 region → 本地事务缓冲（WAL 语义）
  远端 region → 按归属节点聚合成组，走 2PC 协议帧（PREPARE/COMMIT/ABORT）
```

```
状态机：Prepared → LocalCommit → RemoteCommit → Committed
        任一 PREPARE 失败 → Aborting → Aborted（回滚）
        全部幂等（参与者落盘 m:2pc:p / m:2pc:c / m:2pc:a）
```

### 6.2 前置条件

1. 涉及的所有远端节点必须已集群模式启动（`--cluster-addr`）并完成节点注册（`ADD NODE` / 心跳互学）。
2. 相关表已存在 region 路由（`SHOW REGION ROUTES` 可见；未分裂的表为隐式单 region，可 `ASSIGN`/`SPLIT` 后再分布写入）。
3. 节点间网络连通、时钟同步（2PC 与 TSO 依赖）。
4. 单节点部署（无集群模式）执行写语句不会触发 2PC；执行集群管理语句返回 `cluster not enabled`。

### 6.3 错误语义（T14）

| 错误 | 含义 | 处理建议 |
|------|------|----------|
| `TXN_PREPARE_FAILED` | 某参与者 PREPARE 失败，事务整体回滚 | 检查目标节点存活/存储/权限，重试 |
| `TXN_COMMIT_ABORTED` | 提交阶段参与者回滚 | 检查参与者状态与 `m:2pc:*` 记录 |
| `TXN_COMMIT_UNCERTAIN` | 提交结果不确定（协调者与参与者失联窗口） | 查询参与者最终状态后手工判定；记录以 `m:2pc:*` 幂等账本为准 |

---

## 7. 分布式事务与 TSO

- 本地时间戳源：每个节点 Open 时创建本地发号器并注入 SQL 引擎；`StartCluster` 后可切换为指向某节点的远程 TSO 客户端。
- 中心化发号：指定某节点为 TSO 节点。

```bash
# 在 TSO 节点（node-a）启动时或配置文件指定
openxdb start --data-dir /data/openxdb --port 7788 --cluster-addr 10.0.0.1:7791 --tso-node node-a
# 或 openxdb.conf [server] tso_node = node-a
```

```sql
-- 其他节点通过 SET 切换远程 TSO（若未在配置/启动参数指定）
-- 具体 SQL 语句与参数以仓库实际为准（代码路径 pkg/db/db.go SetTSONode + pkg/cluster/tso.go）
```

> TSO 能力（64 位毫秒时间戳 + 24 位逻辑号、分布式快照隔离）以仓库 T8/T14 文档为准；生产建议至少 3 节点时启用中心化 TSO 并做好 TSO 节点高可用规划。

---

## 8. HA 与故障处理

### 8.1 故障检测

- 心跳：PING/PONG，默认 2s/6s；超时标记节点 down（OnDown 回调）。

### 8.2 自动恢复（M6）

- **Region 自动重指派**：down 节点持有的 region 自动重指派到存活节点（最全副本数据源 + `REGION_PUSH`），替代手动 `ASSIGN REGION`。
- **读 failover**：跨节点读遇 down 自动换新归属（QUERY_REQ 重路由），客户端无感知。
- **复制主从切换**：follower 在旧 master down 后可自动提升（`PromoteToMaster` / `EnableAutoFailover` / `StopAutoFailover`），写路径保持可用。

### 8.3 配置项与边界（KNOWN_LIMITATIONS）

- `ha.epochEnabled` 默认**开启**；`ha.quorumEnabled` 默认**关闭**（两节点集群场景保持默认关闭；启用 quorum 的前提与行为以仓库实际为准）。
- HA 相关开关为 Go API / 配置项形态，CLI/SQL 层是否暴露以仓库实际为准。

### 8.4 运维建议

- 至少 3 节点起步，避免 2 节点脑裂风险（quorum 默认关）。
- 重要节点故障演练：kill 节点 → 观察 `SHOW NODES`（State=down）→ 验证读 failover 与 region 重指派 → 恢复节点回连（心跳恢复、路由同步）。
- 故障节点恢复后建议人工核对 `SHOW REGION ROUTES` 与数据一致性，必要时 `ASSIGN REGION` 重新平衡。

---

## 9. 分布式备份

- 支持跨节点一致快照备份（T35 已交付）：备份协调器驱动多节点一致快照，manifest 记录 GlobalLSN（以仓库实际为准）。
- 单节点逻辑备份仍可用：`BACKUP TO '<path>'` / `RESTORE FROM '<path>' [TO LSN <n>]`（见单机文档 §7）。
- 分布式场景建议：定期 `BACKUP` + binlog 保留，配合 `RESTORE ... TO LSN` 做 PITR 演练。

---

## 10. 验证清单（多节点写入）

| # | 验证项 | 命令/语句 | 预期 |
|---|--------|-----------|------|
| 1 | 集群启动 | 各节点 `openxdb doctor --data-dir ... --addr 127.0.0.1:7788` | 退出码 0 |
| 2 | 节点注册 | `SHOW NODES` | 全部节点 Up，Role=Data |
| 3 | 路由可见 | `SHOW REGION ROUTES` | region → node 映射正确 |
| 4 | 分布写入 | 在 node-a 建表后 `ASSIGN REGION ... TO NODE 'node-b'`，再执行跨 region INSERT/UPDATE/DELETE | 无 2PC 错误返回 |
| 5 | 跨节点读 | 在 node-a 上 `SELECT` 远端 region 数据 | 结果正确（转发+合并） |
| 6 | 故障演练 | kill node-b → node-a 执行读/写 | 读 failover 生效，region 重指派完成 |
| 7 | 指标 | `openxdb stats --addr 127.0.0.1:7788 --interval 5` | LSN 增长、region 分布正常 |

---

## 11. 边界与已知限制

- **跨节点写依赖 2PC**：单节点不触发；2PC 协调者/参与者失联时存在 `TXN_COMMIT_UNCERTAIN` 提交不确定窗口（以 `m:2pc:*` 账本为准）。
- **单机模式集群语句报错**：未以 `--cluster-addr` 启动的节点执行 `ADD NODE`/`SHOW NODES`/`BALANCE` 等返回 `cluster not enabled`。
- **HA quorum 默认关闭**、epoch 默认开启；两节点集群不防脑裂（生产建议 ≥3 节点并评估启用 quorum，启用前提以仓库实际为准）。
- **性能数值为单机端侧基准**：README/E5 中的 QPS/tpmC 不反映分布式性能；分布式性能需自行基准测试（仓库暂未提供多节点基准报告）。
- 跨节点 DDL、分布式备份、HA 切换等高级能力的**具体命令/参数形态以仓库实际为准**（部分能力为 Go API 而非 SQL/CLI）。
- 其余以 `KNOWN_LIMITATIONS.md` 与 `docs/T13_m4_multinode.md` / `T14_m5_2pc.md` / `T15_m6_ha.md` 为准。

---

## 12. 参考资料

- `docs/T13_m4_multinode.md`（M4 多节点：节点拓扑/握手/路由/跨节点读）
- `docs/T14_m5_2pc.md`（M5 分布式写/2PC 状态机与错误语义）
- `docs/T15_m6_ha.md`（M6 HA）、`docs/T43_ha_rpo_rejoin.md`（RPO 补追/回连）
- `docs/product_manual_zh.md` 第 8 章（分布式能力）
- `KNOWN_LIMITATIONS.md`、`CHANGELOG.md`

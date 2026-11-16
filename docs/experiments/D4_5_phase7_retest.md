---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_6c0babfdda5b11f19232525400248c00
    ReservedCode1: 5nupcOc8O6je6CuvyxtFHEJ6PsFsCebgk2zPp2gV8VMddsF8LC5olr7Q2bLNeksV8G/xCrxvi0Gilvdo6kpwUIJwnNmiTRRZatv/bjdhb64ZxQjb77PHWlLYO2mkSyMZEwECRDchmIs+47uXHN8Z29ccdVIngvtlS30C6R6U8UKffYC3miCYyCSPP94=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_6c0babfdda5b11f19232525400248c00
    ReservedCode2: 5nupcOc8O6je6CuvyxtFHEJ6PsFsCebgk2zPp2gV8VMddsF8LC5olr7Q2bLNeksV8G/xCrxvi0Gilvdo6kpwUIJwnNmiTRRZatv/bjdhb64ZxQjb77PHWlLYO2mkSyMZEwECRDchmIs+47uXHN8Z29ccdVIngvtlS30C6R6U8UKffYC3miCYyCSPP94=
---

# D4.5 Phase7 复测报告：quorum 开启态 VM 三节点实测复验

- 日期：2026-11-07
- 基线提交：`0303c13`（local HEAD / origin/main / gitee 三端一致，`git fetch` 后核验）
- 环境：Windows 11 本机三节点共置（A `127.0.0.1:7791` / B `:7792` / C `:7793`，node id 分别为 `node-a42c0521`、`node-3d25549b`、`node-4a22640d`），数据目录 `temp/p7/data_g4/{A,B,C}`
- 场景：`SET ha_quorum_enabled=on` + `ha_epoch_enabled=on`（会话级开启）后执行重启/主宕机/TSO 主宕机复验
- 本轮不改 quorum 默认值、不改已入库修复逻辑；脚本与原始输出均为会话中间产物（`temp/p7/`），未入库

## 1. 结论摘要

| 项 | 目标 | 本轮判定 | 依据 |
|---|---|---|---|
| NEW-1 重启双主分叉丢数据 | 闭环 | **部分闭环（丢数据已消除，双主可写未消除）** | §2：二次重启后三端行集一致、无缺行；但重启节点仍自认 owner 并成功写入，覆盖新主已确认值 |
| NEW-2 `ha_*` 持久化后重启 | 闭环 | **未闭环（新崩溃点，比 Phase4 更严重）** | §3：`persisted.json` 中 `ha_quorum_enabled=on` 时启动即进程崩溃（`0xc0000005`），2×2 对照 2 轮稳定复现 |
| B2 TSO HA 生产接入 | 闭环 | **PASS** | §4：TSO 所在节点 C 宕机后，A/B 跨节点写分别在 0.09s / 0.10s 恢复成功，终态三端一致 |
| T3 集群轨 fencing / 真分区 | 闭环 | **NOT VERIFIED（环境受限，未伪造）** | §5：本机无法构造可过滤分区；VM 目标可达但无访问凭据 |

**对 §8 收口条件的判定：`ha.quorumEnabled` 目前不满足默认开启条件。** 三项 GAP 中 B2 已可判 PASS，T3 仍为 NOT VERIFIED，且本轮新增 NEW-2 启动崩溃（阻塞级）。D4.5 报告维持 **v0.2**，不得升 v1.0。

## 2. NEW-1（重启双主分叉丢数据）

### 2.1 场景与数据

region 4 归属 A，先落 30 行基线（id 201~251 段），`SET ha_quorum_enabled=on` 后停 A，等待 B 侧观察 A 失联并接管：

- `failover_ok = true`，`new_owner = B`（`node-3d25549b`），A 停机期间 B 完成升主
- 多数派写（B/C 存活 2/3）：`260='c'`、`262='new'` 写成功（run new1h）
- 启 A：`route_A_on_boot` 显示 region 4 `owner=node-a42c0521`、`local=true`，即 A 启动后仍视该 region 为自有
- A 试探陈旧写：同键 `260='stale-262'` 与新增 `262='stalenew'` **均返回成功**（`stale_write_ok=[true,true]`，无错误）
- 三端读值（A/B/C 完全一致）：`260=stale-262`、`262=stalenew`，`divergent_keys=[]`、`stale_ack_lost=[]`
- A 二次重启后：三端行集与行数完全一致，无 diff、无缺行（对比 Phase4 同一路径下 A 侧 70 行静默丢失）

### 2.2 判定

- **不丢数据部分已闭环**：Phase4 中"A 二次重启后本地行消失"的现象未再复现，三端终态行集一致，行数与写入数吻合。
- **双主可写未闭环**：A 重启后未采纳 B 的新归属（`owner=B`），仍以 `local=true` 自持并接受写；其陈旧写成功落库并**覆盖了 B 已确认的同键值**（`260` 由 `c` 变为 `stale-262`），说明少数派/陈旧主未被 fence。这属于 T3「集群轨 epoch fencing 未生效」的同一根因（集群轨 `masterEpoch` 恒为 0，`AllowWrite` 门槛不生效），并非独立缺陷。
- 结论：NEW-1 由"分叉丢数据"降级为"陈旧主可覆盖写（RPO>0 语义内但破坏新主已确认值）"，**不得单独记为 PASS**，与 T3 合并收口。

## 3. NEW-2（`ha_*` 持久化后重启）

### 3.1 现象

三节点执行 `SET PERSIST ha_epoch_enabled=on` + `SET PERSIST ha_quorum_enabled=on`，`persisted.json` 写入正确（三节点均含两个键，值 `on`）。随后逐个重启，**节点启动即崩溃**：

```
Exception 0xc0000005 0x0 0x0 0x7ff611e0332e
signal arrived during external code execution
  rocksdb._Cfunc_xdb_snapshot_create(0x0)
  rocksdb.(*rocks).Snapshot  rocksdb.go:164
  txn.(*txnManager).Begin    txn_impl.go:124
  sql.(*Executor).regionHoldsLocal  multinode.go:562
  sql.(*Executor).regionHoldsLocalTarget  ha_p1.go:436
  sql.(*Executor).holdsRegion  ha_p1.go:53
  sql.(*Executor).EnsureRegionReplicas  ha_b1.go:196
  sql.(*Executor).b1EnsureAsync.func1  ha_b1.go:314   ← 启动期异步 goroutine
```

另一节点同类崩溃点为 `xdb_snapshot_scan`（`rocksdb.go:262`，`regionHoldsLocal` `multinode.go:567`）。崩溃前日志可见 `holds check scan failed ... connectex: refused`，即对端不可达时仍进入副本补齐流程。

### 3.2 因果对照（2×2，两轮稳定）

仅改 `persisted.json` 的 `ha_quorum_enabled`（`ha_epoch_enabled` 恒为 `on`），单节点 A 启停：

| 轮次 | persisted `ha_quorum_enabled` | 启动 ready | 进程存活 |
|---|---|---|---|
| 1 | on | false | 崩溃退出 |
| 1 | off | true | 正常 |
| 2 | on | false | 崩溃退出 |
| 2 | off | true | 正常 |

四组结果一致；将三节点 `ha_quorum_enabled` 全部改回 `off` 后三节点一次拉起成功、集群视图 3/3 UP、`SELECT COUNT(*)` 正常。**变量唯一，因果成立。**

### 3.3 根因定位

- Phase5 的顺序修复已生效：`db.Open` 中 `eng.SetHAConfigSetter(db)`（`db.go:306`）先于 `LoadPersistedConfig()`（`db.go:311`），故 Phase4 的 `requires cluster HA configuration` 错误不再出现。
- 新崩溃点在其下游：持久化应用 `ha_quorum_enabled=on` 会触发 `b1EnsureAsync()`（`pkg/sql/ha_config.go`），该函数以 goroutine 调用 `EnsureRegionReplicas → holdsRegion → regionHoldsLocal → txnManager.Begin → rocks.Snapshot`，而此刻 Executor 侧的存储/事务句柄尚未完成与 `db.Open` 所建实例的绑定，C 层收到空句柄 `xdb_snapshot_create(0x0)` → 原生访问违例。
- 即"配置加载顺序"缺陷已在 Phase5 修掉，但"持久化开启项触发的启动期异步副本补齐"缺一道就绪门控，崩溃形态由友好报错退化为原生段错误（无 Go panic 信息、无 `crash.log`）。

### 3.4 判定

**NEW-2 未闭环。** 该缺陷为阻塞级：一旦某节点曾 `SET PERSIST ha_quorum_enabled=on`，该节点此后重启必崩，且崩溃发生在启动早期，无法靠 `SET` 回退（只能手工改 `persisted.json`），生产上是不可接受的自锁。修复建议：`b1EnsureAsync` 需以存储就绪事件/原子门控（如 `txnMgr.SetStore` 完成后置位）为前置，或在 `b1EnsureAsync` 内做句柄非空校验并降级为日志告警。

## 4. B2（TSO HA 生产接入）

- 启动参数带 `--tso-ha-group`（Phase5 `a4b33bf` 接线），三节点组网完成后确认 TSO 主在 C。
- `kill` C（`exit_code=1`，强杀）后立即从 A、B 发起跨节点写：
  - A：`first_success_id=400`，耗时 **0.09s**，1 次尝试即成功
  - B：`first_success_id=500`，耗时 **0.10s**，1 次尝试即成功
- 崩溃后数据面（A/B）：末尾行集完全一致（含 400、500），无缺失、无重复
- 重启 C 后 `ready=true`，最终三端行集一致（A/B/C 集合完全相同）

**判定：B2 PASS。** Phase4 记录"TSO 未接线、TSO 节点宕机即全集群跨节点写失败"的阻塞在本基线已消除，TSO 故障切换在秒级内完成。

## 5. T3（集群轨 fencing / 真分区）

### 5.1 构造尝试（全部失败，如实记录）

| 尝试 | 结果 |
|---|---|
| 三节点改用独立 loopback 别名 `127.0.0.11/12/13` | **可绑定成功**（`bind-ok`，三地址均可 listen） |
| Windows 防火墙按 IP 阻断 `127.0.0.11 ↔ 127.0.0.12` | **无效**：规则下发返回 0，但实测 `connect-succeeded`，loopback 流量不经防火墙过滤，无法形成单节点分区 |
| 容器/虚拟化隔离（docker / WSL / vmrun） | `docker=null`、`wsl=""`、`vmrun=false`，本机无可用隔离手段 |
| 目标 VM 上部署三独立 IP 节点 | `192.168.100.200:22` 可达，但无 SSH 凭据/免密（`Permission denied`），无法部署；`192.168.100.201/203` 已不可达 |

历史踩坑复现一致：单 IP 多端口共置 + 主机级规则会退化为三向全断，无法构造"多数派可写 + 少数派被 fence"。

### 5.2 判定

**T3 NOT VERIFIED（环境受限）。** 未构造任何伪造分区结论。可从 §2 观察侧面证据：陈旧主 A 重启后仍被授权写并覆盖新主值，与源码事实一致——集群轨 `masterEpoch` 恒 0，`AllowWrite` 门槛不生效，T3 GAP 在代码层面**仍然存在**，只是无法以真分区形态在本次环境复现量化。收口需具备三独立 IP（或进程级网络隔离）环境，或改走应用层隔离方案。

## 6. 对 §8 收口条件的逐项判定

| §8 条件 | 本轮状态 | 说明 |
|---|---|---|
| 三项 GAP 全 PASS 才可转 v1.0 | **未满足** | B2 PASS；T3 NOT VERIFIED；B1（单副本不可恢复）本轮未覆盖，维持文档既有结论 |
| NEW-1 / NEW-2 修复复验 | **未满足** | NEW-1 部分闭环（不丢数、但双主可写）；NEW-2 未闭环且为阻塞级崩溃 |
| 默认开启条件（§7 前置） | **不满足** | 现基线若把 `ha_quorumEnabled` 置默认 `on`，任何持久化过该开关的节点重启即崩，属可用性事故 |
| 报告版本 | **维持 v0.2** | 不升 v1.0 |

## 7. 后续建议（Phase8 候选）

1. **P0 阻塞（NEW-2 / 启动崩溃）**：`b1EnsureAsync` 增加存储就绪门控与非空句柄校验；补回归测试"persisted `ha_quorum_enabled=on` → 重启 → 正常 ready"（进程级，非函数级）。
2. **T3 合并收口**：将集群轨写入纳入 epoch 授权（或强制按 region 归属路由），使重启节点采纳权威 owner、陈旧主写被拒；与 D4.4 建议 4「写路径强制路由 / 2PC 参与」同批。NEW-1 剩余风险随该项闭合。
3. **环境补齐**：为真分区验证准备三独立 IP（VM 内 `ip addr add` 别名 + 重新 `ADD NODE` 组网）或进程级网络隔离方案，否则 T3 无法出 PASS。
4. **脚本补强**：报错路径需保留崩溃日志（当前原生段错误不落 `crash.log`，只能靠 stdout 抓栈）。

## 8. 产物索引（均为会话中间产物，未入库）

- 复测驱动脚本：`temp/p7/p7run.py`（stages：`new1g` / `new1h` / `new2` / `new2b` / `new2c` / `b2` / `t3probe`）
- 结构化结果：`temp/p7/results/{new1g,new1h,new2,new2b,new2c,b2,new2_cause,t3probe}.json`
- 运行日志：`temp/p7/logs/g4_{A,B,C}_*.log`、`temp/p7/results/run_*.log`
- 版本与提交：本轮无新增提交，仓库 HEAD 与 origin/main 均为 `0303c13`

*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

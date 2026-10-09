---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_b46766b0c38711f197eb525400393706
    ReservedCode1: WoyqqHY0uttq+90HODdWS8xuc7d1tXngDZys0RxX6rQl1Fv9oc1SJ0ull2v2BlrH4EIk/AM3TTFZINQNWz58NFNwplgK0aCIVf75ygz0a+k0cTwwuDSlf/uAXqKEiyJ0Nk1V7Yh9aWVZv+8Kc0Xre0+xjx/QFTRmQH4sfzj+RDu35AvBX7E8vqNtP9Q=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_b46766b0c38711f197eb525400393706
    ReservedCode2: WoyqqHY0uttq+90HODdWS8xuc7d1tXngDZys0RxX6rQl1Fv9oc1SJ0ull2v2BlrH4EIk/AM3TTFZINQNWz58NFNwplgK0aCIVf75ygz0a+k0cTwwuDSlf/uAXqKEiyJ0Nk1V7Yh9aWVZv+8Kc0Xre0+xjx/QFTRmQH4sfzj+RDu35AvBX7E8vqNtP9Q=
---

# T13: M4 多节点真分布式第一簇

> 状态：已完成（代码 + 测试 + 文档 + README 勾选闭环）
> 对应：开发手册 §10（M4 多节点）；README Roadmap「M4 multi-node cluster」
> 基线：M2 复制（T11）、M3 分片（T12）已闭环，本簇在其上做多节点化第一步
> 验收：`go build ./...` 与 `go test ./... -count=1` 全绿（本机真实 CGO 工具链）

## 背景与目标

M2/M3 已提供复制与分片能力，但路由仍停留在单节点内核内（M3 Router 的
region → 键区间映射不感知节点）。M4 第一簇把"region 归属"从单机逻辑提升为
**集群路由表（region → 节点）**，并打通跨节点读路径：SELECT 展开后非本地
region 经 TCP 转发到归属节点执行、取回结果集合并排序。

本簇范围明确：

- ✅ 节点拓扑：NodeInfo + 节点注册表 + 握手 + 存活检测
- ✅ 集群路由：region → 归属节点，本节点 Local 的 region 本地执行
- ✅ 跨节点查询：点查 / 范围 / 全表经路由展开，远端转发执行并合并
- ✅ 单节点部署零回归（所有 region 本地，转发路径零触发）
- ❌ 写路径跨节点（分布式事务 / 2PC）：**留待下一簇**，本簇仅拦截远端写

## 架构形态

```
┌─────────────── node-a (selfID) ───────────────┐
│ pkg/sql/Executor                              │
│   ├─ cluster.Manager（节点注册表 m:nodes）      │
│   ├─ sharding.Router（集群路由表 m:regions）    │
│   └─ ExecutorRouter：getKVCluster/scanMergedCluster
│       本地 region → 本地 tx；远端 region → ScanRemote 转发
│ pkg/cluster.Server（TCP 监听，帧 16-21）        │
└───────────────┬───────────────────────────────┘
                │ 帧：NODE_HELLO(16/17) QUERY(18/19) REGION_PUSH(20/21)
                │     存活：PING/PONG(5/6，复用 M2 心跳协议)
┌───────────────▼───────────────────────────────┐
│ node-b：对等结构；双向注册（HELLO 自动互学）      │
└───────────────────────────────────────────────┘
```

### 节点拓扑（pkg/cluster）

| 类型 | 说明 |
|------|------|
| `NodeInfo` | `NodeID` / `Addr` / `Role`（master/follower）/ `State`（active/down）+ 最近存活时间 |
| `Manager` | 节点注册表：`AddNode` / `GetNode` / `Nodes` / `RegisterPeer` / `NodeAddr`；落盘 `m:nodes`（Open 时恢复，无需手动发现） |
| `Server` | TCP 服务：`handleHello` 双向注册对端并落盘、`handleQuery` 区间扫描、`handlePush` 数据装载；`RegisterSelf` 自我注册 |
| `Client` | 短连接客户端：`hello` / `queryRange` / `queryGet` / `pushRegion` |
| `Heartbeat` | 周期 PING/PONG（默认 2s/6s），超时标记节点 down（复用 M2 心跳协议，无新协议） |

节点管理入口：`ADD NODE '<addr>'` / `SHOW NODES`。
节点身份：`openxdb start --cluster-addr <addr>` 启动集群模式，节点 ID 由
`--node-id` 指定或按数据目录/地址自动生成（`db.StartCluster` 装配）。

### 集群路由表（pkg/sharding + pkg/sql）

- M3 `RegionInfo` 原有字段保留，新增 `Node`（json `node,omitempty`）：region 归属节点；
- `Router` 新增集群方法：`RegionNode` / `SetRegionNode` / `FindRegion` / `AllRegions` / `UpsertRegion`；
- **Local 语义**：`Node == ""` 或 `Node == selfID` 的 region 本地执行（单节点部署所有 region 满足此条件 → 零回归）；
- 路由表落盘 `m:regions`，`ASSIGN REGION <id> TO NODE '<nodeID>'` 更新归属并推送数据；
- 管理入口：`SHOW REGION ROUTES`（region/table/start/end/node/local 一表查看）；
- 隐式单 region（`tableID|1<<63`，未 SPLIT 的表）：`ASSIGN` 时先显式化注册再指派，保证 `SetRegionNode`/`persistRegions` 可定位。

## 跨节点查询执行路径

```
SELECT ... FROM t WHERE ...   （execSelect）
   └─ e.rowRanges(meta)       M3 区间展开（每个 region 一段物理区间）
        └─ scanMergedCluster(tx, ranges)
             ├─ 按 region 归属分组：本地 → tx.Scan（M3 原路径）
             │                 远端 → mgr.ScanRemote(node, start, end)（转发）
             ├─ 远端 QUERY_REQ(op=SCAN/GET) → 归属节点直扫本机存储（仅物理键字节，
             │   不感知行结构，避免 cluster↔sql 循环依赖）→ QUERY_RESP{Key,Value} 字节对
             └─ 结果集按 innerKey 升序稳定合并排序（与 M3 单机语义一致）
```

- 点查（主键等值）：`getKVCluster` 定位 region → 本地走原 GET，远端走 `OpGet` 转发；
- 范围 / 全表：`scanMergedCluster` 对展开区间逐段路由（本地直扫 / 远端转发）；
- 合并后进入原有 project/filter/sort/aggregate 执行器，上层语义零改动；
- 转发协议：帧 18/19，载荷 JSON（`QueryReq{Op,Start,End,Limit}` / `QueryResp{Rows}`），
  `[]byte` 字段 JSON base64 自动编解码；帧格式与 M2 完全一致（len 4B 大端 + type 1B + payload）。

### 写路径拦截

第一簇不实现分布式写事务：INSERT/UPDATE/DELETE 扫描到行后按 `rid` 调
`checkWritable`——region 归属非本地即返回语义错误（"cannot write remote region"），
防止误写远端数据。本地 region 写路径与 M2/M3 完全一致（零回归）。

## 数据装载（ASSIGN REGION）

`ASSIGN REGION <id> TO NODE '<nodeID>'` 执行三步：

1. 源节点扫描该 region 全量物理键数据（`[EncodeRegionKey(rid,StartKey), EncodeRegionKey(rid,EndKey))`，隐式单 region 上界 = 下一 region 前缀）；
2. 通过帧 20（`REGION_PUSH`）推送 `RegionPushReq{Region, Rows, Routes, SrcID}` 到目标节点；
   目标节点在同一 WriteBatch 内：写入全部行 + `UpsertRegion`（本 region Node=""、Local=true）+
   学习源路由视图（其余 region 按其归属记录，Node=="" 补记为 SrcID）+ 落盘 `m:regions`
   —— 数据与路由原子可见，覆盖式装载幂等；
3. 源节点更新本机路由（region → 目标节点）并落盘，返回成功。

**一致性约定**：本簇数据保留在源节点（不做删除搬迁）；读取始终以 region 归属为准，
两端路由视图经 REGION_PUSH 同步后一致，任一端发起查询都能正确展开。
写路径分布式事务（2PC）下簇统一处理。

## 与 M2 / M3 的关系

| 层 | 来源 | M4 中的角色 |
|----|------|-------------|
| 帧格式 / 心跳 | M2 `pkg/replication` | 帧格式完全复用；PING/PONG（5/6）直接复用，无新协议 |
| 键编码 / region 前缀 | M3 `pkg/sharding` | 物理键仍为 `r{regionID:8B}{innerKey}`，转发只搬运字节 |
| 区间展开 / 合并 | M3 `pkg/sql/sharding.go` | `scanMergedCluster` 在 M3 `scanMerged` 之上叠加节点路由 |
| 行解码 / 执行器 | M1/M3 | 合并结果直接进入原 project/filter/sort/aggregate |

## 设计取舍

- **转发只传物理键区间 + 字节行**：cluster 包不感知行结构，避免 cluster↔sql 循环依赖；
  代价是远端执行粒度粗（整区间扫描），下簇可引入谓词下推。
- **源视图为权威路由**：REGION_PUSH 携带源节点完整路由快照，目标节点整体学习，
  保证双节点路由视图完整且收敛（第一簇无 etcd/TSO，路由同步靠管理操作触发）。
- **短连接客户端**：查询/装载每次建立连接，简化生命周期管理；心跳长连接负责存活。
- **隐式 region 显式化**：ASSIGN 未 SPLIT 表时把隐式单 region 提升为显式条目，
  使路由表可落盘、可跨节点传递。
- **写拦截而非自动转发**：分布式写事务（2PC / TSO）复杂度不在本簇范围，先以
  显式报错保证数据安全，避免"看起来能写"的假分布式。

## 测试

### pkg/cluster 单元测试（cluster_test.go）

| 用例 | 覆盖 |
|------|------|
| `TestAddNodeHandshake` | 节点注册 + NODE_HELLO 握手双向互学 |
| `TestQueryForwardScanAndGet` | QUERY_REQ SCAN/GET 转发执行与结果返回 |
| `TestHeartbeatDown` | 心跳超时节点标记 down（存活检测） |
| `TestRegionPushLearnsRoutes` | REGION_PUSH 数据装载 + 目标节点学习源路由视图 |

### pkg/db 集成测试（db_cluster_test.go，进程内双实例）

| 用例 | 覆盖 |
|------|------|
| `TestM4CrossNodeFullConsistency` | SPLIT 后中间 region 指派 B：A/B 两侧 `SELECT *` 全量一致（本地+远端混合）、点查远端/本地、范围查跨 3 region、COUNT 聚合、UPDATE 本地成功 + 远端拦截 |
| `TestM4SingleRegionAssign` | 未 SPLIT 单 region 指派 B：隐式 region 显式化指派、A 侧全量经转发取回、B 侧全本地、远端点查正确 |

### 既有回归

`go test ./... -count=1` 覆盖：replication（binlog/heartbeat）、server、sharding、
sql（含 M3 11 项分片集成）、txn、wal、storage（rocksdb/btree）、db 全绿。

## 验收结果

本机真实工具链（MinGW-w64 + RocksDB 11.8.1 静态链接）：

```
go build ./...  -> 全绿
go test ./... -count=1  -> 全绿（cluster / db / replication / server / sharding / sql / storage / btree / rocksdb / txn / wal）
```

## 新增 / 修改文件

| 文件 | 变更 |
|------|------|
| `pkg/cluster/`（新） | 节点拓扑 + 握手 + 心跳 + 查询转发 + 数据装载（帧 16-21） |
| `pkg/sql/multinode.go`（新） | 集群执行器：SetCluster / getKVCluster / scanMergedCluster / execAddNode / execShowNodes / execShowRegionRoutes / execAssignRegion / 持久化 |
| `pkg/sql/lexer.go / ast.go / parser.go` | ADD / NODE / NODES / REGION / ROUTES / ASSIGN 语法与语句节点 |
| `pkg/sql/executor.go` | Executor 注入 mgr/selfID；execStmt 4 个集群分支；读路径集群感知；INSERT/UPDATE/DELETE 远端写拦截 |
| `pkg/sql/sql.go` | ExecutorRouter 包装；LocateRegion 导出 |
| `pkg/db/db.go` | Cluster/ClusterSrv 字段；StartCluster；Open 恢复 m:nodes；Close 关闭集群 |
| `cmd/openxdb/main.go` | start 增加 --cluster-addr / --node-id 参数 |
| `pkg/db/db_cluster_test.go`（新） | M4 双节点集成测试 |
| `pkg/cluster/cluster_test.go`（新） | cluster 单元测试 |
| `docs/T13_m4_multinode.md`（本文件） | M4 设计记录 |
| `README.md` | Status / Highlights / Architecture / Roadmap 勾选 M4 |
*（内容由AI生成，仅供参考）*

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_86f4216cc39e11f18019525400248c00
    ReservedCode1: r+9LNmtjgzsDdC0MWSNx5fYYc+8q1esCbMu6KFFwSrJf3v56Qo3P1HVmZ7d2du0yjFo5P0pamm7UW6SMURlmuTZXWVdD2MqxlBYq54FndZsf1kijYTtvlA/Y/uBzllfejQcCQ7IYQOF8g/bkaHvwd5UjrErv0bAukGxZ6eDS/cYBzHnrKDFe57uh1bY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_86f4216cc39e11f18019525400248c00
    ReservedCode2: r+9LNmtjgzsDdC0MWSNx5fYYc+8q1esCbMu6KFFwSrJf3v56Qo3P1HVmZ7d2du0yjFo5P0pamm7UW6SMURlmuTZXWVdD2MqxlBYq54FndZsf1kijYTtvlA/Y/uBzllfejQcCQ7IYQOF8g/bkaHvwd5UjrErv0bAukGxZ6eDS/cYBzHnrKDFe57uh1bY=
---

# T15 M6 高可用与故障转移（HA/Failover）

- 状态：已落地（v5.0-P4-M6，v0.6.0-alpha）
- 前置：M2 复制（binlog/replicator/follower/heartbeat）、M4 集群（NodeInfo 注册表 + PING/PONG + ASSIGN REGION）、M5 2PC 写路径
- 目标：心跳超时故障检测 → 节点 down → region 自动重指派 → 读 failover → 主从切换，全链路不向客户端报错、写路径保持可用
- 设计取舍：单节点部署与健康集群路径零触发（M4/M5 行为不变）

## 1. 背景与问题

M4/M5 已提供：

- 节点注册表 `m:nodes`（NodeInfo{ID, Addr, Role, State, LastSeen}），`ADD NODE` 握手注册、`SHOW NODES` 查询；
- 心跳 PING/PONG（复用 M2 帧），超时置 `StateDown`（`MarkDown`）；
- region 路由表 `m:regions`（RegionInfo{RegionID, StartKey, EndKey, State, Node}），`ASSIGN REGION` 手动指派 + `REGION_PUSH` 数据推送；
- 跨节点读：`QUERY_REQ/QUERY_RESP`（物理键区间）转发 + 合并；
- 跨节点写：2PC 协调者/参与者（帧 22-27），崩溃恢复清理标记。

不足：节点 down 后只是标记状态，region 仍挂在 down 节点上，跨节点读会返回 `ErrNodeDown` 类错误；复制链路 master down 后 follower 只能停在 `offline`，写路径中断。M6 补齐闭环。

## 2. 总体设计

M6 由三层协同实现，全部挂接在既有心跳/路由/复制机制上：

| 层 | 文件 | 职责 |
|---|---|---|
| 集群心跳 | `pkg/cluster/heartbeat.go` | PING 超时 → `MarkDown` → 触发 `OnDown(nodeID)` 回调 |
| 集群注册表 | `pkg/cluster/cluster.go` | `IsDown` 查询、`SetOnDown` 装配、`notifyDown` 内部派发 |
| SQL 层 HA | `pkg/sql/ha.go` | region 自动重指派（`FailoverDownNode`）、读 failover（`resolveRegionOwner`） |
| DB 层 HA | `pkg/db/ha.go` | 复制主从切换（`PromoteToMaster` / `EnableAutoFailover` / `StopAutoFailover`） |

### 2.1 故障检测（心跳超时 → down）

- `Manager.StartHeartbeat(interval, timeout)` 周期向所有注册节点发 PING；超过 timeout 未见 PONG 即 `MarkDown(id)`，将 `NodeInfo.State` 置 `StateDown` 并更新 `LastSeen`。
- 新增 `SetOnDown(fn)` 装配回调、`IsDown(id)` 查询、`notifyDown(id)` 内部派发（带 `onDownMu` 互斥，防止并发通知重复触发）。
- 心跳 tick 在 `MarkDown` 后调用 `mgr.notifyDown(n.ID)`；仅当节点此前为存活状态（`StateUp`）时才回调，避免重复 down 反复重指派。
- 回调只做"通知"不做数据搬迁：SQL 层异步重指派，不阻塞心跳循环。

### 2.2 Region 自动重指派（替代手动 ASSIGN REGION）

- `Engine.FailoverDownNode(nodeID)` 是入口代理（`pkg/sql/sql.go`），内部加 `failoverMu` 互斥 + `failoverDone` 去重（同节点只处理一次），避免并发回调重入。
- 对路由表中所有 `RegionInfo.Node == nodeID` 的 region 执行 `failoverRegion`：
  1. 选目标：`ListNodes()` 中剔除 selfID 与 down 节点，优先存活远端；无存活远端时回退到本节点（selfID）；
  2. 数据追赶：在本节点与所有存活远端中，对每个候选执行 `scanRegionRows(rid, [start,end))` 收集该 region 物理键空间内的行（KV + 索引），选出行数最多的作为"最全副本"数据源；
  3. 推送：若目标为远端，调用 `PushRegion(remote, rid, rows, regionsView, srcID)`（复用 M4 REGION_PUSH 帧），目标节点 `applyPush` 本地写入 + 学路由 + 落盘 `m:regions`；
  4. 路由更新：本节点 `SetRegionNode(rid, target)` + `persistRegions()`，其余存活节点经下一次心跳路由同步（`getClusterRoutes`）学习新归属。
- 行数为 0 的 region（空 region 或仅 down 节点持有数据）也照常推进路由，保证路由表始终指向存活节点。

### 2.3 读 failover（QUERY_REQ 遇 down 自动换目标）

- 读路径统一收敛到 `resolveRegionOwner(rid)`（`pkg/sql/ha.go`）：
  - 先查路由归属（`FindRegion`）；
  - 若归属节点为 selfID → 本地读；
  - 若归属节点 down（`mgr.IsDown(owner)`）→ 触发 `FailoverDownNode(owner)` 完成重指派，再用重指派后的新归属继续；
  - 否则 → 转发 `QUERY_REQ` 到归属节点。
- 点查（`getKVCluster`）与范围/全表扫描（`scanMergedCluster`）均改走 `resolveRegionOwner`，遇 down 自动 failover 到新持有者，客户端无感知。
- 单节点部署（`mgr == nil`）与健康集群（无 down）路径与 M4/M5 完全一致（零触发）。

### 2.4 主从切换（复制链路故障转移）

- `PromoteToMaster()`（`pkg/db/ha.go`）：停止 follower 拉取线程，启动本机 replicator 监听，follower 目录内的既有数据完整保留，旧 master 已复制数据不丢失，可立即继续写。
- `EnableAutoFailover(onPromote func())`：启动后台 goroutine，每 500ms 检查 follower 状态；当 follower 进入 `offline`（旧 master 心跳/连接超时）且未提升时自动调用 `PromoteToMaster()` 并触发回调（幂等：`failoverMu` 保证只提升一次）。
- `StopAutoFailover()`：停止 goroutine（`failoverStop chan` + WaitGroup），`DB.Close()` 中自动调用，避免泄漏。

## 3. 关键实现

### 3.1 pkg/cluster/heartbeat.go

```go
if n.State != cluster.StateDown {   // 仅存活→down 时通知
    h.mgr.MarkDown(n.ID)
    h.mgr.notifyDown(n.ID)          // 回调 SQL 层重指派
}
```

### 3.2 pkg/sql/ha.go

```go
// FailoverDownNode 节点 down 后自动重指派其全部 region 到存活节点。
func (e *Executor) FailoverDownNode(nodeID string) (int, error) {
    e.failoverMu.Lock()
    defer e.failoverMu.Unlock()
    if e.failoverDone[nodeID] {
        return 0, nil          // 幂等：同节点只处理一次
    }
    e.failoverDone[nodeID] = true
    regions := e.router.AllRegions()
    n := 0
    for _, rg := range regions {
        if rg.Node != nodeID {
            continue
        }
        if err := e.failoverRegion(rg.RegionID); err != nil {
            return n, err
        }
        n++
    }
    return n, nil
}
```

- `failoverRegion` 选目标（存活远端优先、selfID 兜底）→ 全副本扫描比行数取最全 → `PushRegion` 推送 → `SetRegionNode` + `persistRegions`。

### 3.3 pkg/db/ha.go

```go
func (d *DB) PromoteToMaster() error {
    d.failoverMu.Lock()
    defer d.failoverMu.Unlock()
    if d.Follower == nil {
        return errors.New("promote: not a follower")
    }
    d.Follower.Stop()
    d.Follower = nil
    if err := d.StartReplication(":0"); err != nil {
        return err
    }
    return nil
}
```

## 4. 测试矩阵

新增 4 个故障场景测试（`pkg/db/db_ha_test.go` + `pkg/db/db_ha_repl_test.go`）：

| 测试 | 场景 | 断言 |
|---|---|---|
| `TestM6DownNodeReadFailover` | 双节点：B 持有中间 region 后 down | A 心跳感知 `StateDown`；region 自动重指派回 A；A 上 SELECT 全量 10 行仍完整 |
| `TestM6AutoReassignRegionAfterDown` | 三节点：B down 后其 region 重指派 | region 归属迁移到存活远端 C；C 本地点查 id=6 命中；A 全量 SELECT 仍一致 |
| `TestM6PromoteFollowerWriteContinues` | 复制：master down 后 follower 手动升主 | follower `offline`；`PromoteToMaster` 后写 3 条新值；旧 10 条 + 新 3 条一致 |
| `TestM6AutoPromoteFollowerOnMasterDown` | 复制：master down 自动升主 | 回调触发；`Replicator != nil`；写继续；旧数据完整 |

辅助设施：`waitNodeState` / `waitRegionNode`（轮询节点状态与路由归属）、`speedupHeartbeat`（重启心跳为 150ms/450ms 加速故障感知）、`waitFollowerOffline`。

验收命令（本机 CGO 工具链：winlibs gcc16.2.0 + go1.27.1）：

```sh
set PATH=<OpenXDBTools>/mingw64/bin;%PATH%
set CGO_ENABLED=1
set CGO_CFLAGS=-I<OpenXDBTools>/rocksdb-11.8.1/include
set CGO_CXXFLAGS=-I<OpenXDBTools>/rocksdb-11.8.1/include
set CGO_LDFLAGS=-L<OpenXDBTools>/rocksdb-build -lrocksdb
go build ./...
go test ./... -count=1
```

结果：全仓 `go build ./...` 与 `go test ./... -count=1` 全绿（含新增 4 个 M6 测试）。

## 5. 边界语义与零触发保证

- **零回归**：`mgr == nil`（单节点）时 `resolveRegionOwner` 直接本地、`FailoverDownNode` 不被装配；健康集群无 down 节点时心跳不回调、路由不迁移，M4/M5 行为不变。
- **幂等**：`failoverDone` 保证同节点只重指派一次；`IsDown` 检查避免对已 down 节点重复处理；`PromoteToMaster` 用互斥保证只提升一次。
- **并发安全**：`onDownMu`（cluster）、`failoverMu`（executor/db）保护注册表与重指派互斥；心跳循环不被回调阻塞。
- **数据一致性**：重指派前全副本扫描选"最全副本"推送（含索引行），目标节点 `applyPush` 原子落盘 + 学路由；读路径在新归属生效后立即指向新持有者。
- **有限降级**：所有存活节点均 down 时 region 回退到本节点，保证本节点读不报错（单节点自持语义）。

## 6. 文件清单

| 文件 | 变更 |
|---|---|
| `pkg/cluster/cluster.go` | Manager 增加 `onDown`/`onDownMu` 字段、`IsDown`、`SetOnDown`、`notifyDown` |
| `pkg/cluster/heartbeat.go` | tick 超时 MarkDown 后触发 `notifyDown` |
| `pkg/sql/sql.go` | `Engine.FailoverDownNode` 代理方法 |
| `pkg/sql/executor.go` | Executor 增加 `failoverMu` / `failoverDone`（NewExecutor 初始化） |
| `pkg/sql/multinode.go` | 点查/范围扫描改走 `resolveRegionOwner` |
| `pkg/sql/ha.go` | 新增：自动重指派 + 读 failover 核心实现 |
| `pkg/db/db.go` | DB 增加 `failoverStop`/`failoverMu`；`StartCluster` 装配 OnDown 回调；`Close` 停止自动故障转移 |
| `pkg/db/ha.go` | 新增：`PromoteToMaster` / `EnableAutoFailover` / `StopAutoFailover` / `failoverLoop` |
| `pkg/db/db_ha_test.go` | 新增：集群故障场景测试（读 failover + 自动重指派） |
| `pkg/db/db_ha_repl_test.go` | 新增：主从切换测试（手动提升 + 自动提升） |
| `pkg/storage/rocksdb/rocksdb.go` | cgo LDFLAGS 修正（消除 libstdc++ 双重链接，补 shlwapi/rpcrt4/ws2_32） |
| `docs/T15_m6_ha.md` | 本文档 |
| `README.md` | Status v5.0-P4-M6、Highlights、Architecture、Tests、Roadmap 勾选 M6 |
*（内容由AI生成，仅供参考）*

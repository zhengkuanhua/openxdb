# OpenXDB v2.0.4 Release Notes

发布日期：2026-11-25

## 版本亮点

- **根治「未切分表没有 region 归属」（P0）**：v2.0.3 只解决显式 region 在本机视图缺失，未切分表（隐式整表 region）的归属此前**从未广播**——它只存在于 owner 本机内存，`Node` 字段为空、`TableMeta` 无归属字段、路由条目不落盘。后果是集群里每个节点都认为整表归自己（普通节点各自本地写、数据互不可见），纯转发节点则永久报 `region N missing from local route view (forward-only node)` 不可写。
- **归属改为可派生 + 随权威视图下发**：未切分表的整表归属按确定性规则派生（注册表内节点 ID 字典序最小者，候选含本机，三节点结论一致），随 `REGION_PUSH` 的独立字段 `implicit_routes` 下发；接收端只写内存映射并据此走 `routeRemoteOwned` 正常 2PC 转发，**不落盘、不改 `m:regions`**，显式路由的持久化与一致性判定完全不受影响。
- **绝不静默落本地**：纯转发节点在学到归属前，写路径判 `REGION_NOT_ASSIGNED`（`region ... missing from local route view ... retry shortly`）并异步请求视图同步，学到后即转发真实 owner。
- **顺带修掉一个自死锁**：写路径 `regionOwnershipFence → AllowWriteRegion → routeDecision → forwardOnlyNode → db.ForwardOnlyNode` 在持有 `epochMu` 的调用链内重入加锁会自死锁（`TestD45P9RestartOwnerConfirmedByPeersE2E` 由 3s 变 4min 超时）。改为原子镜像 + `TryLock`：能取锁则刷新镜像，取不到锁读最近镜像值，写路径不再二次进锁。

## 变更明细

| 编号 | 内容 |
| --- | --- |
| 归属派生 | `pkg/sql/implicit_route.go`（新增）`implicitRouteOwner()` / `implicitRegionRouteFor()` / `implicitRoutesForViewSync()` |
| 视图下发 | `pkg/cluster` `RegionPushReq.ImplicitRoutes` 独立字段（旧节点忽略）；`SyncRouteViewToEpochWithImplicit` 携带；`serveRegionPush` 学习 |
| 接收学习 | `LearnImplicitRoutesFromPeers` 仅写 `Router.implicit`，nil 载荷等价清空；`RegionNode` 对隐式 ID 兜底查该映射 |
| 写路径 | `routeViewMissing` 覆盖「隐式 + 纯转发 + 未学到归属」；`routeDecision` 对该情形返回 `routeUnassigned`，由 `decideWriteRoute` 同步后重判 |
| 删表清理 | `ApplyRemoteDDL` 在 `DDL_OP_DROP` 后 `forgetTableRoutes`（清显式条目与隐式归属） |
| 死锁修复 | `pkg/db` 新增 `forwardOnlyMirror atomic.Bool`；`ForwardOnlyNode` 改 `TryLock` + 镜像，`forwardOnlyLocked` 与启动装载同步刷新 |
| 版本口径 | `pkg/version` → `v2.0.4`，同步 CHANGELOG / RELEASE_NOTES（v2.0.3 说明归档 `docs/RELEASE_NOTES_v2.0.3.md`） |

## 测试证据

- 新增 `pkg/sql/v204_implicit_route_test.go`（8 组）：派生归属三节点一致、单机返回 nil 零回归、发送/学习闭环、纯转发节点学归属前拒写（`REGION_NOT_ASSIGNED` + `missing from local route view`）且零落盘、学到后判 `routeRemoteOwned`、nil 载荷清空、删表清理。
- 移除 v2.0.3 中与新语义冲突的 `TestV203ForwardOnlyImplicitRegionZeroRegression`（其断言"隐式 region 恒本地"正是本版要推翻的行为）。
- 全仓 `go test ./... -p 1 -count=1` 连续三轮通过（零 FAIL，`pkg/db` ~65s、`pkg/sql` ~70s，单轮墙钟约 200s）。
- 回归对照：修复前 `pkg/db` 全包 602.8s 超时失败（`epoch.go` / `ha_p9.go` 锁冲突），修复后 62.8s 通过。

## 已知限制

- 跨节点读取未切分表仍可能取不到数据：读侧聚合的跨节点读视图在重启后未重建，与本次写路径修复无关，登记为后续 NEW 项单独立项。
- 纯转发节点首次写入某张新建未切分表时，若尚未收到权威视图，会先返回 `REGION_NOT_ASSIGNED ... retry shortly`，同步完成后重试即成功——这是刻意的保守语义，不做静默本地落盘。

## 升级说明

- 三机滚动替换二进制即可，无配置变更、无数据迁移、无 DDL 要求。
- 混跑注意：纯转发节点升到 v2.0.4 而 owner 仍是 v2.0.3 时，旧节点不发送 `implicit_routes`，纯转发节点仍无法写未切分表（与升级前一致，不会变差）；三机统一到 v2.0.4 后生效。

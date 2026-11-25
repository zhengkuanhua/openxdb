# OpenXDB v2.0.3 Release Notes

发布日期：2026-11-25

## 版本亮点

- **合并 v2.0.2 未发布内容**：v2.0.2 仅本地提交、未打 tag、未外发二进制，本版一次性发布两批 P0 修复——「纯转发节点被复制轨只读降级永久 fence」与「显式 region 在本机路由视图缺失被误判为本机归属」。对外可见版本线：v2.0.1 → v2.0.3。
- **纯转发节点写能力恢复（v2.0.2 内容）**：不持有任何 region 的节点（node-c）直连写入不再恒报 `demoted stale master is read-only`。豁免须同时满足 `clusterNonOwner && clusterOwnTerm>0 && !clusterOwned`——即「集群轨曾授予归属、随后被权威视图收回」；复制轨脑裂演练中的陈旧主（`clusterOwnTerm==0`）不进豁免。
- **视图缺失不再被伪装成「归属被收回」（本版核心）**：node-c 重启后本地路由视图缺新表 region（5816/5817），旧逻辑把它当作「表未切分的隐式单 region」，写请求落本地后被节点级归属 fence 拒为 `cluster ownership revoked`——归因指向错误方向，重新授权也解不开。本版把「隐式 region ID」「有条目未切分」「显式 region 缺条目」三种情形彻底分开，缺条目时要么异步拉权威视图自愈、要么明确报 `REGION_NOT_ASSIGNED`（`region missing from local route view`），**绝不静默落本地**。
- **fencing 强度未削弱**：复制轨陈旧主仍拒写自己持有的 region，`clusterPend` 重启待确认拒写与节点级 `AllowWrite` 契约不变。

## 变更明细

| 编号 | 内容 |
| --- | --- |
| 隐式/显式判别 | `pkg/sharding` 新增 `ImplicitRegionBit` / `IsImplicitRegionID`；`routeDecision` 改为「隐式 ID → 本机」「有条目未标 Explicit → 本机」「显式 ID 无条目 → 归属未知」，仅纯转发节点把第三种判 `routeUnassigned` |
| 错误语义 | `pkg/db` 新增 `ErrRegionViewMissing`；`clusterOwnershipFenceRegion` 在纯转发节点视图缺失时短路返回它，不再落回 `ErrFenced` |
| 视图自愈 | `pkg/sql` 新增 `forwardOnlyNode()` / `routeViewMissing()` / `requestRouteViewSync()`（异步拉权威 region 视图，冷却 + 互斥限频）/ `decideWriteRoute()`（拉取后重判） |
| 重启识别 | `loadClusterTermMeta` 在 `!clusterOwned && clusterOwnTerm>0` 时启动即置 `clusterNonOwner=true` |
| 运维文案 | `groupOps` 视图缺失时报 `region missing from local route view on this node ... route view resync requested, retry shortly`，不再引导执行 `ASSIGN REGION` |
| 版本口径 | `pkg/version` → `v2.0.3`，同步 CHANGELOG / README / RELEASE_NOTES |

## 测试证据

- 新增 `pkg/sql/v203_route_fix_test.go`（3 组）：转发节点缺视图拒写且零落盘、错误文案不含 `ownership revoked`、普通节点零回归、隐式 region 零回归。
- 新增 `pkg/db/v203_forward_view_test.go`（4 组）：启动识别纯转发、owned 节点不识别、视图缺失返回 `ErrRegionViewMissing`、非转发节点保持 `ErrFenced`。
- 调整 `pkg/db/v1210_new3_region_fence_test.go` 旧断言口径（不可判定 region 由 `ErrFenced` fallback 改为 `ErrRegionViewMissing`）。
- 定向回归：`pkg/sql TestV203` ok；`pkg/db TestV203|TestV2020|TestNew3` ok；回归子集 `Fence|Ownership|BrainSplit|Forward|Cluster|New3|Region` 两包均 ok；`go build ./...` / `go vet` 通过。
- 全仓 `-p 1` 串行回归单轮墙钟超过 600s（工具执行上限截断，历史 pkg/db 全量本就 >500s），未取得完整三轮结论，需在发版流水线上补跑。

## 兼容性说明

- **版本串**：`8.0.36-OpenXDB v2.0.3`；`MySQLServerPrefix`（`8.0.36-OpenXDB `）兼容契约不变，mysql CLI / JDBC / go-sql-driver 仍识别为 MySQL 8.0.x。
- **行为变更两处**：① 纯转发节点（不持有任何 region、且集群轨历史授予已被收回）的复制轨写不再被节点级 fence 拦截；② 显式 region 在本机视图缺失时，写不再"静默落本地后被 fence 拒"，改为明确 `REGION_NOT_ASSIGNED` + 后台自动补视图（补齐后重试即可成功）。
- **开关默认值不变**：`--tso-lease`、`ha_2pc_participant_hook`、`ha_write_route_enforce`、`remote_range_agg`、`ha_quorum` 默认值均不变。
- **元数据零迁移**：`clusterNonOwner` 为纯运行期标记不落盘，升级后无需任何数据操作。

## 已知问题

- **跨节点新表读不可见**：读侧聚合在节点重启后未重建为跨节点视图，属独立问题（读侧 NEW 项），本版写路径修复不覆盖。
- **HA quorum 未启用**：`ha_quorum` 默认关闭，三独立 IP 环境级 quorum 与网络分区验收仍为欠账。
- **`--tso-lease` 仍默认 off**：启用前请在自有环境做一轮双向 leader 终止演练。
- 其余 SQL / 性能 / 分布式边界见 `KNOWN_LIMITATIONS.md` §1–§5。

## 升级路径

- 二进制替换后按 node-a → node-b → node-c 滚动优雅重启；无元数据迁移、无停机改配置。
- 纯转发节点（node-c）替换后首次写新表 region 可能先返回一次 `REGION_NOT_ASSIGNED (route view missing)`，后台视图同步完成后重试即成功；若持续返回该错误，检查 peer 在线状态与 `SHOW REGION ROUTES`。

## 上一版本摘要（v2.0.2，未发布）

纯转发节点复制轨 fence 豁免：新增运行期标记 `clusterNonOwner`（权威全量视图确认本机不持有任何 region 时置位、`selfOwns=true` 时清除，不持久化）；`copyTrackWriteFenceLocked` 在 `clusterOwned` 之外追加带 `clusterOwnTerm>0 && !clusterOwned` 约束的豁免；配套 5 组单测 `pkg/db/v2020_nonowner_forward_test.go`。

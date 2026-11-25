# OpenXDB v2.0.5 Release Notes

发布日期：2026-11-25

## 版本亮点

- **根治「新建未切分表首写失败窗口」（P0 收敛）**：v2.0.4 解决了未切分表归属"从来不广播"，但纯转发节点的首写仍要走一遍"失败 → 拉视图 → 重试"的自愈链路。本版把归属登记提前到建表动作本身：`CREATE TABLE` / `CREATE TABLE AS SELECT` 提交并释放 DDL 锁后，异步补一次隐式整表 region 归属——本机先登记认知，再向在线对端推一档权威路由视图。新建表在 owner 侧写入直达本地，纯转发节点在推送到达后即知归属，不再依赖首写失败触发。
- **建表返回路径零副作用**：v2.0.4 已记录"DDL 路径上新增任何事务都会打乱其后副本维护时序"（B1 单副本用例 32s 超时即由此复现）。本版严格约束：建表返回路径只做一次轻量状态判断 + goroutine 派生，不开事务、不遍历表目录、不落盘；归属学习与视图推送全部在异步 goroutine 内完成。单机部署不派生任何行为，语义零变化。
- **推送去抖**：对端推送带 200ms 冷却窗口，批量建表（含脚本灌表）只推一轮，避免把管理面打成广播风暴；错过的表由下一次建表 / 视图刷新自愈覆盖。
- **用例确定性修复（非产品语义变更）**：`TestD45B1SingleCopyRecoverableE2E` 与 `TestV1200C4RegionSyncReconnectCatchUp` 的偶发 `lost=3` 定位为播种顺序与隐式归属规则耦合——隐式整表 region 归"在线节点中 ID 字典序最小者"，播种节点非 owner 时切分前的 10 行被转发到 owner，切分后 owner 本地无行。播种用例改为"建表 → 切 3 region → 写入"，并按 `SelfID` 固定节点角色。该 flaky 在 v2.0.4 基线即存在，非本版回归。

## 变更明细

| 项目 | 内容 |
| --- | --- |
| 新增 | `pkg/sql/implicit_push.go`：`notifyImplicitRouteView()` / `learnImplicitRouteLocal()` / `implicitPushCooldown` |
| 接线 | `pkg/sql/executor.go`：`execCreateTable`、`execCreateTableAsSelect` 在 `broadcastDDL`/`commitTx` 之后调用；`Executor` 新增 `implicitPushMu/Last/Busy/Hook` |
| 测试 | `pkg/sql/v205_implicit_push_test.go`（5 组：建表学归属、owner 本地、对端推送钩子、单机零回归、切分表不被覆盖） |
| 用例 | `pkg/db/d4_4_route_view_test.go`（`seedThreeRegions` 改为先切分后写入）、`pkg/db/d4_5_b1_replica_test.go`（三节点按 ID 排定角色） |
| 版本 | `pkg/version/version.go` → `v2.0.5`（握手串 `8.0.36-OpenXDB v2.0.5`） |

## 验证

| 用例 | 结果 |
| --- | --- |
| `TestV205*`（5 组） | 全 PASS |
| `TestD45B1SingleCopyRecoverableE2E` × 8 | 8/8 PASS |
| `TestV1200C4RegionSyncReconnectCatchUp` × 6 | 6/6 PASS |
| `pkg/sql` + `pkg/db` 全量（`-p 1`） | PASS（99.165s / 58.850s） |

## 全量回归

`go test ./pkg/sql ./pkg/db -count=1 -p 1`：`ok pkg/sql 99.165s`、`ok pkg/db 58.850s`。
`go test ./... -count=1 -p 1`：全部 22 个包 `ok`（`pkg/db` 57.151s、`pkg/sql` 58.589s），无 FAIL。

## 已知事项

- 跨节点读取未切分表仍可能取不到数据（读侧聚合视图重启后未重建），沿用 v2.0.4 登记的后续 NEW 项，本版未涉及。
- v2.1.0 的 TSO 租约兜底（`--tso-lease`，默认关闭）仍在 Unreleased 开发线，本版不含。

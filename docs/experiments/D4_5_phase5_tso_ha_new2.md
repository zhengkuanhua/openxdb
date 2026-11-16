---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_6aa0a8eeda5b11f19280525400393706
    ReservedCode1: sb4iasmJUDApjMaXFV4odNwxVXqM/y4xOM/wqo4FqHf3URKUmSN7Pc5a200q8tr/JHp0yDlecAS2oguuv/rfKepX6UsuVFYaGKRjV6pbFNztGF/wTzIyzTQ3dKKCY3rAdpjHnkxdjXMeEGhh8Wfc9qQK/lCq9bjmBcflfghWau8Zr2BtMt+prxi3gxc=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_6aa0a8eeda5b11f19280525400393706
    ReservedCode2: sb4iasmJUDApjMaXFV4odNwxVXqM/y4xOM/wqo4FqHf3URKUmSN7Pc5a200q8tr/JHp0yDlecAS2oguuv/rfKepX6UsuVFYaGKRjV6pbFNztGF/wTzIyzTQ3dKKCY3rAdpjHnkxdjXMeEGhh8Wfc9qQK/lCq9bjmBcflfghWau8Zr2BtMt+prxi3gxc=
---

# D4.5 Phase5 验证记录：TSO HA 生产接线（A）+ ha_* 持久化加载顺序（B）

- 基线 HEAD：`396b4bb`
- 本次提交：`a4b33bf` fix(ha): Phase5 接线 TSO HA 生产启动路径并修复 ha_* 持久化配置加载顺序
- 双远端核验：gitee/main = origin/main = `a4b33bf548b327838e4a0587e53831c8069696e7`
- 全仓测试：`go test ./... -count=1` 全绿（23 个包，含 cmd/openxdb、pkg/db、pkg/sql、pkg/cluster 等）

## A 项：B2 TSO HA 接入生产启动路径

### 根因
`pkg/db/tso_ha.go` 的 `EnableTSOHA(group, batch)` 已实现组内主备发号与 epoch 选主，
但生产启动路径 `cmd/openxdb/main.go` 的 `cmdStart` 从未调用它——全仓唯一调用点是
`pkg/db/tso_ha_test.go`。因此真实三节点集群启动后 TSO 仍为单点/本地发号，
Phase4 复测中出现"主节点宕机后跨节点写 100% 失败"（B2 FAIL）。

### 修复内容
| 位置 | 变更 |
|---|---|
| `cmd/openxdb/main.go` | 新增 `--tso-ha-group <id1,id2,id3>`、`--tso-ha-batch <n>` 与 `[server] tso_ha_group` / `tso_ha_batch` 配置项，命令行优先于配置文件 |
| `parseTSOHAGroup` | 逗号分隔解析：去空白、丢空项、按出现顺序去重（配置稳定即首选主稳定） |
| `setupTSOHA` | 生产装配入口：调用 `EnableTSOHA` 并校验 `TSOHAClient()` 已安装，失败即返回错误（不静默降级） |
| `cmdStart` | 在 `StartCluster` 之后装配 TSO HA；配置了组但缺 `--cluster-addr` 时直接报错 |
| 互斥处理 | 同时配置 `--tso-node` 与 HA 组时 HA 优先，单点配置被忽略并输出 WARN，避免两套发号装配互相覆盖 |

### 端到端验证（`cmd/openxdb/phase5_tso_ha_wiring_test.go`）
| 用例 | 验证点 | 结果 |
|---|---|---|
| `TestParseTSOHAGroup` | 空串/纯空白/重复项/带空格/顺序保持 | PASS |
| `TestSetupTSOHAWiresProductionPath` | `setupTSOHA` 后节点 TSO 确为 `HATSOClient` 且组信息一致；空组显式报错 | PASS |
| `TestSetupTSOHAFailoverE2E` | 三节点同组 → A 首选当选主 → B 本地写 + 跨节点写成功 → 关闭 A → B/C 多数派自动选主 → 跨节点写自动恢复、时间戳严格越过故障前最大值且无重复、故障前后写入行均可读 | PASS |

## B 项：NEW-2 持久化 ha_* 导致重启退出

### 根因
`db.Open` 中 `LoadPersistedConfig()` 早于 `SetHAConfigSetter` 等 HA 装配执行。
`SET PERSIST ha_epoch_enabled/ha_quorum_enabled` 落盘的 `persisted.json` 在重启时
于装配之前被 apply，被判定为 `requires cluster HA configuration`，节点直接退出。

### 修复内容
`pkg/db/db.go` 将 `SetWriteFence` / `SetHAConfigSetter` / `SetEpochSource` /
`SetQuorumArbiter` 四步装配整体前移到 `LoadPersistedConfig()` 之前，使持久化
ha_* 与运行时 `SET ha_*` 共用同一"已装配"路径。集群相关入口（clusterAddr、
TSO HA）仍由 `StartCluster` 后续补齐，默认部署不启用 quorum 仲裁，零回归。

### 回归验证（`pkg/db/db_ha_config_persist_test.go`）
| 用例 | 验证点 | 结果 |
|---|---|---|
| `TestHAConfigPersistedRestart` | SET PERSIST 两个开关 → 持久化文件确有该键 → 重启成功 → 开关值真实生效 → 无集群时仍可执行 SQL | PASS |
| `TestHAConfigPersistedRestartIdempotent` | 连续两次重启均成功且 `ha_epoch_enabled=off` 正确应用 | PASS |

### 缺陷可复现性证明（关键证据）
临时回退 `pkg/db/db.go` 后运行 `TestHAConfigPersistedRestart`：

```
--- FAIL: TestHAConfigPersistedRestart (0.31s)
    restart with persisted ha_* must succeed (NEW-2): db: load persisted config:
    persisted config ha_quorum_enabled="on" invalid:
    SQL error: ha_quorum_enabled requires cluster HA configuration (start cluster first)
```

恢复修复后同用例 PASS，证明该测试真实覆盖 NEW-2，而非空转通过。

## 约束遵守
- 未改动 NEW-1（重启双主分叉）相关逻辑，未改动 T3 fencing 与 B1 单副本已入库实现。
- 提交使用显式 `git add`（4 个文件），未使用 `git add -A/-u`；排期文档与 `tests/bench` 保持 untracked。

## 对全局目标的影响
Phase4 报告中 B2（主宕机跨节点写全失败）与 NEW-2（持久化启动崩溃）两项阻塞已闭环。
剩余阻塞：NEW-1 重启双主分叉丢数据、T3 真分区未验证（后续 Phase 处理）。
`ha.quorumEnabled` 默认开启的前置缺陷中，配置加载顺序一项已消除。
*（内容由AI生成，仅供参考）*

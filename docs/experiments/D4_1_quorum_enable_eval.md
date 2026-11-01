# D4.1 quorumEnabled=true 开启评估（三节点真实组网）

- 版本：v1.18.0 D4 Phase B / D4.1
- 日期：2026-11-02
- 工作区：`e775e7e`（双远端同步后）
- 目标：在三节点组网验证场开启 `ha.quorumEnabled=true`，评估三节点多数派场景下升主/降级行为，确认不破坏三节点自动升主；对照 `KNOWN_LIMITATIONS §3.4` 与 `HA_BRAIN_SPLIT_DESIGN §5.5`（quorum 默认关闭）既有结论；暴露实现缺口则定位代码并给出修复方案或记录已知边界。
- 归档：`tests/bench/bench_data/d41/`（可重跑脚本 + 运行输出 + 服务端日志样本）
- 提交：B1

## 1. 评估方法

### 1.1 验证场拓扑

复用 D3.3 三节点组网范式（见 `tests/bench/bench_data/d33/run_d33_cross_node_rw_consistency.ps1`）：

| 节点 | 地址 | 角色 | TSO |
|---|---|---|---|
| A | 127.0.0.1:17881（SQL 13316） | DATA，注册表发起者 | 中心 TSO（`--tso-node` 自指） |
| B | 127.0.0.1:17882（SQL 13317） | DATA | 远端 TSO 客户端（指向 A） |
| C | 127.0.0.1:17883（SQL 13318） | DATA | 远端 TSO 客户端（指向 A） |

心跳参数：interval=2s、timeout=6s（`pkg/db/db.go` `d.Cluster.StartHeartbeat(2*time.Second, 6*time.Second)`），down 判定约 6s 内生效。

### 1.2 开关操作

- 三端均执行 `SET ha_quorum_enabled = on`（session 级生效，`SHOW CONFIG` 呈现 `on / session`）；
- `ha_epoch_enabled` 保持默认 `on`（epoch fencing 不受影响）。

### 1.3 场景矩阵

| 场景 | 操作 | 存活/总数 | quorum=N/2+1 | 预期行为（对照 §5.5 P0 语义） |
|---|---|---|---|---|
| S1 | 三节点组网 + 开启 quorum | 3/3 | 2 | 配置生效、注册表 3 节点 |
| S2 | 建表 t3（27 行，3 region 各持 1） | 3/3 | 2 | 三端 COUNT=27 / SUM=378 |
| S3 | kill A | 2/3 | 2（满足） | 心跳置 DOWN；failover 仲裁通过（quorumOK），尝试重指派 |
| S4 | 再 kill B | 1/3 | 2（不满足） | failover 被 quorum 门拒绝（reachable 1/3, need 2），不重指派/不升主 |
| S5 | 重启 A/B | 3/3 | 2 | 注册表恢复 3 节点，数据收敛 |

## 2. 执行结果

脚本：`tests/bench/bench_data/d41/run_d41_quorum_enable_eval.ps1`
输出：`tests/bench/bench_data/d41/run_d41_output.txt`
结果：**PASS 16 / FAIL 2 / GAP 2**（FAIL/GAP 均为缺口观察项，见 §3）

### 2.1 S1/S2 基线（全 PASS）

- 三节点注册表 count=3；`ha_quorum_enabled=on(session)` 三端生效；`ha_epoch_enabled=on(default)` 保持；
- t3 拆 3 region 并 ASSIGN 到 A/B/C；三端 COUNT=27、SUM=378 一致。

### 2.2 S3 多数派场景（A down，B/C 存活 2/3）

| 检查项 | 结果 |
|---|---|
| B/C 心跳判定 A DOWN | PASS（9s 内 SHOW NODES 见 DOWN） |
| failover 仲裁 | quorumOK(2/3) 通过，FailoverDownNode 实际执行（服务端日志佐证） |
| rA 重指派 | GAP：重指派未完成——单副本分片下 B/C 不持有 rA 数据副本，日志 `sql: failover skip candidate node-b5d542b7 for region 3: sql: region 3 not held by candidate node-b5d542b7`，无存活数据副本 → failoverRegion 放弃重指派 |
| B/C 聚合读 | FAIL：COUNT=25（丢 rA 上 2 行，路由仍指向 down 的 A，读回退本机 0 行） |
| B/C 写 | FAIL：`TXN_PREPARE_FAILED: tso unavailable`（中心 TSO=A，A down 后取号失败） |

### 2.3 S4 少数派场景（A+B down，仅 C 1/3）

| 检查项 | 结果 |
|---|---|
| failover 仲裁 | PASS：quorum 门正确拒绝，日志 `sql: failover down node node-b5d542b7 skipped: quorum not met: db: quorum not met, refuse to promote: reachable 1/3, need 2`；对 A、B 两个 down 节点均拒绝重指派 |
| 路由视图 | GAP：rB 仍指向 down 的 B（未被重指派到 C）——与 quorum 门拒绝一致，符合预期降级 |
| C 读 | COUNT=23（A、B 上 region 数据均不可 failover 恢复） |
| C 写 | `tso unavailable`（TSO 仍不可用） |

### 2.4 S5 恢复（重启 A/B）

- 注册表恢复 3 节点 UP；三端 COUNT=27 收敛一致（down 期间写入全部被 TSO 拒绝，无增量数据；A/B 重启后复制位点无差异，集群轨路由恢复）。

## 3. 暴露缺口与修复方案/已知边界

| 编号 | 现象 | 性质 | 代码位置 | 修复方向 / 已知边界 |
|---|---|---|---|---|
| B1 | down 节点 region 无存活数据副本，集群轨 failover 无法恢复数据（聚合读丢行：25/23） | **设计边界（非 quorum 回归）** | `pkg/sql/ha.go` `failoverRegion`（`best <= 0` → `no live data copy`）、`failover skip candidate ... region not held` | shared-nothing 单副本分片固有边界（D3.2 G6、T15_m6_ha 语义：ASSIGN 不复制数据副本）。修复方向：region 多副本复制（写入期同步副本）或 ASSIGN 时向目标推送数据副本；或依赖复制轨整库复制覆盖。双轨叠加语义需在文档中明确定义（长期记忆已记录该缺口） |
| B2 | A（中心 TSO）down 后 B/C 分布式事务写全部失败 | **已知限制** | `pkg/cluster/tso.go` + `db.SetTSONode` | TSO 中心化单点，无备份/自动切换。生产级需 TSO HA（备份/切换/epoch 源降级），不阻塞本评估 |
| B3 | 无读请求时 `SHOW REGION ROUTES` 不更新（路由仍指向 down 节点） | **M6 设计（lazy failover）** | `pkg/sql/ha.go` `resolveRegionOwner` / 心跳回调 | 读路径/心跳回调触发式重指派，非主动扫描。可接受为既有设计，文档注明外部观察时需先触发读 |
| B4 | 真实进程组网下复制轨（PromoteToMaster / EnableAutoFailover）无法经 SQL 触发，三节点"自动升主"由进程内测试覆盖 | **评估方法边界** | `pkg/db/ha.go`（复制轨 Go API） | 真实组网复制轨自动升主需 Go API 驱动；已有进程内三节点测试全绿（见 §4.1），后续 D4.x 可扩展真实组网复制轨演练 |

## 4. 结论（对照 §3.4 / §5.5）

### 4.1 三节点多数派升主/降级行为

- **升主前置不破坏**：quorum 开启后三节点多数派（2/3）场景 `quorumOK` 通过，FailoverDownNode 正常进入重指派流程（S3 实测）；少数派（1/3）被 quorum 门拒绝（S4 实测，日志 `reachable 1/3, need 2`），与 §5.5「不足多数派拒绝升主」一致。
- **既有进程内测试佐证（全绿）**：`TestP0QuorumPromoteSucceeds`（三节点 quorum 开启升主成功）、`TestP0QuorumPromoteInsufficient`（少数派拒绝）、`TestP1BrainSplitDrill`（三节点组网级脑裂演练：B 升主 / C 被拒 / A 降级 fencing / 回迁就绪）、`TestP2T9FailoverWithEpochQuorum`。运行基线见 `run_d41_output.txt`（S6 段）与既有测试记录。
- **结论：三节点自动升主语义在 quorum 开启下保持**，未出现双主/路由分裂回归。

### 4.2 对照既有文档结论的更新

- `KNOWN_LIMITATIONS §3.4` / `HA_BRAIN_SPLIT_DESIGN §5.5` 建议「三节点及以上真实组网灰度验证 quorum 仲裁行为后再评估全量开启」——本评估完成三节点真实组网灰度验证：**仲裁行为符合预期（多数派通过、少数派拒绝），未破坏三节点自动升主**；
- 同时暴露**集群轨单副本数据不可 failover 恢复（B1）与 TSO 单点写失败（B2）**，二者与 quorum 开关无关（quorum 关闭时同样存在），属生产级 HA 硬缺口；
- **评估建议**：三节点及以上组网可按需开启 `ha.quorumEnabled`（不破坏升主、增强防双主）；**维持单机/两节点默认关闭**（§3.4 既有结论不变）；生产级全量开启前需先补齐 region 副本（B1）与 TSO HA（B2）。

## 5. 可重跑资产

- 脚本：`tests/bench/bench_data/d41/run_d41_quorum_enable_eval.ps1`（含 UTF-8 BOM；运行输出 `run_d41_output.txt`）
- 服务端日志样本：`tests/bench/bench_data/d41/logs_sample/node-c.log.err`（quorum 拒绝 / failover skip / tso unavailable 原始日志）
- 运行环境：`bin/openxdb.exe` / `bin/oxsql.exe` 构建于 2026-11-02 06:44（新于源码 mtime 06:42，无需重建）
- 依赖：Windows + PowerShell 5.1，数据目录自动置于 `%TEMP%\openxdb_d41_<stamp>\`（ASCII 路径约束）

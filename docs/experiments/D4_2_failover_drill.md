# D4.2 宕机 failover 演练（三节点 quorum 开启）

- 阶段：v1.18.0 Phase B — D4 HA quorum 灰度全链路演练
- 承接：D4.1 quorum 开启评估（c855ea2，16P/2F/2G）
- 日期：2026-11-02
- 结论：**PASS=33 / FAIL=2（均为预期 FAIL）/ GAP=1**
  - 单副本边界（B1 复现）：region 唯一副本在被杀节点 → 无存活副本 → failover 放弃重指派 → 聚合读丢 2 行（预期 FAIL）
  - 存活副本可读路径：region 在存活节点有真实副本（ASSIGN 后创建者保留副本）→ 自动重指派成功 → 数据仍可读（PASS）
  - 写路径：中心 TSO 存活时跨节点写成功；中心 TSO down 时写失败 `tso unavailable`（B2 单点再次确认）

## 1. 背景与代码位

- D4.1 结论：三节点开启 `ha.quorumEnabled=true` 不破坏多数派（2/3）升主；1/3 时 failover 被 quorum 门拒绝。B1 缺口：单副本分片 down 后 `failoverRegion` 放弃重指派。
- 代码位：
  - `pkg/sql/ha.go`：`FailoverDownNode`（P1 quorum 前置）、`failoverRegionP1` / `tryFailoverTargetP1`（候选目标逐个做 `regionHoldsLocalTarget` 物理行数持有校验）、`resolveRegionOwner`（归属 down 时触发 failover，仍无持有者回退本节点本地副本）
  - `pkg/sql/multinode.go`：`execAssignRegion`（`ASSIGN REGION` 全量推送数据到目标节点，**源节点保留副本**；路由归属迁移为目标节点）
- 参照：KNOWN_LIMITATIONS §3.4（quorum 默认关闭）、HA_BRAIN_SPLIT_DESIGN §5.5（脑裂防护/TSO HA 为生产级硬缺口）

## 2. 拓扑与配置

| 项 | 值 |
|---|---|
| 节点 | A/B/C 三节点；A 兼中心 TSO（`--tso-node` 自指），B/C 挂 A 的 TSO 客户端 |
| 端口 | cluster 17881/17882/17883；SQL 13316/13317/13318 |
| 心跳 | interval=2s timeout=6s；杀进程后等待 9s 收敛 DOWN 判定 |
| 数据目录 | `%TEMP%\openxdb_d42_<stamp>`（专用验证场，破坏性杀进程仅在此执行） |
| quorum | `SET ha_quorum_enabled = on`（session）三端生效；N=3 → quorum=2 |

数据模型：`t3 (id INT PRIMARY KEY, v VARCHAR(64))`，SPLIT 3 regions（rA/rB/rC），`ASSIGN rB→B`、`ASSIGN rC→C`，三端各写 9 行共 27 行。
关键副本语义：**A 为创建者，ASSIGN 后保留全部 3 个 region 的真实副本**；B 持 rB（归属+副本），C 持 rC（归属+副本），rA 唯一副本在 A。

## 3. 场景与结果

| 场景 | 操作 | 预期 | 实际 | 判定 |
|---|---|---|---|---|
| S1 组网 | 3 节点 UP + `ha_quorum_enabled=on` session 三端 | UP、quorum on | listening / registry=3 / quorum on 三端 | PASS ×8 |
| S1b 基线 | t3 27 行；路由 rA=A rB=B rC=C | count=27 sum=378 | 三端一致；路由正确 | PASS ×5 |
| S2 单副本边界（B1 复现） | 杀 A（rA 唯一副本+归属 down） | failover 放弃重指派，聚合读丢 rA 行 | B/C 心跳 DOWN；rA owner 保持 A；B/C count=25（丢 2 行）；日志 `failover skip candidate ... region 3 ... not held by candidate` | **FAIL ×2（预期 FAIL）** + GAP ×1（owner 保持 A，脚本按 GAP 记录）+ PASS ×3 |
| S2 写观察 | B 上 INSERT 101 | 观察 TSO 单点影响 | `TXN_PREPARE_FAILED: tso unavailable`（中心 TSO=A down） | 记录（B2 确认） |
| S2R 恢复 | 重启 A（原 data-dir） | 三端 count=27 | registry=3；A/B/C count=27 | PASS ×4 |
| S3 存活副本可读路径 | 杀 B（rB 归属 down，**A 保留 rB 真实副本**） | resolveRegionOwner 自动重指派 rB→A，数据仍可读 | A/C 心跳 DOWN；rB 重指派到 A（A/C 视图一致）；A/C count=27；C INSERT id=28 成功（TSO=A 存活）→ count=28 | **PASS ×6** |
| S3R 恢复 | 重启 B | 三端 count=28 | registry=3；A/B/C count=28 | PASS ×4 |

**PASS 33 / FAIL 2 / GAP 1**（GAP 为 S2 rA owner 记录项：行为符合预期——failover 放弃重指派、owner 保持 A，脚本按 GAP 分类登记）。

### 关键路由证据（S3 杀 B 后）

`SHOW REGION ROUTES`（@A / @C 一致）：
- region 3（rA）：node=A（不变，A 持有副本）
- region 4（rB）：node=**A**（重指派，原 B）
- region 2（rC）：node=C（不变）

## 4. 结论

1. **读 failover 有效前提 = 存活副本**：`resolveRegionOwner` → `FailoverDownNode` → 候选目标 `tryFailoverTargetP1` 持有校验（物理行数 > 0）通过才重指派。S3 中 A 作为创建者保留 rB 副本，杀 B 后重指派到 A，聚合读 27 行完整。
2. **B1 缺口真实组网复现**：单副本 region（rA 唯一副本在 A）被杀后，候选目标（B/C）均不持有 rA 数据、本机也不持有 → `failover skip candidate` → 放弃重指派 → 聚合读丢 2 行。**生产多副本是 failover 可用的硬前提**。
3. **quorum 门（P1）正常**：S2/S3 均为 2/3 多数派存活，failover 通过 quorum 门后由存活副本校验决定是否重指派；1/3 拒绝场景已在 D4.1 S4 验证，未重复。
4. **B2 TSO 单点再次确认**：中心 TSO（A）down 时跨节点写失败 `tso unavailable`；读路径不受影响。生产级需 TSO HA。
5. **恢复语义**：宕机节点重启（原 data-dir）后 registry 自动恢复 3 节点，数据收敛三端一致；复制位点不持久化、重启全量补拉（KNOWN_LIMITATIONS）。

## 5. 与 D4.1 B1 缺口的关系及后续

- 本演练将 B1 从代码路径确认升级为真实组网可复现：单副本分片 down → `failover skip candidate ... not held by candidate` 日志 → 聚合读丢行。
- 运维侧：`ASSIGN REGION` 可作手工建副本手段（S3 所示存活副本路径有效）；但生产级仍需自动副本复制/raft 组 + epoch 防分裂（HA_BRAIN_SPLIT_DESIGN §5.5），否则单副本 region 的宕机即数据不可读。

## 6. 归档与复跑

- 脚本：`tests/bench/bench_data/d42/run_d42_failover_drill.ps1`
- 结果：`tests/bench/bench_data/d42/run_d42_output.txt`
- 复跑：`powershell -ExecutionPolicy Bypass -File tests\bench\bench_data\d42\run_d42_failover_drill.ps1`
- 注意：脚本含 `Stop-Process` 杀 openxdb 进程的破坏性步骤，仅在 `%TEMP%` 专用验证场执行，不影响仓库/系统目录。

## 7. commit 计划

- B2：`docs/experiments/D4_2_failover_drill.md` + `tests/bench/bench_data/d42/`（脚本 + output）

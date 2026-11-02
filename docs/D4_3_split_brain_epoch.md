---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_a5ce2d5bd69a11f1b2d8525400393706
    ReservedCode1: gG/3PUwco3g8tMxi1BR28ghx2yul3JzLoQetfFZxNe64tqlolhgDnDrHVF5PW1abdv6xxS0XkcvobkFyUpgKA+KvYbQRkvWkSwIhMu098CcKF9ZGkju9Vgn2zNEVjHAm5mtFIfFc0QVtRkV9WZxKv1iMyMfT+VGK0MN3cU4KvA5lAT9jbcNXJrfj8k8=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_a5ce2d5bd69a11f1b2d8525400393706
    ReservedCode2: gG/3PUwco3g8tMxi1BR28ghx2yul3JzLoQetfFZxNe64tqlolhgDnDrHVF5PW1abdv6xxS0XkcvobkFyUpgKA+KvYbQRkvWkSwIhMu098CcKF9ZGkju9Vgn2zNEVjHAm5mtFIfFc0QVtRkV9WZxKv1iMyMfT+VGK0MN3cU4KvA5lAT9jbcNXJrfj8k8=
---

# D4.3 脑裂 / epoch 拒绝验证

- 日期：2026-11-02
- 环境：VM 192.168.100.200（Oracle Linux 9.7），OpenXDB v1.17.0 三节点集群轨
- 依据：docs/HA_BRAIN_SPLIT_DESIGN.md §5.5（T1/T2/T3/T6/T8/T9 演练矩阵）
- 代码锚点：pkg/db/epoch.go（acquireEpoch / AllowWrite / quorumGuard）、pkg/db/ha.go（ReconcileAfterPartition / PromoteToMaster）、pkg/cluster/heartbeat.go（全互联 PING/PONG）、pkg/sql/ha.go（FailoverDownNode）

## 1. 目标

网络分区（脑裂）期间与恢复后，验证 epoch fencing 对旧主写路径的拒绝行为；对照 §5.5 矩阵逐项给结论。

## 2. 环境与基线

| 节点 | node ID | cluster 端口 | mysql 端口 | 角色 |
|------|---------|-------------|-----------|------|
| A | node-24b6a076 | 7791 | 13306 | 中心 TSO（本地发号） |
| B | node-bdbff1cc | 7792 | 13307 | DATA（--tso-node 指向 A） |
| C | node-cab8c15a | 7793 | 13308 | DATA（--tso-node 指向 A） |

关键配置修正：B/C 的 `--tso-node` 必须指向 A 的真实注册 node ID（node-24b6a076）。早期演练使用别名 `node-a`，该 ID 不在集群注册表中，导致 B/C 写路径恒报 `tso unavailable: cluster: node not registered` —— 属配置错误而非分区效果，修正后方可进入演练。

配置基线（SHOW CONFIG）：`ha_epoch_enabled=on(default)`、`ha_quorum_enabled=off(default)`。

数据基线：`d43_t1` 经 `SPLIT REGION 9223372036854775809` + `SPLIT REGION 2` 拆为 3 个 region（r1/r3/r4），`ASSIGN REGION r3 TO NODE 'B'`、`ASSIGN REGION r4 TO NODE 'C'`；A 写 1-5、B 写 10-12、C 写 20-22 后三端聚合读一致：count=11、sum=111。

## 3. 双向隔离方法

心跳为全互联 PING/PONG（pkg/cluster/heartbeat.go：每周期对每个已注册远端节点拨号发 PING，同连接收 PONG 更新 LastSeen；超过 timeout 未更新则 MarkDown）。

仅用 `INPUT --dport 7791 DROP` 隔离 A 是**半脑裂**：A 主动发起的 B/C 连接（目标 7792/7793）不受 INPUT 规则影响，PONG 回包经同一连接回流，A 侧心跳不翻转。必须双向断连：

```bash
iptables -I INPUT  -p tcp --dport 7791 -j DROP                          # 断 B/C -> A 入站
iptables -I OUTPUT -m owner --uid-owner 2001 -p tcp --dport 7792 -j DROP # 断 A -> B 出站
iptables -I OUTPUT -m owner --uid-owner 2001 -p tcp --dport 7793 -j DROP # 断 A -> C 出站
```

- A 进程以独立用户 oxd43（uid=2001）运行，`runuser -u oxd43 -- openxdb start ...`，数据目录 `/data/d43-node1` 已 `chown -R oxd43:oxd43`。
- iptables nf_tables 后端不支持 `--pid-owner`（unknown option，xt_owner 不可用），故用 `--uid-owner`。
- 隔离效果：双向不可达；B<->C 保持连通（B/C 侧构成 2/3 多数派）。

## 4. 分区观测（网络隔离期间）

| 观测 | 结果 |
|------|------|
| 心跳视图 | A 侧：B/C=DOWN；B 侧：A=DOWN、C=UP；C 侧：A=DOWN、B=UP |
| A（旧主 / TSO 主）写 `INSERT 100` | 成功，A 本地 count=6 —— 分区期间旧主写未被 fencing 拒绝（GAP，见 §6 T3） |
| B 写 `INSERT 200` | `TXN_PREPARE_FAILED: tso unavailable: dial tcp 192.168.100.200:7791: i/o timeout` |
| C 写 `INSERT 300` | 同上（TSO 单点在 A，分区导致从端写被阻塞） |
| B 聚合读 d43_t1 | count=9（r3 本地 3 行 + r4 经 C 3 行 + 本地残留 3 行；r1 归属 A 的 5 行丢失） |
| A 聚合读 d43_t1 | count=6（本地 r1 5 行 + 分区期写入 100；r3/r4 经 B/C 不可达） |

## 5. 恢复收敛

清 iptables 后三端 `SHOW NODES` 全 UP（心跳自动恢复）。数据状态：

| 节点 | 恢复后 count | 组成 |
|------|-------------|------|
| A | 6 | 1-5 + 100（100 本地提交于 r4 区间） |
| B | 11 | 1-5, 10-12, 20-22（聚合读，看不到 100） |
| C | 11 | 同上 |

恢复后 A 再写 `INSERT 400`：A 变 7（1-5,100,400）；B/C 仍 11。

结论：分区期旧主写与恢复后旧主写均**未进入 B/C 聚合视图**。原因：100/400 落在 r4 区间（id>=20），r4 归属 C，读路由（region routes）指向 C；而 A 的写为本地提交（不按 region 归属强制路由），A 本地的 100/400 被排除在聚合视图之外。集群轨无复制轨，恢复后**无自动合并 / 数据回灌补偿**。

## 6. T 矩阵结论

| 用例 | 结论 |
|------|------|
| T1 主从断开、从升主 | 集群轨无复制轨升主路径（PROMOTE 无 SQL，升主仅复制轨 offline 触发）；从端写被 TSO 单点阻塞。不适用/FAIL（语义差异） |
| T2 并发升主、epoch 裁决 | 无升主路径，不适用 |
| T3 分区恢复合并、旧主降级只读 + fencing | **GAP**：集群轨 masterEpoch=0 无授权，AllowWrite 恒放行；分区期旧主写成功，恢复后旧主仍可写且数据不自动收敛 |
| T6 epoch 拒绝陈旧消息 | 复制轨有效。本机 go test 全部 PASS（ok pkg/db 3.2s）：TestP1BrainSplitDrill、TestP0EpochMonotonicPersist、TestP0QuorumGuardRejectsInsufficient、TestP0AllowWriteFencing、TestP0EpochDefaultOnZeroRegression |
| T8 开关回归 | ha_epoch_enabled=on(default)、ha_quorum_enabled=off(default)；SET ha_epoch_enabled / ha_quorum_enabled 热切换生效 |
| T9 单点 down 正常 failover | D4.2 已验证：存活副本场景 PASS、单副本边界 FAIL（B1 GAP，failoverRegionP1 放弃重指派） |

## 7. 根因与后续（B4 建议）

1. 集群轨（无复制轨）写为**本地提交**：B/C 直写即本地可见（早期观测），不强制按 region 归属路由提交；读经 region 路由聚合（D4.2 创建者保留副本语义）。
2. epoch fencing 仅在复制轨生效（StartReplicationTLS 时 acquireEpoch + setMasterEpoch + broadcastEpoch；AllowWrite 在 masterEpoch>0 且收到更大 epoch 广播后返回 ErrFenced）。集群轨 masterEpoch=0 → AllowWrite 恒放行。
3. 分区期间旧主写放行 + 恢复后无补偿 → 数据发散不可自动收敛。
4. 建议：集群轨引入复制轨/分布式事务（M8 2PC）或 region 归属强制提交 + 重联数据回灌（ReconcileAfterPartition 扩展），方可闭环 T3。

## 8. 产物

- `tests/bench/bench_data/d43/run_d43_split_brain_epoch.py`：可复现演练脚本（paramiko 驱动 VM）
- `tests/bench/bench_data/d43/run_d43_output.txt`：本次观测输出记录
*（内容由AI生成，仅供参考）*

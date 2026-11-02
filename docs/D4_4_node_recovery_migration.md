# D4.4 节点恢复回迁演练（B4）

- 日期：2026-11-03
- 版本：v1.17.0（VM 三节点集群，集群轨）
- 前置：D4.3（commit B3=04722dd）已收口；本演练承接"节点宕机 → 恢复 → 数据回迁/追平 → 路由收敛 → 读聚合恢复"闭环验证
- 目的：对照 D4.2（单点 down failover）、D4.3（网络分区恢复），补齐**节点进程级宕机重启**场景的回迁行为证据

## 1. 环境基线

| 项 | 值 |
|---|---|
| VM | 192.168.100.200（Oracle Linux 9.7，root/123456） |
| 节点 A | node-24b6a076，cluster 7791 / mysql 13306 / 内部端口 7788，oxd43(uid=2001) 运行，TSO 中心 |
| 节点 B | node-bdbff1cc，cluster 7792 / mysql 13307 / 内部端口 **7789** |
| 节点 C | node-cab8c15a，cluster 7793 / mysql 13308 / 内部端口 7790 |
| 数据表 | d44_t1(id INT PK, v TEXT)，SPLIT REGION 9223372036854775809 + SPLIT REGION 2 → r1/r3/r4；ASSIGN r3→B、r4→C |
| 基线数据 | A 写 1-5、B 写 10-12、C 写 20-22；聚合期望 count=11 sum=111 |

> 内部端口教训：三节点内部端口须互异（A=7788、B=7789、C=7790）。B 重启时误用 7790 与 C 冲突（`bind: address already in use`），B 的正确内部端口见 `/data/d43-node2/node-b.log` 的 `serving on :7789`。

## 2. 演练步骤与观测

### S1 宕机检测（B 强杀）

```
kill -9 <B pid>          # B pid=10944
```

观测：A/C 侧 `SHOW NODES` 中 B 变 **DOWN**（心跳检测生效）。

### S2 down 期行为

- **failover 重指派**：B down 后，A/C 侧 `SHOW REGION ROUTES` 中 r3 自动重指派到 A（node-24b6a076，local=true）——与 D4.2 S3 的 rB 重指派行为一致。
- **down 期写入**：A 在 B down 期间 `INSERT id=30` 成功（本地提交，TSO 单点无碍），A 本地 count 6（1-5+30）。

### S3 恢复（B 重启）

```
/usr/local/bin/openxdb start --data-dir /data/d43-node2 --port 7789 \
  --cluster-addr 192.168.100.200:7792 --tso-node node-24b6a076 --mysql-port 13307
```

| 观测项 | 结果 | 判定 |
|---|---|---|
| 心跳恢复 | 三端 `SHOW NODES` 全 UP | ✅ PASS |
| 路由收敛 | **不回迁**：A/C 视图 r3→A（failover 残留），B 本地视图 r3→B（本地 meta），三端视图分裂；手动 `ASSIGN REGION 3 TO B` 后三端一致 | ❌ FAIL / GAP |
| 数据回迁/追平 | **无回迁**：A 的 30 在 B 不可见（`WHERE id=30` 空）；B 恢复后写 31 在 A/C 不可见；三端读本地化 A=6 / B=4 / C=3（期望聚合 12~13） | ❌ FAIL / GAP |
| 读聚合恢复 | **不恢复**：三端仅见本地行；与 D4.3 网络恢复后 B/C 聚合 11、D4.2 重启后三端 28 对照，进程重启场景聚合读失效 | ❌ FAIL / GAP |
| 写路径 | B 恢复后可写（31 本地提交成功），但不进全局聚合视图 | ⚠️ 部分 |

### S4 对照实验（排除强杀残留）

SIGTERM 优雅退出 B → 重启：三端 UP、路由三端一致（r1→A/r3→B/r4→C），三端读**仍本地化**（A=6/B=4/C=3）。结论：聚合读失效非 kill -9 孤儿事务（WAL 中 `2pc-...-4` begin_ts=0 残留）所致，而是**重启节点数据面未重建**的稳定行为。

## 3. 结论矩阵（对照 D4.3 §5.5）

| 验收项 | 结论 |
|---|---|
| 宕机检测（心跳 DOWN） | ✅ PASS |
| failover region 重指派（存活副本接管） | ✅ PASS（存在，自动重指派到 A） |
| 节点恢复后路由自动回迁 / 收敛 | ❌ **GAP**：failover 接管 region 不回迁；B 本地 meta 与集群视图分裂；人工 ASSIGN 可收敛 |
| 节点恢复后数据回迁 / 追平 | ❌ **GAP**：down 期写入与恢复后写入均不进聚合视图，三端数据发散 |
| 恢复后读聚合恢复 | ❌ **GAP**：重启节点跨节点拉取失效（路由三端一致亦不恢复） |

## 4. GAP 归因与修复建议

归因（基于观测，非臆断）：

1. **集群轨无复制轨/回迁机制**：region 归属仅是路由指针变更，无数据回灌；复制轨的 `TryRejoinAsFollower` / `FullAlignViaRegionPush`（docs/T43_ha_rpo_rejoin.md）未在集群轨装配。
2. **重启节点数据面未重建**：心跳/元数据 UP ≠ 数据面 READY；B 重启后跨节点 region 拉取通道未 join，读聚合与跨节点写（2PC）失效。
3. **failover 接管不回迁**：无接管记录与回迁优先级策略，B 本地 meta 亦未被集群视图覆盖（路由分裂）。

修复建议（后续排期）：

1. 集群轨节点 join 时向 TSO 同步全量 region 路由并覆盖本地 meta，重建数据面通道（join 完成 + 路由同步 + 数据面 READY 作为读聚合前置条件）。
2. failover 接管 region 记录接管来源；源节点恢复后按策略回迁（或提供手动 ASSIGN 兜底——本次已验证人工回迁可收敛路由，但数据仍发散，故路由收敛 ≠ 数据一致）。
3. 数据追平：借鉴 T43 阶段二 region push 全量对齐思路，集群轨实现 region 级对齐（恢复节点从归属节点拉取当前行覆盖本地），补齐 down 期写入。
4. 写路径：恢复节点本地提交需经 region 归属强制路由（或 2PC 参与），避免写出发散。

## 5. 产物

- `tests/bench/bench_data/d44/run_d44_node_recovery.py`：可复现演练脚本（paramiko 驱动 VM，含 S1-S4）
- `tests/bench/bench_data/d44/run_d44_output.txt`：本次观测输出记录
- 本文档：docs/D4_4_node_recovery_migration.md

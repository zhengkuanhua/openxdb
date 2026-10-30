---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_7dd10736c91211f18b7f525400393706
    ReservedCode1: UDm3b/NfHHOkbknWca+ttp8cSfMCckglKfbESPE7h17FKwsXlvhweB/iFshQHxuiq9o9Jk6/hIsv0Gku9XZrg2r7w9bLEXfjvZSrWIv+Lg0phzlhFgGdmu5akERzFrIeUVXhtyfq/J26k6yK9nK94KWV00B0NI8gBSiSTP5XEq+YF3qlPKs2DtT/CKM=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_7dd10736c91211f18b7f525400393706
    ReservedCode2: UDm3b/NfHHOkbknWca+ttp8cSfMCckglKfbESPE7h17FKwsXlvhweB/iFshQHxuiq9o9Jk6/hIsv0Gku9XZrg2r7w9bLEXfjvZSrWIv+Lg0phzlhFgGdmu5akERzFrIeUVXhtyfq/J26k6yK9nK94KWV00B0NI8gBSiSTP5XEq+YF3qlPKs2DtT/CKM=
---

# OpenXDB v1.17.0 Release Notes

发布日期：2026-10-30

## 版本亮点

- **显式事务写路径优化**：三项独立优化——① UPDATE 快照刷新锁外化（`snapshot_lock_external`，锁内刷新移到锁外，降低写语句延迟）；② autocommit 写 pipeline 深化（`write_pipeline`，写语句接入连接内 pipeline，减少端侧逐条往返）；③ WAL 组提交主动定时窗口（`group_commit_active_window`，写密集下主动定时刷新合并 fsync）。后两项默认关闭可热切换，不影响 WAL 重放/崩溃恢复/复制 2PC 顺序，可随时回退 v1.16.0 基线行为。
- **性能大幅提升（发版前 E5 受控复测）**：OLTP **write QPS +61%**、**read-write TPS +55%**、read 无回归（+36%）；TPC-C 5 仓 **tpmC 774.50**（较 v1.16.0 的 254.00 提升约 **3.0 倍**），全部达成 v1.17.0 验收目标。
- **可观测性闭环（D3）**：EXPLAIN 节点级耗时/行数估算（`rows=N est_ms=X.XX`）+ `SHOW SLOWQUERIES SUMMARY TOP N` 阈值区间桶汇总报表，配合 `slow_query_threshold` 热配置，形成慢查询诊断闭环。
- **稳定性验证（D4）**：8 连接写密集 30min 长稳 `errs=0` / `wal.log` 受控（自动 checkpoint/truncate，45.6MB）；bench 工具 CLI 参数化，复测流程可复现。
- **回退记录（D2）**：会话锁调度精细化因并发伸缩验收未达标整体回退（16 连接 read TPS 10.80 较 8 连接 11.85 为 -8.9%、read-write -17.1%，未达"较 8 连接提升 ≥20%"），恢复 D1 基线行为。

## 新增能力（相对 v1.16.0）

- **D1.1 UPDATE 快照刷新锁外化**（`snapshot_lock_external`）：锁内刷新移到锁外，降低写语句延迟。
- **D1.2 autocommit 写 pipeline 深化**（`write_pipeline`，默认 off 可热切换）：写语句接入连接内 pipeline，减少端侧逐条往返。
- **D1.3 WAL 组提交主动定时窗口**（`group_commit_active_window`，默认 off 回退 v1.16.0 C1 单等待者免窗口）：写密集下主动定时刷新合并 fsync，不影响 WAL 重放/崩溃恢复/复制 2PC 顺序。
- **D3 可观测性与诊断闭环**：EXPLAIN 节点级耗时/行数估算 + `SHOW SLOWQUERIES SUMMARY TOP N` 阈值区间桶汇总。
- **D4 并发规模与稳定性验证**：8 连接写密集 30min 长稳 `errs=0`；bench 工具 CLI 参数化。
- **D2 回退**：会话锁调度精细化因并发伸缩验收未达标整体回退，恢复 D1 基线行为（无遗留配置项）。

## 性能（发版前 E5 受控复测）

8 连接、每轮 prepare 重置 + 3 轮取中位数（OLTP 8 线程 × 60s、TPC-C 5 仓 × 120s）：

| 指标 | v1.16.0 | v1.17.0 | 变化 |
|------|---------|---------|------|
| OLTP read TPS / QPS | 10.37 / 145.13 | **14.15 / 198.10** | +36% / +37% |
| OLTP write TPS / QPS | 8.33 / 33.33 | **13.40 / 53.60** | +61% / +61% |
| OLTP read-write TPS / QPS | 8.15 / 146.70 | **12.60 / 226.80** | +55% / +55% |
| TPC-C 5 仓 tpmC / tpm_total | 254.00 / 581.00 | **774.50 / 1746.50** | 约 3.0 倍 |
| 连接错误 errs / reconnects | 0 / 0 | 0 / 0 | 保持 |

v1.17.0 验收目标全部达成：TPS read ≥11（14.15）/ write ≥9（13.40）/ rw ≥8.5（12.60）、QPS read ≥145（198.10）/ write ≥40（53.60）/ rw ≥146（226.80）、tpmC ≥300（774.50）、errs=0 / reconnects=0。详情见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。

## 兼容性说明

- **数据目录兼容**：v1.17.0 保持既有数据文件格式版本（`pkg/db.DataFormatVersion`），已初始化的 v1.0.0 至 v1.16.0 数据目录可直接打开，无需迁移重建。
- **协议兼容**：自研二进制协议（pkg/server，非 MySQL wire protocol）。历史客户端（JDBC/Python 驱动）按既有帧格式可继续工作；驱动与服务器版本匹配即可。
- **配置兼容**：新增配置项（`snapshot_lock_external` / `write_pipeline` / `group_commit_active_window` 等）默认值与既有行为兼容、均可热切换回退；既有命令行参数与 SET PERSIST 配置项保持向后兼容。
- **无破坏性变更**：本次发布未引入数据文件格式变更或协议破坏性变更；D2 会话锁调度精细化已整体回退，不遗留配置项。

## 已知问题

详见 [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md)。主要关注：

- SQL 为子集：暂无完整 MySQL 语法覆盖（自研二进制协议 + 可选 MySQL wire 兼容监听）。
- 性能边界：E5 实测为单机端侧基准（8 连接），非生产级吞吐；D2 会话锁调度精细化已回退，16 连接并发伸缩收益未达预期。
- 单机 → 分布式边界：分布式写需 2PC，远程写有前置条件限制。

## 升级路径

1. 备份：`openxdb backup` 或脚本备份数据目录（参考 scripts/rolling_upgrade.ps1）。
2. 预检：`openxdb preflight --data-dir <dir>` 确认版本与数据格式匹配。
3. 优雅停机：`openxdb shutdown --graceful` 或 SIGTERM。
4. 替换二进制为 v1.17.0 并启动（数据目录不变）。
5. 验证：`openxdb doctor`、`openxdb stats`、`openxdb version` 确认 v1.17.0；抽查业务读写与备份恢复。

## 文档与支持

- 产品手册（中文）：docs/product_manual_zh.md
- 产品手册（英文）：docs/product_manual_en.md
- 变更记录：CHANGELOG.md
- 已知限制：KNOWN_LIMITATIONS.md
- 设计文档：docs/（T1–T42、M 系列、E 系列）
- 基准实验：docs/experiments/（E1–E5）

---

# OpenXDB v1.0.0 Release Notes

发布日期：2026-10-16

## 开发中增量（v1.5.0-dev，2026-10-23）

v1.5.0「协议与吞吐」第 1 项「客户端 pipeline 协议」已先行启动并落地（详见 CHANGELOG）：

- Python 驱动新增 `Cursor.pipeline()` / `Connection.pipeline()` 流水线批量执行（底层 `OpenXDBProtocol.pipeline` 一次发送多行、按序读取响应）；
- JDBC 驱动 `addBatch` / `executeBatch` / `executeLargeBatch` 升级为真实流水线批量执行；
- 服务端无需改动（`REPLSession` 逐行按序处理），新增 `TestREPLSessionConsecutiveLinesInOrder` 回归测试；
- 双驱动补正确性测试（混合语句交错）与吞吐基准（批量 vs 逐条约 1.2–1.3 倍提升）。

OpenXDB 是从零自研的单机/分布式关系型数据库内核（Go + RocksDB），历经 M1–M21、T1–T42、E1–E5 全部里程碑，本版本为 v1.0.0 正式发布版。

## 版本亮点

- **自研单机 → 分布式内核完整能力**：从 RocksDB 存储、WAL、快照隔离事务起步，完整交付复制（M2）、分片（M3）、多节点集群（M4）、分布式 2PC 写（M5）、HA 故障转移（M6）、region 自动分裂与负载均衡（M7）、分布式事务（TSO + 分布式快照隔离 + 2PC 恢复加固，M8）等分布式能力，单机部署零触发、零回归。
- **SQL 兼容簇**：M9–M19 依次补齐在线 DDL/查询缓存/批量导入/外键/视图、CTE/窗口函数/upsert、认证权限、分布式 SQL（join/ORDER BY/子查询）、可观测（慢查询/指标/状态）、生态接入（mysqldump 导入/binlog CDC）、M18 MySQL 兼容（prepared statement + information_schema）、M19 生态导出（JSON/Parquet）；T40–T42 收尾 MAX/MIN 聚合、算术表达式、SELECT DISTINCT、IF NOT EXISTS、CREATE TABLE AS SELECT、INSERT INTO SELECT。
- **商业加固**：M17 审计日志与细粒度 RBAC，M21 三步加固——TLS 加密传输（T36）、监控告警（T37）、真实用户验证（T38）。
- **可验证的性能基准**：E1–E5 基准实验全链路可复现；E5 采用标准 sysbench OLTP / TPC-C 小仓基准对齐（因自研二进制协议非 MySQL wire，以 Python PEP 249 驱动等价实测）：OLTP read 1.73 TPS（QPS 24.27）/ write 0.93 TPS（QPS 3.73）/ read-write 1.32 TPS（QPS 23.70），TPC-C 5 仓 tpmC 13.50（TPM 32.50），8 线程 × 60s/120s，0 错误。
- **产品文档体系**：新增中英文产品手册（docs/product_manual_zh.md / docs/product_manual_en.md，11 章节对应结构）、CHANGELOG、RELEASE_NOTES、KNOWN_LIMITATIONS 与 MkDocs 站点骨架。

## 新增能力（相对 v0.30.0-alpha）

- **中英文产品手册**：11 章节完整产品文档，覆盖架构、SQL、复制/分片/集群、运维、性能基准、已知限制与 FAQ。
- **T41 DDL 兼容簇（v0.29.0-alpha 起，本版本纳入发布线）**：CREATE 系列 IF NOT EXISTS（TABLE/VIEW/MATERIALIZED VIEW/INDEX/USER/ROLE/TRIGGER/PROCEDURE）+ CREATE TABLE AS SELECT。
- **T42 INSERT INTO SELECT（v0.30.0-alpha 起）**：INSERT/REPLACE INTO ... SELECT 全闭环。
- **发布收尾配套**：CHANGELOG.md / RELEASE_NOTES.md / KNOWN_LIMITATIONS.md 新增，README 版本定稿 v1.0.0，`openxdb version` 输出 v1.0.0，MkDocs 站点骨架（mkdocs.yml + docs/index.md）。

## 兼容性说明

- **数据目录兼容**：v1.0.0 保持既有数据文件格式版本（`pkg/db.DataFormatVersion`），已初始化的 v0.1.0-alpha 至 v0.30.0-alpha 数据目录可直接由 v1.0.0 打开，无需迁移重建。数据格式版本号以 `openxdb version` 输出为准。
- **协议兼容**：自研二进制协议（pkg/server，非 MySQL wire protocol）。历史客户端（JDBC/Python 驱动）按既有帧格式可继续工作；驱动与服务器版本匹配即可。
- **配置兼容**：命令行参数与 SET PERSIST 配置项保持向后兼容；TLS 默认仅加密监听，如需明文/TLS 共存可配置 `--tls-allow-legacy` / `SET PERSIST tls_allow_legacy=on`。
- **升级路径**：从既有 -alpha 版本升级为 v1.0.0 时：先执行 `openxdb preflight` 检查，备份数据目录，用新版本二进制启动（数据目录不变），再执行 `openxdb doctor` 与 `openxdb stats` 验证健康；单机与集群拓扑均支持。建议先在小数据量副本上演练再切生产。
- **无破坏性变更声明（需谨慎核实）**：本次发布未引入数据文件格式变更或协议破坏性变更，但正式发布前请在目标环境完成一次 BACKUP + RESTORE 演练与全量回归，确认无误后再替换生产二进制。

## 已知问题

详见 [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md)。主要关注：

- SQL 为子集：暂无 HAVING、部分 DDL 类型、无 MySQL wire protocol 兼容（自研二进制协议）。
- 性能边界：全局会话锁串行 + 逐语句往返，E5 实测为单机端侧基准，非生产级吞吐。
- 单机 → 分布式边界：分布式写需 2PC，远程写有前置条件限制。
- 文档边界：docs/ 设计文档为中文开发记录。

## 升级路径

1. 备份：`openxdb backup` 或脚本备份数据目录（参考 scripts/rolling_upgrade.ps1）。
2. 预检：`openxdb preflight --data-dir <dir>` 确认版本与数据格式匹配。
3. 优雅停机：`openxdb shutdown --graceful` 或 SIGTERM。
4. 替换二进制为 v1.0.0 并启动（数据目录不变）。
5. 验证：`openxdb doctor`、`openxdb stats`、`openxdb version` 确认 v1.0.0；抽查业务读写与备份恢复。

## 文档与支持

- 产品手册（中文）：docs/product_manual_zh.md
- 产品手册（英文）：docs/product_manual_en.md
- 变更记录：CHANGELOG.md
- 已知限制：KNOWN_LIMITATIONS.md
- 设计文档：docs/（T1–T42、M 系列、E 系列）
- 基准实验：docs/experiments/（E1–E5）
*（内容由AI生成，仅供参考）*

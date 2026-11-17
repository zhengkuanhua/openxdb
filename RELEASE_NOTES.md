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

# OpenXDB v1.18.0 Release Notes

发布日期：2026-11-17

## 版本亮点

- **跨节点数据分布落地（D3，P0）**：显式 REGION 指派（隐式 region 显式化 + `ASSIGN REGION`）在三节点组网完成分布，`SHOW REGION ROUTES` 由空变为可见 region → node 映射；跨节点点查可见（node-b 写、node-a 读 cnt=1），COUNT/SUM 跨节点聚合不丢不重，非并发全链路验证通过。HA 灰度长期缺失的"数据分布层未接线"前置阻塞项就此解除。
- **HA quorum 灰度全链路演练（D4，P0）**：`ha.quorumEnabled=true` 三节点多数派开启评估、宕机 failover（读 failover 自动重指派）、脑裂 + epoch fencing 拒绝旧主写、节点恢复回迁与路由收敛四项演练完成，13 项结论矩阵中 8 项通过、4 项代码已修待真机复验、1 项部分落地；最终判定 **维持 `ha.quorumEnabled=off`、`ha.epochEnabled=on`**，结构性原因（集群轨无共识提交、写为归属节点本地提交 + 副本异步补齐）与升级路径已写入灰度报告。
- **会话锁重试收口（D2，P0）**：v1.17.0 整体回退的会话锁重试项在干净库重建后复测通过——16 连接 read **17.12 TPS**（较 8 连接基线 11.85 提升 **+44.6%**）、read_write 8 连接 **13.58 TPS** 无回归，errs=0。
- **读路径优化（D6，P2）**：批量点查合并把 routed JOIN 回表与 IN 子查询由逐行/逐值点查改为一次 `mgetKVCluster`（N=64 实测 **33.3ms → 0.52ms**，点查往返 64 次 → 1 次）；块缓存调优实测 **-1.6% 负收益已回退**，确认瓶颈为 cgo 往返而非缓存容量；Snapshot 批量 Scan 评估确认快照复用、多区间合并扫描、版本可见性批量过滤均已具备，无新增代码。
- **Linux 平台口径补齐（D5，P1）**：RHEL 9.7 目标机以仓库脚本从零构建部署打通，同口径 E5 受控复测落地（OLTP 8.17 / 8.13 / 8.10 TPS、TPC-C tpmC 749.50，14 组采集 errs=0 / reconnects=0），跨平台可比性边界成文。

## 新增能力（相对 v1.17.0）

- **D2 会话锁重试收口**：v1.17.0 回退后在干净库重建复测，16 连接 read 17.12 TPS（≥ 验收线 14.22，+44.6%）、read_write 8 连接 13.58 TPS 无回归，errs=0（commit `c2436cf` 三端一致）。
- **D3 跨节点数据分布策略**：显式 REGION 指派（隐式 region 显式化 + `ASSIGN REGION`，不强制先 SPLIT）三节点分布落地；`SHOW REGION ROUTES` 可见指派结果，跨节点点查可见性、COUNT/SUM 跨节点聚合不丢不重，非并发全链路验证通过；三节点数据分布操作指南成文。
- **D4 HA quorum 灰度全链路演练（D4.1–D4.5）**：`ha.quorumEnabled=true` 三节点多数派评估、宕机 failover（读 failover 自动重指派 + 数据可读）、脑裂与 epoch fencing 拒绝旧主写、节点恢复回迁与路由收敛四项演练完成；灰度报告（13 项结论矩阵）与 `HA_BRAIN_SPLIT_DESIGN` §5.5 结论更新。
- **D6.1 块缓存调优（负收益，不采纳）**：块缓存调至 256MB 实测 -1.6%，已回退；确认 SQL 路径瓶颈为 cgo 往返（`runtime.cgocall` flat 63.75%）而非引擎缓存容量。
- **D6.2 批量点查合并**：`routedJoin`（routed JOIN 回表）与 `evalInSubqueryRouted`（IN 子查询）由逐行/逐值点查改为一次 `mgetKVCluster` 批量读，保持左行原序、LEFT JOIN 补空、未命中跳过与路由错误前置返回语义；N=64 基准 33.3ms → 0.52ms，验证用例断言 `Get=0` / `MGet=1`。
- **D6.3 Snapshot 批量 Scan 评估**：确认快照号语句级/事务级复用、多区间单次合并扫描、版本可见性批量过滤均已具备，无新增代码；跨 region 远端区间聚合（多 `QUERY_REQ` 合并）转 v1.19.0 与写路径 2PC 一并处理。
- **D5 Linux E5 受控复测**：D5.1 在 RHEL 9.7 目标机（2 vCPU / 2.8GB / Go 1.22.12 / RocksDB 11.8.1 CGO，源码 HEAD `dac6843`）以 `scripts/build.sh --rocks-src/--rocks-build` 构建并安装为 `/usr/local/bin/openxdb`，bench 依赖（仓库内 `drivers/python`）对齐，Linux 从零部署链路打通；D5.2 与 Windows 完全同口径（同脚本、8 连接、每轮 prepare 重置、3 轮取中位数）完成采集。
- **D7 发版收尾**：版本常量升级 `v1.18.0`（握手串 `8.0.36-OpenXDB v1.18.0` 与 `select version()` 同源注入、自动联动）；`oxsql` 交互提示符由 `oxsql>` 改为 `openxdb>`（命令名仍为 `oxsql`）；RELEASE_NOTES / README / KNOWN_LIMITATIONS 文档同步；`go build ./...` 与 `go vet ./...` 全绿。

## 性能（发版前 E5 受控复测）

同脚本、8 连接、每轮 prepare 重置、3 轮取中位数（OLTP 8 线程 × 60s、TPC-C 5 仓 × 120s）。D5 在 Linux 目标机（2 vCPU）按完全同口径复测，与 Windows v1.17.0 基线对比：

| 指标 | v1.17.0（Windows 16 逻辑核基线） | v1.18.0（RHEL 9.7 / 2 vCPU） | 相对基线 |
|------|---------|---------|------|
| OLTP read TPS / QPS | 14.15 / 198.10 | **8.17 / 114.33** | 57.7% |
| OLTP write TPS / QPS | 13.40 / 53.60 | **8.13 / 32.53** | 60.7% |
| OLTP read-write TPS / QPS | 12.60 / 226.80 | **8.10 / 145.80** | 64.3% |
| TPC-C 5 仓 tpmC / tpm_total | 774.50 / 1746.50 | **749.50 / 1684.50** | 96.8% |
| 采集组数 errs / reconnects | 0 / 0 | 0 / 0（14 组） | 保持 |

口径说明：Linux 数值仅用于同口径功能与稳定性验证，**不作跨平台性能对标**——差异主因为 CPU 配额（2 vCPU vs 16 逻辑核），非代码路径：同机 threads=2 与 threads=8 的 read TPS 同为约 8.2，而 p50 延迟由 0.24s 升至 0.95s，表明 2 vCPU 下吞吐已饱和、8 连接属超配口径；TPC-C 因客户端 Python 8 线程 GIL 与服务端队列共同限流，跨平台差异被掩盖（仅剩 3.2%）。性能基线仍以 Windows 平台为准。另确认"每轮 prepare 重置"为可比性必要条件（不重置连跑 TPC-C tpmC 衰减 565.00 → 230.50 → 220.00，属 LSM compaction 积压而非版本回归）。详见 [docs/experiments/D5_linux_e5_controlled_retest.md](docs/experiments/D5_linux_e5_controlled_retest.md)。

## 兼容性说明

- **数据目录兼容**：v1.18.0 保持既有数据文件格式版本（`pkg/db.DataFormatVersion`），已初始化的 v1.0.0 至 v1.17.0 数据目录可直接打开，无需迁移重建。
- **协议兼容**：自研二进制协议（pkg/server，非 MySQL wire protocol；`--mysql-port` 启用兼容监听）。历史客户端（JDBC/Python 驱动）按既有帧格式可继续工作；驱动与服务器版本匹配即可。握手版本串随版本常量升级为 `8.0.36-OpenXDB v1.18.0`。
- **配置兼容**：D3/D4/D6 均以既有权能或独立开关推进；`ha.quorumEnabled` / `ha.epochEnabled` 默认值不变（quorum off、epoch on）；块缓存调优因负收益已回退，未引入新默认值。既有命令行参数与 SET PERSIST 配置项保持向后兼容。
- **客户端体验变更**：`oxsql` 交互模式提示符由 `oxsql>` 改为 `openxdb>`；命令名、命令行参数与输出格式不变，脚本化调用（`-c`）不受影响。
- **无破坏性变更**：本次发布未引入数据文件格式变更或协议破坏性变更。

## 已知问题

详见 [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md)。主要关注：

- **HA quorum 仍维持默认关闭**：集群轨尚无一致性协议（无 raft 式日志复制与选举），写提交为归属节点本地提交 + 副本异步推送补齐，存在"单副本可写窗口"与 RPO>0；解除默认关闭的前置是 v1.19.0 M8（写路径强制路由 + 2PC 参与 + 副本间持续复制/共识提交）。
- **D4.5 灰度报告 §8.2 未闭环项**：NEW-1 / T3 / NEW-2 三项真机复验与演练脚本补强，v1.18.0 发版前不阻塞，**v1.19.0 前必须完成**（T3 需三个独立 IP，属环境卡点）。
- **跨 region 远端区间聚合**（多 `QUERY_REQ` 合并）与远端 Scan 协议未落地，顺延 v1.19.0 与写路径 2PC 共用请求骨架。
- **通用边界**：SQL 为子集（暂无完整 MySQL 语法覆盖，自研二进制协议 + 可选 MySQL wire 兼容监听）；性能为单机端侧基准（8 连接），非生产级吞吐承诺。

## 升级路径

1. 备份：`openxdb backup` 或脚本备份数据目录（参考 scripts/rolling_upgrade.ps1）。
2. 预检：`openxdb preflight --data-dir <dir>` 确认版本与数据格式匹配。
3. 优雅停机：`openxdb shutdown --graceful` 或 SIGTERM。
4. 替换二进制为 v1.18.0 并启动（数据目录不变）。
5. 验证：`openxdb doctor`、`openxdb stats`、`openxdb version` 确认 v1.18.0；集群拓扑下抽查 `SHOW REGION ROUTES`、跨节点点查与聚合，以及业务读写与备份恢复。

## 文档与支持

- 产品手册（中文）：docs/product_manual_zh.md
- 产品手册（英文）：docs/product_manual_en.md
- 变更记录：CHANGELOG.md
- 已知限制：KNOWN_LIMITATIONS.md
- HA quorum 灰度报告：docs/D4_5_ha_quorum_gray_report.md
- Linux E5 复测报告：docs/experiments/D5_linux_e5_controlled_retest.md
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

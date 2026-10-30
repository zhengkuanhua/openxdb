---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_a98494a2d2c611f18460525400393706
    ReservedCode1: iLDTRG50fxXG7uZsQuvQ1PywbIyR6eOm1s5ELBKuERIYwQydR0J0hY2TX8N09x5A0qi4/FFNLdkApoVAqkeETsH31w/+XMcxPGLK5BWhmeujo/+AvjI9myVd/8mzXn/MsANhUFLn+DhvEO7Bs99sxXWdOxjOp62jsCdKXHugl5z0euHijllnzOM0oWo=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_a98494a2d2c611f18460525400393706
    ReservedCode2: iLDTRG50fxXG7uZsQuvQ1PywbIyR6eOm1s5ELBKuERIYwQydR0J0hY2TX8N09x5A0qi4/FFNLdkApoVAqkeETsH31w/+XMcxPGLK5BWhmeujo/+AvjI9myVd/8mzXn/MsANhUFLn+DhvEO7Bs99sxXWdOxjOp62jsCdKXHugl5z0euHijllnzOM0oWo=
---

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_7d29f5c7c91211f196f9525400248c00
    ReservedCode1: ytEZT7sfCrFtdGbvc6CcEL+GFQjiv9F1tIX4uImXrz/tfGu9Sr6p8pebTKxbwLJ8z89fe4he4t9cg1XcI1VpxOMGR9EabN67UEupkAIAmrLKjjI7U9q5CkYs9I0b6FfgX8TXSCmLr8l8FzRI4QGsiRIvotyOOXzj8+tNKHozuVXOrJSPWqPEpI+vPCM=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_7d29f5c7c91211f196f9525400248c00
    ReservedCode2: ytEZT7sfCrFtdGbvc6CcEL+GFQjiv9F1tIX4uImXrz/tfGu9Sr6p8pebTKxbwLJ8z89fe4he4t9cg1XcI1VpxOMGR9EabN67UEupkAIAmrLKjjI7U9q5CkYs9I0b6FfgX8TXSCmLr8l8FzRI4QGsiRIvotyOOXzj8+tNKHozuVXOrJSPWqPEpI+vPCM=
---

# Changelog

OpenXDB 版本变更记录。

> 版本线说明：历史版本以仓库 README Status 行、docs/ 设计文档头部标注与 git 提交记录为准。早期部分中间 -alpha 版本未单独打标（git 与文档均无显式版本号），按里程碑区间归纳，版本号不做臆造。

## v1.17.0 (2026-10-30)

v1.17.0（显式事务写路径优化 + 可观测性/稳定性补强）正式发布。按 DEV_PLAN_v1.17.0 完成 D1 三项写路径改造（D2 会话锁调度精细化因并发伸缩验收未达标整体回退）、D3 可观测性与诊断闭环、D4 并发规模与稳定性验证，并经 D5 发版前 E5 受控复测，将 write 吞吐从"贴近目标线"拉到"稳定余量"（QPS +50%）；全仓回归全绿。

**Added**

- **D1.1 UPDATE 快照刷新锁外化**：显式事务 UPDATE 的锁内快照刷新移到锁外（对齐 C2 Begin 锁外化同型改造），降低写语句延迟；`snapshot_lock_external` 独立 flag，失败可回退锁内刷新。
- **D1.2 autocommit 写 pipeline 深化**：写语句接入连接内 pipeline（原 pipeline 仅覆盖读路径），减少端侧逐条往返；`write_pipeline` 独立 flag 默认 off，可热切换回退逐条执行。
- **D1.3 WAL 组提交主动定时窗口**：`group_commit_active_window`（默认 off 回退 v1.16.0 C1 单等待者免窗口）在写密集下主动定时刷新合并 fsync，降低 fsync 次数与写放大；不影响 WAL 重放 / 崩溃恢复 / 复制 2PC 顺序。
- **D3.1 EXPLAIN 节点级耗时/行数估算**：执行计划 planLines 追加 `rows=N est_ms=X.XX` 节点级耗时与行数估算，真实行数走存储层 CountRange 计数，输出向后兼容追加字段，旧解析器不受影响。
- **D3.2 slowlog 阈值区间 TOP N 汇总**：`SHOW SLOWQUERIES SUMMARY` 按阈值区间汇总 TOP N（降序稳定排序 + bucket 区间聚合），配合 `SET slow_query_threshold` 热配置形成诊断闭环。
- **D4.1 并发规模长稳验证**：8 连接写密集 30min 长稳——errs=0、goroutines 恒 2、heap 42MB、wal.log 受控（45.6MB）、崩溃恢复 check COUNT 严格等价（2355864）。
- **D4.2 bench 工具参数化**：`bench_oltp.py --rows` CLI 化、`run_e5.ps1 -OltpRows`、`load.go` 追加 goroutine/heap 观测，便于复测与 CI。
- **E5 受控复测（发版最终）**（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read / write / read-write **14.15 / 13.40 / 12.60 TPS**（QPS 198.10 / 53.60 / 226.80，errs=0 / reconnects=0），较 v1.16.0（10.37 / 8.33 / 8.15 TPS，QPS 145.13 / 33.33 / 146.70）**write QPS +61%**、read-write TPS +55%、read 无回归 +36%；TPC-C 5 仓 tpmC **774.50** / tpm_total 1746.50（较 v1.16.0 的 254.00 / 581.00 提升约 **3.0 倍**）。**达成 v1.17.0 验收目标：OLTP TPS read ≥11（14.15）/ write ≥9（13.40）/ rw ≥8.5（12.60），QPS read ≥145（198.10）/ write ≥40（53.60）/ rw ≥146（226.80），TPC-C tpmC ≥300（774.50），errs=0 / reconnects=0**。详见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。

**回退记录（D2）**

- **D2 会话锁与调度精细化已回退**：D2.1 session lock granularity split（d7660f5）与 D2.2 read-parallel in write txn（442c4cb）因并发伸缩验收未达标（D2.3 复测 16 连接 read TPS 10.80 较 8 连接 11.85 为 -8.9%、read-write 10.28 较基线 12.40 为 -17.1%，未达"16 连接较 8 连接提升 ≥20%"验收目标）而整体回退，恢复 D1 基线行为（等价 cdd62e7）；D2.3 并发伸缩 bench 数据与实验文档（e1ba367）保留作为记录。回退后全仓回归全绿（`go test ./... -count=1 -p 1`），E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read / write / read-write **12.75 / 12.63 / 10.97 TPS**（QPS 178.50 / 50.53 / 197.40，errs=0 / reconnects=0），TPC-C 5 仓 tpmC **709.00** / tpm_total 1568.00。**D1 指标复测仍达标：write QPS 中位 ≥40（50.53）、TPS write ≥9（12.63）、tpmC ≥300（709.00），read / read-write 无回归，errs=0 / reconnects=0**。

**边界声明**

- TPS/QPS 受全局会话锁与单机端侧往返制约，数值为**单机端侧功能基准**，非生产级吞吐承诺；write 模式已脱离吞吐下限（QPS 中位 53.60，三轮最低 52.87 仍高于 v1.16.0 中位 33.33）。
- 所有优化均有独立 feature flag 可灰度回退；与 v1.16.0 兼容、可回退。

## v1.16.0 (2026-10-28)

v1.16.0（SQL 执行内核并发模型优化 + WAL 持久化增强）正式发布。按 DEV_PLAN_2026Q8 完成 C1-C7 六项内核并发优化与 E5 受控复测，并落地 WAL 自动 checkpoint / truncate 持久化收敛；全仓回归全绿。

**Added**

- **C1 WAL 组提交增强**：单等待者免 0.5ms 窗口 + 批量刷盘，写事务 COMMIT 延迟显著下降；`group_commit_window_ms` 可回退。
- **C2 Begin 快照创建锁外化**：快照创建移至锁外，ID / 序号仍单调；点查 / autocommit 密集场景 Begin 锁竞争消除。
- **C3 ro_parallel 下沉 executor + 显式事务只读并行**：只读并行覆盖 REPL 批与 autocommit 直调场景，显式事务内纯只读语句并行执行（MVCC 快照语义）；`ro_parallel` 默认 on 可回退。
- **C4 提交应用流水线化 / 组提交**：阶段 3 按 seq 排队改写为注册聚合，WAL 重放结果严格等价、复制 / 2PC 顺序不变。
- **C5 cgo 扫描拷贝优化**：arena 单块 xscan + 两趟 do_scan + 零拷贝 xscan_item（unsafe 边界专项测试）；全表 / 范围读加速。
- **C6 连接内语句 pipeline / 快照复用**：autocommit 顶层纯只读 SELECT 共享快照 + 连接内 pipeline flush；`snapshot_share` / `pipeline_flush` 独立 flag 默认开。
- **WAL 自动 checkpoint / truncate**：新增安全位点模型（safe = min(LastSyncedLSN, AppliedLSN)，截断前 RocksDB Flush 落 SST），后台周期检查自动截断，长期运行 wal.log 大小受控；配置 `wal_auto_checkpoint`（默认 on）/ `wal_checkpoint_interval_ms`（60000）/ `wal_checkpoint_max_mb`（64）；新增 LastSyncedLSN / AppliedLSN 位点基础设施与 storage.Flusher 接口。
- **C7 E5 受控复测**（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read / write / read-write **10.37 / 8.33 / 8.15 TPS**（QPS 145.13 / 33.33 / 146.70，errs=0 / reconnects=0），较 v1.15.0（0.38 / 0.40 / 0.40 TPS）TPS 提升约 **20-27 倍**、QPS 提升约 **22-27 倍**，达成验收目标（TPS≥1.5、QPS≥30）；TPC-C 5 仓 tpmC **254.00** / tpm_total 581.00（较 v1.15.0 81.50 / 170.50 提升约 3.1-3.4 倍）。详见 [docs/experiments/C7_e5_controlled_retest.md](docs/experiments/C7_e5_controlled_retest.md)。

**边界声明**

- TPS/QPS 受全局会话锁与单机端侧往返制约，数值为**单机端侧功能基准**，非生产级吞吐承诺；write 模式为吞吐下限（QPS 中位 33.33）。
- 所有优化均有独立 feature flag 可灰度回退；与 C1-C6 兼容、可回退。

## v1.15.0 (2026-10-27)

v1.15.0（SQL 执行层单语句路径优化）落地四项优化：W1 Scan 快路径、W2 COUNT 快速路径、W3 列投影下推、W4 ORDER BY 索引顺序扫描免排序 + DISTINCT 相邻去重免排序。

**Added**

- **W1 Scan 快路径**：txn 无未提交写时快照直透，跳过全量物化 + 重建排序。
- **W2 COUNT(*)/COUNT(1) 快速路径**：无 WHERE/GROUP 等时走存储层 RangeCounter 逐 region 计数求和。
- **W3 SELECT 列投影下推**：按需解码列，JOIN / 子查询 / 星号保守全列回退。
- **W4 ORDER BY 索引顺序扫描免排序 + DISTINCT 相邻去重免排序**：单列二级索引 / 主键 + 运行时 O(n) 已序验证。
- **E5 受控复测**（3 轮取中位数，8 连接）：OLTP read 0.38 / write 0.40 / read-write 0.40 TPS（与 v1.14.0 持平、无回归）；TPC-C tpmC 81.50。单语句专项：COUNT(*) 全表 580ms → **5.18ms**（约 103-112 倍）、全表扫描多列 613.51ms（约 1.27-1.47 倍）、全表扫描单列 532.33ms（约 1.47-1.69 倍）、ORDER BY 主键 / DISTINCT 免排序生效（详见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)）。

## v1.6.0 ~ v1.14.0（里程碑区间摘要）

介于 v1.5.0 与 v1.15.0 之间的正式版本按里程碑区间归纳（细节见 README 版本状态节）：

- **v1.6.0 阶段一/二（2026-10-24）**：ALTER TABLE 列级操作补全（MODIFY COLUMN）、CREATE DATABASE / 库级管理（`db.tbl` 前缀）、多列窗口扩展、binlog / mysqldump 全选项、存储过程受限子集补齐（DECLARE/游标/OUT 参数）、HA 回迁自动化。
- **v1.7.0（2026-10-24）**：HA 全量对齐兜底联动；空表 AVG 语义转正。
- **v1.8.0（2026-10-24）**：触发器行级语义（FOR EACH ROW、NEW/OLD、体回滚与递归策略）。
- **v1.9.0（2026-10-24）**：物化视图增量刷新（CONCURRENTLY 快照交换、依赖链管理）。
- **v1.10.0（2026-10-25）**：HA 脑裂防护生产化（ha.epochEnabled 默认开启）。
- **v1.11.0（2026-10-25）**：商业生态增强与文档对齐（Parquet 多 RowGroup、告警 webhook、TLS 证书热轮换）。
- **v1.12.0（2026-10-25）**：性能与精度优化（bufio 读覆层 + COM_QUERY 多语句、DECIMAL 按列 scale、IMPORT BATCH 强化、WAL 组提交默认开启，E5 tpmC 13.5→41.0）。
- **v1.13.0（2026-10-25）**：服务端多连接并发处理 + E5 pipeline 化，TPC-C tpmC 41.00→78.00。
- **v1.14.0（2026-10-25）**：只读 SELECT 并行（ro_parallel）+ WAL 组提交定时窗口，E5 tpmC 78.00→85.50。

## v1.5.0-dev (2026-10-23)

v1.5.0（协议与吞吐）第 1 项「客户端 pipeline 协议」先行启动落地。服务端 `REPLSession` 本就逐行按序处理，改造集中在驱动层。

**Added**

- **Python 驱动流水线批量执行**：`OpenXDBProtocol.pipeline()` 一次性连续发送多条命令并按序批量读取响应；`Cursor.pipeline()` / `Connection.pipeline()` 暴露 `PipelineResult`（`ok` / `affected` / `table` / `error` 四类结果）；复用既有单条响应边界解析，与 `command()` 行为完全一致。
- **JDBC 驱动批量执行**：`OpenXDBStatement.addBatch()` / `executeBatch()` / `executeLargeBatch()` 由 `SQLFeatureNotSupportedException` 升级为真实流水线实现（底层 `OpenXDBProtocol.pipeline` 一次写出多行并按序读回），失败语句抛 `BatchUpdateException` 且保留已成功语句更新计数。
- **服务端回归测试**：`pkg/server/server_pipeline_test.go` 新增 `TestREPLSessionConsecutiveLinesInOrder`，覆盖连续多行混合语句（DDL/DML/查询/非法语句交错）按序执行、错误不中断、表格回复边界。
- **吞吐基准**：Python 侧 `tests/test_pipeline.py` 与 JDBC 侧 `OpenXDBPipelineTest.java` 内置批量 vs 逐条对比基准（本地实测约 1.2–1.3 倍提升）。

## v1.0.0 (2026-10-16)

正式发布版，自研单机/分布式关系型数据库内核（Go + RocksDB）功能闭环收尾。

**Added**

- **中英文产品手册**：新增 [docs/product_manual_zh.md](docs/product_manual_zh.md)（中文版）与 [docs/product_manual_en.md](docs/product_manual_en.md)（英文版），11 章节对应结构，覆盖架构、SQL、复制/分片/集群、运维、性能基准、已知限制与 FAQ。
- **发布收尾文档**：新增 CHANGELOG.md、[RELEASE_NOTES.md](RELEASE_NOTES.md)、[KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md)，README 版本号定稿 v1.0.0 并补充文档导航。
- **docs 站点骨架**：新增 [mkdocs.yml](mkdocs.yml) 与 [docs/index.md](docs/index.md)，提供 MkDocs 文档站点入口。
- **版本定稿**：`cmd/openxdb` 发行版本常量更新为 v1.0.0，与 README Status 行保持一致。

**Milestones covered（随历史版本交付，本版本汇总确认）**

- M1–M21、T1–T42、E1–E5 全部里程碑：存储/WAL/事务/SQL 子集/二级索引、复制、分片、多节点集群、2PC 分布式写、HA、自动分裂与负载均衡、分布式事务（TSO + 快照隔离）、备份恢复与 PITR、客户端驱动（JDBC/Python）、运维工具链、SQL 解析修复、M9–M16 功能/兼容/可观测/生态/运维簇、M17 审计与 RBAC、M18 MySQL 兼容（prepared statement + information_schema）、M19 生态导出（JSON/Parquet）、M20 分布式纵深（同步/半同步复制、跨节点 DDL、分布式备份一致性）、M21 商业加固（TLS、监控告警、真实用户验证）、E1–E5 基准与稳定性实验。

## v0.30.0-alpha

- **T42 INSERT INTO SELECT**：`INSERT [INTO] table [(cols)] SELECT` / `REPLACE INTO ... SELECT`，列清单可选、全 SELECT 源兼容（表达式/别名/聚合/DISTINCT/WHERE/GROUP BY/ORDER BY/LIMIT/OFFSET）、原子事务与回滚、自插入快照语义。见 [docs/T42_insert_select.md](docs/T42_insert_select.md)。
- **中英文产品手册**：docs/product_manual_zh.md 与 docs/product_manual_en.md（本版本内新增）。

## v0.29.0-alpha

- **T41 DDL 兼容簇**：CREATE 系列 IF NOT EXISTS（TABLE/VIEW/MATERIALIZED VIEW/INDEX/USER/ROLE/TRIGGER/PROCEDURE 幂等跳过）+ CREATE TABLE AS SELECT（表达式/聚合/DISTINCT/别名、结果驱动列类型推断、原子创建+插入+回滚）。见 [docs/T41_ddl_compat_cluster.md](docs/T41_ddl_compat_cluster.md)。

## v0.28.0-alpha

- **T40 SQL 能力簇**：MAX/MIN 聚合（全局/GROUP BY/window、空表返回 0）、算术表达式（+ - * / %、优先级、一元负号、除零报错）、SELECT DISTINCT（单/多列、与 WHERE/ORDER BY/LIMIT/聚合组合）。见 [docs/T40_sql_capability_cluster.md](docs/T40_sql_capability_cluster.md)。

## v0.27.0-alpha

- **T38 真实用户验证**：端到端商业场景验证（订单/库存/用户中心业务 schema、事务 ACID、查询分析、运维生态脚本、Python 驱动 30+ 场景全绿）。见 [docs/T38_commercial_real_user_validation.md](docs/T38_commercial_real_user_validation.md)。
- **T39 SQL 能力差距评估**：SQL 能力差距盘点与排期。见 [docs/T39_sql_capability_gap_assessment.md](docs/T39_sql_capability_gap_assessment.md)。
- **E5 标准基准对齐**：标准 sysbench/TPC-C 因非 wire 协议不可直接运行，改用 Python PEP 249 驱动等价实测：OLTP read 1.73 TPS / write 0.93 / read-write 1.32（8 线程 × 60s），TPC-C 5 仓 tpmC 13.50 / TPM 32.50（8 线程 × 120s），0 错误。见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。

## v0.26.0-alpha

- **T37 监控与告警**：告警规则模型（metric/op/threshold/duration_sec/severity，JSON 持久化）+ 评估引擎（NORMAL/ALERT/RESOLVED 状态机 + 持续时长去抖）+ logx 告警通道（环缓冲/文件/webhook）+ SHOW ALERT RULES / STATUS / ALERTS。见 [docs/T37_commercial_alerting.md](docs/T37_commercial_alerting.md)。

## v0.25.0-alpha

- **T36 TLS 合规**：连接层 TLS 加密与证书管理（默认仅 TLS 监听、`--tls-allow-legacy` 明文/TLS 共存、会话级 secure 标记、verify-full/verify-ca 客户端配置、`openxdb gen-cert` 自签名证书生成器、JDBC/Python 驱动 TLS 参数）。见 [docs/T36_commercial_tls.md](docs/T36_commercial_tls.md)。

## v0.24.0-alpha

- **T35 分布式备份一致性**：协调者四阶段多节点全局一致备份（PREPARE 收集 LastLSN/TSO → GlobalLSN=max(all LSN) → DO 以 global_lsn 对齐快照 → manifest 一致性标记）+ 分布式 RESTORE 恢复到全局时间点，产物格式向后兼容。见 [docs/T35_m20_dist_backup_consistency.md](docs/T35_m20_dist_backup_consistency.md)。

## v0.23.0-alpha

- **T34 跨节点 DDL**：DDL 元数据经 DDL_PUSH 帧广播到全部在线节点、按 TableID 幂等收敛、离线/失败记 pending 并在下次广播前或 ReplayPendingDDL 重放，单机零触发兼容。见 [docs/T34_m20_cross_node_ddl.md](docs/T34_m20_cross_node_ddl.md)。

## v0.22.0-alpha

- **T33 同步/半同步复制**：异步（M2）默认兼容 + sync/semi-sync 两种增强模式（WaitAck 在事务锁外确认、ACK 超时/无从降级异步、SET replication_mode/ack/timeout 热切换）。见 [docs/T33_m20_sync_semi_sync_replication.md](docs/T33_m20_sync_semi_sync_replication.md)。

## v0.21.0-alpha

- **M18 MySQL 兼容簇**：prepared statement（PREPARE/EXECUTE/DEALLOCATE + `?` 占位符 + `@var` 用户变量、会话级缓存）+ information_schema 只读虚拟视图（schemata/tables/columns/statistics）。见 [docs/T31_m18_mysql_compat.md](docs/T31_m18_mysql_compat.md)。
- **M19 生态导出**：EXPORT TABLE FORMAT JSON（JSON Lines）+ FORMAT PARQUET（自包含极简 Parquet v1 writer，Thrift Compact 编码、单 row group、PLAIN/UNCOMPRESSED）。见 [docs/T32_m19_ecosystem_export.md](docs/T32_m19_ecosystem_export.md)。

## v0.20.0-alpha

- **T29 M16 运维簇**：热配置重载（SET PERSIST/SHOW CONFIG、persisted.json 原子写、重启生效）+ 优雅关闭（SHUTDOWN [GRACEFUL]、drain 活跃事务）+ 升级辅助（openxdb preflight 检查、rolling_upgrade.ps1）。见 [docs/T29_m16_hot_config.md](docs/T29_m16_hot_config.md)、[docs/T29_m16_graceful_shutdown.md](docs/T29_m16_graceful_shutdown.md)、[docs/T29_m16_upgrade.md](docs/T29_m16_upgrade.md)。
- **T30 配置与告警日志链路**：统一配置文件与结构化日志告警。见 [docs/T30_config_log_alert.md](docs/T30_config_log_alert.md)。

## v0.19.0-alpha

- **T28 M15 SQL 功能簇（完成）**：物化视图（shadow-table 物化 + REFRESH [CONCURRENTLY] + DROP ... RESTRICT）+ 触发器 + 存储过程。见 [docs/T28_m15_matview.md](docs/T28_m15_matview.md)、[docs/T28_m15_trigger.md](docs/T28_m15_trigger.md)、[docs/T28_m15_procedure.md](docs/T28_m15_procedure.md)。

## v0.18.0-alpha

- **T27 M14 生态接入簇（推进）**：mysqldump 兼容导入（IMPORT MYSQLDUMP）+ binlog 订阅 CDC（pkg/cdc Subscriber pull-based）+ 工具链闭环（openxdb import CLI、examples/binlog_subscriber）。见 [docs/T27_m14_mysqldump_import.md](docs/T27_m14_mysqldump_import.md)、[docs/T27_m14_binlog_sub.md](docs/T27_m14_binlog_sub.md)、[docs/T27_m14_toolchain.md](docs/T27_m14_toolchain.md)。
- **T28 M15 SQL 功能簇（推进）**：物化视图/触发器/存储过程开始落地。

## v0.16.0-alpha

- **T26 M13 可观测簇（推进）**：慢查询日志（SHOW SLOWQUERIES / SET slow_query_threshold，环缓冲 + 持久化）。见 [docs/T26_m13_slowlog.md](docs/T26_m13_slowlog.md)。

## v0.14.0-alpha

- **T23 M10 SQL 兼容簇（推进）**：CTE（WITH 链/环保护）+ upsert（ON DUPLICATE KEY UPDATE + REPLACE INTO）。见 [docs/T23_m10_cte.md](docs/T23_m10_cte.md)、[docs/T23_m10_upsert.md](docs/T23_m10_upsert.md)。
- **T24 M11 认证与权限簇**：用户管理（SHA-256+salt）+ GRANT/REVOKE 权限模型 + 连接认证与权限校验。见 [docs/T24_m11_user.md](docs/T24_m11_user.md)、[docs/T24_m11_grant.md](docs/T24_m11_grant.md)、[docs/T24_m11_auth.md](docs/T24_m11_auth.md)。
- **T25 M12 分布式 SQL 簇**：分布式 join + 跨节点 ORDER BY/LIMIT + 分布式子查询。见 [docs/T25_m12_join.md](docs/T25_m12_join.md)、[docs/T25_m12_order_limit.md](docs/T25_m12_order_limit.md)、[docs/T25_m12_subquery.md](docs/T25_m12_subquery.md)。
- **T26 M13 可观测簇（推进）**：Prometheus 指标导出 + 状态视图。见 [docs/T26_m13_metrics.md](docs/T26_m13_metrics.md)、[docs/T26_m13_status.md](docs/T26_m13_status.md)。
- **T27 M14 生态接入簇（推进）**：binlog 订阅 CDC 方向。见 [docs/T27_m14_binlog_sub.md](docs/T27_m14_binlog_sub.md)。

## v0.13.0-alpha

- **T22 M9 SQL 功能簇**：在线 DDL（ALTER TABLE）+ 查询缓存 + 批量导入 + 外键（ON DELETE CASCADE|RESTRICT）+ 视图（CREATE/DROP VIEW、定义展开、环保护）。见 [docs/T22_m9_online_ddl.md](docs/T22_m9_online_ddl.md)、[docs/T22_m9_query_cache.md](docs/T22_m9_query_cache.md)、[docs/T22_m9_bulk_import.md](docs/T22_m9_bulk_import.md)、[docs/T22_m9_foreign_key.md](docs/T22_m9_foreign_key.md)、[docs/T22_m9_view.md](docs/T22_m9_view.md)。
- **T23 M10 SQL 兼容簇（推进）**：窗口函数（ROW_NUMBER/RANK/DENSE_RANK + SUM/AVG/COUNT，PARTITION BY/ORDER BY）。见 [docs/T23_m10_window.md](docs/T23_m10_window.md)。

## v0.12.0-alpha

- **T21 SQL 解析修复**：DROP TABLE IF EXISTS（幂等删除）+ 内联列级 PRIMARY KEY（`id INT PRIMARY KEY`）。见 [docs/T21_sql_parser_fixes.md](docs/T21_sql_parser_fixes.md)。

## v0.11.0-alpha

- **T20 运维工具链**：`openxdb doctor` 健康巡检（数据目录/WAL/RocksDB/元数据/日志 LSN/端口/慢查询，退出码 0/1/2）+ `openxdb stats` 实时指标（连接/表/region/慢查询/日志 LSN）+ 一键部署脚本（deploy.ps1 / deploy.sh）。见 [docs/T20_ops_toolchain.md](docs/T20_ops_toolchain.md)。

## v0.9.0-alpha ~ v0.10.0-alpha（里程碑区间，版本未逐版留档）

- **T18 备份与恢复**：一致性快照逻辑备份（单文件、version + CRC32）+ 幂等恢复（drop-then-recreate）+ LSN 基 PITR（RESTORE ... TO LSN）。见 [docs/T18_backup_restore.md](docs/T18_backup_restore.md)。
- **T19 客户端驱动**：JDBC 驱动（jdbc:openxdb://、META-INF/services 自动注册）+ Python 驱动（PEP 249 connection/cursor/protocol、qmark paramstyle），包装 pkg/server TCP 协议。见 [docs/T19_client_drivers.md](docs/T19_client_drivers.md)。

## v0.8.0-alpha

- **T17 M8 分布式事务增强**：全局 TSO（64 位 ms+logical 布局、批量发放、事务边界 Reset）+ 分布式快照隔离（版本记录 m:ver + begin_ts/commit_ts 全局过滤）+ 2PC 恢复加固（CoordRecord 持久化、重启后 commit/abort 决策）。见 [docs/T17_m8_dist_txn.md](docs/T17_m8_dist_txn.md)。

## v0.7.0-alpha

- **T16 M7 region 自动分裂与负载均衡**：写路径自动分裂（DML 计数水印 → 行数检查 → 数据中点分裂）+ BALANCE/周期触发负载均衡（最空闲节点、热 region 在线迁移）。见 [docs/T16_m7_split_balance.md](docs/T16_m7_split_balance.md)。

## v0.6.0-alpha

- **T15 M6 高可用与故障转移**：心跳超时节点下线检测 + OnDown 回调、region 自动重分配（副本扫描 + REGION_PUSH）、跨节点读故障转移、复制链路主从自动提升。见 [docs/T15_m6_ha.md](docs/T15_m6_ha.md)。

## v0.2.0-alpha ~ v0.5.0-alpha（里程碑区间，版本未逐版留档）

- **B2 写路径组提交**：批提交/批量 fsync（group commit / batch fsync）。见 [docs/B2_group_commit.md](docs/B2_group_commit.md)。
- **P0 SQL 增强**：JOIN / GROUP BY / FROM+IN 子查询 / BETWEEN / 会话事务（BEGIN/COMMIT/ROLLBACK）。见 [docs/T8_p0_sql_enhancements.md](docs/T8_p0_sql_enhancements.md)。
- **P1 CSV 类型**：CSV 导入导出 + DATE/DECIMAL/BLOB 类型。见 [docs/T9_p1_csv_types.md](docs/T9_p1_csv_types.md)。
- **P2 运维语句与表达式**：SHOW TABLES / SHOW INDEX / EXPLAIN / 慢查询 + LIKE / CASE WHEN。见 [docs/T10_p2_ops_expr.md](docs/T10_p2_ops_expr.md)。
- **M2 复制**：binlog + 复制器 + 从节点 + 心跳（异步主从）。见 [docs/T11_m2_replication.md](docs/T11_m2_replication.md)。
- **M3 分片**：RegionInfo 元数据 + Router + region 前缀物理键 + 跨 region 查询。见 [docs/T12_m3_sharding.md](docs/T12_m3_sharding.md)。
- **M4 多节点集群**：节点拓扑 + 握手/存活 + 集群路由表 + 跨节点 SELECT 转发/合并。见 [docs/T13_m4_multinode.md](docs/T13_m4_multinode.md)。
- **M5 分布式写路径 / 2PC**：跨节点事务两阶段提交（协调者/参与者、prepare 持久化、幂等 commit/abort、崩溃恢复、单机零触发）。见 [docs/T14_m5_2pc.md](docs/T14_m5_2pc.md)。
- **E1–E4 基准与稳定性**：RocksDB 写基准（E1a）/B+Tree 对比（E1b）、fsync 同步写基准（E2，batch=1000 时 sync 747k TPS seq / 198k rand）、SQL OLTP 端到端基准（E3，cgo MultiGet 修复后 256.3 TPS 8:2 / 605.4 TPS 5:5）、长时间稳定性与持久性（E4，6 分钟混合负载 307-385 TPS 无退化、重启 COUNT(*) 一致）。见 [docs/experiments/](docs/experiments/)。

## v0.1.0-alpha

- **M1 基础内核**：RocksDB cgo 桥存储层（+ 纯 Go 内存 B+Tree 参考实现）、追加式 WAL 与重放恢复、快照隔离本地事务、SQL 子集与二级索引、CLI/REPL 与 TCP 服务。见 [docs/T1_rocksdb_storage_impl.md](docs/T1_rocksdb_storage_impl.md)、[docs/T2_wal_impl.md](docs/T2_wal_impl.md)、[docs/T3_sql_layer.md](docs/T3_sql_layer.md)、[docs/T3_txn_impl.md](docs/T3_txn_impl.md)、[docs/T5_cli_impl.md](docs/T5_cli_impl.md)、[docs/T6_index_layer.md](docs/T6_index_layer.md)。
- **设计决策与早期文档**：ACID 笔记（[docs/T4_acid_notes.md](docs/T4_acid_notes.md)）、M2/M3 接口预留（[docs/T7_m2m3_reservations.md](docs/T7_m2m3_reservations.md)）、引擎选型 ADR（[docs/W4_adr_engine_selection.md](docs/W4_adr_engine_selection.md)）、博客 [docs/blog_why_openxdb_rocksdb.md](docs/blog_why_openxdb_rocksdb.md)。
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_fff9e8edd40011f180d5525400248c00
    ReservedCode1: nRLYRIDP/1GeKXFMzy+MkIeUkaOQy4C2TM8wea/j3bgFbddNNJt5q2nbcNbUtzqSASvzNtcPcWXJ4ZzrkqkvWQDHtJFRP7Kh+QVrKtP1qN6vu/M0NVhvssZ6dBppNPAFMQT6A/vK0Nmvm8iIe6AUMGdwV4tTxq2+yaZJif9rSk17Rxbo+6FYYH4hzxU=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_fff9e8edd40011f180d5525400248c00
    ReservedCode2: nRLYRIDP/1GeKXFMzy+MkIeUkaOQy4C2TM8wea/j3bgFbddNNJt5q2nbcNbUtzqSASvzNtcPcWXJ4ZzrkqkvWQDHtJFRP7Kh+QVrKtP1qN6vu/M0NVhvssZ6dBppNPAFMQT6A/vK0Nmvm8iIe6AUMGdwV4tTxq2+yaZJif9rSk17Rxbo+6FYYH4hzxU=
---

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_4a29eb5dcdd111f1b307525400393706
    ReservedCode1: YtWrJY5X9ga7JOaUVWHvfrtfOs6/DmVKYPZqRM7TZBswg+vqfvgKAQs+bWEhiopH81vKtcBqQ57DUYMz1eThDRPUTeI2yo/d6EvqsEpz6ClNMNoRWRohmub6O4nTXhn/Nd+75LfhDIA1kKzyCPyGEKIj/3BEp51ofE10DcjBXlKsfk7uOxETjyzcNYU=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_4a29eb5dcdd111f1b307525400393706
    ReservedCode2: YtWrJY5X9ga7JOaUVWHvfrtfOs6/DmVKYPZqRM7TZBswg+vqfvgKAQs+bWEhiopH81vKtcBqQ57DUYMz1eThDRPUTeI2yo/d6EvqsEpz6ClNMNoRWRohmub6O4nTXhn/Nd+75LfhDIA1kKzyCPyGEKIj/3BEp51ofE10DcjBXlKsfk7uOxETjyzcNYU=
---

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_7e7b432cc91211f196f9525400248c00
    ReservedCode1: WhCXMgXbRb2HXDCj2bcNX2HOXocawQTWgIJKMrlGyVTEGQlHZiqnGXR5CqWo9lsxfNyvmLAan4sBjGWpRFGU21li/NsYOjAWLe/ahPQ7/ke6iG7/wORZSuFIYkFtRYQACl0R6NKX89zDYe35sAdwxT9ujKvixLBO/2Z+w6pexlriOw8NcMDc0cJtHLA=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_7e7b432cc91211f196f9525400248c00
    ReservedCode2: WhCXMgXbRb2HXDCj2bcNX2HOXocawQTWgIJKMrlGyVTEGQlHZiqnGXR5CqWo9lsxfNyvmLAan4sBjGWpRFGU21li/NsYOjAWLe/ahPQ7/ke6iG7/wORZSuFIYkFtRYQACl0R6NKX89zDYe35sAdwxT9ujKvixLBO/2Z+w6pexlriOw8NcMDc0cJtHLA=
---

# OpenXDB Known Limitations

版本：v1.17.0（2026-10-30）

本文档列出 OpenXDB v1.17.0 的已知限制与边界。每条目给出限制描述、影响与建议，帮助用户在选型、部署与使用时做出合理判断。详细功能范围请以 [docs/product_manual_zh.md](docs/product_manual_zh.md)（第 4 章 SQL 参考）与设计文档 docs/ 为准。

---

## 1. SQL 能力边界

### 1.1 SQL 为子集（非完整标准）

- **限制描述**：OpenXDB 实现的是 SQL 子集，并非完整 SQL 标准。当前 `HAVING` 子句、聚合表达式参数（如 `SUM(a+b)`）、`COUNT(DISTINCT expr)` 及 `GROUP BY` 后 `ORDER BY`/`LIMIT` 已支持（v1.2.0，详见 docs/T41_sql_having_groupby.md）；v1.6.0 起补齐 `ALTER TABLE ... MODIFY COLUMN` 列级类型变更、`CREATE DATABASE [IF NOT EXISTS]` / `DROP DATABASE` / `SHOW DATABASES` / `USE <db>` 多库目录与 `db.tbl` 前缀（详见 docs/T43_sql_database_alter_window）；窗口函数支持多列 `ORDER BY`（RANGE 帧仍限首列键域）；DDL/DML/查询能力以产品手册第 4 章 SQL 参考为准。
- **影响**：迁移自 MySQL/PostgreSQL 等系统的存量 SQL 可能需要改写；依赖完整窗口（如多列窗口排序列）与子查询等高级分析场景仍受限。
- **建议**：迁移前对照 SQL 参考做语法兼容性评审；复杂分析需求可先用 `EXPLAIN` 与子查询/视图组合验证等价改写。

### 1.2 MySQL wire protocol 兼容为子集（--mysql-port）

- **限制描述**：OpenXDB 默认使用自研二进制 TCP 协议（`pkg/server`）；自 v1.1.0 起可通过 `openxdb start --mysql-port <port>` 启用 **MySQL wire protocol 兼容监听**（`pkg/mysqlproto`），支持 MySQL 官方客户端、go-sql-driver/mysql、JDBC（MySQL 方言）驱动直接连接，覆盖常用查询/DML/预处理语句（含 COM_CHANGE_USER、COM_STMT_SEND_LONG_DATA、常用参数类型映射）等能力。协议兼容为常用能力子集；v1.6.0 起已支持 binlog 复制（COM_REGISTER_SLAVE / COM_BINLOG_DUMP 位点订阅）与 mysqldump 常用选项/整库导入（T44），更外围的 MySQL 生态工具链场景仍不受支持。
- **影响**：MySQL 生态监控、备份、管理工具链中依赖更新协议扩展（如 GTID、半同步原生协议、复制过滤）的场景仍不可直接复用；E5 基准采用 Python PEP 249 驱动等价实测，而非标准 sysbench/TPC-C 工具直连。
- **建议**：标准 MySQL 客户端与驱动场景使用 `openxdb start --mysql-port <port>` 直连；需要完整生态工具链的场景请使用驱动包装层或适配层，或使用内置 REPL（`openxdb repl`）与随仓库交付的 JDBC / Python 驱动。
- **v1.17.0 D7 更新**：补齐 MySQL 兼容缺口五项——① 握手版本串由硬编码 `8.0.36-OpenXDB` 改为与 v1.17.0 版本常量同源注入，mysql CLI 登录显示 `8.0.36-OpenXDB v1.17.0`（G1）；② `select version();` 返回 `8.0.36-OpenXDB v1.17.0`（G3）；③ `SHOW CREATE TABLE <t>` 基于存储元数据重建可执行 schema（列定义/类型/主键/二级索引/约束）（G4）；④ `SHOW INDEXES` 与 `SHOW INDEX` 等价（含大小写归一化）（G5）；⑤ 自研交互 CLI 提示符 `oxsql>` 生效（G2）。边界：握手/`version()` 版本号与版本常量同源、发版需同步更新；`oxsql>` 为自研 CLI 提示符，第三方 mysql CLI 的 `mysql>` 属客户端侧展示不受影响；五项均为读路径/客户端体验改动，不触碰事务、复制与 WAL 语义，全仓回归全绿。详见 [docs/experiments/D7_mysql_compat_validate.md](docs/experiments/D7_mysql_compat_validate.md)。

### 1.3 存储过程/触发器为受限子集

- **限制描述**：存储过程与触发器覆盖设计文档（T28）所定义的语法与语义：v1.2.0 补齐 IF/循环/CASE/递归深度上限，v1.6.0 补齐 DECLARE/DEFAULT、IF/ELSIF/ELSE、WHILE/LOOP/FOR/REPEAT、游标（OPEN/FETCH/CLOSE）与 OUT/INOUT 参数回写；v1.8.0 补齐触发器行级语义（FOR EACH ROW、NEW/OLD 行引用、体回滚递归策略）。仍不支持完整 SQL/PL 能力（如动态 SQL、异常处理语法等）。
- **影响**：复杂业务逻辑若依赖完整 PL/SQL 需要改写为应用层逻辑或其他等价实现。
- **建议**：业务逻辑优先在应用层实现；确需数据库内逻辑时，使用文档内已支持的 CREATE PROCEDURE / TRIGGER 语法子集。

### 1.4 行协议与类型精度

- **限制描述**：行协议单元格边界自 v1.5.0 起已消除 NULL/空串二义：NULL 渲染为 `\N` 字面量、空字符串渲染为空单元格；单元格内换行/回车/反斜杠由服务端转义为 `\n` / `\r` / `\\` 并在驱动端解码还原，文本参数通过 `E'...'` 转义字符串字面量发送（详见 docs/T19_client_drivers.md §2.2/§5）。DECIMAL 自 v1.12.0 起支持按列声明 scale 的可配置精度：内部值对象携带小数位（DS）字段，跨 scale 比较/舍入按对齐规则处理，行协议与 JSON 导出按声明 scale 保留小数位，不再强制 4 位规范化。
- **影响**：高精度 DECIMAL（>4 位小数）按列精度直接存取，无需整数单位换算；未声明 scale 时沿用默认 4 位；超列 scale 的导入值按拒绝策略处理（宽松模式跳过该行）。
- **建议**：金融等要求高精度的场景在 DDL 中显式声明 DECIMAL scale（如 `DECIMAL(18,6)`）；文本/NULL/空串语义边界保持不变。

## 2. 性能边界

### 2.1 协议/解析性能边界（v1.13.0 已落地 P1 并发处理；v1.14.0 落地 P0 只读并行 + P1 组提交定时窗口）

- **限制描述**：v1.3.0 已将全局会话锁消除为会话级细粒度锁，v1.5.0 起 Python/JDBC 驱动支持客户端 pipeline 批量流水线执行；v1.12.0 完成协议解析两项 P1 优化（① 全量对话边界 bufio 读覆层，mysqlConn 外层 bufio.Reader 减少逐字节读系统调用；② COM_QUERY 多语句在线切分执行，splitSQLStatements 安全跳过引号/注释内分号，多结果以 `serverStatusMoreResults=0x0008` 状态位串行返回，单语句行为完全兼容）；v1.13.0 完成服务端多连接并发处理：协议读侧 REPLSession 改造为 reader goroutine 预读 + 有界 channel + 主循环顺序执行，消除单连接逐命令串行读等待，每连接独立 goroutine 使多连接请求可并行进入执行层；会话级隔离保持（connSession 独立 SQL 会话），既有 server/mysqlproto 测试与单节点部署零回归。E5 脚本侧 bench_oltp.py / bench_tpcc.py 事务内语句启用客户端 pipeline 批量发送（整事务一次往返），并新增连接超时防护（--timeout 默认 120s、连接级异常自动重建并计数）。
- **影响**：E5 受控复测（干净数据重置 + 3 轮取中位数，8 连接）：OLTP read 0.40 / write 0.40 / read-write 0.40 TPS（errs=0 / reconnects=0）；TPC-C 5 仓 tpmC 78.00 / tpm_total 164.50（errs=0 / reconnects=0，较 v1.12.0 的 41.00 / 88.50 提升约 90%）。说明：OLTP read 侧仍受引擎聚合/范围查询执行开销制约（SUM/GROUP BY/ORDER BY 范围查询单条约 560ms、COUNT 全表约 580ms、点查约 3ms；SQL 执行层 v1.12→v1.13 零改动，属既有引擎特征），单连接 pipeline 与串行对照无差异证明瓶颈在服务端执行而非网络往返；v1.12.0 记录的 read 1.33 TPS 与当前受控复测不一致，以本次为准。TPC-C 高并发连接超时问题已修复：客户端默认超时提升至 120s 且连接级异常自动重建，3 轮复测 errs=0 / reconnects=0。
- **v1.14.0 更新**：落地 P0 只读 SELECT 并行（REPLSession 批内连续 autocommit 只读 SELECT 经 worker pool 并发执行、按序重组、批尾统一 Flush，开关 `ro_parallel` 默认 on 可回退）与 P1 WAL 组提交定时窗口（SyncUpTo 窗口内合并 fsync，配置 `group_commit_window_ms` 默认 0.5ms、0=关闭回退 v1.13.0 被动攒批）。E6 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read 0.43 / write 0.43 / read-write 0.43 TPS（较 v1.13.0 +7.5%），TPC-C tpmC 85.50（较 v1.13.0 +9.6%，3 轮原始 80.00 / 85.50 / 114.50，errs=0）。边界：read 模式仍受引擎聚合/范围查询执行开销制约（执行内核未并行，P3 写语句并行不在 v1.14.0 范围）；组提交窗口对延迟敏感单写事务附加 ≤0.5ms 同步等待（可配 0 关闭）；tpmC 轮 3 出现 114.50 高值，波动来源待查（候选 GC / RocksDB compaction / Python GIL 释放时序）。详见 [docs/experiments/E6_v1140_ro_parallel_wal_groupcommit.md](docs/experiments/E6_v1140_ro_parallel_wal_groupcommit.md)。
- **v1.15.0 更新**：落地 SQL 执行层四项优化——W1 Scan 快路径（txn 无未提交写时快照直透，跳过全量物化+重建排序）、W2 COUNT(*)/COUNT(1) 快速路径（无 WHERE/GROUP 等时走存储层 RangeCounter 逐 region 计数求和）、W3 SELECT 列投影下推（按需解码列，JOIN/子查询/星号保守全列回退）、W4 ORDER BY 索引顺序扫描免排序 + DISTINCT 相邻去重免排序（单列二级索引/主键 + 运行时 O(n) 已序验证）。E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read 0.38 / write 0.40 / read-write 0.40 TPS（errs=0，与 v1.14.0 持平、无回归），TPC-C tpmC 81.50（3 轮原始 63.50 / 81.50 / 102.00，errs=0，与 v1.14.0 的 85.50 持平）。单语句专项压测（PEP 249 驱动，N=7-9 取中位数，sbtest 10,000 行）：COUNT(*) 全表 **580ms → 5.18ms（约 103-112 倍）**、全表扫描多列 613.51ms（基线 780-900ms，约 1.27-1.47 倍）、全表扫描单列 532.33ms（约 1.47-1.69 倍）、点查 2.09-3.25ms（基线 ~3ms，持平）、范围聚合 525.36ms（基线 ~560ms，约 1.07 倍）；ORDER BY 主键 548.51ms 与 DISTINCT 619.88ms 均与无排序/无去重投影持平（免排序生效），ORDER BY 二级索引 764.39ms 为索引扫描+批量回表开销但免去全量 sort。边界：OLTP 事务级吞吐仍受全局会话锁与单机端侧网络往返制约（个位数 TPS 量级）；ORDER BY 二级索引快速路径在 10k 行量级显示索引+回表开销高于排序收益，仅在更大数据量/索引覆盖场景收益为正；免排序/DISTINCT 相邻去重仅限单列裸列场景，多列/表达式 ORDER BY 仍走原路径。详见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。
- **v1.17.0 更新**：落地显式事务写路径优化三项——D1.1 UPDATE 快照刷新锁外化（`snapshot_lock_external`，显式事务 UPDATE 锁内快照刷新移到锁外，对齐 C2 Begin 锁外化同型改造）、D1.2 autocommit 写 pipeline 深化（`write_pipeline` 默认 off 可热切换，写语句接入连接内 pipeline，减少端侧逐条往返）、D1.3 WAL 组提交主动定时窗口（`group_commit_active_window` 默认 off 回退 v1.16.0 C1 单等待者免窗口，写密集下主动定时刷新合并 fsync，降低 fsync 次数与写放大；不影响 WAL 重放 / 崩溃恢复 / 复制 2PC 顺序）。E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read **11.85** / write **12.03** / read-write **12.40** TPS（QPS 165.90 / 48.13 / 223.20，errs=0 / reconnects=0），较 v1.16.0（read 10.37 / write 8.33 / read-write 8.15 TPS，QPS 145.13 / 33.33 / 146.70）**write QPS +44%、read-write TPS +52%、read 无回归 +14%**；TPC-C 5 仓 tpmC **776.00** / tpm_total 1720.50（3 轮原始 810.00 / 679.50 / 776.00，errs=0，较 v1.16.0 的 254.00 / 581.00 提升约 3.0-3.1 倍）。**达成 D1 验收目标：write QPS 中位 ≥40（48.13）、TPS write ≥9（12.03）、tpmC ≥300（776.00），其余指标无回归、errs=0 / reconnects=0**。边界：write 模式已脱离吞吐下限（单轮最低 QPS 41.00 仍高于 v1.16.0 中位 33.33）；TPS/QPS 仍受全局会话锁与单机端侧往返制约，数值为单机端侧功能基准；所有优化均有独立 feature flag 可灰度回退。详见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。
- - **v1.17.0 D6 更新**：落地备库常驻读服务（`openxdb replica --mysql-port <port>`，D6.1）与只读强制（D6.2，写语句按 `isWriteStmt` 白名单拒绝）。并发模型：复制应用以 `f.mu` 串行写存储，读路径经 `ReadBarrier` 以 `AppliedLSN` 为界对齐后快照读，存储原子批次 + 快照隔离保证无半批次可见；读侧仅短临界区等待当前批次，不阻塞后续复制、无死锁。边界：备库读可见性以查询开始时的 `AppliedLSN` 为界（随后到达的复制批次不影响当前查询）；复制应用仍为单 goroutine 串行（吞吐上限与主端 binlog dump 速率相关）；备库写入被拒绝，仅支持读。专项回归 6 项验收全通过（常驻查询 / 写拒 / 滞后窗口 / 崩溃恢复二次重启 / 全仓 build+test 全绿 / 主端无回归），详见 [docs/experiments/D6_replica_resident_read.md](docs/experiments/D6_replica_resident_read.md)。
**v1.17.0 D2 回退记录**：D2 会话锁与调度精细化（D2.1 session lock granularity split + D2.2 read-parallel in write txn）因并发伸缩验收未达标（D2.3 复测 16 连接 read TPS 10.80 较 8 连接 11.85 为 -8.9%、read-write 10.28 较基线 12.40 为 -17.1%，未达"16 连接较 8 连接提升 ≥20%"验收目标）已整体回退，恢复 D1 基线行为（等价 cdd62e7）；D2.3 并发伸缩 bench 数据与实验文档（e1ba367）保留作为记录。回退后 E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read / write / read-write **12.75 / 12.63 / 10.97 TPS**（QPS 178.50 / 50.53 / 197.40，errs=0 / reconnects=0），TPC-C 5 仓 tpmC **709.00** / tpm_total 1568.00，**D1 验收指标复测仍达标（write QPS 中位 ≥40 为 50.53、TPS write ≥9 为 12.63、tpmC ≥300 为 709.00，read / read-write 无回归，errs=0 / reconnects=0）**；全局会话锁制约仍为单机端侧量级。详见 [docs/experiments/D2_3_concurrency_scaling.md](docs/experiments/D2_3_concurrency_scaling.md)。
- **v1.17.0 D5 发版最终复测**：D3 可观测性与诊断闭环（EXPLAIN 节点级耗时/行数估算 + slowlog 阈值区间 TOP N 汇总）与 D4 并发规模与稳定性验证（8 连接写密集 30min 长稳 errs=0、goroutines 恒 2、wal.log 受控 45.6MB、check COUNT 严格等价 2355864；bench 工具参数化）落地。D5 发版前 E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read / write / read-write **14.15 / 13.40 / 12.60 TPS**（QPS 198.10 / 53.60 / 226.80，errs=0 / reconnects=0），较 v1.16.0（10.37 / 8.33 / 8.15 TPS，QPS 145.13 / 33.33 / 146.70）**write QPS +61%、read-write TPS +55%、read 无回归 +36%**；TPC-C 5 仓 tpmC **774.50** / tpm_total 1746.50（较 v1.16.0 的 254.00 / 581.00 提升约 3.0 倍）。**达成 v1.17.0 验收目标：TPS read ≥11（14.15）/ write ≥9（13.40）/ rw ≥8.5（12.60），QPS read ≥145（198.10）/ write ≥40（53.60）/ rw ≥146（226.80），tpmC ≥300（774.50），errs=0 / reconnects=0**。边界：write 模式已脱离吞吐下限（QPS 中位 53.60，单轮最低 52.87 仍高于 v1.16.0 中位 33.33）；D2 回退后全局会话锁制约仍为单机端侧量级；所有优化均有独立 feature flag 可灰度回退。详见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。
- **v1.16.0 更新**：落地 SQL 执行内核并发模型六项优化——C1 WAL 组提交增强（单等待者免窗口 + 批量刷盘，`group_commit_window_ms` 可回退）、C2 Begin 快照创建锁外化（锁内创建改为锁外，ID/序号仍单调）、C3 ro_parallel 下沉 executor + 显式事务只读并行（`ro_parallel` 默认 on 可回退）、C4 提交应用流水线化/组提交、C5 cgo 扫描拷贝优化（unsafe 边界专项）、C6 连接内语句 pipeline / 快照复用（`snapshot_share`、`pipeline_flush` 独立 flag 默认开）；另落地 **WAL 自动 checkpoint/truncate**（`wal_auto_checkpoint` 默认 on、`wal_checkpoint_interval_ms` 默认 60000、`wal_checkpoint_max_mb` 默认 64；安全位点 safe=min(synced,applied)，截断前 Flush 落 SST，只删已 fsync 且已应用记录；长期运行 wal.log 大小受控、崩溃恢复结果与截断前严格等价，详见 docs/experiments/WAL_auto_checkpoint_truncate.md）。C7 E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read **10.37** / write **8.33** / read-write **8.15** TPS（QPS 145.13 / 33.33 / 146.70，errs=0 / reconnects=0），较 v1.15.0（read 0.38 / write 0.40 / read-write 0.40 TPS，QPS 5.37 / 1.53 / 6.30）TPS 提升约 20-27 倍、QPS 提升约 22-27 倍，**达成 v1.16.0 验收目标（TPS≥1.5、QPS≥30）**；TPC-C 5 仓 tpmC **254.00** / tpm_total 581.00（3 轮原始 224.00 / 301.50 / 254.00，errs=0，较 v1.15.0 的 81.50 / 170.50 提升约 3.1-3.4 倍）。边界：write 模式为吞吐下限（QPS 中位 33.33，单轮波动至 29.13 仍满足目标）；所有优化均有独立 feature flag 可灰度回退；TPS/QPS 受全局会话锁与单机端侧往返制约，仍为单机端侧量级。详见 [docs/experiments/C7_e5_controlled_retest.md](docs/experiments/C7_e5_controlled_retest.md)。
- **建议**：将上述数值视为**单机端侧功能基准**，非生产级吞吐承诺；生产负载请先按真实数据量/并发做压测，评估是否需要单机优化或集群扩展。

### 2.2 备份/恢复与大批量导入耗时

- **限制描述**：逻辑备份（BACKUP）与 mysqldump 导入为文本/逻辑路径，大表耗时与数据量线性相关；导入为文本规则解析，覆盖常见 dump 语法子集。
- **影响**：超大库备份/导入窗口较长；未知类型在严格模式报错、宽松模式跳过该表；单元格解析失败在宽松模式丢弃该行。
- **建议**：定期备份并验证恢复演练；导入使用 `IMPORT BATCH n` 控制批次与 `IGNORE ERRORS` 策略；关注日志中的跳过/失败计数。

## 3. 单机 → 分布式边界

### 3.1 跨节点写依赖 2PC

- **限制描述**：跨节点写（涉及多个 region/节点）必须走两阶段提交（2PC）协议帧；单节点部署不会进入 2PC 路径（零触发、零回归）。
- **影响**：集群模式下跨节点写有 2PC 的 PREPARE/COMMIT 往返开销；任何 PREPARE 失败或本地提交失败会整体 ABORT 语句。
- **建议**：合理设计 region 布局减少跨节点写事务；评估 2PC 延迟是否符合业务 SLA。

### 3.2 单机模式集群语句受限

- **限制描述**：单机（未启用集群）部署下，集群语句如 `BALANCE` 明确报 `cluster not enabled`；region 分配、节点注册等仅在集群模式生效。
- **影响**：单机用户误用集群语句会收到明确报错；不会静默产生无效配置。
- **建议**：单机部署按单机文档使用；需要集群能力时用 `--cluster-addr` 启动并走 ADD NODE / ASSIGN REGION 流程。

### 3.3 复制模式与一致性语义

- **限制描述**：复制模式支持 async / semi-sync / sync（热配置切换）。semi-sync 在 ACK 超时/无从节点时降级为异步（记录 DegradedCount/LSN）；sync 模式无法满足 ACK（无在线从节点或超时）时拒绝伪同步（ErrNoSlave / ErrAckTimeout）。
- **影响**：全同步写入在从节点不可用时会失败，保证不产生"伪同步"假象；半同步降级期间一致性窗口扩大。
- **建议**：按 RPO 要求选择模式；监控 DegradedCount 与复制 LSN 滞后，及时恢复从节点。

### 3.4 HA 脑裂防护 quorum 开关（默认关闭）

- **限制描述**：HA 脑裂防护含 `ha.epochEnabled`（epoch fencing，v1.10.0 起默认开启）与 `ha.quorumEnabled`（多数派仲裁）两个开关。quorum 保持默认关闭：开启后升主需多数派节点确认，单节点/两节点部署无法满足多数派时将拒绝升主。
- **影响**：v1.10.0 单机/测试框架评估结论为直接全量开启会破坏单节点自动升主与两节点主从场景的主节点恢复语义（详见 docs/HA_BRAIN_SPLIT_DESIGN.md §5.5）；保持默认关闭不影响 epoch fencing 的防脑裂效果。
- **建议**：三节点及以上真实组网灰度验证 quorum 仲裁行为后再评估全量开启；单机/两节点部署维持默认关闭。

## 4. 文档边界

- **限制描述**：docs/ 下设计文档（T1–T42、M 系列、E 系列）为中文开发记录，供实现追溯与深入阅读；产品手册提供中英文 11 章节面向用户文档。
- **影响**：英文用户深度查阅设计细节需依赖英文产品手册与代码注释；部分早期文档格式为中文技术笔记。
- **建议**：用户场景以 product_manual_en.md / product_manual_zh.md 为准；实现细节以 docs/ 设计文档与源码为准；文档站点（mkdocs）建立后统一导航。

## 5. 其他边界

- **鉴权兼容模式**：无 AUTH 握手时连接以默认 root 用户运行；旧客户端在 `AllowLegacy` 开启时回退受限 guest 模式，严格模式拒绝。建议生产环境配置认证并保持严格模式。
- **TLS 默认策略**：默认明文监听；TLS 经 `SetTLS` / 自签或 CA 证书装配后启用（T36 提供 gen_cert 生成工具），需明文/TLS 共存时配置 `--tls-allow-legacy`（AutoListener 同端口自动识别）；v1.11.0 起支持证书热轮换（`ReloadTLS`，AllowLegacy 模式运行中即时生效）。生产环境建议使用受信任 CA 证书并定期轮换。
- **备份格式**：BACKUP 产物为单文件逻辑备份（version + CRC32），跨版本恢复请先确认 DataFormatVersion 兼容（`openxdb version` 查看）。
- **告警通知通道**：告警引擎内置 logx 通知与 v1.11.0 新增的规则级 webhook 回调（触发/恢复异步 POST JSON 载荷）；钉钉/企业微信等原生渠道未接入，可经 webhook 网关转发。
- **测试规模**：全仓回归 694+ 测试覆盖主要路径，但非商用级验证范围；E5 真实用户验证覆盖 30+ 商业场景检查，生产使用仍需独立验收。

---

*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

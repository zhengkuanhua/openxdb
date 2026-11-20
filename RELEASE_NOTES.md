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

# OpenXDB v1.19.0 Release Notes

发布日期：2026-11-20

## 版本亮点

- **存储过程能力补齐（D2，P0）**：异常处理与动态 SQL 同时落地——`DECLARE {CONTINUE|EXIT} HANDLER FOR <cond>`（`SQLSTATE` / 数字错误码 / `SQLEXCEPTION` / `SQLWARNING` / `NOT FOUND`，按"最近声明优先"匹配、随块退出自动出栈）、`SIGNAL` / `RESIGNAL`（含 `SET MESSAGE_TEXT|MYSQL_ERRNO|SQLSTATE`）、`PREPARE <stmt> FROM '<文本>'|@var` / `EXECUTE <stmt> [USING ...]` / `DEALLOCATE PREPARE`（`?` 按序绑定、过程体白名单校验、不跨 CALL 泄漏）；同时支持嵌套 `BEGIN…END` 独立作用域、`DECLARE <name> CONDITION FOR ...` 命名条件与 `GET [CURRENT] DIAGNOSTICS`（`ROW_COUNT` / `CONDITION <n>` 的 `RETURNED_SQLSTATE` / `MYSQL_ERRNO` / `MESSAGE_TEXT`）。
- **触发器体升级为复合块（D3，P0）**：`CREATE TRIGGER ... FOR EACH ROW BEGIN <stmt>; ... END` 复用存储过程解析与执行器，自动获得块级 `DECLARE`、`IF` / `LOOP` / `WHILE` / `REPEAT` / `CASE` 与 `HANDLER`；`BEFORE INSERT/UPDATE` 支持行级 `SET NEW.<col> = <expr>` 回写（该行全部 BEFORE 触发器执行完毕后按表结构统一写回，`OLD.<col>` 只读）；`FOLLOWS` / `PRECEDES` 同表同事件命中集做稳定拓扑排序，无依赖保持创建顺序、成环按创建顺序兜底。
- **事务保存点（D4，P1）**：`SAVEPOINT <name>` / `ROLLBACK [WORK] TO [SAVEPOINT] <name>` / `RELEASE SAVEPOINT <name>` 全语义落地，ORM 嵌套事务可直接依赖。
- **递归 CTE（D5，P1）**：`WITH RECURSIVE <name> [(<col>, ...)] AS (<anchor> UNION [ALL] <recursive>) [, ...] <SELECT>`，支持与非递归 CTE 混用、作 `FROM` / `JOIN` 右端；深度上限由配置项 `recursive_cte_max_depth`（默认 1000，对齐 MySQL `cte_max_recursion_depth`）控制，超限报错不静默截断。附带修正 `JOIN ... ON <cond>` 紧跟外层 `)` 时被吞括号的解析缺陷。
- **SQL JSON 函数族（D6，P1）**：新增 `JSON_VALID` / `JSON_EXTRACT` / `JSON_UNQUOTE` / `JSON_TYPE` / `JSON_LENGTH` / `JSON_OBJECT` / `JSON_ARRAY` / `JSON_SET` 共 8 个函数，自带保序文档模型（键序不被打乱、数字按 `UseNumber` 保真），输出对齐 MySQL 紧凑风格；可在投影列、`WHERE`、`UPDATE SET`、`CALL` 参数等任意表达式位置使用。
- **远端区间聚合 + 远端 Scan 流式协议（D1，P1，`remote_range_agg` 默认 off）**：跨 region 多区间读由"逐区间独立 `QUERY_REQ` 往返"合并为"按归属节点聚合的单次 `QUERY_MULTI` 往返"，服务端按区间复用既有扫描路径后以 512 行 / 4MB 预算分块流式回传；结果与逐区间路径逐行等价，开关关闭时完全走 v1.18.0 旧路径、不发新帧。
- **可更新视图与 `CHECK OPTION`（A1 / A2）**：单表裸列视图的 `INSERT` / `UPDATE` / `DELETE` 由视图列映射重写到基表并下沉至 `execStmt`，存储过程体、动态 SQL 与触发器体内的视图写与顶层同口径；`WITH [CASCADED|LOCAL] CHECK OPTION` 对新行求值，越界报 `CHECK OPTION failed '<view>'` 并整条语句回滚。
- **发版口径收口（D7）**：版本常量单点化到零依赖包 `pkg/version`，`cmd/openxdb` 版本输出、MySQL 握手版本串与 `select version()` 三者同源；握手串此前停留在 `v1.17.0` 的历史漂移一并修正为 `8.0.36-OpenXDB v1.19.0`。

## 新增能力（相对 v1.18.0）

- **D1 远端区间聚合**：新增帧 `msgQueryMultiReq` / `msgQueryMultiResp`（编号 66/67，避开既有 REGION_PUSH 20-21、2PC 22-27、TSO 28-29、DDL 30-31、注册表 32-33、备份恢复 40-45、epoch 50、全量对齐 60-61、TSO HA 70-71）；客户端新增 `Client.ScanMulti` 与 `Manager.ScanMultiRemote`；`pkg/sql` 的 `scanMergedCluster` 在开关开启时分流至 `scanMergedClusterAgg`（按 `sharding.DecodeRegionKey` 分类，本地区间一次合并扫描、同归属节点远端区间分组单次取回，结果按内层键全局稳定排序），保留 M6 读 failover、P3 READY 前置与 P2-13 Serializable 远端读集登记语义；`remote_range_agg` 四级透传（配置默认 off → Engine → Executor 原子读写 → 执行器分流），关闭零回归可灰度回退。
- **D2 存储过程异常处理**：条件匹配覆盖 `SQLSTATE '<值>'`、数字错误码、`SQLEXCEPTION`（非 01/02 类）、`SQLWARNING`（01 类）、`NOT FOUND`（02 类）；游标取尽（`02000`）纳入 `NOT FOUND`；无 SQLSTATE 的过程错误统一归入 `HY000`；处理器执行期间禁止 `RESIGNAL` 自捕获，`RESIGNAL` 无参即重抛当前错误。
- **D2 存储过程动态 SQL**：`?` 占位符按出现顺序以实参字面量绑定（个数不符报 `argument count mismatch`），绑定结果走标准 `Parse` + `execStmt`，语句类型受过程体白名单校验（防止经动态 SQL 绕过过程体限制）；预处理语句表为单次 CALL 调用级。
- **D2 嵌套块与诊断**：嵌套 `BEGIN…END` 独立作用域（块级快照整体出栈，同名变量/游标/处理器不跨块泄漏）；`GET [CURRENT] DIAGNOSTICS` 可取 `ROW_COUNT` 与 `CONDITION <n>` 的 `RETURNED_SQLSTATE` / `MYSQL_ERRNO` / `MESSAGE_TEXT`；`HANDLER` 体支持复合块，变量代入普通 SQL 时单引号按 SQL 标准转义。
- **D3 触发器体复合块与执行顺序**：`BEGIN…END` 触发器体复用 `parseProcedureBody` + `procRunner`；`BEFORE INSERT/UPDATE` 的 `SET NEW.<col>` 行上下文累积后统一回写；`FOLLOWS` / `PRECEDES` 创建期校验互斥声明、自引用与依赖存在性。
- **D4 事务保存点**：`pkg/txn` 新增 `SavepointTxn` 接口与 `ErrSavepointNotFound`；保存点回滚按 2PC 写集前缀截断，跨 region 保存点回滚边界见 `KNOWN_LIMITATIONS` §1.5。
- **D5 递归 CTE**：anchor + 递归项、`UNION` / `UNION ALL`、列名列表、与非递归 CTE 混用；深度超限即报错，不静默截断。
- **D6 JSON 函数族**：仅使用既有 `Value` 的 TEXT / INT / NULL 类型，不引入新 Kind、协议层零改动；错误口径明确（非法文本统一报 `invalid JSON text`）。
- **A1 可更新视图**：`ViewMeta` 新增 `Updatable` / `NotUpdatableReason` / `BaseTable` / `ColumnMap` / `ColumnOrder`（均 `omitempty`，旧视图目录反序列化后按不可更新处理）；`INSERT` 无列清单时按视图列顺序映射、视图未暴露基表列走类型默认值，`UPDATE` 的 `SET` 列必须落在视图列集合内，`WHERE` 与 `ON DUPLICATE KEY UPDATE` 中的视图列引用一并重写，未暴露列报 `view <name> has no column: <col>`；写目标改为基表后权限按基表判定。
- **A2 `WITH CHECK OPTION`**：建视图时把定义 `WHERE` 规范化为"基表裸列名"条件副本随视图目录持久化，`INSERT` / `UPDATE` 对新行求值、失败整条语句回滚；`DELETE` 不受限；`ON DUPLICATE KEY UPDATE` 更新分支与 `REPLACE` 冲突替换分支不施加校验；本版本 `LOCAL` 与 `CASCADED` 语义等价（可更新性判定要求基表为真实表，不存在下层视图级联）。
- **D7 发版收尾**：新增零依赖包 `pkg/version`（`Version` / `MySQLServerPrefix` / `ServerVersion`），`cmd/openxdb/main.go` 版本常量、`pkg/mysqlproto` 握手版本串与 `pkg/sql` 的 `select version()` 全部取自同一常量（消除两处独立常量、修正握手串停留 `v1.17.0` 的漂移）；`RELEASE_NOTES` / `README` / `KNOWN_LIMITATIONS` / `docs/index.md` 文档口径同步；`go build ./...`、`go vet ./...`、`gofmt -l`、`go test ./...` 全绿。

## 性能

本版本以 SQL 兼容能力补齐为主，未改动存储引擎、WAL、事务提交与复制主路径，未重新执行 E5 标准基准复测；性能基线沿用 v1.18.0 口径（Windows 平台、8 连接、每轮 prepare 重置、3 轮取中位数）：OLTP read / write / read-write **14.15 / 13.40 / 12.60 TPS**（QPS 198.10 / 53.60 / 226.80），TPC-C 5 仓 tpmC **774.50** / tpm_total 1746.50，errs=0 / reconnects=0。Linux（RHEL 9.7 / 2 vCPU）同口径结果为 8.17 / 8.13 / 8.10 TPS、tpmC 749.50，仅作功能与稳定性验证、不作跨平台性能对标（详见 [docs/experiments/D5_linux_e5_controlled_retest.md](docs/experiments/D5_linux_e5_controlled_retest.md)）。

D1 远端区间聚合的收益为往返次数下降（同归属节点多区间由"区间数"次 `QUERY_REQ` 降为"节点数"次 `QUERY_MULTI`），该项默认关闭、未纳入标准基准口径。

## 兼容性说明

- **数据目录兼容**：v1.19.0 保持既有数据文件格式版本（`pkg/db.DataFormatVersion`），已初始化的 v1.0.0 至 v1.18.0 数据目录可直接打开，无需迁移重建。
- **协议兼容**：自研二进制协议（pkg/server，非 MySQL wire protocol；`--mysql-port` 启用兼容监听）。新增帧 66/67 仅在 `remote_range_agg` 开启时使用，关闭时与 v1.18.0 完全一致；握手版本串随版本常量升级为 `8.0.36-OpenXDB v1.19.0`，`select version()` 同源联动。
- **配置兼容**：新增配置项 `recursive_cte_max_depth`（默认 1000）与 `remote_range_agg`（默认 off），均为新增项、不改变既有默认值；`ha.quorumEnabled` / `ha.epochEnabled` 默认值不变（quorum off、epoch on）；既有命令行参数与 SET PERSIST 配置项保持向后兼容。
- **SQL 语义增量**：新增过程 `HANDLER` / `SIGNAL` / 动态 SQL / 嵌套块 / `GET DIAGNOSTICS`、触发器体复合块与 `SET NEW`、`FOLLOWS` / `PRECEDES`、`SAVEPOINT` 系列、`WITH RECURSIVE`、8 个 JSON 函数、可更新视图与 `CHECK OPTION`，均为新语法/新能力，不改变既有语句语义。
- **无破坏性变更**：本次发布未引入数据文件格式变更或协议破坏性变更。

## 已知问题

详见 [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md)。主要关注：

- **HA quorum 仍维持默认关闭**：集群轨尚无一致性协议（无 raft 式日志复制与选举），写提交为归属节点本地提交 + 副本异步推送补齐，存在"单副本可写窗口"与 RPO>0；解除默认关闭的前置为写路径强制路由 + 2PC 参与 + 副本间持续复制/共识提交，已列入下一轮候选。
- **HA 灰度报告未闭环项**：`docs/D4_5_ha_quorum_gray_report.md` §8.2 三项真机欠账的状态与结论以该报告原文为准，本次发布不改写其表述。
- **跨 region 远端区间聚合默认关闭**：`remote_range_agg` 为灰度开关，开启前须通过逐行等价验证；关闭即走 v1.18.0 逐区间路径。
- **视图能力边界**：可更新视图仅支持单表裸列定义（聚合、`JOIN`、子查询、视图套视图等定义不可更新，报错带具体条款）；列映射在建视图期固化，基表 `ALTER TABLE` 后需重建视图；`LOCAL` 与 `CASCADED` 本版本语义等价。
- **通用边界**：SQL 为子集（暂无完整 MySQL 语法覆盖，自研二进制协议 + 可选 MySQL wire 兼容监听）；性能为单机端侧基准（8 连接），非生产级吞吐承诺。

## 升级路径

1. 备份：`openxdb backup` 或脚本备份数据目录（参考 scripts/rolling_upgrade.ps1）。
2. 预检：`openxdb preflight --data-dir <dir>` 确认版本与数据格式匹配。
3. 优雅停机：`openxdb shutdown --graceful` 或 SIGTERM。
4. 替换二进制为 v1.19.0 并启动（数据目录不变）。
5. 验证：`openxdb doctor`、`openxdb stats`、`openxdb version` 确认 v1.19.0；集群拓扑下抽查 `SHOW REGION ROUTES`、跨节点点查与聚合，以及业务读写与备份恢复。

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

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

版本：v1.10.0（2026-10-25）

本文档列出 OpenXDB v1.10.0 的已知限制与边界。每条目给出限制描述、影响与建议，帮助用户在选型、部署与使用时做出合理判断。详细功能范围请以 [docs/product_manual_zh.md](docs/product_manual_zh.md)（第 4 章 SQL 参考）与设计文档 docs/ 为准。

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

### 1.3 存储过程/触发器为受限子集

- **限制描述**：存储过程与触发器覆盖设计文档（T28）所定义的语法与语义：v1.2.0 补齐 IF/循环/CASE/递归深度上限，v1.6.0 补齐 DECLARE/DEFAULT、IF/ELSIF/ELSE、WHILE/LOOP/FOR/REPEAT、游标（OPEN/FETCH/CLOSE）与 OUT/INOUT 参数回写；v1.8.0 补齐触发器行级语义（FOR EACH ROW、NEW/OLD 行引用、体回滚递归策略）。仍不支持完整 SQL/PL 能力（如动态 SQL、异常处理语法等）。
- **影响**：复杂业务逻辑若依赖完整 PL/SQL 需要改写为应用层逻辑或其他等价实现。
- **建议**：业务逻辑优先在应用层实现；确需数据库内逻辑时，使用文档内已支持的 CREATE PROCEDURE / TRIGGER 语法子集。

### 1.4 行协议与类型精度

- **限制描述**：行协议单元格边界自 v1.5.0 起已消除 NULL/空串二义：NULL 渲染为 `\N` 字面量、空字符串渲染为空单元格；单元格内换行/回车/反斜杠由服务端转义为 `\n` / `\r` / `\\` 并在驱动端解码还原，文本参数通过 `E'...'` 转义字符串字面量发送（详见 docs/T19_client_drivers.md §2.2/§5）。DECIMAL 内部规范化为固定 4 位小数，JSON 导出中经 `strconv.ParseFloat` 近似（或保留字符串）。
- **影响**：高精度 DECIMAL（>4 位小数）场景存在精度损失；文本/NULL/空串语义损失已消除。
- **建议**：金融等要求高精度的场景将金额换算为整数单位（如分）存储，或接受 4 位小数规范化语义。

## 2. 性能边界

### 2.1 协议/解析性能边界（会话锁已细化、客户端已流水线化）

- **限制描述**：v1.3.0 已将全局会话锁消除为会话级细粒度锁（并发度显著提升），v1.5.0 起 Python/JDBC 驱动支持客户端 pipeline 批量流水线执行（消除逐语句往返）；服务端协议解析仍为逐条串行处理，高并发写入/混合负载的协议层吞吐优化排入后续版本（v1.12.0）。
- **影响**：高并发写入/混合负载场景吞吐受限。E5 实测（Python 驱动等价，8 线程）：OLTP read 1.73 TPS（QPS 24.27）/ write 0.93 TPS（QPS 3.73）/ read-write 1.32 TPS（QPS 23.70）；TPC-C 5 仓 tpmC 13.50 / TPM 32.50，0 错误。
- **建议**：将上述数值视为**单机端侧功能基准**，非生产级吞吐承诺；生产负载请先按真实数据量/并发做压测，评估是否需要单机优化或集群扩展；后续版本计划优化会话锁粒度与协议批量能力。

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

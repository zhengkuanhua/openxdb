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

版本：v1.0.0（2026-10-16）

本文档列出 OpenXDB v1.0.0 的已知限制与边界。每条目给出限制描述、影响与建议，帮助用户在选型、部署与使用时做出合理判断。详细功能范围请以 [docs/product_manual_zh.md](docs/product_manual_zh.md)（第 4 章 SQL 参考）与设计文档 docs/ 为准。

---

## 1. SQL 能力边界

### 1.1 SQL 为子集（非完整标准）

- **限制描述**：OpenXDB 实现的是 SQL 子集，并非完整 SQL 标准。当前不支持 `HAVING` 子句；DDL/DML/查询能力以产品手册第 4 章 SQL 参考为准。更多 DDL 类型（如 `ALTER` 的列级扩展、`CREATE DATABASE` 等）未完整支持。
- **影响**：迁移自 MySQL/PostgreSQL 等系统的存量 SQL 可能需要改写；依赖复杂分析查询（如 `HAVING` 过滤分组）的场景不可直接运行。
- **建议**：迁移前对照 SQL 参考做语法兼容性评审；复杂分析需求可先用 `EXPLAIN` 与子查询/视图组合验证等价改写；后续版本将按 T39 SQL 能力差距评估逐步补齐。

### 1.2 无 MySQL wire protocol 兼容（自研二进制协议）

- **限制描述**：OpenXDB 使用自研二进制 TCP 协议（`pkg/server`），**不是** MySQL wire protocol。MySQL 官方客户端、标准 sysbench、官方 mysqldump 等 MySQL 生态工具无法直接连接。
- **影响**：MySQL 生态监控、备份、管理工具链不可直接复用；E5 基准采用 Python PEP 249 驱动等价实测，而非标准 sysbench/TPC-C 工具直连。
- **建议**：使用内置 REPL（`openxdb repl`）或随仓库交付的 JDBC / Python 驱动（支持 `?` 占位符、TLS 参数）；需要标准工具的场景请使用驱动包装层或适配层。

### 1.3 存储过程/触发器为受限子集

- **限制描述**：存储过程与触发器仅覆盖设计文档（T28）所定义的语法与语义，不支持完整 SQL/PL 能力（如完整游标、动态 SQL、异常处理语法等）。
- **影响**：复杂业务逻辑若依赖完整 PL/SQL 需要改写为应用层逻辑或其他等价实现。
- **建议**：业务逻辑优先在应用层实现；确需数据库内逻辑时，使用文档内已支持的 CREATE PROCEDURE / TRIGGER 语法子集。

### 1.4 行协议与类型精度

- **限制描述**：行协议不支持单元格内换行（多行文本参数会被驱动拒绝）；空单元格同时表示 NULL 与空字符串；DECIMAL 内部规范化为固定 4 位小数，JSON 导出中经 `strconv.ParseFloat` 近似（或保留字符串）。
- **影响**：极端文本（含换行）与高精度 DECIMAL（>4 位小数）场景存在精度/语义损失。
- **建议**：文本字段避免换行；金融等要求高精度的场景将金额换算为整数单位（如分）存储，或接受 4 位小数规范化语义。

## 2. 性能边界

### 2.1 全局会话锁串行 + 逐语句往返

- **限制描述**：当前会话层串行化全局锁是主要吞吐瓶颈；客户端逐语句往返（无 pipeline/批处理协议）进一步限制高并发吞吐。
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

## 4. 文档边界

- **限制描述**：docs/ 下设计文档（T1–T42、M 系列、E 系列）为中文开发记录，供实现追溯与深入阅读；产品手册提供中英文 11 章节面向用户文档。
- **影响**：英文用户深度查阅设计细节需依赖英文产品手册与代码注释；部分早期文档格式为中文技术笔记。
- **建议**：用户场景以 product_manual_en.md / product_manual_zh.md 为准；实现细节以 docs/ 设计文档与源码为准；文档站点（mkdocs）建立后统一导航。

## 5. 其他边界

- **鉴权兼容模式**：无 AUTH 握手时连接以默认 root 用户运行；旧客户端在 `AllowLegacy` 开启时回退受限 guest 模式，严格模式拒绝。建议生产环境配置认证并保持严格模式。
- **TLS 默认策略**：v1.0.0 默认 TLS-only 监听（明文客户端拒绝）；需明文/TLS 共存时配置 `--tls-allow-legacy` / `SET PERSIST tls_allow_legacy=on`。生产环境建议使用受信任 CA 证书。
- **备份格式**：BACKUP 产物为单文件逻辑备份（version + CRC32），跨版本恢复请先确认 DataFormatVersion 兼容（`openxdb version` 查看）。
- **测试规模**：全仓回归 694+ 测试覆盖主要路径，但非商用级验证范围；E5 真实用户验证覆盖 30+ 商业场景检查，生产使用仍需独立验收。

---

*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

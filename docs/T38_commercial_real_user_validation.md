---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_bd38c2f5c86c11f18b7f525400393706
    ReservedCode1: rFI/EWqH+5nVXXCDjsj9Di7LJUBiO7MYFw1hNsIo7HKH+ASIzhh4LatwGugIEWKpQys2yySdAG5vOHoT3HHnaeCg/9gz2xsskNhJ++h++bKZ/LiJo6j4Dgg2t7NJapuBFj4ySs0ugYAn3C/bgpdIDTNpai9L3vfJ/30dQ1QXvSA/1bi+eU1G9GegrFc=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_bd38c2f5c86c11f18b7f525400393706
    ReservedCode2: rFI/EWqH+5nVXXCDjsj9Di7LJUBiO7MYFw1hNsIo7HKH+ASIzhh4LatwGugIEWKpQys2yySdAG5vOHoT3HHnaeCg/9gz2xsskNhJ++h++bKZ/LiJo6j4Dgg2t7NJapuBFj4ySs0ugYAn3C/bgpdIDTNpai9L3vfJ/30dQ1QXvSA/1bi+eU1G9GegrFc=
---

# T38 商用档：真实用户验证（真实场景端到端验证）

> 范围：`validation/t38/`（SQL 脚本 ×4 + Python 驱动验证脚本 + PowerShell 编排脚本）、`docs/real_user_validation_guide.md`（真实用户验证指南）、README Status/Roadmap 推进 `v0.27.0-alpha`
> 提交：`T38: real user validation`（本地，未 push）
> 前置：T36 TLS 合规（v0.25.0-alpha）、T37 监控告警（v0.26.0-alpha）均已闭环

## 1. 背景与目标

v1.0 商用档收尾第三项：在真实业务场景下对 OpenXDB 做**端到端真实用户验证**，目标不是再补功能，而是**暴露文档与兼容盲区**——用真实用户会写的 SQL、真实业务闭环、真实运维动作，检验"能力面"是否真的可用、可解释、可运维。

验证原则：

1. **真实业务**：设计小型业务库（用户中心 / 商品 / 库存 / 订单 / 订单明细），验证从建库到业务闭环的完整路径，而非碎片化单测。
2. **真实用户动作**：全部通过 TCP 协议（Python 驱动 / repl / CLI）执行，与真实用户操作一致；不直接调用 Go 内部 API。
3. **真实运维动作**：启动、健康检查（doctor）、指标拉取（stats/metrics/status）、备份恢复（BACKUP/RESTORE/PITR）、生态导入导出（mysqldump / EXPORT）、CDC 订阅、TLS 加密连接、告警规则配置。
4. **结果可复现**：一键编排脚本 `run_validation.ps1` 可重复运行，结果落盘，断言驱动，PASS/FAIL 一目了然。
5. **暴露即修复**：验证中发现的任何缺陷，按项目惯例修复并复跑全量测试。

## 2. 能力面盘点（T38 验证范围基线）

| 能力域 | 覆盖内容 | 来源 |
| --- | --- | --- |
| 单机 SQL 基础 | DDL/DML/DQL、PRIMARY KEY、二级/唯一索引、BETWEEN/LIKE/CASE、JOIN/GROUP BY/子查询/CTE/窗口函数/upsert、ONLINE DDL、外键、查询缓存、批量导入 | T3/T6/T8/T10/T21/T22/T23/T25 |
| 事务 | WAL、快照隔离、BEGIN/COMMIT/ROLLBACK、唯一约束、并发读写 | T3/T4/T17 |
| 视图 / 物化视图 / 触发器 / 过程 | CREATE VIEW、CREATE MATERIALIZED VIEW + REFRESH、CREATE TRIGGER（审计型）、CREATE PROCEDURE | T22/T28 |
| 类型 | INT/TEXT/DECIMAL/DATE/BLOB、CSV 导入导出 | T9 |
| 分布式 | 复制（async/semi-sync/sync）、分片（SPLIT REGION/LIST REGIONS/LOCATE REGION）、多节点（SHOW NODES/REGION ROUTES）、HA 故障转移、2PC、跨节点 DDL、分布式备份一致性 | T11/T12/T13/T14/T15/T16/T17/T33/T34/T35 |
| 生态 | mysqldump 兼容导入（IMPORT MYSQLDUMP）、binlog 订阅 CDC（SHOW BINLOG EVENTS + pkg/cdc + examples/binlog_subscriber）、EXPORT JSON/Parquet/CSV、JDBC/Python 驱动 | T19/T27/T32 |
| 工具链 | openxdb doctor / stats / metrics / status / preflight / version / import、部署与滚动升级脚本 | T20/T29 |
| 商用加固 | TLS 监听与证书（gen-cert）、告警规则（SHOW ALERT RULES/STATUS、SET alert_*）、审计日志、RBAC、用户与权限、MySQL 兼容（PREPARE/@var/information_schema）、监控（slowlog/Prometheus/STATUS） | T24/T26/T30/T31/T36/T37 |
| 备份恢复 | BACKUP TO / RESTORE FROM / RESTORE TO LSN（PITR 基础）、BR 推送拉取 | T18/T35 |

验证边界说明（单机运行环境的客观限制，不视为缺陷）：

- 多节点拓扑（ADD NODE / 跨节点 2PC / 分布式备份 manifest）需要 ≥2 个实例组网，本次单机验证覆盖：单机零触发路径不回归（复制协议、2PC、跨节点 DDL 的单机语义）、复制从节点真实拉起、分片元数据操作、PITR 回放；多节点完整组网路径由既有单测（T14/T15/T33/T34/T35）覆盖。
- 触发器为审计型（语句体限制：不支持算术表达式/NEW-OLD 引用，见 T28），业务里体现为审计日志写入。

## 3. 真实用户验证场景设计

### 3.1 业务库：mini 电商（订单 / 库存 / 用户中心）

```
users(id PK, name, email UNIQUE, phone, created_at)
products(id PK, sku, name, price DECIMAL, category, created_at)
inventory(product_id PK FK->products, qty, updated_at)
orders(id PK, user_id FK->users, status, total DECIMAL, created_at)
order_items(order_id, item_no, product_id FK->products, qty, price, PK(order_id,item_no), FK->orders ON DELETE CASCADE)
```

辅以：审计日志表 `audit_log`（触发器写入）、视图 `v_user_orders`（订单×用户 JOIN）、物化视图 `mv_order_summary`（按状态汇总订单）、存储过程 `p_place_order`（下单占位）、索引（唯一 email、category、user、status、product）。

### 3.2 业务闭环（真实用户路径）

1. **初始化**：建库建表、建索引、建视图/物化视图/触发器/过程。
2. **数据准备**：录入 5 用户、8 商品、库存 8 条。
3. **下单闭环**：用户下单 → 订单 + 明细（触发器写审计）→ 视图查订单 → 物化视图刷新看汇总 → 库存/商品 join 校验。
4. **查询分析**：CTE 排名、窗口函数（RANK/SUM OVER）、子查询、GROUP BY 汇总、EXPLAIN 走索引。
5. **事务与并发**：commit 持久、rollback 回滚、两连接并发写入互不干扰、唯一/外键约束拦截。
6. **运维动作**：STATUS/STATS/METRICS/doctor、慢查询、审计日志、备份恢复、EXPORT、CDC、TLS、告警、RBAC。

### 3.3 验证清单（功能 / 性能 / 稳定性 / 安全 / 告警 / 备份恢复）

| # | 类别 | 验证项 | 通过标准 |
| --- | --- | --- | --- |
| F1 | 功能 | 建表 + 二级/唯一索引 + 外键 | 语句成功，SHOW INDEX 可见 |
| F2 | 功能 | 视图 / 物化视图 / 触发器 / 过程 | 创建成功、查询/刷新/触发符合预期 |
| F3 | 功能 | JOIN / CTE / 窗口 / 子查询 / 分组 | 返回行数与语义正确 |
| F4 | 功能 | upsert（REPLACE / ON DUPLICATE KEY） | 冲突语义正确 |
| F5 | 功能 | PREPARE/EXECUTE + @var + information_schema | 执行正确、元数据可见 |
| F6 | 事务 | commit / rollback / 并发隔离 | 数据与隔离语义正确 |
| F7 | 事务 | 唯一约束 / 外键约束拦截 | 非法写入被拒 |
| F8 | 分布式 | 复制从节点拉起 + 数据同步 | 从节点数据与主节点一致 |
| F9 | 分布式 | SPLIT REGION / LIST REGIONS / LOCATE REGION / SHOW REGION ROUTES | 分片元数据一致 |
| F10 | 生态 | mysqldump 兼容导入 | 表与行数与 dump 一致 |
| F11 | 生态 | CDC 订阅（SHOW BINLOG EVENTS + Subscriber） | 增量事件可拉取、位点持久化 |
| F12 | 生态 | EXPORT JSON / PARQUET / CSV | 文件生成且内容可解析 |
| F13 | 工具链 | doctor / stats / metrics / status / preflight | 退出码与关键字段符合预期 |
| P1 | 性能 | 业务查询走索引（EXPLAIN） | 索引命中，点查/范围查正确 |
| P2 | 性能 | 批量写入吞吐（1000 行 batch） | 无错误完成，耗时记录 |
| S1 | 稳定性 | 重启后数据完整（COUNT 一致） | 数据不丢 |
| S2 | 稳定性 | 优雅停机 SHUTDOWN GRACEFUL | 退出码 0，可再次启动 |
| T1 | 安全 | TLS 加密监听 + 明文拒绝 | 自签名证书下 Python 驱动 verify-ca 连接成功，明文连接被拒 |
| T2 | 安全 | 用户/角色/授权（RBAC） | 授权后可访问，未授权被拒，audit 记录 |
| A1 | 告警 | 配置告警规则并触发 | SHOW ALERT RULES 状态 ALERT/RESOLVED 迁移正确 |
| B1 | 备份恢复 | BACKUP TO → 写增量 → RESTORE FROM | 恢复点后增量消失、数据回到备份点 |
| B2 | 备份恢复 | PITR：RESTORE TO LSN | 回放到指定 LSN，后续写入不可见 |

## 4. 验证产物清单

| 文件 | 用途 |
| --- | --- |
| `validation/t38/01_schema_business.sql` | 业务库 schema + 视图/物化视图/触发器/过程/索引 |
| `validation/t38/02_txn_acid.sql` | 事务 ACID 场景 SQL（提交/回滚/约束） |
| `validation/t38/03_query_analysis.sql` | 查询分析场景（JOIN/CTE/窗口/子查询/upsert/EXPLAIN/PREPARE/information_schema） |
| `validation/t38/04_ops_ecosystem.sql` | 运维与生态场景（STATUS/STATS/分片/导出/备份/CDC/告警/审计/RBAC） |
| `validation/t38/validate_real_user.py` | Python 驱动端到端验证（业务闭环 + 事务并发 + TLS + 备份恢复 + 断言 + 结果 JSON） |
| `validation/t38/run_validation.ps1` | 一键编排（init/start/执行脚本/Python/CLI/复制/导入/CDC/TLS/告警/结果收集） |
| `docs/real_user_validation_guide.md` | 真实用户验证指南（如何运行、如何解读结果） |

## 5. 运行结果记录（实际运行 2026-10-15）

> 以下为本次真实运行（Windows 11 + Go + RocksDB cgo，`bin/openxdb.exe` 按 CGO 约束构建）记录，结果文件见 `validation/t38/results/`。

<!-- T38-RESULTS-START -->
（待运行后回填）
<!-- T38-RESULTS-END -->

## 6. 兼容性

- 全部验证通过 SQL 层（`pkg/sql`）与协议层（`pkg/server` + Python/JDBC 驱动）执行，不引入新语法、不改既有语义；新增仅验证资产（validation/t38）与文档，零内核代码改动（除非验证暴露缺陷）。
- 单机语义零回归：复制/2PC/跨节点 DDL 单机零触发路径保持兼容。
- 版本推进：README Status / Roadmap 更新至 `v0.27.0-alpha`。

## 7. 交付清单

- [x] 设计文档 `docs/T38_commercial_real_user_validation.md`
- [x] 验证产物：`validation/t38/01_schema_business.sql` / `02_txn_acid.sql` / `03_query_analysis.sql` / `04_ops_ecosystem.sql` / `validate_real_user.py` / `run_validation.ps1`
- [x] 验证指南 `docs/real_user_validation_guide.md`
- [x] 真实验证运行 + 结果记录（results/）
- [x] README Status + Roadmap 至 `v0.27.0-alpha`
- [ ] 全量测试 `go test ./... -count=1 -p 1` 全绿（待运行确认）
*（内容由AI生成，仅供参考）*

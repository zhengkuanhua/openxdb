---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_7f20c331c91211f196f9525400248c00
    ReservedCode1: WZdoakklY73SGEqv/oNcstCatUvM4GDRPIlO+PT5sDz26Ex2p7w/BsFpJXRUFASW6Lw4d+MdflA5VctoQZaXsY7JwLFj8TNAjOF9PeAIsXqlfhlHEFOW9kHKlZKqapeMxajg+pbhXqDMf4pGWFaNRnDkHRT8Rv0LewSVyPFYqyUlhrz38He/mwR+H4g=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_7f20c331c91211f196f9525400248c00
    ReservedCode2: WZdoakklY73SGEqv/oNcstCatUvM4GDRPIlO+PT5sDz26Ex2p7w/BsFpJXRUFASW6Lw4d+MdflA5VctoQZaXsY7JwLFj8TNAjOF9PeAIsXqlfhlHEFOW9kHKlZKqapeMxajg+pbhXqDMf4pGWFaNRnDkHRT8Rv0LewSVyPFYqyUlhrz38He/mwR+H4g=
---

# OpenXDB

> 从零自研的单机/分布式关系型数据库内核（Go + RocksDB），全栈自研，**v1.0.0 正式发布**。

OpenXDB 是一个从零自研的单机/分布式关系型数据库内核（Go + RocksDB），历经 M1–M21、T1–T42、E1–E5 全部里程碑。它实现了基于 RocksDB（cgo）的存储引擎、追加式 WAL、快照隔离事务、带二级索引的 SQL 子集，并在此基础上完整交付复制、分片、多节点集群、分布式 2PC 写、HA、region 自动分裂与负载均衡、分布式事务（TSO + 分布式快照隔离）、备份恢复与 PITR、客户端驱动、运维工具链、认证权限（审计 + RBAC）、商业加固（TLS / 监控告警 / 真实用户验证）与生态导出（JSON / Parquet）等能力。

## 项目一句话定位

**一个仓库，全栈覆盖。** 单机可跑通完整 SQL 与事务，分布式可完成复制、分片、2PC、HA 与全局一致备份——从存储引擎到客户端驱动全部自研，适合学习数据库内核，也适合作为工程实验与二次开发的底座。

## 核心特性亮点

| 维度 | 亮点 |
|------|------|
| **存储** | RocksDB cgo 桥存储引擎 + 内存 B+Tree 参考实现（LSM comparator） |
| **WAL** | 追加式 WAL + 崩溃重放恢复，group commit 批量 fsync |
| **事务** | 快照隔离本地事务 + 分布式事务（TSO 全局时间戳 + 2PC + 分布式快照隔离） |
| **SQL** | SELECT/JOIN/GROUP BY/子查询/CTE/窗口函数/upsert/视图/物化视图/触发器/存储过程/INSERT INTO SELECT 等 |
| **集群** | 异步/半同步/同步复制、region 分片、多节点 2PC、HA 故障转移、自动分裂与负载均衡 |
| **安全** | 审计日志、细粒度 RBAC 权限、TLS 加密传输、监控告警 |
| **备份恢复** | 一致性快照备份、幂等恢复、LSN 基 PITR、分布式一致备份 |
| **生态** | JDBC/Python 驱动、mysqldump 导入、binlog CDC、JSON/Parquet 导出 |

## 快速开始

### 编译

```bash
# Linux/macOS
./scripts/build.sh --test
# Windows
powershell -ExecutionPolicy Bypass -File scripts\build.ps1 -RunTests
```

### 启动与连接

```bash
openxdb init --data-dir ./data     # 初始化数据目录
openxdb start --data-dir ./data    # 启动 TCP 服务（默认端口 7788）
openxdb repl --data-dir ./data     # 或直接进入本地 REPL
```

支持三种连接方式：**REPL**、**Python 驱动**（PEP 249）、**JDBC 驱动**（`jdbc:openxdb://127.0.0.1:7788`）。

### 第一个示例

```sql
CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id));
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35);
CREATE INDEX idx_age ON users (age);
SELECT name FROM users WHERE age >= 25 AND age < 35 ORDER BY age;
```

## 性能基准（E5 实测数据）

标准 sysbench OLTP / TPC-C 基准对齐（Python PEP 249 驱动等价实测，8 线程 × 60s / 120s，0 错误）：

- **OLTP read**：1.73 TPS（QPS 24.27）
- **OLTP write**：0.93 TPS（QPS 3.73）
- **OLTP read-write**：1.32 TPS（QPS 23.70）
- **TPC-C 5 仓**：tpmC 13.50 / TPM 32.50，0 错误

> 说明：因自研二进制协议非 MySQL wire protocol，标准 sysbench/TPC-C 工具不可直连，E5 采用 Python PEP 249 驱动等价实测；数值为单机端侧基准，非生产级吞吐承诺。详见 [KNOWN_LIMITATIONS](../KNOWN_LIMITATIONS.md) 与 [E5 实验文档](experiments/E5_standard_bench_alignment.md)。

## 文档与 GitHub 入口

- [产品手册（中文）](product_manual_zh.md)：11 章节中文产品手册，含架构、SQL 参考、运维、性能基准、FAQ
- [Product Manual (English)](product_manual_en.md)：11-chapter English product manual
- [Why RocksDB](blog_why_openxdb_rocksdb.md)：存储引擎选型博客
- [CHANGELOG](../CHANGELOG.md)：完整版本变更记录
- [RELEASE_NOTES](../RELEASE_NOTES.md)：v1.0.0 发布说明
- [KNOWN_LIMITATIONS](../KNOWN_LIMITATIONS.md)：已知限制与边界
- [设计文档与基准实验](https://github.com/zhengkuanhua/openxdb/tree/main/docs)：T1–T42 / M 系列 / E1–E5 全部设计记录

**GitHub 仓库**：[zhengkuanhua/openxdb](https://github.com/zhengkuanhua/openxdb) —— Star 支持、提交 Issue、参与共建，欢迎你的加入。

## 构建本站点

本目录为 MkDocs 站点骨架，构建方式：

```bash
pip install mkdocs
mkdocs build   # 输出到 site/
```

推送到 `main` 分支后，GitHub Actions 会自动构建并部署到 GitHub Pages（见 `.github/workflows/pages.yml`）。
*（内容由AI生成，仅供参考）*

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

OpenXDB 是从零自研的单机/分布式关系型数据库内核（Go + RocksDB），历经 M1–M21、T1–T42、E1–E5 全部里程碑，当前版本 **v1.0.0**（2026-10-16 正式发布）。

它实现了基于 RocksDB（cgo）的存储引擎、追加式 WAL、快照隔离事务、带二级索引的 SQL 子集，并在此基础上完整交付复制、分片、多节点集群、分布式 2PC 写、HA、region 自动分裂与负载均衡、分布式事务（TSO + 分布式快照隔离）、备份恢复与 PITR、客户端驱动、运维工具链、认证权限（审计 + RBAC）、商业加固（TLS / 监控告警 / 真实用户验证）与生态导出（JSON / Parquet）等能力。

## 文档导航

- [产品手册（中文）](product_manual_zh.md)：11 章节中文产品手册，含架构、SQL 参考、运维、性能基准、FAQ
- [Product Manual (English)](product_manual_en.md)：11-chapter English product manual
- [Why RocksDB](blog_why_openxdb_rocksdb.md)：存储引擎选型博客
- [CHANGELOG](../CHANGELOG.md)：完整版本变更记录
- [RELEASE_NOTES](../RELEASE_NOTES.md)：v1.0.0 发布说明
- [KNOWN_LIMITATIONS](../KNOWN_LIMITATIONS.md)：已知限制与边界

## 核心特性（Highlights）

- **存储**：RocksDB cgo 桥 + 内存 B+Tree 参考实现（LSM comparator）
- **WAL**：追加式 WAL + 重放恢复
- **事务**：快照隔离本地事务 + 分布式事务（TSO + 2PC）
- **SQL 子集**：SELECT/JOIN/GROUP BY/子查询/CTE/窗口函数/upsert/视图/物化视图/触发器/存储过程/INSERT INTO SELECT 等
- **复制/分片/集群**：异步/半同步/同步复制、region 分片、多节点集群、HA 故障转移
- **运维**：热配置、优雅关闭、doctor/stats、审计日志、RBAC、TLS、监控告警
- **生态**：JDBC/Python 驱动、mysqldump 导入、binlog CDC、JSON/Parquet 导出
- **备份恢复**：一致性快照备份、幂等恢复、LSN 基 PITR、分布式一致备份

## 性能基准（E5，Python 驱动等价实测）

- OLTP read 1.73 TPS（QPS 24.27）/ write 0.93 TPS（QPS 3.73）/ read-write 1.32 TPS（QPS 23.70）
- TPC-C 5 仓 tpmC 13.50 / TPM 32.50，0 错误
- 说明：因自研二进制协议非 MySQL wire protocol，标准 sysbench/TPC-C 工具不可直连，E5 采用 Python PEP 249 驱动等价实测；数值为单机端侧基准，非生产级吞吐承诺。详见 [KNOWN_LIMITATIONS](../KNOWN_LIMITATIONS.md)。

## 快速开始

```bash
# 初始化
openxdb init --data-dir ./data
# 启动
openxdb start --data-dir ./data
# REPL
openxdb repl --data-dir ./data
```

编译环境：设置 `CGO_CFLAGS` / `CGO_CXXFLAGS` 指向 RocksDB include，`CGO_LDFLAGS` 指向 RocksDB 构建目录（`-lrocksdb`）；严禁额外追加 `-lstdc++` 等未声明参数。一键构建见 scripts/build.ps1 / scripts/build.sh。

## 构建本站点

本目录为 MkDocs 站点骨架，构建方式：

```bash
pip install mkdocs
mkdocs build   # 输出到 site/
```
*（内容由AI生成，仅供参考）*

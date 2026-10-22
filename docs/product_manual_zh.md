---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_4af21075cdd111f1898e525400248c00
    ReservedCode1: x2s9rj4MGVZ1nA4POXhScN+hTOPZvsYpMMxDoSy+W4m+SSVYIdUpY4PP7SnOeGoeaigkY7tC1a9bRZv5joabyTPxMCBJjDEH6g3o/SL/AV7QSSJcm3hunepYhjygIA6d2i2uLqS/HjNUd7r4cORcYos6mq+i8ACPsI0+aigQj17w4y+F6k3BUG1RmwU=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_4af21075cdd111f1898e525400248c00
    ReservedCode2: x2s9rj4MGVZ1nA4POXhScN+hTOPZvsYpMMxDoSy+W4m+SSVYIdUpY4PP7SnOeGoeaigkY7tC1a9bRZv5joabyTPxMCBJjDEH6g3o/SL/AV7QSSJcm3hunepYhjygIA6d2i2uLqS/HjNUd7r4cORcYos6mq+i8ACPsI0+aigQj17w4y+F6k3BUG1RmwU=
---

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_7ba0af56c91211f196f9525400248c00
    ReservedCode1: /Hidy1UX3iuZHTta/OagDFGb64+9FitizuQSx7liNK2kEEziZI54iswdWMMuLMB54sbW3W20cPMaqdoxD4NAsMphaewTggGGYwypZTArzAQfgXBmefiZND4GXKkfplIla/gxWvywp25qwumuhKY4KBpvJj3tRU+u+rEPdilRgxGCTeTyWLryYiS285w=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_7ba0af56c91211f196f9525400248c00
    ReservedCode2: /Hidy1UX3iuZHTta/OagDFGb64+9FitizuQSx7liNK2kEEziZI54iswdWMMuLMB54sbW3W20cPMaqdoxD4NAsMphaewTggGGYwypZTArzAQfgXBmefiZND4GXKkfplIla/gxWvywp25qwumuhKY4KBpvJj3tRU+u+rEPdilRgxGCTeTyWLryYiS285w=
---

# OpenXDB 产品手册（中文版）

> 版本：v1.0.0
> 更新日期：2026-10-16
> 定位：单机端侧基准的关系型数据库内核。本文档所有功能与数据均来自 README.md 与 docs/ 目录下的真实设计文档，不包含任何臆造参数或数字。

---

## 目录

1. [产品简介与定位](#1-产品简介与定位)
2. [快速开始](#2-快速开始)
3. [安装与部署](#3-安装与部署)
4. [SQL 参考](#4-sql-参考)
5. [客户端驱动](#5-客户端驱动)
6. [数据安全](#6-数据安全)
7. [运维与可观测](#7-运维与可观测)
8. [分布式能力](#8-分布式能力)
9. [性能基准](#9-性能基准)
10. [已知限制与边界](#10-已知限制与边界)
11. [FAQ](#11-faq)

---

## 1. 产品简介与定位

OpenXDB 是一个**从零自研**的单机/分布式关系型数据库内核，使用 Go 语言编写，存储引擎基于 RocksDB（通过 cgo 薄桥接层接入）。内核完整包含 WAL（预写日志）、事务、SQL 解析与执行、复制、分片、2PC、HA（高可用）、备份恢复等能力，从教学与实验出发，逐步演进为具备分布式能力的完整内核。

### 1.1 定位边界

- OpenXDB 默认使用**自研二进制 TCP 协议**（`pkg/server`）；自 v1.1.0 起可通过 `openxdb start --mysql-port <port>` 启用 **MySQL wire protocol 兼容监听**（`pkg/mysqlproto`），支持 MySQL CLI / go-sql-driver / JDBC 驱动直接连接（详见[第 2 章](#2-快速开始)与[第 5 章](#5-客户端驱动)）；依赖完整 MySQL 生态扩展（如 binlog 复制工具）的场景仍不可直接复用。
- 性能定位为**单机端侧基准**：当前内核实现了完整功能链路，性能优化仍在持续进行，实测吞吐数据见[第 9 章](#9-性能基准)。
- 不将 TiDB、TDSQL、MySQL 等作为对标对象，OpenXDB 是一个独立实现的内核，用于学习、实验与端侧场景。

### 1.2 核心能力

| 能力 | 说明 |
|---|---|
| 存储引擎 | RocksDB-backed KV 存储（cgo 桥接 `pkg/storage/rocksdb`）+ 纯 Go 内存 B+Tree（`pkg/storage/btree`，作为参考实现与比较器） |
| WAL | 追加式预写日志，崩溃恢复重放（`pkg/wal`） |
| 事务 | 本地 ACID 事务，快照隔离（snapshot isolation）；读己之写合并、墓碑、点/范围扫描（`pkg/txn`） |
| SQL 子集 | DDL/DML/查询/运维语句，详见[第 4 章](#4-sql-参考) |
| 二级索引 | 创建时回填、DML 自动维护、等值/范围/ORDER BY 查询优化 |
| 复制 | master-follower 异步复制（M2），支持 async / semi-sync / sync 三种模式（T33） |
| 分片 | 逻辑分片：RegionInfo 元数据 + Router 半开区间 `[StartKey, EndKey)` + region 前缀物理键（M3） |
| 多节点集群 | 节点拓扑、集群路由表（region → 节点）、跨节点 SELECT 转发与合并（M4） |
| 分布式写 / 2PC | 跨节点写原子性，两阶段提交（M5） |
| HA / 故障转移 | 心跳故障检测、region 自动重指派、读 failover、复制链路主从切换（M6） |
| Region 分裂与负载均衡 | 写路径自动分裂、手动/周期负载均衡、在线迁移（M7） |
| 分布式事务 / TSO | 全局单调时间戳、分布式快照隔离、2PC 恢复加固（M8） |
| 备份恢复（BR） | 一致快照逻辑备份、幂等恢复、基于 LSN 的 PITR（时间点恢复） |
| 客户端驱动 | JDBC 与 Python 驱动（T19），自研协议封装 |
| 运维工具链 | `openxdb doctor` / `stats`、一键部署脚本（T20） |
| 安全 | TLS 加密传输（T36）、RBAC 角色权限（M11/M17）、审计日志（T30） |
| 可观测 | 慢查询日志、Prometheus 指标、状态视图、告警规则（M13/T37） |
| 生态互操作 | CSV/JSON Lines/Parquet 导出（M19）、mysqldump 兼容导入（M14） |

### 1.3 架构分层

| 层 | 包 | 职责 |
|---|---|---|
| 协议 + CLI | `cmd/openxdb`、`pkg/server` | TCP server、REPL、二进制协议帧 |
| SQL | `pkg/sql` | 递归下降解析器、执行器（project/filter/sort/aggregate）、元数据目录、二级索引层 |
| 事务 | `pkg/txn` | Begin/commit/rollback、快照隔离、读己之写 |
| 存储 | `pkg/storage` | KV 接口、键编码；RocksDB 引擎与 B+Tree 引擎 |
| WAL | `pkg/wal` | 追加式日志、重放恢复 |
| 复制 | `pkg/replication`、`pkg/txn`、`pkg/db` | Binlog 存储、master 复制器（push + 心跳）、follower 幂等应用、commit-hook 装配 |
| 分片 | `pkg/sharding`、`pkg/sql`、`pkg/db` | RegionInfo 元数据、Router 键→region 映射、region 前缀物理键、跨 region 查询合并、分裂管理 |
| 集群 | `pkg/cluster`、`pkg/sql`、`pkg/db` | 节点注册表、握手/存活、集群路由表、跨节点 SELECT 转发与合并、region 数据装载 |
| 分布式写 / 2PC | `pkg/cluster`、`pkg/sql/2pc.go`、`pkg/db` | 两阶段提交协调器、参与者 prepare 持久化、幂等 commit/abort、崩溃恢复 |
| HA / 故障转移 | `pkg/cluster`、`pkg/sql/ha.go`、`pkg/db/ha.go` | 心跳超时 → 节点 down、region 自动重指派、读 failover、复制主从切换 |
| 分裂与均衡 | `pkg/sql/balance.go`、`pkg/cluster`、`pkg/db` | 写路径自动分裂、手动/自动负载均衡、集群未启用防护 |
| 全局时间戳（TSO） | `pkg/tso`、`pkg/cluster/tso.go`、`pkg/db` | 全局单调时间戳（64 位毫秒 + 逻辑位布局）、批量签发、溢出保护、事务边界 Reset |
| 快照隔离（分布式） | `pkg/cluster/txn2pc.go`、`pkg/sql/executor.go`、`pkg/db` | 版本记录（m:ver）、begin_ts/commit_ts 全局过滤、跨节点版本透传 |
| 备份 / 恢复 | `pkg/sql/backup.go`、`pkg/sql`、`pkg/db` | 一致快照逻辑备份（单文件、版本 + CRC32）、幂等恢复、binlog 重放到目标 LSN |
| 数据目录 | `pkg/db` | 初始化、引擎 + WAL + 事务管理器装配、崩溃恢复 |

### 1.4 键编码

```
物理键: r{regionID:8B}{innerKey}
行键:   s{tableID:8B}{pk}                          (inner, M1)
索引键: i{tableID:8B}{indexID:8B}{idxVal}p{pk}    (inner, T6)
```

每个存储键都带 region 前缀（`r` + 8 字节大端 regionID）；内层键保持 M1/T6 布局，保证键在 region 内连续、region 边界显式。索引值使用排序友好编码：INT 翻转符号位（`v ^ 0x8000000000000000`），TEXT 保持原始字节，字节序即值序。

---

## 2. 快速开始

### 2.1 构建要求

- Go 1.22+
- C/C++ 工具链（Windows 上使用 MinGW-w64；Linux/macOS 使用 gcc/clang）
- RocksDB（已在 v11.8.1 源码构建环境测试，无需第三方压缩库）

### 2.2 构建（Windows + MinGW + Go）

使用一键构建脚本（自动检测 RocksDB 安装位置，然后 `go build` + 可选 `go test`）：

```powershell
# Windows：构建 bin\openxdb.exe，验证 go build ./...，运行 go test ./...
powershell -ExecutionPolicy Bypass -File scripts\build.ps1 -RunTests
# 显式指定 RocksDB 源码/构建目录：
powershell -ExecutionPolicy Bypass -File scripts\build.ps1 -RocksSrcDir C:\rocksdb-src -RocksBuildDir C:\rocksdb-build -RunTests
```

```sh
# Linux/macOS：
./scripts/build.sh --test
./scripts/build.sh --rocks-src /path/to/rocksdb-11.8.1 --rocks-build /path/to/librocksdb-dir --test
```

手动 cgo 配置（等价做法，适用于自定义工具链）：

```sh
export CGO_CFLAGS=-I<rocksdb>/include
export CGO_CXXFLAGS=-I<rocksdb>/include
export CGO_LDFLAGS=-L<rocksdb-build> -lrocksdb   # Windows: 追加 -lshlwapi -lrpcrt4 -lws2_32

go build ./...
```

`pkg/storage/rocksdb` 已按平台声明 `#cgo` 链接标志（Windows：`-lwinpthread -lshlwapi -lrpcrt4 -lws2_32`；Linux：`-lstdc++ -lpthread`；Darwin：`-lstdc++`），因此只需通过 `CGO_CFLAGS` / `CGO_LDFLAGS` 提供 RocksDB include/lib 目录。

> **注意**：仓库 #cgo 附加标志含 `-static-libstdc++ -static-libgcc`，请勿额外追加 `-lstdc++` / `-lz` 等未声明链接参数，避免链接冲突。

### 2.3 启动 REPL / TCP server

```sh
# 初始化数据目录
openxdb init --data-dir ./data

# 本地 REPL
openxdb repl --data-dir ./data

# TCP server（默认端口 7788）
openxdb start --data-dir ./data [--port 7788]

# 可选：启用 MySQL wire protocol 兼容监听（mysql CLI / go-sql-driver / JDBC 直连）
openxdb start --data-dir ./data --mysql-port 3306

# 运维检查
openxdb doctor --data-dir ./data [--addr 127.0.0.1:7788] [--json]
openxdb stats [--addr 127.0.0.1:7788] [--interval 5] [--json]
```

### 2.4 建表、插数、查询示例

```sql
CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id));
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35);
CREATE INDEX idx_age ON users (age);
SELECT name FROM users WHERE age >= 25 AND age < 35 ORDER BY age;
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE id = 3;
```

### 2.5 运行测试

```sh
go test ./...
```

全部包通过：`cluster`（节点握手、查询转发、心跳 down、region push 路由学习、节点幂等重注册）、`db`（复制集成、M4 两节点集成、M5 2PC 集成、M6 HA 集成、M7 分裂/均衡集成、M8 分布式事务集成、BR 备份恢复集成）、`replication`、`server`、`sharding`、`sql`、`storage`、`storage/btree`、`storage/rocksdb`、`tso`、`txn`、`wal`。

---

## 3. 安装与部署

### 3.1 配置文件 openxdb.conf

OpenXDB 使用 INI 风格配置文件（参考 `docs/openxdb.conf.example`），核心段如下：

```ini
[server]
data_dir = ./data
port = 7788
replica_port = 7789
metrics_port = 7790
# tls_cert = cert.pem
# tls_key = key.pem

[sql]
query_cache = on
slow_query_threshold = 100
backup_dir = ./backup
```

- `[server]` 段：`data_dir` 数据目录、`port` 主服务端口（默认 7788）、`replica_port` 复制端口、`metrics_port` Prometheus 指标端口、`tls_cert`/`tls_key` TLS 证书与私钥路径。
- `[sql]` 段：`query_cache` 查询缓存开关、`slow_query_threshold` 慢查询阈值（毫秒，默认 100ms）、`backup_dir` 备份目录。

启动方式：

```sh
openxdb start --config openxdb.conf
```

> 提示：配置文件中的可热更新项（如 `query_cache`、`slow_query_threshold`、`slow_query_log`）也可通过 `SET` / `SET PERSIST` 语句运行时修改，详见[第 4 章运维语句](#47-运维语句)。

### 3.2 一键部署脚本

仓库提供 Windows 与 Linux/macOS 两套一键部署脚本：

```powershell
# Windows
powershell -ExecutionPolicy Bypass -File scripts\deploy.ps1 -DataDir C:\openxdb\data -Port 7788
```

```sh
# Linux/macOS
./scripts/deploy.sh --data-dir /var/lib/openxdb --port 7788
```

脚本流程：参数化初始化（`-DataDir` / `-Port` / `--data-dir` / `--port`）→ 启动服务 → `openxdb doctor` 健康自检 → 输出部署摘要。

### 3.3 启动模式汇总

| 命令 | 说明 |
|---|---|
| `openxdb init --data-dir <dir>` | 初始化数据目录 |
| `openxdb repl --data-dir <dir>` | 本地交互式 REPL |
| `openxdb start --data-dir <dir> [--port 7788]` | 启动 TCP server（默认端口 7788） |
| `openxdb start --data-dir <dir> [--mysql-port 3306]` | 可选启用 MySQL wire protocol 兼容监听（MySQL CLI / go-sql-driver / JDBC 直连，见[5.1](#51-通用协议说明)） |
| `openxdb start --cluster-addr <addr>` | 以集群模式启动（节点 ID 由 `--node-id` 指定或自动生成） |
| `openxdb doctor` | 健康检查（退出码 0=通过 / 1=警告 / 2=错误） |
| `openxdb stats` | 实时指标查看 |
| `openxdb gen-cert cert.pem key.pem` | 生成自签名 TLS 证书 |

---

## 4. SQL 参考

> 本章所有语法均来自 README.md 与 docs/ 下 T* 设计文档。SQL 能力清单包含 T41/T42 新增能力（IF NOT EXISTS、CREATE TABLE AS SELECT、INSERT INTO SELECT）。

### 4.1 数据类型

| 类型 | 说明 |
|---|---|
| `INT` | 整数 |
| `TEXT` | 文本 |
| `DATE` | 日期 |
| `DECIMAL` | 定点小数（内部规范化固定 4 位小数） |
| `BLOB` | 二进制大对象 |

### 4.2 DDL

#### CREATE TABLE

```sql
-- 基本建表（行内 PRIMARY KEY 与表级 PRIMARY KEY 等价）
CREATE TABLE users (
  id INT PRIMARY KEY,
  name TEXT,
  age INT
);

CREATE TABLE orders (
  id INT,
  user_id INT,
  amount DECIMAL,
  PRIMARY KEY (id)
);

-- IF NOT EXISTS：幂等建表（T41 新增）
CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT);

-- CREATE TABLE AS SELECT（CTAS，T41 新增）：基于查询结果建表
CREATE TABLE adult_users AS SELECT id, name FROM users WHERE age >= 18;
```

#### CREATE INDEX

```sql
CREATE INDEX idx_age ON users (age);
```

#### 视图 / 物化视图 / 触发器 / 存储过程

```sql
-- 视图
CREATE VIEW v_adult AS SELECT id, name FROM users WHERE age >= 18;
DROP VIEW v_adult;

-- 物化视图
CREATE MATERIALIZED VIEW mv_adult AS SELECT id, name FROM users WHERE age >= 18;
REFRESH MATERIALIZED VIEW mv_adult;
DROP MATERIALIZED VIEW mv_adult;

-- 触发器
CREATE TRIGGER trg_after_insert AFTER INSERT ON users FOR EACH ROW ...;
DROP TRIGGER trg_after_insert;
SHOW TRIGGERS;

-- 存储过程
CREATE PROCEDURE p_get_user(IN uid INT) ...;
CALL p_get_user(1);
DROP PROCEDURE p_get_user;
SHOW PROCEDURES;
```

#### 用户 / 角色

```sql
CREATE USER 'alice' IDENTIFIED BY 'pwd123';        -- 重复建报错
DROP USER [IF EXISTS] 'alice';                     -- 不存在报错；IF EXISTS 静默
SHOW USERS;                                        -- 按名称排序
ALTER USER 'alice' IDENTIFIED BY 'newpwd';         -- 重置密码

CREATE ROLE 'analyst';
DROP ROLE [IF EXISTS] 'analyst';
SHOW ROLES;
GRANT ROLE 'analyst' TO 'alice';
REVOKE ROLE 'analyst' FROM 'alice';
SHOW GRANTS FOR 'alice';
```

#### DROP ... IF EXISTS

```sql
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS orders;
```

- `DROP TABLE IF EXISTS`：表不存在时幂等成功（T21 修复）。
- `DROP USER IF EXISTS` / `DROP ROLE IF EXISTS`：幂等删除。
- 删除用户/表/角色会级联清理相关授权记录。

### 4.3 DML

#### INSERT

```sql
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25);
INSERT INTO users (id, name) VALUES (3, 'carol');
```

#### INSERT INTO ... SELECT（T42 新增）

```sql
-- 将查询结果插入目标表（可省略列清单；完整 SELECT 源兼容；原子事务 + 回滚）
INSERT INTO archive_users SELECT * FROM users WHERE age >= 30;
INSERT INTO archive_users (id, name) SELECT id, name FROM users WHERE age >= 30;
```

- 自插入采用快照语义（self-insert snapshot semantics）。
- `REPLACE INTO ... SELECT` 同样支持。

#### REPLACE / UPSERT

```sql
-- REPLACE：主键冲突时先删后插
REPLACE INTO users VALUES (1, 'alice2', 31);

-- INSERT ... ON DUPLICATE KEY UPDATE：冲突时更新
INSERT INTO users VALUES (1, 'alice', 30)
  ON DUPLICATE KEY UPDATE age = 31;
```

#### UPDATE / DELETE

```sql
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE id = 3;
```

### 4.4 查询（SELECT 全能力）

```sql
-- 基础查询 + WHERE
SELECT * FROM users WHERE age >= 25;

-- 排序与分页（GROUP BY/聚合结果同样支持 LIMIT/OFFSET，T40）
SELECT name FROM users ORDER BY age LIMIT 10 OFFSET 5;

-- GROUP BY + 聚合（MAX/MIN/COUNT/SUM/AVG，MAX/MIN 为 T40 新增）
SELECT age, COUNT(*), SUM(amount), AVG(amount) FROM orders GROUP BY age;
SELECT MAX(age), MIN(age) FROM users;

-- DISTINCT（T40 新增）
SELECT DISTINCT age FROM users;

-- JOIN
SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;

-- 子查询
SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100);

-- CTE（M10）
WITH adult AS (SELECT id, name FROM users WHERE age >= 18)
SELECT * FROM adult;

-- 窗口函数（M10；P0-5 起支持显式 ROWS/RANGE 帧）
SELECT name, age, ROW_NUMBER() OVER (ORDER BY age) AS rn FROM users;
SELECT name, age, SUM(age) OVER (ORDER BY age ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS move_sum FROM users;
SELECT name, age, SUM(age) OVER (ORDER BY age RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp_sum FROM users;

-- BETWEEN / IN / LIKE
SELECT * FROM users WHERE age BETWEEN 25 AND 35;
SELECT * FROM users WHERE name IN ('alice', 'bob');
SELECT * FROM users WHERE name LIKE 'a%';

-- CASE WHEN 与表达式（算术表达式为 T40 新增）
SELECT name, CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END AS bucket FROM users;
SELECT id, amount * 2 AS double_amount FROM orders;
SELECT -age AS neg_age FROM users;
```

### 4.5 事务

```sql
BEGIN;
INSERT INTO users VALUES (4, 'dave', 40);
UPDATE users SET age = 41 WHERE id = 4;
COMMIT;

-- 或回滚
BEGIN;
DELETE FROM users WHERE id = 4;
ROLLBACK;
```

事务特性：本地 ACID + 快照隔离（snapshot isolation）；读己之写合并、墓碑、点/范围扫描。

### 4.6 数据导入导出

```sql
-- CSV 导入（P1）
IMPORT INTO users FROM '/path/users.csv';

-- mysqldump 兼容导入（M14，T27）：解析 mysqldump 导出的 CREATE TABLE + INSERT
IMPORT MYSQLDUMP FROM '/path/dump.sql' [BATCH 100] [IGNORE ERRORS];

-- 导出（P1 + M19 生态导出）
EXPORT TABLE users TO '/path/users.csv';                        -- 等价 FORMAT CSV
EXPORT TABLE users TO '/path/users.csv' FORMAT CSV;
EXPORT TABLE users TO '/path/users.jsonl' FORMAT JSON;          -- JSON Lines
EXPORT TABLE users TO '/path/users.parquet' FORMAT PARQUET;     -- Parquet v1
```

- CSV：面向表格工具。
- JSON（JSON Lines）：流式对象序列，每行一条记录，适合日志管道/数据湖摄取/ETL。
- Parquet：列式二进制存储，由项目内自实现最小 Parquet v1 写入器产出（单 Row Group、PLAIN/UNCOMPRESSED 编码），可被 Spark/Hive/DuckDB/Polars 直接消费。

### 4.7 运维语句

#### SHOW 语句

```sql
SHOW TABLES;                          -- 列出全部表
SHOW INDEX FROM users;                -- 查看表索引
SHOW VIEWS;                           -- 列出视图
SHOW USERS;                           -- 列出用户
SHOW ROLES;                           -- 列出角色
SHOW GRANTS FOR 'alice';              -- 查看用户授权
SHOW TRIGGERS;                        -- 列出触发器
SHOW PROCEDURES;                      -- 列出存储过程
SHOW SLOWQUERIES;                     -- 慢查询（新→旧，列：time|user|duration_ms|rows|error|sql）
SHOW STATS;                           -- 服务状态指标
SHOW CONFIG;                          -- 查看当前配置
SHOW PERSISTED CONFIG;                -- 查看持久化配置
SHOW AUDITLOG;                        -- 审计日志
SHOW ALERT RULES;                     -- 告警规则
SHOW ALERT STATUS;                    -- 告警状态
SHOW ALERTS;                          -- 告警事件
SHOW NODES;                           -- 集群节点
SHOW REGION ROUTES;                   -- region 路由（region/table/start/end/node/local）
LIST REGIONS;                         -- 列出 region
LOCATE REGION <key>;                  -- 定位键所在 region
```

#### EXPLAIN

```sql
EXPLAIN <SELECT>;
```

#### SET / SET PERSIST

```sql
SET slow_query_threshold = 100;       -- 慢查询阈值（毫秒，<=0 关闭记录）
SET slow_query_log = on;              -- 慢查询落盘开关
SET query_cache = on;                 -- 查询缓存开关
SET replication_mode = async;         -- 复制模式：async / semi-sync / sync
SET replication_ack = 1;              -- 半同步 ACK 数量
SET replication_timeout = 3000;       -- ACK 超时（毫秒）
SET alert_rules = '...';              -- 告警规则（JSON）
SET audit_log = on;                   -- 审计日志热开关
SET PERSIST slow_query_threshold = 100;  -- 持久化配置（重启生效类）
SET PERSIST tls_cert = 'cert.pem';    -- TLS 证书（重启生效）
SET PERSIST tls_key = 'key.pem';      -- TLS 私钥（重启生效）
SET PERSIST tls_allow_legacy = on;    -- 允许明文/ TLS 共存监听
SET @var = 1;                         -- 用户变量
SELECT @var;                          -- 读取用户变量
```

#### BACKUP / RESTORE

```sql
BACKUP TO '/path/backup.bk';                       -- 一致快照逻辑备份
RESTORE FROM '/path/backup.bk';                    -- 幂等恢复（同名表 drop-then-recreate）
RESTORE FROM '/path/backup.bk' TO LSN 12345;       -- PITR：备份点后重放 binlog 到目标 LSN
```

#### PREPARE / EXECUTE / DEALLOCATE（M18）

```sql
PREPARE stmt1 FROM 'SELECT * FROM users WHERE id = ?';
EXECUTE stmt1 USING 1;                             -- 绑定用户变量或字面量
EXECUTE stmt1 USING @var;
DEALLOCATE PREPARE stmt1;
```

- 会话级缓存：同名重 PREPARE 覆盖；EXECUTE/DEALLOCATE 未知名报错；实参数量必须匹配占位符数量。

#### information_schema 只读视图（M18）

```sql
SELECT * FROM information_schema.schemata;
SELECT * FROM information_schema.tables;
SELECT * FROM information_schema.columns;
SELECT * FROM information_schema.statistics;
```

- 只读虚拟库：支持大小写不敏感列投影、`WHERE <col> = <literal>` 等值过滤（可 AND 组合）、ORDER BY、限定 `default.information_schema.tables` FROM；任何针对虚拟库的 CREATE/DROP/INSERT/UPDATE 都会被拒绝。

---

## 5. 客户端驱动

OpenXDB 提供 JDBC 与 Python 两套标准客户端驱动（T19），均基于自研二进制 TCP 协议（`pkg/server`）；自 v1.1.0 起同时提供 **MySQL wire protocol 兼容监听**（`--mysql-port`，`pkg/mysqlproto`），供 MySQL 官方客户端、go-sql-driver/mysql、JDBC（MySQL 方言）等生态工具直接连接。

### 5.1 通用协议说明

- 线协议：UTF-8 行协议，每条命令一行、以 `\n` 结尾；服务端逐行回复。
- 回复形态：简单确认（OK/PONG/BYE）、受影响行（N rows affected）、文本表（首行表头 + 分隔线 + 数据行 + 末行 N rows）、错误（ERR <message>）。
- 空单元格统一映射为 NULL。
- 多行文本不支持（参数化字符串值中 `\n` / `\r` 会被驱动拒绝）。
- 参数化：服务端协议无 prepared statement 二进制参数通道，两驱动均在客户端安全转义 `?` 占位符。
- MySQL wire protocol 兼容（`--mysql-port`）：支持握手认证、查询/DML、预处理语句（COM_STMT_PREPARE / COM_STMT_EXECUTE / COM_STMT_SEND_LONG_DATA）、COM_CHANGE_USER 等常用命令；`mysql -h127.0.0.1 -P3306 -uroot`、go-sql-driver/mysql、MySQL JDBC 均可直连。能力边界见 KNOWN_LIMITATIONS.md 1.2 节。

### 5.2 JDBC 驱动

- 位置：`drivers/jdbc`，Maven 项目，Java 8+，JUnit 5。
- 注册：静态块 `DriverManager.registerDriver(new OpenXDBDriver())` + `META-INF/services/java.sql.Driver` 自动注册。
- URL：`jdbc:openxdb://host:port`（默认端口 7788）；`user`/`password` 属性被接受但默认忽略（无 AUTH 握手时以 root 运行）。
- 核心类：`OpenXDBDriver` / `OpenXDBConnection` / `OpenXDBStatement` / `OpenXDBPreparedStatement`（客户端 `?` 占位符替换）/ `OpenXDBResultSet`（只进只读）/ `OpenXDBProtocol`（socket 连接 5s 超时）。
- 事务：`setAutoCommit(false)` 触发会话事务；执行前若未 BEGIN 自动发送 BEGIN。
- TLS：驱动支持 TLS 连接（`InsecureTLSConfig` 场景可关闭证书校验，生产环境使用 verify-full / verify-ca 配置）。

### 5.3 Python 驱动

- 位置：`drivers/python`，PEP 249 兼容。
- 元信息：`apilevel="2.0"`、`threadsafety=1`、`paramstyle="qmark"`、`__version__="0.10.0"`。
- 模块级 `connect()`、`DEFAULT_PORT`；错误类 PEP 249 风格层级。

```python
import openxdb
conn = openxdb.connect("127.0.0.1", 7788)
cur = conn.cursor()
cur.execute("SELECT id, name FROM users WHERE id > ?", (1,))
for row in cur.fetchall():
    print(row)
conn.close()
```

- `Connection`：`cursor()` / `ping()` / `begin()` / `commit()` / `rollback()` / `close()`。
- `Cursor`：`execute` / `executemany` / `fetchone` / `fetchmany` / `fetchall` / 迭代器。
- TLS：驱动支持 TLS 连接参数（关闭证书校验适用于自签名证书开发场景；生产使用受信任 CA）。

---

## 6. 数据安全

### 6.1 备份与恢复（BR）

- **一致快照逻辑备份**：`BACKUP TO <path>` 在单个一致快照上导出完整表目录、全部数据行、索引键与 region 路由元数据，并记录备份点 LSN。产出单文件（magic + version + JSON meta + 分段 CRC32 完整性校验 + 原子 tmp+rename 写入），可跨机器传输。
- **幂等恢复**：`RESTORE FROM <path>` 先校验版本与完整性，再以 "drop-then-recreate" 语义重建同名表（清除旧数据/索引残留、覆盖目录、整个恢复运行在单事务内、可整体回滚）。
- **PITR（时间点恢复）**：`RESTORE FROM <path> TO LSN <n>` 在备份点之后重放 binlog 记录直到目标 LSN（含），提供备份 + binlog 重放的 PITR 基础。
- **分布式备份一致性（T35）**：协调器发起多节点一致备份：PREPARE 收集各节点 LastLSN/TSO 候选 → GlobalLSN = max(all LSN) 作为全局一致点 → DO 阶段各节点以对齐的 global_lsn 做快照（节点超过 G 重试最多 3 轮）；产物保留 v1 格式 + manifest（GlobalLSN / 节点元数据 / 一致性标记）；`RESTORE ... TO LSN G` 驱动每个节点恢复到全局备份点；单节点零触发保持旧 BACKUP/RESTORE 行为不变。

### 6.2 TLS 加密传输（T36）

- 主服务监听层支持 TLS：`--tls-cert` 与 `--tls-key` 同时提供时启用加密监听，否则保持明文监听（行为兼容）。
- 复制端口（`--replica-port`）可另配 `--replica-tls` 启用 TLS。
- **默认 TLS-only 监听**，明文连接被拒绝；`--tls-allow-legacy` 或 `SET PERSIST tls_allow_legacy=on` 允许明文/TLS 共存（AutoListener）。
- 证书管理：`openxdb gen-cert cert.pem key.pem` 生成自签名证书（开发/测试用；生产请使用受信任 CA 签发证书，勿将私钥提交仓库）；`SET PERSIST tls_cert/tls_key` 为重启生效配置。
- 客户端校验：驱动支持 `verify-full` / `verify-ca` 配置；自签名证书开发场景可关闭校验。

```sh
# 生成自签名证书（openssl 方式）
openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 365 -subj "/CN=openxdb"

# 启动 TLS 服务（两个参数必须同时提供，仅提供一个会报错退出）
openxdb start --data-dir ./data --tls-cert cert.pem --tls-key key.pem

# 验证 TLS 握手
openssl s_client -connect 127.0.0.1:7788 -CAfile cert.pem
```

### 6.3 RBAC 与权限

- **用户管理**：`CREATE USER` / `DROP USER [IF EXISTS]` / `ALTER USER` / `SHOW USERS`；口令以 SHA-256 + 随机 salt 哈希落库（无明文），常数时间比对防时序侧信道。
- **表级权限**：`GRANT <priv[,priv...]> ON <table> TO '<user>'` / `REVOKE ... FROM '<user>'`；priv ∈ {SELECT, INSERT, UPDATE, DELETE, ALL}；重复 GRANT/REVOKE 幂等。
- **数据库级与列级权限（M17）**：`GRANT/REVOKE <priv> ON DATABASE <db> TO/FROM '<user>'`、`GRANT/REVOKE <priv> ON <table> (<col>, ...) TO/FROM ...`；角色目标授权。
- **角色系统（M17）**：`CREATE ROLE` / `DROP ROLE [IF EXISTS]` / `SHOW ROLES`、`GRANT ROLE '<role>' TO '<user>'` / `REVOKE ROLE ... FROM '<user>'`、`SHOW GRANTS FOR '<user>'`。
- **权限聚合校验**：表直接授权 ∪ 数据库授权 ∪ 角色继承，SELECT/INSERT/UPDATE/DELETE 聚合检查 + 列级检查。
- **级联清理**：DROP USER / DROP TABLE / DROP ROLE 自动清理相关授权，避免悬空授权。
- **连接鉴权**：server 支持可选连接鉴权（AUTH 握手）；未交换 AUTH 时以默认 root 运行；`AllowLegacy` 开启时旧客户端回退受限 guest 模式，严格模式直接拒绝。

### 6.4 审计日志（T30）

- 审计事件覆盖：连接（connect）、鉴权（auth）、DML、DDL、权限事件。
- 存储：环形缓冲区 + JSON Lines 持久化。
- 查询：`SHOW AUDITLOG`；热开关：`SET audit_log = on/off`。

---

## 7. 运维与可观测

### 7.1 openxdb doctor 健康检查（T20）

```sh
openxdb doctor --data-dir ./data [--addr 127.0.0.1:7788] [--json]
```

检查项：
- 数据目录与 WAL 完整性；
- RocksDB 打开健康；
- `m:tables` / `m:regions` 元数据一致性 & 路由自洽；
- binlog 可读性 & LSN（只读 `PeekLastLSN`）；
- 可选在线端口/PING 与慢查询检查。

输出：人类可读或 `--json` 报告；退出码 0=通过 / 1=警告 / 2=错误。

### 7.2 openxdb stats 实时指标（T20）

```sh
openxdb stats [--addr 127.0.0.1:7788] [--interval 5] [--json]
```

指标：活跃连接数（server 会话计数）、表数量、region 数量与路由分布、慢查询数量、binlog LSN（可选 `--interval` 两次采样计算增长）、表或 `--json` 输出；由 `SHOW STATS` 语句支撑。

### 7.3 Prometheus 指标（M13）

- 指标端口：`metrics_port`（默认 7790），暴露 Prometheus 格式指标。
- 指标来源：`metrics.Snapshot` 同一数据源（连接数、表/region 数、慢查询、LSN 等）。

### 7.4 慢查询（M13）

- 阈值：默认 100ms；`SET slow_query_threshold = <ms>`（<=0 关闭记录）。
- 落盘：默认 `<数据目录>/slow.log`，可配置；`SET slow_query_log = on/off` 热开关。
- 内存：保留最近 N 条（默认 64，`Engine.SetSlowQueryMax(n)` 可调），`SHOW SLOWQUERIES` 查询（新→旧）。
- 记录字段：SQL 文本、用户、开始时间、耗时（毫秒）、结果行数（失败为 0）、错误信息（成功为空串）。
- 低开销设计：阈值判断先于格式化，正常语句零格式化开销。

### 7.5 告警规则（T37）

- 规则模型：`metric` / `op` / `threshold` / `duration_sec` / `severity`，以 JSON 存储。
- 配置：`SET` / `SET PERSIST alert_rules*`。
- 评估引擎：基于同一 `metrics.Snapshot` 数据源，NORMAL/ALERT/RESOLVED 状态机 + 持续时间去抖。
- 通知：logx 环形缓冲区 + 可选 webhook / 文件通道。
- 管理语句：`SHOW ALERT RULES` / `SHOW ALERT STATUS` / `SHOW ALERTS`。
- 评估循环挂接数据库生命周期。

### 7.6 Region 分裂与负载均衡（M7）

- **自动分裂**：写路径（INSERT/UPDATE/DELETE）提交后累计表级写计数；水位达到阈值一半触发真实行数检查（扫描本地 region 物理键空间）；本地 region 行数超过阈值沿数据中点一分为二，同一事务内完成存量数据重路由 + 路由表替换 + 元数据落盘。
- **手动分裂**：`SPLIT REGION`（按数据中点）或显式边界。
- **手动均衡**：`BALANCE`（返回迁移 region 数；均衡后再 BALANCE 返回 0）。
- **自动均衡**：`Engine.SetAutoBalance(enabled, interval)` 周期触发，比较本节点与最闲存活节点 region 数，把最大 region 在线迁移。
- **在线迁移**：迁移期间源路由不变、读写不中断；扫描源 region 物理行 → `REGION_PUSH` 装载目标节点 → 切换本节点路由并落盘；其余存活节点经下一次心跳路由同步。
- 单机模式执行 `BALANCE` 报 `cluster not enabled`。

### 7.7 HA 故障切换（M6）

- **故障检测**：心跳（PING/PONG，默认 2s/6s）超时 → 节点标记 down（OnDown 回调）。
- **Region 自动重指派**：down 节点持有的 region 自动重指派到存活节点（最全副本数据源 + `REGION_PUSH`），替代手动 `ASSIGN REGION`。
- **读 failover**：跨节点读遇 down 自动换新归属（QUERY_REQ 重路由），客户端无感知。
- **复制主从切换**：follower 在旧 master down 后可自动提升（`PromoteToMaster` / `EnableAutoFailover` / `StopAutoFailover`），写路径保持可用。

---

## 8. 分布式能力

### 8.1 复制（M2 + T33）

- **异步复制（async，默认）**：master-follower 架构；commit-hook binlog（独立追加式文件、单调递增 LSN）；replicator push + 每槽位断点续传；follower 幂等应用；PING/PONG 心跳与 offline 检测。
- **半同步复制（semi-sync）**：`SET replication_mode = semi-sync`；等待部分/全部 ACK（在事务锁外）；ACK 超时或无 slave 时降级为异步。
- **全同步复制（sync）**：`SET replication_mode = sync`；等待全部 ACK，无法满足时拒绝伪同步。
- 热切换配置：`SET replication_mode` / `SET replication_ack` / `SET replication_timeout`。

### 8.2 分片（M3）

- RegionInfo 元数据持久化 `m:regions`；Router 半开区间 `[StartKey, EndKey)` 边界语义。
- Region 前缀物理键（`r` + regionID:8B + 内层键）。
- 跨 region 点查/范围/全表查询：区间展开 + 合并。
- 管理：`SPLIT` / `LIST REGIONS` / `LOCATE REGION`；默认单 region 形态保持未分片行为不变。

### 8.3 多节点集群（M4）

- 节点拓扑：`NodeInfo`（NodeID/Addr/Role master|follower/State active|down + 最近存活时间），节点注册表落盘 `m:nodes`，Open 时自动恢复。
- 管理入口：`ADD NODE '<addr>'` / `SHOW NODES`；`openxdb start --cluster-addr <addr>` 集群模式启动。
- 集群路由表：region → 节点；`ASSIGN REGION <id> TO NODE '<nodeID>'` 更新归属并推送数据；`SHOW REGION ROUTES` 一表查看。
- 跨节点读：非本地 region 经 TCP 转发到归属节点执行、取回结果合并排序；Local 语义（Node 为空或等于 selfID）单节点部署零回归。

### 8.4 分布式写 / 2PC（M5）

- 跨节点写原子性：autocommit INSERT/UPDATE/DELETE（含 CSV IMPORT）同时触达本地与远端 region 时以两阶段提交执行。
- 流程：协调器本地应用 → PREPARE 远端参与者（操作持久化 `m:2pc:*` 供崩溃恢复）→ 本地提交 → 幂等 COMMIT 全部参与者。
- 失败语义：任一 PREPARE 失败或本地提交失败即整体 ABORT；错误码 `TXN_PREPARE_FAILED` / `TXN_COMMIT_ABORTED` / `TXN_COMMIT_UNCERTAIN`。
- 协议帧 22-27 扩展 M4 帧；单节点部署永不进入 2PC 路径（零回归）。

### 8.5 分布式事务 / TSO（M8）

- **全局时间戳（TSO）**：64 位物理毫秒 + 24 位逻辑布局；严格单调（时钟回退安全）；批量签发（GetBatch 分配连续区间，远端客户端缓存批次摊销 RTT）；事务边界 Reset 丢弃未消费缓存，保证新快照读取权威序列头。
- **分布式快照隔离**：版本记录 `m:ver:<key>:<commit_ts>`（键优先，每键范围连续）；begin_ts/commit_ts 全局可比过滤 + 跨节点版本透传。
- **2PC 恢复加固**：协调器持久化 CoordRecord（`m:2pc:coord:<txid>`），重启后决定 commit/abort（RecoverCoordinated2PC）并幂等驱动参与者完成（`TXN_COMMIT_UNCERTAIN` 完成）。

### 8.6 跨节点 DDL（T34）

- DDL 元数据经集群 DDL_PUSH 帧广播到所有在线节点；各节点按 TableID 幂等应用。
- 离线/应用失败节点记录 pending，在下次广播前或 `ReplayPendingDDL` 时重放。
- 单节点零触发兼容；与 T33 复制链路协同工作。

### 8.7 分布式备份一致性（T35）

见[第 6 章 6.1](#61-备份与恢复br)。

---

## 9. 性能基准

> 来源：E5 标准基准对齐实验（`docs/experiments/E5_standard_bench_alignment.md`）。E5 实测时采用 **Python PEP 249 驱动等价**方案（OpenXDB 的 MySQL wire protocol 兼容监听自 v1.1.0 起通过 `--mysql-port` 提供，标准 sysbench/TPC-C 官方工具直连能力见对应版本说明）。性能定位为**单机端侧基准**。

### 9.1 sysbench OLTP（8 线程 × 60s，Python 驱动自研协议）

| 负载 | TPS | QPS | errs |
|---|---|---|---|
| read（只读） | 1.73 | 24.27 | 0 |
| write（只写） | 0.93 | 3.73 | 0 |
| read_write（读写混合） | 1.32 | 23.70 | 0 |

### 9.2 TPC-C 小仓（5 仓 × 8 线程 × 120s）

| 指标 | 值 | errs |
|---|---|---|
| tpmC | 13.50 | 0 |
| TPM | 32.50 | 0 |

### 9.3 瓶颈说明

- 当前吞吐瓶颈：**全局会话锁串行** + **逐语句往返**（每条语句经驱动与服务器逐条交互）。
- E5 实验同时修复了共享会话并发 bug，并完成算术表达式 / DISTINCT / DECIMAL 精度等绕过工作。
- 历史基准参考：E3 SQL OLTP 端到端 256.3 TPS（8:2）/ 605.4 TPS（5:5）；E4 稳定性 6 分钟混合负载 307–385 TPS 无退化、重启后 COUNT(*) == 150466；E2 fsync 引擎基准 sync write 747k TPS seq / 198k rand（batch=1000），group-commit 批量相对逐键 fsync 提升 181x。以上为实验记录，非对外承诺。

---

## 10. 已知限制与边界

> 详细清单见 [KNOWN_LIMITATIONS.md](../KNOWN_LIMITATIONS.md)。以下为概要：

1. **SQL 子集**：OpenXDB 实现的是 SQL 子集，并非完整 SQL 标准。`HAVING` 子句、聚合表达式参数（如 `SUM(a+b)`）、`COUNT(DISTINCT expr)` 及 `GROUP BY` 后 `ORDER BY`/`LIMIT` 已支持（v1.2.0，详见 [T41_sql_having_groupby.md](T41_sql_having_groupby.md)）；DDL/DML/查询能力以[第 4 章](#4-sql-参考)为准。
2. **MySQL wire protocol 兼容为子集（自 v1.1.0 起）**：默认使用自研二进制 TCP 协议；可通过 `openxdb start --mysql-port <port>` 启用 MySQL wire protocol 兼容监听，支持 MySQL CLI / go-sql-driver / JDBC 直连常用查询/DML/预处理语句；依赖完整 MySQL 生态扩展（binlog 复制、mysqldump 全部选项等）的工具仍不可直接复用。
3. **全局会话锁性能边界**：当前会话层串行化全局锁是主要吞吐瓶颈（见[第 9 章](#9-性能基准)），高并发写入场景吞吐受限。
4. **行协议限制**：不支持单元格内换行（多行文本参数会被驱动拒绝）；空单元格同时表示 NULL 与空字符串。
5. **类型与精度**：DECIMAL 内部规范化为固定 4 位小数；JSON 导出中 DECIMAL 经 `strconv.ParseFloat` 近似（或保留字符串）。
6. **鉴权与兼容模式**：无 AUTH 握手时连接以默认 root 运行；旧客户端在 `AllowLegacy` 开启时回退受限 guest 模式，严格模式拒绝。
7. **存储过程/触发器为受限子集**：仅覆盖设计文档所定义的语法与语义，不支持完整 SQL/PL 能力。
8. **单机部署下分布式语句受限**：`BALANCE` 等集群语句在单机模式明确报 `cluster not enabled`；跨节点写仅集群模式生效。
9. **分布式能力边界**：跨节点写依赖 2PC 协议帧；复制模式选择（async/semi-sync/sync）通过热配置切换，全同步模式在无法满足 ACK 时拒绝伪同步。
10. **导入兼容性**：mysqldump 导入为文本规则解析，覆盖常见 dump 语法子集；未知类型在严格模式报错、宽松模式跳过该表；单元格解析失败在宽松模式丢弃该行。

---

## 11. FAQ

### Q1：构建时报 cgo 链接错误怎么办？
检查 CGO 环境变量是否正确指向 RocksDB 目录：`CGO_CFLAGS` / `CGO_CXXFLAGS` 指向 `<rocksdb>/include`，`CGO_LDFLAGS` 指向 `<rocksdb-build> -lrocksdb`。Windows 上需追加 `-lshlwapi -lrpcrt4 -lws2_32`（仓库 #cgo 已包含）。推荐直接使用 `scripts/build.ps1` / `scripts/build.sh` 一键构建。不要额外追加 `-lstdc++` / `-lz` 等未声明参数。

### Q2：表名包含关键字（如 `users` 与 `SHOW USERS` 冲突）怎么办？
`SHOW USERS` 按标识符文本匹配实现，`USERS` 未加入关键字表，因此 `CREATE TABLE users` 与 `SHOW USERS` 可以共存，无需特殊处理。若遇到其他关键字冲突，请参考设计文档中的解析规则。

### Q3：IF NOT EXISTS 的幂等语义是什么？
`CREATE TABLE IF NOT EXISTS` / `DROP TABLE IF EXISTS` / `DROP USER IF EXISTS` / `DROP ROLE IF EXISTS` 在对象不存在时静默成功、不报错；对象存在时按正常语义执行（建表报错或删除成功）。

### Q4：如何备份与恢复？
`BACKUP TO '<path>'` 产出单文件一致快照备份；`RESTORE FROM '<path>'` 幂等恢复（同名表 drop-then-recreate）；需要时间点恢复时用 `RESTORE FROM '<path>' TO LSN <n>` 重放 binlog 到目标 LSN。分布式集群使用 T35 协调器发起多节点一致备份。

### Q5：如何搭建集群？
每个节点用 `openxdb init --data-dir <dir>` 初始化，再用 `openxdb start --cluster-addr <addr>` 集群模式启动；节点间通过 `ADD NODE '<addr>'` 注册，`ASSIGN REGION <id> TO NODE '<nodeID>'` 指派 region 归属，`SHOW NODES` / `SHOW REGION ROUTES` 查看状态。

### Q6：驱动如何连接？
JDBC：`jdbc:openxdb://host:7788`；Python：`openxdb.connect("host", 7788)`。两驱动均基于自研协议，参数化使用 `?` 占位符（客户端侧安全转义）。TLS 场景在 JDBC 配置证书校验模式、Python 传入 TLS 连接参数。

### Q7：如何用 MySQL 客户端连接？
默认端口（7788）为自研二进制 TCP 协议，MySQL 官方客户端无法直接连接；请使用内置 REPL（`openxdb repl`）或 JDBC / Python 驱动。自 v1.1.0 起，可通过 `openxdb start --mysql-port 3306` 启用 **MySQL wire protocol 兼容监听**，即可使用 `mysql -h127.0.0.1 -P3306 -uroot` 或 go-sql-driver / MySQL JDBC 直连。

### Q8：如何查看慢查询？
`SET slow_query_threshold = <ms>` 设置阈值（默认 100ms），执行语句后 `SHOW SLOWQUERIES` 查看最近记录；`SET slow_query_log = on` 开启落盘（默认 `<数据目录>/slow.log`）。

### Q9：如何开启 TLS？
生成证书后以 `openxdb start --data-dir ./data --tls-cert cert.pem --tls-key key.pem` 启动（两参数必须同时提供）；默认 TLS-only 拒绝明文；`SET PERSIST tls_allow_legacy=on` 允许明文/TLS 共存。复制端口用 `--replica-tls` 启用。

### Q10：性能数据可信吗？为什么 TPS 不高？
所有性能数据来自 E5 标准基准对齐实验（`docs/experiments/E5_standard_bench_alignment.md`）真实实测（8 线程 × 60s / 120s，0 错误）。当前吞吐瓶颈为全局会话锁串行 + 逐语句往返；项目定位为单机端侧基准，功能完整性优先，性能优化持续进行中。

---

*本文档由 OpenXDB 项目 README.md 与 docs/ 目录设计文档整理而成，v1.0.0。*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

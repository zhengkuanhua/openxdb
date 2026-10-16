---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_7c8224e9c91211f18b7f525400393706
    ReservedCode1: X+16cVZ0Pb1pFApB3IAV1N+5kYy6M4irDsyedOS8PxY5O0vzxvNImRS1iD0RrhOtXERZtLWJxOU+c1eOFtWIroHUJhXDq8gVAax38hKNHr3Bzc5ZQevLooRGfzYZHnyO5NlBBNVtbeUEOQMV+zDsmXOe4p+b5e9WkUfAijC1S7u8EzMStpXnKSB4lXs=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_7c8224e9c91211f18b7f525400393706
    ReservedCode2: X+16cVZ0Pb1pFApB3IAV1N+5kYy6M4irDsyedOS8PxY5O0vzxvNImRS1iD0RrhOtXERZtLWJxOU+c1eOFtWIroHUJhXDq8gVAax38hKNHr3Bzc5ZQevLooRGfzYZHnyO5NlBBNVtbeUEOQMV+zDsmXOe4p+b5e9WkUfAijC1S7u8EzMStpXnKSB4lXs=
---

# OpenXDB Product Manual (English)

> Version: v1.0.0
> Last updated: 2026-10-16
> Positioning: single-node edge benchmark relational database kernel. All features and data in this manual are based on real content from README.md and docs/ design documents — no fabricated parameters, numbers, or commands.

---

## Table of Contents

1. [Product Overview & Positioning](#1-product-overview--positioning)
2. [Quick Start](#2-quick-start)
3. [Installation & Deployment](#3-installation--deployment)
4. [SQL Reference](#4-sql-reference)
5. [Client Drivers](#5-client-drivers)
6. [Data Security](#6-data-security)
7. [Operations & Observability](#7-operations--observability)
8. [Distributed Capabilities](#8-distributed-capabilities)
9. [Performance Benchmark](#9-performance-benchmark)
10. [Known Limitations & Boundaries](#10-known-limitations--boundaries)
11. [FAQ](#11-faq)

---

## 1. Product Overview & Positioning

OpenXDB is a **from-scratch** single-node/distributed relational database kernel written in Go, with a storage engine built on top of RocksDB (accessed through a thin cgo bridge). The kernel fully includes WAL (write-ahead log), transactions, SQL parsing & execution, replication, sharding, 2PC, HA (high availability), backup & restore — evolving from a learning/experimentation kernel into one with complete distributed capabilities.

### 1.1 Positioning Boundaries

- OpenXDB speaks a **self-developed binary TCP protocol** (`pkg/server`) and is **not a MySQL wire protocol** implementation. It therefore cannot directly interoperate with MySQL ecosystem tools (e.g., standard sysbench, mysqldump beyond the built-in import, binlog tools).
- Performance positioning is **single-node edge benchmark**: the kernel implements the full functional chain, and performance optimization is still ongoing; measured throughput is presented in [Chapter 9](#9-performance-benchmark).
- OpenXDB does not position itself against TiDB, TDSQL, or MySQL. It is an independently implemented kernel for learning, experimentation, and edge scenarios.

### 1.2 Core Capabilities

| Capability | Description |
|---|---|
| Storage engine | RocksDB-backed KV storage (cgo bridge `pkg/storage/rocksdb`) + pure-Go in-memory B+Tree (`pkg/storage/btree`, reference implementation & comparator) |
| WAL | Append-only write-ahead log with replay recovery (`pkg/wal`) |
| Transactions | Local ACID transactions with snapshot isolation; read-your-writes merge, tombstones, point/range scan (`pkg/txn`) |
| SQL subset | DDL/DML/query/ops statements, see [Chapter 4](#4-sql-reference) |
| Secondary indexes | Backfill on creation, automatic maintenance on DML, query optimization for equality/range scans and ORDER BY |
| Replication | master-follower async replication (M2), with async / semi-sync / sync modes (T33) |
| Sharding | Logical sharding: RegionInfo metadata + Router half-open `[StartKey, EndKey)` + region-prefixed physical keys (M3) |
| Multi-node cluster | Node topology, cluster route table (region → node), cross-node SELECT forwarding & merge (M4) |
| Distributed write / 2PC | Cross-node write atomicity via two-phase commit (M5) |
| HA / Failover | Heartbeat-based failure detection, automatic region re-assignment, read failover, replication master failover (M6) |
| Region split & load balancing | Write-path auto-split, manual/periodic load balancing, online migration (M7) |
| Distributed transactions / TSO | Global monotonic timestamp, distributed snapshot isolation, 2PC recovery hardening (M8) |
| Backup & Restore (BR) | Consistent-snapshot logical backup, idempotent restore, LSN-based PITR |
| Client drivers | JDBC & Python drivers (T19) wrapping the pkg/server protocol |
| Ops toolchain | `openxdb doctor` / `stats`, one-click deploy scripts (T20) |
| Security | TLS transport encryption (T36), RBAC roles & privileges (M11/M17), audit log (T30) |
| Observability | Slow query log, Prometheus metrics, status view, alert rules (M13/T37) |
| Ecosystem interop | CSV/JSON Lines/Parquet export (M19), mysqldump-compatible import (M14) |

### 1.3 Architecture Layers

| Layer | Package | Responsibility |
|---|---|---|
| Protocol + CLI | `cmd/openxdb`, `pkg/server` | TCP server, REPL, binary protocol framing |
| SQL | `pkg/sql` | Recursive-descent parser, executor (project/filter/sort/aggregate), metadata catalog, secondary index layer |
| Transaction | `pkg/txn` | Begin/commit/rollback, snapshot isolation, read-your-writes |
| Storage | `pkg/storage` | KV interface, key encoding; RocksDB engine and B+Tree engine |
| WAL | `pkg/wal` | Append-only log, replay recovery |
| Replication | `pkg/replication`, `pkg/txn`, `pkg/db` | Binlog store, master replicator (push + heartbeat), follower idempotent apply, commit-hook wiring |
| Sharding | `pkg/sharding`, `pkg/sql`, `pkg/db` | RegionInfo metadata, Router key→region mapping, region-prefixed physical keys, cross-region query merge, split management |
| Cluster | `pkg/cluster`, `pkg/sql`, `pkg/db` | Node registry (m:nodes), handshake/liveness, cluster route table (region→node), cross-node SELECT forwarding + merge, region data load (REGION_PUSH) |
| Distributed write / 2PC | `pkg/cluster`, `pkg/sql/2pc.go`, `pkg/db` | Two-phase commit coordinator, participant prepare persistence, idempotent commit/abort, crash recovery |
| HA / Failover | `pkg/cluster`, `pkg/sql/ha.go`, `pkg/db/ha.go` | Heartbeat timeout → node down, automatic region re-assignment, read failover, replication master failover |
| Split & Balance | `pkg/sql/balance.go`, `pkg/cluster`, `pkg/db` | Write-path auto-split, manual/auto load balancing, cluster-not-enabled guard |
| Global Timestamp (TSO) | `pkg/tso`, `pkg/cluster/tso.go`, `pkg/db` | Global monotonic timestamp (64-bit ms + logical layout), batch issuance, overflow protection, transaction-boundary Reset |
| Snapshot Isolation (distributed) | `pkg/cluster/txn2pc.go`, `pkg/sql/executor.go`, `pkg/db` | Version records (m:ver), begin_ts/commit_ts global filtering, cross-node version passthrough |
| Backup / Restore | `pkg/sql/backup.go`, `pkg/sql`, `pkg/db` | Consistent-snapshot logical backup (single file, version + CRC32), idempotent restore, binlog replay to target LSN |
| Data dir | `pkg/db` | Initialization, engine + WAL + txn manager wiring, crash recovery |

### 1.4 Key Encoding

```
physical key: r{regionID:8B}{innerKey}
row key:    s{tableID:8B}{pk}                          (inner, M1)
index key:  i{tableID:8B}{indexID:8B}{idxVal}p{pk}    (inner, T6)
```

Every stored key carries a region prefix (`r` + 8-byte big-endian regionID); the inner key keeps the M1/T6 layout, so keys stay contiguous within a region and region boundaries are explicit. Index values use sort-friendly encoding: INT flips the sign bit (`v ^ 0x8000000000000000`), TEXT stays raw bytes, so byte order equals value order.

---

## 2. Quick Start

### 2.1 Build Requirements

- Go 1.22+
- A C/C++ toolchain for cgo (e.g., MinGW-w64 on Windows, gcc/clang on Linux/macOS)
- RocksDB (tested with v11.8.1 source build, no third-party compression libs)

### 2.2 Build (Windows + MinGW + Go)

One-shot reproducible build scripts (auto-detect RocksDB install, then `go build` + optional `go test`):

```powershell
# Windows: build bin\openxdb.exe, verify go build ./..., run go test ./...
powershell -ExecutionPolicy Bypass -File scripts\build.ps1 -RunTests
# explicit paths:
powershell -ExecutionPolicy Bypass -File scripts\build.ps1 -RocksSrcDir C:\rocksdb-src -RocksBuildDir C:\rocksdb-build -RunTests
```

```sh
# Linux/macOS:
./scripts/build.sh --test
./scripts/build.sh --rocks-src /path/to/rocksdb-11.8.1 --rocks-build /path/to/librocksdb-dir --test
```

Manual cgo setup (equivalent, for custom toolchains):

```sh
export CGO_CFLAGS=-I<rocksdb>/include
export CGO_CXXFLAGS=-I<rocksdb>/include
export CGO_LDFLAGS=-L<rocksdb-build> -lrocksdb   # Windows: add -lshlwapi -lrpcrt4 -lws2_32

go build ./...
```

`pkg/storage/rocksdb` declares per-platform `#cgo` link flags (Windows `-lwinpthread -lshlwapi -lrpcrt4 -lws2_32`, Linux `-lstdc++ -lpthread`, Darwin `-lstdc++`), so only the RocksDB include/lib dirs need to be supplied via `CGO_CFLAGS` / `CGO_LDFLAGS`.

> **Note**: the repository `#cgo` flags include `-static-libstdc++ -static-libgcc`; do not add undeclared link flags such as `-lstdc++` / `-lz`, which can cause link conflicts.

### 2.3 Start REPL / TCP Server

```sh
# initialize a data directory
openxdb init --data-dir ./data

# local REPL
openxdb repl --data-dir ./data

# TCP server (default port 7788)
openxdb start --data-dir ./data [--port 7788]

# ops: health check + live metrics
openxdb doctor --data-dir ./data [--addr 127.0.0.1:7788] [--json]
openxdb stats [--addr 127.0.0.1:7788] [--interval 5] [--json]
```

### 2.4 Create Table, Insert, Query Example

```sql
CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id));
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35);
CREATE INDEX idx_age ON users (age);
SELECT name FROM users WHERE age >= 25 AND age < 35 ORDER BY age;
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE id = 3;
```

### 2.5 Run Tests

```sh
go test ./...
```

All packages pass: `cluster` (node handshake, query forwarding, heartbeat down, region push route learning, idempotent node re-register), `db` (replication integration, M4 two-node integration, M5 2PC integration, M6 HA integration, M7 split/balance integration, M8 distributed-txn integration, BR backup/restore integration), `replication`, `server`, `sharding`, `sql`, `storage`, `storage/btree`, `storage/rocksdb`, `tso`, `txn`, `wal`.

---

## 3. Installation & Deployment

### 3.1 Configuration File openxdb.conf

OpenXDB uses an INI-style configuration file (see `docs/openxdb.conf.example`). Core sections:

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

- `[server]`: `data_dir` data directory, `port` main service port (default 7788), `replica_port` replication port, `metrics_port` Prometheus metrics port, `tls_cert`/`tls_key` TLS certificate and private key paths.
- `[sql]`: `query_cache` query cache switch, `slow_query_threshold` slow query threshold in milliseconds (default 100ms), `backup_dir` backup directory.

Start with a config file:

```sh
openxdb start --config openxdb.conf
```

> Tip: hot-reloadable items (e.g., `query_cache`, `slow_query_threshold`, `slow_query_log`) can also be changed at runtime via `SET` / `SET PERSIST` — see [ops statements in Chapter 4](#47-ops-statements).

### 3.2 One-Click Deploy Scripts

The repository ships Windows and Linux/macOS deploy scripts:

```powershell
# Windows
powershell -ExecutionPolicy Bypass -File scripts\deploy.ps1 -DataDir C:\openxdb\data -Port 7788
```

```sh
# Linux/macOS
./scripts/deploy.sh --data-dir /var/lib/openxdb --port 7788
```

Script flow: parameterized init (`-DataDir` / `-Port` / `--data-dir` / `--port`) → start → `openxdb doctor` health self-check → deployment summary.

### 3.3 Startup Modes

| Command | Description |
|---|---|
| `openxdb init --data-dir <dir>` | Initialize a data directory |
| `openxdb repl --data-dir <dir>` | Local interactive REPL |
| `openxdb start --data-dir <dir> [--port 7788]` | Start TCP server (default port 7788) |
| `openxdb start --cluster-addr <addr>` | Start in cluster mode (node ID via `--node-id` or auto-generated) |
| `openxdb doctor` | Health check (exit codes 0=pass / 1=warn / 2=error) |
| `openxdb stats` | Live metrics |
| `openxdb gen-cert cert.pem key.pem` | Generate self-signed TLS certificate |

---

## 4. SQL Reference

> All syntax in this chapter comes from README.md and T* design documents under docs/. The SQL capability list includes T41/T42 additions (IF NOT EXISTS, CREATE TABLE AS SELECT, INSERT INTO SELECT).

### 4.1 Data Types

| Type | Description |
|---|---|
| `INT` | Integer |
| `TEXT` | Text |
| `DATE` | Date |
| `DECIMAL` | Fixed-point decimal (normalized to fixed 4 decimal places internally) |
| `BLOB` | Binary large object |

### 4.2 DDL

#### CREATE TABLE

```sql
-- Basic create (inline PRIMARY KEY is equivalent to table-level PRIMARY KEY)
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

-- IF NOT EXISTS: idempotent create (T41)
CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT);

-- CREATE TABLE AS SELECT (CTAS, T41): create a table from query results
CREATE TABLE adult_users AS SELECT id, name FROM users WHERE age >= 18;
```

#### CREATE INDEX

```sql
CREATE INDEX idx_age ON users (age);
```

#### Views / Materialized Views / Triggers / Stored Procedures

```sql
-- View
CREATE VIEW v_adult AS SELECT id, name FROM users WHERE age >= 18;
DROP VIEW v_adult;

-- Materialized view
CREATE MATERIALIZED VIEW mv_adult AS SELECT id, name FROM users WHERE age >= 18;
REFRESH MATERIALIZED VIEW mv_adult;
DROP MATERIALIZED VIEW mv_adult;

-- Trigger
CREATE TRIGGER trg_after_insert AFTER INSERT ON users FOR EACH ROW ...;
DROP TRIGGER trg_after_insert;
SHOW TRIGGERS;

-- Stored procedure
CREATE PROCEDURE p_get_user(IN uid INT) ...;
CALL p_get_user(1);
DROP PROCEDURE p_get_user;
SHOW PROCEDURES;
```

#### Users / Roles

```sql
CREATE USER 'alice' IDENTIFIED BY 'pwd123';        -- duplicate name errors
DROP USER [IF EXISTS] 'alice';                     -- missing errors; IF EXISTS silent
SHOW USERS;                                        -- sorted by name
ALTER USER 'alice' IDENTIFIED BY 'newpwd';         -- reset password

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

- `DROP TABLE IF EXISTS`: idempotent when the table is missing (T21 fix).
- `DROP USER IF EXISTS` / `DROP ROLE IF EXISTS`: idempotent.
- Dropping users/tables/roles cascades cleanup of related grants.

### 4.3 DML

#### INSERT

```sql
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25);
INSERT INTO users (id, name) VALUES (3, 'carol');
```

#### INSERT INTO ... SELECT (T42)

```sql
-- Insert query results into a target table (column list optional; full SELECT source compatibility; atomic transaction + rollback)
INSERT INTO archive_users SELECT * FROM users WHERE age >= 30;
INSERT INTO archive_users (id, name) SELECT id, name FROM users WHERE age >= 30;
```

- Self-insert uses snapshot semantics (self-insert snapshot semantics).
- `REPLACE INTO ... SELECT` is also supported.

#### REPLACE / UPSERT

```sql
-- REPLACE: on primary-key conflict, delete then insert
REPLACE INTO users VALUES (1, 'alice2', 31);

-- INSERT ... ON DUPLICATE KEY UPDATE: update on conflict
INSERT INTO users VALUES (1, 'alice', 30)
  ON DUPLICATE KEY UPDATE age = 31;
```

#### UPDATE / DELETE

```sql
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE id = 3;
```

### 4.4 Query (Full SELECT Capabilities)

```sql
-- Basic query + WHERE
SELECT * FROM users WHERE age >= 25;

-- ORDER BY & pagination (LIMIT/OFFSET also works on GROUP BY/aggregate results, T40)
SELECT name FROM users ORDER BY age LIMIT 10 OFFSET 5;

-- GROUP BY + aggregates (MAX/MIN/COUNT/SUM/AVG; MAX/MIN added in T40)
SELECT age, COUNT(*), SUM(amount), AVG(amount) FROM orders GROUP BY age;
SELECT MAX(age), MIN(age) FROM users;

-- DISTINCT (T40)
SELECT DISTINCT age FROM users;

-- JOIN
SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;

-- Subquery
SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100);

-- CTE (M10)
WITH adult AS (SELECT id, name FROM users WHERE age >= 18)
SELECT * FROM adult;

-- Window functions (M10)
SELECT name, age, ROW_NUMBER() OVER (ORDER BY age) AS rn FROM users;

-- BETWEEN / IN / LIKE
SELECT * FROM users WHERE age BETWEEN 25 AND 35;
SELECT * FROM users WHERE name IN ('alice', 'bob');
SELECT * FROM users WHERE name LIKE 'a%';

-- CASE WHEN & expressions (arithmetic expressions added in T40)
SELECT name, CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END AS bucket FROM users;
SELECT id, amount * 2 AS double_amount FROM orders;
SELECT -age AS neg_age FROM users;
```

### 4.5 Transactions

```sql
BEGIN;
INSERT INTO users VALUES (4, 'dave', 40);
UPDATE users SET age = 41 WHERE id = 4;
COMMIT;

-- or roll back
BEGIN;
DELETE FROM users WHERE id = 4;
ROLLBACK;
```

Transaction characteristics: local ACID + snapshot isolation; read-your-writes merge, tombstones, point/range scan.

### 4.6 Data Import / Export

```sql
-- CSV import (P1)
IMPORT INTO users FROM '/path/users.csv';

-- mysqldump-compatible import (M14, T27): parses mysqldump CREATE TABLE + INSERT statements
IMPORT MYSQLDUMP FROM '/path/dump.sql' [BATCH 100] [IGNORE ERRORS];

-- Export (P1 + M19 ecosystem export)
EXPORT TABLE users TO '/path/users.csv';                        -- equivalent to FORMAT CSV
EXPORT TABLE users TO '/path/users.csv' FORMAT CSV;
EXPORT TABLE users TO '/path/users.jsonl' FORMAT JSON;          -- JSON Lines
EXPORT TABLE users TO '/path/users.parquet' FORMAT PARQUET;     -- Parquet v1
```

- CSV: for spreadsheet tools.
- JSON (JSON Lines): streaming object sequence, one record per line, suitable for log pipelines / data-lake ingestion / ETL.
- Parquet: columnar binary storage produced by a self-contained minimal Parquet v1 writer (single Row Group, PLAIN/UNCOMPRESSED encoding), directly consumable by Spark/Hive/DuckDB/Polars.

### 4.7 Ops Statements

#### SHOW Statements

```sql
SHOW TABLES;                          -- list all tables
SHOW INDEX FROM users;                -- show indexes of a table
SHOW VIEWS;                           -- list views
SHOW USERS;                           -- list users
SHOW ROLES;                           -- list roles
SHOW GRANTS FOR 'alice';              -- show grants of a user
SHOW TRIGGERS;                        -- list triggers
SHOW PROCEDURES;                      -- list stored procedures
SHOW SLOWQUERIES;                     -- slow queries (newest first; columns: time|user|duration_ms|rows|error|sql)
SHOW STATS;                           -- service status metrics
SHOW CONFIG;                          -- show current configuration
SHOW PERSISTED CONFIG;                -- show persisted configuration
SHOW AUDITLOG;                        -- audit log
SHOW ALERT RULES;                     -- alert rules
SHOW ALERT STATUS;                    -- alert status
SHOW ALERTS;                          -- alert events
SHOW NODES;                           -- cluster nodes
SHOW REGION ROUTES;                   -- region routes (region/table/start/end/node/local)
LIST REGIONS;                         -- list regions
LOCATE REGION <key>;                  -- locate the region containing a key
```

#### EXPLAIN

```sql
EXPLAIN <SELECT>;
```

#### SET / SET PERSIST

```sql
SET slow_query_threshold = 100;       -- slow query threshold (ms; <=0 disables recording)
SET slow_query_log = on;              -- slow query file logging switch
SET query_cache = on;                 -- query cache switch
SET replication_mode = async;         -- replication mode: async / semi-sync / sync
SET replication_ack = 1;              -- semi-sync ACK count
SET replication_timeout = 3000;       -- ACK timeout (ms)
SET alert_rules = '...';              -- alert rules (JSON)
SET audit_log = on;                   -- audit log hot switch
SET PERSIST slow_query_threshold = 100;  -- persist configuration (restart-effective class)
SET PERSIST tls_cert = 'cert.pem';    -- TLS certificate (restart-effective)
SET PERSIST tls_key = 'key.pem';      -- TLS private key (restart-effective)
SET PERSIST tls_allow_legacy = on;    -- allow plaintext/TLS coexistence listener
SET @var = 1;                         -- user variable
SELECT @var;                          -- read user variable
```

#### BACKUP / RESTORE

```sql
BACKUP TO '/path/backup.bk';                       -- consistent-snapshot logical backup
RESTORE FROM '/path/backup.bk';                    -- idempotent restore (same-named tables drop-then-recreate)
RESTORE FROM '/path/backup.bk' TO LSN 12345;       -- PITR: replay binlog after backup point up to target LSN
```

#### PREPARE / EXECUTE / DEALLOCATE (M18)

```sql
PREPARE stmt1 FROM 'SELECT * FROM users WHERE id = ?';
EXECUTE stmt1 USING 1;                             -- bind user variables or literals
EXECUTE stmt1 USING @var;
DEALLOCATE PREPARE stmt1;
```

- Session-scoped cache: re-PREPARE of the same name overwrites; EXECUTE/DEALLOCATE of unknown names errors; argument count must match placeholder count.

#### information_schema Read-Only Views (M18)

```sql
SELECT * FROM information_schema.schemata;
SELECT * FROM information_schema.tables;
SELECT * FROM information_schema.columns;
SELECT * FROM information_schema.statistics;
```

- Read-only virtual database: case-insensitive column projection, `WHERE <col> = <literal>` equality filters (AND-combinable), ORDER BY, qualified `default.information_schema.tables` FROM; any CREATE/DROP/INSERT/UPDATE targeting the virtual library is rejected.

---

## 5. Client Drivers

OpenXDB ships JDBC and Python standard client drivers (T19), both based on the self-developed binary TCP protocol (`pkg/server`) — **not MySQL wire protocol**.

### 5.1 Protocol Notes

- Wire protocol: UTF-8 line protocol; each command is one line ending with `\n`; the server replies line by line.
- Reply forms: simple confirmation (OK/PONG/BYE), affected rows (N rows affected), text table (header row + separator + data rows + trailing N rows), error (ERR <message>).
- Empty cells map uniformly to NULL.
- Multi-line text is unsupported (`\n` / `\r` in parameterized string values are rejected by the driver).
- Parameterization: the server protocol has no prepared-statement binary parameter channel; both drivers safely escape `?` placeholders client-side.

### 5.2 JDBC Driver

- Location: `drivers/jdbc`, Maven project, Java 8+, JUnit 5.
- Registration: static block `DriverManager.registerDriver(new OpenXDBDriver())` + `META-INF/services/java.sql.Driver` auto-registration.
- URL: `jdbc:openxdb://host:port` (default port 7788); `user`/`password` properties are accepted but ignored by default (runs as root when no AUTH handshake is exchanged).
- Core classes: `OpenXDBDriver` / `OpenXDBConnection` / `OpenXDBStatement` / `OpenXDBPreparedStatement` (client-side `?` placeholder substitution) / `OpenXDBResultSet` (forward-only, read-only) / `OpenXDBProtocol` (socket connection with 5s timeout).
- Transactions: `setAutoCommit(false)` triggers a session transaction; sends BEGIN automatically if not started before execution.
- TLS: the driver supports TLS connections (`InsecureTLSConfig` can disable certificate verification for dev; use verify-full / verify-ca in production).

### 5.3 Python Driver

- Location: `drivers/python`, PEP 249 compliant.
- Metadata: `apilevel="2.0"`, `threadsafety=1`, `paramstyle="qmark"`, `__version__="0.10.0"`.
- Module-level `connect()`, `DEFAULT_PORT`; PEP 249 style error hierarchy.

```python
import openxdb
conn = openxdb.connect("127.0.0.1", 7788)
cur = conn.cursor()
cur.execute("SELECT id, name FROM users WHERE id > ?", (1,))
for row in cur.fetchall():
    print(row)
conn.close()
```

- `Connection`: `cursor()` / `ping()` / `begin()` / `commit()` / `rollback()` / `close()`.
- `Cursor`: `execute` / `executemany` / `fetchone` / `fetchmany` / `fetchall` / iterator.
- TLS: the driver supports TLS connection parameters (disable certificate verification for self-signed dev certs; use a trusted CA in production).

---

## 6. Data Security

### 6.1 Backup & Restore (BR)

- **Consistent-snapshot logical backup**: `BACKUP TO <path>` snapshots the full table catalog, all data rows, index keys, and region routing metadata on a single consistent snapshot, plus the backup-point LSN. Produces a single transportable file (magic + version + JSON meta + per-segment CRC32 integrity checks + atomic tmp+rename write).
- **Idempotent restore**: `RESTORE FROM <path>` validates version/integrity first, then rebuilds tables/data/indexes/routing with "drop-then-recreate" semantics for same-named tables (stale rows/index residue cleared, catalog overwritten, whole restore runs in one transaction with full rollback).
- **PITR (point-in-time recovery)**: `RESTORE FROM <path> TO LSN <n>` replays binlog records after the backup point up to the target LSN (inclusive) — the backup + binlog-replay PITR foundation.
- **Distributed backup consistency (T35)**: a coordinator initiates multi-node consistent backup: PREPARE collects per-node LastLSN/TSO candidates → GlobalLSN = max(all LSN) as the global consistency point → DO phase snapshots each node with the aligned global_lsn (retry up to 3 rounds if a node exceeds G); artifacts keep v1 format + a manifest (GlobalLSN / node metadata / consistency marker); `RESTORE ... TO LSN G` drives every node to the global backup point; single-node zero-trigger keeps legacy BACKUP/RESTORE intact.

### 6.2 TLS Transport Encryption (T36)

- The main service listener supports TLS: when `--tls-cert` and `--tls-key` are both provided, encrypted listening is enabled; otherwise plaintext (behavior-compatible).
- The replica port (`--replica-port`) can be separately enabled with `--replica-tls`.
- **TLS-only listener by default**, plaintext rejected; `--tls-allow-legacy` or `SET PERSIST tls_allow_legacy=on` enables plaintext/TLS coexistence (AutoListener).
- Certificate management: `openxdb gen-cert cert.pem key.pem` generates a self-signed certificate (dev/test only; use a trusted-CA-signed certificate in production and never commit private keys); `SET PERSIST tls_cert/tls_key` are restart-effective configs.
- Client verification: drivers support `verify-full` / `verify-ca`; disable verification for self-signed dev certs.

```sh
# Generate a self-signed certificate (openssl method)
openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 365 -subj "/CN=openxdb"

# Start TLS service (both flags required; providing only one errors out)
openxdb start --data-dir ./data --tls-cert cert.pem --tls-key key.pem

# Verify TLS handshake
openssl s_client -connect 127.0.0.1:7788 -CAfile cert.pem
```

### 6.3 RBAC & Privileges

- **User management**: `CREATE USER` / `DROP USER [IF EXISTS]` / `ALTER USER` / `SHOW USERS`; passwords are stored as SHA-256 + per-user random salt hashes (no plaintext), with constant-time comparison against timing side channels.
- **Table-level privileges**: `GRANT <priv[,priv...]> ON <table> TO '<user>'` / `REVOKE ... FROM '<user>'`; priv ∈ {SELECT, INSERT, UPDATE, DELETE, ALL}; repeated GRANT/REVOKE are idempotent.
- **Database-level & column-level privileges (M17)**: `GRANT/REVOKE <priv> ON DATABASE <db> TO/FROM '<user>'`, `GRANT/REVOKE <priv> ON <table> (<col>, ...) TO/FROM ...`; role-targeted grants.
- **Role system (M17)**: `CREATE ROLE` / `DROP ROLE [IF EXISTS]` / `SHOW ROLES`, `GRANT ROLE '<role>' TO '<user>'` / `REVOKE ROLE ... FROM '<user>'`, `SHOW GRANTS FOR '<user>'`.
- **Aggregated privilege checks**: table direct ∪ database ∪ role inheritance for SELECT/INSERT/UPDATE/DELETE, plus column-level checks.
- **Cascade cleanup**: DROP USER / DROP TABLE / DROP ROLE automatically clean related grants to avoid dangling authorizations.
- **Connection auth**: the server supports optional connection auth (AUTH handshake); without it the connection runs as the default root user; when `AllowLegacy` is enabled legacy clients fall back to a restricted guest mode, or are rejected in strict mode.

### 6.4 Audit Log (T30)

- Audited events: connect, auth, DML, DDL, privilege events.
- Storage: ring buffer + JSON Lines persistence.
- Query: `SHOW AUDITLOG`; hot switch: `SET audit_log = on/off`.

---

## 7. Operations & Observability

### 7.1 openxdb doctor Health Check (T20)

```sh
openxdb doctor --data-dir ./data [--addr 127.0.0.1:7788] [--json]
```

Checks:
- Data directory & WAL integrity;
- RocksDB open health;
- `m:tables` / `m:regions` metadata consistency & route self-consistency;
- Binlog readability & LSN (read-only `PeekLastLSN`);
- Optional online port/PING and slow-query checks.

Output: human-readable or `--json` report; exit codes 0=pass / 1=warn / 2=error.

### 7.2 openxdb stats Live Metrics (T20)

```sh
openxdb stats [--addr 127.0.0.1:7788] [--interval 5] [--json]
```

Metrics: active connections (server session counter), table count, region count & route distribution, slow query count, binlog LSN (optional two-sample growth via `--interval`), table or `--json` output; backed by the `SHOW STATS` statement.

### 7.3 Prometheus Metrics (M13)

- Metrics port: `metrics_port` (default 7790), exposing Prometheus-format metrics.
- Metrics source: same `metrics.Snapshot` data source (connections, table/region counts, slow queries, LSN, etc.).

### 7.4 Slow Queries (M13)

- Threshold: default 100ms; `SET slow_query_threshold = <ms>` (<=0 disables recording).
- File logging: default `<data_dir>/slow.log`, configurable; `SET slow_query_log = on/off` hot switch.
- In-memory: keeps the most recent N records (default 64, adjustable via `Engine.SetSlowQueryMax(n)`); query via `SHOW SLOWQUERIES` (newest first).
- Record fields: SQL text, user, start time, duration (ms), result rows (0 on failure), error message (empty on success).
- Low-overhead design: threshold check before formatting; normal statements incur zero formatting cost.

### 7.5 Alert Rules (T37)

- Rule model: `metric` / `op` / `threshold` / `duration_sec` / `severity`, stored as JSON.
- Configuration: `SET` / `SET PERSIST alert_rules*`.
- Evaluation engine: over the same `metrics.Snapshot` data source, NORMAL/ALERT/RESOLVED state machine + duration debounce.
- Notification: logx ring buffer + optional webhook / file channel.
- Management statements: `SHOW ALERT RULES` / `SHOW ALERT STATUS` / `SHOW ALERTS`.
- Evaluation loop wired to the database lifecycle.

### 7.6 Region Split & Load Balancing (M7)

- **Auto-split**: the write path (INSERT/UPDATE/DELETE) accumulates per-table write counters after commits; at half the row threshold a real row-count check runs (scans the local region's physical key space); a local region exceeding the threshold is split in half along its data midpoint inside the same transaction (existing rows re-routed, route table replaced, metadata persisted).
- **Manual split**: `SPLIT REGION` (by data midpoint) or explicit boundaries.
- **Manual balance**: `BALANCE` (returns number of migrated regions; a second BALANCE returns 0).
- **Auto balance**: `Engine.SetAutoBalance(enabled, interval)` periodic trigger; compares local region counts with the least-loaded live node and migrates the largest region there.
- **Online migration**: source route unchanged during migration, reads/writes uninterrupted; scan source region rows → `REGION_PUSH` loads the target → switch local route and persist; other live nodes learn the new owner via the next heartbeat route sync.
- Single-node mode `BALANCE` reports `cluster not enabled`.

### 7.7 HA Failover (M6)

- **Failure detection**: heartbeat (PING/PONG, default 2s/6s) timeout → node marked down (OnDown callback).
- **Automatic region re-assignment**: regions owned by the down node are re-assigned to a live node (fullest replica scan + `REGION_PUSH`), replacing manual `ASSIGN REGION`.
- **Read failover**: cross-node reads transparently re-route to the new owner (QUERY_REQ re-routed), no client error.
- **Replication master failover**: follower auto-promotes when the old master is down (`PromoteToMaster` / `EnableAutoFailover` / `StopAutoFailover`), keeping the write path available.

---

## 8. Distributed Capabilities

### 8.1 Replication (M2 + T33)

- **Async replication (default)**: master-follower; commit-hook binlog (independent append-only file, monotonically increasing LSN); replicator push with per-slot resume; follower idempotent apply; PING/PONG heartbeats with offline detection.
- **Semi-sync replication**: `SET replication_mode = semi-sync`; waits for partial/full ACK (outside the transaction lock); degrades to async on ACK timeout or no slave.
- **Sync replication**: `SET replication_mode = sync`; waits for all ACKs; refuses pseudo-sync when requirements cannot be met.
- Hot-switch config: `SET replication_mode` / `SET replication_ack` / `SET replication_timeout`.

### 8.2 Sharding (M3)

- RegionInfo metadata persisted as `m:regions`; Router with half-open `[StartKey, EndKey)` boundary semantics.
- Region-prefixed physical keys (`r` + regionID:8B + inner key).
- Cross-region point/range/full-table queries via range expansion + merge.
- Management: `SPLIT` / `LIST REGIONS` / `LOCATE REGION`; default single-region shape keeps pre-sharding behavior unchanged.

### 8.3 Multi-Node Cluster (M4)

- Node topology: `NodeInfo` (NodeID/Addr/Role master|follower/State active|down + last seen), node registry persisted to `m:nodes`, auto-restored on open.
- Management: `ADD NODE '<addr>'` / `SHOW NODES`; `openxdb start --cluster-addr <addr>` starts cluster mode.
- Cluster route table: region → node; `ASSIGN REGION <id> TO NODE '<nodeID>'` updates ownership and pushes data; `SHOW REGION ROUTES` shows everything in one table.
- Cross-node reads: non-local regions are forwarded over TCP to their owning node and merged back; Local semantics (Node empty or equal to selfID) keep single-node deployments at zero regression.

### 8.4 Distributed Write / 2PC (M5)

- Cross-node write atomicity: autocommit INSERT/UPDATE/DELETE (incl. CSV IMPORT) touching both local and remote regions execute as two-phase commit.
- Flow: coordinator applies local ops → PREPAREs remote participants (ops persisted under `m:2pc:*` for crash recovery) → commits locally → idempotently COMMITs all participants.
- Failure semantics: any PREPARE failure or local commit failure ABORTs the whole statement; error codes `TXN_PREPARE_FAILED` / `TXN_COMMIT_ABORTED` / `TXN_COMMIT_UNCERTAIN`.
- Protocol frames 22-27 extend M4 framing; single-node deployments never enter the 2PC path (zero regression).

### 8.5 Distributed Transactions / TSO (M8)

- **Global Timestamp (TSO)**: 64-bit physical-ms + 24-bit logical layout; strict monotonicity (clock-fallback safe); batch issuance (GetBatch allocates contiguous ranges, remote clients cache a batch to amortize RTT); transaction-boundary Reset drops unconsumed cache so a new snapshot reads from the authoritative sequence head.
- **Distributed snapshot isolation**: version records `m:ver:<key>:<commit_ts>` (key-first so per-key ranges stay contiguous); begin_ts/commit_ts globally comparable across nodes with cross-node passthrough.
- **2PC recovery hardening**: coordinator persists CoordRecord (`m:2pc:coord:<txid>`), decides commit/abort on restart (RecoverCoordinated2PC), and idempotently drives participants to completion (`TXN_COMMIT_UNCERTAIN` completion).

### 8.6 Cross-Node DDL (T34)

- DDL metadata is broadcast to all online nodes via cluster DDL_PUSH frames; each node applies idempotently by TableID.
- Offline/apply-failure nodes record pending items, replayed before the next broadcast or via `ReplayPendingDDL`.
- Single-node zero-trigger compatibility; works alongside the T33 replication chain.

### 8.7 Distributed Backup Consistency (T35)

See [section 6.1](#61-backup--restore-br).

---

## 9. Performance Benchmark

> Source: E5 standard bench alignment experiment (`docs/experiments/E5_standard_bench_alignment.md`). Because OpenXDB speaks a self-developed protocol (not MySQL wire protocol), standard sysbench/TPC-C official tools cannot connect directly; we used **Python PEP 249 driver equivalence** with real measurements. Performance positioning is **single-node edge benchmark**.

### 9.1 sysbench OLTP (8 threads × 60s, Python driver, self-developed protocol)

| Workload | TPS | QPS | errs |
|---|---|---|---|
| read (read-only) | 1.73 | 24.27 | 0 |
| write (write-only) | 0.93 | 3.73 | 0 |
| read_write (mixed) | 1.32 | 23.70 | 0 |

### 9.2 TPC-C Small Warehouse (5 warehouses × 8 threads × 120s)

| Metric | Value | errs |
|---|---|---|
| tpmC | 13.50 | 0 |
| TPM | 32.50 | 0 |

### 9.3 Bottleneck Notes

- Current throughput bottleneck: **global session-lock serialization** + **per-statement round trips** (each statement is exchanged between driver and server one by one).
- E5 also fixed a shared-session concurrency bug and completed arithmetic-expression / DISTINCT / DECIMAL-precision workarounds.
- Historical benchmark references: E3 SQL OLTP end-to-end 256.3 TPS (8:2) / 605.4 TPS (5:5); E4 stability 6-min mixed load 307–385 TPS without degradation, restart COUNT(*) == 150466; E2 fsync engine benchmark sync write 747k TPS seq / 198k rand (batch=1000), group-commit batch 181x vs per-key fsync. These are experiment records, not external commitments.

---

## 10. Known Limitations & Boundaries

> For the full list see KNOWN_LIMITATIONS.md (to be created). Summary:

1. **SQL subset**: OpenXDB implements a SQL subset, not the full SQL standard. The `HAVING` clause is not supported; DDL/DML/query capabilities are as defined in [Chapter 4](#4-sql-reference).
2. **Not MySQL wire protocol**: uses a self-developed binary TCP protocol, so MySQL ecosystem tools (standard sysbench, official mysqldump client, etc.) cannot directly connect.
3. **Global session-lock performance boundary**: the current serialized global session lock is the primary throughput bottleneck (see [Chapter 9](#9-performance-benchmark)); high-concurrency write throughput is limited.
4. **Line protocol limits**: cell newlines are unsupported (multi-line parameter strings are rejected by drivers); empty cells represent both NULL and empty string.
5. **Types & precision**: DECIMAL is normalized to fixed 4 decimal places internally; in JSON export DECIMAL is approximated via `strconv.ParseFloat` (or kept as a string).
6. **Auth & legacy mode**: without an AUTH handshake connections run as the default root user; legacy clients fall back to a restricted guest mode when `AllowLegacy` is enabled, or are rejected in strict mode.
7. **Stored procedures/triggers are a restricted subset**: only the syntax and semantics defined in the design documents are supported; no full SQL/PL capabilities.
8. **Distributed statements limited in single-node mode**: `BALANCE` and similar cluster statements explicitly report `cluster not enabled`; cross-node writes require cluster mode.
9. **Distributed capability boundaries**: cross-node writes rely on 2PC protocol frames; replication modes (async/semi-sync/sync) are switched via hot config; sync mode refuses pseudo-sync when ACKs cannot be satisfied.
10. **Import compatibility**: mysqldump import is text-rule parsing covering a common dump syntax subset; unknown types error in strict mode and skip the table in lenient mode; cell parse failures drop the row in lenient mode.

---

## 11. FAQ

### Q1: cgo link errors during build?
Check that the CGO environment variables point to RocksDB: `CGO_CFLAGS` / `CGO_CXXFLAGS` → `<rocksdb>/include`, `CGO_LDFLAGS` → `<rocksdb-build> -lrocksdb`. On Windows add `-lshlwapi -lrpcrt4 -lws2_32` (already in repository `#cgo`). Prefer `scripts/build.ps1` / `scripts/build.sh` for one-shot builds. Do not add undeclared flags such as `-lstdc++` / `-lz`.

### Q2: Table names colliding with keywords (e.g., `users` vs `SHOW USERS`)?
`SHOW USERS` is implemented by identifier text matching; `USERS` is not in the keyword table, so `CREATE TABLE users` and `SHOW USERS` coexist without special handling. For other keyword conflicts, consult the parsing rules in the design documents.

### Q3: What is the idempotency semantics of IF NOT EXISTS?
`CREATE TABLE IF NOT EXISTS` / `DROP TABLE IF EXISTS` / `DROP USER IF EXISTS` / `DROP ROLE IF EXISTS` succeed silently when the object does not exist; when it exists, normal semantics apply (create errors or drop succeeds).

### Q4: How to back up and restore?
`BACKUP TO '<path>'` produces a single-file consistent-snapshot backup; `RESTORE FROM '<path>'` idempotently restores (same-named tables drop-then-recreate); for point-in-time recovery use `RESTORE FROM '<path>' TO LSN <n>` to replay binlog up to the target LSN. For distributed clusters use the T35 coordinator for multi-node consistent backup.

### Q5: How to set up a cluster?
Initialize each node with `openxdb init --data-dir <dir>`, then start in cluster mode with `openxdb start --cluster-addr <addr>`; register peers with `ADD NODE '<addr>'`, assign region ownership with `ASSIGN REGION <id> TO NODE '<nodeID>'`, and inspect with `SHOW NODES` / `SHOW REGION ROUTES`.

### Q6: How do drivers connect?
JDBC: `jdbc:openxdb://host:7788`; Python: `openxdb.connect("host", 7788)`. Both drivers use the self-developed protocol with `?` placeholders (client-side safe escaping). For TLS, configure certificate verification mode in JDBC and pass TLS connection parameters in Python.

### Q7: Why can't I connect with the MySQL client?
OpenXDB uses a self-developed binary TCP protocol (`pkg/server`), not MySQL wire protocol, so the official MySQL client cannot connect directly. Use the built-in REPL (`openxdb repl`) or the JDBC / Python drivers.

### Q8: How do I view slow queries?
`SET slow_query_threshold = <ms>` sets the threshold (default 100ms); after executing statements, `SHOW SLOWQUERIES` shows recent records; `SET slow_query_log = on` enables file logging (default `<data_dir>/slow.log`).

### Q9: How do I enable TLS?
Generate a certificate, then start with `openxdb start --data-dir ./data --tls-cert cert.pem --tls-key key.pem` (both flags required); TLS-only is the default with plaintext rejected; `SET PERSIST tls_allow_legacy=on` enables plaintext/TLS coexistence. Use `--replica-tls` for the replica port.

### Q10: Are the performance numbers trustworthy? Why is TPS low?
All performance numbers come from the E5 standard bench alignment experiment (`docs/experiments/E5_standard_bench_alignment.md`) with real measurements (8 threads × 60s / 120s, 0 errors). The current throughput bottleneck is global session-lock serialization + per-statement round trips; the project positions itself as a single-node edge benchmark — functional completeness first, performance optimization ongoing.

---

*This manual is compiled from the OpenXDB README.md and docs/ design documents, v1.0.0.*
*（内容由AI生成，仅供参考）*

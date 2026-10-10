---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_1a6f9c20c31311f197eb525400393706
    ReservedCode1: UMvmyo9vd97tPqgcL7KAc0nQ/6xchD4v7m4dVoS2Tn4JUb2vOzJnh39LkIJgS9ZVA55qrSsa/JxUvcZIk1wHBdYLJU3/QfBYzrYkfCv3XkOx3+1VXoXWJt/FSrh/VCe952atpnszTfKeRR82akERWOvxNFFC8eoKnrJbNWkxtXdzhvBMLsSOvSg1MdE=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_1a6f9c20c31311f197eb525400393706
    ReservedCode2: UMvmyo9vd97tPqgcL7KAc0nQ/6xchD4v7m4dVoS2Tn4JUb2vOzJnh39LkIJgS9ZVA55qrSsa/JxUvcZIk1wHBdYLJU3/QfBYzrYkfCv3XkOx3+1VXoXWJt/FSrh/VCe952atpnszTfKeRR82akERWOvxNFFC8eoKnrJbNWkxtXdzhvBMLsSOvSg1MdE=
---





# OpenXDB

[![CI](https://github.com/zhengkuanhua/openxdb/actions/workflows/ci.yml/badge.svg)](https://github.com/zhengkuanhua/openxdb/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zhengkuanhua/openxdb)](https://github.com/zhengkuanhua/openxdb)
[![License: MIT](https://img.shields.io/github/license/zhengkuanhua/openxdb)](LICENSE)
[![Release](https://img.shields.io/github/v/release/zhengkuanhua/openxdb)](https://github.com/zhengkuanhua/openxdb/releases)

OpenXDB is a from-scratch, single-node relational database kernel built for learning and experimentation. It implements a storage engine on top of RocksDB via cgo, a write-ahead log (WAL), transactional layer with snapshot isolation, a SQL subset with secondary indexes, and an in-memory B+Tree used as both an LSM comparator and a reference implementation.

> **Status: 运维工具链落地（doctor 巡检 + stats 监控 + 一键部署：`openxdb doctor` 数据目录/RocksDB/元数据/binlog/端口巡检与 0/1/2 退出码，`openxdb stats` 连接数/表数/region/慢查询/binlog 位点指标，`scripts/deploy.ps1`/`deploy.sh` 参数化部署）(v5.0-P9-OT), v0.11.0-alpha.** T1–T20 complete (P0/P1/P2 feature clusters + M2 replication + M3 sharding + M4 multi-node read + M5 distributed write/2PC + M6 HA/failover + M7 region auto-split & load balancing + M8 distributed transactions: TSO global timestamp + distributed snapshot isolation + 2PC recovery hardening + BR backup/restore: consistent-snapshot logical backup + idempotent restore + LSN-based PITR + T19 client drivers: JDBC & Python drivers wrapping the pkg/server protocol + T20 ops toolchain: doctor health checks + stats monitoring + one-click deploy scripts), all test suites green.

## Highlights

- **Storage** — RocksDB-backed key-value storage via a thin C++ cgo bridge (`pkg/storage/rocksdb`), plus a pure-Go in-memory B+Tree (`pkg/storage/btree`) used as reference and comparator.
- **WAL** — append-only write-ahead log with replay recovery (`pkg/wal`).
- **Transactions** — local ACID transactions with snapshot isolation; read-your-writes merge, tombstones, and point/range scan (`pkg/txn`).
- **SQL subset** — `CREATE/DROP TABLE` (types INT/TEXT/DATE/DECIMAL/BLOB), `CREATE/DROP INDEX`, `INSERT`, `SELECT` (WHERE, ORDER BY, LIMIT, aggregates COUNT/SUM/AVG, JOIN, GROUP BY, subqueries, BETWEEN, IN), `UPDATE`, `DELETE`, CSV `EXPORT`/`IMPORT`; expressions `LIKE` (`%` / `_`) and `CASE WHEN`; ops statements `SHOW TABLES`, `SHOW INDEX FROM t`, `EXPLAIN <SELECT>`, slow queries via `SET SLOW <ms>` and `SHOW SLOWQUERIES` (`pkg/sql`).
- **Secondary indexes** — backfill on creation, automatic maintenance on DML, and query optimization for equality/range scans and ORDER BY (`T6`).
- **Server & CLI** — REPL and TCP server (`cmd/openxdb`), binary protocol over TCP (`pkg/server`).
- **Ops toolchain (T20)** — `openxdb doctor` health checks: data dir & WAL integrity, RocksDB open, `m:tables`/`m:regions` metadata consistency & route self-consistency, binlog readability & LSN (read-only `PeekLastLSN`), optional online port/PING & slow-query checks, human-readable or `--json` report with exit codes 0=pass / 1=warn / 2=error; `openxdb stats` metrics: active connections (server session counter), table count, region count & route distribution, slow query count, binlog LSN (+ optional two-sample growth via `--interval`), table or `--json` output backed by the new `SHOW STATS` statement; one-click deploy scripts `scripts/deploy.ps1` / `scripts/deploy.sh` (parameterized `-DataDir`/`-Port`, init + start + health self-check + deployment summary) (`cmd/openxdb/doctor.go`, `cmd/openxdb/stats.go`, `pkg/server`, `pkg/sql`, `pkg/replication`, `pkg/db`, `scripts/`).
- **Replication (M2)** — master-follower async replication: commit-hook binlog (independent append-only file, monotonically increasing LSN), replicator push with per-slot resume, follower idempotent apply, PING/PONG heartbeats with offline detection (`pkg/replication`, `pkg/txn`, `pkg/db`).
- **Sharding (M3)** — logical sharding on the single-node kernel: RegionInfo metadata persisted as `m:regions`, Router with half-open `[StartKey, EndKey)` boundary semantics, region-prefixed physical keys (`r` + regionID:8B + inner key), cross-region point/range/full-table queries via range expansion + merge, and SPLIT / LIST REGIONS / LOCATE REGION management; default single-region shape keeps pre-sharding behavior unchanged (`pkg/sharding`, `pkg/sql`, `pkg/db`).
- **Multi-node cluster (M4)** — first distributed cluster: node topology with NodeInfo registry (`ADD NODE` / `SHOW NODES`), handshake and liveness via the reused M2 PING/PONG heartbeat protocol, cluster route table (region → node, `ASSIGN REGION ... TO NODE` / `SHOW REGION ROUTES`), and cross-node SELECT forwarding: non-local regions are executed via TCP on their owning node and merged back into the existing executor semantics (frames 16-21, byte-level physical-key scan to avoid package cycles); remote-region writes are rejected until the distributed write transaction cluster (2PC) lands (`pkg/cluster`, `pkg/sql`, `pkg/db`).
- **Distributed write / 2PC (M5)** — cross-node write atomicity on the M4 cluster: autocommit INSERT/UPDATE/DELETE (incl. CSV IMPORT) that touch both local and remote regions are executed as a two-phase commit — the coordinator applies local ops, PREPAREs remote participants (ops persisted under `m:2pc:*` for crash recovery), commits locally, then idempotently COMMITs all participants; any PREPARE failure or local commit failure ABORTs the whole statement with understandable errors (`TXN_PREPARE_FAILED` / `TXN_COMMIT_ABORTED` / `TXN_COMMIT_UNCERTAIN`). Protocol frames 22-27 extend the M4 framing; single-node deployments never enter the 2PC path (zero regression) (`pkg/cluster`, `pkg/sql/2pc.go`, `pkg/db`).
- **HA / Failover (M6)** — high availability on the M4/M5 cluster: heartbeat-driven failure detection marks a node `down` (PING/PONG timeout, `OnDown` callback), regions owned by the down node are automatically re-assigned to a live node (data caught up via a full replica scan + `REGION_PUSH`, replacing manual `ASSIGN REGION`), cross-node reads fail over transparently to the new owner (no client error), and in the replication link the follower auto-promotes to master when the old master is down (optional callback), keeping the write path available. Single-node and healthy-cluster paths stay untouched (zero regression) (`pkg/cluster`, `pkg/sql/ha.go`, `pkg/db/ha.go`).
- **Region auto-split & load balancing (M7)** — automatic region split on the write path: after DML commits the executor accumulates per-table write counters, and once the watermark reaches half the row threshold a real row-count check runs; a local region exceeding the threshold is split in half along its data midpoint inside the same transaction (existing rows re-routed, route table replaced, metadata persisted; boundary semantics reuse `EncodeRegionKey` — StartKey inclusive / EndKey exclusive, compatible with the M3 manual `SPLIT REGION`). Load balancing runs on demand (`BALANCE`) or on a periodic auto trigger, compares local region counts with the least-loaded live node, and migrates the largest region there while reads/writes stay available: the migration keeps the source route until a scan + `REGION_PUSH` has loaded the target (data + authoritative route view atomically persisted), then switches the local route and persists. Single-node mode reports `cluster not enabled` for `BALANCE` (zero regression) (`pkg/sql/balance.go`, `pkg/cluster`, `pkg/db`).
- **Distributed transactions / TSO (M8)** — global timestamp oracle for cross-node transactions: 64-bit physical-ms + 24-bit logical layout with strict monotonicity (clock-fallback safe) and batch issuance (GetBatch allocates contiguous ranges, remote clients cache a batch to amortize RTT); transaction-boundary Reset drops unconsumed cache so a new snapshot always reads from the authoritative sequence head; distributed snapshot isolation via version records (`m:ver:<key>:<commit_ts>`, key-first so per-key ranges stay contiguous) filtered by begin_ts/commit_ts globally comparable across nodes with cross-node passthrough; 2PC recovery hardening: coordinator persists CoordRecord (`m:2pc:coord:<txid>`) and on restart decides commit/abort (RecoverCoordinated2PC) and drives participants idempotently (`TXN_COMMIT_UNCERTAIN` completion) (`pkg/tso`, `pkg/cluster/tso.go`, `pkg/cluster/txn2pc.go`, `pkg/sql/executor.go`, `pkg/db`).
- **Backup / Restore (BR)** — logical backup and restore on the kernel: `BACKUP TO <path>` snapshots the full table catalog, all data rows and index keys, and region routing metadata on a single consistent snapshot (isolated from concurrent writes via `storage.Snapshot()`), plus the backup-point LSN, and writes a single transportable file (magic + version + JSON meta + per-segment CRC32 integrity checks, atomic tmp+rename); `RESTORE FROM <path>` validates version/integrity first, then rebuilds tables/data/indexes/routing with idempotent "drop-then-recreate" semantics for same-named tables (old rows/index residue cleared, catalog overwritten, whole restore runs in one transaction with full rollback); `RESTORE FROM <path> TO LSN <n>` replays binlog records after the backup point up to the target LSN (inclusive), providing the backup + binlog-replay PITR foundation (`pkg/sql/backup.go`, `pkg/sql`, `pkg/db`).

## Architecture

| Layer | Package | Responsibility |
|-------|---------|----------------|
| Protocol + CLI | `cmd/openxdb`, `pkg/server` | TCP server, REPL, binary protocol framing |
| Ops toolchain (T20) | `cmd/openxdb`, `pkg/server`, `pkg/sql`, `pkg/replication`, `pkg/db`, `scripts/` | `openxdb doctor` health checks (data dir/WAL/RocksDB/metadata/binlog/port/slow queries, exit 0/1/2), `openxdb stats` metrics (connections/tables/regions/slow queries/binlog LSN), `SHOW STATS`, deploy scripts (`deploy.ps1` / `deploy.sh`) |
| SQL | `pkg/sql` | Parser (recursive descent), executor (project/filter/sort/aggregate), metadata catalog, secondary index layer |
| Transaction | `pkg/txn` | Begin/commit/rollback, snapshot isolation, read-your-writes |
| Storage | `pkg/storage` | KV interface, key encoding; RocksDB engine and B+Tree engine |
| WAL | `pkg/wal` | Append-only log, replay recovery |
| Replication | `pkg/replication`, `pkg/txn`, `pkg/db` | Binlog store, master replicator (push + heartbeat), follower (idempotent apply), commit-hook wiring |
| Sharding | `pkg/sharding`, `pkg/sql`, `pkg/db` | RegionInfo metadata, Router key→region mapping, region-prefixed physical keys, cross-region query merge, split management |
| Cluster | `pkg/cluster`, `pkg/sql`, `pkg/db` | Node registry (m:nodes), handshake/liveness, cluster route table (region→node), cross-node SELECT forwarding + merge, region data load (REGION_PUSH) |
| Distributed write / 2PC | `pkg/cluster`, `pkg/sql/2pc.go`, `pkg/db` | Two-phase commit coordinator (ops grouping by region, prepare→local commit→participant commit/abort), participant prepare persistence + idempotent commit/abort markers (m:2pc:*), crash recovery (Recover2PC) |
| HA / Failover | `pkg/cluster`, `pkg/sql/ha.go`, `pkg/db/ha.go` | Heartbeat timeout → node down (OnDown callback), automatic region re-assignment (replica scan + REGION_PUSH to a live node), read failover via owner re-resolution (QUERY_REQ routed to new owner), replication master failover (follower auto-promote with optional callback) |
| Split & Balance | `pkg/sql/balance.go`, `pkg/cluster`, `pkg/db` | Write-path auto-split (DML counter watermark → real row-count check → data-midpoint split in the same txn), manual/auto load balancing (least-loaded live node target, largest local region migration via scan + REGION_PUSH while reads/writes stay available), cluster-not-enabled guard for single-node |
| Global Timestamp (TSO) | `pkg/tso`, `pkg/cluster/tso.go`, `pkg/db` | Global monotonic timestamp (64-bit ms+logical layout), batch issuance, overflow protection, transaction-boundary Reset |
| Snapshot Isolation (distributed) | `pkg/cluster/txn2pc.go`, `pkg/sql/executor.go`, `pkg/db` | Version records (m:ver), begin_ts/commit_ts global filtering, cross-node version passthrough |
| Backup / Restore | `pkg/sql/backup.go`, `pkg/sql`, `pkg/db` | Consistent-snapshot logical backup (single file, version + CRC32), idempotent restore (drop-then-recreate, transactional), binlog replay to target LSN (PITR foundation) |
| Data dir | `pkg/db` | Initialization, engine + WAL + txn manager wiring, crash recovery |

### Row / index key encoding

```
physical key: r{regionID:8B}{innerKey}
row key:    s{tableID:8B}{pk}                         (inner, M1)
index key:  i{tableID:8B}{indexID:8B}{idxVal}p{pk}   (inner, T6)
```

Every stored key carries a region prefix (`r` + 8-byte big-endian regionID); the inner key keeps the M1/T6 layout, so keys stay contiguous within a region and region boundaries are explicit. Index values use sort-friendly encoding: INT flips the sign bit (`v ^ 0x8000000000000000`), TEXT stays raw bytes, so byte order equals value order.

## Build

Requirements:

- Go 1.22+
- A C/C++ toolchain for cgo (e.g. MinGW-w64 on Windows)
- RocksDB (tested with v11.8.1 source build, no third-party compression libs)

Point cgo at your RocksDB installation and build:

```sh
export CGO_CFLAGS=-I<rocksdb>/include
export CGO_CXXFLAGS=-I<rocksdb>/include
export CGO_LDFLAGS=-L<rocksdb-build> -lrocksdb   # Windows: add -lshlwapi -lrpcrt4 -lws2_32

go build ./...
```

## Quick start

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

REPL session:

```sql
CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id));
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35);
CREATE INDEX idx_age ON users (age);
SELECT name FROM users WHERE age >= 25 AND age < 35 ORDER BY age;
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE id = 3;
```

## Tests

```sh
go test ./...
```

All packages pass: `cluster` (node handshake, query forwarding, heartbeat down, region push route learning, idempotent node re-register), `db` (incl. replication integration: binlog on commit, master-follower consistency, idempotent apply; incl. M4 two-node integration: cross-node full consistency, single-region assign; incl. M5 2PC integration: cross-node insert commit consistency, cross-node update/delete, prepare-failure abort rollback, single-node zero regression, crash recovery clears markers; incl. M6 HA integration: read failover after node down, automatic region re-assignment, promote-follower write continues, auto-promote on master down, data consistency after failover; incl. M7 split/balance integration: manual BALANCE migrates hot region, auto-balance periodic trigger, balance skips down node; incl. M8 distributed-txn integration: cross-node snapshot consistency, concurrent txn isolation, coordinator crash recovery commit/abort, single-node zero regression; incl. BR backup/restore integration: backup-restore round trip with explicit regions and secondary indexes, tampered backup rejected on restore, idempotent drop-then-recreate (stale rows/index residue cleared, index re-creatable), RESTORE TO LSN exact replay (backup point / +1 / +2 records), concurrent-write backup snapshot consistency, single-node zero regression), `replication` (binlog persistence/corruption, resume after reconnect, heartbeat timeouts), `server`, `sharding` (7 region-key/router/metadata cases), `sql` (incl. 9 secondary-index cases, 11 sharding integration cases, and M7 split/balance cases: auto-split on write threshold, manual SPLIT REGION boundary, split keeps secondary index consistent, cluster-not-enabled guard), `storage`, `storage/btree` (incl. randomized insert/delete invariant tests), `storage/rocksdb`, `tso`, `txn`, `wal`.

## Development log

Docs live in `docs/` (implementation records) and `docs/experiments/` (benchmarks):

- `T1_rocksdb_storage_impl.md`, `T2_wal_impl.md`, `T3_sql_layer.md`, `T3_txn_impl.md`, `T4_acid_notes.md`, `T5_cli_impl.md`, `T6_index_layer.md`, `T8_p0_sql_enhancements.md`, `T9_p1_csv_types.md`, `T10_p2_ops_expr.md`, `T11_m2_replication.md`, `T12_m3_sharding.md`, `T13_m4_multinode.md`, `T14_m5_2pc.md`, `T15_m6_ha.md`, `T16_m7_split_balance.md`, `T17_m8_dist_txn.md`, `T18_backup_restore.md`, `T19_client_drivers.md`, `T20_ops_toolchain.md`
- `B2_group_commit.md` — write-path group commit (batch fsync)
- `E1_rocksdb_bench.md` — RocksDB write benchmark (E1a) + in-memory B+Tree comparison (E1b)

## Roadmap

- [x] T1 storage layer (RocksDB cgo bridge)
- [x] T2 WAL
- [x] T3 SQL layer + transactions
- [x] T5 protocol + CLI
- [x] T6 secondary indexes + ordered scans
- [x] E1 storage benchmark: RocksDB (E1a) vs in-memory B+Tree (E1b) — see [E1 experiment](docs/experiments/E1_rocksdb_bench.md)
- [x] W4 ADR + storage engine selection write-up — see [ADR-004](docs/W4_adr_engine_selection.md) and [why RocksDB](docs/blog_why_openxdb_rocksdb.md)
- [x] B2 batched commits on the write path (group commit / batch fsync) — see [B2 design](docs/B2_group_commit.md)
- [x] P0 SQL enhancements: JOIN / GROUP BY / FROM+IN subqueries / BETWEEN / session transactions (BEGIN/COMMIT/ROLLBACK) — see [T8 design](docs/T8_p0_sql_enhancements.md)
- [x] P1 CSV import/export + DATE/DECIMAL/BLOB types — see [T9 design](docs/T9_p1_csv_types.md)
- [x] P2 ops statements (SHOW TABLES / SHOW INDEX / EXPLAIN / slow queries) + expressions (LIKE / CASE WHEN) — see [T10 design](docs/T10_p2_ops_expr.md)
- [x] M2 replication: binlog + replicator + follower + heartbeat (async master-follower) — see [T11 design](docs/T11_m2_replication.md)
- [x] M3 sharding: RegionInfo metadata + Router + region-prefixed keys + cross-region queries (SPLIT / LIST REGIONS / LOCATE REGION) — see [T12 design](docs/T12_m3_sharding.md)
- [x] M4 multi-node cluster: node topology + handshake/liveness + cluster route table (region→node) + cross-node SELECT forwarding/merge (ADD NODE / SHOW NODES / ASSIGN REGION / SHOW REGION ROUTES) — see [T13 design](docs/T13_m4_multinode.md)
- [x] M5 distributed write path: cross-node transactions / 2PC (coordinator + participant, prepare persistence, idempotent commit/abort, crash recovery, single-node zero trigger) — see [T14 design](docs/T14_m5_2pc.md)
- [x] M6 high availability / failover: heartbeat-driven node-down detection + OnDown callback, automatic region re-assignment (replica scan + REGION_PUSH), read failover (QUERY_REQ re-routed to new owner), replication master failover (follower promote / auto-promote) — see [T15 design](docs/T15_m6_ha.md)
- [x] M7 region auto-split & load balancing: write-path auto-split (DML counter watermark → real row-count check → data-midpoint split in the same txn, boundary semantics reuse EncodeRegionKey), on-demand / periodic load balancing (BALANCE + auto-trigger, least-loaded live node target, largest region online migration via scan + REGION_PUSH with reads/writes uninterrupted, single-node reports cluster not enabled) — see [T16 design](docs/T16_m7_split_balance.md)
- [x] M8 distributed transactions enhancement: global TSO timestamp (64-bit ms+logical layout, batch issuance, overflow protection, transaction-boundary Reset), distributed snapshot isolation (version records keyed by row-key first, begin_ts/commit_ts global filtering, cross-node passthrough), 2PC recovery hardening (coordinator CoordRecord persistence, commit/abort decision on restart, idempotent participant completion) — see [T17 design](docs/T17_m8_dist_txn.md)
- [x] BR backup / restore: logical backup on a consistent snapshot (`BACKUP TO <path>`: full table catalog + data rows + index keys + region routing + backup-point LSN in a single transportable file with version & CRC32 checks, atomic write), idempotent restore (`RESTORE FROM <path>`: validate then drop-then-recreate same-named tables, rebuild data/indexes/routing in one transactional restore), LSN-based PITR foundation (`RESTORE FROM <path> TO LSN <n>`: binlog replay after the backup point up to the target LSN) — see [T18 design](docs/T18_backup_restore.md)
- [x] T19 client drivers: JDBC driver (`OpenXDBDriver` / `OpenXDBConnection` / `OpenXDBStatement` / `OpenXDBResultSet`, `jdbc:openxdb://` URL, `META-INF/services` auto-registration, client-side `?` escaping) + Python driver (`openxdb` package: PEP 249 `connection` / `cursor` / `protocol`, module-level `connect`, qmark paramstyle, client-side escaping), both wrapping the pkg/server TCP protocol with no server-side auth handshake — see [T19 design](docs/T19_client_drivers.md)
- [x] T20 ops toolchain: `openxdb doctor` health checks (data dir/WAL integrity, RocksDB open health, `m:tables`/`m:regions` metadata consistency & route self-consistency, binlog readability & LSN, optional online port/PING & slow-query checks, human-readable or `--json` report, exit codes 0=pass / 1=warn / 2=error) + `openxdb stats` live metrics (active connections via server session counter, table count, region count & route distribution, slow query count, binlog LSN with optional two-sample growth, table or `--json` output, backed by new `SHOW STATS`) + one-click deploy scripts (`scripts/deploy.ps1` / `scripts/deploy.sh`: parameterized init/start/health self-check/deployment summary) — see [T20 design](docs/T20_ops_toolchain.md)
- [x] M2/M3 interface reservations (regions, versions, multi-node) — see [T7 design](docs/T7_m2m3_reservations.md)

## License

MIT — see [LICENSE](LICENSE).
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

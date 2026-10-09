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

> **Status: M4 多节点落地 (v4.0-P3-M4), v0.4.0-alpha.** T1–T13 complete (P0/P1/P2 feature clusters + M2 replication + M3 sharding + M4 multi-node first cluster), all test suites green.

## Highlights

- **Storage** — RocksDB-backed key-value storage via a thin C++ cgo bridge (`pkg/storage/rocksdb`), plus a pure-Go in-memory B+Tree (`pkg/storage/btree`) used as reference and comparator.
- **WAL** — append-only write-ahead log with replay recovery (`pkg/wal`).
- **Transactions** — local ACID transactions with snapshot isolation; read-your-writes merge, tombstones, and point/range scan (`pkg/txn`).
- **SQL subset** — `CREATE/DROP TABLE` (types INT/TEXT/DATE/DECIMAL/BLOB), `CREATE/DROP INDEX`, `INSERT`, `SELECT` (WHERE, ORDER BY, LIMIT, aggregates COUNT/SUM/AVG, JOIN, GROUP BY, subqueries, BETWEEN, IN), `UPDATE`, `DELETE`, CSV `EXPORT`/`IMPORT`; expressions `LIKE` (`%` / `_`) and `CASE WHEN`; ops statements `SHOW TABLES`, `SHOW INDEX FROM t`, `EXPLAIN <SELECT>`, slow queries via `SET SLOW <ms>` and `SHOW SLOWQUERIES` (`pkg/sql`).
- **Secondary indexes** — backfill on creation, automatic maintenance on DML, and query optimization for equality/range scans and ORDER BY (`T6`).
- **Server & CLI** — REPL and TCP server (`cmd/openxdb`), binary protocol over TCP (`pkg/server`).
- **Replication (M2)** — master-follower async replication: commit-hook binlog (independent append-only file, monotonically increasing LSN), replicator push with per-slot resume, follower idempotent apply, PING/PONG heartbeats with offline detection (`pkg/replication`, `pkg/txn`, `pkg/db`).
- **Sharding (M3)** — logical sharding on the single-node kernel: RegionInfo metadata persisted as `m:regions`, Router with half-open `[StartKey, EndKey)` boundary semantics, region-prefixed physical keys (`r` + regionID:8B + inner key), cross-region point/range/full-table queries via range expansion + merge, and SPLIT / LIST REGIONS / LOCATE REGION management; default single-region shape keeps pre-sharding behavior unchanged (`pkg/sharding`, `pkg/sql`, `pkg/db`).
- **Multi-node cluster (M4)** — first distributed cluster: node topology with NodeInfo registry (`ADD NODE` / `SHOW NODES`), handshake and liveness via the reused M2 PING/PONG heartbeat protocol, cluster route table (region → node, `ASSIGN REGION ... TO NODE` / `SHOW REGION ROUTES`), and cross-node SELECT forwarding: non-local regions are executed via TCP on their owning node and merged back into the existing executor semantics (frames 16-21, byte-level physical-key scan to avoid package cycles); remote-region writes are rejected until the distributed write transaction cluster (2PC) lands (`pkg/cluster`, `pkg/sql`, `pkg/db`).

## Architecture

| Layer | Package | Responsibility |
|-------|---------|----------------|
| Protocol + CLI | `cmd/openxdb`, `pkg/server` | TCP server, REPL, binary protocol framing |
| SQL | `pkg/sql` | Parser (recursive descent), executor (project/filter/sort/aggregate), metadata catalog, secondary index layer |
| Transaction | `pkg/txn` | Begin/commit/rollback, snapshot isolation, read-your-writes |
| Storage | `pkg/storage` | KV interface, key encoding; RocksDB engine and B+Tree engine |
| WAL | `pkg/wal` | Append-only log, replay recovery |
| Replication | `pkg/replication`, `pkg/txn`, `pkg/db` | Binlog store, master replicator (push + heartbeat), follower (idempotent apply), commit-hook wiring |
| Sharding | `pkg/sharding`, `pkg/sql`, `pkg/db` | RegionInfo metadata, Router key→region mapping, region-prefixed physical keys, cross-region query merge, split management |
| Cluster | `pkg/cluster`, `pkg/sql`, `pkg/db` | Node registry (m:nodes), handshake/liveness, cluster route table (region→node), cross-node SELECT forwarding + merge, region data load (REGION_PUSH) |
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

All packages pass: `cluster` (node handshake, query forwarding, heartbeat down, region push route learning), `db` (incl. replication integration: binlog on commit, master-follower consistency, idempotent apply; incl. M4 two-node integration: cross-node full consistency, single-region assign), `replication` (binlog persistence/corruption, resume after reconnect, heartbeat timeouts), `server`, `sharding` (7 region-key/router/metadata cases), `sql` (incl. 9 secondary-index cases and 11 sharding integration cases), `storage`, `storage/btree` (incl. randomized insert/delete invariant tests), `storage/rocksdb`, `txn`, `wal`.

## Development log

Docs live in `docs/` (implementation records) and `docs/experiments/` (benchmarks):

- `T1_rocksdb_storage_impl.md`, `T2_wal_impl.md`, `T3_sql_layer.md`, `T3_txn_impl.md`, `T4_acid_notes.md`, `T5_cli_impl.md`, `T6_index_layer.md`, `T8_p0_sql_enhancements.md`, `T9_p1_csv_types.md`, `T10_p2_ops_expr.md`, `T11_m2_replication.md`, `T12_m3_sharding.md`, `T13_m4_multinode.md`
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
- [ ] M5 distributed write path: cross-node transactions / 2PC (planned next cluster)
- [x] M2/M3 interface reservations (regions, versions, multi-node) — see [T7 design](docs/T7_m2m3_reservations.md)

## License

MIT — see [LICENSE](LICENSE).
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

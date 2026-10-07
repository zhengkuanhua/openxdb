---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_17a03bfcc22411f197eb525400393706
    ReservedCode1: 818bBAZWeB4yMs8kJFTG4LCntQraeH2/Wv2DcRzvXhWG5gfNPTNIfL3mDqmOmzJM2huDpzJsrRet5mm0fHloXWPZZKaY4zjIRzTrGpvtfCn/DUDwdnvUmgB5ASmbIkQTnnKlrZpJUQPQPTkWDRo8NljJZfzo+lvI3lUvyJYcsQ6OvK/nQFtZW9Wb3jI=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_17a03bfcc22411f197eb525400393706
    ReservedCode2: 818bBAZWeB4yMs8kJFTG4LCntQraeH2/Wv2DcRzvXhWG5gfNPTNIfL3mDqmOmzJM2huDpzJsrRet5mm0fHloXWPZZKaY4zjIRzTrGpvtfCn/DUDwdnvUmgB5ASmbIkQTnnKlrZpJUQPQPTkWDRo8NljJZfzo+lvI3lUvyJYcsQ6OvK/nQFtZW9Wb3jI=
---

# OpenXDB

OpenXDB is a from-scratch, single-node relational database kernel built for learning and experimentation. It implements a storage engine on top of RocksDB via cgo, a write-ahead log (WAL), transactional layer with snapshot isolation, a SQL subset with secondary indexes, and an in-memory B+Tree used as both an LSM comparator and a reference implementation.

> **Status: M1 milestone (v2.0-FP), v0.1.0-alpha.** T1–T6 complete, all test suites green.

## Highlights

- **Storage** — RocksDB-backed key-value storage via a thin C++ cgo bridge (`pkg/storage/rocksdb`), plus a pure-Go in-memory B+Tree (`pkg/storage/btree`) used as reference and comparator.
- **WAL** — append-only write-ahead log with replay recovery (`pkg/wal`).
- **Transactions** — local ACID transactions with snapshot isolation; read-your-writes merge, tombstones, and point/range scan (`pkg/txn`).
- **SQL subset** — `CREATE/DROP TABLE`, `CREATE/DROP INDEX`, `INSERT`, `SELECT` (WHERE, ORDER BY, LIMIT, aggregates COUNT/SUM/AVG), `UPDATE`, `DELETE` (`pkg/sql`).
- **Secondary indexes** — backfill on creation, automatic maintenance on DML, and query optimization for equality/range scans and ORDER BY (`T6`).
- **Server & CLI** — REPL and TCP server (`cmd/openxdb`), binary protocol over TCP (`pkg/server`).

## Architecture

| Layer | Package | Responsibility |
|-------|---------|----------------|
| Protocol + CLI | `cmd/openxdb`, `pkg/server` | TCP server, REPL, binary protocol framing |
| SQL | `pkg/sql` | Parser (recursive descent), executor (project/filter/sort/aggregate), metadata catalog, secondary index layer |
| Transaction | `pkg/txn` | Begin/commit/rollback, snapshot isolation, read-your-writes |
| Storage | `pkg/storage` | KV interface, key encoding; RocksDB engine and B+Tree engine |
| WAL | `pkg/wal` | Append-only log, replay recovery |
| Data dir | `pkg/db` | Initialization, engine + WAL + txn manager wiring, crash recovery |

### Row / index key encoding

```
row key:    s{tableID:8B}{pk}
index key:  i{tableID:8B}{indexID:8B}{idxVal}p{pk}
```

Index values use sort-friendly encoding: INT flips the sign bit (`v ^ 0x8000000000000000`), TEXT stays raw bytes, so byte order equals value order.

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

All packages pass: `db`, `server`, `sql` (incl. 9 secondary-index cases), `storage`, `storage/btree` (incl. randomized insert/delete invariant tests), `storage/rocksdb`, `txn`, `wal`.

## Development log

Docs live in `docs/` (implementation records) and `docs/experiments/` (benchmarks):

- `T1_rocksdb_storage_impl.md`, `T2_wal_impl.md`, `T3_sql_layer.md`, `T3_txn_impl.md`, `T5_cli_impl.md`, `T6_index_layer.md`
- `E1_rocksdb_bench.md` — RocksDB write benchmark (E1a), B+Tree comparison (E1b) pending

## Roadmap

- [x] T1 storage layer (RocksDB cgo bridge)
- [x] T2 WAL
- [x] T3 SQL layer + transactions
- [x] T5 protocol + CLI
- [x] T6 secondary indexes + ordered scans
- [ ] E1b B+Tree vs RocksDB benchmark comparison
- [ ] W4 ADR + storage engine selection write-up
- [ ] B2 batched commits on the write path
- [ ] M2/M3 interface reservations (regions, versions, multi-node)

## License

MIT (to be confirmed before first release).
*（内容由AI生成，仅供参考）*

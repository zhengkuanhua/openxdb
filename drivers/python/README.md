# openxdb — Python driver for OpenXDB

Native Python driver for the OpenXDB database. It speaks the OpenXDB TCP
line protocol (see `docs/T5_cli_impl.md` and `docs/T19_client_drivers.md`)
with no third-party runtime dependencies.

## Install

```bash
cd drivers/python
pip install -e .
```

## Quick start

```python
import openxdb

conn = openxdb.connect("127.0.0.1", 7788)
cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT)")
cur.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "alice"))
cur.execute("SELECT id, name FROM users WHERE id > ?", (0,))
for row in cur.fetchall():
    print(row)          # (1, 'alice')

conn.close()
```

## Transactions

```python
conn.begin()
conn.cursor().execute("INSERT INTO users (id, name) VALUES (?, ?)", (2, "bob"))
conn.commit()           # persisted
conn.begin()
conn.cursor().execute("DELETE FROM users WHERE id = ?", (2,))
conn.rollback()         # still there
```

## Tests

```bash
cd drivers/python
pytest -v
```

The pytest suite boots a real OpenXDB server (temporary data dir, random
port) and runs end-to-end against it. See `drivers/scripts/build_openxdb.ps1`
to (re)build the server binary.

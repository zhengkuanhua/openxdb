# -*- coding: utf-8 -*-
"""Pipeline (batch) execution tests for the openxdb Python driver.

Verifies the new :meth:`openxdb.protocol.OpenXDBProtocol.pipeline` /
:meth:`openxdb.cursor.Cursor.pipeline` channel: several statements are sent in
one network burst and replies are consumed in order, including mixed
query / non-query / failing statements. A throughput micro-benchmark compares
batch vs one-round-trip-per-statement execution against a real server.
"""

import time

import pytest

import openxdb
from openxdb import OperationalError, ParameterError, ProgrammingError
from openxdb.cursor import PipelineResult


@pytest.fixture(scope="session")
def conn(server):
    host, port, _ = server
    c = openxdb.connect(host, port)
    yield c
    c.close()


@pytest.fixture(autouse=True)
def clean_pipe(conn):
    """Drop the pipeline test table before/after each test."""
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE t_pipe")
    except Exception:
        pass
    yield
    try:
        cur.execute("DROP TABLE t_pipe")
    except Exception:
        pass


def test_pipeline_mixed_statements_in_order(conn):
    """Mixed CREATE/INSERT/SELECT/UPDATE/error/DELETE replies match order."""
    results = conn.pipeline([
        "CREATE TABLE t_pipe (id INT, v TEXT, PRIMARY KEY (id))",
        "INSERT INTO t_pipe VALUES (1, 'a')",
        "INSERT INTO t_pipe VALUES (2, 'b'), (3, 'c')",
        "SELECT id, v FROM t_pipe ORDER BY id",
        "UPDATE t_pipe SET v = 'x' WHERE id = 2",
        "SELECT v FROM t_pipe WHERE id = 2",
        "SELECT * FROM t_no_such_table_pipeline",
        "DELETE FROM t_pipe WHERE id = 1",
    ])
    assert len(results) == 8
    assert [r.kind for r in results] == [
        "affected", "affected", "affected", "table",
        "affected", "table", "error", "affected",
    ]
    # CREATE / INSERT / UPDATE / DELETE carry the correct affected counts.
    assert results[0].rowcount == 0
    assert results[1].rowcount == 1
    assert results[2].rowcount == 2
    assert results[4].rowcount == 1
    assert results[7].rowcount == 1
    # SELECT tables are fully parsed (cells are strings, per driver semantics).
    assert results[3].description == ["id", "v"]
    assert results[3].rows == [("1", "a"), ("2", "b"), ("3", "c")]
    assert results[5].rows == [("x",)]
    # The failing statement is captured and does not abort the batch.
    assert isinstance(results[6].error, OperationalError)
    assert "t_no_such_table_pipeline" in str(results[6].error)
    # Server-side order: DELETE ran after the error, row 1 is gone.
    cur = conn.cursor()
    cur.execute("SELECT id, v FROM t_pipe ORDER BY id")
    assert cur.fetchall() == [("2", "x"), ("3", "c")]


def test_pipeline_matches_single_command(conn):
    """pipeline([sql]) yields the same result as execute(sql) + fetchall."""
    conn.pipeline(["CREATE TABLE t_pipe (id INT, v TEXT, PRIMARY KEY (id))"])
    conn.pipeline(["INSERT INTO t_pipe VALUES (1, 'a')"])

    cur = conn.cursor()
    cur.execute("SELECT id, v FROM t_pipe")
    single = cur.fetchall()

    pipe = conn.pipeline(["SELECT id, v FROM t_pipe"])[0]
    assert pipe.kind == "table"
    assert pipe.description == ["id", "v"]
    assert pipe.rows == single
    assert pipe.rows == [("1", "a")]


def test_pipeline_with_params_list(conn):
    conn.pipeline(["CREATE TABLE t_pipe (id INT, v TEXT, PRIMARY KEY (id))"])
    results = conn.pipeline(
        [
            "INSERT INTO t_pipe VALUES (?, ?)",
            "INSERT INTO t_pipe VALUES (?, ?)",
            "SELECT v FROM t_pipe WHERE id = ?",
        ],
        [(1, "alpha"), (2, "beta"), (2,)],
    )
    assert [r.kind for r in results] == ["affected", "affected", "table"]
    assert results[2].rows == [("beta",)]


def test_pipeline_validation(conn):
    from openxdb import ConnectionClosedError
    cur = conn.cursor()
    with pytest.raises(ProgrammingError):
        cur.pipeline([])
    with pytest.raises(ProgrammingError):
        cur.pipeline(["SELECT 1", 42])
    with pytest.raises(ParameterError):
        cur.pipeline(["INSERT INTO t_pipe VALUES (?)"], [(1,), (2,)])  # length mismatch
    with pytest.raises(ParameterError):
        cur.pipeline(["SELECT ?"], [])  # placeholder but no params
    cur.close()
    with pytest.raises(ConnectionClosedError):
        cur.pipeline(["SELECT 1"])


def test_pipeline_throughput_benchmark(conn):
    """Batch execution must be strictly faster than one round trip per stmt."""
    conn.pipeline(["CREATE TABLE t_pipe (id INT, v TEXT, PRIMARY KEY (id))"])
    n = 60
    inserts = [f"INSERT INTO t_pipe VALUES ({i}, 'value-{i}')" for i in range(n)]

    # One round trip per statement.
    cur = conn.cursor()
    t0 = time.perf_counter()
    for sql in inserts:
        cur.execute(sql)
    sequential = time.perf_counter() - t0
    cur.execute("DELETE FROM t_pipe")

    # Single batch.
    t0 = time.perf_counter()
    results = conn.pipeline(inserts)
    batch = time.perf_counter() - t0

    assert len(results) == n
    assert all(r.kind == "affected" and r.rowcount == 1 for r in results)
    assert batch < sequential, (
        f"pipeline ({batch:.4f}s) should be faster than sequential "
        f"({sequential:.4f}s) for {n} statements"
    )
    speedup = sequential / batch if batch > 0 else float("inf")
    print(f"\n[pipeline benchmark] {n} INSERTs: sequential={sequential:.4f}s "
          f"batch={batch:.4f}s speedup={speedup:.1f}x")
    cur.execute("SELECT COUNT(*) FROM t_pipe")
    assert cur.fetchone() == ("60",)

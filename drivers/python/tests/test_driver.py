# -*- coding: utf-8 -*-
"""End-to-end tests for the openxdb Python driver against a real server."""

import socket

import pytest

import openxdb
from openxdb import OperationalError, ProgrammingError, ParameterError, ConnectionClosedError


@pytest.fixture(scope="session")
def conn(server):
    host, port, _ = server
    c = openxdb.connect(host, port)
    yield c
    c.close()


@pytest.fixture()
def cur(conn):
    c = conn.cursor()
    yield c
    c.close()


@pytest.fixture(autouse=True)
def clean(conn):
    """Drop any leftover tables so tests are independent."""
    cur = conn.cursor()
    for table in ("t_driver", "t_txn"):
        try:
            cur.execute(f"DROP TABLE {table}")
        except Exception:
            pass
    yield
    try:
        conn.rollback()  # leave no open transaction behind (may be no-op)
    except Exception:
        pass


# -- connection ----------------------------------------------------------

def test_connect_and_ping(server):
    host, port, _ = server
    c = openxdb.connect(host, port)
    c.ping()
    assert not c.closed
    c.close()
    assert c.closed


def test_connect_with_auth_args_ignored(server):
    # Protocol has no auth; user/password must be accepted and ignored.
    host, port, _ = server
    c = openxdb.connect(host, port, user="u", password="p")
    c.ping()
    c.close()


def test_connect_refused():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    with pytest.raises(OperationalError):
        openxdb.connect("127.0.0.1", port, timeout=1.0)


# -- DDL / DML / affected rows ------------------------------------------

def test_ddl_and_dml_affected_rows(cur):
    cur.execute("CREATE TABLE t_driver (id INT, name TEXT, PRIMARY KEY (id))")
    assert cur.rowcount == 0
    cur.execute("INSERT INTO t_driver (id, name) VALUES (1, 'alice')")
    assert cur.rowcount == 1
    cur.execute("INSERT INTO t_driver (id, name) VALUES (2, 'bob')")
    assert cur.rowcount == 1
    cur.execute("UPDATE t_driver SET name = 'carol' WHERE id = 1")
    assert cur.rowcount == 1
    cur.execute("DELETE FROM t_driver WHERE id = 2")
    assert cur.rowcount == 1


def test_select_result_parsing(cur):
    cur.execute("CREATE TABLE t_driver (id INT, name TEXT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_driver (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
    cur.execute("SELECT id, name FROM t_driver ORDER BY id")
    assert cur.description[0][0] == "id"
    assert cur.description[1][0] == "name"
    rows = cur.fetchall()
    assert rows == [("1", "alice"), ("2", "bob"), ("3", "carol")]


def test_select_types_and_null(cur):
    cur.execute("CREATE TABLE t_driver (id INT, v TEXT, d DECIMAL, dt DATE, PRIMARY KEY (id))")
    cur.execute(
        "INSERT INTO t_driver (id, v, d, dt) VALUES (1, 'x', 12.3400, '2026-10-01'), "
        "(2, '', 0.5000, '2026-10-02')"
    )
    cur.execute("SELECT id, v, d, dt FROM t_driver ORDER BY id")
    rows = cur.fetchall()
    # DECIMAL is canonicalised to fixed 4 fractional digits; empty TEXT cell
    # is indistinguishable from NULL on the wire and maps to None.
    assert rows[0] == ("1", "x", "12.3400", "2026-10-01")
    assert rows[1][1] is None
    assert rows[1][2] == "0.5000"


def test_fetchone_fetchmany(cur):
    cur.execute("CREATE TABLE t_driver (id INT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_driver (id) VALUES (1), (2), (3), (4), (5)")
    cur.execute("SELECT id FROM t_driver ORDER BY id")
    assert cur.fetchone() == ("1",)
    assert cur.fetchmany(2) == [("2",), ("3",)]
    assert cur.fetchall() == [("4",), ("5",)]
    assert cur.fetchone() is None


def test_fetchall_iteration(cur):
    cur.execute("CREATE TABLE t_driver (id INT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_driver (id) VALUES (1), (2)")
    cur.execute("SELECT id FROM t_driver ORDER BY id")
    assert [row for row in cur] == [("1",), ("2",)]


def test_column_with_pipe_in_value(cur):
    # Row splitting must keep the last column verbatim even when it has "|".
    cur.execute("CREATE TABLE t_driver (id INT, name TEXT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_driver (id, name) VALUES (1, 'a | b')")
    cur.execute("SELECT id, name FROM t_driver")
    assert cur.fetchall() == [("1", "a | b")]


# -- errors --------------------------------------------------------------

def test_sql_error_raises_operational_error(cur):
    cur.execute("CREATE TABLE t_driver (id INT, PRIMARY KEY (id))")
    with pytest.raises(OperationalError):
        cur.execute("SELECT * FROM no_such_table")


def test_disconnect_error(conn):
    c = openxdb.connect(conn.host, conn.port)
    cc = c.cursor()
    c.close()
    with pytest.raises(ConnectionClosedError):
        cc.execute("SELECT * FROM t_driver")


def test_close_then_execute_raises(conn):
    c = openxdb.connect(conn.host, conn.port)
    cur = c.cursor()
    cur.execute("SHOW TABLES")
    c.close()
    with pytest.raises(ConnectionClosedError):
        cur.execute("SELECT * FROM t_driver")


# -- parameterised execution (client-side escaping) ----------------------

def test_parameterized_insert_and_select(cur):
    cur.execute("CREATE TABLE t_driver (id INT, name TEXT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_driver (id, name) VALUES (?, ?)", (1, "alice"))
    cur.execute("INSERT INTO t_driver (id, name) VALUES (?, ?)", (2, "bob"))
    cur.execute("SELECT id, name FROM t_driver WHERE id >= ? ORDER BY id", (1,))
    assert cur.fetchall() == [("1", "alice"), ("2", "bob")]
    cur.execute("SELECT id, name FROM t_driver WHERE name = ?", ("bob",))
    assert cur.fetchall() == [("2", "bob")]


def test_parameterized_quote_escaping(cur):
    cur.execute("CREATE TABLE t_driver (id INT, name TEXT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_driver (id, name) VALUES (?, ?)", (1, "O'Brien"))
    cur.execute("SELECT id, name FROM t_driver WHERE name = ?", ("O'Brien",))
    assert cur.fetchall() == [("1", "O'Brien")]


def test_parameterized_update_delete(cur):
    cur.execute("CREATE TABLE t_driver (id INT, name TEXT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_driver (id, name) VALUES (?, ?)", (1, "a"))
    cur.execute("INSERT INTO t_driver (id, name) VALUES (?, ?)", (2, "b"))
    cur.execute("UPDATE t_driver SET name = ? WHERE id = ?", ("z", 1))
    assert cur.rowcount == 1
    cur.execute("DELETE FROM t_driver WHERE id = ?", (2,))
    assert cur.rowcount == 1
    cur.execute("SELECT id, name FROM t_driver")
    assert cur.fetchall() == [("1", "z")]


def test_parameterized_numeric_and_float(cur):
    cur.execute("CREATE TABLE t_driver (id INT, price DECIMAL, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_driver (id, price) VALUES (?, ?)", (1, 9.99))
    cur.execute("SELECT id, price FROM t_driver WHERE price > ?", (9.0,))
    assert cur.fetchall() == [("1", "9.9900")]


def test_parameterized_bytes_hex(cur):
    cur.execute("CREATE TABLE t_driver (id INT, name TEXT, blob_col BLOB, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_driver (id, name, blob_col) VALUES (?, ?, ?)", (1, "", b"\x0a\xff"))
    cur.execute("SELECT id, blob_col FROM t_driver")
    assert cur.fetchall() == [("1", "0AFF")]


def test_parameter_mismatch_raises(cur):
    cur.execute("CREATE TABLE t_driver (id INT, PRIMARY KEY (id))")
    with pytest.raises(ParameterError):
        cur.execute("INSERT INTO t_driver (id) VALUES (?)", (1, 2))
    with pytest.raises(ParameterError):
        cur.execute("INSERT INTO t_driver (id) VALUES (?)")


def test_parameter_null_unsupported(cur):
    with pytest.raises(ParameterError):
        cur.execute("SELECT 1 WHERE 1 = ?", (None,))


def test_parameter_newline_rejected(cur):
    with pytest.raises(ParameterError):
        cur.execute("SELECT ?", ("a\nb",))


# -- transactions ---------------------------------------------------------

def test_transaction_commit(server, conn):
    cur = conn.cursor()
    conn.begin()
    cur.execute("CREATE TABLE t_txn (id INT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_txn (id) VALUES (1)")
    conn.commit()

    host, port, _ = server
    other = openxdb.connect(host, port)
    try:
        c2 = other.cursor()
        c2.execute("SELECT id FROM t_txn")
        assert c2.fetchall() == [("1",)]
    finally:
        other.close()


def test_transaction_rollback(server, conn):
    cur = conn.cursor()
    conn.begin()
    cur.execute("CREATE TABLE t_txn (id INT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_txn (id) VALUES (1)")
    conn.rollback()

    host, port, _ = server
    other = openxdb.connect(host, port)
    try:
        c2 = other.cursor()
        c2.execute("SELECT id FROM t_txn")
        rows = c2.fetchall()
    except OperationalError:
        # rollback 后表与数据均不可见，服务器对不存在表报错。
        rows = None
    finally:
        other.close()
    assert rows is None or rows == []


def test_transaction_read_your_writes(conn):
    cur = conn.cursor()
    conn.begin()
    cur.execute("CREATE TABLE t_txn (id INT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_txn (id) VALUES (1)")
    cur.execute("SELECT id FROM t_txn")
    assert cur.fetchall() == [("1",)]
    conn.rollback()


def test_commit_without_begin_raises(conn):
    with pytest.raises(OperationalError):
        conn.commit()


def test_rollback_without_begin_raises(conn):
    with pytest.raises(OperationalError):
        conn.rollback()


# -- DDL / misc statements -----------------------------------------------

def test_show_and_explain_statements(cur):
    cur.execute("CREATE TABLE t_driver (id INT, PRIMARY KEY (id))")
    cur.execute("SHOW TABLES")
    names = {r[0] for r in cur.fetchall()}
    assert "t_driver" in names


def test_drop_table(cur):
    cur.execute("CREATE TABLE t_driver (id INT, PRIMARY KEY (id))")
    cur.execute("INSERT INTO t_driver (id) VALUES (1)")
    cur.execute("DROP TABLE t_driver")
    with pytest.raises(OperationalError):
        cur.execute("SELECT id FROM t_driver")

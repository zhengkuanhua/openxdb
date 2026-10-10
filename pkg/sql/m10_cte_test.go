package sql_test

import (
	"testing"
)

// M10 子功能1：CTE（WITH 子句）。
// 覆盖：单/多 CTE、CTE 引用 CTE、与视图互引、JOIN 中使用 CTE、
// 未定义 CTE 名称、循环 CTE 引用（直接/间接）。

func TestCTESingle(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, 10)")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (2, 20)")
	rows := mustRows(t, e, "WITH c AS (SELECT id, v FROM t) SELECT id FROM c ORDER BY id")
	if len(rows) != 2 || rows[0][0].I != 1 || rows[1][0].I != 2 {
		t.Fatalf("cte single: want [1,2], got %+v", rows)
	}
}

func TestCTEMultipleAndChain(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, 10)")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (2, 20)")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (3, 30)")
	rows := mustRows(t, e, "WITH a AS (SELECT id, v FROM t), b AS (SELECT id, v FROM a WHERE v >= 20) SELECT id FROM b ORDER BY id")
	if len(rows) != 2 || rows[0][0].I != 2 || rows[1][0].I != 3 {
		t.Fatalf("cte chain: want [2,3], got %+v", rows)
	}
}

func TestCTEWithView(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, 10)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, v FROM t")
	rows := mustRows(t, e, "WITH c AS (SELECT id FROM v) SELECT id FROM c ORDER BY id")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("cte with view: got %+v", rows)
	}
}

func TestCTEJoin(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE a (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE b (bid INT PRIMARY KEY, aid INT, amount INT)")
	mustExec(t, e, "INSERT INTO a (id, name) VALUES (1, 'x')")
	mustExec(t, e, "INSERT INTO b (bid, aid, amount) VALUES (10, 1, 100)")
	rows := mustRows(t, e, "WITH c AS (SELECT id FROM a) SELECT c.id, b.amount FROM c JOIN b ON c.id = b.aid")
	if len(rows) != 1 || rows[0][0].I != 1 || rows[0][1].I != 100 {
		t.Fatalf("cte join: got %+v", rows)
	}
}

func TestCTEUndefined(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	// 未定义 CTE 名退化为普通表名解析，报 table not exists。
	mustErr(t, e, "WITH c AS (SELECT id FROM t) SELECT id FROM missing", "table not exists: missing")
}

func TestCTECyclic(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustErr(t, e, "WITH a AS (SELECT id FROM a) SELECT id FROM a", "cyclic CTE reference")
}

func TestCTECyclicIndirect(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustErr(t, e, "WITH a AS (SELECT id FROM b), b AS (SELECT id FROM a) SELECT id FROM a", "cyclic CTE reference")
}

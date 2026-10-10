package sql_test

import (
	"testing"
)

// M9 子功能 5：视图（CREATE VIEW / DROP VIEW / SHOW VIEWS）
// 定义展开执行、循环引用防护。

func TestCreateViewBasic(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t (id, name) VALUES (1, 'a')")
	mustExec(t, e, "INSERT INTO t (id, name) VALUES (2, 'b')")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name FROM t")
	rows := mustRows(t, e, "SELECT * FROM v ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("view rows: want 2, got %d", len(rows))
	}
	if rows[0][1].S != "a" || rows[1][1].S != "b" {
		t.Fatalf("view content: got %+v", rows)
	}
}

func TestCreateViewWithFilterAndOrder(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	mustExec(t, e, "INSERT INTO t (id, score) VALUES (1, 30)")
	mustExec(t, e, "INSERT INTO t (id, score) VALUES (2, 80)")
	mustExec(t, e, "INSERT INTO t (id, score) VALUES (3, 55)")
	mustExec(t, e, "CREATE VIEW top AS SELECT id, score FROM t WHERE score >= 50")
	rows := mustRows(t, e, "SELECT id FROM top ORDER BY score DESC")
	if len(rows) != 2 || rows[0][0].I != 2 {
		t.Fatalf("filtered view: want [2,3], got %+v", rows)
	}
}

func TestCreateViewWithJoin(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, amount INT)")
	mustExec(t, e, "INSERT INTO users (id, name) VALUES (1, 'u1')")
	mustExec(t, e, "INSERT INTO orders (oid, uid, amount) VALUES (10, 1, 100)")
	mustExec(t, e, "INSERT INTO orders (oid, uid, amount) VALUES (11, 1, 200)")
	mustExec(t, e, "CREATE VIEW order_summary AS SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.uid")
	rows := mustRows(t, e, "SELECT name, amount FROM order_summary ORDER BY amount")
	if len(rows) != 2 || rows[0][0].S != "u1" || rows[0][1].I != 100 {
		t.Fatalf("join view: got %+v", rows)
	}
}

func TestCreateViewQualifiedQuery(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, 7)")
	mustExec(t, e, "CREATE VIEW vw AS SELECT id, v FROM t")
	rows := mustRows(t, e, "SELECT vw.v FROM vw WHERE vw.id = 1")
	if len(rows) != 1 || rows[0][0].I != 7 {
		t.Fatalf("qualified view col: got %+v", rows)
	}
}

func TestCreateViewDuplicate(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id FROM t")
	mustErr(t, e, "CREATE VIEW v AS SELECT id FROM t", "view already exists")
}

func TestCreateViewNameConflictsTable(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustErr(t, e, "CREATE VIEW t AS SELECT id FROM t", "view name conflicts with table")
}

func TestCreateViewMissingTable(t *testing.T) {
	e := newEngine(t)
	mustErr(t, e, "CREATE VIEW v AS SELECT id FROM ghost", "table not exists")
}

func TestCreateViewSelfCycle(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustErr(t, e, "CREATE VIEW v AS SELECT id FROM v", "cyclic view reference")
}

func TestCreateViewIndirectCycle(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE VIEW v1 AS SELECT id FROM t")
	// v2 引用 v1：合法
	mustExec(t, e, "CREATE VIEW v2 AS SELECT id FROM v1")
	// v1 再引用 v2：形成 v1 → v2 → v1 间接环，拒绝
	mustErr(t, e, "DROP VIEW v1", "referenced by view v2")
	// 直接验证间接环防护：删除 v2 后 v1 仍无法引用 v2（v2 已不存在）
	mustExec(t, e, "DROP VIEW v2")
	mustErr(t, e, "CREATE VIEW v1b AS SELECT id FROM v2", "table not exists")
}

func TestViewOverView(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, tag TEXT)")
	mustExec(t, e, "INSERT INTO t (id, tag) VALUES (1, 'x')")
	mustExec(t, e, "INSERT INTO t (id, tag) VALUES (2, 'y')")
	mustExec(t, e, "CREATE VIEW v1 AS SELECT id, tag FROM t WHERE tag = 'x'")
	mustExec(t, e, "CREATE VIEW v2 AS SELECT id FROM v1")
	rows := mustRows(t, e, "SELECT id FROM v2")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("view over view: got %+v", rows)
	}
}

func TestDropView(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id FROM t")
	mustExec(t, e, "DROP VIEW v")
	mustErr(t, e, "SELECT id FROM v", "table not exists")
	// 删除后可重建同名视图
	mustExec(t, e, "CREATE VIEW v AS SELECT id FROM t")
}

func TestDropViewIfExists(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "DROP VIEW IF EXISTS ghost")
	mustErr(t, e, "DROP VIEW ghost", "view not exists")
}

func TestDropViewReferencedRestrict(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE VIEW v1 AS SELECT id FROM t")
	mustExec(t, e, "CREATE VIEW v2 AS SELECT id FROM v1")
	// 被 v2 引用的 v1 禁止删除
	mustErr(t, e, "DROP VIEW v1", "referenced by view v2")
}

func TestShowViews(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE VIEW v1 AS SELECT id FROM t")
	mustExec(t, e, "CREATE VIEW v2 AS SELECT id FROM t WHERE id > 0")
	res, err := e.Execute("SHOW VIEWS")
	if err != nil {
		t.Fatalf("SHOW VIEWS: %v", err)
	}
	if len(res.Columns) != 2 || res.Columns[0] != "view" {
		t.Fatalf("SHOW VIEWS columns: %+v", res.Columns)
	}
	names := map[string]bool{}
	for _, r := range res.Rows {
		names[r[0].S] = true
		if r[1].S == "" {
			t.Fatalf("SHOW VIEWS empty definition for %s", r[0].S)
		}
	}
	if !names["v1"] || !names["v2"] {
		t.Fatalf("SHOW VIEWS rows: %+v", res.Rows)
	}
}

func TestViewReflectsUnderlyingChanges(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
	mustExec(t, e, "INSERT INTO t (id, val) VALUES (1, 10)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, val FROM t")
	// 视图定义展开执行：底层数据变更后查询立即反映
	mustExec(t, e, "INSERT INTO t (id, val) VALUES (2, 20)")
	mustExec(t, e, "UPDATE t SET val = 99 WHERE id = 1")
	rows := mustRows(t, e, "SELECT val FROM v ORDER BY id")
	if len(rows) != 2 || rows[0][0].I != 99 || rows[1][0].I != 20 {
		t.Fatalf("view reflect changes: got %+v", rows)
	}
}

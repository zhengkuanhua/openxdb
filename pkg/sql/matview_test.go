package sql_test

import (
	"strings"
	"testing"
)

// M15 物化视图测试：创建 / 快照语义 / REFRESH / 源表 DDL 后刷新失败 / DROP /
// 与虚拟视图共存 / 聚合 / DECIMAL / BLOB / 循环与依赖 / 保留列名 / schema 演化。

func TestM15CreateAndQueryMatView(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv_users AS SELECT id, name, age FROM users")

	rows := mustRows(t, e, "SELECT * FROM mv_users")
	if len(rows) != 3 {
		t.Fatalf("SELECT * FROM mv_users: want 3 rows, got %d", len(rows))
	}
	want := [][3]string{{"1", "alice", "30"}, {"2", "bob", "25"}, {"3", "carol", "35"}}
	for i, w := range want {
		got := [3]string{rows[i][0].String(), rows[i][1].String(), rows[i][2].String()}
		if got != w {
			t.Fatalf("row %d: want %v, got %v", i, w, got)
		}
	}

	// 隐藏主键列对用户不可见：SELECT * 列数 = 定义列数
	res := mustExec(t, e, "SELECT * FROM mv_users")
	if len(res.Columns) != 3 {
		t.Fatalf("SELECT *: want 3 columns, got %d (%v)", len(res.Columns), res.Columns)
	}

	// SHOW TABLES 可见物化视图影子表
	found := false
	for _, r := range mustRows(t, e, "SHOW TABLES") {
		if r[0].String() == "mv_users" {
			found = true
		}
	}
	if !found {
		t.Fatal("SHOW TABLES: mv_users not listed")
	}
}

func TestM15MatViewSnapshotNotAutoUpdated(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM users")

	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	rows := mustRows(t, e, "SELECT id FROM mv")
	if len(rows) != 3 {
		t.Fatalf("materialized view must keep snapshot: want 3 rows, got %d", len(rows))
	}

	mustExec(t, e, "REFRESH MATERIALIZED VIEW mv")
	rows = mustRows(t, e, "SELECT id FROM mv")
	if len(rows) != 4 {
		t.Fatalf("after REFRESH: want 4 rows, got %d", len(rows))
	}
}

func TestM15RefreshConcurrentlySyntax(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, age FROM users")
	mustExec(t, e, "UPDATE users SET age = 31 WHERE id = 1")
	mustExec(t, e, "REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
	rows := mustRows(t, e, "SELECT age FROM mv WHERE id = 1")
	if len(rows) != 1 || rows[0][0].String() != "31" {
		t.Fatalf("after REFRESH CONCURRENTLY: want age 31, got %v", rows)
	}
}

func TestM15RefreshFailsAfterSourceDrop(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM users")
	mustExec(t, e, "DROP TABLE users")

	mustErr(t, e, "REFRESH MATERIALIZED VIEW mv", "table not exists")
	// 刷新失败后旧快照仍可读
	rows := mustRows(t, e, "SELECT id FROM mv")
	if len(rows) != 3 {
		t.Fatalf("old snapshot should remain readable: want 3 rows, got %d", len(rows))
	}
}

func TestM15RefreshFailsAfterSourceAlter(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM users")
	mustExec(t, e, "ALTER TABLE users DROP COLUMN name")

	mustErr(t, e, "REFRESH MATERIALIZED VIEW mv", "unknown column")
	rows := mustRows(t, e, "SELECT * FROM mv")
	if len(rows) != 3 {
		t.Fatalf("old snapshot should remain: want 3 rows, got %d", len(rows))
	}
}

func TestM15DropMatView(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM users")

	mustExec(t, e, "DROP MATERIALIZED VIEW mv")
	mustErr(t, e, "SELECT * FROM mv", "table not exists")
	mustErr(t, e, "DROP MATERIALIZED VIEW mv", "materialized view not exists")
	mustExec(t, e, "DROP MATERIALIZED VIEW IF EXISTS mv")

	// 同名表可重建
	mustExec(t, e, "CREATE TABLE mv (x INT, PRIMARY KEY (x))")
	mustExec(t, e, "INSERT INTO mv (x) VALUES (1)")
	rows := mustRows(t, e, "SELECT x FROM mv")
	if len(rows) != 1 {
		t.Fatalf("recreated table: want 1 row, got %d", len(rows))
	}
}

func TestM15NameConflictsAndCoexist(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustErr(t, e, "CREATE MATERIALIZED VIEW users AS SELECT * FROM users", "conflicts with table")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name FROM users")
	mustErr(t, e, "CREATE MATERIALIZED VIEW v AS SELECT * FROM users", "conflicts with view")

	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM users")
	mustErr(t, e, "CREATE VIEW mv AS SELECT * FROM users", "conflicts with table")
	mustErr(t, e, "CREATE TABLE mv (x INT, PRIMARY KEY (x))", "table already exists")

	// 共存：物化视图基于虚拟视图；虚拟视图基于物化视图
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv_from_view AS SELECT * FROM v")
	rows := mustRows(t, e, "SELECT * FROM mv_from_view")
	if len(rows) != 3 {
		t.Fatalf("mv_from_view: want 3 rows, got %d", len(rows))
	}
	mustExec(t, e, "CREATE VIEW v_from_mv AS SELECT * FROM mv")
	rows = mustRows(t, e, "SELECT id FROM v_from_mv")
	if len(rows) != 3 {
		t.Fatalf("v_from_mv: want 3 rows, got %d", len(rows))
	}

	// 虚拟视图即时反映源表；物化视图保持快照
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	rows = mustRows(t, e, "SELECT id FROM v")
	if len(rows) != 4 {
		t.Fatalf("virtual view should reflect source: want 4 rows, got %d", len(rows))
	}
	rows = mustRows(t, e, "SELECT id FROM mv")
	if len(rows) != 3 {
		t.Fatalf("materialized view snapshot unchanged: want 3 rows, got %d", len(rows))
	}
}

func TestM15MatViewAggregate(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW dept_age AS SELECT age, COUNT(*) AS c FROM users GROUP BY age")
	rows := mustRows(t, e, "SELECT * FROM dept_age")
	if len(rows) != 3 {
		t.Fatalf("dept_age snapshot: want 3 rows, got %d", len(rows))
	}

	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 25)")
	rows = mustRows(t, e, "SELECT * FROM dept_age")
	if len(rows) != 3 {
		t.Fatalf("dept_age before refresh: want 3 rows, got %d", len(rows))
	}
	mustExec(t, e, "REFRESH MATERIALIZED VIEW dept_age")
	rows = mustRows(t, e, "SELECT * FROM dept_age")
	if len(rows) != 3 {
		t.Fatalf("dept_age after refresh: want 3 rows, got %d", len(rows))
	}
}

func TestM15MatViewDecimal(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE orders (id INT, amount DECIMAL, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO orders (id, amount) VALUES (1, 10.50), (2, 3.25)")
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, amount FROM orders")
	rows := mustRows(t, e, "SELECT amount FROM mv ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("decimal mat view: want 2 rows, got %d", len(rows))
	}
	if rows[0][0].Kind != "DECIMAL" || rows[1][0].Kind != "DECIMAL" {
		t.Fatalf("decimal mat view kinds: got %v / %v", rows[0][0].Kind, rows[1][0].Kind)
	}
	if rows[0][0].String() == "" || rows[1][0].String() == "" {
		t.Fatalf("decimal mat view empty values: got %v", rows)
	}
}

func TestM15MatViewBlob(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE files (id INT, data BLOB, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO files (id, data) VALUES (1, '0A0B')")
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, data FROM files")
	rows := mustRows(t, e, "SELECT data FROM mv")
	if len(rows) != 1 {
		t.Fatalf("blob mat view: want 1 row, got %d", len(rows))
	}
	if !strings.EqualFold(rows[0][0].S, "0A0B") {
		t.Fatalf("blob mat view value: got %q", rows[0][0].S)
	}
}

func TestM15SelfReferenceAndDependencies(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustErr(t, e, "CREATE MATERIALIZED VIEW m1 AS SELECT * FROM m1", "cyclic materialized view reference")
	mustErr(t, e, "CREATE MATERIALIZED VIEW m1 AS SELECT * FROM no_such_table", "table not exists")

	mustExec(t, e, "CREATE MATERIALIZED VIEW a AS SELECT id, name FROM users")
	mustExec(t, e, "CREATE MATERIALIZED VIEW b AS SELECT * FROM a")
	mustErr(t, e, "DROP MATERIALIZED VIEW a", "depends on it")
	mustExec(t, e, "DROP MATERIALIZED VIEW b")
	mustExec(t, e, "DROP MATERIALIZED VIEW a")

	// 被虚拟视图引用时同样拒绝删除
	mustExec(t, e, "CREATE MATERIALIZED VIEW c AS SELECT id FROM users")
	mustExec(t, e, "CREATE VIEW vc AS SELECT * FROM c")
	mustErr(t, e, "DROP MATERIALIZED VIEW c", "depends on it")
	mustExec(t, e, "DROP VIEW vc")
	mustExec(t, e, "DROP MATERIALIZED VIEW c")
}

func TestM15ReservedColumnName(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustErr(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id AS __mv_rid FROM users", "reserved")
}

func TestM15MatViewSchemaEvolution(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM users")

	// 源表新增列不影响未引用的定义
	mustExec(t, e, "ALTER TABLE users ADD COLUMN email TEXT")
	mustExec(t, e, "REFRESH MATERIALIZED VIEW mv")
	res := mustExec(t, e, "SELECT * FROM mv")
	if len(res.Columns) != 2 {
		t.Fatalf("mv columns should stay 2, got %d (%v)", len(res.Columns), res.Columns)
	}

	// 定义引用的列被删除后 REFRESH 失败，旧快照保留
	mustExec(t, e, "ALTER TABLE users DROP COLUMN name")
	mustErr(t, e, "REFRESH MATERIALIZED VIEW mv", "unknown column")
	rows := mustRows(t, e, "SELECT * FROM mv")
	if len(rows) != 3 {
		t.Fatalf("old snapshot should remain: want 3 rows, got %d", len(rows))
	}
}

func TestM15MatViewSelectWithLimit(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM users LIMIT 2")
	rows := mustRows(t, e, "SELECT * FROM mv")
	if len(rows) != 2 {
		t.Fatalf("mat view with LIMIT: want 2 rows, got %d", len(rows))
	}
}

func TestM15MatViewJoin(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE orders (uid INT, amount INT, PRIMARY KEY (uid))")
	mustExec(t, e, "INSERT INTO orders (uid, amount) VALUES (1, 100), (2, 200)")
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.uid")
	rows := mustRows(t, e, "SELECT name, amount FROM mv ORDER BY name")
	if len(rows) != 2 {
		t.Fatalf("join mat view: want 2 rows, got %d", len(rows))
	}
}

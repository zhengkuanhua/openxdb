package sql_test

import (
	"strings"
	"testing"

	"github.com/openxdb/openxdb/pkg/db"
	"github.com/openxdb/openxdb/pkg/sql"
)

func newEngine(t *testing.T) *sql.Engine {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d.SQL
}

func mustExec(t *testing.T, e *sql.Engine, stmt string) *sql.Result {
	t.Helper()
	res, err := e.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute(%q): %v", stmt, err)
	}
	return res
}

func mustErr(t *testing.T, e *sql.Engine, stmt, wantSub string) {
	t.Helper()
	_, err := e.Execute(stmt)
	if err == nil {
		t.Fatalf("Execute(%q): want error containing %q, got nil", stmt, wantSub)
	}
	if !strings.Contains(err.Error(), wantSub) {
		t.Fatalf("Execute(%q): want error containing %q, got %v", stmt, wantSub, err)
	}
}

func mustRows(t *testing.T, e *sql.Engine, stmt string) [][]sql.Value {
	t.Helper()
	res := mustExec(t, e, stmt)
	if len(res.Columns) == 0 {
		t.Fatalf("Execute(%q): expected result set, got DML result", stmt)
	}
	return res.Rows
}

func setupUsers(t *testing.T, e *sql.Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35)")
}

func TestCreateInsertSelectAll(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	rows := mustRows(t, e, "SELECT * FROM users")
	if len(rows) != 3 {
		t.Fatalf("SELECT *: want 3 rows, got %d", len(rows))
	}
	if rows[0][0].Kind != "INT" || rows[0][0].I != 1 {
		t.Fatalf("row0 col0: want 1, got %+v", rows[0][0])
	}
	if rows[0][1].S != "alice" {
		t.Fatalf("row0 col1: want alice, got %q", rows[0][1].S)
	}
}

func TestSelectProjection(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	res := mustExec(t, e, "SELECT name, age FROM users")
	if len(res.Columns) != 2 || res.Columns[0] != "name" || res.Columns[1] != "age" {
		t.Fatalf("columns: got %v", res.Columns)
	}
	if len(res.Rows) != 3 || res.Rows[0][0].S != "alice" || res.Rows[0][1].I != 30 {
		t.Fatalf("projection rows mismatch: %+v", res.Rows)
	}
}

func TestSelectWhere(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	rows := mustRows(t, e, "SELECT id FROM users WHERE age > 25")
	if len(rows) != 2 {
		t.Fatalf("WHERE age>25: want 2 rows, got %d (%+v)", len(rows), rows)
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE age >= 25 AND age < 35")
	if len(rows) != 2 {
		t.Fatalf("AND range: want 2 rows, got %d", len(rows))
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE name != 'bob'")
	if len(rows) != 2 {
		t.Fatalf("!= filter: want 2 rows, got %d", len(rows))
	}
}

func TestSelectPointLookup(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	rows := mustRows(t, e, "SELECT name FROM users WHERE id = 2")
	if len(rows) != 1 || rows[0][0].S != "bob" {
		t.Fatalf("point lookup: want [bob], got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT name FROM users WHERE id = 999")
	if len(rows) != 0 {
		t.Fatalf("missing point lookup: want 0 rows, got %d", len(rows))
	}
}

func TestPrimaryKeyUnique(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustErr(t, e, "INSERT INTO users (id, name, age) VALUES (1, 'dup', 99)", "duplicate primary key")
	// 冲突事务应回滚，不留下半行
	rows := mustRows(t, e, "SELECT * FROM users")
	if len(rows) != 3 {
		t.Fatalf("after conflict: want 3 rows, got %d", len(rows))
	}
}

func TestUpdate(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	res := mustExec(t, e, "UPDATE users SET age = 31 WHERE id = 1")
	if res.AffectedRows != 1 {
		t.Fatalf("UPDATE: want 1 affected, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT age FROM users WHERE id = 1")
	if rows[0][0].I != 31 {
		t.Fatalf("after update: want 31, got %+v", rows[0][0])
	}
}

func TestDelete(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	res := mustExec(t, e, "DELETE FROM users WHERE age > 30")
	if res.AffectedRows != 1 {
		t.Fatalf("DELETE: want 1 affected, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id FROM users")
	if len(rows) != 2 {
		t.Fatalf("after delete: want 2 rows, got %d", len(rows))
	}
}

func TestOrderByLimit(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	rows := mustRows(t, e, "SELECT name FROM users ORDER BY age DESC LIMIT 2")
	if len(rows) != 2 || rows[0][0].S != "carol" || rows[1][0].S != "alice" {
		t.Fatalf("ORDER BY DESC LIMIT: got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT name FROM users ORDER BY name ASC LIMIT 1")
	if len(rows) != 1 || rows[0][0].S != "alice" {
		t.Fatalf("ORDER BY name: got %+v", rows)
	}
}

func TestAggregates(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	if rows := mustRows(t, e, "SELECT COUNT(*) FROM users"); rows[0][0].I != 3 {
		t.Fatalf("COUNT: want 3, got %+v", rows[0][0])
	}
	if rows := mustRows(t, e, "SELECT SUM(age) FROM users"); rows[0][0].I != 90 {
		t.Fatalf("SUM: want 90, got %+v", rows[0][0])
	}
	if rows := mustRows(t, e, "SELECT AVG(age) FROM users WHERE age > 25"); rows[0][0].I != 32 {
		t.Fatalf("AVG: want 32, got %+v", rows[0][0])
	}
}

func TestDropTable(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	res := mustExec(t, e, "DROP TABLE users")
	if res.AffectedRows != 3 {
		t.Fatalf("DROP: want 3 rows dropped, got %d", res.AffectedRows)
	}
	mustErr(t, e, "SELECT * FROM users", "table not exists")
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	e := d.SQL
	mustExec(t, e, "CREATE TABLE t (id INT, v TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t VALUES (1, 'hello')")
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	d2, err := db.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	rows := mustRows(t, d2.SQL, "SELECT v FROM t WHERE id = 1")
	if len(rows) != 1 || rows[0][0].S != "hello" {
		t.Fatalf("after reopen: got %+v", rows)
	}
}

func TestParseErrors(t *testing.T) {
	e := newEngine(t)
	mustErr(t, e, "SELEC * FROM x", "expected statement")
	mustErr(t, e, "SELECT * FROM users WHERE", "expected")
	mustErr(t, e, "CREATE TABLE bad (id FLOAT)", "unsupported column type")
	mustErr(t, e, "INSERT INTO t VALUES (1, 2, 3)", "table not exists")
}

// ---- T6 二级索引 ----

func TestCreateIndexLookup(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	// 索引等值查询
	rows := mustRows(t, e, "SELECT name FROM users WHERE age = 30")
	if len(rows) != 1 || rows[0][0].S != "alice" {
		t.Fatalf("index eq: want [alice], got %+v", rows)
	}
	// 不存在的值 → 空
	if rows := mustRows(t, e, "SELECT name FROM users WHERE age = 99"); len(rows) != 0 {
		t.Fatalf("index eq missing: want 0 rows, got %d", len(rows))
	}
	// 重复索引名 / 未知列 / 未知表
	mustErr(t, e, "CREATE INDEX idx_age ON users (age)", "index already exists")
	mustErr(t, e, "CREATE INDEX idx_bad ON users (nope)", "unknown column")
	mustErr(t, e, "CREATE INDEX idx_bad ON nope (age)", "table not exists")
}

func TestCreateIndexBackfill(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	// 数据已存在时建索引，回填后立即可查
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	rows := mustRows(t, e, "SELECT id FROM users WHERE age >= 30")
	if len(rows) != 2 {
		t.Fatalf("backfill range: want 2 rows, got %d", len(rows))
	}
}

func TestIndexRangeAndOrderBy(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	// 范围查询走索引
	rows := mustRows(t, e, "SELECT name FROM users WHERE age >= 25 AND age < 35 ORDER BY age")
	if len(rows) != 2 || rows[0][0].S != "bob" || rows[1][0].S != "alice" {
		t.Fatalf("index range+order: got %+v", rows)
	}
	// ORDER BY 走索引序（LIMIT 前两小）
	rows = mustRows(t, e, "SELECT name FROM users WHERE age >= 25 ORDER BY age LIMIT 2")
	if len(rows) != 2 || rows[0][0].S != "bob" || rows[1][0].S != "alice" {
		t.Fatalf("index ordered limit: got %+v", rows)
	}
	// DESC 仍需排序，结果正确
	rows = mustRows(t, e, "SELECT name FROM users WHERE age < 35 ORDER BY age DESC")
	if len(rows) != 2 || rows[0][0].S != "alice" || rows[1][0].S != "bob" {
		t.Fatalf("index desc: got %+v", rows)
	}
}

func TestIndexNegativeIntOrder(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE neg (id INT, v INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO neg VALUES (1, -5), (2, -1), (3, 3)")
	mustExec(t, e, "CREATE INDEX idx_v ON neg (v)")
	rows := mustRows(t, e, "SELECT v FROM neg WHERE v >= -2 ORDER BY v")
	if len(rows) != 2 || rows[0][0].I != -1 || rows[1][0].I != 3 {
		t.Fatalf("neg range: got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT v FROM neg WHERE v < 0 ORDER BY v")
	if len(rows) != 2 || rows[0][0].I != -5 || rows[1][0].I != -1 {
		t.Fatalf("neg order: got %+v", rows)
	}
}

func TestIndexMaintenance(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	// INSERT 维护索引
	mustExec(t, e, "INSERT INTO users VALUES (4, 'dave', 40)")
	rows := mustRows(t, e, "SELECT name FROM users WHERE age = 40")
	if len(rows) != 1 || rows[0][0].S != "dave" {
		t.Fatalf("index after insert: got %+v", rows)
	}
	// UPDATE 维护索引：改索引列
	mustExec(t, e, "UPDATE users SET age = 41 WHERE id = 4")
	if rows := mustRows(t, e, "SELECT name FROM users WHERE age = 40"); len(rows) != 0 {
		t.Fatalf("old index value still visible: %+v", rows)
	}
	rows = mustRows(t, e, "SELECT name FROM users WHERE age = 41")
	if len(rows) != 1 || rows[0][0].S != "dave" {
		t.Fatalf("new index value missing: %+v", rows)
	}
	// DELETE 维护索引
	mustExec(t, e, "DELETE FROM users WHERE id = 4")
	if rows := mustRows(t, e, "SELECT name FROM users WHERE age = 41"); len(rows) != 0 {
		t.Fatalf("index after delete: %+v", rows)
	}
}

func TestDropIndex(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	res := mustExec(t, e, "DROP INDEX idx_age ON users")
	if res.AffectedRows != 3 {
		t.Fatalf("DROP INDEX: want 3 index keys, got %d", res.AffectedRows)
	}
	mustErr(t, e, "DROP INDEX idx_age ON users", "index not exists")
	// 删索引后全表查询仍正常
	if rows := mustRows(t, e, "SELECT id FROM users WHERE age = 30"); len(rows) != 1 {
		t.Fatalf("after drop index: got %+v", rows)
	}
	// 同名索引可重建
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	if rows := mustRows(t, e, "SELECT id FROM users WHERE age = 30"); len(rows) != 1 {
		t.Fatalf("recreated index: got %+v", rows)
	}
}

func TestDropTableWithIndex(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	mustExec(t, e, "DROP TABLE users")
	// 重建同名表不应受旧索引键影响
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO users VALUES (1, 'alice', 30)")
	rows := mustRows(t, e, "SELECT id FROM users WHERE age = 30")
	if len(rows) != 1 {
		t.Fatalf("recreated table with stale index: got %+v", rows)
	}
}

func TestMultiColumnIndexes(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	mustExec(t, e, "CREATE INDEX idx_name ON users (name)")
	// 两个索引并存，各自生效
	if rows := mustRows(t, e, "SELECT id FROM users WHERE age = 25"); len(rows) != 1 {
		t.Fatalf("idx_age lookup: got %+v", rows)
	}
	if rows := mustRows(t, e, "SELECT id FROM users WHERE name = 'carol'"); len(rows) != 1 {
		t.Fatalf("idx_name lookup: got %+v", rows)
	}
	// 删除其中一个，另一个不受影响
	mustExec(t, e, "DROP INDEX idx_name ON users")
	if rows := mustRows(t, e, "SELECT id FROM users WHERE age = 25"); len(rows) != 1 {
		t.Fatalf("idx_age after drop other: got %+v", rows)
	}
}

func TestIndexPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	e := d.SQL
	mustExec(t, e, "CREATE TABLE t (id INT, v TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t VALUES (1, 'x'), (2, 'y')")
	mustExec(t, e, "CREATE INDEX idx_v ON t (v)")
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	d2, err := db.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	rows := mustRows(t, d2.SQL, "SELECT id FROM t WHERE v = 'y'")
	if len(rows) != 1 || rows[0][0].I != 2 {
		t.Fatalf("index after reopen: got %+v", rows)
	}
}

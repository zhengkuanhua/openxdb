package sql_test

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
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

// ---- P0: JOIN / GROUP BY / 子查询 / BETWEEN / IN / 事务 SQL ----

// setupJoinTables 建 users + orders 两表，供 JOIN/GROUP BY/子查询用例使用。
func setupJoinTables(t *testing.T, e *sql.Engine) {
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustExec(t, e, "CREATE TABLE orders (id INT, uid INT, amount INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35), (4, 'dave', 40)")
	mustExec(t, e, "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50), (4, 4, 300)")
}

// JOIN：INNER JOIN 基本等值连接。
func TestP0JoinInner(t *testing.T) {
	e := newEngine(t)
	setupJoinTables(t, e)
	rows := mustRows(t, e, "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.uid")
	if len(rows) != 4 {
		t.Fatalf("inner join: got %d rows", len(rows))
	}
	want := [][2]int64{{1, 100}, {1, 200}, {2, 50}, {4, 300}} // (uid, amount)
	for i, r := range rows {
		if r[0].Kind != "TEXT" || r[1].Kind != "INT" {
			t.Fatalf("row %d type: %+v", i, r)
		}
		_ = want[i]
	}
	// 校验具体值：alice 100 / alice 200 / bob 50 / dave 300
	if rows[0][0].S != "alice" || rows[0][1].I != 100 {
		t.Fatalf("row0: got %+v", rows[0])
	}
	if rows[1][0].S != "alice" || rows[1][1].I != 200 {
		t.Fatalf("row1: got %+v", rows[1])
	}
	if rows[2][0].S != "bob" || rows[2][1].I != 50 {
		t.Fatalf("row2: got %+v", rows[2])
	}
	if rows[3][0].S != "dave" || rows[3][1].I != 300 {
		t.Fatalf("row3: got %+v", rows[3])
	}
}

// JOIN：LEFT JOIN 无匹配行补空。
func TestP0JoinLeft(t *testing.T) {
	e := newEngine(t)
	setupJoinTables(t, e)
	rows := mustRows(t, e, "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.uid")
	if len(rows) != 5 {
		t.Fatalf("left join: got %d rows, want 5", len(rows))
	}
	// carol（无订单）应为空串补列（行序：alice×2, bob, carol, dave）
	if rows[3][0].S != "carol" || rows[3][1].Kind != "TEXT" || rows[3][1].S != "" {
		t.Fatalf("left join null padding row: got %+v", rows[3])
	}
	// dave 有订单 300
	if rows[4][0].S != "dave" || rows[4][1].I != 300 {
		t.Fatalf("left join dave: got %+v", rows[4])
	}
}

// JOIN：ON 多条件（AND 组合）与 LEFT 语义。
func TestP0JoinMultiCond(t *testing.T) {
	e := newEngine(t)
	setupJoinTables(t, e)
	// INNER：uid 匹配且 amount >= 100
	rows := mustRows(t, e, "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.uid AND o.amount >= 100")
	if len(rows) != 3 {
		t.Fatalf("inner multi-cond: got %d rows, want 3", len(rows))
	}
	if rows[0][1].I != 100 || rows[1][1].I != 200 || rows[2][1].I != 300 {
		t.Fatalf("inner multi-cond amounts: got %+v", rows)
	}
	// LEFT：bob/carol 无匹配也保留
	rows = mustRows(t, e, "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.uid AND o.amount >= 100")
	if len(rows) != 5 {
		t.Fatalf("left multi-cond: got %d rows, want 5", len(rows))
	}
	if rows[1][1].S != "" || rows[2][1].S != "" {
		t.Fatalf("left multi-cond nulls: got %+v", rows)
	}
}

// JOIN：限定列用于 WHERE / ORDER BY / SELECT 列。
func TestP0JoinQualifiedRefs(t *testing.T) {
	e := newEngine(t)
	setupJoinTables(t, e)
	rows := mustRows(t, e, "SELECT u.name FROM users u JOIN orders o ON u.id = o.uid WHERE o.amount >= 200 ORDER BY u.name")
	if len(rows) != 2 {
		t.Fatalf("qualified refs: got %d rows, want 2", len(rows))
	}
	if rows[0][0].S != "alice" || rows[1][0].S != "dave" {
		t.Fatalf("qualified refs order: got %+v", rows)
	}
}

// GROUP BY：多组分组聚合。
func TestP0GroupBy(t *testing.T) {
	e := newEngine(t)
	setupJoinTables(t, e)
	rows := mustRows(t, e, "SELECT uid, COUNT(*), SUM(amount) FROM orders GROUP BY uid")
	if len(rows) != 3 {
		t.Fatalf("group by: got %d rows, want 3", len(rows))
	}
	// 组序 = 表扫描序：uid 1, 2, 4
	if rows[0][0].I != 1 || rows[0][1].I != 2 || rows[0][2].I != 300 {
		t.Fatalf("group1: got %+v", rows[0])
	}
	if rows[1][0].I != 2 || rows[1][1].I != 1 || rows[1][2].I != 50 {
		t.Fatalf("group2: got %+v", rows[1])
	}
	if rows[2][0].I != 4 || rows[2][1].I != 1 || rows[2][2].I != 300 {
		t.Fatalf("group3: got %+v", rows[2])
	}
}

// GROUP BY：JOIN 结果按限定列分组。
func TestP0GroupByJoin(t *testing.T) {
	e := newEngine(t)
	setupJoinTables(t, e)
	rows := mustRows(t, e, "SELECT u.name, COUNT(*), SUM(o.amount) FROM users u JOIN orders o ON u.id = o.uid GROUP BY u.name")
	if len(rows) != 3 {
		t.Fatalf("group by join: got %d rows, want 3", len(rows))
	}
	if rows[0][0].S != "alice" || rows[0][1].I != 2 || rows[0][2].I != 300 {
		t.Fatalf("alice group: got %+v", rows[0])
	}
	if rows[1][0].S != "bob" || rows[1][1].I != 1 || rows[1][2].I != 50 {
		t.Fatalf("bob group: got %+v", rows[1])
	}
	if rows[2][0].S != "dave" || rows[2][1].I != 1 || rows[2][2].I != 300 {
		t.Fatalf("dave group: got %+v", rows[2])
	}
}

// GROUP BY：非聚合列不在分组键 → 报错。
func TestP0GroupByNonGroupColError(t *testing.T) {
	e := newEngine(t)
	setupJoinTables(t, e)
	_, err := e.Execute("SELECT id, COUNT(*) FROM orders GROUP BY uid")
	if err == nil {
		t.Fatal("expected error for non-grouped column")
	}
}

// 子查询：FROM 派生表 + 外层限定引用。
func TestP0FromSubquery(t *testing.T) {
	e := newEngine(t)
	setupJoinTables(t, e)
	rows := mustRows(t, e, "SELECT t.name, t.age FROM (SELECT id, name, age FROM users WHERE age >= 30) t")
	if len(rows) != 3 {
		t.Fatalf("from subquery: got %d rows, want 3", len(rows))
	}
	if rows[0][0].S != "alice" || rows[0][1].I != 30 {
		t.Fatalf("row0: got %+v", rows[0])
	}
	if rows[2][0].S != "dave" || rows[2][1].I != 40 {
		t.Fatalf("row2: got %+v", rows[2])
	}
}

// 子查询：嵌套（外层 WHERE 引用派生列）。
func TestP0SubqueryNested(t *testing.T) {
	e := newEngine(t)
	setupJoinTables(t, e)
	rows := mustRows(t, e, "SELECT s.name FROM (SELECT u.name, u.age FROM users u WHERE u.age > 25) s WHERE s.age < 40 ORDER BY s.name")
	if len(rows) != 2 {
		t.Fatalf("nested subquery: got %d rows, want 2", len(rows))
	}
	if rows[0][0].S != "alice" || rows[1][0].S != "carol" {
		t.Fatalf("nested subquery order: got %+v", rows)
	}
}

// WHERE：BETWEEN a AND b。
func TestP0WhereBetween(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	rows := mustRows(t, e, "SELECT name FROM users WHERE age BETWEEN 25 AND 35")
	if len(rows) != 3 {
		t.Fatalf("between: got %d rows, want 3", len(rows))
	}
	if rows[0][0].S != "alice" || rows[1][0].S != "bob" || rows[2][0].S != "carol" {
		t.Fatalf("between names: got %+v", rows)
	}
}

// WHERE：IN 列表。
func TestP0WhereInList(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	rows := mustRows(t, e, "SELECT name FROM users WHERE id IN (1, 3)")
	if len(rows) != 2 {
		t.Fatalf("in list: got %d rows, want 2", len(rows))
	}
	if rows[0][0].S != "alice" || rows[1][0].S != "carol" {
		t.Fatalf("in list names: got %+v", rows)
	}
}

// WHERE：IN 子查询。
func TestP0WhereInSubquery(t *testing.T) {
	e := newEngine(t)
	setupJoinTables(t, e)
	rows := mustRows(t, e, "SELECT name FROM users WHERE id IN (SELECT uid FROM orders WHERE amount > 100)")
	if len(rows) != 2 {
		t.Fatalf("in subquery: got %d rows, want 2", len(rows))
	}
	if rows[0][0].S != "alice" || rows[1][0].S != "dave" {
		t.Fatalf("in subquery names: got %+v", rows)
	}
}

// 事务：BEGIN-COMMIT 显式提交。
func TestP0TxnBeginCommit(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO users VALUES (5, 'eve', 28)")
	// 事务内可见
	if rows := mustRows(t, e, "SELECT id FROM users WHERE id = 5"); len(rows) != 1 {
		t.Fatalf("txn read-your-writes: got %+v", rows)
	}
	mustExec(t, e, "COMMIT")
	// 提交后持久可见
	rows := mustRows(t, e, "SELECT id FROM users WHERE id = 5")
	if len(rows) != 1 {
		t.Fatalf("after commit: got %+v", rows)
	}
}

// 事务：BEGIN-ROLLBACK 丢弃写。
func TestP0TxnBeginRollback(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO users VALUES (5, 'eve', 28)")
	if rows := mustRows(t, e, "SELECT id FROM users WHERE id = 5"); len(rows) != 1 {
		t.Fatalf("txn read-your-writes: got %+v", rows)
	}
	mustExec(t, e, "ROLLBACK")
	rows := mustRows(t, e, "SELECT id FROM users WHERE id = 5")
	if len(rows) != 0 {
		t.Fatalf("after rollback should be gone: got %+v", rows)
	}
}

// 事务：DDL 也纳入显式事务，ROLLBACK 后表不存在。
func TestP0TxnRollbackDDL(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "CREATE TABLE tmp_t (id INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO tmp_t VALUES (1)")
	mustExec(t, e, "ROLLBACK")
	if _, err := e.Execute("SELECT * FROM tmp_t"); err == nil {
		t.Fatal("tmp_t should not exist after rollback")
	}
	// 原表不受影响
	if rows := mustRows(t, e, "SELECT COUNT(*) FROM users"); len(rows) != 1 || rows[0][0].I != 3 {
		t.Fatalf("users after txn: got %+v", rows)
	}
}

// 事务：状态约束错误。
func TestP0TxnStateErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	if _, err := e.Execute("COMMIT"); err == nil {
		t.Fatal("commit without begin should error")
	}
	if _, err := e.Execute("ROLLBACK"); err == nil {
		t.Fatal("rollback without begin should error")
	}
	mustExec(t, e, "BEGIN")
	if _, err := e.Execute("BEGIN"); err == nil {
		t.Fatal("double begin should error")
	}
	mustExec(t, e, "COMMIT")
	// 结束后可重新开启
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "ROLLBACK")
}

// 事务：未 BEGIN 时保持单语句隐式提交。
func TestP0TxnAutocommitDefault(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "INSERT INTO users VALUES (5, 'eve', 28)")
	rows := mustRows(t, e, "SELECT id FROM users WHERE id = 5")
	if len(rows) != 1 {
		t.Fatalf("autocommit insert visible: got %+v", rows)
	}
}

// ---- P1：类型扩充 DATE / DECIMAL / BLOB + CSV 导入导出 ----

// P1-1 新类型建表、插入与读取（含大写 hex 归一）。
func TestP1CreateInsertNewTypes(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE emp (id INT, hired DATE, salary DECIMAL, photo BLOB, note TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO emp VALUES (1, '2024-03-15', 12345.5, '0a1b', 'alice')")
	mustExec(t, e, "INSERT INTO emp VALUES (2, '2023-12-31', 99.9999, 'ff00aa', 'bob')")
	rows := mustRows(t, e, "SELECT id, hired, salary, photo, note FROM emp WHERE id = 1")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	r := rows[0]
	if r[1].Kind != "DATE" || r[1].S != "2024-03-15" {
		t.Fatalf("hired: got %+v", r[1])
	}
	if r[2].Kind != "DECIMAL" || r[2].S != "12345.5000" {
		t.Fatalf("salary: got %+v", r[2])
	}
	if r[3].Kind != "BLOB" || r[3].S != "0A1B" {
		t.Fatalf("photo: got %+v", r[3])
	}
	if r[4].Kind != "TEXT" || r[4].S != "alice" {
		t.Fatalf("note: got %+v", r[4])
	}
	// DECIMAL 小数位超限报错
	mustErr(t, e, "INSERT INTO emp VALUES (3, '2024-01-01', 1.12345, '00', 'x')", "DECIMAL too many")
	// DATE 非法值报错
	mustErr(t, e, "INSERT INTO emp VALUES (3, '2024-02-30', 1.0, '00', 'x')", "invalid DATE")
	mustErr(t, e, "INSERT INTO emp VALUES (3, '24-01-01', 1.0, '00', 'x')", "invalid DATE")
	// BLOB 非法 hex 报错
	mustErr(t, e, "INSERT INTO emp VALUES (3, '2024-01-01', 1.0, 'xyz', 'x')", "BLOB")
}

// P1-2 DATE 合法性：闰年与边界校验。
func TestP1DateValidation(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE d (id INT, day DATE, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO d VALUES (1, '2024-02-29')") // 闰年
	mustErr(t, e, "INSERT INTO d VALUES (2, '2023-02-29')", "invalid DATE")
	mustErr(t, e, "INSERT INTO d VALUES (2, '2024-13-01')", "invalid DATE")
	mustErr(t, e, "INSERT INTO d VALUES (2, '2024-00-10')", "invalid DATE")
	mustErr(t, e, "INSERT INTO d VALUES (2, '2024-01-32')", "invalid DATE")
	// TEXT → DATE 列自动归一
	mustExec(t, e, "INSERT INTO d VALUES (2, '2024-06-30')")
	rows := mustRows(t, e, "SELECT day FROM d WHERE id = 2")
	if len(rows) != 1 || rows[0][0].Kind != "DATE" || rows[0][0].S != "2024-06-30" {
		t.Fatalf("date coercion: got %+v", rows)
	}
}

// P1-3 DECIMAL 比较、排序与等值匹配。
func TestP1DecimalCompareOrder(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE p (id INT, price DECIMAL, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO p VALUES (1, 10.5), (2, 2.25), (3, 10.5000), (4, -1.0), (5, 0.0001)")
	rows := mustRows(t, e, "SELECT id FROM p ORDER BY price")
	want := []int64{4, 5, 2, 1, 3}
	if len(rows) != len(want) {
		t.Fatalf("order rows: got %+v", rows)
	}
	for i, w := range want {
		if rows[i][0].I != w {
			t.Fatalf("order[%d]: want id %d, got %+v", i, w, rows[i])
		}
	}
	// 等值：10.5000 与 10.5 归一后相等
	rows = mustRows(t, e, "SELECT id FROM p WHERE price = 10.5")
	if len(rows) != 2 {
		t.Fatalf("eq 10.5: got %+v", rows)
	}
	// 范围比较
	rows = mustRows(t, e, "SELECT id FROM p WHERE price > 2.0 AND price <= 10.5")
	if len(rows) != 3 {
		t.Fatalf("range: got %+v", rows)
	}
	// INT 与 DECIMAL 跨类型比较（1 < 10.5）
	mustExec(t, e, "CREATE TABLE p2 (id INT, price DECIMAL, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO p2 VALUES (1, 1.0)")
	rows = mustRows(t, e, "SELECT id FROM p2 WHERE price > 0")
	if len(rows) != 1 {
		t.Fatalf("cross-int compare: got %+v", rows)
	}
}

// P1-4 DECIMAL 聚合 SUM/AVG。
func TestP1DecimalAgg(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE a (id INT, amount DECIMAL, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO a VALUES (1, 1.25), (2, 2.50), (3, 0.25)")
	rows := mustRows(t, e, "SELECT SUM(amount), AVG(amount) FROM a")
	if len(rows) != 1 {
		t.Fatalf("agg rows: got %+v", rows)
	}
	if rows[0][0].Kind != "DECIMAL" || rows[0][0].S != "4.0000" {
		t.Fatalf("SUM: got %+v", rows[0][0])
	}
	// AVG = (1.25+2.50+0.25)/3 = 4.00/3 = 1.3333（缩放整数除法截断）
	if rows[0][1].Kind != "DECIMAL" || rows[0][1].S != "1.3333" {
		t.Fatalf("AVG: got %+v", rows[0][1])
	}
	// 空表 AVG 返回 0
	mustExec(t, e, "CREATE TABLE a2 (id INT, amount DECIMAL, PRIMARY KEY (id))")
	rows = mustRows(t, e, "SELECT AVG(amount) FROM a2")
	if len(rows) != 1 || rows[0][0].S != "0.0000" {
		t.Fatalf("empty AVG: got %+v", rows)
	}
}

// P1-5 BLOB 存取：小写 hex 归一为大写、原始字节经索引/主键保真。
func TestP1BlobRoundtrip(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE b (id INT, data BLOB, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO b VALUES (1, 'deadBEEF')")
	mustExec(t, e, "INSERT INTO b VALUES (2, '00ff')")
	rows := mustRows(t, e, "SELECT data FROM b WHERE id = 1")
	if len(rows) != 1 || rows[0][0].Kind != "BLOB" || rows[0][0].S != "DEADBEEF" {
		t.Fatalf("blob upper: got %+v", rows)
	}
	// 等值匹配
	rows = mustRows(t, e, "SELECT id FROM b WHERE data = 'deadbeef'")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("blob eq: got %+v", rows)
	}
	// BLOB 作主键
	mustExec(t, e, "CREATE TABLE b2 (k BLOB, v TEXT, PRIMARY KEY (k))")
	mustExec(t, e, "INSERT INTO b2 VALUES ('a1b2', 'x')")
	rows = mustRows(t, e, "SELECT v FROM b2 WHERE k = 'A1B2'")
	if len(rows) != 1 || rows[0][0].S != "x" {
		t.Fatalf("blob pk: got %+v", rows)
	}
}

// P1-6 CSV 导出→导入往返一致性。
func TestP1CsvExportImportRoundtrip(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE src (id INT, name TEXT, hired DATE, salary DECIMAL, data BLOB, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO src VALUES (1, 'alice', '2024-03-15', 12345.5, '0a1b')")
	mustExec(t, e, "INSERT INTO src VALUES (2, 'bob, \"jr\"', '2023-12-31', 99.9999, 'ff00aa')")
	// 含引号与逗号的 TEXT 也要往返保真
	csvPath := filepath.Join(t.TempDir(), "src.csv")
	res := mustExec(t, e, "EXPORT TABLE src TO '"+csvPath+"'")
	if res.AffectedRows != 2 {
		t.Fatalf("export affected: got %d", res.AffectedRows)
	}
	// 校验文件头
	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("open csv: %v", err)
	}
	r := csv.NewReader(f)
	recs, err := r.ReadAll()
	f.Close()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("csv lines: got %d", len(recs))
	}
	wantHeader := []string{"id", "name", "hired", "salary", "data"}
	for i, h := range wantHeader {
		if recs[0][i] != h {
			t.Fatalf("header[%d]: got %q", i, recs[0][i])
		}
	}
	if recs[1][1] != "alice" || recs[1][2] != "2024-03-15" || recs[1][3] != "12345.5000" || recs[1][4] != "0A1B" {
		t.Fatalf("row1: got %v", recs[1])
	}
	if recs[2][1] != "bob, \"jr\"" {
		t.Fatalf("row2 quoted text: got %q", recs[2][1])
	}
	// 导入到新表
	mustExec(t, e, "CREATE TABLE dst (id INT, name TEXT, hired DATE, salary DECIMAL, data BLOB, PRIMARY KEY (id))")
	res = mustExec(t, e, "IMPORT INTO dst FROM '"+csvPath+"'")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 2 || res.Rows[0][1].I != 0 {
		t.Fatalf("import result: got %+v", res.Rows)
	}
	// 逐行对比
	srcRows := mustRows(t, e, "SELECT * FROM src ORDER BY id")
	dstRows := mustRows(t, e, "SELECT * FROM dst ORDER BY id")
	if len(srcRows) != len(dstRows) {
		t.Fatalf("row count mismatch: %d vs %d", len(srcRows), len(dstRows))
	}
	for i := range srcRows {
		for j := range srcRows[i] {
			if srcRows[i][j].String() != dstRows[i][j].String() {
				t.Fatalf("row %d col %d mismatch: %v vs %v", i, j, srcRows[i][j], dstRows[i][j])
			}
		}
	}
}

// P1-7 CSV 列子集导出。
func TestP1CsvExportColumns(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t VALUES (1, 'alice', 30), (2, 'bob', 25)")
	csvPath := filepath.Join(t.TempDir(), "cols.csv")
	res := mustExec(t, e, "EXPORT TABLE t (name, age) TO '"+csvPath+"'")
	if len(res.Columns) != 2 || res.Columns[0] != "name" || res.Columns[1] != "age" {
		t.Fatalf("export columns: got %v", res.Columns)
	}
	f, _ := os.Open(csvPath)
	r := csv.NewReader(f)
	recs, _ := r.ReadAll()
	f.Close()
	if len(recs) != 3 || recs[0][0] != "name" || recs[0][1] != "age" || recs[1][0] != "alice" || recs[1][1] != "30" {
		t.Fatalf("cols csv: got %v", recs)
	}
	// 未知列报错
	mustErr(t, e, "EXPORT TABLE t (nope) TO '"+filepath.Join(t.TempDir(), "x.csv")+"'", "unknown column")
}

// P1-8 CSV 导入：重复主键跳过、非法单元格整体回滚。
func TestP1CsvImportSemantics(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, v TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t VALUES (1, 'existing')")
	bad := filepath.Join(t.TempDir(), "bad.csv")
	os.WriteFile(bad, []byte("id,v\n1,dup\nabc,no\n"), 0o644)
	if _, err := e.Execute("IMPORT INTO t FROM '" + bad + "'"); err == nil {
		t.Fatal("import with invalid INT should fail")
	}
	// 原子回滚：existing 行与 dup 行均未生效
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 1 {
		t.Fatalf("atomic rollback: got %+v", rows)
	}
	// 重复主键跳过
	dup := filepath.Join(t.TempDir(), "dup.csv")
	os.WriteFile(dup, []byte("id,v\n1,dup\n2,new\n"), 0o644)
	res := mustExec(t, e, "IMPORT INTO t FROM '"+dup+"'")
	if res.Rows[0][0].I != 1 || res.Rows[0][1].I != 1 {
		t.Fatalf("dup skip: got %+v", res.Rows)
	}
	rows = mustRows(t, e, "SELECT id FROM t ORDER BY id")
	if len(rows) != 2 || rows[0][0].I != 1 || rows[1][0].I != 2 {
		t.Fatalf("after dup import: got %+v", rows)
	}
	// 表头不匹配报错
	hd := filepath.Join(t.TempDir(), "hd.csv")
	os.WriteFile(hd, []byte("id,wrong\n1,x\n"), 0o644)
	if _, err := e.Execute("IMPORT INTO t FROM '" + hd + "'"); err == nil || !strings.Contains(err.Error(), "header") {
		t.Fatalf("header mismatch: got %v", err)
	}
}

// P1-9 新类型与 INT/TEXT 混用：WHERE、UPDATE、二级索引。
func TestP1MixedTypesWithIndex(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE m (id INT, day DATE, score DECIMAL, blobd BLOB, tag TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO m VALUES (1, '2024-01-01', 10.0, 'aa', 'x'), (2, '2024-02-01', 20.0, 'bb', 'y')")
	mustExec(t, e, "CREATE INDEX idx_day ON m (day)")
	mustExec(t, e, "CREATE INDEX idx_score ON m (score)")
	// 索引列等值/范围查询
	rows := mustRows(t, e, "SELECT id FROM m WHERE day = '2024-02-01'")
	if len(rows) != 1 || rows[0][0].I != 2 {
		t.Fatalf("index date eq: got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM m WHERE score > 5.0 AND score <= 20.0")
	if len(rows) != 2 {
		t.Fatalf("index decimal range: got %+v", rows)
	}
	// UPDATE 新类型列
	mustExec(t, e, "UPDATE m SET day = '2024-03-01', score = 25.5 WHERE id = 1")
	rows = mustRows(t, e, "SELECT day, score FROM m WHERE id = 1")
	if rows[0][0].S != "2024-03-01" || rows[0][1].S != "25.5000" {
		t.Fatalf("update new types: got %+v", rows)
	}
	// 混用过滤
	rows = mustRows(t, e, "SELECT id FROM m WHERE tag = 'x' AND blobd = 'AA'")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("mixed filter: got %+v", rows)
	}
}

// ---- P2 运维语句：SHOW TABLES ----

func TestP2ShowTables(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	mustExec(t, e, "CREATE TABLE orders (oid INT, uid INT, total INT, PRIMARY KEY (oid))")
	res := mustExec(t, e, "SHOW TABLES")
	wantCols := []string{"table", "columns", "pk", "indexes"}
	if len(res.Columns) != len(wantCols) {
		t.Fatalf("SHOW TABLES columns: got %v", res.Columns)
	}
	for i := range wantCols {
		if res.Columns[i] != wantCols[i] {
			t.Fatalf("SHOW TABLES column %d: want %s, got %s", i, wantCols[i], res.Columns[i])
		}
	}
	if len(res.Rows) != 2 {
		t.Fatalf("SHOW TABLES: want 2 rows, got %d (%+v)", len(res.Rows), res.Rows)
	}
	// SHOW TABLES 按表名排序，定位 users 行校验
	var usersRow []sql.Value
	for _, r := range res.Rows {
		if len(r) > 0 && r[0].S == "users" {
			usersRow = r
			break
		}
	}
	if usersRow == nil {
		t.Fatalf("SHOW TABLES: users row not found: %+v", res.Rows)
	}
	if usersRow[1].S != "id:INT, name:TEXT, age:INT" || usersRow[2].S != "id" {
		t.Fatalf("SHOW TABLES users row: got %+v", usersRow)
	}
	if !strings.Contains(usersRow[3].S, "idx_age") {
		t.Fatalf("SHOW TABLES users indexes: got %q", usersRow[3].S)
	}
}

// ---- P2 运维语句：SHOW INDEX ----

func TestP2ShowIndex(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	res := mustExec(t, e, "SHOW INDEX FROM users")
	if len(res.Rows) != 1 {
		t.Fatalf("SHOW INDEX users: want 1 row, got %d", len(res.Rows))
	}
	r := res.Rows[0]
	if r[0].S != "users" || r[1].S != "idx_age" || r[2].S != "age" || r[3].I != 1 || r[4].S != "NO" {
		t.Fatalf("SHOW INDEX row: got %+v", r)
	}
	mustExec(t, e, "CREATE TABLE plain (id INT, PRIMARY KEY (id))")
	res = mustExec(t, e, "SHOW INDEX FROM plain")
	if len(res.Rows) != 0 {
		t.Fatalf("SHOW INDEX plain: want 0 rows, got %d", len(res.Rows))
	}
	mustErr(t, e, "SHOW INDEX FROM nope", "not exists")
}

// ---- P2 运维语句：EXPLAIN ----

func TestP2Explain(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35)")
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")

	rows := mustRows(t, e, "EXPLAIN SELECT name FROM users WHERE id = 1")
	if !planContains(rows, "point lookup on PK (id)") {
		t.Fatalf("explain point lookup: %+v", rows)
	}
	rows = mustRows(t, e, "EXPLAIN SELECT name FROM users WHERE age >= 25")
	if !planContains(rows, "index scan on idx_age(age)") {
		t.Fatalf("explain index scan: %+v", rows)
	}
	rows = mustRows(t, e, "EXPLAIN SELECT name FROM users WHERE name LIKE 'a%'")
	if !planContains(rows, "table scan (full)") || !planContains(rows, "LIKE") {
		t.Fatalf("explain like full scan: %+v", rows)
	}
	rows = mustRows(t, e, "EXPLAIN SELECT age, COUNT(*) FROM users GROUP BY age")
	if !planContains(rows, "group: age") || !planContains(rows, "aggregate: yes") {
		t.Fatalf("explain group/agg: %+v", rows)
	}
	rows = mustRows(t, e, "EXPLAIN SELECT * FROM users ORDER BY age DESC LIMIT 1")
	if !planContains(rows, "sort: age DESC") || !planContains(rows, "limit: 1") {
		t.Fatalf("explain sort/limit: %+v", rows)
	}
	rows = mustRows(t, e, "EXPLAIN SELECT u.name FROM users u JOIN users v ON u.id = v.id")
	if !planContains(rows, "nested-loop join") {
		t.Fatalf("explain join: %+v", rows)
	}
	mustErr(t, e, "EXPLAIN SELECT * FROM nope", "not exists")
}

func planContains(rows [][]sql.Value, sub string) bool {
	for _, r := range rows {
		if len(r) > 0 && strings.Contains(r[0].S, sub) {
			return true
		}
	}
	return false
}

// ---- P2 慢查询：阈值关闭 / 触发 / SHOW SLOWQUERIES ----

func TestP2SlowQueries(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE big (id INT, v TEXT, PRIMARY KEY (id))")
	var sb strings.Builder
	sb.WriteString("INSERT INTO big (id, v) VALUES ")
	for i := 1; i <= 300; i++ {
		if i > 1 {
			sb.WriteString(", ")
		}
		sb.WriteString("(" + itoaTest(i) + ", 'value-" + itoaTest(i) + "')")
	}
	mustExec(t, e, sb.String())

	// 阈值关闭：执行再多次也不记录
	e.SetSlowThreshold(0)
	for i := 0; i < 3; i++ {
		mustRows(t, e, "SELECT * FROM big")
	}
	if got := len(e.SlowQueries()); got != 0 {
		t.Fatalf("threshold off: want 0 slow queries, got %d", got)
	}
	// 阈值 1ms：全表扫描应触发记录
	e.SetSlowThreshold(1)
	for i := 0; i < 5; i++ {
		mustRows(t, e, "SELECT * FROM big WHERE id > 0")
	}
	recs := e.SlowQueries()
	if len(recs) == 0 {
		t.Fatalf("threshold 1ms: want >=1 slow query, got 0")
	}
	// SHOW SLOWQUERIES 视图
	res := mustExec(t, e, "SHOW SLOWQUERIES")
	if len(res.Rows) == 0 {
		t.Fatalf("SHOW SLOWQUERIES: want rows, got 0")
	}
	row := res.Rows[0]
	if len(row) != 3 || row[0].S == "" || row[1].I <= 0 || !strings.Contains(row[2].S, "SELECT") {
		t.Fatalf("SHOW SLOWQUERIES row: got %+v", row)
	}
	// 关闭后新查询不再追加
	e.SetSlowThreshold(0)
	before := len(e.SlowQueries())
	mustRows(t, e, "SELECT * FROM big")
	if got := len(e.SlowQueries()); got != before {
		t.Fatalf("re-disable: want %d, got %d", before, got)
	}
}

func itoaTest(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// ---- P2 LIKE：% / _ 模式与 AND 组合 ----

func TestP2Like(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	rows := mustRows(t, e, "SELECT id FROM users WHERE name LIKE 'a%'")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("LIKE 'a%%': got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE name LIKE '%ol'")
	if len(rows) != 1 || rows[0][0].I != 3 {
		t.Fatalf("LIKE '%%ol': got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE name LIKE '%o%'")
	if len(rows) != 2 {
		t.Fatalf("LIKE '%%o%%': got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE name LIKE 'a_ice'")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("LIKE 'a_ice': got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE name LIKE 'a_lice'")
	if len(rows) != 0 {
		t.Fatalf("LIKE 'a_lice': want 0 rows, got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE name LIKE '%'")
	if len(rows) != 3 {
		t.Fatalf("LIKE '%%': want 3 rows, got %d", len(rows))
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE name LIKE 'A%'")
	if len(rows) != 0 {
		t.Fatalf("LIKE case-sensitive: want 0 rows, got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE name LIKE '%a%' AND age >= 25")
	if len(rows) != 2 {
		t.Fatalf("LIKE + AND: got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE age > 20 AND name LIKE '%o%'")
	if len(rows) != 2 {
		t.Fatalf("index + LIKE: got %+v", rows)
	}
}

// ---- P2 CASE WHEN：SELECT 多分支 / ELSE / 别名 / WHERE / JOIN 组合 ----

func TestP2CaseSelect(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	res := mustExec(t, e, "SELECT id, CASE WHEN age < 26 THEN 'young' WHEN age < 32 THEN 'mid' ELSE 'senior' END AS grp FROM users ORDER BY id")
	if len(res.Columns) != 2 || res.Columns[1] != "grp" {
		t.Fatalf("case alias columns: got %v", res.Columns)
	}
	want := []string{"mid", "young", "senior"}
	if len(res.Rows) != 3 {
		t.Fatalf("case rows: got %d", len(res.Rows))
	}
	for i, w := range want {
		if res.Rows[i][1].S != w {
			t.Fatalf("case row %d: want %s, got %+v", i, w, res.Rows[i])
		}
	}
	res = mustExec(t, e, "SELECT CASE WHEN age > 100 THEN 'x' END AS c FROM users WHERE id = 1")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "" {
		t.Fatalf("case no else: got %+v", res.Rows)
	}
	res = mustExec(t, e, "SELECT CASE WHEN age >= 30 THEN 100 ELSE 0 END AS n FROM users WHERE id = 1")
	if len(res.Rows) != 1 || res.Rows[0][0].Kind != "INT" || res.Rows[0][0].I != 100 {
		t.Fatalf("case int then: got %+v", res.Rows)
	}
}

func TestP2CaseWhere(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	rows := mustRows(t, e, "SELECT id FROM users WHERE CASE WHEN age >= 30 THEN 1 ELSE 0 END = 1")
	if len(rows) != 2 {
		t.Fatalf("case in where: got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE CASE WHEN age < 35 THEN 'ok' ELSE 'no' END = 'ok' AND name LIKE 'a%'")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("case + like in where: got %+v", rows)
	}
}

func TestP2CaseJoinGroup(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35)")
	mustExec(t, e, "CREATE TABLE scores (uid INT, score INT, PRIMARY KEY (uid))")
	mustExec(t, e, "INSERT INTO scores VALUES (1, 90), (2, 80)")
	res := mustExec(t, e, "SELECT u.name, CASE WHEN s.score >= 85 THEN 'A' ELSE 'B' END AS grade FROM users u JOIN scores s ON u.id = s.uid ORDER BY u.id")
	if len(res.Rows) != 2 {
		t.Fatalf("case join rows: got %+v", res.Rows)
	}
	if res.Rows[0][1].S != "A" || res.Rows[1][1].S != "B" {
		t.Fatalf("case join grades: got %+v", res.Rows)
	}
	res = mustExec(t, e, "SELECT age, CASE WHEN age >= 30 THEN 'old' ELSE 'young' END AS grp FROM users GROUP BY age")
	if len(res.Rows) != 3 {
		t.Fatalf("case group rows: got %+v", res.Rows)
	}
	got := map[int64]string{}
	for _, r := range res.Rows {
		got[r[0].I] = r[1].S
	}
	if got[25] != "young" || got[30] != "old" || got[35] != "old" {
		t.Fatalf("case group values: got %+v", got)
	}
}

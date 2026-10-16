package sql_test

import (
	"reflect"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// T42: INSERT INTO ... SELECT / REPLACE INTO ... SELECT
// 设计文档: docs/T42_insert_select.md

func wantRows(t *testing.T, got [][]sql.Value, want [][]sql.Value) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows mismatch:\n got  %+v\n want %+v", got, want)
	}
}

func TestT42InsertSelectBasic(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42b_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42b_dst (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t42b_src VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	mustExec(t, e, "INSERT INTO t42b_dst SELECT * FROM t42b_src")
	rows := mustRows(t, e, "SELECT id, name FROM t42b_dst ORDER BY id")
	wantRows(t, rows, [][]sql.Value{
		{sql.IntVal(1), sql.StrVal("a")},
		{sql.IntVal(2), sql.StrVal("b")},
		{sql.IntVal(3), sql.StrVal("c")},
	})
}

func TestT42InsertSelectSpecifiedCols(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42c_src (id INT PRIMARY KEY, name TEXT, extra INT)")
	mustExec(t, e, "CREATE TABLE t42c_dst (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t42c_src VALUES (1, 'a', 100), (2, 'b', 200)")

	mustExec(t, e, "INSERT INTO t42c_dst (id, name) SELECT id, name FROM t42c_src")
	rows := mustRows(t, e, "SELECT id, name FROM t42c_dst ORDER BY id")
	wantRows(t, rows, [][]sql.Value{
		{sql.IntVal(1), sql.StrVal("a")},
		{sql.IntVal(2), sql.StrVal("b")},
	})
}

func TestT42InsertSelectColumnMismatch(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42m_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42m_dst (id INT PRIMARY KEY, name TEXT, extra INT)")
	mustExec(t, e, "INSERT INTO t42m_src VALUES (1, 'a')")

	mustErr(t, e, "INSERT INTO t42m_dst SELECT * FROM t42m_src", "column count mismatch")
}

func TestT42InsertSelectWhere(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42w_src (id INT PRIMARY KEY, val INT)")
	mustExec(t, e, "CREATE TABLE t42w_dst (id INT PRIMARY KEY, val INT)")
	mustExec(t, e, "INSERT INTO t42w_src VALUES (1, 10), (2, 20), (3, 30)")

	mustExec(t, e, "INSERT INTO t42w_dst SELECT id, val FROM t42w_src WHERE val >= 20")
	rows := mustRows(t, e, "SELECT id, val FROM t42w_dst ORDER BY id")
	wantRows(t, rows, [][]sql.Value{
		{sql.IntVal(2), sql.IntVal(20)},
		{sql.IntVal(3), sql.IntVal(30)},
	})
}

func TestT42InsertSelectExprAndAlias(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42e_src (id INT PRIMARY KEY, val INT)")
	mustExec(t, e, "CREATE TABLE t42e_dst (id INT PRIMARY KEY, val INT)")
	mustExec(t, e, "INSERT INTO t42e_src VALUES (1, 10), (2, 20)")

	mustExec(t, e, "INSERT INTO t42e_dst SELECT id, val * 2 AS v FROM t42e_src")
	rows := mustRows(t, e, "SELECT id, val FROM t42e_dst ORDER BY id")
	wantRows(t, rows, [][]sql.Value{
		{sql.IntVal(1), sql.IntVal(20)},
		{sql.IntVal(2), sql.IntVal(40)},
	})
}

func TestT42InsertSelectAggGroupBy(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42a_src (id INT PRIMARY KEY, grp TEXT, val INT)")
	mustExec(t, e, "CREATE TABLE t42a_dst (grp TEXT PRIMARY KEY, total INT)")
	mustExec(t, e, "INSERT INTO t42a_src VALUES (1, 'x', 1), (2, 'x', 2), (3, 'y', 5)")

	mustExec(t, e, "INSERT INTO t42a_dst SELECT grp, SUM(val) FROM t42a_src GROUP BY grp")
	rows := mustRows(t, e, "SELECT grp, total FROM t42a_dst ORDER BY grp")
	wantRows(t, rows, [][]sql.Value{
		{sql.StrVal("x"), sql.IntVal(3)},
		{sql.StrVal("y"), sql.IntVal(5)},
	})
}

func TestT42InsertSelectDistinct(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42d_src (id INT PRIMARY KEY, city TEXT)")
	mustExec(t, e, "CREATE TABLE t42d_dst (city TEXT PRIMARY KEY)")
	mustExec(t, e, "INSERT INTO t42d_src VALUES (1, 'bj'), (2, 'sh'), (3, 'bj')")

	mustExec(t, e, "INSERT INTO t42d_dst SELECT DISTINCT city FROM t42d_src")
	rows := mustRows(t, e, "SELECT city FROM t42d_dst ORDER BY city")
	wantRows(t, rows, [][]sql.Value{
		{sql.StrVal("bj")},
		{sql.StrVal("sh")},
	})
}

func TestT42InsertSelectEmptyResult(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42em_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42em_dst (id INT PRIMARY KEY, name TEXT)")

	mustExec(t, e, "INSERT INTO t42em_dst SELECT * FROM t42em_src WHERE id > 100")
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t42em_dst")
	wantRows(t, rows, [][]sql.Value{{sql.IntVal(0)}})
}

func TestT42InsertSelectTypeMismatch(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42ty_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42ty_dst (id INT PRIMARY KEY, num INT)")
	mustExec(t, e, "INSERT INTO t42ty_src VALUES (1, 'abc')")

	mustErr(t, e, "INSERT INTO t42ty_dst SELECT * FROM t42ty_src", "type mismatch for column")
}

func TestT42InsertSelectDuplicatePK(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42pk_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42pk_dst (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t42pk_src VALUES (1, 'a'), (2, 'b')")
	mustExec(t, e, "INSERT INTO t42pk_dst VALUES (1, 'x')")

	mustErr(t, e, "INSERT INTO t42pk_dst SELECT * FROM t42pk_src", "duplicate primary key")
	// 失败回滚：dst 不应混入 src 的 2 行
	rows := mustRows(t, e, "SELECT id, name FROM t42pk_dst")
	wantRows(t, rows, [][]sql.Value{{sql.IntVal(1), sql.StrVal("x")}})
}

func TestT42ReplaceSelect(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42r_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42r_dst (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t42r_src VALUES (1, 'a'), (2, 'b')")
	mustExec(t, e, "INSERT INTO t42r_dst VALUES (1, 'x')")

	mustExec(t, e, "REPLACE INTO t42r_dst SELECT * FROM t42r_src")
	rows := mustRows(t, e, "SELECT id, name FROM t42r_dst ORDER BY id")
	wantRows(t, rows, [][]sql.Value{
		{sql.IntVal(1), sql.StrVal("a")},
		{sql.IntVal(2), sql.StrVal("b")},
	})
}

func TestT42InsertSelectSelfInsert(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42s (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t42s VALUES (1, 'a'), (2, 'b')")

	// 自插入按快照语义：先取结果再写，不会死循环
	mustExec(t, e, "INSERT INTO t42s SELECT id + 10, name FROM t42s")
	rows := mustRows(t, e, "SELECT id, name FROM t42s ORDER BY id")
	wantRows(t, rows, [][]sql.Value{
		{sql.IntVal(1), sql.StrVal("a")},
		{sql.IntVal(2), sql.StrVal("b")},
		{sql.IntVal(11), sql.StrVal("a")},
		{sql.IntVal(12), sql.StrVal("b")},
	})
}

func TestT42InsertSelectRollbackOnError(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42rb_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42rb_dst (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t42rb_src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	mustExec(t, e, "INSERT INTO t42rb_dst VALUES (2, 'x')")

	mustErr(t, e, "INSERT INTO t42rb_dst SELECT * FROM t42rb_src", "duplicate primary key")
	// 第 1 行成功插入后遇冲突，整条语句应回滚，只保留原有 2
	rows := mustRows(t, e, "SELECT id, name FROM t42rb_dst")
	wantRows(t, rows, [][]sql.Value{{sql.IntVal(2), sql.StrVal("x")}})
}

func TestT42InsertSelectExplicitTx(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42tx_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42tx_dst (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t42tx_src VALUES (1, 'a'), (2, 'b')")

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO t42tx_dst SELECT * FROM t42tx_src")
	// 同 session 内 read-your-writes：事务内立即可见
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t42tx_dst")
	wantRows(t, rows, [][]sql.Value{{sql.IntVal(2)}})
	mustExec(t, e, "ROLLBACK")
	// 回滚后外部不可见
	rows = mustRows(t, e, "SELECT COUNT(*) FROM t42tx_dst")
	wantRows(t, rows, [][]sql.Value{{sql.IntVal(0)}})

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO t42tx_dst SELECT * FROM t42tx_src")
	mustExec(t, e, "COMMIT")
	rows = mustRows(t, e, "SELECT id, name FROM t42tx_dst ORDER BY id")
	wantRows(t, rows, [][]sql.Value{
		{sql.IntVal(1), sql.StrVal("a")},
		{sql.IntVal(2), sql.StrVal("b")},
	})
}

func TestT42InsertSelectNoColumnListMismatch(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42nc_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42nc_dst (id INT PRIMARY KEY, name TEXT, extra INT)")
	mustExec(t, e, "INSERT INTO t42nc_src VALUES (1, 'a')")

	// 省略列清单按目标表全列，列数不匹配应报错
	mustErr(t, e, "INSERT INTO t42nc_dst SELECT * FROM t42nc_src", "column count mismatch")
}

func TestT42InsertSelectNoColumnListOK(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42ok_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42ok_dst (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t42ok_src VALUES (1, 'a')")

	mustExec(t, e, "INSERT INTO t42ok_dst SELECT * FROM t42ok_src")
	rows := mustRows(t, e, "SELECT id, name FROM t42ok_dst")
	wantRows(t, rows, [][]sql.Value{{sql.IntVal(1), sql.StrVal("a")}})
}

func TestT42InsertSelectLimitOffset(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42l_src (id INT PRIMARY KEY, val INT)")
	mustExec(t, e, "CREATE TABLE t42l_dst (id INT PRIMARY KEY, val INT)")
	mustExec(t, e, "INSERT INTO t42l_src VALUES (1, 10), (2, 20), (3, 30), (4, 40)")

	mustExec(t, e, "INSERT INTO t42l_dst SELECT id, val FROM t42l_src ORDER BY id LIMIT 2 OFFSET 1")
	rows := mustRows(t, e, "SELECT id, val FROM t42l_dst ORDER BY id")
	wantRows(t, rows, [][]sql.Value{
		{sql.IntVal(2), sql.IntVal(20)},
		{sql.IntVal(3), sql.IntVal(30)},
	})
}

func TestT42InsertSelectAffectedRows(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t42af_src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t42af_dst (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t42af_src VALUES (1, 'a'), (2, 'b')")

	res := mustExec(t, e, "INSERT INTO t42af_dst SELECT * FROM t42af_src")
	if res.AffectedRows != 2 {
		t.Fatalf("AffectedRows want 2, got %d", res.AffectedRows)
	}
}

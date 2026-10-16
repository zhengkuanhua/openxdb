package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// M18-B Prepared Statement：PREPARE / EXECUTE / DEALLOCATE PREPARE + 用户变量。

func TestPrepareExecuteBasic(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExec(t, e, "PREPARE p1 FROM 'SELECT id, name FROM t WHERE id = ?'")
	rows := mustRows(t, e, "EXECUTE p1 USING 2")
	if len(rows) != 1 || rows[0][0].I != 2 || rows[0][1].S != "bob" {
		t.Fatalf("execute p1 using 2: want [(2 bob)], got %+v", rows)
	}
	rows = mustRows(t, e, "EXECUTE p1 USING 3")
	if len(rows) != 1 || rows[0][0].I != 3 {
		t.Fatalf("execute p1 using 3: want [(3 carol)], got %+v", rows)
	}
}

func TestPrepareExecuteStringArg(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'o''brien')")
	mustExec(t, e, "PREPARE p2 FROM 'SELECT id FROM t WHERE name = ?'")
	rows := mustRows(t, e, "EXECUTE p2 USING 'alice'")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("execute string arg: want [(1)], got %+v", rows)
	}
	// 字符串参数含单引号：转义后应精确匹配
	rows = mustRows(t, e, "EXECUTE p2 USING 'o''brien'")
	if len(rows) != 1 || rows[0][0].I != 2 {
		t.Fatalf("execute quoted arg: want [(2)], got %+v", rows)
	}
}

func TestPrepareArgCountMismatch(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustExec(t, e, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, e, "PREPARE p3 FROM 'SELECT id FROM t WHERE id = ?'")
	mustErr(t, e, "EXECUTE p3", "argument")
	mustErr(t, e, "EXECUTE p3 USING 1, 2", "argument")
}

func TestExecuteUnprepared(t *testing.T) {
	e := newEngine(t)
	mustErr(t, e, "EXECUTE ghost USING 1", "not found")
	mustErr(t, e, "DEALLOCATE PREPARE ghost", "not found")
}

func TestPrepareOverwriteAndDeallocate(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')")
	// 同名覆盖：旧定义失效
	mustExec(t, e, "PREPARE p4 FROM 'SELECT id FROM t WHERE id = ?'")
	mustExec(t, e, "PREPARE p4 FROM 'SELECT v FROM t WHERE id = ?'")
	rows := mustRows(t, e, "EXECUTE p4 USING 1")
	if len(rows) != 1 || rows[0][0].S != "a" {
		t.Fatalf("overwrite: want [(a)], got %+v", rows)
	}
	mustExec(t, e, "DEALLOCATE PREPARE p4")
	mustErr(t, e, "EXECUTE p4 USING 1", "not found")
}

func TestPrepareUserVars(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob')")
	// SET @var 赋值（M18 扩展）
	mustExec(t, e, "SET @x = 2")
	mustExec(t, e, "SET @n = 'bob'")
	// SELECT @var 读取
	rows := mustRows(t, e, "SELECT @x")
	if len(rows) != 1 || rows[0][0].I != 2 {
		t.Fatalf("select @x: want [(2)], got %+v", rows)
	}
	// EXECUTE USING @var 绑定
	mustExec(t, e, "PREPARE pv FROM 'SELECT id FROM t WHERE id = ? AND name = ?'")
	rows = mustRows(t, e, "EXECUTE pv USING @x, @n")
	if len(rows) != 1 || rows[0][0].I != 2 {
		t.Fatalf("execute using vars: want [(2)], got %+v", rows)
	}
	// 变量未设置报错
	mustErr(t, e, "EXECUTE pv USING @y, @n", "user variable not set")
	// 变量参与普通查询
	rows = mustRows(t, e, "SELECT name FROM t WHERE id = @x")
	if len(rows) != 1 || rows[0][0].S != "bob" {
		t.Fatalf("select with var: want [(bob)], got %+v", rows)
	}
}

func TestPrepareStatementValidation(t *testing.T) {
	e := newEngine(t)
	// 预编译非法 SQL 报错
	mustErr(t, e, "PREPARE bad FROM 'SELECT FROM WHERE'", "syntax error")
	// 普通 SQL 裸 ? 报语法错误（占位符仅限 PREPARE 文本）
	mustErr(t, e, "SELECT 1 WHERE ?", "")
}

// TestPrepareResultSet 验证 prepared 语句结果集列名保留。
func TestPrepareResultSet(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, 'x')")
	mustExec(t, e, "PREPARE pr FROM 'SELECT id, v FROM t'")
	res := mustExec(t, e, "EXECUTE pr")
	if len(res.Columns) != 2 || res.Columns[0] != "id" || res.Columns[1] != "v" {
		t.Fatalf("prepared columns: got %v", res.Columns)
	}
}

// TestPrepareInsert 覆盖占位符在 INSERT VALUES 中按序展开。
func TestPrepareInsert(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
	mustExec(t, e, "PREPARE pi FROM 'INSERT INTO t (id, v) VALUES (?, ?)'")
	mustExec(t, e, "EXECUTE pi USING 7, 'seven'")
	rows := mustRows(t, e, "SELECT id, v FROM t")
	if len(rows) != 1 || rows[0][0].I != 7 || rows[0][1].S != "seven" {
		t.Fatalf("prepare insert: got %+v", rows)
	}
}

var _ = sql.Engine{}

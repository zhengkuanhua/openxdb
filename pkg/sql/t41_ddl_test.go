package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// T41 DDL 兼容补齐：CREATE 系列 IF NOT EXISTS + CREATE TABLE AS SELECT（CTAS）。

func setupT41(t *testing.T, e *sql.Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE t41_src (id INT PRIMARY KEY, grp TEXT, val INT, price DECIMAL)")
	mustExec(t, e, "INSERT INTO t41_src (id, grp, val, price) VALUES (1, 'a', 10, 1.50), (2, 'a', 20, 2.25), (3, 'a', 20, 3.00), (4, 'b', 5, 0.75), (5, 'b', 30, 9.90)")
}

// ===================== CREATE 系列 IF NOT EXISTS =====================

func TestT41CreateTableIfNotExists(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	// 首次创建成功
	res := mustExec(t, e, "CREATE TABLE IF NOT EXISTS t41_ine (x INT PRIMARY KEY, y TEXT)")
	if res.AffectedRows != 0 {
		t.Fatalf("CREATE IF NOT EXISTS first time: AffectedRows want 0, got %d", res.AffectedRows)
	}

	// 已存在 → 静默跳过（即使结构不同也不覆盖）
	mustExec(t, e, "CREATE TABLE IF NOT EXISTS t41_ine (x INT PRIMARY KEY, y TEXT, z INT)")
	mustExec(t, e, "INSERT INTO t41_ine (x, y) VALUES (1, 'a')")
	rows := mustRows(t, e, "SELECT x, y FROM t41_ine")
	if len(rows) != 1 || rows[0][0].I != 1 || rows[0][1].S != "a" {
		t.Fatalf("original table should be unchanged, got %+v", rows)
	}
	// 原表结构未变：z 列不存在
	mustErr(t, e, "SELECT z FROM t41_ine", "unknown column")
}

func TestT41CreateTableNoFlagDuplicate(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t41_dup (x INT PRIMARY KEY)")
	mustErr(t, e, "CREATE TABLE t41_dup (x INT PRIMARY KEY)", "table already exists")
}

func TestT41CreateViewIfNotExists(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE VIEW IF NOT EXISTS t41v AS SELECT id, val FROM t41_src")
	rows := mustRows(t, e, "SELECT * FROM t41v")
	if len(rows) != 5 {
		t.Fatalf("view first create: want 5 rows, got %d", len(rows))
	}
	// 已存在 → 静默跳过
	mustExec(t, e, "CREATE VIEW IF NOT EXISTS t41v AS SELECT id, val FROM t41_src")
	// 视图内容不被覆盖
	mustExec(t, e, "INSERT INTO t41_src (id, grp, val, price) VALUES (6, 'c', 7, 1.00)")
	if rows = mustRows(t, e, "SELECT * FROM t41v"); len(rows) != 6 {
		t.Fatalf("view should reflect source rows after skip, got %d", len(rows))
	}
}

func TestT41CreateIndexIfNotExists(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE INDEX IF NOT EXISTS t41i ON t41_src (val)")
	mustExec(t, e, "CREATE INDEX IF NOT EXISTS t41i ON t41_src (val)") // 跳过
	// 无 IF 时重复创建仍报错
	mustErr(t, e, "CREATE INDEX t41i ON t41_src (val)", "index already exists")
}

func TestT41CreateUserIfNotExists(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE USER IF NOT EXISTS 't41u' IDENTIFIED BY 'p'")
	mustExec(t, e, "CREATE USER IF NOT EXISTS 't41u' IDENTIFIED BY 'p'") // 跳过
	mustErr(t, e, "CREATE USER 't41u' IDENTIFIED BY 'p'", "user already exists")
}

func TestT41CreateRoleIfNotExists(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE ROLE IF NOT EXISTS 't41r'")
	mustExec(t, e, "CREATE ROLE IF NOT EXISTS 't41r'") // 跳过
	mustErr(t, e, "CREATE ROLE 't41r'", "role already exists")
}

func TestT41CreateMatViewIfNotExists(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE MATERIALIZED VIEW IF NOT EXISTS t41mv AS SELECT id, val FROM t41_src")
	mustExec(t, e, "CREATE MATERIALIZED VIEW IF NOT EXISTS t41mv AS SELECT id, val FROM t41_src") // 跳过
	if rows := mustRows(t, e, "SELECT * FROM t41mv"); len(rows) != 5 {
		t.Fatalf("matview rows: want 5, got %d", len(rows))
	}
	mustErr(t, e, "CREATE MATERIALIZED VIEW t41mv AS SELECT id, val FROM t41_src", "materialized view already exists")
}

func TestT41IdempotentCreateDropScript(t *testing.T) {
	e := newEngine(t)
	for i := 0; i < 3; i++ {
		mustExec(t, e, "CREATE TABLE IF NOT EXISTS t41_idem (x INT PRIMARY KEY)")
		mustExec(t, e, "DROP TABLE IF EXISTS t41_idem")
	}
	// 最终不存在
	mustErr(t, e, "SELECT * FROM t41_idem", "table not exists")
}

func TestT41IfNotExistsParserAST(t *testing.T) {
	stmt, err := sql.Parse("CREATE TABLE IF NOT EXISTS t (x INT)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cts, ok := stmt.(*sql.CreateTableStmt)
	if !ok {
		t.Fatalf("want *CreateTableStmt, got %T", stmt)
	}
	if !cts.IfNotExists {
		t.Fatal("CREATE TABLE IF NOT EXISTS: IfNotExists should be true")
	}

	stmt, err = sql.Parse("CREATE TABLE t2 (x INT)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cts = stmt.(*sql.CreateTableStmt)
	if cts.IfNotExists {
		t.Fatal("CREATE TABLE without IF NOT EXISTS: IfNotExists should be false")
	}

	stmt, err = sql.Parse("CREATE VIEW IF NOT EXISTS v AS SELECT 1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cvs := stmt.(*sql.CreateViewStmt)
	if !cvs.IfNotExists {
		t.Fatal("CREATE VIEW IF NOT EXISTS: IfNotExists should be true")
	}

	// IF 后跟非 NOT 报语法错误
	if _, err := sql.Parse("CREATE TABLE IF EXISTS t (x INT)"); err == nil {
		t.Fatal("CREATE TABLE IF EXISTS (without NOT) should fail to parse")
	}
}

// ===================== CREATE TABLE AS SELECT =====================

func TestT41CtasBasicSelectStar(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE TABLE t41c1 AS SELECT * FROM t41_src")
	res := mustExec(t, e, "SELECT * FROM t41c1")
	wantCols := []string{"id", "grp", "val", "price"}
	if len(res.Columns) != len(wantCols) {
		t.Fatalf("columns: want %v, got %v", wantCols, res.Columns)
	}
	for i, c := range wantCols {
		if res.Columns[i] != c {
			t.Fatalf("column %d: want %q, got %q", i, c, res.Columns[i])
		}
	}
	if len(res.Rows) != 5 {
		t.Fatalf("rows: want 5, got %d", len(res.Rows))
	}
	// 值一致性（SELECT * 默认按 id 排序？逐行比较）
	rows := mustRows(t, e, "SELECT * FROM t41c1 ORDER BY id")
	if rows[0][0].I != 1 || rows[0][1].S != "a" || rows[0][2].I != 10 {
		t.Fatalf("row0: got %+v", rows[0])
	}
	if rows[4][0].I != 5 || rows[4][2].I != 30 {
		t.Fatalf("row4: got %+v", rows[4])
	}
	// 类型推断：id INT / grp TEXT / val INT / price DECIMAL
	if rows[0][0].Kind != "INT" || rows[0][1].Kind != "TEXT" || rows[0][3].Kind != "DECIMAL" {
		t.Fatalf("type inference: got %+v", rows[0])
	}
}

func TestT41CtasExprProjection(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE TABLE t41c2 AS SELECT id, price * 2 AS total FROM t41_src")
	res := mustExec(t, e, "SELECT * FROM t41c2")
	if len(res.Columns) != 2 || res.Columns[0] != "id" || res.Columns[1] != "total" {
		t.Fatalf("columns: got %v", res.Columns)
	}
	rows := mustRows(t, e, "SELECT total FROM t41c2")
	if len(rows) != 5 {
		t.Fatalf("rows: want 5, got %d", len(rows))
	}
	for _, r := range rows {
		if r[0].Kind != "DECIMAL" {
			t.Fatalf("expr col type: want DECIMAL, got %s", r[0].Kind)
		}
	}
	if rows[0][0].I == 0 && rows[0][0].S == "" {
		t.Fatalf("total should be non-zero, got %+v", rows[0][0])
	}
}

func TestT41CtasAggregation(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE TABLE t41c3 AS SELECT grp, COUNT(*) AS c FROM t41_src GROUP BY grp")
	rows := mustRows(t, e, "SELECT * FROM t41c3 ORDER BY grp")
	if len(rows) != 2 {
		t.Fatalf("rows: want 2, got %d", len(rows))
	}
	if rows[0][0].S != "a" || rows[0][1].I != 3 {
		t.Fatalf("group a: got %+v", rows[0])
	}
	if rows[1][0].S != "b" || rows[1][1].I != 2 {
		t.Fatalf("group b: got %+v", rows[1])
	}
	if rows[0][1].Kind != "INT" {
		t.Fatalf("count col type: want INT, got %s", rows[0][1].Kind)
	}
}

func TestT41CtasWhere(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE TABLE t41c4 AS SELECT id, val FROM t41_src WHERE grp = 'a'")
	rows := mustRows(t, e, "SELECT id FROM t41c4 ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("rows: want 3, got %d", len(rows))
	}
	for i, r := range rows {
		if r[0].I != int64(i+1) {
			t.Fatalf("row %d id: want %d, got %d", i, i+1, r[0].I)
		}
	}
}

func TestT41CtasDistinct(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE TABLE t41c5 AS SELECT DISTINCT grp FROM t41_src")
	rows := mustRows(t, e, "SELECT * FROM t41c5 ORDER BY grp")
	if len(rows) != 2 || rows[0][0].S != "a" || rows[1][0].S != "b" {
		t.Fatalf("distinct rows: got %+v", rows)
	}
}

func TestT41CtasAlias(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE TABLE t41c6 AS SELECT id AS uid, grp AS g FROM t41_src")
	res := mustExec(t, e, "SELECT * FROM t41c6")
	if len(res.Columns) != 2 || res.Columns[0] != "uid" || res.Columns[1] != "g" {
		t.Fatalf("alias columns: got %v", res.Columns)
	}
	if len(res.Rows) != 5 {
		t.Fatalf("rows: want 5, got %d", len(res.Rows))
	}
}

func TestT41CtasEmptyResult(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	// 空结果集也建表成功
	mustExec(t, e, "CREATE TABLE t41c7 AS SELECT id, val FROM t41_src WHERE 1 = 0")
	res := mustExec(t, e, "SELECT * FROM t41c7")
	if len(res.Columns) != 2 || res.Columns[0] != "id" || res.Columns[1] != "val" {
		t.Fatalf("empty result columns: got %v", res.Columns)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("empty result: want 0 rows, got %d", len(res.Rows))
	}
	// 空表仍可 INSERT（空结果集列推断为 TEXT）
	mustExec(t, e, "INSERT INTO t41c7 (id, val) VALUES ('1', '9')")
	if rows := mustRows(t, e, "SELECT * FROM t41c7"); len(rows) != 1 {
		t.Fatalf("after insert: want 1 row, got %d", len(rows))
	}
}

func TestT41CtasTypeInference(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE TABLE t41c8 AS SELECT id, grp, val, price FROM t41_src")
	rows := mustRows(t, e, "SELECT id, grp, val, price FROM t41c8")
	if len(rows) != 5 {
		t.Fatalf("rows: want 5, got %d", len(rows))
	}
	if rows[0][0].Kind != "INT" || rows[0][1].Kind != "TEXT" || rows[0][2].Kind != "INT" || rows[0][3].Kind != "DECIMAL" {
		t.Fatalf("type inference: got %+v", rows[0])
	}
	// 列类型确实生效：向 TEXT 列插入数字可以，向 INT 列插入文本报错
	mustErr(t, e, "INSERT INTO t41c8 (id, grp) VALUES ('abc', 'x')", "")
}

func TestT41CtasIfNotExists(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	mustExec(t, e, "CREATE TABLE t41c9 (x INT PRIMARY KEY)")
	mustExec(t, e, "INSERT INTO t41c9 (x) VALUES (100)")
	// 表已存在 → 静默跳过，不执行 SELECT、不覆盖
	mustExec(t, e, "CREATE TABLE IF NOT EXISTS t41c9 AS SELECT * FROM t41_src")
	rows := mustRows(t, e, "SELECT x FROM t41c9")
	if len(rows) != 1 || rows[0][0].I != 100 {
		t.Fatalf("existing table should be unchanged, got %+v", rows)
	}
}

func TestT41CtasRollback(t *testing.T) {
	e := newEngine(t)
	setupT41(t, e)

	// SELECT 引用不存在的表 → 报错且目标表不残留
	mustErr(t, e, "CREATE TABLE t41c10 AS SELECT * FROM no_such_table", "table not exists")
	mustErr(t, e, "SELECT * FROM t41c10", "table not exists")
}

func TestT41CtasParserAST(t *testing.T) {
	stmt, err := sql.Parse("CREATE TABLE t AS SELECT id FROM u")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cts, ok := stmt.(*sql.CreateTableStmt)
	if !ok {
		t.Fatalf("want *CreateTableStmt, got %T", stmt)
	}
	if cts.Select == nil {
		t.Fatal("CTAS: Select should be non-nil")
	}
	if len(cts.Columns) != 0 {
		t.Fatalf("CTAS: Columns should be empty, got %v", cts.Columns)
	}
	if cts.Name != "t" {
		t.Fatalf("name: want t, got %s", cts.Name)
	}

	// IF NOT EXISTS 组合
	stmt, err = sql.Parse("CREATE TABLE IF NOT EXISTS t2 AS SELECT id FROM u")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cts = stmt.(*sql.CreateTableStmt)
	if !cts.IfNotExists || cts.Select == nil {
		t.Fatal("CTAS + IF NOT EXISTS: both flags should be set")
	}
}

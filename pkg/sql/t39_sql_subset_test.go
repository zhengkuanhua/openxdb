package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// T39 SQL 子集（v1.6.0 第 1 项）：
// 1) ALTER 列级类型变更（MODIFY COLUMN，行数据重写 + 零值回填）
// 2) CREATE DATABASE [IF NOT EXISTS] / SHOW DATABASES 多库目录
// 3) 窗口 ORDER BY 多列（任意 ASC/DESC 组合）

// ---- 1) ALTER MODIFY COLUMN ----

func TestT39AlterModifyIntToText(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, v INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)")
	res := mustExec(t, e, "ALTER TABLE t MODIFY COLUMN v TEXT")
	if res.AffectedRows != 2 {
		t.Fatalf("MODIFY INT->TEXT: want affected 2, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id, v FROM t ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	for i, want := range []string{"10", "20"} {
		if rows[i][1].Kind != "TEXT" || rows[i][1].S != want {
			t.Fatalf("row %d: want TEXT %q, got %+v", i, want, rows[i][1])
		}
	}
	// 类型变更后新插入按新类型约束
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (3, 'abc')")
	rows = mustRows(t, e, "SELECT v FROM t WHERE id = 3")
	if rows[0][0].S != "abc" {
		t.Fatalf("insert after modify: want abc, got %+v", rows[0][0])
	}
}

func TestT39AlterModifyTextToIntZeroFill(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, v TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, '42'), (2, 'not-a-number')")
	res := mustExec(t, e, "ALTER TABLE t MODIFY v INT")
	if res.AffectedRows != 2 {
		t.Fatalf("MODIFY TEXT->INT: want affected 2, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id, v FROM t ORDER BY id")
	if rows[0][1].Kind != "INT" || rows[0][1].I != 42 {
		t.Fatalf("row 1: want INT 42, got %+v", rows[0][1])
	}
	// 非数字文本回填零值
	if rows[1][1].Kind != "INT" || rows[1][1].I != 0 {
		t.Fatalf("row 2: want INT 0 (zero fill), got %+v", rows[1][1])
	}
}

func TestT39AlterModifyEmptyTable(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, v INT, PRIMARY KEY (id))")
	res := mustExec(t, e, "ALTER TABLE t MODIFY COLUMN v TEXT")
	if res.AffectedRows != 0 {
		t.Fatalf("empty table modify: want affected 0, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT * FROM t")
	if len(rows) != 0 {
		t.Fatalf("empty table: want 0 rows, got %d", len(rows))
	}
}

func TestT39AlterModifySameTypeIdempotent(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, v TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, 'x')")
	res := mustExec(t, e, "ALTER TABLE t MODIFY COLUMN v TEXT")
	if res.AffectedRows != 0 {
		t.Fatalf("same-type modify: want affected 0, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT v FROM t")
	if rows[0][0].S != "x" {
		t.Fatalf("same-type modify must preserve data, got %+v", rows[0][0])
	}
}

func TestT39AlterModifyDateTextRoundtrip(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, v DATE, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, '2024-01-02')")
	mustExec(t, e, "ALTER TABLE t MODIFY COLUMN v TEXT")
	rows := mustRows(t, e, "SELECT v FROM t")
	if rows[0][0].S != "2024-01-02" {
		t.Fatalf("DATE->TEXT: want 2024-01-02, got %+v", rows[0][0])
	}
	// 经 TEXT 列插入非法日期，MODIFY 回 DATE 时回填零值日期
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (2, 'not-a-date')")
	mustExec(t, e, "ALTER TABLE t MODIFY COLUMN v DATE")
	rows = mustRows(t, e, "SELECT id, v FROM t ORDER BY id")
	if rows[0][1].Kind != "DATE" || rows[0][1].S != "2024-01-02" {
		t.Fatalf("TEXT->DATE: row1 want DATE 2024-01-02, got %+v", rows[0][1])
	}
	if rows[1][1].S != "1970-01-01" {
		t.Fatalf("invalid date: want zero fill 1970-01-01, got %+v", rows[1][1])
	}
}

func TestT39AlterModifyRejections(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, v INT, PRIMARY KEY (id))")
	mustErr(t, e, "ALTER TABLE t MODIFY COLUMN nope INT", "unknown column")
	mustErr(t, e, "ALTER TABLE t MODIFY COLUMN id TEXT", "cannot modify primary key column")
	mustExec(t, e, "ALTER TABLE t ADD INDEX idx_v (v)")
	mustErr(t, e, "ALTER TABLE t MODIFY COLUMN v TEXT", "cannot modify indexed column")
	// 表不存在
	mustErr(t, e, "ALTER TABLE ghost MODIFY COLUMN v INT", "table not exists")
}

// ---- 2) CREATE DATABASE / SHOW DATABASES ----

func TestT39CreateDatabaseShow(t *testing.T) {
	e := newEngine(t)
	res := mustExec(t, e, "CREATE DATABASE analytics")
	if res.AffectedRows != 0 {
		t.Fatalf("create database: want affected 0, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SHOW DATABASES")
	if len(rows) != 2 {
		t.Fatalf("SHOW DATABASES: want 2 rows, got %d (%+v)", len(rows), rows)
	}
	if rows[0][0].S != "default" {
		t.Fatalf("first db must be default, got %+v", rows[0][0])
	}
	if rows[1][0].S != "analytics" {
		t.Fatalf("second db must be analytics, got %+v", rows[1][0])
	}
}

func TestT39CreateDatabaseDuplicate(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE DATABASE a1")
	mustExec(t, e, "CREATE DATABASE b2")
	mustExec(t, e, "CREATE DATABASE c3")
	// 无 IF NOT EXISTS 重复创建报错
	mustErr(t, e, "CREATE DATABASE a1", "database already exists")
	// IF NOT EXISTS 静默成功
	res := mustExec(t, e, "CREATE DATABASE IF NOT EXISTS a1")
	if res.AffectedRows != 0 {
		t.Fatalf("if not exists: want affected 0, got %d", res.AffectedRows)
	}
	// default 恒存在不可重复创建
	mustErr(t, e, "CREATE DATABASE default", "database already exists")
	// 排序可见：default 前置 + 库名排序
	rows := mustRows(t, e, "SHOW DATABASES")
	want := []string{"default", "a1", "b2", "c3"}
	if len(rows) != len(want) {
		t.Fatalf("SHOW DATABASES: want %d rows, got %d", len(want), len(rows))
	}
	for i, w := range want {
		if rows[i][0].S != w {
			t.Fatalf("SHOW DATABASES row %d: want %s, got %+v", i, w, rows[i][0])
		}
	}
}

// ---- 3) 窗口 ORDER BY 多列 ----

func TestT39WindowOrderByMultiCol(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// ROW_NUMBER：ORDER BY dept ASC, salary DESC
	// 排序序列：eng200(2), eng200(3), eng100(1), ops300(6), ops150(4), ops150(5)
	// 按 id 输出 rn：id1=3, id2=1, id3=2, id4=5, id5=6, id6=4
	rows := mustRows(t, e, "SELECT id, ROW_NUMBER() OVER (ORDER BY dept, salary DESC) FROM emp ORDER BY id")
	want := []int64{3, 1, 2, 5, 6, 4}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("multi-col row_number: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestT39WindowOrderByMultiColRank(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// RANK 断点按多列元组：ORDER BY dept, salary ASC
	// 排序：eng100(1), eng200(2), eng200(3), ops150(4), ops150(5), ops300(6)
	// rank 按 id：id1=1, id2=2, id3=2, id4=4, id5=4, id6=6
	rows := mustRows(t, e, "SELECT id, RANK() OVER (ORDER BY dept, salary), DENSE_RANK() OVER (ORDER BY dept, salary) FROM emp ORDER BY id")
	wantRank := []int64{1, 2, 2, 4, 4, 6}
	wantDense := []int64{1, 2, 2, 3, 3, 4}
	for i := 0; i < 6; i++ {
		if rows[i][1].I != wantRank[i] || rows[i][2].I != wantDense[i] {
			t.Fatalf("multi-col rank: row %d got (%d,%d), want (%d,%d)", i, rows[i][1].I, rows[i][2].I, wantRank[i], wantDense[i])
		}
	}
}

func TestT39WindowOrderByMultiColMixed(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 混合 ASC/DESC：ORDER BY salary DESC, dept ASC
	// 排序：ops300(6), eng200(2), eng200(3), ops150(4), ops150(5), eng100(1)
	// 按 id 输出 rn：id1=6, id2=2, id3=3, id4=4, id5=5, id6=1
	rows := mustRows(t, e, "SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC, dept ASC) FROM emp ORDER BY id")
	want := []int64{6, 2, 3, 4, 5, 1}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("mixed multi-col rn: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestT39WindowRangeFrameRejectsMultiOrder(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// ROWS 帧允许多列 ORDER BY（物理行窗口）
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY dept, salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM emp ORDER BY id")
	if len(rows) != 6 {
		t.Fatalf("ROWS frame multi order: want 6 rows, got %d", len(rows))
	}
	// RANGE 帧按键值域需单列 ORDER BY（MySQL 语义），多列报错
	mustErr(t, e, "SELECT id, SUM(salary) OVER (ORDER BY dept, salary RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM emp", "RANGE frame requires single ORDER BY column")
}

func TestT39WindowAggMultiOrder(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 窗口聚合按多列排序的累计：ORDER BY dept, salary ASC
	// 排序：eng100(1), eng200(2), eng200(3), ops150(4), ops150(5), ops300(6)
	// 累计 SUM(salary)：100, 300, 500, 650, 800, 1100（按 id 输出）
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY dept, salary) FROM emp ORDER BY id")
	want := []int64{100, 300, 500, 650, 800, 1100}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("multi-col running sum: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

// 编译期类型断言：确保 AST 新语句实现 Stmt 接口。
var _ sql.Stmt = (*sql.CreateDatabaseStmt)(nil)
var _ sql.Stmt = (*sql.ShowDatabasesStmt)(nil)
var _ sql.Stmt = (*sql.UseStmt)(nil)
var _ sql.Stmt = (*sql.DropDatabaseStmt)(nil)

// T39：db.tbl 库前缀限定 DML/DDL（INSERT/UPDATE/DELETE/SELECT/ALTER/DROP TABLE）。
func TestT39QualifiedTableDML(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE DATABASE db2")
	mustExec(t, e, "USE db2")
	mustExec(t, e, "CREATE TABLE t1 (id INT, v INT, PRIMARY KEY (id))")
	mustExec(t, e, "USE default")
	mustExec(t, e, "CREATE TABLE t2 (id INT, PRIMARY KEY (id))")

	// INSERT 带库前缀
	mustExec(t, e, "INSERT INTO db2.t1 (id, v) VALUES (1, 10), (2, 20)")
	// SELECT 带库前缀
	rows := mustRows(t, e, "SELECT id, v FROM db2.t1 ORDER BY id")
	if len(rows) != 2 || rows[0][0].I != 1 || rows[0][1].I != 10 {
		t.Fatalf("qualified select: got %+v", rows)
	}
	// UPDATE 带库前缀
	mustExec(t, e, "UPDATE db2.t1 SET v = 99 WHERE id = 1")
	rows = mustRows(t, e, "SELECT v FROM db2.t1 WHERE id = 1")
	if rows[0][0].I != 99 {
		t.Fatalf("qualified update: got %+v", rows[0][0])
	}
	// DELETE 带库前缀
	mustExec(t, e, "DELETE FROM db2.t1 WHERE id = 2")
	rows = mustRows(t, e, "SELECT COUNT(*) FROM db2.t1")
	if rows[0][0].I != 1 {
		t.Fatalf("qualified delete: got %+v", rows[0][0])
	}
	// ALTER TABLE 带库前缀（MODIFY）
	mustExec(t, e, "ALTER TABLE db2.t1 MODIFY COLUMN v TEXT")
	rows = mustRows(t, e, "SELECT v FROM db2.t1 WHERE id = 1")
	if rows[0][0].Kind != "TEXT" {
		t.Fatalf("qualified alter modify: got %+v", rows[0][0])
	}
	// 未建库前缀报错
	mustErr(t, e, "SELECT * FROM ghostdb.t1", "database not exists")
	// DROP TABLE 带库前缀
	mustExec(t, e, "DROP TABLE db2.t1")
	mustErr(t, e, "SELECT * FROM db2.t1", "table not exists")
	// 库内已删表不影响 default 库表
	rows = mustRows(t, e, "SELECT id FROM t2")
	if len(rows) != 0 {
		t.Fatalf("default table should be empty: %+v", rows)
	}
}

// T39：SHOW TABLES 按会话当前库（USE 切换）过滤。
func TestT39ShowTablesPerDB(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE DATABASE db2")
	mustExec(t, e, "USE db2")
	mustExec(t, e, "CREATE TABLE a (id INT, PRIMARY KEY (id))")
	mustExec(t, e, "USE default")
	mustExec(t, e, "CREATE TABLE b (id INT, PRIMARY KEY (id))")

	rows := mustRows(t, e, "SHOW TABLES")
	if len(rows) != 1 || rows[0][0].S != "b" {
		t.Fatalf("default SHOW TABLES: want only b, got %+v", rows)
	}
	mustExec(t, e, "USE db2")
	rows = mustRows(t, e, "SHOW TABLES")
	if len(rows) != 1 || rows[0][0].S != "a" {
		t.Fatalf("db2 SHOW TABLES: want only a, got %+v", rows)
	}
}

func TestT39UseDatabase(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE DATABASE db2")
	// USE 已建库成功
	mustExec(t, e, "USE db2")
	// USE 不存在库报错
	mustErr(t, e, "USE not_exists", "database not exists")
	// USE default 恒可切换
	mustExec(t, e, "USE default")
}

func TestT39DropDatabase(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE DATABASE db2")
	mustExec(t, e, "USE db2")
	mustExec(t, e, "CREATE TABLE t (id INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, e, "USE default")
	// DROP DATABASE 级联删除库内表（行 + 元数据）
	res := mustExec(t, e, "DROP DATABASE db2")
	if res.AffectedRows != 1 {
		t.Fatalf("drop database: want affected 1, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SHOW DATABASES")
	if len(rows) != 1 || rows[0][0].S != "default" {
		t.Fatalf("drop database: want only default, got %+v", rows)
	}
	rows = mustRows(t, e, "SHOW TABLES")
	for _, r := range rows {
		if r[0].S == "t" {
			t.Fatalf("drop database: table t still visible: %+v", rows)
		}
	}
	// 重复 DROP 无 IF EXISTS 报错，带 IF EXISTS 静默
	mustErr(t, e, "DROP DATABASE db2", "database not exists")
	mustExec(t, e, "DROP DATABASE IF EXISTS db2")
	// default 库禁止删除
	mustErr(t, e, "DROP DATABASE default", "cannot drop default database")
}

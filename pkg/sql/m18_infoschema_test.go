package sql_test

import (
	"testing"
)

// M18-C Information Schema：只读虚拟库 information_schema 四张视图。

func TestInfoSchemaSchemata(t *testing.T) {
	e := newEngine(t)
	rows := mustRows(t, e, "SELECT SCHEMA_NAME FROM information_schema.schemata")
	if len(rows) != 1 || rows[0][0].S != "default" {
		t.Fatalf("schemata: want [(default)], got %+v", rows)
	}
}

func TestInfoSchemaTables(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE alpha (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE beta (k TEXT PRIMARY KEY, v INT)")
	rows := mustRows(t, e, "SELECT TABLE_NAME, TABLE_TYPE, ENGINE FROM information_schema.tables ORDER BY TABLE_NAME")
	if len(rows) != 2 {
		t.Fatalf("tables: want 2 rows, got %d", len(rows))
	}
	if rows[0][0].S != "alpha" || rows[0][1].S != "BASE TABLE" || rows[0][2].S != "RocksDB" {
		t.Fatalf("tables row0: got %+v", rows[0])
	}
	if rows[1][0].S != "beta" {
		t.Fatalf("tables row1: got %+v", rows[1])
	}
	// WHERE 过滤
	rows = mustRows(t, e, "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME = 'beta'")
	if len(rows) != 1 || rows[0][0].S != "beta" {
		t.Fatalf("tables filter: got %+v", rows)
	}
}

func TestInfoSchemaColumns(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE user (id INT PRIMARY KEY, name TEXT NOT NULL, age INT)")
	rows := mustRows(t, e, "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE FROM information_schema.columns ORDER BY TABLE_NAME, ORDINAL_POSITION")
	if len(rows) != 3 {
		t.Fatalf("columns: want 3 rows, got %d", len(rows))
	}
	// 第一列应为 PK 且不可空
	if rows[0][0].S != "user" || rows[0][1].S != "id" || rows[0][2].I != 1 || rows[0][4].S != "NO" {
		t.Fatalf("columns pk row: got %+v", rows[0])
	}
	if rows[1][1].S != "name" || rows[1][4].S != "YES" {
		t.Fatalf("columns name row: got %+v", rows[1])
	}
	// WHERE 过滤（TABLE_NAME + COLUMN_NAME AND 组合）
	rows = mustRows(t, e, "SELECT DATA_TYPE FROM information_schema.columns WHERE TABLE_NAME = 'user' AND COLUMN_NAME = 'age'")
	if len(rows) != 1 || rows[0][0].S != "INT" {
		t.Fatalf("columns filter: got %+v", rows)
	}
}

func TestInfoSchemaStatistics(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE st (id INT PRIMARY KEY, code TEXT, flag INT, UNIQUE (code))")
	rows := mustRows(t, e, "SELECT TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE FROM information_schema.statistics ORDER BY INDEX_NAME")
	if len(rows) != 2 {
		t.Fatalf("statistics: want 2 rows (PRIMARY + code unique), got %d", len(rows))
	}
	// PRIMARY：非唯一=0
	if rows[0][1].S != "PRIMARY" || rows[0][3].S != "id" || rows[0][4].I != 0 {
		t.Fatalf("statistics primary: got %+v", rows[0])
	}
	// code 唯一索引：NON_UNIQUE=0
	if rows[1][1].S != "code" || rows[1][3].S != "code" || rows[1][4].I != 0 {
		t.Fatalf("statistics unique idx: got %+v", rows[1])
	}
}

func TestInfoSchemaProjectionAndErrors(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE proj (id INT PRIMARY KEY)")
	// 列投影 + 未知视图 / 未知列报错
	rows := mustRows(t, e, "SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME = 'proj'")
	if len(rows) != 1 || rows[0][0].S != "id" || len(mustExec(t, e, "SELECT COLUMN_NAME FROM information_schema.columns").Columns) != 1 {
		t.Fatalf("projection: got %+v cols %v", rows, mustExec(t, e, "SELECT COLUMN_NAME FROM information_schema.columns").Columns)
	}
	mustErr(t, e, "SELECT * FROM information_schema.no_such_view", "unknown information_schema table")
	mustErr(t, e, "SELECT BOGUS FROM information_schema.tables", "unknown column")
}

func TestInfoSchemaQualifiedFrom(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE qf (id INT PRIMARY KEY)")
	// FROM 限定名 default.qf 解析并剥 schema
	rows := mustRows(t, e, "SELECT id FROM default.qf")
	if len(rows) != 0 {
		t.Fatalf("qualified from: want 0 rows, got %+v", rows)
	}
	// 虚拟表限定名亦可访问
	rows = mustRows(t, e, "SELECT TABLE_NAME FROM default.information_schema.tables")
	if len(rows) != 1 || rows[0][0].S != "qf" {
		t.Fatalf("qualified infoschema: got %+v", rows)
	}
}

func TestInfoSchemaWriteRejected(t *testing.T) {
	e := newEngine(t)
	mustErr(t, e, "CREATE TABLE information_schema.x (id INT PRIMARY KEY)", "information_schema")
	mustErr(t, e, "DROP TABLE information_schema.tables", "information_schema")
	mustErr(t, e, "INSERT INTO information_schema.tables (TABLE_NAME) VALUES ('x')", "information_schema")
}

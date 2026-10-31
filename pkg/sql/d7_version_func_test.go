package sql_test

import (
	"testing"
)

// D7.2：select version() 标量函数支持（排期 Phase G / G3）。
// 版本串与 mysqlproto 握手版本同源（G1：8.0.36-OpenXDB v1.17.0）。

func TestD7VersionFunction(t *testing.T) {
	e := newEngine(t)
	cases := []struct {
		stmt string
		want string
	}{
		{"SELECT version()", "8.0.36-OpenXDB v1.17.0"},
		{"SELECT VERSION()", "8.0.36-OpenXDB v1.17.0"},
		{"SELECT Version()", "8.0.36-OpenXDB v1.17.0"},
		{"SELECT version() AS v", "8.0.36-OpenXDB v1.17.0"},
	}
	for _, c := range cases {
		rows := mustRows(t, e, c.stmt)
		if len(rows) != 1 {
			t.Fatalf("%s: want 1 row, got %d", c.stmt, len(rows))
		}
		if rows[0][0].Kind != "TEXT" || rows[0][0].String() != c.want {
			t.Fatalf("%s: want %s, got %+v", c.stmt, c.want, rows[0][0])
		}
	}
	// 列名默认输出 VERSION()
	rows := mustRows(t, e, "SELECT version()")
	if len(rows[0]) != 1 {
		t.Fatalf("version col count: got %+v", rows[0])
	}
}

func TestD7VersionFunctionMixed(t *testing.T) {
	e := newEngine(t)
	// 与常量投影混用（多列）
	rows := mustRows(t, e, "SELECT 1, version()")
	if len(rows) != 1 || len(rows[0]) != 2 {
		t.Fatalf("mixed: got %+v", rows)
	}
	if rows[0][0].String() != "1" || rows[0][1].String() != "8.0.36-OpenXDB v1.17.0" {
		t.Fatalf("mixed values: got %+v", rows[0])
	}
	// 带 FROM 的投影路径（Rel/meta 投影同样生效）
	mustExec(t, e, "CREATE TABLE t_d7v (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t_d7v VALUES (1, 'a'), (2, 'b')")
	rows = mustRows(t, e, "SELECT name, version() FROM t_d7v ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("from: got %d rows", len(rows))
	}
	for i, r := range rows {
		if len(r) != 2 || r[1].String() != "8.0.36-OpenXDB v1.17.0" {
			t.Fatalf("from row %d: got %+v", i, r)
		}
	}
	// version() 带参应报错（仅支持无参形式）
	mustErr(t, e, "SELECT version(1)", "expected")
}

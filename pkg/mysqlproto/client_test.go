package mysqlproto

import (
	"net"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// TestD7ClientHandshakeAndVersion 冒烟验证 G1 握手版本串与 G3 version() 的
// 完整客户端链路（oxsql Dial 基座）：握手解析 serverVersion + COM_QUERY
// 文本结果集。
func TestD7ClientHandshakeAndVersion(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	t.Cleanup(func() { d.Close() })

	srv := New(d.SQL)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ServeListener(ln)
	addr := ln.Addr().String()

	c, err := Dial(addr, "root", "x")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	// G1：握手版本串。
	if want := "8.0.36-OpenXDB v1.17.0"; c.ServerVersion() != want {
		t.Fatalf("ServerVersion = %q, want %q", c.ServerVersion(), want)
	}

	// G3：version() 函数（别名 + 大小写）。
	cols, rows, err := c.Query("SELECT version()")
	if err != nil {
		t.Fatalf("select version(): %v", err)
	}
	if len(rows) != 1 || rows[0][0] != "8.0.36-OpenXDB v1.17.0" {
		t.Fatalf("version() = %v (cols=%v), want single row v1.17.0", rows, cols)
	}
	_, rows, err = c.Query("SELECT VERSION() AS v")
	if err != nil {
		t.Fatalf("select VERSION() as v: %v", err)
	}
	if len(rows) != 1 || rows[0][0] != "8.0.36-OpenXDB v1.17.0" {
		t.Fatalf("VERSION() = %v, want v1.17.0", rows)
	}

	// G5 / G4：客户端链路冒烟（SHOW INDEXES 复数 + SHOW CREATE TABLE）。
	if _, _, err := c.Query("CREATE TABLE t7 (id INT PRIMARY KEY, v TEXT, UNIQUE KEY uq_v (v))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	cols, rows, err = c.Query("SHOW INDEXES FROM t7")
	if err != nil {
		t.Fatalf("show indexes from t7: %v", err)
	}
	if len(rows) < 1 {
		t.Fatalf("SHOW INDEXES rows = %d, want >= 1 (cols=%v)", len(rows), cols)
	}
	cols, rows, err = c.Query("SHOW CREATE TABLE t7")
	if err != nil {
		t.Fatalf("show create table t7: %v", err)
	}
	if len(rows) != 1 || !strings.Contains(rows[0][1], "CREATE TABLE") {
		t.Fatalf("SHOW CREATE TABLE rows = %v (cols=%v), want DDL row", rows, cols)
	}

	// 错误查询路径。
	if _, _, err := c.Query("SELECT * FROM no_such_table_xyz"); err == nil {
		t.Fatal("query on missing table: want error, got nil")
	}
}

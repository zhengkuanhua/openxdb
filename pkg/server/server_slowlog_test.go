package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// newTestSQLServer 装配 SQL 引擎的测试服务器（模拟 cmd start 装配：
// s.SQL = d.SQL，慢查询落盘路径可后续 SetSlowLog 指定）。
func newTestSQLServer(t *testing.T) (*Server, func()) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	s := New(d.Txn)
	s.SQL = d.SQL
	return s, func() { _ = d.Close() }
}

// TestM13ServerSlowQueryView 经 server 行协议：SET 阈值即时生效、
// SHOW SLOWQUERIES 查看内存记录、slow.log 落盘内容。
func TestM13ServerSlowQueryView(t *testing.T) {
	s, closeFn := newTestSQLServer(t)
	defer closeFn()

	logPath := filepath.Join(t.TempDir(), "slow.log")
	mustResp(t, s, "CREATE TABLE big (id INT PRIMARY KEY, name TEXT)")
	// 造 500 行数据，保证 SELECT 跨阈值
	// （v1.16.0 C6 共享快照使 200 行全表扫描降至 ~1ms 阈值临界，偶发不记录，
	// 抬高数据量使慢查询判定稳定）。
	vals := make([]string, 500)
	for i := range vals {
		vals[i] = fmt.Sprintf("(%d, 'x')", i)
	}
	mustResp(t, s, "INSERT INTO big VALUES "+strings.Join(vals, ", "))
	s.SQL.SetSlowLog(logPath, true) // 模拟服务启动装配落盘路径
	mustResp(t, s, "SET slow_query_threshold = 1")
	mustResp(t, s, "SET slow_query_log = on")
	if !s.SQL.SlowLogEnabled() {
		t.Fatal("slow log not enabled via SET")
	}

	if got := mustResp(t, s, "SELECT * FROM big"); strings.Contains(got, "ERR") {
		t.Fatalf("SELECT failed: %q", got)
	}

	out := mustResp(t, s, "SHOW SLOWQUERIES")
	if !strings.Contains(out, "root") || !strings.Contains(out, "SELECT * FROM big") {
		t.Fatalf("SHOW SLOWQUERIES missing record: %q", out)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read slow.log: %v", err)
	}
	if !strings.Contains(string(data), "SELECT * FROM big") {
		t.Fatalf("slow.log missing entry: %q", string(data))
	}
}

// TestM13ServerSlowQueryDisable SET slow_query_log = off 停止落盘（内存仍可查）。
func TestM13ServerSlowQueryDisable(t *testing.T) {
	s, closeFn := newTestSQLServer(t)
	defer closeFn()

	logPath := filepath.Join(t.TempDir(), "slow.log")
	mustResp(t, s, "CREATE TABLE big (id INT PRIMARY KEY, name TEXT)")
	vals := make([]string, 500)
	for i := range vals {
		vals[i] = fmt.Sprintf("(%d, 'x')", i)
	}
	mustResp(t, s, "INSERT INTO big VALUES "+strings.Join(vals, ", "))
	s.SQL.SetSlowLog(logPath, true)
	mustResp(t, s, "SET slow_query_threshold = 1")

	mustResp(t, s, "SELECT * FROM big")
	data1, _ := os.ReadFile(logPath)
	if !strings.Contains(string(data1), "SELECT * FROM big") {
		t.Fatalf("first entry missing: %q", string(data1))
	}

	mustResp(t, s, "SET slow_query_log = off")
	mustResp(t, s, "SELECT * FROM big")
	data2, _ := os.ReadFile(logPath)
	if strings.Count(string(data2), "SELECT * FROM big") != 1 {
		t.Fatalf("slow log should stop after off: %q", string(data2))
	}
}

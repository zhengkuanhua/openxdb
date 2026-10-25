package mysqlproto

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// ---- splitSQLStatements 纯函数测试：引号/注释内分号不切分 ----

func TestSplitSQLStatements(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "single statement",
			sql:  "SELECT 1",
			want: []string{"SELECT 1"},
		},
		{
			name: "two statements",
			sql:  "SELECT 1; SELECT 2",
			want: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name: "semicolon inside single quote",
			sql:  "INSERT INTO t VALUES ('a;b'); SELECT 1",
			want: []string{"INSERT INTO t VALUES ('a;b')", "SELECT 1"},
		},
		{
			name: "semicolon inside double quote",
			sql:  `SELECT "a;b" AS x; SELECT 2`,
			want: []string{`SELECT "a;b" AS x`, "SELECT 2"},
		},
		{
			name: "semicolon inside backtick identifier",
			sql:  "SELECT `a;b` FROM t; SELECT 2",
			want: []string{"SELECT `a;b` FROM t", "SELECT 2"},
		},
		{
			name: "escaped quote inside string",
			sql:  "SELECT 'it''s;ok'; SELECT 2",
			want: []string{"SELECT 'it''s;ok'", "SELECT 2"},
		},
		{
			name: "backslash escaped quote",
			sql:  `SELECT 'a\';x'; SELECT 2`,
			want: []string{`SELECT 'a\';x'`, "SELECT 2"},
		},
		{
			name: "line comment double dash",
			sql:  "SELECT 1 -- comment; not a stmt\n; SELECT 2",
			want: []string{"SELECT 1 -- comment; not a stmt", "SELECT 2"},
		},
		{
			name: "line comment hash",
			sql:  "SELECT 1 # comment; not a stmt\n; SELECT 2",
			want: []string{"SELECT 1 # comment; not a stmt", "SELECT 2"},
		},
		{
			name: "block comment",
			sql:  "SELECT 1 /* a;b */; SELECT 2",
			want: []string{"SELECT 1 /* a;b */", "SELECT 2"},
		},
		{
			name: "trailing empty after semicolon",
			sql:  "SELECT 1;",
			want: []string{"SELECT 1"},
		},
		{
			name: "empty input",
			sql:  "",
			want: []string{},
		},
		{
			name: "whitespace only",
			sql:  "  ;  ;",
			want: []string{},
		},
		{
			name: "semicolon as part of statement but in comment only",
			sql:  "/* only comment ; */",
			want: []string{"/* only comment ; */"},
		},
		{
			name: "three statements with mixed string and comment",
			sql:  "UPDATE t SET v='x;y' WHERE id=1; DELETE FROM t WHERE id=2 /* cleanup; */; SELECT COUNT(*) FROM t",
			want: []string{
				"UPDATE t SET v='x;y' WHERE id=1",
				"DELETE FROM t WHERE id=2 /* cleanup; */",
				"SELECT COUNT(*) FROM t",
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := splitSQLStatements(c.sql)
			if len(got) != len(c.want) {
				t.Fatalf("splitSQLStatements(%q) = %#v, want %#v", c.sql, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("splitSQLStatements(%q) segment %d = %q, want %q", c.sql, i, got[i], c.want[i])
				}
			}
		})
	}
}

// ---- 集成测试：COM_QUERY 多语句切分执行 + SERVER_MORE_RESULTS_EXISTS ----

// readStatusFromPacket 从 OK 包/EOF 包（payload 第 3-4 字节）读取 2 字节 status。
func readStatusFromPacket(p []byte) uint16 {
	if len(p) < 5 {
		return 0
	}
	return binary.LittleEndian.Uint16(p[3:5])
}

// TestCOMQueryMultiStatementsFlow 用底层 packet 循环逐条消费多语句结果流，
// 验证：非最后一条 OK/结果集 EOF 携带 MORE_RESULTS，最后一条不携带。
func TestCOMQueryMultiStatementsFlow(t *testing.T) {
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

	c := dialClient(t, addr)
	_ = c.readGreeting()
	c.sendHandshake("root", "x")
	c.expectOK()

	// 建表（单语句正常路径）。
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")))

	// 多语句：INSERT; INSERT; SELECT（两写一读）。
	if err := c.pc.writePacket(append([]byte{comQuery}, []byte("INSERT INTO t VALUES (1, 'x'); INSERT INTO t VALUES (2, 'y'); SELECT id, v FROM t ORDER BY id")...)); err != nil {
		t.Fatalf("send multi query: %v", err)
	}

	// 响应 1：INSERT OK，应带 MORE_RESULTS。
	p1, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("read resp1: %v", err)
	}
	if p1[0] != 0x00 {
		t.Fatalf("resp1 not ok: %q", p1)
	}
	if st := readStatusFromPacket(p1); st&statusMoreResults == 0 {
		t.Fatalf("resp1 status = 0x%04x, want MORE_RESULTS set", st)
	}

	// 响应 2：INSERT OK，应带 MORE_RESULTS。
	p2, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("read resp2: %v", err)
	}
	if p2[0] != 0x00 {
		t.Fatalf("resp2 not ok: %q", p2)
	}
	if st := readStatusFromPacket(p2); st&statusMoreResults == 0 {
		t.Fatalf("resp2 status = 0x%04x, want MORE_RESULTS set", st)
	}

	// 响应 3：结果集。先读列数包（含 EOF 状态位）→ 列定义 → 列 EOF → 行 → 行 EOF。
	colCount, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("read col count: %v", err)
	}
	n, _, err := readLenencInt(colCount, 0)
	if err != nil || n != 2 {
		t.Fatalf("col count = %d err=%v", n, err)
	}
	for i := 0; i < int(n); i++ {
		if _, err := c.pc.readPacket(); err != nil {
			t.Fatalf("read col def %d: %v", i, err)
		}
	}
	colEOF, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("read col eof: %v", err)
	}
	if st := readStatusFromPacket(colEOF); st&statusMoreResults != 0 {
		t.Fatalf("last result col EOF status = 0x%04x, want NO MORE_RESULTS", st)
	}
	// 两行。
	for i := 0; i < 2; i++ {
		rp, err := c.pc.readPacket()
		if err != nil {
			t.Fatalf("read row %d: %v", i, err)
		}
		if rp[0] == 0xfe && len(rp) < 9 {
			t.Fatalf("unexpected early EOF row")
		}
	}
	rowEOF, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("read row eof: %v", err)
	}
	if st := readStatusFromPacket(rowEOF); st&statusMoreResults != 0 {
		t.Fatalf("last row EOF status = 0x%04x, want NO MORE_RESULTS", st)
	}

	// 单语句行为兼容：单独 SELECT 结果流 EOF 无 MORE_RESULTS。
	if err := c.pc.writePacket(append([]byte{comQuery}, []byte("SELECT 1")...)); err != nil {
		t.Fatalf("send single query: %v", err)
	}
	sc, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("read single col count: %v", err)
	}
	_, _, err = readLenencInt(sc, 0)
	if err != nil {
		t.Fatalf("single col count parse: %v", err)
	}
	if _, err := c.pc.readPacket(); err != nil { // col def
		t.Fatalf("single col def: %v", err)
	}
	se, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("single col eof: %v", err)
	}
	if st := readStatusFromPacket(se); st&statusMoreResults != 0 {
		t.Fatalf("single col EOF status = 0x%04x, want NO MORE_RESULTS", st)
	}
	// 单行 EOF。
	sr, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("single row: %v", err)
	}
	if sr[0] == 0xfe && len(sr) < 9 {
		t.Fatalf("unexpected early EOF")
	}
	sre, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("single row eof: %v", err)
	}
	if st := readStatusFromPacket(sre); st&statusMoreResults != 0 {
		t.Fatalf("single row EOF status = 0x%04x, want NO MORE_RESULTS", st)
	}
}

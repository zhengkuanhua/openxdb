package server

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/openxdb/openxdb/pkg/db"
)

func newTestServer(t *testing.T) (*Server, func()) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	return New(d.Txn), func() { _ = d.Close() }
}

func mustResp(t *testing.T, s *Server, line string) string {
	t.Helper()
	resp, err := s.Handle(line)
	if err != nil {
		return err.Error()
	}
	return resp
}

// TestSetGet 隐式事务 SET 后 GET 返回原值。
func TestSetGet(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	if got := mustResp(t, s, "SET name openxdb"); got != "OK" {
		t.Fatalf("SET: want OK, got %q", got)
	}
	if got := mustResp(t, s, "GET name"); got != "openxdb" {
		t.Fatalf("GET: want openxdb, got %q", got)
	}
}

// TestSetValueWithSpaces SET 的 value 允许含空格。
func TestSetValueWithSpaces(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	mustResp(t, s, "SET note hello world foo")
	if got := mustResp(t, s, "GET note"); got != "hello world foo" {
		t.Fatalf("GET: want 'hello world foo', got %q", got)
	}
}

// TestGetMissing 不存在返回 ERR not found。
func TestGetMissing(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	if got := mustResp(t, s, "GET nope"); !strings.HasPrefix(got, "ERR not found") {
		t.Fatalf("GET missing: want ERR not found, got %q", got)
	}
}

// TestDel 删除后 GET 返回 not found。
func TestDel(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	mustResp(t, s, "SET k v")
	if got := mustResp(t, s, "DEL k"); got != "OK" {
		t.Fatalf("DEL: want OK, got %q", got)
	}
	if got := mustResp(t, s, "GET k"); !strings.HasPrefix(got, "ERR not found") {
		t.Fatalf("GET after DEL: want ERR, got %q", got)
	}
}

// TestExplicitTxnCommit 显式事务内读己写，COMMIT 后数据可见。
func TestExplicitTxnCommit(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	if got := mustResp(t, s, "BEGIN"); got != "OK" {
		t.Fatalf("BEGIN: %q", got)
	}
	if got := mustResp(t, s, "SET txkey txval"); got != "OK" {
		t.Fatalf("SET in txn: %q", got)
	}
	if got := mustResp(t, s, "GET txkey"); got != "txval" {
		t.Fatalf("read-your-writes: want txval, got %q", got)
	}
	if got := mustResp(t, s, "COMMIT"); got != "OK" {
		t.Fatalf("COMMIT: %q", got)
	}
	if got := mustResp(t, s, "GET txkey"); got != "txval" {
		t.Fatalf("GET after COMMIT: want txval, got %q", got)
	}
}

// TestExplicitTxnRollback 回滚后写入不可见。
func TestExplicitTxnRollback(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	mustResp(t, s, "BEGIN")
	mustResp(t, s, "SET gone 1")
	if got := mustResp(t, s, "ROLLBACK"); got != "OK" {
		t.Fatalf("ROLLBACK: %q", got)
	}
	if got := mustResp(t, s, "GET gone"); !strings.HasPrefix(got, "ERR not found") {
		t.Fatalf("GET after ROLLBACK: want ERR, got %q", got)
	}
}

// TestDoubleBegin 嵌套事务被拒绝。
func TestDoubleBegin(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	mustResp(t, s, "BEGIN")
	if got := mustResp(t, s, "BEGIN"); !strings.Contains(got, "already active") {
		t.Fatalf("double BEGIN: want already active, got %q", got)
	}
	mustResp(t, s, "ROLLBACK")
}

// TestCommitWithoutBegin 无活动事务时 COMMIT/ROLLBACK 报错。
func TestCommitWithoutBegin(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	if got := mustResp(t, s, "COMMIT"); !strings.Contains(got, "no active transaction") {
		t.Fatalf("COMMIT w/o BEGIN: %q", got)
	}
	if got := mustResp(t, s, "ROLLBACK"); !strings.Contains(got, "no active transaction") {
		t.Fatalf("ROLLBACK w/o BEGIN: %q", got)
	}
}

// TestSyntaxErrors 参数缺失返回 ERR syntax。
func TestSyntaxErrors(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	for _, line := range []string{"GET", "SET onlykey", "DEL"} {
		if got := mustResp(t, s, line); !strings.HasPrefix(got, "ERR syntax") {
			t.Fatalf("%q: want ERR syntax, got %q", line, got)
		}
	}
	if got := mustResp(t, s, "BOGUS"); !strings.Contains(got, "unknown command") {
		t.Fatalf("BOGUS: want unknown command, got %q", got)
	}
}

// TestPingHelp PING 返回 PONG，HELP 列出命令。
func TestPingHelp(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	if got := mustResp(t, s, "ping"); got != "PONG" {
		t.Fatalf("ping: want PONG, got %q", got)
	}
	if got := mustResp(t, s, "HELP"); !strings.Contains(got, "SET") {
		t.Fatalf("HELP: want command list, got %q", got)
	}
}

// TestTCPRoundtrip 经 TCP 连接完成 SET/GET/QUIT 往返。
func TestTCPRoundtrip(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = s.ServeListener(ln) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	rd := bufio.NewReader(conn)
	write := func(line string) string {
		if _, err := conn.Write([]byte(line + "\n")); err != nil {
			t.Fatalf("write %q: %v", line, err)
		}
		resp, err := rd.ReadString('\n')
		if err != nil {
			t.Fatalf("read after %q: %v", line, err)
		}
		return strings.TrimSpace(resp)
	}

	if got := write("PING"); got != "PONG" {
		t.Fatalf("TCP PING: want PONG, got %q", got)
	}
	if got := write("SET tcpkey tcpval"); got != "OK" {
		t.Fatalf("TCP SET: %q", got)
	}
	if got := write("GET tcpkey"); got != "tcpval" {
		t.Fatalf("TCP GET: want tcpval, got %q", got)
	}
	if got := write("QUIT"); got != "BYE" {
		t.Fatalf("TCP QUIT: want BYE, got %q", got)
	}
}

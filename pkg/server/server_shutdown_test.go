package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// waitShuttingDown 轮询等待服务进入停机状态（拒绝新连接/新事务）。
func waitShuttingDown(t *testing.T, s *Server, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !s.ShuttingDown() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !s.ShuttingDown() {
		t.Fatal("server did not enter shutting-down state")
	}
}

// TestShutdownCommandStartsGraceful SHUTDOWN [GRACEFUL] 命令触发优雅停机；
// 重复触发返回 already shutting down。
func TestShutdownCommandStartsGraceful(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	if got := mustResp(t, s, "SHUTDOWN GRACEFUL"); got != "OK: graceful shutdown started" {
		t.Fatalf("SHUTDOWN GRACEFUL: want started, got %q", got)
	}
	waitShuttingDown(t, s, 2*time.Second)
	if got := mustResp(t, s, "SHUTDOWN"); !strings.HasPrefix(got, "ERR already shutting down") {
		t.Fatalf("second SHUTDOWN: want already shutting down, got %q", got)
	}
	// 无进行中事务时 drain 立即完成，正常退出（非强制）。
	deadline := time.Now().Add(2 * time.Second)
	for s.ForceExited() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if s.ForceExited() {
		t.Fatal("SHUTDOWN without pending txn must not force exit")
	}
}

// TestShutdownRejectsNewCommands 停机中拒绝新命令/新事务，放行 PING/QUIT。
func TestShutdownRejectsNewCommands(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	go func() { _ = s.GracefulShutdown(time.Second) }()
	waitShuttingDown(t, s, 2*time.Second)
	if got := mustResp(t, s, "SET a 1"); !strings.HasPrefix(got, "ERR server shutting down") {
		t.Fatalf("SET during shutdown: want rejected, got %q", got)
	}
	if got := mustResp(t, s, "BEGIN"); !strings.HasPrefix(got, "ERR server shutting down") {
		t.Fatalf("BEGIN during shutdown: want rejected, got %q", got)
	}
	if got := mustResp(t, s, "PING"); got != "PONG" {
		t.Fatalf("PING during shutdown: want PONG, got %q", got)
	}
	if got := mustResp(t, s, "QUIT"); got != "BYE" {
		t.Fatalf("QUIT during shutdown: want BYE, got %q", got)
	}
}

// TestShutdownDrainsActiveTxn 进行中显式事务在停机 drain 窗口内完成提交后
// 正常退出（退出码 0 路径）；停机期间新命令被拒绝。
func TestShutdownDrainsActiveTxn(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	if got := mustResp(t, s, "BEGIN"); got != "OK" {
		t.Fatalf("BEGIN: %q", got)
	}
	if got := mustResp(t, s, "SET k v1"); got != "OK" {
		t.Fatalf("SET in txn: %q", got)
	}
	// 事务内读己写（进行中事务未被截断）。
	if got := mustResp(t, s, "GET k"); got != "v1" {
		t.Fatalf("GET k in txn: want v1, got %q", got)
	}
	done := make(chan error, 1)
	go func() { done <- s.GracefulShutdown(5 * time.Second) }()
	waitShuttingDown(t, s, 2*time.Second)
	// drain 等待期间：新事务被拒，事务终态命令放行。
	if got := mustResp(t, s, "SET other x"); !strings.HasPrefix(got, "ERR server shutting down") {
		t.Fatalf("new txn during drain: want rejected, got %q", got)
	}
	if got := mustResp(t, s, "COMMIT"); got != "OK" {
		t.Fatalf("COMMIT during drain: %q", got)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("GracefulShutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not complete after COMMIT")
	}
	if s.ForceExited() {
		t.Fatal("drained txn must not force exit")
	}
}

// TestShutdownTimeoutForcesExit 进行中事务未在超时内收尾 → 超时强制退出，
// 返回 ErrShutdownTimeout 且存储关闭钩子仍被调用（reopen 数据完整）。
func TestShutdownTimeoutForcesExit(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	if got := mustResp(t, s, "BEGIN"); got != "OK" {
		t.Fatalf("BEGIN: %q", got)
	}
	if got := mustResp(t, s, "SET k v1"); got != "OK" {
		t.Fatalf("SET in txn: %q", got)
	}
	hookCalled := false
	s.SetShutdownHook(func() error {
		hookCalled = true
		return nil
	})
	start := time.Now()
	err := s.GracefulShutdown(80 * time.Millisecond)
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("want ErrShutdownTimeout, got %v", err)
	}
	if time.Since(start) < 80*time.Millisecond {
		t.Fatal("timeout path returned too early")
	}
	if !s.ForceExited() {
		t.Fatal("want forced exit on drain timeout")
	}
	if !hookCalled {
		t.Fatal("storage close hook must be invoked even on timeout (reopen integrity)")
	}
	// 强制路径下服务仍处于停机态（后续命令继续被拒）。
	if got := mustResp(t, s, "SET other y"); !strings.HasPrefix(got, "ERR server shutting down") {
		t.Fatalf("command after forced exit: want rejected, got %q", got)
	}
}

// TestShutdownCloseHookAndReopen 优雅停机 flush/关闭存储后数据完整，可重新
// 打开（进行中事务不被截断、已提交数据不丢失）。
func TestShutdownCloseHookAndReopen(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	s := New(d.Txn)
	s.SetShutdownHook(func() error { return d.Close() })
	// 隐式事务立即提交一条数据，停机 flush 后必须保留。
	if got := mustResp(t, s, "SET persist-key v1"); got != "OK" {
		t.Fatalf("SET: %q", got)
	}
	if got := mustResp(t, s, "BEGIN"); got != "OK" {
		t.Fatalf("BEGIN: %q", got)
	}
	if got := mustResp(t, s, "SET inflight v2"); got != "OK" {
		t.Fatalf("SET inflight: %q", got)
	}
	// 停机前提交进行中事务（drain 完成路径）。
	if got := mustResp(t, s, "COMMIT"); got != "OK" {
		t.Fatalf("COMMIT: %q", got)
	}
	if err := s.GracefulShutdown(5 * time.Second); err != nil {
		t.Fatalf("GracefulShutdown: %v", err)
	}
	if s.ForceExited() {
		t.Fatal("want normal exit")
	}
	// 重新打开同一数据目录，验证数据完整。
	d2, err := db.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = d2.Close() }()
	s2 := New(d2.Txn)
	if got := mustResp(t, s2, "GET persist-key"); got != "v1" {
		t.Fatalf("GET persist-key after reopen: want v1, got %q", got)
	}
	if got := mustResp(t, s2, "GET inflight"); got != "v2" {
		t.Fatalf("GET inflight after reopen: want v2, got %q", got)
	}
}

// TestShutdownRejectsNewConnections 停机后监听器关闭：新 TCP 连接被拒绝，
// 已建立连接发新命令被拒（但 QUIT 放行可正常断开）。
func TestShutdownRejectsNewConnections(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- s.ServeListener(ln) }()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	if _, err := fmt.Fprintln(conn, "SET a 1"); err != nil {
		t.Fatalf("write SET: %v", err)
	}
	line, err := br.ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "OK" {
		t.Fatalf("initial SET: %q err=%v", line, err)
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- s.GracefulShutdown(3 * time.Second) }()
	// 监听器被关闭：ServeListener 正常返回。
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("ServeListener: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("listener did not close")
	}
	// 新连接被拒绝。
	if c2, err := net.DialTimeout("tcp", ln.Addr().String(), 300*time.Millisecond); err == nil {
		_ = c2.Close()
		t.Fatal("expected new connection to be rejected after shutdown")
	}
	// 已建立连接：新命令被拒，QUIT 放行。
	if _, err := fmt.Fprintln(conn, "SET b 2"); err != nil {
		t.Fatalf("write SET after shutdown: %v", err)
	}
	line, err = br.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "ERR server shutting down") {
		t.Fatalf("SET after shutdown: %q err=%v", line, err)
	}
	if _, err := fmt.Fprintln(conn, "QUIT"); err != nil {
		t.Fatalf("write QUIT: %v", err)
	}
	line, err = br.ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "BYE" {
		t.Fatalf("QUIT after shutdown: %q err=%v", line, err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("GracefulShutdown: %v", err)
	}
	if s.ForceExited() {
		t.Fatal("want normal exit")
	}
}

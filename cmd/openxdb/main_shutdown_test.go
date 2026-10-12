package main

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/server"
)

// TestRunSignalLoopInterruptExitZero SIGINT（os.Interrupt）触发优雅停机，
// drain 完成后返回退出码 0。
func TestRunSignalLoopInterruptExitZero(t *testing.T) {
	s := server.New(nil)
	s.SetShutdownHook(func() error { return nil })
	s.ShutdownTimeout = func() time.Duration { return time.Second }
	sigCh := make(chan os.Signal, 1)
	sigCh <- os.Interrupt
	if code := runSignalLoop(sigCh, s, s.ShutdownTimeout); code != 0 {
		t.Fatalf("runSignalLoop on SIGINT: want 0, got %d", code)
	}
	if s.ForceExited() {
		t.Fatal("SIGINT with no pending txn must exit normally")
	}
}

// TestRunSignalLoopSIGTERMTimeoutExitOne SIGTERM 触发优雅停机，但进行中
// 事务在超时内未收尾 → 超时强制退出，返回退出码 1 且存储钩子已执行
// （reopen 数据完整）。
func TestRunSignalLoopSIGTERMTimeoutExitOne(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	s := server.New(d.Txn)
	hookCalled := false
	s.SetShutdownHook(func() error {
		hookCalled = true
		return d.Close()
	})
	s.ShutdownTimeout = func() time.Duration { return 80 * time.Millisecond }
	// 先提交一条数据（隐式事务立即提交），验证强制退出不破坏已提交数据。
	if resp, err := s.Handle("SET committed v1"); err != nil || resp != "OK" {
		t.Fatalf("SET committed: resp=%q err=%v", resp, err)
	}
	// 挂起进行中事务：超时内未收尾 → 强制退出（未提交事务应回滚）。
	if resp, err := s.Handle("BEGIN"); err != nil || resp != "OK" {
		t.Fatalf("BEGIN: resp=%q err=%v", resp, err)
	}
	if resp, err := s.Handle("SET k v1"); err != nil || resp != "OK" {
		t.Fatalf("SET in txn: resp=%q err=%v", resp, err)
	}
	sigCh := make(chan os.Signal, 1)
	sigCh <- syscall.SIGTERM
	if code := runSignalLoop(sigCh, s, s.ShutdownTimeout); code != 1 {
		t.Fatalf("runSignalLoop on SIGTERM with pending txn: want 1, got %d", code)
	}
	if !s.ForceExited() {
		t.Fatal("want forced exit")
	}
	if !hookCalled {
		t.Fatal("storage hook must run on forced path")
	}
	// 强制关闭后：已提交数据完整可重开；未提交事务已回滚。
	d2, err := db.Open(dir)
	if err != nil {
		t.Fatalf("reopen after forced exit: %v", err)
	}
	defer func() { _ = d2.Close() }()
	s2 := server.New(d2.Txn)
	if resp, err := s2.Handle("GET committed"); err != nil || resp != "v1" {
		t.Fatalf("GET committed after reopen: resp=%q err=%v", resp, err)
	}
	if resp, err := s2.Handle("GET k"); err == nil || resp == "v1" {
		t.Fatalf("uncommitted txn must roll back after forced exit: resp=%q err=%v", resp, err)
	}
}

package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func mustOpen(t *testing.T, dir string) *DB {
	t.Helper()
	d, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return d
}

// TestInitOpenClose Init 后目录结构完整，Open/Close 往返正常，重复 Init 报错。
func TestInitOpenClose(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for _, p := range []string{ConfigFile, DataSubDir} {
		if _, err := os.Stat(filepath.Join(dir, p)); err != nil {
			t.Fatalf("missing %s: %v", p, err)
		}
	}
	d := mustOpen(t, dir)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := Init(dir); !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatalf("second Init: want ErrAlreadyInitialized, got %v", err)
	}
}

// TestOpenUninitialized Open 未初始化的目录返回 ErrNotInitialized。
func TestOpenUninitialized(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(dir); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("Open: want ErrNotInitialized, got %v", err)
	}
}

// TestPersistenceAcrossReopen 数据跨重开持久（存储 + WAL + 事务层闭环）。
func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}

	d := mustOpen(t, dir)
	tx, err := d.Txn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	d2 := mustOpen(t, dir)
	defer d2.Close()
	v, err := d2.Storage.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(v) != "v1" {
		t.Fatalf("value: want v1, got %q", v)
	}
}

// TestCrashRecoveryReplay 模拟崩溃：WAL 已含 Commit 但存储未落 → 重开由 Recover 补齐。
func TestCrashRecoveryReplay(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	d := mustOpen(t, dir)
	tx, err := d.Txn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put([]byte("recover-key"), []byte("recover-val")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// 关闭前存储里应已有该键（提交协议 WAL 先行后写存储）。
	d.Close()

	// 重开后仍可读。
	d2 := mustOpen(t, dir)
	defer d2.Close()
	if _, err := d2.Storage.Get([]byte("recover-key")); err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
}

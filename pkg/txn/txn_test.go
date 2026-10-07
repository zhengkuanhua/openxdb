package txn_test

import (
	"path/filepath"
	"testing"

	"github.com/openxdb/openxdb/pkg/storage"
	"github.com/openxdb/openxdb/pkg/storage/rocksdb"
	"github.com/openxdb/openxdb/pkg/txn"
	"github.com/openxdb/openxdb/pkg/wal"
)

type env struct {
	st  storage.Storage
	wal wal.WAL
	mgr txn.TxnManager
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	st, err := rocksdb.Open(filepath.Join(dir, "data"), true)
	if err != nil {
		t.Fatalf("open rocksdb: %v", err)
	}
	w, err := wal.Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	mgr, err := txn.New(st, w)
	if err != nil {
		t.Fatalf("new txn manager: %v", err)
	}
	return &env{st: st, wal: w, mgr: mgr}
}

func (e *env) close() {
	e.mgr.Close()
	e.st.Close()
}

func TestTxnCommitPersist(t *testing.T) {
	e := newEnv(t)
	defer e.close()

	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if tx.ID() != 1 {
		t.Fatalf("txn id = %d, want 1", tx.ID())
	}
	if err := tx.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got, err := e.st.Get([]byte("k1"))
	if err != nil || string(got) != "v1" {
		t.Fatalf("st.Get(k1) = %q, %v; want v1", got, err)
	}
	// WAL 中应有 1 条 Commit 记录
	var commits int
	if err := e.wal.Replay(func(en *wal.WalEntry) {
		if en.State == wal.StateCommit {
			commits++
		}
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if commits != 1 {
		t.Fatalf("commit records = %d, want 1", commits)
	}
}

func TestTxnRollbackDiscards(t *testing.T) {
	e := newEnv(t)
	defer e.close()

	tx, _ := e.mgr.Begin()
	_ = tx.Put([]byte("k1"), []byte("v1"))
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := e.st.Get([]byte("k1")); err != storage.ErrNotFound {
		t.Fatalf("after rollback st.Get(k1) err = %v, want ErrNotFound", err)
	}
}

func TestTxnReadYourWrites(t *testing.T) {
	e := newEnv(t)
	defer e.close()

	tx, _ := e.mgr.Begin()
	_ = tx.Put([]byte("k1"), []byte("v1"))
	got, err := tx.Get([]byte("k1"))
	if err != nil || string(got) != "v1" {
		t.Fatalf("txn.Get(k1) = %q, %v; want v1", got, err)
	}
	// 覆盖写：后写优先
	_ = tx.Put([]byte("k1"), []byte("v2"))
	got, _ = tx.Get([]byte("k1"))
	if string(got) != "v2" {
		t.Fatalf("txn.Get(k1) after overwrite = %q, want v2", got)
	}
	// Delete 覆盖 Put
	_ = tx.Delete([]byte("k1"))
	if _, err := tx.Get([]byte("k1")); err != storage.ErrNotFound {
		t.Fatalf("txn.Get(k1) after delete = %v, want ErrNotFound", err)
	}
}

func TestTxnSnapshotIsolation(t *testing.T) {
	e := newEnv(t)
	defer e.close()

	// 外部先写入 k0
	if err := e.st.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k0"), Value: []byte("old")}}}); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	txA, _ := e.mgr.Begin()
	// 外部在 txA 开始后写入 k0 新值
	if err := e.st.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k0"), Value: []byte("new")}}}); err != nil {
		t.Fatalf("outer write: %v", err)
	}
	// txA 应读到 BEGIN 时刻的 old（快照隔离）
	got, err := txA.Get([]byte("k0"))
	if err != nil || string(got) != "old" {
		t.Fatalf("txA.Get(k0) = %q, %v; want old", got, err)
	}
	_ = txA.Put([]byte("k1"), []byte("v1"))
	if err := txA.Commit(); err != nil {
		t.Fatalf("txA commit: %v", err)
	}

	// 新事务看到最新状态
	txB, _ := e.mgr.Begin()
	got, err = txB.Get([]byte("k0"))
	if err != nil || string(got) != "new" {
		t.Fatalf("txB.Get(k0) = %q, %v; want new", got, err)
	}
	got, err = txB.Get([]byte("k1"))
	if err != nil || string(got) != "v1" {
		t.Fatalf("txB.Get(k1) = %q, %v; want v1", got, err)
	}
	if err := txB.Rollback(); err != nil {
		t.Fatalf("txB rollback: %v", err)
	}
}

func TestTxnRecoverReplaysCommits(t *testing.T) {
	e := newEnv(t)
	defer e.close()

	tx, _ := e.mgr.Begin()
	_ = tx.Put([]byte("k1"), []byte("v1"))
	_ = tx.Put([]byte("k2"), []byte("v2"))
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// 模拟「WAL 有 Commit 但存储未应用」的崩溃场景：
	// 直接向 WAL 追加一条 Commit 记录（真实提交流程中该记录对应一次已落 WAL 的提交）。
	extra := &wal.WalEntry{
		TxnID: 999,
		Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k3"), Value: []byte("v3")}}},
		State: wal.StateCommit,
	}
	if err := e.wal.Append(extra); err != nil {
		t.Fatalf("append extra commit: %v", err)
	}

	// 重开全新存储（空数据目录）+ 同一 WAL，Recover 应补齐两条提交
	if err := e.mgr.Recover(); err != nil {
		t.Fatalf("recover idempotent: %v", err)
	}
	got, err := e.st.Get([]byte("k3"))
	if err != nil || string(got) != "v3" {
		t.Fatalf("after recover st.Get(k3) = %q, %v; want v3", got, err)
	}
}

func TestTxnRecoverFromWALOnly(t *testing.T) {
	dir := t.TempDir()
	// 只有 WAL（含 Commit 记录），存储为空
	w, err := wal.Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.Append(&wal.WalEntry{
		TxnID: 1,
		Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("r1"), Value: []byte("rv")}}},
		State: wal.StateCommit,
	}); err != nil {
		t.Fatalf("append commit: %v", err)
	}
	st, err := rocksdb.Open(filepath.Join(dir, "data"), true)
	if err != nil {
		t.Fatalf("open rocksdb: %v", err)
	}
	defer st.Close()
	mgr, err := txn.New(st, w)
	if err != nil {
		t.Fatalf("new txn manager: %v", err)
	}
	defer mgr.Close()
	got, err := st.Get([]byte("r1"))
	if err != nil || string(got) != "rv" {
		t.Fatalf("recovered st.Get(r1) = %q, %v; want rv", got, err)
	}
}

func TestTxnCommitEmptyBatch(t *testing.T) {
	e := newEnv(t)
	defer e.close()

	before := e.wal.LastLSN()
	tx, _ := e.mgr.Begin()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit empty: %v", err)
	}
	if after := e.wal.LastLSN(); after != before {
		t.Fatalf("empty commit wrote WAL: LastLSN %d -> %d", before, after)
	}
}

func TestTxnStateErrors(t *testing.T) {
	e := newEnv(t)
	defer e.close()

	tx, _ := e.mgr.Begin()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := tx.Put([]byte("k"), []byte("v")); err != txn.ErrTxnClosed {
		t.Fatalf("put after commit err = %v, want ErrTxnClosed", err)
	}
	if _, err := tx.Get([]byte("k")); err != txn.ErrTxnClosed {
		t.Fatalf("get after commit err = %v, want ErrTxnClosed", err)
	}

	tx2, _ := e.mgr.Begin()
	_ = tx2.Rollback()
	if err := tx2.Put([]byte("k"), []byte("v")); err != txn.ErrTxnClosed {
		t.Fatalf("put after rollback err = %v, want ErrTxnClosed", err)
	}
}

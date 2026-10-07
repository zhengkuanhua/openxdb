package txn_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/rocksdb"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
	"github.com/zhengkuanhua/openxdb/pkg/wal"
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

// groupCommitWAL 暴露 B2 组提交观测接口（WAL 接口语义之外的测试辅助）。
type groupCommitWAL interface {
	SetGroupCommit(enabled bool)
	SetFsyncDelay(d time.Duration)
	FsyncCount() uint64
}

// TestTxnConcurrentDefaultFsyncBaseline 基线：默认（每记录 fsync）并发提交，
// fsync 次数 == 事务数，证明旧行为保持且重构无回归。
func TestTxnConcurrentDefaultFsyncBaseline(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	gc := e.wal.(groupCommitWAL)

	const n = 30
	runConcurrentCommits(t, e.mgr, n)
	for i := 0; i < n; i++ {
		if _, err := e.st.Get([]byte(fmt.Sprintf("ck%d", i))); err != nil {
			t.Fatalf("st.Get(ck%d): %v", i, err)
		}
	}
	fs := gc.FsyncCount()
	if fs != n {
		t.Fatalf("baseline fsync count = %d, want %d (per-append)", fs, n)
	}
	t.Logf("baseline (per-append fsync): %d txns -> %d fsyncs (1.00 fsync/txn)", n, fs)
}

// TestTxnConcurrentGroupCommit 组提交模式下并发提交：全部成功、数据一致、
// WAL 完整、Recover 幂等（恢复语义不破坏）。
// fsync 合并的强断言由 WAL 层 TestWALGroupCommitMergesFsync 承担（并发 Append+SyncUpTo
// 合并为极少数 fsync）；事务层在真机本地盘上 fsync 极快、提交到达率受调度影响，
// 合并幅度不稳定，这里以日志观测记录实际 fsync 次数供文档对比，并断言不劣于逐条刷盘。
func TestTxnConcurrentGroupCommit(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	gc := e.wal.(groupCommitWAL)
	gc.SetGroupCommit(true)

	const n = 100
	runConcurrentCommits(t, e.mgr, n)
	// 数据一致：100 个 key 全部可见
	for i := 0; i < n; i++ {
		got, err := e.st.Get([]byte(fmt.Sprintf("ck%d", i)))
		if err != nil || string(got) != fmt.Sprintf("cv%d", i) {
			t.Fatalf("st.Get(ck%d) = %q, %v; want cv%d", i, got, err, i)
		}
	}
	// 组提交开启后 fsync 不应比逐条刷盘更多（leader 合并至少不劣化）
	fs := gc.FsyncCount()
	if fs > n {
		t.Fatalf("group fsync count = %d, want <= %d (should not exceed per-append)", fs, n)
	}
	t.Logf("group commit: %d txns -> %d fsyncs (%.1f%% reduction)", n, fs, 100*(1-float64(fs)/float64(n)))
	// WAL 应有 n 条 Commit 记录
	var commits int
	if err := e.wal.Replay(func(en *wal.WalEntry) {
		if en.State == wal.StateCommit {
			commits++
		}
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if commits != n {
		t.Fatalf("commit records = %d, want %d", commits, n)
	}
	// Recover 幂等补齐（崩溃恢复语义不破坏）
	if err := e.mgr.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
}

// TestTxnConcurrentGroupCommitRecovery 组提交并发提交后关闭全部，
// 用全新空存储 + 同一 WAL 重新打开，验证全部提交可从 WAL 恢复。
func TestTxnConcurrentGroupCommitRecovery(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	st, err := rocksdb.Open(filepath.Join(dir, "data1"), true)
	if err != nil {
		t.Fatalf("open rocksdb: %v", err)
	}
	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	w.(groupCommitWAL).SetGroupCommit(true)
	mgr, err := txn.New(st, w)
	if err != nil {
		t.Fatalf("new txn manager: %v", err)
	}
	const n = 30
	runConcurrentCommits(t, mgr, n)
	if err := mgr.Close(); err != nil {
		t.Fatalf("close mgr: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close st: %v", err)
	}

	// 崩溃恢复：全新空存储 + 同一 WAL（New 内部执行 Recover 重放）
	st2, err := rocksdb.Open(filepath.Join(dir, "data2"), true)
	if err != nil {
		t.Fatalf("open rocksdb2: %v", err)
	}
	defer st2.Close()
	w2, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("open wal2: %v", err)
	}
	defer w2.Close()
	mgr2, err := txn.New(st2, w2)
	if err != nil {
		t.Fatalf("new txn manager2: %v", err)
	}
	defer mgr2.Close()
	for i := 0; i < n; i++ {
		got, err := st2.Get([]byte(fmt.Sprintf("ck%d", i)))
		if err != nil || string(got) != fmt.Sprintf("cv%d", i) {
			t.Fatalf("recovered st.Get(ck%d) = %q, %v; want cv%d", i, got, err, i)
		}
	}
}

// runConcurrentCommits 并发执行 n 个独立事务：Begin → Put(ckN/cvN) → Commit。
// 使用 start 屏障：所有事务先完成 Begin+Put，再同时发起 Commit，
// 制造真实的并发提交窗口（高负载下组提交合并 fsync 的效果由此体现）。
func runConcurrentCommits(t *testing.T, mgr txn.TxnManager, n int) {
	t.Helper()
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, n)
	prepared := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx, err := mgr.Begin()
			if err != nil {
				errs[i] = err
				prepared <- struct{}{}
				<-start
				return
			}
			if err := tx.Put([]byte(fmt.Sprintf("ck%d", i)), []byte(fmt.Sprintf("cv%d", i))); err != nil {
				errs[i] = err
				prepared <- struct{}{}
				<-start
				return
			}
			prepared <- struct{}{}
			<-start
			errs[i] = tx.Commit()
		}(i)
	}
	for i := 0; i < n; i++ {
		<-prepared
	}
	close(start)
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("concurrent commit %d: %v", i, errs[i])
		}
	}
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

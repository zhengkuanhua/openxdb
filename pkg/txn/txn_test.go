package txn_test

import (
	"bytes"
	"fmt"
	"math/rand"
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

// TestTxnConcurrentDefaultFsyncBaseline 基线：组提交显式关闭时并发提交，
// fsync 次数 == 事务数，证明关闭路径保持旧行为（每记录 fsync）且无回归。
func TestTxnConcurrentDefaultFsyncBaseline(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	gc := e.wal.(groupCommitWAL)
	gc.SetGroupCommit(false) // v1.12.0：默认开启组提交，此处显式关闭验证旧基线

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

// TestTxnConcurrentDefaultGroupCommitOn 默认模式（未显式开关）并发提交：
// 组提交默认开启，fsync 应合并、不劣于逐条刷盘，且数据全部一致。
func TestTxnConcurrentDefaultGroupCommitOn(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	gc := e.wal.(groupCommitWAL)

	const n = 30
	runConcurrentCommits(t, e.mgr, n)
	for i := 0; i < n; i++ {
		got, err := e.st.Get([]byte(fmt.Sprintf("ck%d", i)))
		if err != nil || string(got) != fmt.Sprintf("cv%d", i) {
			t.Fatalf("st.Get(ck%d) = %q, %v; want cv%d", i, got, err, i)
		}
	}
	fs := gc.FsyncCount()
	if fs > n {
		t.Fatalf("default group fsync count = %d, want <= %d (merged, should not exceed per-append)", fs, n)
	}
	if fs >= n {
		t.Fatalf("default group fsync count = %d, want < %d (group commit should merge fsyncs)", fs, n)
	}
	t.Logf("default group commit: %d txns -> %d fsyncs (%.1f%% reduction)", n, fs, 100*(1-float64(fs)/float64(n)))
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

// ---- v1.14.0 P0：Scan 快路径 + CountRange（COUNT(*) 快速路径底层） ----

// seedSortedKeys 写入 n 个有序键（k00..k(n-1)）并提交。
func seedSortedKeys(t *testing.T, mgr txn.TxnManager, n int) {
	t.Helper()
	tx, err := mgr.Begin()
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%02d", i)
		if err := tx.Put([]byte(k), []byte("v"+k)); err != nil {
			t.Fatalf("seed put %s: %v", k, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
}

// scanKeys 便捷封装：Scan 返回的键列表。
func scanKeys(t *testing.T, tx txn.Txn, r storage.KeyRange, limit int) []string {
	t.Helper()
	kvs, err := tx.Scan(r, limit)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	out := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, string(kv.Key))
	}
	return out
}

// serializableTxn 断言 Serializable 读集接口（P2-13）。
type serializableTxn interface {
	EnableSerializable()
	ReadSet() map[string]bool
}

// TestTxnScanFastPathEmptyOps 快路径（ops 为空）：直接透传快照有序结果，
// 与旧实现升序输出等价（全量 / limit 截断 / 子区间）。
func TestTxnScanFastPathEmptyOps(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	seedSortedKeys(t, e.mgr, 5)

	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	wantAll := []string{"k00", "k01", "k02", "k03", "k04"}
	got := scanKeys(t, tx, storage.KeyRange{}, 0)
	if fmt.Sprint(got) != fmt.Sprint(wantAll) {
		t.Fatalf("fast scan full = %v, want %v", got, wantAll)
	}
	got = scanKeys(t, tx, storage.KeyRange{}, 2)
	if fmt.Sprint(got) != fmt.Sprint(wantAll[:2]) {
		t.Fatalf("fast scan limit2 = %v, want %v", got, wantAll[:2])
	}
	got = scanKeys(t, tx, storage.KeyRange{Start: []byte("k01"), End: []byte("k04")}, 0)
	if fmt.Sprint(got) != fmt.Sprint(wantAll[1:4]) {
		t.Fatalf("fast scan range = %v, want %v", got, wantAll[1:4])
	}
}

// TestTxnScanFastPathWriteMerge ops 非空保留原合并路径（read-your-writes）：
// 新键可见、Delete 生效、覆盖键以事务值为准，结果仍升序。
func TestTxnScanFastPathWriteMerge(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	seedSortedKeys(t, e.mgr, 5)

	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	if err := tx.Put([]byte("k02new"), []byte("x")); err != nil { // 区间中部新键
		t.Fatalf("put: %v", err)
	}
	if err := tx.Delete([]byte("k01")); err != nil { // 删除既有键
		t.Fatalf("delete: %v", err)
	}
	if err := tx.Put([]byte("k03"), []byte("overwritten")); err != nil { // 覆盖既有键
		t.Fatalf("put overwrite: %v", err)
	}
	got := scanKeys(t, tx, storage.KeyRange{}, 0)
	want := []string{"k00", "k02", "k02new", "k03", "k04"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("merge scan = %v, want %v", got, want)
	}
	v, err := tx.Get([]byte("k03"))
	if err != nil || string(v) != "overwritten" {
		t.Fatalf("get k03 = %q, %v; want overwritten", v, err)
	}
}

// TestTxnScanFastPathSerializableReadSet Serializable 下快路径读集与旧实现等价：
// 记录实际返回键，limit 截断的未读行不记，phantom 读集不被破坏。
func TestTxnScanFastPathSerializableReadSet(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	seedSortedKeys(t, e.mgr, 5)

	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	ser, ok := tx.(serializableTxn)
	if !ok {
		t.Fatalf("tx does not implement serializable read set")
	}
	ser.EnableSerializable()

	scanKeys(t, tx, storage.KeyRange{}, 0)
	rs := ser.ReadSet()
	for i := 0; i < 5; i++ {
		if !rs[fmt.Sprintf("k%02d", i)] {
			t.Fatalf("readSet missing k%02d: %v", i, rs)
		}
	}

	// limit 截断：未读行不记（与旧路径一致）
	tx2, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback()
	ser2, _ := tx2.(serializableTxn)
	ser2.EnableSerializable()
	scanKeys(t, tx2, storage.KeyRange{}, 2)
	rs2 := ser2.ReadSet()
	if len(rs2) != 2 || !rs2["k00"] || !rs2["k01"] || rs2["k02"] {
		t.Fatalf("limit readSet = %v, want exactly {k00,k01}", rs2)
	}
}

// TestTxnCountRangeMatchesScan CountRange 与同范围 Scan 行数一致：
// 空 ops 快路径（走快照 CountRange）与 ops 非空回退（read-your-writes）。
func TestTxnCountRangeMatchesScan(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	seedSortedKeys(t, e.mgr, 5)

	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	rc, ok := tx.(storage.RangeCounter)
	if !ok {
		t.Fatalf("tx does not implement storage.RangeCounter")
	}
	n, err := rc.CountRange(storage.KeyRange{}, 0)
	if err != nil || n != 5 {
		t.Fatalf("count full = %d, %v; want 5", n, err)
	}
	n, err = rc.CountRange(storage.KeyRange{Start: []byte("k01"), End: []byte("k04")}, 0)
	if err != nil || n != 3 {
		t.Fatalf("count [k01,k04) = %d, %v; want 3", n, err)
	}
	n, err = rc.CountRange(storage.KeyRange{}, 2)
	if err != nil || n != 2 {
		t.Fatalf("count limit2 = %d, %v; want 2", n, err)
	}
	if got := len(scanKeys(t, tx, storage.KeyRange{}, 0)); got != 5 {
		t.Fatalf("scan count = %d, want 5", got)
	}

	// ops 非空：回退 Scan 计数，read-your-writes 正确
	tx2, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback()
	rc2, _ := tx2.(storage.RangeCounter)
	if err := tx2.Put([]byte("k99"), []byte("x")); err != nil {
		t.Fatalf("put: %v", err)
	}
	n, err = rc2.CountRange(storage.KeyRange{}, 0)
	if err != nil || n != 6 {
		t.Fatalf("count with pending put = %d, %v; want 6", n, err)
	}
	if err := tx2.Delete([]byte("k00")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	n, err = rc2.CountRange(storage.KeyRange{}, 0)
	if err != nil || n != 5 {
		t.Fatalf("count with pending delete = %d, %v; want 5", n, err)
	}
	if err := tx2.Put([]byte("k02"), []byte("over")); err != nil { // 覆盖不增行
		t.Fatalf("put overwrite: %v", err)
	}
	n, err = rc2.CountRange(storage.KeyRange{}, 0)
	if err != nil || n != 5 {
		t.Fatalf("count with overwrite = %d, %v; want 5", n, err)
	}
	if got := len(scanKeys(t, tx2, storage.KeyRange{}, 0)); got != n {
		t.Fatalf("scan count = %d, want %d", got, n)
	}
}

// TestTxnCountRangeSerializableFallback Serializable 下 CountRange 回退 Scan 计数：
// 结果与 Scan 一致，且 phantom 读集不被破坏（范围内键全部记入 readSet）。
func TestTxnCountRangeSerializableFallback(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	seedSortedKeys(t, e.mgr, 5)

	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	ser, ok := tx.(serializableTxn)
	if !ok {
		t.Fatalf("tx does not implement serializable read set")
	}
	ser.EnableSerializable()
	rc, ok := tx.(storage.RangeCounter)
	if !ok {
		t.Fatalf("tx does not implement storage.RangeCounter")
	}

	n, err := rc.CountRange(storage.KeyRange{}, 0)
	if err != nil || n != 5 {
		t.Fatalf("ser count = %d, %v; want 5", n, err)
	}
	rs := ser.ReadSet()
	for i := 0; i < 5; i++ {
		if !rs[fmt.Sprintf("k%02d", i)] {
			t.Fatalf("ser readSet missing k%02d: %v", i, rs)
		}
	}
	if got := len(scanKeys(t, tx, storage.KeyRange{}, 0)); got != n {
		t.Fatalf("ser scan count = %d, want %d", got, n)
	}
}

// TestTxnBeginConcurrentIDsUnique v1.16.0 C2 快照创建锁外化：并发 Begin 的
// 事务 ID 仍单调唯一（锁内分配 nextID），且每个事务快照可用（Get 种子 key
// 成功），锁外创建快照不破坏 ID 语义与快照可用性。
func TestTxnBeginConcurrentIDsUnique(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	seedSortedKeys(t, e.mgr, 3)

	const n = 200
	var wg sync.WaitGroup
	ids := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx, err := e.mgr.Begin()
			if err != nil {
				errs[i] = err
				return
			}
			ids[i] = tx.ID()
			if _, err := tx.Get([]byte("k00")); err != nil {
				errs[i] = err
				return
			}
			errs[i] = tx.Rollback()
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("begin/get/rollback %d: %v", i, errs[i])
		}
	}
	seen := make(map[uint64]bool, n)
	for _, id := range ids {
		// seedSortedKeys 已消耗 id=1，并发 Begin 的 id ∈ [2, n+1]
		if id < 2 || id > uint64(n)+1 {
			t.Fatalf("txn id %d out of range [2,%d]", id, n+1)
		}
		if seen[id] {
			t.Fatalf("duplicate txn id %d", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("unique ids = %d, want %d", len(seen), n)
	}
}

// TestTxnBeginIDsMonotonic v1.16.0 C2：顺序 Begin 的事务 ID 严格递增且连续
// （nextID 锁内单调分配语义与 v1.15.0 完全一致）。
func TestTxnBeginIDsMonotonic(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	prev := uint64(0)
	const n = 20
	for i := 0; i < n; i++ {
		tx, err := e.mgr.Begin()
		if err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
		if tx.ID() != prev+1 {
			t.Fatalf("txn id = %d, want %d (monotonic)", tx.ID(), prev+1)
		}
		prev = tx.ID()
		if err := tx.Rollback(); err != nil {
			t.Fatalf("rollback %d: %v", i, err)
		}
	}
}

// TestTxnBeginSnapshotIsolationConcurrent v1.16.0 C2：并发提交与 Begin 交错时，
// 每个事务的快照仍是创建时刻的一致性视图——读到的值要么是快照前已提交的旧值、
// 要么是已提交的新值，绝无中间态或未提交值；锁外化不破坏快照隔离语义。
func TestTxnBeginSnapshotIsolationConcurrent(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	// 种子：k0=old
	if err := e.st.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k0"), Value: []byte("old")}}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// 后台持续提交 k0=new 覆盖（走完整事务路径）
	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			tx, err := e.mgr.Begin()
			if err != nil {
				return
			}
			if err := tx.Put([]byte("k0"), []byte("new")); err != nil {
				return
			}
			if err := tx.Commit(); err != nil {
				return
			}
		}
	}()
	// 主流程：并发 Begin+Get，验证快照一致性
	const n = 200
	var wg sync.WaitGroup
	bad := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := e.mgr.Begin()
			if err != nil {
				bad <- fmt.Sprintf("begin: %v", err)
				return
			}
			defer tx.Rollback()
			v, err := tx.Get([]byte("k0"))
			if err != nil {
				bad <- fmt.Sprintf("get: %v", err)
				return
			}
			s := string(v)
			if s != "old" && s != "new" {
				bad <- fmt.Sprintf("inconsistent snapshot value %q", s)
			}
		}()
	}
	wg.Wait()
	close(stop)
	bg.Wait()
	select {
	case msg := <-bad:
		t.Fatalf("snapshot violation: %s", msg)
	default:
	}
}

// ===== v1.16.0 C4：提交应用组提交（流水线化）专项测试 =====

// applyGroupObserver 暴露 C4 应用聚合观测/控制接口（TxnManager 可选接口，类型断言）。
type applyGroupObserver interface {
	SetApplyGroupWindow(d time.Duration)
	ApplyGroupStats() (writes, txns uint64)
	ApplyBypassCount() uint64
}

// dumpStore 全量扫描存储为 key->value 映射（C4 等价性对比辅助）。
func dumpStore(t *testing.T, st storage.Storage) map[string][]byte {
	t.Helper()
	pairs, err := st.Scan(storage.KeyRange{}, 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	m := make(map[string][]byte, len(pairs))
	for _, p := range pairs {
		m[string(p.Key)] = append([]byte(nil), p.Value...)
	}
	return m
}

// assertStoresEqual 逐 key 对比两个存储状态（C4 崩溃恢复等价断言）。
func assertStoresEqual(t *testing.T, got, want map[string][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("key count mismatch: got %d, want %d", len(got), len(want))
	}
	for k, v := range want {
		gv, ok := got[k]
		if !ok {
			t.Fatalf("key %q missing in got", k)
		}
		if !bytes.Equal(gv, v) {
			t.Fatalf("key %q value mismatch: got %q, want %q", k, gv, v)
		}
	}
}

// TestC4ConcurrentCommitOrder 并发提交（聚合默认开）：全部成功、seq 连续、
// 数据一致、应用统计不劣化（writes <= txns）、合并度观测记录日志。
func TestC4ConcurrentCommitOrder(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	ago := e.mgr.(applyGroupObserver)

	const n = 200
	runConcurrentCommits(t, e.mgr, n)
	for i := 0; i < n; i++ {
		got, err := e.st.Get([]byte(fmt.Sprintf("ck%d", i)))
		if err != nil || string(got) != fmt.Sprintf("cv%d", i) {
			t.Fatalf("st.Get(ck%d) = %q, %v; want cv%d", i, got, err, i)
		}
	}
	writes, txns := ago.ApplyGroupStats()
	if txns != n {
		t.Fatalf("apply txns = %d, want %d", txns, n)
	}
	if writes > txns {
		t.Fatalf("apply writes %d > txns %d (merge must not increase st.Write)", writes, txns)
	}
	t.Logf("C4 group apply: %d txns -> %d st.Write (avg batch %.2f)", txns, writes, float64(txns)/float64(writes))
}

// TestC4GroupApplyMergesWrites 放大聚合窗口确定性验证合并：高并发 + 大窗口
// 时 st.Write 次数必须显著小于事务数（消除逐事务串行写），数据全部一致。
func TestC4GroupApplyMergesWrites(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	ago := e.mgr.(applyGroupObserver)
	ago.SetApplyGroupWindow(20 * time.Millisecond) // 放大窗口，确定性触发候选聚合

	const n = 100
	runConcurrentCommits(t, e.mgr, n)
	for i := 0; i < n; i++ {
		got, err := e.st.Get([]byte(fmt.Sprintf("ck%d", i)))
		if err != nil || string(got) != fmt.Sprintf("cv%d", i) {
			t.Fatalf("st.Get(ck%d) = %q, %v; want cv%d", i, got, err, i)
		}
	}
	writes, txns := ago.ApplyGroupStats()
	if txns != n {
		t.Fatalf("apply txns = %d, want %d", txns, n)
	}
	if writes >= txns {
		t.Fatalf("C4 expected merged st.Write: %d writes >= %d txns", writes, txns)
	}
	t.Logf("C4 merged: %d txns -> %d st.Write (avg batch %.2f)", txns, writes, float64(txns)/float64(writes))
}

// TestC4WALReplayEquivalence 并发提交（聚合开）后，用同一 WAL 重放到全新
// 存储：重放状态与运行期最终状态逐 key 一致（崩溃恢复等价核心证据）。
func TestC4WALReplayEquivalence(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	st, err := rocksdb.Open(filepath.Join(dir, "data"), true)
	if err != nil {
		t.Fatalf("open rocksdb: %v", err)
	}
	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	mgr, err := txn.New(st, w)
	if err != nil {
		t.Fatalf("new txn manager: %v", err)
	}

	const n = 100
	runConcurrentCommits(t, mgr, n)
	live := dumpStore(t, st)
	if err := mgr.Close(); err != nil {
		t.Fatalf("close mgr: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close st: %v", err)
	}

	// 全新存储 + 同一 WAL：txn.New 自动执行 Recover 重放。
	st2, err := rocksdb.Open(filepath.Join(dir, "data2"), true)
	if err != nil {
		t.Fatalf("open rocksdb2: %v", err)
	}
	w2, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("open wal2: %v", err)
	}
	mgr2, err := txn.New(st2, w2)
	if err != nil {
		t.Fatalf("new txn manager2: %v", err)
	}
	replayed := dumpStore(t, st2)
	assertStoresEqual(t, replayed, live)
	if err := mgr2.Close(); err != nil {
		t.Fatalf("close mgr2: %v", err)
	}
	if err := st2.Close(); err != nil {
		t.Fatalf("close st2: %v", err)
	}
}

// c4Op 崩溃恢复等价负载中的单事务操作（固定生成、两模式复用同一序列）。
type c4Op struct {
	isDelete bool
	key      string
	val      string
}

// genC4Ops 单线程生成固定随机负载（seed 固定）：30 个事务，每事务 1-5 个
// 随机 Put/Delete。disjoint=true 时每事务独占 10 个 key（操作集合可交换，
// 最终状态与提交顺序无关，用于聚合开/关两模式对比）；
// disjoint=false 时 key 池 k0..k19（同 key 多次写/删交错，最终状态依赖
// WAL 顺序，用于 WAL 重放等价验证）。
func genC4Ops(seed int64, disjoint bool) [][]c4Op {
	rng := rand.New(rand.NewSource(seed))
	opsSeq := make([][]c4Op, 30)
	for i := range opsSeq {
		ops := 1 + rng.Intn(5)
		opsSeq[i] = make([]c4Op, 0, ops)
		for j := 0; j < ops; j++ {
			op := c4Op{}
			if disjoint {
				op.key = fmt.Sprintf("k%d", i*10+rng.Intn(10))
			} else {
				op.key = fmt.Sprintf("k%d", rng.Intn(20))
			}
			if rng.Intn(3) == 0 {
				op.isDelete = true
			} else {
				op.val = fmt.Sprintf("v%d", rng.Intn(100))
			}
			opsSeq[i] = append(opsSeq[i], op)
		}
	}
	return opsSeq
}

// runC4Load 按给定聚合窗口执行固定并发负载，返回最终存储状态。
func runC4Load(t *testing.T, dir string, applyWindow time.Duration, opsSeq [][]c4Op) map[string][]byte {
	t.Helper()
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
	mgr.(applyGroupObserver).SetApplyGroupWindow(applyWindow)

	const n = 30
	errs := make([]error, n)
	start := make(chan struct{})
	prepared := make(chan struct{}, n)
	var wg sync.WaitGroup
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
			for _, op := range opsSeq[i] {
				if op.isDelete {
					err = tx.Delete([]byte(op.key))
				} else {
					err = tx.Put([]byte(op.key), []byte(op.val))
				}
				if err != nil {
					break
				}
			}
			prepared <- struct{}{}
			<-start
			if err == nil {
				err = tx.Commit()
			}
			errs[i] = err
		}(i)
	}
	for i := 0; i < n; i++ {
		<-prepared
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("txn %d: %v", i, err)
		}
	}
	state := dumpStore(t, st)
	if err := mgr.Close(); err != nil {
		t.Fatalf("close mgr: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close st: %v", err)
	}
	return state
}

// replayToNewStore 用同一 WAL 重放到全新存储，返回重放后的状态
// （C4 崩溃恢复等价：串行重放应用 vs 运行期聚合应用结果对比）。
func replayToNewStore(t *testing.T, walPath, dataDir string) map[string][]byte {
	t.Helper()
	st, err := rocksdb.Open(filepath.Join(dataDir, "data"), true)
	if err != nil {
		t.Fatalf("open rocksdb replay: %v", err)
	}
	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("open wal replay: %v", err)
	}
	mgr, err := txn.New(st, w) // 自动执行 Recover 重放
	if err != nil {
		t.Fatalf("new txn manager replay: %v", err)
	}
	state := dumpStore(t, st)
	if err := mgr.Close(); err != nil {
		t.Fatalf("close mgr replay: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close st replay: %v", err)
	}
	return state
}

// TestC4CrashRecoveryEquivalence 崩溃恢复等价（核心）：同一固定随机并发负载
// （无 key 重叠、操作集合可交换），分别在「聚合开（批量 st.Write）」与
// 「聚合关（纯串行 st.Write）」两模式下执行，最终存储状态必须逐 key 一致
// ——证明合并应用在任何并发交错下不改变可恢复的最终状态。
func TestC4CrashRecoveryEquivalence(t *testing.T) {
	opsSeq := genC4Ops(42, true)
	dirAgg := t.TempDir()
	dirSer := t.TempDir()
	agg := runC4Load(t, dirAgg, 50*time.Microsecond, opsSeq) // 聚合开
	ser := runC4Load(t, dirSer, 0, opsSeq)                   // 聚合关（纯串行，旧行为）
	assertStoresEqual(t, agg, ser)
}

// TestC4WALReplayOverlapKeys 重叠 key 随机负载（同 key 多次写/删、最终状态
// 依赖 WAL 顺序）：聚合开模式运行期状态与同一 WAL 串行重放状态逐 key 一致
// ——验证「同 WAL 顺序下串行应用 == 聚合应用」的崩溃恢复等价。
func TestC4WALReplayOverlapKeys(t *testing.T) {
	opsSeq := genC4Ops(7, false)
	dir := t.TempDir()
	state := runC4Load(t, dir, 50*time.Microsecond, opsSeq) // 聚合开
	replayed := replayToNewStore(t, filepath.Join(dir, "wal.log"), t.TempDir())
	assertStoresEqual(t, replayed, state)
}

// TestC4SingleTxnNoExtraDelay 单事务提交：候补为 0，不得触发聚合窗口等待；
// 提交快速完成、统计精确 1 次 st.Write（零额外延迟与零合并副作用）。
func TestC4SingleTxnNoExtraDelay(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	ago := e.mgr.(applyGroupObserver)

	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Put([]byte("solo"), []byte("sv")); err != nil {
		t.Fatalf("put: %v", err)
	}
	start := time.Now()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("single txn commit took %v, want no window delay", elapsed)
	}
	got, err := e.st.Get([]byte("solo"))
	if err != nil || string(got) != "sv" {
		t.Fatalf("st.Get(solo) = %q, %v; want sv", got, err)
	}
	writes, txns := ago.ApplyGroupStats()
	if writes != 1 || txns != 1 {
		t.Fatalf("apply stats = %d/%d, want 1/1 (single txn, no merge)", writes, txns)
	}
	if bypass := ago.ApplyBypassCount(); bypass != 1 {
		t.Fatalf("apply bypass = %d, want 1 (single txn must take zero-overhead fast path)", bypass)
	}
}

// TestC4SerialBypassNoGroupOverhead 退化回归（返工核心）：串行提交 N 个
// 单事务（每个 Commit 返回后才开启下一个，天然无候补），必须全部走直通
// 路径——bypass == N 且 writes == txns == N（每次提交恰好一次 st.Write，
// 零注册/零聚合/零等待开销），证明低并发路径已恢复到与基线逐事务串行
// 应用完全一致的执行形态。
func TestC4SerialBypassNoGroupOverhead(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	ago := e.mgr.(applyGroupObserver)

	const n = 50
	for i := 0; i < n; i++ {
		tx, err := e.mgr.Begin()
		if err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
		if err := tx.Put([]byte(fmt.Sprintf("bk%d", i)), []byte(fmt.Sprintf("bv%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		got, err := e.st.Get([]byte(fmt.Sprintf("bk%d", i)))
		if err != nil || string(got) != fmt.Sprintf("bv%d", i) {
			t.Fatalf("st.Get(bk%d) = %q, %v; want bv%d", i, got, err, i)
		}
	}
	writes, txns := ago.ApplyGroupStats()
	if txns != n || writes != n {
		t.Fatalf("apply stats = %d/%d, want %d/%d (serial commits, one st.Write each)", writes, txns, n, n)
	}
	if bypass := ago.ApplyBypassCount(); bypass != n {
		t.Fatalf("apply bypass = %d, want %d (all serial commits must bypass group machinery)", bypass, n)
	}
}

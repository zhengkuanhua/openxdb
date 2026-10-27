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

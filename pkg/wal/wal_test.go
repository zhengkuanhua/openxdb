package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

func newTestWAL(t *testing.T) (string, WAL) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	return path, w
}

func TestWALAppendReplay(t *testing.T) {
	_, w := newTestWAL(t)
	defer w.Close()

	entries := []*WalEntry{
		{LSN: 1, TxnID: 10, State: StatePrepare, Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k1"), Value: []byte("v1")}}}},
		{LSN: 2, TxnID: 10, State: StateCommit, Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k2"), Value: []byte("v2")}}}},
		{LSN: 3, TxnID: 11, State: StateCommit, Batch: &storage.WriteBatch{Deletes: [][]byte{[]byte("k1")}}},
	}
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("append lsn=%d: %v", e.LSN, err)
		}
	}

	var got []*WalEntry
	if err := w.Replay(func(e *WalEntry) { got = append(got, e) }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("replay count = %d, want 3", len(got))
	}
	for i, e := range got {
		if e.LSN != entries[i].LSN || e.TxnID != entries[i].TxnID || e.State != entries[i].State {
			t.Fatalf("entry %d mismatch: %+v", i, e)
		}
		if len(e.Batch.Puts) != len(entries[i].Batch.Puts) || len(e.Batch.Deletes) != len(entries[i].Batch.Deletes) {
			t.Fatalf("entry %d batch mismatch", i)
		}
	}
	if got[0].Batch.Puts[0].Key[0] != 'k' {
		t.Fatalf("put key wrong: %s", got[0].Batch.Puts[0].Key)
	}
	if last := w.LastLSN(); last != 3 {
		t.Fatalf("LastLSN = %d, want 3", last)
	}
}

func TestWALPersistAcrossReopen(t *testing.T) {
	path, w := newTestWAL(t)
	for i := uint64(1); i <= 5; i++ {
		e := &WalEntry{LSN: storage.LSN(i), TxnID: i, State: StateCommit,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k"), Value: []byte("v")}}}}
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	var count int
	if err := w2.Replay(func(e *WalEntry) { count++ }); err != nil {
		t.Fatalf("replay after reopen: %v", err)
	}
	if count != 5 {
		t.Fatalf("replayed = %d, want 5", count)
	}
	if last := w2.LastLSN(); last != 5 {
		t.Fatalf("LastLSN after reopen = %d, want 5", last)
	}
	// 续写应自动分配下一个 LSN
	if err := w2.Append(&WalEntry{TxnID: 99, State: StateCommit,
		Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("x"), Value: []byte("y")}}}}); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if last := w2.LastLSN(); last != 6 {
		t.Fatalf("LastLSN after extra append = %d, want 6", last)
	}
}

func TestWALTruncate(t *testing.T) {
	path, w := newTestWAL(t)
	defer w.Close()
	for i := uint64(1); i <= 5; i++ {
		e := &WalEntry{LSN: storage.LSN(i), TxnID: i, State: StateCommit,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k"), Value: []byte("v")}}}}
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// 截断 lsn<3（保留 3,4,5）
	if err := w.Truncate(3); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	var lsns []uint64
	if err := w.Replay(func(e *WalEntry) { lsns = append(lsns, uint64(e.LSN)) }); err != nil {
		t.Fatalf("replay after truncate: %v", err)
	}
	if len(lsns) != 3 || lsns[0] != 3 || lsns[2] != 5 {
		t.Fatalf("lsns after truncate = %v, want [3 4 5]", lsns)
	}
	// 关闭重开仍生效
	w.Close()
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after truncate: %v", err)
	}
	defer w2.Close()
	lsns = nil
	if err := w2.Replay(func(e *WalEntry) { lsns = append(lsns, uint64(e.LSN)) }); err != nil {
		t.Fatalf("replay reopened: %v", err)
	}
	if len(lsns) != 3 {
		t.Fatalf("reopened lsns = %v, want 3 items", lsns)
	}
}

func TestWALCorruptionDetected(t *testing.T) {
	path, w := newTestWAL(t)
	for i := uint64(1); i <= 3; i++ {
		e := &WalEntry{LSN: storage.LSN(i), TxnID: i, State: StateCommit,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k"), Value: []byte("v")}}}}
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 破坏第一条记录 body 的一个字节（headerSize 之后）
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	if _, err := f.Seek(int64(headerSize+recHeadSize+4), 0); err != nil {
		t.Fatalf("seek: %v", err)
	}
	b := []byte{0xFF}
	if _, err := f.Write(b); err != nil {
		t.Fatalf("write corrupt byte: %v", err)
	}
	f.Close()

	w2, err := Open(path)
	if err == nil {
		// Open 成功则 Replay 必须报错
		err = w2.Replay(func(e *WalEntry) {})
		w2.Close()
		if err == nil {
			t.Fatal("corrupt log not detected")
		}
		return
	}
	// Open 阶段即失败也是可接受的（扫描末尾记录时 CRC 校验不过）
}

func TestWALGroupCommitDefaultOn(t *testing.T) {
	_, w := newTestWAL(t)
	defer w.Close()
	for i := uint64(1); i <= 5; i++ {
		e := &WalEntry{LSN: storage.LSN(i), TxnID: i, State: StateCommit,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k"), Value: []byte("v")}}}}
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	wi := w.(*walImpl)
	// 默认模式（组提交开启）：Append 仅写缓冲，不触发 fsync
	if got := wi.FsyncCount(); got != 0 {
		t.Fatalf("default fsync count after append = %d, want 0 (buffered)", got)
	}
	// SyncUpTo 合并刷盘：5 条记录一次 fsync
	if err := w.SyncUpTo(5); err != nil {
		t.Fatalf("syncupto: %v", err)
	}
	if got := wi.FsyncCount(); got != 1 {
		t.Fatalf("fsync count after SyncUpTo = %d, want 1 (merged)", got)
	}
	// SyncUpTo(0) 立即返回
	if err := w.SyncUpTo(0); err != nil {
		t.Fatalf("syncupto(0): %v", err)
	}
}

func TestWALGroupCommitMergesFsync(t *testing.T) {
	_, w := newTestWAL(t)
	defer w.Close()
	w.SetGroupCommit(true)
	wi := w.(*walImpl)

	const n = 100
	lsns := make([]storage.LSN, n)
	errs := make([]error, n)

	// 阶段 1：并发 Append（只写不 fsync），快速返回分配的 LSN
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := &WalEntry{TxnID: uint64(i + 1), State: StateCommit,
				Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}}}}
			errs[i] = w.Append(e)
			lsns[i] = e.LSN
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("append %d: %v", i, errs[i])
		}
	}
	// 全部记录已写入缓冲，尚未刷盘
	if got := wi.FsyncCount(); got != 0 {
		t.Fatalf("fsync during buffered append = %d, want 0", got)
	}

	// 阶段 2：并发 SyncUpTo —— leader 机制应合并为极少数 fsync
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = w.SyncUpTo(lsns[i])
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("syncupto %d (lsn=%d): %v", i, lsns[i], errs[i])
		}
	}
	// 全部 LSN 在阶段 2 前已写入，首个 leader 一次刷盘即可覆盖全部
	if got := wi.FsyncCount(); got > 3 {
		t.Fatalf("merged fsync count = %d, want <= 3 for %d txns", got, n)
	}
	t.Logf("group commit: %d appends -> %d fsyncs", n, wi.FsyncCount())
	if last := w.LastLSN(); last != n {
		t.Fatalf("LastLSN = %d, want %d", last, n)
	}
	// 全部记录可完整重放
	var replayed int
	if err := w.Replay(func(e *WalEntry) { replayed++ }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != n {
		t.Fatalf("replayed = %d, want %d", replayed, n)
	}
}

func TestWALGroupCommitPersistAfterSyncUpTo(t *testing.T) {
	path, w := newTestWAL(t)
	w.SetGroupCommit(true)
	// 组提交模式：Append 只写不刷，SyncUpTo 兜底后才可重开恢复
	lsns := make([]storage.LSN, 8)
	for i := uint64(1); i <= 8; i++ {
		e := &WalEntry{TxnID: i, State: StateCommit,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}}}}
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
		lsns[i-1] = e.LSN
	}
	// 只 SyncUpTo 中间一条，leader 会覆盖到当前已写末尾，全部记录落盘
	if err := w.SyncUpTo(lsns[3]); err != nil {
		t.Fatalf("syncupto: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	var count int
	if err := w2.Replay(func(e *WalEntry) { count++ }); err != nil {
		t.Fatalf("replay after reopen: %v", err)
	}
	if count != 8 {
		t.Fatalf("replayed after reopen = %d, want 8", count)
	}
}

// TestWALGroupCommitDefaultWindow v1.14.0 P1：新建 WAL 默认开启组提交定时
// 窗口（500µs），保证稀疏事务场景默认即合并 fsync。
func TestWALGroupCommitDefaultWindow(t *testing.T) {
	_, w := newTestWAL(t)
	defer w.Close()
	wi := w.(*walImpl)
	if wi.groupCommitWindow != 500*time.Microsecond {
		t.Fatalf("default group commit window = %v, want 500µs", wi.groupCommitWindow)
	}
}

// TestSyncUpToGroupCommitWindowMergesSparse v1.14.0 P1：事务稀疏（相邻事务
// Append 间隔小于定时窗口）时，上一事务 SyncUpTo 的 leader 在窗口内等待，
// 下一事务 Append 落入窗口并刷新刷盘末尾，一次 fsync 覆盖多事务——避免
// 每事务一次 fsync。fsyncDelay 模拟盘延迟使 leader 占据窗口更稳定。
// 参数放大：Windows time.Sleep 实际粒度约 15ms，故取窗口 100ms > 间隔 15ms
// 保证稀疏事务确实落入 leader 窗口。
func TestSyncUpToGroupCommitWindowMergesSparse(t *testing.T) {
	_, w := newTestWAL(t)
	defer w.Close()
	w.SetGroupCommit(true)
	wi := w.(*walImpl)
	wi.SetFsyncDelay(5 * time.Millisecond)
	wi.SetGroupCommitWindow(100 * time.Millisecond)

	const n = 10
	var pending *WalEntry
	for i := 1; i <= n; i++ {
		// e 复用上一轮预 append 的记录（模拟另一连接已完成 WAL Append、
		// 尚未调用 SyncUpTo 的稀疏提交）。
		e := pending
		if e == nil {
			e = &WalEntry{TxnID: uint64(i), State: StateCommit,
				Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}}}}
			if err := w.Append(e); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		// 预写下一事务记录到缓冲：间隔 15ms < 窗口 100ms，将落入
		// 本事务 leader 的定时窗口并被并入本次 fsync。
		if i < n {
			time.Sleep(15 * time.Millisecond)
			next := &WalEntry{TxnID: uint64(i + 1), State: StateCommit,
				Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte(fmt.Sprintf("k%d", i+1)), Value: []byte("v")}}}}
			if err := w.Append(next); err != nil {
				t.Fatalf("append next %d: %v", i+1, err)
			}
			pending = next
		}
		if err := w.SyncUpTo(e.LSN); err != nil {
			t.Fatalf("syncupto %d (lsn=%d): %v", i, e.LSN, err)
		}
	}
	// 理想合并：每个 leader 窗口吞入一个后续 Append，fsync 次数约为 n/2；
	// 允许一次调度抖动，断言 <= 6（10 事务，未合并时为 10）。
	if got := wi.FsyncCount(); got > 6 {
		t.Fatalf("sparse group commit window fsync count = %d, want <= 6 for %d txns (window 100ms > spacing 15ms)", got, n)
	}
	t.Logf("sparse txns (window 100ms, spacing 15ms): %d appends -> %d fsyncs", n, wi.FsyncCount())
	var replayed int
	if err := w.Replay(func(e *WalEntry) { replayed++ }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != n {
		t.Fatalf("replayed = %d, want %d", replayed, n)
	}
}

// TestSyncUpToGroupCommitWindowDisabledBaseline v1.14.0 P1 开关回退：窗口设 0
// 关闭定时合并，稀疏事务下回退被动攒批（近似每事务一次 fsync），行为与
// v1.13.0 一致（此测试断言"不合并"，作为开启合并的对照基线）。
func TestSyncUpToGroupCommitWindowDisabledBaseline(t *testing.T) {
	_, w := newTestWAL(t)
	defer w.Close()
	w.SetGroupCommit(true)
	wi := w.(*walImpl)
	wi.SetGroupCommitWindow(0) // 关闭定时窗口

	const n = 5
	for i := uint64(1); i <= n; i++ {
		e := &WalEntry{TxnID: i, State: StateCommit,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}}}}
		if err := w.Append(e); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if err := w.SyncUpTo(e.LSN); err != nil {
			t.Fatalf("syncupto %d (lsn=%d): %v", i, e.LSN, err)
		}
		time.Sleep(1 * time.Millisecond) // 稀疏：无窗口时每次 SyncUpTo 均独立刷盘
	}
	// 被动攒批下 5 个稀疏事务应近似 5 次 fsync（允许调度抖动 1 次余量）。
	if got := wi.FsyncCount(); got < uint64(n-1) {
		t.Fatalf("window disabled baseline fsync count = %d, want >= %d (sparse unmerged)", got, n-1)
	}
	t.Logf("window disabled baseline: %d txns -> %d fsyncs", n, wi.FsyncCount())
}

// TestSyncUpToSingleWaiterSkipsWindow v1.16.0 C1 单等待者免窗口：无并发等待者
// 时 leader 跳过定时窗口直接刷盘，消除单写事务固定窗口延迟。
// 窗口设 100ms：旧实现（无条件 sleep）耗时 >= 100ms；免窗口路径仅一次
// fsync（+fsyncDelay 模拟盘延迟），总耗时显著小于窗口（Windows 调度粒度
// 约 15ms，故上限取 60ms 与窗口 100ms 拉开区分度）。
func TestSyncUpToSingleWaiterSkipsWindow(t *testing.T) {
	_, w := newTestWAL(t)
	defer w.Close()
	w.SetGroupCommit(true)
	wi := w.(*walImpl)
	wi.SetGroupCommitWindow(100 * time.Millisecond)
	wi.SetFsyncDelay(1 * time.Millisecond)

	e := &WalEntry{TxnID: 1, State: StateCommit,
		Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k1"), Value: []byte("v1")}}}}
	if err := w.Append(e); err != nil {
		t.Fatalf("append: %v", err)
	}
	start := time.Now()
	if err := w.SyncUpTo(e.LSN); err != nil {
		t.Fatalf("syncupto: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed >= 60*time.Millisecond {
		t.Fatalf("single waiter SyncUpTo took %v, want < 60ms (window 100ms should be skipped)", elapsed)
	}
	if got := wi.FsyncCount(); got != 1 {
		t.Fatalf("fsync count = %d, want 1", got)
	}
	if last := w.LastLSN(); last != e.LSN {
		t.Fatalf("LastLSN = %d, want %d", last, e.LSN)
	}
	t.Logf("single waiter SyncUpTo elapsed: %v (window 100ms skipped)", elapsed)
}

// TestWALBufferedAppendDefersFileWrite v1.16.0 C1 批量刷盘增强：组提交开启时
// Append 仅写入内存缓冲，文件大小不增长（不触发逐条文件写系统调用）；
// SyncUpTo 后一次性 flush 写入并 fsync，文件完整包含全部记录。
func TestWALBufferedAppendDefersFileWrite(t *testing.T) {
	path, w := newTestWAL(t)
	defer w.Close()
	wi := w.(*walImpl)
	wi.SetGroupCommitWindow(0) // 关闭定时窗口，聚焦缓冲行为

	const n = 5
	for i := uint64(1); i <= n; i++ {
		e := &WalEntry{TxnID: i, State: StateCommit,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}}}}
		if err := w.Append(e); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// 缓冲模式：记录尚未写文件，文件大小应仍为 headerSize
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Size() != int64(headerSize) {
		t.Fatalf("file size after buffered appends = %d, want %d (header only)", fi.Size(), headerSize)
	}
	// SyncUpTo 全部：flush 写入文件并 fsync
	if err := w.SyncUpTo(n); err != nil {
		t.Fatalf("syncupto: %v", err)
	}
	fi, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat after sync: %v", err)
	}
	if fi.Size() <= int64(headerSize) {
		t.Fatalf("file size after sync = %d, want > %d (records flushed)", fi.Size(), headerSize)
	}
	var replayed int
	if err := w.Replay(func(e *WalEntry) { replayed++ }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != n {
		t.Fatalf("replayed = %d, want %d", replayed, n)
	}
}

// TestWALBufferedClosePersistsWithoutSync v1.16.0 C1 持久化语义：缓冲模式下
// 未显式 SyncUpTo 的记录随 Close 冲刷落盘，reopen 可完整恢复（持久化语义
// 与旧版一致：Close 前所有 Append 均可达）。
func TestWALBufferedClosePersistsWithoutSync(t *testing.T) {
	path, w := newTestWAL(t)
	w.SetGroupCommit(true)
	const n = 4
	for i := uint64(1); i <= n; i++ {
		e := &WalEntry{TxnID: i, State: StateCommit,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}}}}
		if err := w.Append(e); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	var count int
	if err := w2.Replay(func(e *WalEntry) { count++ }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != n {
		t.Fatalf("replayed after reopen = %d, want %d", count, n)
	}
}

// TestWALGroupCommitOffPerAppendFsync v1.16.0 C1 开关回退：SetGroupCommit(false)
// 恢复旧版每记录写文件+fsync（返回 = 已持久化）；关闭前缓冲中残留记录先冲刷，
// 顺序语义不丢失。
func TestWALGroupCommitOffPerAppendFsync(t *testing.T) {
	_, w := newTestWAL(t)
	defer w.Close()
	wi := w.(*walImpl)
	w.SetGroupCommit(true)
	// 缓冲模式先积累一条记录，再关闭组提交：该记录应被冲刷（随后续 fsync 落盘）
	if err := w.Append(&WalEntry{TxnID: 1, State: StateCommit,
		Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k0"), Value: []byte("v0")}}}}); err != nil {
		t.Fatalf("append buffered: %v", err)
	}
	w.SetGroupCommit(false)
	// 关闭后每记录一次 fsync
	const n = 5
	for i := uint64(1); i <= n; i++ {
		e := &WalEntry{TxnID: i + 1, State: StateCommit,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}}}}
		if err := w.Append(e); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if got := wi.FsyncCount(); got != n {
		t.Fatalf("fsync count after off = %d, want %d (per-append)", got, n)
	}
	var replayed int
	if err := w.Replay(func(e *WalEntry) { replayed++ }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != n+1 {
		t.Fatalf("replayed = %d, want %d", replayed, n+1)
	}
}

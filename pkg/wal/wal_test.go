package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

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

func TestWALGroupCommitDefaultOff(t *testing.T) {
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
	// 默认模式：每记录一次 fsync（行为与旧版一致）
	if got := wi.FsyncCount(); got != 5 {
		t.Fatalf("default fsync count = %d, want 5 (per-append)", got)
	}
	// SyncUpTo 在默认模式下必须零额外 fsync（Append 已更新 lastSynced）
	if err := w.SyncUpTo(5); err != nil {
		t.Fatalf("syncupto: %v", err)
	}
	if got := wi.FsyncCount(); got != 5 {
		t.Fatalf("fsync count after SyncUpTo = %d, want 5", got)
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

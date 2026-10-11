package replication

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// BinlogReader 只读拉取：正常记录顺序读取 + LSN 过滤 + LastLSN。
func TestBinlogReaderScanFrom(t *testing.T) {
	path, s := mkBinlog(t)
	for i := 1; i <= 3; i++ {
		if err := s.Append(&BinlogEntry{CommitLSN: storage.LSN(100 + i), Batch: &storage.WriteBatch{
			Puts: []storage.KVPair{{Key: []byte("k" + string(rune('0'+i))), Value: []byte("v")}},
		}}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	r, err := OpenBinlogRead(path)
	if err != nil {
		t.Fatalf("OpenBinlogRead: %v", err)
	}
	defer r.Close()

	if got, err := r.LastLSN(); err != nil || got != 3 {
		t.Fatalf("LastLSN = %d err=%v, want 3", got, err)
	}
	// 从位点 2 起只读两条
	var got []uint64
	tail, err := r.ScanFrom(2, func(e *BinlogEntry) error {
		got = append(got, uint64(e.LSN))
		return nil
	})
	if err != nil {
		t.Fatalf("ScanFrom: %v", err)
	}
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("ScanFrom(2) got %v, want [2 3]", got)
	}
	if tail != 3 {
		t.Fatalf("tail = %d, want 3", tail)
	}
	// from 超出文件尾：不返回事件，tail 返回文件尾位点
	got = nil
	tail, err = r.ScanFrom(10, func(e *BinlogEntry) error { got = append(got, uint64(e.LSN)); return nil })
	if err != nil || len(got) != 0 {
		t.Fatalf("ScanFrom(10) = %v err=%v, want empty", got, err)
	}
	if tail != 3 {
		t.Fatalf("tail = %d, want 3 (file end)", tail)
	}
}

// 尾部半条记录（正在追加）视为正常结束，不报损坏。
func TestBinlogReaderToleratesPartialTail(t *testing.T) {
	path, s := mkBinlog(t)
	if err := s.Append(&BinlogEntry{Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k1"), Value: []byte("v1")}}}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	s.Close()

	// 手工在文件尾追加半条记录（模拟并发 Append 写入中间态）。
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("write partial tail: %v", err)
	}
	f.Close()

	r, err := OpenBinlogRead(path)
	if err != nil {
		t.Fatalf("OpenBinlogRead: %v", err)
	}
	defer r.Close()
	var got []uint64
	tail, err := r.ScanFrom(0, func(e *BinlogEntry) error { got = append(got, uint64(e.LSN)); return nil })
	if err != nil {
		t.Fatalf("ScanFrom with partial tail: %v", err)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("got %v, want [1]", got)
	}
	if tail != 1 {
		t.Fatalf("tail = %d, want 1", tail)
	}
	// 严格模式（decodeBinRecordOpt false，原 BinlogStore 语义）仍把半条视为损坏。
	rf, err := os.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rf.Close()
	if _, err := rf.Seek(int64(binHeaderSize), io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}
	// 第一条完整记录可正常解码
	if _, err := decodeBinRecordOpt(rf, false); err != nil {
		t.Fatalf("strict decode first record: %v", err)
	}
	// 半条（正在追加）在严格模式下视为损坏
	if _, err := decodeBinRecordOpt(rf, false); err != ErrCorrupt {
		t.Fatalf("strict decode tail err = %v, want ErrCorrupt", err)
	}
}

// 只读打开不会创建文件。
func TestBinlogReaderOpenMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nope.log")
	if _, err := OpenBinlogRead(path); !os.IsNotExist(err) {
		t.Fatalf("OpenBinlogRead missing err = %v, want os.ErrNotExist", err)
	}
}

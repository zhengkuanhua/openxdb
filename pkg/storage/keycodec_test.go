package storage

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEncodeRowKey(t *testing.T) {
	key := EncodeRowKey(7, []byte("user-1"), 99)
	wantLen := 1 + 8 + 1 + len("user-1") + 1 + 8
	if len(key) != wantLen {
		t.Fatalf("EncodeRowKey length = %d, want %d", len(key), wantLen)
	}
	if key[0] != rowKeyPrefix {
		t.Fatalf("prefix = %q, want 't'", key[0])
	}
	if got := binary.BigEndian.Uint64(key[1:9]); got != 7 {
		t.Fatalf("tableID = %d, want 7", got)
	}
	if key[9] != 'k' {
		t.Fatalf("pk marker = %q, want 'k'", key[9])
	}
	pk := key[10:16]
	if !bytes.Equal(pk, []byte("user-1")) {
		t.Fatalf("pk = %q, want user-1", pk)
	}
	if key[16] != 'v' {
		t.Fatalf("lsn marker = %q, want 'v'", key[16])
	}
	if got := binary.BigEndian.Uint64(key[17:25]); got != 99 {
		t.Fatalf("lsn = %d, want 99", got)
	}
}

func TestEncodeIndexKey(t *testing.T) {
	key := EncodeIndexKey(1, 2, []byte("idx-val"), []byte("pk-1"), 5)
	if key[0] != rowKeyPrefix {
		t.Fatalf("prefix = %q, want 't'", key[0])
	}
	if got := binary.BigEndian.Uint64(key[1:9]); got != 1 {
		t.Fatalf("tableID = %d, want 1", got)
	}
	if key[9] != indexKeyPrefix {
		t.Fatalf("index marker = %q, want 'i'", key[9])
	}
	if got := binary.BigEndian.Uint64(key[10:18]); got != 2 {
		t.Fatalf("indexID = %d, want 2", got)
	}
	idxVal := key[19:26]
	if !bytes.Equal(idxVal, []byte("idx-val")) {
		t.Fatalf("idxVal = %q, want idx-val", idxVal)
	}
	if key[26] != 'p' {
		t.Fatalf("pk marker = %q, want 'p'", key[26])
	}
	if got := binary.BigEndian.Uint64(key[len(key)-8:]); got != 5 {
		t.Fatalf("lsn = %d, want 5", got)
	}
}

// 同表同索引下，idxVal 字典序应与整体 Key 排序一致（二级索引范围扫描依赖）。
func TestIndexKeyOrderPreservesIdxVal(t *testing.T) {
	low := EncodeIndexKey(1, 1, []byte("a"), []byte("pk-1"), 1)
	high := EncodeIndexKey(1, 1, []byte("b"), []byte("pk-1"), 1)
	if bytes.Compare(low, high) >= 0 {
		t.Fatalf("index key order broken: a should sort before b")
	}
	// 不同表 ID 必须整体分隔。
	other := EncodeIndexKey(2, 1, []byte("a"), []byte("pk-1"), 1)
	if bytes.Compare(low, other) >= 0 {
		t.Fatalf("tableID prefix broken: table 1 should sort before table 2")
	}
}

func TestRowKeyOrder(t *testing.T) {
	a := EncodeRowKey(1, []byte("row-1"), 1)
	b := EncodeRowKey(1, []byte("row-2"), 1)
	if bytes.Compare(a, b) >= 0 {
		t.Fatalf("row key order broken")
	}
}

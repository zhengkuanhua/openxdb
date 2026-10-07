package rocksdb

import (
	"bytes"
	"testing"

	"github.com/openxdb/openxdb/pkg/storage"
)

func TestStorageCRUD(t *testing.T) {
	s, err := Open(t.TempDir(), true)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// 写
	if err := s.Write(&storage.WriteBatch{Puts: []storage.KVPair{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
	}}); err != nil {
		t.Fatalf("write: %v", err)
	}

	// 读
	v, err := s.Get([]byte("b"))
	if err != nil || string(v) != "2" {
		t.Fatalf("get b = %q, err %v; want \"2\"", v, err)
	}
	if _, err := s.Get([]byte("zz")); err != storage.ErrNotFound {
		t.Fatalf("get missing key err = %v, want ErrNotFound", err)
	}

	// 更新
	if err := s.Write(&storage.WriteBatch{Puts: []storage.KVPair{
		{Key: []byte("b"), Value: []byte("22")},
	}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	v, _ = s.Get([]byte("b"))
	if string(v) != "22" {
		t.Fatalf("get b after update = %q, want 22", v)
	}

	// 范围扫描
	kvs, err := s.Scan(storage.KeyRange{Start: []byte("a"), End: []byte("d")}, 0)
	if err != nil || len(kvs) != 3 {
		t.Fatalf("scan = %v, err %v; want 3 items", kvs, err)
	}
	if !bytes.Equal(kvs[0].Key, []byte("a")) || !bytes.Equal(kvs[2].Key, []byte("c")) {
		t.Fatalf("scan order wrong: %v", kvs)
	}

	// limit
	kvs, _ = s.Scan(storage.KeyRange{Start: []byte("a")}, 2)
	if len(kvs) != 2 {
		t.Fatalf("scan limit = %d, want 2", len(kvs))
	}

	// 删除（批量含 put+delete 原子性）
	if err := s.Write(&storage.WriteBatch{
		Deletes: [][]byte{[]byte("a")},
		Puts:    []storage.KVPair{{Key: []byte("d"), Value: []byte("4")}},
	}); err != nil {
		t.Fatalf("batch delete+put: %v", err)
	}
	if _, err := s.Get([]byte("a")); err != storage.ErrNotFound {
		t.Fatalf("a should be deleted, err = %v", err)
	}
	if v, _ := s.Get([]byte("d")); string(v) != "4" {
		t.Fatalf("d = %q, want 4", v)
	}
}

func TestSnapshotConsistency(t *testing.T) {
	s, err := Open(t.TempDir(), true)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if err := s.Write(&storage.WriteBatch{Puts: []storage.KVPair{
		{Key: []byte("x"), Value: []byte("1")},
	}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Release()

	// 快照建立后继续写入
	if err := s.Write(&storage.WriteBatch{Puts: []storage.KVPair{
		{Key: []byte("y"), Value: []byte("2")},
	}}); err != nil {
		t.Fatalf("write y: %v", err)
	}

	// 快照应仍只见 x
	if v, err := snap.Get([]byte("x")); err != nil || string(v) != "1" {
		t.Fatalf("snapshot get x = %q, err %v", v, err)
	}
	if _, err := snap.Get([]byte("y")); err != storage.ErrNotFound {
		t.Fatalf("snapshot should not see y, err = %v", err)
	}
	// 实时库能看到 y
	if v, err := s.Get([]byte("y")); err != nil || string(v) != "2" {
		t.Fatalf("live get y = %q, err %v", v, err)
	}
}

func TestKeyEncodingEndToEnd(t *testing.T) {
	s, err := Open(t.TempDir(), true)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	rowKey := storage.EncodeRowKey(1, []byte("id-100"), 10)
	idxKey := storage.EncodeIndexKey(1, 1, []byte("idx-val"), []byte("id-100"), 10)
	if err := s.Write(&storage.WriteBatch{Puts: []storage.KVPair{
		{Key: rowKey, Value: []byte("row-data")},
		{Key: idxKey, Value: []byte("")},
	}}); err != nil {
		t.Fatalf("write encoded keys: %v", err)
	}
	if v, err := s.Get(rowKey); err != nil || string(v) != "row-data" {
		t.Fatalf("get row key = %q, err %v", v, err)
	}
	// 索引范围扫描：前缀 t{1} i{1} k 之下应命中 idxKey
	kvs, err := s.Scan(storage.KeyRange{
		Start: []byte{'t', 0, 0, 0, 0, 0, 0, 0, 1, 'i', 0, 0, 0, 0, 0, 0, 0, 1, 'k'},
		End:   []byte{'t', 0, 0, 0, 0, 0, 0, 0, 1, 'i', 0, 0, 0, 0, 0, 0, 0, 1, 'l'},
	}, 0)
	if err != nil || len(kvs) != 1 {
		t.Fatalf("index prefix scan = %v, err %v; want 1 item", kvs, err)
	}
}

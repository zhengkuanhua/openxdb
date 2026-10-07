package btree_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/openxdb/openxdb/pkg/storage"
	"github.com/openxdb/openxdb/pkg/storage/btree"
)

func key(i int) []byte { return []byte(fmt.Sprintf("%08d", i)) }

// TestPutGet 基础读写。
func TestPutGet(t *testing.T) {
	kv := btree.NewKV(8)
	for i := 0; i < 1000; i++ {
		if err := kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: key(i), Value: []byte(fmt.Sprintf("v%d", i))}}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 1000; i++ {
		v, err := kv.Get(key(i))
		if err != nil || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("get %d: v=%q err=%v", i, v, err)
		}
	}
	if _, err := kv.Get(key(100000)); err != storage.ErrNotFound {
		t.Fatalf("missing key: want ErrNotFound, got %v", err)
	}
}

// TestOverwrite 覆盖写。
func TestOverwrite(t *testing.T) {
	kv := btree.NewKV(8)
	if err := kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: key(1), Value: []byte("a")}}}); err != nil {
		t.Fatal(err)
	}
	if err := kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: key(1), Value: []byte("b")}}}); err != nil {
		t.Fatal(err)
	}
	v, _ := kv.Get(key(1))
	if string(v) != "b" {
		t.Fatalf("want b, got %q", v)
	}
}

// TestOrderedScan 范围扫描按序输出且区间正确。
func TestOrderedScan(t *testing.T) {
	kv := btree.NewKV(8)
	var batch storage.WriteBatch
	for i := 100; i < 200; i++ {
		batch.Puts = append(batch.Puts, storage.KVPair{Key: key(i), Value: []byte("x")})
	}
	if err := kv.Write(&batch); err != nil {
		t.Fatal(err)
	}
	// 乱序写入后扫描仍应有序
	batch.Puts = nil
	for i := 0; i < 100; i++ {
		batch.Puts = append(batch.Puts, storage.KVPair{Key: key(i), Value: []byte("x")})
	}
	if err := kv.Write(&batch); err != nil {
		t.Fatal(err)
	}
	pairs, err := kv.Scan(storage.KeyRange{Start: key(50), End: key(120)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 70 {
		t.Fatalf("want 70 pairs, got %d", len(pairs))
	}
	for i, p := range pairs {
		want := key(50 + i)
		if !bytes.Equal(p.Key, want) {
			t.Fatalf("pair %d: want %s got %s", i, want, p.Key)
		}
	}
	// 无限区间
	pairs, _ = kv.Scan(storage.KeyRange{Start: key(198)}, 0)
	if len(pairs) != 2 {
		t.Fatalf("open end: want 2, got %d", len(pairs))
	}
	// limit
	pairs, _ = kv.Scan(storage.KeyRange{Start: key(0)}, 5)
	if len(pairs) != 5 {
		t.Fatalf("limit: want 5, got %d", len(pairs))
	}
}

// TestDelete 删除与借/合并路径。
func TestDelete(t *testing.T) {
	kv := btree.NewKV(4) // 小 order 强制频繁分裂/借/合并
	for i := 0; i < 500; i++ {
		if err := kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: key(i), Value: []byte("v")}}}); err != nil {
			t.Fatal(err)
		}
	}
	// 删除全部
	for i := 0; i < 500; i++ {
		if err := kv.Write(&storage.WriteBatch{Deletes: [][]byte{key(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	pairs, _ := kv.Scan(storage.KeyRange{}, 0)
	if len(pairs) != 0 {
		t.Fatalf("after delete all: want 0, got %d", len(pairs))
	}
	if _, err := kv.Get(key(0)); err != storage.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	// 删除后重写
	if err := kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: key(0), Value: []byte("again")}}}); err != nil {
		t.Fatal(err)
	}
	if v, _ := kv.Get(key(0)); string(v) != "again" {
		t.Fatalf("rewrite: got %q", v)
	}
}

// TestRandomOps 随机写删读对照（伪随机确定性）。
func TestRandomOps(t *testing.T) {
	kv := btree.NewKV(16)
	rng := rand.New(rand.NewSource(42))
	ref := map[string]string{}
	for step := 0; step < 5000; step++ {
		k := key(rng.Intn(2000))
		switch rng.Intn(3) {
		case 0, 1:
			v := fmt.Sprintf("v%d", rng.Intn(1000))
			_ = kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: k, Value: []byte(v)}}})
			ref[string(k)] = v
		case 2:
			_ = kv.Write(&storage.WriteBatch{Deletes: [][]byte{k}})
			delete(ref, string(k))
		}
	}
	for k, v := range ref {
		got, err := kv.Get([]byte(k))
		if err != nil || string(got) != v {
			t.Fatalf("key %s: want %s got %q err %v", k, v, got, err)
		}
	}
	// 全量扫描与引用集合一致
	pairs, _ := kv.Scan(storage.KeyRange{}, 0)
	if len(pairs) != len(ref) {
		t.Fatalf("scan count: want %d got %d", len(ref), len(pairs))
	}
}

// TestSnapshot 快照隔离：写后快照外不可见。
func TestSnapshot(t *testing.T) {
	kv := btree.NewKV(8)
	_ = kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: key(1), Value: []byte("a")}}})
	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()
	_ = kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: key(1), Value: []byte("b")}, {Key: key(2), Value: []byte("c")}}})
	if v, _ := snap.Get(key(1)); string(v) != "a" {
		t.Fatalf("snapshot should see old value, got %q", v)
	}
	if _, err := snap.Get(key(2)); err != storage.ErrNotFound {
		t.Fatalf("snapshot should not see new key, err=%v", err)
	}
	if v, _ := kv.Get(key(1)); string(v) != "b" {
		t.Fatalf("live should see new value, got %q", v)
	}
}

// TestWriteBatchAtomic 批处理（Puts+Deletes 混合）。
func TestWriteBatchAtomic(t *testing.T) {
	kv := btree.NewKV(8)
	_ = kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: key(1), Value: []byte("x")}, {Key: key(2), Value: []byte("y")}}})
	if err := kv.Write(&storage.WriteBatch{
		Puts:    []storage.KVPair{{Key: key(3), Value: []byte("z")}},
		Deletes: [][]byte{key(1)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Get(key(1)); err != storage.ErrNotFound {
		t.Fatalf("key1 should be deleted, err=%v", err)
	}
	if v, _ := kv.Get(key(3)); string(v) != "z" {
		t.Fatalf("key3: got %q", v)
	}
}

// TestOrderInvariant 随机写后扫描仍严格有序。
func TestOrderInvariant(t *testing.T) {
	kv := btree.NewKV(8)
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 3000; i++ {
		k := []byte(fmt.Sprintf("%06d", rng.Intn(5000)))
		_ = kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: k, Value: []byte("v")}}})
		if rng.Intn(4) == 0 {
			_ = kv.Write(&storage.WriteBatch{Deletes: [][]byte{k}})
		}
	}
	pairs, _ := kv.Scan(storage.KeyRange{}, 0)
	for i := 1; i < len(pairs); i++ {
		if bytes.Compare(pairs[i-1].Key, pairs[i].Key) >= 0 {
			t.Fatalf("order violated at %d: %s >= %s", i, pairs[i-1].Key, pairs[i].Key)
		}
	}
	// 参考集合抽查
	var keys [][]byte
	for _, p := range pairs {
		keys = append(keys, p.Key)
	}
	if !sort.SliceIsSorted(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 }) {
		t.Fatal("keys not sorted")
	}
}

package cdc

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// 构造物理键：region 前缀 + 行键 's'{tableID}{pk}。
func rowKey(rid storage.ID, tableID uint64, pk []byte) []byte {
	inner := append([]byte{logicKeyRow}, make([]byte, 8)...)
	binary.BigEndian.PutUint64(inner[1:9], tableID)
	inner = append(inner, pk...)
	return sharding.EncodeRegionKey(rid, inner)
}

// 构造物理键：region 前缀 + 索引键 'i'{tableID}{indexID}{val}p{pk}。
func indexKey(rid storage.ID, tableID, indexID uint64, val, pk []byte) []byte {
	inner := append([]byte{logicKeyIndex}, make([]byte, 8)...)
	binary.BigEndian.PutUint64(inner[1:9], tableID)
	inner = append(inner, make([]byte, 8)...)
	binary.BigEndian.PutUint64(inner[9:17], indexID)
	inner = append(inner, val...)
	inner = append(inner, 'p')
	inner = append(inner, pk...)
	return sharding.EncodeRegionKey(rid, inner)
}

func be8(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// Decode：行 put/delete、索引键、meta 跳过、region 前缀剥离。
func TestDecodeEvents(t *testing.T) {
	entry := &replication.BinlogEntry{
		LSN: 7, CommitLSN: 6, SchemaVer: 3,
		Batch: &storage.WriteBatch{
			Puts: []storage.KVPair{
				{Key: rowKey(1, 42, []byte("pk1")), Value: []byte(`{"id":1,"name":"alice"}`)},
				{Key: indexKey(1, 42, 5, be8(9), be8(77)), Value: []byte{}},
				{Key: []byte("m:tables"), Value: []byte(`{"id":42}`)}, // 元数据键：跳过
				{Key: []byte{0x01, 0x02}, Value: []byte("x")},         // 无法识别：跳过
			},
			Deletes: [][]byte{
				rowKey(2, 99, []byte("pk2")),
				[]byte("m:regions"), // 元数据键：跳过
			},
		},
	}
	evs := Decode(entry)
	if len(evs) != 3 {
		t.Fatalf("Decode got %d events, want 3: %+v", len(evs), evs)
	}	// 事件 0：行 PUT
	ev := evs[0]
	if ev.Kind != KindPut || ev.TableID != 42 || string(ev.PK) != "pk1" || ev.IsIndex {
		t.Fatalf("ev0 = %+v, want row put table 42 pk pk1", ev)
	}
	if ev.LSN != 7 || ev.CommitLSN != 6 || ev.SchemaVer != 3 {
		t.Fatalf("ev0 meta = %d/%d/%d, want 7/6/3", ev.LSN, ev.CommitLSN, ev.SchemaVer)
	}
	if string(ev.RawValue) != `{"id":1,"name":"alice"}` {
		t.Fatalf("ev0 raw value = %q", ev.RawValue)
	}
	// 事件 1：索引键（IsIndex，PK 取 'p' 后主键后缀）
	if evs[1].Kind != KindPut || !evs[1].IsIndex || evs[1].TableID != 42 || string(evs[1].PK) != string(be8(77)) {
		t.Fatalf("ev1 = %+v, want index put table 42 pk %x", evs[1], be8(77))
	}
	// 事件 2：行 DELETE（table 99 pk pk2）
	ev = evs[2]
	if ev.Kind != KindDelete || ev.TableID != 99 || string(ev.PK) != "pk2" || ev.IsIndex {
		t.Fatalf("ev2 = %+v, want row delete table 99 pk pk2", ev)
	}
	if len(ev.RawValue) != 0 {
		t.Fatalf("delete RawValue should be empty, got %q", ev.RawValue)
	}
}

// Decode 对 nil / 空记录返回 nil。
func TestDecodeNil(t *testing.T) {
	if evs := Decode(nil); evs != nil {
		t.Fatalf("Decode(nil) = %v, want nil", evs)
	}
	if evs := Decode(&replication.BinlogEntry{LSN: 1, Batch: &storage.WriteBatch{}}); len(evs) != 0 {
		t.Fatalf("Decode(empty) = %v, want empty", evs)
	}
}

// mkBinlog 写路径模拟：BinlogStore.Append（同主库提交落盘）。
func mkBinlog(t *testing.T) (string, replication.BinlogStore) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "binlog.log")
	s, err := replication.OpenBinlog(path)
	if err != nil {
		t.Fatalf("OpenBinlog: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return path, s
}

// Subscriber 拉取：全量、limit 分页续读（无重复无遗漏）、尾部半条容忍。
func TestSubscriberFetch(t *testing.T) {
	path, s := mkBinlog(t)
	// 记录 1：单行 put
	append1 := func() {
		if err := s.Append(&replication.BinlogEntry{CommitLSN: 1, Batch: &storage.WriteBatch{
			Puts: []storage.KVPair{{Key: rowKey(1, 10, be8(1)), Value: []byte(`{"id":1}`)}},
		}}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	// 记录 2：行 put + 索引 put + 行 delete（一事务多变更）
	append2 := func() {
		if err := s.Append(&replication.BinlogEntry{CommitLSN: 2, Batch: &storage.WriteBatch{
			Puts:    []storage.KVPair{{Key: rowKey(1, 10, be8(2)), Value: []byte(`{"id":2}`)}, {Key: indexKey(1, 10, 5, be8(2), be8(2)), Value: []byte{}}},
			Deletes: [][]byte{rowKey(1, 10, be8(1))},
		}}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	append1()
	append2()

	sub, err := NewSubscriber(path)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer sub.Close()

	// 全量拉取：2 条记录 → 4 条事件（put；put+index；delete）
	evs, next, err := sub.Fetch(0, 0)
	if err != nil {
		t.Fatalf("Fetch(0,0): %v", err)
	}
	if len(evs) != 4 {
		t.Fatalf("Fetch all got %d events, want 4: %+v", len(evs), evs)
	}
	if next != 3 {
		t.Fatalf("next = %d, want 3 (file end +1)", next)
	}
	// 无新记录续读：返回空事件且位点不倒退
	evs, next2, err := sub.Fetch(next, 0)
	if err != nil || len(evs) != 0 || next2 != next {
		t.Fatalf("Fetch(3) = %d events next=%d err=%v, want 0/%d", len(evs), next2, err, next)
	}
	// from 超前：位点保持 from 不倒退
	_, next3, _ := sub.Fetch(100, 0)
	if next3 != 100 {
		t.Fatalf("Fetch(100) next = %d, want 100", next3)
	}
}

// limit 分页：分批拉取无重复无遗漏（LSN 续读语义）。
func TestSubscriberFetchLimitPaging(t *testing.T) {
	path, s := mkBinlog(t)
	for i := uint64(1); i <= 5; i++ {
		if err := s.Append(&replication.BinlogEntry{CommitLSN: storage.LSN(i), Batch: &storage.WriteBatch{
			Puts: []storage.KVPair{{Key: rowKey(1, 20, be8(i)), Value: be8(i)}},
		}}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	sub, err := NewSubscriber(path)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer sub.Close()

	var seen []storage.LSN
	from := storage.LSN(0)
	for i := 0; i < 5; i++ {
		evs, next, err := sub.Fetch(from, 1) // 每次最多 1 条
		if err != nil {
			t.Fatalf("Fetch(%d): %v", from, err)
		}
		if len(evs) != 1 {
			t.Fatalf("Fetch(%d) got %d events, want 1", from, len(evs))
		}
		seen = append(seen, evs[0].LSN)
		from = next
	}
	for i, lsn := range seen {
		if lsn != storage.LSN(i+1) {
			t.Fatalf("seen[%d] = %d, want %d", i, lsn, i+1)
		}
	}
	// 第 6 次：文件已消费完
	evs, next, err := sub.Fetch(from, 1)
	if err != nil || len(evs) != 0 {
		t.Fatalf("Fetch after drain = %d events err=%v, want 0", len(evs), err)
	}
	if next != 6 {
		t.Fatalf("next after drain = %d, want 6", next)
	}
}

// 并发写期间拉取：尾部半条被容忍，写入完成后可继续读到。
func TestSubscriberFetchConcurrentAppend(t *testing.T) {
	path, s := mkBinlog(t)
	if err := s.Append(&replication.BinlogEntry{CommitLSN: 1, Batch: &storage.WriteBatch{
		Puts: []storage.KVPair{{Key: rowKey(1, 30, be8(1)), Value: be8(1)}},
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// 手工模拟"正在追加的半条"
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.Write([]byte{0xAA, 0xBB}); err != nil {
		t.Fatalf("partial tail: %v", err)
	}
	f.Close()

	sub, err := NewSubscriber(path)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer sub.Close()
	evs, next, err := sub.Fetch(0, 0)
	if err != nil {
		t.Fatalf("Fetch with partial tail: %v", err)
	}
	if len(evs) != 1 || evs[0].LSN != 1 {
		t.Fatalf("got %d events first=%v, want 1 event lsn 1", len(evs), evs)
	}
	if next != 2 {
		t.Fatalf("next = %d, want 2", next)
	}
}

// 订阅会话关闭后 Fetch 报 ErrClosed。
func TestSubscriberClose(t *testing.T) {
	path, s := mkBinlog(t)
	_ = s.Append(&replication.BinlogEntry{Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: rowKey(1, 1, be8(1)), Value: be8(1)}}}})
	sub, err := NewSubscriber(path)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := sub.Fetch(0, 0); err != replication.ErrClosed {
		t.Fatalf("Fetch after Close err = %v, want ErrClosed", err)
	}
}

// FileOffsetStore：初始 0、Save/Load 往返、重开文件恢复（模拟客户端重启）。
func TestFileOffsetStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.txt")
	st := NewFileOffsetStore(path)
	if got, err := st.Load(); err != nil || got != 0 {
		t.Fatalf("Load initial = %d err=%v, want 0", got, err)
	}
	if err := st.Save(42); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// 重启后新实例续读
	st2 := NewFileOffsetStore(path)
	if got, err := st2.Load(); err != nil || got != 42 {
		t.Fatalf("Load after restart = %d err=%v, want 42", got, err)
	}
	// 位点推进
	if err := st2.Save(43); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got, _ := st.Load(); got != 43 {
		t.Fatalf("Load = %d, want 43", got)
	}
}

// 完整消费循环：Fetch → Save(next) → 新订阅者续读（模拟进程重启恢复位点）。
func TestOffsetRestartResume(t *testing.T) {
	path, s := mkBinlog(t)
	for i := uint64(1); i <= 3; i++ {
		if err := s.Append(&replication.BinlogEntry{CommitLSN: storage.LSN(i), Batch: &storage.WriteBatch{
			Puts: []storage.KVPair{{Key: rowKey(1, 40, be8(i)), Value: be8(i)}},
		}}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	offsetPath := filepath.Join(t.TempDir(), "off.txt")
	st := NewFileOffsetStore(offsetPath)

	consume := func() []storage.LSN {
		sub, err := NewSubscriber(path)
		if err != nil {
			t.Fatalf("NewSubscriber: %v", err)
		}
		defer sub.Close()
		from, err := st.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		var lsns []storage.LSN
		for i := 0; i < 2; i++ {
			evs, next, err := sub.Fetch(from, 0)
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			for _, ev := range evs {
				lsns = append(lsns, ev.LSN)
			}
			from = next
			if err := st.Save(next); err != nil {
				t.Fatalf("Save: %v", err)
			}
			if len(evs) == 0 {
				break
			}
		}
		return lsns
	}
	// 第一轮：全量消费 1..3
	got := consume()
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("first consume = %v, want [1 2 3]", got)
	}
	// 追加新记录后"重启"消费：只读到新记录 4
	if err := s.Append(&replication.BinlogEntry{CommitLSN: 4, Batch: &storage.WriteBatch{
		Puts: []storage.KVPair{{Key: rowKey(1, 40, be8(4)), Value: be8(4)}},
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got = consume()
	if len(got) != 1 || got[0] != 4 {
		t.Fatalf("resume consume = %v, want [4]", got)
	}
}

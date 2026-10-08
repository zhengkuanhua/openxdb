package replication

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

func newBtree() storage.Storage { return btree.NewKV(32) }

func mkBinlog(t *testing.T) (string, BinlogStore) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "binlog.log")
	s, err := OpenBinlog(path)
	if err != nil {
		t.Fatalf("OpenBinlog: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return path, s
}

func waitApplied(t *testing.T, f *Follower, want storage.LSN) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.AppliedLSN() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("applied lsn = %d, want >= %d", f.AppliedLSN(), want)
}

func waitSlave(t *testing.T, m *MasterReplicator, online bool, ack storage.LSN) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ss := m.SlaveStatus()
		if len(ss) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ok := true
		for _, s := range ss {
			if s.Online != online {
				ok = false
			}
			if ack > 0 && s.LastAck < ack {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("slave status not reached online=%v ack=%d, got %+v", online, ack, m.SlaveStatus())
}

// ---- Binlog 落盘与回读 ----

func TestBinlogAppendReadBack(t *testing.T) {
	path, s := mkBinlog(t)
	entries := []*BinlogEntry{
		{LSN: 0, CommitLSN: 10, Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k1"), Value: []byte("v1")}}}},
		{LSN: 0, CommitLSN: 0, Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k2"), Value: []byte("v2")}}, Deletes: [][]byte{[]byte("d1")}}},
	}
	for _, e := range entries {
		if err := s.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if got := s.LastLSN(); got != 2 {
		t.Fatalf("LastLSN = %d, want 2", got)
	}
	var got []*BinlogEntry
	if err := s.ReadFrom(0, func(e *BinlogEntry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("read %d entries, want 2", len(got))
	}
	if got[0].LSN != 1 || got[0].CommitLSN != 10 {
		t.Fatalf("entry0 lsn/commitlsn = %d/%d, want 1/10", got[0].LSN, got[0].CommitLSN)
	}
	if string(got[0].Batch.Puts[0].Key) != "k1" || string(got[0].Batch.Puts[0].Value) != "v1" {
		t.Fatalf("entry0 put mismatch")
	}
	if got[1].LSN != 2 || len(got[1].Batch.Deletes) != 1 || string(got[1].Batch.Deletes[0]) != "d1" {
		t.Fatalf("entry1 mismatch: %+v", got[1])
	}
	// 从位点 2 起只读最后一条
	got = nil
	if err := s.ReadFrom(2, func(e *BinlogEntry) error { got = append(got, e); return nil }); err != nil {
		t.Fatalf("ReadFrom(2): %v", err)
	}
	if len(got) != 1 || got[0].LSN != 2 {
		t.Fatalf("ReadFrom(2) got %d entries, want only lsn 2", len(got))
	}
	// 磁盘文件确实存在
	if fi, err := os.Stat(path); err != nil || fi.Size() == 0 {
		t.Fatalf("binlog file not persisted: %v", err)
	}
}

func TestBinlogReopenPersistsLSN(t *testing.T) {
	path, s := mkBinlog(t)
	if err := s.Append(&BinlogEntry{Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("a"), Value: []byte("b")}}}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := OpenBinlog(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got := s2.LastLSN(); got != 1 {
		t.Fatalf("after reopen LastLSN = %d, want 1", got)
	}
	var n int
	if err := s2.ReadFrom(0, func(e *BinlogEntry) error { n++; return nil }); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if n != 1 {
		t.Fatalf("replayed %d entries, want 1", n)
	}
}

func TestBinlogCorruptDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binlog.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("NOTABINLOGXX")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := OpenBinlog(path); err != ErrCorrupt {
		t.Fatalf("OpenBinlog corrupt = %v, want ErrCorrupt", err)
	}
}

// ---- Follower 幂等应用 ----

func TestFollowerApplyIdempotentAndGap(t *testing.T) {
	f := NewFollower(newBtree())
	e1 := &BinlogEntry{LSN: 1, Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("k"), Value: []byte("v")}}}}
	if err := f.ApplyBinlog(e1); err != nil {
		t.Fatalf("apply lsn1: %v", err)
	}
	// 相同位点重放：幂等跳过
	if err := f.ApplyBinlog(e1); err != nil {
		t.Fatalf("re-apply lsn1: %v", err)
	}
	if got := f.AppliedLSN(); got != 1 {
		t.Fatalf("AppliedLSN = %d, want 1", got)
	}
	// 跳号拒绝
	if err := f.ApplyBinlog(&BinlogEntry{LSN: 3, Batch: &storage.WriteBatch{}}); err == nil {
		t.Fatalf("gap apply should fail")
	}
	// 正常连续
	if err := f.ApplyBinlog(&BinlogEntry{LSN: 2, Batch: &storage.WriteBatch{}}); err != nil {
		t.Fatalf("apply lsn2: %v", err)
	}
	if got := f.AppliedLSN(); got != 2 {
		t.Fatalf("AppliedLSN = %d, want 2", got)
	}
}

// ---- 主从全链路 ----

func newMaster(t *testing.T) *MasterReplicator {
	t.Helper()
	path := filepath.Join(t.TempDir(), "binlog.log")
	m, err := NewMaster(path, ":0")
	if err != nil {
		t.Fatalf("NewMaster: %v", err)
	}
	if err := m.Serve(); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func TestMasterFollowerStream(t *testing.T) {
	m := newMaster(t)
	fst := newBtree()
	f := NewFollower(fst)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Start(ctx, m.Addr())

	vals := []string{"v1", "v2", "v3"}
	for i := 1; i <= 3; i++ {
		key := []byte{byte('a' + i)}
		batch := &storage.WriteBatch{Puts: []storage.KVPair{{Key: key, Value: []byte(vals[i-1])}}}
		if err := m.Append(&BinlogEntry{Batch: batch}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	waitApplied(t, f, 3)
	// 从端存储数据一致
	for i := 1; i <= 3; i++ {
		key := []byte{byte('a' + i)}
		got, err := fst.Get(key)
		if err != nil || string(got) != vals[i-1] {
			t.Fatalf("follower key %q = %q err=%v, want %s", key, got, err, vals[i-1])
		}
	}
	// master 侧位点与 ack 推进
	if got := m.LastLSN(); got != 3 {
		t.Fatalf("master LastLSN = %d, want 3", got)
	}
	waitSlave(t, m, true, 3)
}

func TestMasterResumeAfterReconnect(t *testing.T) {
	m := newMaster(t)
	if err := m.Append(&BinlogEntry{Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("a"), Value: []byte("1")}}}}); err != nil {
		t.Fatal(err)
	}
	if err := m.Append(&BinlogEntry{Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("b"), Value: []byte("2")}}}}); err != nil {
		t.Fatal(err)
	}

	f := NewFollower(newBtree())
	ctx1, cancel1 := context.WithCancel(context.Background())
	go f.Start(ctx1, m.Addr())
	waitApplied(t, f, 2)
	// 模拟断线：停止第一次会话
	_ = f.Stop()
	cancel1()
	time.Sleep(50 * time.Millisecond)

	// 主端继续产生条目
	if err := m.Append(&BinlogEntry{Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("c"), Value: []byte("3")}}}}); err != nil {
		t.Fatal(err)
	}
	// 同一 follower 重连：从 AppliedLSN+1=3 续传
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go f.Start(ctx2, m.Addr())
	waitApplied(t, f, 3)
}

// ---- 心跳与离线 ----

// 裸客户端只握手、不回任何帧，验证 master 心跳超时判离线。
func TestMasterHeartbeatOffline(t *testing.T) {
	m := newMaster(t)
	m.HeartbeatInterval = 50 * time.Millisecond
	m.HeartbeatTimeout = 200 * time.Millisecond

	conn, err := net.Dial("tcp", m.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := writeFrame(conn, msgHello, encodeHello(RoleSlave, 0)); err != nil {
		t.Fatal(err)
	}
	typ, _, err := readFrame(conn)
	if err != nil || typ != msgHelloOK {
		t.Fatalf("handshake failed: typ=%d err=%v", typ, err)
	}
	// 保持连接但不再发送任何帧
	waitSlave(t, m, false, 0)
}

// 主端不发任何帧时，follower 读超时标记 offline。
func TestFollowerHeartbeatTimeout(t *testing.T) {
	m := newMaster(t)
	m.HeartbeatInterval = time.Hour // 空闲不主动发 PING，避免干扰
	m.HeartbeatTimeout = time.Hour

	f := NewFollower(newBtree())
	f.ReadTimeout = 200 * time.Millisecond
	started := make(chan error, 1)
	go func() { started <- f.Start(context.Background(), m.Addr()) }()

	select {
	case err := <-started:
		if err == nil {
			t.Fatalf("Start should return timeout error")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Start did not return on heartbeat timeout")
	}
	st := f.Status()
	if st.State != "offline" || st.Connected {
		t.Fatalf("status = %+v, want offline/disconnected", st)
	}
	if st.LastError == "" {
		t.Fatalf("LastError empty, want heartbeat timeout info")
	}
}

package cluster

// v1.20.0 C5：分区快照采样与 stale-while-refresh 判定专项测试。

import (
	"sync"
	"testing"
	"time"
)

// TestPartitionSnapshotSampling 快照采样与 TTL 复用。
func TestPartitionSnapshotSampling(t *testing.T) {
	m := NewManager("n1")
	m.RegisterPeer("n2", "127.0.0.1:1")
	m.RegisterPeer("n3", "127.0.0.1:2")

	var mu sync.Mutex
	calls := 0
	m.SetProber(func(string) bool {
		mu.Lock()
		calls++
		mu.Unlock()
		return true
	})

	if _, ok := m.PartitionSnapshot(); ok {
		t.Fatalf("snapshot must be absent before first sampling")
	}

	minority, known := m.PartitionMinority(PartitionSnapshotTTL)
	if !known || minority {
		t.Fatalf("3/3 reachable should be majority: minority=%v known=%v", minority, known)
	}
	mu.Lock()
	first := calls
	mu.Unlock()
	if first != 2 {
		t.Fatalf("first sampling should probe both peers, calls=%d", first)
	}

	// TTL 内复用快照：不再探活。
	if _, _ = m.PartitionMinority(PartitionSnapshotTTL); true {
		mu.Lock()
		if calls != first {
			mu.Unlock()
			t.Fatalf("fresh snapshot must not re-probe: calls=%d want=%d", calls, first)
		}
		mu.Unlock()
	}

	// 快照过期（白盒把采样时间回拨一小时）→ 必须重新采样。
	m.snapMu.Lock()
	m.snapAt = time.Now().Add(-time.Hour)
	m.snapMu.Unlock()
	if _, _ = m.PartitionMinority(PartitionSnapshotTTL); true {
		mu.Lock()
		if calls == first {
			mu.Unlock()
			t.Fatalf("expired snapshot must re-sample")
		}
		mu.Unlock()
	}

	ps, ok := m.PartitionSnapshot()
	// Total 为注册表 nodes 条目数（本用例注册了 2 个远端）；Reachable 含本机恒 1。
	if !ok || ps.Total != 2 || ps.Reachable != 3 || !ps.MajorityOK {
		t.Fatalf("snapshot want total=2 reachable=3 majority=true, got %+v ok=%v", ps, ok)
	}
	if age := m.PartitionSnapshotAge(); age < 0 {
		t.Fatalf("snapshot age must be measured, got %v", age)
	}
}

// TestPartitionMinorityUnknownBeforeSampling 未采样且刷新被占用时返回 unknown。
func TestPartitionMinorityUnknownBeforeSampling(t *testing.T) {
	m := NewManager("n1")
	m.RegisterPeer("n2", "127.0.0.1:1")

	// 模拟另一调用者正在采样（刚启动、无任何快照）。
	m.snapMu.Lock()
	m.snapBusy = true
	m.snapMu.Unlock()

	minority, known := m.PartitionMinority(PartitionSnapshotTTL)
	if known || minority {
		t.Fatalf("no snapshot + busy refresh must report unknown: minority=%v known=%v", minority, known)
	}
}

// TestCheckPartitionRecoveryRefreshesSnapshot watcher 每轮检查都会刷新快照。
func TestCheckPartitionRecoveryRefreshesSnapshot(t *testing.T) {
	m := NewManager("n1")
	m.RegisterPeer("n2", "127.0.0.1:1")
	m.RegisterPeer("n3", "127.0.0.1:2")

	down := true
	m.SetProber(func(string) bool { return !down })

	// 首轮：1/3 可达 → 少数派，快照落盘。
	_ = m.CheckPartitionRecovery()
	ps, ok := m.PartitionSnapshot()
	if !ok || ps.MajorityOK {
		t.Fatalf("first watch round should record minority snapshot: %+v ok=%v", ps, ok)
	}

	// 恢复：watcher 触发恢复回调并刷新快照。
	down = false
	recovered := false
	m.SetOnPartitionRecovered(func(uint64) { recovered = true })
	if !m.CheckPartitionRecovery() {
		t.Fatalf("majority recovery must be detected")
	}
	deadline := time.Now().Add(time.Second)
	for !recovered && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !recovered {
		t.Fatalf("recovery callback must fire")
	}
	ps, _ = m.PartitionSnapshot()
	if !ps.MajorityOK || ps.Reachable != 3 {
		t.Fatalf("snapshot must converge to majority after recovery: %+v", ps)
	}
}

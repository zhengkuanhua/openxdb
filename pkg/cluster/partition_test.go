package cluster

// P2 分区检测 / 视图 version / 恢复合并（docs/HA_BRAIN_SPLIT_DESIGN.md §5 P2）：
//   - TestP2T5MajorityArbiter（T5）：5 节点集群多副本仲裁——存活 3 可
//     升主/重指派、存活 2 不可；
//   - TestP2ViewVersionMaxWins：视图 version 大者胜——恢复后陈旧分区
//     的低 version 视图不得覆盖新鲜视图；
//   - TestP2PartitionRecoveryCallback：少数派 → 多数派状态转换触发
//     恢复回调（携带恢复时已知最大视图 version）。

import (
	"testing"
	"time"
)

// TestP2T5MajorityArbiter 多副本仲裁（T5）：
// 5 节点集群（本节点 + 4 远端），多数派阈值 N/2+1=3；
// 可达 3 满足 quorum（可升主/重指派），可达 2 不满足（保持只读）。
func TestP2T5MajorityArbiter(t *testing.T) {
	m := NewManager("node-a")
	m.RegisterSelf("127.0.0.1:9")
	for _, id := range []string{"node-b", "node-c", "node-d", "node-e"} {
		m.RegisterPeer(id, "127.0.0.1:1")
	}

	// 全部不可达（1/5）→ 少数派。
	m.SetProber(func(string) bool { return false })
	ps := m.PartitionStatus()
	if ps.MajorityOK || ps.Reachable != 1 || ps.Quorum != 3 || ps.Total != 5 {
		t.Fatalf("partition status(1/5) = %+v, want reachable=1 quorum=3 total=5 minority", ps)
	}

	// 可达 3/5 → 多数派（可升主/重指派）。
	m.SetProber(func(id string) bool {
		return id == "node-b" || id == "node-c" || id == "node-d"
	})
	if !m.MajorityOK() {
		t.Fatalf("3/5 reachable should satisfy quorum, got %+v", m.PartitionStatus())
	}

	// 存活 2 节点（本节点 + node-b）→ 少数派（保持只读）。
	m.SetProber(func(id string) bool {
		return id == "node-b"
	})
	if m.MajorityOK() {
		t.Fatalf("2/5 reachable should NOT satisfy quorum, got %+v", m.PartitionStatus())
	}
}

// TestP2ViewVersionMaxWins 视图 version 大者胜：分区恢复收敛基准——
// 陈旧分区的低 version 视图不得覆盖新鲜视图。
func TestP2ViewVersionMaxWins(t *testing.T) {
	m := NewManager("node-a")
	m.RegisterSelf("127.0.0.1:9")

	if v := m.ViewVersion(); v != 0 {
		t.Fatalf("fresh view version = %d, want 0", v)
	}
	if v := m.TakeViewVersion(3); v != 3 {
		t.Fatalf("take 3 = %d, want 3", v)
	}
	if v := m.TakeViewVersion(1); v != 3 {
		t.Fatalf("stale view (1) must not override: got %d, want 3", v)
	}
	if v := m.TakeViewVersion(5); v != 5 {
		t.Fatalf("take 5 = %d, want 5", v)
	}
}

// TestP2PartitionRecoveryCallback 分区恢复回调：少数派 → 多数派状态
// 转换触发一次恢复回调，回调携带恢复时已知最大视图 version；
// 持续多数派不重复触发。
func TestP2PartitionRecoveryCallback(t *testing.T) {
	m := NewManager("node-a")
	m.RegisterSelf("127.0.0.1:9")
	for _, id := range []string{"node-b", "node-c", "node-d"} {
		m.RegisterPeer(id, "127.0.0.1:1")
	}
	m.TakeViewVersion(9)

	recovered := make(chan uint64, 1)
	m.SetOnPartitionRecovered(func(v uint64) { recovered <- v })

	// 初始：全部不可达（少数派）→ 不触发。
	m.SetProber(func(string) bool { return false })
	if m.CheckPartitionRecovery() {
		t.Fatal("first check (minority) should not trigger recovery")
	}
	// 仍少数派 → 不触发。
	if m.CheckPartitionRecovery() {
		t.Fatal("still minority should not trigger")
	}
	// 恢复多数派（2/4 可达 → 阈值 3 仍不足；改 3/4 可达）→ 触发一次。
	m.SetProber(func(id string) bool {
		return id == "node-b" || id == "node-c" || id == "node-d"
	})
	if !m.CheckPartitionRecovery() {
		t.Fatal("minority->majority transition should trigger recovery")
	}
	select {
	case v := <-recovered:
		if v != 9 {
			t.Fatalf("recovery callback version = %d, want 9", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recovery callback not fired")
	}
	// 持续多数派 → 不重复触发。
	if m.CheckPartitionRecovery() {
		t.Fatal("already majority should not re-trigger")
	}
}

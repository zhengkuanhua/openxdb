package sql

// v1.20.0 C3：参与者规划（preparePlan）单元测试。
//
// 覆盖缺口 B-2（参与者集合 = owner + 副本，quorum = n/2+1）与 B-4（副本记录缺失
// 退化为仅 owner，与 v1.19.0 逐字一致）；不涉及网络，仅校验规划结果。

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// c3PlanEnv 构造"region 归属远端 node-b"的执行器环境。
func c3PlanEnv(t *testing.T) (*Executor, storage.Storage, storage.ID, map[string][]kvOp) {
	t.Helper()
	e, st := newWriteRouteEnv(t, "node-a")
	e.st = st // 副本元数据直读本地存储（生产由 StartCluster 装配）
	e.mgr = newWriteRouteCluster("node-a", "node-b", "node-c")
	rid := storage.ID(9)
	if err := e.router.UpsertRegion(sharding.RegionInfo{
		RegionID: rid, State: sharding.StateActive, Explicit: true,
	}); err != nil {
		t.Fatalf("UpsertRegion: %v", err)
	}
	if err := e.router.SetRegionNode(rid, "node-b"); err != nil {
		t.Fatalf("SetRegionNode: %v", err)
	}
	remote := map[string][]kvOp{
		"node-b": {{Key: sharding.EncodeRegionKey(rid, []byte("k1")), Value: []byte("v1")}},
	}
	return e, st, rid, remote
}

// TestV1200C3PreparePlanOwnerOnlyDegrade 开关关闭 / 副本记录缺失均退化为仅 owner。
func TestV1200C3PreparePlanOwnerOnlyDegrade(t *testing.T) {
	e, _, _, remote := c3PlanEnv(t)

	// 默认 off：与 v1.19.0 完全一致——参与者只有 owner，quorum=1。
	p := e.buildPreparePlan(remote)
	if p.replicated {
		t.Fatalf("switch off must not enable replica participation")
	}
	if len(p.nodes) != 1 || p.nodes[0] != "node-b" || p.quorum != 1 {
		t.Fatalf("switch off plan: nodes=%v quorum=%d", p.nodes, p.quorum)
	}

	// 开关 on 但副本记录缺失：退化仅 owner（B-4）。
	if err := applyHaTwoPCReplicaQuorum(e, "on"); err != nil {
		t.Fatalf("applyHaTwoPCReplicaQuorum on: %v", err)
	}
	if !e.TwoPCReplicaQuorum() {
		t.Fatalf("switch must be on")
	}
	p = e.buildPreparePlan(remote)
	if p.replicated {
		t.Fatalf("missing replica record must degrade to owner-only")
	}
	if len(p.nodes) != 1 || p.nodes[0] != "node-b" || p.quorum != 1 {
		t.Fatalf("degraded plan: nodes=%v quorum=%d", p.nodes, p.quorum)
	}
	if len(p.ops["node-b"]) != 1 {
		t.Fatalf("degraded owner ops: %d", len(p.ops["node-b"]))
	}

	// 多 owner 分组（两个远端节点）、开关 on、仍无副本记录：quorum 仍为 len(nodes)，
	// 与 v1.19.0 的"全部成功"完全一致（不得退化为 n/2+1）。
	remote2 := map[string][]kvOp{
		"node-b": {{Key: sharding.EncodeRegionKey(storage.ID(9), []byte("k1")), Value: []byte("v1")}},
		"node-c": {{Key: sharding.EncodeRegionKey(storage.ID(10), []byte("k1")), Value: []byte("v2")}},
	}
	p = e.buildPreparePlan(remote2)
	if p.replicated || len(p.nodes) != 2 || p.quorum != 2 {
		t.Fatalf("multi-node degrade: replicated=%v nodes=%v quorum=%d", p.replicated, p.nodes, p.quorum)
	}
}

// TestV1200C3PreparePlanOwnerPlusReplicas 声明的副本进入参与者集合，quorum=n/2+1。
func TestV1200C3PreparePlanOwnerPlusReplicas(t *testing.T) {
	e, st, rid, remote := c3PlanEnv(t)
	if err := applyHaTwoPCReplicaQuorum(e, "on"); err != nil {
		t.Fatalf("applyHaTwoPCReplicaQuorum on: %v", err)
	}
	// 副本 {node-a, node-c}（不含 owner node-b）-> 3 个参与者，quorum=2。
	if err := cluster.PutRegionReplicas(st, rid, []string{"node-a", "node-c"}); err != nil {
		t.Fatalf("PutRegionReplicas: %v", err)
	}
	p := e.buildPreparePlan(remote)
	if !p.replicated {
		t.Fatalf("replica record must enable replica participation")
	}
	if len(p.nodes) != 3 || p.quorum != 2 {
		t.Fatalf("plan: nodes=%v quorum=%d", p.nodes, p.quorum)
	}
	for _, n := range []string{"node-a", "node-b", "node-c"} {
		if !cluster.ContainsNode(p.nodes, n) {
			t.Fatalf("participant %s missing: %v", n, p.nodes)
		}
		// 每个参与者都持有同一份 ops（replicated 冗余写入）。
		if len(p.ops[n]) != 1 {
			t.Fatalf("participant %s ops: %d", n, len(p.ops[n]))
		}
	}

	// 关闭开关后立即回到仅 owner（热切换，零回归）。
	if err := applyHaTwoPCReplicaQuorum(e, "off"); err != nil {
		t.Fatalf("applyHaTwoPCReplicaQuorum off: %v", err)
	}
	p = e.buildPreparePlan(remote)
	if p.replicated || len(p.nodes) != 1 || p.quorum != 1 {
		t.Fatalf("switch off after on: replicated=%v nodes=%v quorum=%d", p.replicated, p.nodes, p.quorum)
	}
}

// TestV1200C3ReplicaQuorumConfigKey 配置键取值校验与非法值拒绝。
func TestV1200C3ReplicaQuorumConfigKey(t *testing.T) {
	e, _, _, _ := c3PlanEnv(t)
	if err := applyHaTwoPCReplicaQuorum(e, "bogus"); err == nil {
		t.Fatalf("invalid value must be rejected")
	}
	if spec := lookupConfigSpec("ha_2pc_replica_quorum"); spec == nil {
		t.Fatalf("ha_2pc_replica_quorum must be registered")
	} else if spec.def != "off" {
		t.Fatalf("default must be off, got %q", spec.def)
	}
}

// P9 HA（D4.5 Phase 9 / 候选 B）：集群轨归属纪元通知接线测试。
//
// 断言 SQL 层把"本机接管 region"的授权事实（term + 归属）如实上报 db 层，
// 且未装配通知器时为空操作（零回归）——这是 NEW-1 / T3 同源 masterEpoch 恒 0
// 缺陷收口的授权侧来源。
package sql

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// TestP9LocalTakeoverNotifiesTerm 本机接管成功 → 上报 (term, selfOwns=true)。
func TestP9LocalTakeoverNotifiesTerm(t *testing.T) {
	e := newP1Executor(&fakeEpochSrc{known: 6}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")

	var gotTerm uint64
	var gotSelf, gotAuth bool
	var calls int
	e.SetClusterOwnershipNotify(func(term uint64, selfOwns, authoritative bool) {
		calls++
		gotTerm, gotSelf, gotAuth = term, selfOwns, authoritative
	})
	// v1.21.0 NEW-3：接管上报需目标 region 确实归属本机（含有效 regionEpoch），
	// 否则视为接管未落地、不上报（误授根因收口）。
	if err := e.router.UpsertRegion(regionOwnedBy("node-a", 7, 4)); err != nil {
		t.Fatalf("upsert region: %v", err)
	}

	e.notifyLocalTakeover(7, 7)
	if calls != 1 {
		t.Fatalf("notify calls = %d, want 1", calls)
	}
	if gotTerm != 7 {
		t.Fatalf("notified term = %d, want 7 (接管使用的 clusterEpoch)", gotTerm)
	}
	if !gotSelf {
		t.Fatalf("takeover must report selfOwns=true")
	}
	if gotAuth {
		t.Fatalf("takeover is an incremental grant, not an authoritative full view")
	}

	// 传 nil 撤销装配后，通知为空操作且不 panic（零回归）。
	e.SetClusterOwnershipNotify(nil)
	e.notifyLocalTakeover(7, 8)
	if calls != 1 {
		t.Fatalf("notify calls after detach = %d, want 1", calls)
	}
}

// regionOwnedBy 构造显式 region 路由条目（供接管上报判定使用）。
func regionOwnedBy(node string, rid storage.ID, regionEpoch uint64) sharding.RegionInfo {
	rg := regionOf(rid, node, regionEpoch)
	rg.Explicit = true
	return rg
}

// TestP9RouteSyncCarriesClusterTerm 权威路由视图同步携带本机已知集群纪元，
// 使重启节点能按权威 term 确认/收回归属；未装配 epoch 源时携带 0（旧行为）。
func TestP9RouteSyncCarriesClusterTerm(t *testing.T) {
	e := newP1Executor(&fakeEpochSrc{known: 9}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")
	if got := e.clusterTermForRouteSync(); got != 9 {
		t.Fatalf("clusterTermForRouteSync = %d, want 9", got)
	}

	bare := &Executor{}
	if got := bare.clusterTermForRouteSync(); got != 0 {
		t.Fatalf("no epoch source must carry 0 (version negotiation), got %d", got)
	}
}

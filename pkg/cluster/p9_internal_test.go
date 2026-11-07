// P9 HA（D4.5 Phase 9 / 候选 B）：集群轨归属纪元通知接线点测试。
//
// 集群轨 masterEpoch 恒 0 的根因之一是"装载/路由同步完成后没有任何地方把
// 「本机在哪一 term 上持有归属」上报上层"。本文件锁住三个接线事实：
//   - 装载 PEREGION_PUSH（ClaimOwner=true，接管/迁移落地）后上报 term + selfOwns；
//   - 权威全量路由同步（Authoritative=true，重启刷新路径）后上报，且当权威视图
//     显示本机不再持有 region 时 selfOwns=false（上层据此收回归属 → fence 陈旧主）；
//   - 增量广播（Authoritative=false）只用于授予，不会因"本条不含本机 region"
//     而误报收回。
package cluster

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

type p9NotifyRec struct {
	calls         int
	term          uint64
	selfOwns      bool
	authoritative bool
}

func newP9NotifyServer(t *testing.T, seed []sharding.RegionInfo) (*Server, *p9NotifyRec) {
	t.Helper()
	srv := NewServer(btree.NewKV(32), "node-b")
	router := sharding.NewRouter()
	for _, rg := range seed {
		if err := router.UpsertRegion(rg); err != nil {
			t.Fatalf("seed router: %v", err)
		}
	}
	srv.SetRouter(router)
	rec := &p9NotifyRec{}
	srv.SetOwnershipNotify(func(term uint64, selfOwns, authoritative bool) {
		rec.calls++
		rec.term, rec.selfOwns, rec.authoritative = term, selfOwns, authoritative
	})
	return srv, rec
}

// TestP9ClaimOwnerPushNotifiesGrant 装载接管（ClaimOwner=true）后上报授权事实：
// 目标节点由此在 term 上获得归属，写路径门槛才有据可依。
func TestP9ClaimOwnerPushNotifiesGrant(t *testing.T) {
	srv, rec := newP9NotifyServer(t, nil)
	key := sharding.EncodeRegionKey(2, []byte("k1"))
	if _, err := srv.applyPush(&RegionPushReq{
		SrcID:        "node-c",
		Region:       sharding.RegionInfo{RegionID: 2, StartKey: []byte("a"), EndKey: []byte("z")},
		Rows:         []QueryRow{{Key: key, Value: []byte("1")}},
		ClaimOwner:   true,
		ClusterEpoch: 6,
	}); err != nil {
		t.Fatalf("applyPush: %v", err)
	}
	if rec.calls != 1 {
		t.Fatalf("notify calls = %d, want 1", rec.calls)
	}
	if rec.term != 6 {
		t.Fatalf("notified term = %d, want 6", rec.term)
	}
	if !rec.selfOwns {
		t.Fatalf("ClaimOwner push must report selfOwns=true")
	}
	if rec.authoritative {
		t.Fatalf("claim-owner push is not an authoritative full view")
	}
}

// TestP9AuthoritativeRouteSyncRevokesWhenNotOwner 重启刷新拿到权威全量视图，
// 其中本机已不持有任何 region → selfOwns=false + authoritative=true（上层收回）。
func TestP9AuthoritativeRouteSyncRevokesWhenNotOwner(t *testing.T) {
	// 重启节点的旧视图：r2 归属本机（node-b）。
	srv, rec := newP9NotifyServer(t, []sharding.RegionInfo{{
		RegionID: 2, StartKey: []byte("a"), EndKey: []byte("z"), Node: "node-b", RegionEpoch: 3,
	}})
	if !srv.selfOwnsAny() {
		t.Fatalf("precondition: restarted node should still own region 2 per stale view")
	}

	// 权威视图：r2 已归 node-c（term 7）。
	if _, err := srv.applyRouteSync(&RegionPushReq{
		SrcID:        "node-c",
		SyncOnly:     true,
		Authoritative: true,
		ClusterEpoch: 7,
		Routes: []sharding.RegionInfo{{
			RegionID: 2, StartKey: []byte("a"), EndKey: []byte("z"), Node: "node-c", RegionEpoch: 9,
		}},
	}); err != nil {
		t.Fatalf("applyRouteSync: %v", err)
	}
	if rec.calls != 1 {
		t.Fatalf("notify calls = %d, want 1", rec.calls)
	}
	if rec.term != 7 {
		t.Fatalf("notified term = %d, want 7 (authoritative term)", rec.term)
	}
	if rec.selfOwns {
		t.Fatalf("authoritative view without local region must report selfOwns=false")
	}
	if !rec.authoritative {
		t.Fatalf("route sync from authoritative source must be flagged authoritative")
	}
}

// TestP9IncrementalRouteSyncIsGrantOnly 增量广播只授予不收回：本机仍持有 r2 时
// 收到"别的 region 归别人"的增量路由，selfOwns 仍为 true。
func TestP9IncrementalRouteSyncIsGrantOnly(t *testing.T) {
	srv, rec := newP9NotifyServer(t, []sharding.RegionInfo{{
		RegionID: 2, StartKey: []byte("a"), EndKey: []byte("z"), Node: "node-b", RegionEpoch: 3,
	}})
	if _, err := srv.applyRouteSync(&RegionPushReq{
		SrcID:        "node-b",
		SyncOnly:     true,
		ClusterEpoch: 8,
		Routes: []sharding.RegionInfo{{
			RegionID: 5, StartKey: []byte("m"), EndKey: []byte("n"), Node: "node-c", RegionEpoch: 1,
		}},
	}); err != nil {
		t.Fatalf("applyRouteSync: %v", err)
	}
	if rec.calls != 1 {
		t.Fatalf("notify calls = %d, want 1", rec.calls)
	}
	if !rec.selfOwns {
		t.Fatalf("incremental broadcast must not drop local ownership of region 2")
	}
	if rec.authoritative {
		t.Fatalf("incremental broadcast must not be flagged authoritative")
	}
}

// TestP9NoNotifyHookIsNoop 未装配通知器时为空操作（零回归，不 panic）。
func TestP9NoNotifyHookIsNoop(t *testing.T) {
	srv := NewServer(btree.NewKV(32), "node-b")
	router := sharding.NewRouter()
	srv.SetRouter(router)
	if err := router.UpsertRegion(sharding.RegionInfo{
		RegionID: 1, StartKey: []byte("a"), EndKey: []byte("b"), Node: "node-b",
	}); err != nil {
		t.Fatalf("seed router: %v", err)
	}
	srv.notifyOwnership(3, true) // 不得 panic
	if err := srv.st.Write(&storage.WriteBatch{}); err != nil {
		t.Fatalf("storage sanity: %v", err)
	}
}

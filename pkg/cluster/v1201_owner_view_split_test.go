// v1.20.1 P0-1：集群写轨归属视图分裂收敛（回归用例）。
//
// 缺陷背景（v1.20.0 真机 T3 复验残留）：selfOwnsAny 把"Node 为空"无条件当作
// 本节点归属，"本机不持有、来源也未声明归属"的未指派条目（applyRouteSync /
// applyPush 学习远端视图后 Local=false）因此让节点凭空自认 owner，与真实 owner
// 形成归属视图分裂（192.168.100.201/203 同时报 self_owns=true），写路径被 P9
// 误授权。
//
// 本文件覆盖三条口径：
//  1. 未指派且不持有（Local=false）→ 不构成归属；
//  2. 归属他人却标本地（矛盾条目）→ 应用远端视图时收敛为 Local=false 并计数；
//  3. 未指派但确实持有（Local=true）→ 仍构成归属（单机/装载既有语义零回归）。
package cluster

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

// TestV1201UnownedEntryDoesNotGrantOwnership 未指派且本机不持有的路由条目
// 不得让节点自认 owner（归属视图分裂的直接成因）。
func TestV1201UnownedEntryDoesNotGrantOwnership(t *testing.T) {
	srv := NewServer(btree.NewKV(32), "node-c")
	router := sharding.NewRouter()
	if err := router.UpsertRegion(sharding.RegionInfo{
		RegionID: 7, StartKey: []byte("a"), EndKey: []byte("z"),
		Node: "", Local: false, RegionEpoch: 4,
	}); err != nil {
		t.Fatalf("seed router: %v", err)
	}
	srv.SetRouter(router)

	if srv.selfOwnsAny() {
		t.Fatalf("unowned entry (Node=\"\", Local=false) must not grant ownership")
	}
}

// TestV1201LocalHolderStillOwns 未指派但本机确实持有（Local=true）仍构成归属，
// 保持"未指派即本节点"的单机/装载既有语义零回归。
func TestV1201LocalHolderStillOwns(t *testing.T) {
	srv := NewServer(btree.NewKV(32), "node-a")
	router := sharding.NewRouter()
	if err := router.UpsertRegion(sharding.RegionInfo{
		RegionID: 2, StartKey: []byte("a"), EndKey: []byte("z"),
		Node: "", Local: true, RegionEpoch: 1,
	}); err != nil {
		t.Fatalf("seed router: %v", err)
	}
	srv.SetRouter(router)

	if !srv.selfOwnsAny() {
		t.Fatalf("local holder (Node=\"\", Local=true) must still own the region")
	}
}

// TestV1201RouteSyncConvergesOwnerConflict 归属他人（Node=node-b）却标 Local=true
// 的矛盾条目：应用任一远端视图后必须收敛为 Local=false，冲突计数 +1。
// 该矛盾条目不在本次同步的 Routes 内（否则 normalizeRouteOwner 当场就会改写它），
// 对应"历史迁移遗留的本地标记未被后续增量视图覆盖"的真实形态。
func TestV1201RouteSyncConvergesOwnerConflict(t *testing.T) {
	srv := NewServer(btree.NewKV(32), "node-c")
	router := sharding.NewRouter()
	// 矛盾条目：region 8 归属 node-b，但本机视图里仍标 Local=true。
	if err := router.UpsertRegion(sharding.RegionInfo{
		RegionID: 8, StartKey: []byte("a"), EndKey: []byte("z"),
		Node: "node-b", Local: true, RegionEpoch: 4,
	}); err != nil {
		t.Fatalf("seed router: %v", err)
	}
	srv.SetRouter(router)

	if _, err := srv.applyRouteSync(&RegionPushReq{
		SrcID:         "node-b",
		SyncOnly:      true,
		ClusterEpoch:  9,
		Authoritative: false,
		Routes: []sharding.RegionInfo{{
			RegionID: 7, StartKey: []byte("a"), EndKey: []byte("z"),
			Node: "node-b", RegionEpoch: 4,
		}},
	}); err != nil {
		t.Fatalf("applyRouteSync: %v", err)
	}

	rg, ok := router.FindRegion(8)
	if !ok {
		t.Fatalf("region 8 missing from router")
	}
	if rg.Local {
		t.Fatalf("owner=node-b region must not stay Local=true on node-c")
	}
	if got := srv.OwnerViewConflicts(); got != 1 {
		t.Fatalf("owner view conflicts = %d, want 1", got)
	}
	if srv.selfOwnsAny() {
		t.Fatalf("node must not claim ownership of a region owned by node-b")
	}
}

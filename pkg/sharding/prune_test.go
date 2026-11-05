package sharding_test

// D4.4 次要缺陷修复单元测试：权威路由视图收敛清理。
//
// 缺陷：路由视图同步只做 upsert，不清理"权威视图中不存在"的本地历史 region，
// 重启节点 SHOW REGION ROUTES 因此暴露权威视图没有的 region（且标 local=true）。
// 修复：Router.PruneSupersededRegions 仅移除"RegionID 不在权威视图、且键区间被
// 权威视图中同表 region 完全覆盖"的本地记录；其余情况一律保守保留。
//
// 覆盖点：
//  1. 区间完全重叠的历史 region 被移除；
//  2. 部分重叠 / 无覆盖 / 跨表的历史 region 不被误删；
//  3. 权威视图为空时不做任何清理（防残缺视图清空本地视图）；
//  4. 幂等：同一权威视图重复收敛不再产生删除。

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// bnd 构造边界键（单字节标记即可，仅用于区间比较）。
func bnd(b byte) []byte { return []byte{b} }

// newRouterWith 构造只含给定 region 的路由表。
func newRouterWith(t *testing.T, regions ...sharding.RegionInfo) *sharding.Router {
	t.Helper()
	r := sharding.NewRouter()
	for _, rg := range regions {
		if err := r.UpsertRegion(rg); err != nil {
			t.Fatalf("upsert region %d: %v", rg.RegionID, err)
		}
	}
	return r
}

// hasRegion 判断路由表中是否存在指定 region。
func hasRegion(r *sharding.Router, rid storage.ID) bool {
	_, ok := r.FindRegion(rid)
	return ok
}

// TestPruneSupersededRegionsRemovesStaleOverlap 历史 region 区间被现役 region
// 完全覆盖时应被移除（复现实测场景：rid=3 与现役 rid=7 区间完全重叠）。
func TestPruneSupersededRegionsRemovesStaleOverlap(t *testing.T) {
	stale := sharding.RegionInfo{RegionID: 3, TableID: 1, StartKey: nil, EndKey: bnd(2), State: sharding.StateActive, Local: true}
	live := sharding.RegionInfo{RegionID: 7, TableID: 1, StartKey: nil, EndKey: bnd(2), State: sharding.StateActive, Node: "node-a"}
	r := newRouterWith(t, stale, live)

	pruned := r.PruneSupersededRegions([]sharding.RegionInfo{live})
	if len(pruned) != 1 || pruned[0].RegionID != 3 {
		t.Fatalf("pruned = %+v, want [rid=3]", pruned)
	}
	if hasRegion(r, 3) {
		t.Fatalf("stale region 3 still present in local view")
	}
	if !hasRegion(r, 7) {
		t.Fatalf("authoritative region 7 must be kept")
	}
	// 幂等：再次收敛不应重复删除
	if again := r.PruneSupersededRegions([]sharding.RegionInfo{live}); len(again) != 0 {
		t.Fatalf("second prune = %+v, want empty (idempotent)", again)
	}
}

// TestPruneSupersededRegionsKeepsUncovered 无法被现役 region 完全覆盖的历史
// region 必须保留（保守优先，避免权威视图残缺时误删合法 region）。
func TestPruneSupersededRegionsKeepsUncovered(t *testing.T) {
	cases := []struct {
		name  string
		stale sharding.RegionInfo
		auth  []sharding.RegionInfo
	}{
		{
			name:  "部分重叠（区间更大，未被完全覆盖）",
			stale: sharding.RegionInfo{RegionID: 9, TableID: 1, StartKey: nil, EndKey: bnd(3), Local: true},
			auth:  []sharding.RegionInfo{{RegionID: 7, TableID: 1, StartKey: nil, EndKey: bnd(2)}},
		},
		{
			name:  "无重叠区间",
			stale: sharding.RegionInfo{RegionID: 9, TableID: 1, StartKey: bnd(5), EndKey: bnd(6), Local: true},
			auth:  []sharding.RegionInfo{{RegionID: 7, TableID: 1, StartKey: nil, EndKey: bnd(2)}},
		},
		{
			name:  "跨表（表 2 历史 region 不得被表 1 现役 region 判定覆盖）",
			stale: sharding.RegionInfo{RegionID: 9, TableID: 2, StartKey: nil, EndKey: bnd(2), Local: true},
			auth:  []sharding.RegionInfo{{RegionID: 7, TableID: 1, StartKey: nil, EndKey: bnd(2)}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRouterWith(t, tc.stale)
			pruned := r.PruneSupersededRegions(tc.auth)
			if len(pruned) != 0 {
				t.Fatalf("pruned = %+v, want empty (must be kept)", pruned)
			}
			if !hasRegion(r, tc.stale.RegionID) {
				t.Fatalf("region %d must be kept", tc.stale.RegionID)
			}
		})
	}
}

// TestPruneSupersededRegionsEmptyAuthoritativeIsNoop 权威视图为空（源节点无
// 路由信息/同步消息残缺）时绝不清空本地视图，防止误删。
func TestPruneSupersededRegionsEmptyAuthoritativeIsNoop(t *testing.T) {
	local := sharding.RegionInfo{RegionID: 3, TableID: 1, StartKey: nil, EndKey: bnd(2), Local: true}
	r := newRouterWith(t, local)
	if pruned := r.PruneSupersededRegions(nil); len(pruned) != 0 {
		t.Fatalf("pruned = %+v, want empty", pruned)
	}
	if !hasRegion(r, 3) {
		t.Fatalf("local view cleared by empty authoritative view")
	}
}

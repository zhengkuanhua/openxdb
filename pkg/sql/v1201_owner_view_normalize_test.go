// v1.20.1 P0-2：本地归属变更后的 Local 标记归一（回归用例）。
//
// 缺陷背景：本地 ASSIGN REGION / 迁移 / balance / failover 只调 router.SetRegionNode*
// 改 Node，不改 Local。归属外移后、远端视图同步到达前，本机视图会出现
// "owner 是远端 + local=true" 的矛盾条目，SHOW REGION ROUTES 与 waitRoute 类断言
// 随同步时机抖动（v1.20.0 期 TestV1200C4TwoPCWriteEntersRegionRepl 偶发失败即此）。
// 修复点：persistRegions 落盘前调用 normalizeRegionOwnerView 收敛，保证任何持久化
// 视图都满足"Node 是归属唯一事实源、Local 由 Node 推导"的不变量。
package sql

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// TestV1201NormalizeRegionOwnerView 校验归一规则：仅"归属远端 + local=true"被收敛，
// 归属本机与未指派条目保持原样（不误伤本地持有语义）。
func TestV1201NormalizeRegionOwnerView(t *testing.T) {
	e := NewExecutor(nil)
	e.selfID = "node-a"
	e.router = sharding.NewRouter()

	mustUpsert := func(rg sharding.RegionInfo) {
		if err := e.router.UpsertRegion(rg); err != nil {
			t.Fatalf("UpsertRegion rid=%d: %v", rg.RegionID, err)
		}
	}
	// rid=2 归属远端 node-b 却标本地：矛盾条目，应被归一为 local=false。
	mustUpsert(sharding.RegionInfo{RegionID: 2, TableID: 1, Node: "node-b", Local: true, Explicit: true})
	// rid=3 归属本机：保持 local=true（本机物理持有）。
	mustUpsert(sharding.RegionInfo{RegionID: 3, TableID: 1, Node: "node-a", Local: true, Explicit: true})
	// rid=4 未指派（Node 为空 = 本节点）：保持 local=true。
	mustUpsert(sharding.RegionInfo{RegionID: 4, TableID: 1, Node: "", Local: true, Explicit: true})
	// rid=5 归属远端且已标 local=false：无需改动。
	mustUpsert(sharding.RegionInfo{RegionID: 5, TableID: 1, Node: "node-c", Local: false, Explicit: true})

	if fixed := e.normalizeRegionOwnerView(); fixed != 1 {
		t.Fatalf("归一条目数 = %d, want 1（仅 rid=2 属矛盾视图）", fixed)
	}

	cases := []struct {
		rid   storage.ID
		node  string
		local bool
	}{
		{2, "node-b", false},
		{3, "node-a", true},
		{4, "", true},
		{5, "node-c", false},
	}
	for _, c := range cases {
		rg, ok := e.router.FindRegion(c.rid)
		if !ok {
			t.Fatalf("rid=%d 不在路由视图中", c.rid)
		}
		if rg.Node != c.node || rg.Local != c.local {
			t.Fatalf("rid=%d 归一后 = {node:%q local:%v}, want {node:%q local:%v}",
				c.rid, rg.Node, rg.Local, c.node, c.local)
		}
	}

	// 幂等：再次归一不再产生改动（收敛后视图稳定，不随调用次数抖动）。
	if fixed := e.normalizeRegionOwnerView(); fixed != 0 {
		t.Fatalf("二次归一改动条目数 = %d, want 0（幂等）", fixed)
	}
}


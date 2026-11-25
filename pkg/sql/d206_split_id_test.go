package sql_test

import (
	"fmt"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
)

// TestD206SplitRegionAllocatesExplicitIDs 回归 v2.0.5 三节点撞号缺陷：
// 连续分裂必须在显式 ID 区间内分配，绝不产出 ≥1<<63 的"显式"region ID
// （那会与 implicitRegionID(tableID) 撞号，两表共用物理键前缀 r{regionID}，
// 同时被 IsImplicitRegionID 误判导致水位不再推进、下次分裂复用同一批 ID）。
func TestD206SplitRegionAllocatesExplicitIDs(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	if err := e.SplitTable("users", [][]byte{m7Boundary(4), m7Boundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}

	// check 校验当前 region 集合：全部显式区间内、ID 互不相同，返回升序前的 ID 列表
	check := func(stage string) []uint64 {
		rs, err := e.ListRegions("users")
		if err != nil {
			t.Fatalf("%s ListRegions: %v", stage, err)
		}
		seen := make(map[uint64]bool, len(rs))
		ids := make([]uint64, 0, len(rs))
		for _, r := range rs {
			rid := uint64(r.RegionID)
			if sharding.IsImplicitRegionID(r.RegionID) {
				t.Fatalf("%s: explicit region got implicit-range id %d", stage, rid)
			}
			if rid >= sharding.ExplicitRegionIDLimit {
				t.Fatalf("%s: region id %d above explicit limit", stage, rid)
			}
			if seen[rid] {
				t.Fatalf("%s: duplicated region id %d", stage, rid)
			}
			seen[rid] = true
			ids = append(ids, rid)
		}
		return ids
	}
	ids := check("after SplitTable")
	if len(ids) != 3 {
		t.Fatalf("want 3 regions after SplitTable, got %d", len(ids))
	}
	maxBefore := ids[len(ids)-1]

	// 连续两轮手动分裂：ID 必须继续落在显式区间，且新 ID 严格大于此前最大值
	for round := 0; round < 2; round++ {
		before := check(fmt.Sprintf("round %d before", round))
		rs, err := e.ListRegions("users")
		if err != nil || len(rs) == 0 {
			t.Fatalf("round %d ListRegions: %v", round, err)
		}
		if err := e.SplitRegionNow(uint64(rs[len(rs)-1].RegionID)); err != nil {
			t.Fatalf("round %d SplitRegionNow: %v", round, err)
		}
		after := check(fmt.Sprintf("round %d after", round))
		if len(after) != len(before)+1 {
			t.Fatalf("round %d: region count want %d, got %d", round, len(before)+1, len(after))
		}
		var newID uint64
		newID = after[len(after)-1]
		if newID <= maxBefore {
			t.Fatalf("round %d: seq not advanced, new id %d <= previous max %d", round, newID, maxBefore)
		}
		maxBefore = newID
	}

	// 数据必须完好
	if rows := mustRows(t, e, "SELECT id FROM users ORDER BY id"); len(rows) != 10 {
		t.Fatalf("row count after splits: want 10, got %d", len(rows))
	}
}

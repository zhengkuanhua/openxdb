package sql_test

// v2.0.8 Q2 回归：SPLIT REGION 返回值语义。
// 覆盖：空 region / 有效行不足两行的 region 上执行 SPLIT 返回 AffectedRows=0
// 且不产生新 region；有数据时返回 AffectedRows=1 且 region 数 +1；分裂后数据不丢。

import (
	"fmt"
	"testing"
)

func TestV208SplitRegionEmptyReportsNotSplit(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	// 空表：隐式单 region，无有效行 → SPLIT 不成立
	rs, err := e.ListRegions("users")
	if err != nil || len(rs) != 1 {
		t.Fatalf("before: rs=%v err=%v", rs, err)
	}
	rid := rs[0].RegionID
	res := mustExec(t, e, fmt.Sprintf("SPLIT REGION %d", rid))
	if res.AffectedRows != 0 {
		t.Fatalf("empty region SPLIT: want AffectedRows=0 (not split), got %d", res.AffectedRows)
	}
	rs, _ = e.ListRegions("users")
	if len(rs) != 1 {
		t.Fatalf("empty region SPLIT: want 1 region unchanged, got %d (%+v)", len(rs), rs)
	}

	// 仅 1 行：仍不足两行，求不出中点 → 未分裂
	mustExec(t, e, "INSERT INTO users (id, name) VALUES (1, 'u1')")
	res = mustExec(t, e, fmt.Sprintf("SPLIT REGION %d", rid))
	if res.AffectedRows != 0 {
		t.Fatalf("single-row region SPLIT: want AffectedRows=0, got %d", res.AffectedRows)
	}
	rs, _ = e.ListRegions("users")
	if len(rs) != 1 {
		t.Fatalf("single-row region SPLIT: want 1 region, got %d", len(rs))
	}

	// 补到 3 行：可分裂，返回 AffectedRows=1 且 region 数翻倍
	mustExec(t, e, "INSERT INTO users (id, name) VALUES (2, 'u2'), (3, 'u3')")
	res = mustExec(t, e, fmt.Sprintf("SPLIT REGION %d", rid))
	if res.AffectedRows != 1 {
		t.Fatalf("non-empty region SPLIT: want AffectedRows=1, got %d", res.AffectedRows)
	}
	rs, _ = e.ListRegions("users")
	if len(rs) != 2 {
		t.Fatalf("after split: want 2 regions, got %d (%+v)", len(rs), rs)
	}
	rows := mustRows(t, e, "SELECT id FROM users ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("data lost by split: want 3 rows, got %d", len(rows))
	}
}

// TestV208SplitRegionEmptiedExplicitRegion 显式 region 被清空后再 SPLIT：
// 与"空 region"同语义，返回 AffectedRows=0，不产生新 region。
func TestV208SplitRegionEmptiedExplicitRegion(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 6; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	rs, err := e.ListRegions("users")
	if err != nil || len(rs) != 1 {
		t.Fatalf("before: rs=%v err=%v", rs, err)
	}
	rid := rs[0].RegionID
	if res := mustExec(t, e, fmt.Sprintf("SPLIT REGION %d", rid)); res.AffectedRows != 1 {
		t.Fatalf("first split: want AffectedRows=1, got %d", res.AffectedRows)
	}
	rs, _ = e.ListRegions("users")
	if len(rs) != 2 {
		t.Fatalf("after first split: want 2 regions, got %d", len(rs))
	}
	// 取右半 region（StartKey 更大者）确认其仍有数据、可正常分裂
	right := rs[0]
	for _, r := range rs[1:] {
		if string(r.StartKey) > string(right.StartKey) {
			right = r
		}
	}
	if res := mustExec(t, e, fmt.Sprintf("SPLIT REGION %d", right.RegionID)); res.AffectedRows != 1 {
		t.Fatalf("split right region with data: want AffectedRows=1, got %d", res.AffectedRows)
	}
	rs, _ = e.ListRegions("users")
	if len(rs) != 3 {
		t.Fatalf("after second split: want 3 regions, got %d", len(rs))
	}
	// 清空整表后，任一 region 都无有效行：SPLIT 返回 0 且 region 数不变
	mustExec(t, e, "DELETE FROM users")
	before := len(rs)
	res := mustExec(t, e, fmt.Sprintf("SPLIT REGION %d", rs[0].RegionID))
	if res.AffectedRows != 0 {
		t.Fatalf("emptied region SPLIT: want AffectedRows=0, got %d", res.AffectedRows)
	}
	if after, _ := e.ListRegions("users"); len(after) != before {
		t.Fatalf("emptied region SPLIT must not change region count: %d -> %d", before, len(after))
	}
}

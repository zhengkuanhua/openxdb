package sharding

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// d206Explicit 构造一个显式 region 条目。
func d206Explicit(rid uint64, tid storage.ID) RegionInfo {
	return RegionInfo{
		RegionID: storage.ID(rid),
		TableID:  tid,
		State:    StateActive,
		Local:    true,
		Explicit: true,
	}
}

// TestD206SanitizeRegionSeq 覆盖水位收敛的四类分支。
func TestD206SanitizeRegionSeq(t *testing.T) {
	regs := []RegionInfo{
		d206Explicit(1, 1), d206Explicit(3, 1), d206Explicit(4, 1),
		{RegionID: implicitRegionID(8), TableID: 8, State: StateActive, Local: true},
	}
	cases := []struct {
		name string
		in   uint64
		want uint64
	}{
		{"干净水位且不小于最大值", 7, 7},
		{"水位落后于现存最大显式ID", 2, 4},
		{"水位落在隐式区间（历史污染）", (uint64(1) << 63) + 7, 4},
		{"水位恰为隐式标识位", uint64(1) << 63, 4},
	}
	for _, c := range cases {
		if got := SanitizeRegionSeq(c.in, regs); got != c.want {
			t.Fatalf("%s: seq=%d want %d got %d", c.name, c.in, c.want, got)
		}
	}
	// 无任何显式 region 时，污染水位回退到 0
	if got := SanitizeRegionSeq(uint64(1)<<63, nil); got != 0 {
		t.Fatalf("empty router: want 0, got %d", got)
	}
}

// TestD206SetRegionsSanitizesPollutedSeq 启动加载时污染水位必须被收敛，
// 否则进程会带着撞号风险继续分配显式 region ID。
func TestD206SetRegionsSanitizesPollutedSeq(t *testing.T) {
	r := NewRouter()
	r.SetRegions((uint64(1)<<63)+7, []RegionInfo{d206Explicit(1, 1), d206Explicit(4, 1)})
	seq, _ := r.Dump()
	if seq != 4 {
		t.Fatalf("polluted seq not sanitized: %d", seq)
	}
	if seq >= ExplicitRegionIDLimit {
		t.Fatalf("seq must stay below explicit limit: %d", seq)
	}
}

// TestD206ReplaceRegionsRejectsPollutedSeq 替换 region 时透传的污染水位不得写回。
func TestD206ReplaceRegionsRejectsPollutedSeq(t *testing.T) {
	r := NewRouter()
	r.SetRegions(0, []RegionInfo{d206Explicit(1, 1), d206Explicit(3, 1)})
	// 模拟旧版本调用方把隐式 ID 当水位传进来
	old := implicitRegionID(9)
	r.ReplaceRegions(1, []RegionInfo{d206Explicit(1, 1), d206Explicit(3, 1), d206Explicit(4, 1)}, uint64(old))
	seq, _ := r.Dump()
	if seq >= ExplicitRegionIDLimit {
		t.Fatalf("ReplaceRegions accepted polluted seq: %d", seq)
	}
	if seq != 4 {
		t.Fatalf("want seq=4 after sanitize, got %d", seq)
	}
}

// TestD206ImplicitIDNeverCollidesWithExplicit 显式 ID 区间与隐式 ID 区间必须互斥。
func TestD206ImplicitIDNeverCollidesWithExplicit(t *testing.T) {
	for tid := storage.ID(1); tid <= 64; tid++ {
		rid := implicitRegionID(tid)
		if !IsImplicitRegionID(rid) {
			t.Fatalf("implicit id of table %d not recognized: %d", tid, rid)
		}
		if uint64(rid) < ExplicitRegionIDLimit {
			t.Fatalf("implicit id of table %d falls into explicit range: %d", tid, rid)
		}
	}
	// 边界：max explicit = limit-1，仍是显式
	if IsImplicitRegionID(storage.ID(ExplicitRegionIDLimit - 1)) {
		t.Fatalf("limit-1 must be explicit")
	}
}

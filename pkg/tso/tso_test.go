package tso

// M8 全局时间戳（TSO）单元测试：严格单调、批量连续发放、批量后消费衔接、
// 时钟回拨兜底、跨物理毫秒溢出保护。

import (
	"testing"
)

// TestTSOMonotonicGet 连续单发 5000 次必须严格递增（含同毫秒内逻辑序号推进）。
func TestTSOMonotonicGet(t *testing.T) {
	ts := New()
	prev, err := ts.Get()
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	for i := 0; i < 5000; i++ {
		cur, err := ts.Get()
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if cur <= prev {
			t.Fatalf("not strictly monotonic: %d then %d", prev, cur)
		}
		prev = cur
	}
}

// TestTSOBatchContiguousMonotonic 多次 GetBatch 返回连续区间，且批次间严格递增
// （同毫秒时下一批起点 = 上一批终点 + 1，不留洞）。
func TestTSOBatchContiguousMonotonic(t *testing.T) {
	ts := New()
	var prevEnd uint64
	for i := 0; i < 64; i++ {
		start, n := ts.GetBatch(16)
		if n != 16 {
			t.Fatalf("batch %d: got n=%d want 16", i, n)
		}
		if start == 0 {
			t.Fatalf("batch %d: zero start", i)
		}
		end := start + n - 1
		if prevEnd != 0 && start <= prevEnd {
			t.Fatalf("batch %d not monotonic: prevEnd=%d start=%d", i, prevEnd, start)
		}
		prevEnd = end
	}
	// 批量之后单发仍续在批次序列之后（批内缓存消费完才补充，且补充起点 > last）。
	v, err := ts.Get()
	if err != nil {
		t.Fatalf("Get after batch: %v", err)
	}
	if v <= prevEnd {
		t.Fatalf("Get after batch not after prevEnd: got %d prevEnd=%d", v, prevEnd)
	}
}

// TestTSOGetBatchConsumesFromCache GetBatch(10) 后，Get 从缓存逐号消费，
// 返回的号与批量区间严格衔接（start+10、start+11 ...），批量缓存剩余递减。
func TestTSOGetBatchConsumesFromCache(t *testing.T) {
	ts := NewWithBatch(3)
	start, n := ts.GetBatch(10)
	if n != 10 {
		t.Fatalf("GetBatch n=%d want 10", n)
	}
	v, err := ts.Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v != start+10 {
		t.Fatalf("Get after GetBatch: got %d want %d", v, start+10)
	}
	_, count := ts.Batch()
	if count != 9 {
		t.Fatalf("cache count after one Get: %d want 9", count)
	}
	v2, _ := ts.Get()
	if v2 != start+11 {
		t.Fatalf("second Get: got %d want %d", v2, start+11)
	}
}

// TestTSOClockFallback 时钟回拨：把 last 手工拨到未来（模拟系统时钟倒退），
// 发号必须续在 last 之后而非回到物理时间，保证全局单调不被回拨破坏。
func TestTSOClockFallback(t *testing.T) {
	ts := New()
	ts.mu.Lock()
	fake := nowTS() + (1 << logicalBits) + 1000
	ts.last = fake
	ts.mu.Unlock()
	v, err := ts.Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v <= fake {
		t.Fatalf("clock fallback not honored: got %d <= fake last %d", v, fake)
	}
}

// TestTSOBatchOverflowProtection 批量分配跨物理毫秒边界时，分配器推进物理
// 毫秒格并保持区间完整无洞（end-start+1 == n）。构造：手工把 last 拨到当前
// 毫秒内已发一个号（低位=1），随后 GetBatch(2^24) 必跨毫秒边界触发保护。
func TestTSOBatchOverflowProtection(t *testing.T) {
	ts := New()
	ts.mu.Lock()
	ts.last = nowTS() + 1 // 同毫秒内已发一个号，低位=1
	ts.mu.Unlock()
	n := uint64(1) << logicalBits
	start, got := ts.GetBatch(n)
	if got != n {
		t.Fatalf("GetBatch n=%d got %d", n, got)
	}
	end := start + n - 1
	if end-start+1 != n {
		t.Fatalf("batch not contiguous: start=%d end=%d", start, end)
	}
	// 保护触发：跨物理毫秒边界后高 40 位物理毫秒部分推进一格。
	if end>>logicalBits != (start>>logicalBits)+1 {
		t.Fatalf("overflow protection not triggered: start=%d end=%d", start, end)
	}
}

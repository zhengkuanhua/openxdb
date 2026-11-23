package cluster

// v1.21.0 NEW-5（P0）：TSO 号段预取 + 租约兜底的单元测试。
//
// 缺陷背景（v1.21.0 Phase A 三机真机复验 §10.4）：单点 --tso-node 部署下
// TSO 客户端是 pull-on-demand（缓存耗尽才拉批），TSO 节点极短抖动就让正在
// 进行的事务直接拿到 txn prepare failed: tso unavailable。
//
// 本文件覆盖：
//   a) 低水位（batch/4）异步预取确实触发，且不重复/不越界发放；
//   b) 抖动期间剩余号段可继续消耗，只有池真正耗尽才报错；
//   c) maxSeen 回退保护仍生效（预取批与同步批两条路径）；
//   d) HA 客户端具备同等预取能力（GetBatch 缺省批、池耗尽语义不变）；
//   e) 并发取号不重复、不死锁。
//
// 单测通过 fetchHook 注入拉批结果，不依赖真实网络与节点注册表。

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/tso"
)

// seqFetcher 顺序发号假远端：第 k 次调用返回 [k*100+1, k*100+1+size)。
type seqFetcher struct {
	mu    sync.Mutex
	calls int
	size  uint64
	err   error        // 非 nil 时全部调用失败
	errAfter int       // >0 时仅前 errAfter 次成功，其余失败
	regressAt int      // >0 时第 regressAt 次调用返回回退起点
}

func (f *seqFetcher) fetch(id string, want uint64) (uint64, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	n := f.size
	if want > n {
		n = want
	}
	if f.err != nil {
		return 0, 0, f.err
	}
	if f.errAfter > 0 && f.calls > f.errAfter {
		return 0, 0, errors.New("dial tcp 10.0.0.9:7788: connect: connection refused")
	}
	if f.regressAt > 0 && f.calls == f.regressAt {
		// 远端序列回退：起点落在已下发区间之内。
		return 1, n, nil
	}
	return uint64(f.calls-1)*100 + 1, n, nil
}

func (f *seqFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// one 单点客户端（无节点参数）的拉批注入点。
func (f *seqFetcher) one() (uint64, uint64, error) { return f.fetch("", 0) }

// waitFor 轮询等待条件成立（异步预取完成），超时即失败。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("超时等待 %s", what)
}

// TestNew5LowWatermarkPrefetch 低水位预取触发，且下一批严格衔接、不重复发放。
func TestNew5LowWatermarkPrefetch(t *testing.T) {
	f := &seqFetcher{size: 8}
	c := &tsoClient{nodeID: "node-a", batch: 8, fetchHook: f.one}
	if got := c.lowWatermark(); got != 2 {
		t.Fatalf("batch=8 时低水位应为 2，得到 %d", got)
	}

	var got []uint64
	for i := 0; i < 8; i++ {
		v, err := c.Get()
		if err != nil {
			t.Fatalf("第 %d 次取号失败: %v", i+1, err)
		}
		got = append(got, v)
	}
	// 消费到低水位后应触发一次后台预取并挂到 pend（尚未消费）。
	waitFor(t, "预取批就绪", func() bool {
		ok, _ := c.PrefetchStats()
		return ok >= 1
	})
	c.mu.Lock()
	pendBase, pendCount, maxSeen := c.pendBase, c.pendCount, c.maxSeen
	c.mu.Unlock()
	if pendCount != 8 || pendBase != 101 {
		t.Fatalf("预取批应为 [101,108]，得到 base=%d count=%d", pendBase, pendCount)
	}
	if maxSeen != 108 {
		t.Fatalf("预取批应推进 maxSeen 到 108，得到 %d", maxSeen)
	}

	// 池耗尽后无缝切到预取批继续取号：序号连续且严格递增。
	for i := 0; i < 4; i++ {
		v, err := c.Get()
		if err != nil {
			t.Fatalf("切换预取批后第 %d 次取号失败: %v", i+1, err)
		}
		got = append(got, v)
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("时间戳必须严格递增：%v", got)
		}
	}
	if got[8] != 101 {
		t.Fatalf("第 9 个号应来自预取批起点 101，得到 %d", got[8])
	}
	// 不越界：单轮消费（8+4 个号、跨两个批次）最多触发 1 次预取（在飞标志去重）。
	if n := f.callCount(); n > 3 {
		t.Fatalf("预取调用次数过多（%d），存在重复补拉", n)
	}
}

// TestNew5PrefetchAbsorbsJitter 抖动期间剩余号段仍可消耗，池耗尽才报错。
func TestNew5PrefetchAbsorbsJitter(t *testing.T) {
	f := &seqFetcher{size: 8, errAfter: 1} // 首次拉批成功，此后远端不可达
	c := &tsoClient{nodeID: "node-a", batch: 8, fetchHook: f.one}

	// 先用掉 3 个号（剩余 5 > 低水位 2，此时不该触发预取）。
	for i := 0; i < 3; i++ {
		if _, err := c.Get(); err != nil {
			t.Fatalf("抖动前取号失败: %v", err)
		}
	}
	if ok, fails := c.PrefetchStats(); ok != 0 || fails != 0 {
		t.Fatalf("剩余量高于低水位时不应预取，得到 ok=%d fails=%d", ok, fails)
	}

	// 继续消费触发预取；预取失败只计数，不影响当前请求。
	for i := 0; i < 5; i++ {
		if _, err := c.Get(); err != nil {
			t.Fatalf("抖动期间第 %d 次取号失败（租约兜底未生效）: %v", i+1, err)
		}
	}
	waitFor(t, "预取失败计数", func() bool {
		_, fails := c.PrefetchStats()
		return fails >= 1
	})
	c.mu.Lock()
	pend := c.pendCount
	c.mu.Unlock()
	if pend != 0 {
		t.Fatalf("远端不可达时不应挂上预取批，得到 pendCount=%d", pend)
	}

	// 池真正耗尽：此时才允许报错（单点语义不变）。
	if _, err := c.Get(); err == nil {
		t.Fatalf("号池耗尽且远端不可达时应返回错误")
	}
}

// TestNew5MaxSeenRegressionGuard 回退保护：起点必须严格大于 maxSeen。
func TestNew5MaxSeenRegressionGuard(t *testing.T) {
	// a) 预取批验收路径。
	c := &tsoClient{nodeID: "node-a", batch: 8, maxSeen: 108}
	if err := c.acceptPrefetchedLocked(50, 8); err == nil {
		t.Fatalf("起点 50 <= maxSeen 108，预取批应被拒绝")
	}
	c.mu.Lock()
	pend := c.pendCount
	c.mu.Unlock()
	if pend != 0 {
		t.Fatalf("被拒绝的预取批不得挂到 pend，得到 %d", pend)
	}
	// 起点恰好等于 maxSeen+1 才被接受。
	if err := c.acceptPrefetchedLocked(109, 8); err != nil {
		t.Fatalf("起点 109 > maxSeen 108 应被接受: %v", err)
	}

	// b) 同步拉批路径：远端回退时 Get 返回错误而非发放重复号。
	f := &seqFetcher{size: 8, regressAt: 2}
	c2 := &tsoClient{nodeID: "node-a", batch: 8, fetchHook: f.one}
	for i := 0; i < 8; i++ {
		if _, err := c2.Get(); err != nil {
			t.Fatalf("首批取号失败: %v", err)
		}
	}
	// 第 2 次调用返回起点 1（回退响应），小于 maxSeen=8 → 应被拒绝。
	if _, err := c2.Get(); err == nil {
		t.Fatalf("同步拉批回退时应返回错误（start <= maxSeen）")
	}
	c2.mu.Lock()
	maxSeen := c2.maxSeen
	c2.mu.Unlock()
	if maxSeen != 8 {
		t.Fatalf("回退响应不得改写 maxSeen，得到 %d", maxSeen)
	}
}

// TestNew5PrefetchBackoff 退避时长单调且封顶。
func TestNew5PrefetchBackoff(t *testing.T) {
	prev := time.Duration(0)
	for attempt := 0; attempt <= 10; attempt++ {
		d := prefetchBackoff(attempt)
		if d < tsoPrefetchBackoffMin || d > tsoPrefetchBackoffMax {
			t.Fatalf("attempt=%d 退避 %v 越界 [%v,%v]", attempt, d, tsoPrefetchBackoffMin, tsoPrefetchBackoffMax)
		}
		if attempt > 0 && d < prev {
			t.Fatalf("退避应单调不减：attempt=%d 得到 %v < %v", attempt, d, prev)
		}
		prev = d
	}
}

// TestNew5HAPrefetch HA 客户端具备同等预取能力，且池耗尽语义不变。
func TestNew5HAPrefetch(t *testing.T) {
	f := &seqFetcher{size: 8}
	c := &HATSOClient{
		group:     []string{"node-a"},
		batch:     8,
		leader:    "node-a",
		fetchHook: f.fetch,
	}
	if got := c.lowWatermark(); got != 2 {
		t.Fatalf("batch=8 时低水位应为 2，得到 %d", got)
	}

	var got []uint64
	for i := 0; i < 8; i++ {
		v, err := c.Get()
		if err != nil {
			t.Fatalf("第 %d 次取号失败: %v", i+1, err)
		}
		got = append(got, v)
	}
	waitFor(t, "HA 预取批就绪", func() bool {
		ok, _ := c.PrefetchStats()
		return ok >= 1
	})
	for i := 0; i < 3; i++ {
		v, err := c.Get()
		if err != nil {
			t.Fatalf("HA 切换预取批后第 %d 次取号失败: %v", i+1, err)
		}
		got = append(got, v)
	}
	if got[8] != 101 {
		t.Fatalf("第 9 个号应来自预取批起点 101，得到 %d", got[8])
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("HA 时间戳必须严格递增：%v", got)
		}
	}

	// 池耗尽 + 主不可达 + 组内无候选：返回 (0,0)（单点/HA 一致的失败语义）。
	// 装一个空的 Manager：候选节点未注册，选主必失败且不会空指针。
	f.err = errors.New("dial tcp 10.0.0.9:7788: connect: connection refused")
	c.mgr = NewManager("node-a")
	c.mu.Lock()
	c.count, c.pendCount = 0, 0
	c.mu.Unlock()
	if start, n := c.GetBatch(1); start != 0 || n != 0 {
		t.Fatalf("无可用主时应返回 (0,0)，得到 (%d,%d)", start, n)
	}
}

// TestNew5ConcurrentGetNoDup 并发取号：不重复、不跳号、不死锁。
func TestNew5ConcurrentGetNoDup(t *testing.T) {
	f := &seqFetcher{size: 8}
	c := &tsoClient{nodeID: "node-a", batch: 8, fetchHook: f.one}

	const workers, per = 8, 40
	var (
		mu    sync.Mutex
		all   = make([]uint64, 0, workers*per)
		wg    sync.WaitGroup
		fatal error
	)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				v, err := c.Get()
				if err != nil {
					mu.Lock()
					if fatal == nil {
						fatal = err
					}
					mu.Unlock()
					return
				}
				mu.Lock()
				all = append(all, v)
				mu.Unlock()
			}
		}()
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("并发取号超时（疑似死锁）")
	}
	if fatal != nil {
		t.Fatalf("并发取号失败: %v", fatal)
	}
	seen := make(map[uint64]struct{}, len(all))
	for _, v := range all {
		if _, dup := seen[v]; dup {
			t.Fatalf("时间戳重复发放: %d", v)
		}
		seen[v] = struct{}{}
	}
	if len(all) != workers*per {
		t.Fatalf("取号数量应为 %d，得到 %d", workers*per, len(all))
	}
}

// TestNew5SourceInterfaceUnchanged 预取改造不得改变 tso.Source 契约。
func TestNew5SourceInterfaceUnchanged(t *testing.T) {
	var _ tso.Source = (*tsoClient)(nil)
	var _ tso.Source = (*HATSOClient)(nil)
}

package cluster

// v2.1.0 H1（NEW-5）：TSO 租约兜底与失效切换的单元测试。
//
// 缺陷背景（v1.21.0 Phase A 三机真机复验 §10.4）：TSO 是写路径上的硬单点。
// 停掉持有发号权的节点后全集群写失败（tso unavailable），恢复只能人工改
// --tso-node。epoch 选主解决了"客户端知道换谁"，但没有解决"何时可以安全
// 换人"——网络分区/半失联下旧主仍可能继续发号，造成重号。
//
// 本文件覆盖（对应用户验收点）：
//   a) 租约过期：主失联/续约失败后，租约到期即自行降级、拒绝发号；
//   b) 抢主：多数派授予后才当选，接任批次严格越过已观测最大戳；
//   c) 号段回退拒绝：客户端 acceptLocked 与服务端 last 提升双重护栏；
//   d) 双主互斥：租约窗口内任何其它候选都拿不到多数派（分区两侧不同主）；
//   e) 开关默认关闭时行为与 v1.21.0 完全一致（无租约校验）。
//
// 全部用例通过 voteHook/renewHook + 假时钟注入，不依赖真实网络、真实时钟
// 与节点注册表，因此可在 CI 稳定复现"租约过期/分区/续约失败"等时序。

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// leaseClock 可手动推进的假时钟：租约判定只用"本地时长"，因此假时钟
// 足以精确验证过期语义（无需 sleep，也不受机器负载影响）。
type leaseClock struct {
	mu sync.Mutex
	t  time.Time
}

func newLeaseClock() *leaseClock {
	return &leaseClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *leaseClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *leaseClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// leaseNet 内存版组内链路：模拟投票/续约帧的投递与可达性（分区）。
type leaseNet struct {
	ids    []string
	nodes  map[string]*tsoHAState
	clk    *leaseClock
	ttl    time.Duration
	mu     sync.Mutex
	denied map[string]map[string]bool // denied[from][to]=true 表示 from 到 to 不可达
}

func newLeaseNet(ttl time.Duration, ids ...string) *leaseNet {
	n := &leaseNet{
		ids:    ids,
		nodes:  make(map[string]*tsoHAState, len(ids)),
		clk:    newLeaseClock(),
		ttl:    ttl,
		denied: make(map[string]map[string]bool, len(ids)),
	}
	for _, id := range ids {
		n.nodes[id] = n.newState(id, true)
	}
	return n
}

// newState 构造一个节点状态机；withLease=false 模拟"开关关闭"的旧行为。
func (n *leaseNet) newState(id string, withLease bool) *tsoHAState {
	st := &tsoHAState{
		selfID:    id,
		group:     append([]string(nil), n.ids...),
		now:       n.clk.Now,
		renewStop: make(chan struct{}),
	}
	if withLease {
		st.enableLease(n.ttl, 0)
	}
	// 测试用假时钟，必须关掉后台续约协程（它按真实 ticker 驱动），
	// 续约一律由用例显式调用 renewOnce 触发，保证时序可复现。
	st.renewStop = nil
	from := id
	st.voteHook = func(to string, req TsoElectReq) (TsoElectResp, error) {
		if !n.reachable(from, to) {
			return TsoElectResp{}, errors.New("partition: unreachable")
		}
		tgt, ok := n.nodes[to]
		if !ok || tgt == nil {
			return TsoElectResp{}, errors.New("partition: unknown node")
		}
		return tgt.vote(req.Epoch, req.Observed, req.Candidate), nil
	}
	st.renewHook = func(to string, req TsoLeaseRenewReq) (TsoLeaseRenewResp, error) {
		if !n.reachable(from, to) {
			return TsoLeaseRenewResp{}, errors.New("partition: unreachable")
		}
		tgt, ok := n.nodes[to]
		if !ok || tgt == nil {
			return TsoLeaseRenewResp{}, errors.New("partition: unknown node")
		}
		return tgt.applyRenew(req.Epoch, req.Leader), nil
	}
	return st
}

func (n *leaseNet) reachable(from, to string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if m, ok := n.denied[from]; ok && m[to] {
		return false
	}
	return true
}

// isolate 把 group 切成若干互不可达的分区（同分区内可通）。
func (n *leaseNet) isolate(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.denied = make(map[string]map[string]bool, len(n.ids))
	sideOf := make(map[string]int, len(n.ids))
	for i, g := range groups {
		for _, id := range g {
			sideOf[id] = i
		}
	}
	for _, from := range n.ids {
		n.denied[from] = make(map[string]bool, len(n.ids))
		for _, to := range n.ids {
			if from == to {
				continue
			}
			if sideOf[from] != sideOf[to] {
				n.denied[from][to] = true
			}
		}
	}
}

func (n *leaseNet) heal() {
	n.mu.Lock()
	n.denied = make(map[string]map[string]bool, len(n.ids))
	n.mu.Unlock()
}

func (n *leaseNet) st(id string) *tsoHAState { return n.nodes[id] }

// issue 断言该节点当前可发号，返回分配到的号段 [start, start+count)。
func issue(t *testing.T, st *tsoHAState, want uint64) (uint64, uint64) {
	t.Helper()
	start, count, ok, _ := st.serve(want)
	if !ok {
		t.Fatalf("node %s: expected to serve but refused", st.selfID)
	}
	if count == 0 {
		t.Fatalf("node %s: served empty batch", st.selfID)
	}
	return start, count
}

// TestTSOLeaseVoteAndMonotonic 抢主：多数派授予后当选，首批发号越过观测值。
func TestTSOLeaseVoteAndMonotonic(t *testing.T) {
	n := newLeaseNet(10*time.Second, "a", "b", "c")
	a := n.st("a")

	resp := a.runElect(1, 500)
	if !resp.Granted {
		t.Fatalf("a should be granted with 3-node majority, got err=%q", resp.Err)
	}
	if resp.Epoch != 1 {
		t.Fatalf("epoch = %d, want 1", resp.Epoch)
	}
	if resp.LeaseMs != 10000 {
		t.Fatalf("lease ms = %d, want 10000", resp.LeaseMs)
	}

	// 号段单调不减：接任时 last 提升到调用方观测值（500），首批发号必须更大。
	start, count := issue(t, a, 8)
	if count != 8 {
		t.Fatalf("count = %d, want 8", count)
	}
	if start <= 500 {
		t.Fatalf("first batch start = %d, must be > observed 500 (号段回退拒绝)", start)
	}

	// 租约窗口内其它候选不得当选（双主互斥）。
	b := n.st("b")
	br := b.runElect(1, 0)
	if br.Granted {
		t.Fatal("b must not be granted while a holds a valid lease (double leader)")
	}
	if br.RetryAfterMs <= 0 {
		t.Fatalf("b should be told to retry after lease expiry, got retry_after=%d", br.RetryAfterMs)
	}
	if _, _, ok, _ := b.serve(1); ok {
		t.Fatal("b must not serve after being rejected")
	}
}

// TestTSOLeaseExpireAndTakeover 租约过期与失效切换：旧主到期自降，新主接续。
func TestTSOLeaseExpireAndTakeover(t *testing.T) {
	n := newLeaseNet(10*time.Second, "a", "b", "c")
	a, b := n.st("a"), n.st("b")

	if resp := a.runElect(1, 0); !resp.Granted {
		t.Fatalf("a should be granted: %s", resp.Err)
	}
	start, count := issue(t, a, 8)
	maxSeen := start + count - 1 // 客户端已消费到的最大戳

	// 未到期：仍可发号。
	n.clk.Advance(9 * time.Second)
	issue(t, a, 8)
	// 旧主持续发号，客户端观测值随之前推（模拟客户端 maxSeen 前进）。
	maxSeen += 8

	// 越过 TTL：旧主租约自然过期，自行降级且拒绝发号（不依赖任何 fence）。
	n.clk.Advance(2 * time.Second)
	if _, _, ok, _ := a.serve(8); ok {
		t.Fatal("expired lease must refuse to serve")
	}
	if _, leader, _, _, _ := a.snapshotForTest(); leader {
		t.Fatal("expired leader must demote itself")
	}

	// 新主接续：拿多数派租约，首批发号严格越过客户端已观测到的最大戳。
	resp := b.runElect(2, maxSeen)
	if !resp.Granted {
		t.Fatalf("b should take over after a's lease expired: %s", resp.Err)
	}
	if resp.Epoch != 2 {
		t.Fatalf("takeover epoch = %d, want 2", resp.Epoch)
	}
	nstart, _ := issue(t, b, 8)
	if nstart <= maxSeen {
		t.Fatalf("takeover batch start = %d, must be > maxSeen %d (跨节点不重号)", nstart, maxSeen)
	}
}

// TestTSOLeaseTakeoverAboveGroupWatermark 跨节点不重号（调用方水位落后的场景）。
//
// 客户端刚启动时 maxSeen 可能为 0，若新主只按调用方 observed 抬 last，
// 首批号会落到旧主已发过的区间下方 → 重号。租约模式必须以"投票回执里
// 各副本的 last"为水位：新主首批号必须严格大于旧主发过的最大号。
func TestTSOLeaseTakeoverAboveGroupWatermark(t *testing.T) {
	n := newLeaseNet(10*time.Second, "a", "b", "c")
	a := n.st("a")

	// a 当选并连发两批，把群内水位推到 a 的 last。
	a.runElect(1, 0)
	_, c1 := issue(t, a, 8)
	_, c2 := issue(t, a, 8)
	n.clk.Advance(11 * time.Second) // a 失联：租约自然过期，无人续约
	a.mu.Lock()
	oldLast := a.last
	a.mu.Unlock()
	if oldLast == 0 {
		t.Fatal("old leader must have advanced last")
	}
	if _, _, ok, _ := a.serve(1); ok {
		t.Fatal("expired leader must not serve")
	}

	// b 被一个 observed=0 的新客户端拉票接任（客户端完全不知道旧水位）。
	b := n.st("b")
	if resp := b.runElect(2, 0); !resp.Granted {
		t.Fatalf("b should take over after lease expiry: %s", resp.Err)
	}
	_ = c1
	_ = c2
	start, count := issue(t, b, 1)
	if start <= oldLast {
		t.Fatalf("new leader first ts %d must be above old watermark %d", start, oldLast)
	}
	if count == 0 {
		t.Fatal("empty batch")
	}
}

// TestTSOLeaseRenewAndRenewFailStepDown 续约成功延长租约；续约失败到期退位。
func TestTSOLeaseRenewAndRenewFailStepDown(t *testing.T) {
	n := newLeaseNet(10*time.Second, "a", "b", "c")
	a := n.st("a")
	if resp := a.runElect(1, 0); !resp.Granted {
		t.Fatalf("a should be granted: %s", resp.Err)
	}

	// 到期前续约成功：租约被延长（续约窗口从本轮发起时刻起算）。
	n.clk.Advance(9 * time.Second)
	if !a.renewOnce() {
		t.Fatal("renew with quorum reachable should succeed")
	}
	n.clk.Advance(5 * time.Second) // 累计 14s，超过原始 TTL(10s) 但未超续约后的 19s
	issue(t, a, 1)

	// 续约失败（组内不可达）：本轮不延长，租约到期后自行退位。
	n.isolate([]string{"a"}, []string{"b", "c"})
	n.clk.Advance(4 * time.Second) // 累计 18s
	if a.renewOnce() {
		t.Fatal("renew without quorum must fail")
	}
	n.clk.Advance(2 * time.Second) // 累计 20s，越过 19s 的租约终点
	if _, _, ok, _ := a.serve(1); ok {
		t.Fatal("leader must step down after renew failure and lease expiry")
	}
}

// TestTSOLeaseNoDoubleLeaderUnderPartition 双主互斥：分区两侧不会各出一主。
func TestTSOLeaseNoDoubleLeaderUnderPartition(t *testing.T) {
	n := newLeaseNet(10*time.Second, "a", "b", "c", "d", "e")
	// 分区：{a,b} 与 {c,d,e}。少数派侧拿不到多数派，无法自封主。
	n.isolate([]string{"a", "b"}, []string{"c", "d", "e"})

	a := n.st("a")
	if resp := a.runElect(1, 0); resp.Granted {
		t.Fatal("minority side must not win a lease (would split brain)")
	}
	if _, _, ok, _ := a.serve(1); ok {
		t.Fatal("minority side must not serve")
	}

	c := n.st("c")
	cresp := c.runElect(1, 0)
	if !cresp.Granted {
		t.Fatalf("majority side should win the lease: %s", cresp.Err)
	}
	issue(t, c, 4)

	// 少数派侧在租约窗口内再次竞逐：仍必须被拒（他主租约未到期）。
	if resp := a.runElect(2, 0); resp.Granted {
		t.Fatal("a must not win while c's lease is still valid")
	}

	// 分区恢复后，新纪元仍需等 c 的租约过期才能接任——窗口内一律拒绝。
	n.heal()
	if resp := a.runElect(3, 0); resp.Granted {
		t.Fatal("a must not win before c's lease expires after heal")
	}
	// 租约到期后允许接任，且号段继续单调。
	maxSeen := uint64(0)
	s, cnt, ok, _ := c.serve(4)
	if !ok {
		t.Fatal("c should still serve inside its lease")
	}
	maxSeen = s + cnt - 1
	n.clk.Advance(11 * time.Second)
	resp := a.runElect(4, maxSeen)
	if !resp.Granted {
		t.Fatalf("a should take over after c's lease expired: %s", resp.Err)
	}
	astart, _ := issue(t, a, 4)
	if astart <= maxSeen {
		t.Fatalf("takeover start = %d must be > %d", astart, maxSeen)
	}
}

// TestTSOLeaseQuorumFailureRollsBackSelfVote 拉票失败不留自锁：失败后其它候选仍可当选。
func TestTSOLeaseQuorumFailureRollsBackSelfVote(t *testing.T) {
	n := newLeaseNet(10*time.Second, "a", "b", "c", "d", "e")
	a := n.st("a")
	n.isolate([]string{"a"}, []string{"b", "c", "d", "e"})

	resp := a.runElect(1, 0)
	if resp.Granted {
		t.Fatal("a must not be granted without quorum")
	}
	if !strings.Contains(resp.Err, "quorum") {
		t.Fatalf("err = %q, want quorum failure", resp.Err)
	}
	if _, leader, _, _, _ := a.snapshotForTest(); leader {
		t.Fatal("failed candidate must not stay leader")
	}

	// 失败候选撤销了自投，不能把自己锁成"他主持有租约"：
	// 分区恢复后另一个候选可正常当选。
	n.heal()
	c := n.st("c")
	if cresp := c.runElect(2, 0); !cresp.Granted {
		t.Fatalf("c should be granted after a's rollback: %s", cresp.Err)
	}
}

// TestTSOLeaseDisabledKeepsLegacyBehavior 开关默认关闭：行为与 v1.21.0 一致。
func TestTSOLeaseDisabledKeepsLegacyBehavior(t *testing.T) {
	n := newLeaseNet(10*time.Second, "a", "b", "c")
	a := n.newState("a", false) // 未调用 enableLease（开关关闭）

	if resp := a.vote(1, 0, "a"); resp.Err == "" || !strings.Contains(resp.Err, "not enabled") {
		t.Fatalf("vote without lease must be rejected, got %+v", resp)
	}
	// 关闭时不看租约：epoch 选主直接授予并可发号（旧语义），且授予结果不带租约。
	resp := a.runElect(1, 0)
	if !resp.Granted {
		t.Fatalf("legacy elect should grant: %s", resp.Err)
	}
	if resp.LeaseMs != 0 {
		t.Fatalf("lease ms = %d, want 0 when lease disabled", resp.LeaseMs)
	}
	start, _ := issue(t, a, 4)
	if start == 0 {
		t.Fatal("legacy path must still allocate")
	}
	// 关闭租约后不存在"过期降级"：时间推进不影响发号。
	n.clk.Advance(time.Hour)
	issue(t, a, 4)
}

// TestTSOLeaseFenceRecordsNewLeaderLease 降级通告携带租约：窗口内本地竞逐被拒。
func TestTSOLeaseFenceRecordsNewLeaderLease(t *testing.T) {
	n := newLeaseNet(10*time.Second, "a", "b", "c")
	a := n.st("a")
	// b 收到"c 在 epoch 3 持有 10s 租约"的降级通告。
	a.commitLease(3, 42, "c", 10000)
	if a.epoch != 3 {
		t.Fatalf("epoch = %d, want 3", a.epoch)
	}
	if a.last != 42 {
		t.Fatalf("last = %d, want 42 (fence 需抬高本地序列)", a.last)
	}
	if resp := a.runElect(3, 0); resp.Granted {
		t.Fatal("a must not win while c's advertised lease is valid")
	}
	n.clk.Advance(11 * time.Second)
	resp := a.runElect(4, 42)
	if !resp.Granted {
		t.Fatalf("a should win after advertised lease expires: %s", resp.Err)
	}
	if start, _ := issue(t, a, 2); start <= 42 {
		t.Fatalf("start = %d, want > 42", start)
	}
}

// TestTSOLeaseClientRollbackGuard 号段回退拒绝（客户端护栏未被改动）。
func TestTSOLeaseClientRollbackGuard(t *testing.T) {
	c := &HATSOClient{batch: 8}
	c.maxSeen = 100
	if err := c.acceptLocked(50, 10); err == nil {
		t.Fatal("client must reject a batch starting below maxSeen")
	}
	if err := c.acceptLocked(101, 10); err != nil {
		t.Fatalf("client should accept monotonic batch: %v", err)
	}
}

// TestTSOLeaseClientModeSwitch 客户端租约开关默认关闭、按需开启。
func TestTSOLeaseClientModeSwitch(t *testing.T) {
	c := &HATSOClient{batch: 8}
	if c.LeaseMode() {
		t.Fatal("lease mode must default to off")
	}
	c.SetLeaseMode(0)
	if !c.LeaseMode() {
		t.Fatal("SetLeaseMode should enable lease mode")
	}
	if c.leaseTTL != DefaultTSOLeaseTTL {
		t.Fatalf("ttl = %s, want default %s", c.leaseTTL, DefaultTSOLeaseTTL)
	}
	if got, want := c.leaseWaitBudgetLocked(), DefaultTSOLeaseTTL*3/2; got != want {
		t.Fatalf("wait budget = %s, want %s", got, want)
	}
	// nil 客户端不 panic（旧部署/未装配 HA 的兼容路径）。
	var nilClient *HATSOClient
	nilClient.SetLeaseMode(time.Second)
	if nilClient.LeaseMode() || nilClient.LeaseWaitCount() != 0 {
		t.Fatal("nil client must be inert")
	}
	// 回执解析：仅他主租约拒绝才给出等待时长。
	if d := leaseRetryAfter(TsoElectResp{RetryAfterMs: 1500}); d != 1500*time.Millisecond {
		t.Fatalf("retry after = %s, want 1.5s", d)
	}
	if d := leaseRetryAfter(TsoElectResp{Err: "no quorum"}); d != 0 {
		t.Fatalf("retry after = %s, want 0", d)
	}
}

// TestTSOLeaseServerEnableRequiresHA 服务端租约开关依赖 HA 装配（默认关闭不再报错）。
func TestTSOLeaseServerEnableRequiresHA(t *testing.T) {
	s := &Server{}
	if err := s.EnableTSOLease(0, 0); err == nil {
		t.Fatal("EnableTSOLease without EnableTSOHA must fail fast")
	}
	if s.TSOLeaseTTL() != 0 {
		t.Fatal("lease ttl must be 0 when not enabled")
	}
	if on, _, _, _, _ := s.TSOLeaseSnapshot(); on {
		t.Fatal("lease snapshot must report disabled")
	}
}

// snapshotForTest 读取状态机快照（测试内部使用，避免直接触碰字段）。
func (st *tsoHAState) snapshotForTest() (on, leader bool, remain, otherMs int64, other string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.leaseOn {
		return false, st.leader, 0, 0, ""
	}
	now := st.nowLocked()
	holder, remainOther := st.otherLeaseLocked(now)
	return true, st.leaseValidLocked(now), st.leaseRemainLocked(now).Milliseconds(), remainOther.Milliseconds(), holder
}

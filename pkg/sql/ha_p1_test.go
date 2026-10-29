package sql

// P1 HA 防路由分裂（集群轨）测试：基于 P0（epoch/quorum）机制，
// 验证 region 重指派两步校验（持有校验 + epoch 校验）与 quorum 前置，
// 保证分区双端并发重指派至多一端成功、陈旧 failover 条目失效、
// 开关默认关闭零回归。
//
// 用例覆盖（docs/HA_BRAIN_SPLIT_DESIGN.md §6 T6/T7/T8/T9 的 P1 部分）：
//   - TestP1CheckPushEpoch：陈旧 clusterEpoch/regionEpoch 消息拒绝；
//     旧节点消息（clusterEpoch=0）跳过校验；开关关闭放行。
//   - TestP1RegionEpochSnapshot：regionEpoch 递增 + 本机快照大者胜 +
//     staleFailover 陈旧判定。
//   - TestP1DisabledZeroRegression：未装配 P1（M6 旧行为）零回归。
//   - TestP1QuorumGateFailover：quorum 不足降级只读不重指派，且不置
//     failoverDone（允许恢复后重试）。
//   - TestP1HoldCheckSkipsCandidate：持有校验不通过的候选被跳过，
//     尝试下一候选；全部未持有则放弃且不污染路由。
//   - TestP1ConcurrentFailoverOnlyOneWins：双端并发重指派（共享单调
//     epoch 源），quorum 失败端被拒，至多一端成功。
//   - TestP1StaleResolveOwner：resolveRegionOwner 识别陈旧 failover
//     条目并失效（不采用陈旧路由，回退只读/重指派）。

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// fakeEpochSrc P1 测试用 clusterEpoch 取号源：本地单调递增（可共享，
// 模拟全局唯一发号源；known 为当前已知最大 epoch）。
type fakeEpochSrc struct {
	mu       sync.Mutex
	known    uint64
	acquired int
}

func (f *fakeEpochSrc) Epoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.known
}

func (f *fakeEpochSrc) AcquireEpoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquired++
	f.known++
	return f.known
}

// fakeQuorum P1 测试用 quorum 仲裁器：返回注入错误（nil = 多数派可达）。
type fakeQuorum struct{ err error }

func (f *fakeQuorum) QuorumOK() error { return f.err }

// fakeCfg P1 测试用 HA 开关控制器（实现 HAConfigSetter）。
type fakeCfg struct {
	epoch  bool
	quorum bool
}

func (f *fakeCfg) SetEpochEnabled(on bool)  { f.epoch = on }
func (f *fakeCfg) SetQuorumEnabled(on bool) { f.quorum = on }
func (f *fakeCfg) EpochEnabled() bool       { return f.epoch }
func (f *fakeCfg) QuorumEnabled() bool      { return f.quorum }

// p1TxnMgr P1 测试用 TxnManager：Begin 返回带固定行的扫描事务
// （模拟本节点持有 region 键空间 1 行，使数据源选择/落盘可离线通过）。
type p1TxnMgr struct {
	rows []storage.KVPair
}

func (m *p1TxnMgr) Begin() (txn.Txn, error) { return &p1TxnT{rows: m.rows}, nil }
func (m *p1TxnMgr) Recover() error          { return nil }
func (m *p1TxnMgr) SetCommitHook(func(lsn storage.LSN, seq uint64, batch *storage.WriteBatch) error) {
}
func (m *p1TxnMgr) SetCommitWaitHook(func(lsn storage.LSN, seq uint64) error) {}
func (m *p1TxnMgr) HookErr() error                                           { return nil }
func (m *p1TxnMgr) FlushDeferredSync() error                               { return nil }
func (m *p1TxnMgr) SetGroupCommitWindow(time.Duration)                        {}
func (m *p1TxnMgr) Close() error                                             { return nil }

type p1TxnT struct{ rows []storage.KVPair }

func (t *p1TxnT) ID() uint64                                                   { return 1 }
func (t *p1TxnT) Get(key []byte) ([]byte, error)                               { return nil, storage.ErrNotFound }
func (t *p1TxnT) MGet(keys [][]byte) (map[string][]byte, error)                { return nil, nil }
func (t *p1TxnT) Scan(r storage.KeyRange, limit int) ([]storage.KVPair, error) { return t.rows, nil }
func (t *p1TxnT) Put(key, value []byte) error                                  { return nil }
func (t *p1TxnT) Delete(key []byte) error                                      { return nil }
func (t *p1TxnT) Commit() error                                                { return nil }
func (t *p1TxnT) Rollback() error                                              { return nil }
func (t *p1TxnT) SetDeferSync(bool)                                            {}

// newP1Executor 构造启用 P1 的测试执行器（同包白盒：直接装配字段，
// 不依赖真实存储/网络；持有校验默认注入可控钩子）。
func newP1Executor(src *fakeEpochSrc, qa *fakeQuorum, cfg *fakeCfg, selfID string) *Executor {
	e := NewExecutor(nil)
	e.epochSrc = src
	e.quorumArb = qa
	e.haCfg = cfg
	e.selfID = selfID
	e.mgr = cluster.NewManager(selfID)
	e.router = sharding.NewRouter()
	return e
}

// regionOf 构造测试 region 路由条目。
func regionOf(rid storage.ID, node string, regionEpoch uint64) sharding.RegionInfo {
	return sharding.RegionInfo{
		RegionID:    rid,
		StartKey:    []byte("a"),
		EndKey:      []byte("z"),
		Node:        node,
		Local:       false,
		RegionEpoch: regionEpoch,
	}
}

// TestP1CheckPushEpoch 陈旧 epoch 消息拒绝（T6）：
//   - clusterEpoch 落后于本机已知最大 → 拒绝；
//   - regionEpoch 落后于本机快照 → 拒绝；
//   - 消息不带 epoch（clusterEpoch=0，旧节点/旧路径）→ 跳过校验（版本协商防呆）；
//   - 开关关闭 / 未装配 → 一律放行（零回归）。
func TestP1CheckPushEpoch(t *testing.T) {
	src := &fakeEpochSrc{known: 5}
	cfg := &fakeCfg{epoch: true}
	e := newP1Executor(src, &fakeQuorum{}, cfg, "node-a")

	// clusterEpoch 陈旧：3 < known 5 → 拒绝。
	if err := e.checkRegionPushEpoch(3, 0, 1); err == nil {
		t.Fatal("stale clusterEpoch=3 should be rejected")
	}
	// clusterEpoch 新鲜：6 >= 5 → 放行。
	if err := e.checkRegionPushEpoch(6, 0, 1); err != nil {
		t.Fatalf("fresh clusterEpoch=6 should pass: %v", err)
	}
	// regionEpoch 陈旧：本机快照 3 > 消息 2 → 拒绝。
	e.p1Epoch[1] = p1RegionEpoch{clusterEpoch: 6, regionEpoch: 3}
	if err := e.checkRegionPushEpoch(6, 2, 1); err == nil {
		t.Fatal("stale regionEpoch=2 should be rejected")
	}
	// regionEpoch 相等：3 == 3 → 放行。
	if err := e.checkRegionPushEpoch(6, 3, 1); err != nil {
		t.Fatalf("equal regionEpoch=3 should pass: %v", err)
	}
	// 旧节点消息（clusterEpoch=0）→ 跳过校验。
	if err := e.checkRegionPushEpoch(0, 2, 1); err != nil {
		t.Fatalf("legacy message (clusterEpoch=0) should pass: %v", err)
	}
	// 开关关闭 → 放行。
	e2 := newP1Executor(src, &fakeQuorum{}, &fakeCfg{epoch: false}, "node-a")
	if err := e2.checkRegionPushEpoch(3, 0, 1); err != nil {
		t.Fatalf("epoch disabled should pass: %v", err)
	}
	// 未装配（epochSrc nil）→ 放行。
	e3 := NewExecutor(nil)
	if err := e3.checkRegionPushEpoch(3, 0, 1); err != nil {
		t.Fatalf("unwired should pass: %v", err)
	}
}

// TestP1RegionEpochSnapshot regionEpoch 递增 + 快照大者胜 + 陈旧判定。
func TestP1RegionEpochSnapshot(t *testing.T) {
	src := &fakeEpochSrc{known: 1}
	cfg := &fakeCfg{epoch: true}
	e := newP1Executor(src, &fakeQuorum{}, cfg, "node-a")

	// 首次重指派：regionEpoch 0 -> 1。
	if got := e.nextRegionEpoch(1, 1); got != 1 {
		t.Fatalf("first bump = %d, want 1", got)
	}
	// 再次重指派：1 -> 2。
	if got := e.nextRegionEpoch(1, 2); got != 2 {
		t.Fatalf("second bump = %d, want 2", got)
	}
	// 快照大者胜：旧 epoch 记录不覆盖新值。
	e.recordRegionEpoch(1, 2, 2)
	e.recordRegionEpoch(1, 1, 1)
	if got := e.regionEpochOf(1); got != 2 {
		t.Fatalf("regionEpochOf = %d, want 2 (max wins)", got)
	}

	// staleFailover：
	//   - 路由条目 epoch 落后于快照 → 陈旧。
	r := regionOf(1, "node-b", 1)
	if !e.staleFailover(r) {
		t.Fatal("region epoch 1 < snapshot 2 should be stale")
	}
	//   - 条目 epoch 等于快照 → 新鲜。
	r.RegionEpoch = 2
	if e.staleFailover(r) {
		t.Fatal("region epoch 2 == snapshot 2 should be fresh")
	}
	//   - 无快照 → 不陈旧（兼容首次学习）。
	e2 := newP1Executor(src, &fakeQuorum{}, cfg, "node-a")
	if e2.staleFailover(regionOf(2, "node-b", 0)) {
		t.Fatal("no snapshot should not be stale")
	}
}

// TestP1DisabledZeroRegression 开关关闭 / 未装配零回归（T8）。
func TestP1DisabledZeroRegression(t *testing.T) {
	// 完全未装配（M6 旧行为）：
	e := NewExecutor(nil)
	// FailoverDownNode 无集群 → 直接返回 0（不 panic、不报错）。
	if moved, err := e.FailoverDownNode("node-b"); err != nil || moved != 0 {
		t.Fatalf("unwired FailoverDownNode = (%d,%v), want (0,nil)", moved, err)
	}
	// checkRegionPushEpoch 放行。
	if err := e.checkRegionPushEpoch(9, 9, 1); err != nil {
		t.Fatalf("unwired check should pass: %v", err)
	}
	// staleFailover 恒不陈旧。
	if e.staleFailover(regionOf(1, "node-b", 0)) {
		t.Fatal("unwired staleFailover should be false")
	}

	// 装配但 epoch 开关关闭：
	e2 := newP1Executor(&fakeEpochSrc{known: 3}, &fakeQuorum{}, &fakeCfg{epoch: false}, "node-a")
	if err := e2.checkRegionPushEpoch(1, 0, 1); err != nil {
		t.Fatalf("disabled check should pass: %v", err)
	}
	if e2.p1Enabled() {
		t.Fatal("p1Enabled should be false when epoch switch off")
	}
}

// TestP1QuorumGateFailover quorum 前置（T9 quorum 不足降级只读）：
// quorum 失败时不执行重指派、不置 failoverDone（允许恢复后重试）；
// quorum 通过后正常执行（此处路由为空 → moved=0 且幂等标记生效）。
func TestP1QuorumGateFailover(t *testing.T) {
	quorumErr := errors.New("sql: test quorum not met")
	cfg := &fakeCfg{epoch: true, quorum: true}

	// quorum 失败端：
	e1 := newP1Executor(&fakeEpochSrc{}, &fakeQuorum{err: quorumErr}, cfg, "node-a")
	e1.mgr.RegisterPeer("node-b", "127.0.0.1:1")
	moved, err := e1.FailoverDownNode("node-b")
	if err == nil || moved != 0 {
		t.Fatalf("quorum-fail FailoverDownNode = (%d,%v), want (0,err)", moved, err)
	}
	// 不置 failoverDone：再次调用仍可重试（仍返回 quorum 错误而非幂等跳过）。
	if _, err2 := e1.FailoverDownNode("node-b"); err2 == nil {
		t.Fatal("second call should still fail with quorum error (not idempotent-skip)")
	}

	// quorum 通过端（路由为空 → moved=0、failoverDone 置位幂等）：
	e2 := newP1Executor(&fakeEpochSrc{}, &fakeQuorum{}, cfg, "node-a")
	e2.mgr.RegisterPeer("node-b", "127.0.0.1:1")
	moved, err = e2.FailoverDownNode("node-b")
	if err != nil || moved != 0 {
		t.Fatalf("quorum-ok FailoverDownNode = (%d,%v), want (0,nil)", moved, err)
	}
	moved2, err2 := e2.FailoverDownNode("node-b")
	if err2 != nil || moved2 != 0 {
		t.Fatalf("second call should be idempotent (0,nil), got (%d,%v)", moved2, err2)
	}
}

// TestP1HoldCheckSkipsCandidate 持有校验跳过候选（T7）：
// 候选 node-b/node-c 均未持有目标 region → 依次跳过，回本节点仍未持有 →
// 放弃重指派且不污染路由（region owner 保持 node-b）。
func TestP1HoldCheckSkipsCandidate(t *testing.T) {
	src := &fakeEpochSrc{}
	cfg := &fakeCfg{epoch: true}
	e := newP1Executor(src, &fakeQuorum{}, cfg, "node-a")
	e.mgr.RegisterPeer("node-b", "127.0.0.1:1")
	e.mgr.RegisterPeer("node-c", "127.0.0.1:2")
	e.router.SetRegions(1, []sharding.RegionInfo{regionOf(1, "node-b", 0)})

	// 注入持有校验：全部候选（含本节点）均未持有。
	var calls []string
	e.holdsChecker = func(rg sharding.RegionInfo, target string) (bool, error) {
		calls = append(calls, target)
		return false, nil
	}

	rg, _ := e.router.FindRegion(1)
	err := e.failoverRegionP1(rg, "node-b")
	if err == nil {
		t.Fatal("failoverRegionP1 should fail when no candidate holds the region")
	}
	// 候选尝试顺序：node-c（存活远端）→ node-a（本节点兜底）；
	// node-b 为 down 目标不参与候选（ListNodes 按 ID 排序）。
	if len(calls) != 2 || calls[0] != "node-c" || calls[1] != "node-a" {
		t.Fatalf("hold-check calls = %v, want [node-c node-a]", calls)
	}
	// 路由未被污染：region 仍归属 node-b（未被错误改指到未持有节点）。
	rg2, _ := e.router.FindRegion(1)
	if rg2.Node != "node-b" {
		t.Fatalf("region owner polluted to %s, want node-b", rg2.Node)
	}
}

// TestP1ConcurrentFailoverOnlyOneWins 双端并发重指派至多一端成功：
// 共享单调 epoch 源（模拟全局唯一发号），quorum 失败端被拒、
// 成功端取得新 clusterEpoch 授权并完成本端重指派。
func TestP1ConcurrentFailoverOnlyOneWins(t *testing.T) {
	quorumErr := errors.New("sql: test quorum not met")
	shared := &fakeEpochSrc{}
	cfg := &fakeCfg{epoch: true, quorum: true}

	// 分区两端（多数派端 A、少数派端 B）：
	sideA := newP1Executor(shared, &fakeQuorum{}, cfg, "node-a")
	sideA.mgr.RegisterSelf("127.0.0.1:9")
	sideA.tm = &p1TxnMgr{rows: []storage.KVPair{{Key: []byte("k"), Value: []byte("v")}}}
	sideA.router.SetRegions(1, []sharding.RegionInfo{regionOf(1, "node-b", 0)})
	// 本节点真实持有（注入钩子，避免依赖真实存储）。
	sideA.holdsChecker = func(rg sharding.RegionInfo, target string) (bool, error) {
		return target == "node-a", nil
	}

	sideB := newP1Executor(shared, &fakeQuorum{err: quorumErr}, cfg, "node-b2")
	sideB.mgr.RegisterPeer("node-a", "127.0.0.1:2")

	// 并发触发同一 down 节点重指派。
	_, errA := sideA.FailoverDownNode("node-b")
	_, errB := sideB.FailoverDownNode("node-b")

	// A 端（多数派）成功；B 端（少数派）被 quorum 拒绝。
	if errA != nil {
		t.Fatalf("side A (quorum ok) should succeed, got %v", errA)
	}
	if errB == nil {
		t.Fatal("side B (quorum fail) should be rejected")
	}
	// 至多一端取得 clusterEpoch 授权（A 端取得，B 端在 quorum 前置被拦）。
	if shared.acquired != 1 {
		t.Fatalf("clusterEpoch acquired = %d, want 1 (only winning side)", shared.acquired)
	}
	// A 端路由已收敛到本节点（重指派成功）。
	rg, ok := sideA.router.FindRegion(1)
	if !ok || rg.Node != "node-a" {
		t.Fatalf("side A region owner = %+v, want node-a", rg)
	}
	// B 端未重指派（regionEpoch 快照为空）。
	if got := sideB.regionEpochOf(1); got != 0 {
		t.Fatalf("side B regionEpochOf = %d, want 0 (not reassigned)", got)
	}
}

// TestP1StaleResolveOwner resolveRegionOwner 陈旧 failover 条目失效：
// 路由条目 epoch（1）落后于本机快照（2）→ 视为无效；quorum 不足时
// 降级只读（回退 selfID），不采用陈旧路由（不返回 down 的 node-b）。
func TestP1StaleResolveOwner(t *testing.T) {
	src := &fakeEpochSrc{known: 2}
	cfg := &fakeCfg{epoch: true, quorum: true}
	e := newP1Executor(src, &fakeQuorum{err: errors.New("sql: test quorum not met")}, cfg, "node-a")
	e.mgr.RegisterPeer("node-b", "127.0.0.1:1")
	e.mgr.MarkDown("node-b")
	// 路由条目：node-b down，epoch=1；本机快照 regionEpoch=2（曾被更高纪元取代）。
	e.router.SetRegions(1, []sharding.RegionInfo{regionOf(1, "node-b", 1)})
	e.p1Epoch[1] = p1RegionEpoch{clusterEpoch: 2, regionEpoch: 2}

	// 陈旧条目识别。
	rg, _ := e.router.FindRegion(1)
	if !e.staleFailover(rg) {
		t.Fatal("region epoch 1 < snapshot 2 should be stale")
	}
	// resolveRegionOwner：陈旧条目失效 → 触发重指派（quorum 不足降级）
	// → 回退只读 selfID，不返回陈旧 down owner。
	if owner := e.resolveRegionOwner(1); owner != "node-a" {
		t.Fatalf("resolveRegionOwner = %q, want node-a (degrade read-only)", owner)
	}
}

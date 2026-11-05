package sql

// P3 HA 集群轨 epoch/归属授权（D4.5 报告 §6 第 3 项 / T3）单元测试。
//
// 缺口复现口径（D4.3 T3 GAP）：集群轨无复制轨、masterEpoch 恒 0，
// db.AllowWrite 恒放行；双向网络分区期间被隔离的旧主仍按本地 region 归属
// 视图接受写入，分区恢复后也不自动收敛。
//
// 用例覆盖：
//   - TestT3FenceDisabledZeroRegression：quorum 关闭 / 未装配 → 写路径与
//     groupOps 全放行（默认生产配置零回归）。
//   - TestT3MinorityPartitionRejectsWrite：分区少数派 → 写路径入口拒绝，
//     计数递增；恢复多数派（TTL 过期后重探）→ 自动放行。
//   - TestT3MajorityProbeCached：TTL 内复用同一探测结果（不逐语句 PING）。
//   - TestT3OnPartitionRecoveredUnfences：恢复回调清缓存立即放行 +
//     收敛计数 + 权威路由拉取钩子被触发。
//   - TestT3RegionOwnerFence：region 已被更高 regionEpoch 指派他人而本地
//     路由陈旧 → 本地 op 落盘前拒绝；路由纪元已收敛 → 放行。
//   - TestT3GroupOpsAppliesRegionOwnerFence：写批次组装阶段即拦截（含本地 op）。

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// p3Quorum 可切换的 quorum 仲裁器：minority=true 模拟分区少数派。
type p3Quorum struct {
	mu       sync.Mutex
	minority bool
	probes   int
}

func (q *p3Quorum) QuorumOK() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.probes++
	if q.minority {
		return errQuorumMinority
	}
	return nil
}

func (q *p3Quorum) setMinority(v bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.minority = v
}

func (q *p3Quorum) probeCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.probes
}

// errQuorumMinority 分区少数派的 quorum 失败错误（语义与 db.ErrQuorumNotMet 一致）。
var errQuorumMinority = &SQLError{Msg: "quorum not met: reachable 1/3, need 2"}

// newT3Executor 构造启用集群轨写 fencing 的执行器（同包白盒，离线无网络）。
func newT3Executor(q *p3Quorum, cfg *fakeCfg, selfID string) *Executor {
	e := newP1Executor(&fakeEpochSrc{known: 1}, &fakeQuorum{}, cfg, selfID)
	// 用可切换 quorum 覆盖固定返回值实现，便于同一执行器内切换分区状态。
	e.quorumArb = q
	return e
}

// localOp 构造本地 region 写入操作（key 使用 region 编码）。
func localOp(rid storage.ID, inner string) kvOp {
	return kvOp{Key: sharding.EncodeRegionKey(rid, []byte(inner)), Value: []byte("v")}
}

// TestT3FenceDisabledZeroRegression quorum 关闭 / 仲裁器未装配时集群轨写
// fencing 不生效：写路径入口与 groupOps 均放行（默认生产配置零回归）。
func TestT3FenceDisabledZeroRegression(t *testing.T) {
	// 1) quorum 关闭（ha_quorum_enabled 默认关）。
	q := &p3Quorum{minority: true}
	cfg := &fakeCfg{epoch: true, quorum: false}
	e := newT3Executor(q, cfg, "node-a")
	if err := e.writeFenceCheck(); err != nil {
		t.Fatalf("quorum disabled: writeFenceCheck must pass, got %v", err)
	}
	if _, _, err := e.groupOps([]kvOp{localOp(1, "k1")}); err != nil {
		t.Fatalf("quorum disabled: groupOps must pass, got %v", err)
	}

	// 2) 仲裁器未装配（单机/M6 兼容路径）。
	e2 := newP1Executor(&fakeEpochSrc{}, &fakeQuorum{}, &fakeCfg{epoch: true, quorum: true}, "node-a")
	e2.quorumArb = nil
	if err := e2.writeFenceCheck(); err != nil {
		t.Fatalf("arbiter nil: writeFenceCheck must pass, got %v", err)
	}
	if got := e2.FencedWriteCount(); got != 0 {
		t.Fatalf("fenced count = %d, want 0", got)
	}
	// 关闭状态下不得产生任何多数派探测开销。
	if got := q.probeCount(); got != 0 {
		t.Fatalf("quorum disabled must not probe, probes=%d", got)
	}
}

// TestT3MinorityPartitionRejectsWrite 分区少数派拒绝写：
//   - 少数派 → 写路径入口返回 "cluster track write fenced" 错误，计数 +1；
//   - 恢复多数派后（TTL 过期重探）→ 自动放行，无需人工干预；
//   - 重复拒绝持续累计计数（观测项可用于演练断言）。
func TestT3MinorityPartitionRejectsWrite(t *testing.T) {
	q := &p3Quorum{minority: true}
	cfg := &fakeCfg{epoch: true, quorum: true}
	e := newT3Executor(q, cfg, "node-a")

	err := e.writeFenceCheck()
	if err == nil {
		t.Fatal("minority partition: cluster track write must be fenced")
	}
	if !strings.Contains(err.Error(), p3FencedPrefix) {
		t.Fatalf("error = %v, want prefix %q", err, p3FencedPrefix)
	}
	if got := e.FencedWriteCount(); got != 1 {
		t.Fatalf("fenced count = %d, want 1", got)
	}
	// 第二次写入仍被拒（分区未恢复）且计数累计。
	if err := e.writeFenceCheck(); err == nil {
		t.Fatal("minority partition: second write must be fenced")
	}
	if got := e.FencedWriteCount(); got != 2 {
		t.Fatalf("fenced count = %d, want 2", got)
	}

	// 分区恢复：多数派可达，TTL 过期后写路径自动放行。
	q.setMinority(false)
	time.Sleep(p3FenceTTL + 50*time.Millisecond)
	if err := e.writeFenceCheck(); err != nil {
		t.Fatalf("majority restored: write must pass after TTL, got %v", err)
	}
	if got := e.FencedWriteCount(); got != 2 {
		t.Fatalf("fenced count after recovery = %d, want 2 (no new rejections)", got)
	}
}

// TestT3MajorityProbeCached 多数派探测结果按 TTL 缓存：同一窗口内的连续写入
// 只产生一次仲裁探测，避免逐语句 PING 放大（分区判定为秒级语义）。
func TestT3MajorityProbeCached(t *testing.T) {
	q := &p3Quorum{}
	cfg := &fakeCfg{epoch: true, quorum: true}
	e := newT3Executor(q, cfg, "node-a")

	for i := 0; i < 5; i++ {
		if err := e.writeFenceCheck(); err != nil {
			t.Fatalf("write %d must pass: %v", i, err)
		}
	}
	if got := q.probeCount(); got != 1 {
		t.Fatalf("probes within TTL = %d, want 1 (cached)", got)
	}
	// TTL 过期后重新探测。
	time.Sleep(p3FenceTTL + 50*time.Millisecond)
	if err := e.writeFenceCheck(); err != nil {
		t.Fatalf("write after TTL must pass: %v", err)
	}
	if got := q.probeCount(); got != 2 {
		t.Fatalf("probes after TTL = %d, want 2", got)
	}
}

// TestT3OnPartitionRecoveredUnfences 分区恢复收敛：恢复回调立即清空探测缓存
// （写路径在 TTL 内即可放行）、累计收敛计数、触发权威路由视图拉取钩子，
// 并把恢复时的视图 version 纳入本机已知最大 version（大者胜）。
func TestT3OnPartitionRecoveredUnfences(t *testing.T) {
	q := &p3Quorum{minority: true}
	cfg := &fakeCfg{epoch: true, quorum: true}
	e := newT3Executor(q, cfg, "node-a")

	if err := e.writeFenceCheck(); err == nil {
		t.Fatal("minority partition: write must be fenced")
	}

	var resync int
	var resyncMu sync.Mutex
	e.SetPartitionRouteResync(func() {
		resyncMu.Lock()
		resync++
		resyncMu.Unlock()
	})

	q.setMinority(false)
	e.OnPartitionRecovered(42)

	// 缓存已清：无需等待 TTL 即可放行。
	if err := e.writeFenceCheck(); err != nil {
		t.Fatalf("after recovery: write must pass immediately, got %v", err)
	}
	if got := e.PartitionRecoveredCount(); got != 1 {
		t.Fatalf("recovered count = %d, want 1", got)
	}
	// 视图 version 大者胜。
	if got := e.mgr.ViewVersion(); got != 42 {
		t.Fatalf("view version = %d, want 42", got)
	}
	// 权威路由拉取钩子异步触发。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resyncMu.Lock()
		n := resync
		resyncMu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("route resync hook was not triggered after partition recovery")
}

// TestT3RegionOwnerFence region 归属 epoch 校验：
//   - 本机快照记录该 region 已被更高 regionEpoch 指派给远端，而本地路由
//     条目仍停留在旧纪元 → 本地写拒绝（恢复窗口内旧主不再写被接管 region）；
//   - 路由纪元已追平快照 → 放行（走正常归属路由）；
//   - 无快照 / epoch 未启用 → 放行（零回归）。
func TestT3RegionOwnerFence(t *testing.T) {
	e := newT3Executor(&p3Quorum{}, &fakeCfg{epoch: true, quorum: true}, "node-a")
	const rid storage.ID = 7
	if err := e.router.UpsertRegion(regionOf(rid, "node-a", 3)); err != nil {
		t.Fatalf("upsert region: %v", err)
	}

	// 无快照：放行。
	if err := e.regionOwnerFence(rid); err != nil {
		t.Fatalf("no snapshot: must pass, got %v", err)
	}

	// 快照：region 已按 regionEpoch=5 指派给 node-b，本地路由仍为 epoch=3。
	e.recordRegionEpochOwner(rid, 9, 5, "node-b")
	err := e.regionOwnerFence(rid)
	if err == nil {
		t.Fatal("stale local route view must fence the write")
	}
	if !strings.Contains(err.Error(), p3FencedPrefix) {
		t.Fatalf("error = %v, want prefix %q", err, p3FencedPrefix)
	}
	if got := e.FencedWriteCount(); got != 1 {
		t.Fatalf("fenced count = %d, want 1", got)
	}

	// 路由收敛到快照纪元：放行。
	if err := e.router.SetRegionNodeEpoch(rid, "node-b", 5); err != nil {
		t.Fatalf("set region epoch: %v", err)
	}
	if err := e.regionOwnerFence(rid); err != nil {
		t.Fatalf("converged route view: must pass, got %v", err)
	}

	// epoch 开关关闭：即使视图陈旧也放行（零回归）。
	e.haCfg.SetEpochEnabled(false)
	if err := e.regionOwnerFence(rid); err != nil {
		t.Fatalf("epoch disabled: must pass, got %v", err)
	}
}

// TestT3GroupOpsAppliesRegionOwnerFence 写批次组装阶段即拦截：本地 op 在
// 进入事务缓冲前完成归属 epoch 校验，避免"已落盘再报错"的部分写入。
func TestT3GroupOpsAppliesRegionOwnerFence(t *testing.T) {
	e := newT3Executor(&p3Quorum{}, &fakeCfg{epoch: true, quorum: true}, "node-a")
	const rid storage.ID = 3
	if err := e.router.UpsertRegion(regionOf(rid, "node-a", 1)); err != nil {
		t.Fatalf("upsert region: %v", err)
	}
	e.recordRegionEpochOwner(rid, 4, 6, "node-c")

	if _, _, err := e.groupOps([]kvOp{localOp(rid, "k1")}); err == nil {
		t.Fatal("groupOps must reject local op for stale-owned region")
	} else if !strings.Contains(err.Error(), p3FencedPrefix) {
		t.Fatalf("error = %v, want prefix %q", err, p3FencedPrefix)
	}

	// 收敛后（路由条目同纪元且归属回本节点）同一批次可通过本地路径。
	if err := e.router.SetRegionNodeEpoch(rid, "node-a", 6); err != nil {
		t.Fatalf("set region epoch: %v", err)
	}
	if _, _, err := e.groupOps([]kvOp{localOp(rid, "k1")}); err != nil {
		t.Fatalf("converged: groupOps must pass, got %v", err)
	}
}

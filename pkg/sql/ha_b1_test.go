package sql

// B1（D4.5 报告 §6 第 1 项）回归测试：单副本不可恢复 → 副本冗余 + 无损恢复。
//
// 覆盖：
//   TestB1SingleCopyReproUnrecoverable  —— 缺口复现：单副本 owner 损毁且存活节点
//       无副本时，failover 报 no live data copy；修复后该 region 记入 pending
//       且 failoverDone 不置位（旧实现置位后永久放弃 = 单点副本损毁即不可用）。
//   TestB1EnsureRegionReplicas         —— 主动冗余：单副本 region 自动向未持有
//       节点推送影子副本，副本数达到 b1TargetCopies；owner 归属不变。
//   TestB1EnsureIdempotentAndLost      —— 幂等（已达标不重复推送）；集群内零副本
//       时记入 lost 且不伪造空副本（避免覆盖离线节点上的真实副本）。
//   TestB1FailoverRecoverableAfterReplica —— 修复闭环：副本补齐后同一 region
//       的 failover pending 可被清除，重试窗口后不再永久放弃。
//   TestB1DisabledZeroRegression       —— 开关关闭：不推送、不记录、failover
//       行为与此前一致。

import (
	"fmt"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
)

// fakeB1Cfg 在 fakeCfg 基础上实现 sql.ReplicaSwitch（ha_replica_enabled）。
type fakeB1Cfg struct {
	*fakeCfg
	replica bool
}

func (f *fakeB1Cfg) SetReplicaEnabled(on bool) { f.replica = on }
func (f *fakeB1Cfg) ReplicaEnabled() bool      { return f.replica }

// newB1Executor 构造 3 节点（node-a 本机 / node-b / node-c）HA Executor。
func newB1Executor(cfg HAConfigSetter) *Executor {
	// p1TxnMgr 的扫描事务固定返回空行集 —— 精确模拟"本节点不持有该 region 副本"。
	e := newP1Executor(nil, &fakeQuorum{}, &fakeCfg{}, "node-a")
	e.tm = &p1TxnMgr{} // 本地扫描返回空行集
	e.haCfg = cfg
	e.mgr.SetNodes([]cluster.NodeInfo{
		{ID: "node-a", Addr: "127.0.0.1:1", Role: cluster.RoleData, State: cluster.StateUp},
		{ID: "node-b", Addr: "127.0.0.1:2", Role: cluster.RoleData, State: cluster.StateUp},
		{ID: "node-c", Addr: "127.0.0.1:3", Role: cluster.RoleData, State: cluster.StateUp},
	})
	if err := e.router.UpsertRegion(sharding.RegionInfo{
		TableID:     1,
		RegionID:    1,
		StartKey:    []byte("a"),
		EndKey:      []byte("z"),
		Node:        "node-b", // 单副本：仅 node-b 持有
		RegionEpoch: 1,
	}); err != nil {
		panic(err)
	}
	// 默认持有视图：仅 node-b 持有 region 1（单副本初始状态）。
	e.holdsChecker = func(rg sharding.RegionInfo, target string) (bool, error) {
		return target == "node-b", nil
	}
	return e
}

// b1ReplicaRow 构造该 region 的一行测试数据（供 scan 钩子返回）。
func b1ReplicaRow() []cluster.QueryRow {
	return []cluster.QueryRow{{Key: []byte("r\x00\x00\x00\x01a"), Value: []byte("v")}}
}

// TestB1SingleCopyReproUnrecoverable 缺口复现 + 修复：单副本 owner 损毁且存活
// 节点无副本时，failover 返回 no live data copy，但 B1 修复后不再永久放弃。
func TestB1SingleCopyReproUnrecoverable(t *testing.T) {
	e := newB1Executor(&fakeCfg{quorum: true})
	// 拓扑裁剪为 node-a self + node-b（即将损毁）—— 单副本 region owner 损毁后
	// 集群内只剩本机，且本机无该 region 键空间（真实单副本损毁场景）。
	e.mgr.SetNodes([]cluster.NodeInfo{
		{ID: "node-a", Addr: "127.0.0.1:1", Role: cluster.RoleData, State: cluster.StateUp},
		{ID: "node-b", Addr: "127.0.0.1:2", Role: cluster.RoleData, State: cluster.StateDown},
	})
	// 模拟 node-b 损毁：存活节点均无该 region 副本。
	e.holdsChecker = func(sharding.RegionInfo, string) (bool, error) { return false, nil }

	moved, err := e.FailoverDownNode("node-b")
	if err == nil || !isNoLiveCopy(err) {
		t.Fatalf("single-copy down should report no live copy, got moved=%d err=%v", moved, err)
	}
	if moved != 0 {
		t.Fatalf("no region should be moved when no live copy exists, got %d", moved)
	}
	// 修复点 1：region 记入 pending（可被后续副本补齐/节点恢复消解）。
	if _, ok := e.B1PendingRegions()[1]; !ok {
		t.Fatal("region 1 should be recorded as pending after no-live-copy failover")
	}
	// 修复点 2：failoverDone 不置位 —— 旧实现置位后该 down 节点永不再尝试，
	// 即使副本恢复/节点回 UP 也永久不可用。
	if e.failoverDone["node-b"] {
		t.Fatal("failoverDone should NOT be set when regions still wait for a live copy")
	}
	// 修复点 3：冷却窗口内幂等返回，不重复扫描（避免读路径重试风暴）。
	if moved2, err2 := e.FailoverDownNode("node-b"); moved2 != 0 || err2 != nil {
		t.Fatalf("retry inside cooldown window should be a no-op, got moved=%d err=%v", moved2, err2)
	}
}

// TestB1EnsureRegionReplicas 主动冗余：单副本 region 补齐影子副本。
func TestB1EnsureRegionReplicas(t *testing.T) {
	e := newB1Executor(&fakeB1Cfg{fakeCfg: &fakeCfg{quorum: false}, replica: true})

	var (
		pushTargets []string
		pushRows    int
	)
	e.SetReplicaHooks(
		func(target string, rg sharding.RegionInfo, rows []cluster.QueryRow, srcID string) error {
			pushTargets = append(pushTargets, target)
			pushRows = len(rows)
			// 副本推送后目标节点持有该 region（副本语义：owner 不变）。
			prev := e.holdsChecker
			e.holdsChecker = func(r sharding.RegionInfo, id string) (bool, error) {
				if id == target {
					return true, nil
				}
				return prev(r, id)
			}
			return nil
		},
		func(nodeID string, rg sharding.RegionInfo) ([]cluster.QueryRow, error) {
			return b1ReplicaRow(), nil
		},
	)

	n, err := e.EnsureRegionReplicas()
	if err != nil {
		t.Fatalf("ensure replicas should succeed: %v", err)
	}
	if n != 1 || len(pushTargets) != 1 {
		t.Fatalf("expected exactly 1 region replicated, got n=%d pushes=%v", n, pushTargets)
	}
	if pushTargets[0] == "node-b" {
		t.Fatalf("replica must not be pushed to the existing owner, got %s", pushTargets[0])
	}
	if pushRows != 1 {
		t.Fatalf("replica should carry source rows, got %d rows", pushRows)
	}
	// 副本数达到目标，且 owner 归属不变（仍是 node-b，影子副本不改归属）。
	if got := e.B1ReplicasOf(1); len(got) < b1TargetCopies {
		t.Fatalf("region 1 should have %d copies after ensure, got %v", b1TargetCopies, got)
	}
	if owner, _ := e.router.FindRegion(1); owner.Node != "node-b" {
		t.Fatalf("replica ensure must not change owner, got %q", owner.Node)
	}
	if e.B1EnsureCount() != 1 {
		t.Fatalf("ensure counter should be 1, got %d", e.B1EnsureCount())
	}
}

// TestB1EnsureIdempotentAndLost 幂等 + 零副本场景不伪造副本。
func TestB1EnsureIdempotentAndLost(t *testing.T) {
	e := newB1Executor(&fakeCfg{quorum: true})

	var pushes int
	e.SetReplicaHooks(
		func(target string, rg sharding.RegionInfo, rows []cluster.QueryRow, srcID string) error {
			pushes++
			return nil
		},
		func(nodeID string, rg sharding.RegionInfo) ([]cluster.QueryRow, error) {
			return b1ReplicaRow(), nil
		},
	)

	// 已达标（node-b + node-c 双副本）→ 不推送。
	e.holdsChecker = func(rg sharding.RegionInfo, target string) (bool, error) {
		return target == "node-b" || target == "node-c", nil
	}
	if n, err := e.EnsureRegionReplicas(); n != 0 || err != nil || pushes != 0 {
		t.Fatalf("replicated region should be skipped, got n=%d err=%v pushes=%d", n, err, pushes)
	}

	// 集群内零副本 → 记 lost、报错、不推送空副本（禁止伪造数据覆盖离线副本）。
	e.holdsChecker = func(sharding.RegionInfo, string) (bool, error) { return false, nil }
	n, err := e.EnsureRegionReplicas()
	if n != 0 || err == nil {
		t.Fatalf("zero-copy region should be reported, got n=%d err=%v", n, err)
	}
	if pushes != 0 {
		t.Fatalf("must not fabricate replica for zero-copy region, got %d pushes", pushes)
	}
	if lost := e.B1LostRegions(); len(lost) != 1 || lost[0] != 1 {
		t.Fatalf("region 1 should be marked lost, got %v", lost)
	}
}

// TestB1FailoverRecoverableAfterReplica 修复闭环：副本补齐后 pending 可清除，
// 且重试窗口结束后 failover 不再因旧实现"已置 failoverDone"而永久放弃。
func TestB1FailoverRecoverableAfterReplica(t *testing.T) {
	e := newB1Executor(&fakeCfg{quorum: true})
	e.mgr.SetNodes([]cluster.NodeInfo{
		{ID: "node-a", Addr: "127.0.0.1:1", Role: cluster.RoleData, State: cluster.StateUp},
		{ID: "node-b", Addr: "127.0.0.1:2", Role: cluster.RoleData, State: cluster.StateDown},
		{ID: "node-c", Addr: "127.0.0.1:3", Role: cluster.RoleData, State: cluster.StateUp},
	})
	e.holdsChecker = func(sharding.RegionInfo, string) (bool, error) { return false, nil }

	if _, err := e.FailoverDownNode("node-b"); !isNoLiveCopy(err) {
		t.Fatalf("expected no live copy, got %v", err)
	}
	if _, ok := e.B1PendingRegions()[1]; !ok {
		t.Fatal("region 1 should be pending")
	}

	// 模拟原节点恢复（数据回来）：node-b 重新持有该 region 行。
	e.holdsChecker = func(rg sharding.RegionInfo, target string) (bool, error) {
		return target == "node-b", nil
	}
	// 越过冷却窗口（等待一个重试间隔）。
	e.clearB1RetryAt("node-b")
	e.holdsChecker = func(rg sharding.RegionInfo, target string) (bool, error) { return false, nil }

	// 重试应重新进入失败判定（说明未被永久放弃），而非直接返回 0,nil。
	if _, err := e.FailoverDownNode("node-b"); !isNoLiveCopy(err) {
		t.Fatalf("retry after cooldown should re-attempt failover (not silently skip), got %v", err)
	}

	// 副本补齐成功后 pending 清除（数据重新可被接管）。
	e.holdsChecker = func(rg sharding.RegionInfo, target string) (bool, error) {
		return target == "node-c", nil // node-c 成为存活副本持有者
	}
	e.SetReplicaHooks(
		func(string, sharding.RegionInfo, []cluster.QueryRow, string) error { return nil },
		func(string, sharding.RegionInfo) ([]cluster.QueryRow, error) { return b1ReplicaRow(), nil },
	)
	if _, err := e.EnsureRegionReplicas(); err != nil {
		t.Fatalf("ensure after recovery should succeed: %v", err)
	}
	if got := e.B1ReplicasOf(1); len(got) < b1TargetCopies {
		t.Fatalf("region 1 should reach %d copies after recovery, got %v", b1TargetCopies, got)
	}
	// 补齐成功后 pending 立即清除（不再挂在待恢复队列里）。
	if _, ok := e.B1PendingRegions()[1]; ok {
		t.Fatal("pending should be cleared once a live replica has been ensured")
	}
}

// TestB1DisabledZeroRegression 开关关闭：完全零行为（不推送/不记录/不改
// failover 语义）。
func TestB1DisabledZeroRegression(t *testing.T) {
	cfg := &fakeCfg{quorum: false}
	e := newB1Executor(cfg)
	var pushes int
	e.SetReplicaHooks(
		func(string, sharding.RegionInfo, []cluster.QueryRow, string) error { pushes++; return nil },
		func(string, sharding.RegionInfo) ([]cluster.QueryRow, error) { return b1ReplicaRow(), nil },
	)
	if n, err := e.EnsureRegionReplicas(); n != 0 || err != nil || pushes != 0 {
		t.Fatalf("disabled replica switch must be a no-op, got n=%d err=%v pushes=%d", n, err, pushes)
	}
	if len(e.B1PendingRegions()) != 0 || len(e.B1LostRegions()) != 0 {
		t.Fatal("disabled switch must not record B1 observations")
	}
	// 单机（未装配 haCfg）同样零回归。
	e2 := newP1Executor(nil, &fakeQuorum{}, nil, "node-a")
	e2.haCfg = nil // newP1Executor 收 nil 时留下 typed-nil 接口，此处显式置为空接口
	if n, err := e2.EnsureRegionReplicas(); n != 0 || err != nil {
		t.Fatalf("no HA config should be a no-op, got n=%d err=%v", n, err)
	}
}

// TestB1InsuffientNodesNoop 存活节点不足 2 个时无可冗余落点（单机/双节点降级）。
func TestB1InsuffientNodesNoop(t *testing.T) {
	e := newB1Executor(&fakeCfg{quorum: true})
	e.mgr.SetNodes([]cluster.NodeInfo{
		{ID: "node-a", Addr: "127.0.0.1:1", Role: cluster.RoleData, State: cluster.StateUp},
		{ID: "node-b", Addr: "127.0.0.1:2", Role: cluster.RoleData, State: cluster.StateDown},
	})
	var pushes int
	e.SetReplicaHooks(
		func(string, sharding.RegionInfo, []cluster.QueryRow, string) error { pushes++; return nil },
		func(string, sharding.RegionInfo) ([]cluster.QueryRow, error) { return b1ReplicaRow(), nil },
	)
	if n, err := e.EnsureRegionReplicas(); n != 0 || err != nil || pushes != 0 {
		t.Fatalf("single healthy node should skip replication, got n=%d err=%v pushes=%d", n, err, pushes)
	}
}

// TestB1ReplicaNodeCooldown 对端不可达时进入节点级冷却：同一窗口内不再
// 反复扫描/推送该节点，避免维护循环对刚失联（尚未被判 DOWN）的节点刷日志。
func TestB1ReplicaNodeCooldown(t *testing.T) {
	e := newB1Executor(&fakeB1Cfg{fakeCfg: &fakeCfg{quorum: false}, replica: true})

	var targets []string
	e.SetReplicaHooks(
		func(target string, rg sharding.RegionInfo, rows []cluster.QueryRow, srcID string) error {
			targets = append(targets, target)
			return fmt.Errorf("dial %s: connection refused", target)
		},
		func(string, sharding.RegionInfo) ([]cluster.QueryRow, error) { return b1ReplicaRow(), nil },
	)

	// 首轮：目标为 node-a（alive 排序首个非持有者），推送失败 → 进入冷却。
	if n, err := e.EnsureRegionReplicas(); n != 0 || err == nil {
		t.Fatalf("failed push should be reported, got n=%d err=%v", n, err)
	}
	if len(targets) != 1 || targets[0] != "node-a" {
		t.Fatalf("first target should be node-a, got %v", targets)
	}
	if !e.b1NodeCooling("node-a") {
		t.Fatal("node-a should enter cooldown after push failure")
	}

	// 次轮：node-a 冷却中不再作为目标，改选 node-c（仍失败但换节点）。
	_, _ = e.EnsureRegionReplicas()
	if len(targets) != 2 || targets[1] != "node-c" {
		t.Fatalf("cooling node must be skipped on next round, got targets=%v", targets)
	}

	// 全部对端冷却后不再产生推送与错误（降噪）。
	if n, err := e.EnsureRegionReplicas(); n != 0 || err != nil {
		t.Fatalf("all targets cooling should be a quiet no-op, got n=%d err=%v", n, err)
	}
	if len(targets) != 2 {
		t.Fatalf("no push expected while every target is cooling, got %v", targets)
	}

	// 冷却结束后重新允许（节点恢复可达时重新纳入候选）。
	e.clearB1NodeCool("node-a")
	if e.b1NodeCooling("node-a") {
		t.Fatal("cooldown should be cleared explicitly")
	}
}

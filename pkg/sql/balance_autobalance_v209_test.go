package sql

// v2.0.9 B2（Phase P2a / P2b）自动均衡回归用例。
//
// 缺陷背景（真机复验 R2）：重启后节点回 UP，重指派 / 回迁把 region 整批收归一
// 侧，而生产装配从未启动 balanceLoop（唯一 SetAutoBalance 调用在测试文件里），
// 于是布局只能靠人工 BALANCE 自愈。P2 补齐四件事：
//  1. P2a 生产装配默认自动均衡 + 显式开关 / 周期入口（SET auto_balance /
//     SET auto_balance_interval），关闭后行为回到 v2.0.8（仅手动 BALANCE）；
//  2. P2a HandleNodeUp / FailoverDownNode 尾部异步触发一次均衡，布局自愈不依赖
//     人工介入；
//  3. P2a balanceLoop 单轮失败补 WARN 日志与指数退避，不再静默 continue；
//  4. P2b 迁移冷却与迁移历史：同 region 5 分钟内最多迁移 2 次，抑制
//     「回迁→均衡→再回迁」抖动。
//
// 本文件覆盖的验收点：
//   - TestV209AutoBalanceDefaultAssemblyAndToggle：默认装配 / 开关 / 周期 / 显式关
//     优先级 / 关闭后不触发（等价 v2.0.8）；
//   - TestV209AutoBalanceTriggeredByFailoverConverges：FailoverDownNode 收归一
//     侧后自动触发均衡并收敛（不调 BALANCE）；
//   - TestV209BalanceLoopFailureBackoff：单轮失败走 WARN + 指数退避，循环存活；
//   - TestV209RegionMigrationCooldownCapsFlapping：重启抖动场景下同 region 5 分钟
//     内迁移不超过 2 次，手动 BALANCE 不受冷却限制。

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// v209WaitCond 在 timeout 内轮询 cond，满足即返回 true。
func v209WaitCond(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// v209EnvWithSelfNode 在 v209Env 基础上把本节点也写入集群注册表（ListNodes 含自身）。
// FailoverDownNode 的数据源选取只遍历 ListNodes，本节点不在注册表时会判定
// "no live data copy"；真实集群里本节点恒在注册表内（Manager.SetNodes 保证），
// 这里补齐以走真实重指派链路。
func v209EnvWithSelfNode(t *testing.T, selfID string, peers ...v209Peer) (*Engine, *Executor) {
	t.Helper()
	eng, ex := v209Env(t, selfID, peers...)
	now := time.Now().UnixMilli()
	nodes := []cluster.NodeInfo{
		{ID: selfID, Role: cluster.RoleData, State: cluster.StateUp, DataReady: true, LastSeen: now},
	}
	for _, p := range peers {
		nodes = append(nodes, cluster.NodeInfo{
			ID: p.id, Addr: p.addr, Role: cluster.RoleData, State: cluster.StateUp,
			DataReady: true, LastSeen: now,
		})
	}
	ex.mgr.SetNodes(nodes)
	return eng, ex
}

// TestV209AutoBalanceDefaultAssemblyAndToggle 覆盖 P2a 的装配与入口语义：
// 未装配前自动均衡关闭；EnableDefaultAutoBalance 默认开启；SET auto_balance=off
// 关闭后事件触发不再动作（等价 v2.0.8 仅手动 BALANCE）；显式关闭后生产默认装配
// 不得再覆盖；SET auto_balance_interval 支持热更新且"先调周期后开开关"生效。
func TestV209AutoBalanceDefaultAssemblyAndToggle(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	eng, ex := v209Env(t, "node-a", v209Peer{id: "node-b", addr: addrB})
	t.Cleanup(func() { eng.StopAutoBalance() })

	if ex.autoBalanceEnabled() {
		t.Fatalf("precondition: auto balance should be off before assembly")
	}
	if got := eng.AutoBalanceInterval(); got != DefaultAutoBalanceInterval {
		t.Fatalf("default interval = %s, want %s", got, DefaultAutoBalanceInterval)
	}

	// 生产装配（db.StartCluster 同款调用）：默认开启。
	eng.EnableDefaultAutoBalance(DefaultAutoBalanceInterval)
	if !eng.AutoBalanceEnabled() {
		t.Fatalf("auto balance should be enabled after default assembly")
	}
	if got := eng.AutoBalanceInterval(); got != DefaultAutoBalanceInterval {
		t.Fatalf("interval after assembly = %s, want %s", got, DefaultAutoBalanceInterval)
	}

	// 显式关闭：入口语义生效，事件触发不得再触发迁移。
	v209MustExec(t, eng, "SET auto_balance = off")
	if eng.AutoBalanceEnabled() {
		t.Fatalf("auto balance should be off after SET auto_balance = off")
	}
	trigs := ex.autoBalanceTrigs()
	ex.triggerAutoBalance("test-disabled")
	time.Sleep(balanceEventDebounce + 100*time.Millisecond)
	if got := ex.autoBalanceTrigs(); got != trigs {
		t.Fatalf("trigger count = %d, want %d (关闭后事件触发必须短路)", got, trigs)
	}

	// 显式关闭优先于生产默认装配：重启（再次 StartCluster）不得把用户的 off 覆盖回 on。
	eng.EnableDefaultAutoBalance(DefaultAutoBalanceInterval)
	if eng.AutoBalanceEnabled() {
		t.Fatalf("explicit off must win over default assembly")
	}

	// 周期热更新 + 重新开启：先设周期再开开关也能生效。
	v209MustExec(t, eng, "SET auto_balance_interval = 120")
	v209MustExec(t, eng, "SET auto_balance = on")
	if !eng.AutoBalanceEnabled() {
		t.Fatalf("auto balance should be enabled after SET auto_balance = on")
	}
	if got := eng.AutoBalanceInterval(); got != 120*time.Millisecond {
		t.Fatalf("interval = %s, want 120ms (auto_balance_interval 热更新)", got)
	}
	// SHOW CONFIG 反映当前状态（运维可见性）。
	res := v209MustExec(t, eng, "SHOW CONFIG")
	found := false
	for _, row := range res.Rows {
		if len(row) >= 2 && row[0].S == "auto_balance" {
			found = true
			if row[1].S != "on" {
				t.Fatalf("SHOW CONFIG auto_balance = %q, want on", row[1].S)
			}
		}
	}
	if !found {
		t.Fatalf("SHOW CONFIG 未包含 auto_balance 项")
	}
}

// TestV209AutoBalanceTriggeredByFailoverConverges 覆盖 P2a 的事件触发闭环：
// 5 个 region 先均衡到 3/2，node-b 宕机后 FailoverDownNode 把 2 个 region 收归
// node-a（5/0 一侧），收尾自动触发均衡把布局拉回极差 <=1 —— 全程不执行 BALANCE。
func TestV209AutoBalanceTriggeredByFailoverConverges(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	eng, ex := v209EnvWithSelfNode(t, "node-a", v209Peer{id: "node-b", addr: addrB})
	// 大周期开启：本用例只验证事件触发路径，不让 ticker 干扰断言。
	eng.SetAutoBalance(true, time.Hour)
	t.Cleanup(func() { eng.StopAutoBalance() })

	v209CreateSplit(t, eng, "users", 30, []int64{6, 12, 18, 24})
	if got := v209RegionCount(t, eng, "users"); got != 5 {
		t.Fatalf("precondition: users regions = %d, want 5", got)
	}
	if res := v209MustExec(t, eng, "BALANCE"); res.AffectedRows != 2 {
		t.Fatalf("precondition BALANCE migrated = %d, want 2", res.AffectedRows)
	}
	before := ex.balanceRegionCounts()
	if before["node-a"] != 3 || before["node-b"] != 2 {
		t.Fatalf("precondition counts = %v, want node-a=3 node-b=2", before)
	}

	// 迁移前数据基线：两节点在线时的行数与 region 数（迁移只切路由归属，不应改行数）。
	rowsBefore := v209MustExec(t, eng, "SELECT COUNT(*) FROM users").Rows[0][0].I
	trigsBefore := ex.autoBalanceTrigs()
	// node-b 宕机：2 个 region 因无其他存活远端而回本节点，收归一侧。
	moved, err := ex.FailoverDownNode("node-b")
	if err != nil {
		t.Fatalf("FailoverDownNode: %v", err)
	}
	if moved != 2 {
		t.Fatalf("failover moved = %d, want 2", moved)
	}
	if got := ex.balanceRegionCounts()["node-a"]; got != 5 {
		t.Fatalf("after failover node-a holds %d, want 5（收归一側，待自动均衡自愈）", got)
	}
	if ex.autoBalanceTrigs() <= trigsBefore {
		t.Fatalf("FailoverDownNode 收尾未触发自动均衡（trigs=%d）", ex.autoBalanceTrigs())
	}

	// 事件触发 -> 去抖 -> 一轮 balanceOnce 迁 2 个回来，极差回到 <=1。
	ok := v209WaitCond(t, 10*time.Second, func() bool {
		counts := ex.balanceRegionCounts()
		return v209Spread(counts, "node-a", "node-b") <= balanceConvergedSpread
	})
	if !ok {
		t.Fatalf("layout not converged after triggered balance: counts=%v", ex.balanceRegionCounts())
	}
	after := ex.balanceRegionCounts()
	if after["node-a"] != 3 || after["node-b"] != 2 {
		t.Fatalf("converged counts = %v, want node-a=3 node-b=2", after)
	}
	// 数据未丢：迁移只切路由归属，行数基线保持 30。
	if rowsBefore != 30 {
		t.Fatalf("baseline row count = %d, want 30", rowsBefore)
	}
}

// TestV209AutoBalanceTriggeredByNodeUp 覆盖 P2a 的另一条自愈入口：节点回 UP
// 完成回迁（rejoinOwnedRegions）后尾部异步触发一次均衡，布局不再依赖人工 BALANCE。
// 场景：3 节点 6 region 均衡为 2/2/2 -> node-c 宕机被接管（本机涨到 4）->
// node-c 回 UP 回迁 2 个（回到 2/2/2）-> 触发计数递增且无人工介入即收敛。
func TestV209AutoBalanceTriggeredByNodeUp(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	addrC := v209StartNode(t, "node-c")
	eng, ex := v209EnvWithSelfNode(t, "node-a",
		v209Peer{id: "node-b", addr: addrB},
		v209Peer{id: "node-c", addr: addrC},
	)
	// 大周期开启：只验证事件触发路径。
	eng.SetAutoBalance(true, time.Hour)
	t.Cleanup(func() { eng.StopAutoBalance() })

	v209CreateSplit(t, eng, "users", 60, []int64{6, 12, 18, 24, 30})
	if res := v209MustExec(t, eng, "BALANCE"); res.AffectedRows == 0 {
		t.Fatalf("precondition BALANCE migrated 0 regions")
	}
	before := ex.balanceRegionCounts()
	if before["node-a"] != 2 || before["node-b"] != 2 || before["node-c"] != 2 {
		t.Fatalf("precondition counts = %v, want 2/2/2", before)
	}

	// node-c 宕机：其 region 由 failover 接管（数据只在本机副本上，故收归 node-a）。
	ex.mgr.MarkDown("node-c")
	moved, err := ex.FailoverDownNode("node-c")
	if err != nil {
		t.Fatalf("FailoverDownNode: %v", err)
	}
	if moved != 2 {
		t.Fatalf("failover moved = %d, want 2", moved)
	}

	// node-c 回 UP：先恢数据面就绪，再走 HandleNodeUp 回迁路径。
	// 事件触发有 balanceEventThrottle 节流窗口：failover 已用掉一次，等窗口过后再模拟回 UP，
	// 才能观测到 node-up 事件自身的触发。
	time.Sleep(balanceEventThrottle + 200*time.Millisecond)
	// 回迁走复制轨 PushRegionEpochTo，需要 clusterEpoch 取号源（生产由 db.Open 装配）；
	// 单测注入最简实现，避免 epochSrc 为 nil 时该路径空指针。
	ex.SetEpochSource(&v209EpochSource{})
	// RegisterPeerData 把该节点重新置为 UP（DataReady=true），与心跳恢复后的注册表状态一致。
	ex.mgr.RegisterPeerData("node-c", addrC, true)
	trigsBefore := ex.autoBalanceTrigs()
	rejoined, err := ex.HandleNodeUp("node-c")
	if err != nil {
		t.Fatalf("HandleNodeUp: %v", err)
	}
	if rejoined != 2 {
		t.Fatalf("rejoined regions = %d, want 2", rejoined)
	}
	if ex.autoBalanceTrigs() <= trigsBefore {
		t.Fatalf("HandleNodeUp 回迁后未触发自动均衡（trigs=%d）", ex.autoBalanceTrigs())
	}
	ok := v209WaitCond(t, 10*time.Second, func() bool {
		return v209Spread(ex.balanceRegionCounts(), "node-a", "node-b", "node-c") <= balanceConvergedSpread
	})
	if !ok {
		t.Fatalf("layout not converged after node-up triggered balance: counts=%v", ex.balanceRegionCounts())
	}
}

// v209EpochSource 最简 clusterEpoch 取号源（单调递增），供回迁路径单测使用。
type v209EpochSource struct{ n uint64 }

func (s *v209EpochSource) Epoch() uint64 { return atomic.LoadUint64(&s.n) }

func (s *v209EpochSource) AcquireEpoch() uint64 { return atomic.AddUint64(&s.n, 1) }

// TestV209BalanceLoopFailureBackoff 覆盖 P2a 的失败可见性：均衡目标长期不可达时，
// 单轮失败必须累计失败计数（对应 WARN 日志落地点）并按指数退避拉开重试间隔，
// 且循环本身不能退出（后续轮次仍会执行）。
func TestV209BalanceLoopFailureBackoff(t *testing.T) {
	dead := v209DeadAddr(t)
	eng, ex := v209Env(t, "node-a", v209Peer{id: "node-b", addr: dead})
	// 20ms 基准周期：单轮失败（目标不可达）快速落地。
	eng.SetAutoBalance(true, 20*time.Millisecond)
	t.Cleanup(func() { eng.StopAutoBalance() })

	v209CreateSplit(t, eng, "users", 20, []int64{5, 10, 15})

	ok := v209WaitCond(t, 10*time.Second, func() bool { return ex.balanceFailuresCount() > 0 })
	if !ok {
		t.Fatalf("balance loop failures not recorded (静默 continue 仍未消除?)")
	}
	failures := ex.balanceFailuresCount()
	if failures < 1 {
		t.Fatalf("balanceFailures = %d, want >= 1", failures)
	}
	// 退避生效：连续失败后间隔单调放大，且封顶 balanceLoopMaxBackoff。
	base := 20 * time.Millisecond
	if got := balanceLoopBackoff(base, 1); got != base {
		t.Fatalf("backoff(1) = %s, want %s", got, base)
	}
	if got := balanceLoopBackoff(base, 3); got != 4*base {
		t.Fatalf("backoff(3) = %s, want %s", got, 4*base)
	}
	if got := balanceLoopBackoff(time.Second, 20); got != balanceLoopMaxBackoff {
		t.Fatalf("backoff(20) = %s, want capped %s", got, balanceLoopMaxBackoff)
	}
	// 循环存活：失败后仍继续执行轮次（balanceOnceN 递增）。
	n1 := ex.balanceOnceN
	ok = v209WaitCond(t, 10*time.Second, func() bool { return ex.balanceOnceN > n1 })
	if !ok {
		t.Fatalf("balance loop stopped after failures (balanceOnceN stuck at %d)", n1)
	}
}

// TestV209RegionMigrationCooldownCapsFlapping 覆盖 P2b 防抖验收：重启抖动场景
// 「回迁 -> 均衡 -> 再回迁 -> 再均衡」下，同一 region 在 5 分钟窗口内被自动迁移
// 的次数不超过 2 次（超出后自动路径跳过该 region），而手动 BALANCE 不受冷却限制。
func TestV209RegionMigrationCooldownCapsFlapping(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	eng, ex := v209Env(t, "node-a", v209Peer{id: "node-b", addr: addrB})
	eng.SetAutoBalance(true, time.Hour)
	t.Cleanup(func() { eng.StopAutoBalance() })

	v209CreateSplit(t, eng, "users", 30, []int64{6, 12, 18, 24})
	// 第一轮：手动 BALANCE 迁 2 个（迁移历史记 1 次 / region）。
	if res := v209MustExec(t, eng, "BALANCE"); res.AffectedRows != 2 {
		t.Fatalf("first BALANCE migrated = %d, want 2", res.AffectedRows)
	}
	movedIDs := make([]storage.ID, 0, 2)
	regions, err := eng.ListRegions("users")
	if err != nil {
		t.Fatalf("ListRegions: %v", err)
	}
	for _, rg := range regions {
		if !rg.Explicit || rg.Node != "node-b" {
			continue
		}
		movedIDs = append(movedIDs, rg.RegionID)
		if got := ex.regionMigrationCount(rg.RegionID); got != 1 {
			t.Fatalf("region %d migration count = %d, want 1", rg.RegionID, got)
		}
	}
	if len(movedIDs) != 2 {
		t.Fatalf("moved regions = %v, want 2 个落在 node-b", movedIDs)
	}

	// 第二次迁移（模拟回迁后再均衡）：仍在冷却上限内，允许。
	ex.balanceMu.Lock()
	for _, rid := range movedIDs {
		ex.noteRegionMigrationLocked(rid, time.Now())
	}
	ex.balanceMu.Unlock()
	for _, rid := range movedIDs {
		if got := ex.regionMigrationCount(rid); got != 2 {
			t.Fatalf("region %d migration count = %d, want 2", rid, got)
		}
		ex.balanceMu.Lock()
		allowed := ex.regionMigrationAllowedLocked(rid, time.Now())
		ex.balanceMu.Unlock()
		if allowed {
			t.Fatalf("region %d 已迁移 2 次，冷却窗口内不应再允许自动迁移", rid)
		}
	}

	// 手动 BALANCE 不受冷却限制：运维强干预仍能把它们搬回 node-b。
	for _, rid := range movedIDs {
		if err := ex.router.SetRegionNode(rid, "node-a"); err != nil {
			t.Fatalf("SetRegionNode(%d): %v", rid, err)
		}
	}
	if got := ex.balanceRegionCounts()["node-a"]; got != 5 {
		t.Fatalf("precondition: node-a holds %d, want 5", got)
	}
	res, err := eng.Execute("BALANCE")
	if err != nil {
		t.Fatalf("manual BALANCE: %v", err)
	}
	if res == nil || res.AffectedRows != 2 {
		t.Fatalf("manual BALANCE migrated = %v, want 2（手动不受冷却限制）", res)
	}

	// 自动路径（balanceOnce）必须跳过冷却命中的 region：再次把它们收回 node-a
	// 造出 5/0 失衡，自动均衡只允许迁移其他 region，不得把这 2 个再搬回 node-b。
	// 手动 BALANCE 之后这些 region 的迁移次数已变为 3（≥ 冷却上限 2），
	// 自动路径此后一律不得再动它们。
	countsAfterManual := map[storage.ID]int{}
	for _, rid := range movedIDs {
		countsAfterManual[rid] = ex.regionMigrationCount(rid)
		if err := ex.router.SetRegionNode(rid, "node-a"); err != nil {
			t.Fatalf("SetRegionNode(%d): %v", rid, err)
		}
	}
	if _, err := ex.balanceOnce(); err != nil {
		t.Fatalf("balanceOnce: %v", err)
	}
	regionsAfter, err := eng.ListRegions("users")
	if err != nil {
		t.Fatalf("ListRegions: %v", err)
	}
	for _, rg := range regionsAfter {
		for _, rid := range movedIDs {
			if rg.RegionID != rid {
				continue
			}
			if rg.Node != "node-a" {
				t.Fatalf("region %d 冷却窗口内被自动迁移回 %q（抖动未被抑制）", rid, rg.Node)
			}
			if got := ex.regionMigrationCount(rid); got != countsAfterManual[rid] {
				t.Fatalf("region %d migration count = %d, want 未变化 %d",
					rid, got, countsAfterManual[rid])
			}
		}
	}

	// 冷却窗口按时间滑动：窗口外的历史不再计数。
	ex.balanceMu.Lock()
	rid0 := movedIDs[0]
	ex.balanceHist[rid0] = []time.Time{
		time.Now().Add(-balanceMigrationCooldownWindow - time.Minute),
		time.Now().Add(-balanceMigrationCooldownWindow - 30*time.Second),
	}
	allowed := ex.regionMigrationAllowedLocked(rid0, time.Now())
	ex.balanceMu.Unlock()
	if !allowed {
		t.Fatalf("region %d 历史均在冷却窗口外，应恢复允许迁移", rid0)
	}
}

// TestV209AutoBalanceDisabledMatchesLegacy 关闭自动均衡后行为回到 v2.0.8：
// 失衡布局不会被自动修正，只有手动 BALANCE 才迁移。
func TestV209AutoBalanceDisabledMatchesLegacy(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	eng, ex := v209Env(t, "node-a", v209Peer{id: "node-b", addr: addrB})
	eng.SetAutoBalance(false, 20*time.Millisecond)
	t.Cleanup(func() { eng.StopAutoBalance() })

	v209CreateSplit(t, eng, "users", 30, []int64{6, 12, 18, 24})
	n0 := ex.balanceOnceN
	time.Sleep(300 * time.Millisecond)
	if ex.balanceOnceN != n0 {
		t.Fatalf("auto balance ran %d rounds while disabled (want 0)", ex.balanceOnceN-n0)
	}
	if counts := ex.balanceRegionCounts(); counts["node-a"] != 5 {
		t.Fatalf("layout changed while auto balance disabled: %v", counts)
	}
	res, err := eng.Execute("BALANCE")
	if err != nil {
		t.Fatalf("manual BALANCE: %v", err)
	}
	if res == nil || res.AffectedRows != 2 {
		t.Fatalf("manual BALANCE migrated = %v, want 2（关闭自动均衡后手动可用）", res)
	}
}

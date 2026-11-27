package sql

// v2.0.9 Q4 BALANCE 收敛缺陷回归用例（KNOWN_LIMITATIONS §3.9）。
//
// 缺陷背景：v2.0.8 及以前的 balanceOnce 每轮只迁 1 个 region 就返回，且本节点计数
// 走 localRegionLoad（含隐式整表 region）、远端计数走 regionCountOnNode（只数显式
// 路由），两侧口径不一致。结果是三机复测中连续 4 轮 BALANCE 后 node-a 仍滞留 13 个
// region，迁移条数与 SHOW REGION ROUTES 的实际变化也对不上。
//
// 本文件覆盖三项验收点：
//  1. TestV209SingleNodeMajorityConvergesInOneRun：单节点持有多数 region 时，一条
//     BALANCE 在一次执行内持续迁移直至极差 <= balanceConvergedSpread；
//  2. TestV209ImplicitRegionCountedPerNode：未切分表的隐式整表 region
//     （tableID|1<<63）参与各节点计数，归属变更后能反映到对应节点的计数上；
//  3. TestV209MigrateFailureReturnsMigratedCount：迁移中途失败时返回已迁移条数并
//     中止本轮，不吞错、不继续尝试其余 region。
//
// 装配说明：落在 pkg/sql 包内白盒测试，使用 btree + WAL + 真实 TxnManager 装配执行器，
// 并用进程内 cluster.Server 节点承接 REGION_PUSH，使 migrateRegion 走真实迁移链路
// （而非替身），保证"迁移条数"断言对应真实迁移。

import (
	"encoding/binary"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
	"github.com/zhengkuanhua/openxdb/pkg/wal"
)

// v209Boundary 构造 SplitTable 用的内层边界键（8 字节大端整数 id）。
func v209Boundary(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

// v209StartNode 起一个进程内集群节点（btree 存储 + REGION_PUSH 服务端），返回监听地址。
// 目标端必须注入路由视图，否则 applyPush 无法学路由并返回 "router not injected"。
func v209StartNode(t *testing.T, id string) string {
	t.Helper()
	srv := cluster.NewServer(btree.NewKV(32), id)
	srv.SetRouter(sharding.NewRouter())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("%s listen: %v", id, err)
	}
	addr, err := srv.Serve(ln)
	if err != nil {
		t.Fatalf("%s Serve: %v", id, err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return addr
}

// v209DeadAddr 返回一个"监听后立刻关闭"的本机端口：连接被立即拒绝，
// 用于构造迁移目标不可达且不引入 dial 超时等待（dialTimeout 为 3s）。
func v209DeadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close probe listener: %v", err)
	}
	return addr
}

// v209Peer 一个远端节点描述：addr 为真实监听地址，或已关闭端口（不可达）。
type v209Peer struct {
	id   string
	addr string
}

// v209Env 装配引擎 + 集群管理面：本节点使用真实存储（btree + WAL + txn），
// 对端按给定地址注册为集群成员（不做握手，便于注入不可达节点）。
func v209Env(t *testing.T, selfID string, peers ...v209Peer) (*Engine, *Executor) {
	t.Helper()
	st := btree.NewKV(32)
	w, err := wal.Open(filepath.Join(testDir(t), "wal.log"))
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	tm, err := txn.New(st, w)
	if err != nil {
		t.Fatalf("txn.New: %v", err)
	}
	eng := New(tm)
	mgr := cluster.NewManager(selfID)
	for _, p := range peers {
		mgr.RegisterPeer(p.id, p.addr)
	}
	eng.SetCluster(mgr, selfID)
	return eng, eng.ex
}

// v209MustExec 执行 SQL 语句，失败即终止用例。
func v209MustExec(t *testing.T, eng *Engine, stmt string) *Result {
	t.Helper()
	res, err := eng.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute(%q): %v", stmt, err)
	}
	return res
}

// v209CreateSplit 建表并写入 rows 行，再按 boundaries 切分为 len(boundaries)+1 个
// 显式 region（初始全部归属本节点，Node==""）；boundaries 为空则不切分。
func v209CreateSplit(t *testing.T, eng *Engine, table string, rows int, boundaries []int64) {
	t.Helper()
	v209MustExec(t, eng, fmt.Sprintf("CREATE TABLE %s (id INT, name TEXT, PRIMARY KEY (id))", table))
	for i := 1; i <= rows; i++ {
		v209MustExec(t, eng, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (%d, 'r%d')", table, i, i))
	}
	if len(boundaries) == 0 {
		return
	}
	bs := make([][]byte, 0, len(boundaries))
	for _, b := range boundaries {
		bs = append(bs, v209Boundary(b))
	}
	if err := eng.SplitTable(table, bs); err != nil {
		t.Fatalf("SplitTable(%s): %v", table, err)
	}
}

// v209RegionCount 表当前的显式 region 数。
func v209RegionCount(t *testing.T, eng *Engine, table string) int {
	t.Helper()
	rs, err := eng.ListRegions(table)
	if err != nil {
		t.Fatalf("ListRegions(%s): %v", table, err)
	}
	n := 0
	for _, r := range rs {
		if r.Explicit {
			n++
		}
	}
	return n
}

// v209Spread 各节点 region 数极差；未出现在计数里的节点按 0 补齐。
func v209Spread(counts map[string]int, nodes ...string) int {
	all := map[string]bool{}
	for k := range counts {
		all[k] = true
	}
	for _, n := range nodes {
		all[n] = true
	}
	var minC, maxC int
	first := true
	for n := range all {
		c := counts[n]
		if first {
			minC, maxC, first = c, c, false
			continue
		}
		if c < minC {
			minC = c
		}
		if c > maxC {
			maxC = c
		}
	}
	return maxC - minC
}

// TestV209SingleNodeMajorityConvergesInOneRun 单节点持有多数 region 时，一条 BALANCE
// 在当前执行内持续迁移直到极差 <=1：5 个 region 全在本节点，需迁 2 次
// （5/0 -> 4/1 -> 3/2），语句返回 AffectedRows=2；再执行一次已收敛，返回 0。
func TestV209SingleNodeMajorityConvergesInOneRun(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	eng, ex := v209Env(t, "node-a", v209Peer{id: "node-b", addr: addrB})

	v209CreateSplit(t, eng, "users", 30, []int64{6, 12, 18, 24})
	if got := v209RegionCount(t, eng, "users"); got != 5 {
		t.Fatalf("precondition: users regions = %d, want 5", got)
	}
	before := ex.balanceRegionCounts()
	if before["node-a"] != 5 || before["node-b"] != 0 {
		t.Fatalf("precondition counts = %v, want node-a=5 node-b=0", before)
	}

	res, err := eng.Execute("BALANCE")
	if err != nil {
		t.Fatalf("BALANCE: %v", err)
	}
	if res == nil || res.AffectedRows != 2 {
		t.Fatalf("single BALANCE migrated = %v, want 2 (一次执行内收敛 5/0 -> 3/2)", res)
	}

	after := ex.balanceRegionCounts()
	if after["node-a"] != 3 || after["node-b"] != 2 {
		t.Fatalf("after counts = %v, want node-a=3 node-b=2", after)
	}
	if sp := v209Spread(after, "node-a", "node-b"); sp > balanceConvergedSpread {
		t.Fatalf("spread after one BALANCE = %d, want <= %d", sp, balanceConvergedSpread)
	}
	// 迁移只切路由归属、不删路由条目：显式 region 数仍为 5。
	if got := v209RegionCount(t, eng, "users"); got != 5 {
		t.Fatalf("explicit region count = %d, want 5 (路由切换不删条目)", got)
	}
	// 数据仍可读（源节点保留副本，写入目标节点）。
	r := v209MustExec(t, eng, "SELECT COUNT(*) FROM users")
	if len(r.Rows) != 1 || r.Rows[0][0].I != 30 {
		t.Fatalf("row count after migrate = %v, want 30", r.Rows)
	}

	res2, err := eng.Execute("BALANCE")
	if err != nil {
		t.Fatalf("second BALANCE: %v", err)
	}
	if res2 == nil || res2.AffectedRows != 0 {
		t.Fatalf("second BALANCE migrated = %v, want 0 (已收敛)", res2)
	}
}

// TestV209ImplicitRegionCountedPerNode 隐式整表 region（未切分表）必须计入各节点
// 真实持有数：默认按 implicitRouteOwner（节点 ID 字典序最小者）派生归属，
// 学到远端归属后计数随之转移；同时不得与显式路由条目重复计数。
func TestV209ImplicitRegionCountedPerNode(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	eng, ex := v209Env(t, "node-a", v209Peer{id: "node-b", addr: addrB})

	// logs 不切分 -> 1 个隐式整表 region；users 切 3 个边界 -> 4 个显式 region。
	v209CreateSplit(t, eng, "logs", 5, nil)
	v209CreateSplit(t, eng, "users", 20, []int64{5, 10, 15})
	if got := v209RegionCount(t, eng, "users"); got != 4 {
		t.Fatalf("precondition: users regions = %d, want 4", got)
	}

	// 隐式归属派生：selfID 与对端 ID 中字典序最小者 -> node-a。
	before := ex.balanceRegionCounts()
	if before["node-a"] != 5 || before["node-b"] != 0 {
		t.Fatalf("implicit region not counted: counts = %v, want node-a=5 node-b=0", before)
	}
	// 与 localRegionLoad 条数一致，证明隐式 region 只计一次（不重复计数）。
	local, err := ex.localRegionLoad()
	if err != nil {
		t.Fatalf("localRegionLoad: %v", err)
	}
	if len(local) != before["node-a"] {
		t.Fatalf("counts/node-a = %d vs localRegionLoad = %d, want equal (无重复计数)",
			before["node-a"], len(local))
	}

	// 学到远端归属后，隐式 region 计数转移到 node-b：这正是旧 regionCountOnNode
	// （只数显式路由）看不到的部分。
	rs, err := eng.ListRegions("logs")
	if err != nil || len(rs) == 0 {
		t.Fatalf("ListRegions(logs): rs=%v err=%v", rs, err)
	}
	tid := rs[0].TableID
	if !ex.router.SetImplicitRegionOwner(tid, "node-b") {
		t.Fatalf("SetImplicitRegionOwner(logs=%d) rejected", tid)
	}
	after := ex.balanceRegionCounts()
	if after["node-a"] != 4 || after["node-b"] != 1 {
		t.Fatalf("after implicit ownership transfer counts = %v, want node-a=4 node-b=1", after)
	}
	if got := ex.regionCountOnNode("node-b"); got != 1 {
		t.Fatalf("regionCountOnNode(node-b) = %d, want 1 (口径含隐式整表 region)", got)
	}
	// 隐式归属转走后，本节点可迁移条目只剩显式 4 个。
	local2, err := ex.localRegionLoad()
	if err != nil {
		t.Fatalf("localRegionLoad: %v", err)
	}
	if len(local2) != 4 {
		t.Fatalf("localRegionLoad = %d, want 4 (隐式 region 已归远端)", len(local2))
	}
}

// TestV209MigrateFailureReturnsMigratedCount 迁移中途失败：先成功迁 1 个到 node-b，
// 再迁往不可达的 node-c 失败，本轮立即中止并返回已迁移条数 1；
// 语句级 execBalance 同样在 Result 中带出该条数（AffectedRows=1）且返回错误。
func TestV209MigrateFailureReturnsMigratedCount(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	deadC := v209DeadAddr(t)
	eng, ex := v209Env(t, "node-a",
		v209Peer{id: "node-b", addr: addrB},
		v209Peer{id: "node-c", addr: deadC})

	// 6 个 region 全在本节点；候选按节点 ID 排序，最闲节点先取 node-b。
	v209CreateSplit(t, eng, "users", 30, []int64{5, 10, 15, 20, 25})
	if got := v209RegionCount(t, eng, "users"); got != 6 {
		t.Fatalf("precondition: users regions = %d, want 6", got)
	}
	before := ex.balanceRegionCounts()
	if before["node-a"] != 6 {
		t.Fatalf("precondition counts = %v, want node-a=6", before)
	}

	// 语句级：Result 携带已迁移条数，err 非空。
	res, err := ex.execBalance(nil, &BalanceStmt{})
	if err == nil {
		t.Fatalf("execBalance: want error on unreachable target, got nil")
	}
	if res == nil || res.AffectedRows != 1 {
		t.Fatalf("execBalance AffectedRows = %v, want 1 (已迁移条数)", res)
	}
	after := ex.balanceRegionCounts()
	if after["node-a"] != 5 || after["node-b"] != 1 {
		t.Fatalf("after failure counts = %v, want node-a=5 node-b=1", after)
	}

	// 再跑一次：最闲节点变为 node-c（0 个 region），首个迁移即失败，已迁移数 0。
	n2, err2 := ex.balanceOnce()
	if err2 == nil {
		t.Fatalf("balanceOnce: want error on unreachable target, got nil")
	}
	if n2 != 0 {
		t.Fatalf("balanceOnce migrated = %d, want 0 (首个迁移即失败)", n2)
	}
	after2 := ex.balanceRegionCounts()
	if after2["node-a"] != 5 || after2["node-b"] != 1 || after2["node-c"] != 0 {
		t.Fatalf("counts after aborted run = %v, want node-a=5 node-b=1 node-c=0", after2)
	}
	// 迁移失败不破坏路由归属：本节点仍持有 5 个 region。
	if got := ex.regionCountOnNode("node-a"); got != 5 {
		t.Fatalf("regionCountOnNode(node-a) = %d, want 5", got)
	}
}

// v209BalanceNeed 单节点独占 regions 个 region、集群共 nodes 个节点时，收敛到
// 极差 <=1 必须迁出的条数下界：k = N - ⌈N/M⌉（与 balanceMaxMigrationsPerRun
// 注释里的闭式解同式，用例里用它独立推算期望值而不是硬编码数字）。
func v209BalanceNeed(regions, nodes int) int {
	return regions - (regions+nodes-1)/nodes
}

// TestV209_14RegionsSingleNodeConverges 复现 v2.0.8 Q4 现场的三节点 14/0/0 偏斜：
// node-a 持 14 个 region、node-b / node-c 各 0；一条 BALANCE 必须迁出 k(14,3)=9 个
// 并收敛到极差 <=1（5/5/4 一类分布），二次执行迁移数为 0（幂等，不再回摆）。
func TestV209_14RegionsSingleNodeConverges(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	addrC := v209StartNode(t, "node-c")
	eng, ex := v209Env(t, "node-a",
		v209Peer{id: "node-b", addr: addrB},
		v209Peer{id: "node-c", addr: addrC})

	// 14 个 region，每 region 1 行（行数不参与判据，压小负载让用例跑得快）。
	regions := 14
	bounds := make([]int64, 0, regions-1)
	for i := 1; i < regions; i++ {
		bounds = append(bounds, int64(i))
	}
	v209CreateSplit(t, eng, "users", regions, bounds)
	if got := v209RegionCount(t, eng, "users"); got != regions {
		t.Fatalf("precondition: users regions = %d, want %d", got, regions)
	}
	before := ex.balanceRegionCounts()
	if before["node-a"] != regions || before["node-b"] != 0 || before["node-c"] != 0 {
		t.Fatalf("precondition counts = %v, want node-a=%d node-b=0 node-c=0", before, regions)
	}

	// 单轮平衡：迁移数必须等于闭式下界（旧上限 8 在这里必然截断，留下残差）。
	wantMigrated := v209BalanceNeed(regions, 3) // 14 - ⌈14/3⌉ = 9
	res, err := eng.Execute("BALANCE")
	if err != nil {
		t.Fatalf("BALANCE: %v", err)
	}
	if res == nil || res.AffectedRows != wantMigrated {
		t.Fatalf("single BALANCE migrated = %v, want %d (14/0/0 一条语句收敛)", res, wantMigrated)
	}
	if wantMigrated >= balanceMaxMigrationsPerRun {
		t.Fatalf("case no longer exercises the limit: need %d >= limit %d",
			wantMigrated, balanceMaxMigrationsPerRun)
	}

	after := ex.balanceRegionCounts()
	if sp := v209Spread(after, "node-a", "node-b", "node-c"); sp > balanceConvergedSpread {
		t.Fatalf("spread after one BALANCE = %d, want <= %d (counts=%v)",
			sp, balanceConvergedSpread, after)
	}
	if after["node-a"] != regions-wantMigrated {
		t.Fatalf("counts = %v, want node-a=%d", after, regions-wantMigrated)
	}
	if after["node-b"]+after["node-c"] != wantMigrated {
		t.Fatalf("counts = %v, want node-b+node-c=%d", after, wantMigrated)
	}
	// 迁移只切路由归属、不删条目、不丢数据。
	if got := v209RegionCount(t, eng, "users"); got != regions {
		t.Fatalf("explicit region count = %d, want %d (路由切换不删条目)", got, regions)
	}
	r := v209MustExec(t, eng, "SELECT COUNT(*) FROM users")
	if len(r.Rows) != 1 || r.Rows[0][0].I != int64(regions) {
		t.Fatalf("row count after migrate = %v, want %d", r.Rows, regions)
	}

	// 二次执行：已收敛，迁移数为 0（不做无谓回摆）。
	res2, err := eng.Execute("BALANCE")
	if err != nil {
		t.Fatalf("second BALANCE: %v", err)
	}
	if res2 == nil || res2.AffectedRows != 0 {
		t.Fatalf("second BALANCE migrated = %v, want 0 (已收敛)", res2)
	}
	if again := ex.balanceRegionCounts(); v209Spread(again, "node-a", "node-b", "node-c") > balanceConvergedSpread {
		t.Fatalf("counts drifted after second BALANCE: %v", again)
	}
}

// TestV209MigrationLimitIsBounded 单轮迁移上限在新取值（32）下仍然生效：三节点构造
// 需要超过上限次迁移的偏斜，一条 BALANCE 的迁移数恒等于 balanceMaxMigrationsPerRun
// 且被截断（极差仍 >1），剩余偏斜单调递减地留给下一轮（不丢条目、不回摆）。
func TestV209MigrationLimitIsBounded(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	addrC := v209StartNode(t, "node-c")
	eng, ex := v209Env(t, "node-a",
		v209Peer{id: "node-b", addr: addrB},
		v209Peer{id: "node-c", addr: addrC})

	// 让收敛所需迁移数 k(N,3)=N-⌈N/3⌉ 严格大于单轮上限：N = limit+20 = 52 -> 需迁 34。
	regions := balanceMaxMigrationsPerRun + 20
	if need := v209BalanceNeed(regions, 3); need <= balanceMaxMigrationsPerRun {
		t.Fatalf("case no longer exceeds the limit: need %d <= limit %d",
			need, balanceMaxMigrationsPerRun)
	}
	bounds := make([]int64, 0, regions-1)
	for i := 1; i < regions; i++ {
		bounds = append(bounds, int64(i))
	}
	v209CreateSplit(t, eng, "users", regions, bounds)
	if got := v209RegionCount(t, eng, "users"); got != regions {
		t.Fatalf("precondition: users regions = %d, want %d", got, regions)
	}

	n, err := ex.balanceOnce()
	if err != nil {
		t.Fatalf("balanceOnce: %v", err)
	}
	if n != balanceMaxMigrationsPerRun {
		t.Fatalf("balanceOnce migrated = %d, want limit %d", n, balanceMaxMigrationsPerRun)
	}
	counts := ex.balanceRegionCounts()
	if counts["node-a"] != regions-balanceMaxMigrationsPerRun {
		t.Fatalf("counts = %v, want node-a=%d (本节点只减了上限条)",
			counts, regions-balanceMaxMigrationsPerRun)
	}
	if counts["node-b"]+counts["node-c"] != balanceMaxMigrationsPerRun {
		t.Fatalf("counts = %v, want node-b+node-c=%d", counts, balanceMaxMigrationsPerRun)
	}
	if sp := v209Spread(counts, "node-a", "node-b", "node-c"); sp <= balanceConvergedSpread {
		t.Fatalf("spread = %d, want > %d (达上限后仍有偏斜)", sp, balanceConvergedSpread)
	}

	// 下一轮继续朝收敛方向推进且未再触上限：偏斜单调递减。
	n2, err := ex.balanceOnce()
	if err != nil {
		t.Fatalf("second balanceOnce: %v", err)
	}
	if n2 <= 0 || n2 >= balanceMaxMigrationsPerRun {
		t.Fatalf("second balanceOnce migrated = %d, want 0 < n < limit %d", n2, balanceMaxMigrationsPerRun)
	}
	after := ex.balanceRegionCounts()
	if after["node-a"] != counts["node-a"]-n2 {
		t.Fatalf("counts after second run = %v, want node-a=%d (单调递减)", after, counts["node-a"]-n2)
	}
	if sp := v209Spread(after, "node-a", "node-b", "node-c"); sp >= v209Spread(counts, "node-a", "node-b", "node-c") {
		t.Fatalf("spread not decreasing: before=%d after=%d",
			v209Spread(counts, "node-a", "node-b", "node-c"), sp)
	}
}

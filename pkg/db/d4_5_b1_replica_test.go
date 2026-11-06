package db_test

// D4.5 §6 第 1 项（B1）端到端回归：单副本不可恢复。
//
// 缺口（修复前）：region 只有一份物理副本（初始建表/分裂后均只在 owner 上），
// owner 所在节点损毁后，集群内无任何存活副本，FailoverDownNode 返回
// no live data copy，路由仍指向已失联节点 —— 该 region 永久不可读，
// 即"单点副本损毁 = 整体不可用"。
//
// 修复（pkg/sql/ha_b1.go + pkg/db/ha_replica.go，ha_replica_enabled /
// ha_quorum_enabled 生效）：周期/开关/节点回 UP 三处触发主动副本补齐
// （EnsureRegionReplicas 以副本语义推影子副本、不改 owner），使任一单节点
// 损毁后仍有存活副本可被 failover 接管；遇无存活副本时不再永久放弃，
// 记 pending 并冷却重试。
//
// 本测试走真实三节点组网（真实心跳/路由/存储）：
//  1. A 建表写入并切 3 region，此时 3 个 region 均只有 A 一份副本（缺口原貌）；
//  2. 三端开启副本冗余，断言 A 完成补齐（B1Stats ensured 增长）；
//  3. 关闭 A（模拟 owner 节点单点损毁）；
//  4. B/C 自动 failover 接管后，users 全表数据必须仍可读（修复前此处
//     no live data copy，数据整体不可用）。

import (
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// openNodeWithDir 打开一个节点并返回（DB、监听地址、数据目录）。
func openNodeWithDir(t *testing.T, name string) (*db.DB, string, string) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("%s init: %v", name, err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("%s open: %v", name, err)
	}
	addr, err := d.StartCluster("127.0.0.1:0")
	if err != nil {
		t.Fatalf("%s start cluster: %v", name, err)
	}
	return d, addr, dir
}

// waitB1Ensured 轮询直到节点累计补齐副本数达到 want（副本维护循环/开关触发）。
func waitB1Ensured(t *testing.T, d *db.DB, want uint64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var pending, lost int
	var ensured uint64
	for time.Now().Before(deadline) {
		pending, lost, ensured = d.B1Stats()
		if ensured >= want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("B1 ensure count = %d (want >= %d), pending=%d lost=%d", ensured, want, pending, lost)
}

// waitUsersReadable 轮询直到节点可读出 users 全表 want 行（owner 失联后的
// failover 接管需要若干心跳周期，故只能轮询不能一次断言）。
func waitUsersReadable(t *testing.T, d *db.DB, want int) {
	t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	var lastErr error
	var lastRows int
	for time.Now().Before(deadline) {
		res, err := d.SQL.Execute("SELECT id FROM users ORDER BY id")
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		lastRows = len(res.Rows)
		if lastRows == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("users readable rows = %d (want %d), lastErr=%v", lastRows, want, lastErr)
}

// b1StatsOf 便捷读取 B1 三项观测（诊断输出用）。
func b1StatsOf(d *db.DB) [3]int {
	pending, lost, ensured := d.B1Stats()
	return [3]int{pending, lost, int(ensured)}
}

// TestD45B1SingleCopyRecoverableE2E 验证单副本 region 被主动补齐后，
// owner 节点损毁不再导致整体不可用。
func TestD45B1SingleCopyRecoverableE2E(t *testing.T) {
	dA, addrA, _ := openNodeWithDir(t, "node-a")
	closedA := false
	t.Cleanup(func() {
		if !closedA {
			_ = dA.Close()
		}
	})
	dB, addrB, _ := openNodeWithDir(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })
	dC, addrC, _ := openNodeWithDir(t, "node-c")
	t.Cleanup(func() { _ = dC.Close() })

	aID, bID, cID := dA.Cluster.SelfID, dB.Cluster.SelfID, dC.Cluster.SelfID

	// ---- 组网就绪（先组网、后写入，保证 region 数据只落 owner 一份）----
	for _, d := range []*db.DB{dA, dB, dC} {
		for _, peer := range []string{addrA, addrB, addrC} {
			if peer == d.Cluster.SelfAddr {
				continue
			}
			mustSQL(t, d, "ADD NODE '"+peer+"'")
		}
	}
	for _, d := range []*db.DB{dA, dB, dC} {
		speedupHeartbeat(t, d)
	}
	waitEverUp(t, dA, bID)
	waitEverUp(t, dA, cID)
	waitEverUp(t, dB, aID)
	waitEverUp(t, dC, aID)

	// 组网后由 A 建表写入并切 3 region：表元数据随 DDL 同步到 B/C，
	// 但物理行只写 owner（A）一侧 —— 即 B1 缺口原貌（单副本）。
	regions := seedThreeRegions(t, dA)

	// 缺口原貌：尚未开启冗余时不应有任何补齐动作（零回归）。
	if pending, lost, ensured := dA.B1Stats(); ensured != 0 || pending != 0 || lost != 0 {
		t.Fatalf("baseline B1 stats = {pending:%d lost:%d ensured:%d}, want all 0 before enabling", pending, lost, ensured)
	}
	for _, d := range []*db.DB{dA, dB, dC} {
		if d.ReplicaMaintaining() {
			t.Fatal("replica maintainer must stay off by default (zero regression)")
		}
	}

	// ---- 开启副本冗余：单副本 region 立即/周期补齐影子副本 ----
	for _, d := range []*db.DB{dA, dB, dC} {
		d.SetReplicaMaintainInterval(300 * time.Millisecond)
		d.SetReplicaEnabled(true)
		if !d.ReplicaMaintaining() {
			t.Fatal("replica maintainer should be running after SetReplicaEnabled(true)")
		}
	}
	if n, err := dA.SQL.EnsureRegionReplicas(); err != nil || n == 0 {
		all := dA.SQL.ExecutorRouter().AllRegions()
		t.Logf("diag manual ensure: n=%d err=%v stats=%v allRegions=%d", n, err, b1StatsOf(dA), len(all))
		for _, rg := range all {
			t.Logf("diag region %d node=%q local=%v rows=%d..%d", rg.RegionID, rg.Node, rg.Local, rg.StartKey, rg.EndKey)
		}
	}
	waitB1Ensured(t, dA, uint64(len(regions)))
	// owner 归属不因补齐而改变：region 仍归属 A（副本语义，不改路由）。
	for _, rg := range regions {
		waitRoute(t, dA, rg.RegionID, aID, true)
	}

	// ---- 单点损毁：owner 节点 A 下线 ----
	_ = dA.Close()
	closedA = true

	// 修复前：no live data copy → 3 个 region 永久不可读。
	// 修复后：影子副本已在 B/C 上，failover 自动接管，全表数据可读。
	waitUsersReadable(t, dB, 10)
	waitUsersReadable(t, dC, 10)
}

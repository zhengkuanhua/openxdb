// P6 HA（NEW-1 修复）：重启双主分叉丢数据回归测试。
//
// 复现链路（Phase 4 三节点复测 NEW-1）：
//  1. region 2 在 A 停机期间被多数派重指派给 C（权威 epoch 以多数派为准）；
//  2. A 重启，磁盘装载重启前的旧归属视图（r2→A），日志
//     `rejoin region 2 failed: stale region epoch 1 < known 2 (rid=2)`；
//  3. 旧实现以本机自增 regionEpoch 单键比较，把多数派权威 epoch 判为陈旧而
//     拒收，A 保留旧视图继续接受本地写入（写 70），B/C 写 71/72，双主；
//  4. A 再次重启后收敛到权威视图，A 侧 70 被静默丢弃。
//
// 本文件断言修复后的三条不变量：
//   - 同类 stale-epoch rejoin 不再拒收权威视图（term 优先的字典序比较 + 无快照
//     时以权威源为准采纳），视图必须收敛、不得保留归属旧视图；
//   - 归属 epoch 与 quorum 授权纪元同域，跨节点可比（本机 counter 落后也能接受
//     权威指派）；
//   - 重启后归属视图未与权威源确认前，集群 + quorum + epoch 全开场景禁止本地
//     region 落盘写（双写分叉熔断），确认后自动解除；回迁失败主动拉取权威视图
//     收敛，且不删除本地任何数据（不静默丢弃）。
package sql

import (
	"sync"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// TestP6New1StaleEpochRejoinAdoptsAuthoritative 核心回归：
// 重启节点持有旧归属视图（r2→本机、本地自增 epoch 2），多数派权威宣告
// r2→C（term 7 / epoch 7）时必须被接受并采纳，不得判 stale 拒绝。
func TestP6New1StaleEpochRejoinAdoptsAuthoritative(t *testing.T) {
	src := &fakeEpochSrc{known: 1}
	e := newP1Executor(src, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")
	// A 的重启前视图：r2 归属 A，本地自增纪元 2（跨节点不可比，正是旧实现
	// 误判 stale 的来源）。
	if err := e.router.UpsertRegion(regionOf(2, "node-a", 2)); err != nil {
		t.Fatalf("seed stale route view: %v", err)
	}

	// 权威源（多数派 C 侧）宣告 r2 的归属纪元：term 7 / epoch 7。
	if err := e.checkRegionPushEpoch(7, 7, 2); err != nil {
		t.Fatalf("authoritative region epoch must be accepted (NEW-1): %v", err)
	}
	ep, ok := e.regionEpochView(2)
	if !ok {
		t.Fatalf("region epoch view must be recorded after adopt")
	}
	if ep.clusterEpoch != 7 || ep.regionEpoch != 7 {
		t.Fatalf("adopted view: want term/epoch 7/7, got %d/%d", ep.clusterEpoch, ep.regionEpoch)
	}
	if !e.regionViewConfirmed() {
		t.Fatalf("view must be marked confirmed once authoritative epoch accepted")
	}

	// 更旧的权威消息仍必须拒绝（防回退到旧视图）。
	if err := e.checkRegionPushEpoch(7, 6, 2); err == nil {
		t.Fatalf("older regionEpoch in the same term must be rejected")
	}
	if err := e.checkRegionPushEpoch(6, 1, 2); err == nil {
		t.Fatalf("older term must be rejected")
	}
}

// TestP6New1OwnershipEpochCrossNodeComparable 归属 epoch 与 quorum 授权纪元
// 同域：本机 counter 落后（2）的节点必须接受 term 7 的权威指派，本机取号也
// 不得低于授权纪元。
func TestP6New1OwnershipEpochCrossNodeComparable(t *testing.T) {
	a := newP1Executor(&fakeEpochSrc{known: 5}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")
	if got := a.nextRegionEpoch(2, 7); got != 7 {
		t.Fatalf("nextRegionEpoch must follow quorum term: want 7, got %d", got)
	}
	if got := a.nextRegionEpoch(2, 7); got != 8 {
		t.Fatalf("nextRegionEpoch must stay monotonic: want 8, got %d", got)
	}

	// 另一端节点本地 counter 仅 2：仍须接受 term 7 的权威指派。
	b := newP1Executor(&fakeEpochSrc{known: 6}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-b")
	b.recordRegionEpoch(2, 2, 2)
	if err := b.checkRegionPushEpoch(7, 7, 2); err != nil {
		t.Fatalf("node with smaller local counter must accept authoritative epoch: %v", err)
	}
}

// TestP6New1AuthorizeFollowsQuorumEpoch region 重指派取号必须纳入 quorum 裁决：
// 本机已知归属纪元比本次取得的授权纪元更新 → 视图落后，拒绝重指派（不得凭
// 旧视图自升主形成双主）。
func TestP6New1AuthorizeFollowsQuorumEpoch(t *testing.T) {
	e := newP1Executor(&fakeEpochSrc{known: 9}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")
	e.recordRegionEpoch(2, 9, 9)
	if err := e.authorizeRegionEpoch(5); err == nil {
		t.Fatalf("reassign at epoch 5 must be rejected when region view already at epoch 9")
	}
	if err := e.authorizeRegionEpoch(9); err != nil {
		t.Fatalf("reassign at epoch 9 must be allowed: %v", err)
	}
	if err := e.authorizeRegionEpoch(10); err != nil {
		t.Fatalf("reassign at epoch 10 must be allowed: %v", err)
	}
}

// TestP6New1UnconfirmedViewWriteFence 重启装载旧视图后、权威源确认前的本地
// region 写必须被拒绝（双写分叉熔断）；权威消息到达后自动解除。
func TestP6New1UnconfirmedViewWriteFence(t *testing.T) {
	e := newP1Executor(&fakeEpochSrc{known: 3}, &fakeQuorum{}, &fakeCfg{epoch: true, quorum: true}, "node-a")
	e.mgr.RegisterPeer("node-b", "127.0.0.1:1")
	if err := e.router.UpsertRegion(regionOf(2, "node-a", 2)); err != nil {
		t.Fatalf("seed route: %v", err)
	}

	e.MarkRegionOwnershipUnconfirmed()
	if err := e.regionOwnerFence(2); err == nil {
		t.Fatalf("write must be fenced before authoritative view confirmation (NEW-1)")
	}
	if e.p3Fenced == 0 {
		t.Fatalf("fenced counter must be bumped")
	}

	// 权威路由消息被接受 → 视图确认（未确认栅栏解除）；但本地路由条目仍停在
	// 旧纪元 2 < 权威纪元 3，属于归属视图未收敛，必须继续拦截本地提交。
	if err := e.checkRegionPushEpoch(3, 3, 2); err != nil {
		t.Fatalf("checkRegionPushEpoch: %v", err)
	}
	if err := e.regionOwnerFence(2); err == nil {
		t.Fatalf("stale local route view must still be fenced after epoch adoption")
	}
	// 路由收敛到权威归属（applyPush 学习路由后 Node/epoch 同步更新）→ 放行。
	if err := e.router.UpsertRegion(regionOf(2, "node-c", 3)); err != nil {
		t.Fatalf("converge route: %v", err)
	}
	if err := e.regionOwnerFence(2); err != nil {
		t.Fatalf("write fence must release after route convergence: %v", err)
	}
}

// TestP6New1ConfirmedViewReleasesFence 权威视图拉取成功（db 侧
// MarkRegionViewConfirmed）后，即使路由条目纪元未变也不再被"未确认"栅栏拦截。
func TestP6New1ConfirmedViewReleasesFence(t *testing.T) {
	e := newP1Executor(&fakeEpochSrc{known: 3}, &fakeQuorum{}, &fakeCfg{epoch: true, quorum: true}, "node-a")
	e.mgr.RegisterPeer("node-b", "127.0.0.1:1")
	if err := e.router.UpsertRegion(regionOf(2, "node-a", 2)); err != nil {
		t.Fatalf("seed route: %v", err)
	}
	e.MarkRegionOwnershipUnconfirmed()
	if err := e.regionOwnerFence(2); err == nil {
		t.Fatalf("write must be fenced before confirmation")
	}
	e.MarkRegionViewConfirmed()
	if err := e.regionOwnerFence(2); err != nil {
		t.Fatalf("write fence must release after authoritative view refresh: %v", err)
	}
}

// TestP6New1FenceSkippedForSingleNode 单机/无远端节点不得被归属视图栅栏拦截
// （零回归）。
func TestP6New1FenceSkippedForSingleNode(t *testing.T) {
	e := newP1Executor(&fakeEpochSrc{known: 3}, &fakeQuorum{}, &fakeCfg{epoch: true, quorum: true}, "node-a")
	e.MarkRegionOwnershipUnconfirmed()
	if err := e.regionOwnerFence(2); err != nil {
		t.Fatalf("single-node cluster must not be fenced: %v", err)
	}
}

// TestP6New1RejoinFailureTriggersResync 回迁被拒（目标不可达/归属纪元陈旧）时
// 必须主动拉取权威路由视图收敛，且不删除本地任何数据。
func TestP6New1RejoinFailureTriggersResync(t *testing.T) {
	e := newP1Executor(&fakeEpochSrc{known: 3}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")
	e.mgr.RegisterPeer("node-b", "127.0.0.1:1") // 不可达：回迁推送必失败
	done := make(chan struct{}, 1)
	var once sync.Once
	e.SetPartitionRouteResync(func() { once.Do(func() { close(done) }) })

	rg := regionOf(2, "node-a", 2)
	rg.Local = true
	rg.SourceOwner = "node-b" // A 停机期间从 B 接管，需回迁
	if err := e.router.UpsertRegion(rg); err != nil {
		t.Fatalf("seed route: %v", err)
	}
	row := storage.KVPair{Key: sharding.EncodeRegionKey(2, []byte("k70")), Value: []byte("70")}
	// A 侧已写入的数据：回迁失败也必须保留（不得静默丢弃）。
	e.tm = &p1TxnMgr{rows: []storage.KVPair{row}}

	if _, err := e.rejoinOwnedRegions("node-b"); err == nil {
		t.Fatalf("rejoin to unreachable node must fail")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("stale-epoch rejoin must trigger authoritative route resync")
	}

	// 视图仍持有该 region 记录（等待权威收敛），本地数据行未被删除。
	if _, ok := e.router.FindRegion(2); !ok {
		t.Fatalf("route entry must be kept until authoritative convergence (no silent drop)")
	}
	rows, err := e.scanRegionRows(e.selfID, sharding.EncodeRegionKey(2, []byte("a")), sharding.EncodeRegionKey(3, nil))
	if err != nil {
		t.Fatalf("scan local rows: %v", err)
	}
	if len(rows) != 1 || string(rows[0].Value) != "70" {
		t.Fatalf("already-written data must be retained, got %+v", rows)
	}
}

// TestP6New1RemotePeerDetection hasRemotePeer 判定（归属视图栅栏生效前提）。
func TestP6New1RemotePeerDetection(t *testing.T) {
	e := newP1Executor(&fakeEpochSrc{known: 1}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")
	if e.hasRemotePeer() {
		t.Fatalf("fresh manager must not report remote peer")
	}
	e.mgr.RegisterPeer("node-b", "127.0.0.1:1")
	if !e.hasRemotePeer() {
		t.Fatalf("registered peer must be detected")
	}
	if mgr := cluster.NewManager("node-a"); mgr != nil {
		solo := newP1Executor(&fakeEpochSrc{known: 1}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")
		solo.mgr = mgr
		if solo.hasRemotePeer() {
			t.Fatalf("empty manager must not report remote peer")
		}
	}
}

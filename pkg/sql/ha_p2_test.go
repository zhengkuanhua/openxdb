package sql

// P2 分区恢复合并（集群轨路由收敛 + 冲突标记）测试：
// docs/HA_BRAIN_SPLIT_DESIGN.md §6 T4/T6 的 P2 部分。
//   - TestP2T4RegionViewReconcile（T4）：双 owner 形成后恢复网络，
//     regionEpoch 大者胜收敛为单一 owner（快照大者胜 / 路由大者胜）；
//   - TestP2T6StalePushConflictCount（T6）：陈旧 PushRegion 消息被拒并
//     累计冲突标记；开关关闭放行且计数恒 0（T8 零回归）。

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// TestP2T4RegionViewReconcile 分区恢复合并（T4，集群轨）：
// 双 owner 形成后恢复网络，路由视图按 regionEpoch 大者胜收敛。
func TestP2T4RegionViewReconcile(t *testing.T) {
	src := &fakeEpochSrc{known: 10}
	cfg := &fakeCfg{epoch: true}
	e := newP1Executor(src, &fakeQuorum{}, cfg, "node-a")

	// 场景 A：本机快照（新鲜侧）owner=node-a、regionEpoch=3；路由仍指向
	// 陈旧 owner=node-b、regionEpoch=1（分区内陈旧视图）→ 大者胜收敛到 node-a。
	e.recordRegionEpochOwner(1, 10, 3, "node-a")
	e.router.SetRegions(1, []sharding.RegionInfo{regionOf(1, "node-b", 1)})

	if err := e.ReconcileRegionView(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	rg, ok := e.router.FindRegion(1)
	if !ok || rg.Node != "node-a" {
		t.Fatalf("reconciled owner = %+v, want node-a (snapshot max wins)", rg)
	}
	if got := e.RegionConflictCount(); got != 0 {
		t.Fatalf("legit convergence conflict = %d, want 0", got)
	}

	// 场景 B：路由条目 epoch 更大（远端新视图 5 > 快照 3）→ 快照大者胜
	// 升级吸收路由条目 owner=node-b。
	e.router.SetRegions(1, []sharding.RegionInfo{regionOf(1, "node-b", 5)})
	if err := e.ReconcileRegionView(); err != nil {
		t.Fatalf("reconcile B: %v", err)
	}
	rg, _ = e.router.FindRegion(1)
	if rg.Node != "node-b" {
		t.Fatalf("route owner after absorb = %q, want node-b (route max wins)", rg.Node)
	}
	if got := e.regionEpochOf(1); got != 5 {
		t.Fatalf("regionEpochOf = %d, want 5 (absorbed route epoch)", got)
	}
	if got := e.p1Epoch[1].owner; got != "node-b" {
		t.Fatalf("snapshot owner = %q, want node-b (absorbed)", got)
	}

	// 开关关闭：不收敛（零回归），路由保持原状。
	e2 := newP1Executor(&fakeEpochSrc{known: 1}, &fakeQuorum{}, &fakeCfg{epoch: false}, "node-a")
	e2.recordRegionEpochOwner(1, 1, 9, "node-a")
	e2.router.SetRegions(1, []sharding.RegionInfo{regionOf(1, "node-b", 1)})
	if err := e2.ReconcileRegionView(); err != nil {
		t.Fatalf("disabled reconcile: %v", err)
	}
	rg2, _ := e2.router.FindRegion(1)
	if rg2.Node != "node-b" {
		t.Fatalf("disabled reconcile changed owner to %q, want node-b (zero regression)", rg2.Node)
	}
}

// TestP2T6StalePushConflictCount 陈旧 PushRegion 消息被拒并累计冲突标记
// （T6）；开关关闭时放行（旧行为）且计数恒 0（T8 零回归）。
func TestP2T6StalePushConflictCount(t *testing.T) {
	src := &fakeEpochSrc{known: 2}
	cfg := &fakeCfg{epoch: true}
	e := newP1Executor(src, &fakeQuorum{}, cfg, "node-a")
	e.p1Epoch[1] = p1RegionEpoch{clusterEpoch: 5, regionEpoch: 3}

	// 陈旧 PushRegion（regionEpoch 1 < 快照 3）→ 拒绝 + 计数。
	if err := e.checkRegionPushEpoch(1, 4, 1); err == nil {
		t.Fatal("stale push should be rejected")
	}
	if got := e.RegionConflictCount(); got != 1 {
		t.Fatalf("conflict count = %d, want 1", got)
	}
	// 再次陈旧 → 继续拒绝并累计。
	if err := e.checkRegionPushEpoch(1, 4, 1); err == nil {
		t.Fatal("second stale push should be rejected")
	}
	if got := e.RegionConflictCount(); got != 2 {
		t.Fatalf("conflict count after second stale push = %d, want 2", got)
	}
	// 新鲜 PushRegion（clusterEpoch 5 >= known 2、regionEpoch 3 == 快照）→ 放行、不计数。
	if err := e.checkRegionPushEpoch(5, 3, 1); err != nil {
		t.Fatalf("fresh push should pass: %v", err)
	}
	if got := e.RegionConflictCount(); got != 2 {
		t.Fatalf("conflict count after fresh push = %d, want 2", got)
	}

	// 开关关闭：陈旧消息放行（旧行为）、计数恒 0。
	e2 := newP1Executor(&fakeEpochSrc{known: 2}, &fakeQuorum{}, &fakeCfg{epoch: false}, "node-a")
	e2.p1Epoch[1] = p1RegionEpoch{clusterEpoch: 5, regionEpoch: 3}
	if err := e2.checkRegionPushEpoch(1, 4, 1); err != nil {
		t.Fatalf("disabled stale push should pass (zero regression): %v", err)
	}
	if got := e2.RegionConflictCount(); got != 0 {
		t.Fatalf("disabled conflict count = %d, want 0", got)
	}
}

var _ = storage.ID(0) // 保持 storage 导入（regionOf 内部使用同包，此处仅为显式依赖）

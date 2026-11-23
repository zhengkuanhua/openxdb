// v1.21.0 NEW-3（P0）：归属写 fencing 下沉 region 粒度的 db 侧回归测试。
//
// 缺陷现场（真机 node-c）：cluster_term.meta 持久化 own_term>0 / owned=false
// （历史误授被权威视图收回），而 v1.20.x 的 clusterOwnershipFenceLocked 是
// 节点级全局判定（!clusterOwned 即 ErrFenced），连"目标 region 归属其它节点、
// 本机只做 2PC 转发"的写也被拦死。
//
// 本文件覆盖修复要求 4 的两条反向用例：
//
//	a) TestNew3ForwardWriteNotFencedByRegionOwnership：节点不持有目标 region
//	   时，转发型写不被归属 fence 拦截；
//	b) TestNew3OwnedRegionRevokedStillFenced：本节点持有的 region 被权威视图
//	   收回后仍返回 ErrFenced。
//
// 以及零回归与重启待确认的 region 粒度语义。
package db

import (
	"errors"
	"testing"
)

// new3SetRegionOwner 装配 region 归属判定：owned 中的 rid 视为本机持有该
// region，known 中的 rid 视为归属可判定（其余不可判定，交由节点级判定兜底）。
func new3SetRegionOwner(d *DB, owned, known map[uint64]bool) {
	d.SetLocalRegionOwner(func(rid uint64) (bool, bool) {
		return owned[rid], known[rid]
	})
}

// TestNew3ForwardWriteNotFencedByRegionOwnership 用例 a：非 owner 节点的
// 转发型写不被归属 fence 拦截。
func TestNew3ForwardWriteNotFencedByRegionOwnership(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)

	// 复现真机 node-c 的持久化状态：曾获授权（own_term>0）但已被权威视图收回。
	d.NoteOwnershipView(5, true, false)
	if !d.ClusterOwned() {
		t.Fatal("grant must record ownership")
	}
	d.NoteOwnershipView(9, false, true)
	if d.ClusterOwned() {
		t.Fatal("authoritative view must revoke local ownership")
	}

	// 节点级判定已拒写（这是缺陷现场的全部写被拦的直接原因）。
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("node-level AllowWrite = %v, want ErrFenced", err)
	}

	// region 1 归属其它节点（本机仅转发）；region 2 归属本机（仍持有）。
	new3SetRegionOwner(d, map[uint64]bool{2: true}, map[uint64]bool{1: true, 2: true})

	// 用例 a：转发型写不被归属 fence 拦截。
	if err := d.AllowWriteRegion(1); err != nil {
		t.Fatalf("forwarded write to region owned by another node = %v, want nil", err)
	}

	// 用例 b：本机持有的 region 被权威视图收回后仍拒写。
	if err := d.AllowWriteRegion(2); !errors.Is(err, ErrFenced) {
		t.Fatalf("write to locally-owned (revoked) region = %v, want ErrFenced", err)
	}

	// 归属不可判定（未指派 / owner 未注册）→ 退回节点级判定，保守拒写。
	if err := d.AllowWriteRegion(3); !errors.Is(err, ErrFenced) {
		t.Fatalf("undecidable region ownership = %v, want node-level ErrFenced fallback", err)
	}

	// 语句级入口只做复制轨判定：节点级归属 fence 不再拦入口（NEW-3 主症状）。
	if err := d.AllowWriteStatement(); err != nil {
		t.Fatalf("statement-level fence = %v, want nil (node ownership fence is region-scoped now)", err)
	}

	// 未装配 region 归属判定（旧装配路径）→ 全部退回节点级判定，行为与
	// v1.20.x 一致（零回归）。
	d.SetLocalRegionOwner(nil)
	if err := d.AllowWriteRegion(1); !errors.Is(err, ErrFenced) {
		t.Fatalf("no region owner predicate: AllowWriteRegion = %v, want ErrFenced", err)
	}
}

// TestNew3OwnedRegionRevokedStillFenced 用例 b 的授权—收回链路：
// region 粒度下沉不得削弱 P9 既有契约——授予可写、收回拒写、更高 term 再授予恢复。
func TestNew3OwnedRegionRevokedStillFenced(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)
	const rid uint64 = 7
	new3SetRegionOwner(d, map[uint64]bool{rid: true}, map[uint64]bool{rid: true})

	// 授予：本机在 term 5 接管该 region。
	d.NoteOwnershipView(5, true, false)
	if err := d.AllowWriteRegion(rid); err != nil {
		t.Fatalf("granted owner: AllowWriteRegion = %v, want nil", err)
	}
	if got := d.MasterEpoch(); got != 0 {
		t.Fatalf("cluster rail must not hijack replication masterEpoch, got %d", got)
	}

	// 收回：权威视图 term 7 显示本机不持有归属 → 该 region 写入立即拒。
	d.NoteOwnershipView(7, false, true)
	if err := d.AllowWriteRegion(rid); !errors.Is(err, ErrFenced) {
		t.Fatalf("revoked owner: AllowWriteRegion = %v, want ErrFenced", err)
	}
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("revoked owner: node-level AllowWrite = %v, want ErrFenced", err)
	}

	// 更高 term 再授予 → region 写入恢复。
	d.NoteOwnershipView(11, true, false)
	if err := d.AllowWriteRegion(rid); err != nil {
		t.Fatalf("re-granted owner: AllowWriteRegion = %v, want nil", err)
	}
	if got := d.ClusterOwnTerm(); got != 11 {
		t.Fatalf("ClusterOwnTerm = %d, want 11", got)
	}
}

// TestNew3RestartPendingRegionGranularity 重启待确认按 region 粒度生效：
// 本机持有归属但尚未被权威视图确认时，写本机 region 拒（ErrOwnershipPending），
// 转发型写不受影响；权威确认后恢复。
func TestNew3RestartPendingRegionGranularity(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)
	const rid uint64 = 3
	new3SetRegionOwner(d, map[uint64]bool{rid: true}, map[uint64]bool{rid: true, 4: true})

	// 重启装载旧归属视图 + 集群存在其它节点 → 待权威确认。
	d.noteRestartOwnershipView(true, 2)
	if err := d.AllowWriteRegion(rid); !errors.Is(err, ErrOwnershipPending) {
		t.Fatalf("pending owner: AllowWriteRegion = %v, want ErrOwnershipPending", err)
	}
	// 转发型写（region 4 归属其它节点）不受本机待确认状态影响。
	if err := d.AllowWriteRegion(4); err != nil {
		t.Fatalf("forwarded write during pending = %v, want nil", err)
	}

	// 权威视图确认本机持有归属 → 恢复可写。
	d.NoteOwnershipView(2, true, true)
	if err := d.AllowWriteRegion(rid); err != nil {
		t.Fatalf("confirmed owner: AllowWriteRegion = %v, want nil", err)
	}

	// 关闭 epoch 开关时一律放行（零回归）。
	d.SetEpochEnabled(false)
	d.NoteOwnershipView(4, false, true)
	if err := d.AllowWriteRegion(rid); err != nil {
		t.Fatalf("epoch disabled: AllowWriteRegion = %v, want nil", err)
	}
}

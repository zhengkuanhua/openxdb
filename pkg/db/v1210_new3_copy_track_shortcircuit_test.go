package db

import (
	"errors"
	"strings"
	"testing"
)

// new3DemoteStaleMaster 复现复制轨降级现场：本节点曾是复制轨 master（masterEpoch>0），
// 收到更高广播 epoch 后被判定陈旧主 → readOnly=true 且 d.epoch>masterEpoch。
func new3DemoteStaleMaster(t *testing.T, d *DB) {
	t.Helper()
	d.SetEpochEnabled(true)
	d.setMasterEpoch(100)
	d.NoteEpoch(200)
	if !d.ReadOnly() {
		t.Fatalf("setup: ReadOnly = false, want true (demoted stale master)")
	}
	if d.epoch <= d.MasterEpoch() {
		t.Fatalf("setup: local epoch %d must exceed master epoch %d", d.epoch, d.MasterEpoch())
	}
}

// TestNew3CopyTrackFenceSkippedWhenClusterOwned 真机回归用例：集群轨持有归属的节点
// 处于复制轨只读降级（readOnly=true，滚动重启后历史降级未复位）时，语句级与
// region 级写入口都必须放行，不得再报 "demoted stale master is read-only"。
func TestNew3CopyTrackFenceSkippedWhenClusterOwned(t *testing.T) {
	d := openTestDB(t)
	new3DemoteStaleMaster(t, d)

	const rid uint64 = 1
	new3SetRegionOwner(d, map[uint64]bool{rid: true}, map[uint64]bool{rid: true})

	d.NoteOwnershipView(300, true, false) // 权威视图确认本机持有集群轨归属
	if !d.ClusterOwned() {
		t.Fatal("ownership view must confirm local cluster-owned state")
	}
	if !d.ReadOnly() {
		t.Fatal("clusterOwned shortcircuit must not reset replication-rail readOnly flag")
	}

	if err := d.AllowWriteStatement(); err != nil {
		t.Fatalf("statement-level fence with clusterOwned = %v, want nil", err)
	}
	if err := d.AllowWriteRegion(rid); err != nil {
		t.Fatalf("region-level fence with clusterOwned = %v, want nil", err)
	}
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("node-level AllowWrite with clusterOwned = %v, want nil", err)
	}
	if got := d.MasterEpoch(); got != 100 {
		t.Fatalf("MasterEpoch = %d, want 100 (cluster rail must not write masterEpoch)", got)
	}
}

// TestNew3CopyTrackFenceKeptWhenNotClusterOwned clusterOwned=false 时保留原有拒写语义：
// 只读降级仍以 ErrFenced 拦截语句级 / region 级写入口（fencing 未被削弱）。
func TestNew3CopyTrackFenceKeptWhenNotClusterOwned(t *testing.T) {
	d := openTestDB(t)
	new3DemoteStaleMaster(t, d)

	if d.ClusterOwned() {
		t.Fatal("fresh node must not be cluster-owned")
	}
	if got := d.ClusterOwnTerm(); got != 0 {
		t.Fatalf("ClusterOwnTerm = %d, want 0", got)
	}

	err := d.AllowWriteStatement()
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("statement-level fence = %v, want ErrFenced", err)
	}
	if !strings.Contains(err.Error(), "demoted stale master is read-only") {
		t.Fatalf("statement-level fence message = %q, want read-only demotion reason", err.Error())
	}

	const rid uint64 = 2
	new3SetRegionOwner(d, map[uint64]bool{rid: true}, map[uint64]bool{rid: true})
	if err := d.AllowWriteRegion(rid); !errors.Is(err, ErrFenced) {
		t.Fatalf("region-level fence = %v, want ErrFenced", err)
	}
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("node-level AllowWrite = %v, want ErrFenced", err)
	}
}

package db

import (
	"errors"
	"strings"
	"testing"
)

// v2.0.2：纯转发节点（权威视图确认本机不持有任何 region）不再被复制轨
// "只读降级 / 陈旧主"判定永久 fence。
//
// 真机症状（三机升级 v2.0.1 后复验）：node-c 不拥有任何 region，直连写入报
//
//	write fenced by newer cluster epoch: demoted stale master is read-only
//
// 根因：语句级入口 AllowWriteStatement 只做复制轨判定，而 copyTrackWriteFenceLocked
// 仅在 clusterOwned=true 时短路；node-c 的 clusterOwned=false（历史误授后被权威
// 视图收回）且 readOnly=true（历史降级未复位）→ 转发型写被节点级判定拦死，
// 与 v1.21.0 NEW-3 的 region 粒度归属判定口径冲突。

// v2020NonOwnerNode 复现 node-c 现场：曾获归属授权、被权威视图收回、复制轨处于
// 只读降级。
func v2020NonOwnerNode(t *testing.T, d *DB) {
	t.Helper()
	d.SetEpochEnabled(true)
	d.NoteOwnershipView(5, true, false) // 历史误授（非权威增量视图）
	d.NoteOwnershipView(9, false, true) // 权威全量视图：本机不持有任何 region
	new3DemoteStaleMaster(t, d)         // 复制轨降级：readOnly=true 且 epoch>masterEpoch
	if d.ClusterOwned() {
		t.Fatal("setup: node-c replica must not be cluster-owned")
	}
	if !d.ReadOnly() {
		t.Fatal("setup: node-c must keep replication-rail readOnly flag")
	}
}

// TestV2020NonOwnerForwardWriteNotFenced 主症状用例：纯转发节点的语句级与
// region 级（目标 region 归属其它节点）写入口必须放行。
func TestV2020NonOwnerForwardWriteNotFenced(t *testing.T) {
	d := openTestDB(t)
	v2020NonOwnerNode(t, d)

	const local, remote uint64 = 1, 2
	new3SetRegionOwner(d,
		map[uint64]bool{local: true, remote: false},
		map[uint64]bool{local: true, remote: true})

	if err := d.AllowWriteStatement(); err != nil {
		t.Fatalf("statement-level fence on forward node = %v, want nil", err)
	}
	if err := d.AllowWriteRegion(remote); err != nil {
		t.Fatalf("region-level fence on forwarded region %d = %v, want nil", remote, err)
	}
}

// TestV2020NonOwnerStillFencedForLocalRegion fencing 未被削弱：归属已被权威视图
// 收回的陈旧主，写"本机仍被认为持有的 region"照旧 ErrFenced。
func TestV2020NonOwnerStillFencedForLocalRegion(t *testing.T) {
	d := openTestDB(t)
	v2020NonOwnerNode(t, d)

	const stale uint64 = 1
	new3SetRegionOwner(d,
		map[uint64]bool{stale: true},
		map[uint64]bool{stale: true})

	err := d.AllowWriteRegion(stale)
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("region-level fence for revoked local region = %v, want ErrFenced", err)
	}
	if !strings.Contains(err.Error(), "cluster ownership revoked") {
		t.Fatalf("region-level fence reason = %q, want cluster ownership revoked", err.Error())
	}

	// 节点级入口保持 P9 契约（own_term>0 且归属被收回 → 拒写）。
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("node-level AllowWrite on revoked replica = %v, want ErrFenced", err)
	}
}

// TestV2020NonOwnerZeroRegressionWithoutAuthoritativeView 零回归①：从未收到权威视图
// 的节点不会被豁免，只读降级仍拦写（单机 / 普通复制轨节点行为不变）。
func TestV2020NonOwnerZeroRegressionWithoutAuthoritativeView(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)
	new3DemoteStaleMaster(t, d)

	err := d.AllowWriteStatement()
	if !errors.Is(err, ErrFenced) || !strings.Contains(err.Error(), "demoted stale master is read-only") {
		t.Fatalf("statement-level fence without ownership view = %v, want read-only demotion", err)
	}
}

// TestV2020NonOwnerZeroRegressionCopyTrackStaleMaster 零回归②（脑裂演练口径）：
// 复制轨陈旧主虽收到 selfOwns=false 的权威视图，但从未参与集群轨归属授权
// （clusterOwnTerm==0），不得被当作纯转发节点豁免，仍须 ErrFenced，否则其本地
// 写入会与多数派新主分叉（brain_split_drill_test.go 覆盖的真实回归点）。
func TestV2020NonOwnerZeroRegressionCopyTrackStaleMaster(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)
	d.NoteOwnershipView(3, false, true) // 权威视图：本机无 region，但从未被授予归属
	new3DemoteStaleMaster(t, d)

	if d.ClusterOwned() {
		t.Fatal("setup: node must not be cluster-owned")
	}
	err := d.AllowWriteStatement()
	if !errors.Is(err, ErrFenced) || !strings.Contains(err.Error(), "demoted stale master is read-only") {
		t.Fatalf("statement-level fence for copy-track stale master = %v, want read-only demotion", err)
	}
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("node-level AllowWrite for copy-track stale master = %v, want ErrFenced", err)
	}
}

// TestV2020NonOwnerClearedWhenRegranted 重新获得 region 归属后，纯转发标记必须清除，
// 复制轨短路回落到 clusterOwned 口径。
func TestV2020NonOwnerClearedWhenRegranted(t *testing.T) {
	d := openTestDB(t)
	v2020NonOwnerNode(t, d)

	d.NoteOwnershipView(11, true, true) // 权威视图重新授予归属
	if !d.ClusterOwned() {
		t.Fatal("ownership must be granted again")
	}
	const rid uint64 = 3
	new3SetRegionOwner(d, map[uint64]bool{rid: true}, map[uint64]bool{rid: true})
	if err := d.AllowWriteStatement(); err != nil {
		t.Fatalf("statement-level fence after regrant = %v, want nil", err)
	}
	if err := d.AllowWriteRegion(rid); err != nil {
		t.Fatalf("region-level fence after regrant = %v, want nil", err)
	}
}

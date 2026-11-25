package db

// v2.0.3 缺陷修复专项测试（db 层）：持久化 revoked ownership 的纯转发节点
// 在"本地无该 region 路由"时的写 fence 语义。
//
// 真机症状（node-c / 203）：cluster_term.meta 为旧纪元 owned=0，本地路由视图
// 又缺新表 region 条目，直连写入报 "cluster ownership revoked at term ..."。
// 归因错在两点：
//   ① 启动时未据落盘状态识别纯转发节点（只等权威视图帧），导致 v2.0.2 的
//      豁免条件 clusterNonOwner 恒 false；
//   ② region 归属不可判定时直接落回节点级判定，把"视图缺失"报成"归属被收回"。

import (
	"errors"
	"testing"
)

// v203PersistRevokedMeta 落盘 node-c 式状态（owned=0 且 own>0）后清空内存态，
// 再按启动路径重新装载。
func v203PersistRevokedMeta(t *testing.T, d *DB, term, own uint64) {
	t.Helper()
	d.epochMu.Lock()
	d.clusterTerm = term
	d.clusterOwnTerm = own
	d.clusterOwned = false
	d.clusterPend = false
	d.clusterNonOwner = false
	d.persistClusterTermLocked()
	d.epochMu.Unlock()

	d.epochMu.Lock()
	d.clusterTerm = 0
	d.clusterOwnTerm = 0
	d.clusterOwned = false
	d.clusterPend = false
	d.clusterNonOwner = false
	d.epochMu.Unlock()

	d.loadClusterTermMeta()
}

// TestV203ForwardOnlyRecognizedAtStartup 落盘 owned=0 且 own>0 的节点启动即
// 被识别为纯转发节点（不必等权威视图帧）。
func TestV203ForwardOnlyRecognizedAtStartup(t *testing.T) {
	d := openTestDB(t)
	v203PersistRevokedMeta(t, d, 100, 50)

	if d.ClusterOwned() {
		t.Fatal("owned must stay false after reload")
	}
	if d.ClusterOwnTerm() == 0 {
		t.Fatal("own term must be restored from disk")
	}
	if !d.ForwardOnlyNode() {
		t.Fatal("node with persisted owned=0 && own>0 must be recognized as forward-only at startup")
	}
}

// TestV203StartupOwnedNodeIsNotForwardOnly 零回归：落盘 owned=1（重启前仍持有
// 归属）的节点不得被识别为纯转发节点，仍按"待确认"处理。
func TestV203StartupOwnedNodeIsNotForwardOnly(t *testing.T) {
	d := openTestDB(t)
	d.epochMu.Lock()
	d.clusterTerm = 100
	d.clusterOwnTerm = 50
	d.clusterOwned = true
	d.clusterPend = false
	d.persistClusterTermLocked()
	d.epochMu.Unlock()

	d.epochMu.Lock()
	d.clusterTerm = 0
	d.clusterOwnTerm = 0
	d.clusterOwned = false
	d.clusterNonOwner = false
	d.epochMu.Unlock()
	d.loadClusterTermMeta()

	if d.ForwardOnlyNode() {
		t.Fatal("node with persisted owned=1 must not be treated as forward-only")
	}
	if !d.ClusterOwned() {
		t.Fatal("owned must be restored from disk")
	}
}

// TestV203ForwardOnlyUnknownRegionReportsViewMissing 纯转发节点写"本机视图
// 无条目"的显式 region → ErrRegionViewMissing（不包装 ErrFenced）。
func TestV203ForwardOnlyUnknownRegionReportsViewMissing(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)
	v203PersistRevokedMeta(t, d, 100, 50)
	// 归属判定器：目标 region 归属不可判定（known=false）。
	new3SetRegionOwner(d, map[uint64]bool{}, map[uint64]bool{})
	if !d.ForwardOnlyNode() {
		t.Fatal("setup: node must be forward-only")
	}

	const unknown uint64 = 5816
	err := d.AllowWriteRegion(unknown)
	if err == nil {
		t.Fatalf("forward-only node must reject write to unknown region, got nil")
	}
	if !errors.Is(err, ErrRegionViewMissing) {
		t.Fatalf("error must be ErrRegionViewMissing, got: %v", err)
	}
	if errors.Is(err, ErrFenced) {
		t.Fatalf("error must not be attributed to ownership revocation (ErrFenced): %v", err)
	}
}

// TestV203NonForwardOnlyUnknownRegionKeepsRevoked 零回归：非纯转发节点在归属
// 不可判定时仍落回节点级判定（保持既有 ErrFenced revoked 语义）。
func TestV203NonForwardOnlyUnknownRegionKeepsRevoked(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)
	d.NoteOwnershipView(5, true, false)
	d.NoteOwnershipView(9, false, true) // 权威收回（运行期路径置位 clusterNonOwner）
	d.epochMu.Lock()
	d.clusterNonOwner = false // 模拟"非纯转发节点"（如复制轨脑裂陈旧主）
	d.epochMu.Unlock()
	new3SetRegionOwner(d, map[uint64]bool{}, map[uint64]bool{})
	if d.ForwardOnlyNode() {
		t.Fatal("setup: forward-only marker must be cleared")
	}
	if err := d.AllowWriteRegion(5816); !errors.Is(err, ErrFenced) {
		t.Fatalf("non-forward node must keep node-level fence, got: %v", err)
	}
}

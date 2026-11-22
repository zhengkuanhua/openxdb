package db

// D4.5 Phase9 补测（T3 真机分区复验回归）：集群轨归属与复制轨陈旧主判定的关系。
//
// 缺陷现场（三台真机 192.168.100.201/202/203，iptables 阻断 7791 后恢复）：
// B/C 作为权威视图确认的 region owner 仍被 AllowWrite 拒绝，报
// "local epoch 11674047172550066181 > master epoch 11674044195214983168"。
// 成因：集群轨授权只推进 clusterOwnTerm / d.epoch，不写复制轨 masterEpoch
// （既有契约）；而 session 持续取 TSO 单调号推进 d.epoch，使
// d.epoch > d.masterEpoch 恒成立，合法 owner 被永久误判为陈旧主。
//
// 修复：AllowWrite 在 clusterOwned 成立时跳过复制轨陈旧主判定（归属合法性由
// 权威路由视图仲裁），并在其前对 NoteEpoch 判定的只读降级统一拒写。

import (
	"errors"
	"testing"
)

// TestP9OwnerEpochSyncKeepsAllowWrite 归属授予后 epoch 随 TSO 推进仍可写：
// 复刻 B/C 真机现场——授权后连续取号推进 epoch，写路径不得再被误判为陈旧主。
func TestP9OwnerEpochSyncKeepsAllowWrite(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)

	const grant uint64 = 11_674_044_195_214_983_168 // 归属纪元（= 真机 masterEpoch 量级）
	d.NoteOwnershipView(grant, true, true)

	// 授权后取号若干次，epoch 单调前进（真机上每次事务都会推进）。
	for i := 0; i < 5; i++ {
		d.acquireEpoch()
	}
	if d.Epoch() <= grant {
		t.Fatalf("epoch %d not advanced past grant %d; test cannot reproduce the defect", d.Epoch(), grant)
	}
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("owner write must be allowed after ownership grant, got %v", err)
	}
	if d.ConflictCount() != 0 {
		t.Fatalf("conflict count = %d, want 0 (no false fencing)", d.ConflictCount())
	}
	// 既有契约：集群轨授权不得劫持复制轨 masterEpoch。
	if d.MasterEpoch() != 0 {
		t.Fatalf("cluster rail hijacked masterEpoch = %d, want 0", d.MasterEpoch())
	}
}

// TestP9RevokedOwnerEpochSyncStillFenced 归属被权威视图收回后即使 epoch 已推进
// 也必须拒写：验证修复未削弱陈旧主 fencing。
func TestP9RevokedOwnerEpochSyncStillFenced(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)

	d.NoteOwnershipView(11_674_044_195_214_983_168, true, true)
	d.acquireEpoch()
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("owner should be writable before revoke, got %v", err)
	}

	// 权威全量视图显示本机不再持有任何 region -> 归属收回。
	d.NoteOwnershipView(11_674_044_195_214_983_168, false, true)
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("revoked owner must be fenced, got %v", err)
	}
	if d.ConflictCount() == 0 {
		t.Fatalf("revoked owner write attempt should mark conflict")
	}
}

// TestP9CopyTrackStaleMasterStillFenced 复制轨（无集群轨归属）零回归：
// 未参与集群轨归属的复制轨 master 仍由 epoch > masterEpoch 判定拒写。
func TestP9CopyTrackStaleMasterStillFenced(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)

	d.setMasterEpoch(5)
	for i := 0; i < 3; i++ {
		d.acquireEpoch()
	}
	if d.MasterEpoch() != 5 {
		t.Fatalf("copy-track masterEpoch = %d, want 5 (must not be raised)", d.MasterEpoch())
	}
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale copy-track master must stay fenced, got %v", err)
	}
}

// TestP9OwnerSurvivesReadOnlyDemote 集群轨归属成立时不受复制轨只读降级影响：
// 复刻 A 节点现场——曾为复制轨 master（masterEpoch>0），收到更高 epoch 广播被
// 降级为 readOnly，但它同时是某 region 的合法 owner，该 region 不得整段不可写。
func TestP9OwnerSurvivesReadOnlyDemote(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)

	d.setMasterEpoch(5)                              // 复制轨 master 授权
	d.NoteOwnershipView(11_674_082_223_627_173_890, true, true) // 集群轨 region 归属
	d.NoteEpoch(100)                                 // 复制轨更高广播 -> demote
	if !d.ReadOnly() {
		t.Fatalf("test precondition failed: node not demoted to read-only")
	}
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("region owner must stay writable despite copy-track read-only demotion, got %v", err)
	}
}

// TestP9NonOwnerReadOnlyStillFenced 无集群轨归属时只读降级仍拒写（零回归）。
func TestP9NonOwnerReadOnlyStillFenced(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)

	d.setMasterEpoch(5)
	d.NoteEpoch(100)
	if !d.ReadOnly() {
		t.Fatalf("test precondition failed: node not demoted to read-only")
	}
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("non-owner demoted node must stay fenced, got %v", err)
	}
}

// P9 HA（D4.5 Phase 9 / 候选 B）：集群轨归属纪元（cluster term）与写 fencing 测试。
//
// 覆盖 NEW-1（重启双主）与 T3（真分区）同源缺陷的收口断言：
//   - 集群轨获得可推进的授权纪元：接管成功 → clusterOwnTerm 推到该 term，
//     写路径 AllowWrite 门槛真正生效（不再恒放行）；
//   - 权威全量视图显示本机已不持有归属 → 归属收回，陈旧主写入被拒（ErrFenced）；
//   - 重启装载旧归属视图 → 先按"待确认"拒写（ErrOwnershipPending），
//     待权威视图确认后解除；已确认收回的状态跨重启保持（重启后仍拒写）；
//   - 增量广播不得据此判"收回"（只有权威全量视图可以），避免误 fence
//     仍持有其它 region 的节点；
//   - 零回归：从未获得集群轨归属的节点（单机/普通集群节点）masterEpoch 与
//     写路径行为不变（本文件不接管复制轨 masterEpoch）。
package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// openP9DB 打开 dir 下的数据目录（首次调用负责 Init，后续调用为重启重放）。
func openP9DB(t *testing.T, dir string) *DB {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, ConfigFile)); err != nil {
		if ierr := Init(dir); ierr != nil {
			t.Fatalf("Init: %v", ierr)
		}
	}
	d, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return d
}

// TestP9ClusterTermGrantAndRevoke 集群轨归属纪元推进 / 收回与写门槛：
// 这是 T3 根因（masterEpoch 恒 0 → AllowWrite 永不拦截）的直接回归测试。
func TestP9ClusterTermGrantAndRevoke(t *testing.T) {
	d := openTestDB(t)

	// 零回归：从未获得集群轨归属 → 放行。
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("no cluster ownership: AllowWrite = %v, want nil", err)
	}
	if d.ClusterOwnTerm() != 0 || d.ClusterOwned() {
		t.Fatalf("fresh db must have no cluster ownership, got term=%d", d.ClusterOwnTerm())
	}

	// 授予：本节点在 term 5 接管 region → 集群轨纪元真正推进。
	d.NoteOwnershipView(5, true, false)
	if got := d.ClusterTerm(); got != 5 {
		t.Fatalf("ClusterTerm = %d, want 5", got)
	}
	if got := d.ClusterOwnTerm(); got != 5 {
		t.Fatalf("ClusterOwnTerm = %d, want 5", got)
	}
	if !d.ClusterOwned() {
		t.Fatalf("ownership must be recorded as owned after grant")
	}
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("granted owner: AllowWrite = %v, want nil", err)
	}
	// 不接管复制轨 masterEpoch（复制轨语义原样保留，零回归）。
	if got := d.MasterEpoch(); got != 0 {
		t.Fatalf("cluster rail must not hijack replication masterEpoch, got %d", got)
	}

	// 回退的陈旧消息不降低已知纪元，也不改变归属。
	d.NoteOwnershipView(3, false, false)
	if got := d.ClusterTerm(); got != 5 {
		t.Fatalf("stale notify lowered ClusterTerm to %d", got)
	}
	if !d.ClusterOwned() {
		t.Fatalf("incremental notify must not revoke ownership")
	}
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("after stale incremental notify: AllowWrite = %v, want nil", err)
	}

	// 收回：权威全量视图显示本机不再持有任何 region（term 7 已被取代）。
	d.NoteOwnershipView(7, false, true)
	if d.ClusterOwned() {
		t.Fatalf("authoritative view without local region must revoke ownership")
	}
	if got := d.ClusterOwnTerm(); got != 5 {
		t.Fatalf("revoke must keep own term (授权纪元不回退), got %d", got)
	}
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("revoked stale owner: AllowWrite = %v, want ErrFenced", err)
	}

	// 再次被授予（更高 term 接管）→ 写路径恢复。
	d.NoteOwnershipView(9, true, false)
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("re-granted owner: AllowWrite = %v, want nil", err)
	}
	if got := d.ClusterOwnTerm(); got != 9 {
		t.Fatalf("ClusterOwnTerm after re-grant = %d, want 9", got)
	}
}

// TestP9RestartPendingUntilAuthoritative 重启节点按权威 term 收敛归属：
// 装载旧归属视图后先拒写（待确认），权威视图到达即收敛——NEW-1 的核心断言。
func TestP9RestartPendingUntilAuthoritative(t *testing.T) {
	dir := t.TempDir()
	d := openP9DB(t, dir)
	// 重启前：本节点在 term 4 持有 region（如 A 是 region 2 的 owner）。
	d.NoteOwnershipView(4, true, false)
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("before restart: AllowWrite = %v, want nil", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重启：装载持久化的归属状态 → 归属待权威视图确认，不得按下发前的旧视
	// 图抢先写（NEW-1：A 重启后写 70 覆盖多数派已确认值）。
	a := openP9DB(t, dir)
	defer func() { _ = a.Close() }()
	if !a.OwnershipPending() {
		t.Fatalf("restart with persisted ownership must be pending until confirmed")
	}
	if err := a.AllowWrite(); !errors.Is(err, ErrOwnershipPending) {
		t.Fatalf("pending ownership: AllowWrite = %v, want ErrOwnershipPending", err)
	}
	if got := a.ClusterOwnTerm(); got != 4 {
		t.Fatalf("persisted own term = %d, want 4", got)
	}

	// 权威视图到达，确认本机仍持有 → 解除待确认，恢复可写。
	a.NoteOwnershipView(4, true, true)
	if a.OwnershipPending() {
		t.Fatalf("authoritative view must clear pending state")
	}
	if err := a.AllowWrite(); err != nil {
		t.Fatalf("confirmed owner: AllowWrite = %v, want nil", err)
	}

	// 权威视图显示归属已转给别的节点（term 6）→ 收回，写入被 fence。
	a.NoteOwnershipView(6, false, true)
	if err := a.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale owner after authoritative convergence: AllowWrite = %v, want ErrFenced", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 再次重启：已收回的归属必须跨重启保持（陈旧主不能因重启而复活）。
	b := openP9DB(t, dir)
	defer func() { _ = b.Close() }()
	if b.OwnershipPending() {
		t.Fatalf("revoked state must not become pending after restart")
	}
	if b.ClusterOwned() {
		t.Fatalf("revoked state must persist across restart")
	}
	if err := b.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("restarted stale owner: AllowWrite = %v, want ErrFenced", err)
	}
	if got := b.ClusterTerm(); got != 6 {
		t.Fatalf("persisted known term = %d, want 6", got)
	}
}

// TestP9ConfirmOwnershipLocalNoPeer 单节点/无权威源重启：本地视图即最终视图，
// ConfirmOwnershipLocal 解除待确认，避免节点永久只读（可用性兜底）。
func TestP9ConfirmOwnershipLocalNoPeer(t *testing.T) {
	dir := t.TempDir()
	d := openP9DB(t, dir)
	d.NoteOwnershipView(8, true, false)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	a := openP9DB(t, dir)
	defer func() { _ = a.Close() }()
	if !a.OwnershipPending() {
		t.Fatalf("restart must be pending first")
	}
	if err := a.AllowWrite(); !errors.Is(err, ErrOwnershipPending) {
		t.Fatalf("AllowWrite = %v, want ErrOwnershipPending", err)
	}
	a.ConfirmOwnershipLocal()
	if a.OwnershipPending() {
		t.Fatalf("ConfirmOwnershipLocal must clear pending state")
	}
	if err := a.AllowWrite(); err != nil {
		t.Fatalf("after local confirm: AllowWrite = %v, want nil", err)
	}
	// 确认不改变授权纪元与归属（只解除待确认）。
	if got := a.ClusterOwnTerm(); got != 8 {
		t.Fatalf("ClusterOwnTerm = %d, want 8", got)
	}
}

// TestP9RestartStaleOwnerPendingWithoutAnyTerm 直接复现 Phase7 NEW-1 实测现场：
// 节点从未经过 failover 授权（clusterOwnTerm==0），但本地路由视图仍写着
// owner=本机/local=true；重启且集群里还有其它节点时，归属必须先被权威视图
// 确认，确认前拒绝写入（修复前此处写成功 → 陈旧主覆盖新 owner 已确认数据）。
func TestP9RestartStaleOwnerPendingWithoutAnyTerm(t *testing.T) {
	d := openTestDB(t)

	// 重启装载：本地视图仍持有 region，存在 2 个对端。
	d.noteRestartOwnershipView(true, 2)
	if !d.OwnershipPending() {
		t.Fatalf("restart with local ownership view + peers must be pending")
	}
	err := d.AllowWrite()
	if !errors.Is(err, ErrOwnershipPending) {
		t.Fatalf("pending owner AllowWrite = %v, want ErrOwnershipPending", err)
	}

	// 权威视图确认本机仍是 owner（term 4）：解除待确认，恢复可写。
	d.NoteOwnershipView(4, true, true)
	if d.OwnershipPending() {
		t.Fatalf("authoritative confirm must clear pending")
	}
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("confirmed owner AllowWrite = %v, want nil", err)
	}
	if got := d.ClusterOwnTerm(); got != 4 {
		t.Fatalf("ClusterOwnTerm = %d, want 4", got)
	}

	// 同一节点再次重启：本地视图仍持有归属 → 继续待确认（NEW-1 每次都须确认）。
	d.noteRestartOwnershipView(true, 2)
	if !d.OwnershipPending() {
		t.Fatalf("second restart must re-enter pending")
	}
	// 权威视图：region 已归他人（term 9）→ 归属收回，陈旧主写入被 fence。
	d.NoteOwnershipView(9, false, true)
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale owner AllowWrite = %v, want ErrFenced", err)
	}
	if d.ClusterOwned() {
		t.Fatalf("ownership must be revoked by authoritative view")
	}
}

// TestP9RestartSingleNodeNoPending 单节点集群（无对端）重启不受归属确认约束：
// 本地视图即最终视图，重启后立即可写（零回归）。
func TestP9RestartSingleNodeNoPending(t *testing.T) {
	d := openTestDB(t)
	d.noteRestartOwnershipView(true, 0)
	if d.OwnershipPending() {
		t.Fatalf("single-node restart must not be pending (peerCount=0)")
	}
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("single-node restart AllowWrite = %v, want nil", err)
	}
}

// TestP9RestartPendingPersisted 待确认状态落盘跨重启保持：进程在"待确认"
// 期间再次重启，仍不得凭本地视图写入。
func TestP9RestartPendingPersisted(t *testing.T) {
	dir := t.TempDir()
	d := openP9DB(t, dir)
	d.noteRestartOwnershipView(true, 2)
	d.Close()

	// 重启重放：磁盘上 pending=1 → 装载后仍待确认。
	d2 := openP9DB(t, dir)
	defer d2.Close()
	if !d2.OwnershipPending() {
		t.Fatalf("pending state must survive restart")
	}
	if err := d2.AllowWrite(); !errors.Is(err, ErrOwnershipPending) {
		t.Fatalf("replayed pending AllowWrite = %v, want ErrOwnershipPending", err)
	}
}

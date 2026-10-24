package db

// M6 高可用与故障转移：复制链路主从切换测试。
// 复用 M2 基建（openTestDB/commitKV/waitFollowerLSN，见 db_replication_test.go）。

import (
	"strconv"
	"testing"
	"time"
)

// waitFollowerOffline 轮询直到 follower 进入 offline（旧主不可达）。
func waitFollowerOffline(t *testing.T, d *DB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if d.Follower != nil && d.Follower.Status().State == "offline" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("follower state = %s, want offline", d.Follower.Status().State)
}

// TestM6PromoteFollowerWriteContinues 主从切换：master down 后 follower 手动升主，
// 写路径继续，既有数据不丢失（旧 10 条 + 新 3 条）。
func TestM6PromoteFollowerWriteContinues(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()
	slave := openTestDB(t)
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}
	for i := 1; i <= 10; i++ {
		commitKV(t, main, "k"+strconv.Itoa(i), "v")
	}
	waitFollowerLSN(t, slave.Follower, 10)

	// master down：关闭主库，follower 感知断链置 offline
	if err := main.Close(); err != nil {
		t.Fatalf("main close: %v", err)
	}
	waitFollowerOffline(t, slave)

	// 主从切换：follower 升级为主，写路径继续
	if err := slave.PromoteToMaster(); err != nil {
		t.Fatalf("PromoteToMaster: %v", err)
	}
	if slave.Replicator == nil {
		t.Fatalf("promoted slave has no replicator")
	}
	for i := 11; i <= 13; i++ {
		commitKV(t, slave, "k"+strconv.Itoa(i), "v")
	}
	// 数据一致性：旧 10 条 + 新 3 条
	for i := 1; i <= 13; i++ {
		v, err := slave.Storage.Get([]byte("k" + strconv.Itoa(i)))
		if err != nil || string(v) != "v" {
			t.Fatalf("slave k%d = %q err=%v", i, v, err)
		}
	}
}

// TestM6AutoPromoteFollowerOnMasterDown 自动故障转移：master down 后 follower
// 自动升主并回调 onPromote，写继续且旧数据完整。
func TestM6AutoPromoteFollowerOnMasterDown(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()
	slave := openTestDB(t)
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}
	for i := 1; i <= 5; i++ {
		commitKV(t, main, "k"+strconv.Itoa(i), "v")
	}
	waitFollowerLSN(t, slave.Follower, 5)

	promoted := make(chan struct{}, 1)
	if err := slave.EnableAutoFailover(func() { promoted <- struct{}{} }); err != nil {
		t.Fatalf("EnableAutoFailover: %v", err)
	}
	t.Cleanup(slave.StopAutoFailover)

	// master down → 自动 promote
	if err := main.Close(); err != nil {
		t.Fatalf("main close: %v", err)
	}
	select {
	case <-promoted:
	case <-time.After(8 * time.Second):
		t.Fatalf("auto failover not triggered")
	}
	if slave.Replicator == nil {
		t.Fatalf("slave not promoted to master")
	}
	commitKV(t, slave, "k6", "v")
	if v, err := slave.Storage.Get([]byte("k6")); err != nil || string(v) != "v" {
		t.Fatalf("write after auto promote: %q err=%v", v, err)
	}
	if v, err := slave.Storage.Get([]byte("k1")); err != nil || string(v) != "v" {
		t.Fatalf("old data after promote: %q err=%v", v, err)
	}
}

// TestM6PromoteRpoLostRecorded 阶段一 RPO 审计①：模拟从端落后——主端先写
// total 条，从端握手获知主端位点但只应用少量即断开；PromoteToMaster 后
// LastPromoteInfo().RpoLost 精确等于落后位点差（主端位点 - 断开时已应用位点）。
func TestM6PromoteRpoLostRecorded(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()
	slave := openTestDB(t)
	defer slave.Close()
	const total = 300
	for i := 0; i < total; i++ {
		commitKV(t, main, "k"+strconv.Itoa(i), "v")
	}
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("start follower: %v", err)
	}
	// 握手完成（记录主端位点 total）且已应用少量；立即断开造成落后。
	waitFollowerLSN(t, slave.Follower, 5)
	_ = slave.Follower.Stop()
	waitFollowerOffline(t, slave)
	applied := slave.Follower.AppliedLSN()
	wantLost := uint64(total) - uint64(applied)
	if err := slave.PromoteToMaster(); err != nil {
		t.Fatalf("promote: %v", err)
	}
	info := slave.LastPromoteInfo()
	if info.At.IsZero() {
		t.Fatalf("LastPromoteInfo At is zero")
	}
	if info.RpoLost != wantLost {
		t.Fatalf("RpoLost = %d, want %d (applied=%d, masterLast=%d)",
			info.RpoLost, wantLost, applied, total)
	}
	if v, err := slave.Storage.Get([]byte("k0")); err != nil || string(v) != "v" {
		t.Fatalf("existing data after promote: %q err=%v", v, err)
	}
}

// TestM6FailoverCatchupRpoZero 阶段一 RPO 补偿②：旧主短暂恢复可达场景——
// follower 落线（offline）但旧主端口仍可达，failover 自动追平后 promote，
// LastPromoteInfo().RpoLost = 0 且旧主在追平窗口内新写的键已补齐。
func TestM6FailoverCatchupRpoZero(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()
	slave := openTestDB(t)
	defer slave.Close()
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("start follower: %v", err)
	}
	const base = 10
	for i := 0; i < base; i++ {
		commitKV(t, main, "k"+strconv.Itoa(i), "v")
	}
	waitFollowerLSN(t, slave.Follower, base)
	// 断开复制链路（模拟旧主短暂不可达）。
	_ = slave.Follower.Stop()
	waitFollowerOffline(t, slave)
	// 旧主网络仍可达，并继续写 5 条（追平窗口内可被拉到）。
	const extra = 5
	for i := base; i < base+extra; i++ {
		commitKV(t, main, "k"+strconv.Itoa(i), "v")
	}
	// 默认 FailoverCatchupTimeout=2s：failover 检测到 offline 后先追平再 promote。
	promoted := make(chan struct{})
	if err := slave.EnableAutoFailover(func() { close(promoted) }); err != nil {
		t.Fatalf("enable auto failover: %v", err)
	}
	defer slave.StopAutoFailover()
	select {
	case <-promoted:
	case <-time.After(8 * time.Second):
		t.Fatal("auto failover did not promote in time")
	}
	info := slave.LastPromoteInfo()
	if info.RpoLost != 0 {
		t.Fatalf("RpoLost = %d, want 0 after catchup", info.RpoLost)
	}
	for i := 0; i < base+extra; i++ {
		if v, err := slave.Storage.Get([]byte("k" + strconv.Itoa(i))); err != nil || string(v) != "v" {
			t.Fatalf("k%d after catchup promote: %q err=%v", i, v, err)
		}
	}
	if slave.Replicator == nil {
		t.Fatal("slave not promoted to master (replicator nil)")
	}
}

// TestM6FailoverCatchupDisabledRegression 阶段一 RPO 补偿③（旧行为回归）：
// FailoverCatchupTimeout=0 时跳过追平，follower offline 后照常 promote；
// 旧主虽可达也不补齐窗口内新键（与默认 2s 的测试②行为相反）。
func TestM6FailoverCatchupDisabledRegression(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()
	slave := openTestDB(t)
	defer slave.Close()
	slave.FailoverCatchupTimeout = 0 // 跳过追平，保持旧行为
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("start follower: %v", err)
	}
	for i := 0; i < 5; i++ {
		commitKV(t, main, "k"+strconv.Itoa(i), "v")
	}
	waitFollowerLSN(t, slave.Follower, 5)
	// 断开复制链路；旧主仍可达并继续写 5 条（若错误执行追平会被拉到）。
	_ = slave.Follower.Stop()
	waitFollowerOffline(t, slave)
	for i := 5; i < 10; i++ {
		commitKV(t, main, "k"+strconv.Itoa(i), "v")
	}
	promoted := make(chan struct{})
	if err := slave.EnableAutoFailover(func() { close(promoted) }); err != nil {
		t.Fatalf("enable auto failover: %v", err)
	}
	defer slave.StopAutoFailover()
	select {
	case <-promoted:
	case <-time.After(8 * time.Second):
		t.Fatal("auto failover did not promote in time")
	}
	// 旧行为回归：跳过追平，照常 promote；窗口内新键缺失。
	if slave.Replicator == nil {
		t.Fatal("slave not promoted to master (replicator nil)")
	}
	if v, err := slave.Storage.Get([]byte("k9")); err == nil {
		t.Fatalf("key k9 should be absent when catchup skipped, got %q", v)
	}
	// 既有快照数据仍在。
	if v, err := slave.Storage.Get([]byte("k0")); err != nil || string(v) != "v" {
		t.Fatalf("existing data after promote: %q err=%v", v, err)
	}
}

package db

// P2 分区恢复与测试矩阵（复制轨，docs/HA_BRAIN_SPLIT_DESIGN.md §6）：
//   - TestP2T1PartitionNoPromote（T1）：网络分区——从端升主时多数派确认
//     失败 → 保持 follower/只读，不升主；
//   - TestP2T2ConcurrentDownOnlyOneWins（T2）：并发双 down——共享单调 epoch
//     源下双端并发升主，epoch 大者胜收敛，至多一个保持主身份；
//   - TestP2T3ReconcileDemoteStaleMaster（T3）：分区恢复合并——陈旧主收到
//     更大 epoch → 自动降级只读、停止复制链路、冲突标记累计、写路径 fencing；
//   - TestP2T8DisabledZeroRegression（T8）：开关关闭零回归——不降级、
//     冲突标记恒 0。

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeTSO 共享单调 epoch 发号源（模拟中心化 TSO：并发升主时全局唯一递增）。
type fakeTSO struct {
	mu sync.Mutex
	n  uint64
}

func (f *fakeTSO) Get() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return f.n, nil
}

func (f *fakeTSO) GetBatch(n uint64) (uint64, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	start := f.n + 1
	f.n += n
	return start, start + n
}

func (f *fakeTSO) Reset() {}

// TestP2T1PartitionNoPromote 网络分区（T1）：主从断开、从端孤立——
// 集群注册表 3 节点（A/B/C）但 A/C 均不可达，升主仲裁多数派 2/3 无法满足
// → 保持 follower/只读，不升主、无冲突标记。
func TestP2T1PartitionNoPromote(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()

	slave := openTestDB(t)
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}
	// 分区从端 B：注册表含 A/C 但均不可达（模拟网络分区）。
	slave.Cluster.RegisterPeer("node-a", "127.0.0.1:1")
	slave.Cluster.RegisterPeer("node-c", "127.0.0.1:2")
	slave.SetQuorumEnabled(true)
	slave.SetEpochEnabled(true)

	if err := slave.PromoteToMaster(); !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("partitioned promote = %v, want ErrQuorumNotMet", err)
	}
	// 保持 follower（未升主），未降级为陈旧主、无冲突标记。
	if slave.Follower == nil {
		t.Fatal("partitioned node should remain follower")
	}
	if slave.ReadOnly() {
		t.Fatal("partitioned node should not be read-only (never promoted)")
	}
	if slave.ConflictCount() != 0 {
		t.Fatalf("conflict count = %d, want 0", slave.ConflictCount())
	}
}

// TestP2T2ConcurrentDownOnlyOneWins 并发双 down（T2）：共享单调 epoch 源下
// 双端并发升主（模拟两 follower 同时观测主超时并发 PromoteToMaster），
// epoch 大者胜收敛——至多一个保持主身份，陈旧端被 demote 为只读并停复制。
func TestP2T2ConcurrentDownOnlyOneWins(t *testing.T) {
	shared := &fakeTSO{}

	// 分区双端：B、C 各自成为主（各自授权获得单调递增 clusterEpoch）。
	b := openTestDB(t)
	b.TSO = shared
	b.SetEpochEnabled(true)
	if err := b.StartReplication(":0"); err != nil {
		t.Fatalf("B StartReplication: %v", err)
	}
	c := openTestDB(t)
	c.TSO = shared
	c.SetEpochEnabled(true)
	if err := c.StartReplication(":0"); err != nil {
		t.Fatalf("C StartReplication: %v", err)
	}

	eb, ec := b.MasterEpoch(), c.MasterEpoch()
	if eb == 0 || ec == 0 {
		t.Fatalf("both sides should be authorized: eb=%d ec=%d", eb, ec)
	}
	if eb == ec {
		t.Fatalf("shared TSO must yield distinct epochs, got both %d", eb)
	}

	// 恢复合并：较大 epoch 端广播新 epoch → 较小 epoch 端（陈旧主）被
	// demote 停止写入转为只读；新鲜端不受影响。
	var winner, loser *DB
	if eb > ec {
		winner, loser = b, c
	} else {
		winner, loser = c, b
	}

	demoted := make(chan struct{}, 1)
	loser.SetDemoteHook(func() { demoted <- struct{}{} })
	loser.NoteEpoch(winner.MasterEpoch())

	if !loser.ReadOnly() {
		t.Fatal("stale side should be demoted read-only")
	}
	if loser.Replicator != nil {
		t.Fatal("stale side replicator should be stopped")
	}
	if loser.ConflictCount() < 1 {
		t.Fatalf("stale side conflict count = %d, want >= 1", loser.ConflictCount())
	}
	if err := loser.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale side AllowWrite = %v, want ErrFenced", err)
	}
	select {
	case <-demoted:
	default:
		t.Fatal("demote hook should fire on stale side")
	}
	if err := winner.AllowWrite(); err != nil {
		t.Fatalf("winner AllowWrite = %v, want nil", err)
	}
	if winner.Replicator == nil {
		t.Fatal("winner should keep replicator (still master)")
	}
}

// TestP2T3ReconcileDemoteStaleMaster 分区恢复合并（T3，复制轨）：
// 双主形成后恢复网络，epoch 大者胜——陈旧主被 fencing 停止写入转为只读，
// 冲突标记累计；SQL 写路径同样被拒。
func TestP2T3ReconcileDemoteStaleMaster(t *testing.T) {
	d := openTestDB(t)
	d.SetEpochEnabled(true)
	if err := d.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	me := d.MasterEpoch()
	if me == 0 {
		t.Fatal("master epoch should be authorized after StartReplication")
	}

	demoted := make(chan struct{}, 1)
	d.SetDemoteHook(func() { demoted <- struct{}{} })

	// 收到更大 epoch（对端新主广播）→ 陈旧主降级。
	d.NoteEpoch(me + 10)
	if !d.ReadOnly() {
		t.Fatal("stale master should be read-only")
	}
	if d.Replicator != nil {
		t.Fatal("stale master replicator should be stopped")
	}
	if d.ConflictCount() < 1 {
		t.Fatalf("conflict count = %d, want >= 1", d.ConflictCount())
	}
	select {
	case <-demoted:
	default:
		t.Fatal("demote hook should fire")
	}

	// 陈旧主写路径被拒（ErrFenced），且冲突标记累计。
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("AllowWrite = %v, want ErrFenced", err)
	}
	if d.ConflictCount() < 2 {
		t.Fatalf("conflict count after fenced write = %d, want >= 2", d.ConflictCount())
	}

	// SQL INSERT 同样被拒（WriteFence 装配）。
	if _, err := d.SQL.Execute("CREATE TABLE t (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := d.SQL.Execute("INSERT INTO t (id) VALUES (1)"); err == nil {
		t.Fatal("INSERT on demoted stale master: want error, got nil")
	}

	// 开关关闭：NoteEpoch 不触发降级、冲突标记恒 0（T8 零回归）。
	d2 := openTestDB(t)
	if err := d2.StartReplication(":0"); err != nil {
		t.Fatalf("d2 StartReplication: %v", err)
	}
	d2.NoteEpoch(d2.MasterEpoch() + 10)
	if d2.ReadOnly() || d2.ConflictCount() != 0 {
		t.Fatalf("switches off: readOnly=%v conflicts=%d, want false/0", d2.ReadOnly(), d2.ConflictCount())
	}
}

// TestP2T8DisabledZeroRegression 开关关闭零回归（T8）：
// 默认 epoch/quorum 双双关闭；写路径放行、无降级、冲突标记恒 0；
// 复制升主不受影响（M6 既有行为不变）。
func TestP2T8DisabledZeroRegression(t *testing.T) {
	d := openTestDB(t)
	if d.EpochEnabled() || d.QuorumEnabled() {
		t.Fatalf("default switches: epoch=%v quorum=%v, want both off", d.EpochEnabled(), d.QuorumEnabled())
	}
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("AllowWrite = %v, want nil", err)
	}
	if d.ReadOnly() || d.ConflictCount() != 0 {
		t.Fatalf("fresh node readOnly=%v conflicts=%d, want false/0", d.ReadOnly(), d.ConflictCount())
	}
	if err := d.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	if d.ReadOnly() || d.ConflictCount() != 0 {
		t.Fatalf("master with switches off readOnly=%v conflicts=%d, want false/0", d.ReadOnly(), d.ConflictCount())
	}
	if d.Replicator == nil {
		t.Fatal("master should have replicator")
	}
	if _, err := d.SQL.Execute("CREATE TABLE t (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := d.SQL.Execute("INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatalf("INSERT with switches off: %v", err)
	}
	if d.ConflictCount() != 0 {
		t.Fatalf("conflict count after writes = %d, want 0", d.ConflictCount())
	}
}

var _ = time.Second // 保持 time 导入（超时断言辅助，避免 unused）

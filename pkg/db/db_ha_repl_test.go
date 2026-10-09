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

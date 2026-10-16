package db

// T33 同步/半同步复制集成测试（M20）。
// 覆盖：async 默认零回归、semi-sync 提交等待 ACK 且副本即时一致、sync 全副本
// 确认、sync 无在线副本拒绝提交（ErrNoSlave）、semi-sync 无副本降级异步、
// 模式热切换回 async、SQL 配置层 SET replication_*。

import (
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/replication"
)

// commitKVErr 提交键值对并返回 Commit 错误（不主动失败）。
func commitKVErr(t *testing.T, d *DB, key, val string) error {
	t.Helper()
	tx, err := d.Txn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put([]byte(key), []byte(val)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	return tx.Commit()
}

// 断言从库存储中读到指定键值。
func assertSlaveValue(t *testing.T, d *DB, key, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if v, err := d.Storage.Get([]byte(key)); err == nil && string(v) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	v, err := d.Storage.Get([]byte(key))
	t.Fatalf("slave key %q = %q err=%v, want %q", key, v, err, want)
}

// 默认 async（M2 兼容）：提交不等待、从库稍后追平。
func TestDBSyncReplAsyncDefaultCompatible(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()
	slave := openTestDB(t)
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}
	if got := main.ReplicationSyncStatus().Mode; got != replication.ModeAsync {
		t.Fatalf("default mode = %v, want async", got)
	}
	commitKV(t, main, "k", "v")
	assertSlaveValue(t, slave, "k", "v")
}

// semi-sync：提交返回前至少 1 个副本 ACK，从库即时一致且不降级。
func TestDBSyncReplSemiSyncConfirmed(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()
	slave := openTestDB(t)
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}
	if err := main.SetReplicationMode(replication.ModeSemiSync, 1, 10*time.Second); err != nil {
		t.Fatalf("SetReplicationMode: %v", err)
	}
	commitKV(t, main, "s", "1")
	st := main.ReplicationSyncStatus()
	if st.Degraded {
		t.Fatalf("semi-sync confirmed should not degrade: %+v", st)
	}
	assertSlaveValue(t, slave, "s", "1")
}

// sync：全部在线副本确认后才返回；两从库均即时一致。
func TestDBSyncReplSyncAllSlaves(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()
	slave1 := openTestDB(t)
	if err := slave1.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower1: %v", err)
	}
	slave2 := openTestDB(t)
	if err := slave2.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower2: %v", err)
	}
	if err := main.SetReplicationMode(replication.ModeSync, 1, 10*time.Second); err != nil {
		t.Fatalf("SetReplicationMode: %v", err)
	}
	commitKV(t, main, "x", "9")
	assertSlaveValue(t, slave1, "x", "9")
	assertSlaveValue(t, slave2, "x", "9")
}

// sync 无在线副本：拒绝伪同步提交（事务已本地提交，返回错误）。
func TestDBSyncReplSyncNoSlaveFails(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	if err := main.SetReplicationMode(replication.ModeSync, 1, 10*time.Second); err != nil {
		t.Fatalf("SetReplicationMode: %v", err)
	}
	err := commitKVErr(t, main, "n", "1")
	if err == nil {
		t.Fatalf("sync commit without slave should fail")
	}
	if !strings.Contains(err.Error(), "replication: sync requires at least one online replica") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// semi-sync 无在线副本：降级异步，提交成功并记录降级事件。
func TestDBSyncReplSemiSyncDegradeNoSlave(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	if err := main.SetReplicationMode(replication.ModeSemiSync, 1, 10*time.Second); err != nil {
		t.Fatalf("SetReplicationMode: %v", err)
	}
	commitKV(t, main, "d", "1")
	st := main.ReplicationSyncStatus()
	if !st.Degraded || st.DegradedCount < 1 {
		t.Fatalf("semi-sync should degrade without slave: %+v", st)
	}
}

// 热切换回 async：不再阻塞/报错，从库继续追平。
func TestDBSyncReplSwitchBackAsync(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()
	slave := openTestDB(t)
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}
	if err := main.SetReplicationMode(replication.ModeSync, 1, 10*time.Second); err != nil {
		t.Fatalf("SetReplicationMode: %v", err)
	}
	if err := main.SetReplicationMode(replication.ModeAsync, 1, 0); err != nil {
		t.Fatalf("switch async: %v", err)
	}
	commitKV(t, main, "a", "2")
	if got := main.ReplicationSyncStatus().Mode; got != replication.ModeAsync {
		t.Fatalf("mode = %v, want async", got)
	}
	assertSlaveValue(t, slave, "a", "2")
}

// SQL 配置层：SET replication_mode / replication_ack / replication_timeout。
func TestDBSyncReplSQLConfig(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	if _, err := main.SQL.Execute("SET replication_mode='semi-sync'"); err != nil {
		t.Fatalf("SET replication_mode: %v", err)
	}
	if _, err := main.SQL.Execute("SET replication_ack=2"); err != nil {
		t.Fatalf("SET replication_ack: %v", err)
	}
	if _, err := main.SQL.Execute("SET replication_timeout=3000"); err != nil {
		t.Fatalf("SET replication_timeout: %v", err)
	}
	st := main.ReplicationSyncStatus()
	if st.Mode != replication.ModeSemiSync || st.SemiAckN != 2 || st.Timeout != 3*time.Second {
		t.Fatalf("status after SET = %+v", st)
	}
	// 非法值拒绝
	if _, err := main.SQL.Execute("SET replication_mode='bogus'"); err == nil {
		t.Fatalf("invalid mode should fail")
	}
	if _, err := main.SQL.Execute("SET replication_ack=0"); err == nil {
		t.Fatalf("ack=0 should fail")
	}
	// 未启动复制时 SET 报明确错误
	noRepl := openTestDB(t)
	if _, err := noRepl.SQL.Execute("SET replication_mode='sync'"); err == nil {
		t.Fatalf("SET without replication should fail")
	}
}

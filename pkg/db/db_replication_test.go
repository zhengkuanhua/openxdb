package db

import (
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// openTestDB 初始化并打开一个真实 RocksDB 数据目录。
func openTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	d, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func commitKV(t *testing.T, d *DB, key, val string) {
	t.Helper()
	tx, err := d.Txn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put([]byte(key), []byte(val)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func waitFollowerLSN(t *testing.T, f *replication.Follower, want storage.LSN) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if f.AppliedLSN() >= want {
			return
		}
		if f.Status().State == "offline" {
			t.Fatalf("follower offline: %s", f.Status().LastError)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("follower applied = %d, want %d", f.AppliedLSN(), want)
}

// 主库提交后 binlog 落盘、位点推进。
func TestDBReplicationBinlogOnCommit(t *testing.T) {
	d := openTestDB(t)
	if err := d.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, slaves := d.ReplicationStatus()
	if addr == "" {
		t.Fatalf("ReplicationStatus address empty")
	}
	if len(slaves) != 0 {
		t.Fatalf("expected no slaves yet, got %d", len(slaves))
	}
	for i := 1; i <= 3; i++ {
		commitKV(t, d, "k"+string(rune('0'+i)), "v")
	}
	if got := d.Replicator.LastLSN(); got != 3 {
		t.Fatalf("master LastLSN = %d, want 3", got)
	}
	if got := d.Txn.HookErr(); got != nil {
		t.Fatalf("commit hook error: %v", got)
	}
	// 单机语义不被破坏：本地读得到提交值
	v, err := d.Storage.Get([]byte("k1"))
	if err != nil || string(v) != "v" {
		t.Fatalf("local read k1 = %q err=%v", v, err)
	}
}

// 主从真实链路：主库提交 → binlog 推送 → 从库应用后数据一致。
func TestDBMasterFollowerConsistency(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()

	slave := openTestDB(t)
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}

	// 先写 2 条（follower 未连上也会通过握手 from=0 全量补）
	commitKV(t, main, "a", "1")
	commitKV(t, main, "b", "2")
	waitFollowerLSN(t, slave.Follower, 2)
	if v, err := slave.Storage.Get([]byte("a")); err != nil || string(v) != "1" {
		t.Fatalf("slave a = %q err=%v", v, err)
	}
	if v, err := slave.Storage.Get([]byte("b")); err != nil || string(v) != "2" {
		t.Fatalf("slave b = %q err=%v", v, err)
	}
	// 主库后续提交增量推送
	commitKV(t, main, "c", "3")
	waitFollowerLSN(t, slave.Follower, 3)
	if v, err := slave.Storage.Get([]byte("c")); err != nil || string(v) != "3" {
		t.Fatalf("slave c = %q err=%v", v, err)
	}
}

// 从库启动在空库：主库提交后从库应用；重复启动不破坏已应用数据。
func TestDBReplicationIdempotentApply(t *testing.T) {
	main := openTestDB(t)
	if err := main.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()

	slave := openTestDB(t)
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}
	commitKV(t, main, "x", "1")
	commitKV(t, main, "y", "2")
	waitFollowerLSN(t, slave.Follower, 2)
	// 直接再 apply 同一位点，应被幂等跳过（无副作用、无错误）
	if err := slave.Follower.ApplyBinlog(&replication.BinlogEntry{LSN: 2, Batch: &storage.WriteBatch{}}); err != nil {
		t.Fatalf("idempotent apply: %v", err)
	}
	if got := slave.Follower.AppliedLSN(); got != 2 {
		t.Fatalf("applied = %d, want 2", got)
	}
}

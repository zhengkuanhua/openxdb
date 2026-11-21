package db_test

// v1.20.0 C1：2PC 参与者提交接入事务提交钩子的 db 层测试。
// 覆盖：ApplyParticipantBatch 经事务链路落库并产生 binlog、从端可复制；
// 集群跨 region 2PC 参与者提交（StartCluster 已装配钩子）产生的 binlog
// 覆盖参与者侧新增键并被从端读到。

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// readBinlog 只读扫描主库 binlog 全部完整记录。
func readBinlog(t *testing.T, dir string) []*replication.BinlogEntry {
	t.Helper()
	r, err := replication.OpenBinlogRead(filepath.Join(dir, db.BinlogFile))
	if err != nil {
		t.Fatalf("open binlog: %v", err)
	}
	defer r.Close()
	var out []*replication.BinlogEntry
	if _, err := r.ScanFrom(0, func(e *replication.BinlogEntry) error {
		out = append(out, e)
		return nil
	}); err != nil {
		t.Fatalf("scan binlog: %v", err)
	}
	return out
}

// batchHasPut 判断 binlog 条目批次是否含指定键值写入。
func batchHasPut(e *replication.BinlogEntry, key, val string) bool {
	if e == nil || e.Batch == nil {
		return false
	}
	for _, p := range e.Batch.Puts {
		if bytes.Equal(p.Key, []byte(key)) && bytes.Equal(p.Value, []byte(val)) {
			return true
		}
	}
	return false
}

// waitStorageValue 轮询直到从端存储读到期望值（复制异步推送）。
func waitStorageValue(t *testing.T, st storage.Storage, key, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v, err := st.Get([]byte(key))
		if err == nil && string(v) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	v, err := st.Get([]byte(key))
	t.Fatalf("follower %q = %q err=%v, want %q", key, v, err, want)
}

// TestApplyParticipantBatchProducesBinlog 参与者提交批次经事务链路落库：
// 本地可见、HookErr 为 nil、产生 binlog 条目、从端 ApplyBinlog 后可读；
// nil 批次不产生副作用。
func TestApplyParticipantBatchProducesBinlog(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()
	if err := d.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	repAddr, _ := d.ReplicationStatus()

	fdir := t.TempDir()
	if err := db.Init(fdir); err != nil {
		t.Fatalf("follower init: %v", err)
	}
	df, err := db.Open(fdir)
	if err != nil {
		t.Fatalf("follower open: %v", err)
	}
	defer df.Close()
	if err := df.StartFollower(repAddr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}

	// 预置一个将被删除的键（走存储直写，不计入待验证的 binlog 条目）。
	if err := d.Storage.Write(&storage.WriteBatch{Puts: []storage.KVPair{
		{Key: []byte("c1:gone"), Value: []byte("old")},
	}}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	before := len(readBinlog(t, dir))
	batch := &storage.WriteBatch{
		Puts:    []storage.KVPair{{Key: []byte("c1:k1"), Value: []byte("v1")}},
		Deletes: [][]byte{[]byte("c1:gone")},
	}
	if err := d.ApplyParticipantBatch("tx-c1", 777, batch); err != nil {
		t.Fatalf("ApplyParticipantBatch: %v", err)
	}

	// ① 本地可见（写入生效、删除生效）。
	if v, err := d.Storage.Get([]byte("c1:k1")); err != nil || string(v) != "v1" {
		t.Fatalf("local c1:k1 = %q err=%v", v, err)
	}
	if _, err := d.Storage.Get([]byte("c1:gone")); err != storage.ErrNotFound {
		t.Fatalf("local c1:gone err=%v, want ErrNotFound", err)
	}
	// ② 提交钩子无错误。
	if herr := d.Txn.HookErr(); herr != nil {
		t.Fatalf("Txn.HookErr = %v, want nil", herr)
	}
	// ③ 产生 binlog 条目（恰好 1 条），含数据写入与删除。
	entries := readBinlog(t, dir)
	if len(entries) != before+1 {
		t.Fatalf("binlog entries = %d, want %d", len(entries), before+1)
	}
	e := entries[len(entries)-1]
	if !batchHasPut(e, "c1:k1", "v1") {
		t.Fatalf("binlog entry missing put c1:k1=v1: %+v", e.Batch)
	}
	var hasDel bool
	for _, k := range e.Batch.Deletes {
		if bytes.Equal(k, []byte("c1:gone")) {
			hasDel = true
		}
	}
	if !hasDel {
		t.Fatalf("binlog entry missing delete c1:gone: %+v", e.Batch)
	}
	// ④ 从端应用后读到对应键。
	waitStorageValue(t, df.Storage, "c1:k1", "v1")
	if _, err := df.Storage.Get([]byte("c1:gone")); err != storage.ErrNotFound {
		t.Fatalf("follower c1:gone err=%v, want ErrNotFound", err)
	}

	// ⑤ nil/空批次无副作用（不新增 binlog）。
	if err := d.ApplyParticipantBatch("", 0, nil); err != nil {
		t.Fatalf("ApplyParticipantBatch(nil): %v", err)
	}
	if err := d.ApplyParticipantBatch("", 0, &storage.WriteBatch{}); err != nil {
		t.Fatalf("ApplyParticipantBatch(empty): %v", err)
	}
	if got := len(readBinlog(t, dir)); got != before+1 {
		t.Fatalf("binlog entries after nil batch = %d, want %d", got, before+1)
	}
}

// TestClusterParticipantCommitProducesBinlog 集群跨 region 2PC：参与者侧
// COMMIT（StartCluster 已装配 ApplyParticipantBatch）新增的全部键都出现在
// 参与者 binlog 条目中，且可从其从端读到。
func TestClusterParticipantCommitProducesBinlog(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	defer dA.Close()
	dB, addrB := openNode(t, "node-b")
	defer dB.Close()

	// 参与者 B 作为复制主：其全部事务提交（含 2PC 参与者提交）落 binlog。
	if err := dB.StartReplication(":0"); err != nil {
		t.Fatalf("node-b StartReplication: %v", err)
	}
	repAddr, _ := dB.ReplicationStatus()
	fdir := t.TempDir()
	if err := db.Init(fdir); err != nil {
		t.Fatalf("follower init: %v", err)
	}
	dF, err := db.Open(fdir)
	if err != nil {
		t.Fatalf("follower open: %v", err)
	}
	defer dF.Close()
	if err := dF.StartFollower(repAddr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}

	// 双节点建表 + A 写 1..5 + SPLIT，中间 region 指派给 B（与 M5 2PC 基线一致）。
	schema := "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))"
	mustSQL(t, dA, schema)
	mustSQL(t, dA, "CREATE INDEX idx_age ON users (age)")
	mustSQL(t, dB, schema)
	mustSQL(t, dB, "CREATE INDEX idx_age ON users (age)")
	for i := 1; i <= 5; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: want 3, got %v err=%v", regions, err)
	}
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", regions[1].RegionID, dB.Cluster.SelfID))

	// 基线快照：B 的存储键集合与 binlog 条目数。
	keysBefore, err := dB.Storage.Scan(storage.KeyRange{}, -1)
	if err != nil {
		t.Fatalf("scan node-b: %v", err)
	}
	exist := make(map[string]bool, len(keysBefore))
	for _, p := range keysBefore {
		exist[string(p.Key)] = true
	}
	entriesBefore := len(readBinlog(t, dB.Dir))

	// 跨 region INSERT id=6：归属 B，A 上触发 2PC（prepare B → commit B）。
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)

	// ① 参与者本地新增键非空。
	keysAfter, err := dB.Storage.Scan(storage.KeyRange{}, -1)
	if err != nil {
		t.Fatalf("scan node-b after: %v", err)
	}
	vals := make(map[string]string, len(keysAfter))
	var newKeys []string
	for _, p := range keysAfter {
		vals[string(p.Key)] = string(p.Value)
		if !exist[string(p.Key)] {
			newKeys = append(newKeys, string(p.Key))
		}
	}
	if len(newKeys) == 0 {
		t.Fatalf("participant storage unchanged after cross-region commit")
	}
	// ② binlog 新增条目覆盖参与者全部新增键（= 参与者提交确实产生 binlog）。
	entriesAfter := readBinlog(t, dB.Dir)
	if len(entriesAfter) <= entriesBefore {
		t.Fatalf("binlog entries = %d, want > %d (participant commit must append binlog)", len(entriesAfter), entriesBefore)
	}
	logged := make(map[string]bool)
	for _, e := range entriesAfter[entriesBefore:] {
		if e.Batch == nil {
			continue
		}
		for _, p := range e.Batch.Puts {
			logged[string(p.Key)] = true
		}
	}
	for _, k := range newKeys {
		if !logged[k] {
			t.Fatalf("new participant key %q missing from participant binlog", k)
		}
	}
	// ③ 从端复制到参与者新增键的同值数据。
	for _, k := range newKeys {
		waitStorageValue(t, dF.Storage, k, vals[k])
	}
}

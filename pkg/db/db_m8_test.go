package db_test

// M8 分布式事务增强集成测试：
//   - 跨节点快照隔离：早开始事务的快照读不到后提交的跨节点写入（版本过滤）；
//   - 并发事务隔离：两个先后提交的分布式事务，各自快照读到正确的行集；
//   - 协调者崩溃恢复 commit：参与者 COMMIT 注入失败后重启协调者，
//     依据持久化 CoordRecord 决策 commit 并驱动参与者幂等补交；
//   - 协调者崩溃恢复 abort：preparing 记录重启后决策 abort，参与者清理无残留；
//   - 单节点零回归：装配 TSO 后单机 DML 行为不变。

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// setupM8 双节点基线（同 setup2pc，但额外把 B 的时间戳源指向 A 中心 TSO）：
// A 建 users 表（含二级索引 idx_age）写 1..5，SPLIT 出中间 region [4,8)，
// ADD NODE B 并指派中间 region；随后 B 通过 SetTSONode 使用 A 的全局发号器。
// 此后 A/B 的事务取号来自同一 TSO 序列（全局单调可比）。
func setupM8(t *testing.T) (*db.DB, *db.DB, int64) {
	t.Helper()
	dA, addrA := openNode(t, "node-a")
	t.Cleanup(func() { dA.Close() })
	dB, addrB := openNode(t, "node-b")
	t.Cleanup(func() { dB.Close() })

	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, dA, "CREATE INDEX idx_age ON users (age)")
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
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
	mid := regions[1]
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", mid.RegionID, dB.Cluster.SelfID))
	// 中心化 TSO：B 的取号全部来自 A（批量缓存 8 个/批）。
	if err := dB.SetTSONode(dA.Cluster.SelfID, 8); err != nil {
		t.Fatalf("SetTSONode: %v", err)
	}
	_ = addrA
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5)
	return dA, dB, int64(mid.RegionID)
}

// TestM8SingleNodeZeroRegression 单节点（装配本地 TSO）DML 行为零回归：
// 增删改查、显式事务写后读自身、二级索引全部正常。
func TestM8SingleNodeZeroRegression(t *testing.T) {
	d, _ := openNode(t, "node-a")
	defer d.Close()
	mustSQL(t, d, "CREATE TABLE t1 (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, d, "CREATE INDEX idx_age ON t1 (age)")
	for i := 1; i <= 20; i++ {
		mustSQL(t, d, fmt.Sprintf("INSERT INTO t1 (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	wantIDs(t, d, "SELECT id FROM t1 ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
	mustSQL(t, d, "UPDATE t1 SET age = 99 WHERE id = 7")
	res := mustSQL(t, d, "SELECT age FROM t1 WHERE id = 7")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 99 {
		t.Fatalf("update: %+v", res.Rows)
	}
	mustSQL(t, d, "DELETE FROM t1 WHERE id = 3")
	wantIDs(t, d, "SELECT id FROM t1 ORDER BY id", 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
	// 显式事务：写后读自身可见，提交后新语句可见
	mustSQL(t, d, "BEGIN")
	mustSQL(t, d, "INSERT INTO t1 (id, name, age) VALUES (100, 'u100', 100)")
	res = mustSQL(t, d, "SELECT id FROM t1 WHERE id = 100")
	if len(res.Rows) != 1 {
		t.Fatalf("explicit txn write-read own: %+v", res.Rows)
	}
	mustSQL(t, d, "ROLLBACK")
	res = mustSQL(t, d, "SELECT id FROM t1 WHERE id = 100")
	if len(res.Rows) != 0 {
		t.Fatalf("rollback residue: %+v", res.Rows)
	}
}

// TestM8CrossNodeSnapshotConsistency 跨节点快照隔离：
// B 上显式事务 T2 先开始（快照早于 A 上跨节点 INSERT 6 的提交），
// T2 在其事务内读不到后提交的写入；提交后新快照读到，且两端一致。
func TestM8CrossNodeSnapshotConsistency(t *testing.T) {
	dA, dB, _ := setupM8(t)

	// T2（B 侧显式事务）先开，快照取自 A 的全局 TSO。
	mustSQL(t, dB, "BEGIN")
	// T1（A 侧 autocommit）跨 region INSERT 6 → 2PC 提交，commitTS 晚于 T2 快照。
	res := mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	if res.AffectedRows != 1 {
		t.Fatalf("cross-region insert: affected=%d", res.AffectedRows)
	}
	// T2 读不到 T1 的写入（B 本地 region 版本过滤 + 跨节点读版本透传）。
	res = mustSQL(t, dB, "SELECT id FROM users WHERE id = 6")
	if len(res.Rows) != 0 {
		t.Fatalf("early snapshot saw post-commit write: %+v", res.Rows)
	}
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5)
	// T2 提交后：新快照读到 6，且 A/B 两侧一致。
	mustSQL(t, dB, "COMMIT")
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	// 版本记录（m:ver:*）在协调者与参与者两侧均落盘。
	// 上界用 0xff 后缀（"m:ver0" 字典序小于 "m:ver:"，无法命中任何记录）。
	rowsA, err := dA.Storage.Scan(storage.KeyRange{Start: []byte("m:ver:"), End: []byte("m:ver\xff")}, -1)
	if err != nil || len(rowsA) == 0 {
		t.Fatalf("A version records: %v rows=%d", err, len(rowsA))
	}
	rowsB, err := dB.Storage.Scan(storage.KeyRange{Start: []byte("m:ver:"), End: []byte("m:ver\xff")}, -1)
	if err != nil || len(rowsB) == 0 {
		t.Fatalf("B version records: %v rows=%d", err, len(rowsB))
	}
}

// TestM8ConcurrentTxnIsolation 并发事务隔离：两个先后提交的分布式事务，
// 各事务在自己的快照下读到正确的行集（后提交的写入对早开始事务不可见）。
func TestM8ConcurrentTxnIsolation(t *testing.T) {
	dA, dB, _ := setupM8(t)

	// T1 提交 INSERT 6。
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	// T2 开始（快照晚于 T1 提交）→ T2 能看到 T1。
	mustSQL(t, dB, "BEGIN")
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	// T3 提交 INSERT 7（晚于 T2 快照）→ T2 看不到 T3。
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (7, 'u7', 27)")
	res := mustSQL(t, dB, "SELECT id FROM users WHERE id = 7")
	if len(res.Rows) != 0 {
		t.Fatalf("concurrent txn saw later commit: %+v", res.Rows)
	}
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	// T2 提交后：新快照看到 T3，A/B 一致。
	mustSQL(t, dB, "COMMIT")
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7)
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7)
}

// openNodeAt 以固定地址打开（或重启）一个节点：已初始化目录直接 Open，
// 未初始化则 Init；返回 DB（StartCluster 已执行）。
func openNodeAt(t *testing.T, dir, name, addr string) *db.DB {
	t.Helper()
	if err := db.Init(dir); err != nil && err != db.ErrAlreadyInitialized {
		t.Fatalf("%s init: %v", name, err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("%s open: %v", name, err)
	}
	if _, err := d.StartCluster(addr); err != nil {
		t.Fatalf("%s start cluster: %v", name, err)
	}
	return d
}

// crashSetup 崩溃恢复基线：A 固定端口、B 随机端口；建表/数据/分裂/指派，
// B 时间戳源指向 A。返回 dirA（供重启复用）、dA、dB、midRegionID。
// dA 由调用方负责 Close（重启需要手动关闭）。
func crashSetup(t *testing.T, aAddr string) (string, *db.DB, *db.DB, int64) {
	t.Helper()
	dirA := t.TempDir()
	dA := openNodeAt(t, dirA, "node-a", aAddr)
	dirB := t.TempDir()
	dB := openNodeAt(t, dirB, "node-b", "127.0.0.1:0")
	t.Cleanup(func() { dB.Close() })

	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, dA, "CREATE INDEX idx_age ON users (age)")
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
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
	mid := regions[1]
	addrB := dB.Cluster.SelfAddr
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", mid.RegionID, dB.Cluster.SelfID))
	if err := dB.SetTSONode(dA.Cluster.SelfID, 8); err != nil {
		t.Fatalf("SetTSONode: %v", err)
	}
	return dirA, dA, dB, int64(mid.RegionID)
}

// TestM8CoordinatorCrashRecoveryCommit 协调者崩溃恢复 commit：
// 参与者 COMMIT 阶段注入失败 → 协调者记录 committing 后返回 TXN_COMMIT_UNCERTAIN
// （本地已提交、参与者未提交）；重启协调者后 RecoverCoordinated2PC 依据持久化
// CoordRecord 决策 commit，驱动参与者幂等补交，两端一致，coord 记录清除。
func TestM8CoordinatorCrashRecoveryCommit(t *testing.T) {
	aAddr := "127.0.0.1:39298"
	dirA, dA, dB, _ := crashSetup(t, aAddr)
	defer dA.Close()

	// 注入参与者 B 的 COMMIT 阶段故障（模拟协调者发 COMMIT 时参与者无响应/失败）。
	dB.ClusterSrv.SetCommitFail(func(txID string) error {
		return fmt.Errorf("injected commit failure")
	})
	_, err := dA.SQL.Execute("INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	if err == nil || !strings.Contains(err.Error(), "TXN_COMMIT_UNCERTAIN") {
		t.Fatalf("want TXN_COMMIT_UNCERTAIN, got %v", err)
	}
	// 参与者 B：prepared 标记在、数据未提交（本地读不到 6）。
	rows, err := dB.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:p:"), End: []byte("m:2pc:q")}, -1)
	if err != nil || len(rows) == 0 {
		t.Fatalf("B prepared marker: %v rows=%d", err, len(rows))
	}
	res := mustSQL(t, dB, "SELECT id FROM users WHERE id = 6")
	if len(res.Rows) != 0 {
		t.Fatalf("B saw uncommitted data: %+v", res.Rows)
	}
	// 清除注入，重启协调者 A（同目录同端口）→ RecoverCoordinated2PC 决策 commit。
	dB.ClusterSrv.SetCommitFail(nil)
	dA.Close()
	dA2 := openNodeAt(t, dirA, "node-a", aAddr)
	defer dA2.Close()

	// 参与者 B 被驱动补交：数据落盘、两端一致。
	res = mustSQL(t, dB, "SELECT name FROM users WHERE id = 6")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u6" {
		t.Fatalf("B after recovery: %+v", res.Rows)
	}
	res = mustSQL(t, dA2, "SELECT name FROM users WHERE id = 6")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u6" {
		t.Fatalf("A after recovery: %+v", res.Rows)
	}
	wantIDs(t, dA2, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	// coord 记录已清除。
	rows, err = dA2.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:coord:"), End: []byte("m:2pc:coorq")}, -1)
	if err != nil || len(rows) != 0 {
		t.Fatalf("coord record not cleaned: %v rows=%d", err, len(rows))
	}
	// 再次重启：无残留记录、恢复幂等、数据一致。
	dA2.Close()
	dA3 := openNodeAt(t, dirA, "node-a", aAddr)
	defer dA3.Close()
	res = mustSQL(t, dA3, "SELECT name FROM users WHERE id = 6")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u6" {
		t.Fatalf("A3 after re-recovery: %+v", res.Rows)
	}
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
}

// TestM8CoordinatorCrashRecoveryAbort 协调者崩溃恢复 abort：
// 手工持久化 preparing 状态 CoordRecord（模拟协调者在 prepare 全部完成前崩溃），
// 参与者 B 已真实 prepare；重启协调者后决策 abort，参与者清理无残留、coord 删除。
func TestM8CoordinatorCrashRecoveryAbort(t *testing.T) {
	aAddr := "127.0.0.1:39299"
	dirA, dA, dB, _ := crashSetup(t, aAddr)
	defer dA.Close()

	// 模拟协调者已写 preparing 记录、并已向 B 发出 prepare（B 落盘 prepared 标记）
	// 后崩溃。
	txID := "m8-abort-1"
	rec := cluster.CoordRecord{
		TxID:         txID,
		BeginTS:      1000,
		CommitTS:     0,
		Participants: []string{dB.Cluster.SelfID},
		LocalOps:     nil,
		State:        cluster.CoordPreparing,
	}
	raw, err := cluster.MarshalCoord(rec)
	if err != nil {
		t.Fatalf("marshal coord: %v", err)
	}
	if err := dA.Storage.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: cluster.CoordKey(txID), Value: raw}}}); err != nil {
		t.Fatalf("put coord: %v", err)
	}
	if err := dA.Cluster.PrepareTxn(dB.Cluster.SelfID, txID, 1000, []cluster.TxnOp{{Key: []byte("k1"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("prepare B: %v", err)
	}
	// B 侧：prepared 标记存在、数据未落盘。
	rows, err := dB.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:p:"), End: []byte("m:2pc:q")}, -1)
	if err != nil || len(rows) == 0 {
		t.Fatalf("B prepared marker: %v rows=%d", err, len(rows))
	}
	if _, err := dB.Storage.Get([]byte("k1")); err == nil {
		t.Fatalf("prepared data should not be visible")
	}
	// 重启协调者 → RecoverCoordinated2PC 决策 abort。
	dA.Close()
	dA2 := openNodeAt(t, dirA, "node-a", aAddr)
	defer dA2.Close()

	// 参与者 B：prepared 清理、aborted 标记落盘、数据无残留。
	rows, err = dB.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:p:"), End: []byte("m:2pc:q")}, -1)
	if err != nil || len(rows) != 0 {
		t.Fatalf("B prepared not cleaned: %v rows=%d", err, len(rows))
	}
	rows, err = dB.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:a:"), End: []byte("m:2pc:b")}, -1)
	if err != nil || len(rows) == 0 {
		t.Fatalf("B aborted marker: %v rows=%d", err, len(rows))
	}
	if _, err := dB.Storage.Get([]byte("k1")); err == nil {
		t.Fatalf("aborted data residue")
	}
	// 协调者 coord 记录已删除。
	rows, err = dA2.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:coord:"), End: []byte("m:2pc:coorq")}, -1)
	if err != nil || len(rows) != 0 {
		t.Fatalf("coord record not cleaned: %v rows=%d", err, len(rows))
	}
}

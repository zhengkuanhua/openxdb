package db_test

// M5 分布式写路径 / 2PC 集成测试：
// 双节点跨 region INSERT/UPDATE/DELETE 两阶段提交原子性（prepare → commit/abort），
// 参与者崩溃恢复（Recover2PC 清理幂等标记），单节点零回归，prepare 失败本地无残留。

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// setup2pc 构建 M5 双节点基线：
// A 建 users 表（含二级索引 idx_age）写 1..5，SPLIT 出中间 region [4,8)，
// ADD NODE B 并将中间 region 指派给 B。返回 (dA, dB, midRegionID)。
// 此后 A 上 INSERT/UPDATE/DELETE 命中 id∈[4,8) 即触发跨节点 2PC。
func setup2pc(t *testing.T) (*db.DB, *db.DB, int64) {
	t.Helper()
	dA, _ := openNode(t, "node-a")
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
	// 基线：中间 region [4,8) 为空，A/B 两侧 1..5 一致
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5)
	return dA, dB, int64(mid.RegionID)
}

// TestM5CrossNodeInsertCommit 双节点跨 region INSERT 成功提交且两端数据一致
// （含二级索引键随 2PC 落参与者，索引可命中）。
func TestM5CrossNodeInsertCommit(t *testing.T) {
	dA, dB, _ := setup2pc(t)

	// id=6 落入中间 region [4,8)，由 B 持有 → A 上触发 2PC（prepare B → commit）
	res := mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	if res.AffectedRows != 1 {
		t.Fatalf("INSERT cross-region: affected=%d", res.AffectedRows)
	}
	// 两端一致：A 全量 1..6，B 全量 1..6
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	res = mustSQL(t, dA, "SELECT name FROM users WHERE id = 6")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u6" {
		t.Fatalf("A point id=6 (remote region): %+v", res.Rows)
	}
	// 二级索引：age=26 的索引键落在 B（与行同 region），B 侧索引查询命中
	res = mustSQL(t, dB, "SELECT id FROM users WHERE age = 26 ORDER BY id")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 6 {
		t.Fatalf("B index scan age=26 (2pc index keys): %+v", res.Rows)
	}
	// 参与方 commit 幂等标记存在（m:2pc:c:*）
	rows, err := dB.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:c:"), End: []byte("m:2pcq")}, -1)
	if err != nil || len(rows) == 0 {
		t.Fatalf("B committed markers: %v rows=%d", err, len(rows))
	}
}

// TestM5CrossNodeUpdateDelete 双节点跨 region UPDATE/DELETE 2PC 后两端一致、
// 删除无残留。
func TestM5CrossNodeUpdateDelete(t *testing.T) {
	dA, dB, _ := setup2pc(t)
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")

	// 跨 region UPDATE：id=6 归属 B，A 上 UPDATE 走 2PC
	mustSQL(t, dA, "UPDATE users SET age = 99 WHERE id = 6")
	res := mustSQL(t, dA, "SELECT age FROM users WHERE id = 6")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 99 {
		t.Fatalf("A update cross-region: %+v", res.Rows)
	}
	res = mustSQL(t, dB, "SELECT age FROM users WHERE id = 6")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 99 {
		t.Fatalf("B update cross-region: %+v", res.Rows)
	}
	// 索引维护：age 由 26→99 后，age=26 索引键被删、age=99 索引键落 B
	res = mustSQL(t, dB, "SELECT id FROM users WHERE age = 26")
	if len(res.Rows) != 0 {
		t.Fatalf("B stale index age=26: %+v", res.Rows)
	}
	res = mustSQL(t, dB, "SELECT id FROM users WHERE age = 99 ORDER BY id")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 6 {
		t.Fatalf("B index age=99: %+v", res.Rows)
	}

	// 跨 region DELETE：A 上 DELETE 走 2PC，两端无残留
	mustSQL(t, dA, "DELETE FROM users WHERE id = 6")
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5)
	res = mustSQL(t, dB, "SELECT id FROM users WHERE id = 6")
	if len(res.Rows) != 0 {
		t.Fatalf("B delete residue: %+v", res.Rows)
	}
}

// TestM5PrepareFailureAbort prepare 阶段失败（参与者拒绝/准备阶段故障）→ 协调者
// abort 整笔事务，本地组无残留：同语句中的本地行（id=0）也必须回滚。
func TestM5PrepareFailureAbort(t *testing.T) {
	dA, dB, _ := setup2pc(t)
	// 注入参与者 prepare 阶段故障（模拟参与方拒绝 prepare）
	dB.ClusterSrv.SetPrepareFail(func(txID string) error {
		return fmt.Errorf("injected prepare failure for %s", txID)
	})

	// 一行本地（id=0 归属 <4）、一行远端（id=6 归属 B）：prepare 失败整体回滚
	_, err := dA.SQL.Execute("INSERT INTO users (id, name, age) VALUES (0, 'x0', 33), (6, 'y6', 66)")
	if err == nil {
		t.Fatalf("INSERT with failing participant: want error, got nil")
	}
	if !strings.Contains(err.Error(), "PREPARE") {
		t.Fatalf("want TXN_PREPARE_FAILED error, got: %v", err)
	}
	// 本地无残留：id=0 未提交（本地组随 abort 回滚）；远端 id=6 拒绝后未应用
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5)
}

// TestM5SingleNodeZeroRegression 单节点部署（StartCluster 但所有 region 本地，
// 无远端节点）2PC 路径零触发：INSERT/UPDATE/DELETE + 索引全部走原路径。
func TestM5SingleNodeZeroRegression(t *testing.T) {
	d, _ := openNode(t, "node-a")
	t.Cleanup(func() { d.Close() })

	mustSQL(t, d, "CREATE TABLE t (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, d, "CREATE INDEX idx_age ON t (age)")
	mustSQL(t, d, "INSERT INTO t (id, name, age) VALUES (1, 'a', 10), (2, 'b', 20)")
	mustSQL(t, d, "UPDATE t SET age = 30 WHERE id = 1")
	mustSQL(t, d, "DELETE FROM t WHERE id = 2")
	wantIDs(t, d, "SELECT id FROM t ORDER BY id", 1)
	res := mustSQL(t, d, "SELECT id FROM t WHERE age = 30")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 1 {
		t.Fatalf("index scan after single-node writes: %+v", res.Rows)
	}
	// 无任何 2PC 标记落盘（零触发）
	rows, err := d.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:"), End: []byte("m:2pcq")}, -1)
	if err != nil || len(rows) != 0 {
		t.Fatalf("single-node 2pc markers should be empty: %v rows=%d", err, len(rows))
	}
}

// TestM5RecoverClearsMarkers 参与者重启恢复：Recover2PC 清理已提交幂等标记，
// 已应用数据保留；未决 prepare 无数据残留。
func TestM5RecoverClearsMarkers(t *testing.T) {
	// B 使用固定目录以便重启（A 用临时目录即可）
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("B init: %v", err)
	}
	dB, err := db.Open(dir)
	if err != nil {
		t.Fatalf("B open: %v", err)
	}
	addrB, err := dB.StartCluster("127.0.0.1:0")
	if err != nil {
		t.Fatalf("B start cluster: %v", err)
	}

	dA, _ := openNode(t, "node-a")
	t.Cleanup(func() { dA.Close() })
	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	for i := 1; i <= 5; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: %v err=%v", regions, err)
	}
	mid := regions[1]
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", mid.RegionID, dB.Cluster.SelfID))

	// 跨 region INSERT 成功 → B 落 committed 标记
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	rows, err := dB.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:c:"), End: []byte("m:2pcq")}, -1)
	if err != nil || len(rows) == 0 {
		t.Fatalf("B committed markers before restart: %v rows=%d", err, len(rows))
	}

	// 重启 B（close → 重新 Open/StartCluster → Recover2PC）
	if err := dB.Close(); err != nil {
		t.Fatalf("B close: %v", err)
	}
	dB2, err := db.Open(dir)
	if err != nil {
		t.Fatalf("B reopen: %v", err)
	}
	t.Cleanup(func() { dB2.Close() })
	if _, err := dB2.StartCluster("127.0.0.1:0"); err != nil {
		t.Fatalf("B restart cluster: %v", err)
	}
	addrB2 := dB2.ClusterSrv.SelfAddr
	// 幂等标记已清理，数据保留（提交已落盘，恢复不丢失）
	rows, err = dB2.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:"), End: []byte("m:2pcq")}, -1)
	if err != nil || len(rows) != 0 {
		t.Fatalf("B markers after recovery should be empty: %v rows=%d", err, len(rows))
	}
	// 重启后的 B 重新加入 A（ADD NODE 重新握手），数据一致
	mustSQL(t, dA, "ADD NODE '"+addrB2+"'")
	wantIDs(t, dB2, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
}

// ensure binary 引用（splitBoundary 使用）
var _ = binary.BigEndian

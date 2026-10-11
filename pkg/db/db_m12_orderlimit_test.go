package db_test

// M12-2 跨节点 ORDER BY / LIMIT 分页集成测试：
// 双节点集群，users 拆 3 region 且中间 region 指派给 B，10 行数据落在
// A/B/A 三个 region。验证：
//   - ORDER BY + LIMIT：各分片本地排序取 top-N 窗口（远端经 ScanSortedRemote），
//     coordinator 归并后截断；
//   - ORDER BY + OFFSET：各分片取 top-(offset+limit)，coordinator 跳偏移；
//   - 无 ORDER BY 仅 LIMIT/OFFSET：各分片取足量（scanMergedClusterTop），
//     协调端按键序合并截断；
//   - TEXT 列排序下推一致性。

import (
	"fmt"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// setupOrderLimitCluster 在 A 建 users（id/name）写 10 行，拆 3 region，
// 中间 region 指派给 B。返回 A、B。
func setupOrderLimitCluster(t *testing.T) (*db.DB, *db.DB) {
	t.Helper()
	dA, _ := openNode(t, "node-a")
	dB, addrB := openNode(t, "node-b")

	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable users: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: want 3, got %v err=%v", regions, err)
	}
	mid := regions[1]
	res := mustSQL(t, dA, fmt.Sprintf("ADD NODE '%s'", addrB))
	if res.Rows[0][0].S != dB.Cluster.SelfID {
		t.Fatalf("ADD NODE: want %q, got %q", dB.Cluster.SelfID, res.Rows[0][0].S)
	}
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", mid.RegionID, dB.Cluster.SelfID))
	return dA, dB
}

// TestM12OrderLimitCrossNodeTopN ORDER BY 升序 LIMIT：远端 region 参与窗口下推。
func TestM12OrderLimitCrossNodeTopN(t *testing.T) {
	dA, dB := setupOrderLimitCluster(t)
	defer dA.Close()
	defer dB.Close()

	wantIDs(t, dA, "SELECT id FROM users ORDER BY id LIMIT 3", 1, 2, 3)
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id LIMIT 5", 1, 2, 3, 4, 5)
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id LIMIT 10", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
}

// TestM12OrderLimitCrossNodeDesc 降序 top-N 跨节点。
func TestM12OrderLimitCrossNodeDesc(t *testing.T) {
	dA, dB := setupOrderLimitCluster(t)
	defer dA.Close()
	defer dB.Close()

	wantIDs(t, dA, "SELECT id FROM users ORDER BY id DESC LIMIT 3", 10, 9, 8)
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id DESC LIMIT 6", 10, 9, 8, 7, 6, 5)
}

// TestM12OrderLimitCrossNodeOffset ORDER BY + OFFSET：窗口取足量，协调端跳偏移。
func TestM12OrderLimitCrossNodeOffset(t *testing.T) {
	dA, dB := setupOrderLimitCluster(t)
	defer dA.Close()
	defer dB.Close()

	wantIDs(t, dA, "SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 2", 3, 4, 5)
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 7", 8, 9)
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 9", 10)
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id DESC LIMIT 2 OFFSET 1", 9, 8)
}

// TestM12OrderLimitCrossNodeNoOrderBy 无 ORDER BY 仅 LIMIT/OFFSET：取足量再截断。
func TestM12OrderLimitCrossNodeNoOrderBy(t *testing.T) {
	dA, dB := setupOrderLimitCluster(t)
	defer dA.Close()
	defer dB.Close()

	wantIDs(t, dA, "SELECT id FROM users LIMIT 4", 1, 2, 3, 4)
	wantIDs(t, dA, "SELECT id FROM users LIMIT 3 OFFSET 2", 3, 4, 5)
	wantIDs(t, dA, "SELECT id FROM users LIMIT 3 OFFSET 9", 10)
}

// TestM12OrderLimitCrossNodeText TEXT 列排序下推：u1 < u10 < u2 < ...
func TestM12OrderLimitCrossNodeText(t *testing.T) {
	dA, dB := setupOrderLimitCluster(t)
	defer dA.Close()
	defer dB.Close()

	wantIDs(t, dA, "SELECT id FROM users ORDER BY name LIMIT 3", 1, 10, 2)
	wantIDs(t, dA, "SELECT id FROM users ORDER BY name DESC LIMIT 3", 9, 8, 7)
}

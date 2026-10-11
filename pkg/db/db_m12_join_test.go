package db_test

// M12 分布式 JOIN 跨节点集成测试：
// 双节点集群，中间 region 指派给 B（驱动扫描与右表点查均跨节点转发），
// 验证 INNER / LEFT 等值 JOIN 路由合并结果正确。

import (
	"fmt"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// setupJoinCluster 在 A 建 users/orders 并写入，拆 3 region，中间 region 指派给 B。
// 返回 A、B、中间 region ID。
func setupJoinCluster(t *testing.T) (*db.DB, *db.DB, int64) {
	t.Helper()
	dA, _ := openNode(t, "node-a")
	dB, addrB := openNode(t, "node-b")

	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dA, "CREATE TABLE orders (id INT, uname TEXT, amount INT, PRIMARY KEY (id))")
	// B 建同结构表（第一簇无全局元数据同步，查询发起方须持有表定义）
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dB, "CREATE TABLE orders (id INT, uname TEXT, amount INT, PRIMARY KEY (id))")

	for i := 1; i <= 10; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	mustSQL(t, dA, "INSERT INTO orders (id, uname, amount) VALUES (1, 'u1', 100), (2, 'u2', 200), (5, 'u5', 500), (10, 'u10', 1000)")

	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable users: %v", err)
	}
	if err := dA.SQL.SplitTable("orders", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable orders: %v", err)
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
	return dA, dB, int64(mid.RegionID)
}

// TestM12JoinCrossNodeInner 跨节点 INNER JOIN：驱动全表扫描跨 region 合并，
// 右表按主键路由点查（命中远端 region 时经 B 转发），结果正确。
func TestM12JoinCrossNodeInner(t *testing.T) {
	dA, dB, _ := setupJoinCluster(t)
	defer dA.Close()
	defer dB.Close()

	wantIDs(t, dA, "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.id ORDER BY u.id", 1, 2, 5, 10)
	res := mustSQL(t, dA, "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.id ORDER BY u.id")
	for i, want := range []int64{100, 200, 500, 1000} {
		if res.Rows[i][1].I != want {
			t.Fatalf("row %d amount: want %d, got %+v", i, want, res.Rows[i][1])
		}
	}
}

// TestM12JoinCrossNodeLeft 跨节点 LEFT JOIN：未匹配左行补空，空分区/远端 region 均正确。
func TestM12JoinCrossNodeLeft(t *testing.T) {
	dA, dB, _ := setupJoinCluster(t)
	defer dA.Close()
	defer dB.Close()

	res := mustSQL(t, dA, "SELECT u.id, o.amount FROM users u LEFT JOIN orders o ON u.id = o.id ORDER BY u.id")
	if len(res.Rows) != 10 {
		t.Fatalf("LEFT JOIN: want 10 rows, got %d", len(res.Rows))
	}
	amountByID := map[int64]int64{1: 100, 2: 200, 5: 500, 10: 1000}
	for i := 0; i < 10; i++ {
		id := res.Rows[i][0].I
		if want, ok := amountByID[id]; ok {
			if res.Rows[i][1].I != want {
				t.Fatalf("row %d amount: want %d, got %+v", i, want, res.Rows[i][1])
			}
		} else if res.Rows[i][1].Kind != "TEXT" || res.Rows[i][1].S != "" {
			t.Fatalf("row %d amount: want empty, got %+v", i, res.Rows[i][1])
		}
	}
}

// TestM12JoinCrossNodeReverse 方向二写法（o.id = u.id）跨节点路由。
func TestM12JoinCrossNodeReverse(t *testing.T) {
	dA, dB, _ := setupJoinCluster(t)
	defer dA.Close()
	defer dB.Close()

	wantIDs(t, dA, "SELECT u.id FROM users u JOIN orders o ON o.id = u.id ORDER BY u.id", 1, 2, 5, 10)
}

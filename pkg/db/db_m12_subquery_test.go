package db_test

// M12-3 分布式子查询跨节点测试：两节点集群（中间 region 指派节点 B），验证
// IN / EXISTS / 标量 子查询在真实跨节点（ScanRemote 远端分片归并 + 主键路由点查）
// 场景下结果正确。复用 setupJoinCluster：users/orders 各 3 region，中间 region
// （id 4..7）指派 B 节点。

import (
	"testing"
)

// TestM12SubqueryCrossNodeIn IN 子查询跨节点归并：orders 中间分片在 B 节点，
// amount>100 命中 id 2(本地 A)、5(B 远端)、10(本地 A)，客户端归并后过滤。
func TestM12SubqueryCrossNodeIn(t *testing.T) {
	dA, dB, _ := setupJoinCluster(t)
	defer dA.Close()
	defer dB.Close()

	wantIDs(t, dA, "SELECT u.id FROM users u WHERE u.id IN (SELECT o.id FROM orders o WHERE o.amount > 100) ORDER BY u.id", 2, 5, 10)
	// 全量集合：orders 全表跨节点归并（含 B 的 id 5）。
	wantIDs(t, dA, "SELECT u.id FROM users u WHERE u.id IN (SELECT o.id FROM orders o) ORDER BY u.id", 1, 2, 5, 10)
}

// TestM12SubqueryCrossNodeExists EXISTS 跨节点判定：金额>=500 的订单 id 5 位于
// B 节点（远端分片归并判定），配合主查询条件返回 id 5..10。
func TestM12SubqueryCrossNodeExists(t *testing.T) {
	dA, dB, _ := setupJoinCluster(t)
	defer dA.Close()
	defer dB.Close()

	wantIDs(t, dA, "SELECT u.id FROM users u WHERE u.id >= 5 AND EXISTS (SELECT o.id FROM orders o WHERE o.amount >= 500) ORDER BY u.id", 5, 6, 7, 8, 9, 10)
	// 远端无满足行 → 空。
	wantIDs(t, dA, "SELECT u.id FROM users u WHERE EXISTS (SELECT o.id FROM orders o WHERE o.amount >= 99999) ORDER BY u.id")
}

// TestM12SubqueryCrossNodeScalar 标量子查询跨节点取数：o.amount=500 的订单
// id=5 位于 B 节点（远端分片），标量值跨节点取回后参与主查询比较。
func TestM12SubqueryCrossNodeScalar(t *testing.T) {
	dA, dB, _ := setupJoinCluster(t)
	defer dA.Close()
	defer dB.Close()

	wantIDs(t, dA, "SELECT u.id FROM users u WHERE u.id = (SELECT o.id FROM orders o WHERE o.amount = 500) ORDER BY u.id", 5)

	// 零行 → NULL 语义 → 空结果。
	wantIDs(t, dA, "SELECT u.id FROM users u WHERE u.id = (SELECT o.id FROM orders o WHERE o.amount = 99999) ORDER BY u.id")

	// 多行 → 报错。
	if _, err := dA.SQL.Execute("SELECT u.id FROM users u WHERE u.id = (SELECT o.id FROM orders o)"); err == nil {
		t.Fatal("scalar subquery with multiple rows: want error, got nil")
	}
}

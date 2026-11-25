package db_test

// M7 Region 自动分裂与负载均衡：集群侧测试。
// 覆盖：手动 BALANCE 跨节点迁移过载 region（路由切换 + 数据一致）、
// 自动均衡周期触发迁移、迁移后读写可用与 REGION_PUSH 装载验证。

import (
	"fmt"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// regionCountOnNode 从路由视图统计归属 nodeID 的 region 数。
func regionCountOnNode(t *testing.T, d *db.DB, nodeID string) int {
	t.Helper()
	routes := mustSQL(t, d, "SHOW REGION ROUTES")
	cnt := 0
	for _, row := range routes.Rows {
		if row[5].S == nodeID {
			cnt++
		}
	}
	return cnt
}

// waitRegionCount 轮询直到路由视图中 nodeID 的 region 数达到 want。
func waitRegionCount(t *testing.T, d *db.DB, nodeID string, want int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if regionCountOnNode(t, d, nodeID) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("region count on %s: want %d, got %d", nodeID, want, regionCountOnNode(t, d, nodeID))
}

// m7ThreePlusOneTopology 搭 4 region 拓扑：A 本地 3 个、B 本地 1 个。
// 返回 A、B 与中间被指派给 B 的 region ID。
func m7ThreePlusOneTopology(t *testing.T) (dA *db.DB, dB *db.DB, assigned storage.ID) {
	t.Helper()
	dA, _ = openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, _ = openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })

	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 12; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(3), splitBoundary(6), splitBoundary(9)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 4 {
		t.Fatalf("ListRegions: %v err=%v", regions, err)
	}
	assigned = regions[1].RegionID // [3,6) 指派给 B
	mustSQL(t, dA, "ADD NODE '"+dB.Cluster.SelfAddr+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", assigned, dB.Cluster.SelfID))
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
	return dA, dB, assigned
}

// TestM7ManualBalanceMigratesHotRegion 手动 BALANCE：A 本地 3 region > B 本地 1 region，
// 最大 region [9,nil)（4 行）在线迁移到 B；迁移后路由切换、A/B 读写一致。
func TestM7ManualBalanceMigratesHotRegion(t *testing.T) {
	dA, dB, _ := m7ThreePlusOneTopology(t)
	if got := regionCountOnNode(t, dA, dA.Cluster.SelfID); got != 3 {
		t.Fatalf("A local regions: want 3, got %d", got)
	}
	if got := regionCountOnNode(t, dA, dB.Cluster.SelfID); got != 1 {
		t.Fatalf("B regions: want 1, got %d", got)
	}

	res := mustSQL(t, dA, "BALANCE")
	if res.AffectedRows != 1 {
		t.Fatalf("BALANCE: want 1 migration, got %d", res.AffectedRows)
	}
	// 路由切换：A 本地 2、B 2
	waitRegionCount(t, dA, dB.Cluster.SelfID, 2)
	if got := regionCountOnNode(t, dA, dA.Cluster.SelfID); got != 2 {
		t.Fatalf("A after balance: want 2 local, got %d", got)
	}
	// 迁移后 A/B 两侧读写一致（迁移 region 为 [9,nil)，id 9..12 经转发）
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
	// 点查迁移 region 内 id（A 上命中远端）
	res = mustSQL(t, dA, "SELECT name FROM users WHERE id = 10")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u10" {
		t.Fatalf("point read migrated region: %+v", res.Rows)
	}
	// 迁移后写：id=13 落入迁移 region [9,nil)，走 2PC 远端写
	mustSQL(t, dA, "INSERT INTO users (id, name) VALUES (13, 'u13')")
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
	// 迁移目标节点 B 本地可服务迁移 region 的点查
	res = mustSQL(t, dB, "SELECT name FROM users WHERE id = 11")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u11" {
		t.Fatalf("B local read migrated region: %+v", res.Rows)
	}
	// 已均衡：再 BALANCE 无迁移
	res = mustSQL(t, dA, "BALANCE")
	if res.AffectedRows != 0 {
		t.Fatalf("second BALANCE: want 0 migration, got %d", res.AffectedRows)
	}
}

// TestM7AutoBalanceTriggersMigration 自动均衡：开启短周期后无需手动触发，
// 过载 region 被自动迁移到空闲节点。
func TestM7AutoBalanceTriggersMigration(t *testing.T) {
	dA, dB, _ := m7ThreePlusOneTopology(t)
	dA.SQL.SetAutoBalance(true, 150*time.Millisecond)
	defer dA.SQL.StopAutoBalance()

	waitRegionCount(t, dA, dB.Cluster.SelfID, 2)
	if got := regionCountOnNode(t, dA, dA.Cluster.SelfID); got != 2 {
		t.Fatalf("A after auto balance: want 2 local, got %d", got)
	}
	// 自动迁移后读写一致
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
}

// TestM7BalanceSkipsDownNode 节点 down 的 region 不计入迁移目标：
// B down 后 BALANCE 不迁移（无其他存活候选），数据仍完整（走 M6 failover 语义）。
func TestM7BalanceSkipsDownNode(t *testing.T) {
	dA, dB, _ := m7ThreePlusOneTopology(t)
	speedupHeartbeat(t, dA)
	waitEverUp(t, dA, dB.Cluster.SelfID)
	if err := dB.Close(); err != nil {
		t.Fatalf("B close: %v", err)
	}
	waitNodeState(t, dA, dB.Cluster.SelfID, cluster.StateDown)
	// down 节点不作为迁移目标：无候选，BALANCE 返回 0
	res := mustSQL(t, dA, "BALANCE")
	if res.AffectedRows != 0 {
		t.Fatalf("BALANCE with down node: want 0, got %d", res.AffectedRows)
	}
	// M6 failover 已把原 B region 重指派回 A（或其余存活节点），数据完整
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
}

// TestM7BalanceSkipsRemoteImplicitRegion BALANCE 的本地负载统计不得把已归属远端的
// 隐式单 region 计入：该 region 路由登记为远端后仍会被 localRegionLoad 的隐式段
// 遍历到，旧实现无归属过滤，迁移时以 "region not local" 使整轮 BALANCE 失败
// （v2.0.6 真机回归暴露的既有缺陷）。
func TestM7BalanceSkipsRemoteImplicitRegion(t *testing.T) {
	dA, dB, _ := m7ThreePlusOneTopology(t)
	// 未分片表 -> 隐式单 region，12 行高于任一显式 region（3 行），
	// 旧实现下必然被选为迁移首选。
	mustSQL(t, dA, "CREATE TABLE audit_log (id INT, msg TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 12; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO audit_log (id, msg) VALUES (%d, 'm%d')", i, i))
	}
	regions, err := dA.SQL.ListRegions("audit_log")
	if err != nil || len(regions) != 1 || regions[0].Explicit {
		t.Fatalf("ListRegions(audit_log): want 1 implicit region, got %+v err=%v", regions, err)
	}
	implicitID := regions[0].RegionID
	// 隐式 region 指派给 B：路由归属远端，但表元数据仍未显式分片
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", implicitID, dB.Cluster.SelfID))
	wantIDs(t, dA, "SELECT id FROM audit_log ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

	// 旧实现此处返回 region not local；修复后正常完成（本地显式 region 不再被隐式段污染）
	if res := mustSQL(t, dA, "BALANCE"); res == nil {
		t.Fatalf("BALANCE: nil result")
	}
	// 隐式 region 归属保持 B，A 侧读转发一致
	routes := mustSQL(t, dA, "SHOW REGION ROUTES")
	owner := ""
	for _, row := range routes.Rows {
		if row[0].I == int64(implicitID) {
			owner = row[5].S
		}
	}
	if owner != dB.Cluster.SelfID {
		t.Fatalf("implicit region owner: want %s, got %q", dB.Cluster.SelfID, owner)
	}
	wantIDs(t, dA, "SELECT id FROM audit_log ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
}

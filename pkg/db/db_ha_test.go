package db_test

// M6 高可用与故障转移：集群侧故障场景测试（读 failover / region 自动重指派）。
// 复用 M4/M5 基建（openNode/mustSQL/wantIDs/splitBoundary，见 db_cluster_test.go）。

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// waitNodeState 轮询直到节点进入目标状态（心跳降速场景用）。
func waitNodeState(t *testing.T, d *db.DB, id string, want cluster.NodeState) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		n, ok := d.Cluster.GetNode(id)
		if ok && n.State == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	n, _ := d.Cluster.GetNode(id)
	t.Fatalf("node %s state = %v, want %v", id, n.State, want)
}

// waitRegionNode 轮询直到 region 归属节点变为 want（心跳回调异步重指派）。
func waitRegionNode(t *testing.T, d *db.DB, rid storage.ID, want string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		rg, ok := d.SQL.ExecutorRouter().FindRegion(rid)
		if ok && rg.Node == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	rg, _ := d.SQL.ExecutorRouter().FindRegion(rid)
	t.Fatalf("region %d node = %q, want %q", rid, rg.Node, want)
}

// speedupHeartbeat 重启心跳为快速间隔，加速故障感知（默认 2s/6s 太慢）。
func speedupHeartbeat(t *testing.T, d *db.DB) {
	t.Helper()
	d.Cluster.Stop()
	d.Cluster.StartHeartbeat(150*time.Millisecond, 450*time.Millisecond)
}

// waitEverUp 轮询直到本进程曾成功收到目标节点 PONG（启动竞态防护判定）。
// 心跳循环对从未收到过 PONG 的节点不做离线判定（EverUp 防护，避免集群
// 滚动启动误判 DOWN），因此测试关闭节点前必须先确认其曾在线，否则将
// 永远无法标记 DOWN（与 pkg/cluster TestHeartbeatDown 同类竞态）。
func waitEverUp(t *testing.T, d *db.DB, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if d.Cluster.EverUp(id) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %s never confirmed up before close", id)
}

// TestM6DownNodeReadFailover 节点 down 后读 failover：持有中间 region 的 B 下线，
// A 自动重指派回本节点并继续返回全量数据，不向客户端报错。
func TestM6DownNodeReadFailover(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, _ := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })

	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: %v err=%v", regions, err)
	}
	mid := regions[1]
	mustSQL(t, dA, "ADD NODE '"+dB.Cluster.SelfAddr+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", mid.RegionID, dB.Cluster.SelfID))
	// 基线：B 持有中间 region [4,8)，两端全量一致
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	speedupHeartbeat(t, dA)

	// 关闭 B（模拟节点 down）→ 心跳感知 DOWN
	if err := dB.Close(); err != nil {
		t.Fatalf("B close: %v", err)
	}
	waitNodeState(t, dA, dB.Cluster.SelfID, cluster.StateDown)
	// 自动重指派：原 B 的 region 回到本节点（无其他存活远端）
	waitRegionNode(t, dA, mid.RegionID, dA.Cluster.SelfID)

	// 读 failover：A 上 SELECT 全量仍完整
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
}

// TestM6AutoReassignRegionAfterDown 三节点 region 自动重指派：B down 后其 region
// 重指派给存活远端 C（数据经 REGION_PUSH 从最全副本 A 推送），C 可本地服务该 region。
func TestM6AutoReassignRegionAfterDown(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, _ := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })
	dC, _ := openNode(t, "node-c")
	t.Cleanup(func() { _ = dC.Close() })

	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dC, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: %v err=%v", regions, err)
	}
	mid := regions[1]
	mustSQL(t, dA, "ADD NODE '"+dB.Cluster.SelfAddr+"'")
	mustSQL(t, dA, "ADD NODE '"+dC.Cluster.SelfAddr+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", mid.RegionID, dB.Cluster.SelfID))
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	speedupHeartbeat(t, dA)
	waitEverUp(t, dA, dB.Cluster.SelfID)

	// B down → 自动重指派给存活远端 C
	if err := dB.Close(); err != nil {
		t.Fatalf("B close: %v", err)
	}
	waitNodeState(t, dA, dB.Cluster.SelfID, cluster.StateDown)
	waitRegionNode(t, dA, mid.RegionID, dC.Cluster.SelfID)

	// 数据已推送到 C：C 本地点查命中中间 region 的行
	res := mustSQL(t, dC, "SELECT id FROM users WHERE id = 6")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 6 {
		t.Fatalf("C point id=6 after reassign: %+v", res.Rows)
	}
	// A 读路径经 C 转发仍全量一致
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
}

// TestP0QuorumPromoteSucceeds P0 升主多数派仲裁成功：3 节点集群全部在线时，
// follower 经 quorum 确认（N/2+1=2 可达）后升主成功（ha_quorum_enabled 开启）。
func TestP0QuorumPromoteSucceeds(t *testing.T) {
	dA, addrA := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, _ := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })
	dC, addrC := openNode(t, "node-c")
	t.Cleanup(func() { _ = dC.Close() })

	// 互联注册表：B 侧需看到 A、C（quorum 统计全部注册节点）。
	mustSQL(t, dB, "ADD NODE '"+addrA+"'")
	mustSQL(t, dB, "ADD NODE '"+addrC+"'")
	mustSQL(t, dA, "ADD NODE '"+addrC+"'")

	// B 是 A 的复制 follower（M6 场景：旧主 A 曾在线）。
	if err := dA.StartReplication(":0"); err != nil {
		t.Fatalf("A StartReplication: %v", err)
	}
	raddr, _ := dA.ReplicationStatus()
	if err := dB.StartFollower(raddr); err != nil {
		t.Fatalf("B StartFollower: %v", err)
	}
	waitEverUp(t, dB, dA.Cluster.SelfID)
	waitEverUp(t, dB, dC.Cluster.SelfID)

	// 开启 quorum 后升主：3 节点全部可达（quorum=2）→ 成功。
	dB.SetQuorumEnabled(true)
	if err := dB.PromoteToMaster(); err != nil {
		t.Fatalf("promote with full quorum: %v", err)
	}
	if dB.Replicator == nil {
		t.Fatalf("promoted B has no replicator")
	}
}

// TestP0QuorumPromoteInsufficient P0 升主仲裁不足：多数派不可达时升主被拒
// （ErrQuorumNotMet），节点保持 follower 不升主（防分区内多 follower 双主）。
func TestP0QuorumPromoteInsufficient(t *testing.T) {
	dA, addrA := openNode(t, "node-a")
	dB, _ := openNode(t, "node-b")
	t.Cleanup(func() { _ = dA.Close() })
	t.Cleanup(func() { _ = dB.Close() })
	dC, addrC := openNode(t, "node-c")
	t.Cleanup(func() { _ = dC.Close() })

	// 互联注册表：B 侧看到 A、C。
	mustSQL(t, dB, "ADD NODE '"+addrA+"'")
	mustSQL(t, dB, "ADD NODE '"+addrC+"'")
	mustSQL(t, dA, "ADD NODE '"+addrC+"'")

	// B 是 A 的复制 follower。
	if err := dA.StartReplication(":0"); err != nil {
		t.Fatalf("A StartReplication: %v", err)
	}
	raddr, _ := dA.ReplicationStatus()
	if err := dB.StartFollower(raddr); err != nil {
		t.Fatalf("B StartFollower: %v", err)
	}
	waitEverUp(t, dB, dA.Cluster.SelfID)
	waitEverUp(t, dB, dC.Cluster.SelfID)

	// 关闭 A、C：B 侧仅自身可达（1/3 < quorum=2）→ 拒绝升主。
	if err := dA.Close(); err != nil {
		t.Fatalf("A close: %v", err)
	}
	if err := dC.Close(); err != nil {
		t.Fatalf("C close: %v", err)
	}
	dB.SetQuorumEnabled(true)
	if err := dB.PromoteToMaster(); !errors.Is(err, db.ErrQuorumNotMet) {
		t.Fatalf("promote without quorum = %v, want ErrQuorumNotMet", err)
	}
	// 仍保持 follower（未升主）。
	if dB.Follower == nil {
		t.Fatalf("B lost follower after rejected promote")
	}
}

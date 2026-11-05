package db_test

// D4.4 未闭合根因回归测试（A+B 同簇修复）：
//   GAP A：重启/加入节点 region 归属视图未从集群权威源刷新，误判 local=true
//          形成孤立读视图（r3 不回迁、B 重启后视图与 A/C 不一致）。
//   GAP C：重启节点把他人 region 判为本地，写请求静默落本地而不报错，
//          导致其他节点不可见（写分裂）。
//
// 覆盖点：
//   1. join 路由同步（ADD NODE / 节点 UP）按副本语义声明权威归属，
//      新节点不会把他人 region 误判为本地；
//   2. 重启节点主动从权威源刷新归属视图，覆盖本地 m:regions 陈旧 owner；
//   3. 非 owner 写入被明确拒绝（不静默写本地、不做 2PC 写转发）。

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// hostPort 分离 host 与 port。
func hostPort(t *testing.T, addr string) (string, string) {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split %q: %v", addr, err)
	}
	return host, port
}

// waitRoute 轮询直到本节点路由视图中 rid 的归属与本地标记达到期望。
func waitRoute(t *testing.T, d *db.DB, rid storage.ID, wantNode string, wantLocal bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last sharding.RegionInfo
	var ok bool
	for time.Now().Before(deadline) {
		last, ok = d.SQL.ExecutorRouter().FindRegion(rid)
		if ok && nodeMatches(last.Node, wantNode, wantLocal) && last.Local == wantLocal {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("region %d route = {node:%q local:%v ok:%v}, want {node:%q local:%v}",
		rid, last.Node, last.Local, ok, wantNode, wantLocal)
}

// nodeMatches 归一归属断言：本地归属既可能是本节点 ID，也可能是空串
// （"未指派即本机"既有语义），两者等价；远端归属必须精确等于目标节点。
func nodeMatches(got, want string, wantLocal bool) bool {
	if wantLocal {
		return got == "" || got == want
	}
	return got == want
}

// seedThreeRegions A 侧建表写入 10 行并切分为 3 个 region。
func seedThreeRegions(t *testing.T, dA *db.DB) []sharding.RegionInfo {
	t.Helper()
	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: %v err=%v", regions, err)
	}
	return regions
}

// TestD44JoinSyncKeepsRemoteOwnership（疑点 2 回归）：
// 新节点经 join 路由同步学到的归属必须是"他人 region 非本地"，而不是把
// 全部 region 判为自己持有；被指派后才本地，且非 owner 区间的写入必须被拒。
// 修复前：applyPush 无条件把接收 region 置 Local=true，B 加入后 3 个 region
// 全部判为本地，B 对 id=1 的写入静默落本地（A 不可见）。
func TestD44JoinSyncKeepsRemoteOwnership(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, addrB := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })

	regions := seedThreeRegions(t, dA)
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")

	// join 同步后：B 的 3 个 region 全部归属 A 且非本地（修复前为本地）
	for _, rg := range regions {
		waitRoute(t, dB, rg.RegionID, dA.Cluster.SelfID, false)
	}

	// 非 owner 写入：B 上写 r3 区间（id=12，归属 A）绝不允许静默落 B 本地。
	// 两种合法结果：①被明确拒绝（write to remote region）；②被转发到 owner 并
	// 在权威节点可见。唯一不允许的是"返回成功但只在 B 本地可见"（GAP C 现象）。
	if _, err := dB.SQL.Execute("INSERT INTO users (id, name, age) VALUES (12, 'evil', 99)"); err != nil {
		t.Logf("非 owner 写被拒绝（B 修复主路径）：%v", err)
		if !strings.Contains(err.Error(), "write to remote region") &&
			!strings.Contains(err.Error(), "remote region") {
			t.Fatalf("unexpected error: %v", err)
		}
	} else {
		t.Logf("非 owner 写经 2PC 转发至权威 owner，全局可见")
		wantIDs(t, dA, "SELECT id FROM users WHERE id = 12", 12)
	}

	// 指派尾 region 给 B：B 本地化，其余 region 仍为远端归属
	mid := regions[2]
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", mid.RegionID, dB.Cluster.SelfID))
	waitRoute(t, dB, mid.RegionID, dB.Cluster.SelfID, true)
	for _, rg := range regions {
		if rg.RegionID == mid.RegionID {
			continue
		}
		waitRoute(t, dB, rg.RegionID, dA.Cluster.SelfID, false)
	}

	// B 对本地 region 的写入成功，且 A 侧可见（无写分裂）
	mustSQL(t, dB, "INSERT INTO users (id, name, age) VALUES (11, 'b11', 25)")
	wantIDs(t, dA, "SELECT id FROM users WHERE id = 11", 11)
	// A 侧非本地区间读仍完整（跨节点转发不因归属变更断链）
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
}

// TestD44RestartRefreshesAuthoritativeRoutes（疑点 1/3 回归）：
// 节点重启后必须从集群权威源刷新 region 归属视图，覆盖本地 m:regions 陈旧
// owner，避免形成孤立读视图；陈旧本地归属下的写入必须被拒绝（B 修复）。
// 修复前：A 侧 failover 已把 r2 收回，B 重启后仍按本地残留视图认定 r2 本地，
// 写 id=5 静默落 B 本地，A 读不到（D4.4 GAP C 现象）。
func TestD44RestartRefreshesAuthoritativeRoutes(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })

	dirB := t.TempDir()
	if err := db.Init(dirB); err != nil {
		t.Fatalf("init B: %v", err)
	}
	dB := openNodeAt(t, dirB, "node-b", "127.0.0.1:0")
	addrB := dB.Cluster.SelfAddr
	hostB, portB := hostPort(t, addrB)
	bID := dB.Cluster.SelfID

	regions := seedThreeRegions(t, dA)
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	mid := regions[2]
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", mid.RegionID, bID))
	waitRoute(t, dB, mid.RegionID, bID, true)

	// 关闭 B → A 心跳感知 DOWN → r2 重指派回 A（权威视图：r2 归 A）
	speedupHeartbeat(t, dA)
	waitEverUp(t, dA, bID)
	if err := dB.Close(); err != nil {
		t.Fatalf("B close: %v", err)
	}
	waitNodeState(t, dA, bID, cluster.StateDown)
	waitRegionNode(t, dA, mid.RegionID, dA.Cluster.SelfID)

	// 重启 B（同一数据目录 + 同一端口，本地 m:regions 保留旧视图 r2 归自己）
	dB2 := openNodeAt(t, dirB, "node-b", net.JoinHostPort(hostB, portB))
	t.Cleanup(func() { _ = dB2.Close() })
	if dB2.Cluster.SelfID != bID {
		t.Fatalf("restarted node id = %q, want %q", dB2.Cluster.SelfID, bID)
	}

	// 重启后视图必须从集群权威源刷新：B 侧 r2 归属必须收敛到 A 的权威归属
	//（两者不一致即"孤立读视图"，正是 GAP A/C 的直接成因）。
	waitNodeState(t, dA, bID, cluster.StateUp)
	waitOwnerAgree(t, dA, dB2, mid.RegionID)

	rgA, _ := dA.SQL.ExecutorRouter().FindRegion(mid.RegionID)
	if rgA.Node == "" || rgA.Node == bID {
		// 权威归属已随节点恢复回迁给 B：B 本地可写，且写入全集群可见（GAP A/C 闭合）
		waitRoute(t, dB2, mid.RegionID, bID, true)
		mustSQL(t, dB2, "INSERT INTO users (id, name, age) VALUES (42, 'back', 55)")
		wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42)
		return
	}

	// 权威归属仍在 A（尚未回迁）：B 的陈旧本地归属必须被权威视图覆盖，
	// 且该区间写入必须被明确拒绝，绝不静默写本地
	waitRoute(t, dB2, mid.RegionID, rgA.Node, false)
	if _, err := dB2.SQL.Execute("INSERT INTO users (id, name, age) VALUES (42, 'ghost', 55)"); err == nil {
		t.Fatalf("write to region owned by %s must be rejected after restart", rgA.Node)
	} else if !strings.Contains(err.Error(), "remote region") {
		t.Fatalf("unexpected error: %v", err)
	}
	// A 侧仍持有完整数据，重启节点未产生分叉写
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
}

// waitOwnerAgree 轮询直到两台节点对该 region 的归属收敛一致（归属指权威 owner，
// 空串与本地标记等价归一），用于断言重启节点不再保留孤立读视图。
func waitOwnerAgree(t *testing.T, d1, d2 *db.DB, rid storage.ID) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var a, b sharding.RegionInfo
	for time.Now().Before(deadline) {
		ra, okA := d1.SQL.ExecutorRouter().FindRegion(rid)
		rb, okB := d2.SQL.ExecutorRouter().FindRegion(rid)
		if okA && okB {
			a, b = ra, rb
			normA, normB := ra.Node, rb.Node
			if normA == "" {
				normA = d1.Cluster.SelfID
			}
			if normB == "" {
				normB = d2.Cluster.SelfID
			}
			if normA == normB {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("region %d ownership not converged: %q(local=%v) vs %q(local=%v)",
		rid, a.Node, a.Local, b.Node, b.Local)
}

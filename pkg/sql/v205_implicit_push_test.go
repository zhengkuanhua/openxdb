package sql

// v2.0.5 建表后补隐式整表 region 归属（首写零失败）专项用例。
//
// 验收项：
//   V205-1 建未切分表后本机立即学到隐式归属（纯转发节点不再"首写必失败"）；
//   V205-2 归属节点（ID 字典序最小）学到"归本机"，写入不走转发；
//   V205-3 建表成功路径确实触发一次对端推送（钩子注入，不发起真实 RPC）；
//   V205-4 单机（无远端节点）零回归：不学习、不推送、写入照常成功；
//   V205-5 已切分表不被隐式归属覆盖（学习只登记未切分表）；
//   V205-6 隐式归属绝不进入显式 region 列表（SHOW REGION ROUTES 零污染）。

import (
	"sync/atomic"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// v205CreateUnsplit 走真实建表路径（execCreateTable）创建未切分表，返回表 ID。
func v205CreateUnsplit(t *testing.T, e *Executor, name string) storage.ID {
	t.Helper()
	stmt := &CreateTableStmt{
		Name:    name,
		Columns: []ColumnDef{{Name: "id", Type: "INT"}, {Name: "v", Type: "TEXT"}},
		PK:      "id",
	}
	if _, err := e.execCreateTable(e.globalSess, stmt); err != nil {
		t.Fatalf("CREATE TABLE %s: %v", name, err)
	}
	tx, err := e.tm.Begin()
	if err != nil {
		t.Fatalf("load tables tx: %v", err)
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		t.Fatalf("load tables: %v", err)
	}
	for _, tb := range tabs {
		if tb.Name == name {
			return storage.ID(tb.ID)
		}
	}
	t.Fatalf("table %s not found in catalog", name)
	return 0
}

// TestV205CreateTableLearnsImplicitRoute 建表后本机立即学到隐式归属。
func TestV205CreateTableLearnsImplicitRoute(t *testing.T) {
	for _, tc := range []struct {
		self        string
		forwardOnly bool
	}{
		{"node-c", true},  // 纯转发节点（真机 203 场景）
		{"node-a", false}, // 归属节点（ID 字典序最小者）
	} {
		e := v204Env(t, tc.self, tc.forwardOnly)
		e.implicitPushHook = func() {}
		rid := v205CreateUnsplit(t, e, "v205_t1")
		imp := sharding.ImplicitRegionID(rid)
		if got := e.router.RegionNode(imp); got != "node-a" {
			t.Fatalf("self=%s implicit route node = %q, want node-a（首写前必须已知归属）", tc.self, got)
		}
		node, kind := e.routeDecision(imp)
		wantKind := routeRemoteOwned
		if tc.self == "node-a" {
			wantKind = routeLocalOwned
		}
		if kind != wantKind || node != "node-a" {
			t.Fatalf("self=%s route decision = (%q,%v), want (node-a,%v)", tc.self, node, kind, wantKind)
		}
		if v204ImplicitInExplicit(e, imp) {
			t.Fatalf("self=%s implicit region must NOT enter explicit region list", tc.self)
		}
	}
}

// TestV205OwnerNodeLearnsImplicitLocal 归属节点学到"归本机"，写入不走转发。
func TestV205OwnerNodeLearnsImplicitLocal(t *testing.T) {
	e := v204Env(t, "node-a", false)
	e.implicitPushHook = func() {}
	rid := v205CreateUnsplit(t, e, "v205_t2")
	imp := sharding.ImplicitRegionID(rid)
	if got := e.router.RegionNode(imp); got != "node-a" {
		t.Fatalf("owner implicit route node = %q, want node-a", got)
	}
	if !e.regionLocal(imp) {
		t.Fatalf("owner must treat implicit region %d as local", imp)
	}
}

// TestV205NotifyPushesPeers 建表成功路径触发对端推送（每次建表一次）。
func TestV205NotifyPushesPeers(t *testing.T) {
	e := v204Env(t, "node-c", true)
	var calls int64
	e.implicitPushHook = func() { atomic.AddInt64(&calls, 1) }
	for _, name := range []string{"v205_p1", "v205_p2"} {
		v205CreateUnsplit(t, e, name)
	}
	if n := atomic.LoadInt64(&calls); n != 2 {
		t.Fatalf("peer push hook calls = %d, want 2（每次建表一次）", n)
	}
}

// TestV205SingleNodeZeroRegression 单机：不学习、不推送、写入语义不变。
func TestV205SingleNodeZeroRegression(t *testing.T) {
	e, _ := newWriteRouteEnv(t, "node-solo")
	var calls int64
	e.implicitPushHook = func() { atomic.AddInt64(&calls, 1) }
	rid := v205CreateUnsplit(t, e, "v205_solo")
	if n := atomic.LoadInt64(&calls); n != 0 {
		t.Fatalf("single node must not push to peers, hook calls = %d", n)
	}
	if routes := e.implicitRoutesForViewSync(); len(routes) != 0 {
		t.Fatalf("single node must not derive implicit routes, got %+v", routes)
	}
	// 单机：隐式 region 未登记 = 全部归本机（旧语义不变）。
	if got := e.router.RegionNode(rid); got != "" {
		t.Fatalf("single node must keep implicit region unrouted, got %q", got)
	}
}

// TestV205SplitTableNotOverridden 已切分表：隐式学习不覆盖显式分片视图。
func TestV205SplitTableNotOverridden(t *testing.T) {
	e := v204Env(t, "node-c", true)
	e.implicitPushHook = func() {}
	rid := v205CreateUnsplit(t, e, "v205_split")
	// 模拟后续切分：本地出现显式 region 条目。
	if err := e.router.UpsertRegion(sharding.RegionInfo{
		RegionID: storage.ID(9501), TableID: rid, State: sharding.StateActive,
		Explicit: true, Node: "node-b",
	}); err != nil {
		t.Fatalf("upsert explicit region: %v", err)
	}
	e.notifyImplicitRouteView(uint64(rid))
	if got := e.router.RegionNode(storage.ID(9501)); got != "node-b" {
		t.Fatalf("explicit region node = %q, want node-b（隐式学习不得覆盖）", got)
	}
	if routes := e.implicitRoutesForViewSync(); len(routes) != 0 {
		t.Fatalf("split table must not be carried as implicit, got %+v", routes)
	}
}

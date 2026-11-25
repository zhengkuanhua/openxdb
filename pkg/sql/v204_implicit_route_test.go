package sql

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// v2.0.4 隐式 region 路由广播用例（视图同步派生口径）：
//   ① 隐式整表 region 归属不在 DDL 路径登记，由权威路由视图同步通道派生下发；
//   ② 纯转发节点学习后按归属转发（不再把隐式 region 当本机归属本地写）；
//   ③ 学习幂等，已切分表绝不被隐式条目覆盖，删表清理；
//   ④ 隐式归属绝不进入显式 region 列表（SHOW REGION ROUTES / 副本维护零回归）；
//   ⑤ 单机/无远端节点不下发载荷，旧语义零回归。

// v204Env 构造"三节点视图全可达"的执行器（self 为被测节点，其余为对端）。
func v204Env(t *testing.T, self string, forwardOnly bool) *Executor {
	peers := []string{"node-a", "node-b", "node-c"}
	e, _ := newWriteRouteEnv(t, self)
	m := newWriteRouteCluster(self, peers...)
	m.SetProber(func(string) bool { return true })
	m.RefreshPartitionSnapshot()
	e.mgr = m
	e.SetWriteFence(&v203Fence{forwardOnly: forwardOnly})
	e.SetWriteRouteEnforce(true)
	return e
}

// v204SeedTable 写入表目录（未切分表；saveTables/loadTables 与生产路径同源）。
func v204SeedTable(t *testing.T, e *Executor, id uint64, name string) {
	t.Helper()
	tx, err := e.tm.Begin()
	if err != nil {
		t.Fatalf("seed table tx: %v", err)
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		t.Fatalf("seed table load: %v", err)
	}
	tabs = append(tabs, &TableMeta{ID: id, Name: name, PK: "id"})
	raw, err := saveTables(tabs)
	if err != nil {
		t.Fatalf("seed table save: %v", err)
	}
	if err := tx.Put(metaKey, raw); err != nil {
		t.Fatalf("seed table put: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("seed table commit: %v", err)
	}
}

// v204ImplicitInExplicit 隐式 region ID 是否出现在显式 region 列表（出现即为污染）。
func v204ImplicitInExplicit(e *Executor, rid storage.ID) bool {
	for _, rg := range e.router.AllRegions() {
		if rg.RegionID == rid {
			return true
		}
	}
	return false
}

// v204FindImplicit 在载荷里找指定表的隐式条目。
func v204FindImplicit(routes []sharding.RegionInfo, tableID uint64) (sharding.RegionInfo, bool) {
	for _, rg := range routes {
		if rg.TableID == storage.ID(tableID) && sharding.IsImplicitRegionID(rg.RegionID) {
			return rg, true
		}
	}
	return sharding.RegionInfo{}, false
}

// TestV204ImplicitOwnerRule 归属规则：注册表内节点 ID 字典序最小者（各节点同一结论）。
func TestV204ImplicitOwnerRule(t *testing.T) {
	for _, self := range []string{"node-a", "node-b", "node-c"} {
		e := v204Env(t, self, false)
		if got := e.implicitRouteOwner(); got != "node-a" {
			t.Fatalf("self=%s implicit owner = %q, want node-a", self, got)
		}
	}
	// 单机（无远端节点）：归属本机，旧语义零回归。
	solo, _ := newWriteRouteEnv(t, "node-solo")
	if got := solo.implicitRouteOwner(); got != "node-solo" {
		t.Fatalf("single node implicit owner = %q, want node-solo", got)
	}
}

// TestV204ViewSyncDerivesUnsplitRoutes 视图同步载荷：未切分表派生隐式归属，已切分表不下发。
func TestV204ViewSyncDerivesUnsplitRoutes(t *testing.T) {
	e := v204Env(t, "node-c", false)
	v204SeedTable(t, e, 7101, "unsplit")
	v204SeedTable(t, e, 7102, "split")
	// 7102 已切分：本地有显式 region 条目。
	if err := e.router.UpsertRegion(sharding.RegionInfo{
		RegionID: storage.ID(9101), TableID: storage.ID(7102), State: sharding.StateActive,
		Explicit: true, Node: "node-b",
	}); err != nil {
		t.Fatalf("seed explicit region: %v", err)
	}
	routes := e.implicitRoutesForViewSync()
	rg, ok := v204FindImplicit(routes, 7101)
	if !ok {
		t.Fatalf("view sync must carry implicit route for unsplit table, got %+v", routes)
	}
	if rg.Node != "node-a" || rg.Local {
		t.Fatalf("unsplit route node/local = %q/%v, want node-a/false", rg.Node, rg.Local)
	}
	want := sharding.ImplicitRegionID(storage.ID(7101))
	if rg.RegionID != want {
		t.Fatalf("route id = %d, want implicit id %d", rg.RegionID, want)
	}
	if _, ok := v204FindImplicit(routes, 7102); ok {
		t.Fatalf("split table must NOT be carried in view sync payload: %+v", routes)
	}
	if v204ImplicitInExplicit(e, want) {
		t.Fatalf("implicit region %d must NOT be mixed into explicit region list", want)
	}
}

// TestV204SingleNodeNoViewSyncPayload 单机：无远端节点不下发载荷，写路径保持旧语义。
func TestV204SingleNodeNoViewSyncPayload(t *testing.T) {
	e, _ := newWriteRouteEnv(t, "node-solo")
	v204SeedTable(t, e, 7103, "solo")
	if routes := e.implicitRoutesForViewSync(); len(routes) != 0 {
		t.Fatalf("single node must not carry implicit routes, got %+v", routes)
	}
	rid := sharding.ImplicitRegionID(storage.ID(7103))
	if _, kind := e.routeDecision(rid); kind != routeLocalOwned {
		t.Fatalf("single node implicit region decision = %v, want legacy local", kind)
	}
	if err := e.LearnImplicitRoutesFromPeers(nil); err != nil {
		t.Fatalf("learn nil must be a no-op: %v", err)
	}
}

// TestV204ForwardOnlyLearnsFromViewSync 纯转发节点经权威视图同步学到归属并转发。
func TestV204ForwardOnlyLearnsFromViewSync(t *testing.T) {
	src := v204Env(t, "node-a", false)
	v204SeedTable(t, src, 7201, "unsplit")
	routes := src.implicitRoutesForViewSync()
	if _, ok := v204FindImplicit(routes, 7201); !ok {
		t.Fatalf("source must carry implicit route, got %+v", routes)
	}

	dst := v204Env(t, "node-c", true)
	v204SeedTable(t, dst, 7201, "unsplit")
	rid := sharding.ImplicitRegionID(storage.ID(7201))
	if _, ok := dst.router.ImplicitRegionOwner(storage.ID(7201)); ok {
		t.Fatalf("precondition: forward-only node must start without implicit owner")
	}
	if _, kind := dst.routeDecision(rid); kind != routeUnassigned {
		t.Fatalf("precondition: unlearned implicit region on forward-only node must be routeUnassigned, got %v", kind)
	}
	if err := dst.LearnImplicitRoutesFromPeers(routes); err != nil {
		t.Fatalf("LearnImplicitRoutesFromPeers: %v", err)
	}
	node, ok := dst.router.ImplicitRegionOwner(storage.ID(7201))
	if !ok || node != "node-a" {
		t.Fatalf("forward-only node must learn implicit owner node-a, got %q ok=%v", node, ok)
	}
	if got := dst.router.RegionNode(rid); got != "node-a" {
		t.Fatalf("learned RegionNode(%d) = %q, want node-a", rid, got)
	}
	if dst.regionLocal(rid) {
		t.Fatalf("regionLocal must be false for node-c after learning route")
	}
	got, kind := dst.routeDecision(rid)
	if kind != routeRemoteOwned || got != "node-a" {
		t.Fatalf("route decision = (%q,%v), want (node-a, routeRemoteOwned)", got, kind)
	}
	if v204ImplicitInExplicit(dst, rid) {
		t.Fatalf("learned implicit region must NOT be mixed into explicit region list")
	}
	// 幂等：重复学习不改变结论。
	if err := dst.LearnImplicitRoutesFromPeers(routes); err != nil {
		t.Fatalf("second learn must be a no-op: %v", err)
	}
	if node, _ := dst.router.ImplicitRegionOwner(storage.ID(7201)); node != "node-a" {
		t.Fatalf("implicit owner must stay node-a, got %q", node)
	}
}

// TestV204SplitTableIgnoredOnLearn 已切分表：学习到的隐式归属被忽略，分片视图优先。
func TestV204SplitTableIgnoredOnLearn(t *testing.T) {
	e := v204Env(t, "node-c", true)
	tid := storage.ID(7301)
	if err := e.router.UpsertRegion(sharding.RegionInfo{
		RegionID: storage.ID(9301), TableID: tid, State: sharding.StateActive,
		Explicit: true, Node: "node-b",
	}); err != nil {
		t.Fatalf("seed explicit region: %v", err)
	}
	learn := []sharding.RegionInfo{{
		RegionID: sharding.ImplicitRegionID(tid), TableID: tid, State: sharding.StateActive,
		Explicit: false, Node: "node-a",
	}}
	if err := e.LearnImplicitRoutesFromPeers(learn); err != nil {
		t.Fatalf("LearnImplicitRoutesFromPeers: %v", err)
	}
	if node, ok := e.router.ImplicitRegionOwner(tid); ok {
		t.Fatalf("split table must ignore learned implicit route, got owner %q", node)
	}
	if got := e.router.RegionNode(storage.ID(9301)); got != "node-b" {
		t.Fatalf("explicit region owner must stay node-b, got %q", got)
	}
}

// TestV204DropForgetsRoutes 删表：隐式归属随表一起清理。
func TestV204DropForgetsRoutes(t *testing.T) {
	e := v204Env(t, "node-c", true)
	e.router.SetImplicitRegionOwner(storage.ID(7401), "node-a")
	if _, ok := e.router.ImplicitRegionOwner(storage.ID(7401)); !ok {
		t.Fatalf("precondition: implicit owner must exist before drop")
	}
	e.forgetTableRoutes(7401)
	if _, ok := e.router.ImplicitRegionOwner(storage.ID(7401)); ok {
		t.Fatalf("drop must forget implicit owner of table 7401")
	}
	if got := e.router.RegionNode(sharding.ImplicitRegionID(storage.ID(7401))); got != "" {
		t.Fatalf("RegionNode after drop = %q, want empty", got)
	}
}

// TestV204ForwardOnlyImplicitRejectedUntilViewSync 未切分表写入（v2.0.4 行为变更）：
// 纯转发节点在学到权威归属前判 routeUnassigned 并触发视图同步，绝不本地写，
// 错误文案指明"视图缺失"而非"无归属"；学到归属后同一写入判远端 owner。
func TestV204ForwardOnlyImplicitRejectedUntilViewSync(t *testing.T) {
	e, st := newWriteRouteEnv(t, "node-c")
	m := newWriteRouteCluster("node-c", "node-a", "node-b", "node-c")
	m.SetProber(func(string) bool { return true })
	m.RefreshPartitionSnapshot()
	e.mgr = m
	e.SetWriteFence(&v203Fence{forwardOnly: true})
	e.SetWriteRouteEnforce(true)
	v204SeedTable(t, e, 7601, "unsplit")
	rid := sharding.ImplicitRegionID(storage.ID(7601))
	if !e.routeViewMissing(rid) {
		t.Fatalf("unlearned implicit region on forward-only node must be view-missing")
	}
	key := sharding.EncodeRegionKey(rid, []byte("k1"))
	_, _, err := e.groupOps([]kvOp{{Key: key, Value: []byte("v1")}})
	if err == nil {
		t.Fatalf("forward-only node must not write implicit region before learning owner")
	}
	if code := writeRouteErrCode(err); code != ErrCodeRegionNotAssigned {
		t.Fatalf("error code = %q, want %q (err=%v)", code, ErrCodeRegionNotAssigned, err)
	}
	if !strings.Contains(err.Error(), "missing from local route view") {
		t.Fatalf("error must attribute route view missing, got: %v", err)
	}
	if strings.Contains(err.Error(), "ownership revoked") {
		t.Fatalf("error must not be attributed to ownership revocation, got: %v", err)
	}
	if n := countKeysWithPrefix(t, st, []byte("r")); n != 0 {
		t.Fatalf("no local write must happen, found %d keys", n)
	}
	if err := e.LearnImplicitRoutesFromPeers([]sharding.RegionInfo{{
		RegionID: rid, TableID: storage.ID(7601), State: sharding.StateActive,
		Explicit: false, Node: "node-a",
	}}); err != nil {
		t.Fatalf("learn: %v", err)
	}
	if e.routeViewMissing(rid) {
		t.Fatalf("learned implicit region must no longer be view-missing")
	}
	if owner, kind := e.routeDecision(rid); kind != routeRemoteOwned || owner != "node-a" {
		t.Fatalf("route decision after learn = (%q,%v), want (node-a, routeRemoteOwned)", owner, kind)
	}
}

// TestV204DDLPathRegistersNothing DDL 路径不登记隐式归属（避免建表路径新增写事务）。

func TestV204DDLPathRegistersNothing(t *testing.T) {
	e := v204Env(t, "node-a", false)
	v204SeedTable(t, e, 7501, "unsplit")
	if m := e.router.ImplicitRegions(); len(m) != 0 {
		t.Fatalf("create path must not register implicit routes in router, got %+v", m)
	}
	// 但视图同步载荷仍能派生该表归属（可派生性保证无需落盘/登记）。
	routes := e.implicitRoutesForViewSync()
	rg, ok := v204FindImplicit(routes, 7501)
	if !ok {
		t.Fatalf("derived payload must still carry the unsplit table, got %+v", routes)
	}
	if rg.Node != "node-a" {
		t.Fatalf("derived owner = %q, want node-a", rg.Node)
	}
}

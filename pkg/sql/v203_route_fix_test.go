package sql

// v2.0.3 缺陷修复专项测试：显式 region 在本机路由视图缺失 × 纯转发节点。
//
// 缺陷现象（真机 node-c / 203）：节点持久化 cluster_term.meta 为旧纪元
// owned=0（集群轨曾授予归属后被权威视图收回），本地路由视图又缺新建表的
// region 条目。写路径 routeDecision 把"显式 region 无本地条目"与"隐式单
// region"混为一谈判成本机归属，随后被节点级归属 fence 以
// "cluster ownership revoked" 拒绝——归因指错方向，且无法靠重新授权解除。
//
// 修复验收（sql 层）：
//   - V203-1 纯转发节点 + 显式 region 无本地条目 → 拒写且错误文案指向
//     "route view missing"（不再是 ownership revoked），且绝不本地落盘；
//   - V203-2 普通节点同场景 → 保持旧行为（本地路径，零回归）；
//   - V203-3 纯转发节点 + 隐式单 region（表未切分）→ 旧行为，本地写。

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// v203Fence 可注入的写 fence 桩：实现语句级与 region 级能力，并按需声明
// 本机是否为纯转发节点（v2.0.3 可选能力 forwardOnlyWriteFence）。
type v203Fence struct {
	forwardOnly bool
	nodeErr     error
	regionErr   error
}

func (f *v203Fence) AllowWrite() error             { return f.nodeErr }
func (f *v203Fence) AllowWriteStatement() error    { return nil }
func (f *v203Fence) AllowWriteRegion(uint64) error { return f.regionErr }
func (f *v203Fence) ForwardOnlyNode() bool         { return f.forwardOnly }

// v203MissingRegionID 一个"显式 region"ID（不带隐式高位标记），且不在本地
// 路由视图中注册——对应真机上新建表 region 5816/5817 未出现在 route view。
func v203MissingRegionID() storage.ID { return storage.ID(5816) }

// v203Env 构造"三节点视图全可达"的执行器（避免分区拒写抢先拦截路由判定），
// 并按需装配 fence 与写路由强制开关。
func v203Env(t *testing.T, forwardOnly bool) (*Executor, storage.Storage) {
	t.Helper()
	e, st := newWriteRouteEnv(t, "node-c")
	m := newWriteRouteCluster("node-c", "node-a", "node-b")
	m.SetProber(func(string) bool { return true })
	m.RefreshPartitionSnapshot()
	e.mgr = m
	e.SetWriteFence(&v203Fence{forwardOnly: forwardOnly})
	e.SetWriteRouteEnforce(true)
	return e, st
}

// TestV203ForwardOnlyMissingRegionRejected V203-1：纯转发节点写显式 region，
// 本地视图无该条目 → REGION_NOT_ASSIGNED，且文案区分"视图缺失"。
func TestV203ForwardOnlyMissingRegionRejected(t *testing.T) {
	e, st := v203Env(t, true)

	rid := v203MissingRegionID()
	if !e.routeViewMissing(rid) {
		t.Fatalf("precondition: region %d must be missing from local route view", rid)
	}
	if sharding.IsImplicitRegionID(rid) {
		t.Fatalf("precondition: region %d must not be an implicit region", rid)
	}
	if !e.forwardOnlyNode() {
		t.Fatalf("precondition: injected fence must report forward-only node")
	}

	key := sharding.EncodeRegionKey(rid, []byte("k1"))
	_, _, err := e.groupOps([]kvOp{{Key: key, Value: []byte("v1")}})
	if err == nil {
		t.Fatalf("forward-only node must reject write to region %d missing from local view", rid)
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
}

// TestV203NormalNodeMissingRegionZeroRegression V203-2：非纯转发节点同场景
// 保持旧行为——判本机归属并落本地（零回归）。
func TestV203NormalNodeMissingRegionZeroRegression(t *testing.T) {
	e, _ := v203Env(t, false)

	rid := v203MissingRegionID()
	if e.routeViewMissing(rid) != true {
		t.Fatalf("routeViewMissing is a pure view fact and must stay true")
	}
	if _, kind := e.routeDecision(rid); kind != routeLocalOwned {
		t.Fatalf("normal node must keep legacy route decision (routeLocalOwned), got %v", kind)
	}
	if _, _, err := e.groupOps([]kvOp{{Key: sharding.EncodeRegionKey(rid, []byte("k1")), Value: []byte("v1")}}); err != nil {
		t.Fatalf("normal node legacy path must not reject: %v", err)
	}
}

// TestV203ForwardOnlyImplicitRegionZeroRegression V203-3：纯转发节点写隐式
// 单 region（未切分表）仍走本地，旧语义零回归。
func TestV203ForwardOnlyImplicitRegionZeroRegression(t *testing.T) {
	e, _ := v203Env(t, true)

	rid := storage.ID(sharding.ImplicitRegionBit | 1)
	if !sharding.IsImplicitRegionID(rid) {
		t.Fatalf("precondition: region %d must be implicit", rid)
	}
	if e.routeViewMissing(rid) {
		t.Fatalf("implicit region must never be treated as view-missing")
	}
	if _, kind := e.routeDecision(rid); kind != routeLocalOwned {
		t.Fatalf("implicit region must route locally, got %v", kind)
	}
	if _, _, err := e.groupOps([]kvOp{{Key: sharding.EncodeRegionKey(rid, []byte("k1")), Value: []byte("v1")}}); err != nil {
		t.Fatalf("implicit region write must not reject: %v", err)
	}
}

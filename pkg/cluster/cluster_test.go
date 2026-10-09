package cluster_test

import (
	"errors"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

// newTestNode 创建进程内集群节点：btree 存储 + Server + Manager。
// 返回 manager、server、存储与监听地址。
func newTestNode(t *testing.T, id string, router *sharding.Router) (*cluster.Manager, *cluster.Server, *btree.KV, string) {
	t.Helper()
	st := btree.NewKV(32)
	srv := cluster.NewServer(st, id)
	if router != nil {
		srv.SetRouter(router)
	}
	addr, err := srv.ListenAndServe("127.0.0.1:0")
	if err != nil {
		t.Fatalf("%s ListenAndServe: %v", id, err)
	}
	mgr := cluster.NewManager(id)
	mgr.RegisterSelf(addr)
	return mgr, srv, st, addr
}

func TestAddNodeHandshake(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	mgrB, srvB, _, _ := newTestNode(t, "node-b", nil)
	defer srvB.Close()

	// A 注册 B：握手返回对方 NodeID
	id, err := mgrA.AddNode(srvB.SelfAddr)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if id != "node-b" {
		t.Fatalf("AddNode: want node-b, got %q", id)
	}
	// 节点注册表完整（自注册 + 远端）
	nodes := mgrA.ListNodes()
	if len(nodes) != 2 {
		t.Fatalf("ListNodes: want 2 nodes, got %d (%+v)", len(nodes), nodes)
	}
	nb, ok := mgrA.GetNode("node-b")
	if !ok || nb.Addr == "" || nb.State != cluster.StateUp {
		t.Fatalf("GetNode(node-b): %+v ok=%v", nb, ok)
	}
	// 重复注册幂等：节点重启（换地址）后重新 ADD NODE 更新地址，保持同一 NodeID
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("second AddNode (idempotent re-register): want nil, got %v", err)
	}
	nb2, ok := mgrA.GetNode("node-b")
	if !ok || nb2.Addr != srvB.SelfAddr {
		t.Fatalf("re-registered node-b: %+v ok=%v", nb2, ok)
	}
	// 未注册的地址握手失败
	if _, err := mgrA.AddNode("127.0.0.1:1"); err == nil {
		t.Fatalf("AddNode(dead addr): want error, got nil")
	}
	// B 侧独立注册表不受影响
	if _, ok := mgrB.GetNode("node-a"); ok {
		t.Fatalf("mgrB should not know node-a yet")
	}
}

func TestQueryForwardScanAndGet(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	_, srvB, stB, _ := newTestNode(t, "node-b", nil)
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// 在 B 的存储中写入数据（模拟远端节点已持有该 region 数据）
	batch := &storage.WriteBatch{Puts: []storage.KVPair{
		{Key: []byte("r\x00\x00\x00\x00\x00\x00\x00\x01k1"), Value: []byte("v1")},
		{Key: []byte("r\x00\x00\x00\x00\x00\x00\x00\x01k2"), Value: []byte("v2")},
		{Key: []byte("r\x00\x00\x00\x00\x00\x00\x00\x01k3"), Value: []byte("v3")},
	}}
	if err := stB.Write(batch); err != nil {
		t.Fatalf("seed B: %v", err)
	}

	// 转发区间扫描 [k1, k3)
	rows, err := mgrA.ScanRemote("node-b", []byte("r\x00\x00\x00\x00\x00\x00\x00\x01k1"), []byte("r\x00\x00\x00\x00\x00\x00\x00\x01k3"), 0)
	if err != nil {
		t.Fatalf("ScanRemote: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("ScanRemote: want 2 rows, got %d", len(rows))
	}
	if string(rows[0].Value) != "v1" || string(rows[1].Value) != "v2" {
		t.Fatalf("ScanRemote values: %q %q", rows[0].Value, rows[1].Value)
	}
	// 转发点查
	v, err := mgrA.GetRemote("node-b", []byte("r\x00\x00\x00\x00\x00\x00\x00\x01k2"))
	if err != nil {
		t.Fatalf("GetRemote: %v", err)
	}
	if string(v) != "v2" {
		t.Fatalf("GetRemote: want v2, got %q", v)
	}
	// 未命中
	if _, err := mgrA.GetRemote("node-b", []byte("r\x00\x00\x00\x00\x00\x00\x00\x01kx")); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetRemote(miss): want ErrNotFound, got %v", err)
	}
	// 未知节点
	if _, err := mgrA.ScanRemote("ghost", nil, nil, 0); err == nil {
		t.Fatalf("ScanRemote(unknown node): want error, got nil")
	}
}

func TestHeartbeatDown(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	_, srvB, _, _ := newTestNode(t, "node-b", nil)
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	mgrA.StartHeartbeat(40*time.Millisecond, 100*time.Millisecond)
	defer mgrA.Stop()

	// B 下线后，A 心跳循环应将其标记 DOWN
	if err := srvB.Close(); err != nil {
		t.Fatalf("close B: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		n, _ := mgrA.GetNode("node-b")
		if n.State == cluster.StateDown {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node-b not marked down after heartbeat timeout")
}

func TestRegionPushLearnsRoutes(t *testing.T) {
	// A 源视图：region5 未指派（Node=""，即 A 本地持有）、region7 即将指派
	routerA := sharding.NewRouter()
	routerA.UpsertRegion(sharding.RegionInfo{RegionID: 5, TableID: 1, Local: true})
	routerA.UpsertRegion(sharding.RegionInfo{RegionID: 7, TableID: 1, Local: true})
	mgrA, srvA, _, _ := newTestNode(t, "node-a", routerA)
	defer srvA.Close()

	// B：接收方，带自己的 Router（初始空）
	routerB := sharding.NewRouter()
	_, srvB, stB, _ := newTestNode(t, "node-b", routerB)
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	rows := []cluster.QueryRow{
		{Key: []byte("r\x00\x00\x00\x00\x00\x00\x00\x07k1"), Value: []byte("v1")},
		{Key: []byte("r\x00\x00\x00\x00\x00\x00\x00\x07k2"), Value: []byte("v2")},
	}
	routes := []sharding.RegionInfo{
		{RegionID: 5, TableID: 1, Node: "", Local: false}, // A 本地持有（Node 空，SrcID 补记）
		{RegionID: 7, TableID: 1, Node: "", Local: false},
	}
	rg := sharding.RegionInfo{RegionID: 7, TableID: 1}
	if err := mgrA.PushRegion("node-b", rg, rows, routes, "node-a"); err != nil {
		t.Fatalf("PushRegion: %v", err)
	}

	// B 存储收到数据
	if v, err := stB.Get([]byte("r\x00\x00\x00\x00\x00\x00\x00\x07k2")); err != nil || string(v) != "v2" {
		t.Fatalf("B storage: want v2, got %q err=%v", v, err)
	}
	// B 路由：region7 本地（Node 空 + Local true）
	got, ok := routerB.FindRegion(7)
	if !ok || got.Local != true || got.Node != "" {
		t.Fatalf("B region7: want local, got %+v ok=%v", got, ok)
	}
	// B 路由：region5 学自源视图（Node=node-a、非本地）
	got, ok = routerB.FindRegion(5)
	if !ok || got.Node != "node-a" || got.Local {
		t.Fatalf("B region5: want node-a remote, got %+v ok=%v", got, ok)
	}
}

package sql

// v1.19.0 远端区间聚合（remote_range_agg）执行器侧验收用例。
//
// 覆盖点：
//  1. 同一归属节点的多个远端区间合并为单次 QUERY_MULTI 往返（以 listener
//     Accept 次数计），旧路径（flag 关闭）为每区间一次往返；
//  2. 本地 region 与远端 region 混合时，结果集与旧路径逐行等价、按内层键序
//     全局有序，且本地区间不产生远端请求；
//  3. P2-13 Serializable 读集：聚合返回的远端行 key 必须登记；
//  4. P3 READY 前置：目标节点数据面未就绪时整体拒绝且不发起往返。

import (
	"net"
	"sync/atomic"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// remoteAggListener 统计 Accept 次数：M4 远端请求均为短连接，Accept 增量
// 等价于网络往返次数。
type remoteAggListener struct {
	net.Listener
	accepts int64
}

func (l *remoteAggListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		atomic.AddInt64(&l.accepts, 1)
	}
	return c, err
}

func (l *remoteAggListener) count() int64 { return atomic.LoadInt64(&l.accepts) }

// remoteAggTxn 只读事务替身：Scan 在预置行集上按区间过滤并升序返回，
// 供不依赖 WAL/提交语义的扫描路径测试使用。
type remoteAggTxn struct{ rows []storage.KVPair }

func (t *remoteAggTxn) ID() uint64                                    { return 1 }
func (t *remoteAggTxn) Get(key []byte) ([]byte, error)                { return nil, storage.ErrNotFound }
func (t *remoteAggTxn) MGet(keys [][]byte) (map[string][]byte, error) { return nil, nil }
func (t *remoteAggTxn) Put(key, value []byte) error                   { return nil }
func (t *remoteAggTxn) Delete(key []byte) error                       { return nil }
func (t *remoteAggTxn) Commit() error                                 { return nil }
func (t *remoteAggTxn) Rollback() error                               { return nil }
func (t *remoteAggTxn) SetDeferSync(bool)                             {}

func (t *remoteAggTxn) Scan(r storage.KeyRange, limit int) ([]storage.KVPair, error) {
	out := make([]storage.KVPair, 0, len(t.rows))
	for _, kv := range t.rows {
		if r.Start != nil && string(kv.Key) < string(r.Start) {
			continue
		}
		if r.End != nil && string(kv.Key) >= string(r.End) {
			continue
		}
		out = append(out, kv)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	// 预置行集按构造保证升序，scanMerged 依赖存储侧有序返回。
	return out, nil
}

var _ txn.Txn = (*remoteAggTxn)(nil)

// remoteAggKey 构造带 region 前缀的物理键（'r' + 8 字节大端 regionID + 内层键）。
func remoteAggKey(rid storage.ID, inner string) []byte {
	k := make([]byte, 0, 9+len(inner))
	k = append(k, 'r')
	for i := 7; i >= 0; i-- {
		k = append(k, byte(uint64(rid)>>(8*uint(i))))
	}
	return append(k, inner...)
}

// remoteAggRange 构造单个 region 的整区间。
func remoteAggRange(rid storage.ID) storage.KeyRange {
	return storage.KeyRange{Start: remoteAggKey(rid, ""), End: remoteAggKey(rid, "\xff")}
}

// remoteAggStartNode 起一个进程内节点（带 Accept 计数）。
func remoteAggStartNode(t *testing.T, id string) (*cluster.Server, *btree.KV, *remoteAggListener, string) {
	t.Helper()
	st := btree.NewKV(32)
	srv := cluster.NewServer(st, id)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cl := &remoteAggListener{Listener: ln}
	addr, err := srv.Serve(cl)
	if err != nil {
		t.Fatalf("%s Serve: %v", id, err)
	}
	return srv, st, cl, addr
}

// remoteAggFixture A/B 两节点装配：B 持有 region1/region3 数据，A 本地持有
// region2 数据；region1/region3 路由指向 node-b，region2 未指派（本地）。
type remoteAggFixture struct {
	e     *Executor
	srvA  *cluster.Server
	srvB  *cluster.Server
	clB   *remoteAggListener
	tx    *remoteAggTxn
	rngs  []storage.KeyRange
	local int
	rmt   int
}

func newRemoteAggFixture(t *testing.T, remoteReady bool) *remoteAggFixture {
	t.Helper()
	srvA, _, _, addrA := remoteAggStartNode(t, "node-a")
	t.Cleanup(func() { srvA.Close() })
	srvB, stB, clB, addrB := remoteAggStartNode(t, "node-b")
	t.Cleanup(func() { srvB.Close() })

	// 远端 B：region1 三行（k1..k3）、region3 两行（k6..k7）。
	// 内层键按真实 region 切分不跨 region 重叠，保证合并后的全局行序唯一。
	if err := stB.Write(&storage.WriteBatch{Puts: []storage.KVPair{
		{Key: remoteAggKey(1, "k1"), Value: []byte("a1")},
		{Key: remoteAggKey(1, "k2"), Value: []byte("a2")},
		{Key: remoteAggKey(1, "k3"), Value: []byte("a3")},
		{Key: remoteAggKey(3, "k6"), Value: []byte("c6")},
		{Key: remoteAggKey(3, "k7"), Value: []byte("c7")},
	}}); err != nil {
		t.Fatalf("seed B: %v", err)
	}

	mgr := cluster.NewManager("node-a")
	mgr.RegisterSelf(addrA)
	if _, err := mgr.AddNode(addrB); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if !remoteReady {
		mgr.RegisterPeerData("node-b", addrB, false)
	}

	e := NewExecutor(nil)
	e.SetCluster(mgr, "node-a")
	// region1 / region3 归属 node-b；region2 未指派 -> 本地
	e.router.UpsertRegion(sharding.RegionInfo{RegionID: 1, TableID: 1, Node: "node-b"})
	e.router.UpsertRegion(sharding.RegionInfo{RegionID: 3, TableID: 1, Node: "node-b"})
	e.router.UpsertRegion(sharding.RegionInfo{RegionID: 2, TableID: 1})

	// 本地 region2 两行（内层键 k4/k5，与远端区间不重叠）
	local := []storage.KVPair{
		{Key: remoteAggKey(2, "k4"), Value: []byte("b4")},
		{Key: remoteAggKey(2, "k5"), Value: []byte("b5")},
	}
	return &remoteAggFixture{
		e:    e,
		srvA: srvA,
		srvB: srvB,
		clB:  clB,
		tx:   &remoteAggTxn{rows: local},
		rngs: []storage.KeyRange{
			remoteAggRange(1),
			remoteAggRange(2),
			remoteAggRange(3),
		},
		local: len(local),
		rmt:   5,
	}
}

// aggRowValues 提取结果行的值序列（用于等价性比对）。
func aggRowValues(pairs []storage.KVPair) []string {
	out := make([]string, 0, len(pairs))
	for _, kv := range pairs {
		out = append(out, string(kv.Value))
	}
	return out
}

// TestRemoteRangeAggReducesRoundTrips 验证开关开启时同节点多区间合并为单次
// 往返、关闭时回退逐区间往返，且两种路径结果集完全一致。
func TestRemoteRangeAggReducesRoundTrips(t *testing.T) {
	f := newRemoteAggFixture(t, true)
	sess := &Session{txIso: IsoSerializable}

	// 基线：旧路径（flag 默认关闭）——远端 2 个区间 -> 2 次往返
	if f.e.RemoteRangeAgg() {
		t.Fatalf("precondition: remote_range_agg must default to off")
	}
	base := f.clB.count()
	legacy, err := f.e.scanMergedCluster(sess, f.tx, f.rngs)
	if err != nil {
		t.Fatalf("legacy scanMergedCluster: %v", err)
	}
	if n := f.clB.count() - base; n != 2 {
		t.Fatalf("legacy path round trips = %d, want 2 (per remote range)", n)
	}
	if len(legacy) != f.local+f.rmt {
		t.Fatalf("legacy rows = %d, want %d", len(legacy), f.local+f.rmt)
	}

	// 聚合路径：远端 2 个区间（同归属节点）-> 1 次往返
	f.e.SetRemoteRangeAgg(true)
	sessAgg := &Session{txIso: IsoSerializable}
	baseAgg := f.clB.count()
	agg, err := f.e.scanMergedCluster(sessAgg, f.tx, f.rngs)
	if err != nil {
		t.Fatalf("aggregate scanMergedCluster: %v", err)
	}
	if n := f.clB.count() - baseAgg; n != 1 {
		t.Fatalf("aggregate path round trips = %d, want 1 (per owner node)", n)
	}

	// 结果等价（顺序与逐行内容）
	lv, av := aggRowValues(legacy), aggRowValues(agg)
	if len(lv) != len(av) {
		t.Fatalf("row count legacy=%d aggregate=%d", len(lv), len(av))
	}
	for i := range lv {
		if lv[i] != av[i] {
			t.Fatalf("row %d value legacy=%q aggregate=%q (aggregate must keep row order/content)", i, lv[i], av[i])
		}
	}

	// 行序契约：合并后按内层键全局升序（内层键不跨 region 重叠时序列唯一）
	want := []string{"a1", "a2", "a3", "b4", "b5", "c6", "c7"}
	if len(av) != len(want) {
		t.Fatalf("aggregate rows = %d, want %d", len(av), len(want))
	}
	for i := range want {
		if av[i] != want[i] {
			t.Fatalf("aggregate row %d = %q, want %q (global inner-key order)", i, av[i], want[i])
		}
	}

	// P2-13：Serializable 隔离级下远端行 key 纳入读集
	if len(sessAgg.serialRemoteReads) != f.rmt {
		t.Fatalf("serialRemoteReads = %d keys, want %d (remote scanned rows must be registered)",
			len(sessAgg.serialRemoteReads), f.rmt)
	}
	// 仅远端行登记：本地区间行不得进入远端读集
	for _, kv := range f.tx.rows {
		if _, ok := sessAgg.serialRemoteReads[string(kv.Key)]; ok {
			t.Fatalf("local row %q must not be registered in serial remote read set", kv.Key)
		}
	}
}

// TestRemoteRangeAggLocalRangesNoRoundTrip 验证全本地区间不发起任何远端请求，
// 且非 Serializable 会话不登记读集。
func TestRemoteRangeAggLocalRangesNoRoundTrip(t *testing.T) {
	f := newRemoteAggFixture(t, true)
	f.e.SetRemoteRangeAgg(true)

	sess := &Session{}
	base := f.clB.count()
	pairs, err := f.e.scanMergedCluster(sess, f.tx, []storage.KeyRange{remoteAggRange(2)})
	if err != nil {
		t.Fatalf("scanMergedCluster(local only): %v", err)
	}
	if n := f.clB.count() - base; n != 0 {
		t.Fatalf("local-only ranges round trips = %d, want 0", n)
	}
	if len(pairs) != f.local {
		t.Fatalf("local rows = %d, want %d", len(pairs), f.local)
	}
	if len(sess.serialRemoteReads) != 0 {
		t.Fatalf("non-serializable session must not register read set, got %d", len(sess.serialRemoteReads))
	}
}

// TestRemoteRangeAggRejectsNotReadyNode 验证 P3 READY 前置：目标节点数据面
// 未就绪时整体拒绝（错误前置返回），且不发起远端往返。
func TestRemoteRangeAggRejectsNotReadyNode(t *testing.T) {
	f := newRemoteAggFixture(t, false)
	f.e.SetRemoteRangeAgg(true)

	base := f.clB.count()
	if _, err := f.e.scanMergedCluster(&Session{}, f.tx, f.rngs); err == nil {
		t.Fatalf("not-ready node should be rejected, got nil error")
	}
	if n := f.clB.count() - base; n != 0 {
		t.Fatalf("rejected scan must not dial remote, got %d round trips", n)
	}
}

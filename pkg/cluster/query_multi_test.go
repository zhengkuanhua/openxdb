package cluster_test

// v1.19.0 远端区间聚合（QUERY_MULTI_REQ / QUERY_MULTI_RESP）验收用例。
//
// 覆盖点：
//  1. 多区间一次往返：同一节点的 N 个区间合并为 1 次网络往返（以 listener
//     Accept 次数计），逐区间旧路径对应 N 次往返；
//  2. 结果等价：聚合返回与逐区间 ScanRemote 逐行一致（含区间下标归组）；
//  3. 流式分块：单区间行数超过 multiChunkRows 预算时仍完整、有序；
//  4. 边界与错误前置：空区间返回空切片、未知节点立即报错。

import (
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

// countingListener 包装 listener 统计 Accept 次数。
// M4 远端请求均为短连接（每请求一次 dial/close），因此 Accept 增量等价于
// 网络往返次数，用于量化"多区间聚合"的收益。
type countingListener struct {
	net.Listener
	accepts int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		atomic.AddInt64(&l.accepts, 1)
	}
	return c, err
}

func (l *countingListener) count() int64 { return atomic.LoadInt64(&l.accepts) }

// newCountingNode 起一个带 Accept 计数的进程内节点。
func newCountingNode(t *testing.T, id string) (*btree.KV, *cluster.Server, *countingListener) {
	t.Helper()
	st := btree.NewKV(32)
	srv := cluster.NewServer(st, id)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cl := &countingListener{Listener: ln}
	if _, err := srv.Serve(cl); err != nil {
		t.Fatalf("%s Serve: %v", id, err)
	}
	return st, srv, cl
}

// regionKey 构造带 region 前缀的物理键（与 sharding.EncodeRegionKey 同布局：
// 'r' + 8 字节大端 regionID + 内层键）。
func regionKey(rid uint64, inner string) []byte {
	k := make([]byte, 0, 9+len(inner))
	k = append(k, 'r')
	for i := 7; i >= 0; i-- {
		k = append(k, byte(rid>>(8*uint(i))))
	}
	return append(k, inner...)
}

// regionRange 构造单个 region 的整区间 [prefix, prefix+0xff)。
func regionRange(rid uint64) ([]byte, []byte) {
	start := regionKey(rid, "")
	return start, regionKey(rid, "\xff")
}

// seedRows 向节点存储写入测试行。
func seedRows(t *testing.T, st *btree.KV, puts []storage.KVPair) {
	t.Helper()
	if err := st.Write(&storage.WriteBatch{Puts: puts}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
}

// TestScanMultiRemoteSingleRoundTripAndEquivalence 验证多区间聚合的往返次数
// 收益与结果等价性：同一节点 2 个区间走 1 次往返，且逐区间结果与旧路径一致。
func TestScanMultiRemoteSingleRoundTripAndEquivalence(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	stB, srvB, clB := newCountingNode(t, "node-b")
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// B 侧数据：region1 三行、region3 两行（region2 无数据，用于空区间场景）
	s1s, s1e := regionRange(1)
	s3s, s3e := regionRange(3)
	seedRows(t, stB, []storage.KVPair{
		{Key: regionKey(1, "k1"), Value: []byte("a1")},
		{Key: regionKey(1, "k2"), Value: []byte("a2")},
		{Key: regionKey(1, "k3"), Value: []byte("a3")},
		{Key: regionKey(3, "k1"), Value: []byte("c1")},
		{Key: regionKey(3, "k2"), Value: []byte("c2")},
	})

	ranges := []cluster.QueryRange{
		{Start: s1s, End: s1e},
		{Start: s3s, End: s3e},
	}

	base := clB.count()
	got, err := mgrA.ScanMultiRemote("node-b", ranges, 0, "")
	if err != nil {
		t.Fatalf("ScanMultiRemote: %v", err)
	}
	if n := clB.count() - base; n != 1 {
		t.Fatalf("multi-range aggregate should take 1 round trip, got %d", n)
	}
	if len(got) != len(ranges) {
		t.Fatalf("result groups = %d, want %d", len(got), len(ranges))
	}
	if len(got[0]) != 3 || len(got[1]) != 2 {
		t.Fatalf("rows per range = %d/%d, want 3/2", len(got[0]), len(got[1]))
	}

	// 旧路径：逐区间独立请求，往返次数 = 区间数，且结果逐行等价
	baseOld := clB.count()
	for i, r := range ranges {
		rows, err := mgrA.ScanRemote("node-b", r.Start, r.End, r.Limit, 0, "")
		if err != nil {
			t.Fatalf("ScanRemote(range %d): %v", i, err)
		}
		if len(rows) != len(got[i]) {
			t.Fatalf("range %d rows = %d (legacy) / %d (aggregate)", i, len(rows), len(got[i]))
		}
		for j := range rows {
			if string(rows[j].Key) != string(got[i][j].Key) || string(rows[j].Value) != string(got[i][j].Value) {
				t.Fatalf("range %d row %d mismatch: legacy (%q,%q) aggregate (%q,%q)",
					i, j, rows[j].Key, rows[j].Value, got[i][j].Key, got[i][j].Value)
			}
		}
	}
	if n := clB.count() - baseOld; n != int64(len(ranges)) {
		t.Fatalf("legacy per-range path should take %d round trips, got %d", len(ranges), n)
	}
}

// TestScanMultiRemoteStreamsLargeRange 验证单区间超过分块预算时结果完整有序
// （multiChunkRows=512，这里 1200 行必然多块）。
func TestScanMultiRemoteStreamsLargeRange(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	stB, srvB, clB := newCountingNode(t, "node-b")
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	const rows = 1200
	puts := make([]storage.KVPair, 0, rows)
	for i := 0; i < rows; i++ {
		puts = append(puts, storage.KVPair{
			Key:   regionKey(5, fmt.Sprintf("k%05d", i)),
			Value: []byte(fmt.Sprintf("v%05d", i)),
		})
	}
	seedRows(t, stB, puts)

	start, end := regionRange(5)
	base := clB.count()
	got, err := mgrA.ScanMultiRemote("node-b", []cluster.QueryRange{{Start: start, End: end}}, 0, "")
	if err != nil {
		t.Fatalf("ScanMultiRemote: %v", err)
	}
	if n := clB.count() - base; n != 1 {
		t.Fatalf("streamed large range should still take 1 round trip, got %d", n)
	}
	if len(got) != 1 || len(got[0]) != rows {
		t.Fatalf("streamed rows = %d, want %d", len(got[0]), rows)
	}
	for i, row := range got[0] {
		want := fmt.Sprintf("v%05d", i)
		if string(row.Value) != want {
			t.Fatalf("row %d value = %q, want %q (逐块回传必须保持升序与原序)", i, row.Value, want)
		}
	}
}

// TestScanMultiRemoteEmptyAndUnknownNode 验证空区间与未知节点的边界语义。
func TestScanMultiRemoteEmptyAndUnknownNode(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	_, srvB, _ := newCountingNode(t, "node-b")
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// region2 / region9 均无数据：应返回与请求等长的空切片组，而非报错
	es, ee := regionRange(2)
	ns, ne := regionRange(9)
	got, err := mgrA.ScanMultiRemote("node-b", []cluster.QueryRange{
		{Start: es, End: ee},
		{Start: ns, End: ne},
	}, 0, "")
	if err != nil {
		t.Fatalf("ScanMultiRemote(empty): %v", err)
	}
	if len(got) != 2 || len(got[0]) != 0 || len(got[1]) != 0 {
		t.Fatalf("empty ranges groups = %d (%d/%d rows), want 2 (0/0)", len(got), len(got[0]), len(got[1]))
	}

	// 未知节点：不得静默返回空结果（错误必须前置）
	if _, err := mgrA.ScanMultiRemote("ghost", []cluster.QueryRange{{Start: es, End: ee}}, 0, ""); err == nil {
		t.Fatalf("ScanMultiRemote(unknown node): want error, got nil")
	}
}

// TestScanMultiRemoteSortWindow 验证区间级排序窗口（M12-2 语义）在聚合协议下
// 仍由归属节点本地排序后截窗，与 QUERY_REQ 的 OpScan 行为一致。
func TestScanMultiRemoteSortWindow(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	stB, srvB, _ := newCountingNode(t, "node-b")
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	puts := make([]storage.KVPair, 0, 10)
	for i := 0; i < 10; i++ {
		puts = append(puts, storage.KVPair{
			Key:   regionKey(7, fmt.Sprintf("k%d", i)),
			Value: []byte(fmt.Sprintf("[%d]", i)),
		})
	}
	seedRows(t, stB, puts)

	start, end := regionRange(7)
	spec := &cluster.SortSpec{ColIndex: 0, Kind: "INT", Desc: true, Limit: 2}
	got, err := mgrA.ScanMultiRemote("node-b", []cluster.QueryRange{
		{Start: start, End: end, Sort: spec},
	}, 0, "")
	if err != nil {
		t.Fatalf("ScanMultiRemote(sort): %v", err)
	}
	if len(got) != 1 || len(got[0]) != 2 {
		t.Fatalf("sorted window rows = %d, want 2", len(got[0]))
	}
	if string(got[0][0].Value) != "[9]" || string(got[0][1].Value) != "[8]" {
		t.Fatalf("sorted window values = %q/%q, want [9]/[8]", got[0][0].Value, got[0][1].Value)
	}
}

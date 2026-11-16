package sql

// v1.18.0 D6.2 批量点查合并（routedJoin / evalInSubqueryRouted）验证与基准。
//
// D6 验收口径（DEV_PLAN_v1.18.0.md）要求「批量点查合并落地 + 基准对比 + 无回归」。
// 本文件提供可复算证据：
//   - TestD62RoutedJoinBatchPointGet：N 行驱动表 JOIN 右表回表，统计 Get/MGet
//     调用次数——批量路径应为 0 次点查 + 1 次批量读，替代原先 N 次点查；
//   - TestD62RoutedJoinLeftSemantics：LEFT JOIN 未命中补空与原序输出；
//   - TestD62InSubqueryBatchPointGet：IN 子查询逐值点查压缩为单次批量读，
//     含左值去重与未命中跳过；
//   - BenchmarkD62RoutedJoinBatch / BenchmarkD62RoutedJoinPerRow：注入固定往返
//     延迟（模拟 runtime.cgocall，storage_engine_selection.md §5 记为 flat 63.75%
//     的瓶颈项）对比两种路径耗时，量化收益。
//
// 说明：mgr == nil 时 mgetKVCluster 走「本地键一次 tx.MGet」分支，与单机部署
// 路径一致；本例不依赖真实存储与集群网络，可直接离线复算。

import (
	"fmt"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// d62RoundTrip 模拟一次点查往返成本（cgo 调用量级），用于基准中放大差异。
const d62RoundTrip = 5 * time.Microsecond

// d62Txn 计数事务桩：统计 Get / MGet 调用次数与键数，可注入单次往返延迟。
type d62Txn struct {
	data                map[string][]byte
	delay               time.Duration
	getCalls, getKeys   int
	mgetCalls, mgetKeys int
}

func (t *d62Txn) ID() uint64 { return 1 }

func (t *d62Txn) Get(key []byte) ([]byte, error) {
	t.getCalls++
	t.getKeys++
	if t.delay > 0 {
		time.Sleep(t.delay)
	}
	if v, ok := t.data[string(key)]; ok {
		return v, nil
	}
	return nil, storage.ErrNotFound
}

func (t *d62Txn) MGet(keys [][]byte) (map[string][]byte, error) {
	t.mgetCalls++
	t.mgetKeys += len(keys)
	if t.delay > 0 {
		time.Sleep(t.delay)
	}
	out := make(map[string][]byte, len(keys))
	for _, k := range keys {
		if v, ok := t.data[string(k)]; ok {
			out[string(k)] = v
		}
	}
	return out, nil
}

func (t *d62Txn) Scan(r storage.KeyRange, limit int) ([]storage.KVPair, error) { return nil, nil }
func (t *d62Txn) Put(key, value []byte) error                                  { return nil }
func (t *d62Txn) Delete(key []byte) error                                      { return nil }
func (t *d62Txn) Commit() error                                                { return nil }
func (t *d62Txn) Rollback() error                                              { return nil }
func (t *d62Txn) SetDeferSync(bool)                                            {}

// d62RightMeta 构造右表（被驱动表）元数据：单列 INT 主键 + 一列 TEXT。
func d62RightMeta() *TableMeta {
	return &TableMeta{
		ID:   9002,
		Name: "d62_right",
		PK:   "id",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "tag", Type: "TEXT"},
		},
	}
}

// d62DriverRel 构造驱动表（左表）：1..n 连续主键值。
func d62DriverRel(n int) *Rel {
	rel := &Rel{Cols: []string{"id"}, Types: []string{"INT"}}
	for i := 1; i <= n; i++ {
		rel.Rows = append(rel.Rows, []Value{{Kind: "INT", I: int64(i)}})
	}
	return rel
}

// d62RightRel 构造右表列结构（与 d62RightMeta 对齐）。
func d62RightRel() *Rel {
	return &Rel{Cols: []string{"id", "tag"}, Types: []string{"INT", "TEXT"}}
}

// d62Seed 生成右表数据：偶数主键存在（奇数缺失，覆盖未命中分支）。
// 键按 rowKeyAt 编码（带 region 前缀），与 routedJoin 路由结果可直接对上。
func d62Seed(e *Executor, m *TableMeta, n int) map[string][]byte {
	data := map[string][]byte{}
	for i := 2; i <= n; i += 2 {
		v := Value{Kind: "INT", I: int64(i)}
		pk, err := pkBytes(m, v)
		if err != nil {
			continue
		}
		rid, err := e.locateRow(m, pk)
		if err != nil {
			continue
		}
		raw, err := encodeRow(m, []Value{v, {Kind: "TEXT", S: fmt.Sprintf("tag-%d", i)}})
		if err != nil {
			continue
		}
		data[string(e.rowKeyAt(m, rid, pk))] = raw
	}
	return data
}

// TestD62RoutedJoinBatchPointGet 验证 routedJoin 把 N 次回表点查压成 1 次批量读。
func TestD62RoutedJoinBatchPointGet(t *testing.T) {
	e := NewExecutor(nil)
	sess := &Session{}
	rmeta := d62RightMeta()
	const n = 12
	tx := &d62Txn{data: d62Seed(e, rmeta, n)}

	out, err := e.routedJoin(sess, tx, d62DriverRel(n), d62RightRel(), rmeta,
		&JoinClause{Type: "INNER"}, joinRoutePlan{pkOnIdx: 0, leftIdx: 0})
	if err != nil {
		t.Fatalf("routedJoin: %v", err)
	}
	if tx.getCalls != 0 {
		t.Fatalf("逐行点查未被消除：Get 调用 %d 次（期望 0）", tx.getCalls)
	}
	if tx.mgetCalls != 1 {
		t.Fatalf("批量点查次数 = %d（期望 1）", tx.mgetCalls)
	}
	if tx.mgetKeys != n {
		t.Fatalf("批量点查键数 = %d（期望 %d：每个驱动行一个键）", tx.mgetKeys, n)
	}
	if len(out.Rows) != n/2 {
		t.Fatalf("INNER JOIN 命中行数 = %d（期望 %d）", len(out.Rows), n/2)
	}
	// 原序输出：2,4,6,...（未命中的奇数行被丢弃，但不打乱顺序）。
	for i, row := range out.Rows {
		want := int64((i + 1) * 2)
		if row[0].I != want {
			t.Fatalf("第 %d 行驱动键 = %d（期望 %d，输出未保持左行原序）", i, row[0].I, want)
		}
		if row[2].S != fmt.Sprintf("tag-%d", want) {
			t.Fatalf("第 %d 行右表列值 = %q（期望 tag-%d）", i, row[2].S, want)
		}
	}
}

// TestD62RoutedJoinLeftSemantics 验证 LEFT JOIN 未命中补空、命中不补空。
func TestD62RoutedJoinLeftSemantics(t *testing.T) {
	e := NewExecutor(nil)
	sess := &Session{}
	rmeta := d62RightMeta()
	const n = 6
	tx := &d62Txn{data: d62Seed(e, rmeta, n)}

	out, err := e.routedJoin(sess, tx, d62DriverRel(n), d62RightRel(), rmeta,
		&JoinClause{Type: "LEFT"}, joinRoutePlan{pkOnIdx: 0, leftIdx: 0})
	if err != nil {
		t.Fatalf("routedJoin LEFT: %v", err)
	}
	if len(out.Rows) != n {
		t.Fatalf("LEFT JOIN 行数 = %d（期望 %d：不丢驱动行）", len(out.Rows), n)
	}
	if tx.getCalls != 0 || tx.mgetCalls != 1 {
		t.Fatalf("LEFT JOIN 点查形态异常：Get=%d MGet=%d（期望 0/1）", tx.getCalls, tx.mgetCalls)
	}
	for i, row := range out.Rows {
		id := int64(i + 1)
		if row[0].I != id {
			t.Fatalf("第 %d 行驱动键 = %d（期望 %d）", i, row[0].I, id)
		}
		if id%2 == 1 {
			// 未命中：右列补空。
			if row[2].S != "" {
				t.Fatalf("第 %d 行（未命中）右表列应为空串，实际 %q", i, row[2].S)
			}
		} else if row[2].S != fmt.Sprintf("tag-%d", id) {
			t.Fatalf("第 %d 行（命中）右表列 = %q（期望 tag-%d）", i, row[2].S, id)
		}
	}
}

// TestD62InSubqueryBatchPointGet 验证 IN 子查询逐值点查压成 1 次批量读（含去重）。
func TestD62InSubqueryBatchPointGet(t *testing.T) {
	e := NewExecutor(nil)
	sess := &Session{}
	metaS := &TableMeta{
		ID:   9003,
		Name: "d62_sub",
		PK:   "id",
		Columns: []ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "tag", Type: "TEXT"},
		},
	}
	const n = 10
	tx := &d62Txn{data: d62Seed(e, metaS, n)}

	// 驱动列含重复值（每个值出现两次），验证「不同值」去重后只查一次。
	rel := &Rel{Cols: []string{"id"}, Types: []string{"INT"}}
	for i := 1; i <= n; i++ {
		v := Value{Kind: "INT", I: int64(i)}
		rel.Rows = append(rel.Rows, []Value{v}, []Value{v})
	}
	c := &Cond{Col: "id", Op: "IN", Sub: &SelectStmt{}}

	set, err := e.evalInSubqueryRouted(sess, tx, metaS, rel, c)
	if err != nil {
		t.Fatalf("evalInSubqueryRouted: %v", err)
	}
	if tx.getCalls != 0 {
		t.Fatalf("逐值点查未被消除：Get 调用 %d 次（期望 0）", tx.getCalls)
	}
	if tx.mgetCalls != 1 {
		t.Fatalf("批量点查次数 = %d（期望 1）", tx.mgetCalls)
	}
	if tx.mgetKeys != n {
		t.Fatalf("批量点查键数 = %d（期望 %d：按不同值去重）", tx.mgetKeys, n)
	}
	if len(set.vals) != n/2 {
		t.Fatalf("命中值个数 = %d（期望 %d）", len(set.vals), n/2)
	}
	for i, v := range set.vals {
		want := int64((i + 1) * 2)
		if v.I != want {
			t.Fatalf("第 %d 个命中值 = %d（期望 %d，命中集合未保持原序）", i, v.I, want)
		}
	}
}

// d62BenchJoin 基准公共装配：返回可复用的执行器与驱动表（避免把构造计入耗时）。
func d62BenchJoin(n int) (*Executor, *Session, *TableMeta, *Rel, *Rel, map[string][]byte) {
	e := NewExecutor(nil)
	rmeta := d62RightMeta()
	return e, &Session{}, rmeta, d62DriverRel(n), d62RightRel(), d62Seed(e, rmeta, n)
}

// BenchmarkD62RoutedJoinBatch D6.2 后：1 次批量点查。
func BenchmarkD62RoutedJoinBatch(b *testing.B) {
	const n = 64
	e, sess, rmeta, left, right, data := d62BenchJoin(n)
	tx := &d62Txn{data: data, delay: d62RoundTrip}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.routedJoin(sess, tx, left, right, rmeta,
			&JoinClause{Type: "INNER"}, joinRoutePlan{pkOnIdx: 0, leftIdx: 0}); err != nil {
			b.Fatalf("routedJoin: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(tx.mgetCalls)/float64(b.N), "mget/op")
	b.ReportMetric(float64(tx.getCalls)/float64(b.N), "get/op")
}

// BenchmarkD62RoutedJoinPerRow D6.2 前路径（对照）：每个驱动行一次 getKVCluster。
func BenchmarkD62RoutedJoinPerRow(b *testing.B) {
	const n = 64
	e, sess, rmeta, left, _, data := d62BenchJoin(n)
	tx := &d62Txn{data: data, delay: d62RoundTrip}
	keys := make([][]byte, 0, len(left.Rows))
	for _, lr := range left.Rows {
		pk, err := pkBytes(rmeta, lr[0])
		if err != nil {
			b.Fatalf("pkBytes: %v", err)
		}
		rid, err := e.locateRow(rmeta, pk)
		if err != nil {
			b.Fatalf("locateRow: %v", err)
		}
		keys = append(keys, e.rowKeyAt(rmeta, rid, pk))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			if _, err := e.getKVCluster(sess, tx, key); err != nil && err != storage.ErrNotFound {
				b.Fatalf("getKVCluster: %v", err)
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(tx.mgetCalls)/float64(b.N), "mget/op")
	b.ReportMetric(float64(tx.getCalls)/float64(b.N), "get/op")
}

package sql

import (
	"errors"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/metrics"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// errFake 提交失败替身错误。
var errFake = errors.New("fake commit error")

// fakeTxn 白盒测试用事务替身：只关心 Commit/Rollback 成功与否。
type fakeTxn struct {
	commitErr   error
	rollbackErr error
}

func (f *fakeTxn) ID() uint64                            { return 1 }
func (f *fakeTxn) Get(key []byte) ([]byte, error)        { return nil, storage.ErrNotFound }
func (f *fakeTxn) MGet(keys [][]byte) (map[string][]byte, error) { return nil, nil }
func (f *fakeTxn) Scan(r storage.KeyRange, limit int) ([]storage.KVPair, error) { return nil, nil }
func (f *fakeTxn) Put(key, value []byte) error           { return nil }
func (f *fakeTxn) Delete(key []byte) error               { return nil }
func (f *fakeTxn) Commit() error                         { return f.commitErr }
func (f *fakeTxn) Rollback() error                       { return f.rollbackErr }

// TestM13MetricsSQLAndSlow 注入指标后：execTimed 每次执行计 SQL、
// recordSlow 命中计慢查询（低开销路径不落盘也可计数）。
func TestM13MetricsSQLAndSlow(t *testing.T) {
	e := newExecutorInternal()
	m := metrics.New()
	e.SetMetrics(m)
	// 关闭自动慢查询记录：本测试只验证显式 recordSlow 的计数路径，
	// 避免慢机器/高负载下 execTimed 超过默认 100ms 阈值造成时序脆弱。
	e.SetSlowThreshold(0)

	if _, err := e.execTimed(e.globalSess, "root", "SELECT 1", nil, func() (*Result, error) {
		return &Result{}, nil
	}); err != nil {
		t.Fatalf("execTimed: %v", err)
	}
	if _, err := e.execTimed(e.globalSess, "root", "SELECT 2", nil, func() (*Result, error) {
		return &Result{}, nil
	}); err != nil {
		t.Fatalf("execTimed: %v", err)
	}
	e.recordSlow(SlowQueryRecord{SQL: "SELECT slow", User: "root", Duration: 1000})

	out := m.Render()
	if !containsMetric(out, "openxdb_queries_total{type=\"sql\"} 2") {
		t.Fatalf("sql counter missing:\n%s", out)
	}
	if !containsMetric(out, "openxdb_slow_query_total 1") {
		t.Fatalf("slow counter missing:\n%s", out)
	}
}

// TestM13MetricsTxnCommitRollback 成功提交/回滚分别计数；失败不计数。
func TestM13MetricsTxnCommitRollback(t *testing.T) {
	e := newExecutorInternal()
	m := metrics.New()
	e.SetMetrics(m)

	// 成功提交
	e.mu.Lock()
	e.globalSess.inTx = true
	e.globalSess.curTx = &fakeTxn{}
	e.mu.Unlock()
	if err := e.commitTx(e.globalSess); err != nil {
		t.Fatalf("commitTx: %v", err)
	}
	// 成功回滚
	e.mu.Lock()
	e.globalSess.inTx = true
	e.globalSess.curTx = &fakeTxn{}
	e.mu.Unlock()
	if err := e.rollbackTx(e.globalSess); err != nil {
		t.Fatalf("rollbackTx: %v", err)
	}
	// 提交失败不计数
	e.mu.Lock()
	e.globalSess.inTx = true
	e.globalSess.curTx = &fakeTxn{commitErr: errFake}
	e.mu.Unlock()
	if err := e.commitTx(e.globalSess); err == nil {
		t.Fatal("want commit error")
	}

	out := m.Render()
	if !containsMetric(out, "openxdb_txn_commit_total 1") {
		t.Fatalf("commit counter missing:\n%s", out)
	}
	if !containsMetric(out, "openxdb_txn_rollback_total 1") {
		t.Fatalf("rollback counter missing:\n%s", out)
	}
}

// TestM13MetricsRegionStats 按路由表聚合 region 总数/状态/本地与远端。
func TestM13MetricsRegionStats(t *testing.T) {
	e := newExecutorInternal()
	e.router.UpsertRegion(sharding.RegionInfo{
		RegionID: 1, TableID: 1, State: sharding.StateActive, Explicit: true, Node: "",
	})
	e.router.UpsertRegion(sharding.RegionInfo{
		RegionID: 2, TableID: 1, State: sharding.StateLeaving, Explicit: true, Node: "node-2",
	})
	rs := e.MetricsRegionStats()
	if rs.Total != 2 {
		t.Fatalf("region total: got %d, want 2", rs.Total)
	}
	if rs.ByState[string(sharding.StateActive)] != 1 || rs.ByState[string(sharding.StateLeaving)] != 1 {
		t.Fatalf("region by state: %v", rs.ByState)
	}
	if rs.Local != 1 || rs.Remote != 1 {
		t.Fatalf("local/remote: got %d/%d, want 1/1", rs.Local, rs.Remote)
	}
}

// TestM13MetricsNilSafety 未注入指标时所有计数路径不 panic（nil 安全）。
func TestM13MetricsNilSafety(t *testing.T) {
	e := newExecutorInternal()
	// 不调用 SetMetrics（m == nil）
	e.incLocal()
	e.incRemote()
	e.recordSlow(SlowQueryRecord{SQL: "x"})
	_, _ = e.execTimed(e.globalSess, "root", "SELECT 1", nil, func() (*Result, error) { return &Result{}, nil })
	e.mu.Lock()
	e.globalSess.inTx = true
	e.globalSess.curTx = &fakeTxn{}
	e.mu.Unlock()
	_ = e.commitTx(e.globalSess)
	_ = e.rollbackTx(e.globalSess)
	_ = e.MetricsRegionStats()
}

// containsMetric 断言渲染文本包含某指标行。
func containsMetric(out, want string) bool {
	for i := 0; i+len(want) <= len(out); i++ {
		if out[i:i+len(want)] == want {
			return true
		}
	}
	return false
}

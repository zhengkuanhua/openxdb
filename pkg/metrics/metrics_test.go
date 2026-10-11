package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fixedNow 固定时间源，隔离 QPS 秒窗跨秒行为（避免真实时钟波动）。
func fixedNow(sec int64) func() time.Time {
	base := time.Unix(sec, 0)
	return func() time.Time { return base }
}

// TestM13MetricsRenderFormat 渲染输出符合 Prometheus 文本规范：
// 每个指标含 # HELP / # TYPE，指标行格式 name{labels} value。
func TestM13MetricsRenderFormat(t *testing.T) {
	m := New()
	m.now = fixedNow(1700000000)
	out := m.Render()

	for _, want := range []string{
		"# HELP openxdb_queries_total Total queries executed.\n# TYPE openxdb_queries_total counter\nopenxdb_queries_total{type=\"sql\"} 0",
		"# HELP openxdb_connections_active Current active TCP connections.\n# TYPE openxdb_connections_active gauge\nopenxdb_connections_active 0",
		"# TYPE openxdb_region_count gauge\nopenxdb_region_count 0",
		"# TYPE openxdb_txn_commit_total counter\nopenxdb_txn_commit_total 0",
		"# TYPE openxdb_query_local_total counter\nopenxdb_query_local_total 0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("render missing block %q:\n%s", want, out)
		}
	}
	if !strings.HasPrefix(out, "# HELP openxdb_queries_total") {
		t.Fatalf("render should start with HELP block:\n%s", out)
	}
}

// TestM13MetricsCounters 计数器累加后渲染输出正确值。
func TestM13MetricsCounters(t *testing.T) {
	m := New()
	m.now = fixedNow(1700000000)
	m.IncSQL()
	m.IncSQL()
	m.IncKV()
	m.IncSlow()
	m.IncCommit()
	m.IncRollback()
	m.IncLocal()
	m.IncRemote()
	m.IncLocal()

	out := m.Render()
	for _, want := range []string{
		"openxdb_queries_total{type=\"sql\"} 2",
		"openxdb_queries_total{type=\"kv\"} 1",
		"openxdb_slow_query_total 1",
		"openxdb_txn_commit_total 1",
		"openxdb_txn_rollback_total 1",
		"openxdb_query_local_total 2",
		"openxdb_query_remote_total 1",
		"openxdb_qps{type=\"sql\"} 2",
		"openxdb_qps{type=\"kv\"} 1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("render missing %q:\n%s", want, out)
		}
	}
}

// TestM13MetricsQPSWindow 秒窗跨秒重置：同秒累加、跨秒后旧秒计数归零。
func TestM13MetricsQPSWindow(t *testing.T) {
	m := New()
	sec := int64(1700000000)
	m.now = fixedNow(sec)
	m.IncSQL()
	m.IncSQL()
	if v := m.sqlQPS.value(sec); v != 2 {
		t.Fatalf("same-second qps: got %d, want 2", v)
	}
	// 跨秒：新秒 bump 后旧秒窗口归零（渲染输出即时值）
	m.now = fixedNow(sec + 1)
	m.IncSQL()
	if v := m.sqlQPS.value(sec); v != 0 {
		t.Fatalf("old-second qps should reset, got %d", v)
	}
	if v := m.sqlQPS.value(sec + 1); v != 1 {
		t.Fatalf("new-second qps: got %d, want 1", v)
	}
	if !strings.Contains(m.Render(), "openxdb_qps{type=\"sql\"} 1") {
		t.Fatalf("render should show current-window qps:\n%s", m.Render())
	}
}

// TestM13MetricsInjectedSources 注入采集源后渲染输出实时值；未注入输出 0。
func TestM13MetricsInjectedSources(t *testing.T) {
	m := New()
	m.now = fixedNow(1700000000)
	out := m.Render()
	if !strings.Contains(out, "openxdb_connections_active 0") ||
		!strings.Contains(out, "openxdb_slow_query_threshold_ms 0") ||
		!strings.Contains(out, "openxdb_storage_bytes 0") {
		t.Fatalf("nil sources should render 0:\n%s", out)
	}

	m.SetConnSource(func() int64 { return 7 })
	m.SetSlowThresholdSource(func() int64 { return 250 })
	m.SetStorageBytesSource(func() int64 { return 1048576 })
	m.SetRegionStatsSource(func() RegionStats {
		return RegionStats{
			Total:   5,
			ByState: map[string]int64{"RUNNING": 3, "SPLITTING": 2},
			Local:   4,
			Remote:  1,
		}
	})
	out = m.Render()
	for _, want := range []string{
		"openxdb_connections_active 7",
		"openxdb_slow_query_threshold_ms 250",
		"openxdb_storage_bytes 1048576",
		"openxdb_region_count 5",
		"openxdb_region_state{state=\"RUNNING\"} 3",
		"openxdb_region_state{state=\"SPLITTING\"} 2",
		"openxdb_region_local 4",
		"openxdb_region_remote 1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("render missing %q:\n%s", want, out)
		}
	}
}

// TestM13MetricsLabelEscape region 状态标签含引号/反斜杠时按规范转义。
func TestM13MetricsLabelEscape(t *testing.T) {
	m := New()
	m.now = fixedNow(1700000000)
	m.SetRegionStatsSource(func() RegionStats {
		return RegionStats{ByState: map[string]int64{`we"ird\state`: 1}}
	})
	out := m.Render()
	if !strings.Contains(out, `openxdb_region_state{state="we\"ird\\state"} 1`) {
		t.Fatalf("label escape failed:\n%s", out)
	}
}

// TestM13MetricsConcurrentIncrement 并发递增线程安全：10*1000 次无竞态。
func TestM13MetricsConcurrentIncrement(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				m.IncSQL()
				m.IncKV()
			}
		}()
	}
	wg.Wait()
	if got := m.sqlTotal.Load(); got != 10000 {
		t.Fatalf("sql total: got %d, want 10000", got)
	}
	if got := m.kvTotal.Load(); got != 10000 {
		t.Fatalf("kv total: got %d, want 10000", got)
	}
}

// TestM13MetricsHandler HTTP 处理器：200 + text/plain; version=0.0.4。
func TestM13MetricsHandler(t *testing.T) {
	m := New()
	m.now = fixedNow(1700000000)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handler status: got %d, want 200", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") || !strings.Contains(ct, "version=0.0.4") {
		t.Fatalf("content-type: got %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "# HELP openxdb_queries_total") {
		t.Fatalf("handler body missing metrics:\n%s", rec.Body.String())
	}
}

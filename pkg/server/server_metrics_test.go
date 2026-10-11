package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/metrics"
)

// TestM13MetricsCommandNotEnabled 未装配指标时 METRICS 明确报错。
func TestM13MetricsCommandNotEnabled(t *testing.T) {
	s, closeFn := newTestSQLServer(t)
	defer closeFn()

	if got := mustResp(t, s, "METRICS"); !strings.Contains(got, "ERR metrics not enabled") {
		t.Fatalf("METRICS without registry: got %q", got)
	}
}

// TestM13MetricsCommandRender METRICS 命令输出 Prometheus 文本格式指标，
// SQL/KV/事务/节点计数器随真实命令递增。
func TestM13MetricsCommandRender(t *testing.T) {
	s, closeFn := newTestSQLServer(t)
	defer closeFn()

	m := metrics.New()
	m.SetConnSource(func() int64 { return s.ActiveConns() })
	s.SetMetrics(m)
	s.SQL.SetMetrics(m) // 与 cmdStart 装配一致：server 与 SQL 引擎共享同一 registry

	// KV 命令计数
	mustResp(t, s, "SET k1 v1")
	mustResp(t, s, "SET k2 v2")
	mustResp(t, s, "GET k1")
	mustResp(t, s, "DEL k1")
	// SQL 查询计数
	mustResp(t, s, "CREATE TABLE mt (id INT PRIMARY KEY, name TEXT)")
	mustResp(t, s, "INSERT INTO mt VALUES (1, 'a')")
	mustResp(t, s, "SELECT * FROM mt")
	// 事务计数（server 层 BEGIN/COMMIT/ROLLBACK 路由到 SQL 会话事务）
	mustResp(t, s, "BEGIN")
	mustResp(t, s, "COMMIT")
	mustResp(t, s, "BEGIN")
	mustResp(t, s, "ROLLBACK")
	// 活跃连接数（模拟 ServeListener 计数）
	atomic.StoreInt64(&s.conns, 3)

	out := mustResp(t, s, "METRICS")
	for _, want := range []string{
		"# HELP openxdb_queries_total Total queries executed.",
		"# TYPE openxdb_queries_total counter",
		`openxdb_queries_total{type="kv"} 4`,
		`openxdb_queries_total{type="sql"} 7`,
		"openxdb_connections_active 3",
		"openxdb_txn_commit_total 1",
		"openxdb_txn_rollback_total 1",
		"openxdb_region_count 0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("METRICS output missing %q:\n%s", want, out)
		}
	}
}

// TestM13MetricsHTTPHandler 内置 HTTP /metrics 端点返回 Prometheus 文本。
func TestM13MetricsHTTPHandler(t *testing.T) {
	m := metrics.New()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "version=0.0.4") {
		t.Fatalf("content-type: got %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "openxdb_queries_total{type=\"sql\"} 0") {
		t.Fatalf("body missing metric:\n%s", rec.Body.String())
	}
}

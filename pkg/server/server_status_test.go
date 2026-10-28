package server

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/metrics"
)

// TestM13StatusCommandText STATUS 聚合节点/region/存储/慢查询/事务/连接/QPS，
// 输出人类可读表格且不报错。
func TestM13StatusCommandText(t *testing.T) {
	s, closeFn := newTestSQLServer(t)
	defer closeFn()

	m := metrics.New()
	m.SetConnSource(func() int64 { return s.ActiveConns() })
	m.SetSlowThresholdSource(s.SQL.SlowQueryThreshold)
	m.SetStorageBytesSource(func() int64 { return 12345 })
	m.SetRegionStatsSource(s.SQL.MetricsRegionStats)
	s.SetMetrics(m)
	s.SQL.SetMetrics(m)

	// 造数据：SQL/KV/事务/慢查询
	mustResp(t, s, "SET k1 v1")
	mustResp(t, s, "CREATE TABLE st (id INT PRIMARY KEY, name TEXT)")
	mustResp(t, s, "INSERT INTO st VALUES (1, 'a')")
	mustResp(t, s, "BEGIN")
	mustResp(t, s, "COMMIT")
	atomic.StoreInt64(&s.conns, 3)

	out := mustResp(t, s, "STATUS")
	if strings.HasPrefix(out, "ERR") {
		t.Fatalf("STATUS failed: %q", out)
	}
	for _, want := range []string{
		"OPENXDB STATUS (aggregated snapshot)",
		"status        | ok",
		"connections   | 3",
		"storage_bytes | 12345",
		"transactions  | commit=1 rollback=0",
		"regions:",
		"slow_queries:",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("STATUS text missing %q:\n%s", want, out)
		}
	}
	// 节点：单机回退本节点（db.Open 已注入 cluster manager）
	if !strings.Contains(out, "DATA") || !strings.Contains(out, "UP") {
		t.Fatalf("STATUS text missing node row:\n%s", out)
	}
}

// TestM13StatusCommandJSON STATUS JSON 输出可解析的聚合快照且字段正确。
func TestM13StatusCommandJSON(t *testing.T) {
	s, closeFn := newTestSQLServer(t)
	defer closeFn()

	m := metrics.New()
	m.SetConnSource(func() int64 { return s.ActiveConns() })
	m.SetSlowThresholdSource(s.SQL.SlowQueryThreshold)
	m.SetStorageBytesSource(func() int64 { return 2048 })
	m.SetRegionStatsSource(s.SQL.MetricsRegionStats)
	s.SetMetrics(m)
	s.SQL.SetMetrics(m)

	mustResp(t, s, "SET k1 v1")
	mustResp(t, s, "CREATE TABLE stj (id INT PRIMARY KEY, name TEXT)")
	mustResp(t, s, "INSERT INTO stj VALUES (1, 'a')")
	atomic.StoreInt64(&s.conns, 5)

	out := mustResp(t, s, "STATUS JSON")
	if strings.HasPrefix(out, "ERR") {
		t.Fatalf("STATUS JSON failed: %q", out)
	}
	var rep statusReport
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("STATUS JSON unmarshal: %v\n%s", err, out)
	}
	if rep.Server != "openxdb" || rep.Status != "ok" {
		t.Fatalf("server/status fields: %+v", rep)
	}
	if rep.Connections != 5 || rep.StorageBytes != 2048 {
		t.Fatalf("conns/storage: %+v", rep)
	}
	if len(rep.Nodes) == 0 {
		t.Fatalf("nodes should include self node: %+v", rep)
	}
	// region 聚合与 SQL 源口径一致（普通表单 region 隐式不落盘，Total 可为 0，
	// 但字段必须原样转发而非凭空编造）。
	want := s.SQL.MetricsRegionStats()
	if rep.Regions.Total != want.Total || rep.Regions.Local != want.Local || rep.Regions.Remote != want.Remote {
		t.Fatalf("regions should mirror SQL source: got %+v want %+v", rep.Regions, want)
	}
	if rep.Time == "" {
		t.Fatalf("time missing: %+v", rep)
	}
}

// TestM13StatusCommandDegraded 未装配 metrics/SQL 时 STATUS 不整体失败：
// 返回 degraded 状态与降级警告，JSON 仍可解析。
func TestM13StatusCommandDegraded(t *testing.T) {
	s, closeFn := newTestSQLServer(t)
	defer closeFn()

	// 不装配 metrics（newTestSQLServer 默认 s.m == nil）
	out := mustResp(t, s, "STATUS JSON")
	if strings.HasPrefix(out, "ERR") {
		t.Fatalf("STATUS JSON should degrade not fail: %q", out)
	}
	var rep statusReport
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("STATUS JSON unmarshal: %v\n%s", err, out)
	}
	if rep.Status != "degraded" {
		t.Fatalf("expected degraded status, got %q: %+v", rep.Status, rep)
	}
	found := false
	for _, w := range rep.Warnings {
		if strings.Contains(w, "metrics registry unavailable") {
			found = true
		}
	}
	if !found {
		t.Fatalf("warnings should mention metrics registry: %+v", rep.Warnings)
	}
	if rep.Connections != 0 || rep.StorageBytes != 0 {
		t.Fatalf("unassembled metrics should zero out: %+v", rep)
	}
}

// TestM13StatusSlowSummary STATUS 慢查询摘要：阈值/启用/最近条数与明细。
func TestM13StatusSlowSummary(t *testing.T) {
	s, closeFn := newTestSQLServer(t)
	defer closeFn()

	m := metrics.New()
	m.SetConnSource(func() int64 { return s.ActiveConns() })
	m.SetSlowThresholdSource(s.SQL.SlowQueryThreshold)
	m.SetStorageBytesSource(func() int64 { return 0 })
	m.SetRegionStatsSource(s.SQL.MetricsRegionStats)
	s.SetMetrics(m)
	s.SQL.SetMetrics(m)

	mustResp(t, s, "CREATE TABLE sts (id INT PRIMARY KEY, name TEXT)")
	vals := make([]string, 200)
	for i := range vals {
		vals[i] = "(" + strings.Repeat("x", 10) + ")" // 占位由下方真实 INSERT 覆盖
	}
	// 造 500 行数据，保证 SELECT 跨阈值
	// （v1.16.0 C6 共享快照使 200 行全表扫描降至 ~1ms 阈值临界，偶发不记录，
	// 抬高数据量使慢查询判定稳定，与 server_slowlog_test.go 同口径）。
	rows := make([]string, 500)
	for i := range rows {
		rows[i] = "(" + itoa(i) + ", 'x')"
	}
	_ = vals
	mustResp(t, s, "INSERT INTO sts VALUES "+strings.Join(rows, ", "))
	mustResp(t, s, "SET slow_query_threshold = 1")
	mustResp(t, s, "SET slow_query_log = on")
	mustResp(t, s, "SELECT * FROM sts")

	out := mustResp(t, s, "STATUS")
	if !strings.Contains(out, "slow_queries:") ||
		!strings.Contains(out, "threshold_ms=1 enabled=true") {
		t.Fatalf("STATUS slow summary missing: %q", out)
	}
	// 最近条数 >= 1 且明细包含 SQL 片段
	if !strings.Contains(out, "SELECT * FROM sts") {
		t.Fatalf("STATUS slow summary missing recent detail: %q", out)
	}
}

// itoa 简单整数转字符串（避免引入 strconv import 之外的依赖）。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

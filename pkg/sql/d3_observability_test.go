package sql_test

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// ---- D3.1 EXPLAIN 输出增强：节点级耗时 / 行数估算（向后兼容追加字段） ----

func TestD3ExplainNodeStats(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")

	// 点查：节点级行数估算 rows=1 + 耗时字段 est_ms=
	rows := mustRows(t, e, "EXPLAIN SELECT name FROM users WHERE id = 1")
	if !planContains(rows, "point lookup on PK (id)") {
		t.Fatalf("explain point lookup prefix lost: %+v", rows)
	}
	if !planContains(rows, "rows=1") {
		t.Fatalf("point lookup: want rows=1 estimate, got %+v", rows)
	}
	if !planContains(rows, "projection: name rows=1") {
		t.Fatalf("projection: want rows=1 estimate, got %+v", rows)
	}
	for _, r := range rows {
		if len(r) > 0 && !strings.Contains(r[0].S, "est_ms=") {
			t.Fatalf("explain line missing est_ms field: %q", r[0].S)
		}
	}

	// 全表扫描：行数估算 = 表真实行数（3）
	rows = mustRows(t, e, "EXPLAIN SELECT * FROM users")
	if !planContains(rows, "table scan (full)") {
		t.Fatalf("explain full scan prefix lost: %+v", rows)
	}
	if !planContains(rows, "rows=3") {
		t.Fatalf("full scan: want rows=3 estimate, got %+v", rows)
	}

	// 索引扫描：行数估算 + 过滤/排序/投影级统计
	rows = mustRows(t, e, "EXPLAIN SELECT name FROM users WHERE age >= 25 ORDER BY name")
	if !planContains(rows, "index scan on") {
		t.Fatalf("explain index scan prefix lost: %+v", rows)
	}
	if !planContains(rows, "filter: age") || !planContains(rows, "rows=") {
		t.Fatalf("filter with stats: %+v", rows)
	}
	if !planContains(rows, "sort: name") || !planContains(rows, "est_ms=") {
		t.Fatalf("sort with stats: %+v", rows)
	}

	// 分组聚合 + limit/offset 级统计
	rows = mustRows(t, e, "EXPLAIN SELECT id, COUNT(*) FROM users GROUP BY id ORDER BY id LIMIT 2 OFFSET 1")
	if !planContains(rows, "group: id rows=") || !planContains(rows, "aggregate: yes rows=") {
		t.Fatalf("group/agg with stats: %+v", rows)
	}
	if !planContains(rows, "limit: 2 rows=") || !planContains(rows, "offset: 1 rows=") {
		t.Fatalf("limit/offset with stats: %+v", rows)
	}
}

func TestD3ExplainBackwardCompat(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	// 历史断言子串（TestP2Explain 等）在追加字段后仍应命中
	rows := mustRows(t, e, "EXPLAIN SELECT name FROM users WHERE id = 1")
	if !planContains(rows, "point lookup on PK (id)") {
		t.Fatalf("compat point lookup: %+v", rows)
	}
	rows = mustRows(t, e, "EXPLAIN SELECT name FROM users WHERE age >= 25")
	if !planContains(rows, "index scan on idx_age(age)") {
		t.Fatalf("compat index scan: %+v", rows)
	}
	rows = mustRows(t, e, "EXPLAIN SELECT name FROM users WHERE name LIKE 'a%'")
	if !planContains(rows, "table scan (full)") || !planContains(rows, "LIKE") {
		t.Fatalf("compat like full scan: %+v", rows)
	}
	rows = mustRows(t, e, "EXPLAIN SELECT age, COUNT(*) FROM users GROUP BY age")
	if !planContains(rows, "group: age") || !planContains(rows, "aggregate: yes") {
		t.Fatalf("compat group/agg: %+v", rows)
	}
	rows = mustRows(t, e, "EXPLAIN SELECT * FROM users ORDER BY age DESC LIMIT 1")
	if !planContains(rows, "sort: age DESC") || !planContains(rows, "limit: 1") {
		t.Fatalf("compat sort/limit: %+v", rows)
	}
	rows = mustRows(t, e, "EXPLAIN SELECT u.name FROM users u JOIN users v ON u.id = v.id")
	if !planContains(rows, "nested-loop join") {
		t.Fatalf("compat join: %+v", rows)
	}
}

// ---- D3.2 slowlog 闭环：阈值热配置 + 阈值区间汇总 TOP N 报表 ----

func TestD3SlowSummary(t *testing.T) {
	e := newEngine(t)
	// 准备阶段关闭阈值，避免大 INSERT 误记
	e.SetSlowThreshold(0)
	mustExec(t, e, "CREATE TABLE big (id INT, v TEXT, PRIMARY KEY (id))")
	var sb strings.Builder
	sb.WriteString("INSERT INTO big (id, v) VALUES ")
	for i := 1; i <= 300; i++ {
		if i > 1 {
			sb.WriteString(", ")
		}
		sb.WriteString("(" + itoaTest(i) + ", 'value-" + itoaTest(i) + "')")
	}
	mustExec(t, e, sb.String())

	// 阈值热配置生效（沿用 SetSlowThreshold 预留，运行时即时变更）
	e.SetSlowThreshold(1)
	for i := 0; i < 5; i++ {
		mustRows(t, e, "SELECT * FROM big WHERE id > 0")
	}
	recs := e.SlowQueries()
	if len(recs) == 0 {
		t.Fatalf("slow threshold: want >=1 slow query, got 0")
	}
	// 关闭阈值：避免后续 SHOW SLOWQUERIES 语句自身执行被统一 execTimed 埋点计入慢查询，
	// 污染 SUMMARY/TOP N 的数量断言（慢机器上 SHOW 本身耗时可能 >= 阈值）。
	e.SetSlowThreshold(0)

	// 默认 TOP 10 报表
	res := mustExec(t, e, "SHOW SLOWQUERIES SUMMARY")
	if len(res.Columns) != 2 || res.Columns[0] != "metric" || res.Columns[1] != "value" {
		t.Fatalf("summary columns: %+v", res.Columns)
	}
	kv := summaryKV(res)
	if _, ok := kv["summary.threshold_ms"]; !ok {
		t.Fatalf("summary missing threshold key: %+v", kv)
	}
	if _, ok := kv["summary.records"]; !ok {
		t.Fatalf("summary missing records key: %+v", kv)
	}
	if _, ok := kv["summary.avg_ms"]; !ok {
		t.Fatalf("summary missing avg key: %+v", kv)
	}
	if _, ok := kv["summary.max_ms"]; !ok {
		t.Fatalf("summary missing max key: %+v", kv)
	}
	bucketCount := 0
	for k := range kv {
		if strings.HasPrefix(k, "bucket.") {
			bucketCount++
			if !strings.Contains(kv[k], "count=") || !strings.Contains(kv[k], "max_ms=") {
				t.Fatalf("bucket %s malformed: %q", k, kv[k])
			}
		}
	}
	if bucketCount == 0 {
		t.Fatalf("summary missing bucket rows: %+v", kv)
	}
	// TOP N 明细：默认 10，但不超过记录数
	topCount := countTop(kv)
	want := len(recs)
	if want > 10 {
		want = 10
	}
	if topCount != want {
		t.Fatalf("summary top rows: want %d, got %d (%+v)", want, topCount, kv)
	}
	// TOP 明细按耗时降序
	if !topDescending(kv) {
		t.Fatalf("summary top not descending: %+v", kv)
	}

	// 显式 TOP 3
	res3 := mustExec(t, e, "SHOW SLOWQUERIES SUMMARY TOP 3")
	kv3 := summaryKV(res3)
	if countTop(kv3) != 3 {
		t.Fatalf("summary top 3: got %d (%+v)", countTop(kv3), kv3)
	}
	if _, ok := kv3["top.4"]; ok {
		t.Fatalf("summary top 3: unexpected top.4 (%+v)", kv3)
	}

	// TOP 0 按默认 10 处理
	res0 := mustExec(t, e, "SHOW SLOWQUERIES SUMMARY TOP 0")
	if countTop(summaryKV(res0)) != want {
		t.Fatalf("summary top 0: want default %d, got %+v", want, summaryKV(res0))
	}
}

func TestD3SlowSummaryParseErrors(t *testing.T) {
	e := newEngine(t)
	mustErr(t, e, "SHOW SLOWQUERIES SUMMARY TOP abc", "top n")
	mustErr(t, e, "SHOW SLOWQUERIES SUMMARY TOP", "top n")
}

// summaryKV 将 SHOW SLOWQUERIES SUMMARY 的 metric|value 结果转为 map。
// value 同时可能是 INT（summary.* 统计）或 TEXT（bucket./top. 明细）。
func summaryKV(res *sql.Result) map[string]string {
	kv := make(map[string]string)
	for _, r := range res.Rows {
		if r[1].Kind == "INT" {
			kv[r[0].S] = itoaTest(int(r[1].I))
		} else {
			kv[r[0].S] = r[1].S
		}
	}
	return kv
}

// countTop 统计 top.<rank> 明细条数。
func countTop(kv map[string]string) int {
	n := 0
	for k := range kv {
		if strings.HasPrefix(k, "top.") {
			n++
		}
	}
	return n
}

// topDescending 校验 top.<rank> 明细的耗时字段（duration_ms 前缀）按降序排列。
// 注意：TOP N 输出为最慢优先（耗时单调不增），并列耗时允许相等。
func topDescending(kv map[string]string) bool {
	var prev int64 = 1<<62 - 1 // 哨兵：极大值，首条恒通过
	for rank := 1; ; rank++ {
		v, ok := kv["top."+itoaTest(rank)]
		if !ok {
			break
		}
		idx := strings.Index(v, "ms|rows=")
		if idx <= 0 {
			return false
		}
		// 从行尾向前截取 duration_ms 数值（格式 time|user|Nms|rows=...）
		start := strings.LastIndex(v[:idx], "|")
		var d int64
		for _, c := range v[start+1 : idx] {
			if c < '0' || c > '9' {
				return false
			}
			d = d*10 + int64(c-'0')
		}
		if d > prev {
			return false
		}
		prev = d
	}
	return true
}

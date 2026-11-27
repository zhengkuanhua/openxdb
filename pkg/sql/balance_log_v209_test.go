package sql

// v2.0.9 P3 / 缺陷 B3：BALANCE 两处打点 spread 语义一致性回归用例。
//
// 背景：v2.0.8 的 logBalanceRound 把 spread 打成「本节点 - 最闲节点」，
// logBalanceSummary 把 spread 打成「全节点极差 max-min」，同一字段名两种语义，
// 运维按 spread 读收敛度会误判。P3 统一为「spread 只表示全局极差」，
// 本节点差值改由独立字段 local_gap 输出，converged 统一引用 balanceConvergedSpread。
// 本用例断言两处日志的字段口径与新定义一致。

import (
	"bytes"
	"log"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// v209BalLogCapture 把标准 log 输出临时重定向到缓冲区，返回该缓冲区。
func v209BalLogCapture(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	oldWriter, oldFlags := log.Writer(), log.Flags()
	log.SetOutput(buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})
	return buf
}

var v209BalLogNumRE = regexp.MustCompile(`([A-Za-z_]+)=([^\s,}]+)`)

// v209BalLogParse 把一行打点拆成「头部字段表」与「nodes 计数表」。
// nodes={a=1,b=2} 里的节点计数单独解析，避免与头部同名字段互相污染。
func v209BalLogParse(t *testing.T, line string) (map[string]string, map[string]int) {
	t.Helper()
	head := line
	nodes := map[string]int{}
	if i := strings.Index(line, " nodes={"); i >= 0 {
		head = line[:i]
		body := line[i+len(" nodes={"):]
		body = strings.TrimSuffix(body, "}")
		for _, kv := range strings.Split(body, ",") {
			if kv == "" {
				continue
			}
			eq := strings.Index(kv, "=")
			if eq < 0 {
				t.Fatalf("bad nodes entry %q in %q", kv, line)
			}
			n, err := strconv.Atoi(kv[eq+1:])
			if err != nil {
				t.Fatalf("bad node count %q in %q", kv, line)
			}
			nodes[kv[:eq]] = n
		}
	}
	fields := map[string]string{}
	for _, m := range v209BalLogNumRE.FindAllStringSubmatch(head, -1) {
		fields[m[1]] = m[2]
	}
	return fields, nodes
}

// v209BalLogInt 取整数型字段，缺失即判失败。
func v209BalLogInt(t *testing.T, fields map[string]string, key, line string) int {
	t.Helper()
	raw, ok := fields[key]
	if !ok {
		t.Fatalf("log line %q missing field %q", line, key)
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("log line %q field %q = %q not an int", line, key, raw)
	}
	return v
}

// v209BalLogBool 取布尔型字段（fmt %v 输出 true/false）。
func v209BalLogBool(t *testing.T, fields map[string]string, key, line string) bool {
	t.Helper()
	raw, ok := fields[key]
	if !ok {
		t.Fatalf("log line %q missing field %q", line, key)
	}
	switch raw {
	case "true":
		return true
	case "false":
		return false
	}
	t.Fatalf("log line %q field %q = %q not a bool", line, key, raw)
	return false
}

// v209BalLogLines 按前缀筛出打点行。
func v209BalLogLines(buf *bytes.Buffer, prefix string) []string {
	out := []string{}
	for _, ln := range strings.Split(buf.String(), "\n") {
		if strings.Contains(ln, prefix) {
			out = append(out, ln)
		}
	}
	return out
}

// nodesRange 由 nodes 计数表重算 min / max / 极差，作为独立于被测代码的期望值来源。
func v209BalNodesRange(t *testing.T, nodes map[string]int) (minCount, maxCount int) {
	t.Helper()
	if len(nodes) == 0 {
		t.Fatalf("log line has empty nodes set")
	}
	first := true
	for _, c := range nodes {
		if first {
			minCount, maxCount, first = c, c, false
			continue
		}
		if c < minCount {
			minCount = c
		}
		if c > maxCount {
			maxCount = c
		}
	}
	return minCount, maxCount
}

// TestV209BalanceLogSpreadSemantics 断言两处打点的 spread 都是全局极差，本节点差值
// 走独立的 local_gap 字段，converged 引用 balanceConvergedSpread 而非硬编码 1。
//
// 关键反例：counts = {node-a:1, node-b:3, node-c:7} 时，
// 旧 logBalanceRound 的 spread 会打成 local-minRegions = 2，
// 新实现必须打成全局极差 6，同时 local_gap=2。
func TestV209BalanceLogSpreadSemantics(t *testing.T) {
	ex := &Executor{selfID: "node-b"}
	counts := map[string]int{"node-a": 1, "node-b": 3, "node-c": 7}

	buf := v209BalLogCapture(t)
	ex.logBalanceRound(counts, 3, 1, "migrate")
	ex.logBalanceSummary(2, counts, "max-migrations")

	rounds := v209BalLogLines(buf, "sql: balance round:")
	if len(rounds) != 1 {
		t.Fatalf("balance round log lines = %d, want 1 (%q)", len(rounds), buf.String())
	}
	summaries := v209BalLogLines(buf, "sql: balance summary:")
	if len(summaries) != 1 {
		t.Fatalf("balance summary log lines = %d, want 1 (%q)", len(summaries), buf.String())
	}

	// --- round ---
	rf, rnodes := v209BalLogParse(t, rounds[0])
	if got := v209BalLogInt(t, rf, "local", rounds[0]); got != 3 {
		t.Fatalf("round local = %d, want 3", got)
	}
	if got := v209BalLogInt(t, rf, "min", rounds[0]); got != 1 {
		t.Fatalf("round min = %d, want 1", got)
	}
	if got := v209BalLogInt(t, rf, "max", rounds[0]); got != 7 {
		t.Fatalf("round max = %d, want 7", got)
	}
	if got := v209BalLogInt(t, rf, "spread", rounds[0]); got != 6 {
		t.Fatalf("round spread = %d, want 6 (全局极差 max-min；旧实现会错打成 2 = local-最闲)", got)
	}
	if got := v209BalLogInt(t, rf, "local_gap", rounds[0]); got != 2 {
		t.Fatalf("round local_gap = %d, want 2 (本节点-候选最闲节点)", got)
	}
	if v209BalLogBool(t, rf, "converged", rounds[0]) {
		t.Fatalf("round converged = true, want false (spread 6 > %d)", balanceConvergedSpread)
	}
	if rf["action"] != "migrate" {
		t.Fatalf("round action = %q, want migrate", rf["action"])
	}
	if len(rnodes) != 3 || rnodes["node-a"] != 1 || rnodes["node-b"] != 3 || rnodes["node-c"] != 7 {
		t.Fatalf("round nodes = %v, want node-a=1 node-b=3 node-c=7", rnodes)
	}

	// --- summary：与 round 同口径 ---
	sf, snodes := v209BalLogParse(t, summaries[0])
	if got := v209BalLogInt(t, sf, "spread", summaries[0]); got != 6 {
		t.Fatalf("summary spread = %d, want 6", got)
	}
	if got := v209BalLogInt(t, sf, "local_gap", summaries[0]); got != 2 {
		t.Fatalf("summary local_gap = %d, want 2 (本节点-最小节点)", got)
	}
	if got := v209BalLogInt(t, sf, "min", summaries[0]); got != 1 {
		t.Fatalf("summary min = %d, want 1", got)
	}
	if got := v209BalLogInt(t, sf, "max", summaries[0]); got != 7 {
		t.Fatalf("summary max = %d, want 7", got)
	}
	if v209BalLogBool(t, sf, "converged", summaries[0]) {
		t.Fatalf("summary converged = true, want false (spread 6 > %d)", balanceConvergedSpread)
	}
	if sf["reason"] != "max-migrations" {
		t.Fatalf("summary reason = %q, want max-migrations", sf["reason"])
	}
	if got := v209BalLogInt(t, sf, "migrated", summaries[0]); got != 2 {
		t.Fatalf("summary migrated = %d, want 2", got)
	}
	if len(snodes) != 3 || snodes["node-c"] != 7 {
		t.Fatalf("summary nodes = %v, want {node-a:1 node-b:3 node-c:7}", snodes)
	}

	// 两处 spread 必须同源同值：同一 counts 下 round 与 summary 的 spread 相等。
	if rv, sv := v209BalLogInt(t, rf, "spread", rounds[0]), v209BalLogInt(t, sf, "spread", summaries[0]); rv != sv {
		t.Fatalf("spread inconsistent: round=%d summary=%d", rv, sv)
	}
}

// TestV209BalanceLogConvergedUsesConstant 断言 converged 判定严格引用
// balanceConvergedSpread：极差 == 该常量时算收敛，== 常量+1 时算未收敛。
// 旧 logBalanceRound 里硬编码的 `<= 1` 在阈值被调整后会与 summary 判定分叉，
// 本用例通过遍历阈值两侧封锁该分叉。
func TestV209BalanceLogConvergedUsesConstant(t *testing.T) {
	ex := &Executor{selfID: "node-a"}
	for _, spread := range []int{0, balanceConvergedSpread, balanceConvergedSpread + 1, balanceConvergedSpread + 3} {
		counts := map[string]int{"node-a": spread, "node-b": 0}
		buf := v209BalLogCapture(t)
		ex.logBalanceRound(counts, spread, 0, "skip-no-spread")
		ex.logBalanceSummary(0, counts, "converged")

		want := spread <= balanceConvergedSpread
		rf, _ := v209BalLogParse(t, v209BalLogLines(buf, "sql: balance round:")[0])
		sf, _ := v209BalLogParse(t, v209BalLogLines(buf, "sql: balance summary:")[0])
		if got := v209BalLogInt(t, rf, "spread", buf.String()); got != spread {
			t.Fatalf("round spread = %d, want %d", got, spread)
		}
		if got := v209BalLogBool(t, rf, "converged", buf.String()); got != want {
			t.Fatalf("round converged = %v, want %v (spread=%d threshold=%d)",
				got, want, spread, balanceConvergedSpread)
		}
		if got := v209BalLogBool(t, sf, "converged", buf.String()); got != want {
			t.Fatalf("summary converged = %v, want %v (spread=%d threshold=%d)",
				got, want, spread, balanceConvergedSpread)
		}
	}
}

// TestV209BalanceLogFieldsMatchCounts 端到端：真实三节点集群跑一条 BALANCE，
// 抓取全部打点行，逐行用 nodes={} 里的真实计数重算 min/max/spread 并比对字段，
// 防止字段口径只在单元层面成立、运行时仍是旧值。
func TestV209BalanceLogFieldsMatchCounts(t *testing.T) {
	addrB := v209StartNode(t, "node-b")
	addrC := v209StartNode(t, "node-c")
	eng, ex := v209Env(t, "node-a",
		v209Peer{id: "node-b", addr: addrB},
		v209Peer{id: "node-c", addr: addrC})

	regions := 5
	bounds := make([]int64, 0, regions-1)
	for i := 1; i < regions; i++ {
		bounds = append(bounds, int64(i))
	}
	v209CreateSplit(t, eng, "users", regions, bounds)

	buf := v209BalLogCapture(t)
	if _, err := eng.Execute("BALANCE"); err != nil {
		t.Fatalf("BALANCE: %v", err)
	}

	rounds := v209BalLogLines(buf, "sql: balance round:")
	summaries := v209BalLogLines(buf, "sql: balance summary:")
	if len(rounds) == 0 || len(summaries) == 0 {
		t.Fatalf("no balance log lines captured (rounds=%d summaries=%d, log=%q)",
			len(rounds), len(summaries), buf.String())
	}

	check := func(kind, line string) {
		fields, nodes := v209BalLogParse(t, line)
		minCount, maxCount := v209BalNodesRange(t, nodes)
		if got := v209BalLogInt(t, fields, "min", line); got != minCount {
			t.Fatalf("%s min = %d, want %d (nodes=%v)", kind, got, minCount, nodes)
		}
		if got := v209BalLogInt(t, fields, "max", line); got != maxCount {
			t.Fatalf("%s max = %d, want %d (nodes=%v)", kind, got, maxCount, nodes)
		}
		if got := v209BalLogInt(t, fields, "spread", line); got != maxCount-minCount {
			t.Fatalf("%s spread = %d, want 全局极差 %d (nodes=%v)", kind, got, maxCount-minCount, nodes)
		}
		wantConv := maxCount-minCount <= balanceConvergedSpread
		if got := v209BalLogBool(t, fields, "converged", line); got != wantConv {
			t.Fatalf("%s converged = %v, want %v (spread=%d threshold=%d)",
				kind, got, wantConv, maxCount-minCount, balanceConvergedSpread)
		}
		if kind == "summary" {
			if got := v209BalLogInt(t, fields, "local_gap", line); got != nodes[ex.selfID]-minCount {
				t.Fatalf("summary local_gap = %d, want %d (self=%d min=%d)",
					got, nodes[ex.selfID]-minCount, nodes[ex.selfID], minCount)
			}
		} else {
			// round 的 local_gap = local - 候选最闲；等于 local - min 的前提是
			// 候选集合覆盖了计数最小节点，这里只断言其为非负且与 local/min 自洽。
			if got := v209BalLogInt(t, fields, "local_gap", line); got < 0 {
				t.Fatalf("round local_gap = %d, want >= 0 (%s)", got, line)
			}
		}
	}
	for _, ln := range rounds {
		check("round", ln)
	}
	for _, ln := range summaries {
		check("summary", ln)
	}

	// 收敛后的分布本身也应满足「全局极差 <= 阈值」。
	counts := ex.balanceRegionCounts()
	if minC, maxC := v209BalNodesRange(t, counts); maxC-minC > balanceConvergedSpread {
		t.Fatalf("counts after BALANCE not converged: %v", counts)
	}
}

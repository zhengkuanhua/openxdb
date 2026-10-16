package sql

// T37 监控告警：评估引擎内部测试（状态机/持续时长防抖/通知出口）。
// 直接装配 AlertRulesEngine 并注入时钟与通知收集器，不依赖 db.Open。

import (
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/metrics"
)

// newTestAlertEngine 构造测试引擎：注入可拨动时钟 + 通知收集器。
// nowRef 为时钟指针（测试内通过 *nowRef = ... 拨动），events 收集通知
// "level:event:msg" 文本。
func newTestAlertEngine(t *testing.T) (ar *AlertRulesEngine, nowRef *time.Time, events *[]string) {
	t.Helper()
	ar = newAlertRulesEngine(nil)
	base := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	nowRef = &base
	events = &[]string{}
	ar.now = func() time.Time { return *nowRef }
	ar.notifier = func(level, module, event, msg string) {
		*events = append(*events, level+":"+event+":"+msg)
	}
	return ar, nowRef, events
}

// setTestRules 便捷装配两条规则（均 enabled）。
func setTestRules(ar *AlertRulesEngine, rules ...AlertRule) {
	for i := range rules {
		rules[i].Enabled = true
	}
	ar.SetRules(rules)
	ar.SetEnabled(true)
}

// TestAlertRuleParseValid 规则 JSON 解析：合法输入通过并保留字段。
func TestAlertRuleParseValid(t *testing.T) {
	js := `[{"id":"high-slow","metric":"slow_query_total","op":">","threshold":10,"duration_sec":0,"severity":"warn","enabled":true}]`
	rules, err := parseAlertRulesJSON(js)
	if err != nil {
		t.Fatalf("parse valid rules: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("want 1 rule, got %d", len(rules))
	}
	r := rules[0]
	if r.ID != "high-slow" || r.Metric != "slow_query_total" || r.Op != ">" ||
		r.Threshold != 10 || r.DurationSec != 0 || r.Severity != "warn" || !r.Enabled {
		t.Fatalf("rule fields mismatch: %+v", r)
	}
}

// TestAlertRuleParseEmpty 空串 = 清空规则（合法）。
func TestAlertRuleParseEmpty(t *testing.T) {
	rules, err := parseAlertRulesJSON("")
	if err != nil {
		t.Fatalf("parse empty: %v", err)
	}
	if len(rules) != 0 {
		t.Fatalf("want 0 rules, got %d", len(rules))
	}
}

// TestAlertRuleParseRejectsBad 非法规则整体报错：未知指标/非法 op/负阈值/
// 非法级别/重复 id/非 JSON。
func TestAlertRuleParseRejectsBad(t *testing.T) {
	cases := []struct {
		name string
		js   string
		want string
	}{
		{"not-json", `not-json`, "JSON"},
		{"unknown-metric", `[{"id":"r1","metric":"cpu_usage","op":">","threshold":1,"severity":"warn"}]`, "unsupported metric"},
		{"bad-op", `[{"id":"r1","metric":"active_conns","op":"==","threshold":1,"severity":"warn"}]`, "invalid op"},
		{"negative-threshold", `[{"id":"r1","metric":"active_conns","op":">","threshold":-1,"severity":"warn"}]`, "threshold"},
		{"bad-severity", `[{"id":"r1","metric":"active_conns","op":">","threshold":1,"severity":"fatal"}]`, "severity"},
		{"empty-id", `[{"id":"","metric":"active_conns","op":">","threshold":1,"severity":"warn"}]`, "id is required"},
		{"dup-id", `[{"id":"r1","metric":"active_conns","op":">","threshold":1,"severity":"warn"},{"id":"r1","metric":"sql_total","op":">","threshold":1,"severity":"warn"}]`, "duplicated"},
		{"bad-duration", `[{"id":"r1","metric":"active_conns","op":">","threshold":1,"duration_sec":-5,"severity":"warn"}]`, "duration_sec"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := parseAlertRulesJSON(c.js); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want error containing %q, got %v", c.want, err)
			}
		})
	}
}

// TestAlertRulesImmediateTrigger duration=0：越界立即 NORMAL→ALERT 并通知一次。
func TestAlertRulesImmediateTrigger(t *testing.T) {
	ar, _, events := newTestAlertEngine(t)
	setTestRules(ar, AlertRule{ID: "r1", Metric: "active_conns", Op: ">", Threshold: 100, Severity: "warn"})
	ar.Evaluate(metrics.Snapshot{ActiveConns: 150}, ar.now())
	if st := ar.Status()[0]; st.State != string(ruleStateAlert) {
		t.Fatalf("state: want ALERT, got %s", st.State)
	}
	if len(*events) != 1 || !strings.Contains((*events)[0], "rule_triggered") || !strings.Contains((*events)[0], "warn:") {
		t.Fatalf("want one warn trigger event, got %v", *events)
	}
	// 持续越界：不重复通知。
	ar.Evaluate(metrics.Snapshot{ActiveConns: 200}, ar.now())
	if len(*events) != 1 {
		t.Fatalf("must not re-notify while alert persists, got %v", *events)
	}
}

// TestAlertRulesDurationDebounce duration>0：越界持续满 duration 才触发；
// 中途恢复则重置计时（NORMAL 防抖）。
func TestAlertRulesDurationDebounce(t *testing.T) {
	ar, nowRef, events := newTestAlertEngine(t)
	setTestRules(ar, AlertRule{ID: "r1", Metric: "active_conns", Op: ">", Threshold: 100, DurationSec: 5, Severity: "critical"})
	// t0 越界：未满 duration，不触发。
	ar.Evaluate(metrics.Snapshot{ActiveConns: 150}, *nowRef)
	if st := ar.Status()[0]; st.State != string(ruleStateNormal) {
		t.Fatalf("t0: want NORMAL, got %s", st.State)
	}
	if len(*events) != 0 {
		t.Fatalf("t0: no event expected, got %v", *events)
	}
	// t0+3s 仍越界：不满 5s，不触发。
	*nowRef = nowRef.Add(3 * time.Second)
	ar.Evaluate(metrics.Snapshot{ActiveConns: 160}, *nowRef)
	if len(*events) != 0 {
		t.Fatalf("t+3s: no event expected, got %v", *events)
	}
	// t0+5s 越界：满 duration，触发 ALERT。
	*nowRef = nowRef.Add(2 * time.Second)
	ar.Evaluate(metrics.Snapshot{ActiveConns: 160}, *nowRef)
	if st := ar.Status()[0]; st.State != string(ruleStateAlert) {
		t.Fatalf("t+5s: want ALERT, got %s", st.State)
	}
	if len(*events) != 1 || !strings.Contains((*events)[0], "rule_triggered") || !strings.Contains((*events)[0], "critical:") {
		t.Fatalf("t+5s: want one critical trigger, got %v", *events)
	}
}

// TestAlertRulesRecoverResolved ALERT 后恢复：转 RESOLVED 并发一次性恢复通知。
func TestAlertRulesRecoverResolved(t *testing.T) {
	ar, nowRef, events := newTestAlertEngine(t)
	setTestRules(ar, AlertRule{ID: "r1", Metric: "active_conns", Op: ">", Threshold: 100, Severity: "warn"})
	ar.Evaluate(metrics.Snapshot{ActiveConns: 150}, *nowRef)
	*nowRef = nowRef.Add(10 * time.Second)
	ar.Evaluate(metrics.Snapshot{ActiveConns: 80}, *nowRef)
	if st := ar.Status()[0]; st.State != string(ruleStateResolved) {
		t.Fatalf("want RESOLVED, got %s", st.State)
	}
	if len(*events) != 2 {
		t.Fatalf("want trigger+resolved events, got %v", *events)
	}
	if !strings.Contains((*events)[1], "rule_resolved") || !strings.Contains((*events)[1], "info:") {
		t.Fatalf("resolved event: want info rule_resolved, got %v", *events)
	}
	// 持续正常：不重复恢复通知。
	ar.Evaluate(metrics.Snapshot{ActiveConns: 80}, ar.now())
	if len(*events) != 2 {
		t.Fatalf("must not re-notify resolved, got %v", *events)
	}
}

// TestAlertRulesRecoverThenReviolate RESOLVED 后再次越界：回 NORMAL 重新计时
// （防抖），不直接 ALERT。
func TestAlertRulesRecoverThenReviolate(t *testing.T) {
	ar, nowRef, events := newTestAlertEngine(t)
	setTestRules(ar, AlertRule{ID: "r1", Metric: "active_conns", Op: ">", Threshold: 100, DurationSec: 5, Severity: "warn"})
	ar.Evaluate(metrics.Snapshot{ActiveConns: 150}, *nowRef) // NORMAL 计时
	*nowRef = nowRef.Add(5 * time.Second)
	ar.Evaluate(metrics.Snapshot{ActiveConns: 150}, *nowRef) // ALERT
	*nowRef = nowRef.Add(1 * time.Second)
	ar.Evaluate(metrics.Snapshot{ActiveConns: 80}, *nowRef) // RESOLVED
	// 恢复后 1s 再次越界：回 NORMAL 重新计时，未满 duration 不触发。
	*nowRef = nowRef.Add(1 * time.Second)
	ar.Evaluate(metrics.Snapshot{ActiveConns: 150}, *nowRef)
	st := ar.Status()[0]
	if st.State != string(ruleStateNormal) {
		t.Fatalf("after reviolate: want NORMAL (re-debounce), got %s", st.State)
	}
	if len(*events) != 2 {
		t.Fatalf("want only trigger+resolved, no third event, got %v", *events)
	}
	// 重新计时满 5s 后再次 ALERT。
	*nowRef = nowRef.Add(5 * time.Second)
	ar.Evaluate(metrics.Snapshot{ActiveConns: 150}, *nowRef)
	if st := ar.Status()[0]; st.State != string(ruleStateAlert) {
		t.Fatalf("after re-debounce: want ALERT, got %s", st.State)
	}
	if len(*events) != 3 {
		t.Fatalf("want third trigger event, got %v", *events)
	}
}

// TestAlertRulesDisabledRule 规则级 enabled=false：不评估不通知，状态保持 NORMAL。
func TestAlertRulesDisabledRule(t *testing.T) {
	ar, _, events := newTestAlertEngine(t)
	ar.SetRules([]AlertRule{{ID: "r1", Metric: "active_conns", Op: ">", Threshold: 100, Severity: "warn", Enabled: false}})
	ar.SetEnabled(true)
	ar.Evaluate(metrics.Snapshot{ActiveConns: 150}, ar.now())
	if st := ar.Status()[0]; st.State != string(ruleStateNormal) {
		t.Fatalf("disabled rule: want NORMAL, got %s", st.State)
	}
	if len(*events) != 0 {
		t.Fatalf("disabled rule: no event expected, got %v", *events)
	}
}

// TestAlertRulesEngineDisabled 引擎级总开关 off：不评估不通知。
func TestAlertRulesEngineDisabled(t *testing.T) {
	ar, _, events := newTestAlertEngine(t)
	ar.SetRules([]AlertRule{{ID: "r1", Metric: "active_conns", Op: ">", Threshold: 100, Severity: "warn"}})
	ar.SetEnabled(false)
	ar.Evaluate(metrics.Snapshot{ActiveConns: 150}, ar.now())
	if len(*events) != 0 {
		t.Fatalf("engine disabled: no event expected, got %v", *events)
	}
}

// TestAlertRulesUnknownMetric 指标不在支持表：跳过规则不报错。
func TestAlertRulesUnknownMetric(t *testing.T) {
	ar, _, events := newTestAlertEngine(t)
	// 直接构造规则（绕过 JSON 校验）以验证运行时容错。
	ar.SetRules([]AlertRule{{ID: "r1", Metric: "cpu_usage", Op: ">", Threshold: 1, Severity: "warn"}})
	ar.SetEnabled(true)
	ar.Evaluate(metrics.Snapshot{}, ar.now())
	if len(*events) != 0 {
		t.Fatalf("unknown metric: no event expected, got %v", *events)
	}
}

// TestAlertRulesStatusValue SHOW ALERT RULES 数据：last_value/has_value/state。
func TestAlertRulesStatusValue(t *testing.T) {
	ar, _, _ := newTestAlertEngine(t)
	setTestRules(ar, AlertRule{ID: "r1", Metric: "sql_total", Op: ">", Threshold: 100, Severity: "warn"})
	// 未评估：has_value=false。
	if st := ar.Status()[0]; st.HasValue {
		t.Fatalf("before eval: has_value must be false")
	}
	ar.Evaluate(metrics.Snapshot{SQLTotal: 500}, ar.now())
	st := ar.Status()[0]
	if !st.HasValue || st.LastValue != 500 || st.State != string(ruleStateAlert) {
		t.Fatalf("after eval: want has_value=true value=500 state=ALERT, got %+v", st)
	}
}

// TestAlertRulesStartStop 启动/停止循环幂等，Start 后配置变更唤醒评估。
func TestAlertRulesStartStop(t *testing.T) {
	ar, _, events := newTestAlertEngine(t)
	ar.Start()
	ar.Start() // 幂等
	setTestRules(ar, AlertRule{ID: "r1", Metric: "active_conns", Op: ">", Threshold: 100, Severity: "warn"})
	ar.SetRules([]AlertRule{{ID: "r1", Metric: "active_conns", Op: ">", Threshold: 100, Severity: "warn"}})
	ar.SetEnabled(true)
	// maybeEval 经 wake 触发；但默认 interval 未设（0=跳过）。设置后验证。
	ar.SetEvalInterval(1)
	// 循环中 maybeEval 依赖 snapshot；此处未注入 snapshot，仅验证无 panic/幂等。
	ar.Stop()
	ar.Stop()
	if len(*events) != 0 {
		t.Fatalf("no snapshot: no events expected, got %v", *events)
	}
}

package sql_test

// T37 监控告警：SQL 层集成测试（配置热更新 / 持久化 / SHOW 语句 / 触发通知）。
// 覆盖：SET alert_rules* 即时生效、非法值拒绝、SET PERSIST 落盘重启生效、
// SHOW ALERT RULES / SHOW ALERT STATUS 输出、手动评估触发后 SHOW ALERTS 可见。

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/metrics"
)

// initAlertEngineDir 初始化数据目录并注入 metrics（告警评估数据源，db.Open
// 不自动装配；注入后规则评估可手动触发）。
func initAlertEngineDir(t *testing.T) (string, *db.DB) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d := openEngineDir(t, dir)
	m := metrics.New()
	m.SetConnSource(func() int64 { return 5 }) // active_conns 恒为 5，供触发测试
	d.SQL.SetMetrics(m)
	return dir, d
}

// TestAlertRulesConfigDefaults SHOW ALERT STATUS 规则告警行默认值：
// 总开关 off、周期 10、规则数 0。
func TestAlertRulesConfigDefaults(t *testing.T) {
	_, d := initAlertEngineDir(t)
	rows := mustRows(t, d.SQL, "SHOW ALERT STATUS")
	got := map[string]string{}
	for _, r := range rows {
		if len(r) >= 2 {
			got[r[0].String()] = r[1].String()
		}
	}
	for _, k := range []string{"alert_rules_enabled", "alert_eval_interval", "alert_rule_count"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("SHOW ALERT STATUS missing row %q: %v", k, got)
		}
	}
	if got["alert_rules_enabled"] != "off" || got["alert_eval_interval"] != "10" || got["alert_rule_count"] != "0" {
		t.Fatalf("defaults mismatch: %v", got)
	}
	// SHOW CONFIG 同样可见默认值。
	if v := cfgValue(t, d.SQL, "alert_rules_enabled"); v != "off" {
		t.Fatalf("alert_rules_enabled default: want off, got %q", v)
	}
	if v := cfgValue(t, d.SQL, "alert_eval_interval"); v != "10" {
		t.Fatalf("alert_eval_interval default: want 10, got %q", v)
	}
}

// TestAlertRulesSetSession SET 即时生效（session 来源），不持久化。
func TestAlertRulesSetSession(t *testing.T) {
	_, d := initAlertEngineDir(t)
	mustExec(t, d.SQL, "SET alert_rules_enabled = on")
	mustExec(t, d.SQL, "SET alert_eval_interval = 30")
	if v := cfgValue(t, d.SQL, "alert_rules_enabled"); v != "on" {
		t.Fatalf("enabled: want on, got %q", v)
	}
	if s := cfgSource(t, d.SQL, "alert_rules_enabled"); s != "session" {
		t.Fatalf("source: want session, got %q", s)
	}
	if v := cfgValue(t, d.SQL, "alert_eval_interval"); v != "30" {
		t.Fatalf("interval: want 30, got %q", v)
	}
	if v := cfgValue(t, d.SQL, "alert_rules_enabled"); v != "on" {
		t.Fatalf("enabled after interval set: want on, got %q", v)
	}
	// 未持久化。
	if m := persistedConfig(t, d.SQL); len(m) != 0 {
		t.Fatalf("session SET must not persist: got %v", m)
	}
}

// TestAlertRulesSetInvalid 非法值拒绝且保持原值。
func TestAlertRulesSetInvalid(t *testing.T) {
	_, d := initAlertEngineDir(t)
	mustExec(t, d.SQL, "SET alert_eval_interval = 30")
	if _, err := d.SQL.Execute("SET alert_eval_interval = 0"); err == nil {
		t.Fatalf("interval 0 must be rejected")
	}
	if _, err := d.SQL.Execute("SET alert_eval_interval = 99999"); err == nil {
		t.Fatalf("interval 99999 must be rejected")
	}
	if v := cfgValue(t, d.SQL, "alert_eval_interval"); v != "30" {
		t.Fatalf("interval after rejects: want 30, got %q", v)
	}
	if _, err := d.SQL.Execute("SET alert_rules_enabled = maybe"); err == nil {
		t.Fatalf("enabled maybe must be rejected")
	}
	if _, err := d.SQL.Execute("SET alert_rules = not-json"); err == nil {
		t.Fatalf("rules not-json must be rejected")
	}
}

// TestAlertRulesSetAndShowRules SET alert_rules 合法规则：SHOW ALERT RULES
// 展示规则与状态；空规则清空。
func TestAlertRulesSetAndShowRules(t *testing.T) {
	_, d := initAlertEngineDir(t)
	mustExec(t, d.SQL, `SET alert_rules = '[{"id":"high-slow","metric":"slow_query_total","op":">","threshold":10,"duration_sec":0,"severity":"warn","enabled":true}]'`)
	rows := mustRows(t, d.SQL, "SHOW ALERT RULES")
	if len(rows) != 1 {
		t.Fatalf("SHOW ALERT RULES: want 1 row, got %d", len(rows))
	}
	r := rows[0]
	fields := map[string]string{
		"id": r[0].String(), "metric": r[1].String(), "op": r[2].String(),
		"threshold": r[3].String(), "duration_sec": r[4].String(),
		"severity": r[5].String(), "enabled": r[6].String(), "state": r[7].String(),
	}
	if fields["id"] != "high-slow" || fields["metric"] != "slow_query_total" ||
		fields["op"] != ">" || fields["threshold"] != "10" || fields["duration_sec"] != "0" ||
		fields["severity"] != "warn" || fields["enabled"] != "on" || fields["state"] != "NORMAL" {
		t.Fatalf("rule row mismatch: %v", fields)
	}
	// last_value 未评估为空串。
	if r[8].String() != "" {
		t.Fatalf("last_value before eval: want empty, got %q", r[8].String())
	}
	// SHOW CONFIG 展示规则 JSON。
	if v := cfgValue(t, d.SQL, "alert_rules"); !strings.Contains(v, "high-slow") {
		t.Fatalf("SHOW CONFIG alert_rules: want contain high-slow, got %q", v)
	}
	// 清空规则。
	mustExec(t, d.SQL, "SET alert_rules = ''")
	if rows := mustRows(t, d.SQL, "SHOW ALERT RULES"); len(rows) != 0 {
		t.Fatalf("after clear: want 0 rows, got %d", len(rows))
	}
}

// TestAlertRulesInvalidRuleKeepsExisting 非法规则整体拒绝，已有规则保持不变。
func TestAlertRulesInvalidRuleKeepsExisting(t *testing.T) {
	_, d := initAlertEngineDir(t)
	mustExec(t, d.SQL, `SET alert_rules = '[{"id":"r1","metric":"active_conns","op":">","threshold":100,"severity":"warn"}]'`)
	if _, err := d.SQL.Execute(`SET alert_rules = '[{"id":"r2","metric":"cpu_usage","op":">","threshold":1,"severity":"warn"}]'`); err == nil {
		t.Fatalf("unknown metric rule must be rejected")
	}
	rows := mustRows(t, d.SQL, "SHOW ALERT RULES")
	if len(rows) != 1 || rows[0][0].String() != "r1" {
		t.Fatalf("existing rule must survive reject: got %v", rows)
	}
}

// TestAlertRulesPersist SET PERSIST alert_rules*：落盘并重启生效。
func TestAlertRulesPersist(t *testing.T) {
	dir, d := initAlertEngineDir(t)
	mustExec(t, d.SQL, "SET PERSIST alert_rules_enabled = on")
	mustExec(t, d.SQL, "SET PERSIST alert_eval_interval = 20")
	mustExec(t, d.SQL, `SET PERSIST alert_rules = '[{"id":"p1","metric":"active_conns","op":">","threshold":50,"duration_sec":3,"severity":"critical","enabled":true}]'`)
	// 落盘断言。
	m := persistedConfig(t, d.SQL)
	if m["alert_rules_enabled"] != "on" || m["alert_eval_interval"] != "20" {
		t.Fatalf("persisted values mismatch: %v", m)
	}
	if _, err := os.Stat(filepath.Join(dir, db.PersistedConfigFile)); err != nil {
		t.Fatalf("persisted file: %v", err)
	}
	// 重启：持久化生效。
	_ = d.Close()
	e2 := openEngineDir(t, dir).SQL
	if v := cfgValue(t, e2, "alert_rules_enabled"); v != "on" {
		t.Fatalf("after restart enabled: want on, got %q", v)
	}
	if v := cfgValue(t, e2, "alert_eval_interval"); v != "20" {
		t.Fatalf("after restart interval: want 20, got %q", v)
	}
	rows := mustRows(t, e2, "SHOW ALERT RULES")
	if len(rows) != 1 || rows[0][0].String() != "p1" {
		t.Fatalf("after restart rules: want p1, got %v", rows)
	}
	if s := cfgSource(t, e2, "alert_rules_enabled"); s != "persisted" {
		t.Fatalf("after restart source: want persisted, got %q", s)
	}
}

// TestAlertRulesEvalTriggerShowsAlerts 手动评估：规则触发后 SHOW ALERTS 出现
// rule_triggered 记录（数据源 logx 环形缓冲；level 为规则 severity）。
func TestAlertRulesEvalTriggerShowsAlerts(t *testing.T) {
	_, d := initAlertEngineDir(t)
	mustExec(t, d.SQL, "SET alert_rules_enabled = on")
	mustExec(t, d.SQL, "SET alert_eval_interval = 1")
	// active_conns 注入恒为 5，规则 >0 立即触发。
	mustExec(t, d.SQL, `SET alert_rules = '[{"id":"eval2","metric":"active_conns","op":">","threshold":0,"duration_sec":0,"severity":"critical","enabled":true}]'`)
	d.SQL.EvaluateAlertRulesNow()
	rows := mustRows(t, d.SQL, "SHOW ALERT RULES")
	if len(rows) != 1 {
		t.Fatalf("SHOW ALERT RULES: want 1 row, got %d", len(rows))
	}
	if rows[0][7].String() != "ALERT" {
		t.Fatalf("state after eval: want ALERT, got %q", rows[0][7].String())
	}
	alerts := d.SQL.RecentAlerts()
	found := false
	for _, a := range alerts {
		if a.Module == "alert-rules" && strings.Contains(a.Msg, "eval2") {
			found = true
			if a.Level != "critical" || a.Event != "rule_triggered" {
				t.Fatalf("alert record mismatch: %+v", a)
			}
		}
	}
	if !found {
		t.Fatalf("SHOW ALERTS must contain rule_triggered for eval2, got %+v", alerts)
	}
}

// TestAlertRulesStatusCount SHOW ALERT STATUS 规则数统计。
func TestAlertRulesStatusCount(t *testing.T) {
	_, d := initAlertEngineDir(t)
	mustExec(t, d.SQL, `SET alert_rules = '[{"id":"a","metric":"sql_total","op":">","threshold":1,"severity":"info"},{"id":"b","metric":"kv_total","op":">","threshold":1,"severity":"warn"}]'`)
	rows := mustRows(t, d.SQL, "SHOW ALERT STATUS")
	for _, r := range rows {
		if r[0].String() == "alert_rule_count" && r[1].String() != "2" {
			t.Fatalf("rule count: want 2, got %q", r[1].String())
		}
	}
}

package sql_test

// M23 配置文件与结构化日志告警体系：SHOW ALERTS / SHOW ALERT STATUS /
// SET log_level / SET alert_* 在线视图与动态调整。

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/logx"
)

// TestLogAlertShowStatus 验证 SHOW ALERT STATUS 默认值、SET log_level/alert_* 动态调整。
func TestLogAlertShowStatus(t *testing.T) {
	_, d := initEngineDir(t)
	e := d.SQL
	// 默认级别 info、keep 100、开关 off。
	res := mustRows(t, e, "SHOW ALERT STATUS")
	got := map[string]string{}
	for _, row := range res {
		if len(row) >= 2 {
			got[row[0].String()] = row[1].String()
		}
	}
	if got["level"] != "info" {
		t.Fatalf("level default: want info, got %q", got["level"])
	}
	if got["enabled"] != "off" {
		t.Fatalf("enabled default: want off, got %q", got["enabled"])
	}
	if got["keep"] != "100" {
		t.Fatalf("keep default: want 100, got %q", got["keep"])
	}
	// 动态调整日志级别（内存级，会话/进程内生效）。
	mustExec(t, e, "SET log_level = 'warn'")
	res = mustRows(t, e, "SHOW ALERT STATUS")
	for _, row := range res {
		if len(row) >= 2 && row[0].String() == "level" {
			if row[1].String() != "warn" {
				t.Fatalf("after SET log_level: want warn, got %q", row[1].String())
			}
		}
	}
	if lv := e.LogLevel(); lv != logx.LevelWarn {
		t.Fatalf("Engine.LogLevel: want warn, got %v", lv)
	}
	// 非法级别拒绝。
	mustErr(t, e, "SET log_level = 'noisy'", "invalid value for log_level")
	// 告警开关与保留条数。
	mustExec(t, e, "SET alert_enabled = 'on'")
	mustExec(t, e, "SET alert_keep = 7")
	cfg := e.AlertConfig()
	if !cfg.Enabled {
		t.Fatal("AlertConfig.Enabled: want true")
	}
	if cfg.Keep != 7 {
		t.Fatalf("AlertConfig.Keep: want 7, got %d", cfg.Keep)
	}
	res = mustRows(t, e, "SHOW ALERT STATUS")
	for _, row := range res {
		if len(row) >= 2 && row[0].String() == "keep" && row[1].String() != "7" {
			t.Fatalf("after SET alert_keep: want 7, got %q", row[1].String())
		}
	}
	// 非法布尔拒绝。
	mustErr(t, e, "SET alert_enabled = 'maybe'", "invalid")
}

// TestShowAlertsReflectsTriggered 验证 SHOW ALERTS 能查看到已触发的告警记录。
func TestShowAlertsReflectsTriggered(t *testing.T) {
	_, d := initEngineDir(t)
	e := d.SQL
	logx.DefaultAlerts.Alert("wal", "wal_recover_failed", "smoke alert")
	res := mustRows(t, e, "SHOW ALERTS")
	found := false
	for _, row := range res {
		if len(row) >= 4 && row[2].String() == "wal" && row[3].String() == "wal_recover_failed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("SHOW ALERTS missing triggered alert: %v", res)
	}
}

package configfile

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestParseBasics 验证解析：段/键/注释/引号/大小写不敏感/重复键覆盖。
func TestParseBasics(t *testing.T) {
	text := `# comment
; semicolon comment

[server]
port = 7788
data_dir = ./data
Alert_Enabled = off
webhook = 'http://h/x'

[server]
port = 7789

[sql]
query_cache = 1
`
	f, err := Parse(text)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(f.Sections) != 2 {
		t.Fatalf("want 2 sections, got %d", len(f.Sections))
	}
	if v, _ := f.Get("server", "port"); v != "7789" {
		t.Fatalf("port: want 7789 (last wins), got %q", v)
	}
	if v, _ := f.Get("SERVER", "alert_enabled"); v != "off" {
		t.Fatalf("alert_enabled (case-insensitive): want off, got %q", v)
	}
	if v, _ := f.Get("server", "webhook"); v != "http://h/x" {
		t.Fatalf("quoted value: want http://h/x, got %q", v)
	}
	if _, ok := f.Get("server", "data_dir"); !ok {
		t.Fatal("data_dir missing")
	}
}

// TestParseErrors 验证格式错误拒绝。
func TestParseErrors(t *testing.T) {
	for name, text := range map[string]string{
		"outside-section": "port = 7788\n",
		"malformed-head":  "[server\nport = 1\n",
		"empty":           "",
		"no-eq":           "[server]\nport\n",
	} {
		if _, err := Parse(text); err == nil {
			t.Fatalf("%s: Parse must fail", name)
		}
	}
}

// TestLoadAndValidate 验证 Load + Validate：未知段/未知键被拒绝。
func TestLoadAndValidate(t *testing.T) {
	f, err := Load(filepath.Join(t.TempDir(), "missing.conf"))
	if err == nil {
		t.Fatal("Load missing file must fail")
	}
	known := map[string]map[string]bool{
		"server": {"port": true},
		"sql":    {"query_cache": true},
	}
	f, err = Parse("[server]\nport = 7788\nportt = 1\n[unknown]\na = 1\n[sql]\nquery_cache = 1\n")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	errs := Validate(f, known)
	if len(errs) != 2 {
		t.Fatalf("want 2 errors (unknown key + unknown section), got %v", errs)
	}
	joined := strings.Join(errs, ";")
	if !strings.Contains(joined, `[server] unknown key "portt"`) || !strings.Contains(joined, "unknown section [unknown]") {
		t.Fatalf("unexpected errors: %v", errs)
	}
	// 合法配置通过。
	f2, _ := Parse("[server]\nport = 7788\n[sql]\nquery_cache = 1\n")
	if errs := Validate(f2, known); len(errs) != 0 {
		t.Fatalf("valid config must pass, got %v", errs)
	}
}

// TestValidateServerValues 验证 [server] 值格式校验。
func TestValidateServerValues(t *testing.T) {
	good := "[server]\nport = 7788\nreplica_port = 0\nmetrics_port = 0\nshutdown_timeout = 30\nlog_level = info\nalert_enabled = true\nalert_webhook = http://h/x\nalert_keep = 50\n"
	f, err := Parse(good)
	if err != nil {
		t.Fatalf("Parse good: %v", err)
	}
	if errs := ValidateServerValues(f); len(errs) != 0 {
		t.Fatalf("good config must pass, got %v", errs)
	}
	bad := "[server]\nport = 0\nreplica_port = -1\nmetrics_port = 70000\nshutdown_timeout = -3\nalert_keep = -1\nlog_level = noisy\nalert_enabled = maybe\nalert_webhook = ftp://x\n"
	f2, err := Parse(bad)
	if err != nil {
		t.Fatalf("Parse bad: %v", err)
	}
	errs := ValidateServerValues(f2)
	if len(errs) != 8 {
		t.Fatalf("want 8 errors, got %v", errs)
	}
	joined := strings.Join(errs, ";")
	for _, want := range []string{"port=0", "replica_port=-1", "metrics_port=70000", "shutdown_timeout=-3", "alert_keep=-1", "log_level=\"noisy\"", "alert_enabled=\"maybe\"", "alert_webhook=\"ftp://x\""} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing error for %s in %v", want, errs)
		}
	}
}

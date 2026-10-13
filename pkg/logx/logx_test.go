package logx

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// TestLoggerJSONLine 验证单行 JSON 输出：固定字段 time/level/module/event/msg + 动态 kv。
func TestLoggerJSONLine(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf)
	l.Info("db", "open", "data directory opened", "dir", "/tmp/x")
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("invalid JSON line: %v\n%s", err, buf.String())
	}
	for key, want := range map[string]string{
		"level": "info", "module": "db", "event": "open",
		"msg": "data directory opened", "dir": "/tmp/x",
	} {
		if m[key] != want {
			t.Fatalf("field %s: want %q, got %v", key, want, m[key])
		}
	}
	if m["time"] == nil {
		t.Fatal("missing time field")
	}
	if strings.Count(buf.String(), "\n") != 1 {
		t.Fatalf("want exactly 1 newline, got:\n%s", buf.String())
	}
}

// TestLoggerLevelFilter 验证级别过滤：低于当前级别的日志不输出。
func TestLoggerLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf)
	l.SetLevel(LevelWarn)
	l.Debug("db", "scan", "d")
	l.Info("db", "scan", "i")
	l.Warn("db", "scan", "w")
	l.Error("db", "scan", "e")
	got := buf.String()
	if strings.Contains(got, `"event":"scan","msg":"d"`) || strings.Contains(got, `"event":"scan","msg":"i"`) {
		t.Fatalf("debug/info must be filtered at warn level:\n%s", got)
	}
	if !strings.Contains(got, `"msg":"w"`) || !strings.Contains(got, `"msg":"e"`) {
		t.Fatalf("warn/error must be emitted:\n%s", got)
	}
}

// TestParseLevel 验证级别解析：大小写不敏感、支持 warning 别名、非法拒绝。
func TestParseLevel(t *testing.T) {
	cases := map[string]Level{
		"debug": LevelDebug, "INFO": LevelInfo, "Warn": LevelWarn,
		"warning": LevelWarn, "error": LevelError,
	}
	for s, want := range cases {
		got, err := ParseLevel(s)
		if err != nil || got != want {
			t.Fatalf("ParseLevel(%q) = %v, %v; want %v", s, got, err, want)
		}
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Fatal("ParseLevel(verbose) must fail")
	}
}

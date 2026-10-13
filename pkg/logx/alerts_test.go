package logx

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// withSilentDefault 将全局 Default 输出静音，避免测试向 stderr 输出 JSON；结束后恢复。
func withSilentDefault(t *testing.T) {
	t.Helper()
	old := Default.out
	Default.mu.Lock()
	Default.out = io.Discard
	Default.mu.Unlock()
	t.Cleanup(func() {
		Default.mu.Lock()
		Default.out = old
		Default.mu.Unlock()
	})
}

// TestAlertManagerKeepRing 验证内存环形缓冲：保留最近 keep 条，字段完整。
func TestAlertManagerKeepRing(t *testing.T) {
	withSilentDefault(t)
	m := NewAlertManager()
	m.Configure(AlertConfig{Keep: 2})
	for i := 0; i < 5; i++ {
		m.Alert("wal", "wal_recover_failed", "boom")
	}
	rec := m.Recent()
	if len(rec) != 2 {
		t.Fatalf("Recent len: want 2, got %d", len(rec))
	}
	if rec[0].Module != "wal" || rec[0].Level != "error" || rec[0].Time == "" || rec[0].Msg == "" {
		t.Fatalf("bad alert record: %+v", rec[0])
	}
}

// TestAlertManagerFileAndDisabled 验证落盘 JSON 行与开关：Enabled=false 后不再落盘。
func TestAlertManagerFileAndDisabled(t *testing.T) {
	withSilentDefault(t)
	dir := t.TempDir()
	fp := filepath.Join(dir, "alerts.jsonl")
	m := NewAlertManager()
	m.Configure(AlertConfig{Enabled: true, File: fp, Keep: 3})
	m.Alert("network", "listen_failed", "port busy")
	m.Alert("backup", "backup_failed", "disk full")
	m.Configure(AlertConfig{Enabled: false, File: fp, Keep: 3})
	m.Alert("sql", "txn_init_failed", "ignored")
	_ = m.Close()
	b, err := os.ReadFile(fp)
	if err != nil {
		t.Fatalf("read alert file: %v", err)
	}
	if strings.Count(string(b), "\n") != 2 {
		t.Fatalf("want 2 alert lines (disabled alert must not persist), got:\n%s", b)
	}
	var a Alert
	line := strings.Split(strings.TrimSpace(string(b)), "\n")[0]
	if err := json.Unmarshal([]byte(line), &a); err != nil {
		t.Fatalf("invalid alert line: %v\n%s", err, line)
	}
	if a.Event != "listen_failed" || a.Module != "network" {
		t.Fatalf("unexpected alert: %+v", a)
	}
}

// TestAlertManagerWebhook 验证异步 webhook 推送（本地 httptest 接收）。
func TestAlertManagerWebhook(t *testing.T) {
	withSilentDefault(t)
	got := make(chan Alert, 1)
	srv := newTestHook(t, got)
	m := NewAlertManager()
	m.Configure(AlertConfig{Enabled: true, Webhook: srv.URL, Keep: 5})
	m.Alert("storage", "storage_open_failed", "cannot open rocksdb")
	select {
	case a := <-got:
		if a.Event != "storage_open_failed" || a.Module != "storage" {
			t.Fatalf("unexpected webhook payload: %+v", a)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("webhook push timeout")
	}
}

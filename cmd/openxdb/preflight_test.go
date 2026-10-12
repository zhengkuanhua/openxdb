package main

// M16-3 preflight 升级前健康检查测试：PASS/FAIL 路径、升级 reopen
// 数据完整、JSON 输出。范式沿用 M16-2 main_shutdown_test.go
// （db.Init + db.Open + server.New + Handle）。

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/server"
)

// helperInitOpen 初始化并打开数据目录，返回已装配 server 的句柄。
func helperPreflightOpen(t *testing.T, dir string) (*db.DB, *server.Server) {
	t.Helper()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	s := server.New(d.Txn)
	s.SQL = d.SQL
	return d, s
}

// findCheck 按名称取检查项。
func findCheck(t *testing.T, rep *preflightReport, name string) *preflightCheck {
	t.Helper()
	for i := range rep.Checks {
		if rep.Checks[i].Name == name {
			return &rep.Checks[i]
		}
	}
	t.Fatalf("missing check %q in %+v", name, rep.Checks)
	return nil
}

// TestPreflightPass 已初始化且写入过数据的目录 → 全部 PASS，退出码 0，
// 数据格式版本识别为 1。
func TestPreflightPass(t *testing.T) {
	dir := t.TempDir()
	d, s := helperPreflightOpen(t, dir)
	if resp, err := s.Handle("SET k v1"); err != nil || resp != "OK" {
		t.Fatalf("SET: resp=%q err=%v", resp, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rep := runPreflightChecks(dir, "v0.20.0-alpha")
	if rep.ExitCode != 0 {
		t.Fatalf("want exit 0, got %d: %+v", rep.ExitCode, rep.Checks)
	}
	for _, c := range rep.Checks {
		if c.Status != pfPass {
			t.Fatalf("check %s want PASS, got %s: %s", c.Name, c.Status, c.Message)
		}
	}
	if rep.DataFormatVersion != 1 {
		t.Fatalf("dir format: want 1, got %d", rep.DataFormatVersion)
	}
}

// TestPreflightUpgradeReopenDataIntact 升级路径（旧数据目录被新版本打开）：
// preflight PASS 后 reopen，已提交数据完整、持久化配置（M16-1）保留。
func TestPreflightUpgradeReopenDataIntact(t *testing.T) {
	dir := t.TempDir()
	d, s := helperPreflightOpen(t, dir)
	for _, k := range []string{"a1", "a2", "a3"} {
		if resp, err := s.Handle("SET k" + k + " v" + k); err != nil || resp != "OK" {
			t.Fatalf("SET k%s: resp=%q err=%v", k, resp, err)
		}
	}
	// SET PERSIST（M16-1 能力）：升级后配置应保留。
	// 注：SET PERSIST 属 SQL 引擎语法（server 层 SET 路由为 KV 语义，
	// 不走 SQL 赋值），此处与 pkg/sql config_test 一致经 Engine 执行。
	if _, err := d.SQL.Execute("SET PERSIST slow_query_threshold = 250"); err != nil {
		t.Fatalf("SET PERSIST: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 升级前检查通过（旧数据可被当前版本打开）。
	rep := runPreflightChecks(dir, "")
	if rep.ExitCode != 0 {
		t.Fatalf("preflight before reopen: %d %+v", rep.ExitCode, rep.Checks)
	}
	// 升级后 reopen：新版本打开旧数据目录，数据完整。
	d2, err := db.Open(dir)
	if err != nil {
		t.Fatalf("reopen after upgrade: %v", err)
	}
	defer func() { _ = d2.Close() }()
	s2 := server.New(d2.Txn)
	s2.SQL = d2.SQL
	for _, k := range []string{"a1", "a2", "a3"} {
		if resp, err := s2.Handle("GET k" + k); err != nil || resp != "v"+k {
			t.Fatalf("GET k%s after reopen: resp=%q err=%v", k, resp, err)
		}
	}
	if got := d2.SQL.SlowQueryThreshold(); got != 250 {
		t.Fatalf("persisted config after reopen: want 250, got %d", got)
	}
}

// TestPreflightFailNotInitialized 未初始化目录 → data_dir FAIL，退出码 2。
func TestPreflightFailNotInitialized(t *testing.T) {
	dir := t.TempDir() // 未 Init
	rep := runPreflightChecks(dir, "")
	if rep.ExitCode != 2 {
		t.Fatalf("want exit 2, got %d", rep.ExitCode)
	}
	c := findCheck(t, rep, "data_dir")
	if c.Status != pfFail {
		t.Fatalf("data_dir want FAIL, got %s: %s", c.Status, c.Message)
	}
}

// TestPreflightFailCorruptPersistedConfig 持久化配置 JSON 损坏
// （M16-1 persisted.json）→ persisted_config FAIL，退出码 2。
func TestPreflightFailCorruptPersistedConfig(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, db.PersistedConfigFile), []byte("{bad json"), 0o644); err != nil {
		t.Fatalf("write bad persisted.json: %v", err)
	}
	rep := runPreflightChecks(dir, "")
	if rep.ExitCode != 2 {
		t.Fatalf("want exit 2, got %d: %+v", rep.ExitCode, rep.Checks)
	}
	c := findCheck(t, rep, "persisted_config")
	if c.Status != pfFail {
		t.Fatalf("persisted_config want FAIL, got %s: %s", c.Status, c.Message)
	}
}

// TestPreflightFailFutureDataFormat 数据目录格式版本高于当前二进制
// （未来版本数据）→ version_compat FAIL，退出码 2（升级前需先升级二进制）。
func TestPreflightFailFutureDataFormat(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, db.DataFormatVersionFile), []byte("999\n"), 0o644); err != nil {
		t.Fatalf("write future marker: %v", err)
	}
	rep := runPreflightChecks(dir, "v9")
	if rep.ExitCode != 2 {
		t.Fatalf("want exit 2, got %d: %+v", rep.ExitCode, rep.Checks)
	}
	c := findCheck(t, rep, "version_compat")
	if c.Status != pfFail {
		t.Fatalf("version_compat want FAIL, got %s: %s", c.Status, c.Message)
	}
	if !strings.Contains(c.Message, "v999") {
		t.Fatalf("version_compat message should mention dir format v999: %s", c.Message)
	}
}

// TestPreflightJSONOutput preflight 报告可序列化为 JSON 且含关键字段。
func TestPreflightJSONOutput(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	rep := runPreflightChecks(dir, "v0.20.0-alpha")
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["binary_version"] != version {
		t.Fatalf("json binary_version: want %q, got %v", version, m["binary_version"])
	}
	if m["data_dir"] != dir {
		t.Fatalf("json data_dir mismatch: %v", m["data_dir"])
	}
	checks, ok := m["checks"].([]any)
	if !ok || len(checks) < 4 {
		t.Fatalf("json checks: want >=4 items, got %v", m["checks"])
	}
	if m["target_version"] != "v0.20.0-alpha" {
		t.Fatalf("json target_version mismatch: %v", m["target_version"])
	}
}

// TestPreflightTextOutput 人类可读输出含 PREFLIGHT RESULT 与 PASS 标记。
func TestPreflightTextOutput(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	rep := runPreflightChecks(dir, "")
	rep.Generated = "fixed"
	out := capturePrint(func() { printPreflightText(rep) })
	if !strings.Contains(out, "PREFLIGHT RESULT: PASS") {
		t.Fatalf("text output missing result: %s", out)
	}
	if !strings.Contains(out, "[PASS]") {
		t.Fatalf("text output missing PASS checks: %s", out)
	}
	if !strings.Contains(out, fmt.Sprintf("data format v%d", db.DataFormatVersion)) {
		t.Fatalf("text output missing data format info: %s", out)
	}
}

// capturePrint 临时重定向 os.Stdout 捕获 f 的打印输出（测试断言用）。
func capturePrint(f func()) string {
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	os.Stdout = w
	f()
	_ = w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

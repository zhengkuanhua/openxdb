// 巡检工具 doctor 测试（T20 运维工具链）。
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// openHealthyDir 构造一个"健康"数据目录：init + Open（生成引擎）+ 启用复制（生成 binlog）。
func openHealthyDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	if err := d.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("d.Close: %v", err)
	}
	return dir
}

func hasCheck(t *testing.T, rep *doctorReport, name, status string) bool {
	t.Helper()
	for _, c := range rep.Checks {
		if c.Name == name && c.Status == status {
			return true
		}
	}
	return false
}

// TestDoctorHealthyDir：正常目录无错误、退出码为警告级（未传 --addr 跳过在线检查）。
func TestDoctorHealthyDir(t *testing.T) {
	dir := openHealthyDir(t)
	rep := runDoctorChecks(dir, "")
	if rep.ExitCode != 1 {
		t.Fatalf("healthy dir expect warn-only exit 1, got %d", rep.ExitCode)
	}
	for _, c := range rep.Checks {
		if c.Status == "error" {
			t.Fatalf("healthy dir unexpected error check %s: %s", c.Name, c.Message)
		}
	}
	if !hasCheck(t, rep, "rocksdb", "ok") {
		t.Fatalf("rocksdb check should be ok, checks=%+v", rep.Checks)
	}
	if !hasCheck(t, rep, "binlog", "ok") {
		t.Fatalf("binlog check should be ok (replication enabled), checks=%+v", rep.Checks)
	}
	if !hasCheck(t, rep, "metadata", "ok") {
		t.Fatalf("metadata check should be ok, checks=%+v", rep.Checks)
	}
}

// TestDoctorCorruptWAL：删除 WAL 文件 → 错误级退出码 2。
func TestDoctorCorruptWAL(t *testing.T) {
	dir := openHealthyDir(t)
	if err := os.Remove(filepath.Join(dir, db.WALFile)); err != nil {
		t.Fatalf("remove WAL: %v", err)
	}
	rep := runDoctorChecks(dir, "")
	if rep.ExitCode != 2 {
		t.Fatalf("corrupt WAL expect exit 2, got %d", rep.ExitCode)
	}
	if !hasCheck(t, rep, "wal", "error") {
		t.Fatalf("wal check should be error, checks=%+v", rep.Checks)
	}
}

// TestDoctorCorruptEngine：删除 RocksDB 引擎目录 → 错误级退出码 2。
func TestDoctorCorruptEngine(t *testing.T) {
	dir := openHealthyDir(t)
	if err := os.RemoveAll(filepath.Join(dir, db.DataSubDir)); err != nil {
		t.Fatalf("remove engine dir: %v", err)
	}
	rep := runDoctorChecks(dir, "")
	if rep.ExitCode != 2 {
		t.Fatalf("corrupt engine expect exit 2, got %d", rep.ExitCode)
	}
	if !hasCheck(t, rep, "rocksdb", "error") {
		t.Fatalf("rocksdb check should be error, checks=%+v", rep.Checks)
	}
}

// TestDoctorMissingDir：目录不存在 → 错误级退出码 2。
func TestDoctorMissingDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "does-not-exist")
	rep := runDoctorChecks(dir, "")
	if rep.ExitCode != 2 {
		t.Fatalf("missing dir expect exit 2, got %d", rep.ExitCode)
	}
	if !hasCheck(t, rep, "data_dir", "error") {
		t.Fatalf("data_dir check should be error, checks=%+v", rep.Checks)
	}
}

// TestDoctorJSONOutput：--json 报告可被解析且字段完整。
func TestDoctorJSONOutput(t *testing.T) {
	dir := openHealthyDir(t)
	rep := runDoctorChecks(dir, "")
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded doctorReport
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("json output not parseable: %v", err)
	}
	if len(decoded.Checks) == 0 {
		t.Fatalf("decoded report has no checks")
	}
	if decoded.ExitCode != rep.ExitCode {
		t.Fatalf("decoded exit code %d != %d", decoded.ExitCode, rep.ExitCode)
	}
	if decoded.DataDir != rep.DataDir {
		t.Fatalf("decoded data dir %q != %q", decoded.DataDir, rep.DataDir)
	}
}

// TestParseRowCount：文本表行数解析。
func TestParseRowCount(t *testing.T) {
	cases := map[string]int{
		"OK":                          0,
		"ERR unknown":                 0,
		"col1 | col2\n--- | ---\na | b\n(3 rows)": 3,
		"col1\n---\n(0 rows)":          0,
		"(7 rows)":                     7,
	}
	for in, want := range cases {
		if got := parseRowCount(in); got != want {
			t.Errorf("parseRowCount(%q)=%d want %d", in, got, want)
		}
	}
}

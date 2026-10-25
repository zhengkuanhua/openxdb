package sql

// P2 backup_schedule 专项测试（v1.10.0，docs/HA_BRAIN_SPLIT_DESIGN.md §8）：
// 备份调度器（pkg/sql/backup_schedule.go）触发 / 跳过 / 失败恢复 / 保留轮转
// 四类场景。触发路径复用真实 execBackup（btree 内存存储 + 空 binlog），
// 验证到点自动执行生成 openxdb_backup_*.bak 文件并更新状态。

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

// newBackupSchedTestEngine 构造注入内存存储的 Executor 及其备份调度器。
func newBackupSchedTestEngine(t *testing.T) (*Executor, *BackupScheduler) {
	t.Helper()
	e := NewExecutor(nil)
	e.SetBackupEnv(btree.NewKV(32), "")
	return e, e.backupSched
}

// waitBackupCount 轮询等待调度器成功执行次数达到 n（deadline 5s）。
func waitBackupCount(t *testing.T, bs *BackupScheduler, n int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if st := bs.Status(); st.RunCount >= n && st.LastError == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	st := bs.Status()
	t.Fatalf("backup run count = %d (want >= %d), last_err=%q", st.RunCount, n, st.LastError)
}

// TestBackupScheduleTriggerOnInterval 触发：周期开启后到点自动执行，
// 生成备份文件并记录状态（run_count/last_file/next_run 均更新）。
func TestBackupScheduleTriggerOnInterval(t *testing.T) {
	_, bs := newBackupSchedTestEngine(t)
	dir := t.TempDir()

	bs.Update(BackupScheduleConfig{IntervalSec: 1, Keep: 0, Dir: dir})
	if st := bs.Status(); !st.Enabled || st.NextRunAt == "" {
		t.Fatalf("schedule should be enabled with next_run set: %+v", st)
	}
	bs.Start()
	defer bs.Stop()

	waitBackupCount(t, bs, 1)
	st := bs.Status()
	if st.LastFile == "" {
		t.Fatal("last_file should be set after scheduled run")
	}
	if _, err := os.Stat(st.LastFile); err != nil {
		t.Fatalf("backup file not found on disk: %v", err)
	}
	if !strings.HasPrefix(filepath.Base(st.LastFile), backupSchedFilePrefix) {
		t.Fatalf("unexpected backup filename: %s", filepath.Base(st.LastFile))
	}
	if !strings.HasSuffix(st.LastFile, backupSchedFileSuffix) {
		t.Fatalf("backup file should end with %s: %s", backupSchedFileSuffix, st.LastFile)
	}
	if st.Running {
		t.Fatal("scheduler should not be running after completed run")
	}
	if st.LastRunAt == "" {
		t.Fatal("last_run should be set after scheduled run")
	}
}

// TestBackupScheduleSkipDisabled 跳过：interval=0（未开启）与未到点均不执行。
func TestBackupScheduleSkipDisabled(t *testing.T) {
	_, bs := newBackupSchedTestEngine(t)
	dir := t.TempDir()

	// 未开启（interval=0）：Start 后无任何执行，且无 next_run 计划。
	bs.Update(BackupScheduleConfig{IntervalSec: 0, Dir: dir})
	bs.Start()
	defer bs.Stop()
	time.Sleep(700 * time.Millisecond) // 覆盖至少一个 tick（500ms）
	st := bs.Status()
	if st.Enabled {
		t.Fatal("interval=0 should not enable schedule")
	}
	if st.NextRunAt != "" {
		t.Fatalf("interval=0 should clear next_run, got %q", st.NextRunAt)
	}
	if st.RunCount != 0 {
		t.Fatalf("disabled schedule should not run, run_count=%d", st.RunCount)
	}

	// 未到点（interval 大、next_run 在未来）：短等待内不执行。
	bs.Update(BackupScheduleConfig{IntervalSec: 3600, Dir: dir})
	time.Sleep(700 * time.Millisecond)
	st = bs.Status()
	if !st.Enabled {
		t.Fatal("interval>0 should enable schedule")
	}
	if st.RunCount != 0 {
		t.Fatalf("schedule should not run before next_run, run_count=%d", st.RunCount)
	}
}

// TestBackupScheduleFailureAndRecovery 失败与恢复：Dir 未配置 → 记录错误；
// 补齐 Dir 后再次执行成功 → last_error 清空、备份文件生成。
func TestBackupScheduleFailureAndRecovery(t *testing.T) {
	_, bs := newBackupSchedTestEngine(t)
	dir := t.TempDir()

	// 失败：备份目录未配置。
	bs.Update(BackupScheduleConfig{IntervalSec: 0, Keep: 0, Dir: ""})
	path, err := bs.runOnce()
	if err == nil {
		t.Fatalf("runOnce with empty dir should fail, got path %q", path)
	}
	st := bs.Status()
	if st.LastError == "" {
		t.Fatal("last_error should record failed run")
	}
	if st.LastFile != "" {
		t.Fatalf("failed run should not set last_file, got %q", st.LastFile)
	}
	if st.RunCount != 1 {
		t.Fatalf("failed run should count, run_count=%d", st.RunCount)
	}

	// 恢复：补齐 Dir 后手动执行成功，状态复位。
	bs.Update(BackupScheduleConfig{IntervalSec: 0, Keep: 0, Dir: dir})
	path, err = bs.runOnce()
	if err != nil {
		t.Fatalf("runOnce after dir configured: %v", err)
	}
	if path == "" {
		t.Fatal("recovered run should return backup path")
	}
	if _, serr := os.Stat(path); serr != nil {
		t.Fatalf("recovered backup file missing: %v", serr)
	}
	st = bs.Status()
	if st.LastError != "" {
		t.Fatalf("last_error should be cleared after success, got %q", st.LastError)
	}
	if st.LastFile != path {
		t.Fatalf("last_file = %q, want %q", st.LastFile, path)
	}
	if st.RunCount != 2 {
		t.Fatalf("successful run should count, run_count=%d", st.RunCount)
	}
}

// TestBackupScheduleRetainRotation 保留轮转：keep=2 时仅保留最近 2 份，
// 删除最旧 3 份（文件名时间戳前缀字典序 = 新旧序）；keep<=0 不删除。
func TestBackupScheduleRetainRotation(t *testing.T) {
	dir := t.TempDir()
	for _, ts := range []string{
		"20261001_000001", "20261002_000001", "20261003_000001",
		"20261004_000001", "20261005_000001",
	} {
		p := filepath.Join(dir, backupSchedFilePrefix+ts+backupSchedFileSuffix)
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatalf("seed backup %s: %v", p, err)
		}
	}
	// keep=0：不清理。
	if removed, err := retainBackups(dir, 0); err != nil || len(removed) != 0 {
		t.Fatalf("keep=0 should remove nothing: removed=%v err=%v", removed, err)
	}
	// keep=2：删除最旧 3 份，保留最新 2 份。
	removed, err := retainBackups(dir, 2)
	if err != nil {
		t.Fatalf("retainBackups: %v", err)
	}
	if len(removed) != 3 {
		t.Fatalf("keep=2 should remove 3 old files, removed=%v", removed)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, backupSchedFilePrefix+"*"+backupSchedFileSuffix))
	if len(matches) != 2 {
		t.Fatalf("after keep=2 rotation, want 2 files, got %d: %v", len(matches), matches)
	}
	if !strings.Contains(matches[0], "20261004") || !strings.Contains(matches[1], "20261005") {
		t.Fatalf("kept files should be the two newest: %v", matches)
	}
	// 再次轮转：len <= keep，不删除。
	if removed, err := retainBackups(dir, 2); err != nil || len(removed) != 0 {
		t.Fatalf("second retain with len<=keep should remove nothing: removed=%v err=%v", removed, err)
	}
}

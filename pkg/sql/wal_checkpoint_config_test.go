package sql_test

// v1.16.0 WAL 自动 checkpoint 的 SQL 配置层测试：
//   - SHOW CONFIG 默认值 / 来源
//   - SET wal_auto_checkpoint / wal_checkpoint_interval_ms / wal_checkpoint_max_mb 热更新
//   - SET PERSIST 重启后仍生效（配置注册表闭环）
//   - 热更新真实驱动后台自动截断（WAL 收敛）
//   - 未装配控制器时 SET wal_* 返回明确错误（零回归）

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// TestWalCheckpointConfigDefaults SHOW CONFIG 三个 wal_* 键默认值。
func TestWalCheckpointConfigDefaults(t *testing.T) {
	_, d := initEngineDir(t)
	e := d.SQL
	if v := cfgValue(t, e, "wal_auto_checkpoint"); v != "on" {
		t.Fatalf("wal_auto_checkpoint default: want on, got %q", v)
	}
	if v := cfgValue(t, e, "wal_checkpoint_interval_ms"); v != "60000" {
		t.Fatalf("wal_checkpoint_interval_ms default: want 60000, got %q", v)
	}
	if v := cfgValue(t, e, "wal_checkpoint_max_mb"); v != "64" {
		t.Fatalf("wal_checkpoint_max_mb default: want 64, got %q", v)
	}
	for _, k := range []string{"wal_auto_checkpoint", "wal_checkpoint_interval_ms", "wal_checkpoint_max_mb"} {
		if s := cfgSource(t, e, k); s != "default" {
			t.Fatalf("source of %s: want default, got %q", k, s)
		}
	}
}

// TestWalCheckpointConfigHotSet SET 热更新：三个键即时生效且来源为 session。
func TestWalCheckpointConfigHotSet(t *testing.T) {
	_, d := initEngineDir(t)
	e := d.SQL
	mustExec(t, e, "SET wal_auto_checkpoint = off")
	mustExec(t, e, "SET wal_checkpoint_interval_ms = 2000")
	mustExec(t, e, "SET wal_checkpoint_max_mb = 8")
	if v := cfgValue(t, e, "wal_auto_checkpoint"); v != "off" {
		t.Fatalf("wal_auto_checkpoint: want off, got %q", v)
	}
	if v := cfgValue(t, e, "wal_checkpoint_interval_ms"); v != "2000" {
		t.Fatalf("wal_checkpoint_interval_ms: want 2000, got %q", v)
	}
	if v := cfgValue(t, e, "wal_checkpoint_max_mb"); v != "8" {
		t.Fatalf("wal_checkpoint_max_mb: want 8, got %q", v)
	}
	// 非法值拒绝。
	for _, bad := range []string{"SET wal_auto_checkpoint = maybe", "SET wal_checkpoint_interval_ms = -5", "SET wal_checkpoint_max_mb = abc"} {
		if _, err := e.Execute(bad); err == nil {
			t.Fatalf("%s: want error, got nil", bad)
		}
	}
}

// TestWalCheckpointConfigPersist SET PERSIST 重启后仍生效。
func TestWalCheckpointConfigPersist(t *testing.T) {
	dir, d := initEngineDir(t)
	e := d.SQL
	mustExec(t, e, "SET PERSIST wal_checkpoint_interval_ms = 1500")
	mustExec(t, e, "SET PERSIST wal_checkpoint_max_mb = 2")
	if m := persistedConfig(t, e); m["wal_checkpoint_interval_ms"] != "1500" || m["wal_checkpoint_max_mb"] != "2" {
		t.Fatalf("persisted config: want interval=1500 max_mb=2, got %v", m)
	}
	_ = d.Close()
	e2 := openEngineDir(t, dir).SQL
	if v := cfgValue(t, e2, "wal_checkpoint_interval_ms"); v != "1500" {
		t.Fatalf("after restart interval: want 1500, got %q", v)
	}
	if v := cfgValue(t, e2, "wal_checkpoint_max_mb"); v != "2" {
		t.Fatalf("after restart max_mb: want 2, got %q", v)
	}
}

// TestWalCheckpointHotDrivesTruncate SET 热更新真实驱动后台自动截断：
// interval 调小 + max_mb=0（每次检查都截断）后，写入数据 WAL 自动收敛。
func TestWalCheckpointHotDrivesTruncate(t *testing.T) {
	dir, d := initEngineDir(t)
	e := d.SQL
	mustExec(t, e, "SET wal_checkpoint_interval_ms = 30")
	mustExec(t, e, "SET wal_checkpoint_max_mb = 0") // 每次检查都截断

	// 通过事务管理器写入一批提交（WAL 增长）。
	tx, err := d.Txn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := tx.Put([]byte("hk"), []byte("hv")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	walPath := filepath.Join(dir, db.WALFile)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fi, err := os.Stat(walPath)
		if err == nil && fi.Size() < 2048 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fi, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal.log: %v", err)
	}
	if fi.Size() >= 2048 {
		t.Fatalf("WAL not truncated by hot config: size=%d", fi.Size())
	}
	// 数据不丢（直接经存储层读取校验）。
	if v, err := d.Storage.Get([]byte("hk")); err != nil || string(v) != "hv" {
		t.Fatalf("value after truncate: want hv, got %q err=%v", v, err)
	}
}

// TestWalCheckpointConfigUnwired 未装配控制器时 SET wal_* 返回明确错误（零回归）。
func TestWalCheckpointConfigUnwired(t *testing.T) {
	e := sql.New(nil) // 模拟未装配 checkpoint 控制器的引擎
	for _, stmt := range []string{"SET wal_auto_checkpoint = off", "SET wal_checkpoint_interval_ms = 100", "SET wal_checkpoint_max_mb = 1"} {
		_, err := e.Execute(stmt)
		if err == nil {
			t.Fatalf("%s: want error (unwired), got nil", stmt)
		}
		if !strings.Contains(err.Error(), "requires checkpoint controller") {
			t.Fatalf("%s: want 'requires checkpoint controller' error, got %v", stmt, err)
		}
	}
}

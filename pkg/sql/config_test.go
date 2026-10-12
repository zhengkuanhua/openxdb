package sql_test

// M16-1 在线配置热更新测试（config.go / SET PERSIST / SHOW CONFIG）。
// 覆盖：默认值、会话生效不持久化、PERSIST 生效+落盘+重启仍生效、
// 非法键、非法值、类型错误、SHOW PERSISTED CONFIG、损坏持久化文件拒绝启动。

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// openReopenEngine 打开数据目录 dir 返回 DB；t.Cleanup 负责关闭。
func openEngineDir(t *testing.T, dir string) *db.DB {
	t.Helper()
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open(%s): %v", dir, err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// initEngineDir 初始化全新数据目录（Init + Open）。
func initEngineDir(t *testing.T) (string, *db.DB) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	return dir, openEngineDir(t, dir)
}

// cfgValue 执行 SHOW CONFIG 并返回指定变量当前值；不存在返回 ""。
func cfgValue(t *testing.T, e *sql.Engine, key string) string {
	t.Helper()
	res := mustRows(t, e, "SHOW CONFIG")
	for _, row := range res {
		if len(row) >= 2 && row[0].String() == key {
			return row[1].String()
		}
	}
	return ""
}

// cfgSource 执行 SHOW CONFIG 并返回指定变量来源（default/persisted/session）。
func cfgSource(t *testing.T, e *sql.Engine, key string) string {
	t.Helper()
	res := mustRows(t, e, "SHOW CONFIG")
	for _, row := range res {
		if len(row) >= 3 && row[0].String() == key {
			return row[2].String()
		}
	}
	return ""
}

// persistedConfig 执行 SHOW PERSISTED CONFIG 返回 键→值 映射。
func persistedConfig(t *testing.T, e *sql.Engine) map[string]string {
	t.Helper()
	res := mustRows(t, e, "SHOW PERSISTED CONFIG")
	m := map[string]string{}
	for _, row := range res {
		if len(row) >= 2 {
			m[row[0].String()] = row[1].String()
		}
	}
	return m
}

// TestHotConfigShowDefaults SHOW CONFIG 初始状态：真实默认值 + default 来源。
func TestHotConfigShowDefaults(t *testing.T) {
	_, d := initEngineDir(t)
	if v := cfgValue(t, d.SQL, "query_cache"); v != "off" {
		t.Fatalf("query_cache default: want off, got %q", v)
	}
	if v := cfgValue(t, d.SQL, "slow_query_threshold"); v != "100" {
		t.Fatalf("slow_query_threshold default: want 100, got %q", v)
	}
	if v := cfgValue(t, d.SQL, "slow_query_log"); v != "off" {
		t.Fatalf("slow_query_log default: want off, got %q", v)
	}
	for _, k := range []string{"query_cache", "slow_query_threshold", "slow_query_log"} {
		if s := cfgSource(t, d.SQL, k); s != "default" {
			t.Fatalf("source of %s: want default, got %q", k, s)
		}
	}
	// SHOW PERSISTED CONFIG 初始为空。
	if m := persistedConfig(t, d.SQL); len(m) != 0 {
		t.Fatalf("SHOW PERSISTED CONFIG initial: want empty, got %v", m)
	}
}

// TestHotConfigSetSessionNotPersisted SET（非 PERSIST）：当前会话/进程生效，
// 来源标记 session；重启后回退默认值，持久化文件不产生。
func TestHotConfigSetSessionNotPersisted(t *testing.T) {
	dir, d := initEngineDir(t)
	mustExec(t, d.SQL, "SET slow_query_threshold = 250")
	if v := cfgValue(t, d.SQL, "slow_query_threshold"); v != "250" {
		t.Fatalf("SET session: want 250, got %q", v)
	}
	if s := cfgSource(t, d.SQL, "slow_query_threshold"); s != "session" {
		t.Fatalf("source after SET: want session, got %q", s)
	}
	if m := persistedConfig(t, d.SQL); len(m) != 0 {
		t.Fatalf("session SET must not persist: got %v", m)
	}
	if _, err := os.Stat(filepath.Join(dir, db.PersistedConfigFile)); !os.IsNotExist(err) {
		t.Fatalf("session SET must not create persisted config file: %v", err)
	}
	// 模拟重启：重新 Open 后回退默认值。
	_ = d.Close()
	e2 := openEngineDir(t, dir).SQL
	if v := cfgValue(t, e2, "slow_query_threshold"); v != "100" {
		t.Fatalf("after restart: want default 100, got %q", v)
	}
}

// TestHotConfigSetPersistEffectiveAndReload SET PERSIST：立即生效 + 落盘 +
// 重启后仍生效（来源 persisted），覆盖 slow_query_threshold 与 query_cache。
func TestHotConfigSetPersistEffectiveAndReload(t *testing.T) {
	dir, d := initEngineDir(t)
	mustExec(t, d.SQL, "SET PERSIST slow_query_threshold = 750")
	mustExec(t, d.SQL, "SET PERSIST query_cache = on")
	// 立即生效。
	if v := cfgValue(t, d.SQL, "slow_query_threshold"); v != "750" {
		t.Fatalf("SET PERSIST immediate: want 750, got %q", v)
	}
	if v := cfgValue(t, d.SQL, "query_cache"); v != "on" {
		t.Fatalf("SET PERSIST query_cache immediate: want on, got %q", v)
	}
	// 来源标记 persisted；SHOW PERSISTED CONFIG 含两键。
	if s := cfgSource(t, d.SQL, "slow_query_threshold"); s != "persisted" {
		t.Fatalf("source after SET PERSIST: want persisted, got %q", s)
	}
	pc := persistedConfig(t, d.SQL)
	if pc["slow_query_threshold"] != "750" || pc["query_cache"] != "on" {
		t.Fatalf("SHOW PERSISTED CONFIG: want both keys, got %v", pc)
	}
	// 配置文件确实落盘。
	b, err := os.ReadFile(filepath.Join(dir, db.PersistedConfigFile))
	if err != nil {
		t.Fatalf("read persisted config file: %v", err)
	}
	if !strings.Contains(string(b), "slow_query_threshold") || !strings.Contains(string(b), "query_cache") {
		t.Fatalf("persisted config file content missing keys: %s", string(b))
	}
	// 模拟重启：重新 Open 后仍生效。
	_ = d.Close()
	e2 := openEngineDir(t, dir).SQL
	if v := cfgValue(t, e2, "slow_query_threshold"); v != "750" {
		t.Fatalf("after restart: want 750, got %q", v)
	}
	if s := cfgSource(t, e2, "slow_query_threshold"); s != "persisted" {
		t.Fatalf("source after restart: want persisted, got %q", s)
	}
	if v := cfgValue(t, e2, "query_cache"); v != "on" {
		t.Fatalf("after restart query_cache: want on, got %q", v)
	}
	// 持久化 slow_query_log 重启意图暴露给服务装配层。
	mustExec(t, e2, "SET PERSIST slow_query_log = on")
	if i := e2.PersistedSlowLogIntent(); i != 1 {
		t.Fatalf("PersistedSlowLogIntent: want 1, got %d", i)
	}
}

// TestHotConfigSetPersistSlowLogIntent SET PERSIST slow_query_log=off：
// 服务装配意图为 0（关闭落盘），重启遵循。
func TestHotConfigSetPersistSlowLogIntent(t *testing.T) {
	dir, d := initEngineDir(t)
	mustExec(t, d.SQL, "SET PERSIST slow_query_log = off")
	if i := d.SQL.PersistedSlowLogIntent(); i != 0 {
		t.Fatalf("PersistedSlowLogIntent(off): want 0, got %d", i)
	}
	_ = d.Close()
	e2 := openEngineDir(t, dir).SQL
	if i := e2.PersistedSlowLogIntent(); i != 0 {
		t.Fatalf("PersistedSlowLogIntent after restart: want 0, got %d", i)
	}
}

// TestHotConfigInvalidKey 非法键：SET 与 SET PERSIST 均明确报 unknown session variable，
// 且不落盘。
func TestHotConfigInvalidKey(t *testing.T) {
	_, d := initEngineDir(t)
	mustErr(t, d.SQL, "SET no_such_key = 1", "unknown session variable: no_such_key")
	mustErr(t, d.SQL, "SET PERSIST no_such_key = 1", "unknown session variable: no_such_key")
	if m := persistedConfig(t, d.SQL); len(m) != 0 {
		t.Fatalf("invalid key must not persist: got %v", m)
	}
}

// TestHotConfigInvalidValue 非法值/类型错误：明确报错且不落盘。
func TestHotConfigInvalidValue(t *testing.T) {
	dir, d := initEngineDir(t)
	mustErr(t, d.SQL, "SET query_cache = maybe", "invalid value for query_cache")
	mustErr(t, d.SQL, "SET slow_query_threshold = abc", "invalid value for slow_query_threshold")
	mustErr(t, d.SQL, "SET slow_query_log = banana", "invalid value for slow_query_log")
	// PERSIST 版本同样校验值：parser 能接受、apply 拒绝的类型错误明确报错。
	mustErr(t, d.SQL, "SET PERSIST slow_query_threshold = abc", "invalid value for slow_query_threshold")
	if m := persistedConfig(t, d.SQL); len(m) != 0 {
		t.Fatalf("invalid value must not persist: got %v", m)
	}
	if _, err := os.Stat(filepath.Join(dir, db.PersistedConfigFile)); !os.IsNotExist(err) {
		t.Fatalf("invalid value must not create file: %v", err)
	}
}

// TestHotConfigShowPersistedOnly SHOW PERSISTED CONFIG 只列出已持久化键；
// SHOW CONFIG 区分 session 与 persisted 来源。
func TestHotConfigShowPersistedOnly(t *testing.T) {
	_, d := initEngineDir(t)
	mustExec(t, d.SQL, "SET PERSIST query_cache = on")
	mustExec(t, d.SQL, "SET slow_query_threshold = 500")
	pc := persistedConfig(t, d.SQL)
	if len(pc) != 1 || pc["query_cache"] != "on" {
		t.Fatalf("SHOW PERSISTED CONFIG: want only query_cache=on, got %v", pc)
	}
	if s := cfgSource(t, d.SQL, "query_cache"); s != "persisted" {
		t.Fatalf("query_cache source: want persisted, got %q", s)
	}
	if s := cfgSource(t, d.SQL, "slow_query_threshold"); s != "session" {
		t.Fatalf("slow_query_threshold source: want session, got %q", s)
	}
	if s := cfgSource(t, d.SQL, "slow_query_log"); s != "default" {
		t.Fatalf("slow_query_log source: want default, got %q", s)
	}
}

// TestHotConfigCompatExistingSet 兼容既有 SET slow_query_threshold（M13）行为：
// 阈值变更即时生效（SHOW SLOWQUERIES 口径）。
func TestHotConfigCompatExistingSet(t *testing.T) {
	_, d := initEngineDir(t)
	mustExec(t, d.SQL, "CREATE TABLE t (id INT, PRIMARY KEY (id))")
	mustExec(t, d.SQL, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, d.SQL, "SET slow_query_threshold = 1")
	// 阈值已调整：Engine 读取口径与 SET 一致。
	if got := d.SQL.SlowQueryThreshold(); got != 1 {
		t.Fatalf("SlowQueryThreshold after SET: want 1, got %d", got)
	}
	// 非法值仍走原报错，未回归。
	mustErr(t, d.SQL, "SET slow_query_threshold = x", "invalid value for slow_query_threshold")
}

// TestHotConfigPersistRequiresPath 未装配持久化路径时 SET PERSIST 明确报错
// （db.Open 已装配；本测试显式清空模拟未装配场景）。
func TestHotConfigPersistRequiresPath(t *testing.T) {
	_, d := initEngineDir(t)
	d.SQL.SetPersistedConfigPath("")
	mustErr(t, d.SQL, "SET PERSIST slow_query_threshold = 100", "persisted config not available")
	// 非 PERSIST SET 不受影响。
	mustExec(t, d.SQL, "SET slow_query_threshold = 200")
}

// TestHotConfigCorruptFileRejected 损坏/非法持久化文件：Open 明确报错拒绝启动，
// 绝不静默忽略。
func TestHotConfigCorruptFileRejected(t *testing.T) {
	cases := []struct {
		name, content, wantSub string
	}{
		{"invalid-json", "{not-json", "persisted config"},
		{"unknown-key", `{"bogus_key": "1"}`, "unknown key"},
		{"invalid-value", `{"slow_query_threshold": "notanum"}`, "invalid value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := db.Init(dir); err != nil {
				t.Fatalf("db.Init: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, db.PersistedConfigFile), []byte(tc.content), 0o644); err != nil {
				t.Fatalf("write persisted config: %v", err)
			}
			_, err := db.Open(dir)
			if err == nil {
				t.Fatalf("db.Open with corrupt persisted config: want error containing %q, got nil", tc.wantSub)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("db.Open error: want containing %q, got %v", tc.wantSub, err)
			}
		})
	}
}

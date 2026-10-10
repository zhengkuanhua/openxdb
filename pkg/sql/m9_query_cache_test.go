package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// M9 子功能 2：查询缓存（SET query_cache + SELECT 结果缓存 + 写失效联动）。

// statsValue 从 SHOW STATS 结果中取指定 metric 的 value。
func statsValue(t *testing.T, e *sql.Engine, metric string) sql.Value {
	t.Helper()
	rows := mustRows(t, e, "SHOW STATS")
	for _, r := range rows {
		if r[0].S == metric {
			return r[1]
		}
	}
	t.Fatalf("metric %q not found in SHOW STATS", metric)
	return sql.Value{}
}

func intStats(t *testing.T, e *sql.Engine, metric string) int64 {
	t.Helper()
	v := statsValue(t, e, metric)
	if v.Kind != "INT" {
		t.Fatalf("metric %q: want INT, got %+v", metric, v)
	}
	return v.I
}

func TestQueryCacheDefaultOff(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "SELECT * FROM users")
	mustExec(t, e, "SELECT * FROM users")
	if v := statsValue(t, e, "query_cache_enabled"); v.S != "off" {
		t.Fatalf("default: want off, got %+v", v)
	}
	if h := intStats(t, e, "query_cache_hits"); h != 0 {
		t.Fatalf("default off: want 0 hits, got %d", h)
	}
}

func TestQueryCacheHitMiss(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "SET query_cache = on")
	if v := statsValue(t, e, "query_cache_enabled"); v.S != "on" {
		t.Fatalf("after SET on: want on, got %+v", v)
	}
	// 首次查询 miss 并写入缓存
	rows := mustRows(t, e, "SELECT name FROM users ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	if m := intStats(t, e, "query_cache_misses"); m != 1 {
		t.Fatalf("first query: want 1 miss, got %d", m)
	}
	// 第二次同 SQL 命中，结果一致
	rows2 := mustRows(t, e, "SELECT name FROM users ORDER BY id")
	if len(rows2) != 3 || rows2[0][0].S != "alice" {
		t.Fatalf("cached result mismatch: %+v", rows2)
	}
	if h := intStats(t, e, "query_cache_hits"); h != 1 {
		t.Fatalf("second query: want 1 hit, got %d", h)
	}
	// 关闭缓存后清空，再次查询 miss
	mustExec(t, e, "SET query_cache = off")
	mustExec(t, e, "SELECT name FROM users ORDER BY id")
	if v := statsValue(t, e, "query_cache_enabled"); v.S != "off" {
		t.Fatalf("after SET off: want off, got %+v", v)
	}
}

func TestQueryCacheNormalizeKey(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "SET query_cache = on")
	mustExec(t, e, "SELECT name FROM users ORDER BY id")
	// 大小写/空白不同的同语义 SQL 命中同一缓存条目
	mustExec(t, e, "  select  NAME from users  order by id; ")
	if h := intStats(t, e, "query_cache_hits"); h != 1 {
		t.Fatalf("normalized key: want 1 hit, got %d", h)
	}
}

func TestQueryCacheInvalidateOnDML(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "SET query_cache = on")
	mustExec(t, e, "SELECT name FROM users ORDER BY id")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	if n := intStats(t, e, "query_cache_invalidations"); n != 1 {
		t.Fatalf("after INSERT: want 1 invalidation, got %d", n)
	}
	// 缓存已失效：同 SQL 重新执行得到新数据（4 行）
	rows := mustRows(t, e, "SELECT name FROM users ORDER BY id")
	if len(rows) != 4 {
		t.Fatalf("after invalidation+re-run: want 4 rows, got %d", len(rows))
	}
	// UPDATE 也触发失效
	mustExec(t, e, "UPDATE users SET age = 99 WHERE id = 1")
	if n := intStats(t, e, "query_cache_invalidations"); n != 2 {
		t.Fatalf("after UPDATE: want 2 invalidations, got %d", n)
	}
	// DELETE 也触发失效
	mustExec(t, e, "DELETE FROM users WHERE id = 4")
	if n := intStats(t, e, "query_cache_invalidations"); n != 3 {
		t.Fatalf("after DELETE: want 3 invalidations, got %d", n)
	}
}

func TestQueryCacheInvalidateOnDDL(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "SET query_cache = on")
	mustExec(t, e, "SELECT id, name FROM users ORDER BY id")
	// ALTER TABLE 是 DDL：成功后失效缓存
	mustExec(t, e, "ALTER TABLE users ADD COLUMN score INT")
	if n := intStats(t, e, "query_cache_invalidations"); n != 1 {
		t.Fatalf("after ALTER: want 1 invalidation, got %d", n)
	}
	// CREATE TABLE / INSERT / DROP TABLE 均触发失效（累计 1+1+1+1=4）
	mustExec(t, e, "CREATE TABLE t2 (id INT PRIMARY KEY)")
	if n := intStats(t, e, "query_cache_invalidations"); n != 2 {
		t.Fatalf("after CREATE TABLE: want 2 invalidations, got %d", n)
	}
	mustExec(t, e, "INSERT INTO t2 (id) VALUES (1)")
	mustExec(t, e, "SELECT id FROM t2")
	mustExec(t, e, "DROP TABLE t2")
	if n := intStats(t, e, "query_cache_invalidations"); n != 4 {
		t.Fatalf("after DROP TABLE: want 4 invalidations, got %d", n)
	}
}

func TestSetQueryCacheParse(t *testing.T) {
	e := newEngine(t)
	// on/off/1/0 四种写法均可解析
	mustExec(t, e, "SET query_cache = 1")
	mustExec(t, e, "SET query_cache = 0")
	mustExec(t, e, "SET query_cache = true")
	mustExec(t, e, "SET query_cache = false")
	// 非法值 / 未知变量报错
	mustErr(t, e, "SET query_cache = maybe", "invalid value")
	mustErr(t, e, "SET nonexistent = 1", "unknown session variable")
}

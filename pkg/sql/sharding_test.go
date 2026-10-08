package sql_test

import (
	"encoding/binary"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// splitBoundary 构造 INT 主键分片边界（pkBytes 为 8 字节大端，与主键编码一致）。
func splitBoundary(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

// setupShardedUsers 建表插 10 行并按 id<4、4<=id<8、id>=8 分为 3 个显式 region。
func setupShardedUsers(t *testing.T, e *sql.Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustExec(t, e, "INSERT INTO users (id, name, age) VALUES ("+itoa(int64(i))+", 'u"+itoa(int64(i))+"', "+itoa(int64(20+i))+")")
	}
	if err := e.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [24]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func TestSplitTableListRegions(t *testing.T) {
	e := newEngine(t)
	setupShardedUsers(t, e)
	rs, err := e.ListRegions("users")
	if err != nil {
		t.Fatalf("ListRegions: %v", err)
	}
	if len(rs) != 3 {
		t.Fatalf("ListRegions: want 3 regions, got %d (%+v)", len(rs), rs)
	}
	for _, rg := range rs {
		if !rg.Explicit || !rg.Local || rg.State != sharding.StateActive {
			t.Fatalf("region %+v: expected explicit active local", rg)
		}
		if rg.TableID != 1 {
			t.Fatalf("region table id = %d, want 1", rg.TableID)
		}
	}
	// 未分片表返回隐式单 region
	mustExec(t, e, "CREATE TABLE plain (id INT, PRIMARY KEY (id))")
	rs, err = e.ListRegions("plain")
	if err != nil {
		t.Fatalf("ListRegions(plain): %v", err)
	}
	if len(rs) != 1 || rs[0].Explicit {
		t.Fatalf("plain table: want implicit single region, got %+v", rs)
	}
}

func TestSplitTableFullSelectConsistency(t *testing.T) {
	e := newEngine(t)
	// 未分片基线
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustExec(t, e, "INSERT INTO users (id, name, age) VALUES ("+itoa(int64(i))+", 'u"+itoa(int64(i))+"', "+itoa(int64(20+i))+")")
	}
	base := mustRows(t, e, "SELECT id FROM users")
	if len(base) != 10 {
		t.Fatalf("baseline: want 10 rows, got %d", len(base))
	}
	// split 后全量一致（跨 region 合并排序去重）
	if err := e.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	got := mustRows(t, e, "SELECT id FROM users")
	if len(got) != len(base) {
		t.Fatalf("after split: want %d rows, got %d", len(base), len(got))
	}
	for i := range base {
		if base[i][0].I != got[i][0].I {
			t.Fatalf("row %d: want %d, got %d (merge ordering broken)", i, base[i][0].I, got[i][0].I)
		}
	}
}

func TestSplitTablePointAndRange(t *testing.T) {
	e := newEngine(t)
	setupShardedUsers(t, e)
	// 点查：跨 region 边界两侧
	rows := mustRows(t, e, "SELECT name FROM users WHERE id = 1")
	if len(rows) != 1 || rows[0][0].S != "u1" {
		t.Fatalf("point id=1: %+v", rows)
	}
	rows = mustRows(t, e, "SELECT name FROM users WHERE id = 4")
	if len(rows) != 1 || rows[0][0].S != "u4" {
		t.Fatalf("point id=4 (StartKey): %+v", rows)
	}
	rows = mustRows(t, e, "SELECT name FROM users WHERE id = 10")
	if len(rows) != 1 || rows[0][0].S != "u10" {
		t.Fatalf("point id=10: %+v", rows)
	}
	// 范围查跨 3 个 region
	rows = mustRows(t, e, "SELECT id FROM users WHERE id >= 3 AND id < 9")
	want := []int64{3, 4, 5, 6, 7, 8}
	if len(rows) != len(want) {
		t.Fatalf("range: want %d rows, got %d (%+v)", len(want), len(rows), rows)
	}
	for i, w := range want {
		if rows[i][0].I != w {
			t.Fatalf("range row %d: want %d, got %d", i, w, rows[i][0].I)
		}
	}
	// ORDER BY 跨 region
	rows = mustRows(t, e, "SELECT id FROM users ORDER BY id DESC LIMIT 3")
	if len(rows) != 3 || rows[0][0].I != 10 || rows[1][0].I != 9 || rows[2][0].I != 8 {
		t.Fatalf("order by desc limit: %+v", rows)
	}
	// 聚合跨 region
	rows = mustRows(t, e, "SELECT COUNT(*) FROM users")
	if len(rows) != 1 || rows[0][0].I != 10 {
		t.Fatalf("count: %+v", rows)
	}
}

func TestSplitTableLocateRegion(t *testing.T) {
	e := newEngine(t)
	setupShardedUsers(t, e)
	rs, _ := e.ListRegions("users")
	// id=1 属于第一个 region（StartKey=nil）
	rid, err := e.LocateRegion("users", splitBoundary(1))
	if err != nil {
		t.Fatalf("LocateRegion(id=1): %v", err)
	}
	if rid != rs[0].RegionID {
		t.Fatalf("id=1 located %d, want %d", rid, rs[0].RegionID)
	}
	// id=4 属于第二个 region（StartKey 含）
	rid, err = e.LocateRegion("users", splitBoundary(4))
	if err != nil {
		t.Fatalf("LocateRegion(id=4): %v", err)
	}
	if rid != rs[1].RegionID {
		t.Fatalf("id=4 located %d, want %d", rid, rs[1].RegionID)
	}
	// id=10 属于第三个 region
	rid, err = e.LocateRegion("users", splitBoundary(10))
	if err != nil {
		t.Fatalf("LocateRegion(id=10): %v", err)
	}
	if rid != rs[2].RegionID {
		t.Fatalf("id=10 located %d, want %d", rid, rs[2].RegionID)
	}
	// 未分片表：隐式单 region
	mustExec(t, e, "CREATE TABLE plain (id INT, PRIMARY KEY (id))")
	if _, err := e.LocateRegion("plain", splitBoundary(42)); err != nil {
		t.Fatalf("LocateRegion(plain): %v", err)
	}
	// 不存在表：明确报错
	if _, err := e.LocateRegion("nope", splitBoundary(1)); err == nil || !strings.Contains(err.Error(), "not exists") {
		t.Fatalf("LocateRegion(nope): want table-not-exists error, got %v", err)
	}
}

func TestSplitTableWithIndex(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	for i := 1; i <= 10; i++ {
		mustExec(t, e, "INSERT INTO users (id, name, age) VALUES ("+itoa(int64(i))+", 'u"+itoa(int64(i))+"', "+itoa(int64(20+i))+")")
	}
	if err := e.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	// 索引查询（等值 + 范围）
	rows := mustRows(t, e, "SELECT id FROM users WHERE age = 24")
	if len(rows) != 1 || rows[0][0].I != 4 {
		t.Fatalf("index eq age=24: %+v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM users WHERE age >= 23 AND age < 27 ORDER BY id")
	want := []int64{3, 4, 5, 6}
	if len(rows) != len(want) {
		t.Fatalf("index range: want %d, got %d (%+v)", len(want), len(rows), rows)
	}
	for i, w := range want {
		if rows[i][0].I != w {
			t.Fatalf("index range row %d: want %d, got %d", i, w, rows[i][0].I)
		}
	}
	// split 后建新索引回填
	mustExec(t, e, "CREATE INDEX idx_name ON users (name)")
	rows = mustRows(t, e, "SELECT id FROM users WHERE name = 'u8'")
	if len(rows) != 1 || rows[0][0].I != 8 {
		t.Fatalf("backfilled index: %+v", rows)
	}
}

func TestSplitTableDMLRegression(t *testing.T) {
	e := newEngine(t)
	setupShardedUsers(t, e)
	// UPDATE 跨 region
	mustExec(t, e, "UPDATE users SET age = 99 WHERE id IN (2, 7)")
	rows := mustRows(t, e, "SELECT age FROM users WHERE id = 7")
	if len(rows) != 1 || rows[0][0].I != 99 {
		t.Fatalf("update age: %+v", rows)
	}
	// DELETE 跨 region
	mustExec(t, e, "DELETE FROM users WHERE id IN (3, 8)")
	rows = mustRows(t, e, "SELECT COUNT(*) FROM users")
	if len(rows) != 1 || rows[0][0].I != 8 {
		t.Fatalf("after delete count: %+v", rows)
	}
	// INSERT 到边界（region3 起始键 id=8 已被 DELETE 删除，可重新插入验证边界路由）
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (8, 'dup8', 28)")
	mustErr(t, e, "INSERT INTO users (id, name, age) VALUES (8, 'dup8', 28)", "duplicate primary key")
	rows = mustRows(t, e, "SELECT id FROM users WHERE id = 8")
	if len(rows) != 1 {
		t.Fatalf("insert boundary id=8: %+v", rows)
	}
}

func TestSplitTableDropTable(t *testing.T) {
	e := newEngine(t)
	setupShardedUsers(t, e)
	mustExec(t, e, "DROP TABLE users")
	// region 元数据随表清理
	if _, err := e.ListRegions("users"); err == nil || !strings.Contains(err.Error(), "not exists") {
		t.Fatalf("ListRegions after drop: want not-exists, got %v", err)
	}
	// 重建同表名可继续使用（新隐式单 region，seq 水位不冲突）
	mustExec(t, e, "CREATE TABLE users (id INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO users (id) VALUES (1)")
	rows := mustRows(t, e, "SELECT id FROM users")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("recreated table: %+v", rows)
	}
}

func TestSplitTableBoundaryValidation(t *testing.T) {
	e := newEngine(t)
	setupShardedUsers(t, e)
	if err := e.SplitTable("users", [][]byte{splitBoundary(8), splitBoundary(4)}); err == nil {
		t.Fatalf("non-ascending boundaries should error")
	}
	if err := e.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(4)}); err == nil {
		t.Fatalf("duplicate boundaries should error")
	}
}

func TestSplitTableEmptyTable(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	if err := e.SplitTable("users", [][]byte{splitBoundary(4)}); err != nil {
		t.Fatalf("split empty table: %v", err)
	}
	mustExec(t, e, "INSERT INTO users (id, name) VALUES (1, 'a'), (5, 'b')")
	rows := mustRows(t, e, "SELECT id FROM users ORDER BY id")
	if len(rows) != 2 || rows[0][0].I != 1 || rows[1][0].I != 5 {
		t.Fatalf("empty-split insert: %+v", rows)
	}
}

func TestRegionMetaPersistRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	e := d.SQL
	setupShardedUsers(t, e)
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// 重新打开：分片元数据落盘回读
	d2, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open(2): %v", err)
	}
	defer d2.Close()
	e2 := d2.SQL
	rs, err := e2.ListRegions("users")
	if err != nil {
		t.Fatalf("ListRegions after reopen: %v", err)
	}
	if len(rs) != 3 {
		t.Fatalf("after reopen: want 3 regions, got %d", len(rs))
	}
	rows := mustRows(t, e2, "SELECT COUNT(*) FROM users")
	if len(rows) != 1 || rows[0][0].I != 10 {
		t.Fatalf("after reopen count: %+v", rows)
	}
	rows = mustRows(t, e2, "SELECT id FROM users WHERE id = 4")
	if len(rows) != 1 || rows[0][0].I != 4 {
		t.Fatalf("after reopen point: %+v", rows)
	}
}

func TestSplitTableOrderingMerge(t *testing.T) {
	// 多 region 下 scanMerged 按内层键序合并，验证跨 region SELECT 全量排序正确。
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, PRIMARY KEY (id))")
	for i := 20; i >= 1; i-- {
		mustExec(t, e, "INSERT INTO t (id) VALUES ("+itoa(int64(i))+")")
	}
	if err := e.SplitTable("t", [][]byte{splitBoundary(5), splitBoundary(10), splitBoundary(15)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	rows := mustRows(t, e, "SELECT id FROM t")
	if len(rows) != 20 {
		t.Fatalf("merged rows = %d, want 20", len(rows))
	}
	for i := 0; i < 20; i++ {
		if rows[i][0].I != int64(i+1) {
			t.Fatalf("merged row %d: want %d, got %d", i, i+1, rows[i][0].I)
		}
	}
}

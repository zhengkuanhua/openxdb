package sql_test

// M7 Region 自动分裂与负载均衡：单机侧测试。
// 覆盖：写路径自动分裂触发与边界正确性、手动 SPLIT REGION 语句、
// 函数式 SplitRegionNow 入口、分裂后数据一致性与二级索引一致性、
// 未开集群时 BALANCE 语句拒绝。

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// m7Boundary INT 主键分片边界（8 字节大端，与主键编码一致）。
func m7Boundary(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

// m7TKey 返回含表前缀的内层行键（'s'+tableID:8B+内键），
// 与 RegionInfo.StartKey/EndKey 落盘格式一致（见 rowRanges 注释）。
func m7TKey(id int64) []byte {
	key := make([]byte, 0, 17)
	key = append(key, 's')
	key = binary.BigEndian.AppendUint64(key, 1) // users 表 id=1
	key = binary.BigEndian.AppendUint64(key, uint64(id))
	return key
}

// m7RIDs 返回表当前显式 region 的 RegionID 列表。
func m7RIDs(t *testing.T, e interface{ ListRegions(string) ([]sharding.RegionInfo, error) }, table string) []storage.ID {
	t.Helper()
	rs, err := e.ListRegions(table)
	if err != nil {
		t.Fatalf("ListRegions(%s): %v", table, err)
	}
	ids := make([]storage.ID, 0, len(rs))
	for _, r := range rs {
		if r.Explicit {
			ids = append(ids, r.RegionID)
		}
	}
	return ids
}

// TestM7AutoSplitTriggersOnWriteThreshold 写路径超阈值自动分裂：
// 开启阈值=20，累计写水位达到一半后检查，本地 region 行数达到 20 即沿数据中点
// 分裂为两个显式 region；分裂后全量读写一致、行按边界正确分布、继续写不误分裂。
func TestM7AutoSplitTriggersOnWriteThreshold(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	// 未开启自动分裂前：单隐式 region，不分裂
	rs, err := e.ListRegions("users")
	if err != nil {
		t.Fatalf("ListRegions: %v", err)
	}
	if len(rs) != 1 {
		t.Fatalf("before enable: want 1 region, got %d", len(rs))
	}

	e.SetAutoSplit(true, 20)
	// 再写 12 行（总 22）：水位累计到阈值一半(10)触发检查，行数 20 超阈值分裂
	for i := 11; i <= 22; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	ids := m7RIDs(t, e, "users")
	if len(ids) != 2 {
		t.Fatalf("auto split: want 2 explicit regions, got %d", len(ids))
	}
	rs, _ = e.ListRegions("users")
	var left, right *sharding.RegionInfo
	for i := range rs {
		if rs[i].RegionID == ids[0] {
			left = &rs[i]
		}
		if rs[i].RegionID == ids[1] {
			right = &rs[i]
		}
	}
	if left == nil || right == nil {
		t.Fatalf("missing split regions: %+v", rs)
	}
	// 分裂发生时共 20 行，行键排序中点 = 第 10 个（id=11）：左 [nil,11) 右 [11,nil)
	if !bytes.Equal(left.EndKey, m7TKey(11)) || !bytes.Equal(right.StartKey, m7TKey(11)) {
		t.Fatalf("boundary: left.End=%x right.Start=%x, want 11", left.EndKey, right.StartKey)
	}
	// 分裂后全量读写一致
	rows := mustRows(t, e, "SELECT id FROM users ORDER BY id")
	if len(rows) != 22 {
		t.Fatalf("SELECT count: want 22, got %d", len(rows))
	}
	for i, r := range rows {
		if r[0].I != int64(i+1) {
			t.Fatalf("row %d: want id=%d, got %d", i, i+1, r[0].I)
		}
	}
	// 行按边界正确分布：左 region 含 1..10，右 region 含 11..22
	res := mustExec(t, e, "SELECT COUNT(*) FROM users WHERE id < 11")
	if res.Rows[0][0].I != 10 {
		t.Fatalf("left region rows: want 10, got %d", res.Rows[0][0].I)
	}
	res = mustExec(t, e, "SELECT COUNT(*) FROM users WHERE id >= 11")
	if res.Rows[0][0].I != 12 {
		t.Fatalf("right region rows: want 12, got %d", res.Rows[0][0].I)
	}
	// 分裂后继续写入可正常路由到对应 region
	mustExec(t, e, "INSERT INTO users (id, name) VALUES (100, 'tail')")
	mustExec(t, e, "INSERT INTO users (id, name) VALUES (0, 'head')")
	rows = mustRows(t, e, "SELECT id FROM users ORDER BY id")
	if len(rows) != 24 || rows[0][0].I != 0 || rows[23][0].I != 100 {
		t.Fatalf("post-split writes misrouted: %+v", rows)
	}
	// 子 region 行数 11/13 < 阈值 20，继续少量写不触发新分裂
	for i := 200; i <= 210; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	if got := len(m7RIDs(t, e, "users")); got != 2 {
		t.Fatalf("no further split: want 2 regions, got %d", got)
	}
	// 关闭自动分裂后写不触发新分裂
	e.SetAutoSplit(false, 20)
	for i := 300; i <= 310; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	if got := len(m7RIDs(t, e, "users")); got != 2 {
		t.Fatalf("after disable: want 2 regions, got %d", got)
	}
}

// TestM7SplitRegionManualStmtBoundary 手动 SPLIT REGION 语句沿数据中点分裂：
// 3 个显式 region 中分裂中间 region [4,8)，数据拆到两个子 region，边界为行键中点。
func TestM7SplitRegionManualStmtBoundary(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	if err := e.SplitTable("users", [][]byte{m7Boundary(4), m7Boundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	ids := m7RIDs(t, e, "users")
	if len(ids) != 3 {
		t.Fatalf("want 3 regions, got %d", len(ids))
	}
	mid := ids[1] // [4,8)
	mustExec(t, e, fmt.Sprintf("SPLIT REGION %d", mid))

	rs, err := e.ListRegions("users")
	if err != nil {
		t.Fatalf("ListRegions: %v", err)
	}
	if len(rs) != 4 {
		t.Fatalf("after split: want 4 regions, got %d (%+v)", len(rs), rs)
	}
	// 中间 region [4,8) 内 4 行（id 4..7）→ 边界 id=6：左 [4,6) 右 [6,8)
	var splitLeft, splitRight bool
	for _, r := range rs {
		if bytes.Equal(r.StartKey, m7TKey(4)) && bytes.Equal(r.EndKey, m7TKey(6)) {
			splitLeft = true
		}
		if bytes.Equal(r.StartKey, m7TKey(6)) && bytes.Equal(r.EndKey, m7TKey(8)) {
			splitRight = true
		}
	}
	if !splitLeft || !splitRight {
		t.Fatalf("split boundary wrong: %+v", rs)
	}
	// 数据完整且分布正确
	rows := mustRows(t, e, "SELECT id FROM users ORDER BY id")
	if len(rows) != 10 {
		t.Fatalf("total rows: want 10, got %d", len(rows))
	}
	res := mustExec(t, e, "SELECT COUNT(*) FROM users WHERE id >= 4 AND id < 6")
	if res.Rows[0][0].I != 2 {
		t.Fatalf("left of split: want 2, got %d", res.Rows[0][0].I)
	}
	res = mustExec(t, e, "SELECT COUNT(*) FROM users WHERE id >= 6 AND id < 8")
	if res.Rows[0][0].I != 2 {
		t.Fatalf("right of split: want 2, got %d", res.Rows[0][0].I)
	}
}

// TestM7SplitRegionNowFunc 函数式手动分裂入口（兼容旧手动 SPLIT）。
func TestM7SplitRegionNowFunc(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 6; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	rs, err := e.ListRegions("users")
	if err != nil || len(rs) != 1 {
		t.Fatalf("before: rs=%v err=%v", rs, err)
	}
	// 隐式单 region 的 RegionID 可直接通过 ListRegions 拿到
	if err := e.SplitRegionNow(uint64(rs[0].RegionID)); err != nil {
		t.Fatalf("SplitRegionNow: %v", err)
	}
	rs, _ = e.ListRegions("users")
	if len(rs) != 2 || !rs[0].Explicit || !rs[1].Explicit {
		t.Fatalf("after SplitRegionNow: want 2 explicit, got %+v", rs)
	}
	if got := mustRows(t, e, "SELECT COUNT(*) FROM users")[0][0].I; got != 6 {
		t.Fatalf("rows: want 6, got %d", got)
	}
	// 不存在的 region 报错
	if err := e.SplitRegionNow(9999); err == nil {
		t.Fatalf("SplitRegionNow(9999): want error, got nil")
	}
}

// TestM7SplitKeepsSecondaryIndexConsistent 分裂后二级索引读写一致。
func TestM7SplitKeepsSecondaryIndexConsistent(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	for i := 1; i <= 10; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	e.SetAutoSplit(true, 20)
	for i := 11; i <= 22; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	rs, _ := e.ListRegions("users")
	if len(rs) != 2 {
		t.Fatalf("auto split: want 2 regions, got %d", len(rs))
	}
	// 索引等值查询仍命中
	rows := mustRows(t, e, "SELECT id FROM users WHERE age = 25")
	if len(rows) != 1 || rows[0][0].I != 5 {
		t.Fatalf("index eq age=25: %+v", rows)
	}
	// 索引范围查询覆盖分裂边界
	rows = mustRows(t, e, "SELECT id FROM users WHERE age >= 26 AND age <= 30 ORDER BY id")
	if len(rows) != 5 {
		t.Fatalf("index range: want 5, got %d (%+v)", len(rows), rows)
	}
	// 分裂后 UPDATE/DELETE 正常
	mustExec(t, e, "UPDATE users SET age = 99 WHERE id = 8")
	rows = mustRows(t, e, "SELECT age FROM users WHERE id = 8")
	if len(rows) != 1 || rows[0][0].I != 99 {
		t.Fatalf("update post-split: %+v", rows)
	}
	mustExec(t, e, "DELETE FROM users WHERE id = 9")
	if got := mustRows(t, e, "SELECT COUNT(*) FROM users")[0][0].I; got != 21 {
		t.Fatalf("delete post-split: want 21, got %d", got)
	}
}

// TestM7BalanceClusterDisabled 未开启集群时 BALANCE 语句拒绝执行。
func TestM7BalanceClusterDisabled(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, PRIMARY KEY (id))")
	mustErr(t, e, "BALANCE", "cluster not enabled")
	// SPLIT REGION 不存在的 region 报错
	mustErr(t, e, "SPLIT REGION 9999", "region not found")
}

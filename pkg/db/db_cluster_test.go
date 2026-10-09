package db_test

// M4 多节点真分布式（第一簇）集成测试：
// 进程内双 db 实例 + StartCluster，覆盖 节点注册/握手、ASSIGN REGION 数据推送
// 与路由视图学习、region 归属本地/远端分支、跨节点 SELECT 点查/范围/全量合并，
// 以及写路径第一簇"仅本地"的语义约束。

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// splitBoundary INT 主键分片边界（8 字节大端，与主键编码一致）。
func splitBoundary(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

// mustSQL 执行 SQL，失败即 fatal；返回结果。
func mustSQL(t *testing.T, d *db.DB, stmt string) *sql.Result {
	t.Helper()
	res, err := d.SQL.Execute(stmt)
	if err != nil {
		t.Fatalf("SQL %q: %v", stmt, err)
	}
	return res
}

// sqlRows SELECT 语句的 id 列断言（ORDER BY id 升序）。
func wantIDs(t *testing.T, d *db.DB, stmt string, want ...int64) {
	t.Helper()
	res := mustSQL(t, d, stmt)
	if len(res.Rows) != len(want) {
		t.Fatalf("%q: want %d rows, got %d (%+v)", stmt, len(want), len(res.Rows), res.Rows)
	}
	for i, w := range want {
		if res.Rows[i][0].I != w {
			t.Fatalf("%q row %d: want %d, got %d", stmt, i, w, res.Rows[i][0].I)
		}
	}
}

// openNode 初始化并打开一个数据目录，启动集群链路，返回 DB 与监听地址。
func openNode(t *testing.T, name string) (*db.DB, string) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("%s init: %v", name, err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("%s open: %v", name, err)
	}
	addr, err := d.StartCluster("127.0.0.1:0")
	if err != nil {
		t.Fatalf("%s start cluster: %v", name, err)
	}
	return d, addr
}

// TestM4CrossNodeFullConsistency 双节点各持部分 region：A 保留 2 个本地 region，
// 中间 region 指派给 B；验证 A/B 两侧 SELECT * 全量一致、点查/范围查跨节点转发正确。
func TestM4CrossNodeFullConsistency(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	defer dA.Close()
	dB, addrB := openNode(t, "node-b")
	defer dB.Close()

	// A 建表写 10 行，按 id<4、4<=id<8、id>=8 分为 3 个显式 region
	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	// B 建同结构表：第一簇不引入全局元数据同步，查询发起方须持有表定义
	// （B 为空库，users 为第一个表，tableID 与 A 一致，物理键解码互通）
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: want 3, got %v err=%v", regions, err)
	}
	// 指派前单机基线
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	// ADD NODE：握手返回 B 的节点 ID
	res := mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	if res.Rows[0][0].S != dB.Cluster.SelfID {
		t.Fatalf("ADD NODE: want node id %q, got %q", dB.Cluster.SelfID, res.Rows[0][0].S)
	}
	// SHOW NODES：A 侧注册表 2 个节点
	res = mustSQL(t, dA, "SHOW NODES")
	if len(res.Rows) != 2 {
		t.Fatalf("SHOW NODES: want 2 nodes, got %d (%+v)", len(res.Rows), res.Rows)
	}

	// 把中间 region（id 4..8）指派给 B：数据推送 + 路由视图学习
	mid := regions[1]
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", mid.RegionID, dB.Cluster.SelfID))

	// A 路由视图：中间 region 归属 B、非本地；其余本地
	routes := mustSQL(t, dA, "SHOW REGION ROUTES")
	if len(routes.Rows) != 3 {
		t.Fatalf("SHOW REGION ROUTES: want 3, got %d (%+v)", len(routes.Rows), routes.Rows)
	}
	for _, row := range routes.Rows {
		isMid := row[0].I == int64(mid.RegionID)
		if isMid && (row[5].S != dB.Cluster.SelfID || row[6].S != "false") {
			t.Fatalf("route %d: want node=%s local=false, got %+v", mid.RegionID, dB.Cluster.SelfID, row)
		}
		if !isMid && (row[5].S != dA.Cluster.SelfID || row[6].S != "true") {
			t.Fatalf("route %d: want local, got %+v", row[0].I, row)
		}
	}

	// B 路由视图：从 REGION_PUSH 学到路由，中间 region 本地、其余归属 A
	routesB := mustSQL(t, dB, "SHOW REGION ROUTES")
	if len(routesB.Rows) != 3 {
		t.Fatalf("B SHOW REGION ROUTES: want 3, got %d (%+v)", len(routesB.Rows), routesB.Rows)
	}
	for _, row := range routesB.Rows {
		isMid := row[0].I == int64(mid.RegionID)
		if isMid && (row[5].S != dB.Cluster.SelfID || row[6].S != "true") {
			t.Fatalf("B route %d: want local, got %+v", mid.RegionID, row)
		}
		if !isMid && (row[5].S != dA.Cluster.SelfID || row[6].S != "false") {
			t.Fatalf("B route %d: want node=%s, got %+v", row[0].I, dA.Cluster.SelfID, row)
		}
	}

	// A 侧跨节点 SELECT * 全量一致（本地 2 region + 远端 1 region 合并）
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	// B 侧（中间 region 本地、其余经 A 转发）同样全量一致
	tr := mustSQL(t, dB, "SHOW TABLES")
	t.Logf("DEBUG B tables: %+v", tr.Rows)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	// 点查：A 上命中远端 region（id=5）与本地 region（id=1）
	res = mustSQL(t, dA, "SELECT name FROM users WHERE id = 5")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u5" {
		t.Fatalf("A point id=5 (remote): %+v", res.Rows)
	}
	res = mustSQL(t, dA, "SELECT name FROM users WHERE id = 1")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u1" {
		t.Fatalf("A point id=1 (local): %+v", res.Rows)
	}
	// 范围查跨 3 个 region（A 上本地+远端混合）
	wantIDs(t, dA, "SELECT id FROM users WHERE id >= 3 AND id < 9 ORDER BY id", 3, 4, 5, 6, 7, 8)
	// 聚合跨节点
	res = mustSQL(t, dA, "SELECT COUNT(*) FROM users")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 10 {
		t.Fatalf("A COUNT(*): %+v", res.Rows)
	}
	// B 上点查远端 region（id=1 归属 A）与本地 region（id=5）
	res = mustSQL(t, dB, "SELECT name FROM users WHERE id = 1")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u1" {
		t.Fatalf("B point id=1 (remote): %+v", res.Rows)
	}
	res = mustSQL(t, dB, "SELECT name FROM users WHERE id = 5")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u5" {
		t.Fatalf("B point id=5 (local): %+v", res.Rows)
	}
	// 更新/删除等写路径第一簇仅本地：A 上写本地 region 正常，写远端 region 报错
	mustSQL(t, dA, "UPDATE users SET age = 99 WHERE id = 1")
	if _, err := dA.SQL.Execute("UPDATE users SET age = 99 WHERE id = 5"); err == nil {
		t.Fatalf("A UPDATE remote region: want error, got nil")
	}
	// 读侧验证本地更新可见（写路径未破坏 M2/M3 语义）
	res = mustSQL(t, dA, "SELECT age FROM users WHERE id = 1")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 99 {
		t.Fatalf("A read after local update: %+v", res.Rows)
	}
}

// TestM4SingleRegionAssign 单 region（未 split）指派给 B：A 上跨节点查询、B 上全本地。
func TestM4SingleRegionAssign(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	defer dA.Close()
	dB, addrB := openNode(t, "node-b")
	defer dB.Close()

	mustSQL(t, dA, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dB, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 5; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO t VALUES (%d, 'n%d')", i, i))
	}
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	regions, err := dA.SQL.ListRegions("t")
	if err != nil || len(regions) != 1 {
		t.Fatalf("ListRegions(t): %v err=%v", regions, err)
	}
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", regions[0].RegionID, dB.Cluster.SelfID))

	// A：单 region 远端，SELECT 全量经转发取回
	wantIDs(t, dA, "SELECT id FROM t ORDER BY id", 1, 2, 3, 4, 5)
	// B：全本地
	wantIDs(t, dB, "SELECT id FROM t ORDER BY id", 1, 2, 3, 4, 5)
	// A 点查远端
	res := mustSQL(t, dA, "SELECT name FROM t WHERE id = 3")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "n3" {
		t.Fatalf("A point id=3: %+v", res.Rows)
	}
}

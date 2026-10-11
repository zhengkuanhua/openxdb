package sql_test

// M12-2 跨节点 ORDER BY / LIMIT 分页（引擎层）测试：
// 单引擎内所有 region 均本地（clusterEnabled=false），覆盖退化路径与
// 单机语义一致性：分片后排序分页结果正确、OFFSET 语法、无 ORDER BY
// 取足量截断、TEXT/DECIMAL/DATE 排序、WHERE 全量兜底、单分区退化、EXPLAIN。
// 真实跨节点下推（ScanSortedRemote / scanMergedClusterTop 远端分支）见
// pkg/db/db_m12_orderlimit_test.go。

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// setupOrderUsers 10 行 users（id INT PK，name TEXT，amt DECIMAL，born DATE）。
func setupOrderUsers(t *testing.T, e *sql.Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, amt DECIMAL, born DATE, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustExec(t, e, "INSERT INTO users (id, name, amt, born) VALUES ("+itoaTest(i)+", 'u"+itoaTest(i)+"', '"+itoaTest(i)+".5000', '2026-01-"+two(i)+"')")
	}
}

// splitOrderTables 将 users 拆为 3 个显式 region（id<4、4<=id<8）。
func splitOrderTables(t *testing.T, e *sql.Engine) {
	t.Helper()
	if err := e.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable users: %v", err)
	}
}

// two 两位补零（测试辅助：日期日段）。
func two(i int) string {
	if i < 10 {
		return "0" + itoaTest(i)
	}
	return itoaTest(i)
}

// TestM12OrderLimitShardedAsc 分片 3 region 后 ORDER BY 升序 + LIMIT（退化本地路径结果正确）。
func TestM12OrderLimitShardedAsc(t *testing.T) {
	e := newEngine(t)
	setupOrderUsers(t, e)
	splitOrderTables(t, e)

	rows := mustRows(t, e, "SELECT id FROM users ORDER BY id LIMIT 3")
	assertIntCol(t, rows, 1, 2, 3)
	rows = mustRows(t, e, "SELECT id FROM users ORDER BY id LIMIT 5")
	assertIntCol(t, rows, 1, 2, 3, 4, 5)
}

// TestM12OrderLimitShardedDesc 降序 top-N。
func TestM12OrderLimitShardedDesc(t *testing.T) {
	e := newEngine(t)
	setupOrderUsers(t, e)
	splitOrderTables(t, e)

	rows := mustRows(t, e, "SELECT id FROM users ORDER BY id DESC LIMIT 3")
	assertIntCol(t, rows, 10, 9, 8)
}

// TestM12OrderLimitOffset LIMIT+OFFSET：先排序再跳偏移，结果与单机一致。
func TestM12OrderLimitOffset(t *testing.T) {
	e := newEngine(t)
	setupOrderUsers(t, e)
	splitOrderTables(t, e)

	rows := mustRows(t, e, "SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 2")
	assertIntCol(t, rows, 3, 4, 5)
	rows = mustRows(t, e, "SELECT id FROM users ORDER BY id DESC LIMIT 2 OFFSET 1")
	assertIntCol(t, rows, 9, 8)
	// 偏移超出：空结果
	rows = mustRows(t, e, "SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 10")
	if len(rows) != 0 {
		t.Fatalf("OFFSET beyond: want 0 rows, got %d (%+v)", len(rows), rows)
	}
}

// TestM12OrderLimitNoOrderBy 无 ORDER BY 仅 LIMIT/OFFSET：内层物理键序截断。
func TestM12OrderLimitNoOrderBy(t *testing.T) {
	e := newEngine(t)
	setupOrderUsers(t, e)
	splitOrderTables(t, e)

	rows := mustRows(t, e, "SELECT id FROM users LIMIT 4")
	assertIntCol(t, rows, 1, 2, 3, 4)
	rows = mustRows(t, e, "SELECT id FROM users LIMIT 3 OFFSET 2")
	assertIntCol(t, rows, 3, 4, 5)
}

// TestM12OrderLimitTypeSort TEXT/DECIMAL/DATE 列排序（跨类型一致性）。
func TestM12OrderLimitTypeSort(t *testing.T) {
	e := newEngine(t)
	setupOrderUsers(t, e)
	splitOrderTables(t, e)

	// TEXT 字典序：u1 < u10 < u2 < ... < u9
	rows := mustRows(t, e, "SELECT id FROM users ORDER BY name LIMIT 3")
	assertIntCol(t, rows, 1, 10, 2)
	rows = mustRows(t, e, "SELECT id FROM users ORDER BY name DESC LIMIT 3")
	assertIntCol(t, rows, 9, 8, 7)
	// DECIMAL scale=4 定点排序
	rows = mustRows(t, e, "SELECT id FROM users ORDER BY amt LIMIT 3")
	assertIntCol(t, rows, 1, 2, 3)
	rows = mustRows(t, e, "SELECT id FROM users ORDER BY amt DESC LIMIT 3")
	assertIntCol(t, rows, 10, 9, 8)
	// DATE 字典序即时间序
	rows = mustRows(t, e, "SELECT id FROM users ORDER BY born LIMIT 3")
	assertIntCol(t, rows, 1, 2, 3)
}

// TestM12OrderLimitWhereFallback 带 WHERE 走全量兜底（正确性优先），结果一致。
func TestM12OrderLimitWhereFallback(t *testing.T) {
	e := newEngine(t)
	setupOrderUsers(t, e)
	splitOrderTables(t, e)

	rows := mustRows(t, e, "SELECT id FROM users WHERE id > 3 ORDER BY id LIMIT 2")
	assertIntCol(t, rows, 4, 5)
	rows = mustRows(t, e, "SELECT id FROM users WHERE id > 3 ORDER BY id LIMIT 2 OFFSET 1")
	assertIntCol(t, rows, 5, 6)
}

// TestM12OrderLimitSinglePartitionDegrade 未分片表 ORDER BY/LIMIT 与单机一致。
func TestM12OrderLimitSinglePartitionDegrade(t *testing.T) {
	e := newEngine(t)
	setupOrderUsers(t, e)

	rows := mustRows(t, e, "SELECT id FROM users ORDER BY id LIMIT 3")
	assertIntCol(t, rows, 1, 2, 3)
	rows = mustRows(t, e, "SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 7")
	assertIntCol(t, rows, 8, 9, 10)
}

// TestM12OrderLimitExplainOffset EXPLAIN 输出 offset 行。
func TestM12OrderLimitExplainOffset(t *testing.T) {
	e := newEngine(t)
	setupOrderUsers(t, e)

	rows := mustRows(t, e, "EXPLAIN SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 2")
	found := false
	for _, r := range rows {
		s := ""
		if len(r) > 0 {
			s = r[0].S
		}
		if strings.Contains(s, "offset: 2") {
			found = true
		}
	}
	if !found {
		t.Fatalf("EXPLAIN: want offset line, got %+v", rows)
	}
}

// TestM12OrderLimitAliasFallback 别名投影（增强路径）不误走分布式捷径，结果正确。
func TestM12OrderLimitAliasFallback(t *testing.T) {
	e := newEngine(t)
	setupOrderUsers(t, e)
	splitOrderTables(t, e)

	res := mustRows(t, e, "SELECT id AS uid FROM users ORDER BY id LIMIT 3")
	if len(res) != 3 {
		t.Fatalf("alias: want 3 rows, got %d", len(res))
	}
	assertIntCol(t, res, 1, 2, 3)
}

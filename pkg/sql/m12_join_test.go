package sql_test

// M12 分布式 JOIN 测试：
// 覆盖路由路径（右表主键等值 → 分片/多节点路由点查 + 合并）、方向二（b.pk = a.x）、
// 多列 AND 等值、非主键关联回退 joinRels、跨类型驱动值回退、
// 单分区退化、空分区、未分片/分片结果一致性。

import (
	"reflect"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// setupJoinUsers 10 行 users（id INT PK，name TEXT）。
func setupJoinUsers(t *testing.T, e *sql.Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustExec(t, e, "INSERT INTO users (id, name) VALUES ("+itoaTest(i)+", 'u"+itoaTest(i)+"')")
	}
}

// setupJoinOrders 4 行 orders（id INT PK，uname TEXT，amount INT）。
func setupJoinOrders(t *testing.T, e *sql.Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE orders (id INT, uname TEXT, amount INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO orders (id, uname, amount) VALUES (1, 'u1', 100), (2, 'u2', 200), (5, 'u5', 500), (10, 'u10', 1000)")
}

// splitJoinTables 将 users/orders 按 id<4、4<=id<8 拆为 3 个显式 region（跨表同边界）。
func splitJoinTables(t *testing.T, e *sql.Engine) {
	t.Helper()
	if err := e.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable users: %v", err)
	}
	if err := e.SplitTable("orders", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable orders: %v", err)
	}
}

// assertIntCol 断言 SELECT 第一列为升序 int64 序列。
func assertIntCol(t *testing.T, rows [][]sql.Value, want ...int64) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("want %d rows, got %d (%+v)", len(want), len(rows), rows)
	}
	for i, w := range want {
		if rows[i][0].Kind != "INT" || rows[i][0].I != w {
			t.Fatalf("row %d col0: want %d, got %+v", i, w, rows[i][0])
		}
	}
}

// TestM12JoinRoutedInner 右主键等值 INNER JOIN 走路由路径，分片后结果正确。
func TestM12JoinRoutedInner(t *testing.T) {
	e := newEngine(t)
	setupJoinUsers(t, e)
	setupJoinOrders(t, e)
	splitJoinTables(t, e)

	rows := mustRows(t, e, "SELECT u.id, u.name, o.amount FROM users u JOIN orders o ON u.id = o.id ORDER BY u.id")
	assertIntCol(t, rows, 1, 2, 5, 10)
	for i, wantAmt := range []int64{100, 200, 500, 1000} {
		if rows[i][2].Kind != "INT" || rows[i][2].I != wantAmt {
			t.Fatalf("row %d amount: want %d, got %+v", i, wantAmt, rows[i][2])
		}
	}
}

// TestM12JoinRoutedLeft LEFT JOIN 路由路径：未匹配左行以空值补右列。
func TestM12JoinRoutedLeft(t *testing.T) {
	e := newEngine(t)
	setupJoinUsers(t, e)
	setupJoinOrders(t, e)
	splitJoinTables(t, e)

	rows := mustRows(t, e, "SELECT u.id, o.amount FROM users u LEFT JOIN orders o ON u.id = o.id ORDER BY u.id")
	if len(rows) != 10 {
		t.Fatalf("LEFT JOIN: want 10 rows, got %d", len(rows))
	}
	amountByID := map[int64]int64{1: 100, 2: 200, 5: 500, 10: 1000}
	for i := 0; i < 10; i++ {
		id := int64(i + 1)
		if rows[i][0].I != id {
			t.Fatalf("row %d id: want %d, got %d", i, id, rows[i][0].I)
		}
		if want, ok := amountByID[id]; ok {
			if rows[i][1].Kind != "INT" || rows[i][1].I != want {
				t.Fatalf("row %d amount: want %d, got %+v", i, want, rows[i][1])
			}
		} else if rows[i][1].Kind != "TEXT" || rows[i][1].S != "" {
			t.Fatalf("row %d amount: want empty NULL, got %+v", i, rows[i][1])
		}
	}
}

// TestM12JoinMultiColEq 多列 AND 等值（右主键 + 一般列）路由后按完整 ON 校验。
func TestM12JoinMultiColEq(t *testing.T) {
	e := newEngine(t)
	setupJoinUsers(t, e)
	mustExec(t, e, "CREATE TABLE orders (id INT, uname TEXT, amount INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO orders (id, uname, amount) VALUES (1, 'u1', 100), (2, 'nobody', 999), (5, 'u5', 500)")
	splitJoinTables(t, e)

	rows := mustRows(t, e, "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.id AND u.name = o.uname ORDER BY u.id")
	assertIntCol(t, rows, 1, 5)
	if rows[0][1].I != 100 || rows[1][1].I != 500 {
		t.Fatalf("amounts: want 100,500, got %+v", rows)
	}
}

// TestM12JoinReverseDirection 方向二写法 b.pk = a.x 同样走路由路径。
func TestM12JoinReverseDirection(t *testing.T) {
	e := newEngine(t)
	setupJoinUsers(t, e)
	setupJoinOrders(t, e)
	splitJoinTables(t, e)

	rows := mustRows(t, e, "SELECT u.id, o.amount FROM users u JOIN orders o ON o.id = u.id ORDER BY u.id")
	assertIntCol(t, rows, 1, 2, 5, 10)
	if rows[0][1].I != 100 || rows[1][1].I != 200 || rows[2][1].I != 500 || rows[3][1].I != 1000 {
		t.Fatalf("amounts: got %+v", rows)
	}
}

// TestM12JoinAliasRoute 别名限定列路由。
func TestM12JoinAliasRoute(t *testing.T) {
	e := newEngine(t)
	setupJoinUsers(t, e)
	setupJoinOrders(t, e)
	splitJoinTables(t, e)

	rows := mustRows(t, e, "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.id ORDER BY u.id")
	assertIntCol(t, rows, 1, 2, 5, 10)
}

// TestM12JoinFallbackNonPK 关联列非右主键 → 回退 joinRels，分片后结果正确。
func TestM12JoinFallbackNonPK(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExec(t, e, "CREATE TABLE orders (id INT, uname TEXT, amount INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO orders (id, uname, amount) VALUES (1, 'alice', 100), (2, 'bob', 200), (3, 'alice', 300)")
	splitJoinTables(t, e)

	// uname 非右主键：不可路由，回退全量 JOIN；alice 两单 + bob 一单 = 3 行。
	rows := mustRows(t, e, "SELECT u.id, o.amount FROM users u JOIN orders o ON u.name = o.uname ORDER BY u.id")
	assertIntCol(t, rows, 1, 1, 2)
	if rows[0][1].I != 100 || rows[1][1].I != 300 || rows[2][1].I != 200 {
		t.Fatalf("amounts: got %+v", rows)
	}
}

// TestM12JoinSinglePartitionDegrade 单分区（未分片）路由 JOIN 退化为本地，行为正确。
func TestM12JoinSinglePartitionDegrade(t *testing.T) {
	e := newEngine(t)
	setupJoinUsers(t, e)
	setupJoinOrders(t, e)

	rows := mustRows(t, e, "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.id ORDER BY u.id")
	assertIntCol(t, rows, 1, 2, 5, 10)
}

// TestM12JoinEmptyPartition 右表某 region 为空时 JOIN 仍正确。
func TestM12JoinEmptyPartition(t *testing.T) {
	e := newEngine(t)
	setupJoinUsers(t, e)
	mustExec(t, e, "CREATE TABLE orders (id INT, uname TEXT, amount INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO orders (id, uname, amount) VALUES (1, 'u1', 100), (10, 'u10', 1000)")
	splitJoinTables(t, e) // orders 仅覆盖 region1/region3，region2 空

	rows := mustRows(t, e, "SELECT u.id, o.amount FROM users u LEFT JOIN orders o ON u.id = o.id ORDER BY u.id")
	if len(rows) != 10 {
		t.Fatalf("LEFT JOIN: want 10 rows, got %d", len(rows))
	}
	if rows[0][1].I != 100 || rows[9][1].I != 1000 {
		t.Fatalf("matched amounts: got %+v", rows)
	}
	for i := 1; i < 9; i++ {
		if rows[i][1].Kind != "TEXT" || rows[i][1].S != "" {
			t.Fatalf("row %d: want empty amount, got %+v", i, rows[i][1])
		}
	}
}

// TestM12JoinSplitConsistency 未分片 vs 分片：INNER/LEFT JOIN 全量结果一致。
func TestM12JoinSplitConsistency(t *testing.T) {
	plain := newEngine(t)
	setupJoinUsers(t, plain)
	setupJoinOrders(t, plain)

	split := newEngine(t)
	setupJoinUsers(t, split)
	setupJoinOrders(t, split)
	splitJoinTables(t, split)

	for _, stmt := range []string{
		"SELECT u.id, u.name, o.id, o.uname, o.amount FROM users u JOIN orders o ON u.id = o.id ORDER BY u.id",
		"SELECT u.id, o.amount FROM users u LEFT JOIN orders o ON u.id = o.id ORDER BY u.id",
		"SELECT u.id, o.amount FROM users u JOIN orders o ON o.id = u.id ORDER BY u.id",
	} {
		want := mustRows(t, plain, stmt)
		got := mustRows(t, split, stmt)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%q: split result mismatch\nwant: %+v\ngot:  %+v", stmt, want, got)
		}
	}
}

// TestM12JoinFallbackTypeMismatch 驱动值类型与右主键编码不兼容 → 回退全量 JOIN。
// users.id 为 TEXT 驱动，orders.id 为 INT 主键：INT 主键不接受 TEXT 键值编码，
// 回退 joinRels 后按字符串比较匹配（单机语义一致）。
func TestM12JoinFallbackTypeMismatch(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id TEXT, name TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO users (id, name) VALUES ('1', 'a'), ('2', 'b'), ('3', 'c')")
	mustExec(t, e, "CREATE TABLE orders (id INT, amount INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO orders (id, amount) VALUES (1, 100), (2, 200)")
	splitJoinTables(t, e)

	rows := mustRows(t, e, "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.id ORDER BY u.id")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d (%+v)", len(rows), rows)
	}
	if rows[0][0].S != "a" || rows[0][1].I != 100 || rows[1][0].S != "b" || rows[1][1].I != 200 {
		t.Fatalf("mismatch: got %+v", rows)
	}
}

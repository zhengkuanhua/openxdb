package sql_test

// M12-3 分布式子查询测试（单引擎分片模拟 + 与单机一致性）：
//   - IN 子查询跨分片归并 / 主键列可下推时按主查询列路由点查（分片裁剪）；
//   - EXISTS 子查询跨分片判定；
//   - 标量（单行单列）子查询跨分片取数；
//   - 子查询表单分区 / 未分片时退化本地执行，行为与单机一致；
//   - 驱动值类型与子查询主键编码不兼容时回退默认模式，结果与单机一致；
// 真实跨节点（ScanRemote 远端分片）场景见 pkg/db/db_m12_subquery_test.go。

import (
	"reflect"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// splitUsersOnly 仅将 users 拆为 3 个 region（orders 保持单分区，用于验证
// 子查询表单分区时的本地退化路径）。
func splitUsersOnly(t *testing.T, e interface {
	SplitTable(name string, boundaries [][]byte) error
}) {
	t.Helper()
	if err := e.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable users: %v", err)
	}
}

// TestM12SubqueryInSharded IN 子查询跨分片：子查询结果跨分片归并后参与过滤。
// users.id 与 orders.id 均为 INT 主键且分片结构一致 → 走路由模式（按主查询列值
// 点查子查询表归属分片），结果与全量归并等价。
func TestM12SubqueryInSharded(t *testing.T) {
	e := newEngine(t)
	setupJoinUsers(t, e)
	setupJoinOrders(t, e)
	splitJoinTables(t, e)

	// 路由模式：驱动值 1,2,5,10 均命中子查询表分片，amount>=500 裁剪掉 1,2。
	rows := mustRows(t, e, "SELECT u.id FROM users u WHERE u.id IN (SELECT o.id FROM orders o WHERE o.amount >= 500) ORDER BY u.id")
	assertIntCol(t, rows, 5, 10)

	// 全量集合（无 WHERE 子查询）：跨分片归并全部 4 行。
	rows = mustRows(t, e, "SELECT u.id FROM users u WHERE u.id IN (SELECT o.id FROM orders o) ORDER BY u.id")
	assertIntCol(t, rows, 1, 2, 5, 10)

	// 无匹配：空集合 → 空结果。
	rows = mustRows(t, e, "SELECT u.id FROM users u WHERE u.id IN (SELECT o.id FROM orders o WHERE o.amount > 99999) ORDER BY u.id")
	if len(rows) != 0 {
		t.Fatalf("want 0 rows, got %+v", rows)
	}
}

// TestM12SubqueryExistsSharded EXISTS 子查询跨分片判定。
func TestM12SubqueryExistsSharded(t *testing.T) {
	e := newEngine(t)
	setupJoinUsers(t, e)
	setupJoinOrders(t, e)
	splitJoinTables(t, e)

	// 存在 amount>=500 的订单 → EXISTS 判定为真，配合主查询条件返回 id 5..10
	//（非关联 EXISTS；关联（correlated）子查询不在本次支持范围，与单机一致）。
	rows := mustRows(t, e, "SELECT u.id FROM users u WHERE u.id >= 5 AND EXISTS (SELECT o.id FROM orders o WHERE o.amount >= 500) ORDER BY u.id")
	assertIntCol(t, rows, 5, 6, 7, 8, 9, 10)

	// 无订单满足 → 空。
	rows = mustRows(t, e, "SELECT u.id FROM users u WHERE EXISTS (SELECT o.id FROM orders o WHERE o.amount >= 99999) ORDER BY u.id")
	if len(rows) != 0 {
		t.Fatalf("want 0 rows, got %+v", rows)
	}

	// 所有用户都有（无条件 EXISTS，子查询存在行）→ 全量。
	rows = mustRows(t, e, "SELECT u.id FROM users u WHERE EXISTS (SELECT o.id FROM orders o) ORDER BY u.id")
	assertIntCol(t, rows, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
}

// TestM12SubqueryScalarSharded 标量子查询（单行单列）跨分片取数。
func TestM12SubqueryScalarSharded(t *testing.T) {
	e := newEngine(t)
	setupJoinUsers(t, e)
	setupJoinOrders(t, e)
	splitJoinTables(t, e)

	// 等值命中：o.amount=500 的订单 id=5（跨分片）。
	rows := mustRows(t, e, "SELECT u.id, u.name FROM users u WHERE u.id = (SELECT o.id FROM orders o WHERE o.amount = 500)")
	if len(rows) != 1 || rows[0][0].I != 5 || rows[0][1].S != "u5" {
		t.Fatalf("scalar eq: want (5,u5), got %+v", rows)
	}

	// 零行 → NULL 语义：条件不成立 → 空结果。
	rows = mustRows(t, e, "SELECT u.id FROM users u WHERE u.id = (SELECT o.id FROM orders o WHERE o.amount = 99999)")
	if len(rows) != 0 {
		t.Fatalf("scalar empty: want 0 rows, got %+v", rows)
	}

	// 多行 → 报错。
	mustErr(t, e, "SELECT u.id FROM users u WHERE u.id = (SELECT o.id FROM orders o)", "more than one row")
}

// TestM12SubquerySinglePartitionDegrade 子查询表单分区/未分片时退化本地执行，
// 行为与单机一致。
func TestM12SubquerySinglePartitionDegrade(t *testing.T) {
	// 完全未分片（隐式单 region）：三种子查询均正常。
	plain := newEngine(t)
	setupJoinUsers(t, plain)
	setupJoinOrders(t, plain)

	rows := mustRows(t, plain, "SELECT u.id FROM users u WHERE u.id IN (SELECT o.id FROM orders o WHERE o.amount >= 500) ORDER BY u.id")
	assertIntCol(t, rows, 5, 10)
	rows = mustRows(t, plain, "SELECT u.id FROM users u WHERE EXISTS (SELECT o.id FROM orders o WHERE o.amount >= 500) ORDER BY u.id")
	assertIntCol(t, rows, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	rows = mustRows(t, plain, "SELECT u.id FROM users u WHERE u.id = (SELECT o.id FROM orders o WHERE o.amount = 200)")
	assertIntCol(t, rows, 2)

	// users 分片、orders 单分区：子查询表单分区 → IN 走默认归并（退化），结果正确。
	e := newEngine(t)
	setupJoinUsers(t, e)
	setupJoinOrders(t, e)
	splitUsersOnly(t, e)

	rows = mustRows(t, e, "SELECT u.id FROM users u WHERE u.id IN (SELECT o.id FROM orders o WHERE o.amount >= 500) ORDER BY u.id")
	assertIntCol(t, rows, 5, 10)
}

// TestM12SubqueryPlainVsSharded 分片与单机结果一致性（IN/EXISTS/标量/空集合）。
func TestM12SubqueryPlainVsSharded(t *testing.T) {
	plain := newEngine(t)
	setupJoinUsers(t, plain)
	setupJoinOrders(t, plain)

	split := newEngine(t)
	setupJoinUsers(t, split)
	setupJoinOrders(t, split)
	splitJoinTables(t, split)

	for _, stmt := range []string{
		"SELECT u.id, u.name FROM users u WHERE u.id IN (SELECT o.id FROM orders o WHERE o.amount > 100) ORDER BY u.id",
		"SELECT u.id FROM users u WHERE u.id IN (SELECT o.id FROM orders o) ORDER BY u.id",
		"SELECT u.id FROM users u WHERE EXISTS (SELECT o.id FROM orders o WHERE o.amount >= 500) ORDER BY u.id",
		"SELECT u.id FROM users u WHERE EXISTS (SELECT o.id FROM orders o WHERE o.amount > 99999) ORDER BY u.id",
		"SELECT u.id FROM users u WHERE u.id = (SELECT o.id FROM orders o WHERE o.amount = 500) ORDER BY u.id",
		"SELECT u.id FROM users u WHERE u.id = (SELECT o.id FROM orders o WHERE o.amount = 99999) ORDER BY u.id",
	} {
		want := mustRows(t, plain, stmt)
		got := mustRows(t, split, stmt)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%q: split result mismatch\nwant: %+v\ngot:  %+v", stmt, want, got)
		}
	}
}

// TestM12SubqueryTypeMismatchFallback 驱动值类型与子查询表主键编码不兼容 →
// 路由模式回退默认归并，结果与单机一致（TEXT 驱动 vs INT 主键，字符串宽松比较）。
func TestM12SubqueryTypeMismatchFallback(t *testing.T) {
	mk := func(t *testing.T) *sql.Engine {
		e := newEngine(t)
		mustExec(t, e, "CREATE TABLE users (id TEXT, name TEXT, PRIMARY KEY (id))")
		mustExec(t, e, "INSERT INTO users (id, name) VALUES ('1', 'a'), ('2', 'b'), ('3', 'c'), ('5', 'e'), ('10', 'j')")
		mustExec(t, e, "CREATE TABLE orders (id INT, amount INT, PRIMARY KEY (id))")
		mustExec(t, e, "INSERT INTO orders (id, amount) VALUES (1, 100), (2, 200), (5, 500), (10, 1000)")
		return e
	}

	plain := mk(t)
	split := newEngine(t)
	mustExec(t, split, "CREATE TABLE users (id TEXT, name TEXT, PRIMARY KEY (id))")
	mustExec(t, split, "INSERT INTO users (id, name) VALUES ('1', 'a'), ('2', 'b'), ('3', 'c'), ('5', 'e'), ('10', 'j')")
	mustExec(t, split, "CREATE TABLE orders (id INT, amount INT, PRIMARY KEY (id))")
	mustExec(t, split, "INSERT INTO orders (id, amount) VALUES (1, 100), (2, 200), (5, 500), (10, 1000)")
	splitJoinTables(t, split)

	stmt := "SELECT u.name FROM users u WHERE u.id IN (SELECT o.id FROM orders o WHERE o.amount > 100) ORDER BY u.id"
	want := mustRows(t, plain, stmt)
	got := mustRows(t, split, stmt)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("type-mismatch fallback mismatch\nwant: %+v\ngot:  %+v", want, got)
	}
}

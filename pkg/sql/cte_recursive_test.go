package sql_test

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// v1.19.0 D5：递归 CTE（WITH RECURSIVE）。
// 覆盖：序列生成、树形父子遍历、UNION 去重 vs UNION ALL、深度上限、
// 列数不匹配、递归引用非法位置、与普通 CTE 混用、空 anchor、
// 多列列名列表、JOIN 右端引用、作用域不跨语句泄漏、非递归 CTE 零回归。

// cteInts 抽取结果集某列整数值。
func cteInts(t *testing.T, rows [][]sql.Value, col int) []int64 {
	t.Helper()
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		if col >= len(r) {
			t.Fatalf("row %+v has no column %d", r, col)
		}
		out = append(out, r[col].I)
	}
	return out
}

// cteWant 断言整数切片与期望值逐位一致。
func cteWant(t *testing.T, got []int64, want ...int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("want %v (%d rows), got %v (%d rows)", want, len(want), got, len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

// 1..N 递推序列（列名列表 + 算术表达式投影）。
func TestRecursiveCTESequence(t *testing.T) {
	e := newEngine(t)
	res := mustExec(t, e, "WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5) SELECT n FROM seq ORDER BY n")
	if len(res.Columns) != 1 || !strings.HasSuffix(res.Columns[0], "n") {
		t.Fatalf("column name: want suffix n, got %v", res.Columns)
	}
	cteWant(t, cteInts(t, res.Rows, 0), 1, 2, 3, 4, 5)
}

// 多列列名列表 + 列间递推（id, lvl 双列）。
func TestRecursiveCTEMultiColumnList(t *testing.T) {
	e := newEngine(t)
	rows := mustRows(t, e, `WITH RECURSIVE chain (id, lvl) AS (
		SELECT 1, 0
		UNION ALL
		SELECT id + 1, lvl + 1 FROM chain WHERE id < 3
	) SELECT id, lvl FROM chain ORDER BY id`)
	cteWant(t, cteInts(t, rows, 0), 1, 2, 3)
	cteWant(t, cteInts(t, rows, 1), 0, 1, 2)
}

// 树形父子表遍历（递归 CTE 作为 JOIN 右端被引用）。
func TestRecursiveCTETreeTraversal(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE emp (id INT, mgr INT, name TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO emp (id, mgr, name) VALUES (1, 0, 'root'), (2, 1, 'a'), (3, 1, 'b'), (4, 2, 'c')")
	rows := mustRows(t, e, `WITH RECURSIVE subtree (id, mgr, lvl) AS (
		SELECT id, mgr, 0 FROM emp WHERE id = 1
		UNION ALL
		SELECT e.id, e.mgr, s.lvl + 1 FROM emp e JOIN subtree s ON e.mgr = s.id
	) SELECT id, lvl FROM subtree ORDER BY id`)
	cteWant(t, cteInts(t, rows, 0), 1, 2, 3, 4)
	cteWant(t, cteInts(t, rows, 1), 0, 1, 1, 2)
}

// UNION 去重 vs UNION ALL 保留重复（菱形有向图：4 经两条路径被访问两次）。
func TestRecursiveCTEUnionDedupVsUnionAll(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE edge (src INT, dst INT, PRIMARY KEY (src, dst))")
	mustExec(t, e, "INSERT INTO edge (src, dst) VALUES (1, 2), (1, 3), (2, 4), (3, 4)")
	all := mustRows(t, e, `WITH RECURSIVE reach (n) AS (
		SELECT 1 UNION ALL SELECT dst FROM edge JOIN reach ON edge.src = reach.n WHERE n < 4
	) SELECT n FROM reach ORDER BY n`)
	cteWant(t, cteInts(t, all, 0), 1, 2, 3, 4, 4)
	distinct := mustRows(t, e, `WITH RECURSIVE reach (n) AS (
		SELECT 1 UNION SELECT dst FROM edge JOIN reach ON edge.src = reach.n WHERE n < 4
	) SELECT n FROM reach ORDER BY n`)
	cteWant(t, cteInts(t, distinct, 0), 1, 2, 3, 4)
}

// UNION 去重使环形图收敛（1→2→3→1 的环在去重后终止且不重复出行）。
func TestRecursiveCTEUnionCycleTerminates(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE ring (src INT, dst INT, PRIMARY KEY (src, dst))")
	mustExec(t, e, "INSERT INTO ring (src, dst) VALUES (1, 2), (2, 3), (3, 1)")
	rows := mustRows(t, e, `WITH RECURSIVE reach (n) AS (
		SELECT 1 UNION SELECT dst FROM ring JOIN reach ON ring.src = reach.n
	) SELECT n FROM reach ORDER BY n`)
	cteWant(t, cteInts(t, rows, 0), 1, 2, 3)
}

// 深度上限：默认 1000（MySQL cte_max_recursion_depth 口径），超限报错而非静默截断。
func TestRecursiveCTEDepthLimit(t *testing.T) {
	e := newEngine(t)
	mustErr(t, e, "WITH RECURSIVE inc (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM inc) SELECT n FROM inc",
		"exceeded maximum recursion depth 1000")
	// 配置项默认值与 SET 热切换均生效。
	if got := cfgValue(t, e, "recursive_cte_max_depth"); got != "1000" {
		t.Fatalf("default recursive_cte_max_depth: want 1000, got %q", got)
	}
	mustExec(t, e, "SET recursive_cte_max_depth = 5")
	mustErr(t, e, "WITH RECURSIVE inc (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM inc) SELECT n FROM inc",
		"exceeded maximum recursion depth 5")
	// 上限调小后，深度未超限的查询仍正常返回。
	rows := mustRows(t, e, "WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5) SELECT n FROM seq ORDER BY n")
	cteWant(t, cteInts(t, rows, 0), 1, 2, 3, 4, 5)
	// 恢复默认，避免影响同引擎后续语句。
	mustExec(t, e, "SET recursive_cte_max_depth = 1000")
	if got := cfgValue(t, e, "recursive_cte_max_depth"); got != "1000" {
		t.Fatalf("restored recursive_cte_max_depth: want 1000, got %q", got)
	}
	// 非法取值报错。
	mustErr(t, e, "SET recursive_cte_max_depth = 0", "invalid value for recursive_cte_max_depth")
}

// 列数不匹配：递归项列数 != anchor 列数；声明的列名列表 != anchor 列数。
func TestRecursiveCTEColumnCountMismatch(t *testing.T) {
	e := newEngine(t)
	mustErr(t, e, "WITH RECURSIVE bad (n) AS (SELECT 1 UNION ALL SELECT n, n FROM bad WHERE n < 3) SELECT n FROM bad",
		"column count mismatch")
	mustErr(t, e, "WITH RECURSIVE bad (n, m) AS (SELECT 1 UNION ALL SELECT n + 1, m FROM bad WHERE n < 3) SELECT n FROM bad",
		"column count mismatch")
}

// 递归引用非法位置：缺失 FROM 引用 / 重复引用 / 子查询内 / 聚合参数内。
func TestRecursiveCTEInvalidReferencePosition(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, v INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, 10)")

	// 递归项未在 FROM 中引用自身。
	mustErr(t, e, "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT v FROM t WHERE id = n) SELECT n FROM c",
		"must be referenced in the FROM clause of the recursive term")
	// 同一 recursive term 内重复引用自身。
	mustErr(t, e, "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT a.n FROM c a JOIN c b ON a.n = b.n) SELECT n FROM c",
		"must not be referenced more than once")
	// 自引用出现在子查询（IN 子查询）内。
	mustErr(t, e, "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n IN (SELECT n FROM c)) SELECT n FROM c",
		"not allowed inside a subquery")
	// 自引用出现在 FROM 派生表内。
	mustErr(t, e, "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT x.n FROM (SELECT n FROM c) x) SELECT n FROM c",
		"not allowed inside a subquery")
	// 自引用作为聚合函数参数。
	mustErr(t, e, "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT MAX(n) FROM c) SELECT n FROM c",
		"not allowed as an aggregate function argument")
}

// 与普通（非递归）CTE 混用：后定义引用先定义，且可与递归结果同语句使用。
func TestRecursiveCTEMixedWithPlainCTE(t *testing.T) {
	e := newEngine(t)
	// 递归 CTE 的 anchor 引用前置普通 CTE。
	rows := mustRows(t, e, `WITH RECURSIVE seed AS (SELECT 3 AS n), seq (n) AS (
		SELECT n FROM seed UNION ALL SELECT n + 1 FROM seq WHERE n < 5
	) SELECT n FROM seq ORDER BY n`)
	cteWant(t, cteInts(t, rows, 0), 3, 4, 5)
	// 前置普通 CTE 引用递归 CTE（普通 CTE 不物化，引用处按作用域取物化结果）。
	rows = mustRows(t, e, `WITH RECURSIVE seq (n) AS (
		SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 3
	), hi AS (SELECT n FROM seq WHERE n > 1) SELECT n FROM hi ORDER BY n`)
	cteWant(t, cteInts(t, rows, 0), 2, 3)
	// 递归 CTE 与普通 CTE 在同一 FROM 中 JOIN。
	rows = mustRows(t, e, `WITH RECURSIVE seq (n) AS (
		SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 3
	), two AS (SELECT 2 AS m) SELECT s.n FROM seq s JOIN two ON s.n = m`)
	cteWant(t, cteInts(t, rows, 0), 2)
}

// anchor 返回空集：迭代不启动，直接返回空结果（不报错）。
func TestRecursiveCTEEmptyAnchor(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE empty_t (id INT, PRIMARY KEY (id))")
	rows := mustRows(t, e, `WITH RECURSIVE c (n) AS (
		SELECT id FROM empty_t UNION ALL SELECT n + 1 FROM c WHERE n < 10
	) SELECT n FROM c`)
	if len(rows) != 0 {
		t.Fatalf("empty anchor: want 0 rows, got %+v", rows)
	}
}

// 作用域不跨语句泄漏：递归 CTE 名在执行结束后不再可见。
func TestRecursiveCTEScopeNotLeaked(t *testing.T) {
	e := newEngine(t)
	mustRows(t, e, "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT n FROM c")
	if _, err := e.Execute("SELECT n FROM c"); err == nil {
		t.Fatalf("scope leak: recursive CTE c should not be visible after statement end")
	}
}

// 非递归 CTE 零回归：既有宏展开语义、循环引用报错、UNION 需 RECURSIVE 均保持原样。
func TestRecursiveCTENonRecursiveUnchanged(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	mustExec(t, e, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
	// 普通 CTE 链式引用（M10 行为）。
	rows := mustRows(t, e, "WITH a AS (SELECT id, v FROM t), b AS (SELECT id FROM a WHERE v >= 20) SELECT id FROM b ORDER BY id")
	cteWant(t, cteInts(t, rows, 0), 2, 3)
	// 循环引用的既有报错文案不变。
	mustErr(t, e, "WITH a AS (SELECT * FROM b), b AS (SELECT * FROM a) SELECT * FROM a", "cyclic CTE reference")
	// 未声明 RECURSIVE 的 UNION 定义报语法级错误。
	mustErr(t, e, "WITH a AS (SELECT 1 UNION SELECT 2) SELECT * FROM a", "requires WITH RECURSIVE")
	// 引用了未定义名称仍报表不存在。
	if _, err := e.Execute("WITH a AS (SELECT id FROM t) SELECT id FROM a JOIN missing ON a.id = missing.id"); err == nil {
		t.Fatalf("undefined table should still error")
	}
}

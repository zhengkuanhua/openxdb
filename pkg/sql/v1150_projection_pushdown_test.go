package sql_test

import (
	"testing"
)

// v1.15.0 W3：列投影下推（relFromScan 按投影列表按需解码列，跳过未投影列 decode）。
// 语义等价覆盖：单列/多列投影、WHERE 引用非投影列、ORDER BY 非投影列、表达式/CASE、
// GROUP BY + 聚合引用非投影列、HAVING 引用非投影列、DISTINCT，
// 以及回退场景 SELECT * / JOIN / FROM 子查询。

// TestProjectionPushdownBasic 基础下推：单列/多列投影、WHERE 引用非投影列、
// ORDER BY 非投影列、表达式与 CASE 投影均正确。
func TestProjectionPushdownBasic(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// 单列投影（下推后 rel 仅含 name）
	res := mustExec(t, e, "SELECT name FROM users")
	if len(res.Columns) != 1 || res.Columns[0] != "name" {
		t.Fatalf("columns = %v, want [name]", res.Columns)
	}
	if len(res.Rows) != 3 || res.Rows[0][0].S != "alice" || res.Rows[2][0].S != "carol" {
		t.Fatalf("rows = %+v, want [alice bob carol]", res.Rows)
	}

	// 多列投影 + WHERE 引用未投影列（age 不投影但需要参与过滤）
	res = mustExec(t, e, "SELECT id, name FROM users WHERE age > 25 ORDER BY id")
	if len(res.Columns) != 2 || res.Columns[0] != "id" || res.Columns[1] != "name" {
		t.Fatalf("columns = %v, want [id name]", res.Columns)
	}
	if len(res.Rows) != 2 || res.Rows[0][0].I != 1 || res.Rows[0][1].S != "alice" {
		t.Fatalf("rows = %+v, want [[1 alice] [3 carol]]", res.Rows)
	}

	// ORDER BY 引用未投影列（投影仅 id，排序用 age）
	res = mustExec(t, e, "SELECT id FROM users ORDER BY age")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 2 || res.Rows[1][0].I != 1 || res.Rows[2][0].I != 3 {
		t.Fatalf("order rows = %+v, want [[2] [1] [3]]", res.Rows)
	}

	// 表达式投影引用列
	res = mustExec(t, e, "SELECT age + 1 FROM users WHERE id = 1")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 31 {
		t.Fatalf("expr rows = %+v, want [[31]]", res.Rows)
	}

	// CASE 投影引用列
	res = mustExec(t, e, "SELECT CASE WHEN age > 26 THEN 'old' ELSE 'young' END FROM users WHERE id = 1")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "old" {
		t.Fatalf("case rows = %+v, want [[old]]", res.Rows)
	}

	// DISTINCT（下推行经去重）
	res = mustExec(t, e, "SELECT DISTINCT age FROM users ORDER BY age")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 25 {
		t.Fatalf("distinct rows = %+v, want [[25] [30] [35]]", res.Rows)
	}

	// 限定列引用投影
	res = mustExec(t, e, "SELECT users.name FROM users WHERE users.id = 2")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "bob" {
		t.Fatalf("qualified rows = %+v, want [[bob]]", res.Rows)
	}
}

// TestProjectionPushdownAggGroup GROUP BY + 聚合引用非投影列、HAVING 引用非投影列。
func TestProjectionPushdownAggGroup(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE sales (pid INT, city TEXT, amt INT, PRIMARY KEY (pid))")
	mustExec(t, e, "INSERT INTO sales VALUES (1, 'bj', 100), (2, 'bj', 200), (3, 'sh', 150), (4, 'sh', 300)")

	// 投影 city，聚合引用非投影列 amt
	res := mustExec(t, e, "SELECT city, SUM(amt) FROM sales GROUP BY city ORDER BY city")
	if len(res.Rows) != 2 || res.Rows[0][0].S != "bj" || res.Rows[0][1].I != 300 {
		t.Fatalf("group rows = %+v, want [[bj 300] [sh 450]]", res.Rows)
	}

	// HAVING 引用非投影列（amt 聚合；bj=300、sh=450 均 >250）
	res = mustExec(t, e, "SELECT city FROM sales GROUP BY city HAVING SUM(amt) > 250 ORDER BY city")
	if len(res.Rows) != 2 || res.Rows[0][0].S != "bj" || res.Rows[1][0].S != "sh" {
		t.Fatalf("having rows = %+v, want [[bj] [sh]]", res.Rows)
	}

	// 投影单列 + 分组别名（分组排序聚合引用属既有限制 ORDER BY SUM 不支持，
	// 此处验证别名投影在 GROUP BY 下正常）
	res = mustExec(t, e, "SELECT city AS c FROM sales GROUP BY city ORDER BY c")
	if len(res.Rows) != 2 || res.Rows[0][0].S != "bj" || res.Rows[1][0].S != "sh" {
		t.Fatalf("alias rows = %+v, want [[bj] [sh]]", res.Rows)
	}
}

// TestProjectionPushdownFallbacks 回退路径：SELECT * / JOIN / FROM 子查询
// 不启用下推（needed=nil 全列），结果与原语义一致。
func TestProjectionPushdownFallbacks(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// SELECT * 星号 → 回退全列
	res := mustExec(t, e, "SELECT * FROM users ORDER BY id")
	if len(res.Columns) != 3 {
		t.Fatalf("star columns = %v, want 3 cols", res.Columns)
	}
	if len(res.Rows) != 3 || res.Rows[0][0].I != 1 || res.Rows[0][1].S != "alice" {
		t.Fatalf("star rows = %+v", res.Rows)
	}

	// JOIN 右表/左表均全列（JOIN ON 需访问两侧列）
	mustExec(t, e, "CREATE TABLE dept (id INT, name TEXT)")
	mustExec(t, e, "INSERT INTO dept VALUES (1, 'eng'), (2, 'ops')")
	res = mustExec(t, e, "SELECT u.name, d.name FROM users u JOIN dept d ON u.id = d.id ORDER BY u.id")
	if len(res.Rows) != 2 || res.Rows[0][0].S != "alice" || res.Rows[0][1].S != "eng" {
		t.Fatalf("join rows = %+v, want [[alice eng] [bob ops]]", res.Rows)
	}

	// FROM 子查询（派生表全列由子查询自身处理，外层按派生列引用）
	res = mustExec(t, e, "SELECT x.n FROM (SELECT id, name AS n FROM users) x WHERE x.id = 2")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "bob" {
		t.Fatalf("subquery rows = %+v, want [[bob]]", res.Rows)
	}
}

// TestProjectionPushdownWindow GROUP BY + 窗口：窗口参数按输出列名解析（既有语义，
// 未投影列报 'unknown column in window'，不因下推改变）；无 GROUP BY 窗口走旧路径不受影响。
func TestProjectionPushdownWindow(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE w (id INT, g TEXT, v INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO w VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)")

	// 无 GROUP BY 窗口（旧路径，不在下推范围）：结果不受影响
	// （窗口聚合为既有运行累加语义：a 组两行分别产出 10、30）
	res := mustExec(t, e, "SELECT g, SUM(v) OVER (PARTITION BY g) AS sv FROM w ORDER BY id")
	if len(res.Rows) != 3 || res.Rows[0][0].S != "a" || res.Rows[1][0].S != "a" || res.Rows[2][0].S != "b" {
		t.Fatalf("window rows = %+v, want [[a ..] [a ..] [b ..]]", res.Rows)
	}
	if res.Rows[0][1].I != 10 || res.Rows[1][1].I != 30 || res.Rows[2][1].I != 30 {
		t.Fatalf("window sv = %+v, want [[a 10] [a 30] [b 30]]", res.Rows)
	}

	// GROUP BY + 窗口参数引用未投影列：保持既有 'unknown column in window' 报错
	mustErr(t, e, "SELECT g, SUM(v) OVER (PARTITION BY g) FROM w GROUP BY g", "unknown column in window")
	mustErr(t, e, "SELECT g, SUM(v) OVER (PARTITION BY g ORDER BY v) FROM w GROUP BY g", "unknown column in window")
}

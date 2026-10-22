package sql_test

import (
	"testing"
)

// P0-4：补全 HAVING 与 GROUP BY 语义。
// 覆盖：HAVING 子句（聚合条件/分组列/全局聚合/表达式左操作数/错误路径）、
// 聚合参数放宽（表达式参数、COUNT(DISTINCT expr) 等）、
// GROUP BY 后 ORDER BY / LIMIT 语义。

// ---- HAVING 基础过滤 ----

func TestP04HavingBasic(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 聚合条件过滤：COUNT(*) > 2 两组合格
	rows := mustRows(t, e, "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept HAVING COUNT(*) > 2 ORDER BY dept")
	if len(rows) != 2 {
		t.Fatalf("having count>2: got %d rows", len(rows))
	}
	if rows[0][0].String() != "eng" || rows[0][1].I != 3 {
		t.Fatalf("row0: got %+v", rows[0])
	}
	if rows[1][0].String() != "ops" || rows[1][1].I != 3 {
		t.Fatalf("row1: got %+v", rows[1])
	}
	// 条件为假 -> 空结果
	rows = mustRows(t, e, "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept HAVING COUNT(*) > 3")
	if len(rows) != 0 {
		t.Fatalf("having count>3: want 0 rows, got %d", len(rows))
	}
	// HAVING 引用聚合别名
	rows = mustRows(t, e, "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept HAVING c = 3 ORDER BY dept")
	if len(rows) != 2 {
		t.Fatalf("having alias: got %d rows", len(rows))
	}
	// HAVING 与 WHERE 组合：WHERE 先行过滤，HAVING 聚合后再过滤
	rows = mustRows(t, e, "SELECT dept, COUNT(*) AS c FROM emp WHERE salary >= 150 GROUP BY dept HAVING COUNT(*) >= 3 ORDER BY dept")
	if len(rows) != 1 || rows[0][0].String() != "ops" || rows[0][1].I != 3 {
		t.Fatalf("where+having: got %+v", rows)
	}
}

// ---- HAVING 各类比较运算符 ----

func TestP04HavingAggOps(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// = / != / <
	rows := mustRows(t, e, "SELECT dept, SUM(salary) AS total FROM emp GROUP BY dept HAVING SUM(salary) = 500")
	if len(rows) != 1 || rows[0][0].String() != "eng" {
		t.Fatalf("having =: got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT dept, SUM(salary) AS total FROM emp GROUP BY dept HAVING SUM(salary) != 500 ORDER BY dept")
	if len(rows) != 1 || rows[0][0].String() != "ops" {
		t.Fatalf("having !=: got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT dept, SUM(salary) AS total FROM emp GROUP BY dept HAVING SUM(salary) < 550")
	if len(rows) != 1 || rows[0][0].String() != "eng" {
		t.Fatalf("having <: got %+v", rows)
	}
	// BETWEEN / IN
	rows = mustRows(t, e, "SELECT dept, SUM(salary) AS total FROM emp GROUP BY dept HAVING SUM(salary) BETWEEN 500 AND 600 ORDER BY dept")
	if len(rows) != 2 {
		t.Fatalf("having between: got %d rows", len(rows))
	}
	rows = mustRows(t, e, "SELECT dept, SUM(salary) AS total FROM emp GROUP BY dept HAVING SUM(salary) IN (500, 999) ORDER BY dept")
	if len(rows) != 1 || rows[0][0].String() != "eng" {
		t.Fatalf("having in: got %+v", rows)
	}
	// LIKE 聚合字符串（MIN(dept)）
	rows = mustRows(t, e, "SELECT dept, MIN(dept) AS m FROM emp GROUP BY dept HAVING MIN(dept) LIKE 'e%'")
	if len(rows) != 1 || rows[0][0].String() != "eng" {
		t.Fatalf("having like: got %+v", rows)
	}
}

// ---- HAVING 引用分组列 ----

func TestP04HavingGroupCol(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 分组列条件（输出中已选）
	rows := mustRows(t, e, "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept HAVING dept = 'eng'")
	if len(rows) != 1 || rows[0][0].String() != "eng" || rows[0][1].I != 3 {
		t.Fatalf("having group col: got %+v", rows)
	}
	// 未选中分组键列也可在 HAVING 引用
	rows = mustRows(t, e, "SELECT COUNT(*) AS c FROM emp GROUP BY dept HAVING dept = 'eng'")
	if len(rows) != 1 || rows[0][0].I != 3 {
		t.Fatalf("having unselected group col: got %+v", rows)
	}
	// LIKE 分组列
	rows = mustRows(t, e, "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept HAVING dept LIKE 'o%'")
	if len(rows) != 1 || rows[0][0].String() != "ops" {
		t.Fatalf("having group col like: got %+v", rows)
	}
}

// ---- HAVING 无 GROUP BY（全局聚合过滤）----

func TestP04HavingNoGroupBy(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 全局聚合 + HAVING：条件命中返回单行
	rows := mustRows(t, e, "SELECT SUM(salary) AS total FROM emp HAVING SUM(salary) > 100")
	if len(rows) != 1 || rows[0][0].I != 1100 {
		t.Fatalf("global having hit: got %+v", rows)
	}
	// 条件未命中返回空
	rows = mustRows(t, e, "SELECT SUM(salary) AS total FROM emp HAVING SUM(salary) > 99999")
	if len(rows) != 0 {
		t.Fatalf("global having miss: want 0 rows, got %d", len(rows))
	}
	// HAVING 引用聚合别名
	rows = mustRows(t, e, "SELECT SUM(salary) AS total, COUNT(*) AS n FROM emp HAVING n = 6")
	if len(rows) != 1 || rows[0][1].I != 6 {
		t.Fatalf("global having alias: got %+v", rows)
	}
}

// ---- HAVING 聚合参数为表达式 ----

func TestP04HavingExprLeft(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// SUM(salary+10)：eng=530, ops=630
	rows := mustRows(t, e, "SELECT dept, SUM(salary+10) AS total FROM emp GROUP BY dept HAVING SUM(salary+10) > 560")
	if len(rows) != 1 || rows[0][0].String() != "ops" || rows[0][1].I != 630 {
		t.Fatalf("having expr left: got %+v", rows)
	}
	// 表达式聚合别名参与 HAVING
	rows = mustRows(t, e, "SELECT dept, SUM(salary+10) AS total FROM emp GROUP BY dept HAVING total > 560")
	if len(rows) != 1 || rows[0][0].String() != "ops" {
		t.Fatalf("having expr alias: got %+v", rows)
	}
}

// ---- HAVING 错误路径 ----

func TestP04HavingErrors(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 非分组非聚合列引用报错
	mustErr(t, e, "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept HAVING id > 1", "unknown column in HAVING")
	// EXISTS 不支持
	mustErr(t, e, "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept HAVING EXISTS (SELECT 1)", "EXISTS not supported in HAVING")
	// 子查询右值在解析期即拦截（parseHavingRight 不接受子查询）
	mustErr(t, e, "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept HAVING COUNT(*) > (SELECT 1)", "expected expression")
	// 无 FROM 时 HAVING 聚合报错（走无 FROM 常量路径）
	mustErr(t, e, "SELECT COUNT(*) HAVING COUNT(*) > 1", "aggregate function without FROM")
}

// ---- 聚合参数放宽：表达式参数 ----

func TestP04AggExprParams(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 无 GROUP BY 表达式聚合（Rel 全局路径）
	rows := mustRows(t, e, "SELECT SUM(salary+100) AS s FROM emp")
	if len(rows) != 1 || rows[0][0].I != 1100+600 {
		t.Fatalf("sum expr global: got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT MAX(salary*2) AS m FROM emp")
	if len(rows) != 1 || rows[0][0].I != 600 {
		t.Fatalf("max expr global: got %+v", rows)
	}
	// 分组表达式聚合
	rows = mustRows(t, e, "SELECT dept, SUM(salary*2) AS s FROM emp GROUP BY dept ORDER BY dept")
	if len(rows) != 2 || rows[0][0].String() != "eng" || rows[0][1].I != 1000 || rows[1][1].I != 1200 {
		t.Fatalf("sum expr group: got %+v", rows)
	}
	rows = mustRows(t, e, "SELECT dept, AVG(salary+100) AS a FROM emp GROUP BY dept ORDER BY dept")
	if len(rows) != 2 || rows[0][1].I != (200+300+300)/3 || rows[1][1].I != (250+250+400)/3 {
		t.Fatalf("avg expr group: got %+v", rows)
	}
	// MIN 表达式
	rows = mustRows(t, e, "SELECT dept, MIN(salary+10) AS m FROM emp GROUP BY dept ORDER BY dept")
	if len(rows) != 2 || rows[0][1].I != 110 || rows[1][1].I != 160 {
		t.Fatalf("min expr group: got %+v", rows)
	}
}

// ---- 聚合参数放宽：COUNT/SUM DISTINCT ----

func TestP04AggDistinct(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// COUNT(DISTINCT col)：单表裸路径
	rows := mustRows(t, e, "SELECT COUNT(DISTINCT salary) AS c FROM emp")
	if len(rows) != 1 || rows[0][0].I != 4 {
		t.Fatalf("count distinct col: got %+v", rows)
	}
	// COUNT(DISTINCT expr)：Rel 路径（取模产生 3 个 distinct 值）
	rows = mustRows(t, e, "SELECT COUNT(DISTINCT salary%200) AS c FROM emp")
	// 100%200=100, 200%200=0, 150%200=150, 300%200=100 -> distinct {0,100,150}
	if len(rows) != 1 || rows[0][0].I != 3 {
		t.Fatalf("count distinct expr: got %+v", rows)
	}
	// SUM(DISTINCT col)
	rows = mustRows(t, e, "SELECT SUM(DISTINCT salary) AS s FROM emp")
	if len(rows) != 1 || rows[0][0].I != 100+150+200+300 {
		t.Fatalf("sum distinct: got %+v", rows)
	}
	// COUNT(DISTINCT *) 报错
	mustErr(t, e, "SELECT COUNT(DISTINCT *) FROM emp", "COUNT(DISTINCT *) is not valid")
	// 分组场景 DISTINCT 聚合
	rows = mustRows(t, e, "SELECT dept, COUNT(DISTINCT salary) AS c FROM emp GROUP BY dept ORDER BY dept")
	if len(rows) != 2 || rows[0][1].I != 2 || rows[1][1].I != 2 {
		t.Fatalf("count distinct group: got %+v", rows)
	}
}

// ---- GROUP BY 后 ORDER BY / LIMIT 语义 ----

func TestP04GroupOrderLimit(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// ORDER BY 聚合别名降序
	rows := mustRows(t, e, "SELECT dept, COUNT(*) AS c, SUM(salary) AS total FROM emp GROUP BY dept ORDER BY total DESC")
	if len(rows) != 2 || rows[0][0].String() != "ops" || rows[0][2].I != 600 || rows[1][0].String() != "eng" {
		t.Fatalf("order by agg alias desc: got %+v", rows)
	}
	// ORDER BY 聚合表达式别名 + LIMIT
	rows = mustRows(t, e, "SELECT dept, SUM(salary+10) AS total FROM emp GROUP BY dept ORDER BY total DESC LIMIT 1")
	if len(rows) != 1 || rows[0][0].String() != "ops" || rows[0][1].I != 630 {
		t.Fatalf("order by expr alias limit: got %+v", rows)
	}
	// ORDER BY 未选中分组键列
	rows = mustRows(t, e, "SELECT COUNT(*) AS c FROM emp GROUP BY dept ORDER BY dept")
	if len(rows) != 2 || rows[0][0].I != 3 || rows[1][0].I != 3 {
		t.Fatalf("order by unselected group col: got %+v", rows)
	}
	// ORDER BY 聚合别名 + OFFSET/LIMIT
	rows = mustRows(t, e, "SELECT dept, COUNT(*) AS c, SUM(salary) AS total FROM emp GROUP BY dept ORDER BY total DESC LIMIT 1 OFFSET 1")
	if len(rows) != 1 || rows[0][0].String() != "eng" {
		t.Fatalf("order by offset: got %+v", rows)
	}
	// HAVING 先过滤、再排序、再 LIMIT
	rows = mustRows(t, e, "SELECT dept, COUNT(*) AS c, SUM(salary) AS total FROM emp GROUP BY dept HAVING COUNT(*) >= 3 ORDER BY total DESC LIMIT 1")
	if len(rows) != 1 || rows[0][0].String() != "ops" {
		t.Fatalf("having+order+limit: got %+v", rows)
	}
	// ORDER BY 升序分组键 + LIMIT
	rows = mustRows(t, e, "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept ORDER BY dept ASC LIMIT 1")
	if len(rows) != 1 || rows[0][0].String() != "eng" {
		t.Fatalf("order by asc limit: got %+v", rows)
	}
	// 未知排序列报错
	mustErr(t, e, "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept ORDER BY unknown_col", "unknown column in ORDER BY")
}

// ---- HAVING 与 LIMIT 组合（HAVING 后分页）----

func TestP04HavingPagination(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 全组合：WHERE -> GROUP BY -> HAVING -> ORDER BY -> LIMIT/OFFSET
	rows := mustRows(t, e, "SELECT dept, SUM(salary) AS total FROM emp WHERE id >= 1 GROUP BY dept HAVING SUM(salary) > 100 ORDER BY total DESC LIMIT 1 OFFSET 0")
	if len(rows) != 1 || rows[0][0].String() != "ops" {
		t.Fatalf("full pipeline: got %+v", rows)
	}
	// 分组键排序 + 空结果（HAVING 全过滤后 OFFSET 越界为空）
	rows = mustRows(t, e, "SELECT dept, SUM(salary) AS total FROM emp GROUP BY dept HAVING SUM(salary) > 99999 ORDER BY total DESC LIMIT 5")
	if len(rows) != 0 {
		t.Fatalf("having empty: want 0 rows, got %d", len(rows))
	}
}

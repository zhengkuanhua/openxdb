package sql_test

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// T40 SQL 能力补齐：MAX/MIN 聚合、算术表达式（+ - * / %）、SELECT DISTINCT。

func setupT40(t *testing.T, e *sql.Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE t40 (id INT PRIMARY KEY, grp TEXT, val INT, price DECIMAL)")
	mustExec(t, e, "INSERT INTO t40 (id, grp, val, price) VALUES (1, 'a', 10, 1.50), (2, 'a', 20, 2.25), (3, 'a', 20, 3.00), (4, 'b', 5, 0.75), (5, 'b', 30, 9.90)")
}

func intRows(rows [][]sql.Value) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		if len(r) == 0 {
			out[i] = -999
			continue
		}
		out[i] = r[0].I
	}
	return out
}

// ---- MAX/MIN ----

func TestT40MaxMinGlobal(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT MAX(val), MIN(val) FROM t40")
	if len(rows) != 1 || rows[0][0].I != 30 || rows[0][1].I != 5 {
		t.Fatalf("MAX/MIN global: want 30/5, got %+v", rows)
	}
}

func TestT40MaxMinEmpty(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT MAX(val), MIN(val) FROM t40 WHERE id = 999")
	if len(rows) != 1 || rows[0][0].I != 0 || rows[0][1].I != 0 {
		t.Fatalf("MAX/MIN empty: want 0/0, got %+v", rows)
	}
}

func TestT40MaxMinGroupBy(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT grp, MAX(val), MIN(val) FROM t40 GROUP BY grp ORDER BY grp")
	if len(rows) != 2 {
		t.Fatalf("group by: want 2 rows, got %d", len(rows))
	}
	if rows[0][0].S != "a" || rows[0][1].I != 20 || rows[0][2].I != 10 {
		t.Fatalf("group a: got %+v", rows[0])
	}
	if rows[1][0].S != "b" || rows[1][1].I != 30 || rows[1][2].I != 5 {
		t.Fatalf("group b: got %+v", rows[1])
	}
}

func TestT40MaxMinWindow(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT id, MAX(val) OVER (PARTITION BY grp), MIN(val) OVER (PARTITION BY grp) FROM t40 ORDER BY id")
	// 窗口聚合按 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 逐行累计
	wantMax := []int64{10, 20, 20, 5, 30}
	wantMin := []int64{10, 10, 10, 5, 5}
	for i, r := range rows {
		if r[1].I != wantMax[i] || r[2].I != wantMin[i] {
			t.Fatalf("window row %d: got (%d,%d), want (%d,%d)", i, r[1].I, r[2].I, wantMax[i], wantMin[i])
		}
	}
	// 窗口内 ORDER BY 运行 MAX/MIN
	rows = mustRows(t, e, "SELECT id, MAX(val) OVER (ORDER BY id), MIN(val) OVER (ORDER BY id) FROM t40 ORDER BY id")
	wantMax = []int64{10, 20, 20, 20, 30, 30}
	wantMin = []int64{10, 10, 10, 5, 5, 5}
	for i, r := range rows {
		if r[1].I != wantMax[i] || r[2].I != wantMin[i] {
			t.Fatalf("running window row %d: got (%d,%d), want (%d,%d)", i, r[1].I, r[2].I, wantMax[i], wantMin[i])
		}
	}
}

func TestT40MaxMinExprArg(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT MAX(val+1), MIN(val*2) FROM t40")
	if len(rows) != 1 || rows[0][0].I != 31 || rows[0][1].I != 10 {
		t.Fatalf("MAX/MIN expr arg: want 31/10, got %+v", rows)
	}
}

func TestT40MaxMinAlias(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	res := mustExec(t, e, "SELECT MAX(val) AS m, MIN(val) AS n FROM t40")
	if len(res.Columns) != 2 || res.Columns[0] != "m" || res.Columns[1] != "n" {
		t.Fatalf("alias: columns %v", res.Columns)
	}
}

// ---- 算术表达式 ----

func TestT40ArithBasic(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT 1+2, 7-3, 4*5, 7/2, 7%3 FROM t40 WHERE id = 1")
	if len(rows) != 1 {
		t.Fatalf("arith: want 1 row, got %d", len(rows))
	}
	r := rows[0]
	if r[0].I != 3 || r[1].I != 4 || r[2].I != 20 {
		t.Fatalf("arith +-*: got %+v", r)
	}
	if r[3].Kind != "DECIMAL" || r[3].S != "3.5000" {
		t.Fatalf("arith 7/2: want DECIMAL 3.5000, got %+v", r[3])
	}
	if r[4].I != 1 {
		t.Fatalf("arith 7%%3: want 1, got %+v", r[4])
	}
}

func TestT40ArithPrecedence(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT 1+2*3, (1+2)*3, 10-2*3, 100/10/2, 100/(10/2) FROM t40 WHERE id = 1")
	r := rows[0]
	if r[0].I != 7 || r[1].I != 9 || r[2].I != 4 {
		t.Fatalf("precedence +*: got %+v", r)
	}
	if r[3].S != "5.0000" || r[4].S != "20.0000" {
		t.Fatalf("precedence /: got %+v", r)
	}
}

func TestT40ArithNegative(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT -5, -val, -val + 3, -(2+3) FROM t40 WHERE id = 1")
	r := rows[0]
	if r[0].I != -5 || r[1].I != -10 || r[2].I != -7 || r[3].I != -5 {
		t.Fatalf("negative: got %+v", r)
	}
}

func TestT40ArithColumnConst(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT id + 100, val * 2, val % 7 FROM t40 ORDER BY id")
	if rows[0][0].I != 101 || rows[0][1].I != 20 || rows[0][2].I != 3 {
		t.Fatalf("col+const row1: got %+v", rows[0])
	}
	if rows[4][0].I != 105 || rows[4][1].I != 60 || rows[4][2].I != 2 {
		t.Fatalf("col+const row5: got %+v", rows[4])
	}
}

func TestT40ArithInWhere(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT id FROM t40 WHERE val * 2 = 40 ORDER BY id")
	if got := intRows(rows); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("where arith: got %v", got)
	}
	rows = mustRows(t, e, "SELECT id FROM t40 WHERE id + 1 > 4 ORDER BY id")
	if got := intRows(rows); len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Fatalf("where arith2: got %v", got)
	}
}

func TestT40ArithDecimal(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT price + 0.25, price * 2, price / 2 FROM t40 WHERE id = 1")
	r := rows[0]
	if r[0].Kind != "DECIMAL" || r[0].S != "1.7500" {
		t.Fatalf("dec +: got %+v", r[0])
	}
	if r[1].Kind != "DECIMAL" || r[1].S != "3.0000" {
		t.Fatalf("dec *: got %+v", r[1])
	}
	if r[2].Kind != "DECIMAL" || r[2].S != "0.7500" {
		t.Fatalf("dec /: got %+v", r[2])
	}
	// 列与列 DECIMAL 运算
	rows = mustRows(t, e, "SELECT price - 1.50 FROM t40 WHERE id = 5")
	if rows[0][0].S != "8.4000" {
		t.Fatalf("dec col-col: got %+v", rows[0][0])
	}
}

func TestT40ArithAggExpr(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT SUM(val + 1), AVG(val * 2) FROM t40")
	// sum(11+21+21+6+31)=90；avg(20+40+40+10+60)=170/5=34
	if rows[0][0].I != 90 {
		t.Fatalf("SUM(val+1): want 90, got %+v", rows[0][0])
	}
	if rows[0][1].I != 34 {
		t.Fatalf("AVG(val*2): want 34, got %+v", rows[0][1])
	}
}

func TestT40ArithErrors(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	mustErr(t, e, "SELECT 1/0 FROM t40 WHERE id = 1", "division by zero")
	mustErr(t, e, "SELECT 1%0 FROM t40 WHERE id = 1", "division by zero")
	mustErr(t, e, "SELECT grp + 1 FROM t40", "arithmetic on non-numeric")
}

// ---- SELECT DISTINCT ----

func TestT40DistinctSingle(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT DISTINCT grp FROM t40 ORDER BY grp")
	if got := []string{rows[0][0].S, rows[1][0].S}; len(rows) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("distinct single: got %+v", rows)
	}
}

func TestT40DistinctMultiCol(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT DISTINCT grp, val FROM t40 ORDER BY grp, val")
	if len(rows) != 4 {
		t.Fatalf("distinct multi: want 4 rows, got %d: %+v", len(rows), rows)
	}
}

func TestT40DistinctWithWhere(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT DISTINCT grp FROM t40 WHERE val >= 20 ORDER BY grp")
	if len(rows) != 2 || rows[0][0].S != "a" || rows[1][0].S != "b" {
		t.Fatalf("distinct where: got %+v", rows)
	}
}

func TestT40DistinctOrderLimit(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT DISTINCT val FROM t40 ORDER BY val DESC LIMIT 2")
	if got := intRows(rows); len(got) != 2 || got[0] != 30 || got[1] != 20 {
		t.Fatalf("distinct order limit: got %v", got)
	}
	rows = mustRows(t, e, "SELECT DISTINCT val FROM t40 ORDER BY val LIMIT 3")
	if got := intRows(rows); len(got) != 3 || got[0] != 5 || got[1] != 10 || got[2] != 20 {
		t.Fatalf("distinct order limit asc: got %v", got)
	}
}

func TestT40DistinctWithAgg(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	// DISTINCT 与 GROUP BY 聚合组合：分组本身唯一，结果与去重一致
	rows := mustRows(t, e, "SELECT DISTINCT grp, MAX(val) FROM t40 GROUP BY grp ORDER BY grp")
	if len(rows) != 2 || rows[0][0].S != "a" || rows[0][1].I != 20 || rows[1][0].S != "b" || rows[1][1].I != 30 {
		t.Fatalf("distinct agg: got %+v", rows)
	}
}

func TestT40DistinctExprCol(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	// DISTINCT 作用于表达式投影列（val/10 整除），重复值被去重
	// val=10,20,20,5,30 → 1,2,2,0,3 → DISTINCT → 0,1,2,3 共 4 行
	rows := mustRows(t, e, "SELECT DISTINCT val / 10 FROM t40")
	if len(rows) != 4 {
		t.Fatalf("distinct expr: want 4 rows, got %d: %+v", len(rows), rows)
	}
}

func TestT40DistinctStar(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT DISTINCT * FROM t40")
	if len(rows) != 5 {
		t.Fatalf("distinct *: want 5 rows, got %d", len(rows))
	}
}

// ---- parser AST 单测 ----

func TestT40ParserAST(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	// DISTINCT 关键字
	res := mustExec(t, e, "SELECT DISTINCT grp FROM t40")
	if len(res.Columns) != 1 {
		t.Fatalf("distinct columns: %v", res.Columns)
	}
	// 表达式列输出名为原始文本
	res = mustExec(t, e, "SELECT val + 1 FROM t40 WHERE id = 1")
	if len(res.Columns) != 1 || !strings.Contains(res.Columns[0], "val") {
		t.Fatalf("expr column name: %v", res.Columns)
	}
	// 聚合输出名为函数原文
	res = mustExec(t, e, "SELECT MAX(val) FROM t40")
	if len(res.Columns) != 1 || res.Columns[0] != "MAX(val)" {
		t.Fatalf("agg column name: %v", res.Columns)
	}
}

// ---- GROUP BY / 全局聚合 LIMIT/OFFSET 截断（T40 遗留小坑） ----

// GROUP BY + LIMIT：截断在分组完成后生效，取前 N 组（分组按首见顺序）。
func TestT40GroupByLimit(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT grp, COUNT(*) FROM t40 GROUP BY grp LIMIT 1")
	if len(rows) != 1 {
		t.Fatalf("group by + limit 1: want 1 row, got %d (%+v)", len(rows), rows)
	}
	if rows[0][0].S != "a" || rows[0][1].I != 3 {
		t.Fatalf("group by + limit 1: got %+v, want a/3", rows[0])
	}
	rows = mustRows(t, e, "SELECT grp, COUNT(*) FROM t40 GROUP BY grp LIMIT 1 OFFSET 1")
	if len(rows) != 1 || rows[0][0].S != "b" || rows[0][1].I != 2 {
		t.Fatalf("group by + offset 1 limit 1: got %+v, want b/2", rows)
	}
}

// GROUP BY + OFFSET：跳过前 N 组（parser 仅支持 LIMIT n OFFSET m 形式）。
func TestT40GroupByOffset(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT grp, COUNT(*) FROM t40 GROUP BY grp LIMIT 100 OFFSET 1")
	if len(rows) != 1 || rows[0][0].S != "b" || rows[0][1].I != 2 {
		t.Fatalf("group by + offset 1: got %+v, want b/2", rows)
	}
	// OFFSET 越界返回空
	rows = mustRows(t, e, "SELECT grp, COUNT(*) FROM t40 GROUP BY grp LIMIT 100 OFFSET 5")
	if len(rows) != 0 {
		t.Fatalf("group by + offset beyond: want 0 rows, got %d (%+v)", len(rows), rows)
	}
}

// LIMIT 大于分组数：不截断，返回全部组。
func TestT40GroupByLimitLarge(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT grp, COUNT(*) FROM t40 GROUP BY grp LIMIT 100")
	if len(rows) != 2 {
		t.Fatalf("group by + limit 100: want 2 rows, got %d", len(rows))
	}
	// LIMIT <= 0 不截断
	rows = mustRows(t, e, "SELECT grp, COUNT(*) FROM t40 GROUP BY grp LIMIT 0")
	if len(rows) != 2 {
		t.Fatalf("group by + limit 0: want 2 rows, got %d", len(rows))
	}
}

// 全局聚合 + LIMIT/OFFSET：结果单行，LIMIT 不截断；OFFSET 越界返回空。
func TestT40AggLimitOffset(t *testing.T) {
	e := newEngine(t)
	setupT40(t, e)
	rows := mustRows(t, e, "SELECT MAX(val) FROM t40 LIMIT 1")
	if len(rows) != 1 || rows[0][0].I != 30 {
		t.Fatalf("global agg + limit 1: got %+v, want 30", rows)
	}
	rows = mustRows(t, e, "SELECT MAX(val) FROM t40 LIMIT 100 OFFSET 1")
	if len(rows) != 0 {
		t.Fatalf("global agg + offset 1: want 0 rows, got %d (%+v)", len(rows), rows)
	}
}

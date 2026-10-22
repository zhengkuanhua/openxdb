package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// M10 子功能2：窗口函数（OVER 子句）。
// 覆盖：ROW_NUMBER/RANK/DENSE_RANK 排名、SUM/AVG/COUNT 窗口聚合、
// 无 PARTITION、单列/多列 PARTITION、窗口内 ORDER BY（ASC/DESC）、
// 别名、与普通列/JOIN/CTE 混合、位置非法报错、缺 OVER 报错。

func setupEmp(t *testing.T, e *sql.Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE emp (id INT PRIMARY KEY, dept TEXT, salary INT)")
	mustExec(t, e, "INSERT INTO emp (id, dept, salary) VALUES (1, 'eng', 100)")
	mustExec(t, e, "INSERT INTO emp (id, dept, salary) VALUES (2, 'eng', 200)")
	mustExec(t, e, "INSERT INTO emp (id, dept, salary) VALUES (3, 'eng', 200)")
	mustExec(t, e, "INSERT INTO emp (id, dept, salary) VALUES (4, 'ops', 150)")
	mustExec(t, e, "INSERT INTO emp (id, dept, salary) VALUES (5, 'ops', 150)")
	mustExec(t, e, "INSERT INTO emp (id, dept, salary) VALUES (6, 'ops', 300)")
}

func TestWindowRowNumber(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	rows := mustRows(t, e, "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM emp ORDER BY id")
	want := []int64{1, 2, 3, 4, 5, 6}
	for i, w := range want {
		if len(rows) != 6 || rows[i][0].I != int64(i+1) || rows[i][1].I != w {
			t.Fatalf("row_number: want id=%d rn=%d, got %+v", i+1, w, rows)
		}
	}
}

func TestWindowRankDenseRank(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// RANK：salary 150 -> 2（有 gap）；DENSE_RANK：连续编号
	rows := mustRows(t, e, "SELECT id, RANK() OVER (ORDER BY salary), DENSE_RANK() OVER (ORDER BY salary) FROM emp ORDER BY id")
	// salary 序：100(1),150(2),150(2),200(4),200(4),300(6)；dense：100->1,150->2,200->3,300->4
	wantRank := []int64{1, 4, 4, 2, 2, 6}
	wantDense := []int64{1, 3, 3, 2, 2, 4}
	for i := 0; i < 6; i++ {
		if rows[i][1].I != wantRank[i] || rows[i][2].I != wantDense[i] {
			t.Fatalf("rank/dense: row %d got (%d,%d), want (%d,%d)", i, rows[i][1].I, rows[i][2].I, wantRank[i], wantDense[i])
		}
	}
}

func TestWindowPartitionSingle(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 单列 PARTITION：按 dept 分区，窗口内按 salary 降序编号
	rows := mustRows(t, e, "SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp ORDER BY id")
	// eng: 200(id2)->1,200(id3)->2,100(id1)->3；ops: 300(id6)->1,150(id4)->2,150(id5)->3
	want := []int64{3, 1, 2, 2, 3, 1}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("partition single: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowPartitionMulti(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 多列 PARTITION：dept+salary 联合分区，每个分区仅 1 行（eng/200 两行同分区）
	rows := mustRows(t, e, "SELECT id, ROW_NUMBER() OVER (PARTITION BY dept, salary ORDER BY id) FROM emp ORDER BY id")
	// 分区: eng/100(id1), eng/200(id2,id3), ops/150(id4,id5), ops/300(id6)
	want := []int64{1, 1, 2, 1, 2, 1}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("partition multi: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowAggRunning(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 窗口聚合：按 id 逐行累计 SUM/AVG/COUNT
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id), AVG(salary) OVER (ORDER BY id), COUNT(*) OVER (ORDER BY id) FROM emp ORDER BY id")
	wantSum := []int64{100, 300, 500, 650, 800, 1100}
	wantAvg := []int64{100, 150, 166, 162, 160, 183}
	for i := 0; i < 6; i++ {
		if rows[i][1].I != wantSum[i] || rows[i][2].I != wantAvg[i] || rows[i][3].I != int64(i+1) {
			t.Fatalf("running agg: row %d got (%d,%d,%d), want (%d,%d,%d)", i, rows[i][1].I, rows[i][2].I, rows[i][3].I, wantSum[i], wantAvg[i], int64(i+1))
		}
	}
}

func TestWindowNoPartition(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 无 PARTITION / 无 ORDER BY：整个结果集一组；RANK 全 1；SUM 按行序累计
	rows := mustRows(t, e, "SELECT id, RANK() OVER (), SUM(salary) OVER () FROM emp ORDER BY id")
	wantSum := []int64{100, 300, 500, 650, 800, 1100}
	for i := 0; i < 6; i++ {
		if rows[i][1].I != 1 || rows[i][2].I != wantSum[i] {
			t.Fatalf("no partition: row %d got (%d,%d), want (1,%d)", i, rows[i][1].I, rows[i][2].I, wantSum[i])
		}
	}
}

func TestWindowOrderDesc(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 窗口内 DESC 排序 + ROW_NUMBER
	rows := mustRows(t, e, "SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM emp ORDER BY id")
	// salary desc: 300(id6)->1,200(id2)->2,200(id3)->3,150(id4)->4,150(id5)->5,100(id1)->6
	want := []int64{6, 2, 3, 4, 5, 1}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("order desc: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowAliasAndPlainCol(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 窗口列别名 + 与普通列混合
	rows := mustRows(t, e, "SELECT id, dept, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM emp ORDER BY id")
	if len(rows) != 6 || rows[0][1].S != "eng" || rows[0][2].I != 1 || rows[5][2].I != 6 {
		t.Fatalf("alias/plain: got %+v", rows)
	}
}

func TestWindowWithJoin(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE dept (code TEXT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO dept (code, name) VALUES ('eng', 'engineering')")
	mustExec(t, e, "INSERT INTO dept (code, name) VALUES ('ops', 'operations')")
	setupEmp(t, e)
	// JOIN 后窗口函数（rel 路径）
	rows := mustRows(t, e, "SELECT e.id, d.name, ROW_NUMBER() OVER (ORDER BY e.id) FROM emp e JOIN dept d ON e.dept = d.code ORDER BY e.id")
	if len(rows) != 6 || rows[0][1].S != "engineering" || rows[0][2].I != 1 || rows[5][2].I != 6 {
		t.Fatalf("join window: got %+v", rows)
	}
}

func TestWindowWithCTE(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// CTE 与窗口函数结合
	rows := mustRows(t, e, "WITH top AS (SELECT id, dept, salary FROM emp WHERE salary >= 200) SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM top ORDER BY id")
	// id 2,3,6
	if len(rows) != 3 || rows[0][0].I != 2 || rows[0][1].I != 1 || rows[2][1].I != 3 {
		t.Fatalf("cte window: got %+v", rows)
	}
}

func TestWindowEmpty(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE emp (id INT PRIMARY KEY, dept TEXT, salary INT)")
	rows := mustRows(t, e, "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM emp")
	if len(rows) != 0 {
		t.Fatalf("empty window: got %+v", rows)
	}
}

func TestWindowMissingOver(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	mustErr(t, e, "SELECT ROW_NUMBER() FROM emp", "requires OVER clause")
	mustErr(t, e, "SELECT RANK() FROM emp", "requires OVER clause")
	mustErr(t, e, "SELECT DENSE_RANK() FROM emp", "requires OVER clause")
}

func TestWindowIllegalPosition(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// WHERE 中窗口函数
	mustErr(t, e, "SELECT id FROM emp WHERE ROW_NUMBER() OVER () = 1", "only allowed in SELECT list")
	// GROUP BY 中窗口函数
	mustErr(t, e, "SELECT dept FROM emp GROUP BY ROW_NUMBER() OVER ()", "only allowed in SELECT list")
	// ORDER BY 中窗口函数
	mustErr(t, e, "SELECT id FROM emp ORDER BY ROW_NUMBER() OVER ()", "only allowed in SELECT list")
}

func TestWindowMixedAggregate(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// P1-10：窗口函数与 GROUP BY 组合已支持，窗口在分组聚合结果上计算。
	// 分组结果两行（eng/ops），无分区无排序时 ROW_NUMBER 保持行序 1..N。
	rows := mustRows(t, e, "SELECT dept, ROW_NUMBER() OVER () FROM emp GROUP BY dept ORDER BY dept")
	if len(rows) != 2 || rows[0][0].String() != "eng" || rows[0][1].I != 1 ||
		rows[1][0].String() != "ops" || rows[1][1].I != 2 {
		t.Fatalf("window with group by: got %+v", rows)
	}
	// 窗口函数与全局聚合混用
	mustErr(t, e, "SELECT COUNT(*), ROW_NUMBER() OVER () FROM emp", "cannot be mixed with aggregate")
}

func TestWindowNonNumericAgg(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 对 TEXT 列做 SUM 窗口聚合报错
	mustErr(t, e, "SELECT id, SUM(dept) OVER (ORDER BY id) FROM emp", "non-numeric")
}

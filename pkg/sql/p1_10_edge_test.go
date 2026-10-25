package sql_test

import (
	"testing"
)

// P1-10：SQL 执行层边缘分支清理。
// 覆盖：无 FROM 常量 SELECT 列（常量/常量表达式）、无 FROM 非法引用语义化报错、
// 一元负号操作数（数值/NULL/非数值语义化报错）、窗口函数与 GROUP BY 组合、
// 同类 unsupported 路径语义化。

// ---- 无 FROM 常量 SELECT 列 ----

func TestP110ConstSelectColumn(t *testing.T) {
	e := newEngine(t)
	cases := []struct {
		stmt string
		want string
	}{
		{"SELECT 1", "1"},
		{"SELECT 'abc'", "abc"},
		{"SELECT 1+2", "3"},
		{"SELECT -1", "-1"},
		{"SELECT (1+2)*3", "9"},
		{"SELECT 1, 'abc', 1+2", "1"},
	}
	for _, c := range cases {
		rows := mustRows(t, e, c.stmt)
		if len(rows) != 1 {
			t.Fatalf("%s: want 1 row, got %d", c.stmt, len(rows))
		}
		if rows[0][0].String() != c.want {
			t.Fatalf("%s: want %s, got %+v", c.stmt, c.want, rows[0][0])
		}
	}
	// 多列常量投影：列数应与 SELECT 列表一致
	rows := mustRows(t, e, "SELECT 1, 'abc', 1+2")
	if len(rows) != 1 || len(rows[0]) != 3 {
		t.Fatalf("multi const: got %+v", rows)
	}
	if rows[0][1].String() != "abc" || rows[0][2].String() != "3" {
		t.Fatalf("multi const values: got %+v", rows[0])
	}
	// NULL 字面量：SELECT NULL / SELECT -NULL
	rows = mustRows(t, e, "SELECT NULL")
	if rows[0][0].Kind != "NULL" {
		t.Fatalf("SELECT NULL: got %+v", rows[0][0])
	}
}

func TestP110ConstColumnSemanticErrors(t *testing.T) {
	e := newEngine(t)
	// 无 FROM 纯列引用：不再是笼统 unsupported
	mustErr(t, e, "SELECT unknown_col", "column reference without FROM")
	mustErr(t, e, "SELECT t.col", "column reference without FROM")
	// 无 FROM 聚合/窗口函数：区分语义化报错
	mustErr(t, e, "SELECT COUNT(*)", "aggregate function without FROM")
	mustErr(t, e, "SELECT SUM(x)", "aggregate function without FROM")
	mustErr(t, e, "SELECT ROW_NUMBER() OVER ()", "window function without FROM")
}

// ---- 一元负号操作数 ----

func TestP110UnaryMinus(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 数值：INT 与 DECIMAL
	rows := mustRows(t, e, "SELECT -1")
	if rows[0][0].Kind != "INT" || rows[0][0].I != -1 {
		t.Fatalf("-1: got %+v", rows[0][0])
	}
	rows = mustRows(t, e, "SELECT -2.5")
	// v1.12.0 高精度：字面量按实际小数位缩放（-2.5 → scale=1，I=-25）。
	if rows[0][0].Kind != "DECIMAL" || rows[0][0].I != -25 || rows[0][0].DS != 1 {
		t.Fatalf("-2.5: got %+v", rows[0][0])
	}
	// 表达式结果取负
	rows = mustRows(t, e, "SELECT -(1+2)")
	if rows[0][0].Kind != "INT" || rows[0][0].I != -3 {
		t.Fatalf("-(1+2): got %+v", rows[0][0])
	}
	// 列值取负（带 FROM）
	rows = mustRows(t, e, "SELECT -salary FROM emp ORDER BY id LIMIT 1")
	if rows[0][0].Kind != "INT" || rows[0][0].I != -100 {
		t.Fatalf("-salary: got %+v", rows[0][0])
	}
	// NULL：-NULL = NULL
	rows = mustRows(t, e, "SELECT -NULL")
	if rows[0][0].Kind != "NULL" {
		t.Fatalf("-NULL: got %+v", rows[0][0])
	}
	// 非数值：语义化报错
	mustErr(t, e, "SELECT -'abc'", "cannot apply unary minus to TEXT value")
}

// ---- 窗口函数与 GROUP BY 组合 ----

func TestP110WindowWithGroupBy(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 窗口按分组聚合结果排序：eng/ops 各 3 行，ORDER BY c 相等时保持分组行序
	rows := mustRows(t, e, "SELECT dept, COUNT(*) AS c, ROW_NUMBER() OVER (ORDER BY c) FROM emp GROUP BY dept ORDER BY dept")
	if len(rows) != 2 {
		t.Fatalf("group by window: got %d rows", len(rows))
	}
	if rows[0][0].String() != "eng" || rows[0][1].I != 3 || rows[0][2].I != 1 {
		t.Fatalf("row0: got %+v", rows[0])
	}
	if rows[1][0].String() != "ops" || rows[1][1].I != 3 || rows[1][2].I != 2 {
		t.Fatalf("row1: got %+v", rows[1])
	}
	// 窗口排序引用聚合别名：total 降序 RANK
	rows = mustRows(t, e, "SELECT dept, SUM(salary) AS total, RANK() OVER (ORDER BY total DESC) FROM emp GROUP BY dept ORDER BY dept")
	if rows[0][0].String() != "eng" || rows[0][1].I != 500 || rows[0][2].I != 2 {
		t.Fatalf("rank eng: got %+v", rows[0])
	}
	if rows[1][0].String() != "ops" || rows[1][1].I != 600 || rows[1][2].I != 1 {
		t.Fatalf("rank ops: got %+v", rows[1])
	}
	// 窗口分区引用分组键：每分区单行
	rows = mustRows(t, e, "SELECT dept, COUNT(*) AS c, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY c) FROM emp GROUP BY dept ORDER BY dept")
	if rows[0][2].I != 1 || rows[1][2].I != 1 {
		t.Fatalf("partition rn: got %+v", rows)
	}
	// 窗口引用未输出列：语义化报错
	mustErr(t, e, "SELECT dept, ROW_NUMBER() OVER (ORDER BY salary) FROM emp GROUP BY dept", "unknown column in window: salary")
	// 窗口聚合参数为表达式：语义化报错（原始行已折叠，无法求值）
	mustErr(t, e, "SELECT dept, SUM(salary+1) OVER () FROM emp GROUP BY dept", "window expression not supported with GROUP BY")
}

// ---- 同类 unsupported 路径语义化 ----

func TestP110OtherUnsupportedSemantics(t *testing.T) {
	e := newEngine(t)
	// 二元算术非数值操作数（既有语义化报错，回归验证）
	mustErr(t, e, "SELECT 'a' + 1", "arithmetic on non-numeric operand")
	// 窗口聚合对非数值列（既有语义化报错）
	setupEmp(t, e)
	mustErr(t, e, "SELECT id, SUM(dept) OVER (ORDER BY id) FROM emp", "window aggregate on non-numeric column")
}

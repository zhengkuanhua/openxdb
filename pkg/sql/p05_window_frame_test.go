package sql_test

import (
	"testing"
)

// P0-5：显式窗口帧（ROWS/RANGE）。
// 覆盖：ROWS 帧 PRECEDING/FOLLOWING/CURRENT ROW（UNBOUNDED 与 n 偏移）、
// RANGE 帧等值行/值域语义（UNBOUNDED/CURRENT ROW/n 偏移）、
// 帧在聚合（SUM/AVG/COUNT/MAX/MIN）与排名函数上的求值、
// 帧与 PARTITION BY / GROUP BY 组合、空帧、非法帧报错。

func TestWindowFrameRowsUnboundedPreceding(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 显式 UNBOUNDED PRECEDING..CURRENT ROW 与默认帧一致
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM emp ORDER BY id")
	want := []int64{100, 300, 500, 650, 800, 1100}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("rows unbounded preceding: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowFrameRowsMovingSumAvg(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// ROWS BETWEEN 1 PRECEDING AND CURRENT ROW：移动求和/平均
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), AVG(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM emp ORDER BY id")
	wantSum := []int64{100, 300, 400, 350, 300, 450}
	wantAvg := []int64{100, 150, 200, 175, 150, 225}
	for i := 0; i < 6; i++ {
		if rows[i][1].I != wantSum[i] || rows[i][2].I != wantAvg[i] {
			t.Fatalf("rows moving: row %d got (%d,%d), want (%d,%d)", i, rows[i][1].I, rows[i][2].I, wantSum[i], wantAvg[i])
		}
	}
}

func TestWindowFrameRowsCurrentToUnboundedFollowing(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING：后缀和
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM emp ORDER BY id")
	want := []int64{1100, 1000, 800, 600, 450, 300}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("rows current to unbounded following: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowFrameRowsSliding(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING：滑动窗口
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM emp ORDER BY id")
	want := []int64{500, 650, 800, 1000, 800, 600}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("rows sliding: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowFrameRowsSingleBound(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 单边界简写：ROWS 2 PRECEDING == 2 PRECEDING..CURRENT ROW
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS 2 PRECEDING) FROM emp ORDER BY id")
	want := []int64{100, 300, 500, 550, 500, 600}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("rows 2 preceding: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
	// 单边界简写：ROWS 2 FOLLOWING == CURRENT ROW..2 FOLLOWING
	rows = mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS 2 FOLLOWING) FROM emp ORDER BY id")
	want = []int64{500, 550, 500, 600, 450, 300}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("rows 2 following: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowFrameRowsBackwardEmpty(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 反向帧（end < start）为空帧：SUM/MAX/MIN/AVG 为 NULL，COUNT 为 0
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), COUNT(*) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), MAX(salary) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) FROM emp ORDER BY id")
	wantSum := []int64{-1, 100, 300, 500, 550, 500} // -1 标记 NULL
	wantCnt := []int64{0, 1, 2, 3, 3, 3}
	for i := 0; i < 6; i++ {
		if rows[i][2].I != wantCnt[i] {
			t.Fatalf("backward empty count: row %d got %d, want %d", i, rows[i][2].I, wantCnt[i])
		}
		if i == 0 {
			if rows[i][1].Kind != "NULL" || rows[i][3].Kind != "NULL" {
				t.Fatalf("backward empty: row %d got %+v, want NULL", i, rows[i])
			}
			continue
		}
		if rows[i][1].I != wantSum[i] {
			t.Fatalf("backward empty sum: row %d got %d, want %d", i, rows[i][1].I, wantSum[i])
		}
	}
}

func TestWindowFrameRankingUnaffected(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 排名函数忽略帧（帧只作用于聚合），帧语法仍须合法
	rows := mustRows(t, e, "SELECT id, ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), RANK() OVER (ORDER BY salary ROWS UNBOUNDED PRECEDING), DENSE_RANK() OVER (ORDER BY salary ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM emp ORDER BY id")
	wantRank := []int64{1, 4, 4, 2, 2, 6}
	wantDense := []int64{1, 3, 3, 2, 2, 4}
	for i := 0; i < 6; i++ {
		if rows[i][1].I != int64(i+1) || rows[i][2].I != wantRank[i] || rows[i][3].I != wantDense[i] {
			t.Fatalf("ranking with frame: row %d got (%d,%d,%d), want (%d,%d,%d)", i, rows[i][1].I, rows[i][2].I, rows[i][3].I, int64(i+1), wantRank[i], wantDense[i])
		}
	}
}

func TestWindowFrameRangeUnboundedPreceding(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// RANGE UNBOUNDED PRECEDING：与当前键等值的行整体纳入帧
	// salary 序：100(1),150(4),150(5),200(2),200(3),300(6)
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY salary RANGE UNBOUNDED PRECEDING) FROM emp ORDER BY id")
	want := []int64{100, 800, 800, 400, 400, 1100}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("range unbounded preceding: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowFrameRangeCurrentRow(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// RANGE CURRENT ROW：帧 = 与当前键等值的全部行
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY salary RANGE CURRENT ROW) FROM emp ORDER BY id")
	want := []int64{100, 400, 400, 300, 300, 300}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("range current row: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowFrameRangeOffset(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING：键值域 [cur-1, cur+1] 内全部行
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (ORDER BY salary RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM emp ORDER BY id")
	want := []int64{100, 400, 400, 300, 300, 300}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("range offset: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowFrameRangeNoOrderBy(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// RANGE 帧无 ORDER BY：退化整分区（分区内全部行）
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (PARTITION BY dept RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM emp ORDER BY id")
	want := []int64{500, 500, 500, 600, 600, 600}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("range no order by: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowFramePartitionRows(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 帧 + PARTITION BY：帧在分区内独立计算
	rows := mustRows(t, e, "SELECT id, SUM(salary) OVER (PARTITION BY dept ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM emp ORDER BY id")
	want := []int64{100, 300, 400, 150, 300, 450}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("partition rows: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowFrameMaxMin(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 帧内 MAX/MIN
	rows := mustRows(t, e, "SELECT id, MAX(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), MIN(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM emp ORDER BY id")
	wantMax := []int64{200, 200, 200, 200, 300, 300}
	wantMin := []int64{100, 100, 150, 150, 150, 150}
	for i := 0; i < 6; i++ {
		if rows[i][1].I != wantMax[i] || rows[i][2].I != wantMin[i] {
			t.Fatalf("frame max/min: row %d got (%d,%d), want (%d,%d)", i, rows[i][1].I, rows[i][2].I, wantMax[i], wantMin[i])
		}
	}
}

func TestWindowFrameExprParam(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 窗口聚合表达式参数在帧内求值：salary+1 移动求和
	rows := mustRows(t, e, "SELECT id, SUM(salary + 1) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM emp ORDER BY id")
	want := []int64{101, 302, 402, 352, 302, 452}
	for i, w := range want {
		if rows[i][1].I != w {
			t.Fatalf("expr param frame: row %d got %d, want %d", i, rows[i][1].I, w)
		}
	}
}

func TestWindowFrameWithGroupBy(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// GROUP BY + 窗口帧：窗口在分组输出行上计算，聚合参数按输出列名解析
	rows := mustRows(t, e, "SELECT dept, COUNT(*) AS c, SUM(c) OVER (ORDER BY dept ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM emp GROUP BY dept ORDER BY dept")
	if len(rows) != 2 {
		t.Fatalf("group by frame: got %d rows", len(rows))
	}
	if rows[0][0].String() != "eng" || rows[0][1].I != 3 || rows[0][2].I != 3 {
		t.Fatalf("group by frame eng: got %+v", rows[0])
	}
	if rows[1][0].String() != "ops" || rows[1][1].I != 3 || rows[1][2].I != 6 {
		t.Fatalf("group by frame ops: got %+v", rows[1])
	}
	// COUNT(*) 窗口帧在分组输出行上统计帧内行数
	rows = mustRows(t, e, "SELECT dept, COUNT(*) AS c, COUNT(*) OVER (ORDER BY dept ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM emp GROUP BY dept ORDER BY dept")
	if rows[0][2].I != 1 || rows[1][2].I != 2 {
		t.Fatalf("group by frame count: got %+v", rows)
	}
}

func TestWindowFrameParseErrors(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// 起点不能是 UNBOUNDED FOLLOWING
	mustErr(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM emp", "UNBOUNDED FOLLOWING cannot be the window frame start")
	// 终点不能是 UNBOUNDED PRECEDING
	mustErr(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM emp", "UNBOUNDED PRECEDING cannot be the window frame end")
	// 帧边界后必须跟合法方向词
	mustErr(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS 2 SIDEWAYS) FROM emp", "PRECEDING or FOLLOWING")
	// 缺帧边界（ROWS 后直接右括号）
	mustErr(t, e, "SELECT id, SUM(salary) OVER (ORDER BY id ROWS) FROM emp", "UNBOUNDED")
}

func TestWindowFrameRangeNonNumericOffset(t *testing.T) {
	e := newEngine(t)
	setupEmp(t, e)
	// RANGE 偏移值域要求数值排序列
	mustErr(t, e, "SELECT id, SUM(salary) OVER (ORDER BY dept RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM emp", "RANGE frame with offset requires numeric ORDER BY key")
}

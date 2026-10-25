package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// v1.12.0 DECIMAL 高精度扩展：CREATE TABLE decimal(p,s) 声明 s>4 时全链路保持精度。
func TestV112DecimalHighPrecisionRoundTrip(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE hp (id INT, price DECIMAL(12,6), PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO hp VALUES (1, 123.456789), (2, -0.000001), (3, 0.000001), (4, 1.5)")
	rows := mustRows(t, e, "SELECT id, price FROM hp ORDER BY id")
	want := []string{"123.456789", "-0.000001", "0.000001", "1.500000"}
	if len(rows) != len(want) {
		t.Fatalf("hp rows: got %d, want %d (%+v)", len(rows), len(want), rows)
	}
	for i, w := range want {
		if rows[i][1].Kind != "DECIMAL" {
			t.Fatalf("row %d kind: want DECIMAL got %s", i, rows[i][1].Kind)
		}
		if rows[i][1].S != w {
			t.Fatalf("row %d price: want %s got %s (I=%d)", i, w, rows[i][1].S, rows[i][1].I)
		}
	}
	// 索引/排序按声明 scale 比较。
	rows = mustRows(t, e, "SELECT id FROM hp ORDER BY price DESC")
	if rows[0][0].I != 1 {
		t.Fatalf("order desc top: want id 1, got %+v", rows[0])
	}
	// 等值比较：列 scale 6 与跨位数字面量。
	rows = mustRows(t, e, "SELECT id FROM hp WHERE price = 0.000001")
	if len(rows) != 1 || rows[0][0].I != 3 {
		t.Fatalf("eq 0.000001: got %+v", rows)
	}
}

// 高精度算术：字面量按实际位数保留，算术结果按较大 scale 对齐。
func TestV112DecimalHighPrecisionArith(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE ar (id INT, a DECIMAL(12,6), PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO ar VALUES (1, 123.456789)")
	rows := mustRows(t, e, "SELECT a + 0.000001, a - 0.000001 FROM ar WHERE id = 1")
	if len(rows) != 1 {
		t.Fatalf("arith rows: %+v", rows)
	}
	if rows[0][0].Kind != "DECIMAL" || rows[0][0].S != "123.456790" {
		t.Fatalf("a+0.000001: got %+v (%s)", rows[0][0], rows[0][0].S)
	}
	if rows[0][1].Kind != "DECIMAL" || rows[0][1].S != "123.456788" {
		t.Fatalf("a-0.000001: got %+v (%s)", rows[0][1], rows[0][1].S)
	}
	// 跨 scale 乘法：1.5(ds1) * 123.456789(ds6) → ds6 对齐。
	rows = mustRows(t, e, "SELECT a * 1.5 FROM ar WHERE id = 1")
	if rows[0][0].Kind != "DECIMAL" || rows[0][0].S != "185.185183" {
		t.Fatalf("a*1.5: got %+v (%s)", rows[0][0], rows[0][0].S)
	}
	// 一元负号保留 scale。
	rows = mustRows(t, e, "SELECT -a FROM ar WHERE id = 1")
	if rows[0][0].Kind != "DECIMAL" || rows[0][0].S != "-123.456789" {
		t.Fatalf("-a: got %+v (%s)", rows[0][0], rows[0][0].S)
	}
}

// 高精度聚合 SUM 取列声明 scale。
func TestV112DecimalHighPrecisionAgg(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE ag (id INT, amt DECIMAL(12,6), PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO ag VALUES (1, 0.000001), (2, 0.000002), (3, 0.000003)")
	rows := mustRows(t, e, "SELECT SUM(amt) FROM ag")
	if len(rows) != 1 || rows[0][0].Kind != "DECIMAL" {
		t.Fatalf("sum rows: %+v", rows)
	}
	if rows[0][0].S != "0.000006" {
		t.Fatalf("SUM: want 0.000006 got %s", rows[0][0].S)
	}
}

// 高精度列 INSERT 超声明 scale 四舍五入（MySQL 兼容）。
func TestV112DecimalRoundOnInsert(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE rd (id INT, price DECIMAL(10,2), PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO rd VALUES (1, 1.235), (2, 1.234), (3, 0.005)")
	rows := mustRows(t, e, "SELECT id, price FROM rd ORDER BY id")
	want := []string{"1.24", "1.23", "0.01"}
	for i, w := range want {
		if rows[i][1].S != w {
			t.Fatalf("rd[%d]: want %s got %s", i, w, rows[i][1].S)
		}
	}
}

// ALTER MODIFY 修改 DECIMAL 小数位后存量值重缩放。
func TestV112DecimalAlterScaleRescale(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE am (id INT, price DECIMAL(10,2), PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO am VALUES (1, 12.34)")
	mustExec(t, e, "ALTER TABLE am MODIFY price DECIMAL(12,6)")
	rows := mustRows(t, e, "SELECT price FROM am WHERE id = 1")
	if len(rows) != 1 || rows[0][0].S != "12.340000" {
		t.Fatalf("after modify: got %+v (%s)", rows[0][0], rows[0][0].S)
	}
}

// DECIMAL(p,s) 声明越界（s>12 / s<0）报解析错误。
func TestV112DecimalScaleBounds(t *testing.T) {
	e := newEngine(t)
	mustErr(t, e, "CREATE TABLE b1 (id INT, price DECIMAL(10,13), PRIMARY KEY (id))", "scale")
	mustErr(t, e, "CREATE TABLE b2 (id INT, price DECIMAL(10,-1), PRIMARY KEY (id))", "scale")
	_ = sql.Value{}
}

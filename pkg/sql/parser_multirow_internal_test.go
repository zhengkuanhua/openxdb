package sql

import "testing"

// 多行 VALUES INSERT 解析器单测（内部包：直接断言 AST 结构）。
// 覆盖：多行解析正确性、单行兼容、显式列/无列列表、REPLACE/ON DUPLICATE 与错误用例。

func TestParseInsertMultiRowValues(t *testing.T) {
	stmt, err := Parse("INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	if err != nil {
		t.Fatalf("parse multi-row: %v", err)
	}
	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("stmt type: got %T, want *InsertStmt", stmt)
	}
	if ins.Table != "t" {
		t.Errorf("table: want t, got %q", ins.Table)
	}
	if len(ins.Columns) != 2 || ins.Columns[0] != "id" || ins.Columns[1] != "name" {
		t.Errorf("columns: want [id name], got %v", ins.Columns)
	}
	if ins.Replace {
		t.Error("Replace: want false for plain INSERT")
	}
	if ins.OnDup != nil {
		t.Errorf("OnDup: want nil, got %+v", ins.OnDup)
	}
	if len(ins.Rows) != 3 {
		t.Fatalf("rows: want 3, got %d", len(ins.Rows))
	}
	for i, row := range ins.Rows {
		if len(row) != 2 {
			t.Fatalf("row %d: want 2 values, got %d", i, len(row))
		}
		if row[0].Kind != "INT" || row[0].I != int64(i+1) {
			t.Errorf("row %d col0: want INT %d, got %+v", i, i+1, row[0])
		}
		if row[1].Kind != "TEXT" || row[1].S != string(rune('a'+i)) {
			t.Errorf("row %d col1: want TEXT %q, got %+v", i, string(rune('a'+i)), row[1])
		}
	}
}

func TestParseInsertMultiRowNoColumnList(t *testing.T) {
	stmt, err := Parse("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ins := stmt.(*InsertStmt)
	if len(ins.Columns) != 0 {
		t.Errorf("columns: want empty, got %v", ins.Columns)
	}
	if len(ins.Rows) != 2 {
		t.Fatalf("rows: want 2, got %d", len(ins.Rows))
	}
}

// 单行形式与多行形式保持完全兼容（Rows 长度为 1）。
func TestParseInsertSingleRowCompat(t *testing.T) {
	stmt, err := Parse("INSERT INTO t (id, name) VALUES (7, 'x')")
	if err != nil {
		t.Fatalf("parse single-row: %v", err)
	}
	ins := stmt.(*InsertStmt)
	if len(ins.Rows) != 1 {
		t.Fatalf("rows: want 1, got %d", len(ins.Rows))
	}
	if len(ins.Rows[0]) != 2 || ins.Rows[0][0].I != 7 || ins.Rows[0][1].S != "x" {
		t.Errorf("single row: want [INT 7 TEXT x], got %+v", ins.Rows[0])
	}
}

// 多行 + REPLACE INTO（Replace=true）。
func TestParseReplaceMultiRow(t *testing.T) {
	stmt, err := Parse("REPLACE INTO t (id, name) VALUES (1, 'a'), (2, 'b')")
	if err != nil {
		t.Fatalf("parse replace multi-row: %v", err)
	}
	ins := stmt.(*InsertStmt)
	if !ins.Replace {
		t.Error("Replace: want true")
	}
	if len(ins.Rows) != 2 {
		t.Fatalf("rows: want 2, got %d", len(ins.Rows))
	}
}

// 多行 + ON DUPLICATE KEY UPDATE（OnDup 非空且仅一组更新项作用于冲突行）。
func TestParseInsertMultiRowOnDup(t *testing.T) {
	stmt, err := Parse("INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b') ON DUPLICATE KEY UPDATE name = 'dup'")
	if err != nil {
		t.Fatalf("parse multi-row on dup: %v", err)
	}
	ins := stmt.(*InsertStmt)
	if ins.OnDup == nil || len(ins.OnDup.Sets) != 1 {
		t.Fatalf("OnDup: want 1 set item, got %+v", ins.OnDup)
	}
	if len(ins.Rows) != 2 {
		t.Fatalf("rows: want 2, got %d", len(ins.Rows))
	}
}

// 多行 DECIMAL / 负数等非平凡字面量（确保每行独立解析）。
func TestParseInsertMultiRowValueKinds(t *testing.T) {
	stmt, err := Parse("INSERT INTO t VALUES (1, 12.34, 'a'), (-2, -0.5, 'b')")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ins := stmt.(*InsertStmt)
	if len(ins.Rows) != 2 {
		t.Fatalf("rows: want 2, got %d", len(ins.Rows))
	}
	if ins.Rows[0][1].Kind != "DECIMAL" || ins.Rows[0][1].S != "12.3400" {
		t.Errorf("row0 dec: got %+v", ins.Rows[0][1])
	}
	if ins.Rows[1][0].Kind != "INT" || ins.Rows[1][0].I != -2 {
		t.Errorf("row1 col0: got %+v", ins.Rows[1][0])
	}
}

// 解析期错误用例：语法不合法必须报错（不允许静默截断或接受残缺多行）。
func TestParseInsertMultiRowErrors(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"no left paren", "INSERT INTO t VALUES 1, 2"},
		{"unterminated row", "INSERT INTO t VALUES (1, 'a'"},
		{"unterminated second row", "INSERT INTO t VALUES (1, 'a'), (2, 'b'"},
		{"missing comma between rows", "INSERT INTO t VALUES (1, 'a') (2, 'b')"},
		{"trailing comma", "INSERT INTO t VALUES (1, 'a'),"},
		{"empty values", "INSERT INTO t VALUES"},
		{"row without close paren", "INSERT INTO t VALUES (1"},
		{"garbage after rows", "INSERT INTO t VALUES (1, 'a'), (2, 'b') xyz"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Parse(c.sql); err == nil {
				t.Fatalf("Parse(%q): want error, got nil", c.sql)
			}
		})
	}
}

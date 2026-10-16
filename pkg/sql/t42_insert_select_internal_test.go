package sql

import "testing"

// T42 内部测试：INSERT INTO ... SELECT 的 parser AST 结构。
// 需要访问 parse / InsertStmt / SelectStmt 等包内符号。

func TestT42InsertSelectParserAST(t *testing.T) {
	stmt, err := Parse("INSERT INTO dst SELECT id, name FROM src WHERE id > 1")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", stmt)
	}
	if ins.Select == nil {
		t.Fatal("expected InsertStmt.Select to be set")
	}
	sel := ins.Select
	if sel.From != "src" {
		t.Fatalf("expected FROM src, got %q", sel.From)
	}
	if sel.Where == nil {
		t.Fatal("expected WHERE clause parsed")
	}
	if sel.Distinct {
		t.Fatal("expected Distinct=false")
	}

	// REPLACE ... SELECT
	rstmt, err := Parse("REPLACE INTO dst SELECT * FROM src")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	rins, ok := rstmt.(*InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", rstmt)
	}
	if !rins.Replace {
		t.Fatal("expected Replace=true")
	}
	if rins.Select == nil {
		t.Fatal("expected REPLACE InsertStmt.Select to be set")
	}
}

func TestT42InsertSelectParserDistinct(t *testing.T) {
	stmt, err := Parse("INSERT INTO dst SELECT DISTINCT city FROM src")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	ins := stmt.(*InsertStmt)
	sel := ins.Select
	if !sel.Distinct {
		t.Fatal("expected Distinct=true")
	}
}

func TestT42InsertSelectParserNoColumnList(t *testing.T) {
	stmt, err := Parse("INSERT INTO dst SELECT * FROM src")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	ins := stmt.(*InsertStmt)
	if len(ins.Columns) != 0 {
		t.Fatalf("expected empty Columns, got %+v", ins.Columns)
	}
	if ins.Select == nil {
		t.Fatal("expected Select set")
	}
}

func TestT42InsertSelectParserValuesStillWorks(t *testing.T) {
	stmt, err := Parse("INSERT INTO dst (id, name) VALUES (1, 'a'), (2, 'b')")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	ins := stmt.(*InsertStmt)
	if ins.Select != nil {
		t.Fatal("expected Select nil for VALUES branch")
	}
	if len(ins.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(ins.Rows))
	}
}

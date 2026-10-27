package sql

import (
	"testing"
)

// v1.15.0 W4 单元测试：候选判定与已序验证（比较基准 compareVal），
// 覆盖编码序偏离比较序的关键回退场景。

// TestOrderByScanCandidate 主键候选判定：单列主键 / 复合主键首列命中，
// DESC 与限定列引用不命中。
func TestOrderByScanCandidate(t *testing.T) {
	meta := &TableMeta{Name: "t", PK: "id", PKCols: []string{"id"}, Columns: []ColumnDef{{Name: "id", Type: "INT"}}}
	s := &SelectStmt{OrderBy: &OrderBy{Col: "id"}}
	if !orderByScanCandidate(meta, s) {
		t.Fatalf("single PK candidate miss")
	}
	s.OrderBy.Desc = true
	if orderByScanCandidate(meta, s) {
		t.Fatalf("desc must not candidate")
	}
	s.OrderBy.Desc = false
	s.OrderBy.Ref = "t"
	if orderByScanCandidate(meta, s) {
		t.Fatalf("qualified ref must not candidate")
	}
	// 复合主键首列
	meta.PK = ""
	meta.PKCols = []string{"a", "b"}
	s.OrderBy.Ref = ""
	s.OrderBy.Col = "a"
	if !orderByScanCandidate(meta, s) {
		t.Fatalf("composite first col candidate miss")
	}
	s.OrderBy.Col = "b"
	if orderByScanCandidate(meta, s) {
		t.Fatalf("composite non-first col must not candidate")
	}
}

// TestOrderByIndexCandidate 单列二级索引候选判定：单列索引命中，
// 复合索引 / 无索引列不命中。
func TestOrderByIndexCandidate(t *testing.T) {
	meta := &TableMeta{Name: "t", PK: "id", Columns: []ColumnDef{{Name: "id", Type: "INT"}, {Name: "age", Type: "INT"}}}
	meta.Indexes = []IndexMeta{{ID: 1, Name: "idx_age", Col: "age"}}
	s := &SelectStmt{OrderBy: &OrderBy{Col: "age"}}
	if orderByIndexCandidate(meta, s) == nil {
		t.Fatalf("single idx candidate miss")
	}
	// 复合索引（Cols 非空）→ 保守不候选
	meta.Indexes[0].Col = ""
	meta.Indexes[0].Cols = []string{"age", "name"}
	if orderByIndexCandidate(meta, s) != nil {
		t.Fatalf("composite idx must not candidate")
	}
	// 无索引列
	meta.Indexes = nil
	s.OrderBy.Col = "name"
	if orderByIndexCandidate(meta, s) != nil {
		t.Fatalf("no idx must nil")
	}
}

// TestRowsSortedByValue 已序验证：有序通过、乱序/编码序偏离必须失败回退。
func TestRowsSortedByValue(t *testing.T) {
	meta := &TableMeta{Name: "t", PK: "id", Columns: []ColumnDef{{Name: "id", Type: "INT"}, {Name: "age", Type: "INT"}}}
	ob := &OrderBy{Col: "age"}
	rows := [][]Value{
		{{Kind: "INT", I: 1}, {Kind: "INT", I: 25}},
		{{Kind: "INT", I: 2}, {Kind: "INT", I: 30}},
		{{Kind: "INT", I: 3}, {Kind: "INT", I: 35}},
	}
	if !rowsSortedByValue(meta, rows, ob) {
		t.Fatalf("sorted rows must pass")
	}
	// 乱序
	rows[1], rows[2] = rows[2], rows[1]
	if rowsSortedByValue(meta, rows, ob) {
		t.Fatalf("unsorted rows must fail")
	}
	// TEXT 列字典序（bob > carol，carol 不应通过升序验证）
	meta2 := &TableMeta{Name: "t", PK: "id", Columns: []ColumnDef{{Name: "id", Type: "INT"}, {Name: "name", Type: "TEXT"}}}
	ob2 := &OrderBy{Col: "name"}
	rows2 := [][]Value{
		{{Kind: "INT", I: 1}, {Kind: "TEXT", S: "alice"}},
		{{Kind: "INT", I: 2}, {Kind: "TEXT", S: "carol"}},
		{{Kind: "INT", I: 3}, {Kind: "TEXT", S: "bob"}},
	}
	if rowsSortedByValue(meta2, rows2, ob2) {
		t.Fatalf("bob < carol must fail")
	}
	// DESC 验证语义：真正降序有序通过（carol > bob > alice）
	rows2 = [][]Value{
		{{Kind: "INT", I: 3}, {Kind: "TEXT", S: "carol"}},
		{{Kind: "INT", I: 2}, {Kind: "TEXT", S: "bob"}},
		{{Kind: "INT", I: 1}, {Kind: "TEXT", S: "alice"}},
	}
	ob2.Desc = true
	if !rowsSortedByValue(meta2, rows2, ob2) {
		t.Fatalf("desc sorted rows must pass")
	}
}

// TestOrderBySinglePKSafe 分布式 Top 快路径静态安全类型判定：
// 仅 TEXT/DATE/BLOB 单列主键安全，INT/复合主键保守回退。
func TestOrderBySinglePKSafe(t *testing.T) {
	meta := &TableMeta{Name: "t", PK: "id", Columns: []ColumnDef{{Name: "id", Type: "INT"}}}
	s := &SelectStmt{OrderBy: &OrderBy{Col: "id"}}
	if orderBySinglePKSafe(meta, s) {
		t.Fatalf("INT pk not safe")
	}
	meta.Columns[0].Type = "TEXT"
	if !orderBySinglePKSafe(meta, s) {
		t.Fatalf("TEXT pk safe")
	}
	// 复合主键不适用
	meta.PKCols = []string{"a", "b"}
	if orderBySinglePKSafe(meta, s) {
		t.Fatalf("composite pk not safe")
	}
}

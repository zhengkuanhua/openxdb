package sql

import (
	"testing"
)

// v1.15.0 W3：decodeRowSelected 按需解码——仅还原指定列，
// 未选列即使编码损坏也不解析（证明跳过未投影列 decode）。

func TestDecodeRowSelectedSkipsUnselected(t *testing.T) {
	meta := &TableMeta{
		Columns: []ColumnDef{
			{Name: "a", Type: "INT"},
			{Name: "b", Type: "TEXT"},
			{Name: "c", Type: "DECIMAL"},
		},
	}
	raw := []byte(`[1,"hello","12.34"]`)

	// 选中全部列时结果与 decodeRow 一致（列序按 idxs）
	all, err := decodeRowSelected(meta, raw, []int{0, 1, 2})
	if err != nil {
		t.Fatalf("decode all: %v", err)
	}
	if len(all) != 3 || all[0].Kind != "INT" || all[0].I != 1 || all[1].S != "hello" {
		t.Fatalf("decode all values = %+v", all)
	}
	if all[2].Kind != "DECIMAL" || all[2].S == "" {
		t.Fatalf("decode decimal = %+v", all[2])
	}

	// 仅选 [a,b]：c 不还原
	sub, err := decodeRowSelected(meta, raw, []int{0, 1})
	if err != nil {
		t.Fatalf("decode subset: %v", err)
	}
	if len(sub) != 2 || sub[0].I != 1 || sub[1].S != "hello" {
		t.Fatalf("decode subset values = %+v", sub)
	}

	// 仅选 [a]：未选 c 编码损坏（DECIMAL 列存数字而非字符串）→ decodeRowSelected 成功，
	// 而 decodeRow 全列解析必失败，证明未投影列完全跳过类型还原。
	bad := []byte(`[1,"hello",123]`)
	one, err := decodeRowSelected(meta, bad, []int{0})
	if err != nil {
		t.Fatalf("decode selected with bad unselected col: %v", err)
	}
	if len(one) != 1 || one[0].I != 1 {
		t.Fatalf("decode selected values = %+v", one)
	}
	if _, err := decodeRow(meta, bad); err == nil {
		t.Fatalf("decodeRow on corrupted col: want error, got nil")
	}

	// NULL 未选/选中均正常
	nullRaw := []byte(`[null,"x","1.20"]`)
	nv, err := decodeRowSelected(meta, nullRaw, []int{0, 2})
	if err != nil {
		t.Fatalf("decode null subset: %v", err)
	}
	if len(nv) != 2 || nv[0].Kind != "NULL" || nv[1].Kind != "DECIMAL" {
		t.Fatalf("decode null subset values = %+v", nv)
	}
	// 行数不匹配 → 报 corrupted row（与 decodeRow 一致）
	if _, err := decodeRowSelected(meta, []byte(`[1]`), []int{0}); err == nil {
		t.Fatalf("corrupted row: want error, got nil")
	}
}

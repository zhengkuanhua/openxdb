package sql_test

import (
	"testing"
)

// D7.3：SHOW INDEXES 复数识别（排期 Phase G / G5）。
// SHOW INDEXES 是 SHOW INDEX 的 MySQL 复数同义词，解析结果与执行结果应完全一致。

func TestD7ShowIndexesPlural(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_d7i (id INT PRIMARY KEY, name TEXT, age INT)")
	mustExec(t, e, "CREATE INDEX idx_d7i_name ON t_d7i (name)")
	mustExec(t, e, "CREATE INDEX idx_d7i_age ON t_d7i (age)")

	singular := mustRows(t, e, "SHOW INDEX FROM t_d7i")
	plural := mustRows(t, e, "SHOW INDEXES FROM t_d7i")
	if len(singular) == 0 {
		t.Fatalf("SHOW INDEX: want >0 rows, got 0")
	}
	if len(singular) != len(plural) {
		t.Fatalf("SHOW INDEXES: want %d rows (same as SHOW INDEX), got %d", len(singular), len(plural))
	}
	// 逐行比对（索引名 + 列名语义一致）
	for i := range singular {
		if singular[i][1].String() != plural[i][1].String() || singular[i][3].String() != plural[i][3].String() {
			t.Fatalf("row %d mismatch: singular=%+v plural=%+v", i, singular[i], plural[i])
		}
	}
	// 复数形式同样支持 FROM 缺省表报错
	mustErr(t, e, "SHOW INDEXES FROM nope", "not exists")
}

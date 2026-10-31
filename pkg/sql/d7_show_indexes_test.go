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

// D7.3-fix：SHOW INDEX / INDEXES 关键字大小写不敏感（与 MySQL 语义一致）。
// 实测发现小写 "show indexes from t" 未识别（INDEXES 为 tokIdent、switch 原样匹配），
// parseShow 已改为 ToUpper 归一化；此处覆盖四种大小写组合的等价性。
func TestD7ShowIndexesCaseInsensitive(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_d7i_ci (id INT PRIMARY KEY, name TEXT, age INT)")
	mustExec(t, e, "CREATE INDEX idx_ci_name ON t_d7i_ci (name)")

	upper := mustRows(t, e, "SHOW INDEX FROM t_d7i_ci")
	for _, sql := range []string{
		"SHOW INDEXES FROM t_d7i_ci",
		"show indexes from t_d7i_ci",
		"show index from t_d7i_ci",
	} {
		got := mustRows(t, e, sql)
		if len(got) != len(upper) {
			t.Fatalf("%s: want %d rows (same as SHOW INDEX), got %d", sql, len(upper), len(got))
		}
		for i := range upper {
			if upper[i][1].String() != got[i][1].String() || upper[i][3].String() != got[i][3].String() {
				t.Fatalf("%s: row %d mismatch: upper=%+v got=%+v", sql, i, upper[i], got[i])
			}
		}
	}
}

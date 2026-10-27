package sql_test

import (
	"testing"
)

// v1.14.0 P0：COUNT(*) 快速路径（executor.tryCountFastPath）。
// 覆盖：快路径与旧实现数值/列名一致、条件不满足回退、事务写回退。

// TestCountFastPathMatchesSlowPath COUNT(*) / COUNT(1) 快速路径结果与既有
// 聚合路径一致：空表 0、3 行 3、COUNT(1) 同值、COUNT('x') 同值、列名 COUNT(*)。
func TestCountFastPathMatchesSlowPath(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// 空表 COUNT(*)=0
	e2 := newEngine(t)
	mustExec(t, e2, "CREATE TABLE empty_t (id INT)")
	rows := mustRows(t, e2, "SELECT COUNT(*) FROM empty_t")
	if len(rows) != 1 || rows[0][0].Kind != "INT" || rows[0][0].I != 0 {
		t.Fatalf("COUNT empty = %+v, want [[INT 0]]", rows)
	}

	// 列名与既有路径一致
	res := mustExec(t, e, "SELECT COUNT(*) FROM users")
	if len(res.Columns) != 1 || res.Columns[0] != "COUNT(*)" {
		t.Fatalf("columns = %v, want [COUNT(*)]", res.Columns)
	}
	if len(res.ColTypes) != 1 || res.ColTypes[0] != "INT" {
		t.Fatalf("coltypes = %v, want [INT]", res.ColTypes)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].I != 3 {
		t.Fatalf("COUNT(*) = %+v, want [[3]]", res.Rows)
	}

	// COUNT(1) 与 COUNT(*) 同值
	r1 := mustRows(t, e, "SELECT COUNT(1) FROM users")
	if len(r1) != 1 || r1[0][0].I != 3 {
		t.Fatalf("COUNT(1) = %+v, want [[3]]", r1)
	}
	// COUNT('x') 字面量同值（快速路径也适用字面量参数）
	r2 := mustRows(t, e, "SELECT COUNT('x') FROM users")
	if len(r2) != 1 || r2[0][0].I != 3 {
		t.Fatalf("COUNT('x') = %+v, want [[3]]", r2)
	}
	// LIMIT 不改变聚合值（返回行数仍为 1）
	r3 := mustRows(t, e, "SELECT COUNT(*) FROM users LIMIT 1")
	if len(r3) != 1 || r3[0][0].I != 3 {
		t.Fatalf("COUNT(*) LIMIT 1 = %+v, want [[3]]", r3)
	}
}

// TestCountFastPathFallbacks 条件不满足一律回退原路径，结果仍正确：
// WHERE / COUNT(列名) / COUNT(DISTINCT) / COUNT(NULL) / GROUP BY / ORDER BY /
// JOIN / DISTINCT。
func TestCountFastPathFallbacks(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// WHERE 回退
	r := mustRows(t, e, "SELECT COUNT(*) FROM users WHERE age > 25")
	if len(r) != 1 || r[0][0].I != 2 {
		t.Fatalf("COUNT WHERE = %+v, want [[2]]", r)
	}
	// COUNT(列名) 回退（NULL 不计数；本表无 NULL，= 3）
	r = mustRows(t, e, "SELECT COUNT(name) FROM users")
	if len(r) != 1 || r[0][0].I != 3 {
		t.Fatalf("COUNT(col) = %+v, want [[3]]", r)
	}
	// COUNT(DISTINCT col) 回退
	r = mustRows(t, e, "SELECT COUNT(DISTINCT age) FROM users")
	if len(r) != 1 || r[0][0].I != 3 {
		t.Fatalf("COUNT(DISTINCT) = %+v, want [[3]]", r)
	}
	// COUNT(NULL) 恒 0（快速路径禁用，回退逐行语义）
	r = mustRows(t, e, "SELECT COUNT(NULL) FROM users")
	if len(r) != 1 || r[0][0].I != 0 {
		t.Fatalf("COUNT(NULL) = %+v, want [[0]]", r)
	}
	// GROUP BY 回退：分组行数 = 组数
	r = mustRows(t, e, "SELECT COUNT(*) FROM users GROUP BY age")
	if len(r) != 3 {
		t.Fatalf("COUNT GROUP BY rows = %d, want 3", len(r))
	}
	// ORDER BY 回退（parser 仅支持列引用排序）
	r = mustRows(t, e, "SELECT COUNT(*) FROM users ORDER BY age")
	if len(r) != 1 || r[0][0].I != 3 {
		t.Fatalf("COUNT ORDER BY = %+v, want [[3]]", r)
	}
	// JOIN 回退
	mustExec(t, e, "CREATE TABLE dept (id INT, name TEXT)")
	mustExec(t, e, "INSERT INTO dept VALUES (1, 'eng'), (2, 'ops')")
	r = mustRows(t, e, "SELECT COUNT(*) FROM users u JOIN dept d ON u.id = d.id")
	if len(r) != 1 || r[0][0].I != 2 {
		t.Fatalf("COUNT JOIN = %+v, want [[2]]", r)
	}
	// DISTINCT 回退
	r = mustRows(t, e, "SELECT DISTINCT COUNT(*) FROM users")
	if len(r) != 1 || r[0][0].I != 3 {
		t.Fatalf("COUNT DISTINCT = %+v, want [[3]]", r)
	}
}

// TestCountFastPathTransactionWrites 显式事务内未提交写时 COUNT 反映
// read-your-writes（回退 Scan 计数），COMMIT 后结果一致。
func TestCountFastPathTransactionWrites(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	// 事务内未提交写可见：回退路径计数 = 4
	r := mustRows(t, e, "SELECT COUNT(*) FROM users")
	if len(r) != 1 || r[0][0].I != 4 {
		t.Fatalf("tx COUNT = %+v, want [[4]]", r)
	}
	mustExec(t, e, "COMMIT")
	// 提交后（新事务）结果一致
	r = mustRows(t, e, "SELECT COUNT(*) FROM users")
	if len(r) != 1 || r[0][0].I != 4 {
		t.Fatalf("committed COUNT = %+v, want [[4]]", r)
	}

	// 回滚后计数恢复
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (5, 'eve', 45)")
	mustExec(t, e, "ROLLBACK")
	r = mustRows(t, e, "SELECT COUNT(*) FROM users")
	if len(r) != 1 || r[0][0].I != 4 {
		t.Fatalf("after rollback COUNT = %+v, want [[4]]", r)
	}
}

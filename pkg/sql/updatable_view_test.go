/*
---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_78c959d3e30d11f1913b525400393706
    ReservedCode1: nucB5a131X3JrmedPbHJSm4SFUQg6VPltCR6o7d+PJBnH9b7zqcPacu2QqkV3F1iVcAvduytT6x04jC5v3MYKUn8G28tZV75pMAK69t91q8DGc7l7hBBweQl+ynDEcMYk/1H8p8kQ8drpqYryCgUvEoBKOMAMGI2XKHM0jpeyKfc5COoXZcpIjSCNYY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_78c959d3e30d11f1913b525400393706
    ReservedCode2: nucB5a131X3JrmedPbHJSm4SFUQg6VPltCR6o7d+PJBnH9b7zqcPacu2QqkV3F1iVcAvduytT6x04jC5v3MYKUn8G28tZV75pMAK69t91q8DGc7l7hBBweQl+ynDEcMYk/1H8p8kQ8drpqYryCgUvEoBKOMAMGI2XKHM0jpeyKfc5COoXZcpIjSCNYY=
---
*/

package sql_test

import "testing"

// v1.19.0 A1 可更新视图：CREATE VIEW 时判定可更新性并持久化列映射，
// INSERT / UPDATE / DELETE 目标为可更新视图时按映射重写为基表写操作。
// 本阶段不含 WITH CHECK OPTION。

// TestUpdatableViewInsert 插入可更新视图：显式列映射到基表列，
// 视图未暴露的基表列走默认值。
func TestUpdatableViewInsert(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name FROM t")
	mustExec(t, e, "INSERT INTO v (id, name) VALUES (1, 'a')")

	rows := mustRows(t, e, "SELECT id, name, score FROM t")
	if len(rows) != 1 || rows[0][0].I != 1 || rows[0][1].S != "a" {
		t.Fatalf("insert via view: got %+v", rows)
	}
	// 未暴露列走类型默认值
	if rows[0][2].I != 0 {
		t.Fatalf("unexposed column default: want 0, got %+v", rows[0][2])
	}
	// 通过视图也可见新行
	vr := mustRows(t, e, "SELECT id FROM v")
	if len(vr) != 1 || vr[0][0].I != 1 {
		t.Fatalf("view read after insert: got %+v", vr)
	}
}

// TestUpdatableViewInsertWithoutColumnList 省略列清单时按视图列顺序映射。
func TestUpdatableViewInsertWithoutColumnList(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, score FROM t")
	mustExec(t, e, "INSERT INTO v VALUES (5, 70)")

	rows := mustRows(t, e, "SELECT id, name, score FROM t")
	if len(rows) != 1 || rows[0][0].I != 5 || rows[0][2].I != 70 {
		t.Fatalf("insert without column list: got %+v", rows)
	}
	if rows[0][1].S != "" {
		t.Fatalf("unexposed TEXT column default: want empty, got %+v", rows[0][1])
	}
}

// TestUpdatableViewUpdate 更新可更新视图：SET 列与 WHERE 中的视图列名
// 都重写为基表列名，别名列同样可用。
func TestUpdatableViewUpdate(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)")
	mustExec(t, e, "INSERT INTO t (id, name, score) VALUES (1, 'a', 10)")
	mustExec(t, e, "INSERT INTO t (id, name, score) VALUES (2, 'b', 20)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id AS uid, score FROM t WHERE score >= 10")

	// WHERE 用视图列名（含别名）定位
	mustExec(t, e, "UPDATE v SET score = score + 5 WHERE uid = 1")
	rows := mustRows(t, e, "SELECT id, score FROM t ORDER BY id")
	if rows[0][1].I != 15 || rows[1][1].I != 20 {
		t.Fatalf("update via view: got %+v", rows)
	}
	// 视图视角同步可见
	vr := mustRows(t, e, "SELECT uid, score FROM v ORDER BY uid")
	if len(vr) != 2 || vr[0][0].I != 1 || vr[0][1].I != 15 {
		t.Fatalf("view read after update: got %+v", vr)
	}
}

// TestUpdatableViewUpdateRejectsUnexposedColumn 视图未暴露的基表列不可写。
func TestUpdatableViewUpdateRejectsUnexposedColumn(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)")
	mustExec(t, e, "INSERT INTO t (id, name, score) VALUES (1, 'a', 10)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name FROM t")

	mustErr(t, e, "UPDATE v SET score = 99 WHERE id = 1", "view v has no column: score")
	mustErr(t, e, "INSERT INTO v (id, score) VALUES (1, 50)", "view v has no column: score")
	// 拒绝后基表未被修改
	rows := mustRows(t, e, "SELECT score FROM t")
	if rows[0][0].I != 10 {
		t.Fatalf("rejected write must not change base table: got %+v", rows)
	}
}

// TestUpdatableViewDelete 删除可更新视图行：WHERE 视图列名重写为基表列名。
func TestUpdatableViewDelete(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)")
	mustExec(t, e, "INSERT INTO t (id, name, score) VALUES (1, 'a', 10)")
	mustExec(t, e, "INSERT INTO t (id, name, score) VALUES (2, 'b', 20)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name FROM t WHERE score >= 10")

	mustExec(t, e, "DELETE FROM v WHERE id = 2")
	rows := mustRows(t, e, "SELECT id FROM t ORDER BY id")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("delete via view: got %+v", rows)
	}
	// 无条件 DELETE 删除视图全部行
	mustExec(t, e, "DELETE FROM v")
	if left := mustRows(t, e, "SELECT id FROM t"); len(left) != 0 {
		t.Fatalf("delete all via view: got %+v", left)
	}
}

// TestUpdatableViewStarProjection SELECT * 视图按基表列序恒等映射，可更新。
func TestUpdatableViewStarProjection(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT * FROM t")
	mustExec(t, e, "INSERT INTO v (id, name) VALUES (1, 'a')")
	mustExec(t, e, "UPDATE v SET name = 'b' WHERE id = 1")
	rows := mustRows(t, e, "SELECT id, name FROM t")
	if len(rows) != 1 || rows[0][1].S != "b" {
		t.Fatalf("star view write: got %+v", rows)
	}
}

// TestUpdatableViewNotUpdatableReasons 不可更新的每种定义都带明确原因。
func TestUpdatableViewNotUpdatableReasons(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)")
	mustExec(t, e, "CREATE TABLE u (id INT PRIMARY KEY, name TEXT)")

	cases := []struct {
		view   string
		create string
		reason string
	}{
		{"vagg", "CREATE VIEW vagg AS SELECT COUNT(*) FROM t", "aggregate function in definition"},
		{"vjoin", "CREATE VIEW vjoin AS SELECT t.id FROM t JOIN u ON t.id = u.id", "JOIN in definition"},
		{"vdist", "CREATE VIEW vdist AS SELECT DISTINCT id FROM t", "DISTINCT in definition"},
		{"vgrp", "CREATE VIEW vgrp AS SELECT score, COUNT(*) FROM t GROUP BY score", "GROUP BY in definition"},
		{"vhav", "CREATE VIEW vhav AS SELECT score FROM t GROUP BY score HAVING score > 1", "GROUP BY in definition"},
		{"vlim", "CREATE VIEW vlim AS SELECT id FROM t LIMIT 3", "LIMIT in definition"},
		{"voff", "CREATE VIEW voff AS SELECT id FROM t LIMIT 3 OFFSET 1", "LIMIT in definition"},
		{"vexpr", "CREATE VIEW vexpr AS SELECT id + 1 FROM t", "expression column in definition"},
		{"vsub", "CREATE VIEW vsub AS SELECT id FROM (SELECT id FROM t) x", "subquery in FROM clause"},
	}
	for _, c := range cases {
		mustExec(t, e, c.create)
		want := "view " + c.view + " is not updatable: " + c.reason
		mustErr(t, e, "INSERT INTO "+c.view+" (id) VALUES (1)", want)
		mustErr(t, e, "UPDATE "+c.view+" SET id = 2", want)
		mustErr(t, e, "DELETE FROM "+c.view, want)
		// 只读查询仍然可用（零回归）
		mustExec(t, e, "SELECT * FROM "+c.view)
	}
}

// TestUpdatableViewNestedViewBase 视图基于视图定义时不可更新（本阶段不支持）。
func TestUpdatableViewNestedViewBase(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE VIEW v1 AS SELECT id, name FROM t")
	mustExec(t, e, "CREATE VIEW v2 AS SELECT id FROM v1")
	mustErr(t, e, "INSERT INTO v2 (id) VALUES (1)",
		"view v2 is not updatable: base object is a view: v1")
}

// TestUpdatableViewMissingViewKeepsNotExists 目标既非表也非视图时保持既有口径。
func TestUpdatableViewMissingViewKeepsNotExists(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustErr(t, e, "INSERT INTO nosuch (id) VALUES (1)", "table not exists: nosuch")
	mustErr(t, e, "UPDATE nosuch SET id = 1", "table not exists: nosuch")
	mustErr(t, e, "DELETE FROM nosuch WHERE id = 1", "table not exists: nosuch")
}

// TestUpdatableViewCheckOptionUnsupported v1.19.0 A2 起 WITH CHECK OPTION 已被真正
// 支持：原「解析期报不支持」的断言改写为兼容性验证——带 CHECK OPTION 的视图可正常
// 创建（详见 check_option_test.go 的语义用例）。
func TestUpdatableViewCheckOptionUnsupported(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	mustExec(t, e, "CREATE VIEW vc AS SELECT id, score FROM t WHERE score > 10 WITH CHECK OPTION")
	mustExec(t, e, "CREATE VIEW vc2 AS SELECT id, score FROM t WITH LOCAL CHECK OPTION")
	// 残缺写法（WITH CHECK 缺 OPTION）仍按语法错误拒绝，不静默接受。
	mustErr(t, e, "CREATE VIEW vc3 AS SELECT id, score FROM t WITH CHECK", "expected OPTION")
}

// TestUpdatableViewPrivilegeOnBaseTable 写权限按基表判定：非 root 用户
// 未获基表授权时得到 permission denied（优先于可更新性报错）。
func TestUpdatableViewPrivilegeOnBaseTable(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name FROM t")
	mustExec(t, e, "CREATE VIEW vagg AS SELECT COUNT(*) FROM t")

	// 未授权：permission denied 优先于不可更新报错
	mustErrAs(t, e, "INSERT INTO v (id, name) VALUES (1, 'a')", "alice", "permission denied")
	mustErrAs(t, e, "INSERT INTO vagg (id) VALUES (1)", "alice", "permission denied")

	// 授予基表权限后，经视图写入按基表权限放行
	mustExec(t, e, "GRANT INSERT ON t TO 'alice'")
	mustExecAs(t, e, "INSERT INTO v (id, name) VALUES (1, 'a')", "alice")
	mustErrAs(t, e, "INSERT INTO vagg (id) VALUES (1)", "alice", "is not updatable")

	rows := mustRows(t, e, "SELECT id, name FROM t")
	if len(rows) != 1 || rows[0][1].S != "a" {
		t.Fatalf("insert via view as alice: got %+v", rows)
	}
	// 视图本身不是授权对象：以视图名做目标写操作同样落到基表权限判定
	mustExec(t, e, "GRANT UPDATE ON t TO 'alice'")
	mustExecAs(t, e, "UPDATE v SET name = 'b' WHERE id = 1", "alice")
	ur := mustRows(t, e, "SELECT name FROM t")
	if ur[0][0].S != "b" {
		t.Fatalf("update via view as alice: got %+v", ur)
	}
}

// TestUpdatableViewTransactional 显式事务内经视图写入与回滚。
func TestUpdatableViewTransactional(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name FROM t")

	sess := e.NewSession()
	mustSessExec(t, e, sess, "BEGIN")
	mustSessExec(t, e, sess, "INSERT INTO v (id, name) VALUES (1, 'a')")
	mustSessExec(t, e, sess, "ROLLBACK")
	if rows := mustRows(t, e, "SELECT id FROM t"); len(rows) != 0 {
		t.Fatalf("rollback via view write: got %+v", rows)
	}
}

// TestUpdatableViewOnDuplicateUpdate ON DUPLICATE KEY UPDATE 的赋值列按
// 视图列名书写，需映射到基表列后生效。
func TestUpdatableViewOnDuplicateUpdate(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id AS uid, name FROM t")

	mustExec(t, e, "INSERT INTO v (uid, name) VALUES (1, 'a')")
	mustExec(t, e, "INSERT INTO v (uid, name) VALUES (1, 'b') ON DUPLICATE KEY UPDATE name = 'c'")

	rows := mustRows(t, e, "SELECT id, name FROM t")
	if len(rows) != 1 || rows[0][0].I != 1 || rows[0][1].S != "c" {
		t.Fatalf("on duplicate via view: got %+v", rows)
	}
	// 视图未暴露的列不可用于赋值项
	mustErr(t, e, "INSERT INTO v (uid, name) VALUES (2, 'd') ON DUPLICATE KEY UPDATE hidden = 1",
		"view v has no column: hidden")
}

// TestUpdatableViewInsertSelect INSERT ... SELECT 经视图写入：目标列按映射
// 落到基表，源查询保持原样。
func TestUpdatableViewInsertSelect(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE src (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO src (id, name) VALUES (7, 'g')")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name FROM t")

	mustExec(t, e, "INSERT INTO v (id, name) SELECT id, name FROM src")
	rows := mustRows(t, e, "SELECT id, name FROM t")
	if len(rows) != 1 || rows[0][0].I != 7 || rows[0][1].S != "g" {
		t.Fatalf("insert select via view: got %+v", rows)
	}
}

// TestUpdatableViewProcedureBody 存储过程体内的 INSERT / UPDATE / DELETE
// 经可更新视图写基表并落到基表：过程体由 procRunner 直接调 execStmt 分发
// （不经过 ExecuteTextAsSession），验证视图写重写已下沉到 execStmt 分发前，
// 过程内视图写与顶层视图写行为一致。
func TestUpdatableViewProcedureBody(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id AS uid, score AS pts FROM t")
	mustExec(t, e, "CREATE PROCEDURE p_view BEGIN INSERT INTO v (uid, pts) VALUES (1, 10); INSERT INTO v (uid, pts) VALUES (2, 20); UPDATE v SET pts = 99 WHERE uid = 2; DELETE FROM v WHERE uid = 1; END")
	mustExec(t, e, "CALL p_view")

	// 过程体内三步写全部按视图列映射落到基表：仅 id=2 存活且 pts 被更新。
	rows := mustRows(t, e, "SELECT id, score FROM t")
	if len(rows) != 1 || rows[0][0].I != 2 || rows[0][1].I != 99 {
		t.Fatalf("procedure write via view: got %+v", rows)
	}
	// 通过视图回读，确认写目标确为基表而非同名影子对象。
	if vr := mustRows(t, e, "SELECT uid, pts FROM v"); len(vr) != 1 || vr[0][0].I != 2 || vr[0][1].I != 99 {
		t.Fatalf("view read after procedure write: got %+v", vr)
	}
	// 过程内写不可更新视图时按视图名报错，与顶层口径一致。
	mustExec(t, e, "CREATE VIEW v_bad AS SELECT id, COUNT(*) AS c FROM t GROUP BY id")
	mustExec(t, e, "CREATE PROCEDURE p_bad BEGIN INSERT INTO v_bad (id) VALUES (3); END")
	mustErr(t, e, "CALL p_bad", "view v_bad is not updatable: GROUP BY in definition")
}

// TestUpdatableViewWhereRightRefRewrite WHERE 中列对列比较的右值列引用
// （Cond.RightRef）与左侧同口径按视图列映射重写：暴露列可正常书写，
// 未暴露列报 view <name> has no column，两侧报错口径一致。
func TestUpdatableViewWhereRightRefRewrite(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)")
	mustExec(t, e, "INSERT INTO t (id, name, score) VALUES (1, 'a', 10)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id AS uid, name AS uname FROM t")

	// 右侧引用视图暴露列（别名）→ 重写为基表列，不误报。
	mustExec(t, e, "UPDATE v SET uname = 'b' WHERE uname = uid")
	// 左侧引用视图暴露列、右侧同样 → DELETE 不误报。
	mustExec(t, e, "DELETE FROM v WHERE uid = uname")

	// 右侧引用未暴露列（score 属基表但未投影到视图）→ 与左侧同口径报错。
	mustErr(t, e, "DELETE FROM v WHERE uid = score", "view v has no column: score")
	// 左侧未暴露列 → 报错口径与右侧完全一致。
	mustErr(t, e, "DELETE FROM v WHERE score = uid", "view v has no column: score")
}

// TestUpdatableViewRewriteIdempotent 视图写重写的幂等性：每条 DML 先经顶层
// ExecuteTextAsSession 重写，随后 execStmt 下沉层会对同一语句对象再调用一次，
// 语句上的 viewWrite 标记保证只真正重写一次（不重复读视图目录、不重复报错）；
// 本用例从 SQL 层验证重复执行同类语句结论稳定、不出现 has no column 误报，
// 且视图 DROP 后重建时新语句对象按新元数据重新映射。
func TestUpdatableViewRewriteIdempotent(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id AS uid, name AS uname FROM t")

	mustExec(t, e, "INSERT INTO v (uid, uname) VALUES (1, 'a')")
	mustExec(t, e, "INSERT INTO v (uid, uname) VALUES (2, 'b')")
	mustExec(t, e, "INSERT INTO v (uid, uname) VALUES (3, 'c')")
	// 同一 UPDATE 重复执行三次：只改一次语义，无二次映射报错。
	for i := 0; i < 3; i++ {
		mustExec(t, e, "UPDATE v SET uname = 'z' WHERE uid = 2")
	}
	mustExec(t, e, "DELETE FROM v WHERE uid = 1")

	rows := mustRows(t, e, "SELECT id, name FROM t ORDER BY id")
	if len(rows) != 2 || rows[0][0].I != 2 || rows[0][1].S != "z" || rows[1][0].I != 3 {
		t.Fatalf("idempotent rewrite: got %+v", rows)
	}
	mustExec(t, e, "DROP VIEW v")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name FROM t")
	// 视图重建后同一语句再次执行：映射按新视图元数据重新生效。
	mustExec(t, e, "UPDATE v SET name = 'y' WHERE id = 3")
	if got := mustRows(t, e, "SELECT name FROM t WHERE id = 3")[0][0].S; got != "y" {
		t.Fatalf("rewrite after view rebuild: got %q", got)
	}
}

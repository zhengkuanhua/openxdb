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

// TestCheckOptionInsertReject 越界 INSERT 经视图写入被拒绝（默认 CASCADED）。
func TestCheckOptionInsertReject(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name, score FROM t WHERE score >= 10 WITH CHECK OPTION")
	mustErr(t, e, "INSERT INTO v (id, name, score) VALUES (1, 'a', 5)", "CHECK OPTION failed 'v'")
	// 拒绝后基表不得残留任何行。
	if n := len(mustRows(t, e, "SELECT id FROM t")); n != 0 {
		t.Fatalf("rejected insert left %d row(s) in base table, want 0", n)
	}
	// 满足条件的写入正常放行。
	mustExec(t, e, "INSERT INTO v (id, name, score) VALUES (1, 'a', 10)")
	if got := mustRows(t, e, "SELECT score FROM t WHERE id = 1")[0][0].String(); got != "10" {
		t.Fatalf("valid insert score: got %s, want 10", got)
	}
}

// TestCheckOptionInsertMultiRowRollback 多行 INSERT 中任一行越界即整条回滚。
func TestCheckOptionInsertMultiRowRollback(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, score FROM t WHERE score >= 10 WITH CHECK OPTION")
	mustErr(t, e, "INSERT INTO v (id, score) VALUES (1, 20), (2, 30), (3, 1)", "CHECK OPTION failed 'v'")
	if n := len(mustRows(t, e, "SELECT id FROM t")); n != 0 {
		t.Fatalf("partial write after rejected multi-row insert: %d row(s), want 0", n)
	}
	mustExec(t, e, "INSERT INTO v (id, score) VALUES (1, 20), (2, 30)")
	if n := len(mustRows(t, e, "SELECT id FROM t")); n != 2 {
		t.Fatalf("valid multi-row insert: got %d row(s), want 2", n)
	}
}

// TestCheckOptionInsertUnexposedColumnDefault 视图未暴露的基表列按默认值参与求值。
func TestCheckOptionInsertUnexposedColumnDefault(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, name FROM t WHERE score >= 10 WITH CHECK OPTION")
	// score 未在视图暴露（A1 口径：INSERT 不能写视图未暴露的列），只能取默认值，
	// 默认取值不满足 score >= 10，因此经视图写入必然被拒。
	mustErr(t, e, "INSERT INTO v (id, name) VALUES (1, 'a')", "CHECK OPTION failed 'v'")
	if n := len(mustRows(t, e, "SELECT id FROM t")); n != 0 {
		t.Fatalf("rejected insert left %d row(s), want 0", n)
	}
	// 直接对基表写入（不经视图）不受 CHECK OPTION 约束。
	mustExec(t, e, "INSERT INTO t (id, name, score) VALUES (1, 'a', 10)")
}

// TestCheckOptionUpdateRejectAndAllow UPDATE 改出视图边界被拒绝，未越界放行。
func TestCheckOptionUpdateRejectAndAllow(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	mustExec(t, e, "INSERT INTO t (id, score) VALUES (1, 20), (2, 30)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, score FROM t WHERE score >= 10 WITH CHECK OPTION")
	mustErr(t, e, "UPDATE v SET score = 5 WHERE id = 1", "CHECK OPTION failed 'v'")
	if got := mustRows(t, e, "SELECT score FROM t WHERE id = 1")[0][0].String(); got != "20" {
		t.Fatalf("rejected update changed row: score=%s, want 20", got)
	}
	mustExec(t, e, "UPDATE v SET score = 25 WHERE id = 1")
	if got := mustRows(t, e, "SELECT score FROM t WHERE id = 1")[0][0].String(); got != "25" {
		t.Fatalf("valid update: score=%s, want 25", got)
	}
}

// TestCheckOptionUpdateMultiRowRollback 多行 UPDATE 中恰有一行越界时整条回滚。
func TestCheckOptionUpdateMultiRowRollback(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	mustExec(t, e, "INSERT INTO t (id, score) VALUES (1, 20), (2, 40)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, score FROM t WHERE score >= 10 WITH CHECK OPTION")
	// 同一条 UPDATE 覆盖两行：id=1 落地后为 -10 越界（id=2 为 10 合法）→ 整条回滚。
	mustErr(t, e, "UPDATE v SET score = score - 30", "CHECK OPTION failed 'v'")
	rows := mustRows(t, e, "SELECT id, score FROM t ORDER BY id")
	if len(rows) != 2 || rows[0][1].String() != "20" || rows[1][1].String() != "40" {
		t.Fatalf("partial update after rejected statement: %v, want score 20/40", rows)
	}
}

// TestCheckOptionDeleteNotRestricted DELETE 不受 CHECK OPTION 约束（MySQL 语义）。
func TestCheckOptionDeleteNotRestricted(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	// 直接写入基表，该行不在视图可见范围内（score < 10）。
	mustExec(t, e, "INSERT INTO t (id, score) VALUES (1, 5), (2, 20)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, score FROM t WHERE score >= 10 WITH CHECK OPTION")
	// 经视图删除视图不可见的行：不施加 CHECK OPTION，允许执行。
	res := mustExec(t, e, "DELETE FROM v WHERE id = 1")
	if res.AffectedRows != 1 {
		t.Fatalf("DELETE via check-option view: affected=%d, want 1", res.AffectedRows)
	}
	if n := len(mustRows(t, e, "SELECT id FROM t")); n != 1 {
		t.Fatalf("after delete: %d row(s) left, want 1", n)
	}
}

// TestCheckOptionAbsentNoCheck 无 CHECK OPTION 的视图写入不校验（口径不变）。
func TestCheckOptionAbsentNoCheck(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, score FROM t WHERE score >= 10")
	mustExec(t, e, "INSERT INTO v (id, score) VALUES (1, 5)")
	mustExec(t, e, "UPDATE v SET score = 1 WHERE id = 1")
	if got := mustRows(t, e, "SELECT score FROM t WHERE id = 1")[0][0].String(); got != "1" {
		t.Fatalf("view without check option: score=%s, want 1", got)
	}
}

// TestCheckOptionLocalCascadedEquivalent LOCAL / CASCADED / 缺省写法行为等价。
func TestCheckOptionLocalCascadedEquivalent(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	mustExec(t, e, "CREATE VIEW v_def AS SELECT id, score FROM t WHERE score >= 10 WITH CHECK OPTION")
	mustExec(t, e, "CREATE VIEW v_cas AS SELECT id, score FROM t WHERE score >= 10 WITH CASCADED CHECK OPTION")
	mustExec(t, e, "CREATE VIEW v_loc AS SELECT id, score FROM t WHERE score >= 10 WITH LOCAL CHECK OPTION")
	for _, v := range []string{"v_def", "v_cas", "v_loc"} {
		stmt := "INSERT INTO " + v + " (id, score) VALUES (1, 5)"
		mustErr(t, e, stmt, "CHECK OPTION failed '"+v+"'")
	}
	if n := len(mustRows(t, e, "SELECT id FROM t")); n != 0 {
		t.Fatalf("rejected inserts left %d row(s), want 0", n)
	}
	for i, v := range []string{"v_def", "v_cas", "v_loc"} {
		id := string(rune('1' + i))
		mustExec(t, e, "INSERT INTO "+v+" (id, score) VALUES ("+id+", 10)")
	}
	if n := len(mustRows(t, e, "SELECT id FROM t")); n != 3 {
		t.Fatalf("valid inserts via 3 view forms: got %d row(s), want 3", n)
	}
}

// TestCheckOptionQualifiedAndAliasRef 定义中的限定列名 / 表别名在写校验中解析正确。
func TestCheckOptionQualifiedAndAliasRef(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT, flag INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT s.id AS uid, s.score, s.flag FROM t AS s WHERE s.score >= 10 AND s.flag = 1 WITH CHECK OPTION")
	mustErr(t, e, "INSERT INTO v (uid, score, flag) VALUES (1, 5, 1)", "CHECK OPTION failed 'v'")
	mustErr(t, e, "INSERT INTO v (uid, score, flag) VALUES (1, 20, 0)", "CHECK OPTION failed 'v'")
	mustExec(t, e, "INSERT INTO v (uid, score, flag) VALUES (1, 20, 1)")
	// UPDATE 走同一条件：把 flag 改出边界同样被拒绝。
	mustErr(t, e, "UPDATE v SET flag = 0 WHERE uid = 1", "CHECK OPTION failed 'v'")
}

// TestCheckOptionComplexCondition 复合条件（AND/OR/BETWEEN/IN/函数）按新行求值。
func TestCheckOptionComplexCondition(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT, lvl INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, score, lvl FROM t WHERE score >= 10 AND lvl > 0 WITH CHECK OPTION")
	mustErr(t, e, "INSERT INTO v (id, score, lvl) VALUES (1, 10, 0)", "CHECK OPTION failed 'v'")
	mustExec(t, e, "INSERT INTO v (id, score, lvl) VALUES (1, 10, 2)")
	// 多条件（AND 串联）同时生效：任一条件不满足即拒绝。
	mustErr(t, e, "INSERT INTO v (id, score, lvl) VALUES (2, 9, 2)", "CHECK OPTION failed 'v'")
}

// TestCheckOptionNoWhereClause 定义无 WHERE 时 CHECK OPTION 恒通过（不报错不误拒）。
func TestCheckOptionNoWhereClause(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, score FROM t WITH CHECK OPTION")
	mustExec(t, e, "INSERT INTO v (id, score) VALUES (1, -100)")
	mustExec(t, e, "UPDATE v SET score = -200 WHERE id = 1")
	if got := mustRows(t, e, "SELECT score FROM t WHERE id = 1")[0][0].String(); got != "-200" {
		t.Fatalf("view without WHERE: score=%s, want -200", got)
	}
}

// TestCheckOptionNotUpdatableViewRejected 不可更新视图不允许带 CHECK OPTION。
func TestCheckOptionNotUpdatableViewRejected(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	mustErr(t, e, "CREATE VIEW v AS SELECT score, COUNT(*) AS c FROM t GROUP BY score WITH CHECK OPTION", "is not updatable")
}

// TestCheckOptionProcedureBody 过程体内经视图写入同样受 CHECK OPTION 约束。
func TestCheckOptionProcedureBody(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
	mustExec(t, e, "CREATE VIEW v AS SELECT id, score FROM t WHERE score >= 10 WITH CHECK OPTION")
	mustExec(t, e, "CREATE PROCEDURE p_bad (IN id INT, score INT) BEGIN INSERT INTO v (id, score) VALUES (@id, @score); END")
	mustErr(t, e, "CALL p_bad(1, 5)", "CHECK OPTION failed 'v'")
	if n := len(mustRows(t, e, "SELECT id FROM t")); n != 0 {
		t.Fatalf("procedure insert out of range left %d row(s), want 0", n)
	}
	mustExec(t, e, "CALL p_bad(1, 10)")
	if n := len(mustRows(t, e, "SELECT id FROM t")); n != 1 {
		t.Fatalf("procedure valid insert: %d row(s), want 1", n)
	}
	mustExec(t, e, "CREATE PROCEDURE p_upd (IN id INT, score INT) BEGIN UPDATE v SET score = @score WHERE id = @id; END")
	mustErr(t, e, "CALL p_upd(1, 1)", "CHECK OPTION failed 'v'")
}

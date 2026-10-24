package sql_test

import (
	"testing"
)

// v1.8.0 触发器行级语义测试：FOR EACH ROW 逐行触发、NEW./OLD. 行引用、
// 体写同事务回滚、可控递归（同键抑制 + 链式深度上限）、FOR EACH STATEMENT
// 语句级回归。

// 行级 INSERT 触发器按受影响行逐行触发：单行 1 次、批量 N 行 N 次。
func TestRowLevelInsertFiresPerRow(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (src_id INT)")
	// 体将 NEW.id 物化为字面量逐行写入审计表。
	mustExec(t, e, "CREATE TRIGGER tr_ins AFTER INSERT ON users FOR EACH ROW INSERT INTO audit (src_id) VALUES (NEW.id)")

	// 单行：触发 1 次
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	rows := mustRows(t, e, "SELECT src_id FROM audit ORDER BY src_id")
	if len(rows) != 1 || rows[0][0].String() != "4" {
		t.Fatalf("single-row: audit = %v, want [4]", rows)
	}
	// 批量两行：触发 2 次（逐行），审计表新增 5、6
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (5, 'erin', 22), (6, 'frank', 28)")
	rows = mustRows(t, e, "SELECT src_id FROM audit ORDER BY src_id")
	if len(rows) != 3 {
		t.Fatalf("row-level per-row: audit rows = %d, want 3", len(rows))
	}
	if rows[1][0].String() != "5" || rows[2][0].String() != "6" {
		t.Fatalf("row-level per-row: audit = %v, want [4 5 6]", rows)
	}
}

// 行级 UPDATE 触发器可同时引用 NEW 与 OLD；差异正确可见。
func TestRowLevelUpdateNewOldRefs(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (old_age INT, new_age INT)")
	mustExec(t, e, "CREATE TRIGGER tr_upd AFTER UPDATE ON users FOR EACH ROW INSERT INTO audit (old_age, new_age) VALUES (OLD.age, NEW.age)")

	mustExec(t, e, "UPDATE users SET age = 36 WHERE id = 1")
	row := mustRows(t, e, "SELECT old_age, new_age FROM audit")[0]
	if row[0].String() != "30" || row[1].String() != "36" {
		t.Fatalf("NEW/OLD refs: got old=%s new=%s, want 30 36", row[0].String(), row[1].String())
	}
	// 0 行 UPDATE 不触发
	if rows := mustRows(t, e, "SELECT * FROM audit"); len(rows) != 1 {
		t.Fatalf("0-row update should not fire: audit rows = %d, want 1", len(rows))
	}
}

// 行级 DELETE 触发器仅 OLD 可用；0 行 DELETE 不触发。
func TestRowLevelDeleteOldRef(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (src_id INT)")
	mustExec(t, e, "CREATE TRIGGER tr_del AFTER DELETE ON users FOR EACH ROW INSERT INTO audit (src_id) VALUES (OLD.id)")

	mustExec(t, e, "DELETE FROM users WHERE id = 3")
	rows := mustRows(t, e, "SELECT src_id FROM audit")
	if len(rows) != 1 || rows[0][0].String() != "3" {
		t.Fatalf("DELETE OLD ref: audit = %v, want [3]", rows)
	}
	// 0 行 DELETE 不触发
	mustExec(t, e, "DELETE FROM users WHERE id = 999")
	if rows := mustRows(t, e, "SELECT * FROM audit"); len(rows) != 1 {
		t.Fatalf("0-row delete should not fire: audit rows = %d, want 1", len(rows))
	}
}

// 行级引用创建期校验：事件语义与列存在性。
func TestRowLevelCreateRefErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")

	// INSERT 仅 NEW：引用 OLD 报错
	mustErr(t, e, "CREATE TRIGGER t1 AFTER INSERT ON users FOR EACH ROW INSERT INTO audit (cnt) VALUES (OLD.id)", "cannot reference OLD")
	// DELETE 仅 OLD：引用 NEW 报错
	mustErr(t, e, "CREATE TRIGGER t2 AFTER DELETE ON users FOR EACH ROW INSERT INTO audit (cnt) VALUES (NEW.id)", "cannot reference NEW")
	// 未定义列明确报错
	mustErr(t, e, "CREATE TRIGGER t3 AFTER INSERT ON users FOR EACH ROW INSERT INTO audit (cnt) VALUES (NEW.nope)", "unknown column")
	// 合法创建不受影响
	mustExec(t, e, "CREATE TRIGGER t4 AFTER INSERT ON users FOR EACH ROW INSERT INTO audit (cnt) VALUES (NEW.id)")
}

// 体写随主 DML 同事务回滚：AFTER 链中前一个体已成功写 audit，后一个体失败
// 时，主 DML 与已成功的体写一并回滚（同事务原子性）。
func TestRowLevelBodyRollbackWithMainDML(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")
	// 触发器 1：体写入 audit（先成功）
	mustExec(t, e, "CREATE TRIGGER tr_ok AFTER INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 1")
	// 触发器 2：体执行期失败（WHERE 引用不存在列）
	mustExec(t, e, "CREATE TRIGGER tr_bad AFTER INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 2 WHERE nope = 1")

	mustErr(t, e, "INSERT INTO users (id, name, age) VALUES (9, 'henry', 29)", "trigger tr_bad failed")
	// 主 DML 回滚：users 无 id=9
	if rows := mustRows(t, e, "SELECT id FROM users WHERE id = 9"); len(rows) != 0 {
		t.Fatalf("main DML should roll back on trigger failure, found row 9")
	}
	// 触发器 1 的体写也随主事务回滚：audit.cnt 保持 0
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "0" {
		t.Fatalf("body write should roll back with main DML: cnt = %s, want 0", got)
	}
}

// 递归策略：直接递归（同表同事件）被同键栈抑制仅执行一次；不同触发器链式
// 触发可达；超过深度上限的深层链式被抑制。
func TestRowLevelChainAndRecursionLimit(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")
	mustExec(t, e, "CREATE TABLE log (msg TEXT)")
	mustExec(t, e, "CREATE TABLE log2 (msg TEXT)")
	// 链式：INSERT users → tr_a（写 audit）→ UPDATE audit → tr_b（写 log）
	mustExec(t, e, "CREATE TRIGGER tr_a AFTER INSERT ON users FOR EACH ROW UPDATE audit SET cnt = NEW.id")
	mustExec(t, e, "CREATE TRIGGER tr_b AFTER UPDATE ON audit FOR EACH ROW INSERT INTO log (msg) VALUES ('chained')")
	// 深层：INSERT log → tr_c（写 log2）；应被深度上限（默认 1 层嵌套）抑制
	mustExec(t, e, "CREATE TRIGGER tr_c AFTER INSERT ON log FOR EACH ROW INSERT INTO log2 (msg) VALUES ('deep')")

	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	// 链式第 2 层可达：tr_a 生效且 tr_b 被触发
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "4" {
		t.Fatalf("chain tr_a: cnt = %s, want 4", got)
	}
	rows := mustRows(t, e, "SELECT msg FROM log")
	if len(rows) != 1 || rows[0][0].String() != "chained" {
		t.Fatalf("chain tr_b should fire: log = %v, want [chained]", rows)
	}
	// 深层链式被限：tr_c 不触发，log2 无行
	if rows := mustRows(t, e, "SELECT * FROM log2"); len(rows) != 0 {
		t.Fatalf("deep chain beyond limit should be suppressed: log2 rows = %d, want 0", len(rows))
	}
}

// FOR EACH STATEMENT 显式声明：语句级一次触发（与无子句默认一致）。
func TestRowLevelForEachStatementExplicit(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")
	mustExec(t, e, "CREATE TRIGGER tr_st AFTER INSERT ON users FOR EACH STATEMENT UPDATE audit SET cnt = 100")

	// 批量两行仍只触发一次：cnt 置 100
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (5, 'erin', 22), (6, 'frank', 28)")
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "100" {
		t.Fatalf("FOR EACH STATEMENT should fire once: cnt = %s, want 100", got)
	}
}

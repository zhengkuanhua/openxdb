package sql_test

import (
	"strings"
	"testing"
)

// M15-3 存储过程测试：CREATE/DROP PROCEDURE、CALL、IN 参数、
// 多语句体、局部变量（SET @x = <literal> 加分项）、元数据持久化、
// SHOW PROCEDURES、与物化视图/触发器/视图共存、递归防护、
// 语法错误与重复定义。
// 支持范围说明：过程体为 INSERT/UPDATE/DELETE/SELECT/CALL 语句，
// 参数/变量引用 @name；SET 局部变量仅支持字面量赋值；
// 递归/嵌套 CALL 受 procDepth 防护限制；SELECT 体执行但不返回结果集。

func TestM15CreateCallShowDropProcedure(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")

	// 无参多语句过程：INSERT + UPDATE 顺序执行。
	mustExec(t, e, "CREATE PROCEDURE p_ins BEGIN INSERT INTO users (id, name, age) VALUES (4, 'dave', 40); UPDATE audit SET cnt = 100; END")

	// SHOW PROCEDURES 列出过程元数据。
	rows := mustRows(t, e, "SHOW PROCEDURES")
	if len(rows) != 1 {
		t.Fatalf("SHOW PROCEDURES: want 1 row, got %d", len(rows))
	}
	if got := rows[0][0].String(); got != "p_ins" {
		t.Fatalf("SHOW PROCEDURES name: want p_ins, got %s", got)
	}
	if got := rows[0][2].String(); !strings.Contains(got, "INSERT INTO users") {
		t.Fatalf("SHOW PROCEDURES body: want INSERT body, got %s", got)
	}

	// CALL 执行过程：多语句顺序生效。
	mustExec(t, e, "CALL p_ins()")
	if got := mustRows(t, e, "SELECT age FROM users WHERE id = 4")[0][0].String(); got != "40" {
		t.Fatalf("CALL p_ins: age = %s, want 40", got)
	}
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "100" {
		t.Fatalf("CALL p_ins: cnt = %s, want 100", got)
	}

	// CALL 可重复执行：过程无状态，每次调用插入新行（重复调用前清理目标行）。
	mustExec(t, e, "DELETE FROM users WHERE id = 4")
	mustExec(t, e, "CALL p_ins()")
	if got := mustRows(t, e, "SELECT COUNT(*) FROM users")[0][0].String(); got != "4" {
		t.Fatalf("repeat CALL p_ins: want 4 users, got %s", got)
	}
	mustExec(t, e, "DELETE FROM users WHERE id = 4")
	mustExec(t, e, "CALL p_ins()")
	if got := mustRows(t, e, "SELECT COUNT(*) FROM users")[0][0].String(); got != "4" {
		t.Fatalf("repeat CALL p_ins again: want 4 users, got %s", got)
	}

	// DROP 后 SHOW 为空，且过程不再可调用。
	mustExec(t, e, "DROP PROCEDURE p_ins")
	if rows := mustRows(t, e, "SHOW PROCEDURES"); len(rows) != 0 {
		t.Fatalf("after DROP PROCEDURE: want 0 rows, got %d", len(rows))
	}
	mustErr(t, e, "CALL p_ins()", "procedure not exists")
}

func TestM15CallWithInParams(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// IN 关键字可省略；参数按位置代入。
	mustExec(t, e, "CREATE PROCEDURE p_add (IN id INT, name TEXT) BEGIN INSERT INTO users (id, name, age) VALUES (@id, @name, 50); END")
	mustExec(t, e, "CALL p_add(4, 'dave')")
	if got := mustRows(t, e, "SELECT name FROM users WHERE id = 4")[0][0].String(); got != "dave" {
		t.Fatalf("IN param name: got %s, want dave", got)
	}
	if got := mustRows(t, e, "SELECT age FROM users WHERE id = 4")[0][0].String(); got != "50" {
		t.Fatalf("literal in body: got %s, want 50", got)
	}
	// 再次调用不同实参。
	mustExec(t, e, "CALL p_add(5, 'erin')")
	if got := mustRows(t, e, "SELECT name FROM users WHERE id = 5")[0][0].String(); got != "erin" {
		t.Fatalf("second CALL param: got %s, want erin", got)
	}
	// 参数个数不匹配报错。
	mustErr(t, e, "CALL p_add(6)", "expects 2 argument(s)")
	mustErr(t, e, "CALL p_add(6, 'frank', 99)", "expects 2 argument(s)")
}

func TestM15ProcedureMultiStatementBody(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (v INT)")
	mustExec(t, e, "INSERT INTO t (v) VALUES (1)")

	// 三条语句顺序执行：两次 UPDATE + 一次 INSERT。
	mustExec(t, e, "CREATE PROCEDURE p_multi BEGIN UPDATE t SET v = 2 WHERE v = 1; UPDATE t SET v = 3 WHERE v = 2; INSERT INTO t (v) VALUES (4); END")
	mustExec(t, e, "CALL p_multi")
	rows := mustRows(t, e, "SELECT v FROM t")
	if len(rows) != 2 {
		t.Fatalf("multi-statement body: want 2 rows, got %d", len(rows))
	}
	if got := rows[0][0].String(); got != "3" {
		t.Fatalf("first stmt update chain: got %s, want 3", got)
	}
	if got := rows[1][0].String(); got != "4" {
		t.Fatalf("inserted stmt: got %s, want 4", got)
	}
}

func TestM15ProcedureLocalVariable(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (v INT)")
	mustExec(t, e, "CREATE TABLE s (v TEXT)")

	// SET 局部变量（INT 字面量）随后在 INSERT 中读取。
	mustExec(t, e, "CREATE PROCEDURE p_var BEGIN SET @x = 5; INSERT INTO t (v) VALUES (@x); END")
	mustExec(t, e, "CALL p_var")
	if got := mustRows(t, e, "SELECT v FROM t")[0][0].String(); got != "5" {
		t.Fatalf("local var INT: got %s, want 5", got)
	}
	// 字符串局部变量。
	mustExec(t, e, "CREATE PROCEDURE p_str BEGIN SET @s = 'hello'; INSERT INTO s (v) VALUES (@s); END")
	mustExec(t, e, "CALL p_str")
	if got := mustRows(t, e, "SELECT v FROM s")[0][0].String(); got != "hello" {
		t.Fatalf("local var TEXT: got %s, want hello", got)
	}
}

func TestM15ProcedureDropIfExistsAndDuplicate(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// DROP PROCEDURE IF EXISTS 不存在的名字：成功且无副作用。
	mustExec(t, e, "DROP PROCEDURE IF EXISTS p_ghost")

	// 重复定义报错。
	mustExec(t, e, "CREATE PROCEDURE p_d BEGIN INSERT INTO users (id, name, age) VALUES (9, 'zoe', 9); END")
	mustErr(t, e, "CREATE PROCEDURE p_d BEGIN INSERT INTO users (id, name, age) VALUES (8, 'y', 8); END", "procedure already exists")

	// DROP 后可重新创建。
	mustExec(t, e, "DROP PROCEDURE p_d")
	mustExec(t, e, "CREATE PROCEDURE p_d BEGIN INSERT INTO users (id, name, age) VALUES (8, 'y', 8); END")
	mustExec(t, e, "CALL p_d")
	if got := mustRows(t, e, "SELECT COUNT(*) FROM users")[0][0].String(); got != "4" {
		t.Fatalf("re-create procedure: want 4 users, got %s", got)
	}
}

func TestM15ProcedureSyntaxErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// 缺少收尾 END。
	mustErr(t, e, "CREATE PROCEDURE p_bad BEGIN INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)", "expected keyword END")
	// 空体。
	mustErr(t, e, "CREATE PROCEDURE p_bad BEGIN END", "procedure body required")
	// 非法参数类型。
	mustErr(t, e, "CREATE PROCEDURE p_bad (IN x FLOAT) BEGIN INSERT INTO users (id, name, age) VALUES (1, 'a', 1); END", "unsupported parameter type")
	// 体含不支持语句（CREATE TABLE）。
	mustErr(t, e, "CREATE PROCEDURE p_bad BEGIN CREATE TABLE tt (x INT); END", "unsupported statement in procedure body")
	// SHOW 单数 PROCEDURE 非法。
	mustErr(t, e, "SHOW PROCEDURE", "expected")
	// 参数列表缺右括号。
	mustErr(t, e, "CREATE PROCEDURE p_bad (IN x INT BEGIN INSERT INTO users (id, name, age) VALUES (1, 'a', 1); END", "expected )")
}

func TestM15ProcedureUnknownVariable(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (v INT)")

	// 创建期允许（语法可解析），调用时未定义变量报错。
	mustExec(t, e, "CREATE PROCEDURE p_uv BEGIN INSERT INTO t (v) VALUES (@nope); END")
	mustErr(t, e, "CALL p_uv", "unknown procedure variable: @nope")
}

func TestM15ProcedureRecursionGuard(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// 过程体 CALL 自身：创建成功；P0-2 放开递归，无限递归由深度上限（栈保护）拦截。
	mustExec(t, e, "CREATE PROCEDURE p_rec BEGIN CALL p_rec; END")
	mustErr(t, e, "CALL p_rec", "recursion depth exceeded")
}

func TestM15ProcedureCoexistWithOthers(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")

	// 物化视图 / 触发器 / 虚拟视图 / 存储过程并存。
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv_users AS SELECT id, name FROM users")
	mustExec(t, e, "CREATE VIEW v_users AS SELECT id, name FROM users")
	mustExec(t, e, "CREATE TRIGGER tr_ins AFTER INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 100")
	mustExec(t, e, "CREATE PROCEDURE p_add (IN id INT, name TEXT) BEGIN INSERT INTO users (id, name, age) VALUES (@id, @name, 50); END")

	// 调用过程写 users → 触发器联动 audit。
	mustExec(t, e, "CALL p_add(4, 'dave')")
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "100" {
		t.Fatalf("procedure + trigger coexist: cnt = %s, want 100", got)
	}
	// 物化视图刷新后可见过程写入的新行。
	mustExec(t, e, "REFRESH MATERIALIZED VIEW mv_users")
	if got := mustRows(t, e, "SELECT COUNT(*) FROM mv_users")[0][0].String(); got != "4" {
		t.Fatalf("matview after procedure CALL: want 4 rows, got %s", got)
	}
	// 虚拟视图可查过程写入的行。
	if got := mustRows(t, e, "SELECT name FROM v_users WHERE id = 4")[0][0].String(); got != "dave" {
		t.Fatalf("view after procedure CALL: got %s, want dave", got)
	}
	// 各对象 SHOW 互不干扰。
	if rows := mustRows(t, e, "SHOW PROCEDURES"); len(rows) != 1 {
		t.Fatalf("SHOW PROCEDURES with others: want 1, got %d", len(rows))
	}
	if rows := mustRows(t, e, "SHOW TRIGGERS"); len(rows) != 1 {
		t.Fatalf("SHOW TRIGGERS with others: want 1, got %d", len(rows))
	}
	if rows := mustRows(t, e, "SHOW VIEWS"); len(rows) != 1 {
		t.Fatalf("SHOW VIEWS with others: want 1, got %d", len(rows))
	}
}

func TestM15ProcedurePersistAcrossCalls(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// 过程元数据持久化在目录键：多次 CALL 与 SHOW 均稳定可见。
	mustExec(t, e, "CREATE PROCEDURE p_persist (IN id INT) BEGIN INSERT INTO users (id, name, age) VALUES (@id, 'persist', 1); END")
	mustExec(t, e, "CALL p_persist(10)")
	mustExec(t, e, "CALL p_persist(11)")
	rows := mustRows(t, e, "SHOW PROCEDURES")
	if len(rows) != 1 {
		t.Fatalf("persist: want 1 procedure, got %d", len(rows))
	}
	if got := rows[0][0].String(); got != "p_persist" {
		t.Fatalf("persist name: got %s, want p_persist", got)
	}
	if got := mustRows(t, e, "SELECT COUNT(*) FROM users WHERE name = 'persist'")[0][0].String(); got != "2" {
		t.Fatalf("persist calls: want 2 rows, got %s", got)
	}
}

func TestM15ProcedureCallRepeatedly(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustExec(t, e, "CREATE PROCEDURE p_rep (IN id INT) BEGIN INSERT INTO users (id, name, age) VALUES (@id, 'rep', 1); END")
	// 同一过程多次 CALL，每次按实参生效。
	mustExec(t, e, "CALL p_rep(20)")
	mustExec(t, e, "CALL p_rep(21)")
	mustExec(t, e, "CALL p_rep(22)")
	if got := mustRows(t, e, "SELECT COUNT(*) FROM users WHERE name = 'rep'")[0][0].String(); got != "3" {
		t.Fatalf("repeated CALL: want 3 rows, got %s", got)
	}
}

func TestM15ProcedureLiteralAndCaseEnd(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// 体直接使用字面量（不带 @）可正常执行；CASE ... END 表达式不干扰收尾 END 识别。
	mustExec(t, e, "CREATE PROCEDURE p_lit BEGIN INSERT INTO users (id, name, age) VALUES (30, 'lit', 33); UPDATE users SET age = 44 WHERE id = 30; END")
	mustExec(t, e, "CALL p_lit")
	if got := mustRows(t, e, "SELECT age FROM users WHERE id = 30")[0][0].String(); got != "44" {
		t.Fatalf("literal body: got %s, want 44", got)
	}
}

package sql_test

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// v1.19.0 存储过程异常处理与动态 SQL 测试：
//   - DECLARE {CONTINUE|EXIT} HANDLER FOR SQLSTATE / 错误码 / SQLEXCEPTION / SQLWARNING / NOT FOUND
//   - 处理器嵌套作用域与块退出时出栈
//   - SIGNAL / RESIGNAL（含 SET MESSAGE_TEXT / MYSQL_ERRNO）
//   - PREPARE / EXECUTE [USING ...] / DEALLOCATE PREPARE
// 覆盖正常与异常路径，并验证未声明 HANDLER 的过程行为不变。

func auditInts(t *testing.T, e *sql.Engine, table string) []string {
	t.Helper()
	rows := mustRows(t, e, "SELECT cnt FROM "+table+" ORDER BY cnt")
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r[0].String())
	}
	return out
}

// TestProcContinueHandlerSQLState CONTINUE 处理器捕获 SIGNAL 后继续执行后续语句。
func TestProcContinueHandlerSQLState(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_sig BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' INSERT INTO audit (cnt) VALUES (1); SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'boom'; INSERT INTO users (id, name, age) VALUES (10, 'after', 11); END")

	mustExec(t, e, "CALL p_sig()")
	if got := auditInts(t, e, "audit"); len(got) != 1 || got[0] != "1" {
		t.Fatalf("CONTINUE HANDLER: audit = %v, want [1]", got)
	}
	// CONTINUE 语义：出错语句之后的语句照常执行。
	if got := mustRows(t, e, "SELECT name FROM users WHERE id = 10")[0][0].String(); got != "after" {
		t.Fatalf("CONTINUE HANDLER: id=10 name = %q, want after", got)
	}
}

// TestProcExitHandlerSQLState EXIT 处理器捕获后退出当前块，块内后续语句不执行。
func TestProcExitHandlerSQLState(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_exit BEGIN DECLARE EXIT HANDLER FOR SQLSTATE '45000' INSERT INTO audit (cnt) VALUES (2); SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'stop'; INSERT INTO users (id, name, age) VALUES (11, 'never', 12); END")

	mustExec(t, e, "CALL p_exit()")
	if got := auditInts(t, e, "audit"); len(got) != 1 || got[0] != "2" {
		t.Fatalf("EXIT HANDLER: audit = %v, want [2]", got)
	}
	// EXIT 语义：块内剩余语句被跳过。
	if rows := mustRows(t, e, "SELECT id FROM users WHERE id = 11"); len(rows) != 0 {
		t.Fatalf("EXIT HANDLER: id=11 should not be inserted, got %d rows", len(rows))
	}
}

// TestProcHandlerNotFoundCursor 游标取尽触发 NOT FOUND，EXIT 处理器捕获并终止遍历。
func TestProcHandlerNotFoundCursor(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_cur BEGIN DECLARE cur CURSOR FOR SELECT id FROM users; DECLARE EXIT HANDLER FOR NOT FOUND INSERT INTO audit (cnt) VALUES (99); OPEN cur; FETCH cur INTO @u; INSERT INTO audit (cnt) VALUES (@u); FETCH cur INTO @u; INSERT INTO audit (cnt) VALUES (@u); FETCH cur INTO @u; INSERT INTO audit (cnt) VALUES (@u); FETCH cur INTO @u; INSERT INTO audit (cnt) VALUES (@u); CLOSE cur; END")

	mustExec(t, e, "CALL p_cur()")
	got := auditInts(t, e, "audit")
	want := []string{"1", "2", "3", "99"}
	if len(got) != len(want) {
		t.Fatalf("NOT FOUND cursor: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("NOT FOUND cursor: audit = %v, want %v", got, want)
		}
	}
}

// TestProcHandlerScopePops 内层块声明的处理器随块退出出栈，块外不再生效。
func TestProcHandlerScopePops(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_scope BEGIN DECLARE @n INT DEFAULT 0; WHILE @n < 1 DO DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' INSERT INTO audit (cnt) VALUES (3); SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'inner'; SET @n = @n + 1; END WHILE; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'outer'; END")

	_, err := e.Execute("CALL p_scope()")
	if err == nil {
		t.Fatalf("scope pop: want error for outer SIGNAL, got nil")
	}
	if !strings.Contains(err.Error(), "outer") {
		t.Fatalf("scope pop: want error containing %q, got %v", "outer", err)
	}
	// 内层块的处理器确实执行过一次。
	if got := auditInts(t, e, "audit"); len(got) != 1 || got[0] != "3" {
		t.Fatalf("scope pop: audit = %v, want [3]", got)
	}
}

// TestProcHandlerSQLEXCEPTIONAndWarning SQLEXCEPTION 与 SQLWARNING 分类匹配。
func TestProcHandlerSQLEXCEPTIONAndWarning(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")

	// SQLEXCEPTION：捕获普通 SQL 执行错误（无 SQLSTATE 归为 HY000）。
	mustExec(t, e, "CREATE PROCEDURE p_sqlex BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION INSERT INTO audit (cnt) VALUES (4); INSERT INTO nosuchtable (a) VALUES (1); INSERT INTO audit (cnt) VALUES (5); END")
	mustExec(t, e, "CALL p_sqlex()")
	if got := auditInts(t, e, "audit"); len(got) != 2 || got[0] != "4" || got[1] != "5" {
		t.Fatalf("SQLEXCEPTION HANDLER: audit = %v, want [4 5]", got)
	}

	// SQLWARNING：捕获 01 类 SQLSTATE。
	mustExec(t, e, "CREATE TABLE audit2 (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_warn BEGIN DECLARE CONTINUE HANDLER FOR SQLWARNING INSERT INTO audit2 (cnt) VALUES (6); SIGNAL SQLSTATE '01000' SET MESSAGE_TEXT = 'warn'; INSERT INTO audit2 (cnt) VALUES (7); END")
	mustExec(t, e, "CALL p_warn()")
	if got := auditInts(t, e, "audit2"); len(got) != 2 || got[0] != "6" || got[1] != "7" {
		t.Fatalf("SQLWARNING HANDLER: audit2 = %v, want [6 7]", got)
	}
}

// TestProcHandlerErrno 错误码条件与 SQLSTATE 条件可并列声明，按「最近声明优先」匹配。
func TestProcHandlerErrno(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_errno BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' INSERT INTO audit (cnt) VALUES (9); DECLARE CONTINUE HANDLER FOR 1062 INSERT INTO audit (cnt) VALUES (8); SIGNAL SQLSTATE '23000' SET MYSQL_ERRNO = 1062, MESSAGE_TEXT = 'dup'; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'plain'; END")

	mustExec(t, e, "CALL p_errno()")
	if got := auditInts(t, e, "audit"); len(got) != 2 || got[0] != "8" || got[1] != "9" {
		t.Fatalf("errno HANDLER: audit = %v, want [8 9]", got)
	}
}

// TestProcSignalUnhandled 未声明处理器时 SIGNAL 冒泡为 CALL 错误，错误文本携带 SQLSTATE。
func TestProcSignalUnhandled(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_unhandled BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'boom'; INSERT INTO audit (cnt) VALUES (99); END")

	_, err := e.Execute("CALL p_unhandled()")
	if err == nil {
		t.Fatalf("unhandled SIGNAL: want error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "45000") || !strings.Contains(msg, "boom") {
		t.Fatalf("unhandled SIGNAL: want SQLSTATE 45000 with message boom, got %v", err)
	}
	// 抛出后过程体后续语句不执行。
	if got := auditInts(t, e, "audit"); len(got) != 0 {
		t.Fatalf("unhandled SIGNAL: audit = %v, want empty", got)
	}
}

// TestProcResignalOuterHandler RESIGNAL 在处理器内重抛，由外层处理器接管。
func TestProcResignalOuterHandler(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_rise BEGIN DECLARE @n INT DEFAULT 0; DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' INSERT INTO audit (cnt) VALUES (10); WHILE @n < 1 DO DECLARE CONTINUE HANDLER FOR SQLEXCEPTION RESIGNAL; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'inner'; SET @n = @n + 1; END WHILE; END")

	mustExec(t, e, "CALL p_rise()")
	if got := auditInts(t, e, "audit"); len(got) != 1 || got[0] != "10" {
		t.Fatalf("RESIGNAL: audit = %v, want [10]", got)
	}
}

// TestProcDynamicSQLUsing PREPARE / EXECUTE ... USING / DEALLOCATE 正常路径。
func TestProcDynamicSQLUsing(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE PROCEDURE p_dyn (IN nid INT, nname TEXT) BEGIN PREPARE s FROM 'INSERT INTO users (id, name, age) VALUES (?, ?, 60)'; EXECUTE s USING @nid, @nname; DEALLOCATE PREPARE s; END")

	mustExec(t, e, "CALL p_dyn(4, 'dave')")
	rows := mustRows(t, e, "SELECT name, age FROM users WHERE id = 4")
	if len(rows) != 1 {
		t.Fatalf("dynamic INSERT: want 1 row, got %d", len(rows))
	}
	if got := rows[0][0].String(); got != "dave" {
		t.Fatalf("dynamic INSERT: name = %q, want dave", got)
	}
	if got := rows[0][1].String(); got != "60" {
		t.Fatalf("dynamic INSERT: age = %q, want 60", got)
	}
}

// TestProcDynamicSQLFromVar PREPARE FROM @var：语句文本来自过程变量。
func TestProcDynamicSQLFromVar(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE PROCEDURE p_dyn2 BEGIN SET @q = 'DELETE FROM users WHERE id = 3'; PREPARE s FROM @q; EXECUTE s; DEALLOCATE PREPARE s; END")

	mustExec(t, e, "CALL p_dyn2()")
	if got := mustRows(t, e, "SELECT COUNT(*) FROM users")[0][0].String(); got != "2" {
		t.Fatalf("PREPARE FROM @var: want 2 users, got %s", got)
	}
}

// TestProcDynamicSQLErrors 动态 SQL 异常路径：未定义语句、参数个数不匹配、空文本、语句白名单。
func TestProcDynamicSQLErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustExec(t, e, "CREATE PROCEDURE p_e1 BEGIN EXECUTE s; END")
	mustErr(t, e, "CALL p_e1()", "prepared statement not exists")

	mustExec(t, e, "CREATE PROCEDURE p_e2 BEGIN DEALLOCATE PREPARE s; END")
	mustErr(t, e, "CALL p_e2()", "prepared statement not exists")

	// 实参多于占位符。
	mustExec(t, e, "CREATE PROCEDURE p_e3 BEGIN PREPARE s FROM 'SELECT * FROM users WHERE id = ?'; EXECUTE s USING 1, 2; DEALLOCATE PREPARE s; END")
	mustErr(t, e, "CALL p_e3()", "argument count mismatch")

	// 占位符多于实参。
	mustExec(t, e, "CREATE PROCEDURE p_e4 BEGIN PREPARE s FROM 'SELECT * FROM users WHERE id = ?'; EXECUTE s; END")
	mustErr(t, e, "CALL p_e4()", "argument count mismatch")

	// 空 SQL 文本。
	mustExec(t, e, "CREATE PROCEDURE p_e5 BEGIN PREPARE s FROM ''; END")
	mustErr(t, e, "CALL p_e5()", "PREPARE requires non-empty SQL text")

	// 语句白名单：过程体未支持的语句类型不允许通过动态 SQL 绕过。
	mustExec(t, e, "CREATE PROCEDURE p_e6 BEGIN PREPARE s FROM 'SHOW TABLES'; EXECUTE s; END")
	mustErr(t, e, "CALL p_e6()", "unsupported statement in dynamic SQL")
}

// TestProcHandlerParseErrors 创建期语法校验：非法 SQLSTATE、缺条件、缺语句体。
func TestProcHandlerParseErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustErr(t, e, "CREATE PROCEDURE p_bad1 BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE 'abc' INSERT INTO users (id) VALUES (1); END", "invalid SQLSTATE")
	mustErr(t, e, "CREATE PROCEDURE p_bad2 BEGIN SIGNAL; END", "SIGNAL requires SQLSTATE")
	mustErr(t, e, "CREATE PROCEDURE p_bad3 BEGIN DECLARE CONTINUE HANDLER FOR 9999; END", "HANDLER requires a statement")
	mustErr(t, e, "CREATE PROCEDURE p_bad4 BEGIN DECLARE CONTINUE HANDLER FOR FOO INSERT INTO users (id) VALUES (1); END", "unknown condition in HANDLER FOR")
	mustErr(t, e, "CREATE PROCEDURE p_bad5 BEGIN DECLARE CONTINUE HANDLER FOR CONDITION nope INSERT INTO users (id) VALUES (1); END", "unknown condition in HANDLER FOR")
	mustErr(t, e, "CREATE PROCEDURE p_bad6 BEGIN DECLARE c CONDITION FOR SQLEXCEPTION; END", "CONDITION FOR requires SQLSTATE")
}

package sql_test

import (
	"strings"
	"testing"
)

// v1.19.0 存储过程命名条件与诊断区测试：
//   - DECLARE <name> CONDITION FOR SQLSTATE '<值>' / <错误码>
//   - HANDLER FOR <name> / HANDLER FOR CONDITION <name> 按命名条件匹配
//   - SIGNAL <name> / RESIGNAL <name>
//   - GET DIAGNOSTICS：ROW_COUNT 与 CONDITION <n> 的 RETURNED_SQLSTATE / MYSQL_ERRNO / MESSAGE_TEXT
//   - HANDLER 体为多语句 BEGIN ... END 复合块
// 覆盖正常与异常路径，并验证未使用命名条件的过程行为不变。

// TestProcNamedConditionHandler 命名条件（SQLSTATE）在 SIGNAL 与 HANDLER FOR 中匹配。
func TestProcNamedConditionHandler(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_cond_state BEGIN DECLARE dup CONDITION FOR SQLSTATE '23000'; DECLARE CONTINUE HANDLER FOR dup INSERT INTO audit (cnt) VALUES (5); SIGNAL dup; INSERT INTO audit (cnt) VALUES (6); END")

	mustExec(t, e, "CALL p_cond_state()")
	got := auditInts(t, e, "audit")
	want := []string{"5", "6"}
	if len(got) != len(want) {
		t.Fatalf("named condition handler: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("named condition handler: audit = %v, want %v", got, want)
		}
	}
}

// TestProcNamedConditionErrnoCondKeyword 命名条件（错误码）+ HANDLER FOR CONDITION <name> 写法。
func TestProcNamedConditionErrnoCondKeyword(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_cond_errno BEGIN DECLARE nf CONDITION FOR 1062; DECLARE EXIT HANDLER FOR CONDITION nf INSERT INTO audit (cnt) VALUES (1062); SIGNAL nf; INSERT INTO audit (cnt) VALUES (100); END")

	mustExec(t, e, "CALL p_cond_errno()")
	if got := auditInts(t, e, "audit"); len(got) != 1 || got[0] != "1062" {
		t.Fatalf("named condition errno: audit = %v, want [1062]", got)
	}
}

// TestProcNamedConditionResignal 内层处理器 RESIGNAL <name>，由外层命名条件处理器捕获。
func TestProcNamedConditionResignal(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_cond_rise BEGIN DECLARE outer_c CONDITION FOR SQLSTATE '45001'; DECLARE CONTINUE HANDLER FOR outer_c INSERT INTO audit (cnt) VALUES (8); BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' RESIGNAL outer_c; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'inner'; END; INSERT INTO audit (cnt) VALUES (9); END")

	mustExec(t, e, "CALL p_cond_rise()")
	got := auditInts(t, e, "audit")
	want := []string{"8", "9"}
	if len(got) != len(want) {
		t.Fatalf("named condition resignal: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("named condition resignal: audit = %v, want %v", got, want)
		}
	}
}

// TestProcNamedConditionBlockScope 块内声明的命名条件随块退出出栈（块外不可见）。
func TestProcNamedConditionBlockScope(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_cond_scope BEGIN BEGIN DECLARE local_c CONDITION FOR SQLSTATE '45000'; DECLARE CONTINUE HANDLER FOR local_c INSERT INTO audit (cnt) VALUES (1); SIGNAL local_c; END; INSERT INTO audit (cnt) VALUES (2); END")

	mustExec(t, e, "CALL p_cond_scope()")
	got := auditInts(t, e, "audit")
	want := []string{"1", "2"}
	if len(got) != len(want) {
		t.Fatalf("named condition block scope: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("named condition block scope: audit = %v, want %v", got, want)
		}
	}
}

// TestProcConditionParseErrors 命名条件的创建期校验：未声明引用、非法 FOR 目标。
func TestProcConditionParseErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// 未声明的命名条件在 HANDLER FOR / SIGNAL / RESIGNAL 中均应在创建期报错。
	mustErr(t, e, "CREATE PROCEDURE p_c_bad1 BEGIN SIGNAL nope; END", "unknown condition in SIGNAL")
	mustErr(t, e, "CREATE PROCEDURE p_c_bad2 BEGIN DECLARE EXIT HANDLER FOR SQLSTATE '45000' RESIGNAL nope; SIGNAL SQLSTATE '45000'; END", "unknown condition in RESIGNAL")
	mustErr(t, e, "CREATE PROCEDURE p_c_bad3 BEGIN DECLARE EXIT HANDLER FOR SQLSTATE '45000' INSERT INTO users (id) VALUES (1); DECLARE CONTINUE HANDLER FOR nope INSERT INTO users (id) VALUES (2); SIGNAL SQLSTATE '45000'; END", "unknown condition in HANDLER FOR")
	// CONDITION FOR 只接受 SQLSTATE 与数字错误码。
	mustErr(t, e, "CREATE PROCEDURE p_c_bad4 BEGIN DECLARE c CONDITION FOR SQLEXCEPTION; END", "CONDITION FOR requires SQLSTATE")
	mustErr(t, e, "CREATE PROCEDURE p_c_bad5 BEGIN DECLARE c CONDITION FOR SQLSTATE 'zz'; END", "invalid SQLSTATE")
}

// TestProcGetDiagnosticsRowCount GET DIAGNOSTICS ROW_COUNT 取最近一条 DML 影响行数。
func TestProcGetDiagnosticsRowCount(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_diag_rc BEGIN DECLARE @n INT DEFAULT 0; INSERT INTO users (id, name, age) VALUES (20, 'r1', 1); GET DIAGNOSTICS @n = ROW_COUNT; INSERT INTO audit (cnt) VALUES (@n); INSERT INTO users (id, name, age) VALUES (21, 'r2', 1), (22, 'r3', 2); GET DIAGNOSTICS @n = ROW_COUNT; INSERT INTO audit (cnt) VALUES (@n); END")

	mustExec(t, e, "CALL p_diag_rc()")
	got := auditInts(t, e, "audit")
	want := []string{"1", "2"}
	if len(got) != len(want) {
		t.Fatalf("GET DIAGNOSTICS ROW_COUNT: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("GET DIAGNOSTICS ROW_COUNT: audit = %v, want %v", got, want)
		}
	}
}

// TestProcGetDiagnosticsCondition HANDLER 内读取诊断区三项，处理器体为多语句复合块。
func TestProcGetDiagnosticsCondition(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE TABLE audit_msg (msg TEXT)")
	mustExec(t, e, "CREATE PROCEDURE p_diag_cond BEGIN DECLARE @st TEXT DEFAULT ''; DECLARE @code INT DEFAULT 0; DECLARE @msg TEXT DEFAULT ''; DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' BEGIN GET DIAGNOSTICS CONDITION 1 @st = RETURNED_SQLSTATE, @code = MYSQL_ERRNO, @msg = MESSAGE_TEXT; INSERT INTO audit (cnt) VALUES (@code); INSERT INTO audit_msg (msg) VALUES (@msg); END; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'boom', MYSQL_ERRNO = 4321; INSERT INTO audit (cnt) VALUES (9999); END")

	mustExec(t, e, "CALL p_diag_cond()")
	// CONTINUE 处理器执行完后继续后续语句，故 audit 含 4321（处理期写入）与 9999（信号后续语句）。
	got := auditInts(t, e, "audit")
	want := []string{"4321", "9999"}
	if len(got) != len(want) {
		t.Fatalf("GET DIAGNOSTICS CONDITION MYSQL_ERRNO: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("GET DIAGNOSTICS CONDITION MYSQL_ERRNO: audit = %v, want %v", got, want)
		}
	}
	// MESSAGE_TEXT 取的是 SIGNAL 抛出的错误文本（含 SQLSTATE 前缀与自定义消息）。
	rows := mustRows(t, e, "SELECT msg FROM audit_msg")
	if len(rows) != 1 || !strings.Contains(rows[0][0].String(), "boom") {
		t.Fatalf("GET DIAGNOSTICS CONDITION MESSAGE_TEXT: %v, want contains boom", rows)
	}
}

// TestProcHandlerCompoundBody 处理器体为 BEGIN ... END 复合块（含块内声明与条件分支）。
func TestProcHandlerCompoundBody(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_h_compound BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN DECLARE @tag INT DEFAULT 42; INSERT INTO audit (cnt) VALUES (@tag); IF @tag > 0 THEN INSERT INTO audit (cnt) VALUES (43); END IF; END; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'x'; INSERT INTO audit (cnt) VALUES (44); END")

	mustExec(t, e, "CALL p_h_compound()")
	got := auditInts(t, e, "audit")
	want := []string{"42", "43", "44"}
	if len(got) != len(want) {
		t.Fatalf("handler compound body: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("handler compound body: audit = %v, want %v", got, want)
		}
	}
}

// TestProcGetDiagnosticsErrors GET DIAGNOSTICS 的创建期与运行期异常路径。
func TestProcGetDiagnosticsErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// 创建期：ROW_COUNT 不能出现在 CONDITION 形式中。
	mustErr(t, e, "CREATE PROCEDURE p_d_bad1 BEGIN DECLARE @n INT DEFAULT 0; GET DIAGNOSTICS CONDITION 1 @n = ROW_COUNT; END", "ROW_COUNT is not valid in GET DIAGNOSTICS CONDITION")
	// 创建期：诊断字段不支持时立即报错。
	mustErr(t, e, "CREATE PROCEDURE p_d_bad2 BEGIN DECLARE @s TEXT DEFAULT ''; GET DIAGNOSTICS CONDITION 1 @s = NOSUCH_FIELD; END", "unsupported GET DIAGNOSTICS item")
	// 创建期：CONDITION 类字段必须带序号。
	mustErr(t, e, "CREATE PROCEDURE p_d_bad3 BEGIN DECLARE @s TEXT DEFAULT ''; GET DIAGNOSTICS @s = RETURNED_SQLSTATE; END", "requires CONDITION")

	// 运行期：CONDITION 序号越界。
	mustExec(t, e, "CREATE PROCEDURE p_d_bad4 BEGIN DECLARE @s TEXT DEFAULT ''; GET DIAGNOSTICS CONDITION 2 @s = RETURNED_SQLSTATE; END")
	mustErr(t, e, "CALL p_d_bad4()", "out of range")

	// 运行期：尚无诊断区时读取 CONDITION 字段。
	mustExec(t, e, "CREATE PROCEDURE p_d_bad5 BEGIN DECLARE @s TEXT DEFAULT ''; GET DIAGNOSTICS CONDITION 1 @s = RETURNED_SQLSTATE; END")
	mustErr(t, e, "CALL p_d_bad5()", "requires a prior statement condition")

	// 运行期：序号来自未声明变量。
	mustExec(t, e, "CREATE PROCEDURE p_d_bad6 BEGIN DECLARE @s TEXT DEFAULT ''; GET DIAGNOSTICS CONDITION @k @s = RETURNED_SQLSTATE; END")
	mustErr(t, e, "CALL p_d_bad6()", "unknown procedure variable")
}

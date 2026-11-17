package sql_test

import (
	"testing"
)

// v1.19.0 存储过程嵌套 BEGIN ... END 块测试：
//   - 块内 DECLARE 局部变量 / 游标 / 处理器的嵌套作用域
//   - 块退出时局部名字出栈（对块外既有变量的赋值保留）
//   - 块内 EXIT 只结束本块，块内 ITERATE 上抛给外层循环
// 覆盖正常与异常路径，并验证未使用嵌套块的过程行为不变。

// TestProcNestedBlockVarScope 块内局部变量在块退出后出栈，块内对外层变量赋值保留。
func TestProcNestedBlockVarScope(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_blk_var BEGIN DECLARE @a INT DEFAULT 1; BEGIN DECLARE @b INT DEFAULT 2; SET @a = @a + @b; END; INSERT INTO audit (cnt) VALUES (@a); END")

	mustExec(t, e, "CALL p_blk_var()")
	if got := auditInts(t, e, "audit"); len(got) != 1 || got[0] != "3" {
		t.Fatalf("nested block var scope: audit = %v, want [3]", got)
	}

	// 异常路径：块内局部变量在块退出后不可见。
	mustExec(t, e, "CREATE PROCEDURE p_blk_leak BEGIN BEGIN DECLARE @b INT DEFAULT 2; END; INSERT INTO audit (cnt) VALUES (@b); END")
	mustErr(t, e, "CALL p_blk_leak()", "unknown procedure variable")
}

// TestProcNestedBlockShadow 内层同名声明遮蔽外层，块退出后恢复外层原值。
func TestProcNestedBlockShadow(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_blk_shadow BEGIN DECLARE @v INT DEFAULT 1; BEGIN DECLARE @v INT DEFAULT 9; INSERT INTO audit (cnt) VALUES (@v); END; INSERT INTO audit (cnt) VALUES (@v); END")

	mustExec(t, e, "CALL p_blk_shadow()")
	got := auditInts(t, e, "audit")
	want := []string{"1", "9"}
	if len(got) != len(want) {
		t.Fatalf("nested block shadow: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("nested block shadow: audit = %v, want %v", got, want)
		}
	}
}

// TestProcNestedBlockExit 块内 EXIT 只结束本块，外层块继续执行。
func TestProcNestedBlockExit(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_blk_exit BEGIN BEGIN INSERT INTO audit (cnt) VALUES (1); EXIT; INSERT INTO audit (cnt) VALUES (2); END; INSERT INTO audit (cnt) VALUES (3); END")

	mustExec(t, e, "CALL p_blk_exit()")
	got := auditInts(t, e, "audit")
	want := []string{"1", "3"}
	if len(got) != len(want) {
		t.Fatalf("nested block EXIT: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("nested block EXIT: audit = %v, want %v", got, want)
		}
	}
}

// TestProcNestedBlockCursor 块内声明的游标具有块作用域，FETCH 到外层变量后保留其值。
func TestProcNestedBlockCursor(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_blk_cur BEGIN DECLARE @n INT DEFAULT 0; BEGIN DECLARE cur CURSOR FOR SELECT id FROM users; OPEN cur; FETCH cur INTO @n; CLOSE cur; END; INSERT INTO audit (cnt) VALUES (@n); END")

	mustExec(t, e, "CALL p_blk_cur()")
	if got := auditInts(t, e, "audit"); len(got) != 1 || got[0] != "1" {
		t.Fatalf("nested block cursor: audit = %v, want [1]", got)
	}
}

// TestProcNestedBlockHandlerScope 块内处理器仅在块内生效，块退出后错误上抛。
func TestProcNestedBlockHandlerScope(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_blk_h BEGIN BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' INSERT INTO audit (cnt) VALUES (7); SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'inner'; END; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'outer-unhandled'; END")

	mustErr(t, e, "CALL p_blk_h()", "outer-unhandled")
	if got := auditInts(t, e, "audit"); len(got) != 1 || got[0] != "7" {
		t.Fatalf("nested block handler scope: audit = %v, want [7]", got)
	}
}

// TestProcNestedBlockIterate 块内 ITERATE 上抛给外层循环消费。
func TestProcNestedBlockIterate(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_blk_iter BEGIN DECLARE @i INT DEFAULT 0; WHILE @i < 3 DO SET @i = @i + 1; BEGIN IF @i = 2 THEN ITERATE; END IF; END; INSERT INTO audit (cnt) VALUES (@i); END WHILE; END")

	mustExec(t, e, "CALL p_blk_iter()")
	got := auditInts(t, e, "audit")
	want := []string{"1", "3"}
	if len(got) != len(want) {
		t.Fatalf("nested block ITERATE: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("nested block ITERATE: audit = %v, want %v", got, want)
		}
	}
}

// TestProcNestedBlockDepth 三层块嵌套与块内处理器逐层出栈。
func TestProcNestedBlockDepth(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE PROCEDURE p_blk_deep BEGIN BEGIN BEGIN DECLARE @x INT DEFAULT 5; INSERT INTO audit (cnt) VALUES (@x); END; INSERT INTO audit (cnt) VALUES (6); END; INSERT INTO audit (cnt) VALUES (7); END")

	mustExec(t, e, "CALL p_blk_deep()")
	got := auditInts(t, e, "audit")
	want := []string{"5", "6", "7"}
	if len(got) != len(want) {
		t.Fatalf("nested block depth: audit = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("nested block depth: audit = %v, want %v", got, want)
		}
	}
}

// TestProcNestedBlockParseErrors 未闭合块与 EOF 提前结束报创建期错误。
func TestProcNestedBlockParseErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustErr(t, e, "CREATE PROCEDURE p_blk_bad1 BEGIN BEGIN INSERT INTO users (id) VALUES (1); END", "invalid procedure body")
	mustErr(t, e, "CREATE PROCEDURE p_blk_bad2 BEGIN BEGIN END", "invalid procedure body")
}

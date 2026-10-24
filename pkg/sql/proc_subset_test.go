package sql_test

import (
	"testing"
)

// P1-3 存储过程受限子集补齐测试：变量声明/赋值、IF/ELSE、WHILE、
// 游标（DECLARE CURSOR/OPEN/FETCH/CLOSE）、IN/OUT/INOUT 参数，
// 以及与既有 REPEAT/ITERATE 控制流回归共存。
// 覆盖：正常执行、嵌套组合、语法错误拒绝。

func TestP13ProcDeclareIfWhile(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE results (v INT)")
	// 变量声明 + WHILE 累加 + IF/ELSE 分支。
	mustExec(t, e, `CREATE PROCEDURE p_calc (IN n INT) BEGIN
		DECLARE @i INT DEFAULT 0;
		DECLARE @sum INT DEFAULT 0;
		WHILE @i < @n DO
			SET @sum = @sum + @i;
			SET @i = @i + 1;
		END WHILE;
		IF @sum > 5 THEN
			INSERT INTO results (v) VALUES (@sum);
		ELSE
			INSERT INTO results (v) VALUES (-1);
		END IF;
	END`)
	mustExec(t, e, "CALL p_calc(3)") // sum=0+1+2=3 <= 5 -> -1
	mustExec(t, e, "CALL p_calc(5)") // sum=0+1+2+3+4=10 > 5 -> 10
	got := mustRows(t, e, "SELECT v FROM results ORDER BY v")
	if len(got) != 2 || got[0][0].I != -1 || got[1][0].I != 10 {
		t.Fatalf("IF/WHILE result mismatch: %v", got)
	}
}

func TestP13ProcWhileIterateNestedIf(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE nums (v INT)")
	// WHILE + ITERATE（跳过 2）+ IF 嵌套回归共存。
	mustExec(t, e, `CREATE PROCEDURE p_wi BEGIN
		DECLARE @i INT DEFAULT 0;
		WHILE @i < 4 DO
			SET @i = @i + 1;
			IF @i = 2 THEN
				ITERATE;
			END IF;
			INSERT INTO nums (v) VALUES (@i);
		END WHILE;
	END`)
	mustExec(t, e, "CALL p_wi")
	got := mustRows(t, e, "SELECT v FROM nums ORDER BY v")
	if len(got) != 3 || got[0][0].I != 1 || got[1][0].I != 3 || got[2][0].I != 4 {
		t.Fatalf("WHILE+ITERATE result mismatch: %v", got)
	}
}

func TestP13ProcCursorFetch(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE picked (id INT, name TEXT)")
	// 游标：DECLARE CURSOR FOR SELECT + OPEN + FETCH INTO + CLOSE。
	mustExec(t, e, `CREATE PROCEDURE p_cur BEGIN
		DECLARE c CURSOR FOR SELECT id, name FROM users ORDER BY id;
		DECLARE @id INT;
		DECLARE @nm TEXT;
		OPEN c;
		FETCH c INTO @id, @nm;
		INSERT INTO picked (id, name) VALUES (@id, @nm);
		FETCH c INTO @id, @nm;
		INSERT INTO picked (id, name) VALUES (@id, @nm);
		CLOSE c;
	END`)
	mustExec(t, e, "CALL p_cur")
	got := mustRows(t, e, "SELECT id, name FROM picked ORDER BY id")
	if len(got) != 2 || got[0][0].I != 1 || got[0][1].S != "alice" || got[1][0].I != 2 || got[1][1].S != "bob" {
		t.Fatalf("cursor fetch result mismatch: %v", got)
	}
}

func TestP13ProcCursorWithVarQuery(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE picked (id INT, name TEXT)")
	// 游标查询引用 IN 参数 @min：OPEN 时按当前 env 代入。
	mustExec(t, e, `CREATE PROCEDURE p_cur2 (IN min_id INT) BEGIN
		DECLARE c CURSOR FOR SELECT id, name FROM users WHERE id >= @min_id ORDER BY id;
		DECLARE @id INT;
		DECLARE @nm TEXT;
		OPEN c;
		FETCH c INTO @id, @nm;
		INSERT INTO picked (id, name) VALUES (@id, @nm);
		CLOSE c;
	END`)
	mustExec(t, e, "CALL p_cur2(2)")
	got := mustRows(t, e, "SELECT id, name FROM picked")
	if len(got) != 1 || got[0][0].I != 2 || got[0][1].S != "bob" {
		t.Fatalf("cursor with var query mismatch: %v", got)
	}
}

func TestP13ProcOutInoutParams(t *testing.T) {
	e := newEngine(t)
	// OUT/INOUT 参数：CALL 实参 @var，结束后回写。
	mustExec(t, e, `CREATE PROCEDURE p_io (IN a INT, OUT b INT, INOUT c INT) BEGIN
		SET @b = @a * 2;
		SET @c = @c + @a;
	END`)
	mustExec(t, e, "SET @inout = 5")
	mustExec(t, e, "CALL p_io(10, @out, @inout)")
	out := mustRows(t, e, "SELECT @out")
	if len(out) != 1 || out[0][0].I != 20 {
		t.Fatalf("OUT param not written back: %v", out)
	}
	ino := mustRows(t, e, "SELECT @inout")
	if len(ino) != 1 || ino[0][0].I != 15 {
		t.Fatalf("INOUT param not written back: %v", ino)
	}
}

func TestP13ProcInParamTakesVarValue(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_in (v INT)")
	mustExec(t, e, "CREATE PROCEDURE p_in (IN a INT) BEGIN INSERT INTO t_in (v) VALUES (@a); END")
	mustExec(t, e, "SET @x = 7")
	mustExec(t, e, "CALL p_in(@x)")
	got := mustRows(t, e, "SELECT v FROM t_in")
	if len(got) != 1 || got[0][0].I != 7 {
		t.Fatalf("IN param should take current var value: %v", got)
	}
}

func TestP13ProcRepeatIterateRegression(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE r (v INT)")
	// REPEAT 先执行后判断 + ITERATE 跳过 + IF 嵌套。
	mustExec(t, e, `CREATE PROCEDURE p_rpt BEGIN
		DECLARE @i INT DEFAULT 0;
		REPEAT
			SET @i = @i + 1;
			IF @i = 2 THEN
				ITERATE;
			END IF;
			INSERT INTO r (v) VALUES (@i);
		UNTIL @i >= 4 END REPEAT;
	END`)
	mustExec(t, e, "CALL p_rpt")
	got := mustRows(t, e, "SELECT v FROM r ORDER BY v")
	if len(got) != 3 || got[0][0].I != 1 || got[1][0].I != 3 || got[2][0].I != 4 {
		t.Fatalf("REPEAT+ITERATE regression mismatch: %v", got)
	}
}

func TestP13ProcCursorSyntaxErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	// 未声明游标即 OPEN：创建期拒绝。
	mustErr(t, e, `CREATE PROCEDURE p_bad1 BEGIN OPEN c; END`, "unknown cursor")
	// 未声明游标即 FETCH：创建期拒绝。
	mustErr(t, e, `CREATE PROCEDURE p_bad2 BEGIN FETCH c INTO @a; END`, "unknown cursor")
	// 未声明游标即 CLOSE：创建期拒绝。
	mustErr(t, e, `CREATE PROCEDURE p_bad3 BEGIN CLOSE c; END`, "unknown cursor")
	// 游标查询非 SELECT：创建期拒绝。
	mustErr(t, e, `CREATE PROCEDURE p_bad4 BEGIN
		DECLARE c CURSOR FOR INSERT INTO users (id, name, age) VALUES (9, 'x', 9);
		OPEN c;
	END`, "only supports SELECT")
	// FETCH INTO 缺变量：创建期拒绝。
	mustErr(t, e, `CREATE PROCEDURE p_bad5 BEGIN
		DECLARE c CURSOR FOR SELECT id FROM users;
		OPEN c;
		FETCH c INTO;
	END`, "FETCH INTO")
	// FETCH 变量数不匹配：运行期拒绝（创建期不校验列数）。
	mustExec(t, e, `CREATE PROCEDURE p_bad6 BEGIN
		DECLARE c CURSOR FOR SELECT id, name, age FROM users;
		DECLARE @a INT;
		OPEN c;
		FETCH c INTO @a;
		CLOSE c;
	END`)
	mustErr(t, e, "CALL p_bad6", "variable count mismatch")
	// OUT 实参非 @var：调用期拒绝。
	mustExec(t, e, "CREATE PROCEDURE p_out (OUT b INT) BEGIN SET @b = 1; END")
	mustErr(t, e, "CALL p_out(1)", "requires a @variable argument")
}

func TestP13ProcCursorFetchOutOfRows(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE one (id INT)")
	mustExec(t, e, "INSERT INTO one (id) VALUES (1)")
	// FETCH 越界（行集仅 1 行，FETCH 两次）：运行期报错。
	mustExec(t, e, `CREATE PROCEDURE p_oob BEGIN
		DECLARE c CURSOR FOR SELECT id FROM one;
		DECLARE @a INT;
		OPEN c;
		FETCH c INTO @a;
		FETCH c INTO @a;
		CLOSE c;
	END`)
	mustErr(t, e, "CALL p_oob", "fetch out of rows")
}

func TestP13ProcCursorDuplicateDecl(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE d (id INT)")
	// 重复声明游标：创建期拒绝。
	mustErr(t, e, `CREATE PROCEDURE p_dup BEGIN
		DECLARE c CURSOR FOR SELECT id FROM d;
		DECLARE c CURSOR FOR SELECT id FROM d;
	END`, "duplicate cursor")
}

func TestP13ProcValueKind(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE vk (v INT)")
	// 确认 FETCH INTO 落库值为 INT 数值（非 VAR 占位）。
	mustExec(t, e, "INSERT INTO vk (v) VALUES (42)")
	mustExec(t, e, `CREATE PROCEDURE p_vk BEGIN
		DECLARE c CURSOR FOR SELECT v FROM vk;
		DECLARE @x INT;
		OPEN c;
		FETCH c INTO @x;
		INSERT INTO vk2 (v) VALUES (@x);
		CLOSE c;
	END`)
	mustExec(t, e, "CREATE TABLE vk2 (v INT)")
	mustExec(t, e, "CALL p_vk")
	got := mustRows(t, e, "SELECT v FROM vk2")
	if len(got) != 1 || got[0][0].I != 42 {
		t.Fatalf("cursor value kind mismatch: %v", got)
	}
}

package db_test

// P0-2 存储过程子集增强集成测试：
//   - IF/ELSIF/ELSE 条件分支 + SET @x := <expr> 局部变量赋值；
//   - WHILE LOOP / DO..END WHILE 循环 + EXIT / LEAVE WHEN 退出；
//   - FOR @i IN .. LOOP（含 TO 别名）循环 + DECLARE 局部变量；
//   - CASE 简单匹配与搜索式 CASE 语句；
//   - 递归调用（countdown 自嵌套）、递归深度上限防护（不崩栈）；
//   - 多层 CALL 调用（跨过程逐层传参）；
//   - 循环 + 条件 + 递归组合；CALL 不存在过程/参数个数不匹配报错。

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// execErr 执行 SQL 并返回错误（期望失败场景）。
func execErr(t *testing.T, d *db.DB, stmt string) error {
	t.Helper()
	_, err := d.SQL.Execute(stmt)
	return err
}

// TestP02ProcIf 存储过程 IF/ELSIF/ELSE 分支与局部变量 := 赋值。
func TestP02ProcIf(t *testing.T) {
	t.Parallel()
	d, _ := openNode(t, "node")
	defer d.Close()
	mustSQL(t, d, "CREATE TABLE t (a INT)")
	mustSQL(t, d, `CREATE PROCEDURE grade(IN score INT) BEGIN
		IF score >= 90 THEN INSERT INTO t VALUES (1);
		ELSIF score >= 80 THEN INSERT INTO t VALUES (2);
		ELSIF score >= 70 THEN INSERT INTO t VALUES (3);
		ELSE INSERT INTO t VALUES (4);
		END IF;
	END`)
	mustSQL(t, d, "CALL grade(95)")
	mustSQL(t, d, "CALL grade(85)")
	mustSQL(t, d, "CALL grade(75)")
	mustSQL(t, d, "CALL grade(60)")
	wantIDs(t, d, "SELECT a FROM t ORDER BY a", 1, 2, 3, 4)
}

// TestP02ProcWhileExit 存储过程 WHILE LOOP + EXIT WHEN + LEAVE 退出。
func TestP02ProcWhileExit(t *testing.T) {
	t.Parallel()
	d, _ := openNode(t, "node")
	defer d.Close()
	mustSQL(t, d, "CREATE TABLE t (a INT)")
	mustSQL(t, d, `CREATE PROCEDURE cnt(IN n INT) BEGIN
		SET @i := 0;
		WHILE @i < n LOOP
			SET @i := @i + 1;
			IF @i > 3 THEN EXIT; END IF;
			INSERT INTO t VALUES (@i);
		END LOOP;
	END`)
	mustSQL(t, d, "CALL cnt(10)")
	wantIDs(t, d, "SELECT a FROM t ORDER BY a", 1, 2, 3)
	mustSQL(t, d, `CREATE PROCEDURE cnt2(IN n INT) BEGIN
		SET @i := 0;
		WHILE @i < n DO
			SET @i := @i + 1;
			LEAVE WHEN @i >= 2;
		END WHILE;
		SET @r := 100 + @i;
		INSERT INTO t VALUES (@r);
	END`)
	mustSQL(t, d, "CALL cnt2(10)")
	wantIDs(t, d, "SELECT a FROM t WHERE a > 100", 102)
}

// TestP02ProcFor 存储过程 FOR IN .. LOOP 循环（含 TO 别名与 DECLARE 局部变量）。
func TestP02ProcFor(t *testing.T) {
	t.Parallel()
	d, _ := openNode(t, "node")
	defer d.Close()
	mustSQL(t, d, "CREATE TABLE t (a INT, b INT)")
	mustSQL(t, d, `CREATE PROCEDURE sum1(IN n INT) BEGIN
		DECLARE @s INT DEFAULT 0;
		FOR @i IN 1 .. n LOOP
			SET @s := @s + @i;
		END LOOP;
		INSERT INTO t VALUES (@s, @i);
	END`)
	mustSQL(t, d, "CALL sum1(10)")
	res := mustSQL(t, d, "SELECT a, b FROM t")
	if len(res.Rows) != 1 {
		t.Fatalf("FOR sum rows mismatch: got %d want 1", len(res.Rows))
	}
	if res.Rows[0][0].I != 55 {
		t.Fatalf("FOR sum result mismatch: got %d want 55", res.Rows[0][0].I)
	}
	if res.Rows[0][1].I != 10 {
		t.Fatalf("FOR loop var leak mismatch: got %d want 10", res.Rows[0][1].I)
	}
	mustSQL(t, d, `CREATE PROCEDURE sum2() BEGIN
		DECLARE @s INT := 0;
		FOR @j IN 1 TO 5 LOOP
			SET @s := @s + @j;
		END LOOP;
		INSERT INTO t VALUES (@s, @j);
	END`)
	mustSQL(t, d, "CALL sum2()")
	res = mustSQL(t, d, "SELECT a, b FROM t WHERE a = 15")
	if len(res.Rows) != 1 || res.Rows[0][1].I != 5 {
		t.Fatalf("FOR TO rows mismatch: got %+v want single (15,5)", res.Rows)
	}
}

// TestP02ProcCase 存储过程 CASE 简单匹配与搜索式 CASE。
func TestP02ProcCase(t *testing.T) {
	t.Parallel()
	d, _ := openNode(t, "node")
	defer d.Close()
	mustSQL(t, d, "CREATE TABLE t (a INT)")
	mustSQL(t, d, `CREATE PROCEDURE pick(IN x INT) BEGIN
		CASE x
			WHEN 1 THEN INSERT INTO t VALUES (10);
			WHEN 2 THEN INSERT INTO t VALUES (20);
			ELSE INSERT INTO t VALUES (99);
		END CASE;
	END`)
	mustSQL(t, d, "CALL pick(1)")
	mustSQL(t, d, "CALL pick(2)")
	mustSQL(t, d, "CALL pick(9)")
	mustSQL(t, d, `CREATE PROCEDURE pick2(IN x INT) BEGIN
		CASE
			WHEN x < 0 THEN INSERT INTO t VALUES (1);
			WHEN x = 0 THEN INSERT INTO t VALUES (2);
			ELSE INSERT INTO t VALUES (3);
		END CASE;
	END`)
	mustSQL(t, d, "CALL pick2(-5)")
	mustSQL(t, d, "CALL pick2(0)")
	mustSQL(t, d, "CALL pick2(5)")
	wantIDs(t, d, "SELECT a FROM t ORDER BY a", 1, 2, 3, 10, 20, 99)
}

// TestP02ProcRecursion 存储过程递归调用（countdown 自嵌套）。
func TestP02ProcRecursion(t *testing.T) {
	t.Parallel()
	d, _ := openNode(t, "node")
	defer d.Close()
	mustSQL(t, d, "CREATE TABLE t (a INT)")
	mustSQL(t, d, `CREATE PROCEDURE countdown(IN n INT) BEGIN
		SET @v := n;
		INSERT INTO t VALUES (@v);
		IF n > 1 THEN CALL countdown(n - 1); END IF;
	END`)
	mustSQL(t, d, "CALL countdown(3)")
	wantIDs(t, d, "SELECT a FROM t ORDER BY a", 1, 2, 3)
}

// TestP02ProcRecursionDepth 递归深度上限：超过 maxProcDepth 报错且不崩栈。
func TestP02ProcRecursionDepth(t *testing.T) {
	t.Parallel()
	d, _ := openNode(t, "node")
	defer d.Close()
	mustSQL(t, d, `CREATE PROCEDURE deep(IN n INT) BEGIN
		CALL deep(n + 1);
	END`)
	if err := execErr(t, d, "CALL deep(0)"); err == nil {
		t.Fatalf("expected recursion depth error")
	}
	mustSQL(t, d, `CREATE PROCEDURE okp() BEGIN
		SET @x := 1;
	END`)
	mustSQL(t, d, "CALL okp()")
	mustSQL(t, d, "DROP PROCEDURE deep")
	mustSQL(t, d, "CALL okp()")
}

// TestP02ProcMultiCall 多层调用：outer_p -> mul2 -> add3 跨过程逐层传参。
func TestP02ProcMultiCall(t *testing.T) {
	t.Parallel()
	d, _ := openNode(t, "node")
	defer d.Close()
	mustSQL(t, d, "CREATE TABLE t (a INT)")
	mustSQL(t, d, `CREATE PROCEDURE add3(IN x INT) BEGIN
		SET @r := x + 3;
		INSERT INTO t VALUES (@r);
	END`)
	mustSQL(t, d, `CREATE PROCEDURE mul2(IN x INT) BEGIN
		CALL add3(x);
		SET @r := x * 2;
		INSERT INTO t VALUES (@r);
	END`)
	mustSQL(t, d, `CREATE PROCEDURE outer_p(IN x INT) BEGIN
		CALL mul2(x);
		SET @r := x + 100;
		INSERT INTO t VALUES (@r);
	END`)
	mustSQL(t, d, "CALL outer_p(10)")
	wantIDs(t, d, "SELECT a FROM t ORDER BY a", 13, 20, 110)
}

// TestP02ProcCombined 循环 + 条件 + 递归调用组合（外层 WHILE 驱动 down 递归链）。
func TestP02ProcCombined(t *testing.T) {
	t.Parallel()
	d, _ := openNode(t, "node")
	defer d.Close()
	mustSQL(t, d, "CREATE TABLE t (a INT)")
	mustSQL(t, d, `CREATE PROCEDURE down(IN n INT) BEGIN
		IF n > 1 THEN CALL down(n - 1); END IF;
	END`)
	mustSQL(t, d, `CREATE PROCEDURE combo(IN k INT) BEGIN
		SET @i := 0;
		WHILE @i < k LOOP
			SET @i := @i + 1;
			CALL down(@i);
			SET @r := @i * 10;
			INSERT INTO t VALUES (@r);
		END LOOP;
	END`)
	mustSQL(t, d, "CALL combo(3)")
	wantIDs(t, d, "SELECT a FROM t ORDER BY a", 10, 20, 30)
}

// TestP02ProcErrorProcNotExists CALL 不存在过程报错。
func TestP02ProcErrorProcNotExists(t *testing.T) {
	t.Parallel()
	d, _ := openNode(t, "node")
	defer d.Close()
	if err := execErr(t, d, "CALL nope(1)"); err == nil {
		t.Fatalf("expected procedure not exists error")
	}
}

// TestP02ProcErrorArgCount 参数个数不匹配报错。
func TestP02ProcErrorArgCount(t *testing.T) {
	t.Parallel()
	d, _ := openNode(t, "node")
	defer d.Close()
	mustSQL(t, d, "CREATE TABLE t (x INT)")
	mustSQL(t, d, `CREATE PROCEDURE p1(IN a INT, IN b INT) BEGIN
		SET @s := a + b;
		INSERT INTO t VALUES (@s);
	END`)
	if err := execErr(t, d, "CALL p1(1)"); err == nil {
		t.Fatalf("expected arg count error")
	}
}

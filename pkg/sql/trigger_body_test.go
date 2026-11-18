package sql_test

import (
	"testing"
)

// v1.19.0 D3 触发器兼容能力测试，覆盖三项新增能力：
//   - 触发器体 BEGIN…END 复合块（复用过程体解释器：DECLARE/IF/WHILE/LOOP/
//     HANDLER/SIGNAL/CALL 存储过程/PREPARE 动态 SQL）；
//   - BEFORE INSERT / BEFORE UPDATE 的 SET NEW.col = expr 回写待写入行；
//   - 同表同事件同时机组内的 FOLLOWS / PRECEDES 执行顺序，未声明依赖按创建顺序。
func TestD3TriggerBlockDeclareIf(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit_d3a (cnt INT)")
	mustExec(t, e, "INSERT INTO audit_d3a (cnt) VALUES (0)")

	body := "BEGIN\n" +
		"  DECLARE step INT;\n" +
		"  SET step = 2;\n" +
		"  IF NEW.age >= 30 THEN\n" +
		"    UPDATE audit_d3a SET cnt = cnt + step;\n" +
		"  END IF;\n" +
		"END"
	mustExec(t, e, "CREATE TRIGGER tr_d3a AFTER INSERT ON users FOR EACH ROW "+body)

	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (10, 'x', 30)")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (11, 'y', 20)")
	if got := mustRows(t, e, "SELECT cnt FROM audit_d3a")[0][0].String(); got != "2" {
		t.Fatalf("复合块 DECLARE+IF: cnt = %s, want 2", got)
	}
}

func TestD3TriggerBlockWhileLoop(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit_d3b (cnt INT)")
	mustExec(t, e, "INSERT INTO audit_d3b (cnt) VALUES (0)")

	body := "BEGIN\n" +
		"  DECLARE i INT;\n" +
		"  DECLARE total INT;\n" +
		"  SET i = 0;\n" +
		"  SET total = 0;\n" +
		"  WHILE i < 3 DO\n" +
		"    SET i = i + 1;\n" +
		"    SET total = total + i;\n" +
		"  END WHILE;\n" +
		"  UPDATE audit_d3b SET cnt = total;\n" +
		"END"
	mustExec(t, e, "CREATE TRIGGER tr_d3b AFTER INSERT ON users FOR EACH ROW "+body)
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (20, 'z', 40)")
	if got := mustRows(t, e, "SELECT cnt FROM audit_d3b")[0][0].String(); got != "6" {
		t.Fatalf("复合块 WHILE: cnt = %s, want 6", got)
	}
}

func TestD3TriggerBlockHandlerSignal(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit_d3c (cnt INT)")
	mustExec(t, e, "INSERT INTO audit_d3c (cnt) VALUES (0)")

	body := "BEGIN\n" +
		"  DECLARE flag INT;\n" +
		"  DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' SET flag = 1;\n" +
		"  SET flag = 0;\n" +
		"  SIGNAL SQLSTATE '45000';\n" +
		"  UPDATE audit_d3c SET cnt = flag + 10;\n" +
		"END"
	mustExec(t, e, "CREATE TRIGGER tr_d3c AFTER INSERT ON users FOR EACH ROW "+body)
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (21, 'h', 40)")
	if got := mustRows(t, e, "SELECT cnt FROM audit_d3c")[0][0].String(); got != "11" {
		t.Fatalf("复合块 HANDLER+CONTINUE: cnt = %s, want 11", got)
	}
}

func TestD3TriggerBlockCallProcedure(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit_d3e (cnt INT)")
	mustExec(t, e, "INSERT INTO audit_d3e (cnt) VALUES (0)")
	mustExec(t, e, "CREATE PROCEDURE bump_d3e() BEGIN UPDATE audit_d3e SET cnt = cnt + 1; END")

	body := "BEGIN\n" +
		"  CALL bump_d3e();\n" +
		"END"
	mustExec(t, e, "CREATE TRIGGER tr_d3e AFTER INSERT ON users FOR EACH ROW "+body)
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (22, 'c', 40)")
	if got := mustRows(t, e, "SELECT cnt FROM audit_d3e")[0][0].String(); got != "1" {
		t.Fatalf("复合块 CALL: cnt = %s, want 1", got)
	}
}

func TestD3TriggerBlockPrepareDynamicSQL(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit_d3f (cnt INT)")
	mustExec(t, e, "INSERT INTO audit_d3f (cnt) VALUES (0)")

	body := "BEGIN\n" +
		"  PREPARE s1 FROM 'UPDATE audit_d3f SET cnt = 42';\n" +
		"  EXECUTE s1;\n" +
		"  DEALLOCATE PREPARE s1;\n" +
		"END"
	mustExec(t, e, "CREATE TRIGGER tr_d3f AFTER INSERT ON users FOR EACH ROW "+body)
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (23, 'd', 40)")
	if got := mustRows(t, e, "SELECT cnt FROM audit_d3f")[0][0].String(); got != "42" {
		t.Fatalf("复合块 PREPARE/EXECUTE: cnt = %s, want 42", got)
	}
}

func TestD3TriggerBeforeInsertSetNew(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE d3t (id INT, amount INT, note TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "CREATE TABLE audit_d3g (id INT, amount INT)")

	before := "BEGIN\n" +
		"  DECLARE lim INT;\n" +
		"  SET lim = 50;\n" +
		"  IF NEW.amount > lim THEN\n" +
		"    SET NEW.amount = lim;\n" +
		"  END IF;\n" +
		"  SET NEW.note = 'capped';\n" +
		"END"
	mustExec(t, e, "CREATE TRIGGER tr_d3g_b BEFORE INSERT ON d3t FOR EACH ROW "+before)
	mustExec(t, e, "CREATE TRIGGER tr_d3g_a AFTER INSERT ON d3t FOR EACH ROW INSERT INTO audit_d3g (id, amount) VALUES (NEW.id, NEW.amount)")

	mustExec(t, e, "INSERT INTO d3t (id, amount, note) VALUES (1, 100, 'raw')")
	rows := mustRows(t, e, "SELECT amount, note FROM d3t WHERE id = 1")
	if got := rows[0][0].String(); got != "50" {
		t.Fatalf("SET NEW.amount 回写: amount = %s, want 50", got)
	}
	if got := rows[0][1].String(); got != "capped" {
		t.Fatalf("SET NEW.note 回写: note = %s, want capped", got)
	}
	// AFTER 触发器应看到回写后的 NEW 行。
	if got := mustRows(t, e, "SELECT amount FROM audit_d3g")[0][0].String(); got != "50" {
		t.Fatalf("AFTER 触发器观察 NEW 行: amount = %s, want 50", got)
	}
}

func TestD3TriggerBeforeUpdateSetNew(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE d3u (id INT, amount INT, PRIMARY KEY (id))")
	mustExec(t, e, "CREATE TABLE audit_d3h (cnt INT)")
	mustExec(t, e, "INSERT INTO audit_d3h (cnt) VALUES (0)")
	mustExec(t, e, "INSERT INTO d3u (id, amount) VALUES (1, 10)")

	body := "BEGIN\n" +
		"  IF NEW.amount < 0 THEN\n" +
		"    SET NEW.amount = 0;\n" +
		"  END IF;\n" +
		"  UPDATE audit_d3h SET cnt = cnt + 1;\n" +
		"END"
	mustExec(t, e, "CREATE TRIGGER tr_d3h BEFORE UPDATE ON d3u FOR EACH ROW "+body)
	mustExec(t, e, "UPDATE d3u SET amount = -5 WHERE id = 1")

	if got := mustRows(t, e, "SELECT amount FROM d3u WHERE id = 1")[0][0].String(); got != "0" {
		t.Fatalf("BEFORE UPDATE SET NEW.amount 回写: amount = %s, want 0", got)
	}
	if got := mustRows(t, e, "SELECT cnt FROM audit_d3h")[0][0].String(); got != "1" {
		t.Fatalf("BEFORE UPDATE 只执行一次: cnt = %s, want 1", got)
	}
}

func TestD3TriggerFollowsPrecedes(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE src_d3o (id INT, PRIMARY KEY (id))")
	// seq 自增列记录写入先后，用于断言触发器真实执行顺序。
	mustExec(t, e, "CREATE TABLE audit_d3o (seq INT AUTO_INCREMENT, nm TEXT, PRIMARY KEY (seq))")

	mustExec(t, e, "CREATE TRIGGER o_a AFTER INSERT ON src_d3o FOR EACH ROW INSERT INTO audit_d3o (nm) VALUES ('a')")
	mustExec(t, e, "CREATE TRIGGER o_b AFTER INSERT ON src_d3o FOR EACH ROW INSERT INTO audit_d3o (nm) VALUES ('b')")
	mustExec(t, e, "CREATE TRIGGER o_c AFTER INSERT ON src_d3o FOR EACH ROW FOLLOWS o_a INSERT INTO audit_d3o (nm) VALUES ('c')")

	mustExec(t, e, "INSERT INTO src_d3o (id) VALUES (1)")
	rows := mustRows(t, e, "SELECT nm FROM audit_d3o ORDER BY seq")
	if len(rows) != 3 {
		t.Fatalf("FOLLOWS: want 3 rows, got %d", len(rows))
	}
	// FOLLOWS 约束：c 必须出现在 a 之后；无约束时稳定保持创建顺序。
	posA, posC := -1, -1
	for i, r := range rows {
		switch r[0].String() {
		case "a":
			posA = i
		case "c":
			posC = i
		}
	}
	if posA < 0 || posC < 0 || posC < posA {
		t.Fatalf("FOLLOWS 约束未满足: 顺序 %v (a@%d, c@%d)", []string{rows[0][0].String(), rows[1][0].String(), rows[2][0].String()}, posA, posC)
	}
}

func TestD3TriggerPrecedes(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE src_d3p (id INT, PRIMARY KEY (id))")
	mustExec(t, e, "CREATE TABLE audit_d3p (seq INT AUTO_INCREMENT, nm TEXT, PRIMARY KEY (seq))")

	// 后创建的 p_b 通过 PRECEDES 强制排到 p_a 之前（创建顺序兜底被覆盖）。
	mustExec(t, e, "CREATE TRIGGER p_a AFTER INSERT ON src_d3p FOR EACH ROW INSERT INTO audit_d3p (nm) VALUES ('a')")
	mustExec(t, e, "CREATE TRIGGER p_b AFTER INSERT ON src_d3p FOR EACH ROW PRECEDES p_a INSERT INTO audit_d3p (nm) VALUES ('b')")
	mustExec(t, e, "CREATE TRIGGER p_c AFTER INSERT ON src_d3p FOR EACH ROW PRECEDES p_b INSERT INTO audit_d3p (nm) VALUES ('c')")

	mustExec(t, e, "INSERT INTO src_d3p (id) VALUES (1)")
	rows := mustRows(t, e, "SELECT nm FROM audit_d3p ORDER BY seq")
	want := []string{"c", "b", "a"}
	if len(rows) != len(want) {
		t.Fatalf("PRECEDES: want %d rows, got %d", len(want), len(rows))
	}
	for i, w := range want {
		if rows[i][0].String() != w {
			t.Fatalf("PRECEDES 执行顺序第 %d 位: got %s, want %s", i+1, rows[i][0].String(), w)
		}
	}
}

func TestD3TriggerCreateOrderFallback(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE src_d3q (id INT, PRIMARY KEY (id))")
	mustExec(t, e, "CREATE TABLE audit_d3q (seq INT AUTO_INCREMENT, nm TEXT, PRIMARY KEY (seq))")

	mustExec(t, e, "CREATE TRIGGER q_x AFTER INSERT ON src_d3q FOR EACH ROW INSERT INTO audit_d3q (nm) VALUES ('x')")
	mustExec(t, e, "CREATE TRIGGER q_y AFTER INSERT ON src_d3q FOR EACH ROW INSERT INTO audit_d3q (nm) VALUES ('y')")
	mustExec(t, e, "CREATE TRIGGER q_z AFTER INSERT ON src_d3q FOR EACH ROW INSERT INTO audit_d3q (nm) VALUES ('z')")

	mustExec(t, e, "INSERT INTO src_d3q (id) VALUES (1)")
	rows := mustRows(t, e, "SELECT nm FROM audit_d3q ORDER BY seq")
	want := []string{"x", "y", "z"}
	for i, w := range want {
		if got := rows[i][0].String(); got != w {
			t.Fatalf("创建顺序兜底第 %d 位: got %s, want %s", i+1, got, w)
		}
	}
}

func TestD3TriggerOrderAndBodyErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE other_d3r (id INT, PRIMARY KEY (id))")
	mustExec(t, e, "CREATE TABLE audit_d3r (nm TEXT)")
	mustExec(t, e, "CREATE TRIGGER r_a AFTER INSERT ON users FOR EACH ROW INSERT INTO audit_d3r (nm) VALUES ('a')")
	mustExec(t, e, "CREATE TRIGGER r_o AFTER INSERT ON other_d3r FOR EACH ROW INSERT INTO audit_d3r (nm) VALUES ('o')")

	// FOLLOWS 与 PRECEDES 互斥。
	mustErr(t, e, "CREATE TRIGGER r_e1 AFTER INSERT ON users FOR EACH ROW FOLLOWS r_a PRECEDES r_a INSERT INTO audit_d3r (nm) VALUES ('e')",
		"FOLLOWS and PRECEDES are mutually exclusive")
	// 自引用。
	mustErr(t, e, "CREATE TRIGGER r_e2 AFTER INSERT ON users FOR EACH ROW FOLLOWS r_e2 INSERT INTO audit_d3r (nm) VALUES ('e')",
		"cannot reference the trigger itself")
	// 依赖触发器不存在。
	mustErr(t, e, "CREATE TRIGGER r_e3 AFTER INSERT ON users FOR EACH ROW PRECEDES nope_d3r INSERT INTO audit_d3r (nm) VALUES ('e')",
		"trigger not exists")
	// 依赖触发器须同表同事件同时机。
	mustErr(t, e, "CREATE TRIGGER r_e4 AFTER INSERT ON users FOR EACH ROW FOLLOWS r_o INSERT INTO audit_d3r (nm) VALUES ('e')",
		"must share the same table, timing and event")
	// 复合块体语法错误。
	mustErr(t, e, "CREATE TRIGGER r_e5 AFTER INSERT ON users FOR EACH ROW BEGIN SET x = ; END",
		"invalid trigger body")
}

func TestD3TriggerSetNewErrors(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE d3v (id INT, amount INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO d3v (id, amount) VALUES (1, 10)")

	// OLD 行只读：执行期拒绝。
	mustExec(t, e, "CREATE TRIGGER tr_d3v_old BEFORE UPDATE ON d3v FOR EACH ROW BEGIN SET OLD.amount = 1; END")
	mustErr(t, e, "UPDATE d3v SET amount = 20 WHERE id = 1", "OLD row is read-only")

	// 未知列：创建期拒绝。
	mustErr(t, e, "CREATE TRIGGER tr_d3v_col BEFORE INSERT ON d3v FOR EACH ROW BEGIN SET NEW.nocol = 1; END",
		"unknown column")
}

func TestD3TriggerSingleStmtBodyUnchanged(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit_d3s (cnt INT)")
	mustExec(t, e, "INSERT INTO audit_d3s (cnt) VALUES (0)")

	// 既有单语句体路径保持不变（不进入复合块解释器）。
	mustExec(t, e, "CREATE TRIGGER tr_d3s AFTER INSERT ON users FOR EACH ROW UPDATE audit_d3s SET cnt = 100")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (30, 's', 40)")
	if got := mustRows(t, e, "SELECT cnt FROM audit_d3s")[0][0].String(); got != "100" {
		t.Fatalf("单语句体: cnt = %s, want 100", got)
	}
	// 单语句体的行级 NEW 引用仍生效。
	mustExec(t, e, "CREATE TABLE audit_d3s2 (id INT)")
	mustExec(t, e, "CREATE TRIGGER tr_d3s2 BEFORE INSERT ON users FOR EACH ROW INSERT INTO audit_d3s2 (id) VALUES (NEW.id)")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (31, 't', 40)")
	if got := mustRows(t, e, "SELECT id FROM audit_d3s2")[0][0].String(); got != "31" {
		t.Fatalf("单语句体 NEW 引用: id = %s, want 31", got)
	}
}

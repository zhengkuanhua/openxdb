package sql_test

import (
	"strings"
	"testing"
)

// M15-2 触发器测试：CREATE/DROP TRIGGER、BEFORE/AFTER INSERT/UPDATE/DELETE、
// FOR EACH ROW（语句级）、简单 SQL 体（UPDATE 条件更新 / INSERT 单行）、
// 元数据持久化、SHOW TRIGGERS、DML 自动触发、与物化视图/虚拟视图共存、
// 递归防护、语法错误与非法事件。
// 支持范围说明：触发器体为简单 SQL（UPDATE 字面量赋值 / INSERT 单行），
// 不支持算术表达式（如 cnt+1），文档化限制见 docs/T28_m15_trigger.md。

func TestM15CreateShowDropTrigger(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")

	mustExec(t, e, "CREATE TRIGGER tr_ins AFTER INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 100")

	// SHOW TRIGGERS 列出触发器
	rows := mustRows(t, e, "SHOW TRIGGERS")
	if len(rows) != 1 {
		t.Fatalf("SHOW TRIGGERS: want 1 row, got %d", len(rows))
	}
	if got := rows[0][0].String(); got != "tr_ins" {
		t.Fatalf("SHOW TRIGGERS trigger name: want tr_ins, got %s", got)
	}
	if got := rows[0][1].String(); got != "INSERT" {
		t.Fatalf("SHOW TRIGGERS event: want INSERT, got %s", got)
	}
	if got := rows[0][2].String(); got != "users" {
		t.Fatalf("SHOW TRIGGERS table: want users, got %s", got)
	}
	if got := rows[0][3].String(); got != "AFTER" {
		t.Fatalf("SHOW TRIGGERS timing: want AFTER, got %s", got)
	}

	// DROP 后 SHOW 为空
	mustExec(t, e, "DROP TRIGGER tr_ins")
	if rows := mustRows(t, e, "SHOW TRIGGERS"); len(rows) != 0 {
		t.Fatalf("after DROP TRIGGER: want 0 rows, got %d", len(rows))
	}
	// 删除后不再触发：先手动复位，再插入，cnt 应保持不变
	mustExec(t, e, "UPDATE audit SET cnt = 0")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String()
	if got != "0" {
		t.Fatalf("trigger should not fire after DROP: cnt = %s, want 0", got)
	}
}

func TestM15AfterInsertTriggerFires(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")
	// 触发器体为 UPDATE 单行计数：源表插入时置位审计值。
	mustExec(t, e, "CREATE TRIGGER tr_ins AFTER INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 100")

	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "100" {
		t.Fatalf("AFTER INSERT trigger: cnt = %s, want 100", got)
	}
	// 复位后批量插入（一次语句两行）仍按语句级触发一次：cnt 置 100。
	mustExec(t, e, "UPDATE audit SET cnt = 0")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (5, 'erin', 22), (6, 'frank', 28)")
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "100" {
		t.Fatalf("statement-level trigger: cnt = %s, want 100", got)
	}
}

func TestM15BeforeInsertTriggerRunsBeforeDML(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE pre (flag INT)")
	mustExec(t, e, "INSERT INTO pre (flag) VALUES (0)")
	// BEFORE 触发器先置位标志列，主 INSERT 提交后可见。
	mustExec(t, e, "CREATE TRIGGER tr_pre BEFORE INSERT ON users FOR EACH ROW UPDATE pre SET flag = 1")

	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (7, 'grace', 31)")
	if got := mustRows(t, e, "SELECT flag FROM pre")[0][0].String(); got != "1" {
		t.Fatalf("BEFORE INSERT trigger: flag = %s, want 1", got)
	}
}

func TestM15BeforeTriggerFailureBlocksDML(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")
	// 体在创建时合法（SET 列存在），但执行期 WHERE 引用不存在列而失败：
	// BEFORE 失败应阻断主 DML。
	mustExec(t, e, "CREATE TRIGGER tr_bad BEFORE INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 1 WHERE nope = 1")

	mustErr(t, e, "INSERT INTO users (id, name, age) VALUES (9, 'henry', 29)", "trigger tr_bad failed")
	// 主 DML 未生效
	if rows := mustRows(t, e, "SELECT id FROM users WHERE id = 9"); len(rows) != 0 {
		t.Fatalf("BEFORE trigger failure should block DML, found row 9")
	}
}

func TestM15TriggerOnUpdate(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")
	mustExec(t, e, "CREATE TRIGGER tr_upd AFTER UPDATE ON users FOR EACH ROW UPDATE audit SET cnt = 100")

	mustExec(t, e, "UPDATE users SET age = 31 WHERE id = 1")
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "100" {
		t.Fatalf("AFTER UPDATE trigger: cnt = %s, want 100", got)
	}
	// 条件未命中（0 行受影响）时不触发：手动复位后执行 0 行 UPDATE
	mustExec(t, e, "UPDATE audit SET cnt = 0")
	mustExec(t, e, "UPDATE users SET age = 32 WHERE id = 999")
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "0" {
		t.Fatalf("UPDATE no-match should not fire: cnt = %s, want 0", got)
	}
}

func TestM15TriggerOnDelete(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")
	mustExec(t, e, "CREATE TRIGGER tr_del AFTER DELETE ON users FOR EACH ROW UPDATE audit SET cnt = 100")

	mustExec(t, e, "DELETE FROM users WHERE id = 3")
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "100" {
		t.Fatalf("AFTER DELETE trigger: cnt = %s, want 100", got)
	}
}

func TestM15MultipleTriggersSameEvent(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (a INT, b INT)")
	mustExec(t, e, "INSERT INTO audit (a, b) VALUES (0, 0)")
	mustExec(t, e, "CREATE TRIGGER tr_a AFTER INSERT ON users FOR EACH ROW UPDATE audit SET a = 100")
	mustExec(t, e, "CREATE TRIGGER tr_b AFTER INSERT ON users FOR EACH ROW UPDATE audit SET b = 200")

	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	row := mustRows(t, e, "SELECT a, b FROM audit")[0]
	if row[0].String() != "100" || row[1].String() != "200" {
		t.Fatalf("multiple triggers: got a=%s b=%s, want 100 200", row[0].String(), row[1].String())
	}
}

func TestM15TriggerInsertBody(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE log (msg TEXT)")
	// INSERT 体：源表插入时写一行审计日志。
	mustExec(t, e, "CREATE TRIGGER tr_log AFTER INSERT ON users FOR EACH ROW INSERT INTO log (msg) VALUES ('row inserted')")

	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	rows := mustRows(t, e, "SELECT msg FROM log")
	if len(rows) != 1 || rows[0][0].String() != "row inserted" {
		t.Fatalf("INSERT trigger body: got %d rows, want 1 with msg 'row inserted'", len(rows))
	}
}

func TestM15TriggerCoexistsWithMatViewAndView(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")
	// 虚拟视图 + 物化视图 + 触发器三者并存，互不冲突。
	mustExec(t, e, "CREATE VIEW v_users AS SELECT id, name FROM users")
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv_users AS SELECT id, name FROM users")
	mustExec(t, e, "CREATE TRIGGER tr_ins AFTER INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 100")

	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40)")
	// 虚拟视图即时可见新行
	if rows := mustRows(t, e, "SELECT id FROM v_users"); len(rows) != 4 {
		t.Fatalf("virtual view should see new row: got %d rows", len(rows))
	}
	// 物化视图保持快照（不自动更新）
	if rows := mustRows(t, e, "SELECT id FROM mv_users"); len(rows) != 3 {
		t.Fatalf("matview should keep snapshot: got %d rows", len(rows))
	}
	// 触发器正常生效
	if got := mustRows(t, e, "SELECT cnt FROM audit")[0][0].String(); got != "100" {
		t.Fatalf("trigger with matview/view: cnt = %s, want 100", got)
	}
	// 刷新物化视图后仍正常
	mustExec(t, e, "REFRESH MATERIALIZED VIEW mv_users")
	if rows := mustRows(t, e, "SELECT id FROM mv_users"); len(rows) != 4 {
		t.Fatalf("after REFRESH: want 4 rows, got %d", len(rows))
	}
}

func TestM15TriggerRecursionGuard(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE loop_t (id INT, v INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO loop_t (id, v) VALUES (1, 0)")
	// 触发器体更新同一张源表：递归防护保证只执行一次（不无限循环）。
	// 插入 v=50，触发器体把整表 v 置为 100，断言目标行被更新且未死循环。
	mustExec(t, e, "CREATE TRIGGER tr_loop AFTER INSERT ON loop_t FOR EACH ROW UPDATE loop_t SET v = 100")

	mustExec(t, e, "INSERT INTO loop_t (id, v) VALUES (2, 50)")
	got := mustRows(t, e, "SELECT v FROM loop_t WHERE id = 2")[0][0].String()
	if got != "100" {
		t.Fatalf("recursion guard: v = %s, want 100 (single trigger execution)", got)
	}
}

func TestM15ShowTriggersFromTable(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "CREATE TRIGGER tr_users AFTER INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 100")
	mustExec(t, e, "CREATE TRIGGER tr_audit AFTER UPDATE ON audit FOR EACH ROW UPDATE audit SET cnt = 100")

	rows := mustRows(t, e, "SHOW TRIGGERS FROM users")
	if len(rows) != 1 || rows[0][0].String() != "tr_users" {
		t.Fatalf("SHOW TRIGGERS FROM users: want 1 row tr_users, got %d", len(rows))
	}
	rows = mustRows(t, e, "SHOW TRIGGERS FROM audit")
	if len(rows) != 1 || rows[0][0].String() != "tr_audit" {
		t.Fatalf("SHOW TRIGGERS FROM audit: want 1 row tr_audit, got %d", len(rows))
	}
}

func TestM15CreateTriggerErrors(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")

	// 缺少时机（BEFORE/AFTER）
	mustErr(t, e, "CREATE TRIGGER t1 INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 1", "expected BEFORE or AFTER")
	// 非法事件
	mustErr(t, e, "CREATE TRIGGER t1 AFTER SELECT ON users FOR EACH ROW UPDATE audit SET cnt = 1", "expected INSERT, UPDATE or DELETE")
	// 缺少 FOR EACH ROW
	mustErr(t, e, "CREATE TRIGGER t1 AFTER INSERT ON users UPDATE audit SET cnt = 1", "expected keyword FOR")
	// 触发器体为空
	mustErr(t, e, "CREATE TRIGGER t1 AFTER INSERT ON users FOR EACH ROW", "trigger body required")
	// 触发器体非受支持语句（SELECT 体拒绝）
	mustErr(t, e, "CREATE TRIGGER t1 AFTER INSERT ON users FOR EACH ROW SELECT * FROM users", "unsupported trigger body")
	// 源表不存在
	mustErr(t, e, "CREATE TRIGGER t1 AFTER INSERT ON nope FOR EACH ROW UPDATE audit SET cnt = 1", "table not exists: nope")
	// 体引用的表不存在
	mustErr(t, e, "CREATE TRIGGER t1 AFTER INSERT ON users FOR EACH ROW UPDATE nope SET cnt = 1", "table not exists: nope")
	// 重名
	mustExec(t, e, "CREATE TRIGGER t1 AFTER INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 1")
	mustErr(t, e, "CREATE TRIGGER t1 AFTER UPDATE ON users FOR EACH ROW UPDATE audit SET cnt = 1", "trigger already exists")
	// 体 UPDATE 引用不存在的 SET 列（创建期预校验）
	mustErr(t, e, "CREATE TRIGGER t2 AFTER INSERT ON users FOR EACH ROW UPDATE audit SET nope = 1", "unknown column: nope")
	// DROP 不存在
	mustErr(t, e, "DROP TRIGGER ghost", "trigger not exists")
	// DROP IF EXISTS 不存在不报错
	mustExec(t, e, "DROP TRIGGER IF EXISTS ghost")
}

func TestM15TriggerMetaPersisted(t *testing.T) {
	// 触发器元数据持久化：SHOW 的 statement 列回显原文。
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (0)")
	mustExec(t, e, "CREATE TRIGGER tr_ins AFTER INSERT ON users FOR EACH ROW UPDATE audit SET cnt = 100")

	if got := strings.TrimSpace(mustRows(t, e, "SHOW TRIGGERS")[0][4].String()); got != "UPDATE audit SET cnt = 100" {
		t.Fatalf("SHOW TRIGGERS statement: got %q", got)
	}
}

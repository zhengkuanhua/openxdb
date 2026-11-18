package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// v1.19.0 D4：SAVEPOINT 事务子语句族的 SQL 层（解析 + 执行）测试。
// 覆盖：三步基本流程（含读己写）、语法变体与大小写不敏感、多层嵌套、
// 重名覆盖、ROLLBACK TO 可重复回、RELEASE 级联删除、未知名与未开事务报错、
// COMMIT 后保存点失效。

// savepointRowCount 统计满足条件的行数（辅助断言读己写可见性）。
func savepointRowCount(t *testing.T, e *sql.Engine, where string) int {
	t.Helper()
	rows := mustRows(t, e, "SELECT id FROM users WHERE "+where)
	return len(rows)
}

// TestSavepointSQLBasicFlow 三步基本流程：SAVEPOINT → 写入 → ROLLBACK TO，
// 回滚后读己写回到标记点状态，COMMIT 只落盘标记点之前的写入。
func TestSavepointSQLBasicFlow(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (10, 'dave', 40)")
	mustExec(t, e, "SAVEPOINT sp1")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (11, 'erin', 41)")
	mustExec(t, e, "UPDATE users SET age = 99 WHERE id = 1")
	if n := savepointRowCount(t, e, "id = 11"); n != 1 {
		t.Fatalf("回滚前事务内应可见 id=11，实际 %d 行", n)
	}
	rows := mustRows(t, e, "SELECT age FROM users WHERE id = 1")
	if len(rows) != 1 || rows[0][0].I != 99 {
		t.Fatalf("回滚前事务内 age 应为 99，实际 %+v", rows)
	}

	mustExec(t, e, "ROLLBACK TO sp1")
	if n := savepointRowCount(t, e, "id = 11"); n != 0 {
		t.Fatalf("回滚后事务内不应可见 id=11，实际 %d 行", n)
	}
	rows = mustRows(t, e, "SELECT age FROM users WHERE id = 1")
	if len(rows) != 1 || rows[0][0].I != 30 {
		t.Fatalf("回滚后 age 应恢复为 30，实际 %+v", rows)
	}
	if n := savepointRowCount(t, e, "id = 10"); n != 1 {
		t.Fatalf("回滚后事务内应保留 id=10，实际 %d 行", n)
	}

	mustExec(t, e, "COMMIT")
	if n := savepointRowCount(t, e, "id = 10"); n != 1 {
		t.Fatalf("COMMIT 后 id=10 应落盘，实际 %d 行", n)
	}
	if n := savepointRowCount(t, e, "id = 11"); n != 0 {
		t.Fatalf("COMMIT 后 id=11 不应落盘，实际 %d 行", n)
	}
	rows = mustRows(t, e, "SELECT age FROM users WHERE id = 1")
	if len(rows) != 1 || rows[0][0].I != 30 {
		t.Fatalf("COMMIT 后 age 应为 30，实际 %+v", rows)
	}
}

// TestSavepointSQLSyntaxVariants 关键字大小写不敏感，ROLLBACK 的 WORK /
// SAVEPOINT 修饰词可省略；RELEASE SAVEPOINT 必须显式写 SAVEPOINT。
func TestSavepointSQLSyntaxVariants(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// 全小写 + ROLLBACK TO 不带 SAVEPOINT 修饰词。
	mustExec(t, e, "begin")
	mustExec(t, e, "savepoint sp_a")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (20, 'frank', 50)")
	mustExec(t, e, "rollback to sp_a")
	if n := savepointRowCount(t, e, "id = 20"); n != 0 {
		t.Fatalf("小写语法回滚后不应可见 id=20，实际 %d 行", n)
	}

	// ROLLBACK WORK TO SAVEPOINT <name>。
	mustExec(t, e, "SavePoint sp_b")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (21, 'grace', 51)")
	mustExec(t, e, "RollBack Work To SavePoint sp_b")
	if n := savepointRowCount(t, e, "id = 21"); n != 0 {
		t.Fatalf("ROLLBACK WORK TO SAVEPOINT 回滚后不应可见 id=21，实际 %d 行", n)
	}

	// ROLLBACK WORK TO <name>（WORK 有、SAVEPOINT 无）。
	mustExec(t, e, "SAVEPOINT sp_c")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (22, 'heidi', 52)")
	mustExec(t, e, "ROLLBACK WORK TO sp_c")
	if n := savepointRowCount(t, e, "id = 22"); n != 0 {
		t.Fatalf("ROLLBACK WORK TO 回滚后不应可见 id=22，实际 %d 行", n)
	}

	// RELEASE SAVEPOINT 小写写法可用。
	mustExec(t, e, "release savepoint sp_c")
	mustErr(t, e, "ROLLBACK TO sp_c", "does not exist")

	// 整事务 ROLLBACK（无 TO）语义不变。
	mustExec(t, e, "ROLLBACK")
	if n := savepointRowCount(t, e, "id >= 10"); n != 0 {
		t.Fatalf("整事务 ROLLBACK 后不应残留任何写入，实际 %d 行", n)
	}
}

// TestSavepointSQLNoActiveTransaction 未开启事务时三条语句均报既有文案。
func TestSavepointSQLNoActiveTransaction(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustErr(t, e, "SAVEPOINT sp", "no active transaction")
	mustErr(t, e, "ROLLBACK TO sp", "no active transaction")
	mustErr(t, e, "RELEASE SAVEPOINT sp", "no active transaction")

	// COMMIT 之后同样视为无活动事务。
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "SAVEPOINT sp")
	mustExec(t, e, "COMMIT")
	mustErr(t, e, "ROLLBACK TO sp", "no active transaction")
	mustErr(t, e, "RELEASE SAVEPOINT sp", "no active transaction")
}

// TestSavepointSQLUnknownName 未登记（或已释放）的保存点名报错。
func TestSavepointSQLUnknownName(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustExec(t, e, "BEGIN")
	mustErr(t, e, "ROLLBACK TO nope", "savepoint nope does not exist")
	mustErr(t, e, "RELEASE SAVEPOINT nope", "savepoint nope does not exist")
	mustExec(t, e, "SAVEPOINT sp")
	mustErr(t, e, "ROLLBACK TO other", "savepoint other does not exist")
	mustExec(t, e, "ROLLBACK")
}

// TestSavepointSQLNested 多层嵌套：由内向外逐层回滚，各层丢弃范围正确。
func TestSavepointSQLNested(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (30, 'ivan', 60)")
	mustExec(t, e, "SAVEPOINT s1")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (31, 'judy', 61)")
	mustExec(t, e, "SAVEPOINT s2")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (32, 'ken', 62)")
	mustExec(t, e, "SAVEPOINT s3")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (33, 'lily', 63)")

	mustExec(t, e, "ROLLBACK TO s3")
	if n := savepointRowCount(t, e, "id = 33"); n != 0 {
		t.Fatalf("回滚到 s3 后不应可见 id=33，实际 %d 行", n)
	}
	if n := savepointRowCount(t, e, "id = 32"); n != 1 {
		t.Fatalf("回滚到 s3 后应保留 id=32，实际 %d 行", n)
	}

	mustExec(t, e, "ROLLBACK TO s2")
	if n := savepointRowCount(t, e, "id = 32"); n != 0 {
		t.Fatalf("回滚到 s2 后不应可见 id=32，实际 %d 行", n)
	}
	if n := savepointRowCount(t, e, "id = 31"); n != 1 {
		t.Fatalf("回滚到 s2 后应保留 id=31，实际 %d 行", n)
	}

	mustExec(t, e, "ROLLBACK TO s1")
	if n := savepointRowCount(t, e, "id = 31"); n != 0 {
		t.Fatalf("回滚到 s1 后不应可见 id=31，实际 %d 行", n)
	}
	if n := savepointRowCount(t, e, "id = 30"); n != 1 {
		t.Fatalf("回滚到 s1 后应保留 id=30，实际 %d 行", n)
	}

	mustExec(t, e, "COMMIT")
	if n := savepointRowCount(t, e, "id = 30"); n != 1 {
		t.Fatalf("COMMIT 后 id=30 应落盘，实际 %d 行", n)
	}
	if n := savepointRowCount(t, e, "id >= 31"); n != 0 {
		t.Fatalf("COMMIT 后 id>=31 不应落盘，实际 %d 行", n)
	}
}

// TestSavepointSQLDuplicateNameOverwrite 重名保存点覆盖旧标记（大小写不敏感）。
func TestSavepointSQLDuplicateNameOverwrite(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (40, 'mike', 70)")
	mustExec(t, e, "SAVEPOINT dup")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (41, 'nina', 71)")
	mustExec(t, e, "SAVEPOINT DUP")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (42, 'oscar', 72)")
	mustExec(t, e, "ROLLBACK TO dup")

	// 标记点已前移到第二次登记处：41 保留，42 丢弃。
	if n := savepointRowCount(t, e, "id = 41"); n != 1 {
		t.Fatalf("重名覆盖后应保留 id=41，实际 %d 行", n)
	}
	if n := savepointRowCount(t, e, "id = 42"); n != 0 {
		t.Fatalf("重名覆盖后不应可见 id=42，实际 %d 行", n)
	}
	mustExec(t, e, "COMMIT")
}

// TestSavepointSQLRollbackRepeatable ROLLBACK TO 不删除保存点，可重复回同一标记点。
func TestSavepointSQLRollbackRepeatable(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (50, 'paul', 80)")
	mustExec(t, e, "SAVEPOINT sp")
	for i := 0; i < 3; i++ {
		mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (51, 'quinn', 81)")
		mustExec(t, e, "ROLLBACK TO sp")
		if n := savepointRowCount(t, e, "id = 51"); n != 0 {
			t.Fatalf("第 %d 次重复回滚后不应可见 id=51，实际 %d 行", i+1, n)
		}
	}
	// 重复回滚后仍可继续写入并正常提交。
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (52, 'rose', 82)")
	mustExec(t, e, "COMMIT")
	if n := savepointRowCount(t, e, "id = 50"); n != 1 {
		t.Fatalf("COMMIT 后应保留 id=50，实际 %d 行", n)
	}
	if n := savepointRowCount(t, e, "id = 52"); n != 1 {
		t.Fatalf("COMMIT 后应保留 id=52，实际 %d 行", n)
	}
	if n := savepointRowCount(t, e, "id = 51"); n != 0 {
		t.Fatalf("COMMIT 后不应落盘 id=51，实际 %d 行", n)
	}
}

// TestSavepointSQLReleaseCascade RELEASE 级联删除该点及其后所有保存点，
// 已写入数据不变，更早的保存点仍可用。
func TestSavepointSQLReleaseCascade(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "SAVEPOINT s1")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (60, 'sam', 90)")
	mustExec(t, e, "SAVEPOINT s2")
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (61, 'tina', 91)")
	mustExec(t, e, "SAVEPOINT s3")
	mustExec(t, e, "RELEASE SAVEPOINT s2")

	mustErr(t, e, "ROLLBACK TO s2", "savepoint s2 does not exist")
	mustErr(t, e, "ROLLBACK TO s3", "savepoint s3 does not exist")
	if n := savepointRowCount(t, e, "id = 61"); n != 1 {
		t.Fatalf("RELEASE 不应撤销已写入数据，实际 %d 行", n)
	}

	// 更早的保存点仍可回滚，丢弃其后的全部写入。
	mustExec(t, e, "ROLLBACK TO s1")
	if n := savepointRowCount(t, e, "id >= 60"); n != 0 {
		t.Fatalf("回滚到 s1 后不应可见 id>=60，实际 %d 行", n)
	}
	mustExec(t, e, "COMMIT")
	if n := savepointRowCount(t, e, "id >= 60"); n != 0 {
		t.Fatalf("COMMIT 后不应落盘 id>=60，实际 %d 行", n)
	}
}

// TestSavepointSQLCommitClears 保存点不跨事务：COMMIT / ROLLBACK 后清空，
// 新事务中同名保存点不存在。
func TestSavepointSQLCommitClears(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "SAVEPOINT sp")
	mustExec(t, e, "COMMIT")
	mustErr(t, e, "ROLLBACK TO sp", "no active transaction")

	mustExec(t, e, "BEGIN")
	mustErr(t, e, "ROLLBACK TO sp", "savepoint sp does not exist")
	mustErr(t, e, "RELEASE SAVEPOINT sp", "savepoint sp does not exist")
	mustExec(t, e, "ROLLBACK")

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "SAVEPOINT sp")
	mustExec(t, e, "ROLLBACK")
	mustExec(t, e, "BEGIN")
	mustErr(t, e, "ROLLBACK TO sp", "savepoint sp does not exist")
	mustExec(t, e, "ROLLBACK")
}

// TestSavepointSQLWriteAfterRollbackCommit 回滚到保存点后再写入仍可正常提交，
// 且提交内容仅含标记点之前的写入与回滚之后的写入。
func TestSavepointSQLWriteAfterRollbackCommit(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE TABLE audit (cnt INT)")

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "SAVEPOINT outer_sp")
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (1)")
	mustExec(t, e, "ROLLBACK TO outer_sp")
	rows := mustRows(t, e, "SELECT cnt FROM audit")
	if len(rows) != 0 {
		t.Fatalf("回滚到 outer_sp 后 audit 应为空，实际 %d 行", len(rows))
	}
	mustExec(t, e, "INSERT INTO audit (cnt) VALUES (2)")
	mustExec(t, e, "COMMIT")
	rows = mustRows(t, e, "SELECT cnt FROM audit")
	if len(rows) != 1 || rows[0][0].I != 2 {
		t.Fatalf("COMMIT 后 audit 应为 [2]，实际 %+v", rows)
	}
}

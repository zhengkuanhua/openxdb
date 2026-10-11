package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// M10 子功能 3：UPSERT（INSERT ... ON DUPLICATE KEY UPDATE + REPLACE INTO）。
// 覆盖主键/唯一键冲突更新、无冲突普通插入、REPLACE 删旧插新、
// 语句内多行混合、事务原子性、外键 RESTRICT/CASCADE 联动与唯一索引建索引校验。

// 主键冲突 → ON DUPLICATE KEY UPDATE 更新既有行；无冲突 → 正常插入。
func TestUpsertOnDuplicateKeyPKConflict(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	// 冲突更新：affected=2（MySQL 语义：更新 1 行记 2）
	res := mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (1, 'alice2', 31) ON DUPLICATE KEY UPDATE name='alice2', age=31")
	if res.AffectedRows != 2 {
		t.Fatalf("ON DUP update: want affected 2, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT name, age FROM users WHERE id = 1")
	if len(rows) != 1 || rows[0][0].S != "alice2" || rows[0][1].I != 31 {
		t.Fatalf("after on-dup update: %+v", rows)
	}
	// 无冲突插入：affected=1
	res = mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 40) ON DUPLICATE KEY UPDATE age=999")
	if res.AffectedRows != 1 {
		t.Fatalf("ON DUP insert: want affected 1, got %d", res.AffectedRows)
	}
	rows = mustRows(t, e, "SELECT name, age FROM users WHERE id = 4")
	if len(rows) != 1 || rows[0][0].S != "dave" || rows[0][1].I != 40 {
		t.Fatalf("after on-dup insert: %+v", rows)
	}
	// 表行数仍为 4
	rows = mustRows(t, e, "SELECT COUNT(*) FROM users")
	if rows[0][0].I != 4 {
		t.Fatalf("row count: want 4, got %d", rows[0][0].I)
	}
}

// 唯一键冲突 → ON DUPLICATE KEY UPDATE 更新冲突行（非待插入行）。
func TestUpsertOnDuplicateKeyUniqueConflict(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE u (id INT PRIMARY KEY, email TEXT)")
	mustExec(t, e, "CREATE UNIQUE INDEX uq_email ON u(email)")
	mustExec(t, e, "INSERT INTO u (id, email) VALUES (1, 'a@x.com')")
	// 新行 id=2 与既有 id=1 唯一键冲突：更新冲突行 id=1，id=2 不插入
	res := mustExec(t, e, "INSERT INTO u (id, email) VALUES (2, 'a@x.com') ON DUPLICATE KEY UPDATE email='b@x.com'")
	if res.AffectedRows != 2 {
		t.Fatalf("unique on-dup update: want affected 2, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id, email FROM u ORDER BY id")
	if len(rows) != 1 || rows[0][0].I != 1 || rows[0][1].S != "b@x.com" {
		t.Fatalf("unique on-dup result: %+v", rows)
	}
}

// 表无唯一索引（仅隐式主键=首列）时，主键不冲突即等同普通 INSERT（MySQL 语义）。
func TestUpsertNoUniqueConstraintLikeInsert(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT)")
	res := mustExec(t, e, "INSERT INTO t (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name='b'")
	if res.AffectedRows != 1 {
		t.Fatalf("no-key insert: want affected 1, got %d", res.AffectedRows)
	}
	// 主键不同不构成冲突：再插一条，两行并存，ON DUP 不触发
	mustExec(t, e, "INSERT INTO t (id, name) VALUES (2, 'c') ON DUPLICATE KEY UPDATE name='x'")
	rows := mustRows(t, e, "SELECT id, name FROM t ORDER BY id")
	if len(rows) != 2 || rows[0][0].I != 1 || rows[0][1].S != "a" || rows[1][0].I != 2 || rows[1][1].S != "c" {
		t.Fatalf("no-key rows: %+v", rows)
	}
}

// REPLACE INTO：主键冲突时删旧行再插入新行（affected=删除数+1）。
func TestUpsertReplaceIntoPK(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	res := mustExec(t, e, "REPLACE INTO users (id, name, age) VALUES (2, 'bob2', 26)")
	if res.AffectedRows != 2 {
		t.Fatalf("REPLACE pk: want affected 2, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT name, age FROM users WHERE id = 2")
	if len(rows) != 1 || rows[0][0].S != "bob2" || rows[0][1].I != 26 {
		t.Fatalf("replace result: %+v", rows)
	}
	// 行数仍 3（删 1 插 1）
	rows = mustRows(t, e, "SELECT COUNT(*) FROM users")
	if rows[0][0].I != 3 {
		t.Fatalf("row count after replace: want 3, got %d", rows[0][0].I)
	}
	// 无冲突 REPLACE 等同插入
	res = mustExec(t, e, "REPLACE INTO users (id, name, age) VALUES (9, 'new9', 90)")
	if res.AffectedRows != 1 {
		t.Fatalf("REPLACE insert: want affected 1, got %d", res.AffectedRows)
	}
}

// REPLACE INTO：唯一键冲突时删除冲突行（旧行），插入新行。
func TestUpsertReplaceIntoUnique(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE u (id INT PRIMARY KEY, email TEXT)")
	mustExec(t, e, "CREATE UNIQUE INDEX uq_email ON u(email)")
	mustExec(t, e, "INSERT INTO u (id, email) VALUES (1, 'a@x.com')")
	res := mustExec(t, e, "REPLACE INTO u (id, email) VALUES (2, 'a@x.com')")
	if res.AffectedRows != 2 {
		t.Fatalf("REPLACE unique: want affected 2, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id, email FROM u")
	if len(rows) != 1 || rows[0][0].I != 2 || rows[0][1].S != "a@x.com" {
		t.Fatalf("replace unique result: %+v", rows)
	}
}

// REPLACE INTO：一行同时命中多个唯一索引冲突时删除全部冲突行（删多插一）。
func TestUpsertReplaceUniqueMultiConflicts(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE m (id INT PRIMARY KEY, a INT, b INT)")
	mustExec(t, e, "CREATE UNIQUE INDEX uq_a ON m(a)")
	mustExec(t, e, "CREATE UNIQUE INDEX uq_b ON m(b)")
	mustExec(t, e, "INSERT INTO m (id, a, b) VALUES (1, 10, 100), (2, 20, 200)")
	// 新行 (3,10,200)：a=10 冲突行1，b=200 冲突行2 → 删 2 行插 1 行
	res := mustExec(t, e, "REPLACE INTO m (id, a, b) VALUES (3, 10, 200)")
	if res.AffectedRows != 3 {
		t.Fatalf("REPLACE multi-conflict: want affected 3, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id, a, b FROM m")
	if len(rows) != 1 || rows[0][0].I != 3 || rows[0][1].I != 10 || rows[0][2].I != 200 {
		t.Fatalf("replace multi-conflict result: %+v", rows)
	}
}

// 多行混合：普通插入 + 主键冲突更新 + 唯一键冲突更新 在同一语句内。
func TestUpsertMultiRowsMixed(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE u (id INT PRIMARY KEY, email TEXT, age INT)")
	mustExec(t, e, "CREATE UNIQUE INDEX uq_email ON u(email)")
	mustExec(t, e, "INSERT INTO u (id, email, age) VALUES (1, 'a@x.com', 10)")
	// 行1：id=3 无冲突插入；行2：id=1 主键冲突更新；行3：email='a@x.com' 唯一冲突更新
	res := mustExec(t, e, "INSERT INTO u (id, email, age) VALUES (3, 'c@x.com', 30), (1, 'ignored@x.com', 99), (2, 'a@x.com', 20) ON DUPLICATE KEY UPDATE age=50")
	// 插入1(aff=1) + 主键更新1(aff=2) + 唯一更新1(aff=2) = 5
	if res.AffectedRows != 5 {
		t.Fatalf("mixed rows: want affected 5, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id, email, age FROM u ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("mixed rows count: %+v", rows)
	}
	// id=1 被唯一冲突行更新（email 保持原样，age=50）
	if rows[0][0].I != 1 || rows[0][1].S != "a@x.com" || rows[0][2].I != 50 {
		t.Fatalf("mixed row id=1: %+v", rows[0])
	}
	// id=3 正常插入
	if rows[1][0].I != 3 || rows[1][1].S != "c@x.com" || rows[1][2].I != 30 {
		t.Fatalf("mixed row id=3: %+v", rows[1])
	}
}

// 语句内重复：同一 INSERT 语句第二行与第一行（pending）主键冲突 → 更新 pending 行。
func TestUpsertOnDuplicateStatementPendingConflict(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	res := mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (10, 'x', 1), (10, 'y', 2) ON DUPLICATE KEY UPDATE name='dup'")
	// 插 1(aff=1) + 更新 pending 1(aff=2) = 3
	if res.AffectedRows != 3 {
		t.Fatalf("pending conflict: want affected 3, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id, name, age FROM users WHERE id = 10")
	if len(rows) != 1 || rows[0][0].I != 10 || rows[0][1].S != "dup" || rows[0][2].I != 1 {
		t.Fatalf("pending conflict result: %+v", rows)
	}
}

// REPLACE 多行混合：主键冲突 + 无冲突 + 主键冲突。
func TestUpsertReplaceMultiRowMixed(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	res := mustExec(t, e, "REPLACE INTO users (id, name, age) VALUES (1, 'new1', 10), (4, 'new4', 40), (2, 'new2', 20)")
	// (删1+插1) + (插1) + (删1+插1) = 5
	if res.AffectedRows != 5 {
		t.Fatalf("REPLACE mixed: want affected 5, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id, name, age FROM users ORDER BY id")
	if len(rows) != 4 {
		t.Fatalf("replace mixed count: %+v", rows)
	}
	if rows[0][0].I != 1 || rows[0][1].S != "new1" || rows[0][2].I != 10 {
		t.Fatalf("replace row1: %+v", rows[0])
	}
	if rows[1][0].I != 2 || rows[1][1].S != "new2" || rows[1][2].I != 20 {
		t.Fatalf("replace row2: %+v", rows[1])
	}
	if rows[3][0].I != 4 || rows[3][1].S != "new4" || rows[3][2].I != 40 {
		t.Fatalf("replace row4: %+v", rows[3])
	}
}

// ON DUPLICATE KEY UPDATE 禁止修改主键列；语句失败整体回滚。
func TestUpsertOnDuplicatePKColumnErr(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustErr(t, e, "INSERT INTO users (id, name, age) VALUES (1, 'x', 1) ON DUPLICATE KEY UPDATE id=99", "cannot update primary key")
	// 原子回滚：原数据不变
	rows := mustRows(t, e, "SELECT name, age FROM users WHERE id = 1")
	if len(rows) != 1 || rows[0][0].S != "alice" || rows[0][1].I != 30 {
		t.Fatalf("rollback after pk-dup err: %+v", rows)
	}
	rows = mustRows(t, e, "SELECT COUNT(*) FROM users")
	if rows[0][0].I != 3 {
		t.Fatalf("row count after err: want 3, got %d", rows[0][0].I)
	}
}

// 普通 INSERT 唯一键冲突 → 报错（非更新）。
func TestUpsertInsertUniqueDuplicateErr(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE u (id INT PRIMARY KEY, email TEXT)")
	mustExec(t, e, "CREATE UNIQUE INDEX uq_email ON u(email)")
	mustExec(t, e, "INSERT INTO u (id, email) VALUES (1, 'a@x.com')")
	mustErr(t, e, "INSERT INTO u (id, email) VALUES (2, 'a@x.com')", "duplicate unique key")
	// 语句失败无部分写入
	rows := mustRows(t, e, "SELECT COUNT(*) FROM u")
	if rows[0][0].I != 1 {
		t.Fatalf("row count after unique err: want 1, got %d", rows[0][0].I)
	}
}

// 已有重复数据时建 UNIQUE 索引 → 回填阶段报错且索引不建立。
func TestUpsertUniqueIndexCreateDuplicateErr(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, email TEXT)")
	mustExec(t, e, "INSERT INTO t (id, email) VALUES (1, 'a@x.com'), (2, 'a@x.com')")
	mustErr(t, e, "CREATE UNIQUE INDEX uq_email ON t(email)", "duplicate unique key")
	// 失败后表仍可用，普通索引等值查询正常
	rows := mustRows(t, e, "SELECT id FROM t WHERE email = 'a@x.com' ORDER BY id")
	if len(rows) != 2 || rows[0][0].I != 1 || rows[1][0].I != 2 {
		t.Fatalf("table usable after failed unique index: %+v", rows)
	}
}

// UPDATE 造成唯一键冲突：磁盘冲突报错；语句内两行更新成同值也报错且整体回滚。
func TestUpsertUpdateUniqueConflictErr(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE u (id INT PRIMARY KEY, email TEXT)")
	mustExec(t, e, "CREATE UNIQUE INDEX uq_email ON u(email)")
	mustExec(t, e, "INSERT INTO u (id, email) VALUES (1, 'a@x.com'), (2, 'b@x.com')")
	// 与既有行冲突（磁盘检查）
	mustErr(t, e, "UPDATE u SET email = 'a@x.com' WHERE id = 2", "duplicate unique key")
	// 语句内两行都改成同值 → pending 检查拦截
	mustErr(t, e, "UPDATE u SET email = 'x@x.com' WHERE id >= 1", "duplicate unique key")
	// 全部回滚：数据不变
	rows := mustRows(t, e, "SELECT id, email FROM u ORDER BY id")
	if len(rows) != 2 || rows[0][1].S != "a@x.com" || rows[1][1].S != "b@x.com" {
		t.Fatalf("rollback after update unique err: %+v", rows)
	}
}

// 外键 RESTRICT：ON DUPLICATE 更新被引用父行 → 拒绝且回滚。
func TestUpsertOnDuplicateFKRestrict(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id))")
	mustExec(t, e, "INSERT INTO parent (id, name) VALUES (1, 'a')")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	mustErr(t, e, "INSERT INTO parent (id, name) VALUES (1, 'a2') ON DUPLICATE KEY UPDATE name='a2'", "foreign key constraint violated")
	rows := mustRows(t, e, "SELECT name FROM parent WHERE id = 1")
	if len(rows) != 1 || rows[0][0].S != "a" {
		t.Fatalf("fk restrict rollback: %+v", rows)
	}
}

// 外键 RESTRICT：REPLACE 被引用父行 → 拒绝且回滚。
func TestUpsertReplaceFKRestrict(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id))")
	mustExec(t, e, "INSERT INTO parent (id, name) VALUES (1, 'a')")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	mustErr(t, e, "REPLACE INTO parent (id, name) VALUES (1, 'b')", "foreign key constraint violated")
	rows := mustRows(t, e, "SELECT name FROM parent WHERE id = 1")
	if len(rows) != 1 || rows[0][0].S != "a" {
		t.Fatalf("replace fk restrict rollback: %+v", rows)
	}
}

// 外键 CASCADE：REPLACE 父行时子行联动删除，删除+插入原子完成。
func TestUpsertReplaceFKCascade(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE)")
	mustExec(t, e, "INSERT INTO parent (id, name) VALUES (1, 'a')")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	res := mustExec(t, e, "REPLACE INTO parent (id, name) VALUES (1, 'b')")
	if res.AffectedRows != 2 {
		t.Fatalf("replace cascade: want affected 2, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id, name FROM parent")
	if len(rows) != 1 || rows[0][0].I != 1 || rows[0][1].S != "b" {
		t.Fatalf("replace cascade parent: %+v", rows)
	}
	rows = mustRows(t, e, "SELECT COUNT(*) FROM child")
	if rows[0][0].I != 0 {
		t.Fatalf("replace cascade child: want 0, got %d", rows[0][0].I)
	}
}

// 表无唯一索引（仅隐式主键=首列）时，REPLACE 等同普通 INSERT（不删任何行）。
func TestUpsertReplaceNoUniqueConstraintLikeInsert(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT)")
	res := mustExec(t, e, "REPLACE INTO t (id, name) VALUES (1, 'a'), (2, 'b')")
	if res.AffectedRows != 2 {
		t.Fatalf("replace no-key: want affected 2, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 2 {
		t.Fatalf("replace no-key rows: want 2, got %d", rows[0][0].I)
	}
}

// Value 编译期使用：确保测试文件引用了 sql 包。
var _ = sql.Value{}

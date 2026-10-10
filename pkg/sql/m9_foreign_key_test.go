package sql_test

import (
	"testing"
)

// M9 子功能 4：外键约束（FOREIGN KEY / REFERENCES / ON DELETE CASCADE）
// 建表校验、插入/更新防护、删除联动、DROP 表外键清理。

func TestForeignKeyTableLevelConstraint(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id))")
	// 子表插入父表已有主键 → 成功
	mustExec(t, e, "INSERT INTO parent (id, name) VALUES (1, 'a')")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	// 子表插入父表不存在主键 → 违反外键
	mustErr(t, e, "INSERT INTO child (id, pid) VALUES (11, 99)", "foreign key constraint violated")
}

func TestForeignKeyColumnLevelReferences(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE dept (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE emp (id INT PRIMARY KEY, dept_id INT REFERENCES dept(id))")
	mustExec(t, e, "INSERT INTO dept (id) VALUES (5)")
	mustExec(t, e, "INSERT INTO emp (id, dept_id) VALUES (1, 5)")
	mustErr(t, e, "INSERT INTO emp (id, dept_id) VALUES (2, 6)", "foreign key constraint violated")
}

func TestForeignKeyCreateValidation(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY, code TEXT)")
	// 引用不存在的表
	mustErr(t, e, "CREATE TABLE t1 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES ghost(id))", "referenced table not exists")
	// 引用列不是父表主键
	mustErr(t, e, "CREATE TABLE t2 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(code))", "must be primary key")
	// 列类型不匹配（子列 INT vs 父主键 TEXT）
	mustExec(t, e, "CREATE TABLE p2 (id TEXT PRIMARY KEY)")
	mustErr(t, e, "CREATE TABLE t3 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES p2(id))", "column type mismatch")
}

func TestForeignKeyOnDeleteRestrict(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))")
	mustExec(t, e, "INSERT INTO parent (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (1, 1)")
	// 被引用父行默认 RESTRICT 禁止删除
	mustErr(t, e, "DELETE FROM parent WHERE id = 1", "references parent")
	// 未引用父行可删除
	mustExec(t, e, "INSERT INTO parent (id) VALUES (2)")
	mustExec(t, e, "DELETE FROM parent WHERE id = 2")
}

func TestForeignKeyOnDeleteCascade(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE)")
	mustExec(t, e, "INSERT INTO parent (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (11, 1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (12, 1)")
	mustExec(t, e, "DELETE FROM parent WHERE id = 1")
	rows := mustRows(t, e, "SELECT id FROM child ORDER BY id")
	if len(rows) != 0 {
		t.Fatalf("CASCADE: want 0 child rows, got %d (%+v)", len(rows), rows)
	}
}

func TestForeignKeyCascadeIndexCleanup(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE)")
	mustExec(t, e, "CREATE INDEX idx_child_pid ON child (pid)")
	mustExec(t, e, "INSERT INTO parent (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	mustExec(t, e, "DELETE FROM parent WHERE id = 1")
	// 索引已清理：按索引查询不再命中已级联删除的行
	rows := mustRows(t, e, "SELECT id FROM child WHERE pid = 1")
	if len(rows) != 0 {
		t.Fatalf("index cleanup: want 0 rows, got %d", len(rows))
	}
}

func TestForeignKeyUpdateParentProtected(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))")
	mustExec(t, e, "INSERT INTO parent (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (1, 1)")
	// 被引用父行禁止更新（ON UPDATE 未支持，RESTRICT 语义）
	mustErr(t, e, "UPDATE parent SET id = 100 WHERE id = 1", "referenced by child table")
	// 未被引用父行可更新
	mustExec(t, e, "INSERT INTO parent (id) VALUES (2)")
	mustExec(t, e, "UPDATE parent SET id = 200 WHERE id = 2")
}

func TestForeignKeyDropTableCleansRefs(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))")
	mustExec(t, e, "INSERT INTO parent (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (1, 1)")
	// DROP 父表：联动移除子表指向它的外键定义（子表数据保留）
	mustExec(t, e, "DROP TABLE parent")
	rows := mustRows(t, e, "SELECT id, pid FROM child ORDER BY id")
	if len(rows) != 1 || rows[0][0].I != 1 || rows[0][1].I != 1 {
		t.Fatalf("child data preserved: want [1,1], got %+v", rows)
	}
	// 外键已解除：可插入任意 pid，不再校验
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (2, 999)")
}

func TestForeignKeyDropParentWithSiblingRefs(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE p1 (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE c1 (id INT PRIMARY KEY, pid INT REFERENCES p1(id))")
	mustExec(t, e, "CREATE TABLE c2 (id INT PRIMARY KEY, pid INT REFERENCES p1(id))")
	mustExec(t, e, "DROP TABLE p1")
	// 两个子表的外键均被联动清理
	mustExec(t, e, "INSERT INTO c1 (id, pid) VALUES (1, 7)")
	mustExec(t, e, "INSERT INTO c2 (id, pid) VALUES (1, 8)")
}

func TestForeignKeyShardingCrossRegion(t *testing.T) {
	// 分片引擎下外键校验仍生效（跨 region 读取父行）
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))")
	mustExec(t, e, "INSERT INTO parent (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	if err := e.SplitTable("child", [][]byte{splitBoundary(5)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	// 分裂后子行落在不同 region，跨 region 外键校验仍生效
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (11, 1)")
	mustErr(t, e, "INSERT INTO child (id, pid) VALUES (12, 77)", "foreign key constraint violated")
}

func TestForeignKeyOrderByScan(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE p (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))")
	for i := 1; i <= 3; i++ {
		mustExec(t, e, "INSERT INTO p (id) VALUES ("+itoa(int64(i))+")")
		mustExec(t, e, "INSERT INTO c (id, pid) VALUES ("+itoa(int64(i*10))+", "+itoa(int64(i))+")")
	}
	rows := mustRows(t, e, "SELECT id, pid FROM c ORDER BY pid DESC")
	if len(rows) != 3 || rows[0][1].I != 3 {
		t.Fatalf("order by desc: want pid=3 first, got %+v", rows)
	}
}

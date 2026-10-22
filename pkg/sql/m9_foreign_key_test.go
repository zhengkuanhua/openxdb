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
	// 被引用父行禁止更新（ON UPDATE 缺省 RESTRICT，与 ON DELETE RESTRICT 同语义）
	mustErr(t, e, "UPDATE parent SET id = 100 WHERE id = 1", "references parent")
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

// ---- P0-3：外键 ON UPDATE 级联动作（RESTRICT / CASCADE / SET NULL） ----

func TestForeignKeyOnUpdateRestrict(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))")
	mustExec(t, e, "INSERT INTO parent (id, name) VALUES (1, 'a')")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	// 显式 ON UPDATE RESTRICT：被引用父行禁止改主键
	mustExec(t, e, "CREATE TABLE parent2 (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE child2 (id INT PRIMARY KEY, pid INT REFERENCES parent2(id) ON UPDATE RESTRICT)")
	mustExec(t, e, "INSERT INTO parent2 (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO child2 (id, pid) VALUES (1, 1)")
	mustErr(t, e, "UPDATE parent2 SET id = 100 WHERE id = 1", "references parent2")
	// 未被引用父行可更新主键
	mustExec(t, e, "INSERT INTO parent2 (id) VALUES (2)")
	mustExec(t, e, "UPDATE parent2 SET id = 200 WHERE id = 2")
	// 被引用行更新非主键列不受外键影响
	mustExec(t, e, "UPDATE parent SET name = 'b' WHERE id = 1")
	// 未触发主键变化的等值更新也放行
	mustExec(t, e, "UPDATE parent SET id = 1 WHERE id = 1")
}

func TestForeignKeyOnUpdateCascade(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id) ON UPDATE CASCADE)")
	mustExec(t, e, "INSERT INTO parent (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (11, 1)")
	mustExec(t, e, "UPDATE parent SET id = 100 WHERE id = 1")
	rows := mustRows(t, e, "SELECT id, pid FROM child ORDER BY id")
	if len(rows) != 2 || rows[0][1].I != 100 || rows[1][1].I != 100 {
		t.Fatalf("CASCADE: want 2 rows both pid=100, got %+v", rows)
	}
	// 新父键可被新子行引用
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (12, 100)")
}

func TestForeignKeyOnUpdateCascadeIndexCleanup(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id) ON UPDATE CASCADE)")
	mustExec(t, e, "CREATE INDEX idx_child_pid ON child (pid)")
	mustExec(t, e, "INSERT INTO parent (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	mustExec(t, e, "UPDATE parent SET id = 100 WHERE id = 1")
	// 旧索引键已清理、新索引键可命中
	oldRows := mustRows(t, e, "SELECT id FROM child WHERE pid = 1")
	if len(oldRows) != 0 {
		t.Fatalf("index cleanup: old pid=1 must be empty, got %d", len(oldRows))
	}
	newRows := mustRows(t, e, "SELECT id FROM child WHERE pid = 100")
	if len(newRows) != 1 {
		t.Fatalf("index hit: want 1 row for pid=100, got %d", len(newRows))
	}
}

func TestForeignKeyOnUpdateSetNull(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE parent (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id) ON UPDATE SET NULL)")
	mustExec(t, e, "INSERT INTO parent (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (10, 1)")
	mustExec(t, e, "INSERT INTO child (id, pid) VALUES (11, 1)")
	mustExec(t, e, "UPDATE parent SET id = 100 WHERE id = 1")
	rows := mustRows(t, e, "SELECT id, pid FROM child ORDER BY id")
	if len(rows) != 2 || rows[0][1].Kind != "NULL" || rows[1][1].Kind != "NULL" {
		t.Fatalf("SET NULL: want 2 rows with NULL pid, got %+v", rows)
	}
}

func TestForeignKeyOnUpdateSetNullNonNullable(t *testing.T) {
	e := newEngine(t)
	// 外键列为主键（隐含 NOT NULL）时 ON UPDATE SET NULL 建表即拒绝
	mustExec(t, e, "CREATE TABLE p (id INT PRIMARY KEY)")
	// 外键列为主键（隐含 NOT NULL）时 ON UPDATE SET NULL 建表即拒绝（表级外键）
	mustErr(t, e, "CREATE TABLE c (id INT PRIMARY KEY, FOREIGN KEY (id) REFERENCES p(id) ON UPDATE SET NULL)", "requires nullable column")
}

func TestForeignKeyOnUpdateMultiLevelChain(t *testing.T) {
	e := newEngine(t)
	// 多级外键链：A ← B ← C，全部 ON UPDATE CASCADE；
	// B 的主键同时是引用 A 的外键列，A 更新会级联改写 B 主键并继续联动 C
	mustExec(t, e, "CREATE TABLE a (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE b (id INT PRIMARY KEY, FOREIGN KEY (id) REFERENCES a(id) ON UPDATE CASCADE)")
	mustExec(t, e, "CREATE TABLE c (id INT PRIMARY KEY, bid INT, FOREIGN KEY (bid) REFERENCES b(id) ON UPDATE CASCADE)")
	mustExec(t, e, "INSERT INTO a (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO b (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO c (id, bid) VALUES (100, 1)")
	mustExec(t, e, "UPDATE a SET id = 1000 WHERE id = 1")
	// A 主键 1→1000 级联 B.id 1→1000（B 主键变化）；B 主键 1→1000 再级联 C.bid 1→1000
	bRows := mustRows(t, e, "SELECT id FROM b")
	if len(bRows) != 1 || bRows[0][0].I != 1000 {
		t.Fatalf("level B: want id=1000, got %+v", bRows)
	}
	cRows := mustRows(t, e, "SELECT id, bid FROM c")
	if len(cRows) != 1 || cRows[0][0].I != 100 || cRows[0][1].I != 1000 {
		t.Fatalf("level C: want (100,1000), got %+v", cRows)
	}
}

func TestForeignKeyOnUpdateWithDelete(t *testing.T) {
	e := newEngine(t)
	// 组合 1：ON DELETE CASCADE + ON UPDATE CASCADE 共存
	mustExec(t, e, "CREATE TABLE p1 (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE c1 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES p1(id) ON DELETE CASCADE ON UPDATE CASCADE)")
	mustExec(t, e, "INSERT INTO p1 (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO c1 (id, pid) VALUES (10, 1)")
	mustExec(t, e, "UPDATE p1 SET id = 100 WHERE id = 1")
	// UPDATE 级联生效后，DELETE 仍按 ON DELETE CASCADE 联动
	mustExec(t, e, "DELETE FROM p1 WHERE id = 100")
	rows1 := mustRows(t, e, "SELECT id FROM c1")
	if len(rows1) != 0 {
		t.Fatalf("delete after update cascade: want 0 rows, got %d", len(rows1))
	}
	// 组合 2：ON DELETE RESTRICT（缺省）+ ON UPDATE SET NULL
	mustExec(t, e, "CREATE TABLE p2 (id INT PRIMARY KEY)")
	mustExec(t, e, "CREATE TABLE c2 (id INT PRIMARY KEY, pid INT REFERENCES p2(id) ON UPDATE SET NULL)")
	mustExec(t, e, "INSERT INTO p2 (id) VALUES (1)")
	mustExec(t, e, "INSERT INTO c2 (id, pid) VALUES (20, 1)")
	mustExec(t, e, "UPDATE p2 SET id = 200 WHERE id = 1")
	// 子行 pid 已置 NULL，父行无引用可删除
	mustExec(t, e, "DELETE FROM p2 WHERE id = 200")
	rows2 := mustRows(t, e, "SELECT id, pid FROM c2")
	if len(rows2) != 1 || rows2[0][1].Kind != "NULL" {
		t.Fatalf("set null + delete: want 1 row with NULL pid, got %+v", rows2)
	}
}

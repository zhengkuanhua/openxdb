package sql_test

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// M9 子功能 1：在线 DDL（ALTER TABLE）单元 + 端到端测试。

func TestAlterAddColumnBackfill(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	// 已有 3 行回填默认值 0
	mustExec(t, e, "ALTER TABLE users ADD COLUMN score INT")
	rows := mustRows(t, e, "SELECT id, score FROM users ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	for i, r := range rows {
		if r[1].Kind != "INT" || r[1].I != 0 {
			t.Fatalf("row %d score: want backfilled 0, got %+v", i, r[1])
		}
	}
	// 新行按新列写入
	mustExec(t, e, "INSERT INTO users (id, name, age, score) VALUES (4, 'dave', 40, 99)")
	rows = mustRows(t, e, "SELECT score FROM users WHERE id = 4")
	if len(rows) != 1 || rows[0][0].I != 99 {
		t.Fatalf("new row score: want 99, got %+v", rows)
	}
}

func TestAlterAddColumnTypeDefaults(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t1 (id INT PRIMARY KEY)")
	mustExec(t, e, "INSERT INTO t1 (id) VALUES (1)")
	mustExec(t, e, "ALTER TABLE t1 ADD COLUMN note TEXT")
	mustExec(t, e, "ALTER TABLE t1 ADD COLUMN born DATE")
	mustExec(t, e, "ALTER TABLE t1 ADD COLUMN price DECIMAL")
	mustExec(t, e, "ALTER TABLE t1 ADD COLUMN blobcol BLOB")
	rows := mustRows(t, e, "SELECT note, born, price, blobcol FROM t1")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	r := rows[0]
	if r[0].Kind != "TEXT" || r[0].S != "" {
		t.Fatalf("TEXT default: %+v", r[0])
	}
	if r[1].Kind != "DATE" || r[1].S != "1970-01-01" {
		t.Fatalf("DATE default: %+v", r[1])
	}
	if r[2].Kind != "DECIMAL" || r[2].S != "0.0000" {
		t.Fatalf("DECIMAL default: %+v", r[2])
	}
	if r[3].Kind != "BLOB" {
		t.Fatalf("BLOB default kind: %+v", r[3])
	}
	// 重复添加同名列报错
	mustErr(t, e, "ALTER TABLE t1 ADD COLUMN note TEXT", "column already exists")
}

func TestAlterDropColumn(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	mustExec(t, e, "ALTER TABLE users DROP COLUMN age")
	rows := mustRows(t, e, "SELECT id, name FROM users ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	// 引用被删列的索引应同步移除：可重新创建同名索引
	mustExec(t, e, "CREATE INDEX idx_age ON users (name)")
	mustErr(t, e, "ALTER TABLE users DROP COLUMN id", "primary key")
	mustErr(t, e, "ALTER TABLE users DROP COLUMN nope", "unknown column")
}

func TestAlterRenameColumn(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")
	mustExec(t, e, "ALTER TABLE users RENAME COLUMN age TO years")
	rows := mustRows(t, e, "SELECT id, years FROM users WHERE years = 30")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("renamed col query: %+v", rows)
	}
	// 旧列名不可再用
	mustErr(t, e, "SELECT age FROM users", "")
	mustErr(t, e, "ALTER TABLE users RENAME COLUMN name TO years", "column already exists")
}

func TestAlterRenamePrimaryKeyColumn(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE pk1 (id INT, v TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO pk1 (id, v) VALUES (1, 'a'), (2, 'b')")
	mustExec(t, e, "ALTER TABLE pk1 RENAME COLUMN id TO uid")
	rows := mustRows(t, e, "SELECT uid FROM pk1 WHERE uid = 2")
	if len(rows) != 1 || rows[0][0].I != 2 {
		t.Fatalf("renamed PK query: %+v", rows)
	}
}

func TestAlterRenameTable(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "ALTER TABLE users RENAME TO members")
	rows := mustRows(t, e, "SELECT id FROM members WHERE id = 1")
	if len(rows) != 1 {
		t.Fatalf("renamed table query: %+v", rows)
	}
	mustErr(t, e, "SELECT id FROM users", "")
	// 目录一致：SHOW TABLES 命中新名
	any := false
	for _, r := range mustRows(t, e, "SHOW TABLES") {
		for _, v := range r {
			if strings.Contains(v.String(), "members") {
				any = true
			}
		}
	}
	if !any {
		t.Fatalf("SHOW TABLES should contain members")
	}
	mustErr(t, e, "ALTER TABLE members RENAME TO members", "table already exists")
}

func TestAlterAddDropIndex(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "ALTER TABLE users ADD INDEX idx_age (age)")
	rows := mustRows(t, e, "SHOW INDEX FROM users")
	if len(rows) != 1 {
		t.Fatalf("SHOW INDEX after ADD INDEX: want 1, got %d", len(rows))
	}
	mustExec(t, e, "ALTER TABLE users DROP INDEX idx_age")
	rows = mustRows(t, e, "SHOW INDEX FROM users")
	if len(rows) != 0 {
		t.Fatalf("SHOW INDEX after DROP INDEX: want 0, got %d", len(rows))
	}
	mustErr(t, e, "ALTER TABLE users ADD INDEX idx_age (nope)", "unknown column")
	mustErr(t, e, "ALTER TABLE users DROP INDEX nope", "index not exists")
}

func TestAlterDDLTransactionRollback(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "ALTER TABLE users ADD COLUMN score INT")
	mustExec(t, e, "ROLLBACK")
	// 回滚后新列不存在
	mustErr(t, e, "SELECT score FROM users", "")
	// 元数据仍保持 3 列：再次 ADD COLUMN 成功
	mustExec(t, e, "ALTER TABLE users ADD COLUMN score INT")
	rows := mustRows(t, e, "SELECT id, score FROM users ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("after re-add: want 3 rows, got %d", len(rows))
	}
}

func TestAlterParseLevel(t *testing.T) {
	for _, stmt := range []string{
		"ALTER TABLE users ADD COLUMN c INT",
		"ALTER TABLE users DROP COLUMN c",
		"ALTER TABLE users RENAME COLUMN c TO d",
		"ALTER TABLE users RENAME TO users2",
		"ALTER TABLE users ADD INDEX i (c)",
		"ALTER TABLE users DROP INDEX i",
	} {
		if _, err := sql.Parse(stmt); err != nil {
			t.Fatalf("Parse(%q): %v", stmt, err)
		}
	}
	if _, err := sql.Parse("ALTER TABLE users FOO COLUMN c"); err == nil {
		t.Fatalf("Parse: expected error for unsupported action")
	}
}

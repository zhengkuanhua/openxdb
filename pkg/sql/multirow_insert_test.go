package sql_test

import (
	"fmt"
	"strings"
	"testing"
)

// 多行 VALUES INSERT 执行路径单测。
// 覆盖：批量写入正确性、列数不匹配/类型错误的整条原子回滚、显式事务内多行参与事务、单行兼容。

func multiRowInsertSQL(n int, prefix string) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO t (id, name) VALUES ")
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "(%d, '%s%04d')", i, prefix, i)
	}
	return sb.String()
}

// 多行 VALUES 批量写入：单语句多行一次落库，AffectedRows 与 COUNT 一致。
func TestMultiRowInsertBatchCorrectness(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	res := mustExec(t, e, multiRowInsertSQL(100, "u"))
	if res.AffectedRows != 100 {
		t.Fatalf("batch insert: want 100 affected, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 100 {
		t.Fatalf("COUNT(*): want 100, got %d", rows[0][0].I)
	}
	// 抽查数据正确性
	got := mustRows(t, e, "SELECT id, name FROM t WHERE id = 42")
	if len(got) != 1 || got[0][0].I != 42 || got[0][1].S != "u0042" {
		t.Fatalf("row 42 mismatch: %+v", got)
	}
	got = mustRows(t, e, "SELECT id, name FROM t WHERE id = 99")
	if len(got) != 1 || got[0][0].I != 99 || got[0][1].S != "u0099" {
		t.Fatalf("row 99 mismatch: %+v", got)
	}
}

// 任一行列数不匹配 → 整条语句报错，已处理的行不得部分生效（原子性）。
func TestMultiRowInsertAtomicColumnMismatch(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	mustErr(t, e, "INSERT INTO t (id, name) VALUES (1, 'a'), (2), (3, 'c')", "column count mismatch")
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 0 {
		t.Fatalf("atomic column mismatch: want 0 rows, got %d", rows[0][0].I)
	}
	// 显式列列表与行值列数不一致同样整条失败
	mustErr(t, e, "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b', 'extra')", "column count mismatch")
	rows = mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 0 {
		t.Fatalf("atomic col count: want 0 rows, got %d", rows[0][0].I)
	}
}

// 任一行值类型错误 → 整条语句报错，无部分写入（原子性）。
func TestMultiRowInsertAtomicTypeError(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	// 第 2 行把 'abc' 放入 INT 列，第 1/3 行本可成功也必须随语句回滚
	mustErr(t, e, "INSERT INTO t (id, name) VALUES (1, 'a'), ('abc', 'b'), (3, 'c')", "type mismatch")
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 0 {
		t.Fatalf("atomic type error: want 0 rows, got %d", rows[0][0].I)
	}
}

// 多行 INSERT 在显式事务中作为一个语句参与事务：ROLLBACK 整体撤销，COMMIT 整体生效。
func TestMultiRowInsertExplicitTx(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "BEGIN")
	mustExec(t, e, multiRowInsertSQL(50, "txn"))
	// 未提交：事务内可见（同会话），随后 ROLLBACK 全部撤销
	mustExec(t, e, "ROLLBACK")
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 0 {
		t.Fatalf("rollback: want 0 rows, got %d", rows[0][0].I)
	}

	mustExec(t, e, "BEGIN")
	mustExec(t, e, multiRowInsertSQL(50, "txn"))
	mustExec(t, e, "COMMIT")
	rows = mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 50 {
		t.Fatalf("commit: want 50 rows, got %d", rows[0][0].I)
	}
	// 显式事务内多行中任一行失败 → 语句报错、事务内不残留部分行（可继续/回滚）
	mustExec(t, e, "BEGIN")
	mustErr(t, e, "INSERT INTO t (id, name) VALUES (100, 'x'), (101, 'y'), (1, 'dup')", "duplicate primary key")
	rows = mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 50 {
		t.Fatalf("txn atomic: want still 50 rows, got %d", rows[0][0].I)
	}
	mustExec(t, e, "ROLLBACK")
}

// 多行与单行混用保持语义一致（单行 INSERT 不受多行路径影响）。
func TestMultiRowInsertSingleRowCompat(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	res := mustExec(t, e, "INSERT INTO t (id, name) VALUES (1, 'solo')")
	if res.AffectedRows != 1 {
		t.Fatalf("single row: want 1 affected, got %d", res.AffectedRows)
	}
	// 多行语句内主键自冲突也整条失败（与单行重复主键一致）
	mustErr(t, e, "INSERT INTO t (id, name) VALUES (2, 'a'), (2, 'b'), (3, 'c')", "duplicate primary key")
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 1 {
		t.Fatalf("in-stmt dup: want 1 row, got %d", rows[0][0].I)
	}
}

// 多行 REPLACE INTO：冲突行被替换，无冲突行插入，原子生效。
func TestMultiRowReplaceInto(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t (id, name) VALUES (1, 'old')")
	res := mustExec(t, e, "REPLACE INTO t (id, name) VALUES (1, 'new'), (2, 'two'), (3, 'three')")
	if res.AffectedRows != 4 { // 1 替换(删1+插1=2) + 2/3 新插入(各1)
		t.Fatalf("replace multi-row: want 4 affected, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 3 {
		t.Fatalf("replace count: want 3, got %d", rows[0][0].I)
	}
	got := mustRows(t, e, "SELECT name FROM t WHERE id = 1")
	if got[0][0].S != "new" {
		t.Fatalf("replace row1: want new, got %+v", got[0][0])
	}
}

// 多行 INSERT ... ON DUPLICATE KEY UPDATE：冲突行更新、无冲突行插入。
func TestMultiRowInsertOnDupKeyUpdate(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t (id, name) VALUES (1, 'old')")
	res := mustExec(t, e, "INSERT INTO t (id, name) VALUES (1, 'upd'), (2, 'two') ON DUPLICATE KEY UPDATE name = 'upd'")
	if res.AffectedRows != 3 { // 1 冲突更新(2) + 2 新插入(1)
		t.Fatalf("on dup multi-row: want 3 affected, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 2 {
		t.Fatalf("on dup count: want 2, got %d", rows[0][0].I)
	}
	got := mustRows(t, e, "SELECT name FROM t WHERE id = 1")
	if got[0][0].S != "upd" {
		t.Fatalf("on dup row1: want upd, got %+v", got[0][0])
	}
}

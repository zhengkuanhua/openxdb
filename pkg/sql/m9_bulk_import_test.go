package sql_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// M9 子功能 3：批量导入（多行 INSERT 批处理 + IMPORT 增强：BATCH / IGNORE ERRORS）。

func writeCSV(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	return p
}

// TestM9MultiRowInsertBatch 多行 INSERT（100 行）一次批量提交，全部落库。
func TestM9MultiRowInsertBatch(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	var sb strings.Builder
	sb.WriteString("INSERT INTO t (id, name) VALUES ")
	for i := 0; i < 100; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "(%d, 'u%d')", i, i)
	}
	res := mustExec(t, e, sb.String())
	if res.AffectedRows != 100 {
		t.Fatalf("batch INSERT: want 100 affected, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 100 {
		t.Fatalf("COUNT(*): want 100, got %d", rows[0][0].I)
	}
	// 批量语句任一行主键冲突 → 整条报错（无部分生效）
	mustErr(t, e, "INSERT INTO t (id, name) VALUES (1, 'dup'), (101, 'ok')", "duplicate primary key")
	rows = mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 100 {
		t.Fatalf("atomic INSERT: want 100 rows after failed batch, got %d", rows[0][0].I)
	}
}

// TestM9ImportStrictAtomicRollback 默认严格模式：坏行整体回滚，不落任何行。
func TestM9ImportStrictAtomicRollback(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	p := writeCSV(t, "id,name\n1,alice\n2,bob\nbad,carol\n")
	mustErr(t, e, "IMPORT INTO t FROM '"+p+"'", "invalid INT")
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 0 {
		t.Fatalf("strict rollback: want 0 rows, got %d", rows[0][0].I)
	}
}

// TestM9ImportIgnoreErrors 宽松模式：坏行计数跳过继续，好行照常落库。
func TestM9ImportIgnoreErrors(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	// 好行 2 条 + 类型坏行 1 条 + 列数坏行 1 条 + 主键重复 1 条
	p := writeCSV(t, "id,name\n1,alice\n2,bob\nbad,carol\n3\n2,dup\n")
	res := mustExec(t, e, "IMPORT INTO t FROM '"+p+"' IGNORE ERRORS")
	if res.AffectedRows != 2 {
		t.Fatalf("IGNORE ERRORS: want 2 inserted, got %d", res.AffectedRows)
	}
	if len(res.Rows) != 1 || res.Rows[0][1].I != 1 {
		t.Fatalf("want skipped=1, got %+v", res.Rows)
	}
	if res.Rows[0][2].I != 2 {
		t.Fatalf("want errors=2, got %+v", res.Rows)
	}
	rows := mustRows(t, e, "SELECT id FROM t ORDER BY id")
	if len(rows) != 2 || rows[0][0].I != 1 || rows[1][0].I != 2 {
		t.Fatalf("imported rows mismatch: %+v", rows)
	}
}

// TestM9ImportBatchCommit 逐批提交策略：BATCH n 每 n 行一批落 WAL，全部插入。
func TestM9ImportBatchCommit(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	var sb strings.Builder
	sb.WriteString("id,name\n")
	for i := 0; i < 5; i++ {
		fmt.Fprintf(&sb, "%d,u%d\n", i, i)
	}
	p := writeCSV(t, sb.String())
	res := mustExec(t, e, "IMPORT INTO t FROM '"+p+"' BATCH 2")
	if res.AffectedRows != 5 {
		t.Fatalf("BATCH import: want 5 inserted, got %d", res.AffectedRows)
	}
	if len(res.Rows) != 1 || res.Rows[0][3].I != 3 {
		t.Fatalf("want 3 batches (2+2+1), got %+v", res.Rows)
	}
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 5 {
		t.Fatalf("BATCH import count: want 5, got %d", rows[0][0].I)
	}
}

// TestM9ImportBatchWithErrors 宽松 + 逐批组合：坏行跳过，批次按成功行计数。
func TestM9ImportBatchWithErrors(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	// 6 行：第 3 行坏（类型）、其余好；BATCH 2 按成功行分批 → 好行 5 条 → 2+2+1 = 3 批
	p := writeCSV(t, "id,name\n1,a\n2,b\nbad,c\n3,d\n4,e\n5,f\n")
	res := mustExec(t, e, "IMPORT INTO t FROM '"+p+"' BATCH 2 IGNORE ERRORS")
	if res.AffectedRows != 5 {
		t.Fatalf("want 5 inserted, got %d", res.AffectedRows)
	}
	if res.Rows[0][1].I != 0 || res.Rows[0][2].I != 1 || res.Rows[0][3].I != 3 {
		t.Fatalf("want skipped=0 errors=1 batches=3, got %+v", res.Rows)
	}
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t")
	if rows[0][0].I != 5 {
		t.Fatalf("want 5 rows, got %d", rows[0][0].I)
	}
}

// TestM9ImportParseOptions 语法层：IMPORT 可选后缀解析到 AST 字段。
func TestM9ImportParseOptions(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	// 非法 BATCH 值报错；未知后缀报错
	mustErr(t, e, "IMPORT INTO t FROM 'x.csv' BATCH 0", "invalid BATCH")
	mustErr(t, e, "IMPORT INTO t FROM 'x.csv' FOO", "unexpected token after IMPORT")
	// 正常路径：IGNORE ERRORS 与 BATCH 组合执行（表空，无文件则文件打开失败属于执行层）
	p := writeCSV(t, "id,name\n7,g\n")
	res := mustExec(t, e, "IMPORT INTO t FROM '"+p+"' IGNORE ERRORS BATCH 1")
	if res.AffectedRows != 1 {
		t.Fatalf("combined options: want 1 inserted, got %d", res.AffectedRows)
	}
	if res.Rows[0][3].I != 1 {
		t.Fatalf("combined options: want 1 batch, got %+v", res.Rows)
	}
	_ = sql.Value{}
}

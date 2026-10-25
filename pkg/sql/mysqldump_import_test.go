package sql_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// writeDump 将 dump 文本写入临时文件并返回路径。
func writeDump(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "dump.sql")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write dump: %v", err)
	}
	return p
}

// importRows 断言 IMPORT MYSQLDUMP 结果统计行。
func importRows(t *testing.T, res *sql.Result) (tables, inserted, skipped, errors, batches int64) {
	t.Helper()
	if len(res.Columns) != 5 || len(res.Rows) != 1 || len(res.Rows[0]) != 5 {
		t.Fatalf("import result shape: cols=%v rows=%v", res.Columns, res.Rows)
	}
	for i, c := range res.Columns {
		if res.Rows[0][i].Kind != "INT" {
			t.Fatalf("stat %s not int: %+v", c, res.Rows[0][i])
		}
	}
	return res.Rows[0][0].I, res.Rows[0][1].I, res.Rows[0][2].I, res.Rows[0][3].I, res.Rows[0][4].I
}

// mustSelectVal 执行 SELECT 并返回第一行第 col 列值。
func mustSelectVal(t *testing.T, e *sql.Engine, stmt string, col int) sql.Value {
	t.Helper()
	res := mustExec(t, e, stmt)
	if len(res.Rows) != 1 || len(res.Rows[0]) <= col {
		t.Fatalf("select %q: rows=%v", stmt, res.Rows)
	}
	return res.Rows[0][col]
}

// TestM14ImportDumpBasic 完整 dump：DROP + 多表 CREATE + 多值 INSERT + 前置 SET/注释。
func TestM14ImportDumpBasic(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, "-- MySQL dump 10.13  Distrib 8.0.36\n"+
		"/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n"+
		"SET NAMES utf8mb4;\n"+
		"DROP TABLE IF EXISTS users;\n"+
		"CREATE TABLE users (\n"+
		"  id INT NOT NULL AUTO_INCREMENT,\n"+
		"  name VARCHAR(64) DEFAULT 'guest',\n"+
		"  age INT,\n"+
		"  PRIMARY KEY (id)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"+
		"INSERT INTO users VALUES (1,'alice',30),(2,'bob',25),(3,'carol',NULL);\n"+
		"DROP TABLE IF EXISTS orders;\n"+
		"CREATE TABLE orders (\n"+
		"  id INT PRIMARY KEY,\n"+
		"  amount DECIMAL(10,2),\n"+
		"  created DATE,\n"+
		"  note BLOB\n"+
		") ENGINE=InnoDB;\n"+
		"INSERT INTO orders VALUES (1,12.50,'2024-01-15',X'48656C6C6F');\n"+
		"INSERT INTO orders VALUES (2,0.99,'2024-02-20',NULL);\n")
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"'")
	tables, inserted, skipped, errors, batches := importRows(t, res)
	if tables != 2 || inserted != 5 || skipped != 0 || errors != 0 || batches != 1 {
		t.Fatalf("stats: tables=%d inserted=%d skipped=%d errors=%d batches=%d", tables, inserted, skipped, errors, batches)
	}
	// users 表数据
	if v := mustSelectVal(t, e, "SELECT name FROM users WHERE id=1", 0); v.S != "alice" {
		t.Fatalf("users.id=1 name: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT age FROM users WHERE id=3", 0); v.Kind != "INT" || v.I != 0 {
		t.Fatalf("users.id=3 NULL age -> 0: %+v", v)
	}
	// orders 表数据：DECIMAL 按列声明 scale（DECIMAL(10,2) → 2 位）、DATE 原样、BLOB hex 大写
	if v := mustSelectVal(t, e, "SELECT amount FROM orders WHERE id=1", 0); v.S != "12.50" {
		t.Fatalf("orders.id=1 amount: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT created FROM orders WHERE id=2", 0); v.S != "2024-02-20" {
		t.Fatalf("orders.id=2 created: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT note FROM orders WHERE id=1", 0); v.S != "48656C6C6F" {
		t.Fatalf("orders.id=1 note: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT note FROM orders WHERE id=2", 0); v.S != "" {
		t.Fatalf("orders.id=2 NULL note: %+v", v)
	}
}

// TestM14ImportDumpBatch BATCH 分批提交。
func TestM14ImportDumpBatch(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, `CREATE TABLE t (id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
`)
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"' BATCH 2")
	tables, inserted, skipped, errors, batches := importRows(t, res)
	if tables != 1 || inserted != 5 || skipped != 0 || errors != 0 || batches != 3 {
		t.Fatalf("stats: tables=%d inserted=%d skipped=%d errors=%d batches=%d", tables, inserted, skipped, errors, batches)
	}
	if v := mustSelectVal(t, e, "SELECT v FROM t WHERE id=5", 0); v.S != "e" {
		t.Fatalf("t.id=5: %+v", v)
	}
}

// TestM14ImportDumpDuplicateRows 文件内重复主键跳过计数。
func TestM14ImportDumpDuplicateRows(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, `CREATE TABLE t (id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a'),(1,'dup'),(2,'b');
`)
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"'")
	_, inserted, skipped, errors, _ := importRows(t, res)
	if inserted != 2 || skipped != 1 || errors != 0 {
		t.Fatalf("stats: inserted=%d skipped=%d errors=%d", inserted, skipped, errors)
	}
}

// TestM14ImportDumpIgnoreErrors 宽松模式跳过坏行。
func TestM14ImportDumpIgnoreErrors(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, `CREATE TABLE t (id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'ok'),(2,'bad',999),(3,'fine');
`)
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"' IGNORE ERRORS")
	_, inserted, skipped, errors, _ := importRows(t, res)
	if inserted != 2 || skipped != 0 || errors != 1 {
		t.Fatalf("stats: inserted=%d skipped=%d errors=%d", inserted, skipped, errors)
	}
	if v := mustSelectVal(t, e, "SELECT v FROM t WHERE id=1", 0); v.S != "ok" {
		t.Fatalf("t.id=1: %+v", v)
	}
}

// TestM14ImportDumpStrictRollback 严格模式坏行整体回滚（表也不存在）。
func TestM14ImportDumpStrictRollback(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, `CREATE TABLE t (id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'ok'),(2,'bad',999);
`)
	_, err := e.Execute("IMPORT MYSQLDUMP FROM '" + p + "'")
	if err == nil {
		t.Fatal("want error on bad row in strict mode")
	}
	if _, err := e.Execute("SELECT v FROM t WHERE id=1"); err == nil {
		t.Fatal("strict rollback: table t should not exist")
	}
}

// TestM14ImportDumpRecreate DROP 重建语义：同名表被替换。
func TestM14ImportDumpRecreate(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
	mustExec(t, e, "INSERT INTO t VALUES (99,'old')")
	p := writeDump(t, `DROP TABLE IF EXISTS t;
CREATE TABLE t (id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'new');
`)
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"'")
	_, inserted, skipped, errors, _ := importRows(t, res)
	if inserted != 1 || skipped != 0 || errors != 0 {
		t.Fatalf("stats: inserted=%d skipped=%d errors=%d", inserted, skipped, errors)
	}
	if v := mustSelectVal(t, e, "SELECT v FROM t WHERE id=1", 0); v.S != "new" {
		t.Fatalf("t.id=1: %+v", v)
	}
	if res := mustExec(t, e, "SELECT v FROM t WHERE id=99"); len(res.Rows) != 0 {
		t.Fatalf("old row should be gone after recreate, got %v", res.Rows)
	}
}

// TestM14ImportDumpEscapes 字符串转义还原（引号/反斜杠/换行）。
func TestM14ImportDumpEscapes(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, `CREATE TABLE t (id INT PRIMARY KEY, s TEXT);
INSERT INTO t VALUES (1,'it\'s'),(2,'line1\nline2'),(3,'back\\slash');
`)
	mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"'")
	if v := mustSelectVal(t, e, "SELECT s FROM t WHERE id=1", 0); v.S != "it's" {
		t.Fatalf("id=1: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT s FROM t WHERE id=2", 0); v.S != "line1\nline2" {
		t.Fatalf("id=2: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT s FROM t WHERE id=3", 0); v.S != "back\\slash" {
		t.Fatalf("id=3: %+v", v)
	}
}

// TestM14ImportDumpParser 语法分支：IMPORT MYSQLDUMP 与旧 CSV 语法互不影响。
func TestM14ImportDumpParser(t *testing.T) {
	e := newEngine(t)
	// 空 dump 文件：合法导入（0 表 0 行）
	p := writeDump(t, "-- just a comment\n")
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"'")
	tables, inserted, _, errors, batches := importRows(t, res)
	if tables != 0 || inserted != 0 || errors != 0 || batches != 1 {
		t.Fatalf("empty dump stats: tables=%d inserted=%d errors=%d batches=%d", tables, inserted, errors, batches)
	}
	// MYSQLDUMP 后缺 FROM 报语法错误
	p2 := writeDump(t, "CREATE TABLE t (id INT);")
	if _, err := e.Execute("IMPORT MYSQLDUMP " + p2); err == nil {
		t.Fatal("want syntax error: MYSQLDUMP without FROM")
	}
}

// TestM14ImportDumpExtendedTypes 扩展类型与字面量端到端导入：
// 位字面量/字符集引导符/科学计数法/SERIAL/UNSIGNED/空间类型。
func TestM14ImportDumpExtendedTypes(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, `CREATE TABLE t (
  id INT PRIMARY KEY,
  flag BIT(8),
  big BIGINT UNSIGNED,
  seq SERIAL,
  loc POINT,
  raw BLOB,
  note TEXT,
  ratio DOUBLE
) ENGINE=InnoDB;
INSERT INTO t VALUES (1, b'1010', 9999999999, 100, 'POINT(1 2)', _binary'rawdata', _utf8mb4'你好', 1.5E-3);
INSERT INTO t VALUES (2, B'11111111', 42, 200, NULL, X'00FF', '科学 1e5 数值', 2E+06);
`)
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"'")
	tables, inserted, skipped, errors, batches := importRows(t, res)
	if tables != 1 || inserted != 2 || skipped != 0 || errors != 0 || batches != 1 {
		t.Fatalf("stats: tables=%d inserted=%d skipped=%d errors=%d batches=%d", tables, inserted, skipped, errors, batches)
	}
	// id=1：BIT 位字面量转十进制、UNSIGNED BIGINT、SERIAL、空间文本、引导符字符串、科学计数法 DECIMAL
	if v := mustSelectVal(t, e, "SELECT flag FROM t WHERE id=1", 0); v.S != "10" {
		t.Fatalf("id=1 flag: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT big FROM t WHERE id=1", 0); v.Kind != "INT" || v.I != 9999999999 {
		t.Fatalf("id=1 big: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT seq FROM t WHERE id=1", 0); v.Kind != "INT" || v.I != 100 {
		t.Fatalf("id=1 seq: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT loc FROM t WHERE id=1", 0); v.S != "POINT(1 2)" {
		t.Fatalf("id=1 loc: %+v", v)
	}
	// BLOB 列统一承载大写 hex 文本：_binary'rawdata' 大小写被规范化（encodeRow 层设计）
	if v := mustSelectVal(t, e, "SELECT raw FROM t WHERE id=1", 0); v.S != "RAWDATA" {
		t.Fatalf("id=1 raw: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT note FROM t WHERE id=1", 0); v.S != "你好" {
		t.Fatalf("id=1 note: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT ratio FROM t WHERE id=1", 0); v.S != "0.0015" {
		t.Fatalf("id=1 ratio: %+v", v)
	}
	// id=2：大写位字面量、hex BLOB、科学计数法 DECIMAL、NULL 空间列
	if v := mustSelectVal(t, e, "SELECT flag FROM t WHERE id=2", 0); v.S != "255" {
		t.Fatalf("id=2 flag: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT raw FROM t WHERE id=2", 0); v.S != "00FF" {
		t.Fatalf("id=2 raw: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT note FROM t WHERE id=2", 0); v.S != "科学 1e5 数值" {
		t.Fatalf("id=2 note: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT ratio FROM t WHERE id=2", 0); v.S != "2000000.0000" {
		t.Fatalf("id=2 ratio: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT loc FROM t WHERE id=2", 0); v.S != "" {
		t.Fatalf("id=2 NULL loc: %+v", v)
	}
}

// ---- P2-12 备份/导入并行化：mysqldump 表级并行导入（CONCURRENCY）----

// parallelDump 构造多表 dump：3 表 × 每表 6 行。
func parallelDump() string {
	return "-- MySQL dump 10.13\n" +
		"DROP TABLE IF EXISTS ta;\n" +
		"CREATE TABLE ta (id INT PRIMARY KEY, v TEXT);\n" +
		"INSERT INTO ta VALUES (1,'a1'),(2,'a2'),(3,'a3'),(4,'a4'),(5,'a5'),(6,'a6');\n" +
		"DROP TABLE IF EXISTS tb;\n" +
		"CREATE TABLE tb (id INT PRIMARY KEY, v TEXT);\n" +
		"INSERT INTO tb VALUES (1,'b1'),(2,'b2'),(3,'b3'),(4,'b4'),(5,'b5'),(6,'b6');\n" +
		"DROP TABLE IF EXISTS tc;\n" +
		"CREATE TABLE tc (id INT PRIMARY KEY, v TEXT);\n" +
		"INSERT INTO tc VALUES (1,'c1'),(2,'c2'),(3,'c3'),(4,'c4'),(5,'c5'),(6,'c6');\n"
}

// TestP2ImportDumpParallelMultiTable 多表并行导入：3 表并发灌入，统计与数据正确。
func TestP2ImportDumpParallelMultiTable(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, parallelDump())
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"' BATCH 2 CONCURRENCY 4")
	tables, inserted, skipped, errors, batches := importRows(t, res)
	if tables != 3 || inserted != 18 || skipped != 0 || errors != 0 {
		t.Fatalf("stats: tables=%d inserted=%d skipped=%d errors=%d batches=%d", tables, inserted, skipped, errors, batches)
	}
	// 每表 6 行按 BATCH 2 分 3 批（3 表共 9 批）
	if batches != 9 {
		t.Fatalf("batches: want 9, got %d", batches)
	}
	for _, tc := range []struct{ tbl, want string }{
		{"ta", "a6"}, {"tb", "b6"}, {"tc", "c6"},
	} {
		if v := mustSelectVal(t, e, "SELECT v FROM "+tc.tbl+" WHERE id=6", 0); v.S != tc.want {
			t.Fatalf("%s.id=6: want %s, got %+v", tc.tbl, tc.want, v)
		}
	}
}

// TestP2ImportDumpParallelConsistent 并行与串行导入结果一致（行集全等）。
func TestP2ImportDumpParallelConsistent(t *testing.T) {
	ser := newEngine(t)
	p := writeDump(t, parallelDump())
	mustExec(t, ser, "IMPORT MYSQLDUMP FROM '"+p+"' BATCH 2")
	par := newEngine(t)
	res := mustExec(t, par, "IMPORT MYSQLDUMP FROM '"+p+"' BATCH 2 CONCURRENCY 8")
	if _, inserted, skipped, errors, _ := importRows(t, res); inserted != 18 || skipped != 0 || errors != 0 {
		t.Fatalf("parallel stats: inserted=%d skipped=%d errors=%d", inserted, skipped, errors)
	}
	for _, tbl := range []string{"ta", "tb", "tc"} {
		want := mustRows(t, ser, "SELECT id, v FROM "+tbl+" ORDER BY id")
		got := mustRows(t, par, "SELECT id, v FROM "+tbl+" ORDER BY id")
		if len(want) != len(got) {
			t.Fatalf("%s rows: want %d, got %d", tbl, len(want), len(got))
		}
		for i := range want {
			if want[i][0].I != got[i][0].I || want[i][1].S != got[i][1].S {
				t.Fatalf("%s row %d: want %+v, got %+v", tbl, i, want[i], got[i])
			}
		}
	}
}

// TestP2ImportDumpParallelStrictError 严格模式：坏行导致该表批次回滚并停止，整体返回错误并报告部分完成。
// 表 ta 灌入成功（批次已提交保留），表 tb 含坏行的批次整批回滚且该表停止，表 tc 未处理（错误优先返回）。
func TestP2ImportDumpParallelStrictError(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, "CREATE TABLE ta (id INT PRIMARY KEY, v TEXT);\n"+
		"INSERT INTO ta VALUES (1,'a1'),(2,'a2'),(3,'a3'),(4,'a4'),(5,'a5'),(6,'a6');\n"+
		"CREATE TABLE tb (id INT PRIMARY KEY, v TEXT);\n"+
		"INSERT INTO tb VALUES (1,'b1'),('oops','bad'),(3,'b3');\n"+
		"CREATE TABLE tc (id INT PRIMARY KEY, v TEXT);\n"+
		"INSERT INTO tc VALUES (1,'c1');\n")
	res, err := e.Execute("IMPORT MYSQLDUMP FROM '" + p + "' BATCH 2 CONCURRENCY 3")
	if err == nil {
		t.Fatalf("want strict error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid INT") {
		t.Fatalf("want invalid INT error, got %v", err)
	}
	if res == nil {
		t.Fatalf("want partial stats, got nil result")
	}
	// 部分完成：3 表全部 CREATE 成功；ta 数据完整（6 行已提交），tb 批次回滚（无行），tc 未灌入
	tables, inserted, skipped, errors, _ := importRows(t, res)
	if tables != 3 || inserted < 6 || skipped != 0 || errors != 0 {
		t.Fatalf("partial stats: tables=%d inserted=%d skipped=%d errors=%d", tables, inserted, skipped, errors)
	}
	if v := mustSelectVal(t, e, "SELECT v FROM ta WHERE id=6", 0); v.S != "a6" {
		t.Fatalf("ta.id=6 preserved: %+v", v)
	}
	// tb 含坏行的批次整体回滚，表停止
	if rows := mustRows(t, e, "SELECT id FROM tb"); len(rows) != 0 {
		t.Fatalf("tb should be empty (batch rolled back), got %d rows", len(rows))
	}
	// tc 已提交的数据保留（错误优先返回，不强制回滚其他 worker 的已完成批次）
	if rows := mustRows(t, e, "SELECT id FROM tc"); len(rows) > 1 {
		t.Fatalf("tc rows: want <=1, got %d", len(rows))
	}
}

// TestP2ImportDumpParallelIgnoreErrors IgnoreErrors 模式：坏行计数跳过，全部表完成。
func TestP2ImportDumpParallelIgnoreErrors(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, "CREATE TABLE ta (id INT PRIMARY KEY, v TEXT);\n"+
		"INSERT INTO ta VALUES (1,'a1'),(2,'a2'),(3,'a3');\n"+
		"CREATE TABLE tb (id INT PRIMARY KEY, v TEXT);\n"+
		"INSERT INTO tb VALUES (1,'b1'),('oops','bad'),(3,'b3');\n")
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"' BATCH 2 CONCURRENCY 4 IGNORE ERRORS")
	tables, inserted, skipped, errors, _ := importRows(t, res)
	if tables != 2 || inserted != 5 || skipped != 0 || errors != 1 {
		t.Fatalf("stats: tables=%d inserted=%d skipped=%d errors=%d", tables, inserted, skipped, errors)
	}
	if v := mustSelectVal(t, e, "SELECT v FROM tb WHERE id=3", 0); v.S != "b3" {
		t.Fatalf("tb.id=3: %+v", v)
	}
}

// TestP2ImportDumpParallelDuplicateRows 文件内重复主键在并行导入下同样计数跳过。
func TestP2ImportDumpParallelDuplicateRows(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT);\n"+
		"INSERT INTO t VALUES (1,'a'),(1,'dup'),(2,'b');\n")
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"' BATCH 2 CONCURRENCY 4")
	tables, inserted, skipped, errors, _ := importRows(t, res)
	if tables != 1 || inserted != 2 || skipped != 1 || errors != 0 {
		t.Fatalf("stats: tables=%d inserted=%d skipped=%d errors=%d", tables, inserted, skipped, errors)
	}
}

// TestP2ImportDumpParallelRequiresBatch CONCURRENCY>1 且无 BATCH：拒绝并行（原子性不可破坏）。
func TestP2ImportDumpParallelRequiresBatch(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, parallelDump())
	mustErr(t, e, "IMPORT MYSQLDUMP FROM '"+p+"' CONCURRENCY 2", "CONCURRENCY requires BATCH")
}

// TestP2ImportDumpParallelNoExplicitTx 显式事务内禁止并行导入。
func TestP2ImportDumpParallelNoExplicitTx(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, parallelDump())
	mustExec(t, e, "BEGIN")
	mustErr(t, e, "IMPORT MYSQLDUMP FROM '"+p+"' BATCH 2 CONCURRENCY 2", "cannot run inside an explicit transaction")
	mustExec(t, e, "ROLLBACK")
}

// TestP2ImportDumpConcurrencySyntax CONCURRENCY 仅 MYSQLDUMP 接受；CSV 导入拒绝。
func TestP2ImportDumpConcurrencySyntax(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT);\nINSERT INTO t VALUES (1,'a');\n")
	mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"' BATCH 1 CONCURRENCY 1") // 并发度 1 = 串行
	csv := filepath.Join(t.TempDir(), "d.csv")
	if err := os.WriteFile(csv, []byte("1,a\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	mustExec(t, e, "CREATE TABLE c (id INT PRIMARY KEY, v TEXT)")
	mustErr(t, e, "IMPORT INTO c FROM '"+csv+"' CONCURRENCY 2", "CONCURRENCY is only supported for IMPORT MYSQLDUMP")
}

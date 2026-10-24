package sql_test

// v1.6.0 P1-2（T44）sql 侧测试：SHOW MASTER STATUS 位点语义（未启用/空/写后），
// 以及端到端整库 dump（CREATE DATABASE + USE + db.tbl 前缀）导入执行。

import (
	"strconv"
	"testing"
)

// TestT44ShowMasterStatus SHOW MASTER STATUS 五列输出与 Position=最后 LSN+1。
func TestT44ShowMasterStatus(t *testing.T) {
	d := openDB(t)
	e := d.SQL

	// 未启用复制：报错。
	mustErr(t, e, "SHOW MASTER STATUS", "binlog")

	// 启用复制：空 binlog 位点 1（下一写位点）。
	if err := d.StartReplication("127.0.0.1:0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	res := mustExec(t, e, "SHOW MASTER STATUS")
	wantCols := []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
	if len(res.Columns) != len(wantCols) {
		t.Fatalf("columns = %v, want %v", res.Columns, wantCols)
	}
	for i, c := range wantCols {
		if res.Columns[i] != c {
			t.Fatalf("column %d = %q, want %q", i, res.Columns[i], c)
		}
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(res.Rows))
	}
	if res.Rows[0][0].String() != "openxdb-binlog" {
		t.Fatalf("File = %q, want openxdb-binlog", res.Rows[0][0].String())
	}
	if res.Rows[0][1].Kind != "INT" || res.Rows[0][1].I != 1 {
		t.Fatalf("Position = %+v, want 1", res.Rows[0][1])
	}

	// 写入后：Position = 最后事件 LSN + 1。
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
	mustExec(t, e, "INSERT INTO t VALUES (1,'a'),(2,'b')")
	rows := showRows(t, e, "SHOW BINLOG EVENTS")
	if len(rows) == 0 {
		t.Fatalf("no binlog events after insert")
	}
	lastLSN := rows[len(rows)-1][0].I
	res2 := mustExec(t, e, "SHOW MASTER STATUS")
	if res2.Rows[0][1].I != lastLSN+1 {
		t.Fatalf("Position = %d, want %d (last LSN + 1)", res2.Rows[0][1].I, lastLSN+1)
	}
}

// TestT44ImportDatabaseLevelDump 端到端整库 dump：CREATE DATABASE/USE/db.tbl 前缀
// 均正确落地，Skipped 统计覆盖 CREATE DATABASE 与 USE。
func TestT44ImportDatabaseLevelDump(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, "-- MySQL dump\n"+
		"CREATE DATABASE IF NOT EXISTS `shop` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;\n"+
		"USE `shop`;\n"+
		"DROP TABLE IF EXISTS `shop`.`old_t`;\n"+
		"CREATE TABLE `shop`.`items` (id INT PRIMARY KEY, name TEXT);\n"+
		"INSERT INTO `shop`.`items` VALUES (1,'pen'),(2,'book');\n")
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"'")
	tables, inserted, skipped, errors, batches := importRows(t, res)
	// 辅助语句（CREATE DATABASE/USE）由解析层 Skipped 计数，执行器统计不含。
	if tables != 1 || inserted != 2 || skipped != 0 || errors != 0 || batches != 1 {
		t.Fatalf("stats: tables=%d inserted=%d skipped=%d errors=%d batches=%d", tables, inserted, skipped, errors, batches)
	}
	if v := mustSelectVal(t, e, "SELECT name FROM items WHERE id=1", 0); v.S != "pen" {
		t.Fatalf("items.id=1 name: %+v", v)
	}
	if v := mustSelectVal(t, e, "SELECT name FROM items WHERE id=2", 0); v.S != "book" {
		t.Fatalf("items.id=2 name: %+v", v)
	}
}

// TestT44ImportDatabaseLevelDumpBare 裸前缀（非反引号）形式同样剥离。
func TestT44ImportDatabaseLevelDumpBare(t *testing.T) {
	e := newEngine(t)
	p := writeDump(t, "CREATE DATABASE shop DEFAULT CHARACTER SET utf8mb4;\n"+
		"USE shop;\n"+
		"CREATE TABLE shop.t (id INT PRIMARY KEY, v INT);\n"+
		"INSERT INTO shop.t VALUES (7,70);\n")
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"'")
	tables, inserted, skipped, errors, _ := importRows(t, res)
	// 辅助语句（CREATE DATABASE/USE）由解析层 Skipped 计数，执行器统计不含。
	if tables != 1 || inserted != 1 || skipped != 0 || errors != 0 {
		t.Fatalf("stats: tables=%d inserted=%d skipped=%d errors=%d", tables, inserted, skipped, errors)
	}
	if v := mustSelectVal(t, e, "SELECT v FROM t WHERE id=7", 0); v.I != 70 {
		t.Fatalf("t.id=7 v: %+v", v)
	}
}

// TestT44ShowMasterStatusPositionRoundTrip 位点与 COM_BINLOG_DUMP 参数一致性：
// SHOW MASTER STATUS 的 Position 可直接作为 dump 起点（越过末尾为空）。
func TestT44ShowMasterStatusPositionRoundTrip(t *testing.T) {
	d := openDB(t)
	e := d.SQL
	if err := d.StartReplication("127.0.0.1:0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
	mustExec(t, e, "INSERT INTO t VALUES (1,'a')")
	res := mustExec(t, e, "SHOW MASTER STATUS")
	pos := res.Rows[0][1].I
	// Position（LSN+1）作为 FROM 起点越过末尾：0 条事件。
	rows := showRows(t, e, "SHOW BINLOG EVENTS FROM "+strconv.FormatInt(pos, 10))
	if len(rows) != 0 {
		t.Fatalf("FROM position rows = %d, want 0", len(rows))
	}
}

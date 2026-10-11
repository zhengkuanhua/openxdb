package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// openDB 打开一个真实引擎（含 binlog 路径注入，但未启用复制）。
func openDB(t *testing.T) *db.DB {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// showRows 执行 SHOW BINLOG EVENTS 并校验列头，返回事件行。
func showRows(t *testing.T, e *sql.Engine, stmt string) [][]sql.Value {
	t.Helper()
	res := mustExec(t, e, stmt)
	wantCols := []string{"lsn", "commit_lsn", "schema_ver", "kind", "table", "pk", "row"}
	if len(res.Columns) != len(wantCols) {
		t.Fatalf("%s columns = %v, want %v", stmt, res.Columns, wantCols)
	}
	for i, c := range wantCols {
		if res.Columns[i] != c {
			t.Fatalf("%s column %d = %q, want %q", stmt, i, res.Columns[i], c)
		}
	}
	return res.Rows
}

// TestM14ShowBinlogEvents 多表变更 + LSN 续读 + 复制开启共存：
// 写入多条记录后按全量 / FROM / LIMIT 三种视角读取变更流。
func TestM14ShowBinlogEvents(t *testing.T) {
	d := openDB(t)
	e := d.SQL

	mustExec(t, e, "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "CREATE TABLE t2 (id INT PRIMARY KEY, v INT)")
	// 启用主写复制（binlog 挂载）。SHOW BINLOG EVENTS 与之共存读取。
	if err := d.StartReplication("127.0.0.1:0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}

	// 多表变更：t1 两行、t2 一行、t1 再删一行。
	mustExec(t, e, "INSERT INTO t1 VALUES (1,'alice'),(2,'bob')")
	mustExec(t, e, "INSERT INTO t2 VALUES (7,70)")
	mustExec(t, e, "INSERT INTO t1 VALUES (3,'carol')")
	mustExec(t, e, "DELETE FROM t1 WHERE id=3")

	// 全量拉取：4 条事务记录、5 条事件（多行 INSERT 每行 1 事件），表名按表目录还原。
	rows := showRows(t, e, "SHOW BINLOG EVENTS")
	if len(rows) != 5 {
		t.Fatalf("SHOW BINLOG EVENTS rows = %d, want 5: %+v", len(rows), rows)
	}
	if rows[0][3].String() != "PUT" || rows[0][4].String() != "t1" || rows[0][6].String() == "" {
		t.Fatalf("row0 = %+v, want PUT t1 with decoded row", rows[0])
	}
	if rows[1][4].String() != "t1" || rows[2][4].String() != "t2" {
		t.Fatalf("row1/row2 table = %q/%q, want t1/t2", rows[1][4].String(), rows[2][4].String())
	}
	// 删除事件
	if rows[4][3].String() != "DELETE" || rows[4][4].String() != "t1" {
		t.Fatalf("row4 = %+v, want DELETE t1", rows[4])
	}
	// LSN 单调非递减（同事务多条事件共享记录 LSN，允许相等，禁止回退）
	for i := 1; i < len(rows); i++ {
		if rows[i][0].I < rows[i-1][0].I {
			t.Fatalf("LSN went backwards: %d -> %d", rows[i-1][0].I, rows[i][0].I)
		}
	}
	firstLSN := rows[0][0].I
	lastLSN := rows[len(rows)-1][0].I

	// LIMIT 分页：只取前 2 条。
	paged := showRows(t, e, "SHOW BINLOG EVENTS LIMIT 2")
	if len(paged) != 2 {
		t.Fatalf("LIMIT 2 rows = %d, want 2", len(paged))
	}

	// FROM 续读：从首条 LSN 起读取 == 全量；越过末尾为空。
	fromAll := showRows(t, e, "SHOW BINLOG EVENTS FROM "+itoa(firstLSN))
	if len(fromAll) != 5 {
		t.Fatalf("FROM first rows = %d, want 5", len(fromAll))
	}
	past := showRows(t, e, "SHOW BINLOG EVENTS FROM "+itoa(lastLSN+1))
	if len(past) != 0 {
		t.Fatalf("FROM past-end rows = %d, want 0", len(past))
	}

	// 与复制路径共存：SHOW 后继续写入仍正常，新事件立即可见。
	mustExec(t, e, "INSERT INTO t1 VALUES (4,'dora')")
	rows2 := showRows(t, e, "SHOW BINLOG EVENTS FROM "+itoa(lastLSN+1))
	if len(rows2) != 1 || rows2[0][4].String() != "t1" {
		t.Fatalf("after append rows = %+v, want 1 t1 event", rows2)
	}
}

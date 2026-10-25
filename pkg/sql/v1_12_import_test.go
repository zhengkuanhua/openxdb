package sql_test

import (
	"os"
	"path/filepath"
	"testing"
)

// v1.12.0 备份/导入提速与 DECIMAL 高精度联动：
//  1. CSV IMPORT 按列声明 scale 解析 DECIMAL（此前固定 4 位拒绝超精度值）；
//  2. BATCH 语义强化为按"数据行数"分批（此前按 kv 操作数，宽表多索引下批偏小）；
//  3. mysqldump 导入还原 DECIMAL(p,s) 声明精度（此前解析层丢弃精度）。

// TestV112CSVImportDecimalHighPrecision CSV IMPORT 高精度 DECIMAL 联动。
func TestV112CSVImportDecimalHighPrecision(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE ci (id INT, price DECIMAL(12,6), PRIMARY KEY (id))")
	p := filepath.Join(t.TempDir(), "hp.csv")
	if err := os.WriteFile(p, []byte("id,price\n1,123.456789\n2,-0.000001\n3,1.5\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	res := mustExec(t, e, "IMPORT INTO ci FROM '"+p+"'")
	if res.AffectedRows != 3 {
		t.Fatalf("import: want 3 inserted, got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT id, price FROM ci ORDER BY id")
	want := []string{"123.456789", "-0.000001", "1.500000"}
	if len(rows) != len(want) {
		t.Fatalf("rows: got %d, want %d (%+v)", len(rows), len(want), rows)
	}
	for i, w := range want {
		if rows[i][1].Kind != "DECIMAL" || rows[i][1].S != w {
			t.Fatalf("row %d price: want %s got %+v", i, w, rows[i][1])
		}
	}
}

// TestV112CSVImportBatchByRows BATCH 按数据行数分批：宽表（多索引）下单行产生多组
// kv 操作，此前按操作数会过早触发提交；现在 BATCH 3 应为 3+3+1=3 批。
func TestV112CSVImportBatchByRows(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE bw (id INT, a INT, b INT, c INT, PRIMARY KEY (id), KEY idx_a (a), KEY idx_b (b))")
	var sb []byte
	sb = append(sb, []byte("id,a,b,c\n")...)
	for i := 0; i < 7; i++ {
		sb = append(sb, []byte{byte('0' + i), ',', byte('0' + i), ',', byte('0' + i), ',', byte('0' + i), '\n'}...)
	}
	p := filepath.Join(t.TempDir(), "batch.csv")
	if err := os.WriteFile(p, sb, 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	res := mustExec(t, e, "IMPORT INTO bw FROM '"+p+"' BATCH 3")
	if res.AffectedRows != 7 {
		t.Fatalf("import: want 7 inserted, got %d", res.AffectedRows)
	}
	// Rows[0] 为统计行：inserted, skipped, errors, batches
	if len(res.Rows) != 1 || res.Rows[0][3].I != 3 {
		t.Fatalf("BATCH by rows: want 3 batches (3+3+1), got %+v", res.Rows)
	}
	rows := mustRows(t, e, "SELECT COUNT(*) FROM bw")
	if rows[0][0].I != 7 {
		t.Fatalf("count: want 7, got %d", rows[0][0].I)
	}
}

// TestV112DumpImportDecimalHighPrecision mysqldump 导入还原 DECIMAL(p,s) 精度。
func TestV112DumpImportDecimalHighPrecision(t *testing.T) {
	e := newEngine(t)
	content := "CREATE TABLE `dmp` (`id` INT PRIMARY KEY, `price` DECIMAL(10,6));\n" +
		"INSERT INTO `dmp` VALUES (1, 123.456789), (2, -0.000001);\n"
	p := filepath.Join(t.TempDir(), "hp.sql")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write dump: %v", err)
	}
	res := mustExec(t, e, "IMPORT MYSQLDUMP FROM '"+p+"'")
	tables, inserted, _, _, _ := importRows(t, res)
	if tables != 1 || inserted != 2 {
		t.Fatalf("dump import: tables=%d inserted=%d", tables, inserted)
	}
	rows := mustRows(t, e, "SELECT id, price FROM dmp ORDER BY id")
	want := []string{"123.456789", "-0.000001"}
	if len(rows) != len(want) {
		t.Fatalf("rows: got %d, want %d (%+v)", len(rows), len(want), rows)
	}
	for i, w := range want {
		if rows[i][1].Kind != "DECIMAL" || rows[i][1].S != w {
			t.Fatalf("row %d price: want %s got %+v", i, w, rows[i][1])
		}
	}
}

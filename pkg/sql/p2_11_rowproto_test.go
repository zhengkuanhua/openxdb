package sql_test

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// 行协议 NULL 标记与转义（与 pkg/sql/csv.go 的 nullToken / escapedNullTok 对应）。
const (
	nullToken      = "\\N"
	escapedNullTok = "\\\\N"
)

// readCSV 读取 CSV 文件全部记录（测试辅助）。
func readCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open csv: %v", err)
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	return recs
}

// compareRows 逐行逐列按 String() 对比两组查询结果（测试辅助）。
func compareRows(t *testing.T, a, b [][]sql.Value) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("row count mismatch: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			t.Fatalf("row %d col count mismatch: %d vs %d", i, len(a[i]), len(b[i]))
		}
		for j := range a[i] {
			if a[i][j].String() != b[i][j].String() {
				t.Fatalf("row %d col %d mismatch: %q vs %q", i, j, a[i][j].String(), b[i][j].String())
			}
		}
	}
}

// P2-11-1：行协议 NULL/空串往返。
// NULL 单元格 = \N；TEXT 空串 = 空字段；导入→导出→导入全程保持区分，
// 且导出文件中 NULL 恒为 \N、空串恒为空字段。
func TestP211RowProtoNullEmptyRoundtrip(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, txt TEXT, num DECIMAL, d DATE, b BLOB, PRIMARY KEY (id))")

	// 构造 CSV：\N 表示 NULL，空字段表示 TEXT 空串
	src := filepath.Join(t.TempDir(), "null.csv")
	body := "id,txt,num,d,b\n" +
		"1,alice,123.4500,2024-03-15,0A1B\n" +
		"2,\\N,\\N,\\N,\\N\n" +
		"3,,55.2500,2024-01-01,FF\n" +
		"4,plain,1.2345,2023-06-30,00\n"
	if err := os.WriteFile(src, []byte(body), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	mustExec(t, e, "IMPORT INTO t FROM '"+src+"'")

	// 源表语义：行2 各列 NULL；行3 txt 为空串（非 NULL）
	rows := mustRows(t, e, "SELECT * FROM t ORDER BY id")
	if len(rows) != 4 {
		t.Fatalf("rows: got %d", len(rows))
	}
	if rows[1][1].Kind != "NULL" || rows[1][2].Kind != "NULL" || rows[1][3].Kind != "NULL" || rows[1][4].Kind != "NULL" {
		t.Fatalf("row2 should be all NULL, got %+v", rows[1])
	}
	if rows[2][1].Kind != "TEXT" || rows[2][1].S != "" {
		t.Fatalf("row3 txt should be empty string, got %+v", rows[2][1])
	}

	// 导出：NULL → \N，空串 → 空字段
	out := filepath.Join(t.TempDir(), "out.csv")
	mustExec(t, e, "EXPORT TABLE t TO '"+out+"'")
	recs := readCSV(t, out)
	if len(recs) != 5 {
		t.Fatalf("csv lines: got %d", len(recs))
	}
	if recs[2][1] != nullToken || recs[2][2] != nullToken || recs[2][3] != nullToken || recs[2][4] != nullToken {
		t.Fatalf("row2 NULL cells: got %v", recs[2])
	}
	if recs[3][1] != "" {
		t.Fatalf("row3 empty-string cell: got %q", recs[3][1])
	}

	// 再导入 → 与源表逐行一致
	mustExec(t, e, "CREATE TABLE t2 (id INT, txt TEXT, num DECIMAL, d DATE, b BLOB, PRIMARY KEY (id))")
	mustExec(t, e, "IMPORT INTO t2 FROM '"+out+"'")
	rows2 := mustRows(t, e, "SELECT * FROM t2 ORDER BY id")
	compareRows(t, rows, rows2)

	// 确认 t2 中 NULL/空串仍区分
	if rows2[1][1].Kind != "NULL" || rows2[2][1].S != "" {
		t.Fatalf("null/empty distinction lost after re-import")
	}
}

// P2-11-2：字面字符串 "\N" 的转义往返。
// TEXT 值为 \N 时导出为 \\N、导入还原为 \N，不误判为 NULL 标记。
func TestP211RowProtoLiteralNullToken(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, txt TEXT, PRIMARY KEY (id))")

	// CSV 单元格 \\N（两个反斜杠 + N）= 字面 "\N"
	src := filepath.Join(t.TempDir(), "lit.csv")
	if err := os.WriteFile(src, []byte("id,txt\n1,\\\\N\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	mustExec(t, e, "IMPORT INTO t FROM '"+src+"'")
	rows := mustRows(t, e, "SELECT * FROM t")
	if len(rows) != 1 || rows[0][1].Kind != "TEXT" || rows[0][1].S != nullToken {
		t.Fatalf("literal null token: got %+v", rows[0][1])
	}

	// 导出：单元格为 \\N
	out := filepath.Join(t.TempDir(), "out.csv")
	mustExec(t, e, "EXPORT TABLE t TO '"+out+"'")
	recs := readCSV(t, out)
	if recs[1][1] != escapedNullTok {
		t.Fatalf("escaped cell: got %q, want %q", recs[1][1], escapedNullTok)
	}

	// 再导入：还原为字面 "\N"，而非 NULL
	mustExec(t, e, "CREATE TABLE t2 (id INT, txt TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "IMPORT INTO t2 FROM '"+out+"'")
	rows2 := mustRows(t, e, "SELECT * FROM t2")
	if rows2[0][1].Kind != "TEXT" || rows2[0][1].S != nullToken {
		t.Fatalf("literal null token lost after re-import: got %+v", rows2[0][1])
	}
}

// P2-11-3：单元格内换行（RFC 4180 多行字段）往返。
// 含换行/逗号/引号/回车的 TEXT 经 导入→导出→导入 全程保真；
// 导出文件对含换行字段使用双引号包裹且换行原样保留。
func TestP211RowProtoEmbeddedNewline(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, txt TEXT, PRIMARY KEY (id))")

	src := filepath.Join(t.TempDir(), "nl.csv")
	f, err := os.Create(src)
	if err != nil {
		t.Fatalf("create csv: %v", err)
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"id", "txt"})
	_ = w.Write([]string{"1", "line1\nline2\"quoted\""})
	_ = w.Write([]string{"2", "a,b"})
	_ = w.Write([]string{"3", "x\ry"})
	_ = w.Write([]string{"4", "single"})
	w.Flush()
	f.Close()

	mustExec(t, e, "IMPORT INTO t FROM '"+src+"'")
	rows := mustRows(t, e, "SELECT * FROM t ORDER BY id")
	want := []string{"line1\nline2\"quoted\"", "a,b", "x\ry", "single"}
	if len(rows) != len(want) {
		t.Fatalf("rows: got %d", len(rows))
	}
	for i, v := range want {
		if rows[i][1].Kind != "TEXT" || rows[i][1].S != v {
			t.Fatalf("row %d text: got %q, want %q", i+1, rows[i][1].S, v)
		}
	}

	// 导出文件：含换行字段被引号包裹、内部引号翻倍、换行保留
	out := filepath.Join(t.TempDir(), "out.csv")
	mustExec(t, e, "EXPORT TABLE t TO '"+out+"'")
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read out: %v", err)
	}
	if !strings.Contains(string(raw), "\"line1\nline2\"\"quoted\"\"\"") {
		t.Fatalf("newline field not quoted-preserved: %q", string(raw))
	}

	// 再导入 → 与源表逐行一致
	mustExec(t, e, "CREATE TABLE t2 (id INT, txt TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "IMPORT INTO t2 FROM '"+out+"'")
	rows2 := mustRows(t, e, "SELECT * FROM t2 ORDER BY id")
	compareRows(t, rows, rows2)
}

// P2-11-4：DECIMAL 精度语义。
// ① ≤4 位小数 导入→导出→导入 无损往返（导出恒为 4 位规范文本）；
// ② 超 4 位小数导入报错（原子回滚，不静默四舍五入）；
// ③ IGNORE ERRORS 时跳过超精度行，其余正常导入。
func TestP211RowProtoDecimalPrecision(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT, v DECIMAL, PRIMARY KEY (id))")

	// ① 精度内无损往返
	src := filepath.Join(t.TempDir(), "dec.csv")
	body := "id,v\n" +
		"1,12345.5000\n" +
		"2,-0.2500\n" +
		"3,1.0001\n" +
		"4,42\n"
	if err := os.WriteFile(src, []byte(body), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	mustExec(t, e, "IMPORT INTO t FROM '"+src+"'")
	rows := mustRows(t, e, "SELECT * FROM t ORDER BY id")
	if rows[0][1].S != "12345.5000" || rows[1][1].S != "-0.2500" || rows[2][1].S != "1.0001" || rows[3][1].S != "42.0000" {
		t.Fatalf("decimal canonical: got %q %q %q %q", rows[0][1].S, rows[1][1].S, rows[2][1].S, rows[3][1].S)
	}
	out := filepath.Join(t.TempDir(), "out.csv")
	mustExec(t, e, "EXPORT TABLE t TO '"+out+"'")
	recs := readCSV(t, out)
	if recs[1][1] != "12345.5000" || recs[3][1] != "1.0001" {
		t.Fatalf("exported decimal cells: %v", recs)
	}
	mustExec(t, e, "CREATE TABLE t2 (id INT, v DECIMAL, PRIMARY KEY (id))")
	mustExec(t, e, "IMPORT INTO t2 FROM '"+out+"'")
	rows2 := mustRows(t, e, "SELECT * FROM t2 ORDER BY id")
	compareRows(t, rows, rows2)

	// ② 超 4 位小数导入报错（原子回滚：不新增行）
	bad := filepath.Join(t.TempDir(), "bad.csv")
	if err := os.WriteFile(bad, []byte("id,v\n1,1.12345\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	if _, err := e.Execute("IMPORT INTO t FROM '" + bad + "'"); err == nil || !strings.Contains(err.Error(), "DECIMAL") {
		t.Fatalf("over-precision import should fail with DECIMAL error, got %v", err)
	}
	if cnt := mustRows(t, e, "SELECT COUNT(*) FROM t")[0][0].I; cnt != 4 {
		t.Fatalf("atomic rollback violated: count=%d", cnt)
	}

	// ③ IGNORE ERRORS 跳过超精度行
	bad2 := filepath.Join(t.TempDir(), "bad2.csv")
	if err := os.WriteFile(bad2, []byte("id,v\n10,2.5000\n11,3.12345\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	res := mustExec(t, e, "IMPORT INTO t FROM '"+bad2+"' IGNORE ERRORS")
	if len(res.Rows) != 1 || res.Rows[0][2].I != 1 {
		t.Fatalf("ignore-errors result: got %+v", res.Rows)
	}
	if cnt := mustRows(t, e, "SELECT COUNT(*) FROM t")[0][0].I; cnt != 5 {
		t.Fatalf("ignore-errors count: got %d", cnt)
	}
	if v := mustRows(t, e, "SELECT v FROM t WHERE id = 10")[0][0].S; v != "2.5000" {
		t.Fatalf("id=10 value: got %q", v)
	}
}

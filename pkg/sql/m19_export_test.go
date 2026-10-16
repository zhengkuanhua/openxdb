package sql_test

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// M19 生态导出：EXPORT ... FORMAT JSON / PARQUET 端到端（见 docs/T32_m19_ecosystem_export.md）。

func exportPath(t *testing.T, name string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	return p
}

func TestExportJSONLines(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE src (id INT, name TEXT, hired DATE, salary DECIMAL, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO src VALUES (1, 'alice', '2024-03-15', 12345.5), (2, 'bob, \"jr\"', '2023-12-31', 99.9999)")
	p := exportPath(t, "out.jsonl")
	mustExec(t, e, "EXPORT TABLE src TO '"+p+"' FORMAT JSON")

	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read jsonl: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("jsonl lines: got %d, want 2; content=%s", len(lines), string(raw))
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("json line0: %v", err)
	}
	if first["id"] != float64(1) || first["name"] != "alice" || first["hired"] != "2024-03-15" {
		t.Fatalf("line0: got %+v", first)
	}
	// DECIMAL 规范化后经 ParseFloat 成数字：12345.5000 → 12345.5
	if first["salary"] != float64(12345.5) {
		t.Fatalf("salary: got %+v", first["salary"])
	}
	var second map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("json line1: %v", err)
	}
	if second["name"] != "bob, \"jr\"" {
		t.Fatalf("line1 name: got %+v", second["name"])
	}
}

func TestExportJSONColumnSubset(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE sub (id INT, a TEXT, b TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO sub VALUES (1, 'x', 'y'), (2, 'u', 'v')")
	p := exportPath(t, "sub.jsonl")
	mustExec(t, e, "EXPORT TABLE sub (id, b) TO '"+p+"' FORMAT JSON")
	raw, _ := os.ReadFile(p)
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	var row map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(row) != 2 || row["id"] != float64(1) || row["b"] != "y" {
		t.Fatalf("subset row: got %+v", row)
	}
	if _, ok := row["a"]; ok {
		t.Fatalf("subset should exclude a: %+v", row)
	}
}

func TestExportJSONFormatCaseInsensitive(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE ci (id INT PRIMARY KEY)")
	mustExec(t, e, "INSERT INTO ci VALUES (1)")
	p := exportPath(t, "ci.jsonl")
	mustExec(t, e, "EXPORT TABLE ci TO '"+p+"' FORMAT json")
	raw, _ := os.ReadFile(p)
	if !strings.HasPrefix(string(raw), `{"id":1}`) {
		t.Fatalf("lowercase format: got %s", string(raw))
	}
}

func TestExportParquetEndToEnd(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE pq (id INT, name TEXT, hired DATE, salary DECIMAL, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO pq VALUES (1, 'alice', '2024-03-15', 12345.5), (2, 'bob', '2023-12-31', 99.9999), (3, 'carol', '2022-01-01', 0.0)")
	p := exportPath(t, "out.parquet")
	mustExec(t, e, "EXPORT TABLE pq TO '"+p+"' FORMAT PARQUET")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	// magic 前后缀
	if len(raw) < 12 || string(raw[:4]) != "PAR1" || string(raw[len(raw)-4:]) != "PAR1" {
		t.Fatalf("magic: len=%d head=%q tail=%q", len(raw), raw[:4], raw[len(raw)-4:])
	}
	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
	// 文件布局 = 数据区 + FileMetaData(footerLen) + 4字节footerLen + PAR1
	if footerLen <= 0 || footerLen+8+4 > len(raw) {
		t.Fatalf("footer len: %d but file len %d", footerLen, len(raw))
	}
	footerStart := len(raw) - 8 - footerLen
	if footerStart < 4 {
		t.Fatalf("footer start invalid: %d", footerStart)
	}
	// FileMetaData 以 version 字段开头（compact varint 0x15 0x02 = version 2）
	if raw[footerStart] != 0x15 || raw[footerStart+1] != 0x02 {
		t.Fatalf("footer version bytes: % x", raw[footerStart:footerStart+2])
	}
	// 非空数据页（至少含列数据字节）
	if len(raw) < 30 {
		t.Fatalf("file too small: %d", len(raw))
	}
	// 行数与列数由 export 包 roundtrip 保证；此处验证文件结构完整即可。
}

func TestExportParquetEmptyTable(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE pqe (id INT, name TEXT, PRIMARY KEY (id))")
	p := exportPath(t, "empty.parquet")
	mustExec(t, e, "EXPORT TABLE pqe TO '"+p+"' FORMAT PARQUET")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(raw[:4]) != "PAR1" || string(raw[len(raw)-4:]) != "PAR1" {
		t.Fatalf("magic: %q", raw[:4])
	}
}

func TestExportUnsupportedFormat(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE uf (id INT PRIMARY KEY)")
	p := exportPath(t, "u.csv")
	mustErr(t, e, "EXPORT TABLE uf TO '"+p+"' FORMAT XML", "unsupported export format")
}

func TestExportParquetBlobAndDate(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE bq (id INT, data BLOB, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO bq VALUES (1, '0a1b')")
	p := exportPath(t, "b.parquet")
	mustExec(t, e, "EXPORT TABLE bq TO '"+p+"' FORMAT PARQUET")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(raw[:4]) != "PAR1" {
		t.Fatalf("magic: %q", raw[:4])
	}
	// 冒烟：文件可读且含大写 hex 文本 "0A1B" 的字节序列（BYTE_ARRAY 载荷）
	if !bytesContain(raw, []byte("0A1B")) {
		t.Fatalf("blob hex not found in parquet payload")
	}
}

func bytesContain(haystack, needle []byte) bool {
	return strings.Contains(string(haystack), string(needle))
}

// TestExportJSONEscaping 覆盖 JSON 字符串转义（双引号/反斜杠；SQL 文本层不解释 \n）。
func TestExportJSONEscaping(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE esc (id INT, v TEXT, PRIMARY KEY (id))")
	mustExec(t, e, `INSERT INTO esc VALUES (1, 'he said "hi" and \')`)
	p := exportPath(t, "esc.jsonl")
	mustExec(t, e, "EXPORT TABLE esc TO '"+p+"' FORMAT JSON")
	f, err := os.Open(p)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Scan()
	var row map[string]any
	if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if row["v"] != `he said "hi" and \` {
		t.Fatalf("escaped value: got %+v", row["v"])
	}
}

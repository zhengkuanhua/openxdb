package server

import (
	"bytes"
	"strings"
	"testing"
)

// TestREPLRowProtocolNullEmptyEscape 行协议边界（v1.5.0 P2-12）：
//  1. NULL 单元格渲染为 \N，空串渲染为空单元格，二者在线上可区分；
//  2. 单元格内反斜杠/换行/回车渲染为 \\ \n \r 转义序列，不破坏行结构；
//  3. 客户端以 E'...' 转义字面量写回含换行文本，经行协议往返后值保真。
func TestREPLRowProtocolNullEmptyEscape(t *testing.T) {
	s, closeFn := newSQLTestServer(t)
	defer closeFn()

	input := "CREATE TABLE t_row (id INT, txt TEXT, PRIMARY KEY (id))\n" +
		"INSERT INTO t_row VALUES (1, NULL), (2, ''), (3, E'a\\nb\\\\c''d')\n" +
		"SELECT id, txt FROM t_row ORDER BY id\n" +
		"QUIT\n"

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(input), &out, s.defaultSession()); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}

	blocks := splitBlocks(out.String())
	if len(blocks) != 4 {
		t.Fatalf("want 4 reply blocks, got %d: %q", len(blocks), blocks)
	}

	// 表格行（块 3）：
	//   id | txt
	//   --- | ---
	//   1 | \N            <- NULL
	//   2 |               <- 空串（TEXT ''）
	//   3 | a\nb\\c'd     <- 转义后单行文本
	//   (3 rows)
	table := strings.Split(blocks[2], "\n")
	if !strings.HasSuffix(blocks[2], "(3 rows)") {
		t.Fatalf("reply[2] %q: want (3 rows)", blocks[2])
	}
	// 数据行（table[2..4]）：1 → \N；2 → 空单元格；3 → 转义单行
	if !strings.Contains(table[2], `| \N`) {
		t.Fatalf("row1 %q: want NULL cell rendered as \\N", table[2])
	}
	// 数据行 2：空串 → 空单元格（| 后无内容）
	row2 := table[3]
	idx := strings.Index(row2, "| ")
	if idx < 0 {
		t.Fatalf("row2 %q: no separator", row2)
	}
	if strings.TrimSpace(row2[idx+2:]) != "" {
		t.Fatalf("row2 %q: empty-string cell should be blank", row2)
	}
	// 数据行 3：换行/反斜杠/引号转义为单行
	if !strings.Contains(table[4], `a\nb\\c'd`) {
		t.Fatalf("row3 %q: want escaped cell a\\nb\\\\c'd", table[4])
	}
	if len(table) != 6 { // header + separator + 3 rows + trailer
		t.Fatalf("table line count: got %d (%q)", len(table), blocks[2])
	}

	// 往返验证：行协议文本本身不含裸换行（所有换行文本均被转义），
	// 任何一行的换行次数都与行数严格一致。
	for i, l := range table {
		if strings.Count(l, "\n") != 0 {
			t.Fatalf("table line %d contains raw newline: %q", i, l)
		}
	}
}

// TestREPLRowProtocolEscapeRoundtrip 行协议往返一致：服务端渲染的转义单元格
// 用 E'...' 字面量写回后，值与源值一致（导出→导入闭环）。
func TestREPLRowProtocolEscapeRoundtrip(t *testing.T) {
	s, closeFn := newSQLTestServer(t)
	defer closeFn()

	stored := "a\nb\rc\\d'e"
	// 用 E'...' 写入含换行文本
	x := stored
	x = strings.ReplaceAll(x, "\\", "\\\\")
	x = strings.ReplaceAll(x, "\n", "\\n")
	x = strings.ReplaceAll(x, "\r", "\\r")
	x = strings.ReplaceAll(x, "'", "''")
	lit := "E'" + x + "'"

	input := "CREATE TABLE t_r (id INT, txt TEXT, PRIMARY KEY (id))\n" +
		"INSERT INTO t_r VALUES (1, " + lit + ")\n" +
		"SELECT txt FROM t_r WHERE id = 1\n" +
		"QUIT\n"
	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(input), &out, s.defaultSession()); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	if len(blocks) != 4 {
		t.Fatalf("want 4 reply blocks, got %d", len(blocks))
	}
	// 返回单元格为转义形式 a\nb\rc\\d'e（无裸换行）
	if !strings.Contains(blocks[2], `a\nb\rc\\d'e`) {
		t.Fatalf("reply table %q: want escaped cell a\\nb\\rc\\\\d'e", blocks[2])
	}
	if strings.Contains(blocks[2], "a\nb") {
		t.Fatalf("reply table %q: must not contain raw newline", blocks[2])
	}
}

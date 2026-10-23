package server

import (
	"bytes"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// newSQLTestServer 创建挂载 SQL 引擎的测试 Server（server_test.go 的
// newTestServer 不挂 SQL，仅覆盖 KV 命令面）。
func newSQLTestServer(t *testing.T) (*Server, func()) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	s := New(d.Txn)
	s.SQL = d.SQL
	return s, func() { _ = d.Close() }
}

// TestREPLSessionConsecutiveLinesInOrder 回归测试：客户端 pipeline 依赖服务端
// REPLSession 对连续多行输入按序逐条处理并回复。构造一批混合语句（DDL/DML/
// 查询/错误语句交错），一次性写入 REPLSession 的 reader，验证：
//  1. 每个输入行都产生一个对应的响应，顺序一一对应；
//  2. 多行表格回复（header/separator/rows/(N rows)）边界解析正确；
//  3. 出错语句返回 ERR 后会话继续处理后续语句（错误不中断流水线）；
//  4. 所有语句按发送顺序执行（后序语句能观察到前序语句的副作用）。
func TestREPLSessionConsecutiveLinesInOrder(t *testing.T) {
	s, closeFn := newSQLTestServer(t)
	defer closeFn()

	input := "CREATE TABLE t_pipe (id INT, v TEXT, PRIMARY KEY (id))\n" +
		"INSERT INTO t_pipe VALUES (1, 'a'), (2, 'b')\n" +
		"SELECT id, v FROM t_pipe ORDER BY id\n" +
		"BOGUS\n" +
		"DELETE FROM t_pipe WHERE id = 1\n" +
		"SELECT id, v FROM t_pipe ORDER BY id\n" +
		"QUIT\n"

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(input), &out, s.defaultSession()); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}

	blocks := splitBlocks(out.String())
	if len(blocks) != 7 {
		t.Fatalf("want 7 reply blocks (one per input line), got %d: %q", len(blocks), blocks)
	}

	// 1-2: DDL / DML replies are "(N rows affected)".
	for i := 0; i < 2; i++ {
		if !strings.Contains(blocks[i], "rows affected") {
			t.Fatalf("reply[%d] %q: want rows-affected reply", i, blocks[i])
		}
	}

	// 3: SELECT returns a text table ending with "(2 rows)".
	if !strings.HasSuffix(blocks[2], "(2 rows)") {
		t.Fatalf("reply[2] %q: want table ending with (2 rows)", blocks[2])
	}
	table := strings.Split(blocks[2], "\n")
	if len(table) < 4 { // header + separator + 2 data rows + trailer
		t.Fatalf("reply[2] %q: want header/separator/2 rows/(2 rows)", blocks[2])
	}
	if !strings.HasPrefix(blocks[3], "ERR ") {
		t.Fatalf("reply[3] %q: want ERR reply for BOGUS", blocks[3])
	}

	// 5: DELETE after the error still executed (order preserved).
	if !strings.Contains(blocks[4], "rows affected") {
		t.Fatalf("reply[4] %q: want rows-affected reply for DELETE", blocks[4])
	}

	// 6: final SELECT observes DELETE side effect -> 1 row left.
	if !strings.HasSuffix(blocks[5], "(1 rows)") {
		t.Fatalf("reply[5] %q: want table ending with (1 rows)", blocks[5])
	}

	// 7: QUIT -> BYE ends the session.
	if blocks[6] != "BYE" {
		t.Fatalf("reply[6] %q: want BYE", blocks[6])
	}
}

// splitBlocks 把 REPLSession 的连续输出切成"响应块"：单行回复
// （rows affected / ERR / OK / PONG / BYE）各自成块，多行表格回复
// （header | separator | rows | (N rows)）合并为一个块。
func splitBlocks(s string) []string {
	lines := splitLines(s)
	var blocks []string
	for i := 0; i < len(lines); {
		l := lines[i]
		if strings.HasSuffix(l, " rows affected)") ||
			strings.HasPrefix(l, "ERR ") ||
			l == "OK" || l == "PONG" || l == "BYE" {
			blocks = append(blocks, l)
			i++
			continue
		}
		j := i
		for j < len(lines) && !strings.HasSuffix(lines[j], " rows)") {
			j++
		}
		if j >= len(lines) {
			tb := strings.Join(lines[i:], "\n")
			blocks = append(blocks, tb)
			break
		}
		blocks = append(blocks, strings.Join(lines[i:j+1], "\n"))
		i = j + 1
	}
	return blocks
}

func splitLines(s string) []string {
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

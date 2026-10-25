package server

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// newSQLTestServer 挂载引擎的测试 Server（server_pipeline_test.go 同款）。
func newSQLTestServerV113(t *testing.T) (*Server, func()) {
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

// TestREPLSessionPipelineBatchOrder v1.13.0 回归：多 reader/worker 管道化
// REPLSession 下，客户端 pipeline 一次性批量发送大量混合命令时：
//  1. 每条输入行都产生一个对应响应，顺序一一对应；
//  2. 出错语句返回 ERR 后会话继续处理后续语句（错误不中断流水线）；
//  3. 后续查询能观察到前序语句的副作用（顺序执行语义保持）；
//  4. QUIT 正常终止会话。
func TestREPLSessionPipelineBatchOrder(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()

	var sb strings.Builder
	sb.WriteString("CREATE TABLE t_pipe (id INT, v TEXT, PRIMARY KEY (id))\n")
	const n = 100
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "INSERT INTO t_pipe (id, v) VALUES (%d, 'v%d')\n", i, i)
	}
	for i := 0; i < n; i += 2 {
		fmt.Fprintf(&sb, "SELECT id, v FROM t_pipe WHERE id = %d\n", i)
	}
	for i := 0; i < 10; i++ {
		sb.WriteString("BOGUS\n")
	}
	sb.WriteString("SELECT COUNT(*) AS n FROM t_pipe\n")
	sb.WriteString("QUIT\n")

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(sb.String()), &out, s.defaultSession()); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	// CREATE + INSERT*n + SELECT*n/2 + BOGUS*10 + COUNT + QUIT
	want := 1 + n + n/2 + 10 + 1 + 1
	if len(blocks) != want {
		t.Fatalf("want %d reply blocks (one per input line), got %d", want, len(blocks))
	}
	// 首条：CREATE TABLE rows affected。
	if !strings.Contains(blocks[0], "rows affected") {
		t.Fatalf("CREATE reply[0] %q: want rows-affected", blocks[0])
	}
	// 随后 n 条：INSERT 逐条 rows affected。
	for i := 0; i < n; i++ {
		if !strings.Contains(blocks[1+i], "rows affected") {
			t.Fatalf("INSERT reply[%d] %q: want rows-affected", 1+i, blocks[1+i])
		}
	}
	// 后续 SELECT 每行一条：(1 rows) 表格。
	for i := 0; i < n/2; i++ {
		idx := 1 + n + i
		if !strings.HasSuffix(blocks[idx], "(1 rows)") {
			t.Fatalf("SELECT reply[%d] %q: want (1 rows)", idx, blocks[idx])
		}
	}
	// BOGUS 逐条 ERR，不中断。
	for i := 0; i < 10; i++ {
		idx := 1 + n + n/2 + i
		if !strings.HasPrefix(blocks[idx], "ERR ") {
			t.Fatalf("BOGUS reply[%d] %q: want ERR", idx, blocks[idx])
		}
	}
	// COUNT 观察到全部 INSERT 副作用 -> 结果表内值为 100（单行聚合）。
	idx := 1 + n + n/2 + 10
	if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], "100") {
		t.Fatalf("COUNT reply[%d] %q: want table containing 100, ending (1 rows)", idx, blocks[idx])
	}
	// QUIT -> BYE。
	if blocks[len(blocks)-1] != "BYE" {
		t.Fatalf("last reply %q: want BYE", blocks[len(blocks)-1])
	}
}

// TestREPLSessionLongLineNoTruncation v1.13.0 回归：Scanner 单行上限提升
// 至 1MB 后，超过默认 64KB 的命令行不再触发 token too long 截断，会话
// 继续处理后续命令直至 QUIT 正常返回。
func TestREPLSessionLongLineNoTruncation(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()

	// >64KB 字符串字面量：旧 Scanner（默认 64KB 上限）会 token too long
	// 终止整个会话；新上限 1MB 下仅作为普通 SQL 执行（引擎若拒绝长字面量
	// 会返回 ERR，但会话必须存活，后续命令继续处理）。
	pad := strings.Repeat("x", 100*1024)
	input := fmt.Sprintf("SELECT '%s' AS v\nSELECT 1 AS one\nQUIT\n", pad)

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(input), &out, s.defaultSession()); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	if len(blocks) != 3 {
		t.Fatalf("want 3 reply blocks, got %d: %q", len(blocks), blocks)
	}
	// 第一条：长行命令不被截断——要么 ERR（引擎拒绝），要么表格；不得是
	// 空响应或丢失（旧实现此处 Scanner.Err 直接终止，后续块缺失）。
	if blocks[0] == "" {
		t.Fatalf("long-line reply is empty: session was truncated")
	}
	// 第二条：会话存活，后续命令正常执行。
	if !strings.HasSuffix(blocks[1], "(1 rows)") {
		t.Fatalf("follow-up SELECT reply %q: want (1 rows)", blocks[1])
	}
	// QUIT -> BYE。
	if blocks[2] != "BYE" {
		t.Fatalf("last reply %q: want BYE", blocks[2])
	}
}

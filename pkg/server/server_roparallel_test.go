package server

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// newRORConnSession 构造 useGlobalSQL=false 的连接会话（模拟 TCP 连接独立
// SQL 会话）。sqlSess 由 REPLSession 主循环在串行段懒创建（roSelectEligible），
// 不在此预建，确保与生产路径一致。
func newRORConnSession() *session {
	return &session{user: sql.SuperUser, authed: true, useGlobalSQL: false}
}

// TestREPLSessionROParallelBatchOrder v1.14.0 P0 回归：只读 SELECT 并行批
// 路径下，pipeline 混合命令流满足：
//  1. 连续 autocommit 只读 SELECT 进入并行批，响应按原始输入行序重组输出
//     （批内并发执行不改变响应顺序）；
//  2. 批中遇到写语句立即中断并退回串行路径，命令顺序语义保持；
//  3. 后续查询能观察到中断点前语句的副作用；
//  4. QUIT 正常终止。
func TestREPLSessionROParallelBatchOrder(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()
	sess := newRORConnSession()

	var sb strings.Builder
	sb.WriteString("CREATE TABLE t_ro (id INT, v TEXT, PRIMARY KEY (id))\n")
	const ins = 10
	for i := 0; i < ins; i++ {
		fmt.Fprintf(&sb, "INSERT INTO t_ro (id, v) VALUES (%d, 'v%d')\n", i, i)
	}
	const sel = 60 // 连续只读 SELECT：进入并行批
	for i := 0; i < sel; i++ {
		fmt.Fprintf(&sb, "SELECT id, v FROM t_ro WHERE id = %d\n", i%ins)
	}
	sb.WriteString("INSERT INTO t_ro (id, v) VALUES (100, 'after')\n") // 中断并行批
	sb.WriteString("SELECT COUNT(*) AS n FROM t_ro\n")
	sb.WriteString("QUIT\n")

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(sb.String()), &out, sess); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	want := 1 + ins + sel + 1 + 1 + 1 // CREATE + INSERT*10 + SELECT*60 + INSERT + COUNT + QUIT
	if len(blocks) != want {
		t.Fatalf("want %d reply blocks (one per input line), got %d", want, len(blocks))
	}
	// CREATE / INSERT 串行段。
	if !strings.Contains(blocks[0], "rows affected") {
		t.Fatalf("CREATE reply[0] %q: want rows-affected", blocks[0])
	}
	for i := 0; i < ins; i++ {
		if !strings.Contains(blocks[1+i], "rows affected") {
			t.Fatalf("INSERT reply[%d] %q: want rows-affected", 1+i, blocks[1+i])
		}
	}
	// 并行批 SELECT：响应块数与输入一一对应，且每行结果值与输入行匹配
	// （验证按序重组——若响应乱序，此处值校验必然失败）。
	for i := 0; i < sel; i++ {
		idx := 1 + ins + i
		wantID := i % ins
		if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], fmt.Sprintf("v%d", wantID)) {
			t.Fatalf("parallel SELECT reply[%d] %q: want row id=%d v%d", idx, blocks[idx], wantID, wantID)
		}
	}
	// 中断后的 INSERT 串行执行，COUNT 观察到 10+1=11。
	idx := 1 + ins + sel
	if !strings.Contains(blocks[idx], "rows affected") {
		t.Fatalf("post-batch INSERT reply[%d] %q: want rows-affected", idx, blocks[idx])
	}
	idx = 1 + ins + sel + 1
	if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], "11") {
		t.Fatalf("COUNT reply[%d] %q: want table containing 11, ending (1 rows)", idx, blocks[idx])
	}
	if blocks[len(blocks)-1] != "BYE" {
		t.Fatalf("last reply %q: want BYE", blocks[len(blocks)-1])
	}
}

// TestREPLSessionROParallelOff v1.14.0 P0 开关回退：ro_parallel 关闭后并行
// 批路径停用，命令流退化为 v1.13.0 逐条串行执行，响应仍一一对应且语义保持。
func TestREPLSessionROParallelOff(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()
	sess := newRORConnSession()
	s.SQL.SetROParallel(false) // 开关回退

	var sb strings.Builder
	sb.WriteString("CREATE TABLE t_ro_off (id INT, v TEXT, PRIMARY KEY (id))\n")
	for i := 0; i < 5; i++ {
		fmt.Fprintf(&sb, "INSERT INTO t_ro_off (id, v) VALUES (%d, 'off%d')\n", i, i)
	}
	for i := 0; i < 30; i++ {
		fmt.Fprintf(&sb, "SELECT id, v FROM t_ro_off WHERE id = %d\n", i%5)
	}
	sb.WriteString("SELECT COUNT(*) AS n FROM t_ro_off\n")
	sb.WriteString("QUIT\n")

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(sb.String()), &out, sess); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	want := 1 + 5 + 30 + 1 + 1
	if len(blocks) != want {
		t.Fatalf("ro_parallel off: want %d reply blocks, got %d", want, len(blocks))
	}
	for i := 0; i < 30; i++ {
		idx := 1 + 5 + i
		if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], fmt.Sprintf("off%d", i%5)) {
			t.Fatalf("serial SELECT reply[%d] %q: want row off%d", idx, blocks[idx], i%5)
		}
	}
	idx := 1 + 5 + 30
	if !strings.Contains(blocks[idx], "5") || !strings.HasSuffix(blocks[idx], "(1 rows)") {
		t.Fatalf("COUNT reply[%d] %q: want table containing 5", idx, blocks[idx])
	}
}

// TestREPLSessionROParallelTxIsolation v1.14.0 P0：显式事务内的 SELECT 不
// 进入并行批（InTx 排除），事务语义（读己之写、命令顺序）保持；COMMIT 后
// 只读 SELECT 恢复并行路径。
func TestREPLSessionROParallelTxIsolation(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()
	sess := newRORConnSession()

	var sb strings.Builder
	sb.WriteString("CREATE TABLE t_ro_tx (id INT, v TEXT, PRIMARY KEY (id))\n")
	sb.WriteString("BEGIN\n")
	sb.WriteString("INSERT INTO t_ro_tx (id, v) VALUES (1, 'tx')\n")
	// 事务内 SELECT：不得进并行批，须看到本事务未提交写入（RYW）。
	sb.WriteString("SELECT id, v FROM t_ro_tx WHERE id = 1\n")
	sb.WriteString("COMMIT\n")
	// 事务外连续只读 SELECT：恢复并行批路径。
	sb.WriteString("SELECT COUNT(*) AS n FROM t_ro_tx\n")
	sb.WriteString("SELECT id, v FROM t_ro_tx WHERE id = 1\n")
	sb.WriteString("QUIT\n")

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(sb.String()), &out, sess); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	// CREATE + BEGIN + INSERT + txn SELECT + COMMIT + COUNT + SELECT + QUIT = 8
	if len(blocks) != 8 {
		t.Fatalf("want 8 reply blocks, got %d", len(blocks))
	}
	// 事务内 SELECT 必须返回 1 行（读己之写）。
	if !strings.HasSuffix(blocks[3], "(1 rows)") || !strings.Contains(blocks[3], "tx") {
		t.Fatalf("txn SELECT reply[3] %q: want row v=tx (read-your-writes)", blocks[3])
	}
	// 事务外 COUNT=1。
	if !strings.HasSuffix(blocks[5], "(1 rows)") || !strings.Contains(blocks[5], "1") {
		t.Fatalf("COUNT reply[5] %q: want table containing 1", blocks[5])
	}
	if blocks[7] != "BYE" {
		t.Fatalf("last reply %q: want BYE", blocks[7])
	}
}

// TestREPLSessionROParallelConfig v1.14.0 P0 配置：SET ro_parallel 可运行时
// 热切换，SHOW CONFIG 可读回；非法值报错不中断会话。
func TestREPLSessionROParallelConfig(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()
	sess := newRORConnSession()

	var sb strings.Builder
	sb.WriteString("SHOW CONFIG\n")
	sb.WriteString("SET ro_parallel = off\n")
	sb.WriteString("SHOW CONFIG\n")
	sb.WriteString("SET ro_parallel = bogus\n")
	sb.WriteString("SELECT 1 AS one\n")
	sb.WriteString("QUIT\n")

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(sb.String()), &out, sess); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	if len(blocks) != 6 {
		t.Fatalf("want 6 reply blocks, got %d", len(blocks))
	}
	if !strings.Contains(blocks[0], "ro_parallel") || !strings.Contains(blocks[0], "on") {
		t.Fatalf("SHOW CONFIG[0] %q: want ro_parallel on by default", blocks[0])
	}
	if !strings.Contains(blocks[2], "ro_parallel") || !strings.Contains(blocks[2], "off") {
		t.Fatalf("SHOW CONFIG after SET off [2] %q: want ro_parallel off", blocks[2])
	}
	if !strings.HasPrefix(blocks[3], "ERR ") {
		t.Fatalf("SET bogus reply[3] %q: want ERR", blocks[3])
	}
	if !strings.HasSuffix(blocks[4], "(1 rows)") {
		t.Fatalf("SELECT reply[4] %q: want (1 rows)", blocks[4])
	}
}

// TestGroupCommitWindowConfig 验证 v1.14.0 P1 配置键 group_commit_window_ms：
// 默认 0.5ms 展示、SET 热切换即时生效、0 关闭（回退被动攒批）、非法值报错。
func TestGroupCommitWindowConfig(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()
	sess := newRORConnSession()

	var sb strings.Builder
	sb.WriteString("SHOW CONFIG\n")
	sb.WriteString("SET group_commit_window_ms = 0.1\n")
	sb.WriteString("SHOW CONFIG\n")
	sb.WriteString("SET group_commit_window_ms = 0\n")
	sb.WriteString("SHOW CONFIG\n")
	sb.WriteString("SET group_commit_window_ms = -1\n")
	sb.WriteString("SELECT 1 AS one\n")
	sb.WriteString("QUIT\n")

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(sb.String()), &out, sess); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	if len(blocks) != 8 {
		t.Fatalf("want 8 reply blocks (incl BYE), got %d", len(blocks))
	}
	// 默认 0.500（NewExecutor 与 WAL 默认 500µs 对齐）。
	if !strings.Contains(blocks[0], "group_commit_window_ms") || !strings.Contains(blocks[0], "0.500") {
		t.Fatalf("SHOW CONFIG[0] %q: want group_commit_window_ms default 0.500", blocks[0])
	}
	// SET 0.1 后展示 0.100。
	if !strings.Contains(blocks[2], "group_commit_window_ms") || !strings.Contains(blocks[2], "0.100") {
		t.Fatalf("SHOW CONFIG after SET 0.1 [2] %q: want group_commit_window_ms 0.100", blocks[2])
	}
	// SET 0 后展示 0.000（定时窗口关闭）。
	if !strings.Contains(blocks[4], "group_commit_window_ms") || !strings.Contains(blocks[4], "0.000") {
		t.Fatalf("SHOW CONFIG after SET 0 [4] %q: want group_commit_window_ms 0.000", blocks[4])
	}
	// 负数非法：ERR。
	if !strings.HasPrefix(blocks[5], "ERR ") {
		t.Fatalf("SET -1 reply[5] %q: want ERR", blocks[5])
	}
	if !strings.HasSuffix(blocks[6], "(1 rows)") {
		t.Fatalf("SELECT reply[6] %q: want (1 rows)", blocks[6])
	}
}

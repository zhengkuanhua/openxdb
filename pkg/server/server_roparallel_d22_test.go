package server

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// D2.2 专项测试：写事务内只读语句并行（v1.17.0，ro_parallel 扩展到显式
// 写事务内 SELECT）。
//
// 核心语义断言：
//  1. 写事务内连续只读 SELECT 进入并行批，共享同一事务快照（curTx）并行
//     执行，read-your-writes 保持（能看到本事务未提交写入）；
//  2. 批内响应按输入行序重组（不破坏语句顺序）；
//  3. 写语句（INSERT/UPDATE）打断并行批退回串行执行，批前批后可见性
//     按命令顺序推进（UPDATE 后 SELECT 读到新值）；
//  4. COMMIT 后其他连接/后续语句可见已提交数据；
//  5. 回退路径（ro_parallel off / Serializable / @var）行为与逐条串行
//     完全等价，响应与值均正确。

// TestREPLSessionROParallelWriteTxn D2.2：显式写事务内 SELECT 并行 + RYW +
// 写打断保序 + COMMIT 可见。
func TestREPLSessionROParallelWriteTxn(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()
	sess := newRORConnSession()

	var sb strings.Builder
	sb.WriteString("CREATE TABLE t_wtx (id INT, v TEXT, PRIMARY KEY (id))\n")
	const baseIns = 5
	for i := 0; i < baseIns; i++ {
		fmt.Fprintf(&sb, "INSERT INTO t_wtx (id, v) VALUES (%d, 'b%d')\n", i, i)
	}
	sb.WriteString("BEGIN\n")
	sb.WriteString("INSERT INTO t_wtx (id, v) VALUES (100, 'w1')\n") // 事务内写
	const sel1 = 30 // 写事务内连续只读 SELECT：进并行批，共享快照
	for i := 0; i < sel1; i++ {
		fmt.Fprintf(&sb, "SELECT id, v FROM t_wtx WHERE id = 100\n")
	}
	sb.WriteString("UPDATE t_wtx SET v = 'w2' WHERE id = 100\n") // 写打断批，串行
	const sel2 = 15 // 新批：读己之写（批前 UPDATE 已执行，顺序可见）
	for i := 0; i < sel2; i++ {
		fmt.Fprintf(&sb, "SELECT id, v FROM t_wtx WHERE id = 100\n")
	}
	sb.WriteString("COMMIT\n")
	sb.WriteString("SELECT COUNT(*) AS n FROM t_wtx\n")
	sb.WriteString("SELECT id, v FROM t_wtx WHERE id = 100\n")
	sb.WriteString("QUIT\n")

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(sb.String()), &out, sess); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	want := 1 + baseIns + 1 + 1 + sel1 + 1 + sel2 + 1 + 1 + 1 + 1
	if len(blocks) != want {
		t.Fatalf("want %d reply blocks (one per input line), got %d", len(blocks), want)
	}
	// 基础 CREATE / INSERT。
	if !strings.Contains(blocks[0], "rows affected") {
		t.Fatalf("CREATE reply[0] %q: want rows-affected", blocks[0])
	}
	for i := 0; i < baseIns; i++ {
		if !strings.Contains(blocks[1+i], "rows affected") {
			t.Fatalf("INSERT reply[%d] %q: want rows-affected", 1+i, blocks[1+i])
		}
	}
	// BEGIN 回显 OK。
	idx := 1 + baseIns
	if blocks[idx] != "OK" {
		t.Fatalf("BEGIN reply[%d] %q: want OK", idx, blocks[idx])
	}
	// 事务内 INSERT。
	idx = 2 + baseIns
	if !strings.Contains(blocks[idx], "rows affected") {
		t.Fatalf("txn INSERT reply[%d] %q: want rows-affected", idx, blocks[idx])
	}
	// 写事务内第一批并行 SELECT：全部读到 v=w1（读己之写 + 共享快照）。
	for i := 0; i < sel1; i++ {
		idx := 3 + baseIns + i
		if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], "w1") {
			t.Fatalf("write-txn SELECT reply[%d] %q: want row v=w1 (read-your-writes)", idx, blocks[idx])
		}
	}
	// UPDATE 打断批后：第二批 SELECT 读到 v=w2（命令顺序/可见性推进）。
	idx = 3 + baseIns + sel1
	if !strings.Contains(blocks[idx], "rows affected") {
		t.Fatalf("UPDATE reply[%d] %q: want rows-affected", idx, blocks[idx])
	}
	for i := 0; i < sel2; i++ {
		idx := 4 + baseIns + sel1 + i
		if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], "w2") {
			t.Fatalf("post-UPDATE SELECT reply[%d] %q: want row v=w2", idx, blocks[idx])
		}
	}
	// COMMIT 后事务外查询：COUNT=baseIns+1=6，id=100 仍 v=w2。
	idx = 5 + baseIns + sel1 + sel2
	if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], fmt.Sprintf("%d", baseIns+1)) {
		t.Fatalf("COUNT reply[%d] %q: want table containing %d", idx, blocks[idx], baseIns+1)
	}
	idx = 6 + baseIns + sel1 + sel2
	if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], "w2") {
		t.Fatalf("post-COMMIT SELECT reply[%d] %q: want row v=w2", idx, blocks[idx])
	}
	if blocks[len(blocks)-1] != "BYE" {
		t.Fatalf("last reply %q: want BYE", blocks[len(blocks)-1])
	}
}

// TestREPLSessionROParallelWriteTxnOff D2.2 回退：ro_parallel 关闭时写事务
// 内 SELECT 逐条串行，响应与值正确（回退 v1.16.0 行为）。
func TestREPLSessionROParallelWriteTxnOff(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()
	sess := newRORConnSession()
	s.SQL.SetROParallel(false) // 开关回退

	var sb strings.Builder
	sb.WriteString("CREATE TABLE t_wtx_off (id INT, v TEXT, PRIMARY KEY (id))\n")
	sb.WriteString("BEGIN\n")
	sb.WriteString("INSERT INTO t_wtx_off (id, v) VALUES (1, 'off')\n")
	const sel = 20
	for i := 0; i < sel; i++ {
		fmt.Fprintf(&sb, "SELECT id, v FROM t_wtx_off WHERE id = 1\n")
	}
	sb.WriteString("COMMIT\n")
	sb.WriteString("QUIT\n")

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(sb.String()), &out, sess); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	want := 1 + 1 + 1 + sel + 1 + 1 // CREATE + BEGIN + txn INSERT + SELECT*20 + COMMIT + BYE
	if len(blocks) != want {
		t.Fatalf("ro_parallel off: want %d reply blocks, got %d", want, len(blocks))
	}
	for i := 0; i < sel; i++ {
		idx := 3 + i
		if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], "off") {
			t.Fatalf("write-txn SELECT reply[%d] %q: want row v=off (serial)", idx, blocks[idx])
		}
	}
}

// TestREPLSessionROParallelSerializableWriteTxn D2.2 保守判定：Serializable
// 隔离下写事务内 SELECT 不进并行（引擎层 roBatchEligible 回退整批串行），
// 响应与值正确。
func TestREPLSessionROParallelSerializableWriteTxn(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()
	sess := newRORConnSession()

	var sb strings.Builder
	sb.WriteString("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE\n")
	sb.WriteString("CREATE TABLE t_wtx_ser (id INT, v TEXT, PRIMARY KEY (id))\n")
	sb.WriteString("BEGIN\n")
	sb.WriteString("INSERT INTO t_wtx_ser (id, v) VALUES (1, 'ser')\n")
	const sel = 20
	for i := 0; i < sel; i++ {
		fmt.Fprintf(&sb, "SELECT id, v FROM t_wtx_ser WHERE id = 1\n")
	}
	sb.WriteString("COMMIT\n")
	sb.WriteString("QUIT\n")

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(sb.String()), &out, sess); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	want := 1 + 1 + 1 + 1 + sel + 1 + 1 // SET + CREATE + BEGIN + txn INSERT + SELECT*20 + COMMIT + BYE
	if len(blocks) != want {
		t.Fatalf("Serializable: want %d reply blocks, got %d", want, len(blocks))
	}
	for i := 0; i < sel; i++ {
		idx := 4 + i
		if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], "ser") {
			t.Fatalf("Serializable write-txn SELECT reply[%d] %q: want row v=ser (serial)", idx, blocks[idx])
		}
	}
}

// TestREPLSessionROParallelUserVarInWriteTxn D2.2 保守判定：写事务内读取用户
// 变量（@var）的 SELECT 不进并行（引擎层 selectHasUserVar 回退整批串行），
// 响应与值正确。
func TestREPLSessionROParallelUserVarInWriteTxn(t *testing.T) {
	s, closeFn := newSQLTestServerV113(t)
	defer closeFn()
	sess := newRORConnSession()

	var sb strings.Builder
	sb.WriteString("CREATE TABLE t_wtx_var (id INT, v TEXT, PRIMARY KEY (id))\n")
	sb.WriteString("BEGIN\n")
	sb.WriteString("SET @marker = 'm1'\n")
	const sel = 20
	for i := 0; i < sel; i++ {
		sb.WriteString("SELECT @marker AS m\n")
	}
	sb.WriteString("COMMIT\n")
	sb.WriteString("QUIT\n")

	var out bytes.Buffer
	if err := s.REPLSession(strings.NewReader(sb.String()), &out, sess); err != nil {
		t.Fatalf("REPLSession: %v", err)
	}
	blocks := splitBlocks(out.String())
	want := 1 + 1 + 1 + sel + 1 + 1 // CREATE + BEGIN + SET @marker + SELECT*20 + COMMIT + BYE
	if len(blocks) != want {
		t.Fatalf("write-txn @var: want %d reply blocks, got %d", want, len(blocks))
	}
	for i := 0; i < sel; i++ {
		idx := 3 + i
		if !strings.HasSuffix(blocks[idx], "(1 rows)") || !strings.Contains(blocks[idx], "m1") {
			t.Fatalf("@var write-txn SELECT reply[%d] %q: want row m=m1 (serial)", idx, blocks[idx])
		}
	}
}

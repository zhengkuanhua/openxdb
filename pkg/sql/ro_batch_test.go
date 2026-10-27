package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// v1.16.0 C3 引擎层只读 SELECT 批执行专项测试。
// 覆盖：autocommit 批并行、显式事务内批并行、批序重组、依赖语句回退、
// 用户变量/Serializable 保守回退、错误传播、read-your-writes、批结果与
// 逐条串行执行一致性。

func setupROBatchTable(t *testing.T, e *sql.Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE ro_t (id INT, name TEXT, score INT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO ro_t VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")
}

// sameResult 深比较两个 Result：列名与逐行值（Kind/I/S）完全一致。
func sameResult(a, b *sql.Result) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if len(a.Columns) != len(b.Columns) || len(a.Rows) != len(b.Rows) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i] != b.Columns[i] {
			return false
		}
	}
	for i := range a.Rows {
		if len(a.Rows[i]) != len(b.Rows[i]) {
			return false
		}
		for j := range a.Rows[i] {
			av, bv := a.Rows[i][j], b.Rows[i][j]
			if av.Kind != bv.Kind || av.I != bv.I || av.S != bv.S {
				return false
			}
		}
	}
	return true
}

// serialExecs 以逐条 ExecuteAsSession 方式执行一组语句，返回结果与错误。
func serialExecs(t *testing.T, e *sql.Engine, stmts []string, sess *sql.Session) ([]*sql.Result, []error) {
	t.Helper()
	results := make([]*sql.Result, len(stmts))
	errs := make([]error, len(stmts))
	for i, s := range stmts {
		results[i], errs[i] = e.ExecuteAsSession(s, sql.SuperUser, sess)
	}
	return results, errs
}

// verifyBatchMatchesSerial 断言批执行结果与逐条串行执行完全一致。
func verifyBatchMatchesSerial(t *testing.T, e *sql.Engine, stmts []string, sess *sql.Session) {
	t.Helper()
	// 独立会话跑串行基准（避免与批会话共享事务状态）。
	serialSess := e.NewSession()
	serialRes, serialErrs := serialExecs(t, e, stmts, serialSess)
	batchRes, batchErrs := e.ExecuteAsSessionBatch(stmts, sql.SuperUser, sess)
	if len(batchRes) != len(stmts) || len(batchErrs) != len(stmts) {
		t.Fatalf("batch len: want (%d,%d), got (%d,%d)", len(stmts), len(stmts), len(batchRes), len(batchErrs))
	}
	for i := range stmts {
		if (batchErrs[i] == nil) != (serialErrs[i] == nil) {
			t.Fatalf("stmt %d (%q): err mismatch batch=%v serial=%v", i, stmts[i], batchErrs[i], serialErrs[i])
		}
		if batchErrs[i] != nil {
			continue
		}
		if !sameResult(batchRes[i], serialRes[i]) {
			t.Fatalf("stmt %d (%q): batch result != serial result\nbatch: %+v\nserial: %+v", i, stmts[i], batchRes[i], serialRes[i])
		}
	}
}

// TestROBatchAutocommitParallel：autocommit 下多条只读 SELECT 批执行，
// 结果与逐条串行一致（批内每条独立临时事务+独立快照）。
func TestROBatchAutocommitParallel(t *testing.T) {
	e := newEngine(t)
	setupROBatchTable(t, e)
	sess := e.NewSession()
	stmts := []string{
		"SELECT id, name FROM ro_t WHERE id = 1",
		"SELECT COUNT(*) FROM ro_t",
		"SELECT score FROM ro_t WHERE id >= 3 ORDER BY score DESC",
	}
	verifyBatchMatchesSerial(t, e, stmts, sess)
}

// TestROBatchExplicitTxParallel：显式事务内连续只读 SELECT 批执行，
// 结果与串行事务一致（共享事务快照并行读）。
func TestROBatchExplicitTxParallel(t *testing.T) {
	e := newEngine(t)
	setupROBatchTable(t, e)
	sess := e.NewSession()
	if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	stmts := []string{
		"SELECT * FROM ro_t WHERE id = 1",
		"SELECT COUNT(*) FROM ro_t",
		"SELECT name FROM ro_t WHERE score BETWEEN 20 AND 40",
	}
	verifyBatchMatchesSerial(t, e, stmts, sess)
	if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
}

// TestROBatchOrderPreserved：批序重组正确性——语句顺序与结果/错误数组
// 严格对应，即使各条执行耗时不同（用不同扫描量放大差异）。
func TestROBatchOrderPreserved(t *testing.T) {
	e := newEngine(t)
	setupROBatchTable(t, e)
	// 追加数据放大扫描量差异：最后一条全表扫描、中间空结果、首条点查。
	mustExec(t, e, "INSERT INTO ro_t VALUES (6,'f',60),(7,'g',70),(8,'h',80),(9,'i',90),(10,'j',100),(11,'k',110),(12,'l',120),(13,'m',130),(14,'n',140),(15,'o',150)")
	sess := e.NewSession()
	stmts := []string{
		"SELECT name FROM ro_t WHERE id = 3",
		"SELECT COUNT(*) FROM ro_t WHERE score > 999",
		"SELECT COUNT(*) FROM ro_t",
	}
	verifyBatchMatchesSerial(t, e, stmts, sess)
	// 显式检查第 1 条确实命中了 'c'（批序重组后结果归属正确）。
	res, errs := e.ExecuteAsSessionBatch([]string{"SELECT name FROM ro_t WHERE id = 3"}, sql.SuperUser, sess)
	if len(res) != 1 || len(errs) != 1 || errs[0] != nil || res[0] == nil || len(res[0].Rows) != 1 {
		t.Fatalf("point query batch: res=%+v errs=%+v", res, errs)
	}
	if res[0].Rows[0][0].S != "c" {
		t.Fatalf("point query row: want c, got %+v", res[0].Rows[0][0])
	}
}

// TestROBatchFallbackOnUserVar：批内含读取用户变量的 SELECT 整批回退
// 串行；SET 写者/读取者语义与串行一致（@var 依赖保守判定）。
func TestROBatchFallbackOnUserVar(t *testing.T) {
	e := newEngine(t)
	setupROBatchTable(t, e)
	sess := e.NewSession()
	if _, err := e.ExecuteAsSession("SET @ro_a = 42", sql.SuperUser, sess); err != nil {
		t.Fatalf("SET @ro_a: %v", err)
	}
	// 含 @var 读取 → 整批回退串行；结果与逐条一致（串行基准会话预置相同 @var）。
	serialSess := e.NewSession()
	if _, err := e.ExecuteAsSession("SET @ro_a = 42", sql.SuperUser, serialSess); err != nil {
		t.Fatalf("SET @ro_a (serial): %v", err)
	}
	stmts := []string{
		"SELECT @ro_a AS a",
		"SELECT id FROM ro_t WHERE id = 1",
		"SELECT @ro_a + 1 AS b",
	}
	serialRes, serialErrs := serialExecs(t, e, stmts, serialSess)
	batchRes, batchErrs := e.ExecuteAsSessionBatch(stmts, sql.SuperUser, sess)
	for i := range stmts {
		if (batchErrs[i] == nil) != (serialErrs[i] == nil) {
			t.Fatalf("stmt %d (%q): err mismatch batch=%v serial=%v", i, stmts[i], batchErrs[i], serialErrs[i])
		}
		if batchErrs[i] == nil && !sameResult(batchRes[i], serialRes[i]) {
			t.Fatalf("stmt %d (%q): batch result != serial result", i, stmts[i])
		}
	}
	// 混入 SET 写者：回退串行后 SET 生效，后续读取可见。
	stmts2 := []string{
		"SELECT id FROM ro_t WHERE id = 2",
		"SET @ro_b = 7",
		"SELECT @ro_b AS c",
	}
	res2, errs2 := e.ExecuteAsSessionBatch(stmts2, sql.SuperUser, sess)
	if errs2[0] != nil || errs2[1] != nil || errs2[2] != nil {
		t.Fatalf("mixed batch errs: %+v", errs2)
	}
	if res2[2] == nil || len(res2[2].Rows) != 1 || res2[2].Rows[0][0].I != 7 {
		t.Fatalf("SET visible check: res=%+v", res2[2])
	}
}

// TestROBatchSerializableFallback：Serializable 隔离下批内 SELECT 整批
// 回退串行（readSet 共享集合非线程安全，保守回退保证语义等价）。
func TestROBatchSerializableFallback(t *testing.T) {
	e := newEngine(t)
	setupROBatchTable(t, e)
	sess := e.NewSession()
	if _, err := e.ExecuteAsSession("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", sql.SuperUser, sess); err != nil {
		t.Fatalf("SET ISOLATION: %v", err)
	}
	if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	stmts := []string{
		"SELECT id FROM ro_t WHERE id = 1",
		"SELECT COUNT(*) FROM ro_t",
	}
	verifyBatchMatchesSerial(t, e, stmts, sess)
	if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
}

// TestROBatchErrorPropagation：批内单条错误按原位置返回，其余语句正常
// 执行（与调用方逐条串行循环一致）。
func TestROBatchErrorPropagation(t *testing.T) {
	e := newEngine(t)
	setupROBatchTable(t, e)
	sess := e.NewSession()
	stmts := []string{
		"SELECT id FROM ro_t WHERE id = 1",
		"SELECT * FROM no_such_table_ro",
		"SELECT id FROM ro_t WHERE id = 2",
	}
	res, errs := e.ExecuteAsSessionBatch(stmts, sql.SuperUser, sess)
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("want stmt0/2 ok, got %v / %v", errs[0], errs[2])
	}
	if errs[1] == nil {
		t.Fatalf("want stmt1 error, got nil")
	}
	if res[0] == nil || res[2] == nil {
		t.Fatalf("want stmt0/2 results, got %v / %v", res[0], res[2])
	}
	// 与逐条串行错误一致。
	serialSess := e.NewSession()
	_, serr := e.ExecuteAsSession(stmts[1], sql.SuperUser, serialSess)
	if serr == nil {
		t.Fatalf("serial stmt1: want error, got nil")
	}
	if serr.Error() != errs[1].Error() {
		t.Fatalf("error mismatch: batch=%v serial=%v", errs[1], serr)
	}
}

// TestROBatchEmptyAndSingle：空批与单条批行为正确。
func TestROBatchEmptyAndSingle(t *testing.T) {
	e := newEngine(t)
	setupROBatchTable(t, e)
	sess := e.NewSession()
	res, errs := e.ExecuteAsSessionBatch(nil, sql.SuperUser, sess)
	if len(res) != 0 || len(errs) != 0 {
		t.Fatalf("empty batch: res=%d errs=%d", len(res), len(errs))
	}
	single := []string{"SELECT id FROM ro_t WHERE id = 4"}
	verifyBatchMatchesSerial(t, e, single, sess)
}

// TestROBatchInTxReadYourWrites：显式事务内先写后批读，批内 SELECT
// 能读到本事务未提交的写入（read-your-writes 语义保持）。
func TestROBatchInTxReadYourWrites(t *testing.T) {
	e := newEngine(t)
	setupROBatchTable(t, e)
	sess := e.NewSession()
	if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := e.ExecuteAsSession("INSERT INTO ro_t VALUES (100, 'z', 999)", sql.SuperUser, sess); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	stmts := []string{
		"SELECT name FROM ro_t WHERE id = 100",
		"SELECT COUNT(*) FROM ro_t",
	}
	res, errs := e.ExecuteAsSessionBatch(stmts, sql.SuperUser, sess)
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("batch errs: %v / %v", errs[0], errs[1])
	}
	if res[0] == nil || len(res[0].Rows) != 1 || res[0].Rows[0][0].S != "z" {
		t.Fatalf("read-your-writes id=100: res=%+v", res[0])
	}
	if res[1] == nil || len(res[1].Rows) != 1 || res[1].Rows[0][0].I != 6 {
		t.Fatalf("count after insert: res=%+v (want 6)", res[1])
	}
	if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
}

// TestROBatchSnapshotIsolation：显式事务批内读取保持 BEGIN 快照（不
// 受其他会话并发提交影响），与串行快照隔离语义一致。
func TestROBatchSnapshotIsolation(t *testing.T) {
	e := newEngine(t)
	setupROBatchTable(t, e)
	sess := e.NewSession()
	other := e.NewSession()
	if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	// 其他会话提交一行（批 BEGIN 之后提交，快照隔离下批内不可见）。
	if _, err := e.ExecuteAsSession("INSERT INTO ro_t VALUES (200, 'other', 1)", sql.SuperUser, other); err != nil {
		t.Fatalf("other INSERT: %v", err)
	}
	stmts := []string{
		"SELECT COUNT(*) FROM ro_t",
		"SELECT name FROM ro_t WHERE id = 200",
	}
	res, errs := e.ExecuteAsSessionBatch(stmts, sql.SuperUser, sess)
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("batch errs: %v / %v", errs[0], errs[1])
	}
	if res[0] == nil || len(res[0].Rows) != 1 || res[0].Rows[0][0].I != 5 {
		t.Fatalf("count under snapshot: res=%+v (want 5, other tx not visible)", res[0])
	}
	if res[1] == nil || len(res[1].Rows) != 0 {
		t.Fatalf("id=200 under snapshot: res=%+v (want empty)", res[1])
	}
	if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
}

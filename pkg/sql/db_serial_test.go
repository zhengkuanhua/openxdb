package sql_test

// P2-13 Serializable 隔离级别接入测试（单机侧）。
// 覆盖：
//   - SET [SESSION] TRANSACTION ISOLATION LEVEL 语法解析与级别映射；
//   - 会话级 / 语句级（仅下一事务）作用域语义；
//   - 提交时 rw-antidependency 冲突检测（storage.ErrConflict 语义）：
//     读集与活跃/已提交写集相交即拒绝，返回 "storage: write conflict"；
//   - 与读已提交（不启用冲突检测）对比，保证既有级别行为不回归；
//   - 冲突后事务已回滚、会话可继续开启新事务。

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// sessExec 以指定 Session 执行语句（多会话并发/隔离场景统一入口）。
func sessExec(t *testing.T, e *sql.Engine, sess *sql.Session, stmt string) (*sql.Result, error) {
	t.Helper()
	return e.ExecuteAsSession(stmt, sql.SuperUser, sess)
}

// mustSessExec 断言执行成功。
func mustSessExec(t *testing.T, e *sql.Engine, sess *sql.Session, stmt string) *sql.Result {
	t.Helper()
	res, err := sessExec(t, e, sess, stmt)
	if err != nil {
		t.Fatalf("ExecuteAsSession(%q): %v", stmt, err)
	}
	return res
}

// sessErr 断言执行失败且错误包含指定子串。
func sessErr(t *testing.T, e *sql.Engine, sess *sql.Session, stmt, wantSub string) {
	t.Helper()
	_, err := sessExec(t, e, sess, stmt)
	if err == nil {
		t.Fatalf("ExecuteAsSession(%q): want error containing %q, got nil", stmt, wantSub)
	}
	if !strings.Contains(err.Error(), wantSub) {
		t.Fatalf("ExecuteAsSession(%q): want error containing %q, got %v", stmt, wantSub, err)
	}
}

// newSerialEngine 建表并返回引擎：t(id PK, v int)，初始 (1,100)。
func newSerialEngine(t *testing.T) *sql.Engine {
	t.Helper()
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	mustExec(t, e, "INSERT INTO t VALUES (1, 100)")
	mustExec(t, e, "INSERT INTO t VALUES (2, 200)")
	return e
}

// TestP213ParseSetIsolationLevels SET TRANSACTION/SESSION 隔离级别语法解析：
// 大小写不敏感、三种级别映射正确；非法级别/缺 LEVEL 拒绝。
func TestP213ParseSetIsolationLevels(t *testing.T) {
	cases := []struct {
		sqlText string
		want    sql.IsoLevel
	}{
		{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", sql.IsoSerializable},
		{"set session transaction isolation level read committed", sql.IsoReadCommitted},
		{"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", sql.IsoSnapshot},
		{"Set Transaction Isolation Level Serializable", sql.IsoSerializable},
	}
	for _, c := range cases {
		st, err := sql.Parse(c.sqlText)
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.sqlText, err)
		}
		ss, ok := st.(*sql.SetStmt)
		if !ok {
			t.Fatalf("Parse(%q): got %T, want *SetStmt", c.sqlText, st)
		}
		if !ss.IsoSet {
			t.Fatalf("Parse(%q): IsoSet=false", c.sqlText)
		}
		if ss.IsoLevel != c.want {
			t.Fatalf("Parse(%q): IsoLevel=%v, want %v", c.sqlText, ss.IsoLevel, c.want)
		}
	}
	bad := []string{
		"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
		"SET TRANSACTION ISOLATION LEVEL BOGUS",
		"SET TRANSACTION ISOLATION", // 缺 LEVEL 值
	}
	for _, s := range bad {
		if _, err := sql.Parse(s); err == nil {
			t.Fatalf("Parse(%q): want error, got nil", s)
		}
	}
}

// TestP213SetIsolationLevelExecutes 隔离级别 SET 语句可执行且不影响既有 SET 变量路径。
func TestP213SetIsolationLevelExecutes(t *testing.T) {
	e := newSerialEngine(t)
	sess := e.NewSession()
	mustSessExec(t, e, sess, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	mustSessExec(t, e, sess, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	mustSessExec(t, e, sess, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	// 普通变量 SET 零回归
	mustSessExec(t, e, sess, "SET @x = 1")
	mustExec(t, e, "SET @y = 2")
}

// TestP213SerializableConflictCommittedWrite Serializable 事务读集与
// 已提交并发写集相交（rw-antidependency）→ 提交时 ErrConflict；
// 冲突后事务已回滚、会话可开启新事务。
func TestP213SerializableConflictCommittedWrite(t *testing.T) {
	e := newSerialEngine(t)
	sA := e.NewSession()
	sB := e.NewSession()
	mustSessExec(t, e, sA, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	mustSessExec(t, e, sB, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")

	// A 读 id=1（读集 {1}）
	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "SELECT v FROM t WHERE id = 1")

	// B 写 id=1 并提交（已提交写集 {1}）
	mustSessExec(t, e, sB, "BEGIN")
	mustSessExec(t, e, sB, "UPDATE t SET v = 200 WHERE id = 1")
	mustSessExec(t, e, sB, "COMMIT")

	// A 提交：读集 ∩ 已提交写集 ≠ ∅ → 冲突
	sessErr(t, e, sA, "COMMIT", "write conflict")

	// 冲突即回滚：再次 COMMIT 报无活动事务
	sessErr(t, e, sA, "COMMIT", "no active transaction")

	// 会话可继续开启新事务（隔离级别仍为 SERIALIZABLE）
	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "COMMIT")
}

// TestP213SerializableConflictActiveWrite Serializable 事务读集与
// 未提交活跃写集相交 → 提交时 ErrConflict（活跃写集检测路径）。
func TestP213SerializableConflictActiveWrite(t *testing.T) {
	e := newSerialEngine(t)
	sA := e.NewSession()
	sB := e.NewSession()
	mustSessExec(t, e, sA, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	mustSessExec(t, e, sB, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")

	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "SELECT v FROM t WHERE id = 1")

	// B 写 id=1 未提交（活跃写集 {1}）
	mustSessExec(t, e, sB, "BEGIN")
	mustSessExec(t, e, sB, "UPDATE t SET v = 300 WHERE id = 1")

	// A 提交 → 冲突；B 回滚释放
	sessErr(t, e, sA, "COMMIT", "write conflict")
	mustSessExec(t, e, sB, "ROLLBACK")
}

// TestP213SerializableNoConflictDifferentKeys 不同 key 读写不相交 →
// 提交成功（不误报冲突）。
func TestP213SerializableNoConflictDifferentKeys(t *testing.T) {
	e := newSerialEngine(t)
	sA := e.NewSession()
	sB := e.NewSession()
	mustSessExec(t, e, sA, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	mustSessExec(t, e, sB, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")

	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "SELECT v FROM t WHERE id = 1")

	mustSessExec(t, e, sB, "BEGIN")
	mustSessExec(t, e, sB, "UPDATE t SET v = 300 WHERE id = 2")
	mustSessExec(t, e, sB, "COMMIT")

	// 读集 {1} 与已提交写集 {2} 不相交 → 提交成功
	mustSessExec(t, e, sA, "COMMIT")
}

// TestP213ReadCommittedAllowsConflict 读已提交（既有级别）同场景不报冲突：
// Serializable 冲突检测开启不影响非 Serializable 事务。
func TestP213ReadCommittedAllowsConflict(t *testing.T) {
	e := newSerialEngine(t)
	sA := e.NewSession()
	sB := e.NewSession()
	mustSessExec(t, e, sA, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	mustSessExec(t, e, sB, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "SELECT v FROM t WHERE id = 1")

	mustSessExec(t, e, sB, "BEGIN")
	mustSessExec(t, e, sB, "UPDATE t SET v = 200 WHERE id = 1")
	mustSessExec(t, e, sB, "COMMIT")

	// 读已提交不做 rw-antidependency 检测：A 提交成功
	mustSessExec(t, e, sA, "COMMIT")
}

// TestP213SnapshotDefaultZeroRegression 默认隔离级别（快照隔离）零回归：
// 不开启冲突检测，既有显式事务行为不变。
func TestP213SnapshotDefaultZeroRegression(t *testing.T) {
	e := newSerialEngine(t)
	sA := e.NewSession()
	sB := e.NewSession()

	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "SELECT v FROM t WHERE id = 1")

	mustSessExec(t, e, sB, "BEGIN")
	mustSessExec(t, e, sB, "UPDATE t SET v = 200 WHERE id = 1")
	mustSessExec(t, e, sB, "COMMIT")

	mustSessExec(t, e, sA, "COMMIT")
}

// TestP213NextTxScopeOnly SET TRANSACTION（语句级）仅影响下一事务：
// 下一事务为 Serializable 并冲突检测生效；该事务结束后复位为会话默认
// （快照隔离），同场景不再冲突。
func TestP213NextTxScopeOnly(t *testing.T) {
	e := newSerialEngine(t)
	sA := e.NewSession()
	sB := e.NewSession()
	mustSessExec(t, e, sB, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")

	// 语句级 SET TRANSACTION：仅影响 sA 的下一事务
	mustSessExec(t, e, sA, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "SELECT v FROM t WHERE id = 1")

	mustSessExec(t, e, sB, "BEGIN")
	mustSessExec(t, e, sB, "UPDATE t SET v = 200 WHERE id = 1")
	mustSessExec(t, e, sB, "COMMIT")

	sessErr(t, e, sA, "COMMIT", "write conflict")

	// 下一事务已复位为会话默认（SI）：同场景提交成功
	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "SELECT v FROM t WHERE id = 1")
	mustSessExec(t, e, sA, "COMMIT")
}

// TestP213SessionScopePersists SESSION 级 SET 对会话内后续事务持续生效，
// 不会被单个事务消费掉。
func TestP213SessionScopePersists(t *testing.T) {
	e := newSerialEngine(t)
	sA := e.NewSession()
	sB := e.NewSession()
	mustSessExec(t, e, sA, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	mustSessExec(t, e, sB, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")

	// 第一轮冲突
	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "SELECT v FROM t WHERE id = 1")
	mustSessExec(t, e, sB, "BEGIN")
	mustSessExec(t, e, sB, "UPDATE t SET v = 201 WHERE id = 1")
	mustSessExec(t, e, sB, "COMMIT")
	sessErr(t, e, sA, "COMMIT", "write conflict")

	// 第二轮：SESSION 级别仍在，再次冲突
	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "SELECT v FROM t WHERE id = 1")
	mustSessExec(t, e, sB, "BEGIN")
	mustSessExec(t, e, sB, "UPDATE t SET v = 202 WHERE id = 1")
	mustSessExec(t, e, sB, "COMMIT")
	sessErr(t, e, sA, "COMMIT", "write conflict")
}

// TestP213SerializableOrderedCommitNoConflict 无交集的顺序提交不误报；
// 且提交推进后新事务读到最新值（与 SI 快照语义一致）。
func TestP213SerializableOrderedCommitNoConflict(t *testing.T) {
	e := newSerialEngine(t)
	sA := e.NewSession()
	sB := e.NewSession()
	mustSessExec(t, e, sA, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	mustSessExec(t, e, sB, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")

	mustSessExec(t, e, sA, "BEGIN")
	mustSessExec(t, e, sA, "SELECT v FROM t WHERE id = 1")
	mustSessExec(t, e, sB, "BEGIN")
	mustSessExec(t, e, sB, "UPDATE t SET v = 300 WHERE id = 2")
	mustSessExec(t, e, sB, "COMMIT")
	mustSessExec(t, e, sA, "COMMIT")

	// 后续新事务读取最新已提交值
	mustSessExec(t, e, sB, "BEGIN")
	res := mustSessExec(t, e, sB, "SELECT v FROM t WHERE id = 2")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 300 {
		t.Fatalf("latest value after serializable commits: %+v", res.Rows)
	}
	mustSessExec(t, e, sB, "COMMIT")
}

package sql_test

import (
	"testing"
)

// D6.2：replica 只读强制（排期 Phase F / F2）。开启后写语句按 isWriteStmt
// 白名单拒绝，只读查询/事务控制放行；关闭后恢复写（提升主节点语义）。
func TestD6ReplicaReadOnly(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_d6ro (id INT PRIMARY KEY, name TEXT)")
	mustExec(t, e, "INSERT INTO t_d6ro VALUES (1, 'a')")

	// 默认（主端）行为：读写均放行。
	if e.IsReplicaReadOnly() {
		t.Fatal("default engine must not be read-only")
	}
	mustRows(t, e, "SELECT id, name FROM t_d6ro")

	// 开启只读强制：SELECT 放行、写语句拒绝。
	e.SetReplicaReadOnly(true)
	if !e.IsReplicaReadOnly() {
		t.Fatal("SetReplicaReadOnly(true) not effective")
	}
	mustRows(t, e, "SELECT id, name FROM t_d6ro")
	mustRows(t, e, "SELECT version()")
	mustRows(t, e, "SELECT COUNT(*) FROM t_d6ro")
	writeStmts := []string{
		"INSERT INTO t_d6ro VALUES (2, 'b')",
		"UPDATE t_d6ro SET name='x' WHERE id=1",
		"DELETE FROM t_d6ro WHERE id=1",
		"CREATE TABLE t_d6ro_x (id INT)",
		"DROP TABLE t_d6ro",
		"CREATE INDEX idx_d6ro ON t_d6ro(name)",
		"ALTER TABLE t_d6ro ADD COLUMN extra INT",
	}
	for _, s := range writeStmts {
		mustErr(t, e, s, "read-only replica")
	}

	// 关闭只读强制（提升主节点）：写恢复。
	e.SetReplicaReadOnly(false)
	if e.IsReplicaReadOnly() {
		t.Fatal("SetReplicaReadOnly(false) not effective")
	}
	mustExec(t, e, "INSERT INTO t_d6ro VALUES (2, 'b')")
	rows := mustRows(t, e, "SELECT COUNT(*) FROM t_d6ro")
	if rows[0][0].String() != "2" {
		t.Fatalf("after re-enable write: got %+v", rows[0])
	}
}

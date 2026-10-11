package sql_test

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// M11 子功能 3：SQL 执行期权限校验（pkg/sql/authz.go）。
// 复用 sql_test.go 的 newEngine/mustExec/mustErr/setupUsers 等公共 helper。

// mustExecAs 以指定用户身份执行并断言成功。
func mustExecAs(t *testing.T, e *sql.Engine, stmt, user string) *sql.Result {
	t.Helper()
	res, err := e.ExecuteAs(stmt, user)
	if err != nil {
		t.Fatalf("ExecuteAs(%q, user=%q): %v", stmt, user, err)
	}
	return res
}

// mustErrAs 以指定用户身份执行并断言返回包含 wantSub 的错误。
func mustErrAs(t *testing.T, e *sql.Engine, stmt, user, wantSub string) {
	t.Helper()
	_, err := e.ExecuteAs(stmt, user)
	if err == nil {
		t.Fatalf("ExecuteAs(%q, user=%q) succeeded, want error containing %q", stmt, user, wantSub)
	}
	if !strings.Contains(err.Error(), wantSub) {
		t.Fatalf("ExecuteAs(%q, user=%q) error = %q, want substring %q", stmt, user, err.Error(), wantSub)
	}
}

// TestExecuteAsAdminOnlySuperuser 管理语句（DDL/用户/授权/运维）仅 root 可执行。
func TestExecuteAsAdminOnlySuperuser(t *testing.T) {
	e := newEngine(t)
	// 初始化：root 身份创建用户与表
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

	// 普通用户 alice 执行各类管理语句一律权限拒绝
	mustErrAs(t, e, "CREATE TABLE t (id INT PRIMARY KEY)", "alice", "permission denied")
	mustErrAs(t, e, "DROP TABLE users", "alice", "permission denied")
	mustErrAs(t, e, "CREATE INDEX idx ON users (name)", "alice", "permission denied")
	mustErrAs(t, e, "DROP INDEX idx ON users", "alice", "permission denied")
	mustErrAs(t, e, "ALTER TABLE users ADD COLUMN age INT", "alice", "permission denied")
	mustErrAs(t, e, "CREATE USER 'bob' IDENTIFIED BY 'pw2'", "alice", "permission denied")
	mustErrAs(t, e, "DROP USER 'bob'", "alice", "permission denied")
	mustErrAs(t, e, "ALTER USER 'alice' IDENTIFIED BY 'newpw'", "alice", "permission denied")
	mustErrAs(t, e, "GRANT SELECT ON users TO 'alice'", "alice", "permission denied")
	mustErrAs(t, e, "REVOKE SELECT ON users FROM 'alice'", "alice", "permission denied")
	mustErrAs(t, e, "CREATE VIEW v_users AS SELECT * FROM users", "alice", "permission denied")
	mustErrAs(t, e, "DROP VIEW v_users", "alice", "permission denied")

	// root 全部放行
	mustExecAs(t, e, "CREATE TABLE t (id INT PRIMARY KEY)", sql.SuperUser)
	mustExecAs(t, e, "GRANT SELECT ON users TO 'alice'", sql.SuperUser)
	// 向后兼容：Engine.Execute 默认以 root 执行，同样放行
	mustExec(t, e, "CREATE TABLE t2 (id INT PRIMARY KEY)")
}

// TestExecuteAsDMLPrivilege 非管理语句执行前经 HasPrivilege 检查表级权限。
func TestExecuteAsDMLPrivilege(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")

	// 未授权：四类 DML 全部拒绝
	mustErrAs(t, e, "SELECT * FROM users", "alice", "permission denied")
	mustErrAs(t, e, "INSERT INTO users (id, name, age) VALUES (9, 'x', 1)", "alice", "permission denied")
	mustErrAs(t, e, "UPDATE users SET age = 1 WHERE id = 1", "alice", "permission denied")
	mustErrAs(t, e, "DELETE FROM users WHERE id = 1", "alice", "permission denied")

	// GRANT SELECT 后仅 SELECT 放行
	mustExec(t, e, "GRANT SELECT ON users TO 'alice'")
	mustExecAs(t, e, "SELECT * FROM users", "alice")
	mustErrAs(t, e, "INSERT INTO users (id, name, age) VALUES (9, 'x', 1)", "alice", "permission denied")
	mustErrAs(t, e, "UPDATE users SET age = 1 WHERE id = 1", "alice", "permission denied")

	// GRANT ALL 后四类 DML 全部放行
	mustExec(t, e, "GRANT ALL ON users TO 'alice'")
	mustExecAs(t, e, "SELECT * FROM users", "alice")
	mustExecAs(t, e, "INSERT INTO users (id, name, age) VALUES (9, 'x', 1)", "alice")
	mustExecAs(t, e, "UPDATE users SET age = 1 WHERE id = 9", "alice")
	mustExecAs(t, e, "DELETE FROM users WHERE id = 9", "alice")

	// 其他表无授权仍拒绝
	mustExec(t, e, "CREATE TABLE t2 (id INT PRIMARY KEY)")
	mustErrAs(t, e, "SELECT * FROM t2", "alice", "permission denied")
}

// TestExecuteAsGuest 降级 guest（空用户）无任何授权：DML 与管理语句拒绝，
// 只读元数据/会话级语句放行；表不存在与无授权统一返回权限拒绝（不泄露元数据）。
func TestExecuteAsGuest(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	mustErrAs(t, e, "SELECT * FROM users", "", "permission denied")
	mustErrAs(t, e, "INSERT INTO users (id, name, age) VALUES (9, 'x', 1)", "", "permission denied")
	mustErrAs(t, e, "CREATE TABLE t (id INT PRIMARY KEY)", "", "permission denied")
	mustErrAs(t, e, "GRANT SELECT ON users TO 'alice'", "", "permission denied")

	// 元数据/会话级语句放行
	mustExecAs(t, e, "SHOW TABLES", "")
	mustExecAs(t, e, "BEGIN", "")
	mustExecAs(t, e, "ROLLBACK", "")
	mustExecAs(t, e, "EXPLAIN SELECT * FROM users", "")

	// 表不存在与无授权统一权限拒绝（存在性不泄露）
	mustErrAs(t, e, "SELECT * FROM ghost", "", "permission denied")
}

// TestExecuteAsRootPasswordRoundTrip root 用户自身经 AuthenticateUser 校验
// 通过后可执行全部语句（与 server 层握手配套的 SQL 层闭环）。
func TestExecuteAsRootPasswordRoundTrip(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE USER 'root' IDENTIFIED BY 'rootpw'")

	ok, err := e.AuthenticateUser("root", "rootpw")
	if err != nil || !ok {
		t.Fatalf("AuthenticateUser(root, rootpw) = %v, %v; want true, nil", ok, err)
	}
	mustExecAs(t, e, "CREATE TABLE t (id INT PRIMARY KEY)", sql.SuperUser)
	mustExecAs(t, e, "DROP TABLE t", sql.SuperUser)
}

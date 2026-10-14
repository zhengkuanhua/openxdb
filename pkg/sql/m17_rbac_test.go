package sql_test

// M17 第二步：细粒度权限/角色体系（RBAC）。
// 覆盖：CREATE ROLE / DROP ROLE / SHOW ROLES / SHOW GRANTS、
// GRANT/REVOKE ... ON DATABASE、GRANT/REVOKE ... ON TABLE(col...)、
// GRANT ROLE / REVOKE ROLE、角色继承后的权限聚合与拒绝、
// DROP USER / DROP TABLE / DROP ROLE 联动清理。
// 沿用 M11 测试风格：真实存储（newEngine）+ root 建表授权 + ExecuteAs 断言。

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

func TestM17RoleLifecycleShowDrop(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")

	// CREATE ROLE 幂等唯一性
	mustExec(t, e, "CREATE ROLE 'analyst'")
	mustExec(t, e, "CREATE ROLE 'viewer'")
	mustErr(t, e, "CREATE ROLE 'analyst'", "role already exists")

	// SHOW ROLES 返回全部角色（排序）
	rows := mustRows(t, e, "SHOW ROLES")
	var names []string
	for _, r := range rows {
		names = append(names, r[0].S)
	}
	if len(names) != 2 || names[0] != "analyst" || names[1] != "viewer" {
		t.Fatalf("SHOW ROLES: want [analyst viewer], got %v", names)
	}

	// GRANT ROLE 挂载角色后 SHOW GRANTS 可见角色行
	mustExec(t, e, "GRANT ROLE analyst TO 'alice'")
	mustExec(t, e, "GRANT SELECT ON users TO ROLE 'analyst'")
	g := mustRows(t, e, "SHOW GRANTS FOR 'alice'")
	if !containsGrant(g, "GRANT ROLE analyst TO 'alice'") {
		t.Fatalf("SHOW GRANTS: want role line, got %v", dumpGrants(g))
	}
	if !containsGrant(g, "(role analyst) GRANT SELECT ON users") {
		t.Fatalf("SHOW GRANTS: want role-expanded line, got %v", dumpGrants(g))
	}

	// DROP ROLE 联动清理用户映射与角色内授权
	mustExec(t, e, "DROP ROLE 'analyst'")
	g = mustRows(t, e, "SHOW GRANTS FOR 'alice'")
	if containsGrant(g, "analyst") {
		t.Fatalf("SHOW GRANTS after DROP ROLE: analyst should be gone, got %v", dumpGrants(g))
	}

	// DROP ROLE IF EXISTS 幂等；不存在且无 IF EXISTS 报错
	mustExec(t, e, "DROP ROLE IF EXISTS 'analyst'")
	mustErr(t, e, "DROP ROLE 'ghost'", "role not exists: ghost")
}

func TestM17GrantRevokeDatabase(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")

	// 库级授权前：无权限
	if ok, err := e.HasPrivilege("alice", "users", "SELECT"); err != nil || ok {
		t.Fatalf("HasPrivilege before db grant: want false, got ok=%v err=%v", ok, err)
	}
	mustErrAs(t, e, "SELECT * FROM users", "alice", "permission denied")

	// GRANT ... ON DATABASE 后：库内任意表获得对应权限
	mustExec(t, e, "GRANT SELECT ON DATABASE default TO 'alice'")
	ok, err := e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || !ok {
		t.Fatalf("HasPrivilege(SELECT via db grant): want true, got ok=%v err=%v", ok, err)
	}
	mustExecAs(t, e, "SELECT * FROM users", "alice")
	// 未授予的操作仍拒绝
	mustErrAs(t, e, "INSERT INTO users (id, name, age) VALUES (9, 'x', 1)", "alice", "permission denied")

	// REVOKE ... ON DATABASE 后恢复拒绝
	mustExec(t, e, "REVOKE SELECT ON DATABASE default FROM 'alice'")
	ok, err = e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege after db revoke: want false, got ok=%v err=%v", ok, err)
	}
	mustErrAs(t, e, "SELECT * FROM users", "alice", "permission denied")
}

func TestM17GrantRevokeColumn(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")

	// 列级 SELECT：仅显式授权列放行
	mustExec(t, e, "GRANT SELECT ON users (id, name) TO 'alice'")
	mustExecAs(t, e, "SELECT id FROM users", "alice")
	mustExecAs(t, e, "SELECT id, name FROM users", "alice")
	mustErrAs(t, e, "SELECT * FROM users", "alice", "permission denied")
	mustErrAs(t, e, "SELECT age FROM users", "alice", "permission denied")

	// 列级 INSERT：显式列清单均被授权时可插入，缺少任一列权限则拒绝
	mustExec(t, e, "GRANT INSERT ON users (id, name, age) TO 'alice'")
	mustExecAs(t, e, "INSERT INTO users (id, name, age) VALUES (99, 'x', 1)", "alice")
	mustExec(t, e, "REVOKE INSERT ON users (age) FROM 'alice'")
	mustErrAs(t, e, "INSERT INTO users (id, name, age) VALUES (99, 'x', 1)", "alice", "permission denied")
	// 无列清单的全列 INSERT 需要表级权限
	mustErrAs(t, e, "INSERT INTO users VALUES (100, 'y', 1)", "alice", "permission denied")

	// 列级 UPDATE：仅授权 SET 列放行
	mustExec(t, e, "GRANT UPDATE ON users (name) TO 'alice'")
	mustExecAs(t, e, "UPDATE users SET name = 'x' WHERE id = 99", "alice")
	mustErrAs(t, e, "UPDATE users SET age = 1 WHERE id = 99", "alice", "permission denied")

	// DELETE 无列级概念：始终需表级权限
	mustErrAs(t, e, "DELETE FROM users WHERE id = 99", "alice", "permission denied")

	// REVOKE 列级后相应列操作拒绝
	mustExec(t, e, "REVOKE SELECT ON users (id, name) FROM 'alice'")
	mustErrAs(t, e, "SELECT id FROM users", "alice", "permission denied")
}

func TestM17RoleInheritanceAggregation(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")

	// 角色表级授权 + 角色挂载 → 继承生效
	mustExec(t, e, "CREATE ROLE 'analyst'")
	mustExec(t, e, "GRANT SELECT ON users TO ROLE 'analyst'")
	mustExec(t, e, "GRANT ROLE analyst TO 'alice'")
	ok, err := e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || !ok {
		t.Fatalf("HasPrivilege(SELECT via role): want true, got ok=%v err=%v", ok, err)
	}
	mustExecAs(t, e, "SELECT * FROM users", "alice")
	mustErrAs(t, e, "INSERT INTO users (id, name, age) VALUES (9, 'x', 1)", "alice", "permission denied")

	// 直授 + 角色聚合：并集生效（角色表级 SELECT + 直授 INSERT）
	mustExec(t, e, "GRANT INSERT ON users TO 'alice'")
	mustExecAs(t, e, "INSERT INTO users (id, name, age) VALUES (9, 'x', 1)", "alice")
	mustExecAs(t, e, "SELECT * FROM users", "alice")

	// REVOKE 角色表级授权：角色内表级权限收回，无其他来源时拒绝
	mustExec(t, e, "REVOKE SELECT ON users FROM ROLE 'analyst'")
	mustErrAs(t, e, "SELECT * FROM users", "alice", "permission denied")

	// 角色库级授权：角色内库级权限同样继承（覆盖库内全部表）
	mustExec(t, e, "CREATE TABLE t2 (id INT PRIMARY KEY)")
	mustErrAs(t, e, "SELECT * FROM t2", "alice", "permission denied")
	mustExec(t, e, "GRANT SELECT ON DATABASE default TO ROLE 'analyst'")
	mustExecAs(t, e, "SELECT * FROM t2", "alice")
	mustExecAs(t, e, "SELECT * FROM users", "alice")

	// REVOKE ROLE 解除角色挂载：角色全部权限失效
	mustExec(t, e, "REVOKE ROLE analyst FROM 'alice'")
	mustErrAs(t, e, "SELECT * FROM t2", "alice", "permission denied")
	mustErrAs(t, e, "SELECT * FROM users", "alice", "permission denied")
	ok, err = e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege after revoke role: want false, got ok=%v err=%v", ok, err)
	}
}

func TestM17DropUserCleansRbac(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "CREATE ROLE 'r1'")
	mustExec(t, e, "GRANT SELECT ON users TO 'alice'")
	mustExec(t, e, "GRANT SELECT ON DATABASE default TO 'alice'")
	mustExec(t, e, "GRANT SELECT ON users (id) TO 'alice'")
	mustExec(t, e, "GRANT ROLE r1 TO 'alice'")

	mustExec(t, e, "DROP USER 'alice'")
	// 删除后 SHOW GRANTS 直接报用户不存在
	mustErr(t, e, "SHOW GRANTS FOR 'alice'", "user not exists: alice")

	// 重建同名用户：表级/库级/列级授权与角色映射均不残留
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	ok, err := e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege after drop+recreate user: want false, got ok=%v err=%v", ok, err)
	}
	mustErrAs(t, e, "SELECT * FROM users", "alice", "permission denied")
	mustErrAs(t, e, "SELECT id FROM users", "alice", "permission denied")
	g := mustRows(t, e, "SHOW GRANTS FOR 'alice'")
	if len(g) != 1 || !strings.Contains(g[0][0].S, "has no grants") {
		t.Fatalf("SHOW GRANTS after drop+recreate: want only 'has no grants', got %v", dumpGrants(g))
	}
}

func TestM17DropTableCleansRbac(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "CREATE ROLE 'analyst'")
	mustExec(t, e, "GRANT SELECT ON users TO 'alice'")
	mustExec(t, e, "GRANT SELECT ON users (id) TO 'alice'")
	mustExec(t, e, "GRANT SELECT ON users TO ROLE 'analyst'")
	mustExec(t, e, "GRANT ROLE analyst TO 'alice'")
	mustExecAs(t, e, "SELECT id FROM users", "alice")

	mustExec(t, e, "DROP TABLE users")
	mustExec(t, e, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")

	// 表级授权清理
	ok, err := e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege after drop+recreate table: want false, got ok=%v err=%v", ok, err)
	}
	mustErrAs(t, e, "SELECT * FROM users", "alice", "permission denied")
	// 列级授权清理
	mustErrAs(t, e, "SELECT id FROM users", "alice", "permission denied")
	// 角色内表授权清理（角色自身与用户映射保留）
	g := mustRows(t, e, "SHOW GRANTS FOR 'alice'")
	if !containsGrant(g, "GRANT ROLE analyst TO 'alice'") {
		t.Fatalf("SHOW GRANTS: role mapping should stay, got %v", dumpGrants(g))
	}
	if containsGrant(g, "ON users") {
		t.Fatalf("SHOW GRANTS: role table grants on users should be cleaned, got %v", dumpGrants(g))
	}
}

func TestM17DropRoleInvalidatesPrivileges(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "CREATE ROLE 'r2'")
	mustExec(t, e, "GRANT SELECT ON users TO ROLE 'r2'")
	mustExec(t, e, "GRANT ROLE r2 TO 'alice'")
	mustExecAs(t, e, "SELECT * FROM users", "alice")

	mustExec(t, e, "DROP ROLE 'r2'")
	// 角色删除后经该角色的权限立即失效，且用户映射同步清理
	mustErrAs(t, e, "SELECT * FROM users", "alice", "permission denied")
	g := mustRows(t, e, "SHOW GRANTS FOR 'alice'")
	if containsGrant(g, "r2") {
		t.Fatalf("SHOW GRANTS after DROP ROLE: r2 should be gone, got %v", dumpGrants(g))
	}
}

// containsGrant 判断 SHOW GRANTS 结果是否包含指定行文本。
func containsGrant(rows [][]sql.Value, want string) bool {
	for _, r := range rows {
		if len(r) > 0 && strings.Contains(r[0].S, want) {
			return true
		}
	}
	return false
}

// dumpGrants 将 SHOW GRANTS 结果转为可读文本（断言失败时输出用）。
func dumpGrants(rows [][]sql.Value) string {
	var sb strings.Builder
	for _, r := range rows {
		if len(r) > 0 {
			sb.WriteString(r[0].S)
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

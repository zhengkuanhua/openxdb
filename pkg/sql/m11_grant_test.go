package sql_test

import (
	"testing"
)

// M11 子功能 2：权限模型（GRANT/REVOKE）。
// 覆盖：GRANT 单/多权限 + HasPrivilege 查询、GRANT ALL 全权限、授权不存在用户报错、
// 表不存在宽松处理、重复 GRANT 幂等、REVOKE 生效与未授权幂等、REVOKE 不存在用户报错、
// DROP USER 联动清理权限、DROP TABLE 联动清理权限、语法错误。

func TestGrantAndHasPrivilege(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "GRANT SELECT ON users TO 'alice'")
	ok, err := e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || !ok {
		t.Fatalf("HasPrivilege(SELECT): want ok, got ok=%v err=%v", ok, err)
	}
	ok, err = e.HasPrivilege("alice", "users", "INSERT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege(INSERT before grant): want fail, got ok=%v err=%v", ok, err)
	}
	ok, err = e.HasPrivilege("alice", "other", "SELECT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege(other table): want fail, got ok=%v err=%v", ok, err)
	}
	ok, err = e.HasPrivilege("bob", "users", "SELECT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege(other user): want fail, got ok=%v err=%v", ok, err)
	}
}

func TestGrantMultipleAndAll(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "GRANT SELECT, INSERT ON users TO 'alice'")
	for _, op := range []string{"SELECT", "INSERT"} {
		ok, err := e.HasPrivilege("alice", "users", op)
		if err != nil || !ok {
			t.Fatalf("HasPrivilege(%s): want ok, got ok=%v err=%v", op, ok, err)
		}
	}
	ok, err := e.HasPrivilege("alice", "users", "UPDATE")
	if err != nil || ok {
		t.Fatalf("HasPrivilege(UPDATE before ALL): want fail, got ok=%v err=%v", ok, err)
	}
	mustExec(t, e, "GRANT ALL ON users TO 'alice'")
	for _, op := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
		ok, err := e.HasPrivilege("alice", "users", op)
		if err != nil || !ok {
			t.Fatalf("HasPrivilege(%s after ALL): want ok, got ok=%v err=%v", op, ok, err)
		}
	}
}

func TestGrantUserNotExists(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustErr(t, e, "GRANT SELECT ON users TO 'nobody'", "user not exists: nobody")
}

func TestGrantTableLoose(t *testing.T) {
	// 表不存在：宽松处理（不校验表存在性），授权成功且 HasPrivilege 可用。
	e := newEngine(t)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "GRANT SELECT ON ghost TO 'alice'")
	ok, err := e.HasPrivilege("alice", "ghost", "SELECT")
	if err != nil || !ok {
		t.Fatalf("HasPrivilege(ghost table): want ok, got ok=%v err=%v", ok, err)
	}
}

func TestGrantIdempotent(t *testing.T) {
	// 重复 GRANT 幂等：不报错，权限保持。
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "GRANT SELECT ON users TO 'alice'")
	mustExec(t, e, "GRANT SELECT ON users TO 'alice'")
	ok, err := e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || !ok {
		t.Fatalf("HasPrivilege(SELECT after dup grant): want ok, got ok=%v err=%v", ok, err)
	}
}

func TestRevokePrivilege(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "GRANT SELECT, UPDATE ON users TO 'alice'")
	mustExec(t, e, "REVOKE SELECT ON users FROM 'alice'")
	ok, err := e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege(SELECT after revoke): want fail, got ok=%v err=%v", ok, err)
	}
	ok, err = e.HasPrivilege("alice", "users", "UPDATE")
	if err != nil || !ok {
		t.Fatalf("HasPrivilege(UPDATE kept): want ok, got ok=%v err=%v", ok, err)
	}
	// 全部权限被撤销后权限记录随之消失。
	mustExec(t, e, "REVOKE UPDATE ON users FROM 'alice'")
	ok, err = e.HasPrivilege("alice", "users", "UPDATE")
	if err != nil || ok {
		t.Fatalf("HasPrivilege(UPDATE after all revoke): want fail, got ok=%v err=%v", ok, err)
	}
}

func TestRevokeIdempotent(t *testing.T) {
	// 未授权的权限 REVOKE 幂等：不报错。
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "GRANT SELECT ON users TO 'alice'")
	mustExec(t, e, "REVOKE INSERT ON users FROM 'alice'")
	mustExec(t, e, "REVOKE DELETE ON users FROM 'alice'")
	ok, err := e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || !ok {
		t.Fatalf("HasPrivilege(SELECT after unrelated revoke): want ok, got ok=%v err=%v", ok, err)
	}
	// 用户完全没有授权记录时 REVOKE 也幂等。
	mustExec(t, e, "CREATE USER 'bob' IDENTIFIED BY 'p2'")
	mustExec(t, e, "REVOKE ALL ON users FROM 'bob'")
}

func TestRevokeUserNotExists(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustErr(t, e, "REVOKE SELECT ON users FROM 'nobody'", "user not exists: nobody")
}

func TestDropUserCleansGrants(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "GRANT SELECT ON users TO 'alice'")
	mustExec(t, e, "DROP USER 'alice'")
	ok, err := e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege after DROP USER: want fail, got ok=%v err=%v", ok, err)
	}
}

func TestDropTableCleansGrants(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "CREATE USER 'bob' IDENTIFIED BY 'secret2'")
	mustExec(t, e, "GRANT SELECT ON users TO 'alice'")
	mustExec(t, e, "GRANT SELECT ON users TO 'bob'")
	mustExec(t, e, "DROP TABLE users")
	ok, err := e.HasPrivilege("alice", "users", "SELECT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege(alice) after DROP TABLE: want fail, got ok=%v err=%v", ok, err)
	}
	ok, err = e.HasPrivilege("bob", "users", "SELECT")
	if err != nil || ok {
		t.Fatalf("HasPrivilege(bob) after DROP TABLE: want fail, got ok=%v err=%v", ok, err)
	}
}

func TestGrantParseErrors(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustErr(t, e, "GRANT SELECT users TO 'alice'", "expected keyword ON")
	mustErr(t, e, "GRANT SELECT ON users 'alice'", "expected keyword TO")
	mustErr(t, e, "GRANT SELECT ON users TO alice", "expected user name")
	mustErr(t, e, "GRANT DROP ON users TO 'alice'", "unsupported privilege")
	mustErr(t, e, "REVOKE SELECT users FROM 'alice'", "expected keyword ON")
	mustErr(t, e, "REVOKE SELECT ON users FROM alice", "expected user name")
}

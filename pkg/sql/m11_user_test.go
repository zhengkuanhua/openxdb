package sql_test

import (
	"testing"
)

// M11 子功能 1：用户管理（CREATE/DROP/SHOW/ALTER USER）。
// 覆盖：建用户 + SHOW USERS 列出、重复建用户报错、删除用户（含 IF EXISTS 静默）、
// 不存在用户删除报错、ALTER USER 重置密码、AuthenticateUser 鉴权接口
// （正确/错误密码、不存在用户）、SHOW USERS 与表名 users 共存（USERS 不作关键字）。

func TestCreateUserAndShow(t *testing.T) {
	e := newEngine(t)
	// 先建一张名为 users 的表，验证 SHOW USERS 与表名 users 不冲突（USERS 不在关键字表）
	setupUsers(t, e)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "CREATE USER 'bob' IDENTIFIED BY 'secret2'")
	rows := mustRows(t, e, "SHOW USERS")
	if len(rows) != 2 {
		t.Fatalf("SHOW USERS: want 2 rows, got %d", len(rows))
	}
	if rows[0][0].S != "alice" || rows[1][0].S != "bob" {
		t.Fatalf("SHOW USERS: want [alice bob], got [%s %s]", rows[0][0].S, rows[1][0].S)
	}
	// 表 users 仍可用
	mustExec(t, e, "SELECT id FROM users ORDER BY id")
}

func TestCreateUserDuplicate(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustErr(t, e, "CREATE USER 'alice' IDENTIFIED BY 'other'", "user already exists: alice")
}

func TestDropUser(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	mustExec(t, e, "CREATE USER 'bob' IDENTIFIED BY 'secret2'")
	mustExec(t, e, "DROP USER 'alice'")
	rows := mustRows(t, e, "SHOW USERS")
	if len(rows) != 1 || rows[0][0].S != "bob" {
		t.Fatalf("after DROP USER alice: want [bob], got %d rows", len(rows))
	}
	// 删除不存在用户报错
	mustErr(t, e, "DROP USER 'alice'", "user not exists: alice")
	// IF EXISTS 静默成功
	res := mustExec(t, e, "DROP USER IF EXISTS 'alice'")
	if res == nil {
		t.Fatalf("DROP USER IF EXISTS: want ok result, got nil")
	}
	mustExec(t, e, "DROP USER 'bob'")
	rows = mustRows(t, e, "SHOW USERS")
	if len(rows) != 0 {
		t.Fatalf("SHOW USERS after all drops: want 0 rows, got %d", len(rows))
	}
}

func TestAlterUserPassword(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'oldpwd'")
	ok, err := e.AuthenticateUser("alice", "oldpwd")
	if err != nil || !ok {
		t.Fatalf("AuthenticateUser(oldpwd): want ok, got ok=%v err=%v", ok, err)
	}
	mustExec(t, e, "ALTER USER 'alice' IDENTIFIED BY 'newpwd'")
	ok, err = e.AuthenticateUser("alice", "oldpwd")
	if err != nil || ok {
		t.Fatalf("AuthenticateUser(oldpwd after alter): want fail, got ok=%v err=%v", ok, err)
	}
	ok, err = e.AuthenticateUser("alice", "newpwd")
	if err != nil || !ok {
		t.Fatalf("AuthenticateUser(newpwd after alter): want ok, got ok=%v err=%v", ok, err)
	}
	// 不存在用户 ALTER 报错
	mustErr(t, e, "ALTER USER 'nobody' IDENTIFIED BY 'x'", "user not exists: nobody")
}

func TestAuthenticateUser(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'secret1'")
	ok, err := e.AuthenticateUser("alice", "secret1")
	if err != nil || !ok {
		t.Fatalf("AuthenticateUser(correct): want ok, got ok=%v err=%v", ok, err)
	}
	ok, err = e.AuthenticateUser("alice", "wrong")
	if err != nil || ok {
		t.Fatalf("AuthenticateUser(wrong pwd): want fail, got ok=%v err=%v", ok, err)
	}
	ok, err = e.AuthenticateUser("nobody", "secret1")
	if err != nil || ok {
		t.Fatalf("AuthenticateUser(no user): want fail, got ok=%v err=%v", ok, err)
	}
}

func TestShowUsersSort(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE USER 'carol' IDENTIFIED BY 'p1'")
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'p2'")
	mustExec(t, e, "CREATE USER 'bob' IDENTIFIED BY 'p3'")
	rows := mustRows(t, e, "SHOW USERS")
	if len(rows) != 3 {
		t.Fatalf("SHOW USERS: want 3 rows, got %d", len(rows))
	}
	for i, want := range []string{"alice", "bob", "carol"} {
		if rows[i][0].S != want {
			t.Fatalf("SHOW USERS[%d]: want %s, got %s", i, want, rows[i][0].S)
		}
	}
}

func TestUserParseErrors(t *testing.T) {
	e := newEngine(t)
	// 缺 IDENTIFIED BY 报语法错误
	mustErr(t, e, "CREATE USER 'alice'", "expected keyword IDENTIFIED")
	// 用户名必须是字符串字面量
	mustErr(t, e, "CREATE USER alice IDENTIFIED BY 'p'", "expected user name")
	// ALTER USER 缺少新密码
	mustExec(t, e, "CREATE USER 'alice' IDENTIFIED BY 'p1'")
	mustErr(t, e, "ALTER USER 'alice' IDENTIFIED BY", "expected password")
	// SHOW USERS 后不允许多余 token
	mustErr(t, e, "SHOW USERS extra", "unexpected token")
}

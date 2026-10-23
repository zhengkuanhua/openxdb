package db_test

// P2-13 Serializable 隔离级别 × 2PC/行锁 组合测试。
// 覆盖：
//   - Serializable 事务跨 region 读（远端读 key 纳入读集）后，并发 2PC 事务
//     改写同一行并提交 → 提交时 rw-antidependency 冲突（storage.ErrConflict
//     语义），事务回滚、数据未被覆盖；
//   - 跨 region 读不存在（phantom）：2PC 事务随后插入该 key 提交 → 冲突；
//   - 不同 key 读写交错 → 不误报冲突；
//   - Serializable 2PC 写提交不影响未读该行的其它 Serializable 事务。

import (
	"strings"
	"testing"
	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// serialExec 在指定会话上执行 SQL 并返回错误（仅关心执行成败）。
func serialExec(t *testing.T, d *db.DB, sess *sql.Session, stmt string) error {
	t.Helper()
	_, err := d.SQL.ExecuteAsSession(stmt, "root", sess)
	return err
}

// TestP213Serializable2PCConflictRemoteWrite Serializable 事务跨 region 读到
// 旧值，并发 2PC 事务改写同一行提交后，本事务 COMMIT 触发冲突。
func TestP213Serializable2PCConflictRemoteWrite(t *testing.T) {
	dA, _, _ := setup2pc(t)
	sA := dA.SQL.NewSession()
	sB := dA.SQL.NewSession()

	if err := serialExec(t, dA, sA, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		t.Fatalf("sA set session: %v", err)
	}
	if err := serialExec(t, dA, sA, "BEGIN"); err != nil {
		t.Fatalf("sA begin: %v", err)
	}
	// id=4 位于中间 region [4,8)，由 B 持有 → 跨 region 点查（读集含该行 key）
	if err := serialExec(t, dA, sA, "SELECT id FROM users WHERE id = 4"); err != nil {
		t.Fatalf("sA select remote: %v", err)
	}

	if err := serialExec(t, dA, sB, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		t.Fatalf("sB set session: %v", err)
	}
	if err := serialExec(t, dA, sB, "BEGIN"); err != nil {
		t.Fatalf("sB begin: %v", err)
	}
	// 跨 region UPDATE id=4 → 2PC（prepare B → commit）
	if err := serialExec(t, dA, sB, "UPDATE users SET age = 44 WHERE id = 4"); err != nil {
		t.Fatalf("sB 2pc update: %v", err)
	}
	if err := serialExec(t, dA, sB, "COMMIT"); err != nil {
		t.Fatalf("sB commit: %v", err)
	}

	// sA 读到了旧值，sB 已提交覆盖 → sA COMMIT 必须冲突
	err := serialExec(t, dA, sA, "COMMIT")
	if err == nil || !strings.Contains(err.Error(), "write conflict") {
		t.Fatalf("sA commit: want write conflict, got %v", err)
	}
	// 冲突后事务已回滚，数据保持 sB 提交值（age=44）
	wantIDs(t, dA, "SELECT age FROM users WHERE id = 4", 44)
}

// TestP213Serializable2PCConflictPhantom Serializable 事务跨 region 读到
// id=6 不存在，并发 2PC 事务插入 id=6 提交后，本事务 COMMIT 触发冲突。
func TestP213Serializable2PCConflictPhantom(t *testing.T) {
	dA, _, _ := setup2pc(t)
	sA := dA.SQL.NewSession()
	sB := dA.SQL.NewSession()

	if err := serialExec(t, dA, sA, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		t.Fatalf("sA set session: %v", err)
	}
	if err := serialExec(t, dA, sA, "BEGIN"); err != nil {
		t.Fatalf("sA begin: %v", err)
	}
	// 跨 region 点查 id=6（中间 region，B 持有），当前不存在 → phantom 读集记录
	if err := serialExec(t, dA, sA, "SELECT id FROM users WHERE id = 6"); err != nil {
		t.Fatalf("sA select missing remote: %v", err)
	}

	if err := serialExec(t, dA, sB, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		t.Fatalf("sB set session: %v", err)
	}
	if err := serialExec(t, dA, sB, "BEGIN"); err != nil {
		t.Fatalf("sB begin: %v", err)
	}
	if err := serialExec(t, dA, sB, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)"); err != nil {
		t.Fatalf("sB 2pc insert: %v", err)
	}
	if err := serialExec(t, dA, sB, "COMMIT"); err != nil {
		t.Fatalf("sB commit: %v", err)
	}

	err := serialExec(t, dA, sA, "COMMIT")
	if err == nil || !strings.Contains(err.Error(), "write conflict") {
		t.Fatalf("sA commit: want write conflict (phantom), got %v", err)
	}
	wantIDs(t, dA, "SELECT id FROM users WHERE id = 6", 6)
}

// TestP213Serializable2PCNoConflictDifferentKeys 不同 key 读写交错不误报：
// sA 读本地 id=1，sB 2PC 写远端 id=6，两者提交均成功。
func TestP213Serializable2PCNoConflictDifferentKeys(t *testing.T) {
	dA, _, _ := setup2pc(t)
	sA := dA.SQL.NewSession()
	sB := dA.SQL.NewSession()

	if err := serialExec(t, dA, sA, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		t.Fatalf("sA set session: %v", err)
	}
	if err := serialExec(t, dA, sA, "BEGIN"); err != nil {
		t.Fatalf("sA begin: %v", err)
	}
	// 本地读（id=1 在 A 持有 region），读集 {1}
	if err := serialExec(t, dA, sA, "SELECT id FROM users WHERE id = 1"); err != nil {
		t.Fatalf("sA select local: %v", err)
	}

	if err := serialExec(t, dA, sB, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		t.Fatalf("sB set session: %v", err)
	}
	if err := serialExec(t, dA, sB, "BEGIN"); err != nil {
		t.Fatalf("sB begin: %v", err)
	}
	if err := serialExec(t, dA, sB, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)"); err != nil {
		t.Fatalf("sB 2pc insert: %v", err)
	}
	if err := serialExec(t, dA, sB, "COMMIT"); err != nil {
		t.Fatalf("sB commit: %v", err)
	}
	if err := serialExec(t, dA, sA, "COMMIT"); err != nil {
		t.Fatalf("sA commit: want success (different keys), got %v", err)
	}
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
}

// TestP213Serializable2PCWriteOnlyNoConflict 纯写 Serializable 事务（2PC）
// 提交后，不读该行的其它 Serializable 事务提交不受影响。
func TestP213Serializable2PCWriteOnlyNoConflict(t *testing.T) {
	dA, _, _ := setup2pc(t)
	sA := dA.SQL.NewSession()
	sB := dA.SQL.NewSession()

	if err := serialExec(t, dA, sA, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		t.Fatalf("sA set session: %v", err)
	}
	if err := serialExec(t, dA, sA, "BEGIN"); err != nil {
		t.Fatalf("sA begin: %v", err)
	}
	// sA 纯写：跨 region INSERT id=6（2PC），无读集
	if err := serialExec(t, dA, sA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)"); err != nil {
		t.Fatalf("sA 2pc insert: %v", err)
	}
	if err := serialExec(t, dA, sA, "COMMIT"); err != nil {
		t.Fatalf("sA commit: %v", err)
	}

	if err := serialExec(t, dA, sB, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		t.Fatalf("sB set session: %v", err)
	}
	if err := serialExec(t, dA, sB, "BEGIN"); err != nil {
		t.Fatalf("sB begin: %v", err)
	}
	// sB 读本地 id=1，与 sA 写集 {6} 无交集 → 提交成功
	if err := serialExec(t, dA, sB, "SELECT id FROM users WHERE id = 1"); err != nil {
		t.Fatalf("sB select local: %v", err)
	}
	if err := serialExec(t, dA, sB, "COMMIT"); err != nil {
		t.Fatalf("sB commit: want success, got %v", err)
	}
}

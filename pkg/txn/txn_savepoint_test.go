package txn_test

import (
	"errors"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// v1.19.0 D4：SAVEPOINT 事务子语句族的事务层测试。
// 覆盖：三步基本流程、多层嵌套、重名覆盖、ROLLBACK TO 可重复回、
// RELEASE 级联删除、未知名与事务结束报错、回滚后读己写回到标记点、
// 回滚后再写入仍可正常提交、COMMIT 后保存点失效。

// asSavepoint 断言事务句柄实现保存点扩展接口（未实现视为不支持，测试失败）。
func asSavepoint(t *testing.T, tx txn.Txn) txn.SavepointTxn {
	t.Helper()
	sp, ok := tx.(txn.SavepointTxn)
	if !ok {
		t.Fatalf("事务句柄 %T 未实现 txn.SavepointTxn", tx)
	}
	return sp
}

// mustPutTxn Put 失败即终止测试。
func mustPutTxn(t *testing.T, tx txn.Txn, k, v string) {
	t.Helper()
	if err := tx.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%s=%s): %v", k, v, err)
	}
}

// mustDelTxn Delete 失败即终止测试。
func mustDelTxn(t *testing.T, tx txn.Txn, k string) {
	t.Helper()
	if err := tx.Delete([]byte(k)); err != nil {
		t.Fatalf("Delete(%s): %v", k, err)
	}
}

// wantGetTxn 事务内读取期望值（读己写路径）。
func wantGetTxn(t *testing.T, tx txn.Txn, k, want string) {
	t.Helper()
	v, err := tx.Get([]byte(k))
	if err != nil {
		t.Fatalf("Get(%s): want %q, got err %v", k, want, err)
	}
	if string(v) != want {
		t.Fatalf("Get(%s): want %q, got %q", k, want, string(v))
	}
}

// wantMissingTxn 事务内读取期望不可见（被回滚丢弃的写不应再出现在 dirty 路径）。
func wantMissingTxn(t *testing.T, tx txn.Txn, k string) {
	t.Helper()
	if v, err := tx.Get([]byte(k)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get(%s): want ErrNotFound, got value %q err %v", k, string(v), err)
	}
}

// wantStored 提交后存储层读取期望值。
func wantStored(t *testing.T, e *env, k, want string) {
	t.Helper()
	v, err := e.st.Get([]byte(k))
	if err != nil {
		t.Fatalf("storage.Get(%s): want %q, got err %v", k, want, err)
	}
	if string(v) != want {
		t.Fatalf("storage.Get(%s): want %q, got %q", k, want, string(v))
	}
}

// wantNotStored 提交后存储层读取期望不存在。
func wantNotStored(t *testing.T, e *env, k string) {
	t.Helper()
	if v, err := e.st.Get([]byte(k)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("storage.Get(%s): want ErrNotFound, got value %q err %v", k, string(v), err)
	}
}

// TestTxnSavepointBasicFlow 三步基本流程：SAVEPOINT → 继续写 → ROLLBACK TO，
// 标记点之后的写被丢弃，标记点之前的写保留并随 COMMIT 落盘。
func TestTxnSavepointBasicFlow(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	sp := asSavepoint(t, tx)
	mustPutTxn(t, tx, "k1", "v1")
	if err := sp.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint(sp1): %v", err)
	}
	mustPutTxn(t, tx, "k2", "v2")
	wantGetTxn(t, tx, "k2", "v2")
	if err := sp.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint(sp1): %v", err)
	}
	wantGetTxn(t, tx, "k1", "v1")
	wantMissingTxn(t, tx, "k2")
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	wantStored(t, e, "k1", "v1")
	wantNotStored(t, e, "k2")
}

// TestTxnSavepointRestoresDirtyState 回滚后读己写回到标记点状态：
// 标记点之后对同一 key 的覆盖写与删除都被丢弃，读取恢复为标记点的值。
func TestTxnSavepointRestoresDirtyState(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	sp := asSavepoint(t, tx)
	mustPutTxn(t, tx, "keep", "before")
	if err := sp.Savepoint("sp"); err != nil {
		t.Fatalf("Savepoint(sp): %v", err)
	}
	mustPutTxn(t, tx, "keep", "after")
	mustPutTxn(t, tx, "gone", "x")
	mustDelTxn(t, tx, "gone")
	wantGetTxn(t, tx, "keep", "after")
	wantMissingTxn(t, tx, "gone")
	if err := sp.RollbackToSavepoint("sp"); err != nil {
		t.Fatalf("RollbackToSavepoint(sp): %v", err)
	}
	// 覆盖写被丢弃：读回标记点的值。
	wantGetTxn(t, tx, "keep", "before")
	// 标记点之后的 Put+Delete 都被丢弃：整体不可见。
	wantMissingTxn(t, tx, "gone")
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	wantStored(t, e, "keep", "before")
	wantNotStored(t, e, "gone")
}

// TestTxnSavepointNested 多层嵌套：回滚到最外层保存点丢弃内层全部写，
// 回滚到内层保存点仅丢弃其后的写。
func TestTxnSavepointNested(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	sp := asSavepoint(t, tx)
	mustPutTxn(t, tx, "l0", "0")
	if err := sp.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint(sp1): %v", err)
	}
	mustPutTxn(t, tx, "l1", "1")
	if err := sp.Savepoint("sp2"); err != nil {
		t.Fatalf("Savepoint(sp2): %v", err)
	}
	mustPutTxn(t, tx, "l2", "2")
	if err := sp.Savepoint("sp3"); err != nil {
		t.Fatalf("Savepoint(sp3): %v", err)
	}
	mustPutTxn(t, tx, "l3", "3")

	// 回到 sp3（登记于 l3 之前）：只丢 l3。
	if err := sp.RollbackToSavepoint("sp3"); err != nil {
		t.Fatalf("RollbackToSavepoint(sp3): %v", err)
	}
	wantGetTxn(t, tx, "l2", "2")
	wantMissingTxn(t, tx, "l3")

	// 再回到 sp2（登记于 l2 之前）：丢 l2。
	if err := sp.RollbackToSavepoint("sp2"); err != nil {
		t.Fatalf("RollbackToSavepoint(sp2): %v", err)
	}
	wantGetTxn(t, tx, "l1", "1")
	wantMissingTxn(t, tx, "l2")
	wantMissingTxn(t, tx, "l3")

	// 再回到 sp1（登记于 l1 之前）：丢 l1。
	if err := sp.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint(sp1): %v", err)
	}
	wantGetTxn(t, tx, "l0", "0")
	wantMissingTxn(t, tx, "l1")
	wantMissingTxn(t, tx, "l2")
	wantMissingTxn(t, tx, "l3")

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	wantStored(t, e, "l0", "0")
	wantNotStored(t, e, "l1")
	wantNotStored(t, e, "l2")
	wantNotStored(t, e, "l3")
}

// TestTxnSavepointDuplicateNameOverwrites 同名保存点覆盖旧标记：
// 重名登记等价于把该点移动到最新位置，回滚只丢最新位置之后的写；
// 保存点名大小写不敏感。
func TestTxnSavepointDuplicateNameOverwrites(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	sp := asSavepoint(t, tx)
	mustPutTxn(t, tx, "d1", "1")
	if err := sp.Savepoint("dup"); err != nil {
		t.Fatalf("Savepoint(dup): %v", err)
	}
	mustPutTxn(t, tx, "d2", "2")
	// 重名登记：标记点前移到此处（d2 落在标记点之前，回滚后仍保留）。
	if err := sp.Savepoint("DUP"); err != nil {
		t.Fatalf("Savepoint(DUP): %v", err)
	}
	mustPutTxn(t, tx, "d3", "3")
	if err := sp.RollbackToSavepoint("dup"); err != nil {
		t.Fatalf("RollbackToSavepoint(dup): %v", err)
	}
	wantGetTxn(t, tx, "d2", "2")
	wantMissingTxn(t, tx, "d3")
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	wantStored(t, e, "d1", "1")
	wantStored(t, e, "d2", "2")
	wantNotStored(t, e, "d3")
}

// TestTxnSavepointRollbackRepeatable ROLLBACK TO 不删除保存点：可重复回到同一点。
func TestTxnSavepointRollbackRepeatable(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	sp := asSavepoint(t, tx)
	mustPutTxn(t, tx, "r0", "0")
	if err := sp.Savepoint("sp"); err != nil {
		t.Fatalf("Savepoint(sp): %v", err)
	}
	for i := 0; i < 3; i++ {
		mustPutTxn(t, tx, "r1", "tmp")
		if err := sp.RollbackToSavepoint("sp"); err != nil {
			t.Fatalf("第 %d 次 RollbackToSavepoint(sp): %v", i+1, err)
		}
		wantMissingTxn(t, tx, "r1")
		wantGetTxn(t, tx, "r0", "0")
	}
	// 重复回滚之后仍可继续写入并提交。
	mustPutTxn(t, tx, "r2", "final")
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	wantStored(t, e, "r0", "0")
	wantStored(t, e, "r2", "final")
	wantNotStored(t, e, "r1")
}

// TestTxnSavepointReleaseCascade RELEASE 级联删除该点及其后所有保存点，
// 被释放的点不可再回滚，更早的保存点仍可用，已写入数据保持不变。
func TestTxnSavepointReleaseCascade(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	sp := asSavepoint(t, tx)
	if err := sp.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint(sp1): %v", err)
	}
	mustPutTxn(t, tx, "c1", "1")
	if err := sp.Savepoint("sp2"); err != nil {
		t.Fatalf("Savepoint(sp2): %v", err)
	}
	mustPutTxn(t, tx, "c2", "2")
	if err := sp.Savepoint("sp3"); err != nil {
		t.Fatalf("Savepoint(sp3): %v", err)
	}
	if err := sp.ReleaseSavepoint("sp2"); err != nil {
		t.Fatalf("ReleaseSavepoint(sp2): %v", err)
	}
	// 该点及其后（sp3）都被删除。
	for _, name := range []string{"sp2", "sp3"} {
		if err := sp.RollbackToSavepoint(name); !errors.Is(err, txn.ErrSavepointNotFound) {
			t.Fatalf("RollbackToSavepoint(%s) after release: want ErrSavepointNotFound, got %v", name, err)
		}
	}
	// 更早的保存点仍有效，且释放不影响已写入数据。
	wantGetTxn(t, tx, "c1", "1")
	wantGetTxn(t, tx, "c2", "2")
	if err := sp.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint(sp1): %v", err)
	}
	wantMissingTxn(t, tx, "c1")
	wantMissingTxn(t, tx, "c2")
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	wantNotStored(t, e, "c1")
	wantNotStored(t, e, "c2")
}

// TestTxnSavepointErrors 未知名与事务结束后的错误语义。
func TestTxnSavepointErrors(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	sp := asSavepoint(t, tx)
	if err := sp.RollbackToSavepoint("nope"); !errors.Is(err, txn.ErrSavepointNotFound) {
		t.Fatalf("RollbackToSavepoint(未知名): want ErrSavepointNotFound, got %v", err)
	}
	if err := sp.ReleaseSavepoint("nope"); !errors.Is(err, txn.ErrSavepointNotFound) {
		t.Fatalf("ReleaseSavepoint(未知名): want ErrSavepointNotFound, got %v", err)
	}

	// COMMIT 后保存点失效：旧事务句柄上的保存点操作一律 ErrTxnClosed。
	if err := sp.Savepoint("sp"); err != nil {
		t.Fatalf("Savepoint(sp): %v", err)
	}
	mustPutTxn(t, tx, "after", "1")
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := sp.Savepoint("sp2"); !errors.Is(err, txn.ErrTxnClosed) {
		t.Fatalf("COMMIT 后 Savepoint: want ErrTxnClosed, got %v", err)
	}
	if err := sp.RollbackToSavepoint("sp"); !errors.Is(err, txn.ErrTxnClosed) {
		t.Fatalf("COMMIT 后 RollbackToSavepoint: want ErrTxnClosed, got %v", err)
	}
	if err := sp.ReleaseSavepoint("sp"); !errors.Is(err, txn.ErrTxnClosed) {
		t.Fatalf("COMMIT 后 ReleaseSavepoint: want ErrTxnClosed, got %v", err)
	}
}

// TestTxnSavepointWriteSetRewind ROLLBACK TO 同步重建写集（Serializable 场景）：
// 被丢弃的 key 不再出现在写集，回滚后再写入仍可正常提交。
func TestTxnSavepointWriteSetRewind(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	ser, ok := tx.(interface {
		EnableSerializable()
		ReadSet() map[string]bool
		WriteSet() map[string]bool
	})
	if !ok {
		t.Fatalf("事务句柄 %T 未实现 Serializable 扩展接口", tx)
	}
	ser.EnableSerializable()
	sp := asSavepoint(t, tx)
	mustPutTxn(t, tx, "w1", "1")
	if err := sp.Savepoint("sp"); err != nil {
		t.Fatalf("Savepoint(sp): %v", err)
	}
	mustPutTxn(t, tx, "w2", "2")
	mustDelTxn(t, tx, "w1")
	if !ser.WriteSet()["w2"] {
		t.Fatalf("回滚前写集应含 w2，实际 %v", ser.WriteSet())
	}
	if err := sp.RollbackToSavepoint("sp"); err != nil {
		t.Fatalf("RollbackToSavepoint(sp): %v", err)
	}
	ws := ser.WriteSet()
	if ws["w2"] {
		t.Fatalf("回滚后写集不应含被丢弃的 w2，实际 %v", ws)
	}
	if !ws["w1"] {
		t.Fatalf("回滚后写集应保留标记点之前的 w1，实际 %v", ws)
	}
	// 回滚后再写入仍可正常提交，写集随之更新。
	mustPutTxn(t, tx, "w3", "3")
	if !ser.WriteSet()["w3"] {
		t.Fatalf("回滚后新写入应进入写集，实际 %v", ser.WriteSet())
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	wantStored(t, e, "w1", "1")
	wantStored(t, e, "w3", "3")
	wantNotStored(t, e, "w2")
}

// TestTxnSavepointEmptyAfterRollbackCommit 回滚到标记点后事务为空批次：
// COMMIT 走空批次路径直接结束，不残留任何写入。
func TestTxnSavepointEmptyAfterRollbackCommit(t *testing.T) {
	e := newEnv(t)
	defer e.close()
	tx, err := e.mgr.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	sp := asSavepoint(t, tx)
	if err := sp.Savepoint("sp"); err != nil {
		t.Fatalf("Savepoint(sp): %v", err)
	}
	mustPutTxn(t, tx, "e1", "1")
	if err := sp.RollbackToSavepoint("sp"); err != nil {
		t.Fatalf("RollbackToSavepoint(sp): %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	wantNotStored(t, e, "e1")
}

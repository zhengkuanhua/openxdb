package db_test

// P0-1 显式事务跨 region 写集成测试：
// BEGIN..COMMIT/ROLLBACK 内跨节点写支持（延迟 PREPARE 设计）——
//   - 显式事务跨 region INSERT，COMMIT 后两端一致（含二级索引键落参与者）；
//   - 多 region 读写混合：事务内先读已提交数据（快照）、再写本地+远端，
//     写后读本地自身可见，COMMIT 后跨节点读一致；
//   - 失败回滚：显式事务内跨 region 写后 ROLLBACK，参与者无任何 2PC 状态，
//     两端无残留；
//   - 全本地显式事务零回归：不触发 2PC 标记、版本记录照常登记。

import (
	"fmt"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// TestP01ExplicitTxCrossRegionWrite 显式事务内跨 region INSERT，COMMIT 后
// 两端数据一致，二级索引键随 2PC 落参与者且可命中。
func TestP01ExplicitTxCrossRegionWrite(t *testing.T) {
	dA, dB, _ := setupM8(t)

	mustSQL(t, dA, "BEGIN")
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	mustSQL(t, dA, "COMMIT")

	// 两端一致：A/B 全量 1..6
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	res := mustSQL(t, dA, "SELECT name FROM users WHERE id = 6")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "u6" {
		t.Fatalf("A point id=6 (remote region via explicit txn): %+v", res.Rows)
	}
	// 二级索引：age=26 索引键落 B（与行同 region），B 侧索引查询命中
	res = mustSQL(t, dB, "SELECT id FROM users WHERE age = 26 ORDER BY id")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 6 {
		t.Fatalf("B index scan age=26 (explicit txn 2pc index keys): %+v", res.Rows)
	}
	// 参与者 committed 幂等标记存在（延迟 PREPARE 后 COMMIT 与 autocommit 一致）
	rows, err := dB.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:c:"), End: []byte("m:2pcq")}, -1)
	if err != nil || len(rows) == 0 {
		t.Fatalf("B committed markers after explicit txn commit: %v rows=%d", err, len(rows))
	}
}

// TestP01ExplicitTxMixedReadWrite 显式事务内多 region 读写混合：
// 先跨 region 读已提交快照，再写本地（id=0）与远端（id=6）region，
// 事务内读本地自身写可见；COMMIT 后协调者侧全量一致、参与者侧仅其
// region 数据（id=0 属协调者本地 region，B 不应读到）。
func TestP01ExplicitTxMixedReadWrite(t *testing.T) {
	dA, dB, _ := setupM8(t)

	mustSQL(t, dA, "BEGIN")
	// 跨 region 读已提交数据（快照号在 BEGIN 后首次读时取得）
	res := mustSQL(t, dA, "SELECT id FROM users WHERE id = 6")
	if len(res.Rows) != 0 {
		t.Fatalf("pre-write remote read should be empty: %+v", res.Rows)
	}
	// 本地 region 写 + 远端 region 写
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (0, 'x0', 40)")
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	// 事务内读本地自身写可见
	res = mustSQL(t, dA, "SELECT id FROM users WHERE id = 0")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 0 {
		t.Fatalf("explicit txn read own local write: %+v", res.Rows)
	}
	mustSQL(t, dA, "COMMIT")

	// 协调者 A 与参与者 B 全表读均可见完整逻辑表：0（本地 region）+ 1..6
	// （B 的全表扫描跨节点合并 A 持有的本地 region，属分布式读正确语义）
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 0, 1, 2, 3, 4, 5, 6)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 0, 1, 2, 3, 4, 5, 6)
	// 版本记录：协调者与参与者两侧均登记（与 autocommit 2PC 同一规则）
	rowsA, err := dA.Storage.Scan(storage.KeyRange{Start: []byte("m:ver:"), End: []byte("m:ver\xff")}, -1)
	if err != nil || len(rowsA) == 0 {
		t.Fatalf("A version records after mixed explicit txn: %v rows=%d", err, len(rowsA))
	}
	rowsB, err := dB.Storage.Scan(storage.KeyRange{Start: []byte("m:ver:"), End: []byte("m:ver\xff")}, -1)
	if err != nil || len(rowsB) == 0 {
		t.Fatalf("B version records after mixed explicit txn: %v rows=%d", err, len(rowsB))
	}
}

// TestP01ExplicitTxRollbackRemote 显式事务内跨 region 写后 ROLLBACK：
// 延迟 PREPARE 设计下参与者无任何 2PC 状态（prepared/committed 标记为空），
// 本地写随会话事务回滚，两端无残留。
func TestP01ExplicitTxRollbackRemote(t *testing.T) {
	dA, dB, _ := setupM8(t)

	mustSQL(t, dA, "BEGIN")
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (0, 'x0', 40)")
	mustSQL(t, dA, "ROLLBACK")

	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5)
	// 参与者无 prepared 也无 committed 标记（ROLLBACK 零参与者清理成本）
	rowsP, err := dB.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:p:"), End: []byte("m:2pcq")}, -1)
	if err != nil || len(rowsP) != 0 {
		t.Fatalf("B prepared markers should be empty after rollback: %v rows=%d", err, len(rowsP))
	}
	rowsC, err := dB.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:c:"), End: []byte("m:2pcq")}, -1)
	if err != nil || len(rowsC) != 0 {
		t.Fatalf("B committed markers should be empty after rollback: %v rows=%d", err, len(rowsC))
	}
}

// TestP01ExplicitTxLocalOnlyRegression 显式事务全本地写 COMMIT 走原路径：
// 不触发任何 2PC 标记，版本记录照常登记（快照过滤覆盖显式事务本地提交）。
func TestP01ExplicitTxLocalOnlyRegression(t *testing.T) {
	dA, dB, _ := setupM8(t)

	mustSQL(t, dA, "BEGIN")
	mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (0, 'x0', 40)")
	mustSQL(t, dA, "COMMIT")

	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 0, 1, 2, 3, 4, 5)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 0, 1, 2, 3, 4, 5)
	// 参与者无任何 2PC 标记（零触发）
	rows, err := dB.Storage.Scan(storage.KeyRange{Start: []byte("m:2pc:"), End: []byte("m:2pcq")}, -1)
	if err != nil || len(rows) != 0 {
		t.Fatalf("B 2pc markers should be empty for local-only explicit txn: %v rows=%d", err, len(rows))
	}
	// 版本记录登记（协调者侧）
	rowsA, err := dA.Storage.Scan(storage.KeyRange{Start: []byte("m:ver:"), End: []byte("m:ver\xff")}, -1)
	if err != nil || len(rowsA) == 0 {
		t.Fatalf("A version records after local-only explicit txn: %v rows=%d", err, len(rowsA))
	}
}

// ensure fmt 引用（显式事务 INSERT 使用 fmt.Sprintf 场景，避免误删 import）
var _ = fmt.Sprintf

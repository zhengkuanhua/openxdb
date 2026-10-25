package sql_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// v1.9.0 物化视图增量刷新测试：
//   - 增量刷新只重算变更部分（AffectedRows = 变更键数，而非全表行数）
//   - REFRESH 幂等跳过（无新变更时 AffectedRows = 0）
//   - CONCURRENTLY 刷新期间并发读旧快照、提交后原子读到新快照
//   - 依赖链：只刷新上层 MV 时按依赖序先刷下层 MV（失效传播）
//   - 全量 REFRESH 回归不破坏（非 Simple 定义）

func TestM19MatViewIncrementalRefresh(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, name, age FROM users")

	// 大批量插入：仅新键进入变更集合
	var sb strings.Builder
	sb.WriteString("INSERT INTO users (id, name, age) VALUES ")
	for i := 4; i <= 103; i++ {
		if i > 4 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "(%d, 'u%d', %d)", i, i, 20+i)
	}
	mustExec(t, e, sb.String())

	// 增量刷新：只重算 100 个变更键，而不是全表 103 行
	res := mustExec(t, e, "REFRESH MATERIALIZED VIEW mv")
	if res.AffectedRows != 100 {
		t.Fatalf("incremental refresh: want 100 affected rows, got %d", res.AffectedRows)
	}

	// 幂等：无新变更时第二次 REFRESH 跳过（0 行重算）
	res = mustExec(t, e, "REFRESH MATERIALIZED VIEW mv")
	if res.AffectedRows != 0 {
		t.Fatalf("idempotent refresh: want 0 affected rows, got %d", res.AffectedRows)
	}

	// 单行 UPDATE：只重算 1 行
	mustExec(t, e, "UPDATE users SET age = 31 WHERE id = 1")
	res = mustExec(t, e, "REFRESH MATERIALIZED VIEW mv")
	if res.AffectedRows != 1 {
		t.Fatalf("incremental single update: want 1 affected row, got %d", res.AffectedRows)
	}

	// 单行 DELETE：只重算 1 行
	mustExec(t, e, "DELETE FROM users WHERE id = 2")
	res = mustExec(t, e, "REFRESH MATERIALIZED VIEW mv")
	if res.AffectedRows != 1 {
		t.Fatalf("incremental single delete: want 1 affected row, got %d", res.AffectedRows)
	}

	// 数据正确性：更新/删除已反映
	rows := mustRows(t, e, "SELECT age FROM mv WHERE id = 1")
	if len(rows) != 1 || rows[0][0].String() != "31" {
		t.Fatalf("mv age for id=1: want 31, got %v", rows)
	}
	rows = mustRows(t, e, "SELECT id FROM mv ORDER BY id")
	if len(rows) != 102 {
		t.Fatalf("mv row count: want 102 (103 inserted minus deleted id=2), got %d", len(rows))
	}
}

func TestM19MatViewConcurrentlySnapshotSwap(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv AS SELECT id, age FROM users")

	// 基表扩到 200 行，给 CONCURRENTLY 全量重建留出可观测窗口
	var sb strings.Builder
	sb.WriteString("INSERT INTO users (id, name, age) VALUES ")
	for i := 4; i <= 203; i++ {
		if i > 4 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "(%d, 'u%d', %d)", i, i, 20+i)
	}
	mustExec(t, e, sb.String())
	mustExec(t, e, "REFRESH MATERIALIZED VIEW mv") // 先同步到最新
	mustExec(t, e, "UPDATE users SET age = 99 WHERE id = 1")

	// 独立会话上 REFRESH CONCURRENTLY：重算完成后、COMMIT 前，其他会话仍读旧快照
	sess := e.NewSession()
	if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	refreshed := make(chan struct{})
	allowCommit := make(chan struct{})
	done := make(chan struct{})
	errCh := make(chan error, 2)
	go func() {
		if _, err := e.ExecuteAsSession("REFRESH MATERIALIZED VIEW CONCURRENTLY mv", sql.SuperUser, sess); err != nil {
			errCh <- err
		}
		close(refreshed) // 刷新已重算但事务未提交
		<-allowCommit
		if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
			errCh <- err
		}
		close(done)
	}()

	<-refreshed
	// 刷新进行中（未提交）：并发读仍服务旧影子表快照
	rows := mustRows(t, e, "SELECT age FROM mv WHERE id = 1")
	if len(rows) != 1 || rows[0][0].String() != "30" {
		t.Fatalf("during concurrent refresh: want old snapshot age=30, got %v", rows)
	}
	close(allowCommit)
	<-done // 等待 REFRESH 事务提交完成，避免读新快照竞态

	// 提交后：原子读到新快照
	rows = mustRows(t, e, "SELECT age FROM mv WHERE id = 1")
	if len(rows) != 1 || rows[0][0].String() != "99" {
		t.Fatalf("after concurrent refresh commit: want new snapshot age=99, got %v", rows)
	}
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent refresh session: %v", err)
	}
}

func TestM19MatViewDependencyRefresh(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	// 依赖链：mv_a 依赖 mv_b（mv_b 依赖基表 users）
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv_b AS SELECT id, name FROM users")
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv_a AS SELECT id, name FROM mv_b WHERE id >= 2")

	// 只 REFRESH mv_a：依赖序应保证 mv_b 先刷新（失效传播）
	mustExec(t, e, "UPDATE users SET name = 'alice2' WHERE id = 1")
	mustExec(t, e, "REFRESH MATERIALIZED VIEW mv_a")
	rows := mustRows(t, e, "SELECT name FROM mv_b WHERE id = 1")
	if len(rows) != 1 || rows[0][0].String() != "alice2" {
		t.Fatalf("dependency propagation: mv_b must refresh before mv_a, got %v", rows)
	}

	// 基表变更在 mv_a 可见（mv_a 全量重建基于已刷新的 mv_b）
	mustExec(t, e, "UPDATE users SET name = 'bob2' WHERE id = 2")
	mustExec(t, e, "REFRESH MATERIALIZED VIEW mv_a")
	rows = mustRows(t, e, "SELECT name FROM mv_a WHERE id = 2")
	if len(rows) != 1 || rows[0][0].String() != "bob2" {
		t.Fatalf("mv_a after chain refresh: want bob2, got %v", rows)
	}

	// 依赖链幂等：无新变更时整条链跳过
	res := mustExec(t, e, "REFRESH MATERIALIZED VIEW mv_a")
	if res.AffectedRows != 0 {
		t.Fatalf("chain idempotent refresh: want 0 affected rows, got %d", res.AffectedRows)
	}
}

func TestM19MatViewFullRefreshRegression(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	// 非 Simple 定义（JOIN + 聚合）：仍走全量路径，语义不破坏
	mustExec(t, e, "CREATE TABLE orders (oid INT, uid INT, amount INT, PRIMARY KEY (oid))")
	mustExec(t, e, "INSERT INTO orders (oid, uid, amount) VALUES (1, 1, 100), (2, 1, 50), (3, 2, 200)")
	mustExec(t, e, "CREATE MATERIALIZED VIEW mv_agg AS SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.uid GROUP BY u.name")

	mustExec(t, e, "UPDATE orders SET amount = 150 WHERE oid = 1")
	res := mustExec(t, e, "REFRESH MATERIALIZED VIEW mv_agg")
	if res.AffectedRows != 2 {
		t.Fatalf("full refresh: want 2 affected rows (alice/bob), got %d", res.AffectedRows)
	}
	rows := mustRows(t, e, "SELECT total FROM mv_agg WHERE name = 'alice'")
	if len(rows) != 1 || rows[0][0].String() != "200" {
		t.Fatalf("mv_agg total for alice: want 200 (100->150 + 50), got %v", rows)
	}

	// 幂等
	res = mustExec(t, e, "REFRESH MATERIALIZED VIEW mv_agg")
	if res.AffectedRows != 0 {
		t.Fatalf("full refresh idempotent: want 0 affected rows, got %d", res.AffectedRows)
	}
}

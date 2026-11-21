package db_test

// v1.20.0 C3：2PC 参与面扩展集成测试（缺口 B-2 / B-4，验收 V-B2）。
//
// V-B2 断言（进程内 A/B/C 三实例，中间 region 归属 B、副本集合 {A, C}，
// 参与者 = owner B + 副本 A/C 共 3 个，quorum = 3/2+1 = 2）：
//   - 断开 C 后跨 region INSERT 仍提交成功（2/3 达多数派），A/B 两侧数据一致；
//   - owner B 在 PREPARE 阶段不可用（叠加 C 已断开）后仅 A 可 prepare（1/3），
//     返回 TXN_PREPARE_QUORUM_FAILED，且协调者无 m:2pc:p:* 暂存残留、目标行未落地。
//
// 装配说明：副本集合走 DEV_PLAN D-2 的 MVP 独立元数据键 m:repl:replicas:<rid>，
// 三节点各自写入同一份副本记录（协调者据此规划参与者，参与者据此校验归属）。

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// countKeysWithPrefixDB 统计本地存储中指定前缀的键数量（仅测试用）。
func countKeysWithPrefixDB(t *testing.T, st storage.Storage, prefix string) int {
	t.Helper()
	rows, err := st.Scan(storage.KeyRange{Start: []byte(prefix), End: []byte("m:2pc:q")}, -1)
	if err != nil {
		t.Fatalf("scan %q: %v", prefix, err)
	}
	return len(rows)
}

// TestV1200C3ReplicaQuorumThreeNodes V-B2：副本参与 + 多数派决策。
func TestV1200C3ReplicaQuorumThreeNodes(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	dB, addrB := openNode(t, "node-b")
	dC, addrC := openNode(t, "node-c")
	closedB, closedC := false, false
	t.Cleanup(func() {
		if !closedC {
			_ = dC.Close()
		}
		if !closedB {
			_ = dB.Close()
		}
		_ = dA.Close()
	})
	selfA, selfB, selfC := dA.Cluster.SelfID, dB.Cluster.SelfID, dC.Cluster.SelfID

	// 基线：A 建表写 1..5，SPLIT 出中间 region [4,8)，中间 region 指派给 B。
	for _, d := range []*db.DB{dA, dB, dC} {
		mustSQL(t, d, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	}
	for i := 1; i <= 5; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: want 3, got %v err=%v", regions, err)
	}
	mid := storage.ID(regions[1].RegionID)
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	mustSQL(t, dA, "ADD NODE '"+addrC+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", regions[1].RegionID, selfB))

	// 副本集合（不含 owner）：中间 region 的副本 = {A, C}；三节点各写一份。
	for _, d := range []*db.DB{dA, dB, dC} {
		if err := cluster.PutRegionReplicas(d.Storage, mid, []string{selfA, selfC}); err != nil {
			t.Fatalf("PutRegionReplicas on %s: %v", d.Cluster.SelfID, err)
		}
	}
	// 开关：三节点均开启 ha_2pc_replica_quorum（协调者规划 + 参与者归属校验）。
	for _, d := range []*db.DB{dA, dB, dC} {
		mustSQL(t, d, "SET ha_2pc_replica_quorum = on")
	}

	// 阶段一：断开 C（副本之一），参与者仅 A/B 可达 = 2/3，达多数派 -> 提交成功。
	if err := dC.Close(); err != nil {
		t.Fatalf("close node-c: %v", err)
	}
	closedC = true
	res := mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	if res.AffectedRows != 1 {
		t.Fatalf("INSERT with one replica down: affected=%d", res.AffectedRows)
	}
	// 协调者与存活参与者两侧数据一致（id=6 归属中间 region，owner=B）。
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)

	// 阶段二：owner B 在 PREPARE 阶段不可用（参与者侧注入失败；真实断连会在更早
	// 的远端探测阶段直接返回连接错误，无法进入 quorum 判定），叠加 C 已断开，
	// 仅 A 可 prepare = 1/3，未达多数派 -> quorum 失败并整体回滚。
	dB.ClusterSrv.SetPrepareFail(func(txID string) error {
		return fmt.Errorf("injected owner prepare failure for %s", txID)
	})
	_, err = dA.SQL.Execute("INSERT INTO users (id, name, age) VALUES (7, 'u7', 27)")
	if err == nil {
		t.Fatalf("INSERT with owner and replica down must fail")
	}
	var se *sql.SQLError
	if !errors.As(err, &se) || se.Code != sql.ErrCodeTxnPrepareQuorumFailed {
		t.Fatalf("want code %s, got %v", sql.ErrCodeTxnPrepareQuorumFailed, err)
	}
	if !strings.Contains(err.Error(), "need majority 2") {
		t.Fatalf("quorum size must be 2 of 3: %v", err)
	}
	// 无残留：已 prepare 的参与者被回滚，协调者本地无暂存、目标行未落地。
	if n := countKeysWithPrefixDB(t, dA.Storage, "m:2pc:p:"); n != 0 {
		t.Fatalf("prepared residue on coordinator: %d keys", n)
	}
	// 目标行未落地：owner B 未 prepare、协调者未提交，查询两侧均为空。
	if rows := mustSQL(t, dA, "SELECT id FROM users WHERE id = 7"); len(rows.Rows) != 0 {
		t.Fatalf("row id=7 must not be persisted, got %d rows", len(rows.Rows))
	}
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
}

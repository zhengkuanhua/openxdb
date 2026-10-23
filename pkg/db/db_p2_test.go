package db_test

// P2 分区恢复与测试矩阵（集群轨端到端，docs/HA_BRAIN_SPLIT_DESIGN.md §6）：
//   - TestP2T9FailoverWithEpochQuorum（T9）：单点 down 正常 failover
//     （兼容性）——epoch/quorum 开关开启时，单节点 down、其余节点满足
//     多数派，自动重指派在 epoch 校验下正常完成（regionEpoch 递增）。

import (
	"fmt"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
)

// TestP2T9FailoverWithEpochQuorum 单点 down 正常 failover（T9）：
// 三节点集群开启 ha_epoch_enabled / ha_quorum_enabled 后，B down，
// A 在 quorum（A+C=2/3）授权下重指派原 B 的 region，路由收敛且
// regionEpoch 递增（epoch 校验不阻碍正常 failover）。
func TestP2T9FailoverWithEpochQuorum(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, _ := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })
	dC, _ := openNode(t, "node-c")
	t.Cleanup(func() { _ = dC.Close() })

	// 开启 HA 开关（DB 层即 SQL 层 haCfg 热切换入口）。
	dA.SetEpochEnabled(true)
	dA.SetQuorumEnabled(true)

	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dC, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 10; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, 'u%d')", i, i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: %v err=%v", regions, err)
	}
	mid := regions[1]
	mustSQL(t, dA, "ADD NODE '"+dB.Cluster.SelfAddr+"'")
	mustSQL(t, dA, "ADD NODE '"+dC.Cluster.SelfAddr+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", mid.RegionID, dB.Cluster.SelfID))
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	speedupHeartbeat(t, dA)
	waitEverUp(t, dA, dB.Cluster.SelfID)
	waitEverUp(t, dA, dC.Cluster.SelfID)

	// B down → 心跳感知 DOWN。
	if err := dB.Close(); err != nil {
		t.Fatalf("B close: %v", err)
	}
	waitNodeState(t, dA, dB.Cluster.SelfID, cluster.StateDown)

	// epoch/quorum 校验下正常 failover：region 被重指派离开 node-b
	// （目标为持有校验通过的存活节点或本节点兜底），regionEpoch 递增。
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		rg, ok := dA.SQL.ExecutorRouter().FindRegion(mid.RegionID)
		if ok && rg.Node != "" && rg.Node != dB.Cluster.SelfID {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	rg, _ := dA.SQL.ExecutorRouter().FindRegion(mid.RegionID)
	if rg.Node == "" || rg.Node == dB.Cluster.SelfID {
		t.Fatalf("region %d owner = %q, want reassigned away from node-b", mid.RegionID, rg.Node)
	}
	if rg.RegionEpoch == 0 {
		t.Fatalf("region %d epoch = 0, want > 0 (epoch-checked reassign)", mid.RegionID)
	}

	// 读路径仍全量一致（failover 后数据可完整访问）。
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
}

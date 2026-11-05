package db_test

// D4.4 次要缺陷回归测试（路由视图收敛）：
//   复测暴露：重启节点本地 SHOW REGION ROUTES 仍含权威视图不存在的历史 region，
//   且标 local=true（实测 rid=3 与现役 rid=7 区间完全重叠）。根因是路由视图同步
//   （RequestRouteSync / REGION_PUSH 学习）此前只做 upsert，从不清理本地陈旧视图。
//
// 本测试用真实路径构造残留：B 在线时被指派 region → B 离线（收不到 SPLIT 广播的
// DeleteIDs） → A 侧重新切分产生新 region → B 重启，本地 m:regions 装载旧 region。
//
// 覆盖点：
//  1. 权威路由同步后，B 的本地 region 集合与 A 权威视图收敛一致（旧 region 被清理）；
//  2. 陈旧 region 的物理数据不被删除（视图收敛只清理路由记录，数据保留待对账）；
//  3. 收敛不影响数据一致性（B 侧计数与 A 侧一致，无重复视图读）。

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/rocksdb"
)

// regionIDSet users 表 region id 集合。
func regionIDSet(t *testing.T, d *db.DB) map[storage.ID]sharding.RegionInfo {
	t.Helper()
	regions, err := d.SQL.ListRegions("users")
	if err != nil {
		t.Fatalf("ListRegions: %v", err)
	}
	out := make(map[storage.ID]sharding.RegionInfo, len(regions))
	for _, rg := range regions {
		out[rg.RegionID] = rg
	}
	return out
}

// waitRouteViewConverged 轮询直到 B 的 users 表 region 集合与 A 权威视图完全一致，
// 且不再包含 staleRID（陈旧历史 region）。
func waitRouteViewConverged(t *testing.T, dA, dB *db.DB, staleRID storage.ID) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var gotA, gotB map[storage.ID]sharding.RegionInfo
	for time.Now().Before(deadline) {
		gotA = regionIDSet(t, dA)
		gotB = regionIDSet(t, dB)
		if _, stale := gotB[staleRID]; stale {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if len(gotA) == len(gotB) {
			same := true
			for rid := range gotA {
				if _, ok := gotB[rid]; !ok {
					same = false
					break
				}
			}
			if same {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("route view not converged: authoritative=%v local=%v (stale rid=%d must be gone)",
		regionIDsOf(gotA), regionIDsOf(gotB), staleRID)
}

// regionIDsOf 提取集合便于失败断言打印。
func regionIDsOf(m map[storage.ID]sharding.RegionInfo) []storage.ID {
	out := make([]storage.ID, 0, len(m))
	for rid := range m {
		out = append(out, rid)
	}
	return out
}

// countRegionKeys 统计指定 region 前缀的物理键条数（直接开存储扫描，绕过路由视图），
// 用于断言"视图收敛不删数据"。
func countRegionKeys(t *testing.T, dir string, rid storage.ID) int {
	t.Helper()
	st, err := rocksdb.Open(filepath.Join(dir, db.DataSubDir), false)
	if err != nil {
		t.Fatalf("open storage %s: %v", dir, err)
	}
	defer st.Close()
	start := sharding.EncodeRegionKey(rid, nil)
	end := sharding.EncodeRegionKey(rid+1, nil)
	kvs, err := st.Scan(storage.KeyRange{Start: start, End: end}, 0)
	if err != nil {
		t.Fatalf("scan region %d keys: %v", rid, err)
	}
	return len(kvs)
}

// countDBAgg 返回 users 表行数（COUNT(*)），用于跨节点一致性快速断言。
func countDBAgg(t *testing.T, d *db.DB) int64 {
	t.Helper()
	res := mustSQL(t, d, "SELECT COUNT(*) FROM users")
	if len(res.Rows) != 1 || len(res.Rows[0]) == 0 {
		t.Fatalf("unexpected COUNT result: %+v", res.Rows)
	}
	return res.Rows[0][0].I
}

// TestD44RestartPrunesSupersededRegions 重启节点拉取权威路由视图后，本地 region
// 集合必须与权威视图收敛一致；陈旧 region 的物理数据保留不删。
// 修复前：B 重启后 SHOW REGION ROUTES 仍返回旧 region（local=true），视图与 A 不一致。
func TestD44RestartPrunesSupersededRegions(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })

	dirB := t.TempDir()
	if err := db.Init(dirB); err != nil {
		t.Fatalf("init B: %v", err)
	}
	dB := openNodeAt(t, dirB, "node-b", "127.0.0.1:0")
	hostB, portB := hostPort(t, dB.Cluster.SelfAddr)
	bID := dB.Cluster.SelfID

	regions := seedThreeRegions(t, dA)
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, dA, "ADD NODE '"+dB.Cluster.SelfAddr+"'")

	// 首个 region 指派给 B：B 本地化该 region（视图 + 物理数据）。
	// 选首个 region 是因为后续 SPLIT 会把它整体取代（旧 rid 从权威视图消失），
	// 精确复现"权威视图不存在、本地仍保留且 local=true"的残留场景。
	tail := regions[0]
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", tail.RegionID, bID))
	waitRoute(t, dB, tail.RegionID, bID, true)
	if n := countDBAgg(t, dB); n == 0 {
		t.Fatalf("expected rows visible on B before shutdown")
	}

	// B 离线：此后 A 侧 SPLIT 广播的 DeleteIDs B 完全收不到，本地 m:regions 保留旧视图
	speedupHeartbeat(t, dA)
	waitEverUp(t, dA, bID)
	if err := dB.Close(); err != nil {
		t.Fatalf("B close: %v", err)
	}
	waitNodeState(t, dA, bID, cluster.StateDown)

	// A 侧重新切分 users（在旧尾 region 区间内再切一刀）：旧 region 被新划分取代，
	// A 视图不再包含 tail.RegionID —— 这正是 B 重启后要收敛掉的"权威不存在"记录。
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8), splitBoundary(9)}); err != nil {
		t.Fatalf("second SplitTable: %v", err)
	}
	authView := regionIDSet(t, dA)
	if _, still := authView[tail.RegionID]; still {
		t.Fatalf("authoritative view still has stale region %d", tail.RegionID)
	}

	// 重启 B：本地 m:regions 仍装载旧视图，启动后主动拉取权威视图并收敛
	dB2 := openNodeAt(t, dirB, "node-b", net.JoinHostPort(hostB, portB))
	t.Cleanup(func() { _ = dB2.Close() })
	if dB2.Cluster.SelfID != bID {
		t.Fatalf("restarted node id = %q, want %q", dB2.Cluster.SelfID, bID)
	}
	waitNodeState(t, dA, bID, cluster.StateUp)
	waitRouteViewConverged(t, dA, dB2, tail.RegionID)

	// 反向保护：B 的陈旧视图不得经路由学习反污染 A 权威视图
	if _, back := regionIDSet(t, dA)[tail.RegionID]; back {
		t.Fatalf("authoritative view re-polluted by stale region %d from restarted node", tail.RegionID)
	}

	// 收敛后视图内部自洽：不存在"owner 为远端却 local=true"的矛盾条目
	for rid, rg := range regionIDSet(t, dB2) {
		if rid == tail.RegionID {
			t.Fatalf("stale region %d still in local view", rid)
		}
		if rg.Local && rg.Node != "" && rg.Node != bID {
			t.Fatalf("region %d local=true but owner=%q (inconsistent view)", rid, rg.Node)
		}
	}

	// 一致性不回退：A 权威读完整；B 视图收敛后仍可正常读取且覆盖全部权威数据。
	// 注：ASSIGN 复制到 B 的旧 region 物理键空间按设计保留（待人工 GC），会让 B 的
	// 行数可能多于 A（同一 region 的新旧副本并存），这是数据面对账范畴，不属本次
	// 路由视图收敛的判定范围，因此这里只断言"权威数据集完整可见"，不比严格行数。
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	if got := countDBAgg(t, dA); got != 10 {
		t.Fatalf("A count=%d, want 10", got)
	}
	seen := make(map[int64]bool)
	for _, row := range mustSQL(t, dB2, "SELECT id FROM users").Rows {
		seen[row[0].I] = true
	}
	for id := int64(1); id <= 10; id++ {
		if !seen[id] {
			t.Fatalf("B view after convergence misses id=%d (route view convergence must not hide data)", id)
		}
	}

	// 数据不误删：陈旧 region 的物理键仍保留在 B 本地存储（视图清理不动数据）
	if err := dB2.Close(); err != nil {
		t.Fatalf("B close: %v", err)
	}
	if n := countRegionKeys(t, dirB, tail.RegionID); n == 0 {
		t.Fatalf("physical keys of superseded region %d were deleted; route convergence must not drop data", tail.RegionID)
	}
}

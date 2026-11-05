package db_test

// D4.5 §6 第 3 项（T3）端到端回归：集群轨 epoch fencing。
//
// 缺口（D4.3 演练 T3 GAP）：写路径的 db.AllowWrite 只在 masterEpoch>0 时
// fencing，而集群轨（无复制轨、不经过 StartReplication/PromoteToMaster）
// masterEpoch 恒为 0，写入恒放行。双向网络分区时被隔离的旧主仍按本地
// region 归属视图接受写入，分区恢复后也不自动收敛——陈旧主与多数派新
// owner 并发写同一 region。
//
// 修复（pkg/sql/ha_p3.go，ha_epoch_enabled + ha_quorum_enabled 双开生效）：
//   - 语句级多数派授权：本节点处于分区少数派即拒绝写（cluster track write fenced）；
//   - region 级归属授权：region 已被更高 regionEpoch 指派给他人而本地路由陈旧时拒绝；
//   - 分区恢复收敛：清空探测缓存 → 写自动放行、路由视图按 epoch 大者胜收敛。
//
// 本测试走真实三节点组网（真实心跳/路由/存储），分区用 cluster 分区视图注入
// （SetProber，与生产脑裂演练同一手段）：三节点就绪后把 region 指派给 B，
// 注入 B↔A/C 双向不可达使 B 只剩 1/3 可达（分区少数派）→ B 上的集群轨写入
// 必须被拒绝；清除分区视图使 B 恢复 2/3 多数派 → B 的写必须**无人工介入**
// 自动放行（分区检测循环触发收敛回调），且新数据对权威节点 A 可见。

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// waitPartitionRecovered 轮询直到本节点完成分区恢复收敛（PartitionRecoveredCount 增长）。
func waitPartitionRecovered(t *testing.T, d *db.DB, want uint64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if d.SQL.PartitionRecoveredCount() >= want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("partition recovered count = %d, want >= %d", d.SQL.PartitionRecoveredCount(), want)
}

// TestD45T3ClusterTrackFencingE2E 验证集群轨写入纳入 epoch/归属授权：
// 分区少数派拒绝写、恢复多数派自动收敛放行，且数据对权威节点可见。
func TestD45T3ClusterTrackFencingE2E(t *testing.T) {
	dA, addrA := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, addrB := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })
	dC, addrC := openNode(t, "node-c")
	t.Cleanup(func() { _ = dC.Close() })

	aID, bID, cID := dA.Cluster.SelfID, dB.Cluster.SelfID, dC.Cluster.SelfID

	// ---- 组网就绪：A 建表切 3 region，B/C 同步表结构，三向注册 ----
	regions := seedThreeRegions(t, dA)
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, dC, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	for _, d := range []*db.DB{dA, dB, dC} {
		for _, peer := range []string{addrA, addrB, addrC} {
			if peer == d.Cluster.SelfAddr {
				continue
			}
			mustSQL(t, d, "ADD NODE '"+peer+"'")
		}
	}
	for _, d := range []*db.DB{dA, dB, dC} {
		speedupHeartbeat(t, d)
	}
	waitEverUp(t, dA, bID)
	waitEverUp(t, dA, cID)
	waitEverUp(t, dB, aID)
	waitEverUp(t, dC, aID)

	// 首个 region 指派给 B：B 本地化该 region（视图 local + 物理落盘）。
	rid := regions[0].RegionID
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", rid, bID))
	waitRoute(t, dB, rid, bID, true)

	// 开启 quorum 仲裁（epoch 默认开）：集群轨 fencing 的生效前提。
	for _, d := range []*db.DB{dA, dB, dC} {
		d.SetQuorumEnabled(true)
	}
	for _, d := range []*db.DB{dA, dB, dC} {
		if !d.QuorumEnabled() {
			t.Fatal("quorum switch should be on")
		}
		if !d.Cluster.MajorityOK() {
			t.Fatal("all nodes should hold majority before partition (3/3 reachable)")
		}
	}

	// 基线：三端全员可达时，B 对其归属 region 的写入正常（集群轨恒放行语义不变）。
	if _, err := dB.SQL.Execute("UPDATE users SET name = 'before-split' WHERE id = 2"); err != nil {
		t.Fatalf("baseline write on B: %v", err)
	}
	if n := dB.SQL.FencedWriteCount(); n != 0 {
		t.Fatalf("fenced count before partition = %d, want 0", n)
	}

	// ---- 分区：B 与 A/C 双向不可达（B 仅剩 1/3 可达，分区少数派）----
	// 用 cluster 分区视图注入（与生产脑裂演练同一手段，不影响进程存活）：
	// B 侧看不到 A/C，A/C 侧均看不到 B（A、C 仍构成 2/3 多数派）。
	dB.Cluster.SetProber(func(id string) bool { return id != aID && id != cID })
	dA.Cluster.SetProber(func(id string) bool { return id != bID })
	dC.Cluster.SetProber(func(id string) bool { return id != bID })
	defer func() {
		dA.Cluster.SetProber(nil)
		dB.Cluster.SetProber(nil)
		dC.Cluster.SetProber(nil)
	}()

	if dB.Cluster.MajorityOK() {
		t.Fatal("B should lose majority (1/3 reachable) after partition")
	}
	if !dA.Cluster.MajorityOK() {
		t.Fatal("A side should hold majority (2/3 reachable) during partition")
	}
	// 让分区检测循环记录"少数派"状态（1s 周期），否则后续恢复不构成状态转换。
	time.Sleep(1500 * time.Millisecond)

	// 核心断言（修复前：写入成功 → T3 GAP）：
	// 少数派侧的旧主不得继续接受其归属 region 的写入。
	_, err := dB.SQL.Execute("UPDATE users SET name = 'during-split' WHERE id = 2")
	if err == nil {
		t.Fatal("minority partition write must be rejected, got success (T3 fencing not effective)")
	}
	if !strings.Contains(err.Error(), "cluster track write fenced") {
		t.Fatalf("rejected write error = %v, want prefix %q", err, "cluster track write fenced")
	}
	if n := dB.SQL.FencedWriteCount(); n == 0 {
		t.Fatal("fenced write count should grow after minority partition rejection")
	}

	// 多数派侧（A）不受影响：其写入不被 fencing（唯少数派被拒）。
	mustSQL(t, dA, "UPDATE users SET name = 'majority-side' WHERE id = 6")

	// ---- 恢复：清除分区视图（B 恢复 2/3 多数派）→ 自动收敛，写放行 ----
	dA.Cluster.SetProber(nil)
	dB.Cluster.SetProber(nil)
	dC.Cluster.SetProber(nil)
	if !dB.Cluster.MajorityOK() {
		t.Fatal("B should regain majority after network recovery")
	}
	waitPartitionRecovered(t, dB, 1)

	// 恢复后无需人工介入：同一 region 的写入自动放行（fencing 解除）。
	mustSQL(t, dB, "UPDATE users SET name = 'after-recover' WHERE id = 2")

	// 收敛后数据一致：B 的恢复后写入在权威节点 A 上可见（归属未因分区错乱）。
	res := mustSQL(t, dA, "SELECT name FROM users WHERE id = 2")
	if len(res.Rows) != 1 || len(res.Rows[0]) == 0 || res.Rows[0][0].S != "after-recover" {
		t.Fatalf("A should see B's post-recovery write, got %+v", res.Rows)
	}
}

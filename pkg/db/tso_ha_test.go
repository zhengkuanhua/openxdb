package db_test

// B2（D4.5 §6 第 2 项）TSO 中心单点端到端测试：
//   - TestB2TSOSinglePointRepro：复现根因——TSO 挂单节点时该节点宕机，
//     即便数据在本节点本地，写事务取戳仍报 tso unavailable；
//   - TestB2TSOHAFailover：启用 TSO HA（主备 + epoch 选主）后，主节点宕机
//     跨节点写自动恢复，且时间戳序列不回落、不重复分配；
//   - TestB2TSOHANoQuorumNoSplitBrain：组内多数派不可达时禁止自封主
//     （防分区两侧各发号 → 时间戳重复）。

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// openHANode 建一个独立数据目录的集群节点。不使用 t.TempDir：本组用例会
// 主动 Close 节点模拟宕机，Windows 下句柄释放滞后会让 TempDir 清理报错，
// 干扰断言；这里目录自管、清理失败忽略。
func openHANode(t *testing.T, name, addr string) *db.DB {
	t.Helper()
	dir, err := os.MkdirTemp("", "b2-"+name+"-")
	if err != nil {
		t.Fatalf("%s mkdtemp: %v", name, err)
	}
	d := openNodeAt(t, dir, name, addr)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return d
}

// createT 在每个节点上建同一张表（跨节点 2PC 需要目标节点存在表结构）。
func createT(t *testing.T, nodes []*db.DB, table string) {
	t.Helper()
	for _, d := range nodes {
		mustSQL(t, d, "CREATE TABLE "+table+" (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	}
}

// TestB2TSOSinglePointRepro B2 根因复现（未启用 HA）：
// A 为 TSO 中心 + r0 归属；B 持 r1。B 的取号全部指向 A。
// A 宕机后 B 对自身本地 region 的写也不得不取号 → 报 tso unavailable。
// 该用例证明"现象与数据可达性无关，纯粹是发号源单点"，是 B2 修复的基线。
func TestB2TSOSinglePointRepro(t *testing.T) {
	dA := openHANode(t, "node-a", "127.0.0.1:0")
	dB := openHANode(t, "node-b", "127.0.0.1:0")
	defer dB.Close()

	createT(t, []*db.DB{dA, dB}, "t1")
	// 留空位：r0=[min,5) 占 1..3，r1=[5,max) 占 5..9；id=4 / id=10 留给后续写入。
	for _, i := range []int{1, 2, 3, 5, 6, 7, 8, 9} {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO t1 (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	if err := dA.SQL.SplitTable("t1", [][]byte{splitBoundary(5)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("t1")
	if err != nil || len(regions) != 2 {
		t.Fatalf("ListRegions: want 2, got %v err=%v", regions, err)
	}
	mustSQL(t, dA, "ADD NODE '"+dB.Cluster.SelfNode().Addr+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", regions[1].RegionID, dB.Cluster.SelfID))
	// 单点 TSO：B 的取号全部走 A。
	if err := dB.SetTSONode(dA.Cluster.SelfID, 8); err != nil {
		t.Fatalf("SetTSONode: %v", err)
	}
	// 基线：A 存活时 B 对自己本地 region 的写正常（取号成功）。
	mustSQL(t, dB, "INSERT INTO t1 (id, name, age) VALUES (10, 'u10', 30)")

	// 模拟 TSO 中心节点宕机。
	dA.Close()

	// 数据在 B 本地、region 归属不变，但取号源不可达 → 写失败。
	err = execErr(t, dB, "INSERT INTO t1 (id, name, age) VALUES (11, 'u11', 31)")
	if err == nil {
		t.Fatalf("want tso unavailable after TSO node down, got success")
	}
	if !strings.Contains(err.Error(), "tso unavailable") {
		t.Fatalf("want tso unavailable, got %v", err)
	}
}

// TestB2TSOHAFailover 主备 + epoch 选主端到端验证（三节点 TSO 组）：
//  1. 三节点同组 [A,B,C]、A 为首选，写入触发选主后 A 为 TSO 主；
//  2. A 存活期 B 的跨节点事务写正常；
//  3. 关闭 A 后剩余两节点构成多数派 → 自动选主接管；
//  4. 接管后 B/C 的写（含跨节点）恢复，取号严格越过故障前最大戳、无重复。
func TestB2TSOHAFailover(t *testing.T) {
	dA := openHANode(t, "node-a", "127.0.0.1:0")
	dB := openHANode(t, "node-b", "127.0.0.1:0")
	dC := openHANode(t, "node-c", "127.0.0.1:0")
	defer dB.Close()
	defer dC.Close()

	createT(t, []*db.DB{dA, dB, dC}, "t2")
	// 三个 region 按 id 切分：r0=[min,4) 属 A、r1=[4,7) 属 B、r2=[7,max) 属 C。
	// 初始化只填 1,2,3 / 5,6 / 8,9，把 id=4（B 本地）与 id=7（C 本地）留空，
	// 供后续写入使用；跨节点写与故障后写统一用 r2 区间的大 id。
	for _, i := range []int{1, 2, 3, 5, 6, 8, 9} {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO t2 (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	if err := dA.SQL.SplitTable("t2", [][]byte{splitBoundary(4), splitBoundary(7)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("t2")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: want 3, got %v err=%v", regions, err)
	}
	mustSQL(t, dA, "ADD NODE '"+dB.Cluster.SelfNode().Addr+"'")
	mustSQL(t, dA, "ADD NODE '"+dC.Cluster.SelfNode().Addr+"'")
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", regions[1].RegionID, dB.Cluster.SelfID))
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", regions[2].RegionID, dC.Cluster.SelfID))

	// 三节点装配同一 TSO 组（A 为首选主）。
	group := []string{dA.Cluster.SelfID, dB.Cluster.SelfID, dC.Cluster.SelfID}
	for _, d := range []*db.DB{dA, dB, dC} {
		if err := d.EnableTSOHA(group, 8); err != nil {
			t.Fatalf("EnableTSOHA: %v", err)
		}
	}

	// 首次取号触发选主：组首选 A 获得授予。
	if _, err := dA.TSO.Get(); err != nil {
		t.Fatalf("first Get on A (elect): %v", err)
	}
	if got := dA.TSOHALeader(); got != dA.Cluster.SelfID {
		t.Fatalf("want leader %s, got %q", dA.Cluster.SelfID, got)
	}
	// B 的客户端应选到 A（非自封主），并成功完成本地写（id=4 属 B 的 r1）
	// 与跨节点写（id=7 属 C 的 r2）。
	mustSQL(t, dB, "INSERT INTO t2 (id, name, age) VALUES (4, 'u4', 24)")
	mustSQL(t, dB, "INSERT INTO t2 (id, name, age) VALUES (7, 'u7', 27)")
	if got := dB.TSOHALeader(); got != dA.Cluster.SelfID {
		t.Fatalf("B want leader %s, got %q", dA.Cluster.SelfID, got)
	}

	// 故障前序列基线：连续取号，记录最大戳与已用集合。
	seen := map[uint64]bool{}
	var maxPre uint64
	for i := 0; i < 32; i++ {
		ts, err := dC.TSO.Get()
		if err != nil {
			t.Fatalf("pre-failover Get: %v", err)
		}
		if seen[ts] {
			t.Fatalf("pre-failover duplicate ts %d", ts)
		}
		seen[ts] = true
		if ts > maxPre {
			maxPre = ts
		}
	}

	// 模拟 TSO 主节点宕机（A 关闭；其 region 数据不可读属 B1 设计边界，本用例不读）。
	_, isLeaderA, epochOld, _, _ := dA.ClusterSrv.TSOHASnapshot()
	if !isLeaderA || epochOld == 0 {
		t.Fatalf("A should be tso leader before failover (leader=%v epoch=%d)", isLeaderA, epochOld)
	}
	dA.Close()

	// 剩余节点需要心跳判死后重新选主：给足重试窗口。
	waitWrite := func(d *db.DB, stmt string) error {
		var lastErr error
		for i := 0; i < 60; i++ {
			if lastErr = execErr(t, d, stmt); lastErr == nil {
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
		return lastErr
	}
	if err := waitWrite(dB, "INSERT INTO t2 (id, name, age) VALUES (41, 'u41', 41)"); err != nil {
		t.Fatalf("post-failover cross-node write on B: %v", err)
	}
	// 再来一条跨节点写：B 发起、落到 C 的 region。
	if err := waitWrite(dB, "INSERT INTO t2 (id, name, age) VALUES (42, 'u42', 42)"); err != nil {
		t.Fatalf("post-failover 2nd cross-node write on B: %v", err)
	}
	// C 侧同样恢复。
	if err := waitWrite(dC, "INSERT INTO t2 (id, name, age) VALUES (43, 'u43', 43)"); err != nil {
		t.Fatalf("post-failover write on C: %v", err)
	}

	// 新主必须不是已宕机的 A，且 epoch 单调推进（新领导权）。
	newLeader := dB.TSOHALeader()
	if newLeader == "" || newLeader == dA.Cluster.SelfID {
		t.Fatalf("want new leader not %s, got %q", dA.Cluster.SelfID, newLeader)
	}
	_, isLeaderB, epochB, lastB, _ := dB.ClusterSrv.TSOHASnapshot()
	_, isLeaderC, _, _, _ := dC.ClusterSrv.TSOHASnapshot()
	if !isLeaderB && !isLeaderC {
		t.Fatalf("neither B nor C became tso leader after A down (epochB=%d)", epochB)
	}
	if epochB != 0 && epochB <= epochOld {
		t.Fatalf("new leader epoch=%d must exceed old epoch=%d", epochB, epochOld)
	}
	if lastB < maxPre {
		t.Fatalf("new leader last=%d regressed below pre-failover max=%d", lastB, maxPre)
	}

	// 不回落、不重复：新主发出的号必须严格大于故障前所有已见号。
	var prev uint64
	for i := 0; i < 32; i++ {
		ts, err := dC.TSO.Get()
		if err != nil {
			t.Fatalf("post-failover Get: %v", err)
		}
		if ts <= maxPre {
			t.Fatalf("post-failover ts %d <= pre-failover max %d (rollback)", ts, maxPre)
		}
		if seen[ts] {
			t.Fatalf("post-failover ts %d duplicated a pre-failover allocation", ts)
		}
		if prev != 0 && ts <= prev {
			t.Fatalf("post-failover ts not monotonic: %d then %d", prev, ts)
		}
		seen[ts] = true
		prev = ts
	}

	// 数据面：查 A 宕机前归属 B 的 r1 与 C 的 r2（id=4/7 在内的既有行 + 故障后
	// 新写行）。刻意不查 r0——它属已宕机的 A，读不到属 B1 设计边界，与本项无关。
	// 取值用等值条件（单点路由），避免范围查询触发跨区归并时打到宕机节点。
	for _, c := range []struct {
		d    *db.DB
		id   int
		note string
	}{
		{dB, 4, "B 本地 r1 既有写入"},
		{dB, 7, "故障前由 B 跨节点写入 C 的 r2"},
		{dB, 41, "故障后由 B 跨节点写入 C 的 r2"},
		{dC, 42, "故障后由 B 跨节点写入 C 的 r2"},
		{dC, 43, "故障后 C 本地写入"},
	} {
		r := mustSQL(t, c.d, fmt.Sprintf("SELECT id FROM t2 WHERE id = %d", c.id))
		if len(r.Rows) != 1 || r.Rows[0][0].I != int64(c.id) {
			t.Fatalf("%s: id=%d not readable after failover, rows=%v", c.note, c.id, r.Rows)
		}
	}
}

// TestB2TSOHANoQuorumNoSplitBrain 分区少数派不自封主：
// 三节点组中另两节点不可达时，本节点取号必须失败，
// 从而不会与另一侧分区各自发号造成时间戳重复。
func TestB2TSOHANoQuorumNoSplitBrain(t *testing.T) {
	dA := openHANode(t, "node-a", "127.0.0.1:0")
	dB := openHANode(t, "node-b", "127.0.0.1:0")
	dC := openHANode(t, "node-c", "127.0.0.1:0")
	defer dA.Close()

	group := []string{dA.Cluster.SelfID, dB.Cluster.SelfID, dC.Cluster.SelfID}
	if err := dA.EnableTSOHA(group, 8); err != nil {
		t.Fatalf("EnableTSOHA: %v", err)
	}
	// 孤立 A：组内另两个成员下线（模拟双向分区少数派）。
	dB.Close()
	dC.Close()

	// 取号必须失败（无多数派 → 不允许自封主），且不能静默降级成本地发号。
	_, err := dA.TSO.Get()
	if err == nil {
		t.Fatalf("want quorum error when majority unreachable, got success")
	}
	if !strings.Contains(err.Error(), "quorum") && !strings.Contains(err.Error(), "tso") {
		t.Fatalf("want quorum/tso error, got %v", err)
	}
	if _, isLeader, _, _, _ := dA.ClusterSrv.TSOHASnapshot(); isLeader {
		t.Fatalf("isolated minority must not become tso leader")
	}
}

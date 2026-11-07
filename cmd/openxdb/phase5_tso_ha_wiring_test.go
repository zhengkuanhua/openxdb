package main

// Phase5 A 项（B2 生产接线）端到端验证：
//   - TestParseTSOHAGroup：--tso-ha-group 解析（空白/空项/去重/顺序稳定）；
//   - TestSetupTSOHAWiresProductionPath：生产装配函数 setupTSOHA 真正把节点
//     发号源切到组内主备客户端（此前 EnableTSOHA 仅被 pkg/db 测试调用，
//     生产启动路径未接线 → D4.5 Phase4 复测主宕机写全失败）；
//   - TestSetupTSOHAFailoverE2E：走生产装配入口的三节点端到端故障切换，
//     主节点宕机后跨节点写自动恢复，时间戳不回落、不重复。

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// prodOpenNode 建一个独立数据目录并启动集群链路的节点（与生产启动顺序一致：
// Init → Open → StartCluster → setupTSOHA）。目录自管，容忍 Windows 句柄
// 释放滞后导致的清理失败。
func prodOpenNode(t *testing.T, name, addr string) *db.DB {
	t.Helper()
	dir, err := os.MkdirTemp("", "p5-"+name+"-")
	if err != nil {
		t.Fatalf("%s mkdtemp: %v", name, err)
	}
	if err := db.Init(dir); err != nil && err != db.ErrAlreadyInitialized {
		t.Fatalf("%s init: %v", name, err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("%s open: %v", name, err)
	}
	if _, err := d.StartCluster(addr); err != nil {
		t.Fatalf("%s start cluster: %v", name, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return d
}

func prodMustSQL(t *testing.T, d *db.DB, stmt string) {
	t.Helper()
	if _, err := d.SQL.Execute(stmt); err != nil {
		t.Fatalf("SQL %q: %v", stmt, err)
	}
}

func prodSplitBoundary(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

// TestParseTSOHAGroup 解析规则：去空白、丢空项、按序去重、空串为 nil。
func TestParseTSOHAGroup(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"n1", []string{"n1"}},
		{"n1,n2,n3", []string{"n1", "n2", "n3"}},
		{" n1 , n2 ,, n1 ", []string{"n1", "n2"}},
		{"n2,n1", []string{"n2", "n1"}},
	}
	for _, c := range cases {
		got := parseTSOHAGroup(c.in)
		if len(got) != len(c.want) {
			t.Fatalf("parseTSOHAGroup(%q)=%v, want %v", c.in, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("parseTSOHAGroup(%q)=%v, want %v", c.in, got, c.want)
			}
		}
	}
}

// TestSetupTSOHAWiresProductionPath 生产装配生效证明：调用 setupTSOHA 后
// 节点 TSO 必须是 HATSOClient（而非本地发号器/单点客户端），且组信息正确。
func TestSetupTSOHAWiresProductionPath(t *testing.T) {
	dA := prodOpenNode(t, "node-a", "127.0.0.1:0")
	defer dA.Close()
	dB := prodOpenNode(t, "node-b", "127.0.0.1:0")
	defer dB.Close()

	group := []string{dA.Cluster.SelfID, dB.Cluster.SelfID}
	if err := setupTSOHA(dA, group, 16); err != nil {
		t.Fatalf("setupTSOHA: %v", err)
	}
	c := dA.TSOHAClient()
	if c == nil {
		t.Fatalf("TSO HA client not installed by setupTSOHA (production path not wired)")
	}
	got := c.Group()
	if len(got) != 2 || got[0] != group[0] || got[1] != group[1] {
		t.Fatalf("group mismatch: want %v, got %v", group, got)
	}
	// 空组必须显式报错，不允许静默保持单点发号。
	if err := setupTSOHA(dA, nil, 16); err == nil {
		t.Fatalf("setupTSOHA with empty group must fail")
	}
}

// TestSetupTSOHAFailoverE2E 生产装配入口的三节点端到端故障切换：
//  1. 三节点同组、A 首选，写入触发选主后 A 为 TSO 主；
//  2. A 存活期 B 的本地写与跨节点写正常；
//  3. 关闭 A（主宕机）后 B/C 构成多数派自动选主；
//  4. 跨节点写自动恢复，取号严格越过故障前最大戳且不重复。
func TestSetupTSOHAFailoverE2E(t *testing.T) {
	dA := prodOpenNode(t, "node-a", "127.0.0.1:0")
	dB := prodOpenNode(t, "node-b", "127.0.0.1:0")
	dC := prodOpenNode(t, "node-c", "127.0.0.1:0")
	defer dB.Close()
	defer dC.Close()

	for _, d := range []*db.DB{dA, dB, dC} {
		prodMustSQL(t, d, "CREATE TABLE t1 (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	}
	for _, i := range []int{1, 2, 3, 5, 6, 8, 9} {
		prodMustSQL(t, dA, fmt.Sprintf("INSERT INTO t1 (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	if err := dA.SQL.SplitTable("t1", [][]byte{prodSplitBoundary(4), prodSplitBoundary(7)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("t1")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: want 3, got %v err=%v", regions, err)
	}
	prodMustSQL(t, dA, "ADD NODE '"+dB.Cluster.SelfNode().Addr+"'")
	prodMustSQL(t, dA, "ADD NODE '"+dC.Cluster.SelfNode().Addr+"'")
	prodMustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", regions[1].RegionID, dB.Cluster.SelfID))
	prodMustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", regions[2].RegionID, dC.Cluster.SelfID))

	// 生产装配入口：三节点同一组，A 为首选。
	group := parseTSOHAGroup(strings.Join([]string{
		dA.Cluster.SelfID, dB.Cluster.SelfID, dC.Cluster.SelfID,
	}, ","))
	if len(group) != 3 {
		t.Fatalf("group parse: %v", group)
	}
	for _, d := range []*db.DB{dA, dB, dC} {
		if err := setupTSOHA(d, group, 8); err != nil {
			t.Fatalf("setupTSOHA on %s: %v", d.Cluster.SelfID, err)
		}
	}

	// 首次取号触发选主：首选 A 获得授予。
	if _, err := dA.TSO.Get(); err != nil {
		t.Fatalf("first Get on A (elect): %v", err)
	}
	if got := dA.TSOHALeader(); got != dA.Cluster.SelfID {
		t.Fatalf("want leader %s, got %q", dA.Cluster.SelfID, got)
	}
	// B 侧本地写（id=4 属 B 的 r1）与跨节点写（id=7 属 C 的 r2）。
	prodMustSQL(t, dB, "INSERT INTO t1 (id, name, age) VALUES (4, 'u4', 24)")
	prodMustSQL(t, dB, "INSERT INTO t1 (id, name, age) VALUES (7, 'u7', 27)")
	if got := dB.TSOHALeader(); got != dA.Cluster.SelfID {
		t.Fatalf("B want leader %s, got %q", dA.Cluster.SelfID, got)
	}

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
	_, isLeaderA, epochOld, _, _ := dA.ClusterSrv.TSOHASnapshot()
	if !isLeaderA || epochOld == 0 {
		t.Fatalf("A should be tso leader before failover (leader=%v epoch=%d)", isLeaderA, epochOld)
	}

	// 主宕机。
	dA.Close()

	waitWrite := func(d *db.DB, stmt string) error {
		var lastErr error
		for i := 0; i < 60; i++ {
			if lastErr = execProdSQL(d, stmt); lastErr == nil {
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
		return lastErr
	}
	// 跨节点写自动恢复（本项核心证明）。
	if err := waitWrite(dB, "INSERT INTO t1 (id, name, age) VALUES (41, 'u41', 41)"); err != nil {
		t.Fatalf("post-failover cross-node write on B: %v", err)
	}
	if err := waitWrite(dB, "INSERT INTO t1 (id, name, age) VALUES (42, 'u42', 42)"); err != nil {
		t.Fatalf("post-failover 2nd cross-node write on B: %v", err)
	}
	if err := waitWrite(dC, "INSERT INTO t1 (id, name, age) VALUES (43, 'u43', 43)"); err != nil {
		t.Fatalf("post-failover write on C: %v", err)
	}

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
	// 取号不回落、不重复。
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

	// 数据面：故障后写入的跨节点行必须可读（B 发起 → C 落盘）。
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
		res, qerr := c.d.SQL.Execute(fmt.Sprintf("SELECT id FROM t1 WHERE id = %d", c.id))
		if qerr != nil {
			t.Fatalf("%s: id=%d query error after failover: %v", c.note, c.id, qerr)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("%s: id=%d not readable after failover, rows=%v", c.note, c.id, res.Rows)
		}
	}
}

func execProdSQL(d *db.DB, stmt string) error {
	_, err := d.SQL.Execute(stmt)
	return err
}

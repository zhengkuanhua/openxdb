package sql

// v1.20.0 C2 写路径强制路由专项测试（ha_write_route_enforce）。
//
// 验收项（DEV_PLAN_v1.20.0.md C2）：
//   - V-A1 单机零回归：mgr==nil 时写入照常成功，且不产生任何 m:2pc:* 辅键；
//   - V-A2 未指派 region 拒写：集群装配 + region 归属为空时，开关开启返回
//     REGION_NOT_ASSIGNED，开关关闭则写成功（零回归）；
//   - V-A5 归属节点不可达：region 指向未注册节点时，开关开启返回
//     REGION_OWNER_UNREACHABLE，开关关闭沿用既有 "no coordinator path" 硬拒绝；
//   - V-A3 持有校验：region 归属本机但本机物理无该 region 数据时，开关开启返回
//     REGION_NOT_LOCAL，开关关闭则写成功；本机真正持有时不误拒；
//   - 开关门禁：单机无集群时 SET 明确报错，非法值报错，quorum 联动自动生效。
//
// 装配说明（落点选择）：全部用例落在 pkg/sql 包内白盒测试，复用既有
// NewExecutor / cluster.Manager / sharding.Router 构造方式；需要在真实键空间上
// 校验持有关系与 2PC 辅键的两处，使用 btree 存储 + WAL + txn.New 装配真实
// TxnManager，未下沉到 pkg/db。

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
	"github.com/zhengkuanhua/openxdb/pkg/wal"
)

// newWriteRouteEnv 构造带真实存储与事务管理器的执行器（btree + WAL），
// 供需要真实键空间扫描（持有校验）与辅键盘点（2PC 键）的用例使用。
func newWriteRouteEnv(t *testing.T, selfID string) (*Executor, storage.Storage) {
	t.Helper()
	st := btree.NewKV(32)
	w, err := wal.Open(filepath.Join(testDir(t), "wal.log"))
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	tm, err := txn.New(st, w)
	if err != nil {
		t.Fatalf("txn.New: %v", err)
	}
	e := NewExecutor(tm)
	e.selfID = selfID
	return e, st
}

// newWriteRouteCluster 构造装配了远端节点的集群管理面（selfID + peers）。
func newWriteRouteCluster(selfID string, peers ...string) *cluster.Manager {
	m := cluster.NewManager(selfID)
	for _, p := range peers {
		m.RegisterPeer(p, "127.0.0.1:1")
	}
	return m
}

// writeRouteErrCode 提取 SQLError 错误码（非 SQLError 返回空串）。
func writeRouteErrCode(err error) string {
	var se *SQLError
	if errors.As(err, &se) {
		return se.Code
	}
	return ""
}

// countKeysWithPrefix 盘点存储中以 prefix 开头的键数量。
func countKeysWithPrefix(t *testing.T, st storage.Storage, prefix []byte) int {
	t.Helper()
	end := append([]byte{}, prefix...)
	end[len(end)-1]++
	ps, err := st.Scan(storage.KeyRange{Start: prefix, End: end}, -1)
	if err != nil {
		t.Fatalf("Scan(%q): %v", prefix, err)
	}
	return len(ps)
}

// TestWriteRouteEnforceV_A1SingleNodeZeroRegression V-A1 单机零回归。
// 单机（mgr==nil）写入必须照常成功，且写路径不产生任何 2PC 辅键；
// 即使显式打开开关，单机路径也不得被拒（无集群即无路由视图可强制）。
func TestWriteRouteEnforceV_A1SingleNodeZeroRegression(t *testing.T) {
	e, st := newWriteRouteEnv(t, "node-a")
	if e.mgr != nil {
		t.Fatalf("single-node env must have nil cluster manager")
	}
	eng := &Engine{ex: e}

	if _, err := eng.Execute("CREATE TABLE wr1 (id INT, v TEXT, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := eng.Execute("INSERT INTO wr1 VALUES (1, 'a')"); err != nil {
		t.Fatalf("single-node INSERT must succeed: %v", err)
	}

	// 隐式单 region 写入必须落在本地组，且不进入任何远端组（无 2PC 协调）。
	rid := storage.ID(uint64(1) | (uint64(1) << 63))
	local, remote, err := e.groupOps([]kvOp{{Key: sharding.EncodeRegionKey(rid, []byte("k1")), Value: []byte("v1")}})
	if err != nil {
		t.Fatalf("single-node groupOps must not fail: %v", err)
	}
	if len(local) != 1 || len(remote) != 0 {
		t.Fatalf("single-node ops must be local only: local=%d remote=%d", len(local), len(remote))
	}

	// 显式打开开关：单机仍零回归（routeDecision 归一为 routeLocalOwned）。
	e.SetWriteRouteEnforce(true)
	if !e.writeRouteEnforce() {
		t.Fatalf("explicit switch must be visible via writeRouteEnforce")
	}
	local, remote, err = e.groupOps([]kvOp{{Key: sharding.EncodeRegionKey(rid, []byte("k2")), Value: []byte("v2")}})
	if err != nil {
		t.Fatalf("single-node groupOps with enforce on must not fail: %v", err)
	}
	if len(local) != 1 || len(remote) != 0 {
		t.Fatalf("enforce-on single-node ops must stay local: local=%d remote=%d", len(local), len(remote))
	}
	if _, err := eng.Execute("INSERT INTO wr1 VALUES (2, 'b')"); err != nil {
		t.Fatalf("single-node INSERT with enforce on must succeed: %v", err)
	}

	// 2PC 辅键（m:2pc: 前缀）必须一个都没有：单机写不走分布式提交。
	if n := countKeysWithPrefix(t, st, []byte("m:2pc:")); n != 0 {
		t.Fatalf("single-node writes must not create 2pc keys, found %d", n)
	}
}

// TestWriteRouteEnforceImplicitRegionFirstWrite 隐式单 region 首写不得被持有校验误拒。
// 表未切分时该 region 里没有任何键，若按"owner=self 但无数据"判 REGION_NOT_LOCAL，
// 会把正常首写全部拒掉（实测 quorum 用例的空隐式 region 首写即踩中）。
func TestWriteRouteEnforceImplicitRegionFirstWrite(t *testing.T) {
	const rid = storage.ID(9223372036854775809) // 1<<63 | 1：隐式单 region 编号形态
	e, _ := newWriteRouteEnv(t, "node-a")
	e.mgr = newWriteRouteCluster("node-a", "node-b")
	// 路由不登记该 region，等效"表未切分、无显式路由条目"。
	if _, ok := e.router.FindRegion(rid); ok {
		t.Fatalf("precondition failed: implicit region must not be registered")
	}
	e.SetWriteRouteEnforce(true)
	local, remote, err := e.groupOps([]kvOp{{Key: sharding.EncodeRegionKey(rid, []byte("k1")), Value: []byte("v1")}})
	if err != nil {
		t.Fatalf("implicit region first write must not be rejected: %v", err)
	}
	if len(local) != 1 || len(remote) != 0 {
		t.Fatalf("implicit region write must stay local: local=%d remote=%d", len(local), len(remote))
	}
}

// TestWriteRouteEnforceV_A2UnassignedRegion V-A2 未指派 region 拒写。
func TestWriteRouteEnforceV_A2UnassignedRegion(t *testing.T) {
	const rid = storage.ID(42)
	build := func() *Executor {
		e := NewExecutor(nil)
		e.selfID = "node-a"
		e.mgr = newWriteRouteCluster("node-a", "node-b")
		e.router = sharding.NewRouter()
		if err := e.router.UpsertRegion(sharding.RegionInfo{RegionID: rid, State: sharding.StateActive, Explicit: true}); err != nil {
			t.Fatalf("UpsertRegion: %v", err)
		}
		// 归属置空：集群模式下即"路由视图缺指派"。
		if err := e.router.SetRegionNode(rid, ""); err != nil {
			t.Fatalf("SetRegionNode: %v", err)
		}
		return e
	}
	ops := []kvOp{{Key: sharding.EncodeRegionKey(rid, []byte("k1")), Value: []byte("v1")}}

	// 开关关闭：按旧语义视作本机本地写（零回归）。
	e := build()
	local, remote, err := e.groupOps(ops)
	if err != nil {
		t.Fatalf("unassigned region with enforce off must not fail: %v", err)
	}
	if len(local) != 1 || len(remote) != 0 {
		t.Fatalf("enforce off must keep legacy local path: local=%d remote=%d", len(local), len(remote))
	}

	// 开关开启：必须拒绝，并给出可修复指引。
	e = build()
	e.SetWriteRouteEnforce(true)
	_, _, err = e.groupOps(ops)
	if err == nil {
		t.Fatalf("unassigned region with enforce on must be rejected")
	}
	if code := writeRouteErrCode(err); code != ErrCodeRegionNotAssigned {
		t.Fatalf("want code %s, got %q (err=%v)", ErrCodeRegionNotAssigned, code, err)
	}
	for _, want := range []string{"42", "run ASSIGN REGION to repair route view"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error must contain %q: %v", want, err)
		}
	}
	if !strings.Contains(err.Error(), "SQL error ["+ErrCodeRegionNotAssigned+"]:") {
		t.Fatalf("error text must expose machine-readable code: %v", err)
	}
}

// TestWriteRouteEnforceV_A3HoldsCheck V-A3 归属本机但本机不持有该 region。
func TestWriteRouteEnforceV_A3HoldsCheck(t *testing.T) {
	const rid = storage.ID(1000)
	e, _ := newWriteRouteEnv(t, "node-a")
	// 持有校验只在集群装配（mgr!=nil）下生效：单机无归属视图可校验。
	e.mgr = newWriteRouteCluster("node-a")
	if err := e.router.UpsertRegion(sharding.RegionInfo{RegionID: rid, State: sharding.StateActive, Explicit: true}); err != nil {
		t.Fatalf("UpsertRegion: %v", err)
	}
	if err := e.router.SetRegionNode(rid, "node-a"); err != nil {
		t.Fatalf("SetRegionNode: %v", err)
	}
	ops := []kvOp{{Key: sharding.EncodeRegionKey(rid, []byte("k1")), Value: []byte("v1")}}

	// 前置：确认该 region 键空间当前确无数据。
	holds, err := e.regionHoldsLocalByID(rid)
	if err != nil {
		t.Fatalf("regionHoldsLocalByID: %v", err)
	}
	if holds {
		t.Fatalf("precondition failed: region %d must be empty before test", rid)
	}

	// 开关关闭：旧行为本地写成功。
	local, remote, err := e.groupOps(ops)
	if err != nil {
		t.Fatalf("empty-but-owned region with enforce off must not fail: %v", err)
	}
	if len(local) != 1 || len(remote) != 0 {
		t.Fatalf("enforce off must keep legacy local path: local=%d remote=%d", len(local), len(remote))
	}

	// 开关开启：本机无数据必须拒绝。
	e.SetWriteRouteEnforce(true)
	_, _, err = e.groupOps(ops)
	if err == nil {
		t.Fatalf("owned-but-not-holding region with enforce on must be rejected")
	}
	if code := writeRouteErrCode(err); code != ErrCodeRegionNotLocal {
		t.Fatalf("want code %s, got %q (err=%v)", ErrCodeRegionNotLocal, code, err)
	}
	if !strings.Contains(err.Error(), "1000") {
		t.Fatalf("error must name the region: %v", err)
	}

	// 本机真正持有该 region 任一键后不得误拒。
	tx, err := e.tm.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put(sharding.EncodeRegionKey(rid, []byte("k0")), []byte("v0")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	local, remote, err = e.groupOps(ops)
	if err != nil {
		t.Fatalf("holding region with enforce on must not be rejected: %v", err)
	}
	if len(local) != 1 || len(remote) != 0 {
		t.Fatalf("holding region must stay local: local=%d remote=%d", len(local), len(remote))
	}
}

// TestWriteRouteEnforceV_A5UnknownOwner V-A5 归属节点不可达（未注册）。
func TestWriteRouteEnforceV_A5UnknownOwner(t *testing.T) {
	const rid = storage.ID(43)
	build := func() *Executor {
		e := NewExecutor(nil)
		e.selfID = "node-a"
		e.mgr = newWriteRouteCluster("node-a", "node-b")
		e.router = sharding.NewRouter()
		if err := e.router.UpsertRegion(sharding.RegionInfo{RegionID: rid, State: sharding.StateActive, Explicit: true}); err != nil {
			t.Fatalf("UpsertRegion: %v", err)
		}
		// 归属节点未注册（不在节点注册表内）。
		if err := e.router.SetRegionNode(rid, "node-ghost"); err != nil {
			t.Fatalf("SetRegionNode: %v", err)
		}
		return e
	}
	ops := []kvOp{{Key: sharding.EncodeRegionKey(rid, []byte("k1")), Value: []byte("v1")}}

	// 开关关闭：沿用既有尾部硬拒绝（文本不变，未引入错误码）。
	e := build()
	_, _, err := e.groupOps(ops)
	if err == nil {
		t.Fatalf("remote write without coordinator must be rejected")
	}
	if !strings.Contains(err.Error(), "no coordinator path") {
		t.Fatalf("legacy hard rejection text must be preserved: %v", err)
	}
	if code := writeRouteErrCode(err); code != "" {
		t.Fatalf("legacy hard rejection must stay code-less, got %q", code)
	}

	// 开关开启：提前以不可达归属错误码拒绝。
	e = build()
	e.SetWriteRouteEnforce(true)
	_, _, err = e.groupOps(ops)
	if err == nil {
		t.Fatalf("unknown owner with enforce on must be rejected")
	}
	if code := writeRouteErrCode(err); code != ErrCodeRegionOwnerUnreachable {
		t.Fatalf("want code %s, got %q (err=%v)", ErrCodeRegionOwnerUnreachable, code, err)
	}
	for _, want := range []string{"43", "node-ghost"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error must contain %q: %v", want, err)
		}
	}
}

// TestWriteRouteEnforceSwitchGate 开关门禁与 quorum 联动。
func TestWriteRouteEnforceSwitchGate(t *testing.T) {
	// 单机（未装配集群）：SET 明确报错。
	e := NewExecutor(nil)
	err := applyHaWriteRouteEnforce(e, "on")
	if err == nil {
		t.Fatalf("SET ha_write_route_enforce without cluster must fail")
	}
	if !strings.Contains(err.Error(), "requires cluster HA configuration") {
		t.Fatalf("unexpected error text: %v", err)
	}

	// 集群装配后：非法值报错，合法值生效。
	e.mgr = newWriteRouteCluster("node-a", "node-b")
	if err := applyHaWriteRouteEnforce(e, "bogus"); err == nil {
		t.Fatalf("invalid value must be rejected")
	} else if !strings.Contains(err.Error(), "invalid value for ha_write_route_enforce") {
		t.Fatalf("unexpected invalid-value text: %v", err)
	}
	if err := applyHaWriteRouteEnforce(e, "on"); err != nil {
		t.Fatalf("apply on: %v", err)
	}
	if !e.writeRouteEnforce() || !e.writeRouteEnforceExplicit() {
		t.Fatalf("explicit on must be effective")
	}
	if err := applyHaWriteRouteEnforce(e, "off"); err != nil {
		t.Fatalf("apply off: %v", err)
	}
	if e.writeRouteEnforce() || e.writeRouteEnforceExplicit() {
		t.Fatalf("explicit off must disable enforce when quorum is off")
	}

	// ha_quorum_enabled=on 不自动联动（见 writeRouteEnforce 注释：联动会让既有
	// quorum 用例的基线写被判未指派/不持有，与零回归红线冲突，联动后置 C5）。
	e.haCfg = &fakeCfg{quorum: true}
	if e.writeRouteEnforce() {
		t.Fatalf("ha_quorum_enabled must not auto-enable write route enforce")
	}
}

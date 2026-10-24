package db

// 阶段二回迁自动化测试（v1.6.0 P2 HA，docs/T43_ha_rpo_rejoin.md）：
// ① 补偿帧 msgCompensate 回灌：旧主崩溃前未复制段推给新主，新主数据与
//    旧主崩溃前一致，且其他从端经新主 binlog 同步到补偿段；
// ② 旧主恢复识别新主 epoch 并重置基线，以全新 follower 追平新主新写；
// ③ 补偿缺失/失败进入全量对齐兜底判定路径。
// 复用 M2 基建（openTestDB/commitKV/waitFollowerLSN）。

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/replication"
)

// TestRejoinCompensationBackfill 主端崩溃前未复制段经补偿帧回灌新主后，
// 新主数据与旧主崩溃前一致，且其他从端同步到补偿段（经新主 binlog）。
func TestRejoinCompensationBackfill(t *testing.T) {
	old := openTestDB(t)
	if err := old.StartReplication(":0"); err != nil {
		t.Fatalf("old StartReplication: %v", err)
	}
	for i := 1; i <= 8; i++ {
		commitKV(t, old, "k"+strconv.Itoa(i), "v")
	}
	// 新主接管；尚未收到补偿前无旧主数据
	nm := openTestDB(t)
	if err := nm.StartReplication(":0"); err != nil {
		t.Fatalf("new master StartReplication: %v", err)
	}
	nmAddr, _ := nm.ReplicationStatus()

	// 旧主崩溃前未复制段 [1,8] 作为补偿批次回灌新主（独立一次性连接）
	entries, err := old.readLocalBinlogRange(1, 8)
	if err != nil {
		t.Fatalf("readLocalBinlogRange: %v", err)
	}
	if len(entries) != 8 {
		t.Fatalf("compensation entries = %d, want 8", len(entries))
	}
	if _, err := replication.PushCompensation(context.Background(), nmAddr, entries, nil); err != nil {
		t.Fatalf("PushCompensation: %v", err)
	}

	// 新主数据与旧主崩溃前一致（k1..k8）
	for i := 1; i <= 8; i++ {
		v, err := nm.Storage.Get([]byte("k" + strconv.Itoa(i)))
		if err != nil || string(v) != "v" {
			t.Fatalf("new master k%d = %q err=%v", i, v, err)
		}
	}
	// 新主 binlog 追加补偿段（LSN 续增 1..8）
	if got := nm.Replicator.LastLSN(); got != 8 {
		t.Fatalf("new master LastLSN = %d, want 8", got)
	}

	// 其他从端同步到补偿段：新主 binlog 已含补偿条目，从端经正常复制流追平
	third := openTestDB(t)
	if err := third.StartFollower(nmAddr); err != nil {
		t.Fatalf("third StartFollower: %v", err)
	}
	waitFollowerLSN(t, third.Follower, 8)
	for i := 1; i <= 8; i++ {
		v, err := third.Storage.Get([]byte("k" + strconv.Itoa(i)))
		if err != nil || string(v) != "v" {
			t.Fatalf("third k%d = %q err=%v", i, v, err)
		}
	}
}

// TestRejoinStaleMasterResetsAndTracksNewMaster 旧主恢复后识别新主更高
// epoch（fencing 降级只读）、补偿回灌成功、归档旧 binlog 重置基线，并以
// 全新 follower 身份从 LSN=1 追平新主完整 binlog（含新主新写）。
func TestRejoinStaleMasterResetsAndTracksNewMaster(t *testing.T) {
	old := openTestDB(t)
	old.SetEpochEnabled(true)
	if err := old.StartReplication(":0"); err != nil {
		t.Fatalf("old StartReplication: %v", err)
	}
	for i := 1; i <= 5; i++ {
		commitKV(t, old, "k"+strconv.Itoa(i), "v")
	}

	nm := openTestDB(t)
	nm.SetEpochEnabled(true)
	if err := nm.StartReplication(":0"); err != nil {
		t.Fatalf("new master StartReplication: %v", err)
	}
	nmAddr, _ := nm.ReplicationStatus()
	// 新主已接管并处理新写（LSN 1,2）
	for i := 6; i <= 7; i++ {
		commitKV(t, nm, "k"+strconv.Itoa(i), "v")
	}

	// 旧主恢复：收到新主更高 clusterEpoch → 判定为陈旧主、降级只读（fencing）
	old.NoteEpoch(nm.Epoch() + 1)
	if !old.ReadOnly() {
		t.Fatalf("old should be read-only after higher epoch fencing")
	}

	// 回迁：补偿回灌 k1..k5 + 归档 binlog 重置基线 + 以全新 follower 追平
	if err := old.TryRejoinAsFollower(nmAddr); err != nil {
		t.Fatalf("TryRejoinAsFollower: %v", err)
	}
	if old.Replicator != nil {
		t.Fatalf("old replicator should be closed after rejoin")
	}
	if old.Follower == nil {
		t.Fatalf("old should run as follower after rejoin")
	}
	// 新主 binlog：k6..k7(LSN1,2) + 补偿 k1..k5(LSN3..7)，旧主追平到 7
	waitFollowerLSN(t, old.Follower, 7)

	// 数据：旧主崩溃前 k1..k5 + 新主新写 k6..k7 全部就位
	for i := 1; i <= 7; i++ {
		v, err := old.Storage.Get([]byte("k" + strconv.Itoa(i)))
		if err != nil || string(v) != "v" {
			t.Fatalf("rejoined old k%d = %q err=%v", i, v, err)
		}
	}
	// 基线重置：旧 binlog 已归档，节点 BinlogLSN 归 0（全新 follower 身份）
	if got, err := old.BinlogLSN(); err != nil || got != 0 {
		t.Fatalf("old BinlogLSN after archive = %d err=%v, want 0", got, err)
	}
	// 回迁后写路径仍被 fencing 拒绝（陈旧主身份不恢复写）
	if err := old.AllowWrite(); err == nil {
		t.Fatalf("old write should be fenced after rejoin")
	}
}

// TestRejoinFallbackPathDecision 补偿缺失/失败时进入全量对齐兜底判定路径：
// 场景A 从未担任主（binlog 为空）→ 非陈旧主跳过；场景B binlog 损坏 →
// 补偿失败，判定走全量对齐兜底；场景C 兜底入口接口骨架返回未就绪。
func TestRejoinFallbackPathDecision(t *testing.T) {
	// A：binlog 为空（从未担任主或已重置基线）→ 非陈旧主，跳过回迁
	fresh := openTestDB(t)
	if err := fresh.TryRejoinAsFollower("127.0.0.1:1"); !errors.Is(err, ErrNotStaleMaster) {
		t.Fatalf("empty binlog rejoin err = %v, want ErrNotStaleMaster", err)
	}

	// B：binlog 损坏 → StaleMasterReady 报错 → 判定补偿失败（走全量对齐兜底）
	corrupt := openTestDB(t)
	if err := corrupt.StartReplication(":0"); err != nil {
		t.Fatalf("corrupt StartReplication: %v", err)
	}
	for i := 1; i <= 3; i++ {
		commitKV(t, corrupt, "k"+strconv.Itoa(i), "v")
	}
	_ = corrupt.Replicator.Close()
	corrupt.Replicator = nil
	binlog := filepath.Join(corrupt.Dir, BinlogFile)
	if err := os.WriteFile(binlog, []byte{0xde, 0xad, 0xbe, 0xef, 0x00}, 0o644); err != nil {
		t.Fatalf("corrupt binlog: %v", err)
	}
	err := corrupt.TryRejoinAsFollower("127.0.0.1:1")
	if !errors.Is(err, ErrCompensationFailed) {
		t.Fatalf("corrupt binlog rejoin err = %v, want ErrCompensationFailed", err)
	}

	// C：全量对齐兜底入口（v1.7.0 已接通）：集群未启用 → 返回明确错误
	// ErrFullAlignFailed（不再返回"未就绪"骨架）。
	err = corrupt.FullAlignViaRegionPush("some-node", "127.0.0.1:1")
	if !errors.Is(err, ErrFullAlignFailed) {
		t.Fatalf("FullAlignViaRegionPush (cluster off) err = %v, want ErrFullAlignFailed", err)
	}
}

// TestRejoinFullAlignFallbackAlignsAndTracks 补偿失败自动降级全量对齐（v1.7.0）：
// 双 DB 集群装配（openTestDB + StartCluster + ADD NODE 双向注册），旧主 binlog
// 损坏触发 TryRejoinAsFollower 补偿失败路径 → 自动定位新主节点 →
// FullAlignViaRegionPush 请求新主把全部 region 当前行 REGION_PUSH 推给旧主
// （applyPush 覆盖本地）；对齐完成后旧主清 binlog 基线、以全新 follower 从
// LSN=1 追平新主新写。
func TestRejoinFullAlignFallbackAlignsAndTracks(t *testing.T) {
	old := openTestDB(t)
	old.SetEpochEnabled(true)
	if _, err := old.StartCluster("127.0.0.1:0"); err != nil {
		t.Fatalf("old StartCluster: %v", err)
	}
	// 旧主建表 + 写 k1..k5（生成 binlog；随后损坏触发补偿失败降级）
	mustExec(t, old, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
	if err := old.StartReplication(":0"); err != nil {
		t.Fatalf("old StartReplication: %v", err)
	}
	for i := 1; i <= 5; i++ {
		mustExec(t, old, fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, 'v%d')", i, i))
	}
	// 关闭复制器并损坏 binlog：StaleMasterReady 报错 → 补偿失败 → 自动降级
	_ = old.Replicator.Close()
	old.Replicator = nil
	binlog := filepath.Join(old.Dir, BinlogFile)
	if err := os.WriteFile(binlog, []byte{0xde, 0xad, 0xbe, 0xef, 0x00}, 0o644); err != nil {
		t.Fatalf("corrupt old binlog: %v", err)
	}

	// 新主：空库首表 tableID 与旧主一致（物理键解码互通），继承 k1..k5 后接管
	nm := openTestDB(t)
	nm.SetEpochEnabled(true)
	nmClusterAddr, err := nm.StartCluster("127.0.0.1:0")
	if err != nil {
		t.Fatalf("new master StartCluster: %v", err)
	}
	mustExec(t, nm, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
	for i := 1; i <= 5; i++ {
		mustExec(t, nm, fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, 'v%d')", i, i))
	}
	if err := nm.StartReplication(":0"); err != nil {
		t.Fatalf("new master StartReplication: %v", err)
	}
	nmReplAddr, _ := nm.ReplicationStatus()
	// 集群注册：旧主 ADD NODE 触发双向握手注册（新主侧 FULLALIGN handler 可
	// 以把全部 region 推给旧主节点 ID）。
	res, err := old.SQL.Execute("ADD NODE '" + nmClusterAddr + "'")
	if err != nil {
		t.Fatalf("ADD NODE: %v", err)
	}
	if res.Rows[0][0].S != nm.Cluster.SelfID {
		t.Fatalf("ADD NODE: want node id %q, got %q", nm.Cluster.SelfID, res.Rows[0][0].S)
	}
	// 旧主识别新主更高 epoch → 降级只读（fencing；回迁前置状态）
	old.NoteEpoch(nm.Epoch() + 1)
	if !old.ReadOnly() {
		t.Fatalf("old should be read-only after higher epoch fencing")
	}

	// 回迁：binlog 损坏 → 补偿失败 → 自动降级全量对齐成功
	if err := old.TryRejoinAsFollower(nmReplAddr); err != nil {
		t.Fatalf("TryRejoinAsFollower with full align fallback: %v", err)
	}
	if old.Follower == nil {
		t.Fatalf("old should run as follower after full align fallback")
	}
	if got, err := old.BinlogLSN(); err != nil || got != 0 {
		t.Fatalf("old BinlogLSN after full align = %d err=%v, want 0", got, err)
	}
	// 全量对齐后旧主本地与继承数据一致（applyPush 覆盖写，k1..k5）
	wantDBIDs(t, old, "SELECT id FROM t ORDER BY id", 1, 2, 3, 4, 5)

	// 新主新写 n1..n2（裸键，跨节点 MVCC 时间戳不互见，用 Storage.Get 直读）
	// → 旧主以全新 follower 追平（SQL INSERT 走集群写路径不入复制 binlog，
	// 裸键写生成 binlog LSN 1..2）
	for i := 1; i <= 2; i++ {
		commitKV(t, nm, "n"+strconv.Itoa(i), "v"+strconv.Itoa(i))
	}
	waitFollowerLSN(t, old.Follower, 2)
	for i := 1; i <= 2; i++ {
		v, err := old.Storage.Get([]byte("n" + strconv.Itoa(i)))
		if err != nil || string(v) != "v"+strconv.Itoa(i) {
			t.Fatalf("rejoined old n%d = %q err=%v", i, v, err)
		}
	}
}

// TestFullAlignViaRegionPushUnregisteredPeer 新主节点未注册（对端不可达/未加入
// 集群）时，全量对齐返回明确错误 ErrFullAlignFailed。
func TestFullAlignViaRegionPushUnregisteredPeer(t *testing.T) {
	d := openTestDB(t)
	if _, err := d.StartCluster("127.0.0.1:0"); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	mustExec(t, d, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
	if err := d.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := d.ReplicationStatus()
	// 未注册节点 ID：GetNode 未命中 → ErrFullAlignFailed
	err := d.FullAlignViaRegionPush("ghost-node", addr)
	if !errors.Is(err, ErrFullAlignFailed) {
		t.Fatalf("FullAlignViaRegionPush unregistered err = %v, want ErrFullAlignFailed", err)
	}
}

// mustExec 执行单条 SQL 语句，失败即 fatal。
func mustExec(t *testing.T, d *DB, stmt string) {
	t.Helper()
	if _, err := d.SQL.Execute(stmt); err != nil {
		t.Fatalf("SQL %q: %v", stmt, err)
	}
}

// wantDBIDs 执行 SELECT 并断言第一列 id 值序列。
func wantDBIDs(t *testing.T, d *DB, stmt string, want ...int64) {
	t.Helper()
	res, err := d.SQL.Execute(stmt)
	if err != nil {
		t.Fatalf("SQL %q: %v", stmt, err)
	}
	if len(res.Rows) != len(want) {
		t.Fatalf("%q: want %d rows, got %d (%+v)", stmt, len(want), len(res.Rows), res.Rows)
	}
	for i, w := range want {
		if res.Rows[i][0].I != w {
			t.Fatalf("%q row %d: want %d, got %d", stmt, i, w, res.Rows[i][0].I)
		}
	}
}

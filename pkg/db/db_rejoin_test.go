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

	// C：全量对齐兜底入口判定（接口骨架，SQL 层联动为后续排期）
	if !errors.Is(corrupt.FullAlignViaRegionPush(), ErrFullAlignNotReady) {
		t.Fatalf("FullAlignViaRegionPush err = %v, want ErrFullAlignNotReady",
			corrupt.FullAlignViaRegionPush())
	}
}

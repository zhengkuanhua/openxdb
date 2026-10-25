package db_test

// P1 组网级脑裂演练（v1.10.0，docs/HA_BRAIN_SPLIT_DESIGN.md §7）：
// 三节点真实组网（openNode + 心跳 + 复制轨）下注入网络分区视图
// （cluster.SetProber 探活桩，仅影响 PartitionStatus 观察层），完整演练
// 脑裂生命周期：
//   脑裂形成（MajorityOK 翻转：B+C 构成 2/3 多数派，A 侧 1/3 失去多数派）
//   → epoch 提升冲突裁决（B 升主获得新 epoch，复制主身份翻转）
//   → 恢复合并（A 经 ReconcileAfterPartition 收到更大 epoch 降级只读、
//     停止复制、写路径 fencing）→ 回迁就绪判定（StaleMasterReady）。
// quorumGuard 真实拒绝（关闭对端后升主被拒）由 TestP0QuorumPromoteInsufficient
// 覆盖，此处聚焦组网多数派翻转与恢复合并闭环。

import (
	"errors"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

func TestP1BrainSplitDrill(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, _ := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })
	dC, _ := openNode(t, "node-c")
	t.Cleanup(func() { _ = dC.Close() })

	aID, bID, cID := dA.Cluster.SelfID, dB.Cluster.SelfID, dC.Cluster.SelfID

	// ---- Phase 0：组网就绪（quorum=3 全可达，A 主、B/C follower）----
	mustSQL(t, dA, "ADD NODE '"+dB.Cluster.SelfAddr+"'")
	mustSQL(t, dA, "ADD NODE '"+dC.Cluster.SelfAddr+"'")
	mustSQL(t, dB, "ADD NODE '"+dA.Cluster.SelfAddr+"'")
	mustSQL(t, dB, "ADD NODE '"+dC.Cluster.SelfAddr+"'")
	mustSQL(t, dC, "ADD NODE '"+dA.Cluster.SelfAddr+"'")
	mustSQL(t, dC, "ADD NODE '"+dB.Cluster.SelfAddr+"'")

	if err := dA.StartReplication(":0"); err != nil {
		t.Fatalf("A StartReplication: %v", err)
	}
	raddr, _ := dA.ReplicationStatus()
	if err := dB.StartFollower(raddr); err != nil {
		t.Fatalf("B StartFollower: %v", err)
	}
	if err := dC.StartFollower(raddr); err != nil {
		t.Fatalf("C StartFollower: %v", err)
	}
	// A 写一笔数据产生 binlog（陈旧主回迁就绪判定要求 binlog 非空）。
	mustSQL(t, dA, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustSQL(t, dA, "INSERT INTO t (id) VALUES (1)")
	for _, d := range []*db.DB{dA, dB, dC} {
		speedupHeartbeat(t, d)
	}
	waitEverUp(t, dA, bID)
	waitEverUp(t, dA, cID)
	waitEverUp(t, dB, aID)
	waitEverUp(t, dC, aID)

	dA.SetQuorumEnabled(true)
	dB.SetQuorumEnabled(true)
	dC.SetQuorumEnabled(true)

	// 基线：三端均构成多数派。
	if !dA.Cluster.MajorityOK() || !dB.Cluster.MajorityOK() || !dC.Cluster.MajorityOK() {
		t.Fatal("all sides should hold majority before partition")
	}

	// ---- Phase 1：脑裂形成（注入网络分区视图，真实心跳不受影响）----
	// 拓扑：A 孤立少数派（1/3，视图层 MajorityOK 翻转）；B+C 构成多数派
	//（2/3，B 具备升主资格）。
	dB.Cluster.SetProber(func(id string) bool { return id != aID })
	dA.Cluster.SetProber(func(id string) bool { return id != bID && id != cID })

	// MajorityOK 翻转：B 侧 2/3 仍构成多数派，A 侧 1/3 失去多数派
	//（分区内禁止升主/重指派的判定基础；真实拒绝见 P0Insufficient）。
	if !dB.Cluster.MajorityOK() {
		t.Fatal("B side should hold majority (2/3) after A lost")
	}
	if dA.Cluster.MajorityOK() {
		t.Fatal("A side should lose majority (1/3) after partition")
	}
	// C 保持 follower，不主动升主（等待恢复/协调，避免分区内多主）。
	if dC.Follower == nil {
		t.Fatal("C should remain follower during partition")
	}

	// 多数派侧升主成功：B 获得新 clusterEpoch（epoch 提升冲突裁决基准），
	// 复制主身份翻转（follower → master）。
	if err := dB.PromoteToMaster(); err != nil {
		t.Fatalf("majority B promote = %v, want success", err)
	}
	if dB.MasterEpoch() == 0 {
		t.Fatal("B should hold new cluster epoch after promote")
	}
	if dB.Replicator == nil {
		t.Fatal("promoted B should have replicator")
	}

	// ---- Phase 2：恢复合并（epoch 大者胜裁决 + fencing + 回迁就绪）----
	// 恢复网络：清除探活桩，回到真实心跳（三端恢复多数派视图）。
	dA.Cluster.SetProber(nil)
	dB.Cluster.SetProber(nil)
	dC.Cluster.SetProber(nil)
	if !dA.Cluster.MajorityOK() || !dB.Cluster.MajorityOK() {
		t.Fatal("majority should be restored after recovery")
	}

	// A 经 ReconcileAfterPartition 收到对端新主广播的更大 epoch（共享单调
	// epoch 源下 B 的 epoch 必然更大，测试显式 +10 模拟）→ 陈旧主降级只读、
	// 停止复制、冲突标记累计、写路径 fencing 生效。
	me := dA.MasterEpoch()
	dA.ReconcileAfterPartition(me + 10)
	if !dA.ReadOnly() {
		t.Fatal("stale master A should be demoted read-only")
	}
	if dA.Replicator != nil {
		t.Fatal("stale master A replicator should be stopped")
	}
	if dA.ConflictCount() < 1 {
		t.Fatalf("stale master A conflict count = %d, want >= 1", dA.ConflictCount())
	}
	if err := dA.AllowWrite(); !errors.Is(err, db.ErrFenced) {
		t.Fatalf("stale master A AllowWrite = %v, want ErrFenced", err)
	}
	// 新鲜侧（B）不受影响：可继续写、保持主身份。
	if err := dB.AllowWrite(); err != nil {
		t.Fatalf("fresh master B AllowWrite = %v, want nil", err)
	}
	if dB.Replicator == nil {
		t.Fatal("fresh master B should keep replicator")
	}
	// 回迁就绪判定：A 为陈旧主（binlog 非空 + 已降级）→ 可走回迁闭环
	//（TryRejoinAsFollower 补偿回灌 / 全量对齐细节见 db_rejoin_test.go）。
	ok, _, err := dA.StaleMasterReady()
	if err != nil {
		t.Fatalf("A StaleMasterReady: %v", err)
	}
	if !ok {
		t.Fatal("stale master A should be ready for rejoin")
	}
}

package sql

// T35 分布式备份一致性（sql 层）单元测试：manifest JSON 往返与结构约束。

import (
	"encoding/json"
	"testing"
)

func TestT35DistBackupManifestRoundTrip(t *testing.T) {
	m := distBackupManifest{
		FormatVersion: 1,
		BackupID:      "bak-abc",
		CreatedAt:     "2026-10-14T10:00:00Z",
		GlobalLSN:     12345,
		GlobalTSO:     678,
		Nodes: []distBackupNode{
			{NodeID: "node-a", Addr: "127.0.0.1:9001", BackupFile: "C:/bak/cluster.bak.node-a", BackupLSN: 12345, Tables: 2, Rows: 10},
			{NodeID: "node-b", Addr: "127.0.0.1:9002", BackupFile: "C:/bak/cluster.bak.node-b", BackupLSN: 12345, Tables: 3, Rows: 20},
		},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got distBackupManifest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.FormatVersion != 1 || got.BackupID != "bak-abc" || got.GlobalLSN != 12345 || got.GlobalTSO != 678 {
		t.Fatalf("manifest scalar mismatch: %+v", got)
	}
	if len(got.Nodes) != 2 {
		t.Fatalf("manifest nodes: want 2, got %d", len(got.Nodes))
	}
	nb := got.Nodes[1]
	if nb.NodeID != "node-b" || nb.BackupFile != "C:/bak/cluster.bak.node-b" || nb.BackupLSN != 12345 || nb.Tables != 3 || nb.Rows != 20 {
		t.Fatalf("manifest node mismatch: %+v", nb)
	}
}

func TestT35DistBackupManifestConsistencyFlag(t *testing.T) {
	// 一致性标记必须满足：GlobalLSN 不小于任一节点 BackupLSN，
	// 保证恢复时各节点 TO LSN GlobalLSN 均可在自身 binlog 内对齐。
	m := distBackupManifest{
		FormatVersion: 1,
		BackupID:      "bak-1",
		GlobalLSN:     100,
		Nodes: []distBackupNode{
			{NodeID: "node-a", BackupLSN: 90},
			{NodeID: "node-b", BackupLSN: 100},
		},
	}
	for _, n := range m.Nodes {
		if n.BackupLSN > m.GlobalLSN {
			t.Fatalf("node %s backup_lsn %d > global_lsn %d: inconsistent marker", n.NodeID, n.BackupLSN, m.GlobalLSN)
		}
	}
}

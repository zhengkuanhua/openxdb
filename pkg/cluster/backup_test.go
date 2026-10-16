package cluster_test

// T35 分布式备份一致性（cluster 层）帧协议测试。
// 覆盖：BACKUP_PREPARE/DO、RESTORE_DO 载荷 JSON 往返、协调者 -> 节点回调
// 传递（候选点收集/备份执行/恢复执行）、Retry 提升传播、未装配回调回执错误。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
)

func TestT35BackupFramesJSONRoundTrip(t *testing.T) {
	prepReq := cluster.BackupPrepareReq{BackupID: "bak-1"}
	b, err := json.Marshal(prepReq)
	if err != nil {
		t.Fatalf("marshal prepare req: %v", err)
	}
	var prepReqGot cluster.BackupPrepareReq
	if err := json.Unmarshal(b, &prepReqGot); err != nil {
		t.Fatalf("unmarshal prepare req: %v", err)
	}
	if prepReqGot.BackupID != "bak-1" {
		t.Fatalf("prepare req round trip mismatch: %+v", prepReqGot)
	}

	prepResp := cluster.BackupPrepareResp{NodeID: "node-b", LastLSN: 100, TSO: 7}
	b, _ = json.Marshal(prepResp)
	var prepRespGot cluster.BackupPrepareResp
	if err := json.Unmarshal(b, &prepRespGot); err != nil {
		t.Fatalf("unmarshal prepare resp: %v", err)
	}
	if prepRespGot.NodeID != "node-b" || prepRespGot.LastLSN != 100 || prepRespGot.TSO != 7 {
		t.Fatalf("prepare resp round trip mismatch: %+v", prepRespGot)
	}

	doReq := cluster.BackupDoReq{BackupID: "bak-1", Path: "C:/bak/node-b.bak", GlobalLSN: 42}
	b, _ = json.Marshal(doReq)
	var doReqGot cluster.BackupDoReq
	if err := json.Unmarshal(b, &doReqGot); err != nil {
		t.Fatalf("unmarshal do req: %v", err)
	}
	if doReqGot.BackupID != "bak-1" || doReqGot.Path != "C:/bak/node-b.bak" || doReqGot.GlobalLSN != 42 {
		t.Fatalf("do req round trip mismatch: %+v", doReqGot)
	}

	doResp := cluster.BackupDoResp{BackupLSN: 42, Tables: 2, Rows: 7, Retry: true, LastLSN: 200}
	b, _ = json.Marshal(doResp)
	var doRespGot cluster.BackupDoResp
	if err := json.Unmarshal(b, &doRespGot); err != nil {
		t.Fatalf("unmarshal do resp: %v", err)
	}
	if doRespGot.BackupLSN != 42 || doRespGot.Tables != 2 || doRespGot.Rows != 7 || !doRespGot.Retry || doRespGot.LastLSN != 200 {
		t.Fatalf("do resp round trip mismatch: %+v", doRespGot)
	}

	restReq := cluster.RestoreDoReq{BackupID: "bak-1", Path: "C:/bak/node-b.bak", GlobalLSN: 42}
	b, _ = json.Marshal(restReq)
	var restReqGot cluster.RestoreDoReq
	if err := json.Unmarshal(b, &restReqGot); err != nil {
		t.Fatalf("unmarshal restore req: %v", err)
	}
	if restReqGot.BackupID != "bak-1" || restReqGot.Path != "C:/bak/node-b.bak" || restReqGot.GlobalLSN != 42 {
		t.Fatalf("restore req round trip mismatch: %+v", restReqGot)
	}

	restResp := cluster.RestoreDoResp{Tables: 2, Rows: 7, LSN: 42}
	b, _ = json.Marshal(restResp)
	var restRespGot cluster.RestoreDoResp
	if err := json.Unmarshal(b, &restRespGot); err != nil {
		t.Fatalf("unmarshal restore resp: %v", err)
	}
	if restRespGot.Tables != 2 || restRespGot.Rows != 7 || restRespGot.LSN != 42 {
		t.Fatalf("restore resp round trip mismatch: %+v", restRespGot)
	}
}

func TestT35ManagerPrepareBackupCollectsCandidate(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	_, srvB, _, _ := newTestNode(t, "node-b", nil)
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	var got cluster.BackupPrepareReq
	srvB.SetBackupPrepare(func(req cluster.BackupPrepareReq) (cluster.BackupPrepareResp, error) {
		got = req
		return cluster.BackupPrepareResp{NodeID: "node-b", LastLSN: 100, TSO: 7}, nil
	})
	resp, err := mgrA.PrepareBackup("node-b", cluster.BackupPrepareReq{BackupID: "bak-x"})
	if err != nil {
		t.Fatalf("PrepareBackup: %v", err)
	}
	if got.BackupID != "bak-x" {
		t.Fatalf("node received req mismatch: %+v", got)
	}
	if resp.NodeID != "node-b" || resp.LastLSN != 100 || resp.TSO != 7 || resp.Err != "" {
		t.Fatalf("PrepareBackup resp mismatch: %+v", resp)
	}
}

func TestT35ManagerBackupDoAppliesOnNode(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	_, srvB, _, _ := newTestNode(t, "node-b", nil)
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	var got cluster.BackupDoReq
	srvB.SetBackupDo(func(req cluster.BackupDoReq) (cluster.BackupDoResp, error) {
		got = req
		return cluster.BackupDoResp{BackupLSN: 42, Tables: 2, Rows: 7}, nil
	})
	resp, err := mgrA.DoBackup("node-b", cluster.BackupDoReq{BackupID: "bak-x", Path: "C:/bak/b.bak", GlobalLSN: 42})
	if err != nil {
		t.Fatalf("DoBackup: %v", err)
	}
	if got.GlobalLSN != 42 || got.Path != "C:/bak/b.bak" || got.BackupID != "bak-x" {
		t.Fatalf("node received req mismatch: %+v", got)
	}
	if resp.BackupLSN != 42 || resp.Tables != 2 || resp.Rows != 7 || resp.Retry || resp.Err != "" {
		t.Fatalf("DoBackup resp mismatch: %+v", resp)
	}
}

func TestT35ManagerBackupDoRetryPropagates(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	_, srvB, _, _ := newTestNode(t, "node-b", nil)
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	srvB.SetBackupDo(func(cluster.BackupDoReq) (cluster.BackupDoResp, error) {
		return cluster.BackupDoResp{Retry: true, LastLSN: 200}, nil
	})
	resp, err := mgrA.DoBackup("node-b", cluster.BackupDoReq{GlobalLSN: 100})
	if err != nil {
		t.Fatalf("DoBackup: %v", err)
	}
	if !resp.Retry || resp.LastLSN != 200 {
		t.Fatalf("retry should propagate: %+v", resp)
	}
}

func TestT35ManagerRestoreDoAppliesOnNode(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	_, srvB, _, _ := newTestNode(t, "node-b", nil)
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	var got cluster.RestoreDoReq
	srvB.SetRestoreDo(func(req cluster.RestoreDoReq) (cluster.RestoreDoResp, error) {
		got = req
		return cluster.RestoreDoResp{Tables: 2, Rows: 7, LSN: 42}, nil
	})
	resp, err := mgrA.DoRestore("node-b", cluster.RestoreDoReq{BackupID: "bak-x", Path: "C:/bak/b.bak", GlobalLSN: 42})
	if err != nil {
		t.Fatalf("DoRestore: %v", err)
	}
	if got.GlobalLSN != 42 || got.Path != "C:/bak/b.bak" || got.BackupID != "bak-x" {
		t.Fatalf("node received req mismatch: %+v", got)
	}
	if resp.Tables != 2 || resp.Rows != 7 || resp.LSN != 42 || resp.Err != "" {
		t.Fatalf("DoRestore resp mismatch: %+v", resp)
	}
}

func TestT35UnconfiguredCallbacksReturnErr(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	_, srvB, _, _ := newTestNode(t, "node-b", nil)
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	resp, err := mgrA.PrepareBackup("node-b", cluster.BackupPrepareReq{BackupID: "b"})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("prepare unconfigured: want not configured err, got %v", err)
	}
	_ = resp
	dresp, err := mgrA.DoBackup("node-b", cluster.BackupDoReq{BackupID: "b", Path: "C:/bak/b.bak", GlobalLSN: 1})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("do unconfigured: want not configured err, got %v", err)
	}
	_ = dresp
	rresp, err := mgrA.DoRestore("node-b", cluster.RestoreDoReq{BackupID: "b", Path: "C:/bak/b.bak", GlobalLSN: 1})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("restore unconfigured: want not configured err, got %v", err)
	}
	_ = rresp
}

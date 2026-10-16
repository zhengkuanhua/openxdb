package sql

// T35 分布式备份一致性：协调者流程 + 备份 manifest + 节点侧回调。
// 详见 docs/T35_m20_dist_backup_consistency.md。
//
// 设计要点：
//   - 仅在 e.mgr != nil 且存在远端节点（clusterEnabled）时触发；单机零回归；
//   - 备份强一致：全部注册节点必须在 PREPARE 阶段在线，离线/失败整体失败；
//   - 两阶段对齐：PREPARE 收集各节点 LastLSN -> G=max -> DO 以 G 落盘；
//     备份期间超过 G 的提交触发 retry（提升 G 重新 DO，最多 3 轮）；
//   - 恢复全有或全无：manifest 节点集合须全部在线且恢复成功，否则整体报错。

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/replication"
)

const (
	distBackupManifestVersion = 1
	distBackupMaxAlignRounds  = 3
	// distBackupRetryHint 用于区分"备份点超过对齐目标"与一般失败。
	distBackupRetryHint = "backup point exceeds global LSN, retry with higher LSN"
)

// distBackupNode manifest 中单个节点的备份条目。
type distBackupNode struct {
	NodeID     string `json:"node_id"`
	Addr       string `json:"addr"`
	BackupFile string `json:"backup_file"`
	BackupLSN  uint64 `json:"backup_lsn"`
	Tables     int    `json:"tables"`
	Rows       int    `json:"rows"`
}

// distBackupManifest 分布式备份一致性标记（协调者本地 <path>.manifest）。
// GlobalLSN 为全局一致性点：恢复时各节点 RESTORE ... TO LSN GlobalLSN 对齐。
type distBackupManifest struct {
	FormatVersion int              `json:"format_version"`
	BackupID      string           `json:"backup_id"`
	CreatedAt     string           `json:"created_at"`
	GlobalLSN     uint64           `json:"global_lsn"`
	GlobalTSO     uint64           `json:"global_tso"`
	Nodes         []distBackupNode `json:"nodes"`
}

// distBackupTarget 参与节点视图（本节点或远端）。
type distBackupTarget struct {
	NodeID string
	Addr   string
}

// newBackupID 生成备份批次标识（审计/测试用）。
func newBackupID() string {
	return fmt.Sprintf("bak-%d", time.Now().UnixNano())
}

// currentLSN 读取本地 binlog 当前最大位点（0 = 未启用）。
func (e *Executor) currentLSN() uint64 {
	if e.binlogPath == "" {
		return 0
	}
	bs, err := replication.OpenBinlog(e.binlogPath)
	if err != nil {
		return 0
	}
	defer bs.Close()
	return uint64(bs.LastLSN())
}

// distNodeSet 分布式备份参与节点：本节点 + 注册表中全部远端节点。
// 任一远端节点离线/不可达时返回错误（备份为全有或全无，不做降级）。
func (e *Executor) distNodeSet() ([]distBackupTarget, error) {
	var targets []distBackupTarget
	seen := map[string]bool{e.selfID: true}
	targets = append(targets, distBackupTarget{NodeID: e.selfID, Addr: e.localAddr()})
	for _, n := range e.mgr.ListNodes() {
		if seen[n.ID] {
			continue
		}
		seen[n.ID] = true
		if e.mgr.IsDown(n.ID) {
			return nil, &SQLError{Msg: fmt.Sprintf("backup failed: node %s is offline", n.ID)}
		}
		targets = append(targets, distBackupTarget{NodeID: n.ID, Addr: n.Addr})
	}
	return targets, nil
}

// localAddr 本节点地址（manifest 记录用）。
func (e *Executor) localAddr() string {
	if e.mgr == nil {
		return ""
	}
	return e.mgr.SelfNode().Addr
}

// distributedBackup BACKUP TO '<path>' 集群协调流程：
//   PREPARE（收集候选点）-> G=max -> DO 轮询（全部节点以 G 落盘，retry 提升 G）
//   -> 写 manifest。
func (e *Executor) distributedBackup(path string) (*Result, error) {
	targets, err := e.distNodeSet()
	if err != nil {
		return nil, err
	}
	backupID := newBackupID()
	// 节点备份文件路径：协调者本地 = path，远端 = path + ".node-" + nodeID。
	fileFor := func(nodeID string) string {
		if nodeID == e.selfID {
			return path
		}
		return path + ".node-" + nodeID
	}

	// 阶段一 PREPARE：收集全部节点候选点 (LastLSN, TSO)。
	type cand struct {
		lastLSN uint64
		tso     uint64
	}
	cands := make(map[string]cand, len(targets))
	var globalTSO uint64
	if e.ts != nil {
		if ts, terr := e.ts.Get(); terr == nil {
			globalTSO = ts
		}
	}
	for _, t := range targets {
		if t.NodeID == e.selfID {
			cands[t.NodeID] = cand{lastLSN: e.currentLSN(), tso: globalTSO}
			continue
		}
		resp, perr := e.mgr.PrepareBackup(t.NodeID, cluster.BackupPrepareReq{BackupID: backupID})
		if perr != nil {
			return nil, &SQLError{Msg: fmt.Sprintf("backup failed: prepare node %s: %v", t.NodeID, perr)}
		}
		if resp.Err != "" {
			return nil, &SQLError{Msg: fmt.Sprintf("backup failed: prepare node %s: %s", t.NodeID, resp.Err)}
		}
		cands[t.NodeID] = cand{lastLSN: resp.LastLSN, tso: resp.TSO}
	}

	// 全局一致性点 G = max(全部节点候选点)。
	globalLSN := uint64(0)
	for _, c := range cands {
		if c.lastLSN > globalLSN {
			globalLSN = c.lastLSN
		}
	}

	// 阶段二 DO 轮询：全部节点以 G 落盘；任一节点 retry 则提升 G 重来。
	outs := make(map[string]*backupOutcome, len(targets))
	for round := 1; round <= distBackupMaxAlignRounds; round++ {
		retry := false
		newG := globalLSN
		for _, t := range targets {
			// 本地节点：先检查当前位点，超过 G 则直接提升（下一轮重写，原子替换）。
			if t.NodeID == e.selfID {
				if cur := e.currentLSN(); cur > globalLSN {
					retry = true
					if cur > newG {
						newG = cur
					}
					continue
				}
				out, berr := e.backupCore(fileFor(t.NodeID), e.selfID, backupID, globalLSN)
				if berr != nil {
					return nil, &SQLError{Msg: fmt.Sprintf("backup failed: do node %s: %v", t.NodeID, berr)}
				}
				if out.BackupLSN > globalLSN {
					retry = true
					if out.BackupLSN > newG {
						newG = out.BackupLSN
					}
					continue
				}
				outs[t.NodeID] = out
				continue
			}
			// 远端节点：Retry 表示其当前/备份点已超过 G，提升 G 重新对齐。
			resp, derr := e.mgr.DoBackup(t.NodeID, cluster.BackupDoReq{
				BackupID:  backupID,
				Path:      fileFor(t.NodeID),
				GlobalLSN: globalLSN,
			})
			if derr != nil {
				return nil, &SQLError{Msg: fmt.Sprintf("backup failed: do node %s: %v", t.NodeID, derr)}
			}
			if resp.Err != "" {
				return nil, &SQLError{Msg: fmt.Sprintf("backup failed: do node %s: %s", t.NodeID, resp.Err)}
			}
			if resp.Retry {
				retry = true
				if resp.LastLSN > newG {
					newG = resp.LastLSN
				}
				continue
			}
			outs[t.NodeID] = &backupOutcome{BackupLSN: resp.BackupLSN, Tables: resp.Tables, Rows: resp.Rows}
		}
		if !retry {
			break
		}
		globalLSN = newG
		if round == distBackupMaxAlignRounds {
			return nil, &SQLError{Msg: fmt.Sprintf(
				"backup failed: cannot converge global LSN after %d rounds (write load too high), retry later", distBackupMaxAlignRounds)}
		}
	}

	// 阶段三：写 manifest（原子替换），作为一致性标记与恢复驱动清单。
	manifest := distBackupManifest{
		FormatVersion: distBackupManifestVersion,
		BackupID:      backupID,
		CreatedAt:     timeNow(),
		GlobalLSN:     globalLSN,
		GlobalTSO:     globalTSO,
		Nodes:         make([]distBackupNode, 0, len(targets)),
	}
	totalTables, totalRows := 0, 0
	for _, t := range targets {
		out := outs[t.NodeID]
		manifest.Nodes = append(manifest.Nodes, distBackupNode{
			NodeID:     t.NodeID,
			Addr:       t.Addr,
			BackupFile: fileFor(t.NodeID),
			BackupLSN:  out.BackupLSN,
			Tables:     out.Tables,
			Rows:       out.Rows,
		})
		totalTables += out.Tables
		totalRows += out.Rows
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, &SQLError{Msg: "backup failed: encode manifest: " + err.Error()}
	}
	tmpPath := path + ".manifest.tmp"
	if err := os.WriteFile(tmpPath, raw, 0o644); err != nil {
		return nil, &SQLError{Msg: "backup failed: write manifest: " + err.Error()}
	}
	if _, err := os.Stat(path + ".manifest"); err == nil {
		if err := os.Remove(path + ".manifest"); err != nil {
			return nil, &SQLError{Msg: "backup failed: replace manifest: " + err.Error()}
		}
	}
	if err := os.Rename(tmpPath, path+".manifest"); err != nil {
		return nil, &SQLError{Msg: "backup failed: finalize manifest: " + err.Error()}
	}

	return &Result{
		Columns:      []string{"path", "nodes", "tables", "rows", "lsn"},
		AffectedRows: totalRows,
		Rows:         [][]Value{{StrVal(path + ".manifest"), IntVal(int64(len(targets))), IntVal(int64(totalTables)), IntVal(int64(totalRows)), IntVal(int64(globalLSN))}},
	}, nil
}

// distributedRestore RESTORE FROM '<path>' 集群协调流程：
//   校验 manifest -> 校验节点覆盖与在线 -> 各节点 restoreCore(path_i, G, true) 对齐。
// 全有或全无：任一失败整体报错；节点侧恢复事务性、幂等，重试收敛。
func (e *Executor) distributedRestore(path string) (*Result, error) {
	raw, err := os.ReadFile(path + ".manifest")
	if err != nil {
		return nil, &SQLError{Msg: "restore failed: read manifest: " + err.Error()}
	}
	var mf distBackupManifest
	if err := json.Unmarshal(raw, &mf); err != nil {
		return nil, &SQLError{Msg: "restore failed: decode manifest: " + err.Error()}
	}
	if mf.FormatVersion != distBackupManifestVersion {
		return nil, &SQLError{Msg: "restore failed: unsupported manifest version"}
	}
	if len(mf.Nodes) == 0 {
		return nil, &SQLError{Msg: "restore failed: empty manifest node list"}
	}
	// 校验节点覆盖：manifest 每个节点必须存在且在线。
	registered := map[string]bool{e.selfID: true}
	for _, n := range e.mgr.ListNodes() {
		registered[n.ID] = true
	}
	for _, mn := range mf.Nodes {
		if !registered[mn.NodeID] {
			return nil, &SQLError{Msg: fmt.Sprintf("restore failed: manifest node %s not registered", mn.NodeID)}
		}
		if mn.NodeID != e.selfID && e.mgr.IsDown(mn.NodeID) {
			return nil, &SQLError{Msg: fmt.Sprintf("restore failed: manifest node %s is offline", mn.NodeID)}
		}
	}

	// 各节点恢复到全局 LSN（PITR 补齐本地 binlog 到 G）。
	totalTables, totalRows := 0, 0
	for _, mn := range mf.Nodes {
		if mn.NodeID == e.selfID {
			out, rerr := e.restoreCore(mn.BackupFile, mf.GlobalLSN, true)
			if rerr != nil {
				return nil, &SQLError{Msg: fmt.Sprintf("restore failed: local node: %v", rerr)}
			}
			totalTables += out.Tables
			totalRows += out.Rows
			continue
		}
		resp, rerr := e.mgr.DoRestore(mn.NodeID, cluster.RestoreDoReq{
			BackupID:  mf.BackupID,
			Path:      mn.BackupFile,
			GlobalLSN: mf.GlobalLSN,
		})
		if rerr != nil {
			return nil, &SQLError{Msg: fmt.Sprintf("restore failed: node %s: %v", mn.NodeID, rerr)}
		}
		if resp.Err != "" {
			return nil, &SQLError{Msg: fmt.Sprintf("restore failed: node %s: %s", mn.NodeID, resp.Err)}
		}
		totalTables += resp.Tables
		totalRows += resp.Rows
	}

	return &Result{
		Columns:      []string{"nodes", "tables", "rows", "lsn"},
		AffectedRows: totalTables,
		Rows:         [][]Value{{IntVal(int64(len(mf.Nodes))), IntVal(int64(totalTables)), IntVal(int64(totalRows)), IntVal(int64(mf.GlobalLSN))}},
	}, nil
}

// ---- 节点侧回调（由 db.StartCluster 装配到 cluster.Server）----

// RemoteBackupPrepare 节点侧：返回本节点备份候选点（LastLSN + TSO）。
func (e *Executor) RemoteBackupPrepare(req cluster.BackupPrepareReq) (cluster.BackupPrepareResp, error) {
	resp := cluster.BackupPrepareResp{NodeID: e.selfID, LastLSN: e.currentLSN()}
	if e.ts != nil {
		if ts, terr := e.ts.Get(); terr == nil {
			resp.TSO = ts
		}
	}
	return resp, nil
}

// RemoteBackupDo 节点侧：以 GlobalLSN 为对齐目标执行本地备份。
// 当前位点已超过 GlobalLSN（协调者 PREPARE 之后本节点又有提交）时直接返回
// Retry（不写文件），由协调者提升 G 重新对齐。
func (e *Executor) RemoteBackupDo(req cluster.BackupDoReq) (cluster.BackupDoResp, error) {
	if cur := e.currentLSN(); cur > req.GlobalLSN {
		return cluster.BackupDoResp{Retry: true, LastLSN: cur}, nil
	}
	out, err := e.backupCore(req.Path, e.selfID, req.BackupID, req.GlobalLSN)
	if err != nil {
		return cluster.BackupDoResp{}, err
	}
	if out.BackupLSN > req.GlobalLSN {
		return cluster.BackupDoResp{Retry: true, LastLSN: out.BackupLSN}, nil
	}
	return cluster.BackupDoResp{BackupLSN: out.BackupLSN, Tables: out.Tables, Rows: out.Rows}, nil
}

// RemoteRestoreDo 节点侧：恢复本节点备份文件并 PITR 对齐到 GlobalLSN。
func (e *Executor) RemoteRestoreDo(req cluster.RestoreDoReq) (cluster.RestoreDoResp, error) {
	out, err := e.restoreCore(req.Path, req.GlobalLSN, true)
	if err != nil {
		return cluster.RestoreDoResp{}, err
	}
	return cluster.RestoreDoResp{Tables: out.Tables, Rows: out.Rows, LSN: out.LSN}, nil
}

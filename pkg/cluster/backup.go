package cluster

// T35 分布式备份一致性协议。
//
// 帧（在 T34 DDL 之后扩展）：
//
//	40 BACKUP_PREPARE -> 41 BACKUP_PREPARE_OK
//	42 BACKUP_DO      -> 43 BACKUP_DO_OK
//	44 RESTORE_DO     -> 45 RESTORE_DO_OK
//
// 协调者在一次 BACKUP TO 中：PREPARE 阶段向全部注册节点收集 (LastLSN, TSO)，
// 取 max 为全局一致性点 G；DO 阶段要求各节点以 G 写入备份文件（备份点超过 G 时
// 回 Retry 提升 G 重新对齐）；RESTORE 阶段按 manifest 驱动各节点
// RESTORE ... TO LSN G。载荷不引用 sql 类型，避免 cluster 与 sql 包循环依赖。
// 详见 docs/T35_m20_dist_backup_consistency.md。

import (
	"errors"
	"net"
)

const (
	msgBackupPrepare   byte = 40
	msgBackupPrepareOK byte = 41
	msgBackupDo        byte = 42
	msgBackupDoOK      byte = 43
	msgRestoreDo       byte = 44
	msgRestoreDoOK     byte = 45
)

// BackupPrepareReq BACKUP_PREPARE 载荷：请求节点返回备份候选点。
type BackupPrepareReq struct {
	BackupID string `json:"backup_id"`
}

// BackupPrepareResp BACKUP_PREPARE_OK 载荷：节点本地备份候选点。
// LastLSN 为本地 binlog 最大位点，TSO 为当前全局时间戳（快照版本参考）。
type BackupPrepareResp struct {
	NodeID  string `json:"node_id"`
	LastLSN uint64 `json:"last_lsn"`
	TSO     uint64 `json:"tso"`
	Err     string `json:"err,omitempty"`
}

// BackupDoReq BACKUP_DO 载荷：协调者下发执行备份。
// GlobalLSN 为当前对齐目标（全局一致性点）；Path 为节点本地备份文件路径。
type BackupDoReq struct {
	BackupID  string `json:"backup_id"`
	Path      string `json:"path"`
	GlobalLSN uint64 `json:"global_lsn"`
}

// BackupDoResp BACKUP_DO_OK 载荷。
// Retry=true 表示节点当前/实际备份点已超过 GlobalLSN（PREPARE 之后又有提交），
// 协调者须提升 GlobalLSN 并让全部节点重新 DO（原子覆盖旧文件）。
type BackupDoResp struct {
	BackupLSN uint64 `json:"backup_lsn"`
	Tables    int    `json:"tables"`
	Rows      int    `json:"rows"`
	Retry     bool   `json:"retry,omitempty"`
	LastLSN   uint64 `json:"last_lsn,omitempty"`
	Err       string `json:"err,omitempty"`
}

// RestoreDoReq RESTORE_DO 载荷：协调者下发执行恢复（RESTORE ... TO LSN GlobalLSN）。
type RestoreDoReq struct {
	BackupID  string `json:"backup_id"`
	Path      string `json:"path"`
	GlobalLSN uint64 `json:"global_lsn"`
}

// RestoreDoResp RESTORE_DO_OK 载荷：LSN 为恢复后最终位点（= GlobalLSN）。
type RestoreDoResp struct {
	Tables int    `json:"tables"`
	Rows   int    `json:"rows"`
	LSN    uint64 `json:"lsn"`
	Err    string `json:"err,omitempty"`
}

// SetBackupPrepare 注入节点侧备份候选点回调（由 db 装配 sql.Executor 的方法）。
func (s *Server) SetBackupPrepare(fn func(req BackupPrepareReq) (BackupPrepareResp, error)) {
	s.backupPrepare = fn
}

// SetBackupDo 注入节点侧执行备份回调（由 db 装配 sql.Executor 的方法）。
func (s *Server) SetBackupDo(fn func(req BackupDoReq) (BackupDoResp, error)) {
	s.backupDo = fn
}

// SetRestoreDo 注入节点侧执行恢复回调（由 db 装配 sql.Executor 的方法）。
func (s *Server) SetRestoreDo(fn func(req RestoreDoReq) (RestoreDoResp, error)) {
	s.restoreDo = fn
}

// handleBackupPrepare 处理 BACKUP_PREPARE：调用注入的候选点回调，回执 BACKUP_PREPARE_OK。
func (s *Server) handleBackupPrepare(conn net.Conn, payload []byte) {
	var req BackupPrepareReq
	resp := BackupPrepareResp{}
	if err := unmarshalJSON(payload, &req); err != nil {
		resp.Err = "cluster: bad backup prepare payload: " + err.Error()
		mustWriteJSON(conn, msgBackupPrepareOK, resp)
		return
	}
	if s.backupPrepare == nil {
		resp.Err = "cluster: backup prepare not configured"
		mustWriteJSON(conn, msgBackupPrepareOK, resp)
		return
	}
	r, err := s.backupPrepare(req)
	if err != nil {
		resp.Err = "cluster: backup prepare failed: " + err.Error()
		mustWriteJSON(conn, msgBackupPrepareOK, resp)
		return
	}
	resp = r
	mustWriteJSON(conn, msgBackupPrepareOK, resp)
}

// handleBackupDo 处理 BACKUP_DO：调用注入的执行备份回调，回执 BACKUP_DO_OK。
func (s *Server) handleBackupDo(conn net.Conn, payload []byte) {
	var req BackupDoReq
	resp := BackupDoResp{}
	if err := unmarshalJSON(payload, &req); err != nil {
		resp.Err = "cluster: bad backup do payload: " + err.Error()
		mustWriteJSON(conn, msgBackupDoOK, resp)
		return
	}
	if s.backupDo == nil {
		resp.Err = "cluster: backup do not configured"
		mustWriteJSON(conn, msgBackupDoOK, resp)
		return
	}
	r, err := s.backupDo(req)
	if err != nil {
		resp.Err = "cluster: backup do failed: " + err.Error()
		mustWriteJSON(conn, msgBackupDoOK, resp)
		return
	}
	resp = r
	mustWriteJSON(conn, msgBackupDoOK, resp)
}

// handleRestoreDo 处理 RESTORE_DO：调用注入的执行恢复回调，回执 RESTORE_DO_OK。
func (s *Server) handleRestoreDo(conn net.Conn, payload []byte) {
	var req RestoreDoReq
	resp := RestoreDoResp{}
	if err := unmarshalJSON(payload, &req); err != nil {
		resp.Err = "cluster: bad restore do payload: " + err.Error()
		mustWriteJSON(conn, msgRestoreDoOK, resp)
		return
	}
	if s.restoreDo == nil {
		resp.Err = "cluster: restore do not configured"
		mustWriteJSON(conn, msgRestoreDoOK, resp)
		return
	}
	r, err := s.restoreDo(req)
	if err != nil {
		resp.Err = "cluster: restore do failed: " + err.Error()
		mustWriteJSON(conn, msgRestoreDoOK, resp)
		return
	}
	resp = r
	mustWriteJSON(conn, msgRestoreDoOK, resp)
}

// PrepareBackup 向目标节点请求备份候选点（协调者 -> 数据节点）。
func (m *Manager) PrepareBackup(nodeID string, req BackupPrepareReq) (BackupPrepareResp, error) {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return BackupPrepareResp{}, err
	}
	return m.clientFor(n.ID).prepareBackup(req)
}

// DoBackup 向目标节点下发执行备份。
func (m *Manager) DoBackup(nodeID string, req BackupDoReq) (BackupDoResp, error) {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return BackupDoResp{}, err
	}
	return m.clientFor(n.ID).doBackup(req)
}

// DoRestore 向目标节点下发执行恢复。
func (m *Manager) DoRestore(nodeID string, req RestoreDoReq) (RestoreDoResp, error) {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return RestoreDoResp{}, err
	}
	return m.clientFor(n.ID).doRestore(req)
}

// prepareBackup 短连接发送 BACKUP_PREPARE 并读取 BACKUP_PREPARE_OK。
func (c *Client) prepareBackup(req BackupPrepareReq) (BackupPrepareResp, error) {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return BackupPrepareResp{}, err
	}
	conn, err := dial(addr)
	if err != nil {
		return BackupPrepareResp{}, err
	}
	defer conn.Close()
	payload, err := marshalJSON(req)
	if err != nil {
		return BackupPrepareResp{}, err
	}
	if err := writeFrame(conn, msgBackupPrepare, payload); err != nil {
		return BackupPrepareResp{}, err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return BackupPrepareResp{}, err
	}
	if typ != msgBackupPrepareOK {
		return BackupPrepareResp{}, errors.New("cluster: unexpected backup prepare reply")
	}
	var resp BackupPrepareResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return BackupPrepareResp{}, err
	}
	if resp.Err != "" {
		return resp, errors.New("cluster: backup prepare failed: " + resp.Err)
	}
	return resp, nil
}

// doBackup 短连接发送 BACKUP_DO 并读取 BACKUP_DO_OK。
func (c *Client) doBackup(req BackupDoReq) (BackupDoResp, error) {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return BackupDoResp{}, err
	}
	conn, err := dial(addr)
	if err != nil {
		return BackupDoResp{}, err
	}
	defer conn.Close()
	payload, err := marshalJSON(req)
	if err != nil {
		return BackupDoResp{}, err
	}
	if err := writeFrame(conn, msgBackupDo, payload); err != nil {
		return BackupDoResp{}, err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return BackupDoResp{}, err
	}
	if typ != msgBackupDoOK {
		return BackupDoResp{}, errors.New("cluster: unexpected backup do reply")
	}
	var resp BackupDoResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return BackupDoResp{}, err
	}
	if resp.Err != "" {
		return resp, errors.New("cluster: backup do failed: " + resp.Err)
	}
	return resp, nil
}

// doRestore 短连接发送 RESTORE_DO 并读取 RESTORE_DO_OK。
func (c *Client) doRestore(req RestoreDoReq) (RestoreDoResp, error) {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return RestoreDoResp{}, err
	}
	conn, err := dial(addr)
	if err != nil {
		return RestoreDoResp{}, err
	}
	defer conn.Close()
	payload, err := marshalJSON(req)
	if err != nil {
		return RestoreDoResp{}, err
	}
	if err := writeFrame(conn, msgRestoreDo, payload); err != nil {
		return RestoreDoResp{}, err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return RestoreDoResp{}, err
	}
	if typ != msgRestoreDoOK {
		return RestoreDoResp{}, errors.New("cluster: unexpected restore do reply")
	}
	var resp RestoreDoResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return RestoreDoResp{}, err
	}
	if resp.Err != "" {
		return resp, errors.New("cluster: restore do failed: " + resp.Err)
	}
	return resp, nil
}

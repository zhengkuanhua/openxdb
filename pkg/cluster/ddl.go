package cluster

// T34 跨节点 DDL 元数据广播协议。
//
// 帧（在 M4/2PC/TSO 之后扩展）：
//
//	30 DDL_PUSH    -> 31 DDL_PUSH_OK
//
// 协调者在 DDL 本地提交成功后，把表目录变更（按表粒度 upsert/delete）广播到
// 全部在线注册节点；载荷不引用 sql 类型（Meta 为 TableMeta 的 JSON 字节），
// 避免 cluster 与 sql 包循环依赖。节点侧 apply 由注入回调（SetDDLApply）执行，
// 应用失败回 Err，协调者据此记录 pending 待重放。详见 docs/T34_m20_cross_node_ddl.md。

import (
	"errors"
	"net"
)

const (
	msgDDLPush    byte = 30
	msgDDLPushOK  byte = 31
)

// DDL 目录操作类型（Op 字段取值）。
const (
	DDLOpCreate = "create" // CREATE TABLE：目标节点无该表则追加，有则覆盖（幂等）
	DDLOpAlter  = "alter"  // ALTER TABLE：按 TableID 覆盖最新表条目
	DDLOpDrop   = "drop"   // DROP TABLE：按 TableID 移除表条目
)

// DDLPushReq DDL_PUSH 载荷。
// DDLID 为协调者生成的广播标识（审计/测试用，幂等不依赖它）。
// Meta 为 TableMeta 的 JSON 序列化字节（Op=create/alter 时携带，JSON 帧自动 base64）；
// Op=drop 时 Meta 为空，以 TableID+Name 定位删除目标。
type DDLPushReq struct {
	DDLID   string `json:"ddl_id"`
	Op      string `json:"op"`
	TableID uint64 `json:"table_id"`
	Name    string `json:"name"`
	Meta    []byte `json:"meta,omitempty"`
}

// DDLPushResp DDL_PUSH_OK 载荷。
type DDLPushResp struct {
	Err  string `json:"err,omitempty"`
	Done bool   `json:"done"`
}

// SetDDLApply 注入节点侧 DDL 元数据 apply 回调（由 db 装配 sql.Executor 的方法）。
// 回调负责：读取本地表目录 -> 按 TableID upsert/delete -> 独立事务写回。
func (s *Server) SetDDLApply(fn func(req DDLPushReq) error) {
	s.ddlApply = fn
}

// handleDDLPush 处理 DDL_PUSH：调用注入的 apply 回调，回执 DDL_PUSH_OK。
// 无注入回调时返回错误（未装配集群 DDL 能力的节点不接收目录变更）。
func (s *Server) handleDDLPush(conn net.Conn, payload []byte) {
	var req DDLPushReq
	resp := DDLPushResp{}
	if err := unmarshalJSON(payload, &req); err != nil {
		resp.Err = "cluster: bad ddl push payload: " + err.Error()
		mustWriteJSON(conn, msgDDLPushOK, resp)
		return
	}
	if s.ddlApply == nil {
		resp.Err = "cluster: ddl apply not configured"
		mustWriteJSON(conn, msgDDLPushOK, resp)
		return
	}
	if err := s.ddlApply(req); err != nil {
		resp.Err = "cluster: ddl apply failed: " + err.Error()
		mustWriteJSON(conn, msgDDLPushOK, resp)
		return
	}
	resp.Done = true
	mustWriteJSON(conn, msgDDLPushOK, resp)
}

// PushDDL 将 DDL 目录变更推送到目标节点（协调者 -> 数据节点）。
func (m *Manager) PushDDL(nodeID string, req DDLPushReq) error {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return err
	}
	return m.clientFor(n.ID).pushDDL(req)
}

// pushDDL 短连接发送 DDL_PUSH 并读取 DDL_PUSH_OK。
func (c *Client) pushDDL(req DDLPushReq) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	payload, err := marshalJSON(req)
	if err != nil {
		return err
	}
	if err := writeFrame(conn, msgDDLPush, payload); err != nil {
		return err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return err
	}
	if typ != msgDDLPushOK {
		return errors.New("cluster: unexpected ddl push reply")
	}
	var resp DDLPushResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return err
	}
	if resp.Err != "" {
		return errors.New("cluster: ddl push failed: " + resp.Err)
	}
	return nil
}

// mustWriteJSON 写 JSON 载荷帧（handle 层错误已由上层记录，此处不回传）。
func mustWriteJSON(conn net.Conn, typ byte, v interface{}) {
	payload, err := marshalJSON(v)
	if err != nil {
		return
	}
	_ = writeFrame(conn, typ, payload)
}

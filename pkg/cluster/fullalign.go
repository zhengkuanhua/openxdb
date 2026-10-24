package cluster

// v1.7.0 全量对齐兜底协议（回迁降级路径）：
// 旧主本地 binlog 损坏或补偿失败时，请求新主把全部 region 当前行推送给自己
// （新主经 SQL 层 pushAllRegionsToNode 逐 region REGION_PUSH，本机 applyPush
// 覆盖落本地），作为回迁阶段二补偿机制的兜底对齐通道。
//
// 帧：FULLALIGN_REQ(type 60) -> FULLALIGN_OK(type 61)。
// 载荷：FullAlignReq{DestNodeID}（请求方要求目标节点把全部 region 推给该节点，
// 回迁场景 DestNodeID 即请求方自身 NodeID）。
// 目标节点收到后执行全量推送（逐 region 扫描数据 + 权威路由视图 REGION_PUSH），
// 全部成功才回 FULLALIGN_OK{Err:""}；任一 region 推送失败（如对端不可达）
// 即回错误并中断，请求方据此返回 ErrFullAlignFailed。

import (
	"errors"
	"net"
)

const (
	msgFullAlignReq byte = 60
	msgFullAlignOK  byte = 61
)

// FullAlignReq FULLALIGN_REQ 载荷：请求目标节点执行全量对齐推送。
type FullAlignReq struct {
	// DestNodeID 推送目的地（被覆盖对齐的节点 NodeID）。
	DestNodeID string `json:"dest_node_id,omitempty"`
}

// FullAlignResp FULLALIGN_OK 载荷。
type FullAlignResp struct {
	Err  string `json:"err,omitempty"`
	Rows int    `json:"rows"`
}

// RequestFullAlign 请求 target 节点把其全部 region 当前行 REGION_PUSH 推送到
// destNodeID（回迁兜底：新主 → 旧主）。同步等待目标节点全量推送完成；
// 目标节点不可达或任一 region 推送失败返回错误。
func (m *Manager) RequestFullAlign(target, destNodeID string) error {
	n, err := m.ensureRemote(target)
	if err != nil {
		return err
	}
	return m.clientFor(n.ID).requestFullAlign(destNodeID)
}

// requestFullAlign 短连接发送 FULLALIGN_REQ 并读取 FULLALIGN_OK。
func (c *Client) requestFullAlign(destNodeID string) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	payload, err := marshalJSON(FullAlignReq{DestNodeID: destNodeID})
	if err != nil {
		return err
	}
	if err := writeFrame(conn, msgFullAlignReq, payload); err != nil {
		return err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return err
	}
	if typ != msgFullAlignOK {
		return errors.New("cluster: unexpected full align reply")
	}
	var resp FullAlignResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return err
	}
	if resp.Err != "" {
		return errors.New("cluster: full align failed: " + resp.Err)
	}
	return nil
}

// handleFullAlign 处理 FULLALIGN_REQ：执行全量对齐推送（目标节点把全部 region
// 当前行推送给请求方），全部成功回 FULLALIGN_OK{Err:""}；任一 region 推送失败
// 即回错误，请求方返回 ErrFullAlignFailed。
func (s *Server) handleFullAlign(conn net.Conn, payload []byte) {
	var req FullAlignReq
	if err := unmarshalJSON(payload, &req); err != nil {
		writeFrame(conn, msgFullAlignOK, mustJSON(FullAlignResp{Err: "bad request"}))
		return
	}
	if req.DestNodeID == "" {
		writeFrame(conn, msgFullAlignOK, mustJSON(FullAlignResp{Err: "empty dest node id"}))
		return
	}
	if s.fullAlignFn == nil {
		writeFrame(conn, msgFullAlignOK, mustJSON(FullAlignResp{Err: "full align handler not installed"}))
		return
	}
	if err := s.fullAlignFn(req.DestNodeID); err != nil {
		writeFrame(conn, msgFullAlignOK, mustJSON(FullAlignResp{Err: err.Error()}))
		return
	}
	writeFrame(conn, msgFullAlignOK, mustJSON(FullAlignResp{}))
}

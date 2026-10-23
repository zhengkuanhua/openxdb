package cluster

// P0 HA 脑裂防护：clusterEpoch 集群广播（防双主 fencing 通知）。
//
// 升主节点授权新 epoch 后向全部已注册远端广播 EPOCH_NOTIFY；接收端
// （db.StartCluster 装配 epochNotify 回调）更新本节点已知最大 epoch，
// 据此 fencing 陈旧主写入（本节点 epoch > masterEpoch 时写路径被拒）。
// 广播为单向尽力而为（失败不阻塞升主；fencing 依心跳/读超时路径兜底）。

import (
	"net"
)

// msgEpochNotify EPOCH_NOTIFY 帧类型（50，避开 M2/M4 既有帧号 1-6、16-29、40-41）。
const msgEpochNotify byte = 50

// EpochNotify EPOCH_NOTIFY 载荷：声明全局最大 clusterEpoch。
type EpochNotify struct {
	SrcID string `json:"src_id,omitempty"`
	Epoch uint64 `json:"epoch"`
}

// epochNotify 接收回调（db.StartCluster 装配 db.DB.NoteEpoch；nil = 忽略）。

// SetEpochNotify 装配 epoch 广播接收回调（P0 HA；db.StartCluster 调用，
// nil 表示不启用——旧节点零回归）。
func (s *Server) SetEpochNotify(fn func(epoch uint64)) { s.epochNotify = fn }

// handleEpoch 处理 EPOCH_NOTIFY：更新本节点已知最大 epoch（fencing 依据）。
// 载荷非法 / epoch=0 忽略。
func (s *Server) handleEpoch(conn net.Conn, payload []byte) {
	var req EpochNotify
	if err := unmarshalJSON(payload, &req); err != nil {
		return
	}
	if req.Epoch == 0 {
		return
	}
	if s.epochNotify != nil {
		s.epochNotify(req.Epoch)
	}
}

// BroadcastEpoch 将新 clusterEpoch 广播到所有已注册远端（不含本节点），
// 尽力而为：任一发送失败只记录首个错误，不阻断升主。
func (m *Manager) BroadcastEpoch(e uint64) error {
	var firstErr error
	for _, n := range m.ListNodes() {
		if n.ID == m.SelfID {
			continue
		}
		if err := m.clientFor(n.ID).sendEpochNotify(e); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// sendEpochNotify 向单个远端发送 EPOCH_NOTIFY（短连接：拨号-发帧-关闭）。
func (c *Client) sendEpochNotify(e uint64) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	payload, err := marshalJSON(EpochNotify{SrcID: c.mgr.SelfID, Epoch: e})
	if err != nil {
		return err
	}
	return writeFrame(conn, msgEpochNotify, payload)
}

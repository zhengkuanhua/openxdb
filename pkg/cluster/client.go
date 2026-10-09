package cluster

// 查询转发客户端：向归属节点发送 QUERY_REQ（物理键区间）并取回 QUERY_RESP。
// 第一簇采用短连接语义：每次查询新建 TCP 连接、请求-响应即断开，
// 可靠优先；长连接复用 / 并发多路复用留待下簇优化（文档已说明）。

import (
	"errors"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// Client 到单个远端节点的转发客户端。
type Client struct {
	nodeID string
	mgr    *Manager
}

// Scan 转发区间扫描 [start, end)，返回远端行（原样字节）。
func (c *Client) Scan(start, end []byte, limit int) ([]QueryRow, error) {
	return c.roundTrip(QueryReq{Op: OpScan, Start: start, End: end, Limit: limit})
}

// Get 转发点查；未命中返回 storage.ErrNotFound。
func (c *Client) Get(key []byte) ([]byte, error) {
	rows, err := c.roundTrip(QueryReq{Op: OpGet, Start: key})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, storage.ErrNotFound
	}
	return rows[0].Value, nil
}

// roundTrip 向目标节点发送一次查询并读取响应。
func (c *Client) roundTrip(req QueryReq) ([]QueryRow, error) {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return nil, err
	}
	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	payload, err := marshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := writeFrame(conn, msgQueryReq, payload); err != nil {
		return nil, err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return nil, err
	}
	if typ != msgQueryResp {
		return nil, errors.New("cluster: unexpected query reply")
	}
	var resp QueryResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return nil, err
	}
	if resp.Err != "" {
		if resp.Err == remoteNotFound {
			return nil, storage.ErrNotFound
		}
		return nil, errors.New("cluster: remote error: " + resp.Err)
	}
	if resp.Row != nil {
		return []QueryRow{*resp.Row}, nil
	}
	return resp.Rows, nil
}

// close 保留接口（Manager.Stop 调用；短连接实现下为空操作）。
func (c *Client) close() {}

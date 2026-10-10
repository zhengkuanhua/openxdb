package cluster

// M8 全局时间戳（TSO）网络组件：
//   - Server 侧：挂载 TSO_REQ/TSO_RESP 帧，用本节点本地发号器批量响应；
//   - Client 侧：实现 tso.Source 接口，向远端 TSO 节点批量拉号并本地缓存，
//     每批一次 RTT 后逐号消费，降低协调开销。
//
// 中心化部署：将集群中所有节点的 TSO 客户端指向同一节点（见 db.SetTSONode），
// 该节点成为全局单调发号者；未配置 / 单机模式退化为本地发号（零回归）。

import (
	"errors"
	"net"
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/tso"
)

// TSO 网络帧类型（M4 节点链路 16-19 之后，M8 追加 28/29）。
const (
	msgTsoReq  byte = 28
	msgTsoResp byte = 29
)

// TsoReq TSO_REQ 载荷：请求发放 n 个连续时间戳。
type TsoReq struct {
	Batch uint64 `json:"batch,omitempty"`
}

// TsoResp TSO_RESP 载荷：成功返回 [Start, Start+Count) 连续区间。
type TsoResp struct {
	Start uint64 `json:"start,omitempty"`
	Count uint64 `json:"count,omitempty"`
	Err   string `json:"err,omitempty"`
}

// tsoClient 远程 TSO 客户端：实现 tso.Source，带批量缓存。
type tsoClient struct {
	nodeID string
	mgr    *Manager
	batch  uint64

	mu    sync.Mutex
	base  uint64 // 缓存批次起点（含）
	count uint64 // 缓存批次剩余数量
}

// NewTSOClient 创建指向 nodeID 节点的远程 TSO 客户端（batch<=0 时默认 64）。
func NewTSOClient(mgr *Manager, nodeID string, batch uint64) tso.Source {
	if batch == 0 {
		batch = 64
	}
	return &tsoClient{nodeID: nodeID, mgr: mgr, batch: batch}
}

// Get 返回远端 TSO 发放的全局时间戳；缓存批次耗尽时发起批量请求。
// 事务边界（BEGIN/COMMIT/ROLLBACK）由上层调用 Reset 丢弃残余缓存，
// 保证新事务取到权威节点当前序列的新鲜号。
func (c *tsoClient) Get() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.count == 0 {
		start, n, err := c.fetchBatch()
		if err != nil {
			return 0, err
		}
		c.base, c.count = start, n
	}
	v := c.base
	c.base++
	c.count--
	return v, nil
}

// Reset 丢弃未消费的缓存批次（事务边界调用）：下次 Get 向远端重新拉批，
// 保证新事务时间戳贴近权威节点当前序列，避免批量缓存长期滞后。
func (c *tsoClient) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count = 0
}

// GetBatch 一次分配 n 个连续时间戳：缓存充足时直接消费，否则向远端
// 请求整批（丢弃残余缓存，与远端序列严格衔接）。远端不可达返回 (0,0)。
func (c *tsoClient) GetBatch(n uint64) (uint64, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n == 0 {
		n = 1
	}
	if c.count >= n {
		start := c.base
		c.base += n
		c.count -= n
		return start, n
	}
	start, _, err := c.fetchBatch()
	if err != nil {
		return 0, 0
	}
	c.base, c.count = start+n, 0
	return start, n
}

// fetchBatch 向远端 TSO 节点请求一批连续号。
func (c *tsoClient) fetchBatch() (uint64, uint64, error) {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return 0, 0, err
	}
	conn, err := dial(addr)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	payload, err := marshalJSON(TsoReq{Batch: c.batch})
	if err != nil {
		return 0, 0, err
	}
	if err := writeFrame(conn, msgTsoReq, payload); err != nil {
		return 0, 0, err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return 0, 0, err
	}
	if typ != msgTsoResp {
		return 0, 0, errors.New("cluster: unexpected tso reply")
	}
	var resp TsoResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return 0, 0, err
	}
	if resp.Err != "" {
		return 0, 0, errors.New("cluster: tso error: " + resp.Err)
	}
	return resp.Start, resp.Count, nil
}

// handleTso 服务端处理 TSO_REQ：用本节点本地发号器批量发放并回执。
func (s *Server) handleTso(conn net.Conn, payload []byte) {
	var req TsoReq
	if err := unmarshalJSON(payload, &req); err != nil {
		writeFrame(conn, msgTsoResp, mustJSON(TsoResp{Err: "bad request"}))
		return
	}
	if s.tso == nil {
		writeFrame(conn, msgTsoResp, mustJSON(TsoResp{Err: "tso not configured"}))
		return
	}
	start, n := s.tso.GetBatch(req.Batch)
	writeFrame(conn, msgTsoResp, mustJSON(TsoResp{Start: start, Count: n}))
}

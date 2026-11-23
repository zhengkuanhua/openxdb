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
	"fmt"
	"log"
	"net"
	"sync"
	"time"

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
// B2 TSO HA 下附带 Epoch（发号主纪元），客户端据此识别更高纪元的新主。
type TsoResp struct {
	Start uint64 `json:"start,omitempty"`
	Count uint64 `json:"count,omitempty"`
	Epoch uint64 `json:"epoch,omitempty"`
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

	// NEW-5 号段预取（单点部署同样具备抖动容忍，但保持单点语义：
	// 远端彻底不可达、池又耗尽时仍然报错）。maxSeen 为已下发最大时间戳，
	// 用于拒绝远端序列回退；pendBase/pendCount 为预取的下一批号段。
	maxSeen       uint64
	pendBase      uint64
	pendCount     uint64
	prefetching   bool
	prefetchOK    int
	prefetchFails int
	// filling/fillCh 同步补拉的在飞合并（singleflight）：池空时只允许一个
	// goroutine 走网络，其余等待其结果。缺了它，多路并发拉批的响应会乱序
	// 到位，较晚验收的那批起点更小而被回退保护判为回退，导致正常并发下的
	// 误报（NEW-5 并发用例）。
	filling bool
	fillCh  chan struct{}
	// fetchHook 测试注入点：非 nil 时 fetchBatch 直接走它，避免单测依赖真实
	// 网络与节点注册表。生产路径恒为 nil，不影响行为。
	fetchHook func() (uint64, uint64, error)
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
	for {
		c.mu.Lock()
		if c.count == 0 {
			// NEW-5：池耗尽时先切预取批（抖动期无需再走网络即成功）。
			c.switchPendingLocked()
		}
		if c.count > 0 {
			v := c.base
			c.base++
			c.count--
			c.maybePrefetchLocked()
			c.mu.Unlock()
			return v, nil
		}
		// 池真正耗尽：并发合并到单个在飞拉批，网络 IO 置于锁外。
		if c.filling {
			ch := c.fillCh
			c.mu.Unlock()
			<-ch
			continue
		}
		c.filling = true
		ch := make(chan struct{})
		c.fillCh = ch
		c.mu.Unlock()

		start, n, err := c.fetchBatch()

		c.mu.Lock()
		var aerr error
		if err == nil {
			aerr = c.acceptFillLocked(start, n)
		}
		c.filling = false
		c.fillCh = nil
		c.mu.Unlock()
		close(ch)
		if err != nil {
			return 0, err
		}
		if aerr != nil {
			return 0, aerr
		}
	}
}

// Reset 丢弃未消费的缓存批次（事务边界调用）：下次 Get 向远端重新拉批，
// 保证新事务时间戳贴近权威节点当前序列，避免批量缓存长期滞后。
func (c *tsoClient) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count = 0
	c.pendCount = 0
}

// GetBatch 一次分配 n 个连续时间戳：缓存充足时直接消费，否则向远端
// 请求整批（丢弃残余缓存，与远端序列严格衔接）。远端不可达返回 (0,0)。
func (c *tsoClient) GetBatch(n uint64) (uint64, uint64) {
	if n == 0 {
		n = 1
	}
	for {
		c.mu.Lock()
		if c.count >= n {
			start := c.base
			c.base += n
			c.count -= n
			c.maybePrefetchLocked()
			c.mu.Unlock()
			return start, n
		}
		// NEW-5：池不足时先启用预取批。
		c.switchPendingLocked()
		if c.count >= n {
			start := c.base
			c.base += n
			c.count -= n
			c.maybePrefetchLocked()
			c.mu.Unlock()
			return start, n
		}
		// 并发合并：同一时刻只允许一个 goroutine 走网络拉批。
		if c.filling {
			ch := c.fillCh
			c.mu.Unlock()
			<-ch
			continue
		}
		c.filling = true
		ch := make(chan struct{})
		c.fillCh = ch
		c.mu.Unlock()

		start, cnt, err := c.fetchBatch()

		c.mu.Lock()
		// 填充在飞期间后台预取可能先到：转正预取批后再判断是否够用。
		c.switchPendingLocked()
		if c.count >= n {
			// 并发取号已补足池：直接消费现成号段，本批放弃（不重复发放）。
			s := c.base
			c.base += n
			c.count -= n
			c.maybePrefetchLocked()
			c.filling = false
			c.fillCh = nil
			c.mu.Unlock()
			close(ch)
			return s, n
		}
		if err != nil || start == 0 || cnt == 0 || (c.maxSeen != 0 && start <= c.maxSeen) {
			// 远端不可达 / 空批 / 序列回退：丢弃该响应，与 HA 客户端回退保护同语义。
			c.filling = false
			c.fillCh = nil
			c.mu.Unlock()
			close(ch)
			return 0, 0
		}
		c.base, c.count = start+cnt, 0
		c.pendCount = 0
		if end := start + cnt - 1; end > c.maxSeen {
			c.maxSeen = end
		}
		c.maybePrefetchLocked()
		c.filling = false
		c.fillCh = nil
		c.mu.Unlock()
		close(ch)
		return start, cnt
	}
}

// ---- NEW-5 号段预取（单点） ----

// lowWatermark 低水位阈值：批大小的 1/4，最小 1。
func (c *tsoClient) lowWatermark() uint64 {
	w := c.batch / 4
	if w < 1 {
		w = 1
	}
	return w
}

// switchPendingLocked 当前池耗尽时切换到预取批（调用方持锁）。
func (c *tsoClient) switchPendingLocked() {
	if c.count > 0 || c.pendCount == 0 {
		return
	}
	c.base, c.count = c.pendBase, c.pendCount
	c.pendBase, c.pendCount = 0, 0
}

// maybePrefetchLocked 剩余号段低于水位时触发后台补充（调用方持锁）。
func (c *tsoClient) maybePrefetchLocked() {
	if c.prefetching {
		return
	}
	if c.count+c.pendCount > c.lowWatermark() {
		return
	}
	c.prefetching = true
	go c.prefetchAsync(c.batch)
}

// prefetchAsync 后台补拉一批号：拉取在锁外进行，失败短退避重试，
// 全部失败仅计数与打日志；池耗尽时由 Get/GetBatch 的同步路径决定成败。
func (c *tsoClient) prefetchAsync(want uint64) {
	if want == 0 {
		want = 1
	}
	var lastErr error
	for attempt := 0; attempt <= tsoPrefetchRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(prefetchBackoff(attempt))
		}
		c.mu.Lock()
		if c.count+c.pendCount > c.lowWatermark() {
			c.prefetching = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		start, n, err := c.fetchBatch()
		if err != nil {
			lastErr = err
			continue
		}
		c.mu.Lock()
		aerr := c.acceptPrefetchedLocked(start, n)
		if aerr == nil {
			c.prefetchOK++
		} else {
			c.prefetchFails++
		}
		c.prefetching = false
		c.mu.Unlock()
		if aerr == nil {
			return
		}
		// 回退保护命中：已有更高号段下发，重试无意义。
		log.Printf("cluster: tso prefetch dropped (node %s): %v", c.nodeID, aerr)
		return
	}
	c.mu.Lock()
	c.prefetchFails++
	c.prefetching = false
	c.mu.Unlock()
	if lastErr != nil {
		log.Printf("cluster: tso prefetch failed (node %s, retries %d): %v", c.nodeID, tsoPrefetchRetries, lastErr)
	}
}

// acceptPrefetchedLocked 验收预取批：空批丢弃、起点须严格大于 maxSeen、
// 不得与在手预取批重叠；通过则挂到 pend 等待池耗尽后消费。
func (c *tsoClient) acceptPrefetchedLocked(start, n uint64) error {
	if n == 0 || start == 0 {
		return errors.New("cluster: tso prefetch empty batch")
	}
	if c.maxSeen != 0 && start <= c.maxSeen {
		return fmt.Errorf("cluster: tso prefetch regression: start %d <= max seen %d", start, c.maxSeen)
	}
	if c.pendCount > 0 && start <= c.pendBase+c.pendCount-1 {
		return fmt.Errorf("cluster: tso prefetch overlap: start %d <= pending end %d", start, c.pendBase+c.pendCount-1)
	}
	c.pendBase, c.pendCount = start, n
	if end := start + n - 1; end > c.maxSeen {
		c.maxSeen = end
	}
	return nil
}

// acceptFillLocked 验收同步补拉的一批号（调用方持锁）：并发下若池已被
// 其它 goroutine 填满则放弃本批；空批或回退一律拒绝。
func (c *tsoClient) acceptFillLocked(start, n uint64) error {
	// 同步拉批在飞期间，后台预取可能已挂上更高号段：先转正预取批并复用，
	// 不能拿本批（起点更小）去撞回退保护，否则并发下会误报序列回退。
	c.switchPendingLocked()
	if c.count > 0 {
		return nil
	}
	if n == 0 || start == 0 {
		return errors.New("cluster: tso empty batch")
	}
	if c.maxSeen != 0 && start <= c.maxSeen {
		return fmt.Errorf("cluster: tso timestamp regression: start %d <= max seen %d", start, c.maxSeen)
	}
	c.base, c.count = start, n
	c.pendCount = 0
	if end := start + n - 1; end > c.maxSeen {
		c.maxSeen = end
	}
	return nil
}

// PrefetchStats 返回号段预取观测值：成功次数、失败（含回退丢弃）次数。
func (c *tsoClient) PrefetchStats() (ok, fails int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prefetchOK, c.prefetchFails
}

// fetchBatch 向远端 TSO 节点请求一批连续号。
func (c *tsoClient) fetchBatch() (uint64, uint64, error) {
	if c.fetchHook != nil {
		return c.fetchHook()
	}
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
//
// B2 TSO HA：启用 HA 时只有"已授权主"可发号——follower 一律回执
// errNotTsoLeader，客户端据此转发选主（epoch 更大者接管），从而避免
// 一个已经失去授权的旧主在分区期间继续发号造成回退/重复。
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
	if st := s.tsoHAState(); st != nil {
		start, n, ok, epoch := st.serve(req.Batch)
		if !ok {
			writeFrame(conn, msgTsoResp, mustJSON(TsoResp{Err: errTSONotLeader(epoch), Epoch: epoch}))
			return
		}
		writeFrame(conn, msgTsoResp, mustJSON(TsoResp{Start: start, Count: n, Epoch: epoch}))
		return
	}
	start, n := s.tso.GetBatch(req.Batch)
	writeFrame(conn, msgTsoResp, mustJSON(TsoResp{Start: start, Count: n}))
}

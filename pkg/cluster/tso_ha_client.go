package cluster

// D4.5 §6-2（B2）TSO 主备客户端：实现 tso.Source，按组优先序选主并自动切换。
//
// 状态机：
//   正常路径  —— 向 leader 拉批，缓存逐号消费（与单点 tsoClient 同语义）；
//   主不可达  —— 按 group 优先序探测候选，TSO_ELECT 成功后立即拉批并切主；
//   回退保护  —— 任何响应必须满足 epoch 不降级、时间戳严格大于 maxSeen，
//                否则拒绝该响应并重新选主（新主以 maxSeen 作为 seed 基线）。
//
// 与单点客户端的关键差异：
//   - tsoClient 目标节点固定，失败即返回错误（B2 根因）；
//   - HATSOClient 目标节点可漂移，失败触发选主，只有在组内全部候选不可达时
//     才返回错误（此时才是真正的"无 TSO 可用"）。

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/tso"
)

// NEW-5 号段预取参数（v1.21.0）：
//   - 剩余号段低于 batch/4（最小 1）时后台异步补拉，池真正耗尽才走同步拉批；
//   - 预取失败按短退避重试，避免主抖动期间形成重试风暴；
//   - 预取响应同样受 maxSeen 单调性护栏约束（start 必须严格大于 maxSeen）。
const (
	tsoPrefetchRetries    = 3
	tsoPrefetchBackoffMin = 20 * time.Millisecond
	tsoPrefetchBackoffMax = 400 * time.Millisecond
)

// HATSOClient TSO 主备客户端（线程安全）。
type HATSOClient struct {
	mgr   *Manager
	group []string
	batch uint64

	mu        sync.Mutex
	selfID    string // 本节点 ID：命中候选时走本地状态机，避免自连
	leader    string // 当前认为的主节点 ID（空 = 未知，需选主）
	epoch     uint64 // 已观测最大选主纪元
	maxSeen   uint64 // 已下发最大时间戳（回退护栏基线）
	base      uint64 // 当前缓存批起点
	count     uint64 // 当前缓存批剩余
	failovers int    // 因取号失败而切换主的次数（观测用）
	elects    int    // 成功选主次数（观测用）

	// NEW-5 号段预取状态：pendBase/pendCount 为已预取、尚未启用的下一批号段
	// （接在当前池之后消费，池耗尽即无缝切换，避免抖动期请求直接报错）；
	// prefetching 保证同一时刻最多一个后台补拉，prefetchOK/prefetchFails 为观测值。
	pendBase      uint64
	pendCount     uint64
	prefetching   bool
	prefetchOK    int
	prefetchFails int
	// fenceState 本节点 TSO HA 服务端状态（同进程装配时用于选主后广播降级）；
	// nil 表示不做降级广播（纯客户端节点，旧主由其自身下一次取号收敛）。
	fenceState *tsoHAState
	// fetchHook 测试注入点：非 nil 时 fetch 直接走它，避免单测依赖真实网络
	// 与节点注册表。生产路径恒为 nil，不影响行为。
	fetchHook func(id string, want uint64) (uint64, uint64, error)
}

// NewHATSOClient 创建 TSO 主备客户端：group 为候选节点 ID 优先序，
// batch<=0 时默认 64。
func NewHATSOClient(mgr *Manager, group []string, batch uint64) *HATSOClient {
	if batch == 0 {
		batch = 64
	}
	return &HATSOClient{mgr: mgr, group: append([]string(nil), group...), batch: batch}
}

// Group 返回候选节点优先序（只读副本）。
func (c *HATSOClient) Group() []string {
	return append([]string(nil), c.group...)
}

// Get 返回一个严格递增的全局时间戳：缓存耗尽时向当前主拉批，
// 主不可达则自动选主切换后重试。
func (c *HATSOClient) Get() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.count == 0 {
		c.switchPendingLocked()
	}
	if c.count == 0 {
		if err := c.fillLocked(1); err != nil {
			return 0, err
		}
	}
	v := c.base
	c.base++
	c.count--
	c.maybePrefetchLocked()
	return v, nil
}

// GetBatch 一次分配 n 个连续时间戳，返回 [start, start+n)。
// 缓存充足直接消费，否则按需拉批；组内无可用主时返回 (0,0)，
// 与单点 tsoClient 的失败语义保持一致（上层据此报 tso unavailable）。
func (c *HATSOClient) GetBatch(n uint64) (uint64, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n == 0 {
		n = 1
	}
	if c.count >= n {
		start := c.base
		c.base += n
		c.count -= n
		c.maybePrefetchLocked()
		return start, n
	}
	// NEW-5：池不足时先启用预取批（抖动期命中预取即可直接成功）。
	c.switchPendingLocked()
	if c.count >= n {
		start := c.base
		c.base += n
		c.count -= n
		c.maybePrefetchLocked()
		return start, n
	}
	if err := c.fillLocked(n); err != nil {
		return 0, 0
	}
	if c.count < n {
		return 0, 0
	}
	start := c.base
	c.base += n
	c.count -= n
	c.maybePrefetchLocked()
	return start, n
}

// Reset 丢弃未消费缓存（事务边界）：下次 Get 向主重新拉批，
// 保证新事务时间戳贴近权威序列，不因批量缓存长期滞后。
func (c *HATSOClient) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 预取批同样是"缓存号"，事务边界一并丢弃，保证新事务取到贴近权威序列的戳。
	c.count = 0
	c.pendCount = 0
}

// Leader 返回当前 TSO 主节点 ID（空 = 尚未选主）。
func (c *HATSOClient) Leader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

// Stats 返回观测值：当前 epoch、已下发最大戳、切换主次数、选主次数。
func (c *HATSOClient) Stats() (epoch, maxSeen uint64, failovers, elects int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch, c.maxSeen, c.failovers, c.elects
}

// fillLocked 填满缓存：优先当前主；失败（不可达 / 被护栏拒绝）则选主后重试。
// want 为本次至少需要的连续号数量（GetBatch 可能大于默认批大小）。
func (c *HATSOClient) fillLocked(want uint64) error {
	if c.leader != "" {
		start, n, err := c.fetch(c.leader, want)
		if err == nil {
			if aerr := c.acceptLocked(start, n); aerr == nil {
				return nil
			}
		}
		// 主不可达或序列回退：清掉失效主，进入选主流程。
		c.failovers++
		c.leader = ""
	}
	return c.electLocked(want)
}

// acceptLocked 验收一批号：拒绝空批、回退（start <= maxSeen）与重复区间。
// 客户端侧最后一道护栏——即使服务端状态异常，也不让调用方看到回退/重复号。
func (c *HATSOClient) acceptLocked(start, n uint64) error {
	if n == 0 || start == 0 {
		return errors.New("cluster: tso ha empty batch")
	}
	if c.maxSeen != 0 && start <= c.maxSeen {
		return fmt.Errorf("cluster: tso ha timestamp regression: start %d <= max seen %d", start, c.maxSeen)
	}
	c.base, c.count = start, n
	// 新批起点高于（含）任何预取批，预取批整体作废，避免重复发放。
	c.pendCount = 0
	if end := start + n - 1; end > c.maxSeen {
		c.maxSeen = end
	}
	return nil
}

// ---- NEW-5 号段预取（低水位异步补充 + 单调性护栏） ----

// lowWatermark 低水位阈值：默认批大小的 1/4，最小 1。
// 剩余号段（在池 + 预取批）不高于该阈值时触发后台补充。
func (c *HATSOClient) lowWatermark() uint64 {
	w := c.batch / 4
	if w < 1 {
		w = 1
	}
	return w
}

// switchPendingLocked 当前池耗尽时无缝切换到预取批（调用方持锁）。
func (c *HATSOClient) switchPendingLocked() {
	if c.count > 0 || c.pendCount == 0 {
		return
	}
	c.base, c.count = c.pendBase, c.pendCount
	c.pendBase, c.pendCount = 0, 0
}

// maybePrefetchLocked 低水位触发后台补充（调用方持锁）。
// 剩余充足、已有补拉在飞、或尚未选主（选主必须走同步路径）时直接返回。
func (c *HATSOClient) maybePrefetchLocked() {
	if c.prefetching || c.leader == "" {
		return
	}
	if c.count+c.pendCount > c.lowWatermark() {
		return
	}
	c.prefetching = true
	go c.prefetchAsync(c.leader, c.batch)
}

// prefetchAsync 后台补充号段：网络拉取在锁外进行（不阻塞并发 Get），
// 失败按短退避重试，全部失败只计数并打日志，绝不使当前请求失败——
// 池真正耗尽时由 Get/GetBatch 走同步 fillLocked 决定成功或报错。
func (c *HATSOClient) prefetchAsync(leader string, want uint64) {
	if want == 0 {
		want = 1
	}
	if leader == "" {
		c.finishPrefetch(false)
		return
	}
	var lastErr error
	for attempt := 0; attempt <= tsoPrefetchRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(prefetchBackoff(attempt))
		}
		// 每轮重读共享状态：预取期间同步路径可能已填满池或已换主。
		c.mu.Lock()
		if c.count+c.pendCount > c.lowWatermark() {
			c.prefetching = false
			c.mu.Unlock()
			return
		}
		leader = c.leader
		c.mu.Unlock()
		if leader == "" {
			// 主已失效：预取不重复选主（选主含纪元推进），交给同步路径。
			break
		}
		start, n, err := c.fetch(leader, want)
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
		// 回退保护命中：说明已有更高号段下发（该响应不可用），重试无意义。
		log.Printf("cluster: tso ha prefetch dropped: %v", aerr)
		return
	}
	c.finishPrefetch(true)
	if lastErr != nil {
		log.Printf("cluster: tso ha prefetch failed (leader %s, retries %d): %v", leader, tsoPrefetchRetries, lastErr)
	}
}

// finishPrefetch 结束一次预取（失败计数 + 释放标志）。
func (c *HATSOClient) finishPrefetch(failed bool) {
	c.mu.Lock()
	if failed {
		c.prefetchFails++
	}
	c.prefetching = false
	c.mu.Unlock()
}

// acceptPrefetchedLocked 验收预取批：空批丢弃、起点必须严格大于 maxSeen
// （沿用回退保护语义）、不得与在手的预取批重叠；通过则挂到 pend 等待消费。
func (c *HATSOClient) acceptPrefetchedLocked(start, n uint64) error {
	if n == 0 || start == 0 {
		return errors.New("cluster: tso ha prefetch empty batch")
	}
	if c.maxSeen != 0 && start <= c.maxSeen {
		return fmt.Errorf("cluster: tso ha prefetch regression: start %d <= max seen %d", start, c.maxSeen)
	}
	if c.pendCount > 0 && start <= c.pendBase+c.pendCount-1 {
		return fmt.Errorf("cluster: tso ha prefetch overlap: start %d <= pending end %d", start, c.pendBase+c.pendCount-1)
	}
	c.pendBase, c.pendCount = start, n
	if end := start + n - 1; end > c.maxSeen {
		// 预取批视为"已下发"：后续任何拉批（同步/预取）起点都必须越过它，
		// 从而杜绝时间戳回退。
		c.maxSeen = end
	}
	return nil
}

// PrefetchStats 返回号段预取观测值：成功次数、失败（含回退丢弃）次数。
func (c *HATSOClient) PrefetchStats() (ok, fails int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prefetchOK, c.prefetchFails
}

// prefetchBackoff 第 attempt 轮预取前的退避时长（指数增长，封顶上限）。
func prefetchBackoff(attempt int) time.Duration {
	d := tsoPrefetchBackoffMin
	for i := 1; i < attempt && d < tsoPrefetchBackoffMax; i++ {
		d *= 2
	}
	if d > tsoPrefetchBackoffMax {
		d = tsoPrefetchBackoffMax
	}
	return d
}

// electLocked 选主：按组优先序探测候选节点，授予者成为新主并立即拉批。
// 两轮探测：第一轮以 c.epoch+1 请求；若组内已有更高 epoch 的主（follower
// 回执真实 epoch），第二轮以观测到的最大 epoch+1 请求，避免无谓失败。
func (c *HATSOClient) electLocked(want uint64) error {
	if len(c.group) == 0 {
		return errors.New("cluster: tso ha group is empty")
	}
	// 双主防护：组内多数派不可达时不允许自封主（否则双向分区两侧各选一主，
	// 各自发号 → 时间戳重复）。副本数 ≤2 时不设门槛：此时"多数派=全部"，
	// 设门槛会让单节点故障直接不可恢复，与 B2 目标相悖。
	if !c.quorumReachableLocked() {
		return errors.New("cluster: tso ha quorum not reachable")
	}
	maxObs := c.epoch
	lastErr := error(errors.New("cluster: tso ha no reachable candidate"))
	for round := 0; round < 2; round++ {
		req := c.epoch + 1
		if round == 1 {
			req = maxObs + 1
		}
		for _, id := range c.group {
			resp, err := c.electNode(id, req, c.maxSeen)
			if err != nil {
				lastErr = err
				continue
			}
			if resp.Epoch > maxObs {
				maxObs = resp.Epoch
			}
			if !resp.Granted {
				lastErr = fmt.Errorf("cluster: tso elect %s rejected: %s (epoch=%d)", id, resp.Err, resp.Epoch)
				continue
			}
			if resp.Epoch > c.epoch {
				c.epoch = resp.Epoch
			}
			c.leader = id
			c.elects++
			start, n, err := c.fetch(id, want)
			if err != nil {
				lastErr = err
				c.leader = ""
				continue
			}
			if aerr := c.acceptLocked(start, n); aerr != nil {
				lastErr = aerr
				c.leader = ""
				continue
			}
			// 选主成功：尽力让旧主降级（异步，不阻塞取号）。
			newLeader, e, seen := id, resp.Epoch, c.maxSeen
			go func() {
				if st := c.electState(); st != nil {
					st.broadcastFence(newLeader, e, seen)
				}
			}()
			return nil
		}
	}
	return fmt.Errorf("cluster: tso ha unavailable: %w", lastErr)
}

// fenceHook 由服务端装配的降级广播入口（客户端与服务端同进程时复用）。
// 未装配时选主成功仅影响本客户端，不影响其它节点的降级。
func (c *HATSOClient) electState() *tsoHAState { return c.fenceState }

// SetFenceState 装配本节点 TSO HA 服务端状态（由 db 层装配），
// 使客户端选主成功后能向组内其它节点广播降级，加速旧主收敛。
func (c *HATSOClient) SetFenceState(st *tsoHAState) { c.fenceState = st }

// SetSelf 装配本节点 ID：候选命中自身时直接走本地状态机（不经网络），
// 既省一次自连 RTT，也避免自指请求占住服务端连接。
func (c *HATSOClient) SetSelf(id string) { c.selfID = id }

// quorumReachableLocked 统计组内可达副本是否达到多数派（含本节点）。
// 仅在选主前调用（低频路径），用主动探测而非心跳状态，避免心跳滞后误判。
func (c *HATSOClient) quorumReachableLocked() bool {
	if len(c.group) <= 2 {
		return true
	}
	alive := 1 // 本节点自身必然可达
	for _, id := range c.group {
		if id == "" || id == c.selfID {
			continue
		}
		if c.mgr != nil && c.mgr.Probe(id) {
			alive++
		}
	}
	return alive >= len(c.group)/2+1
}

// fetch 向指定节点请求一批号（至少 want 个），返回 [start, start+count)。
func (c *HATSOClient) fetch(id string, want uint64) (uint64, uint64, error) {
	if c.fetchHook != nil {
		return c.fetchHook(id, want)
	}
	batch := c.batch
	if want > batch {
		batch = want
	}
	// 候选命中自身：直接问本地状态机，避免自连（self 既是客户端又是服务端）。
	if c.fenceState != nil && id != "" && id == c.selfID {
		start, n, ok, epoch := c.fenceState.serve(batch)
		if !ok {
			return 0, 0, errors.New("cluster: tso error: " + errTSONotLeader(epoch))
		}
		if epoch > c.epoch {
			c.epoch = epoch
		}
		return start, n, nil
	}
	addr, err := c.mgr.NodeAddr(id)
	if err != nil {
		return 0, 0, err
	}
	conn, err := dial(addr)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	payload, err := marshalJSON(TsoReq{Batch: batch})
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
	if resp.Count == 0 {
		return 0, 0, errors.New("cluster: tso ha empty reply")
	}
	return resp.Start, resp.Count, nil
}

// electNode 向单个候选节点请求接任 TSO 主。
func (c *HATSOClient) electNode(id string, reqEpoch, observed uint64) (TsoElectResp, error) {
	var resp TsoElectResp
	// 候选命中自身：本地状态机直接授予/拒绝（不经网络，避免自连）。
	if c.fenceState != nil && id != "" && id == c.selfID {
		return c.fenceState.elect(reqEpoch, observed), nil
	}
	if c.mgr == nil {
		return resp, errors.New("cluster: tso ha manager not initialized")
	}
	addr, err := c.mgr.NodeAddr(id)
	if err != nil {
		return resp, err
	}
	conn, err := dial(addr)
	if err != nil {
		return resp, err
	}
	defer conn.Close()
	payload, err := marshalJSON(TsoElectReq{Epoch: reqEpoch, Observed: observed})
	if err != nil {
		return resp, err
	}
	if err := writeFrame(conn, msgTsoElect, payload); err != nil {
		return resp, err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return resp, err
	}
	if typ != msgTsoElect {
		return resp, errors.New("cluster: unexpected tso elect reply")
	}
	if err := unmarshalJSON(raw, &resp); err != nil {
		return resp, err
	}
	if resp.Err != "" && !resp.Granted {
		return resp, nil // 业务层拒绝（stale epoch）：交给调用方决定下一轮
	}
	return resp, nil
}

// 编译期断言：HATSOClient 满足 tso.Source。
var _ tso.Source = (*HATSOClient)(nil)

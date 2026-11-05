package cluster

// D4.5 §6-2（B2）TSO 中心单点治理：TSO 主备 + epoch 选主（服务端部分）。
//
// 修复前：TSO 挂在单一节点，客户端 tsoClient 指向固定 nodeID，该节点宕机后
// fetchBatch 直接失败 → 2PC 取号报 "tso unavailable" → 全集群跨节点写全失败
// （读路径不取号，不受影响）。
//
// 修复设计（三段式，全部落在"发号权"上）：
//  1. 副本状态机 tsoHAState：每个 TSO 组节点都是候选主，持有
//     (epoch, last, reserved) 三元组。last 为本节点已下发最大时间戳，
//     reserved 为已落盘的"未来可达上界"（last+窗口）。发号前先把窗口推进
//     落盘，重启后从 reserved 恢复 last，天然不回退、不复用已发号；
//  2. epoch 选主：客户端发现当前主不可达时，按组优先序探测候选节点并发
//     TSO_ELECT（携带观测 epoch 与已下发最大戳 maxSeen）；授予者 epoch 单调
//     推进（max(本地+1, 请求值)），且把 last 提升到 maxSeen 之上，因此新主
//     首批发号必然越过旧主已发出的所有号；
//  3. 陈旧主 fencing：新主上位后尽力向组内其它可达节点广播 TSO_FENCE，
//     收到更高 epoch 者立即降级为 follower 并接受 last 提升；客户端再以
//     "响应 epoch 不得低于自身观测、时间戳不得低于 maxSeen"做最后护栏。
//
// 边界（如实声明，不夸大）：本机制解决"主节点宕机/链路不可达"的自动恢复。
// 网络分区下双主并存时，靠 epoch fencing + 客户端 maxSeen 护栏把回退与重复
// 收窄；分区下强一致的单一发号权仍需共识算法（留 v1.19 规划），本簇不改排期。

import (
	"fmt"
	"net"
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/tso"
)

// TSO HA 网络帧（M8 的 28/29 之后，B2 追加 70/71）。
const (
	// msgTsoElect 选主请求：客户端请求目标节点接任 TSO 主。
	// 70/71 避开既有帧：TSO(28-29)/DDL(30-31)/Registry(32-33)/
	// 备份(40-45)/epoch 通知(50)/全量对齐(60-61)。
	msgTsoElect byte = 70
	// msgTsoFence 陈旧主降级通知：新主向组内广播更高 epoch。
	msgTsoFence byte = 71
)

// tsoHAWindow 发号窗口：每次把 last+窗口 落盘后再发号，窗口内不再写盘。
// 窗口越大写盘越少、重启后跳过的号越多（仅浪费号空间，不影响正确性）。
const tsoHAWindow uint64 = 4096

// TsoElectReq 选主请求载荷。
//   - Epoch：客户端期望新主持有的 epoch 下限（通常为观测值+1）；
//   - Observed：客户端已下发的最大时间戳，新主必须把 last 提升到其之上。
type TsoElectReq struct {
	Epoch    uint64 `json:"epoch,omitempty"`
	Observed uint64 `json:"observed,omitempty"`
}

// TsoElectResp 选主响应载荷。
type TsoElectResp struct {
	Granted bool   `json:"granted,omitempty"`
	Epoch   uint64 `json:"epoch,omitempty"`
	Last    uint64 `json:"last,omitempty"`
	Err     string `json:"err,omitempty"`
}

// TsoFenceReq 陈旧主降级通知载荷。
// Candidate 为发起广播的当任主节点 ID：同 epoch 双主并存时（两侧几乎同时
// 授予），双方以节点 ID 字典序决出唯一主（ID 大者胜），避免 epoch 相同导致
// 谁都不降级、两侧各自发号造成时间戳重复。
type TsoFenceReq struct {
	Epoch     uint64 `json:"epoch,omitempty"`
	Last      uint64 `json:"last,omitempty"`
	Candidate string `json:"candidate,omitempty"`
}

// tsoHAState TSO 副本状态机：epoch 单调 + last 单调 + 窗口预留落盘。
type tsoHAState struct {
	mu     sync.Mutex
	selfID string
	mgr    *Manager
	group  []string

	epoch    uint64 // 已知最大选主纪元（单调不减）
	leader   bool   // 本节点是否当前 TSO 主
	last     uint64 // 本节点已下发最大时间戳（单调不减）
	reserved uint64 // 已落盘的未来可达上界
	save     func(epoch, last uint64)
}

// EnableTSOHA 在节点服务端启用 TSO HA（B2）。
//   - group：TSO 副本节点 ID（按客户端选主优先序）；
//   - load：启动时读取持久化的 (epoch, reserved)，无持久化返回 (0,0)；
//   - save：窗口推进/epoch 变更时的持久化回调，nil 表示纯内存。
//
// 启用后 TSO_REQ 只有"本节点为已授权主"时才发号，否则回执
// "not tso leader"，由客户端触发选主切换。
func (s *Server) EnableTSOHA(group []string, load func() (uint64, uint64), save func(uint64, uint64)) {
	st := &tsoHAState{
		selfID: s.SelfID,
		mgr:    s.mgr,
		group:  append([]string(nil), group...),
		save:   save,
	}
	if load != nil {
		epoch, reserved := load()
		st.epoch = epoch
		// 从 reserved 恢复：跳过窗口内未用完的号，保证重启后不回退、不重复。
		st.last = reserved
		st.reserved = reserved
	}
	s.mu2pc.Lock()
	// HA 分支仍以本节点本地发号器为序列底座（仅"已授权主"才调用它），
	// 未装配时补一个，避免 handleTso 在 HA 生效前因 nil 直接回执未配置。
	if s.tso == nil {
		s.tso = tso.New()
	}
	s.tsoHA = st
	s.mu2pc.Unlock()
}

// TSOHAState 返回本节点 TSO HA 观测值（enabled/leader/epoch/last）。
func (s *Server) TSOHAState() (enabled, leader bool, epoch, last uint64) {
	s.mu2pc.Lock()
	st := s.tsoHA
	s.mu2pc.Unlock()
	if st == nil {
		return false, false, 0, 0
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return true, st.leader, st.epoch, st.last
}

// allocLocked 在窗口内分配 n 个连续时间戳，必要时先推进并落盘预留窗口。
func (st *tsoHAState) allocLocked(n uint64) (uint64, uint64) {
	if n == 0 {
		n = 1
	}
	start := tso.NextBase(st.last)
	st.last = start + n - 1
	st.extendWindowLocked()
	return start, n
}

// extendWindowLocked 在已发号上界触及预留窗口边界时推进窗口并落盘。
func (st *tsoHAState) extendWindowLocked() {
	if st.last < st.reserved {
		return
	}
	st.reserved = st.last + tsoHAWindow
	if st.save != nil {
		st.save(st.epoch, st.reserved)
	}
}

// serve 处理一次 TSO_REQ：仅已授权主发号；follower 明确拒绝，不静默降级到
// 本地发号（避免调用方误用未授权主的序列）。返回 ok=false 时附带当前 epoch。
func (st *tsoHAState) serve(n uint64) (start, count uint64, ok bool, epoch uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.leader {
		return 0, 0, false, st.epoch
	}
	start, count = st.allocLocked(n)
	return start, count, true, st.epoch
}

// elect 处理一次 TSO_ELECT：授予领导权，或告知客户端已有更高 epoch 的主。
func (st *tsoHAState) elect(reqEpoch, observed uint64) TsoElectResp {
	st.mu.Lock()
	defer st.mu.Unlock()
	if reqEpoch == 0 {
		reqEpoch = 1
	}
	// 主自身序列滞后于请求方已观测序列（新主刚接任、旧主已发过更多号，
	// 或客户端缓存了大批号）：先把 last 拉到观测值之上再谈授予，否则新主
	// 会发出客户端已用过的号（重复）。这是"不重复"的关键补齐点。
	if st.leader && observed > st.last {
		st.last = observed
		st.extendWindowLocked()
		if st.save != nil {
			st.save(st.epoch, st.reserved)
		}
	}
	// 已是主且 epoch 足够新、序列已越过对方观测值：幂等授予，不推进 epoch
	// （避免多客户端反复探测把 epoch 推高、造成无谓的领导权漂移）。
	if st.leader && st.epoch+1 >= reqEpoch && st.last >= observed {
		return TsoElectResp{Granted: true, Epoch: st.epoch, Last: st.last}
	}
	// 已知更高 epoch 且本节点非主：拒绝，回执真实 epoch 让客户端去别处探测。
	if st.epoch >= reqEpoch {
		return TsoElectResp{Granted: false, Epoch: st.epoch, Last: st.last, Err: "stale epoch"}
	}
	// 授予领导权：epoch 单调推进；last 提升到客户端已下发最大戳之上，
	// 保证新主首批发号严格越过旧主序列（不回退、不重复）。
	ne := st.epoch + 1
	if reqEpoch > ne {
		ne = reqEpoch
	}
	st.epoch = ne
	st.leader = true
	if observed > st.last {
		st.last = observed
	}
	st.extendWindowLocked()
	if st.save != nil {
		st.save(st.epoch, st.reserved)
	}
	return TsoElectResp{Granted: true, Epoch: st.epoch, Last: st.last}
}

// fence 处理陈旧主降级通知：更高 epoch 到达即降级为 follower。
//
// 同 epoch 场景（两侧几乎同时被授予、都认为自己为主）由 Candidate 仲裁：
// 节点 ID 字典序小者降级，保证双主只在极短窗口内存在且必然收敛到唯一主。
// 缺 Candidate 时保持旧行为，仅按 epoch 比较，不做同级仲裁。
func (st *tsoHAState) fence(epoch, last uint64, candidate string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	switch {
	case epoch > st.epoch:
		st.epoch = epoch
		st.leader = false
	case epoch == st.epoch && st.leader && candidate != "" && candidate != st.selfID:
		// 同级双主：ID 大者继续当主，小者让位（确定性收敛，无协商往返）。
		if st.selfID < candidate {
			st.leader = false
		}
	}
	if last > st.last {
		st.last = last
		st.extendWindowLocked()
	}
}

// broadcastFence 尽力向组内其它节点广播降级通知。不可达节点忽略：不可达
// 即无法对外发号，待其恢复后由下一次选主/取号重新收敛。
func (st *tsoHAState) broadcastFence(self string, epoch, last uint64) {
	if st.mgr == nil || epoch == 0 {
		return
	}
	payload, err := marshalJSON(TsoFenceReq{Epoch: epoch, Last: last, Candidate: self})
	if err != nil {
		return
	}
	for _, id := range st.group {
		if id == "" || id == self {
			continue
		}
		addr, err := st.mgr.NodeAddr(id)
		if err != nil || addr == "" {
			continue
		}
		conn, err := dial(addr)
		if err != nil {
			continue
		}
		if err := writeFrame(conn, msgTsoFence, payload); err == nil {
			// 单向通知：不等待回执内容，读一帧即关（对端失败不影响本端）。
			_, _, _ = readFrame(conn)
		}
		_ = conn.Close()
	}
}

// handleTsoElect 服务端处理 TSO_ELECT：授予/拒绝并回执当前 epoch 与 last。
func (s *Server) handleTsoElect(conn net.Conn, payload []byte) {
	var req TsoElectReq
	if err := unmarshalJSON(payload, &req); err != nil {
		writeFrame(conn, msgTsoElect, mustJSON(TsoElectResp{Err: "bad request"}))
		return
	}
	s.mu2pc.Lock()
	st := s.tsoHA
	s.mu2pc.Unlock()
	if st == nil {
		writeFrame(conn, msgTsoElect, mustJSON(TsoElectResp{Err: "tso ha not enabled"}))
		return
	}
	resp := st.elect(req.Epoch, req.Observed)
	writeFrame(conn, msgTsoElect, mustJSON(resp))
	// 授予成功后尽力让旧主降级（异步，不拖慢选主响应）。
	if resp.Granted {
		go st.broadcastFence(st.selfID, resp.Epoch, resp.Last)
	}
}

// handleTsoFence 服务端处理 TSO_FENCE：更高 epoch 到达即降级。
func (s *Server) handleTsoFence(conn net.Conn, payload []byte) {
	var req TsoFenceReq
	if err := unmarshalJSON(payload, &req); err != nil {
		return
	}
	s.mu2pc.Lock()
	st := s.tsoHA
	s.mu2pc.Unlock()
	if st != nil {
		st.fence(req.Epoch, req.Last, req.Candidate)
	}
	// 回执一帧，便于对端 readFrame 收敛。
	_ = writeFrame(conn, msgTsoFence, mustJSON(TsoFenceReq{}))
}

// tsoHAEnabled 返回是否已启用 TSO HA（handleTso 分支判定用）。
func (s *Server) tsoHAState() *tsoHAState {
	s.mu2pc.Lock()
	defer s.mu2pc.Unlock()
	return s.tsoHA
}

// BindTSOHAClient 把本节点 TSO HA 服务端状态装配到客户端：
//   - 客户端命中候选自身时直接走本地状态机（免自连）；
//   - 客户端选主成功后向组内其它节点广播降级，加速旧主收敛。
//
// 未启用 HA（tsoHA==nil）时仅设置 selfID，客户端退化为纯网络选主。
func (s *Server) BindTSOHAClient(c *HATSOClient) {
	if c == nil {
		return
	}
	c.SetSelf(s.SelfID)
	if st := s.tsoHAState(); st != nil {
		c.SetFenceState(st)
	}
}

// TSOHASnapshot 返回本节点 TSO HA 观测快照（运维/测试用）：
// enabled、本节点是否主、epoch、已下发最大戳、组内副本数。
func (s *Server) TSOHASnapshot() (enabled, leader bool, epoch, last uint64, group int) {
	st := s.tsoHAState()
	if st == nil {
		return false, false, 0, 0, 0
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return true, st.leader, st.epoch, st.last, len(st.group)
}

// errTSONotLeader 由 handleTso 回执给客户端，触发其选主切换。
func errTSONotLeader(epoch uint64) string {
	return fmt.Sprintf("not tso leader: epoch=%d", epoch)
}

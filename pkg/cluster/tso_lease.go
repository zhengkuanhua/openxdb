package cluster

// v2.1.0 H1（NEW-5）：TSO 号段服务的租约兜底与失效切换。
//
// 缺陷背景（v1.21.0 Phase A 三机真机复验 §10.4）：TSO 是写路径上的硬单点。
// 停掉持有 TSO 发号权的节点后，全集群写立即失败（tso unavailable），
// 只有人工改 --tso-node 才能恢复。B2 的 epoch 选主解决了"客户端知道换谁"，
// 但没有解决"两个节点同时自认主"与"旧主失联后发号权何时可以安全转移"，
// 因此在网络分区/半失联场景下仍可能重复发号。
//
// 本文件在既有 epoch 选主之上补一层**租约（lease）**：
//  1. 候选节点接任前必须向组内多数派拉票，拿到多数派授予才算当选；
//  2. 多数派一旦授予某候选，就在本地记下"该候选持有租约到 now+TTL"，
//     在租约到期前拒绝任何其它候选的授予请求（双主互斥）；
//  3. 主节点持有租约到期前必须完成一轮续约（同样要多数派），
//     续不上就自行降级——serve 在租约失效时直接拒绝发号，
//     因此"旧主失联"等价于"旧主租约在 TTL 内自然过期"，新主才能接续；
//  4. 新主接任时把 last 提升到客户端已观测的最大戳（maxSeen）之上，
//     保证号段单调不减、跨节点不重号（既有护栏，本层不改动）。
//
// 时钟无关性：租约的有效期一律用**本地单调时钟的时长**表示，不比较任何
// 跨节点的绝对时间戳。授予方记录 seenUntil = 授予时刻 + TTL；持租方把
// 到期时刻取为**投票发起时刻** + TTL（发起时刻早于任何授予时刻），
// 因此持租方的实际持有窗口必然落在每个授予方的估计窗口之内，
// 双主窗口为零，且不依赖任何单点时钟或时钟同步。
//
// 开关：默认关闭（未真机验收前不启用）。关闭时所有路径保持 v1.21.0 行为：
// TSO_ELECT 走原 elect（见 tso_ha.go），serve 不做租约校验，
// 低水位预取与 maxSeen 回退护栏完全不变。

import (
	"errors"
	"fmt"
	"net"
	"time"
)

// 租约相关帧（避开既有帧：TSO(28-29)/DDL(30-31)/Registry(32-33)/
// 备份(40-45)/epoch 通知(50)/全量对齐(60-61)/选主(70-71)）。
const (
	// msgTsoLeaseVote 拉票请求：候选向组内其它副本请求"把租约授予我"。
	msgTsoLeaseVote byte = 72
	// msgTsoLeaseRenew 续约请求：现主向组内其它副本延长自己的租约。
	msgTsoLeaseRenew byte = 73
)

// 租约默认参数。
const (
	// DefaultTSOLeaseTTL 默认租约有效期：主失联后最迟 TTL 后可被接管。
	DefaultTSOLeaseTTL = 10 * time.Second
	// DefaultTSOLeaseRenew 默认续约间隔（TTL 的 1/3，容忍两次续约失败）。
	DefaultTSOLeaseRenew = 3 * time.Second
	// tsoLeaseRenewDiv 续约间隔 = TTL / tsoLeaseRenewDiv。
	tsoLeaseRenewDiv = 3
	// tsoLeaseRenewFloor 续约间隔下限，避免小 TTL 打成忙等。
	tsoLeaseRenewFloor = 100 * time.Millisecond
	// tsoLeaseWaitMax 客户端等待"租约到期后接管"的上限。
	tsoLeaseWaitMax = 30 * time.Second
	// tsoLeaseRetryStep 客户端在租约未到期期间的重试步长。
	tsoLeaseRetryStep = 200 * time.Millisecond
	// tsoLeaseQuickRetryMax 无"租约等待时长"时的快速重试次数上限：
	// 用于同纪元碰撞这类可在毫秒级自愈的场景（每次重试都会推进纪元），
	// 上限保证真正无多数派时写路径仍能快速失败，不做无意义空转。
	tsoLeaseQuickRetryMax = 5
	// tsoLeaseQuickRetryGap 快速重试的退避步长。
	tsoLeaseQuickRetryGap = 20 * time.Millisecond
)

// errTSOLeaseDisabled 未启用租约时对租约帧的回应。
var errTSOLeaseDisabled = errors.New("cluster: tso lease not enabled")

// TsoLeaseRenewReq 续约请求载荷。
//   - Epoch：现主持有的选主纪元；
//   - Leader：现主节点 ID。
type TsoLeaseRenewReq struct {
	Epoch  uint64 `json:"epoch,omitempty"`
	Leader string `json:"leader,omitempty"`
}

// TsoLeaseRenewResp 续约响应载荷。
//   - OK：续约成功；
//   - Epoch：应答方已知的最大纪元（客户端据此修正自己落后的纪元）；
//   - Err：拒绝原因（stale epoch / 租约被他主持有 等）。
type TsoLeaseRenewResp struct {
	OK    bool   `json:"ok,omitempty"`
	Epoch uint64 `json:"epoch,omitempty"`
	Err   string `json:"err,omitempty"`
}

// enableLease 打开租约模式并设置参数（Server.EnableTSOLease 与单测共用）。
// 调用方需持有 st.mu 或保证独占（本函数内部不加锁）。
func (st *tsoHAState) enableLease(ttl, renew time.Duration) {
	if ttl <= 0 {
		ttl = DefaultTSOLeaseTTL
	}
	if renew <= 0 {
		renew = ttl / tsoLeaseRenewDiv
		if renew < tsoLeaseRenewFloor {
			renew = tsoLeaseRenewFloor
		}
	}
	st.leaseOn = true
	st.leaseTTL = ttl
	st.leaseRenew = renew
	if st.renewStop == nil {
		st.renewStop = make(chan struct{})
	}
}

// leaseEnabled 返回是否启用租约模式（自锁）。
func (st *tsoHAState) leaseEnabled() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.leaseOn
}

// leaseTTLValue 返回当前租约有效期（自锁）。
func (st *tsoHAState) leaseTTLValue() time.Duration {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.leaseTTL
}

// leaseMillis 返回租约时长（毫秒）；未启用租约为 0（降级通告据此判断格式）。
func (st *tsoHAState) leaseMillis() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.leaseOn {
		return 0
	}
	return st.leaseTTL.Milliseconds()
}

// leaseNotify 关闭续约协程并唤醒（仅用于显式停用/关停场景）。
func (st *tsoHAState) leaseNotify() {
	st.mu.Lock()
	stop := st.renewStop
	st.renewStop = nil
	st.mu.Unlock()
	if stop != nil {
		close(stop)
	}
}

// nowLocked 返回当前时刻（支持单测注入假时钟）。用 time.Now() 时
// 返回值带单调读数，Sub 比较不受系统时钟跳变影响。
func (st *tsoHAState) nowLocked() time.Time {
	if st.now != nil {
		return st.now()
	}
	return time.Now()
}

// leaseRemainLocked 返回本节点当前租约剩余时长（0 表示已无有效租约）。
func (st *tsoHAState) leaseRemainLocked(now time.Time) time.Duration {
	if st.leaseUntil.IsZero() || !now.Before(st.leaseUntil) {
		return 0
	}
	return st.leaseUntil.Sub(now)
}

// leaseValidLocked 判断本节点当前是否持有有效租约（即有权发号）。
func (st *tsoHAState) leaseValidLocked(now time.Time) bool {
	return st.leaseOn && st.leader && st.leaseRemainLocked(now) > 0
}

// otherLeaseLocked 返回"他主租约"的持有者与剩余时长；无有效他主租约返回空。
func (st *tsoHAState) otherLeaseLocked(now time.Time) (string, time.Duration) {
	if st.seenLeader == "" || st.seenLeader == st.selfID {
		return "", 0
	}
	if now.Before(st.seenUntil) {
		return st.seenLeader, st.seenUntil.Sub(now)
	}
	return "", 0
}

// tsoLeaseQuorum 计算组内多数派门槛（含本节点自身）。
func tsoLeaseQuorum(group []string) int {
	n := 0
	for _, id := range group {
		if id != "" {
			n++
		}
	}
	if n <= 1 {
		return 1
	}
	return n/2 + 1
}

// runElect 处理一次 TSO_ELECT：租约模式下由被请求节点就地拉票并自任。
// 关闭租约时保持 v1.21.0 的 epoch 选主语义（直接委托 elect）。
func (st *tsoHAState) runElect(reqEpoch, observed uint64) TsoElectResp {
	if reqEpoch == 0 {
		reqEpoch = 1
	}
	st.mu.Lock()
	if !st.leaseOn {
		st.mu.Unlock()
		return st.elect(reqEpoch, observed)
	}
	now := st.nowLocked()
	// 本节点已是有效租约主且序列已越过观测值：幂等授予（等于一次续约）。
	if st.leaseValidLocked(now) && st.last >= observed {
		resp := TsoElectResp{
			Granted: true,
			Epoch:   st.epoch,
			Last:    st.last,
			LeaseMs: st.leaseRemainLocked(now).Milliseconds(),
		}
		st.mu.Unlock()
		return resp
	}
	// 他主租约仍在有效期内：本节点无权接任，回执剩余时长让客户端等待重试。
	if holder, remain := st.otherLeaseLocked(now); remain > 0 {
		resp := TsoElectResp{
			Epoch:        st.epoch,
			Last:         st.last,
			Err:          "lease held by " + holder,
			RetryAfterMs: remain.Milliseconds(),
		}
		st.mu.Unlock()
		return resp
	}
	// 自身租约已过期（含旧主失联后被唤醒）：先自行降级，不依赖他人 fence。
	st.leader = false
	st.leaseUntil = time.Time{}
	ne := st.epoch + 1
	// 新纪元还必须越过本地"已投票纪元"：一次失败的竞选会在本地留下
	// votedEpoch=ne / votedFor=self，若下一轮复用同一 ne，组内其它候选
	// 会被 vote() 判为"同纪元已投票"而永远拿不到这关键一票（真机 3 节点
	// 组 1 节点不可达时 grants 恒为 1/2 的死锁）。
	if st.votedEpoch >= ne {
		ne = st.votedEpoch + 1
	}
	if reqEpoch > ne {
		ne = reqEpoch
	}
	// 新主接任前把 last 提升到调用方已观测的最大戳之上：保证首批发号
	// 严格越过旧主发过的所有号（不回退、不重号）。
	if observed > st.last {
		st.last = observed
		st.extendWindowLocked()
	}
	self := st.selfID
	group := append([]string(nil), st.group...)
	roundStart := now
	// 自投：本节点在 ne 纪元把票投给自己，并记下本地"自身持有租约"估计。
	st.votedEpoch, st.votedFor = ne, self
	st.seenLeader, st.seenUntil = self, roundStart.Add(st.leaseTTL)
	observedLast := st.last
	st.mu.Unlock()

	need := tsoLeaseQuorum(group)
	grants := 1
	// 群内已发号水位：投票回执带回各副本的 last（旧主发过的最大号）。
	// 只看调用方 observed 不够——调用方可能刚启动、maxSeen 为 0，
	// 此时必须以副本侧水位为准，否则新主首批号会回退到旧主序列下方 → 重号。
	maxLast := observedLast
	for _, id := range group {
		if id == "" || id == self || grants >= need {
			continue
		}
		resp, err := st.requestLeaseVote(id, TsoElectReq{Epoch: ne, Observed: observedLast, Candidate: self})
		if err == nil && resp.Last > maxLast {
			maxLast = resp.Last
		}
		if err == nil && resp.Granted {
			grants++
		}
	}
	if grants < need {
		// 拉票失败必须完整回滚本地投票态并推进本地纪元，否则本节点会把
		// "在 ne 纪元投过票"永久记住：同纪元其它候选被 vote() 拒票，自己
		// 下一轮又复用同一 ne 反复失败，多数派永远差这一票（死锁）。
		//   - 回滚 votedEpoch/votedFor 与"自身持有租约"估计；
		//   - st.epoch 推进到 ne，使下一次竞选换用更高纪元绕开同纪元互斥。
		// 未当选即推进纪元不产生双主窗口：纪元本身不授予发号权，发号权
		// 只来自多数派授予且未到期的租约窗口（leaseUntil 由授予方各自估计）。
		st.mu.Lock()
		if st.votedEpoch == ne && st.votedFor == self {
			st.votedEpoch, st.votedFor = 0, ""
			st.seenLeader, st.seenUntil = "", time.Time{}
		}
		if st.epoch < ne {
			st.epoch = ne
		}
		st.leader = false
		st.leaseUntil = time.Time{}
		respEpoch, respLast := st.epoch, st.last
		st.mu.Unlock()
		return TsoElectResp{
			Epoch: respEpoch,
			Last:  respLast,
			Err:   fmt.Sprintf("lease quorum not reached (%d/%d)", grants, need),
		}
	}
	st.mu.Lock()
	if st.epoch > ne {
		// 拉票期间观测到更高纪元（他人已接任）：本轮作废，交给客户端下一轮。
		st.seenLeader, st.seenUntil = "", time.Time{}
		resp := TsoElectResp{Epoch: st.epoch, Last: st.last, Err: "stale epoch"}
		st.mu.Unlock()
		return resp
	}
	st.epoch = ne
	// 接任前把窗口抬到群内水位之上：即使调用方 observed 落后，
	// 也保证首批发号严格越过旧主发过的所有号（不回退、不重号）。
	if maxLast > st.last {
		st.last = maxLast
		st.extendWindowLocked()
	}
	st.leader = true
	// 关键：租约到期时刻取"投票发起时刻 + TTL"。发起时刻早于任何副本的
	// 授予时刻，故本节点持有窗口 ⊂ 各副本估计窗口，双主窗口为零。
	st.leaseUntil = roundStart.Add(st.leaseTTL)
	if st.save != nil {
		st.save(st.epoch, st.reserved)
	}
	resp := TsoElectResp{
		Granted: true,
		Epoch:   st.epoch,
		Last:    st.last,
		LeaseMs: st.leaseTTL.Milliseconds(),
	}
	st.mu.Unlock()
	st.kickRenewLoop()
	return resp
}

// vote 处理一次拉票请求（msgTsoLeaseVote）：授予或拒绝把租约交给候选。
//
// 拒绝条件（任一命中即拒绝，保证同刻只有一方能拿到多数派）：
//   - 本节点自身持有未到期租约，且候选不是自己；
//   - 已知他主租约未到期，且持有者不是候选；
//   - 同纪元已把票投给别的候选（防同纪元双主）；
//   - 请求纪元落后于本节点已知纪元。
func (st *tsoHAState) vote(epoch, observed uint64, candidate string) TsoElectResp {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.leaseOn {
		return TsoElectResp{Epoch: st.epoch, Last: st.last, Err: errTSOLeaseDisabled.Error()}
	}
	if epoch == 0 {
		epoch = 1
	}
	if candidate == "" {
		return TsoElectResp{Epoch: st.epoch, Last: st.last, Err: "lease vote: missing candidate"}
	}
	now := st.nowLocked()
	if st.leaseValidLocked(now) && candidate != st.selfID {
		return TsoElectResp{
			Epoch:        st.epoch,
			Last:         st.last,
			Err:          "lease held by " + st.selfID,
			RetryAfterMs: st.leaseRemainLocked(now).Milliseconds(),
		}
	}
	if holder, remain := st.otherLeaseLocked(now); remain > 0 && holder != candidate {
		return TsoElectResp{
			Epoch:        st.epoch,
			Last:         st.last,
			Err:          "lease held by " + holder,
			RetryAfterMs: remain.Milliseconds(),
		}
	}
	if epoch == st.votedEpoch && st.votedFor != "" && st.votedFor != candidate {
		// 同纪元已投给别的候选：拒绝，但并不存在任何他主持有租约，让候选
		// 干等一个 TTL 没有意义（旧实现在此回 TTL，叠加"失败不回滚自投"
		// 后使同一纪元被永久复用 → 永久 1/2）。回执带回本节点已承认的
		// 纪元，候选据此把下一轮抬到更高纪元即可自行打开僵局。
		seen := st.epoch
		if epoch > seen {
			seen = epoch
		}
		return TsoElectResp{
			Epoch: seen,
			Last:  st.last,
			Err:   "lease vote: epoch already voted",
		}
	}
	if epoch < st.epoch {
		return TsoElectResp{Epoch: st.epoch, Last: st.last, Err: "stale epoch"}
	}
	// 授予：记录投票 + 本地租约估计。授予时刻不早于候选的发起时刻，
	// 因此这里的 seenUntil(=授予+TTL) 必然覆盖候选的实际持有窗口。
	// 授予同时承认该纪元（与 elect 的 epoch 语义一致）：落后节点借此追平
	// 集群纪元，"stale epoch" 判定才有意义，也不会被旧纪元请求反复拖回。
	if epoch > st.epoch {
		st.epoch = epoch
	}
	st.votedEpoch, st.votedFor = epoch, candidate
	st.seenLeader, st.seenUntil = candidate, now.Add(st.leaseTTL)
	if observed > st.last {
		st.last = observed
		st.extendWindowLocked()
	}
	return TsoElectResp{
		Granted: true,
		Epoch:   epoch,
		Last:    st.last,
		LeaseMs: st.leaseTTL.Milliseconds(),
	}
}

// renewOnce 执行一轮续约：向多数派续租，成功返回 true。
// 失败时不延长租约——租约自然到期后 serve 即拒绝发号，旧主被动退位。
func (st *tsoHAState) renewOnce() bool {
	st.mu.Lock()
	if !st.leaseOn || !st.leader {
		st.mu.Unlock()
		return false
	}
	now := st.nowLocked()
	if !now.Before(st.leaseUntil) {
		st.leader = false
		st.leaseUntil = time.Time{}
		st.mu.Unlock()
		return false
	}
	roundStart := now
	epoch := st.epoch
	self := st.selfID
	group := append([]string(nil), st.group...)
	st.seenLeader, st.seenUntil = self, roundStart.Add(st.leaseTTL)
	st.mu.Unlock()

	need := tsoLeaseQuorum(group)
	grants := 1
	for _, id := range group {
		if id == "" || id == self || grants >= need {
			continue
		}
		resp, err := st.requestLeaseRenew(id, TsoLeaseRenewReq{Epoch: epoch, Leader: self})
		if err == nil && resp.OK {
			grants++
		}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.leader || st.epoch != epoch {
		return false
	}
	if grants < need {
		return false
	}
	until := roundStart.Add(st.leaseTTL)
	if until.After(st.leaseUntil) {
		st.leaseUntil = until
	}
	return true
}

// applyRenew 处理一次续约请求（msgTsoLeaseRenew）。
func (st *tsoHAState) applyRenew(epoch uint64, leader string) TsoLeaseRenewResp {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.leaseOn {
		return TsoLeaseRenewResp{Err: errTSOLeaseDisabled.Error()}
	}
	if leader == "" || leader == st.selfID {
		return TsoLeaseRenewResp{Epoch: st.epoch, Err: "lease renew: bad leader"}
	}
	if epoch < st.epoch {
		return TsoLeaseRenewResp{Epoch: st.epoch, Err: "stale epoch"}
	}
	now := st.nowLocked()
	if st.leaseValidLocked(now) {
		// 本节点还持有有效租约，不认可另一个主（互斥优先于续约）。
		return TsoLeaseRenewResp{Epoch: st.epoch, Err: "lease held by " + st.selfID}
	}
	if epoch > st.epoch {
		st.epoch = epoch
		if st.save != nil {
			st.save(st.epoch, st.reserved)
		}
	}
	// 续约同时等价于"确认该纪元的主是 leader"，锁死同纪元的其它候选。
	st.votedEpoch, st.votedFor = epoch, leader
	st.seenLeader, st.seenUntil = leader, now.Add(st.leaseTTL)
	return TsoLeaseRenewResp{OK: true, Epoch: st.epoch}
}

// commitLease 处理租约模式的降级通知（TSO_FENCE）：新主公告 epoch 与租约。
// 更高纪元到达即降级，并记录新主租约的本地估计。
func (st *tsoHAState) commitLease(epoch, last uint64, candidate string, leaseMs int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	now := st.nowLocked()
	if epoch > st.epoch {
		st.epoch = epoch
		if candidate != st.selfID {
			st.leader = false
			st.leaseUntil = time.Time{}
		}
		st.votedEpoch, st.votedFor = epoch, candidate
		if st.save != nil {
			st.save(st.epoch, st.reserved)
		}
	}
	if candidate != "" && candidate != st.selfID {
		until := now.Add(st.leaseTTL)
		if leaseMs > 0 {
			until = now.Add(time.Duration(leaseMs) * time.Millisecond)
		}
		if until.After(st.seenUntil) {
			st.seenLeader, st.seenUntil = candidate, until
		}
	}
	if last > st.last {
		st.last = last
		st.extendWindowLocked()
	}
}

// renewLoop 周期性续约；失去主身份即退出（下次抢主时重新拉起）。
func (st *tsoHAState) renewLoop() {
	st.mu.Lock()
	interval := st.leaseRenew
	stop := st.renewStop
	st.mu.Unlock()
	if interval <= 0 {
		interval = DefaultTSOLeaseRenew
	}
	if stop == nil {
		return
	}
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tk.C:
			if !st.renewOnce() {
				st.mu.Lock()
				leader := st.leader
				st.mu.Unlock()
				if !leader {
					return
				}
			}
		}
	}
}

// kickRenewLoop 启动续约协程（同一时刻最多一个）。
func (st *tsoHAState) kickRenewLoop() {
	st.mu.Lock()
	if !st.leaseOn || st.renewing || st.renewStop == nil || (st.mgr == nil && st.renewHook == nil) {
		st.mu.Unlock()
		return
	}
	st.renewing = true
	st.mu.Unlock()
	go func() {
		defer func() {
			st.mu.Lock()
			st.renewing = false
			st.mu.Unlock()
		}()
		st.renewLoop()
	}()
}

// requestLeaseVote 向单个副本请求授予租约（自锁外调用）。
func (st *tsoHAState) requestLeaseVote(id string, req TsoElectReq) (TsoElectResp, error) {
	if st.voteHook != nil {
		return st.voteHook(id, req)
	}
	if st.mgr == nil {
		return TsoElectResp{}, errors.New("cluster: tso ha manager not initialized")
	}
	addr, err := st.mgr.NodeAddr(id)
	if err != nil || addr == "" {
		return TsoElectResp{}, fmt.Errorf("cluster: tso lease peer %s unresolved", id)
	}
	conn, err := dial(addr)
	if err != nil {
		return TsoElectResp{}, err
	}
	defer conn.Close()
	payload, err := marshalJSON(req)
	if err != nil {
		return TsoElectResp{}, err
	}
	if err := writeFrame(conn, msgTsoLeaseVote, payload); err != nil {
		return TsoElectResp{}, err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return TsoElectResp{}, err
	}
	if typ != msgTsoLeaseVote {
		return TsoElectResp{}, errors.New("cluster: unexpected tso lease vote reply")
	}
	var resp TsoElectResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return TsoElectResp{}, err
	}
	return resp, nil
}

// requestLeaseRenew 向单个副本续约（自锁外调用）。
func (st *tsoHAState) requestLeaseRenew(id string, req TsoLeaseRenewReq) (TsoLeaseRenewResp, error) {
	if st.renewHook != nil {
		return st.renewHook(id, req)
	}
	if st.mgr == nil {
		return TsoLeaseRenewResp{}, errors.New("cluster: tso ha manager not initialized")
	}
	addr, err := st.mgr.NodeAddr(id)
	if err != nil || addr == "" {
		return TsoLeaseRenewResp{}, fmt.Errorf("cluster: tso lease peer %s unresolved", id)
	}
	conn, err := dial(addr)
	if err != nil {
		return TsoLeaseRenewResp{}, err
	}
	defer conn.Close()
	payload, err := marshalJSON(req)
	if err != nil {
		return TsoLeaseRenewResp{}, err
	}
	if err := writeFrame(conn, msgTsoLeaseRenew, payload); err != nil {
		return TsoLeaseRenewResp{}, err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return TsoLeaseRenewResp{}, err
	}
	if typ != msgTsoLeaseRenew {
		return TsoLeaseRenewResp{}, errors.New("cluster: unexpected tso lease renew reply")
	}
	var resp TsoLeaseRenewResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return TsoLeaseRenewResp{}, err
	}
	return resp, nil
}

// EnableTSOLease 在节点服务端启用 TSO 租约兜底与失效切换（v2.1.0 H1/NEW-5）。
//
// 前置条件：已调用 EnableTSOHA（租约是 epoch 选主的上层，不替代它）。
// 默认关闭：未调用本方法时全部路径保持 v1.21.0 行为。
//   - ttl：租约有效期（<=0 用 DefaultTSOLeaseTTL），决定主失联后最迟多久可被接管；
//   - renew：续约间隔（<=0 用 ttl/3），续约失败时租约到期即自降。
//
// 启用瞬间若本节点已是主，先按 TTL 自授一份租约并启动续约协程：
// 续约拿不到多数派时租约自然过期，本节点随即退位，不会造成双主。
func (s *Server) EnableTSOLease(ttl, renew time.Duration) error {
	st := s.tsoHAState()
	if st == nil {
		return errors.New("cluster: tso ha not enabled (call EnableTSOHA first)")
	}
	st.mu.Lock()
	st.enableLease(ttl, renew)
	leader := st.leader
	if leader {
		st.leaseUntil = st.nowLocked().Add(st.leaseTTL)
		st.seenLeader, st.seenUntil = st.selfID, st.leaseUntil
	}
	st.mu.Unlock()
	if leader {
		st.kickRenewLoop()
	}
	return nil
}

// TSOLeaseTTL 返回本节点租约有效期；未启用租约返回 0（供客户端配置等待窗口）。
func (s *Server) TSOLeaseTTL() time.Duration {
	st := s.tsoHAState()
	if st == nil {
		return 0
	}
	return st.leaseTTLValue()
}

// TSOLeaseSnapshot 返回租约观测快照（运维/测试用）：
// enabled、本节点是否持有有效租约、剩余毫秒、观测到的他主及其剩余毫秒。
func (s *Server) TSOLeaseSnapshot() (enabled, leader bool, remainMs, otherMs int64, other string) {
	st := s.tsoHAState()
	if st == nil {
		return false, false, 0, 0, ""
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.leaseOn {
		return false, false, 0, 0, ""
	}
	now := st.nowLocked()
	holder, remain := st.otherLeaseLocked(now)
	return true, st.leaseValidLocked(now), st.leaseRemainLocked(now).Milliseconds(), remain.Milliseconds(), holder
}

// handleTsoLeaseVote 服务端处理拉票请求。
func (s *Server) handleTsoLeaseVote(conn net.Conn, payload []byte) {
	var req TsoElectReq
	if err := unmarshalJSON(payload, &req); err != nil {
		_ = writeFrame(conn, msgTsoLeaseVote, mustJSON(TsoElectResp{Err: "bad request"}))
		return
	}
	st := s.tsoHAState()
	if st == nil {
		_ = writeFrame(conn, msgTsoLeaseVote, mustJSON(TsoElectResp{Err: "tso ha not enabled"}))
		return
	}
	_ = writeFrame(conn, msgTsoLeaseVote, mustJSON(st.vote(req.Epoch, req.Observed, req.Candidate)))
}

// handleTsoLeaseRenew 服务端处理续约请求。
func (s *Server) handleTsoLeaseRenew(conn net.Conn, payload []byte) {
	var req TsoLeaseRenewReq
	if err := unmarshalJSON(payload, &req); err != nil {
		_ = writeFrame(conn, msgTsoLeaseRenew, mustJSON(TsoLeaseRenewResp{Err: "bad request"}))
		return
	}
	st := s.tsoHAState()
	if st == nil {
		_ = writeFrame(conn, msgTsoLeaseRenew, mustJSON(TsoLeaseRenewResp{Err: "tso ha not enabled"}))
		return
	}
	_ = writeFrame(conn, msgTsoLeaseRenew, mustJSON(st.applyRenew(req.Epoch, req.Leader)))
}

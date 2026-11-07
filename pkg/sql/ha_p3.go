package sql

// P3 HA（D4.5 报告 §6 第 3 项 / T3）：集群轨写入纳入 epoch/归属授权。
//
// 缺口（D4.3 演练 T3 GAP）：db.AllowWrite 仅在 masterEpoch>0 时生效，而集群轨
// （无复制轨、不经过 StartReplication/PromoteToMaster）masterEpoch 恒为 0，
// 写路径恒放行。双向网络分区时被隔离的旧主仍按本地 region 归属视图接受写入，
// 分区恢复后也不自动收敛——陈旧主与多数派新 owner 并发写入同一 region。
//
// 本文件把集群轨写入纳入 epoch/归属授权，分三层：
//   - clusterFenceCheck（语句级，INSERT/UPDATE/DELETE 入口）：ha_epoch_enabled
//     与 ha_quorum_enabled 双开时，写入需多数派授权；本节点处于分区少数派
//     （Reachable < N/2+1）即拒绝写入，陈旧主不再静默提交本地写。多数派探测
//     结果按 TTL 缓存，避免逐语句 PING 放大；恢复为多数派后自动放行。
//   - regionOwnerFence（region 级，本地 op 落盘前）：以本机 epoch 快照
//     （p1Epoch，regionEpoch 大者胜）对照路由视图，该 region 已被更高
//     regionEpoch 指派给其它 owner 而本地路由条目仍陈旧时拒绝本地提交，
//     防止恢复窗口内旧主继续写已被接管的 region。
//   - OnPartitionRecovered（分区恢复收敛，少数派 → 多数派）：清空多数派
//     探测缓存（写路径立即重探并放行）、按 epoch 大者胜收敛路由视图
//     （复用 P2 ReconcileRegionView）、并向多数派拉取权威路由视图
//     （p3RouteResync，db.StartCluster 装配），使旧主自动收敛为新 owner 的
//     远端路由（其写转为跨节点写并由归属强制路由拒绝）。
//
// 零回归：epoch/quorum 任一未装配或开关关闭时上述三层全部直接放行/空转，
// 单机与默认配置行为不变（ha_epoch_enabled 默认开、ha_quorum_enabled 默认关，
// 故本文件在生产默认配置下不产生任何拦截）。

import (
	"fmt"
	"log"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// p3FenceTTL 多数派探测结果缓存有效期：写路径逐语句复用同一探测结果，
// 避免每条 DML 都对全部节点 PING 放大网络开销；TTL 内状态变化最多延迟
// 一个窗口生效（分区判定本身已是秒级语义）。
const p3FenceTTL = 300 * time.Millisecond

// P3 集群轨写 fencing 标准错误前缀（SQLError.Msg 统一以该前缀开头，便于
// 客户端/演练脚本按前缀识别"分区期写入被拒"，不依赖具体措辞）。
const p3FencedPrefix = "cluster track write fenced"

// clusterFenceEnabled 判定集群轨写 fencing 是否生效：需 epoch 授权装配
// （p1Enabled：取号源 + 仲裁器 + 开关）且 quorum 仲裁开关开启。
// 任一不满足 → 不拦截（默认配置零回归）。
func (e *Executor) clusterFenceEnabled() bool {
	if e.quorumArb == nil {
		return false
	}
	if e.haCfg == nil || !e.haCfg.QuorumEnabled() {
		return false
	}
	return e.p1Enabled()
}

// majorityProbe 返回本节点当前是否满足多数派（带 TTL 缓存）。
// quorumOK 已内含"仲裁器未装配/quorum 关闭 → 放行"语义，此处只做缓存包装。
func (e *Executor) majorityProbe() bool {
	e.p3Mu.Lock()
	if !e.p3At.IsZero() && time.Since(e.p3At) < p3FenceTTL {
		ok := e.p3Majority
		e.p3Mu.Unlock()
		return ok
	}
	e.p3Mu.Unlock()

	err := e.writeQuorumOK()
	ok := err == nil

	e.p3Mu.Lock()
	e.p3Majority = ok
	e.p3At = time.Now()
	e.p3Mu.Unlock()
	return ok
}

// writeQuorumArbiter 可选接口：仲裁器若提供写路径专用判定则优先使用。
// db.DB 实现了该方法（单机/未组网放行，组网后按多数派），升主/重指派仍用
// QuorumArbiter.QuorumOK（单节点规模的集群直接拒绝升主）。
type writeQuorumArbiter interface {
	WriteQuorumOK() error
}

// writeQuorumOK 取写路径多数派授权：优先仲裁器的 WriteQuorumOK，未实现时
// 回退 QuorumOK（旧实现语义）。
func (e *Executor) writeQuorumOK() error {
	if e.quorumArb == nil {
		return nil
	}
	if wq, ok := e.quorumArb.(writeQuorumArbiter); ok {
		return wq.WriteQuorumOK()
	}
	return e.quorumArb.QuorumOK()
}

// clusterFenceCheck 语句级集群轨写入授权校验（写路径入口调用）。
// 返回 nil 表示允许写；返回 SQLError 表示本节点处于分区少数派，写入被拒。
func (e *Executor) clusterFenceCheck() error {
	if !e.clusterFenceEnabled() {
		return nil
	}
	if e.majorityProbe() {
		return nil
	}
	e.p3Mu.Lock()
	e.p3Fenced++
	e.p3Mu.Unlock()
	return &SQLError{Msg: p3FencedPrefix + ": minority partition, write requires majority (N/2+1) authorization; " +
		"this node lost region ownership quorum, wait for partition recovery and route convergence"}
}

// regionOwnerFence region 级写入授权校验：本机 epoch 快照记录该 region 已被
// 更高 regionEpoch 指派给其它 owner，而本地路由条目仍停留在旧纪元（视图陈旧）
// 时拒绝本地提交，避免恢复窗口内旧主继续写已被接管的 region。
// 未启用 P1（epoch 校验）/ 无该 region 快照 → 放行（零回归）。
func (e *Executor) regionOwnerFence(rid storage.ID) error {
	if !e.p1Enabled() {
		return nil
	}
	// NEW-1 修复（重启双主分叉）：本机启动时装载了磁盘持久化的归属视图，且
	// 尚未与权威源确认（join 路由同步/权威视图拉取未到达），此时本机对该
	// region 的归属判断仅来自重启前的旧快照，可能与多数派新 owner 相反。
	// 集群 + quorum + epoch 全开且存在远端节点时，禁止本地 region 落盘写，
	// 必须等权威视图收敛（收到并接受权威路由消息即自动解除），避免"重启
	// 节点写 70、多数派写 71/72"的双写分叉与二次重启后的静默丢数据。
	if !e.regionViewConfirmed() && e.clusterFenceEnabled() && e.hasRemotePeer() {
		e.p3Mu.Lock()
		e.p3Fenced++
		e.p3Mu.Unlock()
		return &SQLError{Msg: fmt.Sprintf(
			"%s: region %d write rejected, region ownership view not yet confirmed by authoritative source "+
				"(node restarted with persisted route view; retry after route sync)", p3FencedPrefix, rid)}
	}
	e.p1Mu.Lock()
	ep, ok := e.p1Epoch[rid]
	e.p1Mu.Unlock()
	if !ok {
		return nil
	}
	rg, exists := e.router.FindRegion(rid)
	if !exists {
		return nil
	}
	if rg.RegionEpoch >= ep.regionEpoch {
		return nil // 路由视图已收敛到快照纪元（owner 变更已生效，走归属路由）
	}
	e.p3Mu.Lock()
	e.p3Fenced++
	e.p3Mu.Unlock()
	// NEW-1 修复：快照纪元已前进但本地路由条目仍停留在旧纪元时，无论快照是否
	// 记录了新 owner（重启节点采纳权威纪元时 owner 尚未知），一律视为归属视图
	// 陈旧并拒绝本地提交——不允许重启节点在视图收敛前按旧归属继续落盘。
	owner := ep.owner
	if owner == "" {
		owner = "unknown"
	}
	return &SQLError{Msg: fmt.Sprintf(
		"%s: region %d reassigned to %s at ownership epoch %d (term %d), local route view epoch %d is stale",
		p3FencedPrefix, rid, owner, ep.regionEpoch, ep.clusterEpoch, rg.RegionEpoch)}
}

// hasRemotePeer 本节点是否已注册除自身以外的节点（多节点集群）。
// 单机/未装配集群返回 false：归属视图确认栅栏仅在存在权威源时生效（零回归）。
func (e *Executor) hasRemotePeer() bool {
	if e.mgr == nil {
		return false
	}
	for _, n := range e.mgr.ListNodes() {
		if n.ID != e.selfID {
			return true
		}
	}
	return false
}

// scheduleRouteResync 异步触发权威路由视图拉取（p3RouteResync 未装配时空操作）。
// 供回迁陈旧失败等"本机视图落后"场景主动收敛，不阻塞调用方。
func (e *Executor) scheduleRouteResync() {
	e.p3Mu.Lock()
	fn := e.p3RouteResync
	e.p3Mu.Unlock()
	if fn == nil {
		return
	}
	go fn()
}

// SetPartitionRouteResync 装配分区恢复后的权威路由拉取钩子（db.StartCluster
// 调用；nil = 不拉取）。恢复回调中异步执行，不阻塞心跳/写路径。
func (e *Executor) SetPartitionRouteResync(fn func()) {
	e.p3Mu.Lock()
	defer e.p3Mu.Unlock()
	e.p3RouteResync = fn
}

// OnPartitionRecovered 分区恢复（少数派 → 多数派）后的自动收敛：
//   - 以恢复时的视图 version 更新本机已知最大 version（大者胜，陈旧分区
//     的低 version 视图后续不得覆盖）；
//   - 清空多数派探测缓存：下一次写入立即重探并放行（fencing 解除）；
//   - 按 epoch 大者胜收敛路由视图（ReconcileRegionView）；
//   - 异步向多数派拉取权威路由视图（p3RouteResync），修正本地陈旧 owner。
//
// 由 cluster.Manager.CheckPartitionRecovery 的状态转换回调触发（db 装配），
// 保证"恢复后自动收敛"不依赖人工介入。
func (e *Executor) OnPartitionRecovered(version uint64) {
	if e.mgr != nil {
		e.mgr.TakeViewVersion(version)
	}
	e.p3Mu.Lock()
	e.p3At = time.Time{} // 清缓存 → 下一次写路径重新探测多数派
	e.p3Recovered++
	resync := e.p3RouteResync
	e.p3Mu.Unlock()

	if err := e.ReconcileRegionView(); err != nil {
		log.Printf("sql: partition recovered: reconcile region view: %v", err)
	}
	if resync != nil {
		go resync()
	}
}

// FencedWriteCount 返回本节点因分区少数派 / region 归属陈旧被拒绝的写入次数
// （D4.5 报告 §5 观测项；默认配置恒 0）。
func (e *Executor) FencedWriteCount() uint64 {
	e.p3Mu.Lock()
	defer e.p3Mu.Unlock()
	return e.p3Fenced
}

// PartitionRecoveredCount 返回本节点检测到"分区恢复"并完成收敛的次数。
func (e *Executor) PartitionRecoveredCount() uint64 {
	e.p3Mu.Lock()
	defer e.p3Mu.Unlock()
	return e.p3Recovered
}

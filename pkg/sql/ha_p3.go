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
	e.p1Mu.Lock()
	ep, ok := e.p1Epoch[rid]
	e.p1Mu.Unlock()
	if !ok || ep.owner == "" || ep.owner == e.selfID {
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
	return &SQLError{Msg: fmt.Sprintf(
		"%s: region %d reassigned to node %s at regionEpoch %d, local route view epoch %d is stale",
		p3FencedPrefix, rid, ep.owner, ep.regionEpoch, rg.RegionEpoch)}
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

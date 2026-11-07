package sql

// P1 HA 防路由分裂（集群轨）：在 P0（epoch/quorum 机制）基础上，
// 对 region 重指派引入"两步校验"（持有校验 + epoch 校验），保证分区
// 双端并发重指派至多一端成功，陈旧 failover 条目失效。
//
// 机制（详见 docs/HA_BRAIN_SPLIT_DESIGN.md §3.3）：
//   - regionEpoch：per-region owner 变更纪元。重指派成功后 +1，写入路由
//     持久化（RegionInfo.RegionEpoch）与本机快照（p1Epoch）；随路由广播
//     携带，接收端对照本地快照，陈旧消息拒绝更新路由并记日志。
//   - 持有校验：failoverRegion 选择目标副本后、推送路由前，调用
//     regionHoldsLocal(rg) 验证目标真实持有该 region 键空间数据；
//     不通过则跳过该候选，尝试下一候选。
//   - resolveRegionOwner：读取 failover 路由时对照 p1Epoch 快照，
//     陈旧 failover 条目视为无效（返回原归属/触发重指派，不采用陈旧决策）。
//   - FailoverDownNode：执行前复用 P0 quorumGuard（quorum 多数派确认或
//     clusterEpoch 授权）；quorum 不足降级只读，不执行重指派。
//   - 开关兼容：ha_epoch_enabled / ha_quorum_enabled 默认关闭时 M6 旧行为
//     零回归；新旧节点混合时旧节点消息不带 epoch（clusterEpoch=0）跳过校验
//     （版本协商防呆）。
//
// 启用条件（e.epochSrc != nil && e.quorumArb != nil 且 haCfg.EpochEnabled()）：
//   - 未装配（单机/M6 兼容）：epoch 校验恒放行，FailoverDownNode 不强制 quorum；
//   - haCfg 缺失或 epoch 开关关闭：消息不带 epoch 语义（PushRegionEpoch 仍可
//     被调用但接收端 clusterEpoch=0 跳过校验——发送侧同样不取新号，
//     见 failoverRegion 条件判断），零回归。

import (
	"fmt"
	"log"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// p1RegionEpoch 本机已重指派 region 的 epoch 快照（大者胜）。
type p1RegionEpoch struct {
	clusterEpoch uint64 // 完成重指派的 clusterEpoch（消息级全局纪元）
	regionEpoch  uint64 // 该 region owner 变更纪元
	owner        string // 该纪元对应的 owner（重指派/收敛后的路由归属）
}

// holdsChecker 可注入持有校验钩子（默认 nil = 使用 regionHoldsLocalTarget
// 真实键空间扫描）。测试/特殊部署可注入以隔离网络与存储依赖。
type holdsChecker func(rg sharding.RegionInfo, target string) (bool, error)

// holdsRegion 执行目标节点持有校验：优先注入钩子，否则走真实键空间扫描。
func (e *Executor) holdsRegion(rg sharding.RegionInfo, target string) (bool, error) {
	if e.holdsChecker != nil {
		return e.holdsChecker(rg, target)
	}
	return e.regionHoldsLocalTarget(rg, target)
}

// p1Enabled 判定 P1 防路由分裂是否启用：取号源与仲裁器均已装配，
// 且 epoch 开关开启（默认关闭 → 零回归；haCfg 缺失等同关闭）。
func (e *Executor) p1Enabled() bool {
	if e.epochSrc == nil || e.quorumArb == nil {
		return false
	}
	if e.haCfg != nil && !e.haCfg.EpochEnabled() {
		return false
	}
	return true
}

// autoFailoverEnabled 自动故障转移开关：quorum 开启后自动重指派需多数派确认；
// quorum 关闭时保留 M6 自动行为（零回归）。供 cluster 心跳装配 / 人工入口判断。
func (e *Executor) autoFailoverEnabled() bool {
	if e.quorumArb == nil {
		return true
	}
	if e.haCfg != nil && !e.haCfg.QuorumEnabled() {
		return true
	}
	// quorum 开启：自动升主/重指派需多数派确认，见 FailoverDownNode 前置仲裁。
	return true
}

// quorumOK 复用 P0 quorum 多数派仲裁：通过则返回 nil 可执行重指派；
// 失败返回错误（调用方降级只读，不重指派）。
// 仲裁器未装配或 quorum 开关关闭时放行（M6 旧行为零回归）。
func (e *Executor) quorumOK() error {
	if e.quorumArb == nil {
		return nil
	}
	if e.haCfg != nil && !e.haCfg.QuorumEnabled() {
		return nil
	}
	return e.quorumArb.QuorumOK()
}

// regionEpochOf 读取指定 region 当前 owner 变更纪元：
// 优先取路由条目携带值（持久化值，重指派成功即写入）；
// 无路由条目时回退本机快照；均无则 0。
func (e *Executor) regionEpochOf(rid storage.ID) uint64 {
	if rg, ok := e.router.FindRegion(rid); ok {
		return rg.RegionEpoch
	}
	e.p1Mu.Lock()
	defer e.p1Mu.Unlock()
	if ep, ok := e.p1Epoch[rid]; ok {
		return ep.regionEpoch
	}
	return 0
}

// recordRegionEpoch 记录 region 重指派后的 epoch 快照（大者胜）。
func (e *Executor) recordRegionEpoch(rid storage.ID, clusterEpoch, regionEpoch uint64) {
	e.p1Mu.Lock()
	defer e.p1Mu.Unlock()
	if old, ok := e.p1Epoch[rid]; ok && old.regionEpoch >= regionEpoch && old.clusterEpoch >= clusterEpoch {
		return
	}
	e.p1Epoch[rid] = p1RegionEpoch{clusterEpoch: clusterEpoch, regionEpoch: regionEpoch}
}

// staleFailover 判定 failover 路由条目是否陈旧：
//   - 未启用 P1 → 不陈旧（M6 旧行为）；
//   - 本机无该 region 快照 → 不陈旧（未重指派过，兼容首次学习）；
//   - 路由条目 RegionEpoch < 快照 regionEpoch → 陈旧（曾被更新纪元取代）。
func (e *Executor) staleFailover(rg sharding.RegionInfo) bool {
	if !e.p1Enabled() {
		return false
	}
	e.p1Mu.Lock()
	ep, ok := e.p1Epoch[rg.RegionID]
	e.p1Mu.Unlock()
	if !ok {
		return false
	}
	return rg.RegionEpoch < ep.regionEpoch
}

// regionEpochView 读取本机 region 归属快照（term + epoch + owner）。
func (e *Executor) regionEpochView(rid storage.ID) (p1RegionEpoch, bool) {
	e.p1Mu.Lock()
	defer e.p1Mu.Unlock()
	ep, ok := e.p1Epoch[rid]
	return ep, ok
}

// epochOlder 判定 (termA, epochA) 是否严格早于 (termB, epochB)：
// 以 clusterEpoch（全局单调的归属授权纪元）为第一比较键、regionEpoch 为第二
// 比较键的字典序。
//
// NEW-1 修复（重启双主分叉）：此前只用 regionEpoch 单键比较，而该值是各节点
// 本地自增序号，跨节点不可比——重启节点本地 known=2 会把多数派权威指派的
// epoch=1 判为陈旧而拒收（日志 `stale region epoch 1 < known 2`），保留旧
// 归属视图并与多数派形成双主。改为 term 优先的字典序后，权威指派不再更旧。
func epochOlder(termA, epochA, termB, epochB uint64) bool {
	if termA != termB {
		return termA < termB
	}
	return epochA < epochB
}

// markRegionViewConfirmed 标记本机 region 归属视图已与权威源对齐（收到并接受
// 带 epoch 的权威路由消息、或完成权威视图拉取后调用），解除重启后的归属视图
// 未确认写栅栏（见 ha_p3.go regionOwnerFence）。
func (e *Executor) markRegionViewConfirmed() {
	e.p1Mu.Lock()
	e.p1ViewConfirmed = true
	e.p1ViewUnconfirmed = false
	e.p1Mu.Unlock()
}

// MarkRegionViewConfirmed 对外入口（db 装配：权威路由视图拉取成功后调用）。
func (e *Executor) MarkRegionViewConfirmed() { e.markRegionViewConfirmed() }

// MarkRegionOwnershipUnconfirmed 标记"本机启动时装载了磁盘持久化的 region 归属
// 视图"（db.Open 装载 m:regions 后调用）。与权威源确认之前，集群 + quorum +
// epoch 全开场景禁止本地 region 落盘写（NEW-1 双主分叉的写路径熔断）。
func (e *Executor) MarkRegionOwnershipUnconfirmed() {
	e.p1Mu.Lock()
	e.p1ViewUnconfirmed = true
	e.p1ViewConfirmed = false
	e.p1Mu.Unlock()
}

// regionViewConfirmed 本机归属视图是否已由权威源确认（未进入未确认状态即视为
// 确认，保证非重启路径与既有行为零回归）。
func (e *Executor) regionViewConfirmed() bool {
	e.p1Mu.Lock()
	defer e.p1Mu.Unlock()
	return e.p1ViewConfirmed || !e.p1ViewUnconfirmed
}

// authorizeRegionEpoch 把 region 归属 epoch 纳入 quorum 裁决：本节点重指派取得
// 的 clusterEpoch 必须不落后于本机已观测到的任一 region 归属授权纪元，否则说明
// 本机视图落后于多数派（已知更新的授权却按旧授权重指派）→ 拒绝重指派、降级
// 只读等待收敛（NEW-1：避免重启节点凭旧视图自升主形成双主）。
func (e *Executor) authorizeRegionEpoch(clusterEpoch uint64) error {
	e.p1Mu.Lock()
	defer e.p1Mu.Unlock()
	for rid, ep := range e.p1Epoch {
		if ep.clusterEpoch > clusterEpoch {
			return fmt.Errorf("sql: region %d ownership epoch %d newer than quorum epoch %d (region view behind quorum)",
				rid, ep.clusterEpoch, clusterEpoch)
		}
	}
	return nil
}

// adoptRegionEpoch 采纳权威源的 region 归属纪元（term/epoch 大者胜），同时把
// 本机视图标记为已由权威源确认。
func (e *Executor) adoptRegionEpoch(rid storage.ID, clusterEpoch, regionEpoch uint64) {
	e.p1Mu.Lock()
	defer e.p1Mu.Unlock()
	old, ok := e.p1Epoch[rid]
	if !ok || !epochOlder(clusterEpoch, regionEpoch, old.clusterEpoch, old.regionEpoch) {
		adopted := p1RegionEpoch{clusterEpoch: clusterEpoch, regionEpoch: regionEpoch}
		if ok {
			adopted.owner = old.owner
		}
		e.p1Epoch[rid] = adopted
	}
	e.p1ViewConfirmed = true
	e.p1ViewUnconfirmed = false
}

// checkRegionPushEpoch 节点侧 P1 路由消息 epoch 校验（cluster.Server 装配调用）：
//   - 未启用 P1（epochSrc 未装配 / 开关关闭）→ 放行（零回归）；
//   - 消息不带 clusterEpoch（=0，旧节点 / ASSIGN 旧路径）→ 放行（版本协商防呆）；
//   - clusterEpoch < 本机已知最大 → 陈旧消息拒绝（分区双端至多一端获胜）；
//   - 消息归属纪元 (clusterEpoch, regionEpoch) 键序落后于本机快照 → 拒绝；
//   - 本机无该 region 归属快照（重启后尚未与权威源交换归属信息）→ 以权威源
//     为准采纳（NEW-1 修复：旧实现用本地路由条目的本地自增 epoch 对照，重启
//     节点因此拒收多数派新归属并保留旧视图）。
//
// 校验失败返回错误，接收端拒绝更新路由并记日志。
func (e *Executor) checkRegionPushEpoch(clusterEpoch, regionEpoch uint64, rid storage.ID) error {
	if !e.p1Enabled() {
		return nil
	}
	if clusterEpoch == 0 {
		// 旧节点消息（不识别 epoch 字段）：版本协商防呆，跳过校验。
		return nil
	}
	if e.epochSrc == nil {
		return nil
	}
	if clusterEpoch < e.epochSrc.Epoch() {
		e.bumpRegionConflict() // 数据冲突标记：陈旧路由消息被拒
		return fmt.Errorf("sql: stale cluster epoch %d < known %d (rid=%d)", clusterEpoch, e.epochSrc.Epoch(), rid)
	}
	ep, ok := e.regionEpochView(rid)
	if !ok {
		// 无归属快照：本机对该 region 从未采纳过任何归属纪元，以权威源为准。
		e.adoptRegionEpoch(rid, clusterEpoch, regionEpoch)
		return nil
	}
	if epochOlder(clusterEpoch, regionEpoch, ep.clusterEpoch, ep.regionEpoch) {
		e.bumpRegionConflict() // 数据冲突标记：陈旧路由消息被拒
		return fmt.Errorf("sql: stale region epoch %d (term %d) < known %d (term %d) (rid=%d)",
			regionEpoch, clusterEpoch, ep.regionEpoch, ep.clusterEpoch, rid)
	}
	// 接受：刷新快照（owner 由后续路由学习/收敛补齐）并标记视图已确认。
	e.adoptRegionEpoch(rid, clusterEpoch, regionEpoch)
	return nil
}

// nextRegionEpoch 在 clusterEpoch 授权后为本 region 取新 owner 纪元：
// 取 max(本机已知 + 1, clusterEpoch)，并记录到本机快照。
//
// NEW-1 修复：旧实现恒为本地序号 +1，epoch 跨节点不可比；新值与授权用
// clusterEpoch 同域（全局单调），全网可直接按字典序比较，权威指派不会再被
// 重启节点误判为陈旧。
func (e *Executor) nextRegionEpoch(rid storage.ID, clusterEpoch uint64) uint64 {
	next := e.regionEpochOf(rid) + 1
	if clusterEpoch > next {
		next = clusterEpoch
	}
	e.recordRegionEpoch(rid, clusterEpoch, next)
	return next
}

// failoverRegionP1 failoverRegion 的 P1 增强版（由 FailoverDownNode 调用）：
// 在原有"选副本 → 推送路由"之间插入两步校验：
//  1. quorum 前置：FailoverDownNode 已做（本函数不再重复）；
//  2. epoch 授权：启用 P1 时通过 epochSrc.AcquireEpoch() 取新 clusterEpoch；
//  3. 持有校验：逐个候选目标调用 regionHoldsLocal(rg) 验证真实持有，
//     不通过则跳过该候选尝试下一候选；
//  4. epoch 校验：重指派消息携带 clusterEpoch + regionEpoch，接收端
//     （checkRegionPushEpoch）校验通过才更新路由，陈旧消息被拒。
//
// 保持与 failoverRegion 相同的返回语义：成功 nil；跳过全部候选返回明确错误。
func (e *Executor) failoverRegionP1(rg sharding.RegionInfo, downID string) error {
	if !e.p1Enabled() {
		// 开关关闭：走 M6 旧路径（failoverRegion 内部不取号、消息不带 epoch）。
		return e.failoverRegion(rg, downID)
	}
	// epoch 授权：多数派确认（FailoverDownNode 前置）已通过，此处取新号。
	clusterEpoch := e.epochSrc.AcquireEpoch()
	// region 归属 epoch 纳入 quorum 裁决（NEW-1）：本机若已知更新的归属授权
	// 纪元，说明视图落后于多数派，拒绝按旧授权重指派（降级只读等待收敛）。
	if err := e.authorizeRegionEpoch(clusterEpoch); err != nil {
		return err
	}
	// 候选目标：存活远端（UP 且非 down 且非自身），逐个持有校验，首个通过即用；
	// 全部不通过则回本节点（本机持有校验通过才允许落本地路由）。
	for _, n := range e.mgr.ListNodes() {
		if n.ID == e.selfID || n.ID == downID || n.State == cluster.StateDown {
			continue
		}
		if err := e.tryFailoverTargetP1(rg, downID, n.ID, clusterEpoch); err == nil {
			return nil
		} else if isHeldElsewhere(err) {
			// 该候选未持有目标 region 键空间：跳过尝试下一候选。
			log.Printf("sql: failover skip candidate %s for region %d: %v", n.ID, rg.RegionID, err)
			continue
		} else {
			return err
		}
	}
	// 无存活远端持有：回本节点（本机真实持有才允许）。
	if err := e.tryFailoverTargetP1(rg, downID, e.selfID, clusterEpoch); err == nil {
		return nil
	} else if isHeldElsewhere(err) {
		return &SQLError{Msg: fmt.Sprintf("no live data copy for region %d (node %s down)", rg.RegionID, downID)}
	} else {
		return err
	}
}

// tryFailoverTargetP1 将 region rg 重指派到候选目标 target：
// 先在候选目标侧做持有校验（regionHoldsLocal），再取数据源、推送路由
// （携带 clusterEpoch + 新 regionEpoch），最后本机更新路由 + 落盘 + 广播。
// 持有校验失败返回 isHeldElsewhere 错误（调用方跳过该候选）。
func (e *Executor) tryFailoverTargetP1(rg sharding.RegionInfo, downID, target string, clusterEpoch uint64) error {
	// 持有校验：目标副本必须真实持有该 region 键空间数据（物理行数/元数据）。
	// regionHoldsLocal 在 target==本节点时直接查本地，否则经 REGION 扫描远端。
	holds, err := e.holdsRegion(rg, target)
	if err != nil {
		return err
	}
	if !holds {
		return errRegionNotHeld(rg.RegionID, target)
	}
	// 新 regionEpoch（owner 变更 +1）并记录本机快照。
	regionEpoch := e.nextRegionEpoch(rg.RegionID, clusterEpoch)
	// 数据源选择：存活节点 + 本节点中该 region 键空间行数最多的副本（同 failoverRegion）。
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	srcID := e.selfID
	best := -1
	var srcRows []cluster.QueryRow
	for _, n := range e.mgr.ListNodes() {
		if n.ID == downID || n.State == cluster.StateDown {
			continue
		}
		pairs, err := e.scanRegionRows(n.ID, start, end)
		if err != nil || len(pairs) <= best {
			continue
		}
		best = len(pairs)
		srcID = n.ID
		srcRows = make([]cluster.QueryRow, 0, len(pairs))
		for _, kv := range pairs {
			srcRows = append(srcRows, cluster.QueryRow{Key: kv.Key, Value: kv.Value})
		}
	}
	if best <= 0 {
		return &SQLError{Msg: fmt.Sprintf("no live data copy for region %d (node %s down)", rg.RegionID, downID)}
	}
	// 目标为本节点：仅更新路由归属 + 落盘 + 广播（携带新 regionEpoch）。
	if target == e.selfID {
		if srcID != e.selfID {
			if err := e.putLocalRows(srcRows); err != nil {
				return err
			}
		}
		updated := rg
		updated.Node = e.selfID
		updated.Local = false
		updated.RegionEpoch = regionEpoch
		// 回本节点接管同样要记录接管来源（原主 downID）：P3 原主恢复 UP 后
		// HandleNodeUp 依据 SourceOwner 触发 tryRejoinAsFollower 自动回迁；
		// 漏记会导致原主恢复后 owner 永不归还（数据仅靠 join 同步追平）。
		updated.SourceOwner = downID
		if err := e.router.SetRegionNodeEpochSource(rg.RegionID, e.selfID, regionEpoch, downID); err != nil {
			return err
		}
		if err := e.persistRegions(); err != nil {
			return err
		}
		e.recordRegionEpochOwner(rg.RegionID, clusterEpoch, regionEpoch, e.selfID)
		return e.broadcastRegionViewEpoch([]sharding.RegionInfo{updated}, nil, clusterEpoch)
	}
	// 远端目标：构建权威路由视图（同 failoverRegion，补充显式化本地归属）。
	regions := e.router.AllRegions()
	for i := range regions {
		if regions[i].RegionID == rg.RegionID {
			regions[i].Node = target
			regions[i].Local = false
			regions[i].RegionEpoch = regionEpoch
		} else if regions[i].Node == "" {
			holds, err := e.regionHoldsLocal(regions[i])
			if err != nil {
				return err
			}
			if holds {
				regions[i].Node = e.selfID
				regions[i].Local = false
			}
		}
	}
	// 推送数据 + 路由视图（携带 clusterEpoch + regionEpoch），接收端校验通过才更新。
	if err := e.mgr.PushRegionEpoch(target, rg, srcRows, regions, srcID, clusterEpoch); err != nil {
		return err
	}
	if err := e.router.SetRegionNodeEpochSource(rg.RegionID, target, regionEpoch, downID); err != nil {
		return err
	}
	if err := e.persistRegions(); err != nil {
		return err
	}
	e.recordRegionEpochOwner(rg.RegionID, clusterEpoch, regionEpoch, target)
	updated := rg
	updated.Node = target
	updated.Local = false
	updated.RegionEpoch = regionEpoch
	updated.SourceOwner = downID
	return e.broadcastRegionViewEpoch([]sharding.RegionInfo{updated}, nil, clusterEpoch)
}

// regionHoldsLocalTarget 判断 target 节点是否真实持有 region rg 键空间数据。
// target == 本节点：直接 regionHoldsLocal；否则经 QUERY_REQ 扫描远端键空间。
// 返回 (true, nil) 表示持有；(false, nil) 表示未持有（不视为错误）。
func (e *Executor) regionHoldsLocalTarget(rg sharding.RegionInfo, target string) (bool, error) {
	if target == e.selfID {
		return e.regionHoldsLocal(rg)
	}
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	pairs, err := e.scanRegionRows(target, start, end)
	if err != nil {
		// 远端不可达/扫描失败：保守视为未持有（跳过该候选）。
		log.Printf("sql: holds check scan failed node=%s rid=%d: %v", target, rg.RegionID, err)
		return false, nil
	}
	return len(pairs) > 0, nil
}

// errRegionNotHeld 表示目标候选未持有 region 键空间（跳过候选信号）。
type errRegionNotHeldError struct {
	rid    storage.ID
	target string
}

func (e *errRegionNotHeldError) Error() string {
	return fmt.Sprintf("sql: region %d not held by candidate %s", e.rid, e.target)
}

func errRegionNotHeld(rid storage.ID, target string) error {
	return &errRegionNotHeldError{rid: rid, target: target}
}

func isHeldElsewhere(err error) bool {
	_, ok := err.(*errRegionNotHeldError)
	return ok
}

// broadcastRegionViewEpoch 带 clusterEpoch 的路由变更广播（P1）：
// 与 broadcastRegionView 相同，但经 BroadcastRegionsEpoch 携带 epoch，
// 接收端校验通过才更新路由（防旧分区广播覆盖新视图）。
func (e *Executor) broadcastRegionViewEpoch(routes []sharding.RegionInfo, deleteIDs []storage.ID, clusterEpoch uint64) error {
	if e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return nil
	}
	out := make([]sharding.RegionInfo, 0, len(routes))
	for i := range routes {
		if routes[i].Node == "" {
			holds, err := e.regionHoldsLocal(routes[i])
			if err != nil {
				return err
			}
			if holds {
				routes[i].Node = e.selfID
				routes[i].Local = false
			}
		}
		out = append(out, routes[i])
	}
	return e.mgr.BroadcastRegionsEpoch(out, deleteIDs, e.selfID, clusterEpoch)
}

package sql

// M6 高可用与故障转移（HA/Failover）。
//
// 形态（详见 docs/T15_m6_ha.md）：
//   - 故障检测：cluster.Manager 心跳超时把 NodeInfo.State 置 DOWN，并异步回调
//     Executor.FailoverDownNode（由 db.StartCluster 装配）；
//   - Region 自动重指派：down 节点持有的显式 region，从存活节点副本取最全数据
//     （创建者/最近接收者，按行数最多者），经 REGION_PUSH 推送到新持有者
//     （首个存活远端，无则回本节点），路由归属落盘 m:regions；
//   - 读 failover：getKVCluster / scanMergedCluster 遇归属节点 down 时先自动
//     重指派再按新路由转发；无存活目标则回退本节点本地副本，不向客户端报错；
//   - 单机模式（mgr == nil）与无 down 节点时全部路径零触发（M4/M5 行为不变）。

import (
	"fmt"
	"log"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// FailoverDownNode 触发节点离线后的 region 自动重指派（M6 故障检测回调/手动入口）。
// 遍历路由表，将 Node == downID 的显式 region 重指派到存活节点：
//   - 目标 = 首个存活远端（UP 且非 down 且非自身）；无存活远端则回本节点；
//   - 数据源 = 存活节点 + 本节点中该 region 键空间行数最多的副本（最全数据）；
//   - 推送（REGION_PUSH）+ 本节点路由归属更新 + 落盘。
//
// 幂等：同一 down 节点仅处理一次（failoverDone 去重；心跳回调与读路径并发
// 触发时由 failoverMu 串行化）；重复调用对已完成重指派的节点直接返回 0。
func (e *Executor) FailoverDownNode(downID string) (int, error) {
	if e.mgr == nil {
		return 0, nil
	}
	e.failoverMu.Lock()
	defer e.failoverMu.Unlock()
	// P1 quorum 前置：复用 P0 quorumGuard（多数派确认或 clusterEpoch 授权）。
	// quorum 不足 → 降级只读，不执行重指派；不置 failoverDone，
	// 允许 quorum 恢复后由读路径/心跳再次触发重试。
	if err := e.quorumOK(); err != nil {
		log.Printf("sql: failover down node %s skipped: quorum not met: %v", downID, err)
		return 0, err
	}
	if e.failoverDone == nil {
		e.failoverDone = map[string]bool{}
	}
	if e.failoverDone[downID] {
		return 0, nil
	}
	// B1（D4.5 §6 第 1 项）：上次因"无存活副本"未能完整重指派时，冷却窗口内
	// 直接返回（幂等，避免读路径高频重试打爆扫描/推送）；窗口外重新尝试，
	// 使"副本补齐 / 原节点恢复"后同一 down 节点可被自动重新接管。
	if !e.b1RetryAllowed(downID) {
		return 0, nil
	}

	var moved int
	var firstErr error
	// pendingB1 本次是否存在"无存活副本"的 region（存在则不置 failoverDone，
	// 保留后续重试机会）。
	pendingB1 := false
	for _, rg := range e.router.AllRegions() {
		if rg.Node != downID {
			continue
		}
		err := e.failoverRegionP1(rg, downID)
		if err == nil {
			e.clearB1Pending(rg.RegionID)
			moved++
			continue
		}
		if isNoLiveCopy(err) {
			// B1：该 region 在存活节点上无任何副本 —— 记录 pending 而不永久放弃，
			// 待副本补齐（EnsureRegionReplicas）或原节点恢复后再次尝试。
			e.recordB1Pending(rg.RegionID, downID)
			pendingB1 = true
			log.Printf("sql: failover region %d deferred (no live copy, will retry): %v", rg.RegionID, err)
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if pendingB1 {
		// 不置 failoverDone：冷却窗口后允许重试；同时尝试补齐副本以避免
		// 该 region 长期裸奔（B1 主动冗余触发点）。
		e.setB1RetryAt(downID)
		e.b1EnsureAsync()
		return moved, firstErr
	}
	e.failoverDone[downID] = true
	e.clearB1RetryAt(downID)
	return moved, firstErr
}

// failoverRegion 将单个 region 从 down 节点重指派到存活节点（见 FailoverDownNode）。
// 数据源选择：ASSIGN REGION 从不删除源节点数据，创建者/最近接收者必然保留副本，
// 取存活节点中该 region 键空间行数最多者（最全副本）；目标 = 首个存活远端。
func (e *Executor) failoverRegion(rg sharding.RegionInfo, downID string) error {
	// 候选目标：存活远端（UP 且非 down 且非自身），按 ID 排序取第一个；无则回本节点。
	target := e.selfID
	for _, n := range e.mgr.ListNodes() {
		if n.ID == e.selfID || n.ID == downID || n.State == cluster.StateDown {
			continue
		}
		target = n.ID
		break
	}
	// 数据源：存活节点 + 本节点中该 region 键空间行数最多的副本。
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		// 开放上界：半开区间覆盖全部 r{rid} 物理键（与 execAssignRegion 一致）
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
		// 存活节点上均无该 region 的真实数据（0 行也算无副本），
		// 禁止把 region 重指派给任意节点并落盘——否则会污染权威路由
		// （无数据 region 被标记归属某节点，后续 failover/迁移基于错误视图决策）。
		return &SQLError{Msg: fmt.Sprintf("no live data copy for region %d (node %s down)", rg.RegionID, downID)}
	}
	// 数据已在目标节点本地：仅更新路由归属并落盘。
	if target == e.selfID {
		if srcID != e.selfID {
			if err := e.putLocalRows(srcRows); err != nil {
				return err
			}
		}
		// P3：记录接管来源（原主），原主恢复 UP 后据此自动回迁。
		_ = e.recordRegionEpochOwnerFor(rg.RegionID, e.selfID, downID)
		if err := e.router.SetRegionNode(rg.RegionID, e.selfID); err != nil {
			return err
		}
		if err := e.persistRegions(); err != nil {
			return err
		}
		// 路由变更全网广播：仅同步本次重指派条目，避免全量视图扩散残留漂移。
		changed := rg
		changed.Node = e.selfID
		changed.Local = false
		changed.SourceOwner = downID
		return e.broadcastRegionView([]sharding.RegionInfo{changed}, nil)
	}
	// 构建权威路由视图（显式化本地归属；本次 region 归属目标节点）。
	regions := e.router.AllRegions()
	for i := range regions {
		if regions[i].RegionID == rg.RegionID {
			regions[i].Node = target
			regions[i].Local = false
		} else if regions[i].Node == "" {
			// 仅本地真正持有数据的 region 补记为本节点；未持有者保持未指派，
			// 避免 Node=="" 但数据在别处的 region 被无条件补记污染为本地归属。
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
	// 推送数据 + 路由视图到目标节点（目标节点同一 WriteBatch 写数据 + 学路由 + 落盘）
	if err := e.mgr.PushRegion(target, rg, srcRows, regions, srcID); err != nil {
		return err
	}
	// P3：记录接管来源（原主），原主恢复 UP 后据此自动回迁。
	_ = e.recordRegionEpochOwnerFor(rg.RegionID, target, downID)
	if err := e.router.SetRegionNode(rg.RegionID, target); err != nil {
		return err
	}
	if err := e.persistRegions(); err != nil {
		return err
	}
	// 路由变更全网广播：仅同步本次重指派条目，避免全量视图扩散残留漂移。
	changed := rg
	changed.Node = target
	changed.Local = false
	changed.SourceOwner = downID
	return e.broadcastRegionView([]sharding.RegionInfo{changed}, nil)
}

// broadcastRegionView 将本次路由变更条目增量广播到所有已注册远端节点（纯路由同步）。
// 路由变更（ASSIGN/SPLIT/failover/migrate）后调用，使各节点视图收敛一致；
// routes 只含本次变更的 region 条目（禁止传全量视图，否则会把本机残留漂移
// 扩散到全网）；deleteIDs 用于 SPLIT 广播移除旧 region。
// 条目内 Node=="" 且本机真正持有数据的 region 补记为本节点，
// 使其他节点学到明确远端归属，避免 Node=="" 语义污染。
func (e *Executor) broadcastRegionView(routes []sharding.RegionInfo, deleteIDs []storage.ID) error {
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
	return e.mgr.BroadcastRegions(out, deleteIDs, e.selfID)
}

// scanRegionRows 扫描 region 键空间 [start, end) 在指定节点上的全量物理行。
// nodeID == 本节点时走本地事务扫描；否则 QUERY_REQ 转发。
func (e *Executor) scanRegionRows(nodeID string, start, end []byte) ([]storage.KVPair, error) {
	if nodeID == e.selfID {
		e.incLocal()
		tx, err := e.tm.Begin()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		return e.scanMerged(tx, []storage.KeyRange{{Start: start, End: end}})
	}
	e.incRemote()
	rows, err := e.mgr.ScanRemote(nodeID, start, end, 0, 0, "")
	if err != nil {
		return nil, err
	}
	pairs := make([]storage.KVPair, 0, len(rows))
	for _, row := range rows {
		pairs = append(pairs, storage.KVPair{Key: row.Key, Value: row.Value})
	}
	return pairs, nil
}

// putLocalRows 将物理键行直接写入本地存储（region 重指派回本节点时恢复远端副本）。
func (e *Executor) putLocalRows(rows []cluster.QueryRow) error {
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, row := range rows {
		if err := tx.Put(row.Key, row.Value); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// resolveRegionOwner 读路径的 region 持有者决策（M6 读 failover）：
//  1. 原归属在线 → 原归属；
//  2. 原归属 down → 触发自动重指派（FailoverDownNode，幂等）→ 新归属；
//  3. 重指派后仍无存活持有者 → 回退本节点本地副本（不向客户端报错）。
func (e *Executor) resolveRegionOwner(rid storage.ID) string {
	node := e.regionNode(rid)
	if e.mgr == nil || !e.mgr.IsDown(node) {
		return node
	}
	// P1：failover 路由条目陈旧（epoch 落后于本机已见快照）→ 视为无效，
	// 以本机快照为准重新决策（FailoverDownNode 幂等，重复调用安全）。
	if rg, ok := e.router.FindRegion(rid); ok && e.staleFailover(rg) {
		log.Printf("sql: stale failover entry invalidated rid=%d epoch=%d < known=%d", rid, rg.RegionEpoch, e.regionEpochOf(rid))
		_, _ = e.FailoverDownNode(node)
		node = e.regionNode(rid)
		if node == "" || e.mgr.IsDown(node) {
			return e.selfID
		}
		return node
	}
	_, _ = e.FailoverDownNode(node)
	node = e.regionNode(rid)
	if node == "" || e.mgr.IsDown(node) {
		return e.selfID
	}
	return node
}

// HandleNodeUp 节点恢复 UP 后的集群轨装配入口（P3 HA，由 Manager.SetOnUp 回调）：
//  1. join 同步路由：把本机权威 region 路由表同步给恢复节点（region 级对齐兜底，
//     避免恢复节点凭旧路由表服务导致路由分裂；PushAllRegionsToNode 内部已做
//     数据面 READY 前置，见 db.StartCluster 装配）。
//  2. failover 接管回迁：若本机持有该节点原主 region 的接管条目（SourceOwner ==
//     nodeID，即此前 failover 重指派记录），自动回迁数据与路由（TryRejoinAsFollower
//     幂等：路由 epoch 陈旧即跳过，原主数据完整性以新 epoch 为准）。
//
// 单机模式与节点未注册时无操作（零回归）；回迁失败不阻断路由同步。
// D4.4 修复（A 簇）：对端重启回 UP 的首次通知可能早于其监听就绪（注册表广播
// 先于 ListenAndServe 完成），此时推送与回迁会因连接被拒而失败；由于 DOWN->UP
// 只会触发一次，失败即永久丢失，重启节点将长期保留孤立读视图。因此按短间隔
// 有限重试，直到路由推送成功且回迁无失败（推送与回迁均幂等）。
func (e *Executor) HandleNodeUp(nodeID string) (int, error) {
	if e.mgr == nil || nodeID == "" || nodeID == e.selfID {
		return 0, nil
	}
	var lastErr error
	for attempt := 0; attempt < nodeUpSyncAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(nodeUpSyncInterval)
		}
		// 先同步权威路由（本机视图为最新，恢复节点以对齐为准）。
		if err := e.syncRegionsToNode(nodeID); err != nil {
			lastErr = err
			log.Printf("sql: node up %s route sync failed (attempt %d/%d): %v",
				nodeID, attempt+1, nodeUpSyncAttempts, err)
			continue
		}
		log.Printf("sql: node up %s route synced", nodeID)
		// 再执行 failover 接管回迁（依赖路由表已学习到接管来源）。
		migrated, rejoinErr := e.rejoinOwnedRegions(nodeID)
		if rejoinErr != nil {
			lastErr = rejoinErr
			continue
		}
		// B1（D4.5 §6 第 1 项）：节点回 UP 且回迁完成后补齐副本——该节点磁盘
		// 可能被清空/重建，此时按现存副本重新冗余，避免 region 长期单副本裸奔。
		e.b1EnsureAsync()
		return migrated, nil
	}
	return 0, lastErr
}

// rejoinOwnedRegions 回迁本机此前从 nodeID 接管（SourceOwner == nodeID）的 region。
// 返回成功回迁数；任一 region 回迁失败即返回错误，交由 HandleNodeUp 重试。
func (e *Executor) rejoinOwnedRegions(nodeID string) (int, error) {
	migrated := 0
	var firstErr error
	for _, rg := range e.router.AllRegions() {
		if rg.SourceOwner != nodeID {
			continue
		}
		if err := e.tryRejoinAsFollower(rg, nodeID); err != nil {
			log.Printf("sql: node up %s rejoin region %d failed: %v", nodeID, rg.RegionID, err)
			// NEW-1 修复（重启双主分叉）：回迁被目标以归属纪元陈旧拒绝时，本机
			// 不得继续按旧视图自持该 region 归属——立即向权威源拉取路由视图收敛
			// （同类 stale-epoch rejoin 不保留归属旧视图）；本机残留数据随后续
			// 权威推送/回迁归并对齐，此处不删除任何数据（不静默丢弃）。
			e.scheduleRouteResync()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		migrated++
	}
	return migrated, firstErr
}

const (
	// nodeUpSyncAttempts 节点回 UP 后路由推送 + 回迁的最大尝试次数（覆盖对端
	// 监听就绪窗口；全失败视为对端仍不可用，交由后续心跳/重启对齐兜底）。
	nodeUpSyncAttempts = 6
	// nodeUpSyncInterval 相邻两次尝试的间隔。
	nodeUpSyncInterval = 500 * time.Millisecond
)

// syncRegionsToNode 将本机权威 region 路由表同步到目标节点（join 同步路由）。
// 复用复制轨 pushRegionsToNode 的副本语义分支（claim=false，D4.4 修复）：
// 只推送本节点持有/归属本节点的 region，并按本节点声明权威归属，接收端保持
// 该归属（local=false），不再把第三方 region 误判为目标本地（旧 claim 语义
// 会把全部 region 判给目标节点，重启节点据此形成孤立读视图并静默本地写）。
// 含数据/路由装载与 epoch 校验；推送全部成功即声明目标节点数据面就绪并广播
// （全网读路径 READY 前置放行跨节点聚合读）；目标不可达/推送失败返回错误，
// 由 HandleNodeUp 记录，下一次 UP 回调重试（尽力而为）。
func (e *Executor) syncRegionsToNode(nodeID string) error {
	if e.mgr == nil || e.selfID == nodeID {
		return nil
	}
	if err := e.pushRegionsToNode(nodeID, false); err != nil {
		return err
	}
	return e.mgr.SetDataReady(nodeID, true)
}

// tryRejoinAsFollower 复制轨装配版（P3 region 级对齐兜底）：
// 将 SourceOwner == 原主的 region 从接管节点回迁数据/路由到原主（follower 语义），
// 成功返回 nil；已回迁/epoch 陈旧/键空间无数据时返回 nil（幂等）。
func (e *Executor) tryRejoinAsFollower(rg sharding.RegionInfo, rejoinID string) error {
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	rows, err := e.scanRegionRows(e.selfID, start, end)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		// 接管节点已无该 region 真实数据（回迁无意义），仅归还路由归属。
		log.Printf("sql: rejoin region %d to %s: no local data, route only", rg.RegionID, rejoinID)
		return e.routeOnlyRejoin(rg, rejoinID)
	}
	// 复制轨 P1 对齐：目标持有校验 + 数据/路由推送（携带新 epoch），
	// 失败（目标未就绪/陈旧 epoch 拒绝）返回错误由调用方跳过。
	if err := e.PushRegionEpochTo(rejoinID, rg, rows, e.selfID); err != nil {
		return err
	}
	return nil
}

// routeOnlyRejoin 无数据场景的纯路由归还：仅将 region 归属改回原主并广播，
// 不推送数据（避免把空数据覆盖原主可能存在的本地副本）。
func (e *Executor) routeOnlyRejoin(rg sharding.RegionInfo, rejoinID string) error {
	if e.mgr == nil {
		return nil
	}
	updated := rg
	updated.Node = rejoinID
	updated.Local = false
	updated.SourceOwner = ""
	// 不 bump epoch：无数据变更，仅归属归还；接收端按 epoch 幂等接受。
	if err := e.router.SetRegionNodeEpochSource(rg.RegionID, rejoinID, rg.RegionEpoch, ""); err != nil {
		return err
	}
	if err := e.persistRegions(); err != nil {
		return err
	}
	return e.broadcastRegionView([]sharding.RegionInfo{updated}, nil)
}

// PushRegionEpochTo 将 region 数据与路由按新 epoch 推送到目标节点（回迁专用）。
// 复用 cluster.Manager.PushRegionEpoch（复制轨 TryRejoinAsFollower 同款通道）：
// 携带 clusterEpoch + regionEpoch，接收端 checkRegionPushEpoch 校验通过才装载。
// 目标节点未就绪/不可达时返回错误，由调用方决定跳过。
func (e *Executor) PushRegionEpochTo(target string, rg sharding.RegionInfo, rows []storage.KVPair, srcID string) error {
	if e.mgr == nil {
		return nil
	}
	regionEpoch := e.nextRegionEpoch(rg.RegionID, e.epochSrc.AcquireEpoch())
	regions := e.router.AllRegions()
	for i := range regions {
		if regions[i].RegionID == rg.RegionID {
			regions[i].Node = target
			regions[i].Local = false
			regions[i].RegionEpoch = regionEpoch
			regions[i].SourceOwner = ""
		}
	}
	clusterEpoch := e.epochSrc.AcquireEpoch()
	srcRows := make([]cluster.QueryRow, 0, len(rows))
	for _, kv := range rows {
		srcRows = append(srcRows, cluster.QueryRow{Key: kv.Key, Value: kv.Value})
	}
	if err := e.mgr.PushRegionEpoch(target, rg, srcRows, regions, srcID, clusterEpoch); err != nil {
		return err
	}
	if err := e.router.SetRegionNodeEpochSource(rg.RegionID, target, regionEpoch, ""); err != nil {
		return err
	}
	if err := e.persistRegions(); err != nil {
		return err
	}
	updated := rg
	updated.Node = target
	updated.Local = false
	updated.RegionEpoch = regionEpoch
	updated.SourceOwner = ""
	return e.broadcastRegionViewEpoch([]sharding.RegionInfo{updated}, nil, clusterEpoch)
}

// recordRegionEpochOwnerFor 旧路径（无 epoch）fallback：将 failover 接管来源
// （原主）记录到本机路由，供原主恢复 UP 后回迁决策；epoch 未启用时落盘
// SourceOwner 字段本身即可（路由视图随 m:regions 持久化）。
func (e *Executor) recordRegionEpochOwnerFor(rid storage.ID, owner, source string) error {
	if rg, ok := e.router.FindRegion(rid); ok {
		rg.SourceOwner = source
		_ = e.router.SetRegionNodeEpochSource(rid, owner, rg.RegionEpoch, source)
	}
	return e.persistRegions()
}

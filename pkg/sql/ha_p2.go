package sql

// P2 HA 分区恢复合并（集群轨路由收敛 + 冲突标记）：在 P1 防路由分裂
// 基础上，对分区恢复后的路由视图按 epoch 大者胜收敛，陈旧视图被拒绝
// 计入冲突标记（docs/HA_BRAIN_SPLIT_DESIGN.md §5 P2）。
//
// 机制：
//   - regionConflict：本节点拒绝陈旧路由消息（checkRegionPushEpoch 失败）
//     的累计计数，作为"数据冲突标记/告警"对外暴露（分区双端各自重指派
//     时，陈旧侧每收到一次新视图即计数）；
//   - recordRegionEpochOwner：记录 region 某 epoch 对应的 owner（重指派
//     成功 / 路由收敛后写入，大者胜）；
//   - ReconcileRegionView：分区恢复后按本机 p1Epoch 快照收敛路由视图：
//     路由条目 epoch < 快照 → 以快照 owner/epoch 收敛（广播携带大者
//     clusterEpoch，接收端 epoch 校验通过才更新）；路由条目 epoch > 快照
//     → 快照大者胜升级（吸收路由条目信息）。

import (
	"log"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// recordRegionEpochOwner 记录 region 重指派/收敛后的 owner（大者胜）。
// 同一 region 仅保留最大 regionEpoch（及其 clusterEpoch/owner）；
// 等大时保留 clusterEpoch 更大者（收敛语义：陈旧 owner 被新鲜 owner 覆盖）。
func (e *Executor) recordRegionEpochOwner(rid storage.ID, clusterEpoch, regionEpoch uint64, owner string) {
	e.p1Mu.Lock()
	defer e.p1Mu.Unlock()
	if old, ok := e.p1Epoch[rid]; ok && (old.regionEpoch > regionEpoch || (old.regionEpoch == regionEpoch && old.clusterEpoch > clusterEpoch)) {
		return
	}
	e.p1Epoch[rid] = p1RegionEpoch{clusterEpoch: clusterEpoch, regionEpoch: regionEpoch, owner: owner}
}

// bumpRegionConflict 累计陈旧路由消息冲突计数（数据冲突标记）。
func (e *Executor) bumpRegionConflict() {
	e.p2Mu.Lock()
	e.regionConflict++
	e.p2Mu.Unlock()
}

// RegionConflictCount 返回本节点拒绝陈旧路由消息的累计次数（开关关闭恒 0）。
func (e *Executor) RegionConflictCount() uint64 {
	e.p2Mu.Lock()
	defer e.p2Mu.Unlock()
	return e.regionConflict
}

// ReconcileRegionView 分区恢复后的路由收敛（epoch 大者胜）：
//   - 未启用 P1 → 直接返回 nil（零回归）；
//   - 对每个有本机快照的 region：路由条目 epoch < 快照 → 以快照 owner/epoch
//     收敛本机路由并广播（携带快照 clusterEpoch，接收端 epoch 校验通过才
//     更新）；路由条目 epoch > 快照 → 快照大者胜升级（吸收路由条目 Node）。
// 返回首个非致命错误（其余继续收敛；无错误返回 nil）。
func (e *Executor) ReconcileRegionView() error {
	if !e.p1Enabled() {
		return nil
	}
	e.p1Mu.Lock()
	snap := make(map[storage.ID]p1RegionEpoch, len(e.p1Epoch))
	for k, v := range e.p1Epoch {
		snap[k] = v
	}
	e.p1Mu.Unlock()

	var firstErr error
	for _, rg := range e.router.AllRegions() {
		ep, ok := snap[rg.RegionID]
		if !ok {
			continue
		}
		if rg.RegionEpoch < ep.regionEpoch {
			// 路由视图陈旧：以快照大者收敛（owner 可为本节点或远端）。
			if ep.owner != "" && ep.owner != rg.Node {
				if err := e.router.SetRegionNode(rg.RegionID, ep.owner); err != nil {
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				changed := rg
				changed.Node = ep.owner
				changed.RegionEpoch = ep.regionEpoch
				if err := e.broadcastRegionViewEpoch([]sharding.RegionInfo{changed}, nil, ep.clusterEpoch); err != nil && firstErr == nil {
					firstErr = err
				}
				log.Printf("sql: reconcile region %d -> owner %s (regionEpoch %d)", rg.RegionID, ep.owner, ep.regionEpoch)
			}
		} else if rg.RegionEpoch > ep.regionEpoch {
			// 路由视图更新：快照大者胜升级（吸收路由条目 owner）。
			e.recordRegionEpochOwner(rg.RegionID, ep.clusterEpoch, rg.RegionEpoch, rg.Node)
		}
	}
	return firstErr
}

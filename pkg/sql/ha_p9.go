package sql

import (
	"log"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// P9（D4.5 Phase 9 / 候选 B）：SQL 侧集群轨归属纪元通知接线。
//
// 背景见 pkg/db/ha_p9.go：写路径 AllowWrite 的 masterEpoch 只由复制轨推进，
// 纯集群轨 masterEpoch 恒 0 → 门槛永不成立。db 层已补齐"集群轨归属纪元"
// （cluster term）与持久化；本文件负责把 SQL 侧的归属事实（接管成功、
// 权威视图应用）通知给 db 层，使 term 真正推进。
//
// 通知来源共三处：
//  1. 节点收到 REGION_PUSH 数据装载 / 路由视图（cluster.Server →
//     db.NoteOwnershipView，不经本文件）；
//  2. 本节点接管 region（tryFailoverTargetP1 target==selfID，本文件
//     notifyClusterOwnership）；
//  3. 权威全量路由视图应用完成（同上，由 cluster.Server 统一通知）。
//
// 零回归：未装配通知器（nil）时全部为空操作，单机/无 failover 场景行为不变。

// SetClusterOwnershipNotify 装配集群轨归属纪元通知器（db.StartCluster 调用，
// 装配 db.DB.NoteOwnershipView）；传 nil 恢复空操作。
func (e *Executor) SetClusterOwnershipNotify(fn func(term uint64, selfOwns, authoritative bool)) {
	e.p9Mu.Lock()
	defer e.p9Mu.Unlock()
	e.p9OwnershipNotify = fn
}

// notifyClusterOwnership 通知本机集群轨归属状态（未装配时空操作）。
func (e *Executor) notifyClusterOwnership(term uint64, selfOwns, authoritative bool) {
	e.p9Mu.Lock()
	fn := e.p9OwnershipNotify
	e.p9Mu.Unlock()
	if fn == nil {
		return
	}
	fn(term, selfOwns, authoritative)
}

// RegionOwnershipForWrite 判定写目标 region 的本地归属（v1.21.0 NEW-3，
// 由 db.StartCluster 装配到 db.DB.SetLocalRegionOwner，供 region 粒度归属
// fence 使用）。返回：
//   - owned：本机是否为该 region 的当前 owner（写会落在本机）；
//   - known：归属是否可判定——仅"显式分区 + owner 已注册"可判定；隐式单
//     region、未指派 region、owner 不在注册表、未装配路由视图均视为不可判定，
//     db 层据此退回节点级归属判定（保守拒写，零回归）。
//
// 语义要点：目标 region 归属其它节点时本机只做 2PC 转发，节点级归属状态
// （本机是否仍持有其它 region 归属）与该写无关，不能被误判成越权写。
func (e *Executor) RegionOwnershipForWrite(rid uint64) (owned, known bool) {
	if e.router == nil {
		return false, false
	}
	id := storage.ID(rid)
	switch _, kind := e.routeDecision(id); kind {
	case routeLocalOwned:
		if !e.regionIsExplicit(id) {
			return true, false // 隐式单 region：归属不可作为"共享归属"依据
		}
		return true, true
	case routeRemoteOwned:
		return false, true
	default:
		return false, false
	}
}

// notifyLocalTakeover 本节点接管 region rg 后推进集群轨归属纪元。
//
// v1.21.0 NEW-3（误授根因收口）：v1.20.x 版本无条件以 selfOwns=true 上报，
// 任何走接管路径的节点都被记为"曾持有归属"，叠加权威视图 self_owns 缺陷留下
// 的历史误授态，非 owner 节点（真机 node-c）被永久 fence 且重启不恢复
// （cluster_term.meta 持久化 own_term>0 / owned=0）。
// 现在只在"接管真正落地 + 本机即该 region 的当前 owner"时才上报：
//   - 路由快照中该 region 的 Node 必须是本机（远端目标接管不上报）；
//   - 该 region 已具备非零 regionEpoch（占位/未完成接管的陈旧快照不上报）；
//   - 若记录过接管使用的 clusterEpoch，则不允许用更旧纪元覆盖更新纪元的归属。
func (e *Executor) notifyLocalTakeover(rid storage.ID, clusterEpoch uint64) {
	if !e.takeoverLandedLocally(rid, clusterEpoch) {
		log.Printf("sql: skip cluster ownership notify for region %d (takeover not landed locally, epoch=%d)", rid, clusterEpoch)
		return
	}
	e.notifyClusterOwnership(clusterEpoch, true, false)
}

// takeoverLandedLocally 判定 region rid 的接管是否已在本机落地（见 notifyLocalTakeover）。
func (e *Executor) takeoverLandedLocally(rid storage.ID, clusterEpoch uint64) bool {
	if e.router == nil {
		return false
	}
	rg, ok := e.router.FindRegion(rid)
	if !ok {
		return false
	}
	if rg.Node != e.selfID || rg.RegionEpoch == 0 {
		return false
	}
	if clusterEpoch > 0 {
		e.p1Mu.Lock()
		rec, seen := e.p1Epoch[rid]
		e.p1Mu.Unlock()
		if seen && rec.clusterEpoch > clusterEpoch {
			return false // 已有更新纪元的接管记录，旧纪元不得推进归属
		}
	}
	return true
}

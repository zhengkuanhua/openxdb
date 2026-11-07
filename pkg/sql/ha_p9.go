package sql

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

// notifyLocalTakeover 本节点接管 region 后推进集群轨归属纪元：
// 以接管使用的 clusterEpoch 作为授权 term，本机在 term 上持有归属。
// 这是 T3（新 owner 获得写授权）与 NEW-1（旧 owner 按权威视图被收回）的
// 授权侧事实来源；term=0（epoch 未启用/旧路径）时 db 层只记录归属不推进。
func (e *Executor) notifyLocalTakeover(clusterEpoch uint64) {
	e.notifyClusterOwnership(clusterEpoch, true, false)
}

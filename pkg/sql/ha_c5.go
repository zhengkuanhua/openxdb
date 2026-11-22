package sql

import (
	"fmt"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
)

// v1.20.0 C5：集群轨写授权统一（承接 C2 后置的分区拒写 V-A4）。
//
// 设计要点（与 docs/DEV_PLAN_v1.20.0.md §2.1 取舍对应）：
//   - C2 只做 region 归属三类判定（REGION_NOT_ASSIGNED / REGION_NOT_LOCAL /
//     REGION_OWNER_UNREACHABLE），分区（网络少数派）场景不拦写；
//   - C5 补齐"节点级分区授权"：本节点处于分区少数派时，集群轨写直接拒绝
//     （错误码 CLUSTER_MINORITY），分区恢复后由 watcher 刷新快照自动放行；
//   - 判定来源为 cluster.Manager 的心跳采样快照（PartitionMinority），
//     写热路径零 IO，不再逐语句真实 PING。
//
// 开关联动口径（D-3 再修订）：
//   - ha_write_route_enforce=on → 分区拒写生效；
//   - ha_quorum_enabled=on → 分区拒写生效（联动范围**仅限**分区拒写，
//     不联动 region 归属三类判定：既有 quorum 用例的集群轨基线写会写
//     未指派/空隐式 region，归属判定联动会直接判 REGION_NOT_ASSIGNED /
//     REGION_NOT_LOCAL 打破零回归红线，见 C2 §2.2 偏离一）。
//   - 两者皆 off → 完全不生效（默认零回归）。

// clusterMinorityPrefix 分区少数派拒写的错误前缀（观测/日志固定文本）。
const clusterMinorityPrefix = "cluster track write rejected: partition minority"

// partitionWriteFenceOn 判定"分区拒写"授权是否生效。
func (e *Executor) partitionWriteFenceOn() bool {
	if e.writeRouteEnforceExplicit() {
		return true
	}
	if e.haCfg == nil {
		return false
	}
	return e.haCfg.QuorumEnabled()
}

// partitionMinoritySnapshot 读集群分区快照（心跳采样口径，写路径零 IO）。
// known=false 表示快照不可用（mgr 未装配 / 单机 / 尚未采样），调用方应放行。
func (e *Executor) partitionMinoritySnapshot() (minority bool, known bool) {
	if e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return false, false
	}
	return e.mgr.PartitionMinority(clusterPartitionSnapshotTTL)
}

// clusterPartitionSnapshotTTL 写授权读快照的最长有效时效。
// 与 cluster.PartitionSnapshotTTL 对齐：快照过期由 stale-while-refresh 兜底，
// 不会在写路径上阻塞等待探活。
const clusterPartitionSnapshotTTL = cluster.PartitionSnapshotTTL

// partitionMinorityWriteAuth 节点级集群轨写授权（V-A4 分区拒写）。
// 返回 nil 表示放行；返回 CLUSTER_MINORITY 表示本节点处于分区少数派。
func (e *Executor) partitionMinorityWriteAuth() error {
	if !e.partitionWriteFenceOn() {
		return nil
	}
	minority, known := e.partitionMinoritySnapshot()
	if !known || !minority {
		return nil
	}

	e.p3Mu.Lock()
	e.p3Fenced++
	e.p3Mu.Unlock()

	ps, _ := e.mgr.PartitionSnapshot()
	return &SQLError{
		Code: ErrCodeClusterMinority,
		Msg: fmt.Sprintf("%s: reachable %d, quorum %d (view total %d), write requires majority "+
			"(N/2+1) authorization; wait for partition recovery and route convergence",
			clusterMinorityPrefix, ps.Reachable, ps.Quorum, ps.Total),
	}
}

// PartitionMinorityFencedCount 返回集群轨分区拒写累计次数（观测口径）。
func (e *Executor) PartitionMinorityFencedCount() uint64 {
	e.p3Mu.Lock()
	defer e.p3Mu.Unlock()
	return e.p3Fenced
}

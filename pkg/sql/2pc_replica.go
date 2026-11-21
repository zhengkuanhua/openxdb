/*
---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_78c959d3e30d11f1913b525400393706
    ReservedCode1: nucB5a131X3JrmedPbHJSm4SFUQg6VPltCR6o7d+PJBnH9b7zqcPacu2QqkV3F1iVcAvduytT6x04jC5v3MYKUn8G28tZV75pMAK69t91q8DGc7l7hBBweQl+ynDEcMYk/1H8p8kQ8drpqYryCgUvEoBKOMAMGI2XKHM0jpeyKfc5COoXZcpIjSCNYY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_78c959d3e30d11f1913b525400393706
    ReservedCode2: nucB5a131X3JrmedPbHJSm4SFUQg6VPltCR6o7d+PJBnH9b7zqcPacu2QqkV3F1iVcAvduytT6x04jC5v3MYKUn8G28tZV75pMAK69t91q8DGc7l7hBBweQl+ynDEcMYk/1H8p8kQ8drpqYryCgUvEoBKOMAMGI2XKHM0jpeyKfc5COoXZcpIjSCNYY=
---
*/

package sql

// v1.20.0 C3：2PC 参与面扩展（缺口 B-2 / B-3 / B-4，验收 V-B2 / V-B3）。
//
// 扩展点与决策：
//   - B-2/B-4：协调者参与者集合由「仅 region owner」扩展为「owner + 该 region
//     副本」，PREPARE 按多数派（n/2+1）判定是否可提交；未达 quorum 时回滚
//     已 prepare 的参与者，返回 TXN_PREPARE_QUORUM_FAILED，两侧无残留。
//   - B-3：参与者侧 PREPARE 校验 ops 中键的 region 归属（owner 或副本），
//     既非 owner 也非副本时返回 2PC_NOT_OWNER 且不落盘 m:2pc:p:<txid>
//     （实现见 pkg/cluster/replicas.go 的 checkParticipantOwnership）。
//   - 副本集合按 DEV_PLAN D-2 决策走 MVP：独立元数据键
//     m:repl:replicas:<rid>（不扩 sharding.RegionInfo.Replicas）。记录缺失时
//     参与者集合退化为仅 owner，行为与 v1.19.0 逐字一致。
//   - 两条既有 2PC 路径（exec2pcWrite / execExplicitCommit2PC）的 PREPARE 与
//     ABORT 公共逻辑统一收敛到本文件的 prepareParticipants / abortPrepared，
//     禁止两处各写一套。
//
// 开关（ha_2pc_replica_quorum，默认 off）：off 时参与者集合仅 owner、任一失败
// 即回滚（旧语义），零回归；on 时启用副本参与、quorum 判定与归属校验。

import (
	"fmt"
	"sort"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ---- 开关键（ha_2pc_replica_quorum）----

// TwoPCReplicaQuorum 返回 2PC 参与面扩展开关状态（默认 false）。
func (e *Executor) TwoPCReplicaQuorum() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.twoPCQuorum
}

// SetTwoPCReplicaQuorum 设置 2PC 参与面扩展开关（e.mu 保护，运行期热切换）。
func (e *Executor) SetTwoPCReplicaQuorum(on bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.twoPCQuorum = on
}

// applyHaTwoPCReplicaQuorum 解析并应用 ha_2pc_replica_quorum。
// 与 C2 的 ha_write_route_enforce 一致：需要先启动集群（副本集合元数据键与
// 参与者都来自集群链路），单机模式置位无意义，直接返回明确错误。
func applyHaTwoPCReplicaQuorum(e *Executor, v string) error {
	if e.mgr == nil {
		return &SQLError{Msg: "ha_2pc_replica_quorum requires cluster HA configuration (start cluster first)"}
	}
	on, err := haParseBool(v)
	if err != nil {
		return &SQLError{Msg: "invalid value for ha_2pc_replica_quorum: " + v + " (expected on/off)"}
	}
	e.SetTwoPCReplicaQuorum(on)
	return nil
}

// ---- 参与者集合规划 ----

// preparePlan PREPARE 阶段的参与者规划结果。
type preparePlan struct {
	// nodes 参与者节点（升序，含 owner 与副本，按节点去重）。
	nodes []string
	// ops 各参与者的待提交操作（副本节点与 owner 持有同 region 的同一批 ops）。
	ops map[string][]kvOp
	// quorum 判定提交所需的最小成功参与者数：扩展开启 = n/2+1，关闭 = n（全等）。
	quorum int
	// replicated 本轮是否真正扩出了副本参与者（决定 quorum 与失败处理语义）：
	// 仅当开关开启且至少一个 region 的副本集合非空（且确有副本节点加入）时为
	// true；否则为 false——参与者仅 owner，语义与 v1.19.0 逐字一致。
	replicated bool
}

// buildPreparePlan 依据远端分组与副本元数据键规划参与者集合。
//
// 关闭扩展时：参与者 = 远端分组节点，quorum = len(nodes)（旧语义：全部成功）。
// 开启扩展时：按 region 把 owner 的 ops 复制给该 region 的副本节点，参与者
// 去重后 quorum = n/2+1；副本记录缺失/读取失败的 region 自动退化为仅 owner
// （该 region 仍计入 owner 参与者，不影响其它 region 的副本扩展）。
//
// 若本轮没有任何 region 真正扩出副本参与者（副本记录缺失或全为 owner），则
// replicated 保持 false：quorum 与失败处理语义都退回 v1.19.0（全部成功、
// TXN_PREPARE_FAILED），保证"退化为仅 owner"时行为逐字一致。
func (e *Executor) buildPreparePlan(remote map[string][]kvOp) preparePlan {
	plan := preparePlan{ops: make(map[string][]kvOp, len(remote))}
	for node, ops := range remote {
		plan.ops[node] = append(plan.ops[node], ops...)
	}
	replicated := e.TwoPCReplicaQuorum() && e.mgr != nil && e.st != nil
	expanded := false
	if replicated {
		// byRegion 按 region 汇总 owner 侧 ops，便于整体复制给该 region 副本。
		for node, ops := range remote {
			byRegion := map[storage.ID][]kvOp{}
			order := make([]storage.ID, 0, 2)
			for _, op := range ops {
				rid := storage.ID(0)
				if len(op.Key) > sharding.RegionKeyPrefixLen && op.Key[0] == 'r' {
					rid, _ = sharding.DecodeRegionKey(op.Key)
				}
				if _, ok := byRegion[rid]; !ok {
					order = append(order, rid)
				}
				byRegion[rid] = append(byRegion[rid], op)
			}
			for _, rid := range order {
				if rid == 0 {
					// 非 region 前缀键（DDL/元数据类）不参与副本复制。
					continue
				}
				reps, err := cluster.LoadRegionReplicas(e.st, rid)
				if err != nil || len(reps) == 0 {
					// 副本记录缺失/损坏：该 region 退化为仅 owner。
					continue
				}
				for _, rep := range reps {
					if rep == "" || rep == node {
						continue
					}
					plan.ops[rep] = append(plan.ops[rep], byRegion[rid]...)
					expanded = true
				}
			}
		}
	}
	plan.replicated = replicated && expanded
	plan.nodes = make([]string, 0, len(plan.ops))
	for node, ops := range plan.ops {
		if len(ops) == 0 {
			continue
		}
		plan.nodes = append(plan.nodes, node)
	}
	sort.Strings(plan.nodes)
	if plan.replicated {
		plan.quorum = len(plan.nodes)/2 + 1
	} else {
		plan.quorum = len(plan.nodes)
	}
	return plan
}

// prepareParticipants 向规划出的全部参与者发送 PREPARE 并做 quorum 判定。
//
// 旧语义（扩展关闭）：任一参与者失败即回滚已 prepare 的参与者，返回
// "TXN_PREPARE_FAILED" 文本错误（与 v1.19.0 逐字一致）。
// 新语义（扩展开启）：失败只记录，达多数派仍然提交；未达多数派回滚全部已
// prepare 参与者并返回 TXN_PREPARE_QUORUM_FAILED（携带成功/总参与者数），
// 保证协调者本地不提交、参与者无 prepared 残留。
//
// 返回的 prepared 为实际 prepare 成功的参与者（供后续 COMMIT/ABORT 使用）。
func (e *Executor) prepareParticipants(txID string, beginTS uint64, remote map[string][]kvOp) ([]string, error) {
	plan := e.buildPreparePlan(remote)
	if len(plan.nodes) == 0 {
		return nil, nil
	}
	prepared := make([]string, 0, len(plan.nodes))
	firstFailed := ""
	var firstErr error
	for _, node := range plan.nodes {
		if err := e.mgr.PrepareTxn(node, txID, beginTS, toClusterTxnOps(plan.ops[node])); err != nil {
			if !plan.replicated {
				// 旧语义：任一失败即整体回滚（保持 v1.19.0 行为与错误文本）。
				e.abortPrepared(prepared, txID)
				return nil, &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: 2pc %s participant %s: %v", txID, node, err)}
			}
			if firstErr == nil {
				firstFailed, firstErr = node, err
			}
			continue
		}
		prepared = append(prepared, node)
	}
	if len(prepared) < plan.quorum {
		e.abortPrepared(prepared, txID)
		if firstErr == nil {
			firstErr = fmt.Errorf("participants unavailable")
		}
		return nil, &SQLError{
			Code: ErrCodeTxnPrepareQuorumFailed,
			Msg: fmt.Sprintf("2pc %s prepared %d/%d participant(s), need majority %d (first failure on %q: %v)",
				txID, len(prepared), len(plan.nodes), plan.quorum, firstFailed, firstErr),
		}
	}
	return prepared, nil
}

// abortPrepared 回滚已 prepare 的参与者（幂等 ABORT，失败忽略：
// 参与者侧未决 prepare 由重启 Recover2PC 清理）。
func (e *Executor) abortPrepared(prepared []string, txID string) {
	if e.mgr == nil {
		return
	}
	for _, node := range prepared {
		_ = e.mgr.AbortTxn(node, txID)
	}
}

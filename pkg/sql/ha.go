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
	if e.failoverDone == nil {
		e.failoverDone = map[string]bool{}
	}
	if e.failoverDone[downID] {
		return 0, nil
	}
	e.failoverDone[downID] = true

	var moved int
	var firstErr error
	for _, rg := range e.router.AllRegions() {
		if rg.Node != downID {
			continue
		}
		if err := e.failoverRegion(rg, downID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		moved++
	}
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
	if best < 0 {
		return &SQLError{Msg: fmt.Sprintf("no live copy for region %d (node %s down)", rg.RegionID, downID)}
	}
	// 数据已在目标节点本地：仅更新路由归属并落盘。
	if target == e.selfID {
		if srcID != e.selfID {
			if err := e.putLocalRows(srcRows); err != nil {
				return err
			}
		}
		if err := e.router.SetRegionNode(rg.RegionID, e.selfID); err != nil {
			return err
		}
		return e.persistRegions()
	}
	// 构建权威路由视图（显式化本地归属；本次 region 归属目标节点）。
	regions := e.router.AllRegions()
	for i := range regions {
		if regions[i].RegionID == rg.RegionID {
			regions[i].Node = target
			regions[i].Local = false
		} else if regions[i].Node == "" {
			regions[i].Node = e.selfID
			regions[i].Local = false
		}
	}
	// 推送数据 + 路由视图到目标节点（目标节点同一 WriteBatch 写数据 + 学路由 + 落盘）
	if err := e.mgr.PushRegion(target, rg, srcRows, regions, srcID); err != nil {
		return err
	}
	if err := e.router.SetRegionNode(rg.RegionID, target); err != nil {
		return err
	}
	return e.persistRegions()
}

// scanRegionRows 扫描 region 键空间 [start, end) 在指定节点上的全量物理行。
// nodeID == 本节点时走本地事务扫描；否则 QUERY_REQ 转发。
func (e *Executor) scanRegionRows(nodeID string, start, end []byte) ([]storage.KVPair, error) {
	if nodeID == e.selfID {
		tx, err := e.tm.Begin()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		return e.scanMerged(tx, []storage.KeyRange{{Start: start, End: end}})
	}
	rows, err := e.mgr.ScanRemote(nodeID, start, end, 0)
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
	_, _ = e.FailoverDownNode(node)
	node = e.regionNode(rid)
	if node == "" || e.mgr.IsDown(node) {
		return e.selfID
	}
	return node
}

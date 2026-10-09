package sql

// M4 多节点集成（第一簇：节点拓扑 + region 跨节点路由与查询）。
//
// 设计形态（详见 docs/T13_m4_multinode.md）：
//   - Executor 注入 cluster.Manager（nil = 单机模式，转发路径零触发）；
//   - 读路径统一走 getKVCluster / scanMergedCluster：
//     点查/索引回表按路由定位 region，本地 region 直读本事务/引擎，
//     远端 region 经 QUERY_REQ/QUERY_RESP（物理键区间）转发到归属节点执行；
//     范围/全表按 rowRanges 逐 region 展开，本地扫描 + 远端转发后
//     按内层键序合并（与 M3 scanMerged 相同语义）；
//   - 管理语句：ADD NODE（握手 + 注册）、SHOW NODES、SHOW REGION ROUTES、
//     ASSIGN REGION（数据推送 REGION_PUSH 到目标节点 + 路由归属更新）；
//   - 写路径（INSERT/UPDATE/DELETE/DDL/SPLIT）第一簇保持"仅本地"：
//     远端 region 的写操作由既有路径返回语义错误，分布式写事务（2PC）留待下一簇。

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// SetCluster 注入集群管理器（nil = 单机模式）。
func (e *Executor) SetCluster(mgr *cluster.Manager, selfID string) {
	e.mgr = mgr
	e.selfID = selfID
}

// Router 返回当前分片路由表（供 db 装配与测试断言）。
func (e *Executor) Router() *sharding.Router { return e.router }

// clusterEnabled 集群是否启用且存在远端节点。
func (e *Executor) clusterEnabled() bool {
	return e.mgr != nil && e.mgr.HasRemoteNodes()
}

// regionLocal region 是否由本节点执行：未指派（Node==""）或归属 == selfID。
// M3 单机数据（Node 字段缺省）与 M4 本地指派均走本地路径，保证零回归。
func (e *Executor) regionLocal(rid storage.ID) bool {
	node := e.router.RegionNode(rid)
	return node == "" || node == e.selfID
}

// regionNode 返回 region 归属节点；未指派视为本节点。
func (e *Executor) regionNode(rid storage.ID) string {
	node := e.router.RegionNode(rid)
	if node == "" {
		return e.selfID
	}
	return node
}

// checkWritable 第一簇写路径约束：仅允许写本节点持有的 region。
// 远端 region 的写入返回语义错误（分布式写事务/2PC 留待下一簇），
// 防止静默写本地副本造成跨节点读不一致。单机模式（mgr==nil）零触发。
func (e *Executor) checkWritable(rid storage.ID) error {
	if e.mgr == nil {
		return nil
	}
	if !e.regionLocal(rid) {
		return &SQLError{Msg: fmt.Sprintf("write to remote region %d not supported in first cluster (distributed write txn next cluster)", rid)}
	}
	return nil
}

// getKVCluster 集群感知点查：定位 region 后，本地直读、远端转发。
// 与 tx.Get 相同的错误语义（未命中返回 storage.ErrNotFound）。
func (e *Executor) getKVCluster(tx txn.Txn, key []byte) ([]byte, error) {
	if e.mgr == nil {
		return tx.Get(key)
	}
	rid, _ := sharding.DecodeRegionKey(key)
	if e.regionLocal(rid) {
		return tx.Get(key)
	}
	return e.mgr.GetRemote(e.regionNode(rid), key)
}

// scanMergedCluster 集群感知范围扫描：按 ranges 逐 region 展开执行并合并。
// 与 scanMerged 相同的结果语义（按内层键序合并、等价单机全表序）：
//   - 本地 region → 直接扫描本事务；
//   - 远端 region → QUERY_REQ 转发区间扫描（归属节点直读其存储快照）。
func (e *Executor) scanMergedCluster(tx txn.Txn, ranges []storage.KeyRange) ([]storage.KVPair, error) {
	if e.mgr == nil {
		return e.scanMerged(tx, ranges)
	}
	var out []storage.KVPair
	for _, r := range ranges {
		rid, _ := sharding.DecodeRegionKey(r.Start)
		var pairs []storage.KVPair
		var err error
		if e.regionLocal(rid) {
			pairs, err = e.scanMerged(tx, []storage.KeyRange{r})
		} else {
			rows, rerr := e.mgr.ScanRemote(e.regionNode(rid), r.Start, r.End, 0)
			if rerr != nil {
				return nil, rerr
			}
			pairs = make([]storage.KVPair, 0, len(rows))
			for _, row := range rows {
				pairs = append(pairs, storage.KVPair{Key: row.Key, Value: row.Value})
			}
			// 单远端区间内按内层键序（与 scanMerged 一致）
			sort.Slice(pairs, func(i, j int) bool {
				return bytes.Compare(innerOf(pairs[i].Key), innerOf(pairs[j].Key)) < 0
			})
		}
		if err != nil {
			return nil, err
		}
		out = append(out, pairs...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return bytes.Compare(innerOf(out[i].Key), innerOf(out[j].Key)) < 0
	})
	return out, nil
}

// ---- 集群管理语句 ----

// execAddNode ADD NODE '<addr>'：注册远端节点。
// 先 TCP 握手（NODE_HELLO/NODE_HELLO_OK 双向身份确认），成功后再写入注册表。
func (e *Executor) execAddNode(s *AddNodeStmt) (*Result, error) {
	if e.mgr == nil {
		return nil, &SQLError{Msg: "cluster not enabled"}
	}
	nodeID, err := e.mgr.AddNode(s.Addr)
	if err != nil {
		return nil, err
	}
	// 注册表落盘 m:nodes（与 m:regions 同层，重启后恢复）
	if err := e.persistNodes(); err != nil {
		return nil, err
	}
	return &Result{
		Columns: []string{"node_id"},
		Rows:    [][]Value{{StrVal(nodeID)}},
	}, nil
}

// execShowNodes SHOW NODES：列出节点注册表。
func (e *Executor) execShowNodes() (*Result, error) {
	if e.mgr == nil {
		return nil, &SQLError{Msg: "cluster not enabled"}
	}
	nodes := e.mgr.ListNodes()
	res := &Result{
		Columns: []string{"node_id", "addr", "role", "state"},
		Rows:    make([][]Value, 0, len(nodes)),
	}
	for _, n := range nodes {
		res.Rows = append(res.Rows, []Value{
			StrVal(n.ID), StrVal(n.Addr), StrVal(string(n.Role)), StrVal(string(n.State)),
		})
	}
	return res, nil
}

// execShowRegionRoutes SHOW REGION ROUTES：列出集群 region 路由表（region → 归属节点）。
func (e *Executor) execShowRegionRoutes() (*Result, error) {
	regions := e.router.AllRegions()
	res := &Result{
		Columns: []string{"region_id", "table_id", "start_key", "end_key", "state", "node", "local"},
		Rows:    make([][]Value, 0, len(regions)),
	}
	for _, rg := range regions {
		res.Rows = append(res.Rows, []Value{
			IntVal(int64(rg.RegionID)),
			IntVal(int64(rg.TableID)),
			StrVal(formatKeyHex(rg.StartKey)),
			StrVal(formatKeyHex(rg.EndKey)),
			StrVal(string(rg.State)),
			StrVal(e.regionNode(rg.RegionID)),
			StrVal(boolStr(e.regionLocal(rg.RegionID))),
		})
	}
	return res, nil
}

// execAssignRegion ASSIGN REGION <id> TO NODE '<nodeID>'：
// 将 region 数据全量推送到目标节点（REGION_PUSH，目标节点学习路由并落盘），
// 随后更新本节点路由归属并落盘 m:regions。
// 数据保留在源节点（第一簇不做删除搬迁）；写路径分布式语义下簇统一。
func (e *Executor) execAssignRegion(s *AssignRegionStmt) (*Result, error) {
	if e.mgr == nil {
		return nil, &SQLError{Msg: "cluster not enabled"}
	}
	rg, ok := e.router.FindRegion(storage.ID(s.RegionID))
	if !ok {
		// 隐式单 region（tableID|1<<63）：表未显式分片时的默认 region，路由未落盘，
		// 按表元数据解析后按显式 region 同流程推送（数据 = 整表物理键空间）。
		rg, ok = e.implicitRegion(storage.ID(s.RegionID))
		if !ok {
			return nil, &SQLError{Msg: fmt.Sprintf("region not found: %d", s.RegionID)}
		}
		// 隐式 region 先显式化注册（否则 SetRegionNode/persistRegions 无法定位条目）
		if err := e.router.UpsertRegion(rg); err != nil {
			return nil, err
		}
	}
	if rg.Node != "" && rg.Node != e.selfID {
		return nil, &SQLError{Msg: "region already assigned to node: " + rg.Node}
	}
	if s.NodeID == e.selfID {
		// 迁回本节点：仅更新路由归属（数据本就在本地）
		if err := e.router.SetRegionNode(rg.RegionID, e.selfID); err != nil {
			return nil, err
		}
		if err := e.persistRegions(); err != nil {
			return nil, err
		}
		return &Result{AffectedRows: 1}, nil
	}
	if _, ok := e.mgr.GetNode(s.NodeID); !ok {
		return nil, &SQLError{Msg: "node not registered: " + s.NodeID}
	}
	// 扫描 region 全量物理数据（[EncodeRegionKey(rid,StartKey), EncodeRegionKey(rid,EndKey))）
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		// 隐式单 region：整表上界 = 下一 region 前缀（半开区间覆盖全部 r{rid} 物理键）
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	pairs, err := e.scanMerged(tx, []storage.KeyRange{{Start: start, End: end}})
	if err != nil {
		return nil, err
	}
	rows := make([]cluster.QueryRow, 0, len(pairs))
	for _, kv := range pairs {
		rows = append(rows, cluster.QueryRow{Key: kv.Key, Value: kv.Value})
	}
	// 源节点路由视图显式化：本地持有（Node==""）的显式 region 补记为本节点，
	// 使视图可跨节点移植（目标节点据 Node 判断转发目标，空串语义仅限本节点）。
	// 本次被指派 region 在视图中归属目标节点（applyPush 会将其单独落本地视图，
	// 但视图本身保持"权威路由"语义，供后续节点学习）。
	regions := e.router.AllRegions()
	for i := range regions {
		if regions[i].RegionID == rg.RegionID {
			regions[i].Node = s.NodeID
			regions[i].Local = false
		} else if regions[i].Node == "" {
			regions[i].Node = e.selfID
			regions[i].Local = false
		}
	}
	// 隐式 region 不在 AllRegions 中：显式追加到视图，使目标节点学到完整路由
	rgView := rg
	rgView.Node = s.NodeID
	rgView.Local = false
	regions = append(regions, rgView)
	// 推送数据 + 路由视图到目标节点（目标节点在同一 WriteBatch 内写数据 + 学习路由 + 落盘）
	if err := e.mgr.PushRegion(s.NodeID, rg, rows, regions, e.selfID); err != nil {
		return nil, err
	}
	// 更新本节点路由归属并落盘
	if err := e.router.SetRegionNode(rg.RegionID, s.NodeID); err != nil {
		return nil, err
	}
	if err := e.persistRegions(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
}

// implicitRegion 解析 M3 隐式单 region（rid = tableID | 1<<63）：
// 表未显式分片时的默认 region，路由不落盘（AllRegions 不含），
// 仅在表元数据存在时按单 region 整表语义构造。StartKey/EndKey 为 nil（开放端）。
func (e *Executor) implicitRegion(rid storage.ID) (sharding.RegionInfo, bool) {
	if uint64(rid)&(uint64(1)<<63) == 0 {
		return sharding.RegionInfo{}, false
	}
	tableID := storage.ID(uint64(rid) &^ (uint64(1) << 63))
	tx, err := e.tm.Begin()
	if err != nil {
		return sharding.RegionInfo{}, false
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return sharding.RegionInfo{}, false
	}
	for _, tb := range tabs {
		if storage.ID(tb.ID) == tableID {
			return sharding.RegionInfo{RegionID: rid, TableID: tableID, State: sharding.StateActive, Node: ""}, true
		}
	}
	return sharding.RegionInfo{}, false
}

// persistRegions 将当前路由表落盘 m:regions（直接写引擎，不经过事务）。
// 仅用于集群管理语句的路由元数据持久化（数据已存在于对应节点）。
func (e *Executor) persistRegions() error {
	raw, err := saveRegionMeta(e)
	if err != nil {
		return err
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.Put(metaRegionsKey, raw); err != nil {
		return err
	}
	return tx.Commit()
}

// persistNodes 将节点注册表落盘 m:nodes（ADD NODE 成功后调用）。
func (e *Executor) persistNodes() error {
	if e.mgr == nil {
		return nil
	}
	raw, err := cluster.MarshalNodes(e.mgr.ListNodes())
	if err != nil {
		return err
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.Put(cluster.MetaNodesKey(), raw); err != nil {
		return err
	}
	return tx.Commit()
}

// ---- 输出辅助 ----

// formatKeyHex 内层键展示为十六进制（nil = 空，表示开放端）。
func formatKeyHex(k []byte) string {
	if len(k) == 0 {
		return ""
	}
	const hexDigits = "0123456789abcdef"
	out := make([]byte, len(k)*2)
	for i, b := range k {
		out[i*2] = hexDigits[b>>4]
		out[i*2+1] = hexDigits[b&0x0f]
	}
	return string(out)
}

// boolStr true/false 展示。
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

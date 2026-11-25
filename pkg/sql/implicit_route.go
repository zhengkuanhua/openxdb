package sql

// v2.0.4 隐式 region 路由广播（根治"未切分表没有 region 归属"）。
//
// 缺陷背景：隐式单 region（rid = tableID | 1<<63）此前从不进入路由表，
// Router.RegionNode 对未登记的 rid 恒返回空串，而空串语义是"本节点归属"。于是集群里
// 每个节点都认为整表归自己：普通节点各自本地写、数据互不可见；纯转发节点
// （clusterOwned=false）的写路径被 routeDecision 判 routeUnassigned，落到归属 fence
// 后报 "region missing from local route view / forward-only node"，永久不可写。
//
// 修复口径（v2.0.4 定稿）：隐式整表 region 的归属**不在建表路径上登记**，而在
// 权威路由视图同步通道里派生并下发：
//   - 发送端：implicitRoutesForViewSync 遍历表目录，对每个"未切分表"按确定性规则
//     （注册表内节点 ID 字典序最小者）声明整表归属，随 Routes 之外独立字段
//     ImplicitRoutes 一起下发；
//   - 接收端：LearnImplicitRoutesFromPeers 只写内存映射 Router.implicit，
//     RegionNode 据此返回真实归属，写路径走 routeRemoteOwned 正常 2PC 转发。
//
// 为什么不在建表/DDL 路径登记、也不落盘：
//  1. DDL 路径上任何新增写事务（哪怕只写 m:region:implicit）都会改变建表/改表与
//     其后切分、副本维护的相对时序，真机与回归用例（D4.5 B1 单副本可恢复性）
//     实测出现稳定复现的时序回归，风险面远大于收益；
//  2. 归属本身是**可派生**的（未切分表 + 确定性 owner 规则），无需持久化：
//     视图同步在启动刷新、对端上线、视图缺失补齐时都会走，学习成本极低；
//  3. 单机/对端全不可达场景下，派生结果本就是"归本机"，与旧语义完全一致。
//
// 为什么独立存放而不是塞进 Router.regions：隐式 region 不是显式分片条目，混入
// regions 会污染 AllRegions / SHOW REGION ROUTES、副本维护与跨节点一致性判定
// （它们只认显式分片）。两类语义各归其位。

import (
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// implicitRouteOwner 隐式 region 归属规则：集群节点 ID 字典序最小者。
//
// 候选集必须含本机：mgr.ListNodes() 只返回对端（self 不入 nodes 表）。若漏掉
// selfID，各节点会得出不同结论（node-a 选到 node-b、node-b 选到 node-a），归属
// 随之漂移。含本机后三节点对同一表得出同一 owner，且与显式分片"首节点优先"一致。
func (e *Executor) implicitRouteOwner() string {
	best := e.selfID
	if e.mgr == nil {
		return best
	}
	for _, n := range e.mgr.ListNodes() {
		if n.ID == "" {
			continue
		}
		if best == "" || n.ID < best {
			best = n.ID
		}
	}
	if best == "" {
		return e.selfID
	}
	return best
}

// implicitRegionRouteFor 构造某表隐式整表 region 的路由视图条目（归属 owner）。
// 该结构仅用于视图同步载荷与管理面展示，不写入 Router.regions。
func (e *Executor) implicitRegionRouteFor(tableID uint64, owner string) sharding.RegionInfo {
	tid := storage.ID(tableID)
	return sharding.RegionInfo{
		RegionID: sharding.ImplicitRegionID(tid),
		TableID:  tid,
		State:    sharding.StateActive,
		Explicit: false,
		Node:     owner,
		Local:    owner == "" || owner == e.selfID,
	}
}

// implicitRoutesForViewSync 本机声明给对端的隐式整表 region 归属，随权威路由
// 视图（REGION_PUSH SyncOnly）的独立字段下发。
//
// 派生口径：
//   - 候选 = 表目录里所有"本地没有显式分片条目"的表（未切分表，隐式整表 region 有效）；
//   - 归属 = 本地已学到的归属（若曾从权威源学过）优先，否则按 implicitRouteOwner 规则；
//   - 单机（无远端节点）返回 nil：无对端可同步，旧语义零回归。
func (e *Executor) implicitRoutesForViewSync() []sharding.RegionInfo {
	if e.router == nil || e.tm == nil || e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return nil
	}
	owner := e.implicitRouteOwner()
	learned := e.router.ImplicitRegions()
	tx, err := e.tm.Begin()
	if err != nil {
		return nil
	}
	tabs, err := loadTables(tx.Get)
	tx.Rollback()
	if err != nil || len(tabs) == 0 {
		return nil
	}
	out := make([]sharding.RegionInfo, 0, len(tabs))
	for _, tb := range tabs {
		if tb == nil || tb.ID == 0 {
			continue
		}
		tid := storage.ID(tb.ID)
		// 已切分表：隐式整表 region 已不存在，以分片视图为准，绝不下发。
		if e.router.HasStoredRegions(tid) {
			continue
		}
		o := owner
		if v, ok := learned[tid]; ok && v != "" {
			o = v
		}
		if o == "" {
			continue
		}
		out = append(out, e.implicitRegionRouteFor(tb.ID, o))
	}
	return out
}

// LearnImplicitRoutesFromPeers 接收端学习权威路由视图携带的隐式整表 region 归属
// （v2.0.4，由 cluster 服务端在收到 REGION_PUSH(SyncOnly) 后转交，见
// cluster.Server.SetImplicitRouteLearnHandler）。
//
// 只写内存映射，不落盘、不开事务：归属可由视图同步随时重学，持久化收益为零而
// 风险（建表/同步路径新增写事务）已在实测中被证伪。幂等：只登记未切分表
// （本地无显式分片条目）的归属，已切分表以分片视图为准。
func (e *Executor) LearnImplicitRoutesFromPeers(routes []sharding.RegionInfo) error {
	if e.router == nil || len(routes) == 0 {
		return nil
	}
	for _, rg := range routes {
		if rg.TableID == 0 || rg.Node == "" || rg.Explicit || !sharding.IsImplicitRegionID(rg.RegionID) {
			continue
		}
		if e.router.HasStoredRegions(rg.TableID) {
			continue
		}
		e.router.SetImplicitRegionOwner(rg.TableID, rg.Node)
	}
	return nil
}

// forgetTableRoutes 表被删除时清理其全部路由条目（含隐式归属）。
func (e *Executor) forgetTableRoutes(tableID uint64) {
	if e.router == nil || tableID == 0 {
		return
	}
	tid := storage.ID(tableID)
	e.router.RemoveTable(tid)
	e.router.ForgetImplicitRegion(tid)
	if e.tm != nil {
		_ = e.persistRegions()
	}
}

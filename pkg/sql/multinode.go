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
	"time"

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

// routeKind 写路径 region 归属判定结果（v1.20.0 C2 强制路由三态+未知归属）。
type routeKind int

const (
	// routeLocalOwned 归属本机（含单机模式与"集群未装配远端节点"兼容路径）。
	routeLocalOwned routeKind = iota
	// routeRemoteOwned 归属已注册的远端节点，需经 2PC 协调者转发。
	routeRemoteOwned
	// routeUnassigned 集群模式下 region 无归属节点（D4.5 孤立视图根因）。
	routeUnassigned
	// routeUnknownOwner 归属节点不在注册表内（owner 不可达/路由残留）。
	routeUnknownOwner
)

// routeDecision 判定写路径 region 归属（v1.20.0 C2）。
//
// 与读路径 regionLocal/regionNode 的区别：读路径把"未指派归属"视为本机
// （M3 单机数据兼容），写路径在集群模式下必须把"未指派"识别为
// routeUnassigned，才能让 ha_write_route_enforce 拒绝静默本地写。
// 单机（mgr==nil）或集群无远端节点时，未指派仍返回 routeLocalOwned，
// 保证 M1-M4 单机语义零回归。
func (e *Executor) routeDecision(rid storage.ID) (string, routeKind) {
	node := e.router.RegionNode(rid)
	if node == "" {
		if e.mgr == nil || !e.mgr.HasRemoteNodes() {
			return e.selfID, routeLocalOwned
		}
		// 隐式单 region（表尚未切分，路由不落盘）没有条目是正常状态，不能
		// 等同于"显式分区丢归属"：否则强制路由会把未切分表的首写判为
		// REGION_NOT_ASSIGNED（D4.5 的孤立视图专指显式 region 的归属缺失）。
		if !e.regionIsExplicit(rid) {
			// v2.0.3 缺陷修复：regionIsExplicit 对"本地视图完全没有该 region
			// 条目"同样返回 false，与"隐式单 region（表未切分）"混为一谈，
			// 于是显式 region 在本机视图缺失时被误判为本机归属，写路径一路
			// 落到本地（紧接被节点级归属 fence 以 "ownership revoked" 拒绝，
			// 归因指错方向，且无法靠重新授权解除）。
			// 这里把三种情形拆开：
			//   ① 隐式 region ID（高位标记）→ 本机，旧语义零回归；
			//   ② 有条目但未标 Explicit（历史未切分表条目）→ 本机，旧语义；
			//   ③ 显式 region ID 且本地无条目 → 归属未知，按缺失处理：
			//      纯转发节点直接判 routeUnassigned（绝不本地写）；
			//      普通节点保持旧行为，避免宽口径回归。
			if sharding.IsImplicitRegionID(rid) {
				// v2.0.4 根治：纯转发节点从未切分表学到归属前绝不本地写
				// （v2.0.3 及以前判 routeLocalOwned，写下去立刻被节点级归属
				// fence 以 "region missing from local route view / forward-only
				// node" 拒绝，用户观感是"未切分表永久不可写"）。判
				// routeUnassigned 后由 decideWriteRoute 触发权威视图同步，
				// 学到归属即走 2PC 直连真实 owner。
				if e.forwardOnlyNode() {
					return "", routeUnassigned
				}
				return e.selfID, routeLocalOwned
			}
			if _, ok := e.router.FindRegion(rid); ok {
				return e.selfID, routeLocalOwned
			}
			if e.forwardOnlyNode() {
				return "", routeUnassigned
			}
			return e.selfID, routeLocalOwned
		}
		return "", routeUnassigned
	}
	if node == e.selfID {
		return node, routeLocalOwned
	}
	if e.mgr == nil {
		return node, routeUnknownOwner
	}
	if _, ok := e.mgr.GetNode(node); !ok {
		return node, routeUnknownOwner
	}
	return node, routeRemoteOwned
}

// forwardOnlyWriteFence 可选能力（v2.0.3）：实现方暴露"本机是否为纯转发
// 节点"（不持有任何 region 归属、写请求全部靠转发完成）。由 db.DB 实现，
// 判定口径与 copyTrackWriteFenceLocked 的豁免条件保持一致。
type forwardOnlyWriteFence interface {
	ForwardOnlyNode() bool
}

// forwardOnlyNode 本机是否为纯转发节点（未装配或实现方不提供该能力 → false）。
func (e *Executor) forwardOnlyNode() bool {
	e.mu.Lock()
	wf := e.writeFence
	e.mu.Unlock()
	if wf == nil {
		return false
	}
	if f, ok := wf.(forwardOnlyWriteFence); ok {
		return f.ForwardOnlyNode()
	}
	return false
}

// routeViewMissing 目标 region 是否"归属未在本机路由视图内"。
//   ① 显式 region：本地完全无该条目；
//   ② 隐式整表 region（表未切分）+ 纯转发节点且尚未从权威视图学到归属：
//      同样是"视图缺失"，而非"无归属"。
func (e *Executor) routeViewMissing(rid storage.ID) bool {
	if e.router == nil {
		return false
	}
	if sharding.IsImplicitRegionID(rid) {
		if !e.forwardOnlyNode() {
			return false
		}
		_, learned := e.router.ImplicitRegionOwner(storage.ID(uint64(rid) &^ sharding.ImplicitRegionBit))
		return !learned
	}
	_, ok := e.router.FindRegion(rid)
	return !ok
}

// routeSyncCooldown 路由视图补齐的冷却窗口：写路径触发补齐后，窗口内不再
// 重复向对端拉取（热点写不会把管理面打成广播风暴）。
const routeSyncCooldown = 2 * time.Second

// requestRouteViewSync 异步向任一在线 peer 拉取一次权威 region 路由视图
// （v2.0.3 自愈）。纯转发节点本地不落任何 region，重启后若权威视图未推送
// 到位，本地视图会长期缺新建表的 region 条目；写路径发现缺失时经此补齐。
// 异步执行，不阻塞写语句；冷却窗口 + 并发互斥限制频率。
func (e *Executor) requestRouteViewSync() {
	if e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return
	}
	e.routeSyncMu.Lock()
	if e.routeSyncBusy || (!e.routeSyncAt.IsZero() && time.Since(e.routeSyncAt) < routeSyncCooldown) {
		e.routeSyncMu.Unlock()
		return
	}
	e.routeSyncAt = time.Now()
	e.routeSyncBusy = true
	e.routeSyncMu.Unlock()
	selfID := e.selfID
	mgr := e.mgr
	go func() {
		defer func() {
			e.routeSyncMu.Lock()
			e.routeSyncBusy = false
			e.routeSyncMu.Unlock()
		}()
		for _, n := range mgr.ListNodes() {
			if n.ID == selfID {
				continue
			}
			if err := mgr.RequestRouteSync(n.ID, selfID); err == nil {
				return
			}
		}
	}()
}

// decideWriteRoute 写路径路由判定（v2.0.3）：在 routeDecision 之上补齐
// "显式 region 本机视图缺失"的自愈。仅在判定为 routeUnassigned 且确实因为
// 视图缺条目时触发一次权威视图拉取，然后重判——视图补齐后本次判定即可
// 得到真实 owner 并走 2PC 转发；补齐尚未完成时返回 routeUnassigned，
// 由调用方以 REGION_NOT_ASSIGNED 拒写（绝不静默落本地）。
func (e *Executor) decideWriteRoute(rid storage.ID) (string, routeKind) {
	owner, kind := e.routeDecision(rid)
	if kind != routeUnassigned || !e.routeViewMissing(rid) {
		return owner, kind
	}
	e.requestRouteViewSync()
	return e.routeDecision(rid)
}

// regionHoldsLocalByID 判定本节点是否物理持有该 region 的键空间数据
// （写路径强制路由的"持有校验"，v1.20.0 C2）。
// 显式 region 取路由条目 RegionInfo；隐式单 region（rid 带 1<<63 位、
// 路由不落盘）走 implicitRegion 构造；两者都解析不到视为不持有。
// tm 未装配（离线装配/测试）时无法判定，放行保持零回归。
func (e *Executor) regionHoldsLocalByID(rid storage.ID) (bool, error) {
	if e.tm == nil {
		return true, nil
	}
	rg, ok := e.router.FindRegion(rid)
	if !ok {
		rg, ok = e.implicitRegion(rid)
		if !ok {
			return false, nil
		}
	}
	return e.regionHoldsLocal(rg)
}

// regionIsExplicit 判定 region 是否为显式分区（表已切分、路由条目已落盘）。
// 写路径强制路由的"持有校验"只对显式 region 生效：隐式单 region 表示该表尚未
// 切分，首次写入前本机当然没有任何键，按持有校验判"不持有"会把正常首写误拒
// （v1.20.0 C2 实测：quorum 用例的空隐式 region 首写被判 REGION_NOT_LOCAL）。
func (e *Executor) regionIsExplicit(rid storage.ID) bool {
	if e.router == nil {
		return false
	}
	rg, ok := e.router.FindRegion(rid)
	if !ok {
		return false
	}
	return rg.Explicit
}

// checkRemoteReady 跨节点读前校验目标节点数据面就绪（P3 读聚合 READY 前置）。
// 未注册/未知节点视为就绪（与"未注册即本地"语义一致，零回归）；
// 显式未就绪（重启对齐中）返回明确错误，调用方拒绝远端转发/降级。
func (e *Executor) checkRemoteReady(nodeID string) error {
	if e.mgr == nil {
		return nil
	}
	if !e.mgr.NodeDataReady(nodeID) {
		return &SQLError{Msg: fmt.Sprintf("node %s data not ready (restart alignment in progress)", nodeID)}
	}
	return nil
}

// getKVCluster 集群感知点查：定位 region 后，本地直读、远端转发。
// 与 tx.Get 相同的错误语义（未命中返回 storage.ErrNotFound）。
// M8 快照隔离：读路径携带 begin_ts，本地/远端均按版本过滤。
func (e *Executor) getKVCluster(sess *Session, tx txn.Txn, key []byte) ([]byte, error) {
	bs := e.snapshotTS(sess)
	if !e.clusterEnabled() {
		bs = 0
	}
	if e.mgr == nil {
		e.incLocal()
		return e.getKVVisible(tx, key, bs)
	}
	rid, _ := sharding.DecodeRegionKey(key)
	if e.regionLocal(rid) {
		e.incLocal()
		return e.getKVVisible(tx, key, bs)
	}
	// M6 读 failover：归属节点 down 时先自动重指派，再按新路由转发。
	// NEW-6：重指派后仍无存活持有者时默认显式报错（不再静默回退本地副本），
	// allow_partial_read=on 才退回旧的"本地副本尽力服务"行为。
	node, oerr := e.resolveReadOwner(rid)
	if oerr != nil {
		return nil, oerr
	}
	if node == e.selfID {
		e.incLocal()
		return e.getKVVisible(tx, key, bs)
	}
	e.incRemote()
	v, err := e.mgr.GetRemote(node, key, bs, e.txContextID(sess))
	if sess.txIso == IsoSerializable {
		// P2-13：远端点查行 key 纳入 Serializable 读集（txn 层不可见）。
		// 无论成败均记录：读不存在（phantom）也要参与检测——后续插入
		// 该 key 的事务提交后本事务提交即冲突。
		sess.mu.Lock()
		if sess.serialRemoteReads == nil {
			sess.serialRemoteReads = map[string]bool{}
		}
		sess.serialRemoteReads[string(key)] = true
		sess.mu.Unlock()
	}
	return v, err
}

// mgetKVCluster 集群感知批量点查：本地键一次批量读（tx.MGet，减少 cgo 往返），
// 远端键逐条走 getKVCluster 转发。与逐条 getKVCluster 语义等价：
// 未命中键不出现在结果中，M8 快照版本过滤行为一致。
func (e *Executor) mgetKVCluster(sess *Session, tx txn.Txn, keys [][]byte) (map[string][]byte, error) {
	bs := e.snapshotTS(sess)
	if !e.clusterEnabled() {
		bs = 0
	}
	got := make(map[string][]byte, len(keys))
	if e.mgr == nil {
		e.incLocal()
		m, err := tx.MGet(keys)
		if err != nil {
			return nil, err
		}
		if bs == 0 {
			return m, nil
		}
		for k, v := range m {
			visible, verr := cluster.VersionVisible(tx.Scan, []byte(k), bs)
			if verr != nil {
				return nil, verr
			}
			if visible {
				got[k] = v
			}
		}
		return got, nil
	}
	var localKeys [][]byte
	for _, key := range keys {
		rid, _ := sharding.DecodeRegionKey(key)
		if e.regionLocal(rid) {
			localKeys = append(localKeys, key)
			continue
		}
		v, err := e.getKVCluster(sess, tx, key)
		if err != nil && err != storage.ErrNotFound {
			return nil, err
		}
		if err == nil {
			got[string(key)] = v
		}
	}
	if len(localKeys) > 0 {
		e.incLocal()
		m, err := tx.MGet(localKeys)
		if err != nil {
			return nil, err
		}
		for k, v := range m {
			if bs == 0 {
				got[k] = v
				continue
			}
			visible, verr := cluster.VersionVisible(tx.Scan, []byte(k), bs)
			if verr != nil {
				return nil, verr
			}
			if visible {
				got[k] = v
			}
		}
	}
	return got, nil
}

// getKVVisible 本地读取 + M8 快照版本过滤：begin_ts=0 时不过滤（旧路径零回归）。
// 命中行若存在 commit_ts>begin_ts 的版本记录则视为不可见（未命中）。
func (e *Executor) getKVVisible(tx txn.Txn, key []byte, beginTS uint64) ([]byte, error) {
	v, err := tx.Get(key)
	if err != nil {
		return nil, err
	}
	if beginTS == 0 {
		return v, nil
	}
	visible, verr := cluster.VersionVisible(tx.Scan, key, beginTS)
	if verr != nil {
		return nil, verr
	}
	dbgLog("VIS key=%q beginTS=%d visible=%v vlen=%d", string(key), beginTS, visible, len(v))
	if !visible {
		return nil, storage.ErrNotFound
	}
	return v, nil
}

// scanMergedVisible 本地合并扫描 + M8 快照版本过滤：begin_ts=0 时不过滤。
func (e *Executor) scanMergedVisible(tx txn.Txn, ranges []storage.KeyRange, beginTS uint64) ([]storage.KVPair, error) {
	dbgLog("SMV enter ranges=%d beginTS=%d mgr=%v", len(ranges), beginTS, e.mgr != nil)
	pairs, err := e.scanMerged(tx, ranges)
	if err != nil {
		return nil, err
	}
	if beginTS == 0 {
		return pairs, nil
	}
	keys := make([][]byte, len(pairs))
	for i, kv := range pairs {
		keys[i] = kv.Key
	}
	vis, verr := cluster.BatchVersionVisible(tx.Scan, keys, beginTS)
	if verr != nil {
		return nil, verr
	}
	out := make([]storage.KVPair, 0, len(pairs))
	for i, kv := range pairs {
		if vis[i] {
			out = append(out, kv)
		}
	}
	return out, nil
}

// scanMergedCluster 集群感知范围扫描：按 ranges 逐 region 展开执行并合并。
// 与 scanMerged 相同的结果语义（按内层键序合并、等价单机全表序）：
//   - 本地 region → 直接扫描本事务（M8 按 begin_ts 快照过滤）；
//   - 远端 region → QUERY_REQ 转发区间扫描（携带 beginTS，归属节点按版本过滤）。
func (e *Executor) scanMergedCluster(sess *Session, tx txn.Txn, ranges []storage.KeyRange) ([]storage.KVPair, error) {
	bs := e.snapshotTS(sess)
	if !e.clusterEnabled() {
		bs = 0
	}
	if e.mgr == nil {
		e.incLocal()
		return e.scanMergedVisible(tx, ranges, bs)
	}
	// v1.19.0：远端区间聚合开关开启时按归属节点合并单次往返（默认关闭，
	// 关闭走下方 v1.18.0 逐区间独立 QUERY_REQ 路径，行为零回归）。
	if e.RemoteRangeAgg() {
		return e.scanMergedClusterAgg(sess, tx, ranges, bs)
	}
	var out []storage.KVPair
	for _, r := range ranges {
		rid, _ := sharding.DecodeRegionKey(r.Start)
		var pairs []storage.KVPair
		var err error
		if e.regionLocal(rid) {
			e.incLocal()
			pairs, err = e.scanMergedVisible(tx, []storage.KeyRange{r}, bs)
		} else {
			// M6 读 failover：归属节点 down 时先自动重指派再转发；
			// NEW-6：无存活持有者时默认显式报错（旧行为需 allow_partial_read=on）。
			node, oerr := e.resolveReadOwner(rid)
			if oerr != nil {
				return nil, oerr
			}
			if node == e.selfID {
				e.incLocal()
				pairs, err = e.scanMergedVisible(tx, []storage.KeyRange{r}, bs)
			} else {
				// P3 读聚合 READY 前置：目标节点数据面未就绪时拒绝远端转发。
				if err = e.checkRemoteReady(node); err != nil {
					return nil, err
				}
				e.incRemote()
				rows, rerr := e.mgr.ScanRemote(node, r.Start, r.End, 0, bs, e.txContextID(sess))
				if rerr != nil {
					return nil, rerr
				}
				pairs = make([]storage.KVPair, 0, len(rows))
				for _, row := range rows {
					pairs = append(pairs, storage.KVPair{Key: row.Key, Value: row.Value})
				}
				// P2-13：远端范围扫描返回的行 key 纳入 Serializable 读集。
				if sess.txIso == IsoSerializable && len(rows) > 0 {
					sess.mu.Lock()
					if sess.serialRemoteReads == nil {
						sess.serialRemoteReads = map[string]bool{}
					}
					for _, row := range rows {
						sess.serialRemoteReads[string(row.Key)] = true
					}
					sess.mu.Unlock()
				}
				// 单远端区间内按内层键序（与 scanMerged 一致）
				sort.Slice(pairs, func(i, j int) bool {
					return bytes.Compare(innerOf(pairs[i].Key), innerOf(pairs[j].Key)) < 0
				})
			}
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

// scanMergedClusterAgg 跨 region 远端区间聚合扫描（v1.19.0 remote_range_agg）：
// 与 scanMergedCluster 结果集语义等价——本地 region 仍走本地合并扫描，远端
// region 按归属节点分组后**每节点单次 QUERY_MULTI_REQ 往返**（旧路径每区间一次
// QUERY_REQ），服务端按区间流式分块回传，把跨节点多区间读的往返次数从
// O(区间数) 降为 O(归属节点数)。以下语义与旧路径逐项对齐：
//   - M6 读 failover：归属节点 down 时先 resolveRegionOwner 自动重指派，无存活
//     目标回退本地扫描（逐区间判定，与旧路径相同）；
//   - P3 读聚合 READY 前置：目标节点数据面未就绪时整体拒绝（错误前置返回）；
//   - P2-13：返回行 key 在 Serializable 隔离级下登记 serialRemoteReads；
//   - 行序：合并后按内层键序全局稳定排序（与扫描输出契约一致）。
//
// 指标口径：incRemote 按"远端请求次数"计（旧路径每区间一次、本路径每节点一次），
// 该口径与真实网络往返数一致；incLocal 仍按本地合并扫描次数计。
func (e *Executor) scanMergedClusterAgg(sess *Session, tx txn.Txn, ranges []storage.KeyRange, bs uint64) ([]storage.KVPair, error) {
	// 1) 区间分类：本地区间合并为一组；远端区间按最终归属节点分组（保持区间原序）
	localRanges := make([]storage.KeyRange, 0, len(ranges))
	groups := make(map[string][]cluster.QueryRange, 1)
	var order []string
	for _, r := range ranges {
		rid, _ := sharding.DecodeRegionKey(r.Start)
		if e.regionLocal(rid) {
			localRanges = append(localRanges, r)
			continue
		}
		// NEW-6：归属不可达且无存活接管者时显式报错，避免聚合读静默少算一部分区段。
		node, oerr := e.resolveReadOwner(rid)
		if oerr != nil {
			return nil, oerr
		}
		if node == e.selfID {
			localRanges = append(localRanges, r)
			continue
		}
		if err := e.checkRemoteReady(node); err != nil {
			return nil, err
		}
		if _, ok := groups[node]; !ok {
			order = append(order, node)
		}
		groups[node] = append(groups[node], cluster.QueryRange{Start: r.Start, End: r.End})
	}
	out := make([]storage.KVPair, 0, len(ranges))
	// 2) 本地区间一次合并扫描（与旧路径逐区间本地扫描的并集等价）
	if len(localRanges) > 0 {
		e.incLocal()
		pairs, err := e.scanMergedVisible(tx, localRanges, bs)
		if err != nil {
			return nil, err
		}
		out = append(out, pairs...)
	}
	// 3) 每个归属节点单次往返取回其全部远端区间
	txID := e.txContextID(sess)
	for _, node := range order {
		e.incRemote()
		perRange, err := e.mgr.ScanMultiRemote(node, groups[node], bs, txID)
		if err != nil {
			return nil, err
		}
		var keys [][]byte
		for _, rows := range perRange {
			for _, row := range rows {
				out = append(out, storage.KVPair{Key: row.Key, Value: row.Value})
				keys = append(keys, row.Key)
			}
		}
		// P2-13：远端范围扫描返回的行 key 纳入 Serializable 读集。
		if sess.txIso == IsoSerializable && len(keys) > 0 {
			sess.mu.Lock()
			if sess.serialRemoteReads == nil {
				sess.serialRemoteReads = map[string]bool{}
			}
			for _, k := range keys {
				sess.serialRemoteReads[string(k)] = true
			}
			sess.mu.Unlock()
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return bytes.Compare(innerOf(out[i].Key), innerOf(out[j].Key)) < 0
	})
	return out, nil
}

// ---- 集群管理语句 ----

// execAddNode ADD NODE '<addr>'：注册远端节点。
// 先 TCP 握手（NODE_HELLO/NODE_HELLO_OK 双向身份确认），成功后再写入注册表；
// 随后向全拓扑广播注册表快照（G2 修复：使 B/C 互相可达，跨节点转发不报 not registered）。
func (e *Executor) execAddNode(sess *Session, s *AddNodeStmt) (*Result, error) {
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
	// G2 修复：注册表广播为尽力而为，单节点失败不阻断 ADD 回执（日志记录）。
	// P3 join 同步路由：新节点加入即标记数据面未就绪并广播（SetDataReady 内部
	// 即广播注册表，读路径 READY 前置拒绝转发，直到其完成数据对齐后
	// SetDataReady(true)）；随后下发本机权威 region 路由表，避免新节点凭空路由
	// /旧路由表服务导致路由分裂。
	if err := e.mgr.SetDataReady(nodeID, false); err != nil {
		dbgLog("add node: registry broadcast partial: %v", err)
	}
	if err := e.syncRegionsToNode(nodeID); err != nil {
		dbgLog("add node: route sync partial: %v", err)
	}
	return &Result{
		Columns: []string{"node_id"},
		Rows:    [][]Value{{StrVal(nodeID)}},
	}, nil
}

// execShowNodes SHOW NODES：列出节点注册表。
func (e *Executor) execShowNodes(sess *Session) (*Result, error) {
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
func (e *Executor) execShowRegionRoutes(sess *Session) (*Result, error) {
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
func (e *Executor) execAssignRegion(sess *Session, s *AssignRegionStmt) (*Result, error) {
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
			// 仅本地真正持有数据的 region 补记为本节点；未持有者保持未指派，
			// 避免 Node=="" 但数据在别处的 region 被无条件补记污染为本地归属。
			holds, err := e.regionHoldsLocal(regions[i])
			if err != nil {
				return nil, err
			}
			if holds {
				regions[i].Node = e.selfID
				regions[i].Local = false
			}
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
	// 路由变更全网广播：向其余所有已注册节点增量同步本次变更条目（纯路由，无数据），
	// 避免其他节点残留旧归属导致写落错节点 / 读转发落空；
	// 仅广播被指派 region 条目，避免全量视图扩散本机残留漂移。
	if err := e.mgr.BroadcastRegions([]sharding.RegionInfo{rgView}, nil, e.selfID); err != nil {
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

// normalizeRegionOwnerView 落盘前按「归属唯一事实源」归一 Local 标记（v1.20.1 P0-2）。
//
// 不变量（与 pkg/cluster.normalizeRouteOwner 同语义）：RegionInfo.Node 是 region 归属的
// 唯一事实源，Local 必须由 Node 推导 —— Node 为本节点或未指派时 Local 保持 true；
// Node 为远端时 Local 必须为 false。
//
// 本地 ASSIGN REGION / 迁移 / balance / failover 只调 router.SetRegionNode* 改 Node，
// 并不改 Local，于是"归属已外移、远端视图同步尚未到达"的窗口内，本机视图会短暂出现
// "owner 是远端 + local=true" 的矛盾条目：SHOW REGION ROUTES 与 waitRoute 类断言随同步
// 时机抖动（v1.20.0 期 TestV1200C4TwoPCWriteEntersRegionRepl 偶发失败即此），读路径也可能
// 把权威归属在远端的 region 误当本地副本。统一在落盘前收敛，保证任何持久化视图都满足
// 不变量，且不依赖远端推送时机。
//
// 仅改内存 Local 标记与序列化结果，不触碰物理键；返回被归一的条目数。
func (e *Executor) normalizeRegionOwnerView() int {
	if e.router == nil || e.selfID == "" {
		return 0
	}
	fixed := 0
	for _, rg := range e.router.AllRegions() {
		if rg.Node == "" || rg.Node == e.selfID || !rg.Local {
			continue
		}
		rg.Local = false
		if err := e.router.UpsertRegion(rg); err != nil {
			continue
		}
		fixed++
	}
	return fixed
}

// persistRegions 将当前路由表落盘 m:regions（直接写引擎，不经过事务）。
// 仅用于集群管理语句的路由元数据持久化（数据已存在于对应节点）。
func (e *Executor) persistRegions() error {
	e.normalizeRegionOwnerView()
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

// regionHoldsLocal 判断本节点物理键空间 [EncodeRegionKey(rid,StartKey), EncodeRegionKey(rid,EndKey))
// 是否存在至少一个键（主表或索引行）。用于 Node=="" 补记 selfID 前的持有校验：
// 仅本地真正持有数据的 region 才有资格补记为本节点，防止未持有 region（同源初始化
// Node 全空 + 未收到 push 的远端 region）被无条件补记污染为本地归属。
func (e *Executor) regionHoldsLocal(rg sharding.RegionInfo) (bool, error) {
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		// 开放上界：半开区间覆盖全部 r{rid} 物理键（与 execAssignRegion 一致）
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	ps, err := tx.Scan(storage.KeyRange{Start: start, End: end}, 1)
	if err != nil {
		return false, err
	}
	return len(ps) > 0, nil
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

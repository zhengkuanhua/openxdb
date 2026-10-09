package sql

// M7：Region 自动分裂 / 负载均衡 / 在线迁移（Split & Balance）。
//
// 设计要点：
//   - 自动分裂：写路径（execInsert/execUpdate/execDelete）提交后累计表写计数，
//     水位达到阈值一半时做真实行数检查，本地 region 行数超过阈值则沿数据中点
//     一分为二（边界复用 EncodeRegionKey 语义，StartKey 含 / EndKey 排他）；
//     分裂在同一事务内完成「存量数据重路由 + 路由表替换 + 元数据落盘」，路由与
//     副本同步更新；与 M3 手动 SplitTable 兼容（SPLIT REGION 语句按数据中点分裂，
//     显式边界仍走 SplitTable）。
//   - 负载均衡：手动 BALANCE 或周期自动触发，统计本节点本地 region 行数负载，
//     当本节点 region 数显著多于最闲存活节点时，把最大 region 在线迁移过去。
//     节点 down 场景由 M6 failover 负责（不在此重复处理）。
//   - 在线迁移：迁移过程源节点路由不变、读写不中断；扫描 + REGION_PUSH 装载
//     目标节点（数据与权威路由视图原子落盘）后，才切换本节点路由归属并落盘。
//     迁移完成后的读写经路由转发到目标节点。

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ---- 自动分裂配置与写路径触发 ----

// setAutoSplit 配置自动分裂：enabled 开启、rowThreshold 为本地 region 行数阈值。
// 关闭时清空写计数水位。
func (e *Executor) setAutoSplit(enabled bool, rowThreshold int64) {
	e.splitMu.Lock()
	defer e.splitMu.Unlock()
	e.splitCfg.enabled = enabled
	e.splitCfg.rowThreshold = rowThreshold
	if !enabled {
		e.splitAcc = map[uint64]int64{}
	}
}

// maybeAutoSplit 写提交后的自动分裂入口（execInsert/execUpdate/execDelete 尾部调用）。
// 显式会话事务内不触发（分裂是独立事务，避免与用户事务交叠——调用点已在 autocommit
// 分支之外，显式事务路径同样经过此处；为保证不破坏显式事务语义，这里用水位+扫描
// 的轻量检查，分裂本身开新事务，与用户事务互不干扰）。
func (e *Executor) maybeAutoSplit(meta *TableMeta) error {
	if meta == nil {
		return nil
	}
	e.splitMu.Lock()
	if !e.splitCfg.enabled || e.splitCfg.rowThreshold <= 0 {
		e.splitMu.Unlock()
		return nil
	}
	acc := e.splitAcc[uint64(meta.ID)] + 1
	if acc < e.splitCfg.rowThreshold/2 {
		e.splitAcc[uint64(meta.ID)] = acc
		e.splitMu.Unlock()
		return nil
	}
	e.splitAcc[uint64(meta.ID)] = 0
	e.splitMu.Unlock()
	return e.checkAutoSplit(meta)
}

// checkAutoSplit 检查表各本地 region 行数，第一个超阈值即分裂（单次分裂一个，
// 下一轮写/水位再触发继续分裂，避免单语句内递归爆栈与长事务）。
func (e *Executor) checkAutoSplit(meta *TableMeta) error {
	for _, rg := range e.regionsOf(meta) {
		if !e.regionLocal(rg.RegionID) {
			continue
		}
		n, err := e.countRegionRows(meta, rg)
		if err != nil {
			return err
		}
		if n >= e.splitCfg.rowThreshold {
			return e.splitRegion(meta, rg)
		}
	}
	return nil
}

// countRegionRows 统计 region 内行键数量（innerKey 前缀 's'；索引键 'i' 不计入）。
func (e *Executor) countRegionRows(meta *TableMeta, rg sharding.RegionInfo) (int64, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		// 开放上界：下一 region 前缀覆盖全部 r{rid} 物理键
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	pairs, err := e.scanMerged(tx, []storage.KeyRange{{Start: start, End: end}})
	if err != nil {
		return 0, err
	}
	var n int64
	for _, kv := range pairs {
		inner := innerOf(kv.Key)
		if len(inner) > 0 && inner[0] == 's' {
			n++
		}
	}
	return n, nil
}

// ---- 分裂 ----

// splitRegion 将单个本地 region 沿数据中点一分为二：
// 边界 = 行键排序中点（EncodeRegionKey 边界语义：r1.EndKey=boundary 排他，
// r2.StartKey=boundary 包含），数据、索引键在同一事务内重路由到两个子 region，
// 路由表替换与 m:regions 落盘同事务原子完成；副本（Node/Local）继承原 region。
func (e *Executor) splitRegion(meta *TableMeta, rg sharding.RegionInfo) error {
	if !e.regionLocal(rg.RegionID) {
		return &SQLError{Msg: fmt.Sprintf("region %d not local", rg.RegionID)}
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	pairs, err := e.scanMerged(tx, []storage.KeyRange{{Start: start, End: end}})
	if err != nil {
		return err
	}
	var innerKeys [][]byte
	for _, kv := range pairs {
		inner := innerOf(kv.Key)
		if len(inner) > 0 && inner[0] == 's' {
			innerKeys = append(innerKeys, inner)
		}
	}
	if len(innerKeys) < 2 {
		return nil // 不足两行不分裂（保持单 region）
	}
	sort.Slice(innerKeys, func(i, j int) bool { return bytes.Compare(innerKeys[i], innerKeys[j]) < 0 })
	boundary := innerKeys[len(innerKeys)/2]
	// 构造表级新 region 列表（显式 ID 从 seq 水位继续分配）
	seq, all := e.router.Dump()
	var newRs []sharding.RegionInfo
	replaced := false
	for _, r := range all {
		if r.TableID != storage.ID(meta.ID) {
			continue
		}
		if r.RegionID == rg.RegionID {
			seq++
			r1 := sharding.RegionInfo{
				RegionID: storage.ID(seq), TableID: storage.ID(meta.ID),
				StartKey: rg.StartKey, EndKey: boundary,
				State: sharding.StateActive, Local: rg.Local, Explicit: true, Node: rg.Node,
			}
			seq++
			r2 := sharding.RegionInfo{
				RegionID: storage.ID(seq), TableID: storage.ID(meta.ID),
				StartKey: boundary, EndKey: rg.EndKey,
				State: sharding.StateActive, Local: rg.Local, Explicit: true, Node: rg.Node,
			}
			newRs = append(newRs, r1, r2)
			replaced = true
		} else {
			newRs = append(newRs, r)
		}
	}
	if !replaced {
		// 隐式单 region（表未显式分片）：整表替换为两个显式 region
		seq++
		r1 := sharding.RegionInfo{
			RegionID: storage.ID(seq), TableID: storage.ID(meta.ID),
			StartKey: nil, EndKey: boundary,
			State: sharding.StateActive, Local: true, Explicit: true,
		}
		seq++
		r2 := sharding.RegionInfo{
			RegionID: storage.ID(seq), TableID: storage.ID(meta.ID),
			StartKey: boundary, EndKey: nil,
			State: sharding.StateActive, Local: true, Explicit: true,
		}
		newRs = append(newRs, r1, r2)
	}
	// 存量数据重路由 + 更新路由表 + 落盘（与 SplitTable 同事务语义）
	if err := e.remapTable(tx, meta, newRs); err != nil {
		return err
	}
	e.router.ReplaceRegions(storage.ID(meta.ID), newRs, seq)
	raw, err := saveRegionMeta(e)
	if err != nil {
		return err
	}
	if err := tx.Put(metaRegionsKey, raw); err != nil {
		return err
	}
	return tx.Commit()
}

// execSplitRegion SPLIT REGION <regionID>：手动分裂入口（沿数据中点）。
func (e *Executor) execSplitRegion(s *SplitRegionStmt) (*Result, error) {
	rid := storage.ID(s.RegionID)
	rg, ok := e.router.FindRegion(rid)
	if !ok {
		rg, ok = e.implicitRegion(rid)
		if !ok {
			return nil, &SQLError{Msg: fmt.Sprintf("region not found: %d", s.RegionID)}
		}
	}
	if !e.regionLocal(rg.RegionID) {
		return nil, &SQLError{Msg: "region not local"}
	}
	meta := e.tableMetaByID(rg.TableID)
	if meta == nil {
		return nil, &SQLError{Msg: "table not found"}
	}
	if err := e.splitRegion(meta, rg); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
}

// splitRegionByID Engine 手动触发入口（函数式，兼容旧手动 SPLIT）。
func (e *Executor) splitRegionByID(regionID uint64) error {
	rid := storage.ID(regionID)
	rg, ok := e.router.FindRegion(rid)
	if !ok {
		rg, ok = e.implicitRegion(rid)
		if !ok {
			return &SQLError{Msg: fmt.Sprintf("region not found: %d", regionID)}
		}
	}
	if !e.regionLocal(rg.RegionID) {
		return &SQLError{Msg: "region not local"}
	}
	meta := e.tableMetaByID(rg.TableID)
	if meta == nil {
		return &SQLError{Msg: "table not found"}
	}
	return e.splitRegion(meta, rg)
}

// tableMetaByID 按表 ID 查表元数据。
func (e *Executor) tableMetaByID(tid storage.ID) *TableMeta {
	tx, err := e.tm.Begin()
	if err != nil {
		return nil
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil
	}
	for _, tb := range tabs {
		if storage.ID(tb.ID) == tid {
			return tb
		}
	}
	return nil
}

// ---- 负载均衡 ----

// regionLoad 单个 region 的负载信息。
type regionLoad struct {
	rg   sharding.RegionInfo
	rows int64
}

// execBalance BALANCE：手动触发一次负载均衡。
func (e *Executor) execBalance(s *BalanceStmt) (*Result, error) {
	// 集群启用判据：管理器存在且注册了至少一个远端节点。
	// 单机模式（db.Open 默认创建 Manager 但无远端节点）视作未启用，拒绝执行。
	if e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return nil, &SQLError{Msg: "cluster not enabled"}
	}
	n, err := e.balanceOnce()
	if err != nil {
		return nil, err
	}
	return &Result{AffectedRows: n}, nil
}

// setAutoBalance 配置自动均衡：enabled 开启后按 interval 周期执行 balanceOnce。
// 重复开启幂等；关闭时停止循环等待退出。
func (e *Executor) setAutoBalance(enabled bool, interval time.Duration) {
	e.balanceMu.Lock()
	defer e.balanceMu.Unlock()
	if enabled && e.balanceStop == nil {
		if interval <= 0 {
			interval = 3 * time.Second
		}
		e.balanceStop = make(chan struct{})
		e.balanceDone = make(chan struct{})
		go e.balanceLoop(interval)
	} else if !enabled && e.balanceStop != nil {
		close(e.balanceStop)
		<-e.balanceDone
		e.balanceStop = nil
		e.balanceDone = nil
	}
}

// stopAutoBalance 停止自动均衡循环（db.Close 装配）。
func (e *Executor) stopAutoBalance() {
	e.setAutoBalance(false, 0)
}

// balanceLoop 自动均衡周期循环。
func (e *Executor) balanceLoop(interval time.Duration) {
	defer close(e.balanceDone)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if e.mgr == nil {
				continue
			}
			if _, err := e.balanceOnce(); err != nil {
				// 单轮失败不中断循环（如目标节点瞬时不可达），下轮重试
				continue
			}
		case <-e.balanceStop:
			return
		}
	}
}

// balanceOnce 执行一轮均衡：本节点本地 region 数显著多于最闲存活节点时，
// 迁移行数最大的本地 region 到该目标节点。返回迁移 region 数（0 表示无需迁移）。
func (e *Executor) balanceOnce() (int, error) {
	e.balanceMu.Lock()
	defer e.balanceMu.Unlock()
	if e.mgr == nil {
		return 0, nil
	}
	var candidates []string
	for _, n := range e.mgr.ListNodes() {
		if n.ID == e.selfID {
			continue
		}
		if e.mgr.IsDown(n.ID) {
			continue
		}
		candidates = append(candidates, n.ID)
	}
	if len(candidates) == 0 {
		return 0, nil
	}
	local, err := e.localRegionLoad()
	if err != nil {
		return 0, err
	}
	if len(local) == 0 {
		return 0, nil
	}
	// 目标 = 持有 region 数最少的存活节点（第一簇不做跨节点行数查询，用 region 数近似负载）
	target := candidates[0]
	minRegions := int(^uint(0) >> 1)
	for _, c := range candidates {
		cnt := e.regionCountOnNode(c)
		if cnt < minRegions {
			minRegions = cnt
			target = c
		}
	}
	// 均衡判据：本节点 region 数 > 目标节点 region 数 + 1 才迁移（避免乒乓迁移）
	if len(local) <= minRegions+1 {
		return 0, nil
	}
	// 迁移行数最大的本地 region
	sort.Slice(local, func(i, j int) bool { return local[i].rows > local[j].rows })
	if err := e.migrateRegion(local[0].rg, target); err != nil {
		return 0, err
	}
	return 1, nil
}

// regionCountOnNode 路由视图中归属 nodeID 的 region 数。
func (e *Executor) regionCountOnNode(nodeID string) int {
	cnt := 0
	for _, rg := range e.router.AllRegions() {
		if rg.Node == nodeID {
			cnt++
		}
	}
	return cnt
}

// localRegionLoad 收集本节点持有的全部本地 region 负载（显式路由 + 各表隐式单 region）。
func (e *Executor) localRegionLoad() ([]regionLoad, error) {
	var out []regionLoad
	for _, rg := range e.router.AllRegions() {
		if !e.regionLocal(rg.RegionID) {
			continue
		}
		meta := e.tableMetaByID(rg.TableID)
		if meta == nil {
			continue
		}
		n, err := e.countRegionRows(meta, rg)
		if err != nil {
			return nil, err
		}
		out = append(out, regionLoad{rg: rg, rows: n})
	}
	// 未显式分片表的隐式单 region（不落盘，AllRegions 不含）
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	for _, tb := range tabs {
		rs := e.regionsOf(tb)
		if len(rs) == 1 && !rs[0].Explicit {
			rid := storage.ID(uint64(tb.ID) | (uint64(1) << 63))
			rg, ok := e.implicitRegion(rid)
			if !ok {
				continue
			}
			n, err := e.countRegionRows(tb, rg)
			if err != nil {
				return nil, err
			}
			out = append(out, regionLoad{rg: rg, rows: n})
		}
	}
	return out, nil
}

// ---- 在线迁移 ----

// migrateRegion 将本地 region 在线迁移到目标节点：
//  1. 扫描 region 全量物理数据（期间路由未切换，读写仍走本节点，服务不中断）；
//  2. REGION_PUSH 装载目标节点（数据 + 权威路由视图在同一 WriteBatch 原子落盘）；
//  3. 迁移完成后切换本节点路由归属并落盘（后续读写按路由转发目标节点）。
//
// 源节点数据保留（作为备份副本），符合"迁移过程保持服务"语义；
// 目标节点 down 由 M6 failover 的重指派路径处理，不在此判断。
func (e *Executor) migrateRegion(rg sharding.RegionInfo, target string) error {
	// 集群启用判据同上：未注册任何远端节点时视作单机模式，拒绝在线迁移。
	if e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return &SQLError{Msg: "cluster not enabled"}
	}
	if !e.regionLocal(rg.RegionID) {
		return &SQLError{Msg: "region not local"}
	}
	if _, ok := e.mgr.GetNode(target); !ok {
		return &SQLError{Msg: "node not registered: " + target}
	}
	// 隐式 region 先显式化注册（与 execAssignRegion 一致，否则 SetRegionNode 无法定位）
	if _, ok := e.router.FindRegion(rg.RegionID); !ok {
		if err := e.router.UpsertRegion(rg); err != nil {
			return err
		}
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	pairs, err := e.scanMerged(tx, []storage.KeyRange{{Start: start, End: end}})
	if err != nil {
		return err
	}
	rows := make([]cluster.QueryRow, 0, len(pairs))
	for _, kv := range pairs {
		rows = append(rows, cluster.QueryRow{Key: kv.Key, Value: kv.Value})
	}
	// 源节点权威路由视图：本地持有（Node==""）的 region 补记为本节点；
	// 被迁移 region 在视图中归属目标节点（供目标节点学习完整视图）。
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
	rgView := rg
	rgView.Node = target
	rgView.Local = false
	if _, ok := e.router.FindRegion(rg.RegionID); !ok {
		regions = append(regions, rgView)
	}
	// 装载目标节点（迁移期间源节点路由不变，读写不中断）
	if err := e.mgr.PushRegion(target, rg, rows, regions, e.selfID); err != nil {
		return err
	}
	// 迁移完成：切换路由归属并落盘（后续读写转发目标节点）
	if err := e.router.SetRegionNode(rg.RegionID, target); err != nil {
		return err
	}
	if err := e.persistRegions(); err != nil {
		return err
	}
	return tx.Commit()
}

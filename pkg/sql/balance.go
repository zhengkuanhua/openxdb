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
	"log"
	"sort"
	"strconv"
	"strings"
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
			// v2.0.8 Q2：splitRegion 返回是否真正分裂；自动分裂路径忽略该标记。
			_, serr := e.splitRegion(meta, rg)
			return serr
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
//
// v2.0.8 Q2：返回 (split, err)。split=false 表示 region 内有效行不足两行、
// 无法求中点因而未产生任何新 region（元数据与路由均未改动，属"未分裂"而非
// "分裂成功"）；调用方据此构造 AffectedRows，避免把空 region 的 SPLIT 误报成功。
func (e *Executor) splitRegion(meta *TableMeta, rg sharding.RegionInfo) (bool, error) {
	if !e.regionLocal(rg.RegionID) {
		return false, &SQLError{Msg: fmt.Sprintf("region %d not local", rg.RegionID)}
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	pairs, err := e.scanMerged(tx, []storage.KeyRange{{Start: start, End: end}})
	if err != nil {
		return false, err
	}
	var innerKeys [][]byte
	for _, kv := range pairs {
		inner := innerOf(kv.Key)
		if len(inner) > 0 && inner[0] == 's' {
			innerKeys = append(innerKeys, inner)
		}
	}
	if len(innerKeys) < 2 {
		// 不足两行不分裂（保持单 region）：v2.0.8 Q2 语义化为"未分裂"。
		return false, nil
	}
	sort.Slice(innerKeys, func(i, j int) bool { return bytes.Compare(innerKeys[i], innerKeys[j]) < 0 })
	boundary := innerKeys[len(innerKeys)/2]
	// 构造表级新 region 列表（显式 ID 从 seq 水位继续分配）
	seq, all := e.router.Dump()
	// 水位可能被历史隐式 ID 污染（≥1<<63）：分配前先收敛回显式 ID 区间，否则下面
	// 造出的"显式" region ID 恰好等于某张表 tableID|1<<63 的隐式 ID，两张表共用
	// 同一物理键前缀 'r'+regionID，重路由后读路径按区间扫描立即串台；同时该 ID 会
	// 被 IsImplicitRegionID 误判为隐式，水位不再推进，每次分裂复用同一批撞号 ID。
	seq = sharding.SanitizeRegionSeq(seq, all)
	if seq+2 >= sharding.ExplicitRegionIDLimit {
		return false, &SQLError{Msg: "explicit region id space exhausted"}
	}
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
		return false, err
	}
	e.router.ReplaceRegions(storage.ID(meta.ID), newRs, seq)
	raw, err := saveRegionMeta(e)
	if err != nil {
		return false, err
	}
	if err := tx.Put(metaRegionsKey, raw); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	// 路由变更全网广播：SPLIT 产生的新 region 划分向所有已注册节点增量同步，
	// 并删除旧 region 路由，避免各节点视图不一致导致路由错乱；
	// 仅广播本次新增的两个 region 条目，避免全量视图扩散残留漂移。
	newID1 := storage.ID(seq - 1)
	newID2 := storage.ID(seq)
	var changed []sharding.RegionInfo
	for _, r := range newRs {
		if r.RegionID == newID1 || r.RegionID == newID2 {
			changed = append(changed, r)
		}
	}
	if err := e.broadcastRegionView(changed, []storage.ID{rg.RegionID}); err != nil {
		return false, err
	}
	return true, nil
}

// execSplitRegion SPLIT REGION <regionID>：手动分裂入口（沿数据中点）。
func (e *Executor) execSplitRegion(sess *Session, s *SplitRegionStmt) (*Result, error) {
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
	split, err := e.splitRegion(meta, rg)
	if err != nil {
		return nil, err
	}
	// v2.0.8 Q2：空 region / 有效行不足两行时分裂不成立，返回 AffectedRows=0
	//（"未分裂"），不再无条件返回 1 掩盖静默失败。
	if !split {
		return &Result{AffectedRows: 0}, nil
	}
	return &Result{AffectedRows: 1}, nil
}

// execDropRegion DROP REGION <regionID> [FORCE]：回收孤儿 region 的路由与归属记录。
//
// v2.0.8 Q3：孤儿 region（如 9223372036854775825 这类隐式 ID 表已无存活副本）此前
// 既无法通过 SPLIT/BALANCE/ASSIGN 收敛，也没有任何语句能清掉它的路由或归属，
// failover 冷却窗口到期后反复重试、日志持续刷屏。本语句提供显式回收入口。
//
// 安全边界（只摘元数据，绝不删数据）：
//  1. 只删除路由条目 / 隐式归属映射，并广播到全网，物理键空间原样保留；
//  2. 未经 FORCE 时，若本节点该 region 键空间仍有行数据则拒绝执行，防止把在用
//     region 的路由摘掉导致数据"看不见"；
//  3. 仅 root 可执行（authz 与本语句同一登记表），幂等：region 不存在时报错而非静默。
func (e *Executor) execDropRegion(sess *Session, s *DropRegionStmt) (*Result, error) {
	rid := storage.ID(s.RegionID)
	rg, ok := e.router.FindRegion(rid)
	implicit := false
	if !ok {
		irg, iok := e.implicitRegion(rid)
		if !iok {
			return nil, &SQLError{Msg: fmt.Sprintf("region not found: %d", s.RegionID)}
		}
		rg, implicit = irg, true
	}
	// 在用数据保护：本节点仍有行时不轻易摘路由（隐式 region 同理）。
	rows, err := e.countRegionRows(e.tableMetaByID(rg.TableID), rg)
	if err != nil {
		return nil, err
	}
	// 默认 dry-run（高危命令的第一道闸）：只回显将影响的对象与动作，不改任何状态。
	// 运维确认无误后再以 DROP REGION <id> EXECUTE [FORCE] 二次提交。
	if !s.Execute {
		action := "would drop routing (data kept)"
		if rows > 0 && !s.Force {
			action = "refused: region still holds local rows, add FORCE to drop routing anyway"
		}
		log.Printf("sql: DROP REGION dry-run region=%d (table=%d implicit=%v rows=%d force=%v) action=%s",
			s.RegionID, rg.TableID, implicit, rows, s.Force, action)
		return &Result{
			Columns: []string{"dry_run", "region_id", "table_id", "implicit", "live_rows", "force", "action"},
			Rows: [][]Value{{
				StrVal("1"),
				StrVal(strconv.FormatUint(s.RegionID, 10)),
				StrVal(strconv.FormatInt(int64(rg.TableID), 10)),
				StrVal(boolValStr(implicit)),
				StrVal(strconv.FormatInt(rows, 10)),
				StrVal(boolValStr(s.Force)),
				StrVal(action),
			}},
		}, nil
	}
	if rows > 0 && !s.Force {
		return nil, &SQLError{Msg: fmt.Sprintf(
			"region %d still holds %d rows locally; use DROP REGION %d FORCE EXECUTE to drop routing only (data is kept)",
			s.RegionID, rows, s.RegionID)}
	}
	// 清除该 region 的 no-live-copy 限频状态，避免回收后残留抑制计数。
	e.clearNoLiveCopyLog(rid)
	e.clearB1Pending(rid)
	if implicit {
		e.router.ForgetImplicitRegion(rg.TableID)
	} else {
		if err := e.router.DeleteRegion(rid); err != nil {
			return nil, err
		}
	}
	if err := e.persistRegions(); err != nil {
		return nil, err
	}
	// 全网广播删除：各节点同步摘掉该 region 视图，避免残留视图继续参与路由决策。
	if err := e.broadcastRegionView(nil, []storage.ID{rid}); err != nil {
		return nil, err
	}
	log.Printf("sql: dropped region %d routing (table=%d implicit=%v rows=%d force=%v, data kept)",
		s.RegionID, rg.TableID, implicit, rows, s.Force)
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
	_, err := e.splitRegion(meta, rg)
	return err
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

// balanceMaxMigrationsPerRun 单条 BALANCE 在一次执行内允许的最大迁移条数（v2.0.9 Q4）。
//
// 取值推导（最坏情况迁移数的闭式解，v2.0.9 二轮修正）：
//   - 设本节点独占 N 个 region、集群有 M 个存活节点。收敛判据是各节点持有数极差
//     <= balanceConvergedSpread(=1)，由于只有本节点持有可迁条目，本节点最终必须降到
//     与其它节点相差不超过 1，即不高于 ⌈N/M⌉；
//   - 每迁出 1 条，本节点减 1、某个远端加 1，本节点与全局最小值之差最多只减 1，
//     故必须迁出的条数存在硬下界：k(N, M) = N - ⌈N/M⌉；
//   - 目标选择为「持有数最少的存活节点、并列取候选顺序（节点 ID 升序）靠前者」时，
//     远端被依次交替填高，实际迁移数等于该下界；若完全交替填谷，最多再多 1 条。
//     三节点复现场景：N=14、M=3 -> k = 14 - ⌈14/3⌉ = 14 - 5 = 9（交替填谷 10），
//     即 14/0/0 的偏斜一条 BALANCE 就要迁 9~10 条才摊平；
//   - 上限的可覆盖范围（M=3）：k(N,3) = N - ⌈N/3⌉ <= 上限，取 32 等价于 N <= 48，
//     即单节点持有至多 48 个 region 的偏斜能在一条 BALANCE 内彻底收敛。旧值 8 只
//     覆盖 N <= 12，连 14/0/0 这个真实复现场景都迁不完（必须靠第二条 BALANCE 收尾），
//     与"一条 BALANCE 应当把偏斜摊平"的语义不符，这是本次上调的直接原因；
//   - 上限的下界约束：单次迁移 = 全量 region 扫描 + REGION_PUSH（数据与路由同批次
//     落盘）+ 路由切换落盘 + 增量广播，是跨节点往返的毫秒~百毫秒级操作。32 条把单条
//     BALANCE 的最坏耗时限制在数十次跨节点往返（秒级），仍在客户端超时之内，也不会
//     长时间独占 balanceMu 阻塞自动均衡 tick 与并发 BALANCE；
//   - 超出上限不丢偏斜：每轮都把本节点降到 ⌈N'/M⌉ 附近，偏斜单调递减（不会来回搬），
//     剩余部分由下一条 BALANCE 或自动均衡下一 tick 继续收敛。
const balanceMaxMigrationsPerRun = 32

// balanceConvergedSpread 收敛阈值：各节点 region 数极差不超过该值即视为已摊平。
// 取 1 沿用 BALANCE 既有语义（「本节点 region 数 > 最闲节点 + 1 才迁移」），即容忍
// 节点间 1 个 region 的粒度差；更细的按行数加权与迁移历史单调性属于后续版本范畴。
const balanceConvergedSpread = 1

// execBalance BALANCE：手动触发一次负载均衡。
func (e *Executor) execBalance(sess *Session, s *BalanceStmt) (*Result, error) {
	// 集群启用判据：管理器存在且注册了至少一个远端节点。
	// 单机模式（db.Open 默认创建 Manager 但无远端节点）视作未启用，拒绝执行。
	if e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return nil, &SQLError{Msg: "cluster not enabled"}
	}
	n, err := e.balanceOnce()
	if err != nil {
		// 中途失败：Result 仍携带已完成的迁移条数（AffectedRows），同时返回错误让
		// 客户端明确本轮未收敛；收敛打点已在 balanceOnce 内汇总（reason=migrate-error）。
		return &Result{AffectedRows: n}, err
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

// balanceOnce 执行一轮均衡：把本节点多出的 region 逐个迁往最闲存活节点，直到
// 各节点 region 数极差 <= balanceConvergedSpread 或本轮迁移数达到
// balanceMaxMigrationsPerRun 上限。返回本轮实际完成的迁移条数（0 表示无需迁移）。
//
// v2.0.9 Q4 修复点：
//  1. 一轮内持续迁移：v2.0.8 及之前每次调用只迁 1 个 region 就返回，客户端必须反复
//     执行 BALANCE 才可能摊平，自动均衡也只在每个 tick 迁 1 个，极差下降极慢；
//  2. 判据与目标选择统一改用 balanceRegionCounts 的真实持有数口径（含隐式整表
//     region）：旧实现本节点用 localRegionLoad 条数（含隐式 region）、远端用
//     regionCountOnNode（只数显式路由），两侧口径不一致，判据恒偏向「本节点更多」，
//     出现「每轮都在迁移但极差不见收敛／迁移条数与实际变化不一致」；
//  3. 迁移中途失败立即中止，返回已完成的迁移条数（不吞错、不继续尝试其余 region）。
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
	// 迁移顺序固定为行数降序：先削最重的 region；删除已迁条目后剩余顺序不变。
	sort.Slice(local, func(i, j int) bool { return local[i].rows > local[j].rows })
	counts := e.balanceRegionCounts()
	migrated := 0
	for migrated < balanceMaxMigrationsPerRun {
		// 目标 = 持有 region 数最少的存活节点（第一簇不做跨节点行数查询，用真实
		// region 持有数近似负载；并列时取候选顺序第一个，结果稳定可复现）。
		target, minRegions := pickBalanceTarget(candidates, counts)
		localCount := counts[e.selfID]
		if localCount <= minRegions+balanceConvergedSpread {
			e.logBalanceRound(counts, localCount, minRegions, "skip-no-spread")
			e.logBalanceSummary(migrated, counts, "converged")
			return migrated, nil
		}
		if len(local) == 0 {
			// 计数口径判定仍有偏斜，但本地已无可迁条目（归属/元数据刚被外部改写）：
			// 不空转，打点退出，剩余偏斜交下一条 BALANCE 或下一 tick 处理。
			e.logBalanceRound(counts, localCount, minRegions, "skip-no-local-candidate")
			e.logBalanceSummary(migrated, counts, "no-local-candidate")
			return migrated, nil
		}
		cur := local[0]
		e.logBalanceRound(counts, localCount, minRegions,
			fmt.Sprintf("migrate-region:%d->%s", cur.rg.RegionID, target))
		if err := e.migrateRegion(cur.rg, target); err != nil {
			// 迁移可能在路由切换之后才失败（如增量广播时对端不可达）：以路由实际归属
			// 判断该条是否已落定，保证返回的已迁移条数与真实进度一致。
			if now, ok := e.router.FindRegion(cur.rg.RegionID); ok && now.Node == target {
				migrated++
				counts[e.selfID]--
				counts[target]++
			}
			e.logBalanceSummary(migrated, counts, "migrate-error")
			return migrated, err
		}
		migrated++
		counts[e.selfID]--
		counts[target]++
		local = local[1:]
	}
	// 达到单轮迁移上限：剩余偏斜由下一条 BALANCE / 下一 tick 继续（单调递减，不回摆）。
	e.logBalanceSummary(migrated, counts, "max-migrations")
	return migrated, nil
}

// logBalanceRound v2.0.8 Q4 打点：输出一轮均衡的收敛度快照。
//
// 字段含义：self=本节点 region 数；各节点 region 数；spread=本轮参与比较的极差
// （本节点 - 最闲节点）；converged=极差是否已在不迁移阈值内（<=1）；action=本轮动作。
// 打点只读路由视图，不改变均衡判定与迁移行为。
func (e *Executor) logBalanceRound(counts map[string]int, localCount, minRegions int, action string) {
	// 节点按名字排序，保证同一拓扑下日志可比对。
	names := make([]string, 0, len(counts))
	for n := range counts {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for i, n := range names {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%s=%d", n, counts[n])
	}
	log.Printf("sql: balance round: local=%d nodes={%s} spread=%d converged=%v action=%s",
		localCount, b.String(), localCount-minRegions, localCount-minRegions <= 1, action)
}

// balanceRegionCounts 统计各节点真实持有的 region 数（v2.0.9 Q4 口径统一）。
//
// 显式 region 与隐式整表 region 都要计，且本节点与远端节点用同一口径：
//  1. 路由视图中的 region 按 Node 归属计数，Node=="" 视作本节点（与 regionLocal
//     同语义：未指派的 region 由本节点持有）；
//  2. 隐式整表 region（tableID|1<<63）不落盘、不在 AllRegions 内，单独补计：
//     归属优先取路由视图已学到的映射（Router.implicit，键为 tableID），未学到时
//     退回 implicitRouteOwner() 的确定性派生规则（节点 ID 字典序最小者），
//     与 implicitRoutesForViewSync 的声明口径一致；
//  3. 已切分表（本地有显式分片条目）不再有隐式整表 region，跳过，避免重复计数。
func (e *Executor) balanceRegionCounts() map[string]int {
	counts := map[string]int{}
	seenExplicit := map[storage.ID]bool{}
	for _, rg := range e.router.AllRegions() {
		owner := rg.Node
		if owner == "" {
			owner = e.selfID
		}
		counts[owner]++
		seenExplicit[rg.RegionID] = true
	}
	countImplicit := func(tid storage.ID, owner string) {
		if owner == "" {
			owner = e.selfID
		}
		counts[owner]++
	}
	learned := e.router.ImplicitRegions()
	for tid, owner := range learned {
		if seenExplicit[sharding.ImplicitRegionID(tid)] {
			continue
		}
		// 已切分表：隐式整表 region 被显式分片条目取代，学到的旧归属不再计入，
		// 否则一条 SPLIT 后会被重复计成 N+1 个 region。
		if e.router.HasStoredRegions(tid) {
			continue
		}
		countImplicit(tid, owner)
	}
	if e.tm == nil {
		return counts
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return counts
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return counts
	}
	defaultOwner := e.implicitRouteOwner()
	for _, tb := range tabs {
		if tb == nil || tb.ID == 0 {
			continue
		}
		tid := storage.ID(tb.ID)
		// 已切分表：隐式整表 region 已不存在，以显式分片条目为准。
		if e.router.HasStoredRegions(tid) {
			continue
		}
		if _, ok := learned[tid]; ok {
			continue // 已在上面按学到的归属计过
		}
		countImplicit(tid, defaultOwner)
	}
	return counts
}

// pickBalanceTarget 从存活远端候选中选出 region 数最少的节点，返回该节点与本轮最闲
// 节点的 region 数。并列时取候选顺序靠前者（candidates 来自 ListNodes 的 ID 排序），
// 保证同一拓扑下目标选择稳定可复现。
func pickBalanceTarget(candidates []string, counts map[string]int) (string, int) {
	target := candidates[0]
	minRegions := counts[target]
	for _, c := range candidates[1:] {
		if cnt := counts[c]; cnt < minRegions {
			minRegions = cnt
			target = c
		}
	}
	return target, minRegions
}

// logBalanceSummary v2.0.9 Q4：汇总一条 BALANCE 的实际迁移条数与终止原因。
// migrated=本轮实际完成的迁移数；spread=全部计数节点 region 数的极差；
// reason 取值：converged（已收敛）/ max-migrations（达单轮上限）/
// migrate-error（迁移失败中止）/ no-local-candidate（无可迁条目）。
// 与 logBalanceRound 一样只读内存计数，不改变均衡判定与迁移行为。
func (e *Executor) logBalanceSummary(migrated int, counts map[string]int, reason string) {
	names := make([]string, 0, len(counts))
	for n := range counts {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	spread := 0
	if len(names) > 0 {
		minCount, maxCount := counts[names[0]], counts[names[0]]
		for i, n := range names {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "%s=%d", n, counts[n])
			if counts[n] < minCount {
				minCount = counts[n]
			}
			if counts[n] > maxCount {
				maxCount = counts[n]
			}
		}
		spread = maxCount - minCount
	}
	log.Printf("sql: balance summary: migrated=%d limit=%d spread=%d converged=%v reason=%s nodes={%s}",
		migrated, balanceMaxMigrationsPerRun, spread, spread <= balanceConvergedSpread, reason, b.String())
}

// regionCountOnNode 归属 nodeID 的真实持有 region 数（与 balanceOnce 判据同口径，
// 含隐式整表 region）；保留该入口供打点与观测复用。
func (e *Executor) regionCountOnNode(nodeID string) int {
	return e.balanceRegionCounts()[nodeID]
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
			// 隐式 region 迁至远端后其路由仍登记在本节点路由表（Node 为远端节点），
			// 需与显式段一致按归属过滤；否则会被当作本地负载参与迁移选主，
			// 迁移时触发 "region not local" 并使整轮 BALANCE 失败。
			if !e.regionLocal(rid) {
				continue
			}
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
			// 仅本地真正持有数据的 region 补记为本节点；未持有者保持未指派，
			// 避免 Node=="" 但数据在别处的 region 被无条件补记污染为本地归属。
			holds, err := e.regionHoldsLocal(regions[i])
			if err != nil {
				return err
			}
			if holds {
				regions[i].Node = e.selfID
				regions[i].Local = false
			}
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
	if err := tx.Commit(); err != nil {
		return err
	}
	// 路由变更全网广播：仅同步本次迁移条目，避免全量视图扩散残留漂移。
	return e.broadcastRegionView([]sharding.RegionInfo{rgView}, nil)
}

// pushAllRegionsToNode 全量对齐推送（v1.7.0 回迁降级兜底）：
// 把本节点全部 region 当前行 REGION_PUSH 推送到目标节点，目标节点 applyPush
// 在单一 WriteBatch 内覆盖写本地 + 学路由 + 落盘 m:regions（幂等，可重复执行）。
// 与 migrateRegion 同源（扫描→组装→推送），但**不切换路由归属、不广播**：
// 源节点保持权威视图，目标节点经 applyPush 学习后本地持全部 region。
// 任一 region 推送失败（如目标不可达）立即返回错误，调用方据此判定对齐失败。
func (e *Executor) pushAllRegionsToNode(target string) error {
	return e.pushRegionsToNode(target, true)
}

// pushRegionsToNode 全量推送实现（D4.4 修复：区分归属声明语义）。
// claim=true：ASSIGN/回迁兜底语义——目标节点经 applyPush 成为全部 region 的 owner；
// claim=false：副本对齐语义——只推送本节点持有（Node=="" 且物理持有，或 Node==selfID）
// 的 region，并按 e.selfID 声明权威归属，接收端据 ClaimOwner=false 保持该归属
// （local=false），不把他人 region 误判为本地。
// 后者用于节点回 UP / 新节点加入的 join 路由同步，避免把第三方 region 判成本地
// 形成孤立读视图（D4.4 GAP A/C 根因）。
func (e *Executor) pushRegionsToNode(target string, claim bool) error {
	if e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return &SQLError{Msg: "cluster not enabled"}
	}
	if _, ok := e.mgr.GetNode(target); !ok {
		return &SQLError{Msg: "node not registered: " + target}
	}
	// 收集全部 region：显式路由 + 各表隐式单 region（未落盘，AllRegions 不含）。
	regions := make([]sharding.RegionInfo, 0, 16)
	regions = append(regions, e.router.AllRegions()...)
	seen := make(map[storage.ID]bool, len(regions))
	for _, rg := range regions {
		seen[rg.RegionID] = true
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return err
	}
	for _, tb := range tabs {
		rs := e.regionsOf(tb)
		if len(rs) == 1 && !rs[0].Explicit {
			rid := storage.ID(uint64(tb.ID) | (uint64(1) << 63))
			rg, ok := e.implicitRegion(rid)
			if ok && !seen[rg.RegionID] {
				regions = append(regions, rg)
				seen[rg.RegionID] = true
			}
		}
	}
	// 权威路由视图：本地持有（Node==""）补记为本节点，供目标节点学习其余 region 归属。
	routes := e.router.AllRegions()
	for i := range routes {
		if routes[i].Node == "" {
			holds, err := e.regionHoldsLocal(routes[i])
			if err != nil {
				return err
			}
			if holds {
				routes[i].Node = e.selfID
				routes[i].Local = false
			}
		}
	}
	for _, rg := range regions {
		// 副本对齐语义（claim=false）：只推送本节点持有或归属本节点的 region，
		// 归属第三方的 region 不进数据推送队列（其归属随 routes 一并下发）。
		if !claim && rg.Node != "" && rg.Node != e.selfID {
			continue
		}
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
		// 本次推送 region 的归属声明：claim 语义归目标节点；副本语义归本节点（源）。
		rgView := rg
		if claim {
			rgView.Node = target
		} else {
			rgView.Node = e.selfID
		}
		rgView.Local = false
		pushRoutes := make([]sharding.RegionInfo, 0, len(routes)+1)
		appended := false
		for _, rt := range routes {
			if rt.RegionID == rg.RegionID {
				pushRoutes = append(pushRoutes, rgView)
				appended = true
			} else {
				pushRoutes = append(pushRoutes, rt)
			}
		}
		if !appended {
			pushRoutes = append(pushRoutes, rgView)
		}
		if claim {
			if err := e.mgr.PushRegion(target, rg, rows, pushRoutes, e.selfID); err != nil {
				return err
			}
			continue
		}
		// 副本语义：接收端按 rgView.Node（本节点）保持权威归属，local=false。
		if err := e.mgr.PushRegionReplica(target, rgView, rows, pushRoutes, e.selfID); err != nil {
			return err
		}
	}
	return nil
}

// PushRouteViewToNode 把本节点权威 region 归属视图（纯路由，不带数据行）同步给
// target 节点（D4.4 修复：重启节点主动刷新视图的服务端实现）。
// 视图构造与 pushRegionsToNode 一致：本地持有（Node==""）补记 selfID，
// 其余保持权威归属；接收端经 applyRouteSync 覆盖学习陈旧归属，
// 从而消除"重启节点凭旧 m:regions 误判 local=true"的孤立读视图。
func (e *Executor) PushRouteViewToNode(target string) error {
	if e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return &SQLError{Msg: "cluster not enabled"}
	}
	if target == "" || target == e.selfID {
		return &SQLError{Msg: "invalid route sync target: " + target}
	}
	if _, ok := e.mgr.GetNode(target); !ok {
		return &SQLError{Msg: "node not registered: " + target}
	}
	routes := e.router.AllRegions()
	for i := range routes {
		if routes[i].Node == "" {
			holds, err := e.regionHoldsLocal(routes[i])
			if err != nil {
				return err
			}
			if holds {
				routes[i].Node = e.selfID
				routes[i].Local = false
			}
		}
	}
	// v2.0.4：权威视图里一并下发隐式整表 region 归属，使重启后的纯转发节点
	// 也能学到"未切分表归谁"（此前它只会按"整表归本机"推断，从而永久拒绝写入）。
	return e.mgr.SyncRouteViewToEpochWithImplicit(target, routes, e.implicitRoutesForViewSync(), e.selfID, e.clusterTermForRouteSync())
}

// clusterTermForRouteSync 权威路由视图同步携带的集群纪元（P9）：取本机已知
// 最大集群纪元，使接收端（重启节点）能按该 term 确认/收回归属；0 表示本机
// 无纪元信息（epoch 未启用/从未 failover），接收端只学归属不推进纪元。
func (e *Executor) clusterTermForRouteSync() uint64 {
	if e.epochSrc == nil {
		return 0
	}
	return e.epochSrc.Epoch()
}

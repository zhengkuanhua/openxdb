package sql

// v1.9.0 物化视图增量刷新：基表变更跟踪（m:mv_dirty:*）+ Simple 行级增量合并
// + 依赖链刷新（CONCURRENTLY 快照交换见 matview.go execRefreshMatView）。

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// ---- 基表变更跟踪 ----

// mvGlobalVerKey 全局变更版本键：每次被 MV 依赖的基表发生 DML/DDL 时 +1。
// 各物化视图 Version 记录其上次刷新消费到的全局版本；REFRESH 时若依赖侧
// 最新版本 <= 自身 Version 则幂等跳过。
var mvGlobalVerKey = []byte("m:mv_ver")

// mvDirtyRowsCap 单表变更键集上限：超限置 Overflow 强制全量刷新，控制
// 变更日志体积（行级增量只保证"大表小变更"场景收益）。
const mvDirtyRowsCap = 512

// mvDirtyKey 返回某基表的变更跟踪键。
func mvDirtyKey(tbl string) []byte {
	return []byte("m:mv_dirty:" + tbl)
}

// mvDirty 某基表的待刷新变更记录。
// Ver：记录写入时的全局版本；Rows：变更行主键 hex 集合（pkBytes 编码）；
// Overflow：变更键集超限或该表发生结构 DDL，依赖它的物化视图须全量刷新。
// dirty 记录由 DML 同事务写入（原子）；REFRESH 消费时只更新各 MV 的
// Version，不改 dirty 本体——多个物化视图共享同一基表时可重复消费同一
// 变更集合（行级重算/删除幂等），最后由 maybeCompactMVDirty 在所有依赖者
// 均消费到 dirty.Ver 后清空 Rows 释放空间。
type mvDirty struct {
	Ver      uint64   `json:"ver"`
	Rows     []string `json:"rows,omitempty"`
	Overflow bool     `json:"overflow,omitempty"`
}

// loadMVDirty 读取某基表变更记录（无记录视为空）。
func loadMVDirty(get func(key []byte) ([]byte, error), tbl string) (*mvDirty, error) {
	raw, err := get(mvDirtyKey(tbl))
	if err != nil {
		if err == storage.ErrNotFound {
			return &mvDirty{}, nil
		}
		return nil, err
	}
	if len(raw) == 0 {
		return &mvDirty{}, nil
	}
	var d mvDirty
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, &SQLError{Msg: "corrupted materialized view change log"}
	}
	return &d, nil
}

// currentMVVersion 读全局变更版本（无记录视为 0）。
func currentMVVersion(get func(key []byte) ([]byte, error)) (uint64, error) {
	raw, err := get(mvGlobalVerKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	if len(raw) == 0 {
		return 0, nil
	}
	var v uint64
	if err := json.Unmarshal(raw, &v); err != nil {
		return 0, &SQLError{Msg: "corrupted materialized view version"}
	}
	return v, nil
}

// recordMVDirtyKeys 把本语句受影响行的主键集合合并进该基表 dirty 记录，
// 并推进全局版本。dirty 与 DML 同事务（调用方传同一 tx，随提交原子落盘）。
func (e *Executor) recordMVDirtyKeys(tx txn.Txn, tbl string, pks [][]byte) error {
	if len(pks) == 0 {
		return nil
	}
	cur, err := loadMVDirty(tx.Get, tbl)
	if err != nil {
		return err
	}
	ver, err := currentMVVersion(tx.Get)
	if err != nil {
		return err
	}
	ver++
	seen := make(map[string]struct{}, len(cur.Rows)+len(pks))
	for _, r := range cur.Rows {
		seen[r] = struct{}{}
	}
	for _, p := range pks {
		seen[hex.EncodeToString(p)] = struct{}{}
	}
	rows := make([]string, 0, len(seen))
	for r := range seen {
		rows = append(rows, r)
	}
	sort.Strings(rows)
	cur.Rows = rows
	cur.Ver = ver
	if len(cur.Rows) > mvDirtyRowsCap {
		cur.Overflow = true
	}
	raw, err := json.Marshal(cur)
	if err != nil {
		return err
	}
	if err := tx.Put(mvDirtyKey(tbl), raw); err != nil {
		return err
	}
	rawV, err := json.Marshal(ver)
	if err != nil {
		return err
	}
	return tx.Put(mvGlobalVerKey, rawV)
}

// markMVDirtyOverflow 基表发生结构变更（ALTER/DROP）时强制所有依赖它的
// 物化视图全量刷新（行级增量无法应对列结构变化）。
func (e *Executor) markMVDirtyOverflow(tx txn.Txn, tbl string) error {
	cur, err := loadMVDirty(tx.Get, tbl)
	if err != nil {
		return err
	}
	ver, err := currentMVVersion(tx.Get)
	if err != nil {
		return err
	}
	ver++
	cur.Ver = ver
	cur.Overflow = true
	cur.Rows = nil
	raw, err := json.Marshal(cur)
	if err != nil {
		return err
	}
	if err := tx.Put(mvDirtyKey(tbl), raw); err != nil {
		return err
	}
	rawV, err := json.Marshal(ver)
	if err != nil {
		return err
	}
	return tx.Put(mvGlobalVerKey, rawV)
}

// maybeCompactMVDirty 物化视图刷新后调用：对每个出现在某 MV.Deps 中的基表，
// 若所有依赖它的物化视图均已消费到 dirty.Ver，则清空 Rows 释放空间
// （Ver 保留，后续变更继续累计；已消费集合重复消费幂等）。
func (e *Executor) maybeCompactMVDirty(tx txn.Txn, mvs []*MatViewMeta) error {
	depSet := map[string]bool{}
	for _, m := range mvs {
		for _, d := range m.Deps {
			depSet[d] = true
		}
	}
	for tbl := range depSet {
		d, err := loadMVDirty(tx.Get, tbl)
		if err != nil {
			return err
		}
		if d.Ver == 0 || len(d.Rows) == 0 {
			continue
		}
		consumed := true
		for _, m := range mvs {
			if m.Version >= d.Ver {
				continue
			}
			for _, dep := range m.Deps {
				if dep == tbl {
					consumed = false
					break
				}
			}
			if !consumed {
				break
			}
		}
		if consumed {
			d.Rows = nil
			raw, err := json.Marshal(d)
			if err != nil {
				return err
			}
			if err := tx.Put(mvDirtyKey(tbl), raw); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- 依赖表缓存（DML 钩子 O(1) 判定） ----

// buildMVDependencySet 从物化视图目录重建依赖表集合（create/drop 后调用）。
func (e *Executor) buildMVDependencySet(mvs []*MatViewMeta) {
	e.mvDepMu.Lock()
	defer e.mvDepMu.Unlock()
	set := map[string]bool{}
	for _, m := range mvs {
		for _, d := range m.Deps {
			set[d] = true
		}
	}
	e.mvDepSet = set
}

// tableTrackedByMVs 判断基表是否被至少一个物化视图依赖。缓存未构建时
// 惰性从目录加载（进程启动后首次 DML 触发），构建后随 create/drop 失效重建。
func (e *Executor) tableTrackedByMVs(tx txn.Txn, tbl string) (bool, error) {
	e.mvDepMu.RLock()
	ok := e.mvDepSet != nil && e.mvDepSet[tbl]
	built := e.mvDepSet != nil
	e.mvDepMu.RUnlock()
	if ok || built {
		return ok, nil
	}
	mvs, err := loadMatViews(tx.Get)
	if err != nil {
		return false, err
	}
	e.buildMVDependencySet(mvs)
	e.mvDepMu.RLock()
	ok = e.mvDepSet[tbl]
	e.mvDepMu.RUnlock()
	return ok, nil
}

// ---- 依赖解析与 Simple 判定 ----

// collectMatViewDeps 解析物化视图定义的引用关系：
//   - 引用基表 → deps；
//   - 引用虚拟视图 → 递归展开其定义（可能再引用视图/表/物化视图），去重合并；
//   - 引用其它物化视图 → depMVs（依赖链刷新顺序与失效传播用）。
//
// 引用名在创建时已校验存在，此处仅防御性忽略未知名。
func collectMatViewDeps(s *SelectStmt, tabs []*TableMeta, views []*ViewMeta, mvs []*MatViewMeta) (deps, depMVs []string) {
	seenD := map[string]bool{}
	seenM := map[string]bool{}
	var walk func(n string, visiting map[string]bool)
	walk = func(n string, visiting map[string]bool) {
		if n == "" || visiting[n] {
			return
		}
		if findMatView(mvs, n) != nil {
			if !seenM[n] {
				seenM[n] = true
				depMVs = append(depMVs, n)
			}
			return
		}
		if findTable(tabs, n) != nil {
			if !seenD[n] {
				seenD[n] = true
				deps = append(deps, n)
			}
			return
		}
		if v := findView(views, n); v != nil {
			visiting[n] = true
			for _, r := range viewRefNames(v.Select) {
				walk(r, visiting)
			}
			delete(visiting, n)
			return
		}
	}
	for _, r := range viewRefNames(s) {
		walk(r, map[string]bool{})
	}
	return deps, depMVs
}

// simpleMatViewInfo Simple 行级增量可行性信息。
type simpleMatViewInfo struct {
	baseTable string // 唯一基表名
	basePK    string // 基表单列主键列名
	pkType    string // 主键列类型（INT/DECIMAL/TEXT；DATE/BLOB 走全量）
	pkIdx     int    // 投影结果行中主键列的位置（增量重算/全量写回提取主键值）
}

// simpleMatViewInfoOf 判定物化视图定义是否可走 Simple 行级增量：
//   - 单基表（无 JOIN/子查询/CTE/聚合/去重/排序/分页）；
//   - 全部投影列为纯列引用或 *（无表达式/CASE/窗口/变量）；
//   - 基表单列主键（类型 INT/DECIMAL/TEXT）；
//   - 投影包含主键列（增量重算时以主键值定位影子表行键）。
//
// 不满足时返回 nil（走全量刷新，语义不变）。
func simpleMatViewInfoOf(s *SelectStmt, tabs []*TableMeta, mvs []*MatViewMeta, views []*ViewMeta) (*simpleMatViewInfo, error) {
	if s == nil || s.From == "" || s.Subquery != nil || len(s.Joins) > 0 || len(s.CTEs) > 0 {
		return nil, nil
	}
	if s.Distinct || s.Having != nil || len(s.GroupBy) > 0 || s.OrderBy != nil ||
		len(s.OrderBys) > 0 || s.Limit > 0 || s.Offset > 0 || s.ForUpdate {
		return nil, nil
	}
	// FROM 必须是真实基表：物化视图影子表（findTable 命中）与虚拟视图排除。
	if findMatView(mvs, s.From) != nil || findView(views, s.From) != nil {
		return nil, nil
	}
	meta := findTable(tabs, s.From)
	if meta == nil || meta.PK == "" || len(meta.PKCols) > 0 {
		return nil, nil
	}
	pkCol := meta.PK
	pkType := ""
	for _, c := range meta.Columns {
		if c.Name == pkCol {
			pkType = c.Type
			break
		}
	}
	if pkType != "INT" && pkType != "DECIMAL" && pkType != "TEXT" {
		return nil, nil
	}
	pkIdx := -1
	for i, c := range s.Columns {
		if c.Raw == "*" {
			pkIdx = colIndex(meta, pkCol)
			break
		}
		if c.Agg != "" || c.Expr != nil || c.Case != nil || c.Window != nil || c.Var != "" {
			return nil, nil
		}
		if c.Col == pkCol && (c.Ref == "" || c.Ref == s.From || c.Ref == s.FromAlias) {
			pkIdx = i
		}
	}
	if pkIdx < 0 {
		return nil, nil
	}
	return &simpleMatViewInfo{baseTable: s.From, basePK: pkCol, pkType: pkType, pkIdx: pkIdx}, nil
}

// pkBytesToValue 把 dirty 记录中的主键 hex 字节还原为 WHERE 比较值。
// 与 pkBytes 编码严格互逆：INT/DECIMAL 定长 8B（DECIMAL 翻转符号位），
// TEXT 原字节；BLOB/DATE 主键已由 Simple 判定排除。
func pkBytesToValue(pkType string, b []byte) Value {
	switch pkType {
	case "INT":
		return IntVal(int64(binary.BigEndian.Uint64(b)))
	case "DECIMAL":
		return IntVal(int64(binary.BigEndian.Uint64(b) ^ (1 << 63)))
	default: // TEXT
		return StrVal(string(b))
	}
}

// ---- Simple 行级增量 apply ----

// applyIncrementalRefresh 只重算 dirty 记录中的变更行：逐主键重跑
// 「定义 SELECT + 主键等值条件」，命中则按主键定位影子表行键 Put 新值，
// 未命中（基表行已删或被定义 WHERE 过滤）则 Delete 影子表行。
// 影子表隐藏 PK 列与基表主键同值（Simple 创建时），行键定位与全量写回
// 完全一致，保证增量/全量路径可相互切换且互不冲突。
func (e *Executor) applyIncrementalRefresh(sess *Session, tx txn.Txn, mv *MatViewMeta, shadow *TableMeta, info *simpleMatViewInfo, dirty *mvDirty) (int, error) {
	affected := 0
	for _, h := range dirty.Rows {
		b, err := hex.DecodeString(h)
		if err != nil {
			continue // 防御：脏记录损坏则跳过该键（后续全量兜底）
		}
		pkV := pkBytesToValue(info.pkType, b)
		cloned, err := cloneSelect(mv.Select)
		if err != nil {
			return 0, err
		}
		cond := Cond{Col: info.basePK, Op: "=", Val: pkV}
		if cloned.Where == nil {
			cloned.Where = &Where{Conds: []Cond{cond}}
		} else {
			cloned.Where.Conds = append(cloned.Where.Conds, cond)
		}
		res, err := e.execSelect(sess, cloned)
		if err != nil {
			return 0, err
		}
		pk, err := pkBytes(shadow, pkV)
		if err != nil {
			return 0, err
		}
		rid, err := e.locateRow(shadow, pk)
		if err != nil {
			return 0, err
		}
		key := e.rowKeyAt(shadow, rid, pk)
		if len(res.Rows) == 0 {
			if err := tx.Delete(key); err != nil {
				return 0, err
			}
			affected++
			continue
		}
		row := res.Rows[0]
		if len(row) != len(shadow.Columns)-1 {
			return 0, &SQLError{Msg: "corrupted materialized view incremental row"}
		}
		vals := make([]Value, 0, len(row)+1)
		for i, v := range row {
			nv, err := normalizeMatViewValue(shadow.Columns[i].Type, shadow.Columns[i].Name, v)
			if err != nil {
				return 0, err
			}
			vals = append(vals, nv)
		}
		vals = append(vals, pkV)
		raw, err := encodeRow(shadow, vals)
		if err != nil {
			return 0, err
		}
		if err := tx.Put(key, raw); err != nil {
			return 0, err
		}
		affected++
	}
	return affected, nil
}

// ---- 依赖链刷新 ----

// refreshMatViewRec 递归刷新单个物化视图（依赖 MV 深度优先先刷新）。
// 共享 mvs/tabs/views 目录切片：递归内更新后上层可见，整体在同一个事务中
// 提交（任一环失败整体回滚，旧快照保持）。visiting 防环（防御，创建约束
// 为 DAG）。
func (e *Executor) refreshMatViewRec(sess *Session, tx txn.Txn, mvs []*MatViewMeta, tabs []*TableMeta, views []*ViewMeta, name string, concurrently bool, visiting map[string]bool) (int, error) {
	if visiting[name] {
		return 0, &SQLError{Msg: "cyclic materialized view dependency: " + name}
	}
	visiting[name] = true
	defer delete(visiting, name)
	mv := findMatView(mvs, name)
	if mv == nil {
		return 0, &SQLError{Msg: "materialized view not exists: " + name}
	}
	affected := 0
	// 1) 依赖的物化视图先刷新（深度优先；其自身依赖递归处理）。
	for _, dep := range mv.DepMVs {
		n, err := e.refreshMatViewRec(sess, tx, mvs, tabs, views, dep, false, visiting)
		if err != nil {
			return 0, err
		}
		affected += n
	}
	// 2) 失效检测：依赖侧最新版本 = max(基表 dirty.Ver, 依赖 MV 刷新后 Version)。
	maxVer := mv.Version
	for _, dep := range mv.Deps {
		d, err := loadMVDirty(tx.Get, dep)
		if err != nil {
			return 0, err
		}
		if d.Ver > maxVer {
			maxVer = d.Ver
		}
	}
	for _, dep := range mv.DepMVs {
		if dm := findMatView(mvs, dep); dm != nil && dm.Version > maxVer {
			maxVer = dm.Version
		}
	}
	if maxVer <= mv.Version {
		return affected, nil // 幂等：依赖侧无新变更，跳过
	}
	shadow := findTable(tabs, mv.Name)
	if shadow == nil {
		return 0, &SQLError{Msg: "materialized view shadow table missing: " + name}
	}
	// 3) 增量路径：Simple 定义、非 CONCURRENTLY（该语义强制全量重建+原子交换）、
	//    唯一依赖基表 dirty 无 Overflow 且有可重算变更键。
	var dirty *mvDirty
	if mv.Simple && !concurrently && len(mv.Deps) == 1 {
		d, err := loadMVDirty(tx.Get, mv.Deps[0])
		if err != nil {
			return 0, err
		}
		if !d.Overflow && len(d.Rows) > 0 {
			dirty = d
		}
	}
	if dirty != nil {
		info, ierr := simpleMatViewInfoOf(mv.Select, tabs, mvs, views)
		if ierr != nil {
			return 0, ierr
		}
		if info == nil {
			return 0, &SQLError{Msg: "materialized view simple flag corrupted: " + name}
		}
		n, err := e.applyIncrementalRefresh(sess, tx, mv, shadow, info, dirty)
		if err != nil {
			return 0, err
		}
		affected += n
		mv.Version = maxVer
		raw, err := saveMatViews(mvs)
		if err != nil {
			return 0, err
		}
		if err := tx.Put(matViewsKey, raw); err != nil {
			return 0, err
		}
		if err := e.maybeCompactMVDirty(tx, mvs); err != nil {
			return 0, err
		}
		return affected, nil
	}
	// 4) 全量路径：重算完整快照 → schema 演化适配 → 差异写回（原子快照交换）。
	// 挂载当前刷新事务透传：依赖 MV 在本事务内已增量写入，execSelect 新建
	// 临时事务会读到旧快照导致依赖链结果不一致；forcedTx 窗口使内部 SELECT
	// 复用同一事务（autocommit 视为 false，不自动提交/回滚）。
	sess.mu.Lock()
	sess.forcedTx = tx
	sess.mu.Unlock()
	res, err := e.execSelect(sess, mv.Select)
	sess.mu.Lock()
	sess.forcedTx = nil
	sess.mu.Unlock()
	if err != nil {
		return 0, err
	}
	cols, err := matViewColumnNames(res.Columns)
	if err != nil {
		return 0, err
	}
	defs, normRows, err := inferMatViewSchema(cols, res.Rows)
	if err != nil {
		return 0, err
	}
	if matViewSchemaChanged(shadow, defs) {
		pkType := "INT"
		if mv.Simple {
			info, ierr := simpleMatViewInfoOf(mv.Select, tabs, mvs, views)
			if ierr != nil {
				return 0, ierr
			}
			if info != nil {
				pkType = info.pkType
			}
		}
		shadow.Columns = append(defs, ColumnDef{Name: mvHiddenPK, Type: pkType})
		raw, err := saveTables(tabs)
		if err != nil {
			return 0, err
		}
		if err := tx.Put(metaKey, raw); err != nil {
			return 0, err
		}
	}
	var n int
	if mv.Simple {
		info, ierr := simpleMatViewInfoOf(mv.Select, tabs, mvs, views)
		if ierr != nil {
			return 0, ierr
		}
		if info == nil {
			return 0, &SQLError{Msg: "materialized view simple flag corrupted: " + name}
		}
		n, err = e.refreshMatViewRowsPK(sess, tx, shadow, normRows, info.pkIdx)
	} else {
		n, err = e.refreshMatViewRows(sess, tx, shadow, normRows)
	}
	if err != nil {
		return 0, err
	}
	affected += n
	mv.Version = maxVer
	raw, err := saveMatViews(mvs)
	if err != nil {
		return 0, err
	}
	if err := tx.Put(matViewsKey, raw); err != nil {
		return 0, err
	}
	// 消费结构变更：清 Overflow（全量刷新已覆盖结构变化）。
	for _, dep := range mv.Deps {
		d, err := loadMVDirty(tx.Get, dep)
		if err != nil {
			return 0, err
		}
		if d.Overflow {
			d.Overflow = false
			raw, err := json.Marshal(d)
			if err != nil {
				return 0, err
			}
			if err := tx.Put(mvDirtyKey(dep), raw); err != nil {
				return 0, err
			}
		}
	}
	if err := e.maybeCompactMVDirty(tx, mvs); err != nil {
		return 0, err
	}
	return affected, nil
}

var _ = strings.TrimSpace // 保留 strings 引用（BLOB 分支注释预留）

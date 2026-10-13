// M12 分布式子查询：跨分片/跨节点 WHERE 子查询（IN / EXISTS / 标量）的归并与路由层。
//
// 设计要点：
//   - 执行模型（默认模式）：子查询独立执行（execSelect 走 scanMergedCluster 跨
//     region/跨节点归并），结果集在 coordinator 预计算缓存一次；主查询逐行过滤
//     时不再重复执行子查询。正确性语义与单机 inSubquery（compareVal 精比较）
//     完全一致。
//   - 路由模式（可下推 + 分片裁剪）：IN 子查询满足静态条件（子查询为单物理表、
//     无 JOIN/GROUP BY/ORDER BY/LIMIT/嵌套子查询、SELECT 恰为子查询表主键列，
//     且子查询表存在多个 region）时，对主查询左列每个不同值按主查询列值路由点查
//     子查询表归属 region（locateRow → rowKeyAt → getKVCluster 本地直读/远端
//     转发），命中后按子查询 WHERE 本地校验。只访问该值归属的分片，其余分片被
//     裁剪，网络传输从"子查询全表跨分片归并"降为"按值点查"。
//   - 退化与兜底：子查询表单分区/未分片（region<=1）或路由条件不满足时回退默认
//     模式（子查询本地全量归并），行为与单机一致；驱动值类型与子查询表主键编码
//     不兼容（如 INT 主键对 TEXT 驱动值）时返回 errSubqueryUnroutable，由
//     buildInSubquerySet 回退默认模式，保证与单机语义零偏差。
//   - EXISTS：子查询跨分片归并，存在任意行即为 true；无 ORDER BY/LIMIT 时克隆
//     子查询并设 LIMIT 1，尽早截断不改变存在性。
//   - 标量：子查询跨分片归并后取单行单列；多行报错、零行视为 NULL（条件不成立）。

package sql

import (
	"errors"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// errSubqueryUnroutable 驱动键值类型与子查询表主键编码不兼容，IN 子查询无法按主键
// 路由。调用方捕获后应回退默认模式（子查询跨分片归并），保证结果与单机一致。
var errSubqueryUnroutable = errors.New("subquery: routed key type incompatible with right PK encoding")

// subqueryResult 预计算后的子查询求值结果（execSelectRel 过滤前一次性计算，行级
// 过滤 O(1) 复用；仅覆盖顶层 WHERE 条件，CASE 分支内子查询走动态执行兜底）。
type subqueryResult struct {
	inSet    *inSubquerySet // IN 子查询命中值集合
	exists   bool           // EXISTS 判定结果
	scalar   Value          // 标量子查询值（scalarOK=true 时有效）
	scalarOK bool           // 标量子查询是否有行（0 行 = NULL，比较条件不成立）
}

// inSubquerySet IN 子查询结果第一列值集合。contains 用 compareVal 精比较，与
// 单机 inSubquery 逐值语义零偏差（允许跨 Kind 宽松相等，如 INT 5 == TEXT "5"）。
type inSubquerySet struct {
	vals []Value
}

func (s *inSubquerySet) contains(v Value) bool {
	for _, b := range s.vals {
		if compareVal(v, b) == 0 {
			return true
		}
	}
	return false
}

// whereHasSubquery WHERE 条件组中是否含子查询条件（路由/行级校验需规避）。
func whereHasSubquery(w *Where) bool {
	if w == nil {
		return false
	}
	for _, c := range w.Conds {
		if c.Sub != nil {
			return true
		}
	}
	return false
}

// precomputeSubqueries 对 WHERE 中所有子查询条件一次性求值（跨分片归并或路由点查），
// 返回按条件指针索引的缓存；无子查询时返回 nil。
func (e *Executor) precomputeSubqueries(sess *Session, tx txn.Txn, tabs []*TableMeta, rel *Rel, w *Where) (map[*Cond]subqueryResult, error) {
	if w == nil {
		return nil, nil
	}
	out := map[*Cond]subqueryResult{}
	for i := range w.Conds {
		c := &w.Conds[i]
		if c.Sub == nil {
			continue
		}
		switch c.Op {
		case "IN":
			set, err := e.buildInSubquerySet(sess, tx, tabs, rel, c)
			if err != nil {
				return nil, err
			}
			out[c] = subqueryResult{inSet: set}
		case "EXISTS":
			ok, err := e.existsSubquery(sess, tx, c.Sub)
			if err != nil {
				return nil, err
			}
			out[c] = subqueryResult{exists: ok}
		default: // 标量比较（= != < > <= >=）
			v, has, err := e.scalarSubquery(sess, tx, c.Sub)
			if err != nil {
				return nil, err
			}
			out[c] = subqueryResult{scalar: v, scalarOK: has}
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// buildInSubquerySet 构造 IN 子查询命中值集合。优先走路由模式（分片裁剪），
// 条件不满足或键值类型不兼容时回退默认模式（子查询跨分片归并一次）。
func (e *Executor) buildInSubquerySet(sess *Session, tx txn.Txn, tabs []*TableMeta, rel *Rel, c *Cond) (*inSubquerySet, error) {
	if metaS, ok := e.inSubqueryRoutable(tabs, c.Sub); ok && len(e.regionsOf(metaS)) > 1 {
		set, err := e.evalInSubqueryRouted(sess, tx, metaS, rel, c)
		if err != errSubqueryUnroutable {
			return set, err
		}
		// 类型不兼容 → 回退默认模式，保证与单机语义一致
	}
	res, err := e.execSelect(sess, c.Sub)
	if err != nil {
		return nil, err
	}
	vals := make([]Value, 0, len(res.Rows))
	for _, r := range res.Rows {
		if len(r) == 0 {
			continue
		}
		vals = append(vals, r[0])
	}
	return &inSubquerySet{vals: vals}, nil
}

// inSubqueryRoutable 静态判定 IN 子查询是否可走路由模式：
//   - 子查询为单物理表（无 FROM 子查询/JOIN/GROUP BY/ORDER BY/LIMIT/OFFSET）；
//   - SELECT 恰为单列裸列，且列名等于子查询表主键列（可点查）；
//   - 子查询 WHERE 无嵌套子查询（路由命中后的行级校验走 matchWhere，不支持 Sub）。
func (e *Executor) inSubqueryRoutable(tabs []*TableMeta, sub *SelectStmt) (*TableMeta, bool) {
	if sub == nil || sub.From == "" || sub.Subquery != nil || len(sub.Joins) > 0 ||
		len(sub.GroupBy) > 0 || sub.OrderBy != nil || sub.Limit > 0 || sub.Offset > 0 ||
		len(sub.Columns) != 1 {
		return nil, false
	}
	col := sub.Columns[0]
	if col.Ref != "" || col.Alias != "" || col.Agg != "" || col.Window != nil {
		return nil, false
	}
	meta := findTable(tabs, sub.From)
	if meta == nil || meta.PK == "" || col.Name != meta.PK {
		return nil, false
	}
	if whereHasSubquery(sub.Where) {
		return nil, false
	}
	return meta, true
}

// evalInSubqueryRouted 路由模式：对主查询左列每个不同值按子查询表主键路由点查
// 归属 region，命中且满足子查询 WHERE 的值加入命中集合。键值类型不兼容时返回
// errSubqueryUnroutable（调用方回退默认模式）。
func (e *Executor) evalInSubqueryRouted(sess *Session, tx txn.Txn, metaS *TableMeta, rel *Rel, c *Cond) (*inSubquerySet, error) {
	// 1. 收集主查询左列所有不同值（宽松 key 去重，避免重复点查）。
	seen := map[string]bool{}
	var vals []Value
	for _, r := range rel.Rows {
		var v Value
		var err error
		if c.LeftCase != nil {
			v, err = e.evalCaseRel(sess, tx, rel, r, c.LeftCase)
		} else {
			v, err = relValue(rel, r, qualifiedName(c.Ref, c.Col))
		}
		if err != nil {
			return nil, err
		}
		k := v.Kind + ":" + v.String()
		if seen[k] {
			continue
		}
		seen[k] = true
		vals = append(vals, v)
	}
	// 2. 兼容性预检：任一值类型与子查询主键编码不兼容 → 整体回退默认模式。
	for _, v := range vals {
		if !joinKeyCompatible(metaS, v) {
			return nil, errSubqueryUnroutable
		}
	}
	// 3. 逐值路由点查：定位归属 region → 编码带 region 前缀行键 → 本地直读/远端转发。
	var hit []Value
	for _, v := range vals {
		pk, err := pkBytes(metaS, v)
		if err != nil {
			return nil, err
		}
		rid, err := e.locateRow(metaS, pk)
		if err != nil {
			return nil, err
		}
		raw, err := e.getKVCluster(sess, tx, e.rowKeyAt(metaS, rid, pk))
		if err == storage.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		row, err := decodeRow(metaS, raw)
		if err != nil {
			return nil, err
		}
		ok, err := matchWhere(metaS, row, c.Sub.Where)
		if err != nil {
			return nil, err
		}
		if ok {
			hit = append(hit, v)
		}
	}
	return &inSubquerySet{vals: hit}, nil
}

// existsSubquery 跨分片执行 EXISTS 子查询：存在任意行即为 true。
// 无 ORDER BY/LIMIT 时克隆子查询并设 LIMIT 1 尽早截断（不改变存在性）。
func (e *Executor) existsSubquery(sess *Session, tx txn.Txn, sub *SelectStmt) (bool, error) {
	sub2 := *sub
	if sub2.OrderBy == nil && sub2.Limit <= 0 && sub2.Offset <= 0 {
		sub2.Limit = 1
	}
	res, err := e.execSelect(sess, &sub2)
	if err != nil {
		return false, err
	}
	return len(res.Rows) > 0, nil
}

// scalarSubquery 跨分片执行标量子查询：取单行单列。
// 多行报错；零行返回 has=false（NULL 语义，外层比较条件不成立）。
func (e *Executor) scalarSubquery(sess *Session, tx txn.Txn, sub *SelectStmt) (Value, bool, error) {
	if len(sub.Columns) != 1 {
		return Value{}, false, &SQLError{Msg: "scalar subquery must return exactly one column"}
	}
	res, err := e.execSelect(sess, sub)
	if err != nil {
		return Value{}, false, err
	}
	if len(res.Rows) > 1 {
		return Value{}, false, &SQLError{Msg: "scalar subquery returned more than one row"}
	}
	if len(res.Rows) == 0 {
		return Value{}, false, nil
	}
	if len(res.Rows[0]) == 0 {
		return Value{}, false, &SQLError{Msg: "scalar subquery returned empty row"}
	}
	return res.Rows[0][0], true, nil
}

// matchWhereRelCached 对 Rel 行执行 WHERE（子查询条件走预计算缓存，其余与
// matchWhereRel 相同）。cache 为 nil 时子查询条件动态逐值执行（正确性兜底）。
func (e *Executor) matchWhereRelCached(sess *Session, tx txn.Txn, rel *Rel, row []Value, w *Where, cache map[*Cond]subqueryResult) (bool, error) {
	if w == nil || len(w.Conds) == 0 {
		return true, nil
	}
	for _, c := range w.Conds {
		ok, err := e.matchCondRelCached(sess, tx, rel, row, &c, cache)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// matchCondRelCached 单条件求值：EXISTS/IN/标量子查询走缓存或动态执行；
// 非子查询条件逻辑与 matchCondRel 一致。
func (e *Executor) matchCondRelCached(sess *Session, tx txn.Txn, rel *Rel, row []Value, c *Cond, cache map[*Cond]subqueryResult) (bool, error) {
	// EXISTS：无左操作数，直接判定。
	if c.Op == "EXISTS" {
		if r, ok := cache[c]; ok {
			return r.exists, nil
		}
		return e.existsSubquery(sess, tx, c.Sub)
	}
	var a Value
	var err error
	if c.LeftCase != nil {
		a, err = e.evalCaseRel(sess, tx, rel, row, c.LeftCase)
		if err != nil {
			return false, err
		}
	} else {
		a, err = relValue(rel, row, qualifiedName(c.Ref, c.Col))
		if err != nil {
			return false, err
		}
	}
	// 子查询条件（IN / 标量比较）。
	if c.Sub != nil {
		if r, ok := cache[c]; ok {
			switch c.Op {
			case "IN":
				return r.inSet.contains(a), nil
			default: // 标量比较
				if !r.scalarOK {
					return false, nil
				}
				return matchCond(a, c.Op, r.scalar), nil
			}
		}
		// 无缓存（CASE 分支 / 兜底路径）：动态执行。
		switch c.Op {
		case "IN":
			return e.inSubquery(sess, tx, a, c.Sub)
		default:
			v, has, err := e.scalarSubquery(sess, tx, c.Sub)
			if err != nil {
				return false, err
			}
			if !has {
				return false, nil
			}
			return matchCond(a, c.Op, v), nil
		}
	}
	// 非子查询条件。
	switch c.Op {
	case "=", "!=", "<", ">", "<=", ">=":
		if c.RightRef != "" {
			b, err := relValue(rel, row, c.RightRef)
			if err != nil {
				return false, err
			}
			return matchCond(a, c.Op, b), nil
		}
		return matchCond(a, c.Op, c.Val), nil
	case "BETWEEN":
		return compareVal(a, c.Val) >= 0 && compareVal(a, c.Val2) <= 0, nil
	case "LIKE":
		return likeMatch(a.String(), c.Val.String()), nil
	case "IN":
		for _, v := range c.InList {
			if compareVal(a, v) == 0 {
				return true, nil
			}
		}
		return false, nil
	}
	return false, errf("unsupported op %q", c.Op)
}

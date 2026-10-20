package sql

// M12 分布式 JOIN：跨分片/跨节点 JOIN 的路由与合并层。
//
// 设计要点：
//   - 路由条件（planJoinRoute）：右表为物理表、ON 全部为等值（=），且至少一条
//     等值条件引用右表主键列（两种书写方向均可：a.x = b.pk 或 b.pk = a.x），
//     驱动侧对应列可在 left Rel 中解析到。
//   - 执行路径（routedJoin）：驱动表仍由 scanMergedCluster 跨 region/跨节点合并
//     得到全量左行；对每个左行按右表主键编码路由点查（getKVCluster 本地直读 /
//     远端转发），命中后 decodeRow 并按完整 ON 条件（matchJoinOn）校验合并，
//     未命中或条件不满足时 INNER 丢弃 / LEFT 补空。每左行至多命中 1 行
//     （右主键唯一），语义与 joinRels 全量拼接完全一致。
//   - 退化与兜底：单分区（隐式单 region）时路由点查即本地点查，行为与单机 JOIN
//     一致；驱动值类型与右主键编码不兼容（如 INT 主键对 TEXT 驱动值）时返回
//     errJoinUnroutable，由调用方回退 joinRels 全量内存 JOIN，保证与单机语义
//     零偏差。子查询右表、非等值 ON、无主键等值条件的 JOIN 一律走 joinRels。

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// errJoinUnroutable 驱动行键值类型与右表主键编码不兼容，JOIN 无法按主键路由。
// 调用方捕获后应回退 joinRels 全量拼接，保证结果与单机一致。
var errJoinUnroutable = errors.New("join: routed key type incompatible with right PK encoding")

// joinRoutePlan 一条可路由 JOIN 的静态计划。
type joinRoutePlan struct {
	// pkOnIdx ON 条件中引用右表主键的等值条件索引（其余 AND 条件由 matchJoinOn 校验）。
	pkOnIdx int
	// leftIdx 驱动左行中与右主键等值的列索引。
	leftIdx int
}

// planJoinRoute 判定 JOIN 是否可走分布式路由路径，并产出路由计划。
// 判定失败（返回 ok=false）时调用方应走 joinRels 全量 JOIN。
func planJoinRoute(left *Rel, jc *JoinClause, rmeta *TableMeta) (joinRoutePlan, bool) {
	if rmeta == nil || rmeta.PK == "" || len(jc.On) == 0 {
		return joinRoutePlan{}, false
	}
	// 右表列名集（别名限定，与 relFromScan 输出一致）。
	alias := orAlias(jc.Table, jc.Alias)
	right := &Rel{Cols: make([]string, 0, len(rmeta.Columns)), Types: make([]string, 0, len(rmeta.Columns))}
	for _, c := range rmeta.Columns {
		right.Cols = append(right.Cols, alias+"."+c.Name)
		right.Types = append(right.Types, c.Type)
	}
	// 仅等值条件可路由；任一非等值 ON 条件回退全量 JOIN。
	for _, c := range jc.On {
		if c.Op != "=" {
			return joinRoutePlan{}, false
		}
	}
	// 寻找引用右表主键的等值条件（两种书写方向）。
	for i, c := range jc.On {
		// 方向 1：右主键在右操作数侧，形如 a.x = b.pk。
		if c.RightRef != "" && baseColName(c.RightRef) == rmeta.PK && relColIndex(right, c.RightRef) >= 0 {
			if li := relColIndex(left, qualifiedName(c.Ref, c.Col)); li >= 0 {
				return joinRoutePlan{pkOnIdx: i, leftIdx: li}, true
			}
		}
		// 方向 2：右主键在左操作数侧，形如 b.pk = a.x。
		rightName := qualifiedName(c.Ref, c.Col)
		if baseColName(rightName) == rmeta.PK && relColIndex(right, rightName) >= 0 {
			if c.RightRef != "" {
				if li := relColIndex(left, c.RightRef); li >= 0 {
					return joinRoutePlan{pkOnIdx: i, leftIdx: li}, true
				}
			}
		}
	}
	return joinRoutePlan{}, false
}

// joinRightSchema 仅以列定义构造右表 Rel（不物化行）：路由 JOIN 命中时由
// execSelect 跳过右表全量扫描（如 order_line 30 万行），路由实现直接从存储
// 区间/点查扫描；若路由执行回退 joinRels，再由 joinRel 补物化右表。
func joinRightSchema(meta *TableMeta, alias string) *Rel {
	rel := &Rel{Cols: make([]string, 0, len(meta.Columns)), Types: make([]string, 0, len(meta.Columns))}
	for _, c := range visibleColumns(meta) {
		rel.Cols = append(rel.Cols, alias+"."+c.Name)
		rel.Types = append(rel.Types, c.Type)
	}
	return rel
}

// joinRel 统一 JOIN 入口：满足分布式路由条件时走 routedJoin（路由点查 + 合并），
// 否则回退 joinRels（单机全量内存 JOIN，行为与 M1 一致）。
func (e *Executor) joinRel(sess *Session, tx txn.Txn, tabs []*TableMeta, left, right *Rel, jc *JoinClause) (*Rel, error) {
	dbgLog("joinRel enter jc.Table=%q lenOn=%d leftCols=%v", jc.Table, len(jc.On), left.Cols)
	for i := range jc.On {
		c := &jc.On[i]
		dbgLog("  on[%d] col=%q ref=%q op=%q rightRef=%q val=%+v rightExpr=%v", i, c.Col, c.Ref, c.Op, c.RightRef, c.Val, c.RightExpr != nil)
	}
	if jc.Subquery == nil && len(jc.On) > 0 {
		meta := findTable(tabs, jc.Table)
		dbgLog("  meta=%v pk=%q pkcols=%v", meta != nil, func() string {
			if meta != nil {
				return meta.PK
			}
			return ""
		}(), func() []string {
			if meta != nil {
				return meta.PKCols
			}
			return nil
		}())
		if plan, ok := planJoinRoute(left, jc, meta); ok {
			dbgLog("  planJoinRoute HIT leftIdx=%d pkOnIdx=%d", plan.leftIdx, plan.pkOnIdx)
			rel, err := e.routedJoin(sess, tx, left, right, meta, jc, plan)
			if err == errJoinUnroutable {
				// 键值类型与右主键编码不兼容 → 回退全量 JOIN 保证语义一致。
				dbgLog("  routedJoin unroutable -> fallback joinRels")
				if len(right.Rows) == 0 {
					right, err = e.relFromTable(sess, tx, tabs, jc.Table, orAlias(jc.Table, jc.Alias), nil, jc.Alias)
					if err != nil {
						return nil, err
					}
				}
				return joinRels(left, right, jc)
			}
			return rel, err
		}
		// 复合主键前缀 + 末列范围路由：右表主键前 k 列等值（驱动左行/字面量）
		// + 末列范围（边界为左行可求值表达式/引用/字面量）时，按有序区间 Seek
		// 替代右表全量扫描（TPC-C STOCK_LEVEL district JOIN order_line 关键路径）。
		if plan, ok := planJoinRange(left, jc, meta); ok {
			dbgLog("  planJoinRange HIT rangeCol=%q eq=%v", plan.rangeCol, plan.eqCols)
			rel, err := e.routedJoinRange(sess, tx, left, right, meta, jc, plan)
			if err == errJoinUnroutable {
				dbgLog("  routedJoinRange unroutable -> fallback joinRels")
				if len(right.Rows) == 0 {
					right, err = e.relFromTable(sess, tx, tabs, jc.Table, orAlias(jc.Table, jc.Alias), nil, jc.Alias)
					if err != nil {
						return nil, err
					}
				}
				return joinRels(left, right, jc)
			}
			return rel, err
		}
		dbgLog("  no join route -> joinRels")
	}
	return joinRels(left, right, jc)
}

// dbgLog 临时调试日志（验证后删除）。
func dbgLog(format string, args ...interface{}) {
	f, err := os.OpenFile("D:/AI项目/OpenXDB开源数据库开发/openxdb/joinrng_debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, format+"\n", args...)
}

// routedJoin 分布式路由 JOIN：驱动左行按右表主键路由点查并合并。
// 每左行至多命中 1 个右行（右主键唯一）；未命中 / 条件不满足时
// INNER 丢弃该行、LEFT 以空值补右列，语义与 joinRels 一致。
func (e *Executor) routedJoin(sess *Session, tx txn.Txn, left, right *Rel, rmeta *TableMeta, jc *JoinClause, plan joinRoutePlan) (*Rel, error) {
	out := &Rel{Cols: append(append([]string{}, left.Cols...), right.Cols...), Types: append(append([]string{}, left.Types...), right.Types...)}
	for _, lr := range left.Rows {
		lv := lr[plan.leftIdx]
		if !joinKeyCompatible(rmeta, lv) {
			return nil, errJoinUnroutable
		}
		pk, err := pkBytes(rmeta, lv)
		if err != nil {
			return nil, err
		}
		// 按分区键路由：驱动值编码右表主键 → locate 所属 region → 编码带 region
		// 前缀的物理行键（与 INSERT 写入键一致）→ 本地直读 / 远端转发点查。
		rid, err := e.locateRow(rmeta, pk)
		if err != nil {
			return nil, err
		}
		key := e.rowKeyAt(rmeta, rid, pk)
		raw, err := e.getKVCluster(sess, tx, key)
		if err != nil {
			if err == storage.ErrNotFound {
				if jc.Type == "LEFT" {
					out.Rows = append(out.Rows, leftJoinNullRow(left, right, lr))
				}
				continue
			}
			return nil, err
		}
		rr, err := decodeRow(rmeta, raw)
		if err != nil {
			return nil, err
		}
		ok, err := matchJoinOn(left, lr, right, rr, jc.On)
		if err != nil {
			return nil, err
		}
		if !ok {
			if jc.Type == "LEFT" {
				out.Rows = append(out.Rows, leftJoinNullRow(left, right, lr))
			}
			continue
		}
		merged := make([]Value, 0, len(lr)+len(rr))
		merged = append(merged, lr...)
		merged = append(merged, rr...)
		out.Rows = append(out.Rows, merged)
	}
	return out, nil
}

// leftJoinNullRow 构造 LEFT JOIN 未匹配行的补空输出（右列全部 TEXT 空串）。
func leftJoinNullRow(left, right *Rel, lr []Value) []Value {
	merged := make([]Value, 0, len(lr)+len(right.Cols))
	merged = append(merged, lr...)
	for range right.Cols {
		merged = append(merged, Value{Kind: "TEXT", S: ""})
	}
	return merged
}

// joinKeyCompatible 驱动键值 Kind 与右表主键列编码是否兼容（编码结果与写入一致）。
// 不兼容时路由点查无法命中而全量 JOIN 可能匹配，必须回退以保证语义一致：
//   - INT 主键仅接受 INT（DECIMAL/TEXT 的 pkBytes 编码不同）；
//   - DECIMAL 主键仅接受 DECIMAL；
//   - BLOB 主键接受 TEXT/BLOB（一律 hex 解码）；
//   - TEXT/DATE 主键接受除 BLOB 外任意 Kind（pkBytes 按 S 原字节编码，与写入一致）。
func joinKeyCompatible(meta *TableMeta, v Value) bool {
	pkType := ""
	for _, c := range meta.Columns {
		if c.Name == meta.PK {
			pkType = c.Type
			break
		}
	}
	switch pkType {
	case "INT":
		return v.Kind == "INT"
	case "DECIMAL":
		return v.Kind == "DECIMAL"
	case "BLOB":
		return v.Kind == "TEXT" || v.Kind == "BLOB"
	default: // TEXT / DATE
		return v.Kind != "BLOB"
	}
}

// joinRangePlan 复合主键前缀 + 末列范围路由 JOIN 计划：
// ON 中右表主键前 k 列按序等值（右侧为左行列引用或字面量，两种书写方向均可），
// 末列出现范围条件（> / >= / < / <=，边界为左行可求值表达式/左列引用/字面量），
// 驱动左行按 [等值前缀 + 末列范围] 构造有序物理区间 Seek 右表。
type joinRangePlan struct {
	// eqCols 等值前缀对应的右表主键列名（按 PKCols 顺序）。
	eqCols []string
	// eqLeftIdx 等值前缀每列对应的左行列索引；eqIsLit[i]=true 时使用 eqLits[i] 字面量。
	eqLeftIdx []int
	eqLits    []Value
	eqIsLit   []bool
	// rangeCol 范围列名（主键末列，须 INT/DECIMAL 定长保序编码）。
	rangeCol string
	// loCond / hiCond 下界（> / >=）与上界（< / <=）ON 条件（边界在 Cond 右侧）。
	loCond *Cond
	hiCond *Cond
}

// planJoinRange 判定 JOIN 是否可走复合主键前缀 + 末列范围路由。
func planJoinRange(left *Rel, jc *JoinClause, rmeta *TableMeta) (joinRangePlan, bool) {
	if rmeta == nil || jc.Subquery != nil || len(jc.On) == 0 {
		return joinRangePlan{}, false
	}
	var pkCols []string
	if len(rmeta.PKCols) > 0 {
		pkCols = rmeta.PKCols
	} else if rmeta.PK != "" {
		pkCols = []string{rmeta.PK}
	} else {
		return joinRangePlan{}, false
	}
	// 至少 1 列等值前缀；范围列（INT/DECIMAL 定长保序）可位于主键任意位置，
	// 其后缀列在编码上被范围列截断自动排除（TPC-C order_line 主键
	// (ol_w_id,ol_d_id,ol_o_id,ol_number)，范围列 ol_o_id 后有 ol_number）。
	if len(pkCols) == 0 {
		return joinRangePlan{}, false
	}
	alias := orAlias(jc.Table, jc.Alias)
	right := &Rel{Cols: make([]string, 0, len(rmeta.Columns)), Types: make([]string, 0, len(rmeta.Columns))}
	for _, c := range rmeta.Columns {
		right.Cols = append(right.Cols, alias+"."+c.Name)
		right.Types = append(right.Types, c.Type)
	}
	// 定位范围列：PKCols 中第一个出现 > / >= / < / <= 条件的列。
	rangeIdx := -1
	for pi, pc := range pkCols {
		for i := range jc.On {
			c := &jc.On[i]
			if c.Col == "" || baseColName(qualifiedName(c.Ref, c.Col)) != pc || relColIndex(right, qualifiedName(c.Ref, c.Col)) < 0 {
				continue
			}
			switch c.Op {
			case ">", ">=", "<", "<=":
				rangeIdx = pi
				break
			}
		}
		if rangeIdx >= 0 {
			break
		}
	}
	if rangeIdx <= 0 || !pkColTypeIn(rmeta, pkCols[rangeIdx], "INT", "DECIMAL") {
		return joinRangePlan{}, false
	}
	plan := joinRangePlan{}
	// 等值前缀：PKCols[0..rangeIdx-1] 按序找到等值条件。
	for pi := 0; pi < rangeIdx; pi++ {
		pc := pkCols[pi]
		found := false
		for i := range jc.On {
			c := &jc.On[i]
			if c.Op != "=" || c.LeftCase != nil || c.LeftExpr != nil || c.Sub != nil {
				continue
			}
			// 方向 1：a.x = b.pk
			if c.RightRef != "" && baseColName(c.RightRef) == pc && relColIndex(right, c.RightRef) >= 0 {
				if li := relColIndex(left, qualifiedName(c.Ref, c.Col)); li >= 0 {
					plan.eqCols = append(plan.eqCols, pc)
					plan.eqLeftIdx = append(plan.eqLeftIdx, li)
					plan.eqIsLit = append(plan.eqIsLit, false)
					found = true
					break
				}
			}
			// 方向 2：b.pk = a.x 或 b.pk = 字面量
			if c.Col != "" && baseColName(qualifiedName(c.Ref, c.Col)) == pc && relColIndex(right, qualifiedName(c.Ref, c.Col)) >= 0 {
				if c.RightRef != "" {
					if li := relColIndex(left, c.RightRef); li >= 0 {
						plan.eqCols = append(plan.eqCols, pc)
						plan.eqLeftIdx = append(plan.eqLeftIdx, li)
						plan.eqIsLit = append(plan.eqIsLit, false)
						found = true
						break
					}
				} else if c.RightExpr == nil {
					plan.eqCols = append(plan.eqCols, pc)
					plan.eqLeftIdx = append(plan.eqLeftIdx, -1)
					plan.eqLits = append(plan.eqLits, c.Val)
					plan.eqIsLit = append(plan.eqIsLit, true)
					found = true
					break
				}
			}
		}
		if !found {
			return joinRangePlan{}, false
		}
	}
	// 范围列：收集 > / >= / < / <= 条件，边界须左行可求值。
	last := pkCols[rangeIdx]
	plan.rangeCol = last
	for i := range jc.On {
		c := &jc.On[i]
		if c.Col == "" || baseColName(qualifiedName(c.Ref, c.Col)) != last || relColIndex(right, qualifiedName(c.Ref, c.Col)) < 0 {
			continue
		}
		if c.Sub != nil || c.LeftCase != nil || c.LeftExpr != nil {
			continue
		}
		// 边界侧：表达式（引用须左行可解析）或左列引用或字面量。
		if c.RightExpr != nil {
			if !joinExprResolvable(left, c.RightExpr) {
				continue
			}
		} else if c.RightRef != "" {
			if relColIndex(left, c.RightRef) < 0 {
				continue
			}
		}
		switch c.Op {
		case ">", ">=":
			if plan.loCond == nil {
				plan.loCond = c
			}
		case "<", "<=":
			if plan.hiCond == nil {
				plan.hiCond = c
			}
		}
	}
	if plan.loCond == nil && plan.hiCond == nil {
		return joinRangePlan{}, false
	}
	return plan, true
}

// joinExprResolvable 表达式中的列引用是否均可在左行解析（执行期仅左行上下文）。
func joinExprResolvable(left *Rel, e *Expr) bool {
	if e == nil {
		return true
	}
	if e.Op == "" {
		if e.Val != nil {
			return true
		}
		return relColIndex(left, qualifiedName(e.Ref, e.Col)) >= 0
	}
	return joinExprResolvable(left, e.Left) && joinExprResolvable(left, e.Right)
}

// joinCondBound 求 ON 范围条件右侧边界值（左行上下文）。
func joinCondBound(left *Rel, lr []Value, c *Cond) (Value, error) {
	if c.RightExpr != nil {
		return evalExprForRow(left, lr, c.RightExpr)
	}
	if c.RightRef != "" {
		return relValue(left, lr, c.RightRef)
	}
	return c.Val, nil
}

// routedJoinRange 复合主键前缀 + 末列范围路由 JOIN 执行：
// 对每个左行求等值前缀与范围边界，构造有序物理区间扫描右表候选行，
// 再以完整 ON 条件（matchJoinOn）行级校验，语义与 joinRels 一致。
func (e *Executor) routedJoinRange(sess *Session, tx txn.Txn, left, right *Rel, rmeta *TableMeta, jc *JoinClause, plan joinRangePlan) (*Rel, error) {
	out := &Rel{Cols: append(append([]string{}, left.Cols...), right.Cols...), Types: append(append([]string{}, left.Types...), right.Types...)}
	for _, lr := range left.Rows {
		vals := make([]Value, 0, len(plan.eqLeftIdx))
		for i := range plan.eqLeftIdx {
			if plan.eqIsLit[i] {
				vals = append(vals, plan.eqLits[i])
			} else {
				vals = append(vals, lr[plan.eqLeftIdx[i]])
			}
		}
		prefix, err := pkPrefixBytes(rmeta, vals)
		if err != nil {
			return nil, err
		}
		// 等值值与右主键列类型编码兼容检查：不兼容回退全量 JOIN 保证语义一致。
		for i := range plan.eqLeftIdx {
			if !joinRangeValCompatible(rmeta, plan.eqCols[i], vals[i]) {
				return nil, errJoinUnroutable
			}
		}
		var lo, hi *Value
		loIncl, hiIncl := false, false
		if plan.loCond != nil {
			v, err := joinCondBound(left, lr, plan.loCond)
			if err != nil {
				return nil, err
			}
			if !joinRangeValCompatible(rmeta, plan.rangeCol, v) {
				return nil, errJoinUnroutable
			}
			lo = &v
			loIncl = plan.loCond.Op == ">="
		}
		if plan.hiCond != nil {
			v, err := joinCondBound(left, lr, plan.hiCond)
			if err != nil {
				return nil, err
			}
			if !joinRangeValCompatible(rmeta, plan.rangeCol, v) {
				return nil, errJoinUnroutable
			}
			hi = &v
			hiIncl = plan.hiCond.Op == "<="
		}
		pairs, err := e.scanMergedCluster(sess, tx, e.joinRangeRanges(rmeta, prefix, lo, loIncl, hi, hiIncl))
		if err != nil {
			return nil, err
		}
		dbgLog("rng scan lr=%d prefix=%x lo=%+v hi=%+v pairs=%d", len(left.Rows) > 0 && len(left.Rows) == 1, prefix, lo, hi, len(pairs))
		if len(pairs) == 0 {
			if jc.Type == "LEFT" {
				out.Rows = append(out.Rows, leftJoinNullRow(left, right, lr))
			}
			continue
		}
		for _, p := range pairs {
			rr, err := decodeRow(rmeta, p.Value)
			if err != nil {
				return nil, err
			}
			ok, err := matchJoinOn(left, lr, right, rr, jc.On)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			merged := make([]Value, 0, len(lr)+len(rr))
			merged = append(merged, lr...)
			merged = append(merged, rr...)
			out.Rows = append(out.Rows, merged)
		}
	}
	return out, nil
}

// joinRangeRanges 等值前缀 + 末列范围物理区间（仿 pkRangeRanges）：
// [EncodeTableKey(tid, prefix+enc(lo)), EncodeTableKey(tid, prefix+enc(hi))) 按 regions 展开。
// 边界编码与 pkPrefixBytes 一致（INT/DECIMAL 定长 8B 大端保序），含等号用 incBytes8 精确递增。
func (e *Executor) joinRangeRanges(meta *TableMeta, prefix []byte, lo *Value, loIncl bool, hi *Value, hiIncl bool) []storage.KeyRange {
	rs := e.regionsOf(meta)
	out := make([]storage.KeyRange, 0, len(rs))
	startInner := EncodeTableKey(meta.ID, prefix)
	endInner := EncodeTableKey(meta.ID, append(append([]byte{}, prefix...), 0xff))
	if lo != nil {
		if lb, err := encodePkColValue(meta, lo); err == nil {
			if !loIncl {
				lb = incBytes8(lb)
			}
			startInner = EncodeTableKey(meta.ID, append(append([]byte{}, prefix...), lb...))
		}
	}
	if hi != nil {
		if hb, err := encodePkColValue(meta, hi); err == nil {
			if hiIncl {
				hb = incBytes8(hb)
			}
			endInner = EncodeTableKey(meta.ID, append(append([]byte{}, prefix...), hb...))
		}
	}
	for _, rg := range rs {
		out = append(out, storage.KeyRange{
			Start: sharding.EncodeRegionKey(rg.RegionID, startInner),
			End:   sharding.EncodeRegionKey(rg.RegionID, endInner),
		})
	}
	return out
}

// encodePkColValue 单列主键值编码（INT/DECIMAL 定长 8B 大端，与 pkPrefixBytes 一致）。
func encodePkColValue(meta *TableMeta, v *Value) ([]byte, error) {
	switch v.Kind {
	case "INT":
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v.I))
		return b, nil
	case "DECIMAL":
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v.I)^(1<<63))
		return b, nil
	case "BLOB":
		raw, err := decodeHex(strings.ToUpper(v.S))
		if err != nil {
			return nil, err
		}
		return raw, nil
	default:
		b := []byte(v.S)
		lb := make([]byte, 2)
		binary.BigEndian.PutUint16(lb, uint16(len(b)))
		return append(lb, b...), nil
	}
}

// joinRangeValCompatible 值与右表主键列类型编码是否兼容（编码结果与写入一致）。
func joinRangeValCompatible(meta *TableMeta, col string, v Value) bool {
	colType := ""
	for _, c := range meta.Columns {
		if c.Name == col {
			colType = c.Type
			break
		}
	}
	switch colType {
	case "INT":
		return v.Kind == "INT"
	case "DECIMAL":
		return v.Kind == "DECIMAL"
	case "BLOB":
		return v.Kind == "TEXT" || v.Kind == "BLOB"
	default: // TEXT / DATE
		return v.Kind != "BLOB"
	}
}

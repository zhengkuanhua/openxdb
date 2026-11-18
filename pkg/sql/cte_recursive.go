package sql

import (
	"strconv"
	"strings"
)

// v1.19.0 D5：递归 CTE（WITH RECURSIVE）。
//
// 语义（MySQL 兼容子集）：
//
//	WITH RECURSIVE name [(col, ...)] AS (
//	    <anchor SELECT>            -- 不含自引用的非递归种子
//	  UNION [ALL]
//	    <recursive SELECT>         -- 自引用仅允许出现在 FROM/JOIN 主表位置
//	) [, ...] <SELECT ...>
//
// 执行策略（与 M10 非递归 CTE 完全隔离）：
//   - 非递归 CTE：仍由 executor.expandCTEs 走「表名替换为定义子查询深拷贝」的
//     宏展开，本文件不参与，行为与 v1.18.0 完全一致（零回归）。
//   - 递归 CTE：物化 worktable 迭代（seminaive 增量迭代）：
//     ① 执行 anchor，结果行入 worktable（UNION 时按行去重）；
//     ② 每轮把「上一轮新增行」作为 CTE 名的物化输入，执行递归项；
//     ③ 产生的新行入库（UNION 去重后丢弃已见行），无新行则收敛；
//     ④ 迭代轮数超过 recursive_cte_max_depth 报错（不静默截断结果）。
//   - 物化结果压入会话语句级作用域（Session.recCTEScopes），嵌套 SELECT 与
//     JOIN 右端均按栈查找；执行结束出栈，不跨语句泄漏。
//   - anchor / 递归项都以 e.execSelect 执行，因此其读路径沿用既有 loadTables →
//     scanMergedCluster（分片 region 路由 + 可见性过滤）与分布式 ORDER 判定，
//     不绕过 region 路由与可见性过滤。

// recCTEMaxDepthDefault 递归 CTE 迭代深度默认上限（MySQL cte_max_recursion_depth 口径）。
const recCTEMaxDepthDefault = 1000

// SetRecursiveCTEMaxDepth 设置递归 CTE 最大迭代深度（SET recursive_cte_max_depth 热更新）。
func (e *Executor) SetRecursiveCTEMaxDepth(n int) {
	if n < 1 {
		n = 1
	}
	e.recCTEMaxDepth = n
}

// RecursiveCTEMaxDepth 返回当前递归 CTE 最大迭代深度（<=0 视为默认值）。
func (e *Executor) RecursiveCTEMaxDepth() int {
	if e.recCTEMaxDepth <= 0 {
		return recCTEMaxDepthDefault
	}
	return e.recCTEMaxDepth
}

// ---- 会话语句级作用域 ----

// recCTEPush 压入本语句物化出的递归 CTE 结果集（执行结束须 recCTEPop）。
func (sess *Session) recCTEPush(scope map[string]*Rel) {
	sess.recCTEScopes = append(sess.recCTEScopes, scope)
}

// recCTEPop 弹出最近压入的递归 CTE 作用域。
func (sess *Session) recCTEPop() {
	if n := len(sess.recCTEScopes); n > 0 {
		sess.recCTEScopes = sess.recCTEScopes[:n-1]
	}
}

// recCTELookup 按栈顶向下查找递归 CTE 物化结果（内层遮蔽外层）。
func (sess *Session) recCTELookup(name string) *Rel {
	if name == "" || len(sess.recCTEScopes) == 0 {
		return nil
	}
	for i := len(sess.recCTEScopes) - 1; i >= 0; i-- {
		if r, ok := sess.recCTEScopes[i][name]; ok {
			return r
		}
	}
	return nil
}

// asRel 以指定表别名重建物化结果的列限定名（行集只读复用，切片头浅拷贝，
// 避免下游排序等原地操作影响其他引用方）。
func (r *Rel) asRel(alias string) *Rel {
	if r == nil {
		return nil
	}
	cols := make([]string, len(r.Cols))
	for i, c := range r.Cols {
		cols[i] = alias + "." + stripRelColPrefix(c)
	}
	rows := make([][]Value, len(r.Rows))
	copy(rows, r.Rows)
	return &Rel{Cols: cols, Types: append([]string(nil), r.Types...), Rows: rows}
}

// stripRelColPrefix 去掉 Rel 列限定前缀（"t.col" → "col"）。
func stripRelColPrefix(c string) string {
	if i := strings.LastIndexByte(c, '.'); i >= 0 {
		return c[i+1:]
	}
	return c
}

// ---- 物化执行 ----

// materializeRecursiveCTEs 物化语句内全部递归 CTE 定义，返回出栈函数
// （nil 表示本语句无递归 CTE，调用方不做任何额外处理）。
//
// prior 语义：按 WITH 声明序逐个物化，第 i 个 CTE 的 anchor/递归项可引用
// 第 0..i-1 个定义（非递归定义以 CTE 段随 anchor 执行，递归定义已进作用域栈）。
func (e *Executor) materializeRecursiveCTEs(sess *Session, s *SelectStmt) (func(), error) {
	if s == nil || len(s.CTEs) == 0 {
		return nil, nil
	}
	var scope map[string]*Rel
	for i := range s.CTEs {
		c := &s.CTEs[i]
		if !c.Recursive {
			continue
		}
		if c.Select == nil || c.RecursiveSelect == nil {
			return nil, &SQLError{Msg: "recursive CTE " + c.Name + " requires an anchor and a recursive term separated by UNION"}
		}
		if err := validateRecursiveTerm(c.RecursiveSelect, c); err != nil {
			return nil, err
		}
		rel, err := e.runRecursiveCTE(sess, c, s.CTEs[:i])
		if err != nil {
			return nil, err
		}
		if scope == nil {
			scope = make(map[string]*Rel, len(s.CTEs))
		}
		scope[c.Name] = rel
	}
	if scope == nil {
		return nil, nil
	}
	sess.recCTEPush(scope)
	return sess.recCTEPop, nil
}

// runRecursiveCTE 以 worktable 迭代物化单个递归 CTE。
func (e *Executor) runRecursiveCTE(sess *Session, c *CTE, prior []CTE) (*Rel, error) {
	// ① anchor：独立深拷贝后执行，避免执行期改写（视图展开等）污染 AST 原文。
	anchor, err := cloneSelect(c.Select)
	if err != nil {
		return nil, err
	}
	anchor.CTEs = cloneNonRecursiveCTEs(prior)
	anchorRes, err := e.execSelect(sess, anchor)
	if err != nil {
		return nil, err
	}
	cols := cteOutputColumns(c, anchorRes.Columns)
	if len(cols) != len(anchorRes.Columns) {
		return nil, &SQLError{Msg: "recursive CTE " + c.Name + " column count mismatch: " +
			strconv.Itoa(len(cols)) + " declared, " + strconv.Itoa(len(anchorRes.Columns)) + " from anchor"}
	}
	types := make([]string, len(cols))
	copy(types, anchorRes.ColTypes)
	rel := &Rel{Cols: make([]string, len(cols)), Types: types, Rows: make([][]Value, 0, len(anchorRes.Rows))}
	for i, nm := range cols {
		rel.Cols[i] = c.Name + "." + nm
	}
	seen := make(map[string]struct{}, len(anchorRes.Rows))
	delta := make([][]Value, 0, len(anchorRes.Rows))
	for _, r := range anchorRes.Rows {
		if len(r) != len(cols) {
			return nil, &SQLError{Msg: "recursive CTE " + c.Name + " column count mismatch: " +
				strconv.Itoa(len(r)) + " from anchor, " + strconv.Itoa(len(cols)) + " declared"}
		}
		if !c.UnionAll {
			k := rowDedupKey(r)
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
		}
		delta = append(delta, r)
	}
	rel.Rows = append(rel.Rows, delta...)
	// ② 迭代：每轮以上一轮新增行为递归项输入，产出新行入库，无新行收敛。
	maxDepth := e.RecursiveCTEMaxDepth()
	for depth := 1; len(delta) > 0; depth++ {
		if depth > maxDepth {
			return nil, &SQLError{Msg: "recursive CTE " + c.Name + " exceeded maximum recursion depth " +
				strconv.Itoa(maxDepth)}
		}
		term, err := cloneSelect(c.RecursiveSelect)
		if err != nil {
			return nil, err
		}
		// 递归项物化输入：仅本轮新增行（seminaive 迭代，避免重复产出已见行）。
		iter := &Rel{Cols: rel.Cols, Types: append([]string(nil), rel.Types...), Rows: delta}
		sess.recCTEPush(map[string]*Rel{c.Name: iter})
		termRes, termErr := e.execSelect(sess, term)
		sess.recCTEPop()
		if termErr != nil {
			return nil, termErr
		}
		if len(termRes.Columns) != len(cols) {
			return nil, &SQLError{Msg: "recursive CTE " + c.Name + " column count mismatch: anchor " +
				strconv.Itoa(len(cols)) + ", recursive term " + strconv.Itoa(len(termRes.Columns))}
		}
		next := make([][]Value, 0, len(termRes.Rows))
		for _, r := range termRes.Rows {
			if len(r) != len(cols) {
				return nil, &SQLError{Msg: "recursive CTE " + c.Name + " column count mismatch: anchor " +
					strconv.Itoa(len(cols)) + ", recursive term " + strconv.Itoa(len(r))}
			}
			if !c.UnionAll {
				k := rowDedupKey(r)
				if _, dup := seen[k]; dup {
					continue
				}
				seen[k] = struct{}{}
			}
			next = append(next, r)
		}
		rel.Rows = append(rel.Rows, next...)
		delta = next
	}
	return rel, nil
}

// cteOutputColumns 计算递归 CTE 的输出列名：显式列名列表优先，
// 否则沿用 anchor 输出列名（空名回退 c1..cN）。
func cteOutputColumns(c *CTE, anchorCols []string) []string {
	if len(c.Columns) > 0 {
		out := make([]string, len(c.Columns))
		for i, nm := range c.Columns {
			out[i] = stripRelColPrefix(nm)
		}
		return out
	}
	out := make([]string, len(anchorCols))
	for i, nm := range anchorCols {
		nm = stripRelColPrefix(nm)
		if nm == "" {
			nm = "c" + strconv.Itoa(i+1)
		}
		out[i] = nm
	}
	return out
}

// cloneNonRecursiveCTEs 深拷贝前置非递归 CTE 定义（递归定义已物化进作用域栈，
// 不再随 anchor 重复物化）。无前置定义时返回 nil。
func cloneNonRecursiveCTEs(prior []CTE) []CTE {
	var out []CTE
	for i := range prior {
		if prior[i].Recursive {
			continue
		}
		c := prior[i]
		if c.Select != nil {
			if cl, err := cloneSelect(c.Select); err == nil {
				c.Select = cl
			}
		}
		out = append(out, c)
	}
	return out
}

// rowDedupKey 行去重键：各列 Value.String() 以 0xff 分隔（与 DISTINCT 去重同口径）。
func rowDedupKey(row []Value) string {
	if len(row) == 0 {
		return ""
	}
	var b strings.Builder
	for i, v := range row {
		if i > 0 {
			b.WriteByte(0xff)
		}
		b.WriteString(v.String())
	}
	return b.String()
}

// stripRecursiveCTEs 返回剔除递归定义后的 CTE 列表（空则返回 nil），
// 供 expandCTEs 只处理非递归定义。
func stripRecursiveCTEs(ctes []CTE) []CTE {
	var out []CTE
	for i := range ctes {
		if ctes[i].Recursive {
			continue
		}
		out = append(out, ctes[i])
	}
	return out
}

// hasRecursiveCTE 判断本语句是否需要走 Rel 执行路径：语句定义了递归 CTE，
// 或 FROM / JOIN 右端引用了本语句作用域内的递归 CTE。
func (e *Executor) hasRecursiveCTE(sess *Session, s *SelectStmt) bool {
	if s == nil {
		return false
	}
	for i := range s.CTEs {
		if s.CTEs[i].Recursive {
			return true
		}
	}
	if s.Subquery != nil {
		return false
	}
	if sess.recCTELookup(s.From) != nil {
		return true
	}
	for i := range s.Joins {
		if s.Joins[i].Subquery == nil && sess.recCTELookup(s.Joins[i].Table) != nil {
			return true
		}
	}
	return false
}

// ---- 递归项校验 ----

// validateRecursiveTerm 校验递归项中自引用的合法位置（MySQL 口径）：
//   - 自引用必须出现且只能出现一次，且必须位于递归项本体（top 层 FROM 主表 /
//     JOIN 右端表）位置；缺失或重复引用报错；
//   - 自引用不得出现在任何子查询内（FROM 派生表、JOIN 右端派生表、IN 子查询）；
//   - 自引用不得作为递归项聚合函数（SUM/MAX/...）的参数。
//
// 说明：递归项输出列与 WHERE / ON 中引用递归 CTE 的列属于合法用法（MySQL 一致），
// 因此仅校验「表引用位置」与「聚合参数」两类非法用法。
func validateRecursiveTerm(term *SelectStmt, c *CTE) error {
	if term == nil {
		return &SQLError{Msg: "recursive CTE " + c.Name + " requires a recursive term"}
	}
	// 先定位「自引用出现在子查询内 / 聚合参数内」这类更具体的违规，
	// 再判断 FROM 位置的缺失与重复，保证报错最贴近用户实际写错的地方。
	sc := &cteRefScanner{name: c.Name, derived: cteDerivedColumns(c)}
	sc.scanSelect(term)
	if len(sc.subHits) > 0 {
		return &SQLError{Msg: "recursive CTE " + c.Name + " is not allowed inside a subquery of the recursive term"}
	}
	if len(sc.aggHits) > 0 {
		return &SQLError{Msg: "recursive CTE " + c.Name + " is not allowed as an aggregate function argument in the recursive term"}
	}
	fromRefs := 0
	if cteNameEq(term.From, c.Name) && term.Subquery == nil {
		fromRefs++
	}
	for i := range term.Joins {
		if term.Joins[i].Subquery == nil && cteNameEq(term.Joins[i].Table, c.Name) {
			fromRefs++
		}
	}
	if fromRefs == 0 {
		return &SQLError{Msg: "recursive CTE " + c.Name + " must be referenced in the FROM clause of the recursive term"}
	}
	if fromRefs > 1 {
		return &SQLError{Msg: "recursive CTE " + c.Name + " must not be referenced more than once in the recursive term"}
	}
	return nil
}

// cteDerivedColumns 收集递归 CTE 的可见列名（用于识别裸列名形式的自引用）。
func cteDerivedColumns(c *CTE) map[string]bool {
	out := make(map[string]bool, len(c.Columns))
	for _, nm := range c.Columns {
		out[strings.ToLower(stripRelColPrefix(nm))] = true
	}
	return out
}

// cteRefScanner 递归项非法自引用扫描器。
type cteRefScanner struct {
	name    string
	derived map[string]bool
	subHits []string
	aggHits []string
}

// isCTECol 判断限定名/裸列名是否指向递归 CTE 的列。
func (s *cteRefScanner) isCTECol(ref, col string) bool {
	if col == "" {
		return false
	}
	if cteNameEq(ref, s.name) {
		return true
	}
	return ref == "" && s.derived[strings.ToLower(col)]
}

// scanSelect 扫描递归项本体：子查询内的表引用 + 聚合参数。
func (s *cteRefScanner) scanSelect(st *SelectStmt) {
	if st == nil {
		return
	}
	// ① 聚合 / 窗口函数参数引用递归 CTE 列。
	for i := range st.Columns {
		col := &st.Columns[i]
		agg := col.Agg
		if agg == "" && col.Window != nil {
			agg = col.Raw
		}
		if agg == "" {
			continue
		}
		if s.isCTECol(col.Ref, col.Col) || s.exprHasCTECol(col.Expr) {
			s.aggHits = append(s.aggHits, agg)
		}
	}
	if st.Having != nil {
		for i := range st.Having.Conds {
			cd := &st.Having.Conds[i]
			if cd.LeftAgg != nil && cd.LeftAgg.Agg != "" && s.isCTECol(cd.LeftAgg.Ref, cd.LeftAgg.Col) {
				s.aggHits = append(s.aggHits, cd.LeftAgg.Agg)
			}
		}
	}
	// ② 子查询内的表引用（FROM 派生表 / JOIN 右端派生表 / IN 子查询）。
	s.checkSub(st.Subquery)
	for i := range st.Joins {
		s.checkSub(st.Joins[i].Subquery)
		for j := range st.Joins[i].On {
			s.checkSub(st.Joins[i].On[j].Sub)
		}
	}
	s.scanWhereSub(st.Where)
	s.scanWhereSub(st.Having)
}

// scanWhereSub 扫描条件组中的 IN 子查询。
func (s *cteRefScanner) scanWhereSub(w *Where) {
	if w == nil {
		return
	}
	for i := range w.Conds {
		s.checkSub(w.Conds[i].Sub)
	}
}

// checkSub 检查子查询体内是否出现递归 CTE 表引用，并递归深入更深层子查询。
func (s *cteRefScanner) checkSub(sub *SelectStmt) {
	if sub == nil {
		return
	}
	if cteNameEq(sub.From, s.name) && sub.Subquery == nil {
		s.subHits = append(s.subHits, sub.From)
	}
	for i := range sub.Joins {
		if sub.Joins[i].Subquery == nil && cteNameEq(sub.Joins[i].Table, s.name) {
			s.subHits = append(s.subHits, sub.Joins[i].Table)
		}
	}
	s.scanSelect(sub)
}

// exprHasCTECol 判断算术表达式叶子是否引用递归 CTE 列。
func (s *cteRefScanner) exprHasCTECol(ex *Expr) bool {
	if ex == nil {
		return false
	}
	if ex.Op == "" && ex.Val == nil && s.isCTECol(ex.Ref, ex.Col) {
		return true
	}
	// v1.19.0 D6：函数调用节点按实参判定递归 CTE 列引用。
	for _, a := range ex.Args {
		if s.exprHasCTECol(a) {
			return true
		}
	}
	return s.exprHasCTECol(ex.Left) || s.exprHasCTECol(ex.Right)
}

// cteNameEq 表名比较（大小写不敏感；空名不等于任何名字）。
func cteNameEq(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	return strings.EqualFold(a, b)
}

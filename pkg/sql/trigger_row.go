package sql

import (
	"strings"
)

// triggerRow 行级触发器的一行上下文（v1.8.0 FOR EACH ROW）。
// meta 为源表元数据（列序）；oldRow / newRow 为按 meta.Columns 对齐的行值，
// nil 表示该类行不存在（INSERT 无 OLD、DELETE 无 NEW、UPDATE 双引用）。
type triggerRow struct {
	meta   *TableMeta
	oldRow []Value // UPDATE/DELETE 的 OLD 行；INSERT 为 nil
	newRow []Value // INSERT/UPDATE 的 NEW 行；DELETE 为 nil
}

// refs 构建该行的 NEW./OLD. 引用解析上下文（值快照）。
func (tr *triggerRow) refs() *rowTRIGRefs {
	rc := &rowTRIGRefs{meta: tr.meta}
	if tr.newRow != nil {
		rc.newVals = tr.rowVals(tr.newRow)
	}
	if tr.oldRow != nil {
		rc.oldVals = tr.rowVals(tr.oldRow)
	}
	return rc
}

// rowVals 将按 meta.Columns 对齐的行值映射为 列名 → Value（缺列补 NULL）。
func (tr *triggerRow) rowVals(row []Value) map[string]Value {
	m := make(map[string]Value, len(tr.meta.Columns))
	for i, c := range tr.meta.Columns {
		if i < len(row) {
			m[c.Name] = row[i]
		} else {
			m[c.Name] = Value{Kind: "NULL"}
		}
	}
	return m
}

// rowTRIGRefs 行级触发器的 NEW./OLD. 引用解析上下文。
type rowTRIGRefs struct {
	meta    *TableMeta
	newVals map[string]Value // NEW.col → 值（INSERT/UPDATE 非 nil）
	oldVals map[string]Value // OLD.col → 值（UPDATE/DELETE 非 nil）
}

// lookup 解析 NEW.col / OLD.col；未定义列或不可用行（INSERT 用 OLD 等）返回 false。
func (c *rowTRIGRefs) lookup(ref string) (Value, bool) {
	dot := strings.IndexByte(ref, '.')
	if dot <= 0 || dot == len(ref)-1 {
		return Value{}, false
	}
	prefix, col := ref[:dot], ref[dot+1:]
	switch prefix {
	case "NEW":
		if c.newVals == nil {
			return Value{}, false
		}
		v, ok := c.newVals[col]
		return v, ok
	case "OLD":
		if c.oldVals == nil {
			return Value{}, false
		}
		v, ok := c.oldVals[col]
		return v, ok
	}
	return Value{}, false
}

// anyRowTrigger 判断源表是否存在任何行级触发器（含 BEFORE/AFTER）。
// 供 execInsert/execUpdate/execDelete 决定是否收集行快照——无行级触发器时
// 保持零额外开销（nil 切片），行快照仅在确有行级触发器时逐行累积。
func (e *Executor) anyRowTrigger(sess *Session, table string) bool {
	trs, err := e.loadTriggerCatalog(sess)
	if err != nil {
		return false
	}
	for _, t := range trs {
		if t.Table == table && t.RowLevel {
			return true
		}
	}
	return false
}

// anyTriggerOn 判断源表某事件是否存在任何触发器（BEFORE/AFTER 任一命中即
// true）。供 execInsert/execUpdate/execDelete 在 autocommit 下决策是否提升
// 会话事务（v1.8.0 触发器体与主 DML 同事务原子性）。
func (e *Executor) anyTriggerOn(sess *Session, table, event string) bool {
	trs, err := e.loadTriggerCatalog(sess)
	if err != nil {
		return false
	}
	for _, t := range trs {
		if t.Table == table && t.Event == event {
			return true
		}
	}
	return false
}

// replaceTrigRefs 将触发器体 AST 中的 TRIGREF 字面量（NEW.col / OLD.col）
// 物化为当前行的实际值（v1.8.0）。体支持 UPDATE / INSERT（含 INSERT...SELECT）
// 白名单：SET 表达式、WHERE 条件、VALUES 行值、CASE 分支均会替换。
// 引用未定义列或行不可用（INSERT 引用 OLD 等）时返回明确错误。
func (e *Executor) replaceTrigRefs(stmt Stmt, ctx *rowTRIGRefs) error {
	switch s := stmt.(type) {
	case *UpdateStmt:
		for i := range s.Sets {
			if s.Sets[i].Expr != nil {
				if err := e.replaceTrigRefExpr(s.Sets[i].Expr, ctx); err != nil {
					return err
				}
			} else if err := e.replaceTrigRefValue(&s.Sets[i].Val, ctx); err != nil {
				return err
			}
		}
		if s.Where != nil {
			return e.replaceTrigRefWhere(s.Where, ctx)
		}
	case *InsertStmt:
		for i := range s.Rows {
			for j := range s.Rows[i] {
				if err := e.replaceTrigRefValue(&s.Rows[i][j], ctx); err != nil {
					return err
				}
			}
		}
		if s.Select != nil {
			return e.replaceTrigRefSelect(s.Select, ctx)
		}
	}
	return nil
}

// replaceTrigRefWhere 遍历 WHERE 条件组。
func (e *Executor) replaceTrigRefWhere(w *Where, ctx *rowTRIGRefs) error {
	if w == nil {
		return nil
	}
	for i := range w.Conds {
		if err := e.replaceTrigRefCond(&w.Conds[i], ctx); err != nil {
			return err
		}
	}
	return nil
}

// replaceTrigRefCond 遍历单个条件（含 IN 列表、元组、左右表达式、CASE 左值）。
func (e *Executor) replaceTrigRefCond(c *Cond, ctx *rowTRIGRefs) error {
	if err := e.replaceTrigRefValue(&c.Val, ctx); err != nil {
		return err
	}
	if err := e.replaceTrigRefValue(&c.Val2, ctx); err != nil {
		return err
	}
	for i := range c.InList {
		if err := e.replaceTrigRefValue(&c.InList[i], ctx); err != nil {
			return err
		}
	}
	for i := range c.TupleVals {
		for j := range c.TupleVals[i] {
			if err := e.replaceTrigRefValue(&c.TupleVals[i][j], ctx); err != nil {
				return err
			}
		}
	}
	if c.LeftExpr != nil {
		if err := e.replaceTrigRefExpr(c.LeftExpr, ctx); err != nil {
			return err
		}
	}
	if c.RightExpr != nil {
		if err := e.replaceTrigRefExpr(c.RightExpr, ctx); err != nil {
			return err
		}
	}
	if c.LeftCase != nil {
		return e.replaceTrigRefCase(c.LeftCase, ctx)
	}
	return nil
}

// replaceTrigRefCase 遍历 CASE WHEN 分支（条件组 + THEN 值 + ELSE 值）。
func (e *Executor) replaceTrigRefCase(cc *CaseWhenClause, ctx *rowTRIGRefs) error {
	if cc == nil {
		return nil
	}
	for i := range cc.Branches {
		if err := e.replaceTrigRefWhere(&cc.Branches[i].Conds, ctx); err != nil {
			return err
		}
		if err := e.replaceTrigRefValue(&cc.Branches[i].Then, ctx); err != nil {
			return err
		}
	}
	if cc.HasElse {
		return e.replaceTrigRefValue(&cc.Else, ctx)
	}
	return nil
}

// replaceTrigRefExpr 递归遍历算术表达式树（叶值替换）。
func (e *Executor) replaceTrigRefExpr(ex *Expr, ctx *rowTRIGRefs) error {
	if ex == nil {
		return nil
	}
	if ex.Op == "" {
		if ex.Val != nil {
			return e.replaceTrigRefValue(ex.Val, ctx)
		}
		return nil
	}
	if ex.Left != nil {
		if err := e.replaceTrigRefExpr(ex.Left, ctx); err != nil {
			return err
		}
	}
	return e.replaceTrigRefExpr(ex.Right, ctx)
}

// replaceTrigRefSelect 遍历 INSERT...SELECT 子查询（WHERE 条件）。
func (e *Executor) replaceTrigRefSelect(s *SelectStmt, ctx *rowTRIGRefs) error {
	if s == nil {
		return nil
	}
	return e.replaceTrigRefWhere(s.Where, ctx)
}

// replaceTrigRefValue 替换单个 Value：TRIGREF 物化为行值，其余原样保留。
func (e *Executor) replaceTrigRefValue(v *Value, ctx *rowTRIGRefs) error {
	if v == nil || v.Kind != "TRIGREF" {
		return nil
	}
	ref, ok := ctx.lookup(v.S)
	if !ok {
		return &SQLError{Msg: "unknown trigger row reference: " + v.S + " (未定义列，或 INSERT 仅 NEW、DELETE 仅 OLD 可用)"}
	}
	*v = ref
	return nil
}

// collectTRIGRefs 收集触发器体中的全部 NEW./OLD. 行引用（创建时校验用）。
// 遍历范围与 replaceTrigRefs 一致：UPDATE SET / WHERE、INSERT VALUES/子查询、
// CASE 分支。返回 "NEW.col" / "OLD.col" 形式列表。
func collectTRIGRefs(stmt Stmt) []string {
	var out []string
	collectTrigRefsStmt(stmt, &out)
	return out
}

func collectTrigRefsStmt(stmt Stmt, out *[]string) {
	switch s := stmt.(type) {
	case *UpdateStmt:
		for i := range s.Sets {
			if s.Sets[i].Expr != nil {
				collectTrigRefsExpr(s.Sets[i].Expr, out)
			} else {
				collectTrigRefsValue(&s.Sets[i].Val, out)
			}
		}
		collectTrigRefsWhere(s.Where, out)
	case *InsertStmt:
		for i := range s.Rows {
			for j := range s.Rows[i] {
				collectTrigRefsValue(&s.Rows[i][j], out)
			}
		}
		collectTrigRefsSelect(s.Select, out)
	}
}

func collectTrigRefsWhere(w *Where, out *[]string) {
	if w == nil {
		return
	}
	for i := range w.Conds {
		collectTrigRefsCond(&w.Conds[i], out)
	}
}

func collectTrigRefsCond(c *Cond, out *[]string) {
	collectTrigRefsValue(&c.Val, out)
	collectTrigRefsValue(&c.Val2, out)
	for i := range c.InList {
		collectTrigRefsValue(&c.InList[i], out)
	}
	for i := range c.TupleVals {
		for j := range c.TupleVals[i] {
			collectTrigRefsValue(&c.TupleVals[i][j], out)
		}
	}
	collectTrigRefsExpr(c.LeftExpr, out)
	collectTrigRefsExpr(c.RightExpr, out)
	collectTrigRefsCase(c.LeftCase, out)
}

func collectTrigRefsCase(cc *CaseWhenClause, out *[]string) {
	if cc == nil {
		return
	}
	for i := range cc.Branches {
		collectTrigRefsWhere(&cc.Branches[i].Conds, out)
		collectTrigRefsValue(&cc.Branches[i].Then, out)
	}
	if cc.HasElse {
		collectTrigRefsValue(&cc.Else, out)
	}
}

func collectTrigRefsExpr(ex *Expr, out *[]string) {
	if ex == nil {
		return
	}
	if ex.Op == "" {
		collectTrigRefsValue(ex.Val, out)
		return
	}
	collectTrigRefsExpr(ex.Left, out)
	collectTrigRefsExpr(ex.Right, out)
}

func collectTrigRefsSelect(s *SelectStmt, out *[]string) {
	if s == nil {
		return
	}
	collectTrigRefsWhere(s.Where, out)
}

func collectTrigRefsValue(v *Value, out *[]string) {
	if v != nil && v.Kind == "TRIGREF" {
		*out = append(*out, v.S)
	}
}

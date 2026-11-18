/*
---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_78c959d3e30d11f1913b525400393706
    ReservedCode1: nucB5a131X3JrmedPbHJSm4SFUQg6VPltCR6o7d+PJBnH9b7zqcPacu2QqkV3F1iVcAvduytT6x04jC5v3MYKUn8G28tZV75pMAK69t91q8DGc7l7hBBweQl+ynDEcMYk/1H8p8kQ8drpqYryCgUvEoBKOMAMGI2XKHM0jpeyKfc5COoXZcpIjSCNYY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_78c959d3e30d11f1913b525400393706
    ReservedCode2: nucB5a131X3JrmedPbHJSm4SFUQg6VPltCR6o7d+PJBnH9b7zqcPacu2QqkV3F1iVcAvduytT6x04jC5v3MYKUn8G28tZV75pMAK69t91q8DGc7l7hBBweQl+ynDEcMYk/1H8p8kQ8drpqYryCgUvEoBKOMAMGI2XKHM0jpeyKfc5COoXZcpIjSCNYY=
---
*/

package sql

import "strings"

// ---- v1.19.0 A1：可更新视图（本阶段不含 WITH CHECK OPTION）----
//
// 判定时机：CREATE VIEW 静态判定可更新性，结果与「视图列名 → 基表列名」
// 映射一并持久化在视图目录（m:views）的 ViewMeta 内；不满足条件时记录
// 不可更新原因，供写入报错使用。
//
// 写入时机：INSERT / UPDATE / DELETE 的目标名为视图时，按持久化映射把写
// 目标重写为基表（表名、列清单、SET 列、WHERE 列名），随后复用既有 DML
// 执行路径——触发器、外键、索引维护、约束校验、物化视图增量跟踪全部复用，
// 权限校验也因重写后的表名为基表而自动落到基表。
//
// 口径说明：本阶段不实现 WITH CHECK OPTION，视图定义中的 WHERE 不参与写
// 过滤（写操作只做列映射重写）；INSERT 未暴露的基表列走默认值 / AUTO_INCREMENT。

// viewWritePlan 可更新视图的写入重写计划（由 ViewMeta 持久化字段投影而来）。
type viewWritePlan struct {
	Base   string            // 基表名
	ColMap map[string]string // 视图列名 → 基表列名
	Order  []string          // 视图列顺序（元素为视图列名）
}

// viewColumnName 视图列名口径：AS 别名优先，其次裸列名，最后输出列名。
// 别名优先使 SELECT id AS uid 的视图列名为 uid，与 MySQL 语义一致。
func viewColumnName(c SelectColumn) string {
	if c.Alias != "" {
		return c.Alias
	}
	if c.Col != "" {
		return c.Col
	}
	return c.Name
}

// buildViewUpdatable 判定视图定义是否可更新：可更新返回重写计划与空原因，
// 不可更新返回 nil 计划与不可更新原因（写入时拼进 view <name> is not
// updatable: <reason> 口径）。
//
// 可更新条件：单表 FROM、无 CTE / DISTINCT / JOIN / FROM 子查询 / GROUP BY /
// HAVING / 聚合 / 窗口函数 / LIMIT / OFFSET，且投影列为可逆裸列引用
// （允许 t.col 与 AS 别名）；SELECT * 按基表列序展开为恒等映射。
func buildViewUpdatable(tabs []*TableMeta, views []*ViewMeta, sel *SelectStmt) (*viewWritePlan, string) {
	if sel == nil {
		return nil, "definition is empty"
	}
	switch {
	case len(sel.CTEs) > 0:
		return nil, "CTE in definition"
	case sel.Distinct:
		return nil, "DISTINCT in definition"
	case len(sel.Joins) > 0:
		return nil, "JOIN in definition"
	case sel.Subquery != nil:
		return nil, "subquery in FROM clause"
	case len(sel.GroupBy) > 0:
		return nil, "GROUP BY in definition"
	case sel.Having != nil:
		return nil, "HAVING in definition"
	case sel.Limit > 0:
		return nil, "LIMIT in definition"
	case sel.Offset > 0:
		return nil, "OFFSET in definition"
	case sel.From == "":
		return nil, "no base table in definition"
	}
	// 基表必须是真实表：视图套视图（多层可更新视图）本阶段不支持。
	meta := findTable(tabs, sel.From)
	if meta == nil {
		if findView(views, sel.From) != nil {
			return nil, "base object is a view: " + sel.From
		}
		return nil, "base table not found: " + sel.From
	}
	for _, c := range sel.Columns {
		if c.Agg != "" {
			return nil, "aggregate function in definition"
		}
		if c.Window != nil {
			return nil, "window function in definition"
		}
		if c.Distinct {
			return nil, "DISTINCT in definition"
		}
	}
	plan := &viewWritePlan{Base: sel.From, ColMap: make(map[string]string, len(sel.Columns)*2)}
	if len(sel.Columns) == 1 && sel.Columns[0].Col == "" && sel.Columns[0].Raw == "*" {
		// SELECT *：按基表列序展开，视图列名即基表列名。
		for i := range meta.Columns {
			name := meta.Columns[i].Name
			plan.ColMap[name] = name
			plan.Order = append(plan.Order, name)
		}
		return plan, ""
	}
	for _, c := range sel.Columns {
		if c.Expr != nil || c.Case != nil || c.Var != "" || c.Func != "" {
			return nil, "expression column in definition: " + c.Raw
		}
		if c.Col == "" || c.Col == "*" {
			return nil, "wildcard column in definition: " + c.Raw
		}
		if colIndex(meta, c.Col) < 0 {
			return nil, "unknown base column: " + c.Col
		}
		name := viewColumnName(c)
		if name == "" {
			return nil, "unnamed column in definition"
		}
		if _, dup := plan.ColMap[name]; dup {
			return nil, "duplicate view column: " + name
		}
		plan.ColMap[name] = c.Col
		plan.Order = append(plan.Order, name)
		// 同时登记输出列名（t.col 形式）：用户按视图输出列名写入时也能命中。
		if c.Name != "" && c.Name != name {
			if _, dup := plan.ColMap[c.Name]; !dup {
				plan.ColMap[c.Name] = c.Col
			}
		}
	}
	if len(plan.Order) == 0 {
		return nil, "no columns in definition"
	}
	return plan, ""
}

// viewWriteMark 一条 DML 语句的视图写重写结论（v1.19.0 A1 幂等标记）。
// done 恒为 true：err == nil 表示语句已重写为基表写操作（或本就不是视图），
// err != nil 表示首次的可更新性 / 列映射报错。顶层 ExecuteTextAsSession、
// 下沉层 execStmt（过程体 / 动态 SQL）会对同一语句对象再次调用，此时复用首次
// 结论，避免重复读视图目录、重复映射与重复报错；失败结论同样保留，防止目标名
// 已改成基表的语句二次进入时绕过可更新性校验。过程体语句与 EXECUTE 文本每次
// 执行都会重新解析，故标记不会跨 CALL / EXECUTE 复用到不同语句对象上。
type viewWriteMark struct {
	done bool
	err  error
}

// viewWriteMarkOf 取 DML 语句的重写标记（未处理过或非 DML 返回 nil）。
func viewWriteMarkOf(stmt Stmt) *viewWriteMark {
	switch s := stmt.(type) {
	case *InsertStmt:
		return s.viewWrite
	case *UpdateStmt:
		return s.viewWrite
	case *DeleteStmt:
		return s.viewWrite
	}
	return nil
}

// setViewWriteMark 记录 DML 语句的重写结论（非 DML 忽略）。
func setViewWriteMark(stmt Stmt, err error) {
	m := &viewWriteMark{done: true, err: err}
	switch s := stmt.(type) {
	case *InsertStmt:
		s.viewWrite = m
	case *UpdateStmt:
		s.viewWrite = m
	case *DeleteStmt:
		s.viewWrite = m
	}
}

// rewriteUpdatableViewDML 在语句执行前把目标为视图的 DML 重写为基表写操作。
// 返回 nil 表示无需重写或重写完成；返回错误表示该语句待报错（不可更新视图 /
// 视图列不存在 / 视图目录损坏）。调用方应先做权限校验再抛该错误，保证无权限
// 用户得到既有的 permission denied 口径。
//
// 幂等：同一语句对象只真正重写一次。顶层写路径先重写、再权限校验，随后
// execStmt 的下沉层会再次调用本函数，此时直接用首次结论返回——已重写语句
// 不会二次映射，首次失败的语句也不会因目标名已变成基表而误报为可写。
func (e *Executor) rewriteUpdatableViewDML(sess *Session, stmt Stmt) error {
	if m := viewWriteMarkOf(stmt); m != nil && m.done {
		return m.err
	}
	err := e.rewriteUpdatableViewDMLOnce(sess, stmt)
	setViewWriteMark(stmt, err)
	return err
}

// rewriteUpdatableViewDMLOnce 执行一次视图写重写（幂等性由调用方保证）。
func (e *Executor) rewriteUpdatableViewDMLOnce(sess *Session, stmt Stmt) error {
	var name string
	switch s := stmt.(type) {
	case *InsertStmt:
		name = s.Table
	case *UpdateStmt:
		name = s.Table
	case *DeleteStmt:
		name = s.Table
	default:
		return nil
	}
	if name == "" || strings.Contains(name, ".") {
		// 带库前缀的表名沿用既有 DB 解析路径，不按视图重写。
		return nil
	}
	views, err := e.viewCatalogForWrite(sess)
	if err != nil {
		return err
	}
	v := findView(views, name)
	if v == nil {
		return nil
	}
	if !v.Updatable || v.BaseTable == "" || len(v.ColumnMap) == 0 {
		reason := v.NotUpdatableReason
		if reason == "" {
			reason = "metadata missing (view created by an older version)"
		}
		// 权限口径与可更新视图一致：能确定单表基表时先把目标落到基表名，
		// 让非 root 用户得到基表上的权限判定，而不是「视图名不是授权对象」
		// 的误导性拒绝；报错仍以视图名为准。
		if base := viewBaseTableName(v.Select); base != "" {
			setDMLTarget(stmt, base)
		}
		return &SQLError{Msg: "view " + name + " is not updatable: " + reason}
	}
	plan := &viewWritePlan{Base: v.BaseTable, ColMap: v.ColumnMap, Order: v.ColumnOrder}
	switch s := stmt.(type) {
	case *InsertStmt:
		return rewriteInsertToView(s, name, plan)
	case *UpdateStmt:
		return rewriteUpdateToView(s, name, plan)
	case *DeleteStmt:
		return rewriteDeleteToView(s, name, plan)
	}
	return nil
}

// viewCatalogForWrite 读取视图目录：显式事务 / 内部透传事务内复用当前事务，
// 否则开一次临时只读事务后丢弃（autocommit 写路径）。
func (e *Executor) viewCatalogForWrite(sess *Session) ([]*ViewMeta, error) {
	if sess != nil {
		sess.mu.Lock()
		tx := sess.curTx
		if sess.forcedTx != nil {
			tx = sess.forcedTx
		}
		sess.mu.Unlock()
		if tx != nil {
			return loadViews(tx.Get)
		}
	}
	if e.tm == nil {
		return nil, nil
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return loadViews(tx.Get)
}

// rewriteInsertToView 重写 INSERT：目标表改基表，列清单按映射换成基表列；
// 未给出列清单时按视图列顺序映射，基表未暴露列由 buildRow 走默认值。
func rewriteInsertToView(s *InsertStmt, viewName string, plan *viewWritePlan) error {
	cols := make([]string, 0, len(s.Columns)+len(plan.Order))
	if len(s.Columns) == 0 {
		for _, vc := range plan.Order {
			cols = append(cols, plan.ColMap[vc])
		}
	} else {
		for _, c := range s.Columns {
			bc, ok := plan.ColMap[c]
			if !ok {
				return &SQLError{Msg: "view " + viewName + " has no column: " + c}
			}
			cols = append(cols, bc)
		}
	}
	s.Columns = cols
	// ON DUPLICATE KEY UPDATE / REPLACE 的赋值项同样以视图列名书写，
	// 一并映射到基表列，避免落到基表同名列或报 unknown column。
	if s.OnDup != nil {
		for i := range s.OnDup.Sets {
			bc, ok := plan.ColMap[s.OnDup.Sets[i].Col]
			if !ok {
				return &SQLError{Msg: "view " + viewName + " has no column: " + s.OnDup.Sets[i].Col}
			}
			s.OnDup.Sets[i].Col = bc
			rewriteViewExpr(s.OnDup.Sets[i].Expr, plan)
		}
	}
	setDMLTarget(s, plan.Base)
	return nil
}

// rewriteUpdateToView 重写 UPDATE：SET 列必须落在视图列集合内并映射为基表列，
// SET 表达式与 WHERE 中的视图列名同样映射为基表列名。
func rewriteUpdateToView(s *UpdateStmt, viewName string, plan *viewWritePlan) error {
	for i := range s.Sets {
		bc, ok := plan.ColMap[s.Sets[i].Col]
		if !ok {
			return &SQLError{Msg: "view " + viewName + " has no column: " + s.Sets[i].Col}
		}
		s.Sets[i].Col = bc
		if s.Sets[i].Expr != nil {
			rewriteViewExpr(s.Sets[i].Expr, plan)
		}
	}
	if err := rewriteViewWhere(s.Where, viewName, plan); err != nil {
		return err
	}
	setDMLTarget(s, plan.Base)
	return nil
}

// rewriteDeleteToView 重写 DELETE：目标表改基表，WHERE 视图列名映射为基表列名。
func rewriteDeleteToView(s *DeleteStmt, viewName string, plan *viewWritePlan) error {
	if err := rewriteViewWhere(s.Where, viewName, plan); err != nil {
		return err
	}
	setDMLTarget(s, plan.Base)
	return nil
}

// viewBaseTableName 取视图定义的单表基表名：定义含 JOIN / FROM 子查询或
// 无 FROM 时返回空串（此时无法把写目标落到确定基表）。
func viewBaseTableName(sel *SelectStmt) string {
	if sel == nil || sel.From == "" || len(sel.Joins) > 0 || sel.Subquery != nil {
		return ""
	}
	return sel.From
}

// setDMLTarget 把 DML 语句的写目标表名改为 base。
func setDMLTarget(stmt Stmt, base string) {
	switch s := stmt.(type) {
	case *InsertStmt:
		s.Table = base
	case *UpdateStmt:
		s.Table = base
	case *DeleteStmt:
		s.Table = base
	}
}

// rewriteViewWhere 重写 WHERE 条件中的列引用（裸列 / 限定列 / 行值 IN 左列 /
// 左右算术表达式）。视图定义之外的列名一律报错，避免误写基表其它列。
func rewriteViewWhere(w *Where, viewName string, plan *viewWritePlan) error {
	if w == nil {
		return nil
	}
	for i := range w.Conds {
		c := &w.Conds[i]
		if c.Col != "" {
			bc, ok := plan.ColMap[c.Col]
			if !ok {
				return &SQLError{Msg: "view " + viewName + " has no column: " + c.Col}
			}
			c.Col = bc
			c.Ref = "" // 重写后按基表列解释，清空视图/基表限定名
		}
		if c.RightRef != "" {
			// 列对列比较的右值列引用（WHERE a = b）：与左侧同口径按视图列
			br, ok := plan.ColMap[c.RightRef]
			if !ok {
				return &SQLError{Msg: "view " + viewName + " has no column: " + c.RightRef}
			}
			c.RightRef = br
		}
		for j := range c.TupleLeft {
			bc, ok := plan.ColMap[c.TupleLeft[j]]
			if !ok {
				return &SQLError{Msg: "view " + viewName + " has no column: " + c.TupleLeft[j]}
			}
			c.TupleLeft[j] = bc
		}
		if c.LeftExpr != nil {
			rewriteViewExpr(c.LeftExpr, plan)
		}
		if c.RightExpr != nil {
			rewriteViewExpr(c.RightExpr, plan)
		}
	}
	return nil
}

// rewriteViewExpr 递归重写算术表达式中叶子列引用的列名（Fn 节点递归实参，
// 与 v1.19.0 D6 的 Expr 结构保持一致）。
func rewriteViewExpr(e *Expr, plan *viewWritePlan) {
	if e == nil {
		return
	}
	if e.Fn != "" {
		for _, a := range e.Args {
			rewriteViewExpr(a, plan)
		}
		return
	}
	if e.Left == nil && e.Right == nil && e.Val == nil && e.Col != "" {
		if bc, ok := plan.ColMap[e.Col]; ok {
			e.Col = bc
			e.Ref = ""
		}
		return
	}
	rewriteViewExpr(e.Left, plan)
	rewriteViewExpr(e.Right, plan)
}

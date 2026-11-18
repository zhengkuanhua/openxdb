// Copyright (c) 2024-2026 OpenXDB Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package sql

import (
	"strings"
)

// v1.19.0 D3：触发器体复合块执行、BEFORE 行回写与执行顺序支持。
//
// 设计约束（零回归优先）：
//   - 体首个 token 不是 BEGIN 时，仍走既有单语句路径（Parse + NEW./OLD. 物化 +
//     execStmt），行为与 v1.8.0 完全一致；
//   - 体为 BEGIN…END 时复用 v1.19.0 已落地的过程体解释器（parseProcedureBody +
//     procRunner），从而自动获得块作用域、DECLARE、IF/LOOP/WHILE/REPEAT/CASE、
//     HANDLER/SIGNAL/RESIGNAL、CALL 与 PREPARE 动态 SQL 能力；
//   - 行上下文（NEW./OLD.）通过 procRunner.rowRefs 注入：表达式求值走
//     "new.<col>"/"old.<col>" 环境键，体语句执行前由 replaceTrigRefs 物化。

// triggerBodyIsBlock 判断触发器体是否为 BEGIN…END 复合块。
func triggerBodyIsBlock(body string) bool {
	toks, err := lex(body)
	if err != nil || len(toks) == 0 {
		return false
	}
	t := toks[0]
	if t.kind != tokIdent && t.kind != tokKeyword {
		return false
	}
	return strings.EqualFold(t.text, "BEGIN")
}

// triggerRowEnv 由行上下文构建过程解释器的表达式环境（键为小写 "new.col"/"old.col"）。
// refs 为 nil（语句级触发器）时返回空环境。
func triggerRowEnv(refs *rowTRIGRefs) map[string]Value {
	env := make(map[string]Value)
	if refs == nil {
		return env
	}
	for col, v := range refs.newVals {
		env[strings.ToLower("new."+col)] = v
	}
	for col, v := range refs.oldVals {
		env[strings.ToLower("old."+col)] = v
	}
	return env
}

// execTriggerBody 执行单个触发器体（语句级 refs 为 nil，行级为当行上下文）。
// 错误文案保持既有格式（"trigger <name>: invalid body: ..." / "trigger <name> failed: ..."）。
func (e *Executor) execTriggerBody(sess *Session, t *TriggerMeta, refs *rowTRIGRefs) error {
	if triggerBodyIsBlock(t.Body) {
		stmts, err := parseProcedureBody(t.Body)
		if err != nil {
			return &SQLError{Msg: "trigger " + t.Name + ": invalid body: " + err.Error()}
		}
		r := &procRunner{
			e:        e,
			sess:     sess,
			env:      triggerRowEnv(refs),
			cursors:  make(map[string]*procCursorState),
			prepared: make(map[string]string),
			rowRefs:  refs,
		}
		if _, err := r.run(stmts); err != nil {
			return &SQLError{Msg: "trigger " + t.Name + " failed: " + err.Error()}
		}
		return nil
	}
	stmt, err := Parse(t.Body)
	if err != nil {
		return &SQLError{Msg: "trigger " + t.Name + ": invalid body: " + err.Error()}
	}
	if refs != nil {
		if err := e.replaceTrigRefs(stmt, refs); err != nil {
			return &SQLError{Msg: "trigger " + t.Name + ": " + err.Error()}
		}
	}
	if _, err := e.execStmt(sess, stmt); err != nil {
		return &SQLError{Msg: "trigger " + t.Name + " failed: " + err.Error()}
	}
	return nil
}

// orderTriggers 按 FOLLOWS/PRECEDES 依赖排序（稳定拓扑排序：无依赖约束时保持
// 创建顺序，即目录切片顺序），返回执行顺序。
// 依赖未匹配同事件组内的触发器名时忽略该依赖（避免创建后改名/删表导致死锁）。
func orderTriggers(hit []*TriggerMeta) []*TriggerMeta {
	if len(hit) < 2 {
		return hit
	}
	byName := make(map[string]*TriggerMeta, len(hit))
	for _, t := range hit {
		byName[strings.ToLower(t.Name)] = t
	}
	// 建边：a 必须先于 b 时 edges[a] 追加 b；入度记在 indeg[b]。
	edges := make(map[string][]string, len(hit))
	indeg := make(map[string]int, len(hit))
	for _, t := range hit {
		indeg[strings.ToLower(t.Name)] += 0
	}
	for _, t := range hit {
		self := strings.ToLower(t.Name)
		if dep := strings.ToLower(t.Follows); dep != "" {
			if _, ok := byName[dep]; ok && dep != self {
				edges[dep] = append(edges[dep], self)
				indeg[self]++
			}
		}
		if dep := strings.ToLower(t.Precedes); dep != "" {
			if _, ok := byName[dep]; ok && dep != self {
				edges[self] = append(edges[self], dep)
				indeg[dep]++
			}
		}
	}
	// Kahn 拓扑排序：每轮取入度为 0 且创建顺序最靠前者（稳定）。
	out := make([]*TriggerMeta, 0, len(hit))
	done := make(map[string]bool, len(hit))
	for len(out) < len(hit) {
		pick := -1
		for i, t := range hit {
			key := strings.ToLower(t.Name)
			if done[key] || indeg[key] != 0 {
				continue
			}
			pick = i
			break
		}
		if pick < 0 {
			// 存在环：剩余触发器按创建顺序追加（兜底，不报错）。
			for _, t := range hit {
				if !done[strings.ToLower(t.Name)] {
					out = append(out, t)
					done[strings.ToLower(t.Name)] = true
				}
			}
			break
		}
		t := hit[pick]
		key := strings.ToLower(t.Name)
		done[key] = true
		out = append(out, t)
		for _, next := range edges[key] {
			indeg[next]--
		}
	}
	return out
}

// triggerRowHook INSERT 路径的 BEFORE 行级触发上下文（nil 表示无 BEFORE 行级触发器，
// 保持既有零额外开销路径）。out 用于把回写后的最终行交给外层收集 NEW 行快照。
type triggerRowHook struct {
	triggers []*TriggerMeta
	out      *triggerRow
}

// trigHook INSERT 路径的行级 BEFORE 触发器钩子（nil 表示无，保持既有零开销）。
type trigHook struct {
	list    []*TriggerMeta // 已按执行顺序排序的 BEFORE INSERT 行级触发器
	applied bool           // 最近一行是否已执行触发器（供调用方取回写后行）
	last    []Value        // 最近一行的回写后行值（按 meta.Columns 对齐）
}

// beforeRowTriggers 返回指定 timing/event/table 的行级触发器（已按执行顺序排序）。
func (e *Executor) beforeRowTriggers(sess *Session, table, timing, event string) ([]*TriggerMeta, error) {
	trs, err := e.loadTriggerCatalog(sess)
	if err != nil {
		return nil, err
	}
	var hit []*TriggerMeta
	for _, t := range trs {
		if t.RowLevel && t.Timing == timing && t.Event == event && t.Table == table {
			hit = append(hit, t)
		}
	}
	if len(hit) == 0 {
		return nil, nil
	}
	return orderTriggers(hit), nil
}

// fireBeforeRowList 逐行执行 BEFORE 行级触发器，并把 NEW 行回写应用到 newRow。
// 递归策略与 fireTriggers 完全一致（同键抑制 + 链式深度上限），共用 triggerStack。
func (e *Executor) fireBeforeRowList(sess *Session, trs []*TriggerMeta, meta *TableMeta, oldRow, newRow []Value) error {
	if len(trs) == 0 {
		return nil
	}
	timing, event := trs[0].Timing, trs[0].Event
	key := timing + "\x00" + event + "\x00" + trs[0].Table
	for _, k := range sess.triggerStack {
		if k == key {
			return nil // 同键递归抑制（与 fireTriggers 一致）
		}
	}
	if len(sess.triggerStack) > e.triggerMaxDepth {
		return nil // 链式深度超限抑制（与 fireTriggers 一致）
	}
	sess.triggerStack = append(sess.triggerStack, key)
	defer func() { sess.triggerStack = sess.triggerStack[:len(sess.triggerStack)-1] }()
	for _, t := range trs {
		rc := (&triggerRow{meta: meta, oldRow: oldRow, newRow: newRow}).refs()
		if err := e.execTriggerBody(sess, t, rc); err != nil {
			return err
		}
		// NEW.col 回写：块体 SET NEW.col 已在 refs.newVals 生效，此处同步回行切片。
		rc.applyToRow(meta, newRow)
	}
	return nil
}

// applyToRow 把 NEW 行上下文（可能被 SET NEW.col 修改）写回按 meta.Columns 对齐的行切片。
func (c *rowTRIGRefs) applyToRow(meta *TableMeta, row []Value) {
	if c == nil || meta == nil || len(row) == 0 {
		return
	}
	for i, col := range meta.Columns {
		if i >= len(row) {
			break
		}
		if v, ok := c.newVals[col.Name]; ok {
			row[i] = v
		}
	}
}

// setNewColumn 设置 NEW.<col> 的回写值（列名大小写不敏感，按 meta 实际列名落键）。
func (c *rowTRIGRefs) setNewColumn(meta *TableMeta, col string, v Value) error {
	if c.newVals == nil {
		return &SQLError{Msg: "NEW row is not available in this trigger: SET NEW." + col}
	}
	name, ok := triggerColumnName(meta, col)
	if !ok {
		return &SQLError{Msg: "unknown column in NEW row: NEW." + col}
	}
	c.newVals[name] = v
	return nil
}

// triggerColumnName 按列名（大小写不敏感）取表中实际列名。
func triggerColumnName(meta *TableMeta, col string) (string, bool) {
	if meta == nil {
		return "", false
	}
	for _, c := range meta.Columns {
		if strings.EqualFold(c.Name, col) {
			return c.Name, true
		}
	}
	return "", false
}

// hasEventRowTrigger 判断某表上是否存在指定 timing/event 的行级触发器。
// 供 DML 路径以语句级粒度预取（逐行触发时复用同一份列表）。
func (e *Executor) hasEventRowTrigger(sess *Session, table, timing, event string) bool {
	trs, err := e.loadTriggerCatalog(sess)
	if err != nil {
		return false
	}
	for _, t := range trs {
		if t.RowLevel && t.Timing == timing && t.Event == event && t.Table == table {
			return true
		}
	}
	return false
}

// collectTriggerBodyRowRefs 收集触发器体中的 NEW.<col>/OLD.<col> 引用（去重、
// 保持首次出现顺序）。单语句体沿用 AST 收集（collectTRIGRefs，行为不变）；
// 复合块体按词法扫描收集，供创建期做事件语义与列存在性校验。
func collectTriggerBodyRowRefs(body string, stmt Stmt) []string {
	if stmt != nil {
		return collectTRIGRefs(stmt)
	}
	toks, err := lex(body)
	if err != nil {
		return nil
	}
	seen := make(map[string]bool)
	var out []string
	for i := 0; i+2 < len(toks); i++ {
		t := toks[i]
		if t.kind != tokIdent && t.kind != tokKeyword {
			continue
		}
		if toks[i+1].kind != tokDot {
			continue
		}
		c := toks[i+2]
		if c.kind != tokIdent && c.kind != tokKeyword {
			continue
		}
		prefix := strings.ToUpper(t.text)
		if prefix != "NEW" && prefix != "OLD" {
			continue
		}
		ref := prefix + "." + c.text
		if seen[ref] {
			continue
		}
		seen[ref] = true
		out = append(out, ref)
	}
	return out
}

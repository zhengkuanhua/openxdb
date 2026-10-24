package sql

import (
	"encoding/json"
	"sort"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// TriggerMeta 触发器元数据：名称 + 时机 + 事件 + 源表 + 触发器体原文（M15-2）。
// 体以原始 SQL 文本持久化，DML 触发时重新 Parse + execStmt，与普通语句共用
// 同一执行路径，保证与物化视图 / 虚拟视图 / 表共存。
type TriggerMeta struct {
	Name     string `json:"name"`
	Timing   string `json:"timing"`              // "BEFORE" | "AFTER"
	Event    string `json:"event"`               // "INSERT" | "UPDATE" | "DELETE"
	Table    string `json:"table"`               // 源表
	Body     string `json:"body"`                // 触发器体原始 SQL 文本
	RowLevel bool   `json:"row_level,omitempty"` // v1.8.0：FOR EACH ROW 行级触发
}

// triggersKey 触发器目录键（独立于 m:tables / m:views / m:matviews）。
var triggersKey = []byte("m:triggers")

// loadTriggers 读取触发器目录。
func loadTriggers(get func(key []byte) ([]byte, error)) ([]*TriggerMeta, error) {
	raw, err := get(triggersKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return nil, nil
	}
	var trs []*TriggerMeta
	if err := json.Unmarshal(raw, &trs); err != nil {
		return nil, &SQLError{Msg: "corrupted trigger catalog"}
	}
	return trs, nil
}

// saveTriggers 序列化触发器目录。
func saveTriggers(trs []*TriggerMeta) ([]byte, error) {
	return json.Marshal(trs)
}

// findTrigger 按名查找触发器。
func findTrigger(trs []*TriggerMeta, name string) *TriggerMeta {
	for _, t := range trs {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// triggerStatement 触发器体支持的语句集合（创建时白名单校验）。
// 返回体语句涉及的目标表（UPDATE/INSERT 的写入表）。
func triggerBodyTable(stmt Stmt) string {
	switch s := stmt.(type) {
	case *UpdateStmt:
		return s.Table
	case *InsertStmt:
		return s.Table
	}
	return ""
}

// execCreateTrigger CREATE TRIGGER <name> BEFORE|AFTER INSERT|UPDATE|DELETE
// ON <table> FOR EACH ROW <body>（M15-2）。
// 创建时校验：名称唯一、时机/事件合法、源表存在、体可解析且为受支持语句
// （UPDATE 条件更新 / INSERT 单行插入）、体引用表存在；随后注册到
// m:triggers 持久化。整个创建在单个事务内完成。
func (e *Executor) execCreateTrigger(sess *Session, s *CreateTriggerStmt) (*Result, error) {
	if s.Name == "" {
		return nil, &SQLError{Msg: "trigger name required"}
	}
	if s.Timing != "BEFORE" && s.Timing != "AFTER" {
		return nil, &SQLError{Msg: "invalid trigger timing: " + s.Timing}
	}
	if s.Event != "INSERT" && s.Event != "UPDATE" && s.Event != "DELETE" {
		return nil, &SQLError{Msg: "invalid trigger event: " + s.Event}
	}
	if s.Body == "" {
		return nil, &SQLError{Msg: "trigger body required"}
	}
	bodyStmt, err := Parse(s.Body)
	if err != nil {
		return nil, &SQLError{Msg: "invalid trigger body: " + err.Error()}
	}
	bodyTable := triggerBodyTable(bodyStmt)
	if bodyTable == "" {
		return nil, &SQLError{Msg: "unsupported trigger body: only UPDATE and INSERT statements are supported"}
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	if findTable(tabs, s.Table) == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	if bodyTable != "" && findTable(tabs, bodyTable) == nil {
		return nil, &SQLError{Msg: "table not exists: " + bodyTable}
	}
	// 轻量预校验：UPDATE 体引用的 SET 列必须存在于体表中（执行期仍会复检）。
	if u, ok := bodyStmt.(*UpdateStmt); ok {
		bm := findTable(tabs, bodyTable)
		for _, set := range u.Sets {
			if colIndex(bm, set.Col) < 0 {
				return nil, &SQLError{Msg: "unknown column: " + set.Col}
			}
		}
	}
	// v1.8.0 行级触发器：NEW./OLD. 引用的事件语义与列存在性校验。
	// INSERT 仅 NEW、DELETE 仅 OLD、UPDATE 双引用；未定义列明确报错。
	if s.RowLevel {
		src := findTable(tabs, s.Table)
		for _, r := range collectTRIGRefs(bodyStmt) {
			if len(r) < 5 {
				return nil, &SQLError{Msg: "invalid trigger row reference: " + r}
			}
			prefix, col := r[:4], r[4:]
			if prefix == "OLD." && s.Event == "INSERT" {
				return nil, &SQLError{Msg: "INSERT row trigger cannot reference OLD row: " + r}
			}
			if prefix == "NEW." && s.Event == "DELETE" {
				return nil, &SQLError{Msg: "DELETE row trigger cannot reference NEW row: " + r}
			}
			if colIndex(src, col) < 0 {
				return nil, &SQLError{Msg: "unknown column in " + r + " (table " + s.Table + ")"}
			}
		}
	}
	trs, err := loadTriggers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findTrigger(trs, s.Name) != nil {
		// T41：CREATE TRIGGER IF NOT EXISTS——触发器已存在时静默跳过
		if s.IfNotExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "trigger already exists: " + s.Name}
	}
	trs = append(trs, &TriggerMeta{
		Name:     s.Name,
		Timing:   s.Timing,
		Event:    s.Event,
		Table:    s.Table,
		Body:     s.Body,
		RowLevel: s.RowLevel,
	})
	raw, err := saveTriggers(trs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(triggersKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 1}, nil
}

// execDropTrigger DROP TRIGGER [IF EXISTS] <name>（M15-2）。
// 仅移除元数据注册，不动任何表数据。
func (e *Executor) execDropTrigger(sess *Session, s *DropTriggerStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	trs, err := loadTriggers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findTrigger(trs, s.Name) == nil {
		if s.IfExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "trigger not exists: " + s.Name}
	}
	var out []*TriggerMeta
	for _, t := range trs {
		if t.Name == s.Name {
			continue
		}
		out = append(out, t)
	}
	raw, err := saveTriggers(out)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(triggersKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 1}, nil
}

// execShowTriggers SHOW TRIGGERS [FROM <table>]（M15-2）。
func (e *Executor) execShowTriggers(sess *Session, s *ShowTriggersStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	trs, err := loadTriggers(tx.Get)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []string{"trigger", "event", "table", "timing", "statement"}}
	sort.Slice(trs, func(i, j int) bool { return trs[i].Name < trs[j].Name })
	for _, t := range trs {
		if s.Table != "" && t.Table != s.Table {
			continue
		}
		res.Rows = append(res.Rows, []Value{
			StrVal(t.Name),
			StrVal(t.Event),
			StrVal(t.Table),
			StrVal(t.Timing),
			StrVal(t.Body),
		})
	}
	return res, nil
}

// loadTriggerCatalog 读取当前可见的触发器目录。
// 读取策略（避免 DML 高频路径的事务与 WAL 开销）：
//   - 显式会话事务内：复用会话事务读（getTx），保证 read-your-writes
//     （本事务未提交的 CREATE/DROP TRIGGER 立即可见），与旧行为一致；
//   - autocommit：优先引擎直读 e.st.Get（无事务、无 WAL、无 fsync，
//     始终读到最新已提交目录——DDL 未提交时不可见，等价于快照隔离下
//     的 loadTriggers(tx.Get)）；e.st 为空（纯测试装配）时退回临时只读
//     事务读（空批次 Commit 免 WAL/fsync，不产生 Rollback 审计记录）。
func (e *Executor) loadTriggerCatalog(sess *Session) ([]*TriggerMeta, error) {
	if !e.autocommit(sess) {
		tx, err := e.getTx(sess)
		if err != nil {
			return nil, err
		}
		return loadTriggers(tx.Get)
	}
	if e.st != nil {
		raw, err := e.st.Get(triggersKey)
		if err == storage.ErrNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			return nil, nil
		}
		var trs []*TriggerMeta
		if err := json.Unmarshal(raw, &trs); err != nil {
			return nil, &SQLError{Msg: "corrupted trigger catalog"}
		}
		return trs, nil
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	trs, err := loadTriggers(tx.Get)
	if err != nil {
		return nil, err
	}
	// 只读临时事务：空批次 Commit 直接结束（无 WAL / 无 fsync）。
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return trs, nil
}

// fireTriggers 在源表 DML 主事务提交前执行匹配的触发器体（v1.8.0：BEFORE 与
// AFTER 均与主 DML 同事务原子性，体写入随主 DML 一并提交或回滚）。
// 匹配规则：时机 + 事件 + 源表名全部一致；触发粒度：
//   - 语句级（默认 / FOR EACH STATEMENT，RowLevel=false）：受影响行数>0 时触发一次；
//   - 行级（FOR EACH ROW，RowLevel=true）：按 rows 逐行触发，每行携带 NEW./OLD.
//     行上下文（INSERT 仅 NEW、DELETE 仅 OLD、UPDATE 双引用）。
//
// 触发器体经完整 Parse + 行上下文物化（NEW./OLD. → 字面量）后 execStmt 执行，
// 与普通语句共用执行路径，并复用调用方传入的同一事务 tx（2PC/autocommit
// 主路径）。递归策略（v1.8.0）：
//   - 同键递归栈（timing\x00event\x00table）：本调用链已触发过同键触发器即整批
//     抑制（防直接递归 / 双向互触发死循环）；
//   - 跨键链式深度上限：len(triggerStack) > triggerMaxDepth（默认 1，即允许
//     最外层 DML + 1 层嵌套触发器链式执行；第 2 层嵌套起抑制）时整批抑制；
//     上限可配置化放宽，保留链式触发能力。
func (e *Executor) fireTriggers(sess *Session, tx txn.Txn, timing, table, event string, rows []triggerRow) error {
	trs, err := e.loadTriggerCatalog(sess)
	if err != nil {
		return err
	}
	var hit []*TriggerMeta
	for _, t := range trs {
		if t.Timing == timing && t.Event == event && t.Table == table {
			hit = append(hit, t)
		}
	}
	if len(hit) == 0 {
		return nil
	}
	key := timing + "\x00" + event + "\x00" + table
	for _, k := range sess.triggerStack {
		if k == key {
			return nil // 同键递归抑制（直接递归 / 双向互触发）
		}
	}
	if len(sess.triggerStack) > e.triggerMaxDepth {
		return nil // 链式深度超限抑制（默认 1：允许 1 层嵌套链式触发）
	}
	sess.triggerStack = append(sess.triggerStack, key)
	defer func() { sess.triggerStack = sess.triggerStack[:len(sess.triggerStack)-1] }()
	for _, t := range hit {
		if !t.RowLevel {
			stmt, err := Parse(t.Body)
			if err != nil {
				return &SQLError{Msg: "trigger " + t.Name + ": invalid body: " + err.Error()}
			}
			if _, err := e.execStmt(sess, stmt); err != nil {
				return &SQLError{Msg: "trigger " + t.Name + " failed: " + err.Error()}
			}
			continue
		}
		// 行级：逐行物化 NEW./OLD. 引用后执行体（每行独立 Parse，体通常很小）。
		for _, tr := range rows {
			stmt, err := Parse(t.Body)
			if err != nil {
				return &SQLError{Msg: "trigger " + t.Name + ": invalid body: " + err.Error()}
			}
			if err := e.replaceTrigRefs(stmt, tr.refs()); err != nil {
				return &SQLError{Msg: "trigger " + t.Name + ": " + err.Error()}
			}
			if _, err := e.execStmt(sess, stmt); err != nil {
				return &SQLError{Msg: "trigger " + t.Name + " failed: " + err.Error()}
			}
		}
	}
	return nil
}

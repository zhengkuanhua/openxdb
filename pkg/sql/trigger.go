package sql

import (
	"encoding/json"
	"sort"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// TriggerMeta 触发器元数据：名称 + 时机 + 事件 + 源表 + 触发器体原文（M15-2）。
// 体以原始 SQL 文本持久化，DML 触发时重新 Parse + execStmt，与普通语句共用
// 同一执行路径，保证与物化视图 / 虚拟视图 / 表共存。
type TriggerMeta struct {
	Name   string `json:"name"`
	Timing string `json:"timing"` // "BEFORE" | "AFTER"
	Event  string `json:"event"`  // "INSERT" | "UPDATE" | "DELETE"
	Table  string `json:"table"`  // 源表
	Body   string `json:"body"`   // 触发器体原始 SQL 文本
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
func (e *Executor) execCreateTrigger(s *CreateTriggerStmt) (*Result, error) {
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
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
	trs, err := loadTriggers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findTrigger(trs, s.Name) != nil {
		return nil, &SQLError{Msg: "trigger already exists: " + s.Name}
	}
	trs = append(trs, &TriggerMeta{
		Name:   s.Name,
		Timing: s.Timing,
		Event:  s.Event,
		Table:  s.Table,
		Body:   s.Body,
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
func (e *Executor) execDropTrigger(s *DropTriggerStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
func (e *Executor) execShowTriggers(s *ShowTriggersStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	if e.autocommit() {
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
func (e *Executor) loadTriggerCatalog() ([]*TriggerMeta, error) {
	if !e.autocommit() {
		tx, err := e.getTx()
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
	tx, err := e.getTx()
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

// fireTriggers 在源表 DML 提交前（BEFORE）/提交后（AFTER）执行匹配的触发器体。
// 匹配规则：时机 + 事件 + 源表名全部一致；触发粒度为语句级（受影响行数>0 时
// 触发一次）。触发器体经完整 Parse + execStmt 执行，与普通语句共用执行路径。
// 原子性说明：触发器体在独立隐式事务中执行并提交（BEFORE 时序上先于主 DML
// 提交；主 DML 提交失败时已提交的体不会回滚——文档化限制，简单 SQL 体场景
// 可接受）。triggerDepth 递归防护：触发器体再次写同一表时不再触发，
// 避免无限递归（体对其它表的写入同样被抑制，文档化限制）。
func (e *Executor) fireTriggers(timing, table, event string) error {
	if e.triggerDepth > 0 {
		return nil
	}
	trs, err := e.loadTriggerCatalog()
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
	tx, err := e.getTx()
	if err != nil {
		return err
	}
	auto := e.autocommit()
	if auto {
		defer tx.Rollback()
	}
	e.triggerDepth++
	defer func() { e.triggerDepth-- }()
	for _, t := range hit {
		stmt, err := Parse(t.Body)
		if err != nil {
			return &SQLError{Msg: "trigger " + t.Name + ": invalid body: " + err.Error()}
		}
		if _, err := e.execStmt(stmt); err != nil {
			return &SQLError{Msg: "trigger " + t.Name + " failed: " + err.Error()}
		}
	}
	return nil
}

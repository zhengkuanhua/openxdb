package sql

import (
	"bytes"
	"sort"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// Executor SQL 执行器（开发手册 §4.2 算子最小集）。
// 依赖：TxnManager（本地 ACID + 快照隔离）；DDL/DML 均以单语句隐式事务提交。
type Executor struct {
	tm txn.TxnManager
}

func NewExecutor(tm txn.TxnManager) *Executor { return &Executor{tm: tm} }

// Execute 执行一条解析后的语句，返回结果集（SELECT）或影响行数（DML/DDL）。
func (e *Executor) Execute(stmt Stmt) (*Result, error) {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		return e.execCreateTable(s)
	case *DropTableStmt:
		return e.execDropTable(s)
	case *CreateIndexStmt:
		return e.execCreateIndex(s)
	case *DropIndexStmt:
		return e.execDropIndex(s)
	case *InsertStmt:
		return e.execInsert(s)
	case *SelectStmt:
		return e.execSelect(s)
	case *UpdateStmt:
		return e.execUpdate(s)
	case *DeleteStmt:
		return e.execDelete(s)
	}
	return nil, ErrUnsupported
}

// ---- DDL ----

func (e *Executor) execCreateTable(s *CreateTableStmt) (*Result, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	if findTable(tabs, s.Name) != nil {
		return nil, &SQLError{Msg: "table already exists: " + s.Name}
	}
	if s.PK == "" && len(s.Columns) > 0 {
		s.PK = s.Columns[0].Name
	}
	meta := &TableMeta{ID: nextTableID(tabs), Name: s.Name, Columns: s.Columns, PK: s.PK}
	tabs = append(tabs, meta)
	raw, err := saveTables(tabs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 0}, nil
}

func (e *Executor) execDropTable(s *DropTableStmt) (*Result, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Name)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Name}
	}
	// 删除表内所有行与索引键
	start, end := TableRange(meta.ID)
	pairs, err := tx.Scan(storage.KeyRange{Start: start, End: end}, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range pairs {
		if err := tx.Delete(kv.Key); err != nil {
			return nil, err
		}
	}
	if len(meta.Indexes) > 0 {
		istart, iend := IndexTableRange(meta.ID)
		ipairs, err := tx.Scan(storage.KeyRange{Start: istart, End: iend}, 0)
		if err != nil {
			return nil, err
		}
		for _, kv := range ipairs {
			if err := tx.Delete(kv.Key); err != nil {
				return nil, err
			}
		}
	}
	// 从目录移除
	var kept []*TableMeta
	for _, t := range tabs {
		if t.Name != s.Name {
			kept = append(kept, t)
		}
	}
	raw, err := saveTables(kept)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: len(pairs)}, nil
}

// execCreateIndex 建二级索引：元数据登记 + 对现有数据回填索引键。
func (e *Executor) execCreateIndex(s *CreateIndexStmt) (*Result, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	if findIndex(meta, s.Name) != nil {
		return nil, &SQLError{Msg: "index already exists: " + s.Name}
	}
	if colIndex(meta, s.Col) < 0 {
		return nil, &SQLError{Msg: "unknown column: " + s.Col}
	}
	idx := IndexMeta{ID: nextIndexID(meta), Name: s.Name, Col: s.Col}
	meta.Indexes = append(meta.Indexes, idx)
	// 回填：全表扫描逐行写索引键（meta.Indexes 已含新索引）
	start, end := TableRange(meta.ID)
	pairs, err := tx.Scan(storage.KeyRange{Start: start, End: end}, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range pairs {
		row, err := decodeRow(meta, kv.Value)
		if err != nil {
			return nil, err
		}
		if err := e.putIndexKeys(tx, meta, row, pkSuffixOf(meta, kv.Key)); err != nil {
			return nil, err
		}
	}
	raw, err := saveTables(tabs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: len(pairs)}, nil
}

// execDropIndex 删索引：元数据移除 + 删除全部索引键。
func (e *Executor) execDropIndex(s *DropIndexStmt) (*Result, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	idx := findIndex(meta, s.Name)
	if idx == nil {
		return nil, &SQLError{Msg: "index not exists: " + s.Name}
	}
	// 仅删该索引区间 [i{tid}{idxID}, i{tid}{idxID+1})
	istart := EncodeIndexKey2(meta.ID, idx.ID, nil, nil)
	iend := EncodeIndexKey2(meta.ID, idx.ID+1, nil, nil)
	pairs, err := tx.Scan(storage.KeyRange{Start: istart, End: iend}, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range pairs {
		if err := tx.Delete(kv.Key); err != nil {
			return nil, err
		}
	}
	var kept []IndexMeta
	for _, i := range meta.Indexes {
		if i.ID != idx.ID {
			kept = append(kept, i)
		}
	}
	meta.Indexes = kept
	raw, err := saveTables(tabs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: len(pairs)}, nil
}

// ---- DML ----

func (e *Executor) execInsert(s *InsertStmt) (*Result, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	affected := 0
	for _, row := range s.Rows {
		vals, err := e.buildRow(meta, s.Columns, row)
		if err != nil {
			return nil, err
		}
		pk, err := e.pkOf(meta, vals)
		if err != nil {
			return nil, err
		}
		// 主键唯一性约束（ACID-C）：已存在则冲突
		key := EncodeTableKey(meta.ID, pk)
		if _, err := tx.Get(key); err == nil {
			return nil, &SQLError{Msg: "duplicate primary key: " + s.Table}
		} else if err != storage.ErrNotFound {
			return nil, err
		}
		raw, err := encodeRow(meta, vals)
		if err != nil {
			return nil, err
		}
		if err := tx.Put(key, raw); err != nil {
			return nil, err
		}
		if err := e.putIndexKeys(tx, meta, vals, pk); err != nil {
			return nil, err
		}
		affected++
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: affected}, nil
}

func (e *Executor) execSelect(s *SelectStmt) (*Result, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.From)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.From}
	}
	var rows [][]Value
	ordered := false // 结果是否已按 ORDER BY 升序（索引序）
	// 点查优化：WHERE 仅含主键等值 → 直接 Get
	if s.Where != nil && len(s.Where.Conds) == 1 {
		c := s.Where.Conds[0]
		if c.Col == meta.PK && c.Op == "=" {
			pk, err := pkBytes(meta, c.Val)
			if err != nil {
				return nil, err
			}
			raw, err := tx.Get(EncodeTableKey(meta.ID, pk))
			if err == nil {
				row, err := decodeRow(meta, raw)
				if err != nil {
					return nil, err
				}
				rows = [][]Value{row}
			} else if err != storage.ErrNotFound {
				return nil, err
			}
			return e.finishSelect(meta, s, rows, ordered)
		}
	}
	// 二级索引优化：WHERE 命中索引列等值/范围
	if rows, ordered, err = e.indexLookup(tx, meta, s); err != nil {
		return nil, err
	}
	if rows == nil {
		// 全表扫描 + 过滤
		start, end := TableRange(meta.ID)
		pairs, err := tx.Scan(storage.KeyRange{Start: start, End: end}, 0)
		if err != nil {
			return nil, err
		}
		rows = make([][]Value, 0, len(pairs))
		for _, kv := range pairs {
			row, err := decodeRow(meta, kv.Value)
			if err != nil {
				return nil, err
			}
			ok, err := matchWhere(meta, row, s.Where)
			if err != nil {
				return nil, err
			}
			if ok {
				rows = append(rows, row)
			}
		}
	}
	return e.finishSelect(meta, s, rows, ordered)
}

// indexLookup 用二级索引满足 WHERE（等值/范围）与 ORDER BY。
// 返回 rows=nil 表示无可用索引条件，应回退全表扫描。
func (e *Executor) indexLookup(tx txn.Txn, meta *TableMeta, s *SelectStmt) ([][]Value, bool, error) {
	if s.Where == nil || len(meta.Indexes) == 0 {
		return nil, false, nil
	}
	// 取第一个命中索引列的条件（非 !=）
	var hit *Cond
	var idx *IndexMeta
	for i := range s.Where.Conds {
		c := &s.Where.Conds[i]
		if c.Op == "!=" {
			continue
		}
		for j := range meta.Indexes {
			if meta.Indexes[j].Col == c.Col {
				hit = c
				idx = &meta.Indexes[j]
				break
			}
		}
		if hit != nil {
			break
		}
	}
	if hit == nil {
		return nil, false, nil
	}
	val, err := idxBytes(hit.Val)
	if err != nil {
		return nil, false, err
	}
	// 构造扫描边界；非等值操作符要求值类型与列类型一致（比较语义）
	var start, end []byte
	prefix := func(v []byte) []byte { return EncodeIndexKey2(meta.ID, idx.ID, v, nil) }
	switch hit.Op {
	case "=":
		start = prefix(val)
		end = prefix(append(append([]byte{}, val...), 0xff))
	case ">=":
		start = prefix(val)
		end = EncodeIndexKey2(meta.ID+1, 0, nil, nil)
	case ">":
		start = prefix(append(append([]byte{}, val...), 0xff))
		end = EncodeIndexKey2(meta.ID+1, 0, nil, nil)
	case "<":
		start = EncodeIndexKey2(meta.ID, idx.ID, nil, nil)
		end = prefix(val)
	case "<=":
		start = EncodeIndexKey2(meta.ID, idx.ID, nil, nil)
		end = prefix(append(append([]byte{}, val...), 0xff))
	default:
		return nil, false, nil
	}
	pairs, err := tx.Scan(storage.KeyRange{Start: start, End: end}, 0)
	if err != nil {
		return nil, false, err
	}
	rows := make([][]Value, 0, len(pairs))
	for _, kv := range pairs {
		pk := indexPKSuffix(kv.Key)
		raw, err := tx.Get(EncodeTableKey(meta.ID, pk))
		if err != nil {
			if err == storage.ErrNotFound {
				continue // 索引脏键（理论上不出现）
			}
			return nil, false, err
		}
		row, err := decodeRow(meta, raw)
		if err != nil {
			return nil, false, err
		}
		ok, err := matchWhere(meta, row, s.Where)
		if err != nil {
			return nil, false, err
		}
		if ok {
			rows = append(rows, row)
		}
	}
	// 索引序 = 该索引列升序；ORDER BY 同列升序时无需再排序
	ordered := s.OrderBy != nil && s.OrderBy.Col == idx.Col && !s.OrderBy.Desc
	return rows, ordered, nil
}

// finishSelect 聚合 / 排序 / 限行 / 投影。
// ordered=true 表示 rows 已按 ORDER BY 列升序（来自索引序），跳过重复排序。
func (e *Executor) finishSelect(meta *TableMeta, s *SelectStmt, rows [][]Value, ordered bool) (*Result, error) {
	// 排序
	if s.OrderBy != nil && !ordered {
		ob := s.OrderBy
		sort.SliceStable(rows, func(i, j int) bool {
			a, _ := rowValue(meta, rows[i], ob.Col)
			b, _ := rowValue(meta, rows[j], ob.Col)
			cmp := compareVal(a, b)
			if ob.Desc {
				return cmp > 0
			}
			return cmp < 0
		})
	}
	// LIMIT
	if s.Limit > 0 && len(rows) > s.Limit {
		rows = rows[:s.Limit]
	}
	// 聚合
	hasAgg := false
	for _, c := range s.Columns {
		if c.Agg != "" {
			hasAgg = true
			break
		}
	}
	if hasAgg {
		return e.doAgg(meta, s, rows)
	}
	// 投影
	res := &Result{}
	for _, c := range s.Columns {
		if c.Name == "*" {
			for _, col := range meta.Columns {
				res.Columns = append(res.Columns, col.Name)
			}
		} else {
			res.Columns = append(res.Columns, c.Name)
		}
	}
	for _, row := range rows {
		var out []Value
		for _, c := range s.Columns {
			if c.Name == "*" {
				out = append(out, row...)
			} else {
				v, err := rowValue(meta, row, c.Name)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
		}
		res.Rows = append(res.Rows, out)
	}
	return res, nil
}

func (e *Executor) doAgg(meta *TableMeta, s *SelectStmt, rows [][]Value) (*Result, error) {
	res := &Result{}
	out := make([]Value, 0, len(s.Columns))
	for _, c := range s.Columns {
		res.Columns = append(res.Columns, c.Name)
		switch c.Agg {
		case "COUNT":
			out = append(out, IntVal(int64(len(rows))))
		case "SUM", "AVG":
			var sum int64
			for _, r := range rows {
				v, err := rowValue(meta, r, c.Col)
				if err != nil {
					return nil, err
				}
				if v.Kind != "INT" {
					return nil, &SQLError{Msg: "aggregate on non-int column: " + c.Col}
				}
				sum += v.I
			}
			if c.Agg == "SUM" {
				out = append(out, IntVal(sum))
			} else if len(rows) > 0 {
				out = append(out, IntVal(sum/int64(len(rows))))
			} else {
				out = append(out, IntVal(0))
			}
		}
	}
	res.Rows = append(res.Rows, out)
	return res, nil
}

func (e *Executor) execUpdate(s *UpdateStmt) (*Result, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	start, end := TableRange(meta.ID)
	pairs, err := tx.Scan(storage.KeyRange{Start: start, End: end}, 0)
	if err != nil {
		return nil, err
	}
	affected := 0
	for _, kv := range pairs {
		row, err := decodeRow(meta, kv.Value)
		if err != nil {
			return nil, err
		}
		ok, err := matchWhere(meta, row, s.Where)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		pk := pkSuffixOf(meta, kv.Key)
		// 维护索引：先删旧索引键
		if err := e.delIndexKeys(tx, meta, row, pk); err != nil {
			return nil, err
		}
		for _, set := range s.Sets {
			idx := colIndex(meta, set.Col)
			if idx < 0 {
				return nil, &SQLError{Msg: "unknown column: " + set.Col}
			}
			if meta.Columns[idx].Type == "INT" && set.Val.Kind != "INT" {
				return nil, &SQLError{Msg: "type mismatch for column: " + set.Col}
			}
			if meta.Columns[idx].Type == "TEXT" {
				set.Val.Kind = "TEXT"
				set.Val.S = set.Val.String()
				set.Val.I = 0
			}
			row[idx] = set.Val
		}
		raw, err := encodeRow(meta, row)
		if err != nil {
			return nil, err
		}
		if err := tx.Put(kv.Key, raw); err != nil {
			return nil, err
		}
		// 写新索引键
		if err := e.putIndexKeys(tx, meta, row, pk); err != nil {
			return nil, err
		}
		affected++
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: affected}, nil
}

func (e *Executor) execDelete(s *DeleteStmt) (*Result, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	start, end := TableRange(meta.ID)
	pairs, err := tx.Scan(storage.KeyRange{Start: start, End: end}, 0)
	if err != nil {
		return nil, err
	}
	affected := 0
	for _, kv := range pairs {
		row, err := decodeRow(meta, kv.Value)
		if err != nil {
			return nil, err
		}
		ok, err := matchWhere(meta, row, s.Where)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if err := e.delIndexKeys(tx, meta, row, pkSuffixOf(meta, kv.Key)); err != nil {
			return nil, err
		}
		if err := tx.Delete(kv.Key); err != nil {
			return nil, err
		}
		affected++
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: affected}, nil
}

// ---- 辅助 ----

func (e *Executor) buildRow(meta *TableMeta, cols []string, vals []Value) ([]Value, error) {
	if len(cols) == 0 {
		if len(vals) != len(meta.Columns) {
			return nil, &SQLError{Msg: "column count mismatch"}
		}
		out := make([]Value, len(vals))
		copy(out, vals)
		return e.coerceRow(meta, out)
	}
	if len(cols) != len(vals) {
		return nil, &SQLError{Msg: "column count mismatch"}
	}
	out := make([]Value, len(meta.Columns))
	filled := make([]bool, len(meta.Columns))
	for i, c := range cols {
		idx := colIndex(meta, c)
		if idx < 0 {
			return nil, &SQLError{Msg: "unknown column: " + c}
		}
		out[idx] = vals[i]
		filled[idx] = true
	}
	for i, f := range filled {
		if !f {
			return nil, &SQLError{Msg: "missing value for column: " + meta.Columns[i].Name}
		}
	}
	return e.coerceRow(meta, out)
}

// coerceRow 按列类型做值类型强制。
func (e *Executor) coerceRow(meta *TableMeta, vals []Value) ([]Value, error) {
	for i, c := range meta.Columns {
		v := &vals[i]
		if c.Type == "INT" {
			if v.Kind != "INT" {
				return nil, &SQLError{Msg: "type mismatch for column: " + c.Name}
			}
		} else {
			if v.Kind == "INT" {
				return nil, &SQLError{Msg: "type mismatch for column: " + c.Name}
			}
		}
	}
	return vals, nil
}

// pkOf 从行值提取主键字节（INT 定长 / TEXT 原字节）。
func (e *Executor) pkOf(meta *TableMeta, vals []Value) ([]byte, error) {
	for i, c := range meta.Columns {
		if c.Name == meta.PK {
			return pkBytes(meta, vals[i])
		}
	}
	return nil, &SQLError{Msg: "table has no primary key: " + meta.Name}
}

// pkSuffixOf 从行键 s{tableID:8B}{pk} 提取主键后缀。
func pkSuffixOf(meta *TableMeta, rowKey []byte) []byte {
	return rowKey[1+8:]
}

// putIndexKeys 为一行写入其所有索引键（值 = 存在标记）。
func (e *Executor) putIndexKeys(tx txn.Txn, meta *TableMeta, row []Value, pk []byte) error {
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		ci := colIndex(meta, idx.Col)
		val, err := idxBytes(row[ci])
		if err != nil {
			return err
		}
		key := EncodeIndexKey2(meta.ID, idx.ID, val, pk)
		if err := tx.Put(key, []byte{1}); err != nil {
			return err
		}
	}
	return nil
}

// delIndexKeys 删除一行在所有索引上的键。
func (e *Executor) delIndexKeys(tx txn.Txn, meta *TableMeta, row []Value, pk []byte) error {
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		ci := colIndex(meta, idx.Col)
		val, err := idxBytes(row[ci])
		if err != nil {
			return err
		}
		key := EncodeIndexKey2(meta.ID, idx.ID, val, pk)
		if err := tx.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// matchWhere 行级条件匹配（AND 组合；无 WHERE 恒真）。
func matchWhere(meta *TableMeta, row []Value, w *Where) (bool, error) {
	if w == nil || len(w.Conds) == 0 {
		return true, nil
	}
	for _, c := range w.Conds {
		v, err := rowValue(meta, row, c.Col)
		if err != nil {
			return false, err
		}
		if !matchCond(v, c.Op, c.Val) {
			return false, nil
		}
	}
	return true, nil
}

func matchCond(a Value, op string, b Value) bool {
	cmp := compareVal(a, b)
	switch op {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case "<":
		return cmp < 0
	case ">":
		return cmp > 0
	case "<=":
		return cmp <= 0
	case ">=":
		return cmp >= 0
	}
	return false
}

// compareVal 值比较：INT 数值序，TEXT 字典序；跨类型按 TEXT 字典序（宽松）。
func compareVal(a, b Value) int {
	if a.Kind == "INT" && b.Kind == "INT" {
		if a.I < b.I {
			return -1
		}
		if a.I > b.I {
			return 1
		}
		return 0
	}
	as := a.String()
	bs := b.String()
	return bytes.Compare([]byte(as), []byte(bs))
}

func colIndex(meta *TableMeta, name string) int {
	for i, c := range meta.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// String Value 文本表示。
func (v Value) String() string {
	if v.Kind == "INT" {
		return itoa(v.I)
	}
	return v.S
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [24]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

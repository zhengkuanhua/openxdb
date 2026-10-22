package sql

import (
	"strconv"

	"github.com/zhengkuanhua/openxdb/pkg/import"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// execImportDump 执行 mysqldump 兼容导入（IMPORT MYSQLDUMP FROM 'path' [BATCH n] [IGNORE ERRORS]）。
//
// 工作流：
//  1. 解析 dump 文件（pkg/import/mysqldump）：抽取 DROP TABLE IF EXISTS、CREATE TABLE、INSERT；
//  2. 在导入事务中依次执行 DROP（重建语义）、CREATE TABLE、逐行 INSERT；
//  3. 无 BATCH 时整个导入为单个原子事务（任一步失败整体回滚）；
//     BATCH>0 时每批提交一次（大表迁移避免单事务过大），DDL 随首批一并提交；
//  4. 主键冲突行始终跳过计数（导入语义，与 CSV 导入一致）；
//     IgnoreErrors 时坏行/建表错误/执行错误跳过计数，否则首个错误即回滚。
//
// 返回 Result：Columns=[tables, inserted, skipped, errors, batches]。
func (e *Executor) execImportDump(sess *Session, s *ImportStmt) (*Result, error) {
	dump, err := mysqldump.ParseFile(s.Path, mysqldump.Options{IgnoreErrors: s.IgnoreErrors})
	if err != nil {
		return nil, &SQLError{Msg: "import failed: " + err.Error()}
	}
	// 导入事务：已在显式会话事务中则复用（不自行提交）；否则自开并控制提交（BATCH 按批）。
	outer := false
	e.mu.Lock()
	if !sess.inTx {
		outer = true
	}
	e.mu.Unlock()
	if outer {
		if err := e.beginTx(sess); err != nil {
			return nil, err
		}
		defer func() { _ = e.rollbackTx(sess) }()
	}
	tables, inserted, skipped, bad, batches := 0, 0, 0, 0, 0
	// flush：仅 BATCH 模式真正提交并开启新事务；原子模式为空操作（结尾统一提交）。
	flush := func() error {
		if !outer || s.Batch <= 0 {
			return nil
		}
		if err := e.commitTx(sess); err != nil {
			return err
		}
		if err := e.beginTx(sess); err != nil {
			return err
		}
		batches++
		return nil
	}
	// 1) DROP TABLE IF EXISTS（重建语义：目标库同名表被替换）
	for _, dn := range dump.Drops {
		if _, err := e.execDropTable(sess, &DropTableStmt{Name: dn, IfExists: true}); err != nil {
			if !s.IgnoreErrors {
				return nil, err
			}
			bad++
		}
	}
	// 2) CREATE TABLE + INSERT 灌入
	for _, t := range dump.Tables {
		if len(t.Columns) == 0 {
			continue
		}
		cols := make([]ColumnDef, len(t.Columns))
		pk := ""
		for i, c := range t.Columns {
			cols[i] = ColumnDef{Name: c.Name, Type: c.Type}
			if c.Primary && pk == "" {
				pk = c.Name
			}
		}
		if _, err := e.execCreateTable(sess, &CreateTableStmt{Name: t.Name, Columns: cols, PK: pk}); err != nil {
			if !s.IgnoreErrors {
				return nil, err
			}
			bad++
			continue
		}
		tables++
		tx, err := e.getTx(sess)
		if err != nil {
			return nil, err
		}
		tabs, err := loadTables(tx.Get)
		if err != nil {
			return nil, err
		}
		meta := findTable(tabs, t.Name)
		if meta == nil {
			return nil, &SQLError{Msg: "table not exists after create: " + t.Name}
		}
		var ops []kvOp
		// seen：文件内重复主键（同事务未提交写不可见，需本表独立去重）
		seen := make(map[string]bool)
		for _, r := range t.Rows {
			vals, verr := dumpRowValues(t, r)
			if verr != nil {
				if !s.IgnoreErrors {
					return nil, verr
				}
				bad++
				continue
			}
			pk, perr := e.pkOf(meta, vals)
			if perr != nil {
				if !s.IgnoreErrors {
					return nil, perr
				}
				bad++
				continue
			}
			if seen[string(pk)] {
				skipped++
				continue
			}
			seen[string(pk)] = true
			rops, ok, oerr := e.insertRowOps(sess, tx, tabs, meta, nil, vals, true)
			if oerr != nil {
				if !s.IgnoreErrors {
					return nil, oerr
				}
				bad++
				continue
			}
			if !ok {
				skipped++
				continue
			}
			ops = append(ops, rops...)
			inserted++
			if s.Batch > 0 && len(ops) >= s.Batch {
				if err := e.applyImportOps(tx, ops); err != nil {
					return nil, err
				}
				ops = nil
				if err := flush(); err != nil {
					return nil, err
				}
				// flush 已开启新事务，重新取 tx/tabs/meta
				tx, err = e.getTx(sess)
				if err != nil {
					return nil, err
				}
				tabs, err = loadTables(tx.Get)
				if err != nil {
					return nil, err
				}
				meta = findTable(tabs, t.Name)
				if meta == nil {
					return nil, &SQLError{Msg: "table not exists after batch: " + t.Name}
				}
			}
		}
		if len(ops) > 0 {
			if err := e.applyImportOps(tx, ops); err != nil {
				return nil, err
			}
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if outer && s.Batch <= 0 {
		batches = 1
		if err := e.commitTx(sess); err != nil {
			return nil, err
		}
	} else if outer {
		// BATCH 模式：末批已提交，回滚残余空事务（数据已在各批落盘）
		_ = e.rollbackTx(sess)
		if batches == 0 {
			batches = 1 // 空导入也计为一次事务（无数据）
		}
	}
	return &Result{
		Columns:      []string{"tables", "inserted", "skipped", "errors", "batches"},
		AffectedRows: inserted,
		Rows:         [][]Value{{IntVal(int64(tables)), IntVal(int64(inserted)), IntVal(int64(skipped)), IntVal(int64(bad)), IntVal(int64(batches))}},
	}, nil
}

// applyImportOps 应用一批导入写入（模拟显式事务内仅支持本地 region 写，与显式事务一致）。
func (e *Executor) applyImportOps(tx txn.Txn, ops []kvOp) error {
	if e.hasRemoteOps(ops) {
		return &SQLError{Msg: "distributed write inside import transaction not supported in M14 (autocommit 2PC only)"}
	}
	return applyOps(tx, ops)
}

// dumpRowValues 将 mysqldump 解析单元格转换为 OpenXDB 存储值：
//   - INT：十进制字符串 → IntVal；解析失败报错（NULL → 0）
//   - TEXT：字符串原样（NULL → 空串）
//   - DATE：校验 YYYY-MM-DD（NULL → 1970-01-01）
//   - DECIMAL：parseDecimal 规范化（NULL → 0.0000）
//   - BLOB：解析层规范化文本（hex 字面量统一大写，普通字符串原样）（NULL → 空串）
//
// OpenXDB 无 NULL 语义，NULL 统一映射为列类型零值（文档 T27 说明）。
func dumpRowValues(t *mysqldump.Table, r []mysqldump.Value) ([]Value, error) {
	if len(r) != len(t.Columns) {
		return nil, &SQLError{Msg: "import failed: row column count mismatch"}
	}
	vals := make([]Value, len(r))
	for i, c := range t.Columns {
		cell := r[i]
		if cell.Null {
			switch c.Type {
			case "INT":
				vals[i] = IntVal(0)
			case "TEXT", "BLOB":
				vals[i] = Value{Kind: c.Type, S: ""}
			case "DATE":
				vals[i] = Value{Kind: "DATE", S: "1970-01-01"}
			case "DECIMAL":
				scaled, canon, err := parseDecimal("0")
				if err != nil {
					return nil, err
				}
				vals[i] = Value{Kind: "DECIMAL", I: scaled, S: canon}
			default:
				return nil, &SQLError{Msg: "import failed: unsupported column type: " + c.Type}
			}
			continue
		}
		switch c.Type {
		case "INT":
			n, err := strconv.ParseInt(cell.Raw, 10, 64)
			if err != nil {
				return nil, &SQLError{Msg: "import failed: invalid INT: " + cell.Raw}
			}
			vals[i] = IntVal(n)
		case "TEXT":
			vals[i] = StrVal(cell.Raw)
		case "DATE":
			if err := validateDate(cell.Raw); err != nil {
				return nil, err
			}
			vals[i] = Value{Kind: "DATE", S: cell.Raw}
		case "DECIMAL":
			scaled, canon, err := parseDecimal(cell.Raw)
			if err != nil {
				return nil, err
			}
			vals[i] = Value{Kind: "DECIMAL", I: scaled, S: canon}
		case "BLOB":
			// 解析层已把 hex 字面量（X''/0x）统一为大写文本；普通字符串按原样存储。
			vals[i] = Value{Kind: "BLOB", S: cell.Raw}
		default:
			return nil, &SQLError{Msg: "import failed: unsupported column type: " + c.Type}
		}
	}
	return vals, nil
}

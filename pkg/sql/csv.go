package sql

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
)

// P1 CSV 导入导出（详见 docs/T9_p1_csv_types.md）。
//
// 语法：
//   EXPORT TABLE t [(col, ...)] TO 'path'   — 导出表（或列子集）为 CSV
//   IMPORT INTO t FROM 'path'               — 从 CSV 导入建行（覆盖式增量）
//
// CSV 规范（RFC 4180 风格）：
//   - UTF-8 文本；首行为表头（列名，与表定义顺序一致）；后续每行为一条记录。
//   - 字段含逗号/双引号/换行时以双引号包裹，内部双引号翻倍（""）。
//   - INT 为十进制整数；DATE 为 YYYY-MM-DD；DECIMAL 为十进制数（≤4 位小数，规范化后保留 4 位）；
//     BLOB 为大写十六进制文本；TEXT 原样。
//
// 导入语义：主键重复的行跳过（不覆盖、不报错）；任一行的列数/类型非法则整体报错回滚（原子导入）。

// execExport 导出表（或指定列）为 CSV 文件。
func (e *Executor) execExport(s *ExportStmt) (*Result, error) {
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
	cols := s.Columns
	if len(cols) == 0 {
		cols = make([]string, len(meta.Columns))
		for i, c := range meta.Columns {
			cols[i] = c.Name
		}
	} else {
		for _, c := range cols {
			if colIndex(meta, c) < 0 {
				return nil, &SQLError{Msg: "unknown column: " + c}
			}
		}
	}
	pairs, err := e.scanMergedCluster(tx, e.rowRanges(meta))
	if err != nil {
		return nil, err
	}
	rows := make([][]Value, 0, len(pairs))
	for _, kv := range pairs {
		row, err := decodeRow(meta, kv.Value)
		if err != nil {
			return nil, err
		}
		out := make([]Value, len(cols))
		for i, c := range cols {
			v, err := rowValue(meta, row, c)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		rows = append(rows, out)
	}
	f, err := os.Create(s.Path)
	if err != nil {
		return nil, &SQLError{Msg: "export failed: " + err.Error()}
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write(cols); err != nil {
		return nil, &SQLError{Msg: "export failed: " + err.Error()}
	}
	for _, row := range rows {
		rec := make([]string, len(row))
		for i, v := range row {
			rec[i] = csvValue(v)
		}
		if err := w.Write(rec); err != nil {
			return nil, &SQLError{Msg: "export failed: " + err.Error()}
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, &SQLError{Msg: "export failed: " + err.Error()}
	}
	return &Result{Columns: cols, AffectedRows: len(rows)}, nil
}

// csvValue 值 → CSV 字段文本（BLOB 输出大写 hex，其余用规范化字符串）。
func csvValue(v Value) string {
	if v.Kind == "BLOB" {
		return strings.ToUpper(v.S)
	}
	return v.String()
}

// execImport 从 CSV 文件导入建行（M9 增强：批量导入）。
//
// 默认（不指定 BATCH）：单事务原子导入——任何坏行（列数/类型非法）整体
// 回滚，主键重复行跳过（skipped 计数），保证"要么全部成功要么不落一行"。
// `BATCH n`：每成功 n 行提交一批（逐批提交策略，适合超大文件：内存中仅
// 保留一批的 ops，每批提交落 WAL，失败批次不影响已提交批次）。
// `IGNORE ERRORS`：宽松模式——坏行（列数/类型非法）计数跳过并继续，
// 不再整体回滚（主键重复始终只跳过计数）。
func (e *Executor) execImport(s *ImportStmt) (*Result, error) {
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
	f, err := os.Open(s.Path)
	if err != nil {
		return nil, &SQLError{Msg: "import failed: " + err.Error()}
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, &SQLError{Msg: "import failed: empty or invalid csv: " + err.Error()}
	}
	if len(header) != len(meta.Columns) {
		return nil, &SQLError{Msg: "import failed: csv column count mismatch"}
	}
	for i, h := range header {
		if strings.TrimSpace(h) != meta.Columns[i].Name {
			return nil, &SQLError{Msg: "import failed: header mismatch at column " + strconv.Itoa(i+1) + ": " + h}
		}
	}
	inserted, skipped, bad, batches := 0, 0, 0, 0
	var ops []kvOp
	// seen 记录"本批（同事务）已收集但未 apply 的行主键"：避免 CSV 文件内
	// 重复主键在事务唯一性检查盲区（未提交写不可见）下被后行覆盖，保证
	// IMPORT 的 skipped 语义对文件内重复同样生效。
	seen := make(map[string]bool)
	// flush 提交当前批（exec2pcWrite 内部 Commit，落 WAL）；BATCH>0 时开启新事务续读。
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		if err := e.exec2pcWrite(tx, ops); err != nil {
			return err
		}
		ops = nil
		if s.Batch <= 0 {
			return nil
		}
		batches++
		nt, err := e.tm.Begin()
		if err != nil {
			return err
		}
		ntabs, err := loadTables(nt.Get)
		if err != nil {
			_ = nt.Rollback()
			return err
		}
		nmeta := findTable(ntabs, s.Table)
		if nmeta == nil {
			_ = nt.Rollback()
			return &SQLError{Msg: "table not exists: " + s.Table}
		}
		tx, meta = nt, nmeta
		seen = make(map[string]bool)
		return nil
	}
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, &SQLError{Msg: "import failed: " + err.Error()}
		}
		if len(rec) != len(meta.Columns) {
			if s.IgnoreErrors {
				bad++
				continue
			}
			return nil, &SQLError{Msg: "import failed: row column count mismatch"}
		}
		vals := make([]Value, len(rec))
		badRow := false
		for i, cell := range rec {
			v, err := parseCell(meta.Columns[i].Type, cell)
			if err != nil {
				if s.IgnoreErrors {
					badRow = true
					break
				}
				return nil, err
			}
			vals[i] = v
		}
		if badRow {
			bad++
			continue
		}
		// 文件内重复主键：跳过并计数（事务内已收集行对唯一性检查不可见，
		// 用 seen 集合保证"同一批内后行不覆盖前行"）
		bvals, err := e.buildRow(meta, nil, vals)
		if err != nil {
			if s.IgnoreErrors {
				bad++
				continue
			}
			return nil, err
		}
		pk, err := e.pkOf(meta, bvals)
		if err != nil {
			if s.IgnoreErrors {
				bad++
				continue
			}
			return nil, err
		}
		if seen[string(pk)] {
			skipped++
			continue
		}
		seen[string(pk)] = true
		rops, ok, err := e.insertRowOps(tx, meta, nil, vals, true)
		if err != nil {
			if s.IgnoreErrors {
				bad++
				continue
			}
			return nil, err
		}
		if ok {
			inserted++
			ops = append(ops, rops...)
			if s.Batch > 0 && len(ops) >= s.Batch {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		} else {
			skipped++
		}
	}
	// 尾批提交；BATCH>0 时释放 flush 预开的空事务
	if err := flush(); err != nil {
		return nil, err
	}
	if s.Batch > 0 {
		_ = tx.Rollback()
	}
	return &Result{Columns: []string{"inserted", "skipped", "errors", "batches"}, AffectedRows: inserted, Rows: [][]Value{{IntVal(int64(inserted)), IntVal(int64(skipped)), IntVal(int64(bad)), IntVal(int64(batches))}}}, nil
}

// parseCell CSV 字段 → 列类型值。
func parseCell(typ, cell string) (Value, error) {
	switch typ {
	case "INT":
		n, err := strconv.ParseInt(strings.TrimSpace(cell), 10, 64)
		if err != nil {
			return Value{}, &SQLError{Msg: "import failed: invalid INT: " + cell}
		}
		return IntVal(n), nil
	case "TEXT":
		return StrVal(cell), nil
	case "DATE":
		if err := validateDate(cell); err != nil {
			return Value{}, &SQLError{Msg: "import failed: invalid DATE: " + cell}
		}
		return Value{Kind: "DATE", S: cell}, nil
	case "DECIMAL":
		scaled, canon, err := parseDecimal(strings.TrimSpace(cell))
		if err != nil {
			return Value{}, &SQLError{Msg: "import failed: invalid DECIMAL: " + cell}
		}
		return Value{Kind: "DECIMAL", I: scaled, S: canon}, nil
	case "BLOB":
		raw, err := decodeHex(cell)
		if err != nil {
			return Value{}, &SQLError{Msg: "import failed: invalid BLOB hex: " + cell}
		}
		return Value{Kind: "BLOB", S: encodeHex(raw)}, nil
	}
	return Value{}, &SQLError{Msg: "unsupported column type: " + typ}
}

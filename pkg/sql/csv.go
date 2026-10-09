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

// execImport 从 CSV 文件导入建行。
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
	inserted, skipped := 0, 0
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, &SQLError{Msg: "import failed: " + err.Error()}
		}
		if len(rec) != len(meta.Columns) {
			return nil, &SQLError{Msg: "import failed: row column count mismatch"}
		}
		vals := make([]Value, len(rec))
		for i, cell := range rec {
			v, err := parseCell(meta.Columns[i].Type, cell)
			if err != nil {
				return nil, err
			}
			vals[i] = v
		}
		ok, err := e.insertRow(tx, meta, nil, vals, true)
		if err != nil {
			return nil, err
		}
		if ok {
			inserted++
		} else {
			skipped++
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Result{Columns: []string{"inserted", "skipped"}, AffectedRows: inserted, Rows: [][]Value{{IntVal(int64(inserted)), IntVal(int64(skipped))}}}, nil
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

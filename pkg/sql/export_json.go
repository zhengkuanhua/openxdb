package sql

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/export"
)

// 本文件实现 M19 生态导出后端（见 docs/T32_m19_ecosystem_export.md）：
//   - exportJSON：JSON Lines 序列化（每行一个对象）；
//   - exportParquet：把 Value 行集映射为 pkg/export 的 Cell 行集后调用最小写入器。

const hexDigits = "0123456789ABCDEF"

// exportJSON 将列与行序列化为 JSON Lines（UTF-8，每行一个对象，以 \n 结尾）。
// 类型映射：INT/DECIMAL → 数字；TEXT/DATE/BLOB → 字符串（BLOB 为大写十六进制文本）。
func exportJSON(w io.Writer, cols []string, rows [][]Value) error {
	bw := bufio.NewWriterSize(w, 64*1024)
	buf := make([]byte, 0, 512)
	for _, row := range rows {
		buf = buf[:0]
		buf = append(buf, '{')
		for i, c := range cols {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendJSONString(buf, c)
			buf = append(buf, ':')
			buf = appendJSONValue(buf, row[i])
		}
		buf = append(buf, '}', '\n')
		if _, err := bw.Write(buf); err != nil {
			return &SQLError{Msg: "export json: " + err.Error()}
		}
	}
	if err := bw.Flush(); err != nil {
		return &SQLError{Msg: "export json: " + err.Error()}
	}
	return nil
}

// appendJSONValue 将 Value 编码为 JSON 标量。
func appendJSONValue(b []byte, v Value) []byte {
	switch v.Kind {
	case "INT":
		return strconv.AppendInt(b, v.I, 10)
	case "DECIMAL":
		// 规范化字符串（固定 4 位小数）经 ParseFloat 近似为数字，保持类型化标量。
		f, err := strconv.ParseFloat(v.S, 64)
		if err != nil {
			return appendJSONString(b, v.S)
		}
		return strconv.AppendFloat(b, f, 'f', -1, 64)
	case "BLOB":
		return appendJSONString(b, strings.ToUpper(v.S))
	default:
		return appendJSONString(b, v.S)
	}
}

// appendJSONString 将字符串编码为 JSON 字符串（含控制字符转义）。
func appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '"':
			b = append(b, '\\', '"')
		case ch == '\\':
			b = append(b, '\\', '\\')
		case ch == '\n':
			b = append(b, '\\', 'n')
		case ch == '\r':
			b = append(b, '\\', 'r')
		case ch == '\t':
			b = append(b, '\\', 't')
		case ch < 0x20:
			b = append(b, '\\', 'u', '0', '0', hexDigits[ch>>4], hexDigits[ch&0x0F])
		default:
			b = append(b, ch)
		}
	}
	return append(b, '"')
}

// exportParquet 将 Value 行集映射为 pkg/export 的 Field/Row 并写出。
// 类型映射：INT/DECIMAL → INT64（scaled int64）；TEXT/DATE/BLOB → BYTE_ARRAY（UTF-8）。
func exportParquet(w io.Writer, meta *TableMeta, cols []string, rows [][]Value) error {
	fields := make([]export.Field, len(cols))
	for i, c := range cols {
		idx := colIndex(meta, c)
		if idx < 0 {
			return &SQLError{Msg: "unknown column: " + c}
		}
		switch strings.ToUpper(meta.Columns[idx].Type) {
		case "INT", "DECIMAL":
			fields[i] = export.Field{Name: c, Type: export.TypeInt64}
		case "TEXT", "DATE", "BLOB":
			fields[i] = export.Field{Name: c, Type: export.TypeByteArray}
		default:
			return &SQLError{Msg: "unsupported parquet column type: " + meta.Columns[idx].Type}
		}
	}
	pr := make([]export.Row, len(rows))
	for ri, row := range rows {
		cr := make(export.Row, len(cols))
		for ci, v := range row {
			switch v.Kind {
			case "INT", "DECIMAL":
				cr[ci] = export.Cell{I: v.I}
			case "TEXT", "DATE", "BLOB":
				cr[ci] = export.Cell{S: v.String()}
			default:
				return &SQLError{Msg: "unsupported parquet value kind: " + v.Kind}
			}
		}
		pr[ri] = cr
	}
	if _, err := export.WriteParquet(w, fields, pr); err != nil {
		return &SQLError{Msg: "export parquet: " + err.Error()}
	}
	return nil
}

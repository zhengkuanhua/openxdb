// Package export 提供生态导出格式的独立写入器（不依赖 pkg/sql）。
//
// 本文件实现 Apache Parquet v1 最小写入器（M19，见 docs/T32_m19_ecosystem_export.md）：
//   - 单 Row Group，每列一个 ColumnChunk；
//   - 编码 PLAIN、压缩 UNCOMPRESSED（无字典）；
//   - 全部列 REQUIRED（导出列无 NULL），不写重复/定义层级字节；
//   - 文件布局：PAR1 + 各列 DataPage + FileMetaData + footer 长度 + PAR1；
//   - FileMetaData 使用 Thrift Compact Protocol 手工编码（仅实现所需子集）。
//
// 正确性由 pkg/export/parquet_test.go 内置 mini reader 做 roundtrip 兜底。
package export

import (
	"bufio"
	"encoding/binary"
	"io"
)

// FieldType 是 Parquet 物理列类型（OpenXDB 类型映射见 T32 §4.2）。
type FieldType int

const (
	// TypeInt64 对应 INT / DECIMAL（scaled int64）。
	TypeInt64 FieldType = iota
	// TypeByteArray 对应 TEXT / DATE / BLOB（UTF-8 文本）。
	TypeByteArray
)

// Field 描述导出的一列。
type Field struct {
	Name string
	Type FieldType
}

// Cell 是行内单元格。TypeInt64 列使用 I；TypeByteArray 列使用 S。
type Cell struct {
	I int64
	S string
}

// Row 是导出的一行。
type Row []Cell

// Parquet 物理类型枚举（parquet.thrift Type）。
const (
	pqTypeInt64     = 2
	pqTypeByteArray = 6
)

// Thrift Compact Protocol 类型 nibble。
const (
	tI32    = 5
	tI64    = 6
	tBinary = 8
	tList   = 9
	tStruct = 12
	tStop   = 0
)

// buf 是 Thrift Compact 编码缓冲。
type buf struct {
	b []byte
}

func (w *buf) byte(v byte) { w.b = append(w.b, v) }

func (w *buf) uvarint(v uint64) {
	for v >= 0x80 {
		w.b = append(w.b, byte(v)|0x80)
		v >>= 7
	}
	w.b = append(w.b, byte(v))
}

func (w *buf) zi32(v int32) { w.uvarint(uint64(uint32((v << 1) ^ (v >> 31)))) }
func (w *buf) zi64(v int64) { w.uvarint(uint64((v << 1) ^ (v >> 63))) }
func (w *buf) bin(s string) {
	w.uvarint(uint64(len(s)))
	w.b = append(w.b, s...)
}

// field 写字段头：短形式（delta 1..15）。
func (w *buf) field(delta int, typ byte) { w.byte(byte(delta<<4) | typ) }

// list 写 list header：元素类型 nibble + size。
func (w *buf) list(elemType byte, size int) {
	if size < 15 {
		w.byte(elemType<<4 | byte(size))
		return
	}
	w.byte(elemType<<4 | 0x0F)
	w.uvarint(uint64(size))
}

// schemaElement 编码根/列 SchemaElement。
func (w *buf) schemaElement(isRoot bool, name string, typ int32, numChildren int32) {
	se := &buf{}
	if isRoot {
		se.field(3, tBinary)
		se.bin(name)
		se.field(1, tI32)
		se.zi32(numChildren)
	} else {
		se.field(1, tI32)
		se.zi32(typ)
		se.field(2, tBinary)
		se.bin(name)
	}
	se.byte(tStop)
	w.b = append(w.b, se.b...)
}

// columnMetaData 编码 ColumnMetaData（PLAIN / UNCOMPRESSED / REQUIRED）。
func (w *buf) columnMetaData(typ int32, name string, numValues, pageSize, pageOffset int64) {
	cm := &buf{}
	// 1 type i32
	cm.field(1, tI32)
	cm.zi32(typ)
	// 2 encodings list<i32> [PLAIN=0]
	cm.field(1, tList)
	cm.list(tI32, 1)
	cm.zi32(0)
	// 3 path_in_schema list<string> [name]
	cm.field(1, tList)
	cm.list(tBinary, 1)
	cm.bin(name)
	// 4 codec i32 = UNCOMPRESSED(0)
	cm.field(1, tI32)
	cm.zi32(0)
	// 5 num_values i64
	cm.field(1, tI64)
	cm.zi64(numValues)
	// 6 total_uncompressed_size i64
	cm.field(1, tI64)
	cm.zi64(pageSize)
	// 7 total_compressed_size i64
	cm.field(1, tI64)
	cm.zi64(pageSize)
	// 9 data_page_offset i64
	cm.field(2, tI64)
	cm.zi64(pageOffset)
	cm.byte(tStop)
	w.b = append(w.b, cm.b...)
}

// WriteParquet 将 rows 写为 Parquet v1 最小文件（单 Row Group、PLAIN、UNCOMPRESSED）。
// 返回写入行数；fields 为空时报错（rows 为空仍写出合法 footer）。
func WriteParquet(w io.Writer, fields []Field, rows []Row) (int64, error) {
	if len(fields) == 0 {
		return 0, &ExportError{Msg: "parquet: no columns"}
	}
	n := int64(len(rows))

	// 逐列生成 DataPage（PLAIN，无 rep/def 层级字节）。
	pages := &buf{}
	colStarts := make([]int64, len(fields))
	pageSizes := make([]int64, len(fields))
	totalBytes := int64(0)
	for ci, f := range fields {
		vals := &buf{}
		switch f.Type {
		case TypeInt64:
			var tmp [8]byte
			for _, r := range rows {
				binary.LittleEndian.PutUint64(tmp[:], uint64(r[ci].I))
				vals.b = append(vals.b, tmp[:]...)
			}
		case TypeByteArray:
			var tmp [4]byte
			for _, r := range rows {
				s := r[ci].S
				binary.LittleEndian.PutUint32(tmp[:], uint32(len(s)))
				vals.b = append(vals.b, tmp[:]...)
				vals.b = append(vals.b, s...)
			}
		default:
			return 0, &ExportError{Msg: "parquet: unsupported field type"}
		}

		// DataPageHeader + PageHeader（compact）。
		hdr := &buf{}
		hdr.field(1, tI32)
		hdr.zi32(int32(n)) // num_values
		hdr.field(1, tI32)
		hdr.zi32(0) // encoding = PLAIN
		hdr.field(1, tI32)
		hdr.zi32(3) // definition_level_encoding = RLE
		hdr.field(1, tI32)
		hdr.zi32(3) // repetition_level_encoding = RLE
		hdr.byte(tStop)

		ph := &buf{}
		ph.field(1, tI32)
		ph.zi32(0) // type = DATA_PAGE
		ph.field(1, tI32)
		ph.zi32(int32(len(vals.b))) // uncompressed_page_size
		ph.field(1, tI32)
		ph.zi32(int32(len(vals.b))) // compressed_page_size
		ph.field(2, tStruct)
		ph.b = append(ph.b, hdr.b...)
		ph.byte(tStop)

		// 偏移相对文件起始（含 PAR1 头 4 字节）。
		colStarts[ci] = int64(len(pages.b)) + 4
		pageSizes[ci] = int64(len(ph.b) + len(vals.b))
		pages.b = append(pages.b, ph.b...)
		pages.b = append(pages.b, vals.b...)
		totalBytes += pageSizes[ci]
	}

	// FileMetaData（compact）。
	fm := &buf{}
	// 1 version i32 = 1
	fm.field(1, tI32)
	fm.zi32(1)
	// 2 schema list<SchemaElement>（根 + 每列）
	fm.field(1, tList)
	fm.list(tStruct, len(fields)+1)
	fm.schemaElement(true, "schema", 0, int32(len(fields)))
	for _, f := range fields {
		var typ int32
		switch f.Type {
		case TypeInt64:
			typ = pqTypeInt64
		case TypeByteArray:
			typ = pqTypeByteArray
		}
		fm.schemaElement(false, f.Name, typ, 0)
	}
	// 3 num_rows i64
	fm.field(1, tI64)
	fm.zi64(n)
	// 4 row_groups list<RowGroup>（单组）
	fm.field(1, tList)
	fm.list(tStruct, 1)
	{
		rg := &buf{}
		// 1 columns list<ColumnChunk>
		rg.field(1, tList)
		rg.list(tStruct, len(fields))
		for ci, f := range fields {
			cc := &buf{}
			var typ int32
			switch f.Type {
			case TypeInt64:
				typ = pqTypeInt64
			case TypeByteArray:
				typ = pqTypeByteArray
			}
			// 2 meta_data STRUCT
			cc.field(2, tStruct)
			cc.columnMetaData(typ, f.Name, n, pageSizes[ci], colStarts[ci])
			// 3 file_offset i64（列 chunk 起始）
			cc.field(1, tI64)
			cc.zi64(colStarts[ci])
			cc.byte(tStop)
			rg.b = append(rg.b, cc.b...)
		}
		// 2 total_byte_size i64
		rg.field(1, tI64)
		rg.zi64(totalBytes)
		// 3 num_rows i64
		rg.field(1, tI64)
		rg.zi64(n)
		rg.byte(tStop)
		fm.b = append(fm.b, rg.b...)
	}
	// 6 created_by binary "openxdb"
	fm.field(2, tBinary)
	fm.bin("openxdb")
	fm.byte(tStop)

	// 组装：PAR1 + pages + FileMetaData + footerLen(LE32) + PAR1
	bw := bufio.NewWriterSize(w, 64*1024)
	if _, err := bw.WriteString("PAR1"); err != nil {
		return 0, &ExportError{Msg: "parquet: " + err.Error()}
	}
	if _, err := bw.Write(pages.b); err != nil {
		return 0, &ExportError{Msg: "parquet: " + err.Error()}
	}
	if _, err := bw.Write(fm.b); err != nil {
		return 0, &ExportError{Msg: "parquet: " + err.Error()}
	}
	var fl [4]byte
	binary.LittleEndian.PutUint32(fl[:], uint32(len(fm.b)))
	if _, err := bw.Write(fl[:]); err != nil {
		return 0, &ExportError{Msg: "parquet: " + err.Error()}
	}
	if _, err := bw.WriteString("PAR1"); err != nil {
		return 0, &ExportError{Msg: "parquet: " + err.Error()}
	}
	if err := bw.Flush(); err != nil {
		return 0, &ExportError{Msg: "parquet: " + err.Error()}
	}
	return n, nil
}

// ExportError 是导出层错误。
type ExportError struct{ Msg string }

func (e *ExportError) Error() string { return e.Msg }

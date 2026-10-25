package export

// 本文件实现 Parquet v1 流式写入器（v1.11.0 T32 增强）：
//   - 多 Row Group：达到 rowGroupRows 即落盘一组，内存仅保留一组行；
//   - 与 WriteParquet 同布局（PLAIN / UNCOMPRESSED / REQUIRED、Thrift Compact
//     手工编码 FileMetaData），单组输出与 WriteParquet 字节级一致；
//   - 大表导出由全量内存缓冲（[]Row）改为分批落盘，峰值内存 O(rowGroupRows)。

import (
	"bufio"
	"encoding/binary"
	"io"
)

// colChunk 描述一个 ColumnChunk（行组内单列数据页）的元信息。
type colChunk struct {
	name       string
	typ        int32
	numValues  int64
	pageSize   int64
	pageOffset int64
}

// rowGroupInfo 描述一个 Row Group 的元信息（写 FileMetaData 用）。
type rowGroupInfo struct {
	numRows    int64
	totalBytes int64
	cols       []colChunk
}

// defaultRowGroupRows 是默认行组行数：达此行数即 flush 一个 Row Group。
const defaultRowGroupRows = 65536

// ParquetWriter 是 Parquet v1 流式写入器。构造后按批次 WriteRows，最后 Close
// 写 FileMetaData 与 footer。Close 后不可再写入。
type ParquetWriter struct {
	w            io.Writer
	bw           *bufio.Writer
	fields       []Field
	pending      []Row
	rowGroupRows int
	dataBytes    int64 // 已写数据区字节（不含 4 字节 PAR1 头）
	written      int64
	groups       []rowGroupInfo
	closed       bool
}

// NewParquetWriter 创建默认行组（65536 行/组）的流式写入器。
func NewParquetWriter(w io.Writer, fields []Field) *ParquetWriter {
	return NewParquetWriterWithRowGroup(w, fields, defaultRowGroupRows)
}

// NewParquetWriterWithRowGroup 创建指定行组大小的流式写入器；
// rowsPerGroup <= 0 时使用默认值。
func NewParquetWriterWithRowGroup(w io.Writer, fields []Field, rowsPerGroup int) *ParquetWriter {
	if rowsPerGroup <= 0 {
		rowsPerGroup = defaultRowGroupRows
	}
	pw := &ParquetWriter{
		w:            w,
		bw:           bufio.NewWriterSize(w, 64*1024),
		fields:       fields,
		rowGroupRows: rowsPerGroup,
	}
	// 数据页偏移相对文件起始（含 PAR1 头 4 字节），头必须先落。
	_, _ = pw.bw.WriteString("PAR1")
	return pw
}

// WriteRows 追加一批行；达到行组阈值时自动 flush 当前 Row Group。
// 返回本批写入行数（含已 flush 部分）。Close 后调用返回错误。
func (pw *ParquetWriter) WriteRows(rows []Row) (int64, error) {
	if pw.closed {
		return 0, &ExportError{Msg: "parquet: writer closed"}
	}
	n := int64(len(rows))
	if n == 0 {
		return 0, nil
	}
	pw.pending = append(pw.pending, rows...)
	for len(pw.pending) >= pw.rowGroupRows {
		chunk := pw.pending[:pw.rowGroupRows]
		pw.pending = pw.pending[pw.rowGroupRows:]
		if err := pw.flushRowGroup(chunk); err != nil {
			return 0, err
		}
	}
	pw.written += n
	return n, nil
}

// Close 结束写入：flush 剩余行组（空表也写一个空组保证合法 footer）、
// 写 FileMetaData 与 footer。返回文件总行数。
func (pw *ParquetWriter) Close() (int64, error) {
	if pw.closed {
		return pw.written, nil
	}
	pw.closed = true
	if len(pw.pending) > 0 || len(pw.groups) == 0 {
		if err := pw.flushRowGroup(pw.pending); err != nil {
			return 0, err
		}
		pw.pending = nil
	}
	fm := encodeFileMetaData(pw.fields, pw.written, pw.groups)
	if _, err := pw.bw.Write(fm.b); err != nil {
		return 0, &ExportError{Msg: "parquet: " + err.Error()}
	}
	var fl [4]byte
	binary.LittleEndian.PutUint32(fl[:], uint32(len(fm.b)))
	if _, err := pw.bw.Write(fl[:]); err != nil {
		return 0, &ExportError{Msg: "parquet: " + err.Error()}
	}
	if _, err := pw.bw.WriteString("PAR1"); err != nil {
		return 0, &ExportError{Msg: "parquet: " + err.Error()}
	}
	if err := pw.bw.Flush(); err != nil {
		return 0, &ExportError{Msg: "parquet: " + err.Error()}
	}
	return pw.written, nil
}

// flushRowGroup 将一组行编码为数据页并落盘（追加到文件数据区）。
func (pw *ParquetWriter) flushRowGroup(rows []Row) error {
	base := int64(4) + pw.dataBytes
	cols, data, total, err := encodeRowGroupPages(pw.fields, rows, base)
	if err != nil {
		return err
	}
	if _, err := pw.bw.Write(data); err != nil {
		return &ExportError{Msg: "parquet: " + err.Error()}
	}
	pw.dataBytes += int64(len(data))
	pw.groups = append(pw.groups, rowGroupInfo{
		numRows:    int64(len(rows)),
		totalBytes: total,
		cols:       cols,
	})
	return nil
}

// parquetType 返回字段对应的 parquet 物理类型枚举。
func parquetType(f Field) int32 {
	switch f.Type {
	case TypeInt64:
		return pqTypeInt64
	case TypeByteArray:
		return pqTypeByteArray
	}
	return 0
}

// encodeColumnData 将单列（col 索引）全部值编码为 PLAIN 数据体：
// INT64 定长小端；BYTE_ARRAY 为 4 字节长度 + UTF-8 载荷。
func encodeColumnData(f Field, col int, rows []Row) ([]byte, error) {
	vals := &buf{}
	switch f.Type {
	case TypeInt64:
		var tmp [8]byte
		for _, r := range rows {
			binary.LittleEndian.PutUint64(tmp[:], uint64(r[col].I))
			vals.b = append(vals.b, tmp[:]...)
		}
	case TypeByteArray:
		var tmp [4]byte
		for _, r := range rows {
			s := r[col].S
			binary.LittleEndian.PutUint32(tmp[:], uint32(len(s)))
			vals.b = append(vals.b, tmp[:]...)
			vals.b = append(vals.b, s...)
		}
	default:
		return nil, &ExportError{Msg: "parquet: unsupported field type"}
	}
	return vals.b, nil
}

// encodeDataPageHeader 编码 DataPageHeader + PageHeader（compact）。
func encodeDataPageHeader(numValues int64, payloadLen int) []byte {
	hdr := &buf{}
	hdr.field(1, tI32)
	hdr.zi32(int32(numValues)) // num_values
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
	ph.zi32(int32(payloadLen)) // uncompressed_page_size
	ph.field(1, tI32)
	ph.zi32(int32(payloadLen)) // compressed_page_size
	ph.field(2, tStruct)
	ph.b = append(ph.b, hdr.b...)
	ph.byte(tStop)
	return ph.b
}

// encodeRowGroupPages 编码一个 Row Group 的完整数据区，返回各列 chunk 元信息、
// 数据字节与组总字节数。pageOffset 相对文件起始（含 PAR1 头）。
func encodeRowGroupPages(fields []Field, rows []Row, baseOffset int64) ([]colChunk, []byte, int64, error) {
	n := int64(len(rows))
	data := make([]byte, 0, 4096)
	total := int64(0)
	cols := make([]colChunk, 0, len(fields))
	for i, f := range fields {
		vals, err := encodeColumnData(f, i, rows)
		if err != nil {
			return nil, nil, 0, err
		}
		ph := encodeDataPageHeader(n, len(vals))
		off := baseOffset + int64(len(data))
		pageSize := int64(len(ph) + len(vals))
		cols = append(cols, colChunk{
			name:       f.Name,
			typ:        parquetType(f),
			numValues:  n,
			pageSize:   pageSize,
			pageOffset: off,
		})
		data = append(data, ph...)
		data = append(data, vals...)
		total += pageSize
	}
	return cols, data, total, nil
}

// encodeFileMetaData 编码 FileMetaData（compact；支持多 Row Group）。
// 单组输出与 parquet.go 原实现字节级一致。
func encodeFileMetaData(fields []Field, numRows int64, groups []rowGroupInfo) *buf {
	fm := &buf{}
	// 1 version i32 = 1
	fm.field(1, tI32)
	fm.zi32(1)
	// 2 schema list<SchemaElement>（根 + 每列）
	fm.field(1, tList)
	fm.list(tStruct, len(fields)+1)
	fm.schemaElement(true, "schema", 0, int32(len(fields)))
	for _, f := range fields {
		fm.schemaElement(false, f.Name, parquetType(f), 0)
	}
	// 3 num_rows i64
	fm.field(1, tI64)
	fm.zi64(numRows)
	// 4 row_groups list<RowGroup>
	fm.field(1, tList)
	fm.list(tStruct, len(groups))
	for _, g := range groups {
		rg := &buf{}
		// 1 columns list<ColumnChunk>
		rg.field(1, tList)
		rg.list(tStruct, len(g.cols))
		for _, cc := range g.cols {
			c := &buf{}
			// 2 meta_data STRUCT
			c.field(2, tStruct)
			c.columnMetaData(cc.typ, cc.name, cc.numValues, cc.pageSize, cc.pageOffset)
			// 3 file_offset i64（列 chunk 起始）
			c.field(1, tI64)
			c.zi64(cc.pageOffset)
			c.byte(tStop)
			rg.b = append(rg.b, c.b...)
		}
		// 2 total_byte_size i64
		rg.field(1, tI64)
		rg.zi64(g.totalBytes)
		// 3 num_rows i64
		rg.field(1, tI64)
		rg.zi64(g.numRows)
		rg.byte(tStop)
		fm.b = append(fm.b, rg.b...)
	}
	// 6 created_by binary "openxdb"
	fm.field(2, tBinary)
	fm.bin("openxdb")
	fm.byte(tStop)
	return fm
}

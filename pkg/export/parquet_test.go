package export

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// 本文件内置 Parquet v1 mini reader（仅解析本包写入的最小布局），
// 用于 roundtrip 校验写入器生成的字节是否可被第三方工具解析。

// cr 是 Thrift Compact 解码器。
type cr struct {
	b []byte
	i int
}

func (r *cr) byte() (byte, error) {
	if r.i >= len(r.b) {
		return 0, &ExportError{Msg: "compact: EOF"}
	}
	v := r.b[r.i]
	r.i++
	return v, nil
}

func (r *cr) uvarint() (uint64, error) {
	var v uint64
	var shift uint
	for {
		b, err := r.byte()
		if err != nil {
			return 0, err
		}
		v |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return v, nil
		}
		shift += 7
		if shift > 63 {
			return 0, &ExportError{Msg: "compact: varint too long"}
		}
	}
}

func (r *cr) zi32() (int32, error) {
	v, err := r.uvarint()
	if err != nil {
		return 0, err
	}
	return int32(uint32(v)>>1) ^ -int32(v&1), nil
}

func (r *cr) zi64() (int64, error) {
	v, err := r.uvarint()
	if err != nil {
		return 0, err
	}
	return int64(v>>1) ^ -int64(v&1), nil
}

func (r *cr) bin() (string, error) {
	n, err := r.uvarint()
	if err != nil {
		return "", err
	}
	if int(n) > len(r.b)-r.i {
		return "", &ExportError{Msg: "compact: binary too long"}
	}
	s := string(r.b[r.i : r.i+int(n)])
	r.i += int(n)
	return s, nil
}

// field 读取字段头：0x00 为 STOP，其余为短形式。
func (r *cr) field() (delta int, typ byte, stop bool, err error) {
	b, err := r.byte()
	if err != nil {
		return 0, 0, false, err
	}
	if b == 0 {
		return 0, 0, true, nil
	}
	return int(b >> 4), b & 0x0F, false, nil
}

// skip 按类型 nibble 跳过一个值。
func (r *cr) skip(typ byte) error {
	switch typ {
	case tStruct:
		return r.skipStruct()
	case tList:
		return r.skipList()
	case tBinary:
		_, err := r.bin()
		return err
	case tI64, tI32:
		_, err := r.uvarint()
		return err
	default:
		return &ExportError{Msg: "compact: cannot skip type"}
	}
}

func (r *cr) skipStruct() error {
	for {
		_, typ, stop, err := r.field()
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
		if err := r.skip(typ); err != nil {
			return err
		}
	}
}

func (r *cr) skipList() error {
	b, err := r.byte()
	if err != nil {
		return err
	}
	elemType := b >> 4
	size := int(b & 0x0F)
	if size == 0x0F {
		v, err := r.uvarint()
		if err != nil {
			return err
		}
		size = int(v)
	}
	for i := 0; i < size; i++ {
		if elemType == tStruct {
			if err := r.skipStruct(); err != nil {
				return err
			}
			continue
		}
		if err := r.skip(elemType); err != nil {
			return err
		}
	}
	return nil
}

// readCol 是 mini reader 还原的单列描述。
type readCol struct {
	name          string
	typ           int32
	numValues     int64
	dataPageOffset int64
}

type readMeta struct {
	numRows int64
	cols    []readCol
}

// parseFileMetaData 解析 footer 中的 FileMetaData（仅所需字段）。
func parseFileMetaData(b []byte) (*readMeta, error) {
	r := &cr{b: b}
	m := &readMeta{}
	last := 0
	for {
		delta, typ, stop, err := r.field()
		if err != nil {
			return nil, err
		}
		if stop {
			break
		}
		fid := last + delta
		last = fid
		switch fid {
		case 1: // version i32
			if _, err := r.zi32(); err != nil {
				return nil, err
			}
		case 2: // schema list<SchemaElement>
			if err := r.parseSchema(m); err != nil {
				return nil, err
			}
		case 3: // num_rows i64
			v, err := r.zi64()
			if err != nil {
				return nil, err
			}
			m.numRows = v
		case 4: // row_groups list<RowGroup>
			if err := r.parseRowGroups(m); err != nil {
				return nil, err
			}
		case 6: // created_by binary
			if _, err := r.bin(); err != nil {
				return nil, err
			}
		default:
			if err := r.skip(typ); err != nil {
				return nil, err
			}
		}
	}
	return m, nil
}

func (r *cr) parseSchema(m *readMeta) error {
	b, err := r.byte()
	if err != nil {
		return err
	}
	if b>>4 != tStruct {
		return &ExportError{Msg: "compact: schema elem type"}
	}
	size := int(b & 0x0F)
	if size == 0x0F {
		v, err := r.uvarint()
		if err != nil {
			return err
		}
		size = int(v)
	}
	isRoot := true
	for i := 0; i < size; i++ {
		var name string
		var typ int32
		last := 0
		for {
			delta, ft, stop, err := r.field()
			if err != nil {
				return err
			}
			if stop {
				break
			}
			fid := last + delta
			last = fid
			switch fid {
			case 1: // type i32
				v, err := r.zi32()
				if err != nil {
					return err
				}
				typ = v
			case 3: // name binary
				v, err := r.bin()
				if err != nil {
					return err
				}
				name = v
			default:
				if err := r.skip(ft); err != nil {
					return err
				}
			}
		}
		// schema 层列不参与数据页解析（无 data_page_offset），仅记录根节点信息。
		if isRoot {
			// root SchemaElement 仅用于结构校验，此处不记录。
		} else {
			_ = name
			_ = typ
		}
		isRoot = false
	}
	return nil
}

func (r *cr) parseRowGroups(m *readMeta) error {
	b, err := r.byte()
	if err != nil {
		return err
	}
	if b>>4 != tStruct {
		return &ExportError{Msg: "compact: rowgroup elem type"}
	}
	size := int(b & 0x0F)
	if size == 0x0F {
		v, err := r.uvarint()
		if err != nil {
			return err
		}
		size = int(v)
	}
	for g := 0; g < size; g++ {
		last := 0
		for {
			delta, ft, stop, err := r.field()
			if err != nil {
				return err
			}
			if stop {
				break
			}
			fid := last + delta
			last = fid
			switch fid {
			case 1: // columns list<ColumnChunk>
				if err := r.parseColumns(m); err != nil {
					return err
				}
			default:
				if err := r.skip(ft); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *cr) parseColumns(m *readMeta) error {
	b, err := r.byte()
	if err != nil {
		return err
	}
	if b>>4 != tStruct {
		return &ExportError{Msg: "compact: column elem type"}
	}
	size := int(b & 0x0F)
	if size == 0x0F {
		v, err := r.uvarint()
		if err != nil {
			return err
		}
		size = int(v)
	}
	for c := 0; c < size; c++ {
		col := readCol{}
		last := 0
		for {
			delta, ft, stop, err := r.field()
			if err != nil {
				return err
			}
			if stop {
				break
			}
			fid := last + delta
			last = fid
			switch fid {
			case 2: // meta_data STRUCT
				if err := r.parseColumnMeta(&col); err != nil {
					return err
				}
			case 3: // file_offset i64
				if _, err := r.zi64(); err != nil {
					return err
				}
			default:
				if err := r.skip(ft); err != nil {
					return err
				}
			}
		}
		m.cols = append(m.cols, col)
	}
	return nil
}

func (r *cr) parseColumnMeta(col *readCol) error {
	last := 0
	for {
		delta, ft, stop, err := r.field()
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
		fid := last + delta
		last = fid
		switch fid {
		case 1: // type i32
			v, err := r.zi32()
			if err != nil {
				return err
			}
			col.typ = v
		case 3: // path_in_schema list<string>
			v, err := r.listFirstString()
			if err != nil {
				return err
			}
			col.name = v
		case 5: // num_values i64
			v, err := r.zi64()
			if err != nil {
				return err
			}
			col.numValues = v
		case 9: // data_page_offset i64
			v, err := r.zi64()
			if err != nil {
				return err
			}
			col.dataPageOffset = v
		default:
			if err := r.skip(ft); err != nil {
				return err
			}
		}
	}
}

// listFirstString 读取 list<string> 的第一个元素。
func (r *cr) listFirstString() (string, error) {
	b, err := r.byte()
	if err != nil {
		return "", err
	}
	if b>>4 != tBinary {
		return "", &ExportError{Msg: "compact: path elem type"}
	}
	size := int(b & 0x0F)
	if size == 0x0F {
		v, err := r.uvarint()
		if err != nil {
			return "", err
		}
		size = int(v)
	}
	if size == 0 {
		return "", &ExportError{Msg: "compact: empty path_in_schema"}
	}
	return r.bin()
}

// parseDataPageHeader 解析 DataPage 的 PageHeader，返回数据体字节数与页面头部字节数。
func parseDataPageHeader(b []byte) (numValues int64, uncompressed int32, headerLen int, err error) {
	r := &cr{b: b}
	last := 0
	for {
		delta, ft, stop, e := r.field()
		if e != nil {
			return 0, 0, 0, e
		}
		if stop {
			break
		}
		fid := last + delta
		last = fid
		switch fid {
		case 2: // uncompressed_page_size i32
			v, e := r.zi32()
			if e != nil {
				return 0, 0, 0, e
			}
			uncompressed = v
		case 5: // data_page_header STRUCT
			v, e := parseDataPageHeaderInner(r)
			if e != nil {
				return 0, 0, 0, e
			}
			numValues = v
		default:
			if e := r.skip(ft); e != nil {
				return 0, 0, 0, e
			}
		}
	}
	return numValues, uncompressed, r.i, nil
}

func parseDataPageHeaderInner(r *cr) (int64, error) {
	var numValues int64
	last := 0
	for {
		delta, ft, stop, err := r.field()
		if err != nil {
			return 0, err
		}
		if stop {
			return numValues, nil
		}
		fid := last + delta
		last = fid
		switch fid {
		case 1: // num_values i32
			v, err := r.zi32()
			if err != nil {
				return 0, err
			}
			numValues = int64(v)
		default:
			if err := r.skip(ft); err != nil {
				return 0, err
			}
		}
	}
}

// miniRead 从 parquet 字节流还原 (列名, 类型, 行值列表)。
// 仅支持本包写入的最小布局；INT64 还原为 int64，BYTE_ARRAY 还原为 string。
func miniRead(data []byte) (numRows int64, cols []readCol, vals [][]any, err error) {
	if len(data) < 12 || string(data[:4]) != "PAR1" || string(data[len(data)-4:]) != "PAR1" {
		return 0, nil, nil, &ExportError{Msg: "parquet: bad magic"}
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	footerStart := len(data) - 8 - footerLen
	if footerStart < 4 {
		return 0, nil, nil, &ExportError{Msg: "parquet: bad footer length"}
	}
	m, err := parseFileMetaData(data[footerStart : len(data)-8])
	if err != nil {
		return 0, nil, nil, err
	}
	for _, col := range m.cols {
		if col.dataPageOffset < 4 || col.dataPageOffset >= int64(len(data)) {
			return 0, nil, nil, &ExportError{Msg: "parquet: bad page offset"}
		}
		pageStart := int(col.dataPageOffset)
		nv, size, hdrLen, err := parseDataPageHeader(data[pageStart:])
		if err != nil {
			return 0, nil, nil, err
		}
		body := data[pageStart+hdrLen : pageStart+hdrLen+int(size)]
		var out []any
		off := 0
		for i := int64(0); i < nv; i++ {
			switch col.typ {
			case pqTypeInt64:
				if off+8 > len(body) {
					return 0, nil, nil, &ExportError{Msg: "parquet: int64 truncated"}
				}
				out = append(out, int64(binary.LittleEndian.Uint64(body[off:off+8])))
				off += 8
			case pqTypeByteArray:
				if off+4 > len(body) {
					return 0, nil, nil, &ExportError{Msg: "parquet: ba truncated"}
				}
				l := int(binary.LittleEndian.Uint32(body[off : off+4]))
				off += 4
				if off+l > len(body) {
					return 0, nil, nil, &ExportError{Msg: "parquet: ba body truncated"}
				}
				out = append(out, string(body[off:off+l]))
				off += l
			default:
				return 0, nil, nil, &ExportError{Msg: "parquet: unsupported type in reader"}
			}
		}
		vals = append(vals, out)
	}
	return m.numRows, m.cols, vals, nil
}

func TestParquetRoundtrip(t *testing.T) {
	fields := []Field{
		{Name: "id", Type: TypeInt64},
		{Name: "name", Type: TypeByteArray},
		{Name: "amount", Type: TypeInt64},
	}
	rows := []Row{
		{{I: 1}, {S: "alice"}, {I: 12345}},
		{{I: 2}, {S: "o'brien"}, {I: -7}},
		{{I: 3}, {S: "中文 测试"}, {I: 0}},
	}
	var buf bytes.Buffer
	n, err := WriteParquet(&buf, fields, rows)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != 3 {
		t.Fatalf("rows: got %d", n)
	}
	numRows, cols, vals, err := miniRead(buf.Bytes())
	if err != nil {
		t.Fatalf("miniRead: %v", err)
	}
	if numRows != 3 {
		t.Fatalf("num_rows: got %d", numRows)
	}
	if len(cols) != 3 || cols[0].name != "id" || cols[1].name != "name" || cols[2].name != "amount" {
		t.Fatalf("cols: got %+v", cols)
	}
	if len(vals) != 3 {
		t.Fatalf("val cols: got %d", len(vals))
	}
	// id 列
	wantID := []any{int64(1), int64(2), int64(3)}
	for i := range wantID {
		if vals[0][i] != wantID[i] {
			t.Fatalf("id[%d]: got %v want %v", i, vals[0][i], wantID[i])
		}
	}
	// name 列
	wantName := []any{"alice", "o'brien", "中文 测试"}
	for i := range wantName {
		if vals[1][i] != wantName[i] {
			t.Fatalf("name[%d]: got %v want %v", i, vals[1][i], wantName[i])
		}
	}
	// amount 列（含负数）
	wantAmt := []any{int64(12345), int64(-7), int64(0)}
	for i := range wantAmt {
		if vals[2][i] != wantAmt[i] {
			t.Fatalf("amount[%d]: got %v want %v", i, vals[2][i], wantAmt[i])
		}
	}
}

func TestParquetEmptyRows(t *testing.T) {
	fields := []Field{{Name: "id", Type: TypeInt64}}
	var buf bytes.Buffer
	n, err := WriteParquet(&buf, fields, nil)
	if err != nil {
		t.Fatalf("write empty: %v", err)
	}
	if n != 0 {
		t.Fatalf("rows: got %d", n)
	}
	numRows, cols, vals, err := miniRead(buf.Bytes())
	if err != nil {
		t.Fatalf("miniRead empty: %v", err)
	}
	if numRows != 0 || len(cols) != 1 || len(vals) != 1 || len(vals[0]) != 0 {
		t.Fatalf("empty meta: rows=%d cols=%d vals=%d", numRows, len(cols), len(vals[0]))
	}
}

func TestParquetNoColumns(t *testing.T) {
	var buf bytes.Buffer
	if _, err := WriteParquet(&buf, nil, nil); err == nil {
		t.Fatal("want error for no columns")
	}
}

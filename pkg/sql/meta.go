package sql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"github.com/openxdb/openxdb/pkg/storage"
)

// 表元数据与 Key 编码（开发手册 §3.2：Key 编码 / §7：MetaFile）。
// M1 简化：表元数据作为单一 meta 键（m:tables）存于存储，经 WAL 原子持久化；
// 行键 s{tableID:8B}{pk}（无 region/版本前缀，M3/T6 演进时外层追加）。

// TableMeta 表结构元数据。
type TableMeta struct {
	ID      uint64      `json:"id"`
	Name    string      `json:"name"`
	Columns []ColumnDef `json:"columns"`
	PK      string      `json:"pk"`
	Indexes []IndexMeta `json:"indexes,omitempty"`
}

// IndexMeta 二级索引元数据（T6）。
type IndexMeta struct {
	ID   uint64 `json:"id"`
	Name string `json:"name"`
	Col  string `json:"col"`
}

// metaKey 表目录键。
var metaKey = []byte("m:tables")

// EncodeTableKey 行键：s{tableID:8B}{pk}
func EncodeTableKey(tableID uint64, pk []byte) []byte {
	key := make([]byte, 0, 1+8+len(pk))
	key = append(key, 's')
	key = binary.BigEndian.AppendUint64(key, tableID)
	key = append(key, pk...)
	return key
}

// EncodeIndexKey2 二级索引键：i{tableID:8B}{indexID:8B}{idxVal}p{pk}
// idxVal 采用排序友好编码（INT 翻转符号位），保证字节序 = 值语义序；
// 'p' 分隔符用于定位 pk 后缀；前缀 'i' < 's'，索引键整体位于行键区间之前，互不干扰。
func EncodeIndexKey2(tableID, indexID uint64, idxVal, pk []byte) []byte {
	key := make([]byte, 0, 1+8+8+len(idxVal)+1+len(pk))
	key = append(key, 'i')
	key = binary.BigEndian.AppendUint64(key, tableID)
	key = binary.BigEndian.AppendUint64(key, indexID)
	key = append(key, idxVal...)
	key = append(key, 'p')
	key = append(key, pk...)
	return key
}

// indexPKSuffix 从索引键 i{tid}{idxID}{idxVal}p{pk} 提取主键后缀。
func indexPKSuffix(key []byte) []byte {
	i := bytes.LastIndexByte(key, 'p')
	return key[i+1:]
}

// IndexRange 索引扫描范围：[i{tableID}{indexID}{lo}, 上界)
// lo 为索引值字节（nil = 无下界，从该索引首个键开始）；
// hi 非 nil 时上界排他（idxVal < hi）；hi 为 nil 时上界为整表索引边界（+inf）。
// 等值匹配请用 EncodeIndexKey2 构造 [prefix(V), prefix(V)+0xff]。
func IndexRange(tableID, indexID uint64, lo, hi []byte) (start, end []byte) {
	start = EncodeIndexKey2(tableID, indexID, lo, nil)
	if hi == nil {
		end = EncodeIndexKey2(tableID+1, 0, nil, nil)
	} else {
		end = EncodeIndexKey2(tableID, indexID, hi, nil)
	}
	return start, end
}

// IndexTableRange 整表索引区间：[i{tableID}, i{tableID+1})
func IndexTableRange(tableID uint64) (start, end []byte) {
	start = EncodeIndexKey2(tableID, 0, nil, nil)
	end = EncodeIndexKey2(tableID+1, 0, nil, nil)
	return start, end
}

// idxBytes 索引列值 → 排序友好字节（INT 翻转符号位；TEXT 原字节）。
func idxBytes(v Value) ([]byte, error) {
	if v.Kind == "INT" {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v.I)^(1<<63))
		return b, nil
	}
	return []byte(v.S), nil
}

// TableRange 表数据扫描范围：[s{tableID}, s{tableID+1})
func TableRange(tableID uint64) (start, end []byte) {
	start = EncodeTableKey(tableID, nil)
	end = EncodeTableKey(tableID+1, nil)
	return start, end
}

// pkBytes 主键列值 → 行键后缀字节（INT 定长 Big Endian；TEXT 原字节）。
func pkBytes(t *TableMeta, v Value) ([]byte, error) {
	if v.Kind == "INT" {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v.I))
		return b, nil
	}
	// TEXT
	return []byte(v.S), nil
}

// encodeRow 行值 → JSON 字节（按列定义序）。
func encodeRow(t *TableMeta, vals []Value) ([]byte, error) {
	arr := make([]interface{}, len(vals))
	for i, v := range vals {
		if v.Kind == "INT" {
			arr[i] = v.I
		} else {
			arr[i] = v.S
		}
	}
	return json.Marshal(arr)
}

// decodeRow JSON 字节 → 行值（按列定义序，类型强制）。
func decodeRow(t *TableMeta, raw []byte) ([]Value, error) {
	var arr []interface{}
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, err
	}
	if len(arr) != len(t.Columns) {
		return nil, &SQLError{Msg: "corrupted row"}
	}
	vals := make([]Value, len(arr))
	for i, a := range arr {
		if t.Columns[i].Type == "INT" {
			f, ok := a.(float64)
			if !ok {
				return nil, &SQLError{Msg: "corrupted row: int column"}
			}
			vals[i] = IntVal(int64(f))
		} else {
			s, ok := a.(string)
			if !ok {
				return nil, &SQLError{Msg: "corrupted row: text column"}
			}
			vals[i] = StrVal(s)
		}
	}
	return vals, nil
}

// rowValue 取某列的值。
func rowValue(t *TableMeta, row []Value, col string) (Value, error) {
	for i, c := range t.Columns {
		if c.Name == col {
			return row[i], nil
		}
	}
	return Value{}, &SQLError{Msg: "unknown column: " + col}
}

// loadTables 读取表目录（无目录时返回空）。
func loadTables(get func(key []byte) ([]byte, error)) ([]*TableMeta, error) {
	raw, err := get(metaKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return nil, nil
	}
	var tabs []*TableMeta
	if err := json.Unmarshal(raw, &tabs); err != nil {
		return nil, &SQLError{Msg: "corrupted table catalog"}
	}
	return tabs, nil
}

// saveTables 序列化表目录。
func saveTables(tabs []*TableMeta) ([]byte, error) {
	return json.Marshal(tabs)
}

// findTable 按名查找。
func findTable(tabs []*TableMeta, name string) *TableMeta {
	for _, t := range tabs {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// nextTableID 分配新表 ID。
func nextTableID(tabs []*TableMeta) uint64 {
	var max uint64
	for _, t := range tabs {
		if t.ID > max {
			max = t.ID
		}
	}
	return max + 1
}

// findIndex 按名查找表内索引。
func findIndex(t *TableMeta, name string) *IndexMeta {
	for i := range t.Indexes {
		if t.Indexes[i].Name == name {
			return &t.Indexes[i]
		}
	}
	return nil
}

// nextIndexID 分配表内新索引 ID。
func nextIndexID(t *TableMeta) uint64 {
	var max uint64
	for _, idx := range t.Indexes {
		if idx.ID > max {
			max = idx.ID
		}
	}
	return max + 1
}

// kvLess key 升序比较。
func kvLess(a, b []byte) bool { return bytes.Compare(a, b) < 0 }

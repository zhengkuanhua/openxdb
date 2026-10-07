package storage

import "encoding/binary"

// ID 表/索引/节点统一标识（开发手册 §1.2）。
type ID uint64

// LSN 日志序列号（单机自增即可，替代 v1.0 的全局 Ts）。
type LSN uint64

// Key 编码常量：统一 Big Endian，保证字节序排序一致（开发手册 §3.2）。
const (
	rowKeyPrefix   = 't' // 表数据
	indexKeyPrefix = 'i' // 二级索引
)

// EncodeRowKey 表数据 Key：t{tableID:8B} k{pk} v{lsn:8B}
// M1 无 region 前缀；M3 分片由 EncodeRegionKey 在外层追加。
func EncodeRowKey(tableID ID, pk []byte, lsn LSN) []byte {
	key := make([]byte, 0, 1+8+1+len(pk)+1+8)
	key = append(key, rowKeyPrefix)
	key = binary.BigEndian.AppendUint64(key, uint64(tableID))
	key = append(key, 'k')
	key = append(key, pk...)
	key = append(key, 'v')
	key = binary.BigEndian.AppendUint64(key, uint64(lsn))
	return key
}

// EncodeIndexKey 二级索引 Key：t{tableID:8B} i{indexID:8B} k{idxVal} p{pk} v{lsn:8B}
func EncodeIndexKey(tableID, indexID ID, idxVal, pk []byte, lsn LSN) []byte {
	key := make([]byte, 0, 1+8+1+8+len(idxVal)+1+len(pk)+1+8)
	key = append(key, rowKeyPrefix)
	key = binary.BigEndian.AppendUint64(key, uint64(tableID))
	key = append(key, indexKeyPrefix)
	key = binary.BigEndian.AppendUint64(key, uint64(indexID))
	key = append(key, 'k')
	key = append(key, idxVal...)
	key = append(key, 'p')
	key = append(key, pk...)
	key = append(key, 'v')
	key = binary.BigEndian.AppendUint64(key, uint64(lsn))
	return key
}

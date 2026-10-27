// Package storage 定义 OpenXDB 单机存储抽象（M1 核心，v2.0-FP 开发手册 §3）。
package storage

// RangeCounter 可选的范围计数能力（v1.14.0 P0 COUNT(*) 快速路径）。
//
// 不写入 Storage / Snapshot / Txn 基础接口，避免破坏现有 mock 与实现：
// 仅由 RocksDB 等原生引擎通过方法实现，调用方用类型断言探测
// （storage.CountRangeOf / storage.SnapshotCountRangeOf），
// 断言失败时回退 Scan 全量计数，语义完全等价。
//
// CountRange 统计 [Start, End) 范围内键的数量，只数 key 不拷贝 value；
// limit<=0 表示不限制。实现必须保证结果与「同范围 Scan 的行数」一致。
type RangeCounter interface {
	CountRange(r KeyRange, limit int) (int, error)
}

// CountRangeOf 从 Storage 实现中探测范围计数能力；不支持时返回 ok=false，
// 调用方应回退 Scan 计数（结果语义等价，仅性能较慢）。
func CountRangeOf(st Storage) (rc RangeCounter, ok bool) {
	rc, ok = st.(RangeCounter)
	return rc, ok
}

// SnapshotCountRangeOf 从 Snapshot 实现中探测范围计数能力；不支持时返回 ok=false，
// 调用方应回退 Scan 计数（结果语义等价，仅性能较慢）。
func SnapshotCountRangeOf(sn Snapshot) (rc RangeCounter, ok bool) {
	rc, ok = sn.(RangeCounter)
	return rc, ok
}

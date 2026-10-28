// Package storage 定义 OpenXDB 单机存储抽象（M1 核心，v2.0-FP 开发手册 §3）。
package storage

import "errors"

// KVPair 键值对。
type KVPair struct {
	Key   []byte
	Value []byte
}

// KeyRange 扫描范围：[Start, End)，End 为空表示无限。
type KeyRange struct {
	Start []byte // 含
	End   []byte // 不含；空 = 无限
}

// WriteBatch 原子写批次（引擎事务保证）。
type WriteBatch struct {
	Puts    []KVPair
	Deletes [][]byte
}

// Snapshot 一致性快照（备份/迁移复用）。
type Snapshot interface {
	Get(key []byte) ([]byte, error)
	// MGet 批量读取：返回命中键（string(key)）→ 值；未命中键不出现在结果中。
	// 等价于对每个键分别 Get 但仅一次底层批量调用（减少 cgo 往返）。
	MGet(keys [][]byte) (map[string][]byte, error)
	Scan(r KeyRange, limit int) ([]KVPair, error)
	Release()
}

// Storage 单机存储抽象：RocksDB 实现 / B+Tree 实现（M0 E1 实验定夺）。
type Storage interface {
	// Get 按 Key 读取，不存在返回 ErrNotFound。
	Get(key []byte) ([]byte, error)
	// Scan 范围扫描 [Start, End)，limit<=0 表示不限制。
	Scan(r KeyRange, limit int) ([]KVPair, error)
	// Write 原子写批次（引擎事务保证）。
	Write(batch *WriteBatch) error
	// Snapshot 获取一致性快照（备份/迁移复用）。
	Snapshot() (Snapshot, error)
	Close() error
}

// 标准错误，供所有 Storage 实现复用。
var (
	ErrNotFound = errors.New("storage: key not found")
	ErrConflict = errors.New("storage: write conflict")
)

// WriteSeqProvider 提供存储级写提交序列（C6 共享快照窗口过期检测）。
// Storage 接口之外的可选能力，通过类型断言使用：st.(storage.WriteSeqProvider)。
// 实现方须在每次成功 Write 后单调递增（本地事务提交、2PC 参与方提交、
// 恢复重放等一切经 Write 落盘的可见写入都计数）；存储重建（Close 后重新
// Open）序列可复位，调用方（executor 共享窗口）须随 storage 重建关闭旧
// 窗口。不实现该接口的实现（测试 mock 等）由调用方回退到事务级序列。
type WriteSeqProvider interface {
	WriteSeq() uint64
}

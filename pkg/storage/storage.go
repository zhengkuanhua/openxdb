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

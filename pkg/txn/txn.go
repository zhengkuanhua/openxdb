// Package txn 提供单机事务层（M1 T3，开发手册 §3.4）。
// 语义：快照隔离读 + 提交串行化（WAL 先行 + 存储原子写 + 崩溃恢复重放）。
package txn

import (
	"errors"

	"github.com/openxdb/openxdb/pkg/storage"
)

// TxnState 事务生命周期状态。
type TxnState uint8

const (
	TxnActive     TxnState = 1
	TxnCommitted  TxnState = 2
	TxnRolledBack TxnState = 3
)

// Txn 单事务句柄：写操作先入内存缓冲，Commit 时经 WAL + 存储原子提交。
type Txn interface {
	// ID 返回事务 ID（从 1 开始递增）。
	ID() uint64
	// Get 读取：优先自己未提交的写（read-your-writes），否则走 BEGIN 时刻快照（隔离读）。
	Get(key []byte) ([]byte, error)
	// Scan 范围扫描 [Start, End)：快照数据 + 合并事务内未提交写（read-your-writes）。
	// limit<=0 表示不限制；结果按 Key 升序。
	Scan(r storage.KeyRange, limit int) ([]storage.KVPair, error)
	// Put 写入事务缓冲。
	Put(key, value []byte) error
	// Delete 删除（写入事务缓冲）。
	Delete(key []byte) error
	// Commit 提交：WAL 写 Commit 记录 → 存储原子应用 → 释放快照。
	Commit() error
	// Rollback 回滚：丢弃缓冲、释放快照；已落 WAL 的记录不会被重放。
	Rollback() error
}

// TxnManager 事务管理器。
type TxnManager interface {
	// Begin 开启新事务（获取一致性快照）。
	Begin() (Txn, error)
	// Recover 启动恢复：按 LSN 顺序重放 WAL 中已 Commit 记录到存储（幂等，可重复调用）。
	Recover() error
	Close() error
}

// 标准错误。
var (
	ErrTxnClosed = errors.New("txn: transaction already finished")
)

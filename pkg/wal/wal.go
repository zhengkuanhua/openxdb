// Package wal 提供 Write-Ahead Log 实现（M1 T2，开发手册 §3.3）。
package wal

import (
	"errors"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// 记录状态（State 字段取值）。
const (
	StatePrepare  uint8 = 1
	StateCommit   uint8 = 2
	StateRollback uint8 = 3
)

// 文件格式常量。
// 文件头：magic(9B) + version(2B) = 11B
// 每条记录：crc(4B) | len(4B) | lsn(8B) | txn(8B) | state(1B) | nputs(4B) | ndeletes(4B) | body
// body = puts: nputs × [klen(4B) vlen(4B) key val]；deletes: ndeletes × [klen(4B) key]
// crc 覆盖 len 字段到记录结尾；统一 Big Endian。
const (
	fileMagic     = "OPENXDBWAL"
	fileVersion   = 1
	headerSize    = len(fileMagic) + 2
	recHeadSize   = 4 + 4 + 8 + 8 + 1 + 4 + 4
	maxRecordBody = 1 << 30
)

// WalEntry 一条 WAL 记录（事务内所有写，供崩溃恢复重放）。
type WalEntry struct {
	LSN   storage.LSN
	TxnID uint64
	Batch *storage.WriteBatch
	State uint8
}

// WAL 顺序日志接口。
type WAL interface {
	// Append 顺序追加一条记录（默认 fsync 落盘；entry.LSN==0 时自动分配）。
	// 组提交开启后仅写缓冲并快速返回分配的 LSN，持久化由 SyncUpTo 兜底。
	Append(entry *WalEntry) error
	// SyncUpTo 阻塞直至 lsn 及之前所有已追加记录 fsync 落盘。
	// 采用 leader 机制合并 fsync：首个等待者一次刷盘覆盖全部 pending 记录后广播，
	// 后续等待者发现 lastSynced 已推进则零额外 fsync 直接返回；无后台定时器。
	SyncUpTo(lsn storage.LSN) error
	// SetGroupCommit 开关组提交。默认关闭：Append 每记录 fsync，行为与旧版完全一致；
	// 开启后 Append 只写不刷，由 SyncUpTo（或 Close）保证落盘。
	SetGroupCommit(enabled bool)
	// Replay 从文件头顺序重放全部记录，损坏时报错并中止。
	Replay(apply func(*WalEntry)) error
	// Truncate 截断 lsn 之前的记录（保留 lsn 及之后；checkpoint 后调用）。
	Truncate(lsn storage.LSN) error
	// LastLSN 返回已分配的最大 LSN（无记录时为 0）。
	LastLSN() storage.LSN
	Close() error
}

// 错误集合。
var (
	ErrCorrupt = errors.New("wal: corrupt log")
	ErrClosed  = errors.New("wal: closed")
)

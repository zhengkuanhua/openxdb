// Package replication 提供 M2 复制闭环的接口预留（开发手册 §8，不在 M1 实现）。
//
// M1 仅实现单机 WAL + 事务闭环；本包只做演进接口定义，不预支复杂度：
//   - 复制日志从 WAL 演进：BinlogEntry 是 WalEntry 的复制视图（携带 SchemaVer）；
//   - Replica 表示一个复制节点，Role 区分 Master / Slave；
//   - M2 目标：Master 写 → 同步/半同步复制 → Slave 只读，半同步下至少 1 个
//     Slave ack 后才返回提交成功（RPO≈0）。
package replication

import (
	"errors"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ReplicaRole 复制节点角色。
type ReplicaRole uint8

const (
	// RoleMaster 主节点：接受写请求并生成复制日志。
	RoleMaster ReplicaRole = 1
	// RoleSlave 从节点：只读，按复制日志回放。
	RoleSlave ReplicaRole = 2
)

// BinlogEntry 一条复制日志条目（M2 启用，从 WAL 演进）。
// 相比 WalEntry 增加 CommitLSN 与 SchemaVer：Slave 据此在一致点切换表结构。
type BinlogEntry struct {
	// LSN 本条复制日志的日志序号（与 WAL LSN 对齐）。
	LSN storage.LSN
	// CommitLSN 该写事务的提交点 LSN（半同步 ack 判定基准）。
	CommitLSN storage.LSN
	// Batch 实际写入集合（Put/Delete）。
	Batch *storage.WriteBatch
	// SchemaVer 写入时表结构版本，Slave 回放前校验。
	SchemaVer uint64
}

// Replica 复制节点描述。
type Replica struct {
	// Role 节点角色（Master / Slave）。
	Role ReplicaRole
	// BinlogFile 该节点复制日志落盘文件（空表示未启用）。
	BinlogFile string
}

// Replicator M2 复制器接口预留（M2 实现，不在 M1 实现）。
// 语义：Master Append 后按策略（同步/半同步）等待 Slave ack；Slave 侧 Pull 回放。
type Replicator interface {
	// Append 将一条复制日志写入 binlog 并按复制策略同步。
	Append(entry *BinlogEntry) error
	// Pull 从指定 LSN 起拉取复制日志（Slave 侧调用）。
	Pull(from storage.LSN) ([]*BinlogEntry, error)
	// Ack 标记某个 Slave 已回放到指定 LSN（半同步判定用）。
	Ack(node string, lsn storage.LSN) error
	Close() error
}

// 标准错误。
var (
	// ErrNotImplemented 预留接口尚未实现：M1 不应实例化本包类型。
	ErrNotImplemented = errors.New("replication: not implemented in M1")
)

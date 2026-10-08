// Package replication 提供主从复制（M2，开发手册 §8）的完整实现：
//   - Binlog：主节点独立复制日志（append-only 落盘），条目携带单调递增位点 LSN；
//   - Master：监听复制端口，握手后按位点流式推送 binlog，心跳检测从节点存活；
//   - Follower：连接主节点，幂等应用 binlog 到本地存储，位点可查询、断线可续传。
//
// 一致性模型（M2）：主节点本地事务提交时同步落 binlog（RPO=0），推送与从库应用
// 为异步（无 ack 等待）；半同步/同步复制留待 M3。详见 docs/T11_m2_replication.md。
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
	// RoleSlave 从节点：连接主节点并按复制日志回放。
	RoleSlave ReplicaRole = 2
)

// BinlogEntry 一条复制日志条目（M2 实现）。
// 相比 WalEntry 增加 CommitLSN 与 SchemaVer：SchemaVer 用于表结构演进（当前恒 0），
// CommitLSN 记录源 WAL 提交点（审计/故障定位），复制位点以 LSN 为准。
type BinlogEntry struct {
	// LSN 本条复制日志位点：binlog 内部单调递增（从 1 开始），
	// 同时作为幂等标识与断点续传基准（follower 相同位点不重复应用）。
	LSN storage.LSN
	// CommitLSN 该写事务在源 WAL 中的提交点 LSN（M2 仅记录，不做同步判定基准）。
	CommitLSN storage.LSN
	// Batch 实际写入集合（Put/Delete）。
	Batch *storage.WriteBatch
	// SchemaVer 写入时表结构版本，Slave 回放前校验（M2 恒为 0）。
	SchemaVer uint64
}

// Replica 复制节点描述。
type Replica struct {
	// Role 节点角色（Master / Slave）。
	Role ReplicaRole
	// BinlogFile 该节点复制日志落盘文件（空表示未启用）。
	BinlogFile string
}

// Replicator 主端复制器（MasterReplicator 实现）。
// 语义：Master 本地提交后 Append 落盘 binlog 并按位点推送；Slave 侧 Pull 用于
// 程序化拉取；Ack 记录从节点已确认位点（断线续传基准）。
type Replicator interface {
	// Append 将一条复制日志写入 binlog 并广播唤醒推送（异步，不等待从节点确认）。
	Append(entry *BinlogEntry) error
	// Pull 从指定 LSN 起拉取复制日志（顺序读取）。
	Pull(from storage.LSN) ([]*BinlogEntry, error)
	// Ack 标记某个从节点已回放到指定 LSN（断线续传用）。
	Ack(node string, lsn storage.LSN) error
	Close() error
}

// 标准错误。
var (
	// ErrGap 从节点收到不连续位点（跳号），数据流存在缺口时返回。
	ErrGap = errors.New("replication: binlog lsn gap detected")
	// ErrCorrupt binlog 或复制流损坏。
	ErrCorrupt = errors.New("replication: corrupt binlog")
	// ErrClosed 复制器/存储已关闭。
	ErrClosed = errors.New("replication: closed")
)

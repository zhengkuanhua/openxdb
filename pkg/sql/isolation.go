// P2-13：隔离级别支持。
//
// OpenXDB 历史上恒为快照隔离（SI，单机 RocksDB Snapshot + 读己写，
// 分布式 TSO + 版本记录 VersionVisible）。本文件引入可切换的隔离级别：
//   - IsoSnapshot（默认）：既有快照隔离，REPEATABLE READ 语义等价；
//   - IsoReadCommitted：每条语句读取最新已提交视图；
//   - IsoSerializable：快照读 + 提交时 rw-antidependency 冲突检测
//     （storage.ErrConflict 语义），结合行锁 / 2PC 提升可串行化。
package sql

import "strings"

// IsoLevel 事务隔离级别。
type IsoLevel uint8

const (
	// IsoSnapshot 快照隔离（默认；与 REPEATABLE READ 语义等价）。
	IsoSnapshot IsoLevel = iota
	// IsoReadCommitted 读已提交：语句级最新已提交视图。
	IsoReadCommitted
	// IsoSerializable 可串行化：提交时冲突检测（ErrConflict）。
	IsoSerializable
)

// String 返回可读名称（SHOW / 报错输出用）。
func (l IsoLevel) String() string {
	switch l {
	case IsoReadCommitted:
		return "READ COMMITTED"
	case IsoSerializable:
		return "SERIALIZABLE"
	default:
		return "SNAPSHOT ISOLATION"
	}
}

// parseIsoLevelName 将 SET 语法 / 变量值映射为 IsoLevel。
// 兼容 MySQL 写法：SERIALIZABLE / REPEATABLE READ / READ COMMITTED /
// READ UNCOMMITTED（不支持），以及 '-' 分隔变量值（READ-COMMITTED、
// REPEATABLE-READ）。
func parseIsoLevelName(s string) (IsoLevel, bool) {
	switch strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(s), "-", " ")) {
	case "SERIALIZABLE":
		return IsoSerializable, true
	case "REPEATABLE READ", "SNAPSHOT", "SNAPSHOT ISOLATION":
		return IsoSnapshot, true
	case "READ COMMITTED":
		return IsoReadCommitted, true
	default:
		return 0, false
	}
}

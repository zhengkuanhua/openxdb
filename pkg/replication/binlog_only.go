package replication

// v1.20.0 C4：持续复制 region 化 —— 仅落盘的 binlog 主端。
//
// 集群轨（pkg/cluster）节点默认不启动 M2 复制端口，"持续复制 region 化"
// 需要 owner 的写入能落成 binlog 供影子副本拉取增量，因此需要一个只开
// binlog 文件、不监听任何端口的 MasterReplicator 形态：
//   - 不 Serve（无 acceptLoop / 无 heartbeatLoop），不占用 TCP 端口；
//   - Append 只落盘 + 唤醒（无 slave 时 wake 为空操作）；
//   - Close 与既有一致（ln 为 nil 时跳过监听器关闭）。
//
// 零回归：只有显式开启 ha_replica_region_sync 的节点才创建该实例，
// 单机与既有集群路径完全不受影响。

import (
	"fmt"
	"time"
)

// NewBinlogOnly 打开仅落盘的 binlog（不监听复制端口）。
func NewBinlogOnly(binlogPath string) (*MasterReplicator, error) {
	store, err := OpenBinlog(binlogPath)
	if err != nil {
		return nil, fmt.Errorf("replication: open binlog: %w", err)
	}
	return &MasterReplicator{
		store:             store,
		clients:           make(map[string]*slaveConn),
		known:             make(map[string]*slaveInfo),
		wake:              make(chan struct{}, 1),
		HeartbeatInterval: 2 * time.Second,
		HeartbeatTimeout:  10 * time.Second,
		mode:              ModeAsync,
		semiAckN:          DefaultSemiAckN,
		ackTimeout:        DefaultAckTimeout,
	}, nil
}

// BinlogOnly 报告该主端是否为"仅落盘（未监听端口）"形态。
func (m *MasterReplicator) BinlogOnly() bool {
	if m == nil {
		return false
	}
	return m.ln == nil
}

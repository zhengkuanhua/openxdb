package db

// M6 主从切换（高可用与故障转移，见 docs/T15_m6_ha.md）：
//   - PromoteToMaster：follower 断链后升级为主节点，binlog 从空开始记录新写，
//     已有数据作为"既有快照"不生成 binlog（设计取舍见文档）；
//   - EnableAutoFailover：监控 follower 状态，检测到 offline 自动 promote
//     并回调 onPromote（测试/上层观察用）；StopAutoFailover 停止监控。
//
// 与集群自动重指派（SQL 层）相互独立：复制链路的主从角色 ≠ 集群节点注册表，
// 两者可独立启用；主从切换聚焦"写路径可继续"，集群重指派聚焦"读路径可继续"。

import (
	"errors"
	"time"
)

var (
	// ErrNotFollower 当前节点不是从节点（无 Follower 组件）。
	ErrNotFollower = errors.New("db: not a follower")
	// ErrAutoFailoverAlreadyStarted 自动故障转移已启动。
	ErrAutoFailoverAlreadyStarted = errors.New("db: auto failover already started")
)

// PromoteToMaster 将从节点升级为主节点（M6 主从切换）：
//  1. 停止 follower 复制循环（断开旧主链路；Start goroutine 返回后置 offline）；
//  2. 清空 follower 身份，以当前数据为既有快照启动主节点复制（binlog 只记录新写）。
//
// 重复调用（已 promote）返回 ErrReplicationStarted。
func (d *DB) PromoteToMaster() error {
	if d.Follower == nil {
		return ErrNotFollower
	}
	_ = d.Follower.Stop()
	d.Follower = nil
	return d.StartReplication(":0")
}

// EnableAutoFailover 启动主从自动故障转移：周期检查 follower 状态，
// 一旦进入 offline（旧主不可达）即自动 PromoteToMaster，成功后调用
// onPromote 回调（nil 忽略）。返回后可调用 StopAutoFailover 停止。
func (d *DB) EnableAutoFailover(onPromote func()) error {
	d.failoverMu.Lock()
	defer d.failoverMu.Unlock()
	if d.failoverStop != nil {
		return ErrAutoFailoverAlreadyStarted
	}
	d.failoverStop = make(chan struct{})
	go d.failoverLoop(onPromote)
	return nil
}

// StopAutoFailover 停止自动故障转移监控（幂等；Close 时自动调用）。
func (d *DB) StopAutoFailover() {
	d.failoverMu.Lock()
	defer d.failoverMu.Unlock()
	if d.failoverStop != nil {
		close(d.failoverStop)
		d.failoverStop = nil
	}
}

// failoverLoop 自动故障转移监控循环：follower 进入 offline 后自动 promote。
// 成功后置空 failoverStop 并结束（StopAutoFailover 幂等）；中途停止信号关闭即退出。
func (d *DB) failoverLoop(onPromote func()) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-d.failoverStop:
			return
		case <-ticker.C:
		}
		f := d.Follower
		if f == nil {
			continue // 从未 StartFollower 或已 promote
		}
		if f.Status().State != "offline" {
			continue
		}
		select {
		case <-d.failoverStop:
			return
		default:
		}
		if err := d.PromoteToMaster(); err != nil {
			continue // 竞态（如刚 promote）；下一轮再试
		}
		d.failoverMu.Lock()
		d.failoverStop = nil
		d.failoverMu.Unlock()
		if onPromote != nil {
			onPromote()
		}
		return
	}
}

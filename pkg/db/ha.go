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
	"context"
	"errors"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/replication"
)

var (
	// ErrNotFollower 当前节点不是从节点（无 Follower 组件）。
	ErrNotFollower = errors.New("db: not a follower")
	// ErrAutoFailoverAlreadyStarted 自动故障转移已启动。
	ErrAutoFailoverAlreadyStarted = errors.New("db: auto failover already started")
)

// promoteInfo 最近一次升主（PromoteToMaster）的 RPO 审计信息（v1.6.0 阶段一）。
type promoteInfo struct {
	// At 升主完成时刻（零值表示从未升主）。
	At time.Time
	// RpoLost 升主时复制落后缺口（追平窗口补偿后剩余；0 表示无丢失）。
	RpoLost uint64
}

// LastPromoteInfo 返回最近一次 PromoteToMaster 的 RPO 审计信息
// （从未升主时 At 为零值、RpoLost=0）。
func (d *DB) LastPromoteInfo() promoteInfo {
	d.promoteMu.Lock()
	defer d.promoteMu.Unlock()
	return d.lastPromote
}

func (d *DB) recordPromoteInfo(info promoteInfo) {
	d.promoteMu.Lock()
	d.lastPromote = info
	d.promoteMu.Unlock()
}

// PromoteToMaster 将从节点升级为主节点（M6 主从切换）：
//  1. P0 HA：升主仲裁（ha_quorum_enabled 开启时）——多数派 N/2+1 节点可达
//     才允许升主；quorum 不足返回 ErrQuorumNotMet，调用方保持 follower/
//     只读并持续监控（自动 failover 循环会重试）；
//  2. 停止 follower 复制循环（断开旧主链路；Start goroutine 返回后置 offline）；
//  3. 记录 RPO 审计（阶段一）：lastMasterLSN - appliedLSN，升主时刻剩余
//     复制落后缺口写入 LastPromoteInfo（追平窗口补偿后通常为 0）；
//  4. 清空 follower 身份，以当前数据为既有快照启动主节点复制（binlog 只记录新写）；
//     启动复制同时完成 epoch 授权（ha_epoch_enabled 开启时）。
//
// 重复调用（已 promote）返回 ErrReplicationStarted。
func (d *DB) PromoteToMaster() error {
	if d.Follower == nil {
		return ErrNotFollower
	}
	if err := d.quorumGuard(); err != nil {
		return err // quorum 不足：拒绝升主（防分区内多 follower 同时升主成双主）
	}
	_ = d.Follower.Stop()
	info := promoteInfo{At: time.Now()}
	if f := d.Follower; f != nil {
		if gap := f.RpoGap(); gap > 0 {
			info.RpoLost = gap
		}
	}
	d.recordPromoteInfo(info)
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
		// 阶段一 RPO 补偿：promote 前追平窗口（0=跳过追平保持旧行为）。
		// failoverLoop 单 goroutine 顺序执行：追平期间不会重复 promote。
		if d.FailoverCatchupTimeout > 0 {
			d.catchupBeforePromote(f)
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

// catchupBeforePromote promote 前尝试从旧主追平（阶段一 RPO 补偿）：
// 旧主地址可达（短 DialTimeout 探测）则复用续传语义以独立一次性连接
// 重连旧主追平，追平到主端通告位点或窗口超时即停；旧主不可达/窗口超时
// 放弃并保留已应用进度（幂等），直接带剩余 RPO 提升。同步执行，不泄漏
// goroutine，不修改 Follower 主复制状态。
func (d *DB) catchupBeforePromote(f *replication.Follower) {
	addr := f.Status().MasterAddr
	if addr == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.FailoverCatchupTimeout)
	defer cancel()
	_, _ = f.PullCatchup(ctx, addr)
}

// ReconcileAfterPartition 分区恢复合并入口（P2，复制轨）：收到对端新
// clusterEpoch（或本节点从分区中恢复）时按 epoch 大者胜收敛——陈旧侧
// 由 NoteEpoch 内部自动降级只读、停止复制链路、累计冲突标记；新鲜侧
// 不受影响。开关关闭时 NoteEpoch 直接忽略（零回归）。
func (d *DB) ReconcileAfterPartition(remoteEpoch uint64) {
	d.NoteEpoch(remoteEpoch)
}

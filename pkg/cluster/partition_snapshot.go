package cluster

import "time"

// v1.20.0 C5：分区状态快照（集群轨写授权统一的数据面）。
//
// 背景（v1.20.0 C2 §2.1 取舍）：PartitionStatus 对每个远端节点逐次真实探活
// （Probe 同步 PING），无缓存、无失败短路。若把它直接挂到逐语句写热路径，
// P95 写延迟会与远端 PING RTT 强绑定，单节点抖动即可引发批量误拒。
//
// C5 的口径：把"分区判定"与"探活采样"解耦——
//   - 采样：分区 watcher（db.startPartitionWatch，1s 周期）经
//     CheckPartitionRecovery → RefreshPartitionSnapshot 持续刷新快照；
//   - 判定：写热路径读快照（O(1)、无 IO），快照过期时按 stale-while-refresh
//     语义处理——最多一个调用者做真实采样，其余立即用旧快照返回，不阻塞。

// PartitionSnapshotTTL 快照在写授权判定中的最长有效时效。
// 超过后触发一次后台刷新（stale-while-refresh），刷新期间沿用旧快照。
const PartitionSnapshotTTL = 2 * time.Second

// RefreshPartitionSnapshot 真实采样一次分区状态并刷新缓存快照。
// 返回本次采样结果。探活在 m.mu 之外进行（见 PartitionStatus），
// 期间不阻塞心跳/注册写路径。
func (m *Manager) RefreshPartitionSnapshot() PartitionStatus {
	ps := m.PartitionStatus()

	m.snapMu.Lock()
	m.snap = ps
	m.snapAt = time.Now()
	m.snapBusy = false
	m.snapMu.Unlock()

	return ps
}

// PartitionSnapshot 返回最近一次采样快照。ok=false 表示本进程尚未采样过。
func (m *Manager) PartitionSnapshot() (PartitionStatus, bool) {
	m.snapMu.Lock()
	defer m.snapMu.Unlock()
	if m.snapAt.IsZero() {
		return PartitionStatus{}, false
	}
	return m.snap, true
}

// PartitionMinority 写授权热路径判定：本节点是否处于分区少数派。
//
// 三级口径：
//  1. 快照新鲜（未超过 maxAge）→ 直接返回快照结论，零 IO；
//  2. 快照过期且无人刷新 → 本调用者做一次真实采样（成为唯一刷新者）；
//  3. 快照过期但已有刷新者 → 沿用旧快照返回（stale-while-refresh，不阻塞
//     写路径）。
//
// known=false 仅出现在"从未采样且无快照可沿用"的极窄窗口（进程刚启动、
// watcher 尚未跑一轮），此时调用方应按放行处理，保持零回归。
func (m *Manager) PartitionMinority(maxAge time.Duration) (minority bool, known bool) {
	if maxAge <= 0 {
		maxAge = PartitionSnapshotTTL
	}

	m.snapMu.Lock()
	ps, at, has := m.snap, m.snapAt, !m.snapAt.IsZero()
	fresh := has && time.Since(at) <= maxAge
	busy := m.snapBusy
	if !fresh && !busy {
		m.snapBusy = true
	}
	m.snapMu.Unlock()

	if fresh {
		return !ps.MajorityOK, true
	}
	if !has && busy {
		// 无任何快照且别人正在采样：放行，避免启动期误拒。
		return false, false
	}
	if has && busy {
		// 有旧快照、别人正在刷新：沿用旧快照（stale-while-refresh）。
		return !ps.MajorityOK, true
	}

	ps = m.PartitionStatus()

	m.snapMu.Lock()
	m.snap = ps
	m.snapAt = time.Now()
	m.snapBusy = false
	m.snapMu.Unlock()

	return !ps.MajorityOK, true
}

// PartitionSnapshotAge 返回距上次采样的时长；从未采样返回 -1。
// 供 SHOW / 诊断口径使用。
func (m *Manager) PartitionSnapshotAge() time.Duration {
	m.snapMu.Lock()
	defer m.snapMu.Unlock()
	if m.snapAt.IsZero() {
		return -1
	}
	return time.Since(m.snapAt)
}

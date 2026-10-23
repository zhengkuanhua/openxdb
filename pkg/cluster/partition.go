package cluster

// P2 HA 分区检测与视图 version（docs/HA_BRAIN_SPLIT_DESIGN.md §5 P2）：
//   - PartitionStatus：统计注册表节点总数与可达节点数，按多数派（N/2+1）
//     判定本节点是否处于分区少数派（分区内禁止升主/重指派，降级只读）；
//   - ViewVersion：本节点已知最大"视图 version"，大者胜语义更新——分区
//     恢复后按 version 收敛，陈旧分区的低 version 视图不得覆盖新鲜视图；
//   - CheckPartitionRecovery：检测"少数派 → 多数派"状态转换，触发恢复
//     回调（上层据此执行路由收敛/合并）。
//
// 探活默认走真实 PING/PONG（Probe）；测试可通过 SetProber 注入桩函数
// 隔离网络依赖。

// PartitionStatus 分区检测结果快照。
type PartitionStatus struct {
	Total      int  // 注册表节点总数（含本节点）
	Reachable  int  // 可达节点数（本节点恒 1 + 远端探活通过数）
	Quorum     int  // 多数派阈值 N/2+1
	MajorityOK bool // 是否满足多数派（Reachable >= Quorum）
}

// SetProber 注入节点探活函数（测试/特殊部署用；nil = 默认真实 PING/PONG）。
func (m *Manager) SetProber(fn func(id string) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prober = fn
}

// PartitionStatus 统计可达节点数并判定多数派（分区检测）。
// 快照注册表后释放锁再探活，避免探活期间阻塞心跳/注册写路径。
func (m *Manager) PartitionStatus() PartitionStatus {
	m.mu.RLock()
	total := len(m.nodes)
	ids := make([]string, 0, total)
	for id := range m.nodes {
		ids = append(ids, id)
	}
	fn := m.prober
	m.mu.RUnlock()

	reachable := 1 // 本节点恒可达
	for _, id := range ids {
		if id == m.SelfID {
			continue
		}
		var ok bool
		if fn != nil {
			ok = fn(id)
		} else {
			ok = m.Probe(id)
		}
		if ok {
			reachable++
		}
	}
	quorum := total/2 + 1
	return PartitionStatus{
		Total:      total,
		Reachable:  reachable,
		Quorum:     quorum,
		MajorityOK: reachable >= quorum,
	}
}

// MajorityOK 快捷分区检测：是否满足多数派（N/2+1）可正常升主/重指派。
func (m *Manager) MajorityOK() bool {
	return m.PartitionStatus().MajorityOK
}

// ViewVersion 返回本节点已知最大视图 version（分区恢复收敛基准）。
func (m *Manager) ViewVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.viewVersion
}

// TakeViewVersion 以大者胜语义更新视图 version，返回更新后的当前值。
// 分区恢复/路由收敛后调用：陈旧分区的低 version 视图不得覆盖新鲜视图。
func (m *Manager) TakeViewVersion(e uint64) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e > m.viewVersion {
		m.viewVersion = e
	}
	return m.viewVersion
}

// SetOnPartitionRecovered 注册"从少数派恢复到多数派"回调（version = 恢复
// 时本节点已知最大视图 version）。仅 CheckPartitionRecovery 检测到状态
// 转换时触发一次；重复注册覆盖旧回调。
func (m *Manager) SetOnPartitionRecovered(fn func(version uint64)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onRecover = fn
}

// CheckPartitionRecovery 检测本节点是否刚从少数派（分区内）恢复到多数派：
// 仅当上次检查为不满足多数派、本次满足时触发恢复回调并返回 true；
// 未进入过分区 / 一直满足多数派返回 false。可由心跳循环或上层定期调用。
func (m *Manager) CheckPartitionRecovery() bool {
	ps := m.PartitionStatus()

	m.mu.Lock()
	defer m.mu.Unlock()
	nowOK := ps.MajorityOK
	was := m.wasMinority
	m.wasMinority = !nowOK
	if nowOK && was {
		if m.onRecover != nil {
			go m.onRecover(m.viewVersion)
		}
		return true
	}
	return false
}

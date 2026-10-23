package db

// P0 HA 脑裂防护（最小防双主）：clusterEpoch 与升主仲裁核心。
//
// 设计（docs/HA_BRAIN_SPLIT_DESIGN.md P0）：
//   - epoch 单调来源：复用 TSO 单调发号（Source.Get()，中心化发号权威）。
//     TSO 不可用或号不足时，以本节点已知最大 epoch + 1 兜底（注册表 master
//     兜底精神：本地持久化防回退），保证本节点发号永不小于已见全局最大。
//   - 持久化防回退：epoch.meta 记录已知最大 epoch，Open 时加载作为单调下限；
//     节点重启后不会发出更小 epoch。
//   - 授权：成为 master（StartReplication / PromoteToMaster）时取新 epoch
//     记 masterEpoch，并向集群广播（BroadcastEpoch）。
//   - 仲裁：升主前须满足多数派（N/2+1）节点可达（ha_quorum_enabled 开启时）；
//     不足则拒绝升主（降级为只读/保持 follower）。
//   - fencing：本节点 epoch > masterEpoch 说明已被更高 epoch 的授权取代
//     （陈旧主），AllowWrite 拒绝写路径（SQL 层经 WriteFence 装配调用）。
//
// 开关：ha_epoch_enabled / ha_quorum_enabled 默认关闭；关闭时所有方法
// 保持旧行为（零回归）。

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

// P0 HA 标准错误。
var (
	// ErrQuorumNotMet 升主仲裁不满足多数派（N/2+1），不允许升主；
	// 节点应保持 follower/只读并持续监控，待多数派恢复后重试。
	ErrQuorumNotMet = errors.New("db: quorum not met, refuse to promote")
	// ErrFenced 陈旧主写入被 fencing 拒绝（本节点已被更高 clusterEpoch
	// 的授权取代；需停止写入、等待同步或由新主接管）。
	ErrFenced = errors.New("db: write fenced by newer cluster epoch")
)

// SetEpochEnabled 开/关 epoch 授权与写入 fencing（ha_epoch_enabled）。
// 默认关闭（零回归）。开启仅影响后续升主/成为 master 的授权与写校验。
func (d *DB) SetEpochEnabled(on bool) {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	d.epochEnabled = on
}

// SetQuorumEnabled 开/关升主多数派仲裁（ha_quorum_enabled）。默认关闭。
func (d *DB) SetQuorumEnabled(on bool) {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	d.quorumEnabled = on
}

// EpochEnabled 返回 epoch 开关状态。
func (d *DB) EpochEnabled() bool {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	return d.epochEnabled
}

// QuorumEnabled 返回 quorum 开关状态。
func (d *DB) QuorumEnabled() bool {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	return d.quorumEnabled
}

// Epoch 返回本节点已知全局最大 epoch（0 = 从未授权/广播）。
func (d *DB) Epoch() uint64 {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	return d.epoch
}

// AcquireEpoch P1 路由重指派取号入口（SQL 层 EpochSource 装配调用）：
// 在 quorum 授权后为一次 region 重指派取新 clusterEpoch（单调递增、
// 持久化防回退；TSO 优先、本地兜底），随路由消息携带供接收端校验。
func (d *DB) AcquireEpoch() uint64 {
	return d.acquireEpoch()
}

// QuorumOK P1 FailoverDownNode 前置多数派仲裁入口（SQL 层 QuorumArbiter
// 装配调用）：封装 quorumGuard；返回 nil 表示多数派可达可重指派，
// 返回 ErrQuorumNotMet 表示 quorum 不足（调用方降级只读，不重指派）。
func (d *DB) QuorumOK() error {
	return d.quorumGuard()
}

// MasterEpoch 返回本节点当前 master 授权 epoch（0 = 未授权主）。
func (d *DB) MasterEpoch() uint64 {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	return d.masterEpoch
}

// loadEpochMeta 加载 epoch.meta 作为已知最大 epoch 的单调下限（防回退）。
// 文件缺失 = 首次启动（0）；内容损坏按 0 处理（不阻断启动）。
func (d *DB) loadEpochMeta() {
	if d.epochFilePath == "" {
		return
	}
	b, err := os.ReadFile(d.epochFilePath)
	if err != nil {
		return
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return
	}
	d.epochMu.Lock()
	if v > d.epoch {
		d.epoch = v
	}
	d.epochMu.Unlock()
}

// persistEpochLocked 将已知最大 epoch 落盘（尽力而为；失败仅影响重启防回退）。
// 调用方须持有 epochMu。
func (d *DB) persistEpochLocked(e uint64) {
	if d.epochFilePath == "" {
		return
	}
	if e <= d.epoch {
		return
	}
	d.epoch = e
	if err := os.WriteFile(d.epochFilePath, []byte(strconv.FormatUint(e, 10)+"\n"), 0o644); err != nil {
		// 尽力而为：内存单调不受影响，仅重启后下限缺失
	}
}

// acquireEpoch 获取新 clusterEpoch：优先 TSO 单调发号（权威全局单调），
// 失败或号不足时以本节点已知最大 epoch + 1 兜底（持久化防回退）。
// 成功后更新并持久化本节点已知最大 epoch。
func (d *DB) acquireEpoch() uint64 {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	var e uint64
	if d.TSO != nil {
		if v, err := d.TSO.Get(); err == nil {
			e = v
		}
	}
	if e <= d.epoch {
		e = d.epoch + 1 // TSO 不可用/号不足：本地兜底（注册表 master 兜底精神）
	}
	d.persistEpochLocked(e)
	return e
}

// setMasterEpoch 记录本节点 master 授权 epoch（成为 master 时调用），
// 并确保已知最大 epoch 不小于授权值。
func (d *DB) setMasterEpoch(e uint64) {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	d.masterEpoch = e
	if e > d.epoch {
		d.persistEpochLocked(e)
	}
}

// NoteEpoch 接收集群广播的新 epoch（升主节点授权后广播），更新已知最大
// epoch 并持久化；这是陈旧主被 fencing 的依据来源。
// P2 增强（分区恢复合并-复制轨）：本节点已授权为主（masterEpoch>0）且
// 收到更大 epoch 时判定为陈旧主——自动降级只读、停止复制链路并累计
// 数据冲突标记（epoch 大者胜收敛，陈旧侧停止写入转为从/只读）。
func (d *DB) NoteEpoch(e uint64) {
	if e == 0 {
		return
	}
	d.epochMu.Lock()
	oldMaster := d.masterEpoch
	d.persistEpochLocked(e)
	demote := d.epochEnabled && oldMaster > 0 && e > oldMaster && !d.readOnly
	if demote {
		d.readOnly = true
		d.conflictCount++
	}
	d.epochMu.Unlock()
	if demote {
		log.Printf("db: partition reconcile: demote stale master (masterEpoch=%d < notify %d) -> read-only", oldMaster, e)
		if d.Replicator != nil {
			_ = d.Replicator.Close()
			d.Replicator = nil
		}
		if d.demoteHook != nil {
			d.demoteHook()
		}
	}
}

// ReadOnly 返回本节点是否处于分区恢复合并后的只读降级状态（陈旧主）。
// 开关关闭或从未成为陈旧主时恒 false（零回归）。
func (d *DB) ReadOnly() bool {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	return d.readOnly
}

// ConflictCount 返回数据冲突标记计数：陈旧主恢复合并降级次数 + 写路径被
// fencing 拒绝次数（开关关闭恒 0，零回归）。
func (d *DB) ConflictCount() uint64 {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	return d.conflictCount
}

// SetDemoteHook 注册降级观察回调（分区恢复合并降级陈旧主时触发；
// 测试/上层通知用，nil 忽略）。
func (d *DB) SetDemoteHook(fn func()) {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	d.demoteHook = fn
}

// AllowWrite 写入 fencing 检查（SQL 层经 WriteFence 装配调用）：
//   - epoch 开关关闭 → 放行（零回归）；
//   - 本节点从未授权为 master（masterEpoch=0，如普通单机/未授权主）→ 放行；
//   - 本节点已知 epoch 大于自身授权 epoch → 已被更高 epoch 取代（陈旧主），
//     拒绝写入（ErrFenced）并累计数据冲突标记（陈旧侧写尝试被拒）。
func (d *DB) AllowWrite() error {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	if !d.epochEnabled {
		return nil
	}
	if d.masterEpoch == 0 {
		return nil
	}
	if d.epoch > d.masterEpoch {
		d.conflictCount++ // 数据冲突标记：陈旧主写尝试被 fencing 拒绝
		return fmt.Errorf("%w: local epoch %d > master epoch %d", ErrFenced, d.epoch, d.masterEpoch)
	}
	return nil
}

// broadcastEpoch 将新 epoch 广播到集群全部已注册远端（尽力而为，
// 失败不阻断升主；接收端 NoteEpoch 更新 fencing 依据）。
func (d *DB) broadcastEpoch(e uint64) {
	if d.Cluster == nil {
		return
	}
	_ = d.Cluster.BroadcastEpoch(e)
}

// quorumGuard 升主多数派仲裁（ha_quorum_enabled 开启时）：统计本节点与
// 全部已注册远端中可达节点数（PING/PONG 探活），要求 >= N/2+1（N=注册表
// 节点总数）；不足时返回 ErrQuorumNotMet，调用方不得升主（保持只读并重试）。
// 复制链路已确认旧主不可达（follower offline 才触发升主），此处再以
// 多数派确认本节点未孤立：分区内少于多数派时禁止升主，防多 follower 双主。
// 开关关闭时直接放行（M6 旧行为，零回归）。
func (d *DB) quorumGuard() error {
	if !d.quorumEnabled {
		return nil
	}
	if d.Cluster == nil {
		return ErrQuorumNotMet
	}
	nodes := d.Cluster.ListNodes()
	n := len(nodes)
	if n < 2 {
		return fmt.Errorf("%w: cluster size %d, need >= 2", ErrQuorumNotMet, n)
	}
	quorum := n/2 + 1
	up := 1 // 本节点自身
	for _, nd := range nodes {
		if nd.ID == d.Cluster.SelfID {
			continue
		}
		if d.Cluster.Probe(nd.ID) {
			up++
		}
	}
	if up < quorum {
		return fmt.Errorf("%w: reachable %d/%d, need %d", ErrQuorumNotMet, up, n, quorum)
	}
	return nil
}

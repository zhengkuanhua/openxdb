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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
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

// ---- 阶段二：回迁自动化（v1.6.0 P2 HA，docs/T43_ha_rpo_rejoin.md）----

// 回迁相关标准错误。
var (
	// ErrNotStaleMaster 节点不满足陈旧主条件（binlog 为空或未降级只读），不可回迁。
	ErrNotStaleMaster = errors.New("db: not a stale master, rejoin skipped")
	// ErrCompensationFailed 补偿段回灌失败（binlog 损坏/连接失败/主端拒绝），
	// 若能在集群注册表定位新主节点则自动降级走全量对齐兜底路径。
	ErrCompensationFailed = errors.New("db: compensation failed, need full align")
	// ErrFullAlignFailed 全量对齐兜底失败（集群未启用/新主未注册/对端不可达/
	// region 推送失败/清基线或追平失败），调用方应停止回迁并告警。
	ErrFullAlignFailed = errors.New("db: full align fallback failed")
)

// StaleMasterReady 判定本节点是否为可回迁的陈旧主（阶段二）：
//   - 本地 binlog 文件存在且非空（崩溃前曾是主节点，有复制历史）；
//   - 已降级只读（NoteEpoch 识别到更高 clusterEpoch，写路径已被 fencing 拒绝）。
//
// 返回是否就绪与 binlog 最大位点；binlog 损坏时返回错误（应走全量对齐兜底）。
func (d *DB) StaleMasterReady() (bool, uint64, error) {
	lsn, err := d.BinlogLSN()
	if err != nil {
		return false, 0, err // binlog 损坏：全量对齐兜底
	}
	if lsn == 0 {
		return false, 0, nil // binlog 为空：从未担任主节点（或已重置），无需回迁
	}
	if !d.ReadOnly() {
		return false, 0, nil // 未降级只读：尚未识别到新主更高 epoch，不可回迁
	}
	return true, lsn, nil
}

// TryRejoinAsFollower 回迁自动化入口（阶段二）：
// 旧主（曾是 master、崩溃后恢复）识别到新主更高 epoch 后：
//  1. 前置校验 StaleMasterReady（binlog 非空 + 已降级只读）；
//  2. 读本地 binlog 全段作为补偿批次，经一次性补偿连接 PushCompensation
//     回灌新主（新主 apply + 追加自身 binlog，从端已确认段也幂等重放）；
//  3. 补偿成功（或 binlog 为空）后归档旧 binlog 并停止旧复制器，重置基线；
//  4. 以全新 follower 身份 StartFollower(newMasterAddr) 从 LSN=1 追平新主
//     完整 binlog（新写覆盖补偿段，幂等）。
// 补偿失败（binlog 损坏/连接失败/主端拒绝）时：若能在集群注册表定位新主节点，
// 自动降级走 FullAlignViaRegionPush 全量对齐兜底（v1.7.0）；无法定位（如集群
// 未启用/未注册）则返回 ErrCompensationFailed，交由调用方显式发起对齐。
func (d *DB) TryRejoinAsFollower(newMasterAddr string) error {
	ok, lastLSN, err := d.StaleMasterReady()
	if err != nil {
		// binlog 损坏：直接降级全量对齐（读不出补偿段）。
		return d.rejoinFallback(newMasterAddr, err)
	}
	if !ok {
		return ErrNotStaleMaster
	}
	// 停止旧复制器（若此前仍挂着监听/复制），重置主端身份。
	if d.Replicator != nil {
		_ = d.Replicator.Close()
		d.Replicator = nil
	}
	// 读 binlog 全段（[1,lastLSN]）作为补偿批次。
	entries, err := d.readLocalBinlogRange(1, lastLSN)
	if err != nil {
		return d.rejoinFallback(newMasterAddr, err)
	}
	if len(entries) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, cerr := replication.PushCompensation(ctx, newMasterAddr, entries, nil); cerr != nil {
			return d.rejoinFallback(newMasterAddr, cerr)
		}
	}
	// 补偿成功：归档旧 binlog（防重启后重复回迁），重置复制状态。
	if err := d.archiveBinlog(); err != nil {
		return fmt.Errorf("%w: %v", ErrCompensationFailed, err)
	}
	if d.Follower != nil {
		_ = d.Follower.Stop()
		d.Follower = nil
	}
	// 以全新 follower 身份从 LSN=1 追平新主完整 binlog（幂等覆盖补偿段）。
	return d.StartFollower(newMasterAddr)
}

// rejoinFallback 补偿失败后的自动降级：在集群注册表定位新主节点 ID，成功则
// 走 FullAlignViaRegionPush 全量对齐兜底；无法定位（集群未启用/对端未注册）
// 返回 ErrCompensationFailed 保留原语义。
func (d *DB) rejoinFallback(newMasterAddr string, cause error) error {
	id := d.locateNewMasterNodeID(newMasterAddr)
	if id == "" {
		return fmt.Errorf("%w: %v", ErrCompensationFailed, cause)
	}
	return d.FullAlignViaRegionPush(id, newMasterAddr)
}

// locateNewMasterNodeID 在集群注册表中定位新主节点 ID：
// 优先精确匹配复制地址（Addr==newMasterAddr）；其次同主机地址
// （复制地址与集群地址端口不同但 host 一致，同机测试场景）；
// 再退化为唯一远端兜底（双节点场景）。
func (d *DB) locateNewMasterNodeID(addr string) string {
	if d.Cluster == nil {
		return ""
	}
	var sameHost, fallback string
	for _, n := range d.Cluster.ListNodes() {
		if n.ID == d.Cluster.SelfID {
			continue
		}
		if n.Addr == addr {
			return n.ID
		}
		if sameHost == "" && sameHostAddr(n.Addr, addr) {
			sameHost = n.ID
		}
		if fallback == "" {
			fallback = n.ID
		}
	}
	if sameHost != "" {
		return sameHost
	}
	return fallback
}

// sameHostAddr 判断两个 host:port 地址是否同主机（端口可不同）。
func sameHostAddr(a, b string) bool {
	ha, _, err := net.SplitHostPort(a)
	if err != nil {
		return false
	}
	hb, _, err := net.SplitHostPort(b)
	if err != nil {
		return false
	}
	return ha == hb
}

// readLocalBinlogRange 读取本地 binlog 位点区间 [from,to]（含端点）条目。
// binlog 损坏/越界返回错误（调用方走全量对齐兜底）。
func (d *DB) readLocalBinlogRange(from, to uint64) ([]*replication.BinlogEntry, error) {
	path := filepath.Join(d.Dir, BinlogFile)
	b, err := replication.OpenBinlog(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = b.Close() }()
	var entries []*replication.BinlogEntry
	err = b.ReadFrom(storage.LSN(from), func(e *replication.BinlogEntry) error {
		if e.LSN <= storage.LSN(to) {
			entries = append(entries, e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// archiveBinlog 归档本地 binlog 文件（改名为 .rejoin-bak-<unixnano>），
// 使节点以全新 follower 身份启动（binlog 位点从 1 重计，防重启重复回迁）。
func (d *DB) archiveBinlog() error {
	path := filepath.Join(d.Dir, BinlogFile)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	backup := fmt.Sprintf("%s.rejoin-bak-%d", path, time.Now().UnixNano())
	return os.Rename(path, backup)
}

// FullAlignViaRegionPush 全量对齐兜底入口（阶段二，v1.7.0 接通 SQL 层 region push）：
// 旧主本地 binlog 损坏或补偿失败时，请求新主把全部 region 当前行经 REGION_PUSH
// 推送给本节点（新主侧 sql.Executor.pushAllRegionsToNode 扫描全量物理键，
// 本机 cluster.Server.applyPush 在单一 WriteBatch 内覆盖落本地 + 学路由 + 落盘，
// 幂等可重复）；全部推送成功后再清 binlog 基线、停止旧复制链路，最后以全新
// follower 身份 StartFollower(newMasterAddr) 从 LSN=1 追平新主完整 binlog
// （新写覆盖对齐段，幂等）。
// newMasterNodeID 为新主集群节点 ID（本节点注册表内）；newMasterAddr 为复制地址。
// 集群未启用/新主未注册/对端不可达/任一 region 推送失败/清基线或追平失败
// 均返回 ErrFullAlignFailed（明确错误，不再返回"未就绪"骨架）。
func (d *DB) FullAlignViaRegionPush(newMasterNodeID, newMasterAddr string) error {
	if d.Cluster == nil || d.ClusterSrv == nil {
		return fmt.Errorf("%w: cluster not enabled", ErrFullAlignFailed)
	}
	if _, ok := d.Cluster.GetNode(newMasterNodeID); !ok {
		return fmt.Errorf("%w: new master node not registered: %s", ErrFullAlignFailed, newMasterNodeID)
	}
	// 请求新主把全部 region 当前行推送到本节点（同步等待逐 region applyPush 完成）。
	if err := d.Cluster.RequestFullAlign(newMasterNodeID, d.Cluster.SelfID); err != nil {
		return fmt.Errorf("%w: %v", ErrFullAlignFailed, err)
	}
	// 对齐完成：清 binlog 基线（防重启重复回迁），停止旧复制链路。
	if err := d.archiveBinlog(); err != nil {
		return fmt.Errorf("%w: %v", ErrFullAlignFailed, err)
	}
	if d.Follower != nil {
		_ = d.Follower.Stop()
		d.Follower = nil
	}
	if d.Replicator != nil {
		_ = d.Replicator.Close()
		d.Replicator = nil
	}
	// 以全新 follower 身份从 LSN=1 追平新主完整 binlog（新写覆盖对齐段，幂等）。
	if err := d.StartFollower(newMasterAddr); err != nil {
		return fmt.Errorf("%w: %v", ErrFullAlignFailed, err)
	}
	return nil
}

package sql

// B1（D4.5 报告 §6 第 1 项）：单副本不可恢复 → 主动副本冗余 + 无损恢复。
//
// 缺口复现（修复前）：region 只有一份物理副本（ASSIGN/SPLIT 均不复制数据），
// 该副本所在节点损毁后 FailoverDownNode 在"存活节点均无该 region 键空间"
// 时返回 no live data copy，路由仍指向 down 节点 → 该 region 永久不可读，
// 即"单点副本损毁 = 整体不可用"。
//
// 修复三件套：
//  1) 主动冗余：EnsureRegionReplicas 为持有者少于 b1TargetCopies 的 region
//     推送影子副本（副本语义 PushRegionReplica：不改 owner、不动原归属），
//     使任一单节点损毁后仍有存活副本可接管；
//  2) 触发点：SET ha_replica_enabled=on / ha_quorum_enabled=on 后立即补齐、
//     节点回 UP（HandleNodeUp）后补齐、db 层维护循环周期补齐；
//  3) 不放弃：FailoverDownNode 遇"无存活副本"不再永久置 failoverDone，
//     记入 pending 并在 b1FailoverRetryInterval 之后允许重试——副本补齐或
//     原节点恢复后同一 region 可自动恢复归属，无需重启进程。
//
// 开关：ha_replica_enabled 默认 off（零回归）；ha_quorum_enabled=on 时自动
// 生效（生产 HA 模式下冗余副本是 quorum 默认开启的前置条件）。

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// isNoLiveCopy 判定错误是否为"存活节点上无该 region 副本"（B1 缺口核心错误）：
// FailoverDownNode 据此把 region 记入 pending 并在冷却后重试，而不是永久放弃。
func isNoLiveCopy(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "no live data copy")
}

const (
	// b1TargetCopies 生产冗余目标副本数：任一单节点损毁后仍有存活副本
	// 可被 failover 接管（2 = owner + 1 影子副本，容忍 1 节点损毁）。
	b1TargetCopies = 2
	// b1FailoverRetryInterval "无存活副本"导致的 failover 失败重试间隔：
	// 避免读路径高频重试打爆扫描/推送，同时保证副本补齐或原节点恢复后
	// 能在一个短窗口内自动恢复（不再永久放弃）。
	b1FailoverRetryInterval = 2 * time.Second
	// b1NodeCoolInterval 副本补齐的节点级冷却时长：对端不可达（推送/扫描失败）
	// 后短期不再作为目标候选，避免维护循环对刚失联、尚未被判 DOWN 的节点
	// 反复扫描推送刷日志与放大开销。
	b1NodeCoolInterval = 5 * time.Second
)

// b1NodeCooling 判定节点是否处于副本补齐冷却窗口内。
func (e *Executor) b1NodeCooling(nodeID string) bool {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	t, ok := e.b1NodeCool[nodeID]
	if !ok {
		return false
	}
	return time.Now().Before(t)
}

// setB1NodeCool 让节点进入副本补齐冷却窗口。
func (e *Executor) setB1NodeCool(nodeID string) {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	if e.b1NodeCool == nil {
		e.b1NodeCool = map[string]time.Time{}
	}
	e.b1NodeCool[nodeID] = time.Now().Add(b1NodeCoolInterval)
}

// clearB1NodeCool 解除节点冷却（推送成功/节点恢复可达）。
func (e *Executor) clearB1NodeCool(nodeID string) {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	delete(e.b1NodeCool, nodeID)
}

// ReplicaSwitch ha_replica_enabled 开关节流（可选接口，由 db.DB 实现）：
// 与 HAConfigSetter 分离，避免既有 fake/装配强制实现新方法（零回归）。
type ReplicaSwitch interface {
	SetReplicaEnabled(on bool)
	ReplicaEnabled() bool
}

// b1Enabled 判定副本冗余修复是否启用：
//   - haCfg 未装配（单机/未启动集群）→ 关闭；
//   - ha_replica_enabled=on → 启用；
//   - ha_quorum_enabled=on → 自动启用（生产 HA 模式要求冗余，避免
//     "quorum 已开启但仍是单副本"的半成品状态）。
func (e *Executor) b1Enabled() bool {
	cfg := e.haCfg // 取一次快照：避免并发改写 haCfg 时两次加载不一致
	if cfg == nil {
		return false
	}
	if rs, ok := cfg.(ReplicaSwitch); ok && rs.ReplicaEnabled() {
		return true
	}
	return cfg.QuorumEnabled()
}

// SetReplicaHooks 注入副本补齐的推送/扫描钩子（测试隔离网络与存储用；
// nil = 使用真实 PushRegionReplica / scanRegionRows）。
func (e *Executor) SetReplicaHooks(push func(target string, rg sharding.RegionInfo, rows []cluster.QueryRow, srcID string) error,
	scan func(nodeID string, rg sharding.RegionInfo) ([]cluster.QueryRow, error)) {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	e.b1Push = push
	e.b1Scan = scan
}

// b1PushRegion 以副本语义把 region 数据推送到目标节点（不改变 owner）。
func (e *Executor) b1PushRegion(target string, rg sharding.RegionInfo, rows []cluster.QueryRow, srcID string) error {
	e.b1Mu.Lock()
	push := e.b1Push
	e.b1Mu.Unlock()
	if push != nil {
		return push(target, rg, rows, srcID)
	}
	// routes 传 nil：副本对齐只装载数据与归属视图，不做权威视图收敛，
	// 避免补齐动作触发目标节点清理历史 region 记录。
	return e.mgr.PushRegionReplica(target, rg, rows, nil, srcID)
}

// b1ScanRegion 扫描 nodeID 上该 region 键空间的真实行。
func (e *Executor) b1ScanRegion(nodeID string, rg sharding.RegionInfo) ([]cluster.QueryRow, error) {
	e.b1Mu.Lock()
	scan := e.b1Scan
	e.b1Mu.Unlock()
	if scan != nil {
		return scan(nodeID, rg)
	}
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		// 开放上界：半开区间覆盖全部 r{rid} 物理键（与 failoverRegion 一致）
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	pairs, err := e.scanRegionRows(nodeID, start, end)
	if err != nil {
		return nil, err
	}
	rows := make([]cluster.QueryRow, 0, len(pairs))
	for _, kv := range pairs {
		rows = append(rows, cluster.QueryRow{Key: kv.Key, Value: kv.Value})
	}
	return rows, nil
}

// EnsureRegionReplicas 为单副本 region 补齐影子副本（B1 主动冗余）。
//
// 语义：
//   - 遍历本机路由视图中全部 region，统计存活节点上的真实持有者；
//   - 持有者 >= b1TargetCopies → 已冗余，跳过；
//   - 持有者 == 0 → 数据已全部丢失（无法从集群内恢复），记入 lost 观测并
//     返回错误，不伪造空副本（避免用空数据覆盖离线节点上可能存在的副本）；
//   - 持有者 >= 1 → 选行数最多的副本为源，向未持有的存活节点推送影子副本
//     （副本语义：owner 归属不变，接收端仅落数据 + 远端归属视图）。
//
// 幂等：已达标 region 不产生任何动作；推送为覆盖式装载。
// 返回本次补齐的 region 数；单机/未启用/存活节点不足时返回 0。
func (e *Executor) EnsureRegionReplicas() (int, error) {
	if e.mgr == nil || !e.b1Enabled() {
		return 0, nil
	}
	alive := make([]string, 0, 4)
	for _, n := range e.mgr.ListNodes() {
		if n.State == cluster.StateDown {
			continue
		}
		// 冷却中的节点（上轮扫描/推送失败，多为刚失联但尚未被判 DOWN）
		// 本轮不参与，避免维护循环对同一失联节点反复扫描推送。
		if e.b1NodeCooling(n.ID) {
			continue
		}
		alive = append(alive, n.ID)
	}
	// 存活节点不足 2 个时不存在冗余落点，直接返回。
	if len(alive) < 2 {
		return 0, nil
	}
	sort.Strings(alive)

	var ensured int
	var firstErr error
	for _, rg := range e.router.AllRegions() {
		holders := make([]string, 0, len(alive))
		for _, id := range alive {
			holds, err := e.holdsRegion(rg, id)
			if err != nil {
				// 节点不可达（尚未被判 DOWN 的失联窗口）：短期冷却，
				// 避免对同一节点反复扫描。
				e.setB1NodeCool(id)
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if holds {
				holders = append(holders, id)
			}
		}
		if len(holders) >= b1TargetCopies {
			e.noteB1Replicas(rg.RegionID, holders)
			continue
		}
		if len(holders) == 0 {
			// 集群内已无任何副本：标记 lost，交由上层/运维介入（不伪造副本）。
			e.markB1Lost(rg.RegionID)
			log.Printf("sql: B1 region %d has no live copy in cluster (cannot recover from cluster)", rg.RegionID)
			if firstErr == nil {
				firstErr = &SQLError{Msg: fmt.Sprintf("region %d has no live copy", rg.RegionID)}
			}
			continue
		}
		// 选源：行数最多的持有者（最全副本）。
		srcID := ""
		var srcRows []cluster.QueryRow
		for _, id := range holders {
			rows, err := e.b1ScanRegion(id, rg)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if len(rows) > len(srcRows) {
				srcID, srcRows = id, rows
			}
		}
		if srcID == "" || len(srcRows) == 0 {
			// 空 region（尚未写入）无需冗余；全部扫描失败时留待下次补齐。
			continue
		}
		// 目标：首选未持有的存活节点（分散副本），无则跳过。
		target := ""
		for _, id := range alive {
			if id == srcID || containsNode(holders, id) {
				continue
			}
			target = id
			break
		}
		if target == "" {
			continue
		}
		pushRG := rg
		if pushRG.Node == "" {
			// 未指派归属的隐式 region：以源副本所在节点为权威 owner。
			pushRG.Node = srcID
		}
		if err := e.b1PushRegion(target, pushRG, srcRows, srcID); err != nil {
			e.setB1NodeCool(target)
			log.Printf("sql: B1 replicate region %d -> %s failed: %v", rg.RegionID, target, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		e.clearB1NodeCool(target)
		ensured++
		e.clearB1Pending(rg.RegionID)
		e.noteB1Replicas(rg.RegionID, append(append([]string{}, holders...), target))
		log.Printf("sql: B1 region %d replicated %s -> %s (%d rows), copies=%d",
			rg.RegionID, srcID, target, len(srcRows), len(holders)+1)
	}
	if ensured > 0 {
		e.b1Mu.Lock()
		e.b1Ensured += uint64(ensured)
		e.b1Mu.Unlock()
	}
	return ensured, firstErr
}

// B1PendingRegions 返回因"无存活副本"而未能完成重指派的 region（观测/测试用）：
// regionID -> 触发 failover 的 down 节点。
func (e *Executor) B1PendingRegions() map[storage.ID]string {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	out := make(map[storage.ID]string, len(e.b1Pending))
	for k, v := range e.b1Pending {
		out[k] = v
	}
	return out
}

// B1ReplicasOf 返回最近一次确认的 region 副本持有节点集合（观测/测试用）。
func (e *Executor) B1ReplicasOf(rid storage.ID) []string {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	return append([]string{}, e.b1Replicas[rid]...)
}

// B1EnsureCount 返回累计补齐的副本数（观测/测试用）。
func (e *Executor) B1EnsureCount() uint64 {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	return e.b1Ensured
}

// b1EnsureAsync 异步触发一次副本补齐（触发点用，不阻塞调用方）。
func (e *Executor) b1EnsureAsync() {
	if e.mgr == nil || !e.b1Enabled() {
		return
	}
	go func() {
		if _, err := e.EnsureRegionReplicas(); err != nil {
			log.Printf("sql: B1 replica ensure: %v", err)
		}
	}()
}

// recordB1Pending 记录"无存活副本"未完成重指派的 region。
func (e *Executor) recordB1Pending(rid storage.ID, downID string) {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	if e.b1Pending == nil {
		e.b1Pending = map[storage.ID]string{}
	}
	e.b1Pending[rid] = downID
}

// clearB1Pending 清除 pending 标记（副本补齐/重指派成功后调用）。
func (e *Executor) clearB1Pending(rid storage.ID) {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	delete(e.b1Pending, rid)
}

// b1RetryAllowed 判定 down 节点是否已过重试冷却窗口（窗口内直接返回，
// 避免读路径高频重试打爆扫描/推送；窗口外允许再次尝试重指派）。
func (e *Executor) b1RetryAllowed(downID string) bool {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	t, ok := e.b1RetryAt[downID]
	if !ok {
		return true
	}
	return !time.Now().Before(t)
}

// setB1RetryAt 记录下次允许重试的时间点。
func (e *Executor) setB1RetryAt(downID string) {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	if e.b1RetryAt == nil {
		e.b1RetryAt = map[string]time.Time{}
	}
	e.b1RetryAt[downID] = time.Now().Add(b1FailoverRetryInterval)
}

// clearB1RetryAt 清除重试冷却（重指派全部成功后调用）。
func (e *Executor) clearB1RetryAt(downID string) {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	delete(e.b1RetryAt, downID)
}

// noteB1Replicas 记录 region 副本持有节点集合（观测）。
func (e *Executor) noteB1Replicas(rid storage.ID, holders []string) {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	if e.b1Replicas == nil {
		e.b1Replicas = map[storage.ID][]string{}
	}
	cp := append([]string{}, holders...)
	sort.Strings(cp)
	e.b1Replicas[rid] = cp
}

// markB1Lost 记录集群内已无副本的 region（观测；不伪造副本）。
func (e *Executor) markB1Lost(rid storage.ID) {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	if e.b1Lost == nil {
		e.b1Lost = map[storage.ID]bool{}
	}
	e.b1Lost[rid] = true
}

// B1LostRegions 返回集群内已无任何副本的 region（观测/测试用）。
func (e *Executor) B1LostRegions() []storage.ID {
	e.b1Mu.Lock()
	defer e.b1Mu.Unlock()
	out := make([]storage.ID, 0, len(e.b1Lost))
	for rid := range e.b1Lost {
		out = append(out, rid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// containsNode 判定节点 ID 是否在列表中。
func containsNode(list []string, id string) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}
	return false
}

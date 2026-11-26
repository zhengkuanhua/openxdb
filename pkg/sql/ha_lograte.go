package sql

// v2.0.8 Q3：孤儿 region "no live copy" 日志限频（只降噪，不改行为）。
//
// 背景：孤儿 region（集群内已无任何存活副本）会在两条热路径上每轮（约 3s）
// 各打印一条日志：
//   - failoverRegion 重试路径： "failover region %d deferred (no live copy, will retry)"
//   - B1 副本维护循环（ha_b1.go）："B1 region %d has no live copy in cluster"
// 单节点一天可刷数万行，淹没真实故障信号。
//
// 策略（与 DEV_PLAN_v2.0.8 验收口径一致）：
//   - 同一 region 首次异常：全量输出一条（含具体错误原因），不做任何裁剪；
//   - 窗口期内重复异常：只累计次数，不输出；
//   - 窗口到期（默认 5 分钟）仍未恢复：输出一条汇总，含窗口内被抑制的条数；
//   - region 重指派成功 / 副本补齐后状态清空，下一次异常重新从全量输出开始，
//     保证"恢复"与"再次故障"都有明确边界日志。
//
// 两条路径各自独立计数（noLiveLog / noLiveB1Log），互不干扰；限频只作用于
// 日志输出，不影响重试行为与 region 状态机。

import (
	"log"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// noLiveCopyLogWindow "no live copy" 同类日志的抑制窗口（5 分钟）。
// 声明为变量仅为单元测试可临时缩短窗口观察汇总行；生产路径不修改该值。
var noLiveCopyLogWindow = 5 * time.Minute

// noLiveLogStat 单个 region 的日志限频状态。
type noLiveLogStat struct {
	lastLog    time.Time // 最近一次实际输出时间（首条或汇总条）
	suppressed int       // 自 lastLog 起被抑制的条数
}

// 限频判定结果。
const (
	noLiveLogSuppress = iota // 窗口期内：本次不输出
	noLiveLogFirst           // 首次异常：全量输出
	noLiveLogSummary         // 窗口到期：输出汇总
)

// allowNoLiveLog 通用限频判定：返回（判定结果, 被抑制条数）。
// table 指向该路径的状态表（failover 或 B1），由调用方在锁内初始化。
func allowNoLiveLog(table *map[storage.ID]*noLiveLogStat, rid storage.ID) (int, int) {
	now := time.Now()
	if *table == nil {
		*table = map[storage.ID]*noLiveLogStat{}
	}
	st, ok := (*table)[rid]
	if !ok {
		// 首次异常：全量输出，建立窗口起点。
		(*table)[rid] = &noLiveLogStat{lastLog: now}
		return noLiveLogFirst, 0
	}
	if now.Sub(st.lastLog) < noLiveCopyLogWindow {
		// 窗口期内：只计数，不输出。
		st.suppressed++
		return noLiveLogSuppress, 0
	}
	suppressed := st.suppressed
	st.suppressed = 0
	st.lastLog = now
	return noLiveLogSummary, suppressed
}

// logNoLiveCopyDeferred 替代 failoverRegion 中无条件的 "no live copy" 重试日志，
// 按 region 限频。
func (e *Executor) logNoLiveCopyDeferred(rid storage.ID, err error) {
	if e == nil {
		return
	}
	e.noLiveMu.Lock()
	kind, suppressed := allowNoLiveLog(&e.noLiveLog, rid)
	e.noLiveMu.Unlock()
	switch kind {
	case noLiveLogFirst:
		log.Printf("sql: failover region %d deferred (no live copy, will retry): %v", rid, err)
	case noLiveLogSummary:
		log.Printf("sql: failover region %d still no live copy: %d entries suppressed in last %s (latest: %v)",
			rid, suppressed, noLiveCopyLogWindow, err)
	}
}

// logNoLiveCopyB1 替代 B1 副本维护循环中无条件的 "no live copy" 日志，
// 按 region 独立限频（与 failover 路径分开计数）。
func (e *Executor) logNoLiveCopyB1(rid storage.ID) {
	if e == nil {
		return
	}
	e.noLiveMu.Lock()
	kind, suppressed := allowNoLiveLog(&e.noLiveB1Log, rid)
	e.noLiveMu.Unlock()
	switch kind {
	case noLiveLogFirst:
		log.Printf("sql: B1 region %d has no live copy in cluster (cannot recover from cluster)", rid)
	case noLiveLogSummary:
		log.Printf("sql: B1 region %d still has no live copy: %d entries suppressed in last %s (cannot recover from cluster)",
			rid, suppressed, noLiveCopyLogWindow)
	}
}

// clearNoLiveCopyLog 清除单个 region 的限频状态（重指派成功 / 副本补齐后调用），
// 使该 region 下次异常重新全量输出。两条路径的状态一并清除。
func (e *Executor) clearNoLiveCopyLog(rid storage.ID) {
	if e == nil {
		return
	}
	e.noLiveMu.Lock()
	delete(e.noLiveLog, rid)
	delete(e.noLiveB1Log, rid)
	e.noLiveMu.Unlock()
}

// NoLiveCopySuppressed 返回当前仍处于抑制窗口内的 region 数（观测/测试用）。
func (e *Executor) NoLiveCopySuppressed() int {
	if e == nil {
		return 0
	}
	e.noLiveMu.Lock()
	defer e.noLiveMu.Unlock()
	seen := map[storage.ID]struct{}{}
	for id := range e.noLiveLog {
		seen[id] = struct{}{}
	}
	for id := range e.noLiveB1Log {
		seen[id] = struct{}{}
	}
	return len(seen)
}

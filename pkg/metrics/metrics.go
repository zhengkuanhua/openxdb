// Package metrics 提供 Prometheus 文本格式指标注册与渲染（M13 子功能二）。
//
// 设计形态：
//   - 计数器（queries_total / slow_query_total / txn_*_total / query_*_total）
//     全部使用 atomic 累加，无锁、低开销（每条查询仅一次原子自增）；
//   - QPS 采用「秒级窗口」无锁 CAS 滚动：同秒内累加、跨秒重置，渲染时输出
//     最近一个完整窗口的计数（gauge 语义，配合 # TYPE gauge 声明）；
//   - 运行态指标（活跃连接数 / 慢查询阈值 / 存储数据量 / region 分片状态）
//     通过注入函数采集（与 T20 SetStatsSources 同思路），避免 metrics 包
//     反向依赖 sql/server 包；
//   - 渲染输出符合 Prometheus 文本规范（# HELP / # TYPE / name{labels} value），
//     标签值使用双引号转义（\ 与 " 按规范转义）。
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// RegionStats region 相关指标快照（由 sql.Executor 按路由表聚合后注入）。
type RegionStats struct {
	Total   int64            // region 总数（显式路由表，与 SHOW STATS 口径一致）
	ByState map[string]int64 // 按分片状态计数（state -> 数量）
	Local   int64            // 数据物理位于本节点的 region 数
	Remote  int64            // 归属远端节点的 region 数
}

// qpsWindow 秒级 QPS 窗口：无锁 CAS 滚动。
//   - bump(nowSec)：同秒计数 +1；跨秒时原子换窗并置 1；
//   - value(nowSec)：返回当前秒已累计的请求数（渲染输出即时值）。
type qpsWindow struct {
	sec   atomic.Int64
	count atomic.Int64
}

func (w *qpsWindow) bump(nowSec int64) {
	for {
		s := w.sec.Load()
		c := w.count.Load()
		if s == nowSec {
			if w.count.CompareAndSwap(c, c+1) {
				return
			}
			continue
		}
		if w.sec.CompareAndSwap(s, nowSec) {
			w.count.Store(1)
			return
		}
	}
}

func (w *qpsWindow) value(nowSec int64) int64 {
	if w.sec.Load() != nowSec {
		return 0
	}
	return w.count.Load()
}

// Registry 线程安全指标注册表：计数器 atomic，采集源注入式，nil 源输出 0。
type Registry struct {
	// 累计计数器
	sqlTotal    atomic.Int64 // SQL 查询累计数（execTimed 每次执行 +1）
	kvTotal     atomic.Int64 // KV 命令累计数（server GET/SET/DEL +1）
	slowTotal   atomic.Int64 // 慢查询累计数（recordSlow 命中阈值 +1）
	txnCommit   atomic.Int64 // 事务提交累计数
	txnRollback atomic.Int64 // 事务回滚累计数
	localTotal  atomic.Int64 // 本节点数据访问累计数
	remoteTotal atomic.Int64 // 远端转发数据访问累计数

	// QPS 秒窗
	sqlQPS qpsWindow
	kvQPS  qpsWindow

	// 注入采集源（nil = 输出 0）
	conns         func() int64
	slowThreshold func() int64
	storageBytes  func() int64
	regionStats   func() RegionStats

	now func() time.Time // 时间源（测试注入）
}

// New 创建空指标注册表（所有计数器为 0，采集源未装配时输出 0）。
func New() *Registry {
	return &Registry{now: time.Now}
}

// ---- 计数器 ----

// IncSQL SQL 查询 +1（SQL 引擎 execTimed 调用）。
func (m *Registry) IncSQL() {
	m.sqlTotal.Add(1)
	m.sqlQPS.bump(m.now().Unix())
}

// IncKV KV 命令 +1（server 层 GET/SET/DEL 调用）。
func (m *Registry) IncKV() {
	m.kvTotal.Add(1)
	m.kvQPS.bump(m.now().Unix())
}

// IncSlow 慢查询 +1（recordSlow 命中阈值调用）。
func (m *Registry) IncSlow() { m.slowTotal.Add(1) }

// IncCommit 事务提交 +1（成功提交路径调用）。
func (m *Registry) IncCommit() { m.txnCommit.Add(1) }

// IncRollback 事务回滚 +1（成功回滚路径调用）。
func (m *Registry) IncRollback() { m.txnRollback.Add(1) }

// IncLocal 本节点数据访问 +1（本地读取/扫描路径调用）。
func (m *Registry) IncLocal() { m.localTotal.Add(1) }

// IncRemote 远端转发数据访问 +1（QUERY_REQ/QUERY_RESP 转发路径调用）。
func (m *Registry) IncRemote() { m.remoteTotal.Add(1) }

// ---- 采集源注入 ----

// SetConnSource 注入活跃连接数采集源（server.ActiveConns）。
func (m *Registry) SetConnSource(f func() int64) { m.conns = f }

// SetSlowThresholdSource 注入慢查询阈值采集源（sql.Engine.SlowQueryThreshold）。
func (m *Registry) SetSlowThresholdSource(f func() int64) { m.slowThreshold = f }

// SetStorageBytesSource 注入存储总数据量采集源（数据目录磁盘占用）。
func (m *Registry) SetStorageBytesSource(f func() int64) { m.storageBytes = f }

// SetRegionStatsSource 注入 region 分片状态采集源（sql.Engine.MetricsRegionStats）。
func (m *Registry) SetRegionStatsSource(f func() RegionStats) { m.regionStats = f }

// ---- 渲染 ----

// writeMetric 写一组 Prometheus 文本（HELP/TYPE + 指标行）。
func writeMetric(sb *strings.Builder, name, help, typ, labels string, val int64) {
	fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s %s\n%s%s %d\n",
		name, help, name, typ, name, labels, val)
}

// escapeLabelValue Prometheus 标签值转义（\" 与 \\）。
func escapeLabelValue(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return strings.ReplaceAll(s, "\"", "\\\"")
}

// Render 输出完整 Prometheus 文本格式指标。
func (m *Registry) Render() string {
	var sb strings.Builder
	nowSec := m.now().Unix()

	writeMetric(&sb, "openxdb_queries_total", "Total queries executed.", "counter",
		`{type="sql"}`, m.sqlTotal.Load())
	writeMetric(&sb, "openxdb_queries_total", "Total queries executed.", "counter",
		`{type="kv"}`, m.kvTotal.Load())
	writeMetric(&sb, "openxdb_qps", "Queries per second (current window).", "gauge",
		`{type="sql"}`, m.sqlQPS.value(nowSec))
	writeMetric(&sb, "openxdb_qps", "Queries per second (current window).", "gauge",
		`{type="kv"}`, m.kvQPS.value(nowSec))

	conns := int64(0)
	if m.conns != nil {
		conns = m.conns()
	}
	writeMetric(&sb, "openxdb_connections_active", "Current active TCP connections.", "gauge",
		"", conns)

	writeMetric(&sb, "openxdb_slow_query_total", "Total slow queries recorded.", "counter",
		"", m.slowTotal.Load())
	thr := int64(0)
	if m.slowThreshold != nil {
		thr = m.slowThreshold()
	}
	writeMetric(&sb, "openxdb_slow_query_threshold_ms", "Slow query threshold in milliseconds.", "gauge",
		"", thr)

	bytes := int64(0)
	if m.storageBytes != nil {
		bytes = m.storageBytes()
	}
	writeMetric(&sb, "openxdb_storage_bytes", "Total on-disk data size in bytes.", "gauge",
		"", bytes)

	var rs RegionStats
	if m.regionStats != nil {
		rs = m.regionStats()
	}
	writeMetric(&sb, "openxdb_region_count", "Total region count.", "gauge",
		"", rs.Total)
	states := make([]string, 0, len(rs.ByState))
	for st := range rs.ByState {
		states = append(states, st)
	}
	sort.Strings(states)
	for _, st := range states {
		writeMetric(&sb, "openxdb_region_state", "Regions by state.", "gauge",
			`{state="`+escapeLabelValue(st)+`"}`, rs.ByState[st])
	}
	writeMetric(&sb, "openxdb_region_local", "Regions physically local.", "gauge",
		"", rs.Local)
	writeMetric(&sb, "openxdb_region_remote", "Regions owned by remote nodes.", "gauge",
		"", rs.Remote)

	writeMetric(&sb, "openxdb_txn_commit_total", "Transactions committed.", "counter",
		"", m.txnCommit.Load())
	writeMetric(&sb, "openxdb_txn_rollback_total", "Transactions rolled back.", "counter",
		"", m.txnRollback.Load())
	writeMetric(&sb, "openxdb_query_local_total", "Data accesses served locally.", "counter",
		"", m.localTotal.Load())
	writeMetric(&sb, "openxdb_query_remote_total", "Data accesses forwarded to remote nodes.", "counter",
		"", m.remoteTotal.Load())
	return sb.String()
}

// Handler 返回 /metrics HTTP 处理器（Prometheus 文本格式，text/plain v0.0.4）。
func (m *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, m.Render())
	})
}

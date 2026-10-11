// M13 子功能三：监控状态查询（aggregated health/status view）。
//
// STATUS 命令聚合节点存活/角色、region 分布、存储用量、慢查询摘要、
// 事务计数、连接数与 metrics 要点，输出人类可读表格（默认）或 JSON
// （STATUS JSON）。状态查询为只读快照：全部数据源为内存原子读/轻量
// 注入函数，不发起网络请求、不加锁等待，不阻塞正常请求；任一数据源
// 未装配或采集失败仅记入 Warnings 单项降级，不导致整体失败。
package server

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/metrics"
)

// nodeStatus 节点存活/角色摘要。
type nodeStatus struct {
	ID    string `json:"id"`
	Addr  string `json:"addr,omitempty"`
	Role  string `json:"role"`
	State string `json:"state"`
}

// regionStatus region 分布摘要（总数/各状态/本地远端）。
type regionStatus struct {
	Total   int64            `json:"total"`
	ByState map[string]int64 `json:"by_state,omitempty"`
	Local   int64            `json:"local"`
	Remote  int64            `json:"remote"`
}

// slowStatus 慢查询摘要（阈值 + 最近保留条数 + 前 5 条明细）。
type slowStatus struct {
	Enabled      bool     `json:"enabled"`
	ThresholdMs  int64    `json:"threshold_ms"`
	RecentCount  int      `json:"recent_count"`
	Recent       []string `json:"recent,omitempty"`
}

// qpsStatus QPS 摘要（当前秒窗）。
type qpsStatus struct {
	SQL int64 `json:"sql"`
	KV  int64 `json:"kv"`
}

// statusReport 聚合状态快照（STATUS 输出模型；字段与 T26_m13_status.md 对齐）。
type statusReport struct {
	Server       string       `json:"server"`
	Status       string       `json:"status"` // ok | degraded
	Time         string       `json:"time"`
	Nodes        []nodeStatus `json:"nodes,omitempty"`
	Regions      regionStatus `json:"regions"`
	StorageBytes int64        `json:"storage_bytes"`
	Slow         slowStatus   `json:"slow_queries"`
	TxnCommit    int64        `json:"txn_commit_total"`
	TxnRollback  int64        `json:"txn_rollback_total"`
	Connections  int64        `json:"active_connections"`
	QPS          qpsStatus    `json:"qps"`
	Warnings     []string     `json:"warnings,omitempty"`
}

// doStatus 聚合当前服务状态：STATUS [JSON]。只读不阻塞；数据源降级不报错。
func (s *Server) doStatus(toks []string) (string, error) {
	jsonMode := false
	if len(toks) > 1 {
		switch strings.ToUpper(toks[1]) {
		case "JSON", "--JSON":
			jsonMode = true
		}
	}
	rep := s.buildStatus()
	if jsonMode {
		b, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return "", fmt.Errorf("ERR status: marshal: %v", err)
		}
		return string(b), nil
	}
	return renderStatusText(rep), nil
}

// buildStatus 采集各数据源并聚合（单项降级：失败/未装配记 Warning，不整体失败）。
func (s *Server) buildStatus() *statusReport {
	rep := &statusReport{
		Server: "openxdb",
		Time:   time.Now().Format(time.RFC3339),
		Status: "ok",
		Regions: regionStatus{ByState: map[string]int64{}},
		Slow:   slowStatus{Enabled: false},
	}

	// 1) 节点存活/角色：集群注册表快照；空注册表回退本节点（单机兜底）。
	if s.SQL != nil {
		nodes := s.SQL.ClusterNodes()
		if len(nodes) == 0 {
			if self, ok := s.SQL.ClusterSelfNode(); ok {
				nodes = append(nodes, self)
			}
		}
		for _, n := range nodes {
			rep.Nodes = append(rep.Nodes, nodeStatus{ID: n.ID, Addr: n.Addr, Role: string(n.Role), State: string(n.State)})
		}
		if len(rep.Nodes) == 0 {
			rep.Warnings = append(rep.Warnings, "cluster nodes unavailable (SQL engine without cluster manager)")
			rep.Status = "degraded"
		}
	} else {
		rep.Warnings = append(rep.Warnings, "SQL engine unavailable (nodes/slow queries omitted)")
		rep.Status = "degraded"
	}

	// 2) region 分布：优先 SQL 路由表直采（与 SHOW STATS 口径一致）；
	//    SQL 不可用时回退 metrics 注入源；均不可用输出 0 并降级。
	if s.SQL != nil {
		rs := s.SQL.MetricsRegionStats()
		rep.Regions.Total = rs.Total
		rep.Regions.ByState = rs.ByState
		rep.Regions.Local = rs.Local
		rep.Regions.Remote = rs.Remote
	} else if s.m != nil {
		rs := s.m.Snapshot().Regions
		rep.Regions.Total = rs.Total
		rep.Regions.ByState = rs.ByState
		rep.Regions.Local = rs.Local
		rep.Regions.Remote = rs.Remote
	}
	if rep.Regions.ByState == nil {
		rep.Regions.ByState = map[string]int64{}
	}

	// 3) metrics 要点（连接数/存储/事务/QPS/慢查询累计）：注册表快照。
	var ms metrics.Snapshot
	if s.m != nil {
		ms = s.m.Snapshot()
	} else {
		rep.Warnings = append(rep.Warnings, "metrics registry unavailable (counters zero)")
		rep.Status = "degraded"
	}
	rep.Connections = ms.ActiveConns
	rep.StorageBytes = ms.StorageBytes
	rep.TxnCommit = ms.TxnCommit
	rep.TxnRollback = ms.TxnRollback
	rep.QPS = qpsStatus{SQL: ms.SQLQPS, KV: ms.KVQPS}

	// 4) 慢查询摘要：阈值与最近记录来自 SQL 引擎（实时），累计数来自 metrics。
	if s.SQL != nil {
		rep.Slow.ThresholdMs = s.SQL.SlowQueryThreshold()
		rep.Slow.Enabled = rep.Slow.ThresholdMs > 0
		recs := s.SQL.SlowQueries()
		rep.Slow.RecentCount = len(recs)
		n := len(recs)
		if n > 5 {
			n = 5
		}
		for _, r := range recs[len(recs)-n:] {
			rep.Slow.Recent = append(rep.Slow.Recent,
				fmt.Sprintf("%s (%dms)", strings.TrimSpace(r.SQL), r.Duration))
		}
	}
	if rep.Slow.RecentCount == 0 {
		rep.Slow.RecentCount = int(ms.SlowQueryTotal)
	}

	return rep
}

// renderStatusText 渲染人类可读表格（key | value 行式，与 SHOW STATS 对齐）。
func renderStatusText(rep *statusReport) string {
	var sb strings.Builder
	sb.WriteString("OPENXDB STATUS (aggregated snapshot)\n")
	fmt.Fprintf(&sb, "status        | %s\n", rep.Status)
	fmt.Fprintf(&sb, "time          | %s\n", rep.Time)
	fmt.Fprintf(&sb, "connections   | %d\n", rep.Connections)
	fmt.Fprintf(&sb, "qps_sql       | %d\n", rep.QPS.SQL)
	fmt.Fprintf(&sb, "qps_kv        | %d\n", rep.QPS.KV)
	fmt.Fprintf(&sb, "storage_bytes | %d\n", rep.StorageBytes)

	// 节点
	sb.WriteString("nodes:\n")
	if len(rep.Nodes) == 0 {
		sb.WriteString("  (none)\n")
	} else {
		for _, n := range rep.Nodes {
			addr := n.Addr
			if addr == "" {
				addr = "-"
			}
			fmt.Fprintf(&sb, "  %-16s %-6s %-6s %s\n", n.ID, n.Role, n.State, addr)
		}
	}

	// region 分布（按状态名排序输出稳定）
	sb.WriteString("regions:\n")
	fmt.Fprintf(&sb, "  total=%d local=%d remote=%d\n", rep.Regions.Total, rep.Regions.Local, rep.Regions.Remote)
	if len(rep.Regions.ByState) > 0 {
		states := make([]string, 0, len(rep.Regions.ByState))
		for st := range rep.Regions.ByState {
			states = append(states, st)
		}
		sort.Strings(states)
		var parts []string
		for _, st := range states {
			parts = append(parts, fmt.Sprintf("%s=%d", st, rep.Regions.ByState[st]))
		}
		fmt.Fprintf(&sb, "  states: %s\n", strings.Join(parts, " "))
	}

	// 慢查询摘要
	sb.WriteString("slow_queries:\n")
	fmt.Fprintf(&sb, "  threshold_ms=%d enabled=%v recent=%d\n",
		rep.Slow.ThresholdMs, rep.Slow.Enabled, rep.Slow.RecentCount)
	for _, r := range rep.Slow.Recent {
		fmt.Fprintf(&sb, "  - %s\n", r)
	}

	// 事务
	fmt.Fprintf(&sb, "transactions  | commit=%d rollback=%d\n", rep.TxnCommit, rep.TxnRollback)

	// 降级警告
	if len(rep.Warnings) > 0 {
		sb.WriteString("warnings:\n")
		for _, w := range rep.Warnings {
			fmt.Fprintf(&sb, "  - %s\n", w)
		}
	}
	return sb.String()
}

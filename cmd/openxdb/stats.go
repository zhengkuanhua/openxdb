// 监控指标：openxdb stats（T20 运维工具链）。
//
// 对本地或远端（--addr host:port，默认 127.0.0.1:7788）服务拉取关键运行指标，
// 输出对齐表格或 JSON（--json）。指标来源：
//
//	连接数、binlog 位点  ← SHOW STATS（server 会话计数 + db 注入，T20 新增）
//	表数量               ← SHOW TABLES 行数
//	region 数量与路由分布 ← SHOW REGION ROUTES 行数 + 按归属节点分布
//	慢查询数量           ← SHOW SLOWQUERIES 行数
//
// 二次采样（--interval <sec>）：间隔后再次拉取，计算 binlog 位点增长。
// 失败（连接/协议/解析错误）退出码 2，成功退出码 0。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// statLine 解析文本表的一行数据：按 " | " 分列（与 server 文本表渲染对齐）。
func statLine(line string) []string {
	return strings.Split(line, " | ")
}

// statsSnapshot 单次采样结果。
type statsSnapshot struct {
	Connections  int64            `json:"connection_count"`
	Tables       int64            `json:"table_count"`
	Regions      int64            `json:"region_count"`
	RouteByNode  map[string]int64 `json:"route_distribution,omitempty"`
	SlowQueries  int64            `json:"slow_query_count"`
	BinlogLSN    uint64           `json:"binlog_lsn"`
	CollectedAt  string           `json:"collected_at"`
	BinlogGrowth int64            `json:"binlog_growth,omitempty"`
}

// cmdStats 拉取并输出服务运行指标。
func cmdStats(args []string) error {
	addr := "127.0.0.1:7788"
	if a, ok := flagValue(args, "--addr"); ok && a != "" {
		addr = a
	}
	interval := int64(0)
	if iv, ok := flagValue(args, "--interval"); ok && iv != "" {
		n, err := strconv.ParseInt(iv, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("invalid --interval value: %s (正整数秒)", iv)
		}
		interval = n
	}
	snap, err := collectStats(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "openxdb stats: %v\n", err)
		os.Exit(2)
		return nil
	}
	if interval > 0 {
		time.Sleep(time.Duration(interval) * time.Second)
		snap2, err2 := collectStats(addr)
		if err2 != nil {
			fmt.Fprintf(os.Stderr, "openxdb stats: 二次采样失败: %v\n", err2)
			os.Exit(2)
			return nil
		}
		if snap2.BinlogLSN >= snap.BinlogLSN {
			snap.BinlogGrowth = int64(snap2.BinlogLSN - snap.BinlogLSN)
		} else {
			snap.BinlogGrowth = -int64(snap.BinlogLSN - snap2.BinlogLSN)
		}
		snap.BinlogLSN = snap2.BinlogLSN
		snap.CollectedAt = snap2.CollectedAt
	}
	if flagBool(args, "--json") {
		b, err := json.MarshalIndent(snap, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
	} else {
		printStatsText(addr, snap)
	}
	return nil
}

// collectStats 对服务执行一轮指标采集（4 条协议查询）。
func collectStats(addr string) (*statsSnapshot, error) {
	snap := &statsSnapshot{RouteByNode: map[string]int64{}, CollectedAt: time.Now().Format(time.RFC3339)}
	// SHOW STATS：连接数 + binlog 位点（同时校验服务端版本能力）
	resp, err := onlineQuery(addr, "SHOW STATS", 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("连接服务 %s 失败: %v", addr, err)
	}
	if strings.HasPrefix(resp, "ERR") {
		return nil, fmt.Errorf("服务不支持 SHOW STATS（需要 T20 版本）: %s", resp)
	}
	statsHasRegions := false
	for _, line := range strings.Split(resp, "\n") {
		cells := statLine(line)
		if len(cells) != 2 {
			continue
		}
		metric, val := strings.TrimSpace(cells[0]), strings.TrimSpace(cells[1])
		switch metric {
		case "connection_count":
			snap.Connections, _ = strconv.ParseInt(val, 10, 64)
		case "region_count":
			snap.Regions, _ = strconv.ParseInt(val, 10, 64)
			statsHasRegions = true
		case "binlog_lsn":
			snap.BinlogLSN, _ = strconv.ParseUint(val, 10, 64)
		}
	}
	// SHOW TABLES：表数量
	if resp, err = onlineQuery(addr, "SHOW TABLES", 5*time.Second); err != nil {
		return nil, fmt.Errorf("SHOW TABLES 失败: %v", err)
	}
	snap.Tables = int64(parseRowCount(resp))
	// SHOW REGION ROUTES：region 数量与路由分布（node 列为第 6 列）
	if resp, err = onlineQuery(addr, "SHOW REGION ROUTES", 5*time.Second); err != nil {
		return nil, fmt.Errorf("SHOW REGION ROUTES 失败: %v", err)
	}
	if strings.HasPrefix(resp, "ERR") {
		return nil, fmt.Errorf("SHOW REGION ROUTES 返回错误: %s", resp)
	}
	lines := strings.Split(resp, "\n")
	// 跳过表头与分隔线：表头行以 "region_id" 开头
	started := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !started {
			if strings.HasPrefix(trimmed, "region_id") {
				started = true
			}
			continue
		}
		if strings.HasPrefix(trimmed, "---") || strings.HasPrefix(trimmed, "(N rows)") {
			continue
		}
		cells := statLine(trimmed)
		if len(cells) < 6 {
			continue
		}
		node := strings.TrimSpace(cells[5])
		if node == "" {
			node = "(local)"
		}
		snap.RouteByNode[node]++
	}
	if !started {
		// 空路由表：无 "region_id" 表头时无路由分布可聚合
		snap.RouteByNode = nil
	}
	// region_count 以 SHOW STATS 为准（单节点/未启用集群时 SHOW REGION ROUTES 无表头行）；
	// 服务端未提供 region_count 时兜底用 SHOW REGION ROUTES 行数。
	if !statsHasRegions {
		snap.Regions = int64(parseRowCount(resp))
	}
	// SHOW SLOWQUERIES：慢查询数量
	if resp, err = onlineQuery(addr, "SHOW SLOWQUERIES", 5*time.Second); err != nil {
		return nil, fmt.Errorf("SHOW SLOWQUERIES 失败: %v", err)
	}
	snap.SlowQueries = int64(parseRowCount(resp))
	return snap, nil
}

// printStatsText 打印人类可读指标表格。
func printStatsText(addr string, snap *statsSnapshot) {
	fmt.Printf("OpenXDB stats: %s (collected at %s)\n", addr, snap.CollectedAt)
	fmt.Printf("%-18s %s\n", "metric", "value")
	fmt.Printf("%-18s %s\n", strings.Repeat("-", 18), strings.Repeat("-", 12))
	fmt.Printf("%-18s %d\n", "connections", snap.Connections)
	fmt.Printf("%-18s %d\n", "tables", snap.Tables)
	fmt.Printf("%-18s %d\n", "regions", snap.Regions)
	fmt.Printf("%-18s %d\n", "slow_queries", snap.SlowQueries)
	fmt.Printf("%-18s %d\n", "binlog_lsn", snap.BinlogLSN)
	if snap.BinlogGrowth != 0 {
		fmt.Printf("%-18s %d\n", "binlog_growth", snap.BinlogGrowth)
	}
	if len(snap.RouteByNode) > 0 {
		fmt.Println("route distribution:")
		for node, n := range snap.RouteByNode {
			fmt.Printf("  %-18s %d\n", node, n)
		}
	}
}

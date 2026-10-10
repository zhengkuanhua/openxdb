// 监控工具 stats 测试（T20 运维工具链）。
package main

import (
	"net"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/server"
)

// startTestServer 启动一个真实本地服务（随机端口），返回地址与关闭函数。
// 装配与 cmdStart 一致：注入 SHOW STATS 统计源。
func startTestServer(t *testing.T) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := server.New(d.Txn)
	s.SQL = d.SQL
	d.SQL.SetStatsSources(s.ActiveConns, d.BinlogLSN)
	done := make(chan struct{})
	go func() {
		_ = s.ServeListener(ln)
		close(done)
	}()
	closeFn := func() {
		_ = ln.Close()
		<-done
		_ = d.Close()
	}
	return ln.Addr().String(), closeFn
}

// TestCollectStatsLocalServer：本地服务上能采集到表格所需全部指标。
func TestCollectStatsLocalServer(t *testing.T) {
	addr, closeFn := startTestServer(t)
	defer closeFn()

	snap, err := collectStats(addr)
	if err != nil {
		t.Fatalf("collectStats: %v", err)
	}
	if snap.Connections < 1 {
		t.Fatalf("connection_count should be >=1 (采样连接自身计入), got %d", snap.Connections)
	}
	if snap.Tables != 0 {
		t.Fatalf("table_count should be 0 on empty db, got %d", snap.Tables)
	}
	if snap.Regions < 0 || snap.SlowQueries < 0 {
		t.Fatalf("region/slow query counts should be >=0, got regions=%d slow=%d", snap.Regions, snap.SlowQueries)
	}
	if snap.BinlogLSN != 0 {
		t.Fatalf("binlog_lsn should be 0 without replication, got %d", snap.BinlogLSN)
	}
}

// TestCollectStatsWithTable：建表后 table_count 增长；单节点无显式 region，
// region_count 与 SHOW REGION ROUTES 语义一致（隐式单 region 不计入 AllRegions）。
func TestCollectStatsWithTable(t *testing.T) {
	addr, closeFn := startTestServer(t)
	defer closeFn()

	if resp, err := onlineQuery(addr, "CREATE TABLE t1 (id INT)", 5*time.Second); err != nil {
		t.Fatalf("create table: %v", err)
	} else if resp == "" || resp[:3] == "ERR" {
		t.Fatalf("create table failed: %s", resp)
	}
	snap, err := collectStats(addr)
	if err != nil {
		t.Fatalf("collectStats: %v", err)
	}
	if snap.Tables != 1 {
		t.Fatalf("table_count should be 1, got %d", snap.Tables)
	}
	if snap.Regions < 0 {
		t.Fatalf("region_count should be >=0 (单节点无显式 region 时为 0), got %d", snap.Regions)
	}
}

// TestCollectStatsConnectionRefused：服务不可达时返回错误。
func TestCollectStatsConnectionRefused(t *testing.T) {
	// 127.0.0.1:1 几乎必然不可达
	if _, err := collectStats("127.0.0.1:1"); err == nil {
		t.Fatalf("collectStats to closed port should error")
	}
}

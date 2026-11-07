// P9 HA（D4.5 Phase 9 / 候选 B）：重启归属收敛端到端测试（真实多节点 + 崩溃重启）。
//
// 断言"修复 NEW-1 不得把正常重启节点锁死"这一回归面：重启节点（本地旧视图
// 仍写着自己持 region）在有权威对端可达时，归属经权威路由视图确认后必须自动
// 恢复可写，且其写入在重启前后都落到同一份数据上。
//
// 补充说明："重启后对端全不可达 → 归属无从确认 → 拒写（ErrOwnershipPending）"
// 这一支无法在单机同 IP 共置环境稳定构造（需 ASSIGN/failover 后对端下线，
// 归属推送与迁移时序不可控），改由 pkg/db 内单元测试覆盖：
//   - TestP9RestartStaleOwnerPendingWithoutAnyTerm（重启本地仍持归属 → 待确认 → 拒写）
//   - TestP9RestartPendingPersisted（待确认状态跨重启保持）
//
package db_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// freeAddr 预留并释放一个本机端口，返回可复用的固定监听地址（重启需同址）。
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// p9SeedOwnerNode A 建表写入（region 归属落 A），三节点互注册并等待 UP。
func p9SeedOwnerNode(t *testing.T, dA, dB, dC *db.DB) {
	t.Helper()
	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, dB, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, dC, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	for i := 1; i <= 3; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	nodes := []*db.DB{dA, dB, dC}
	for _, d := range nodes {
		for _, peer := range nodes {
			if peer.Cluster.SelfAddr == d.Cluster.SelfAddr {
				continue
			}
			mustSQL(t, d, "ADD NODE '"+peer.Cluster.SelfAddr+"'")
		}
	}
	for _, d := range nodes {
		speedupHeartbeat(t, d)
	}
	waitEverUp(t, dA, dB.Cluster.SelfID)
	waitEverUp(t, dA, dC.Cluster.SelfID)
}

// TestD45P9RestartOwnerConfirmedByPeersE2E 重启节点归属经权威视图确认后恢复可写。
func TestD45P9RestartOwnerConfirmedByPeersE2E(t *testing.T) {
	addrA := freeAddr(t)
	dirA := t.TempDir()
	dA := openNodeAt(t, dirA, "node-a", addrA)
	dB, _ := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })
	dC, _ := openNode(t, "node-c")
	t.Cleanup(func() { _ = dC.Close() })

	p9SeedOwnerNode(t, dA, dB, dC)
	// 重启前 A 是以 region owner 身份写入的（本地视图 local=true）。
	mustSQL(t, dA, "UPDATE users SET name = 'before-restart' WHERE id = 1")

	// 崩溃重启：同址重开同一数据目录。
	if err := dA.Close(); err != nil {
		t.Fatalf("close A: %v", err)
	}
	dA2 := openNodeAt(t, dirA, "node-a-restart", addrA)
	t.Cleanup(func() { _ = dA2.Close() })

	// 重启后归属由权威视图确认（对端可达）：最终必须恢复可写，不得永久只读。
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = dA2.AllowWrite()
		if lastErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("restarted owner never regained write permission: %v", lastErr)
	}
	if dA2.OwnershipPending() {
		t.Fatalf("ownership should be confirmed after authoritative route view")
	}
	mustSQL(t, dA2, "UPDATE users SET name = 'after-restart' WHERE id = 1")
	wantIDs(t, dA2, "SELECT id FROM users ORDER BY id", 1, 2, 3)
}

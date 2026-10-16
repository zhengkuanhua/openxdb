package db_test

// T35 分布式备份一致性集成测试（db 层，双节点集群）。
// 覆盖：分布式 BACKUP（协调者 -> 各节点快照、manifest 一致性标记）、
// 备份后写入在 RESTORE 对齐全局 LSN 后不可见、分布式 RESTORE 各节点恢复、
// 节点离线时备份/恢复失败路径、单机 BACKUP/RESTORE 无 manifest 兼容。
//
// 注：并行端口竞争用 -count=1 -p 1 串行验证。

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// pathForSQL 将 Windows 绝对路径转为正斜杠，避免 SQL 字符串内反斜杠转义问题。
func pathForSQL(p string) string { return strings.ReplaceAll(p, "\\", "/") }

// waitReplRows 轮询等待复制收敛：目标节点上 SQL 查询结果达到期望行数。
func waitReplRows(t *testing.T, d *db.DB, stmt string, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if res, err := d.SQL.Execute(stmt); err == nil && len(res.Rows) == n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	res, err := d.SQL.Execute(stmt)
	if err != nil {
		t.Fatalf("waitReplRows %q: err=%v", stmt, err)
	}
	t.Fatalf("waitReplRows %q: want %d rows, got %d", stmt, n, len(res.Rows))
}

func TestT35DistBackupRestoreConsistency(t *testing.T) {
	dir := t.TempDir()
	dA, _ := openNode(t, "node-a")
	defer dA.Close()
	dB, addrB := openNode(t, "node-b")
	defer dB.Close()

	// 启用 M2 复制：A 为 master、B 为 follower，A 的写入（目录 + 数据行）复制收敛到 B，
	// 保证备份时各节点本地状态对应同一全局时间点。
	if err := dA.StartReplication(":0"); err != nil {
		t.Fatalf("A StartReplication: %v", err)
	}
	mAddr, _ := dA.ReplicationStatus()
	if err := dB.StartFollower(mAddr); err != nil {
		t.Fatalf("B StartFollower: %v", err)
	}

	// 协调者 A 统一建表并写入（DDL 目录 + 数据行经复制收敛，规避复制物理覆盖 metaKey 冲突）。
	mustSQL(t, dA, "CREATE TABLE ta (id INT PRIMARY KEY, v TEXT)")
	for i := 1; i <= 3; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO ta VALUES (%d, 'a%d')", i, i))
	}
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	mustSQL(t, dA, "CREATE TABLE tb (id INT PRIMARY KEY, v TEXT)")
	for i := 1; i <= 4; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO tb VALUES (%d, 'b%d')", i, i))
	}
	// 等待复制追平：B 上 ta/tb 目录与数据均收敛后再备份。
	waitReplRows(t, dB, "SELECT id FROM ta ORDER BY id", 3)
	waitReplRows(t, dB, "SELECT id FROM tb ORDER BY id", 4)

	bak := filepath.Join(dir, "cluster.bak")
	res := mustSQL(t, dA, "BACKUP TO '"+pathForSQL(bak)+"'")
	if len(res.Rows) != 1 {
		t.Fatalf("BACKUP: want 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0][1].I; got != 2 {
		t.Fatalf("BACKUP nodes: want 2, got %d", got)
	}
	if got := res.Rows[0][0].S; !strings.HasSuffix(got, ".manifest") {
		t.Fatalf("BACKUP path should point to manifest, got %q", got)
	}
	// manifest 与节点备份文件落盘：协调者本地 = <path>，远端 = <path>.node-<id>。
	if _, err := os.Stat(bak + ".manifest"); err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	if _, err := os.Stat(bak); err != nil {
		t.Fatalf("coordinator local backup missing: %v", err)
	}
	if _, err := os.Stat(bak + ".node-" + dB.Cluster.SelfID); err != nil {
		t.Fatalf("remote node backup file .node-%s missing: %v", dB.Cluster.SelfID, err)
	}

	// 备份后的写入：应被全局 LSN 对齐丢弃（各节点恢复到备份点）。
	mustSQL(t, dA, "INSERT INTO ta VALUES (9, 'after')")
	mustSQL(t, dB, "INSERT INTO tb VALUES (9, 'after')")

	// 分布式 RESTORE：各节点按 manifest 恢复到全局 LSN（备份点后的 9 不可见）。
	mustSQL(t, dA, "RESTORE FROM '"+pathForSQL(bak)+"'")
	if _, err := dA.SQL.Execute("SELECT id FROM ta"); err != nil {
		t.Fatalf("SELECT ta after restore failed: %v", err)
	}
	wantIDs(t, dA, "SELECT id FROM ta ORDER BY id", 1, 2, 3)
	wantIDs(t, dA, "SELECT id FROM tb ORDER BY id", 1, 2, 3, 4)
	wantIDs(t, dB, "SELECT id FROM ta ORDER BY id", 1, 2, 3)
	wantIDs(t, dB, "SELECT id FROM tb ORDER BY id", 1, 2, 3, 4)
	// 备份点后的写入在恢复后不可见。
	if rows, err := dA.SQL.Execute("SELECT id FROM ta WHERE id=9"); err != nil || len(rows.Rows) != 0 {
		t.Fatalf("A ta(9) should be discarded after restore, err=%v rows=%d", err, len(rows.Rows))
	}
	if rows, err := dB.SQL.Execute("SELECT id FROM tb WHERE id=9"); err != nil || len(rows.Rows) != 0 {
		t.Fatalf("B tb(9) should be discarded after restore, err=%v rows=%d", err, len(rows.Rows))
	}
}

func TestT35DistBackupOfflineNodeFails(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	defer dA.Close()
	dB, addrB := openNode(t, "node-b")
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	dB.Close()

	bak := filepath.Join(t.TempDir(), "x.bak")
	_, err := dA.SQL.Execute("BACKUP TO '" + pathForSQL(bak) + "'")
	if err == nil {
		t.Fatalf("BACKUP with offline node should fail")
	}
	if !strings.Contains(err.Error(), "offline") && !strings.Contains(err.Error(), "down") && !strings.Contains(err.Error(), "refused") && !strings.Contains(err.Error(), "dial") {
		t.Fatalf("BACKUP offline error mismatch: %v", err)
	}
	// 失败后不应产出 manifest。
	if _, serr := os.Stat(bak + ".manifest"); serr == nil {
		t.Fatalf("failed BACKUP should not write manifest")
	}
}

func TestT35DistRestoreOfflineNodeFails(t *testing.T) {
	dir := t.TempDir()
	dA, _ := openNode(t, "node-a")
	defer dA.Close()
	dB, addrB := openNode(t, "node-b")
	defer dB.Close()
	mustSQL(t, dA, "CREATE TABLE ta (id INT PRIMARY KEY, v TEXT)")
	mustSQL(t, dA, "INSERT INTO ta VALUES (1, 'x')")
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")

	bak := filepath.Join(dir, "c.bak")
	mustSQL(t, dA, "BACKUP TO '"+pathForSQL(bak)+"'")
	dB.Close()

	_, err := dA.SQL.Execute("RESTORE FROM '" + pathForSQL(bak) + "'")
	if err == nil {
		t.Fatalf("RESTORE with offline node should fail")
	}
	if !strings.Contains(err.Error(), "offline") && !strings.Contains(err.Error(), "down") && !strings.Contains(err.Error(), "refused") && !strings.Contains(err.Error(), "dial") {
		t.Fatalf("RESTORE offline error mismatch: %v", err)
	}
}

func TestT35SingleNodeBackupCompat(t *testing.T) {
	dir := t.TempDir()
	dA, _ := openNode(t, "node-a")
	defer dA.Close()
	mustSQL(t, dA, "CREATE TABLE s1 (id INT PRIMARY KEY, v TEXT)")
	for i := 1; i <= 2; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO s1 VALUES (%d, 'v%d')", i, i))
	}
	bak := filepath.Join(dir, "single.bak")
	mustSQL(t, dA, "BACKUP TO '"+pathForSQL(bak)+"'")
	// 单机路径：不生成 manifest。
	if _, err := os.Stat(bak + ".manifest"); err == nil {
		t.Fatalf("single-node backup should not create manifest")
	}
	if _, err := os.Stat(bak); err != nil {
		t.Fatalf("single-node backup file missing: %v", err)
	}

	mustSQL(t, dA, "DROP TABLE s1")
	mustSQL(t, dA, "RESTORE FROM '"+pathForSQL(bak)+"'")
	wantIDs(t, dA, "SELECT id FROM s1 ORDER BY id", 1, 2)
}

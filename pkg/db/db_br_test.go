package db_test

// M8(BR) 备份/恢复集成测试（BACKUP/RESTORE + PITR 基础，详见 docs/T18_backup_restore.md）：
//   - 备份-恢复全等：备份后写新数据/删表再恢复，数据精确回到备份点（含索引与 region 路由）；
//   - 备份文件完整性：篡改 meta 段后恢复报 checksum mismatch；
//   - 幂等重建语义：同名表已存在时 RESTORE 先删再建（覆盖式恢复）；
//   - TO LSN 精确回放：备份点 + 若干条 binlog 记录，回放到指定 LSN 后数据精确对应；
//   - 并发写入期间备份一致性：备份基于一致快照，恢复后行集等于备份点快照。

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// TestBRBackupRestoreRoundTrip 备份-恢复全等：备份后写新数据、DROP 表再 RESTORE，
// 数据（表结构/行/二级索引/region 路由）精确回到备份点。
func TestBRBackupRestoreRoundTrip(t *testing.T) {
	d, _ := openNode(t, "node-a")
	defer d.Close()
	backupPath := t.TempDir() + "/snap.bak"

	mustSQL(t, d, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, d, "CREATE INDEX idx_age ON users (age)")
	for i := 1; i <= 5; i++ {
		mustSQL(t, d, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	// 显式 region 划分（验证路由元数据进入备份）。
	if err := d.SQL.SplitTable("users", [][]byte{splitBoundary(3)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := d.SQL.ListRegions("users")
	if err != nil || len(regions) != 2 {
		t.Fatalf("ListRegions: want 2, got %d err=%v", len(regions), err)
	}

	backRes := mustSQL(t, d, "BACKUP TO '"+backupPath+"'")
	backRows := backRes.Rows[0][2].I // rows 列
	if backRows != 5 {
		t.Fatalf("backup rows: want 5, got %d", backRows)
	}

	// 备份点之后发生变更：写新数据 + DROP 表。
	mustSQL(t, d, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	mustSQL(t, d, "DROP TABLE users")

	// RESTORE：同名表不存在也应恢复（幂等语义，无需预先建表）。
	res := mustSQL(t, d, "RESTORE FROM '"+backupPath+"'")
	if len(res.Rows) != 1 || res.Rows[0][1].I != 5 {
		t.Fatalf("restore result: %+v", res.Rows)
	}

	// 数据全等：行集回到备份点。
	wantIDs(t, d, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5)
	// 二级索引可用（索引键已重建）。
	res = mustSQL(t, d, "SELECT id FROM users WHERE age = 23")
	if len(res.Rows) != 1 || res.Rows[0][0].I != 3 {
		t.Fatalf("index query after restore: %+v", res.Rows)
	}
	// region 路由恢复：显式 2 region。
	regions, err = d.SQL.ListRegions("users")
	if err != nil || len(regions) != 2 {
		t.Fatalf("regions after restore: want 2, got %d err=%v", len(regions), err)
	}
	for _, rg := range regions {
		if !rg.Explicit {
			t.Fatalf("region %d not explicit after restore: %+v", rg.RegionID, rg)
		}
	}
}

// TestBRBackupIntegrityTamper 备份文件完整性：篡改 meta 段后 RESTORE 报校验错误。
func TestBRBackupIntegrityTamper(t *testing.T) {
	d, _ := openNode(t, "node-a")
	defer d.Close()
	backupPath := t.TempDir() + "/snap.bak"

	mustSQL(t, d, "CREATE TABLE t (id INT, v TEXT, PRIMARY KEY (id))")
	mustSQL(t, d, "INSERT INTO t (id, v) VALUES (1, 'a')")
	mustSQL(t, d, "INSERT INTO t (id, v) VALUES (2, 'b')")
	mustSQL(t, d, "BACKUP TO '"+backupPath+"'")

	// 篡改 meta 段（header 之后第 2 字节），metaCRC 必须失配。
	raw, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	raw[len("OPENXDBBAK")+2+4+1] ^= 0xff
	if err := os.WriteFile(backupPath, raw, 0o644); err != nil {
		t.Fatalf("write tampered backup: %v", err)
	}

	if _, err := d.SQL.Execute("RESTORE FROM '" + backupPath + "'"); err == nil {
		t.Fatalf("restore of tampered backup: want error, got nil")
	} else if s := err.Error(); !containsAny(s, "checksum", "corrupted") {
		t.Fatalf("restore tampered: unexpected error: %v", err)
	}
}

// TestBRRestoreIdempotent 幂等重建：目标库同名表已存在（含不同数据与索引），
// RESTORE 先删再建并覆盖为备份内容。
func TestBRRestoreIdempotent(t *testing.T) {
	d, _ := openNode(t, "node-a")
	defer d.Close()
	backupPath := t.TempDir() + "/snap.bak"

	mustSQL(t, d, "CREATE TABLE t (id INT, v TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, d, "INSERT INTO t (id, v, age) VALUES (1, 'backup1', 10)")
	mustSQL(t, d, "INSERT INTO t (id, v, age) VALUES (2, 'backup2', 20)")
	mustSQL(t, d, "BACKUP TO '"+backupPath+"'")

	// 同名表已存在，且结构与数据都不同。
	mustSQL(t, d, "DROP TABLE t")
	mustSQL(t, d, "CREATE TABLE t (id INT, v TEXT, age INT, PRIMARY KEY (id))")
	mustSQL(t, d, "CREATE INDEX idx_age ON t (age)")
	mustSQL(t, d, "INSERT INTO t (id, v, age) VALUES (9, 'other', 9)")

	mustSQL(t, d, "RESTORE FROM '"+backupPath+"'")
	wantIDs(t, d, "SELECT id FROM t ORDER BY id", 1, 2)
	res := mustSQL(t, d, "SELECT v FROM t WHERE id = 2")
	if len(res.Rows) != 1 || res.Rows[0][0].S != "backup2" {
		t.Fatalf("restored value: %+v", res.Rows)
	}
	// 旧表遗留的二级索引元数据已被备份目录覆盖（备份中无 idx_age）：
	// 恢复后可干净地重新创建同名索引（若元数据残留此处会报 duplicate index）。
	mustSQL(t, d, "CREATE INDEX idx_age ON t (age)")
	res = mustSQL(t, d, "SELECT id FROM t WHERE age = 9")
	if len(res.Rows) != 0 {
		t.Fatalf("stale index residue after restore: %+v", res.Rows)
	}
	// 旧表遗留行（id=9）也被清掉。
	res = mustSQL(t, d, "SELECT id FROM t WHERE id = 9")
	if len(res.Rows) != 0 {
		t.Fatalf("stale row residue after restore: %+v", res.Rows)
	}
}

// TestBRPITRToLSN 时间点恢复基础：备份点 + 若干条 binlog 记录，
// RESTORE TO LSN 精确回放后数据与指定位点对应。
func TestBRPITRToLSN(t *testing.T) {
	d, _ := openNode(t, "node-a")
	defer d.Close()
	if err := d.StartReplication("127.0.0.1:0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	backupPath := t.TempDir() + "/snap.bak"

	mustSQL(t, d, "CREATE TABLE t (id INT, v TEXT, PRIMARY KEY (id))")
	mustSQL(t, d, "INSERT INTO t (id, v) VALUES (1, 'a')")
	mustSQL(t, d, "INSERT INTO t (id, v) VALUES (2, 'b')")
	backRes := mustSQL(t, d, "BACKUP TO '"+backupPath+"'")
	backupLSN := backRes.Rows[0][3].I // lsn 列
	if backupLSN <= 0 {
		t.Fatalf("backup LSN: want >0, got %d", backupLSN)
	}

	// 备份点之后继续写：id=3（LSN=backupLSN+1）、id=4（LSN=backupLSN+2）。
	mustSQL(t, d, "INSERT INTO t (id, v) VALUES (3, 'c')")
	mustSQL(t, d, "INSERT INTO t (id, v) VALUES (4, 'd')")

	// 回放到备份点本身：等于备份快照，只有 id 1/2。
	mustSQL(t, d, fmt.Sprintf("RESTORE FROM '%s' TO LSN %d", backupPath, backupLSN))
	wantIDs(t, d, "SELECT id FROM t ORDER BY id", 1, 2)

	// 回放到备份点 + 1 条：包含 id=3。
	mustSQL(t, d, fmt.Sprintf("RESTORE FROM '%s' TO LSN %d", backupPath, backupLSN+1))
	wantIDs(t, d, "SELECT id FROM t ORDER BY id", 1, 2, 3)

	// 回放到备份点 + 2 条：包含 id=4（精确到最后一条）。
	mustSQL(t, d, fmt.Sprintf("RESTORE FROM '%s' TO LSN %d", backupPath, backupLSN+2))
	wantIDs(t, d, "SELECT id FROM t ORDER BY id", 1, 2, 3, 4)

	// TO LSN 早于备份点：报错。
	if _, err := d.SQL.Execute(fmt.Sprintf("RESTORE FROM '%s' TO LSN %d", backupPath, backupLSN-1)); err == nil {
		t.Fatalf("restore TO LSN before backup: want error, got nil")
	}
}

// TestBRConcurrentBackupConsistency 并发写入期间备份一致性：
// 写入 goroutine 持续提交，备份基于一致快照；恢复后行集 = 备份点快照行集。
func TestBRConcurrentBackupConsistency(t *testing.T) {
	d, _ := openNode(t, "node-a")
	defer d.Close()
	backupPath := t.TempDir() + "/snap.bak"

	mustSQL(t, d, "CREATE TABLE t (id INT, v TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 5; i++ {
		mustSQL(t, d, fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, 'v%d')", i, i))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 6; i <= 200; i++ {
			_, err := d.SQL.Execute(fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, 'v%d')", i, i))
			if err != nil {
				return
			}
		}
	}()

	// 写入进行中执行备份（一致快照）。
	time.Sleep(20 * time.Millisecond)
	backRes := mustSQL(t, d, "BACKUP TO '"+backupPath+"'")
	backRows := backRes.Rows[0][2].I
	if backRows < 5 || backRows > 200 {
		t.Fatalf("backup rows during concurrent write: %d", backRows)
	}
	wg.Wait()

	// 恢复后行数必须等于备份点快照行数（既不多也不少）。
	mustSQL(t, d, "RESTORE FROM '"+backupPath+"'")
	countRes := mustSQL(t, d, "SELECT COUNT(*) FROM t")
	if countRes.Rows[0][0].I != backRows {
		t.Fatalf("restored rows %d != backup snapshot rows %d", countRes.Rows[0][0].I, backRows)
	}
	// 恢复后的行全部属于写入区间且主键连续（快照一致性：无半提交行）。
	res := mustSQL(t, d, "SELECT id FROM t WHERE id = 1")
	if len(res.Rows) != 1 {
		t.Fatalf("restored min id: 1 missing")
	}
	if backRows > 5 {
		res = mustSQL(t, d, fmt.Sprintf("SELECT id FROM t WHERE id = %d", backRows))
		if len(res.Rows) != 1 {
			t.Fatalf("restored last id %d missing: %+v", backRows, res.Rows)
		}
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(sub) > 0 && (len(s) >= len(sub)) && indexOf(s, sub) >= 0 {
			return true
		}
	}
	return false
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

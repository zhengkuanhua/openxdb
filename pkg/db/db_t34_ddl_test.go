package db_test

// T34 跨节点 DDL 集成测试（db 层，双节点集群）。
// 覆盖：ADD NODE 之后 CREATE/ALTER/DROP 目录变更广播到数据节点（目录一致）、
// 节点离线时 DDL 本地成功且广播记 pending、ReplayPendingDDL 对离线节点保留待重放。
//
// 注：行数据同步由复制链路（M2/T33）负责，本测试仅断言表目录一致性；
// 并行端口竞争用 -count=1 -p 1 串行验证。

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

func TestT34CrossNodeDDLBroadcast(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	defer dA.Close()
	dB, addrB := openNode(t, "node-b")
	defer dB.Close()

	// ADD NODE 之前提交的 DDL 不广播：B 目录不应有 users
	mustSQL(t, dA, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	if _, err := dB.SQL.Execute("SELECT * FROM users"); err == nil {
		t.Fatalf("B should not have pre-join table users")
	}

	// CREATE 广播：ADD NODE 之后建表，B 目录出现 t2
	mustSQL(t, dA, "CREATE TABLE t2 (id INT PRIMARY KEY, v TEXT)")
	res := mustSQL(t, dB, "SELECT * FROM t2")
	if len(res.Columns) == 0 || res.Columns[0] != "id" {
		t.Fatalf("B t2 columns after create broadcast: %+v", res.Columns)
	}

	// ALTER 广播：加列后 B 目录列集合一致
	mustSQL(t, dA, "ALTER TABLE t2 ADD COLUMN score INT")
	res = mustSQL(t, dB, "SELECT * FROM t2")
	hasScore := false
	for _, c := range res.Columns {
		if c == "score" {
			hasScore = true
		}
	}
	if !hasScore {
		t.Fatalf("B t2 missing score column after alter broadcast: %+v", res.Columns)
	}

	// DROP 广播：B 侧表目录移除
	mustSQL(t, dA, "DROP TABLE t2")
	if _, err := dB.SQL.Execute("SELECT * FROM t2"); err == nil || !strings.Contains(err.Error(), "table not exists") {
		t.Fatalf("B: t2 should be dropped, err=%v", err)
	}
}

func TestT34DDLOfflinePendingAndReplay(t *testing.T) {
	dA, _ := openNode(t, "node-a")
	defer dA.Close()
	dB, addrB := openNode(t, "node-b")
	defer dB.Close()

	mustSQL(t, dA, "CREATE TABLE t3 (id INT PRIMARY KEY, v TEXT)")
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")

	// B 离线后：A 侧 DDL 本地成功，广播失败进入 pending
	if err := dB.Close(); err != nil {
		t.Fatalf("close B: %v", err)
	}
	mustSQL(t, dA, "ALTER TABLE t3 ADD COLUMN x INT")

	rows, err := dA.Storage.Scan(storage.KeyRange{Start: []byte("m:ddl:p"), End: []byte("m:ddl:q")}, -1)
	if err != nil || len(rows) == 0 {
		t.Fatalf("A pending list: err=%v rows=%d", err, len(rows))
	}

	// 重放：B 仍离线，pending 保留
	n, err := dA.SQL.ReplayPendingDDL()
	if err != nil {
		t.Fatalf("ReplayPendingDDL: %v", err)
	}
	if n == 0 {
		t.Fatalf("replay should keep pending for offline node, got %d", n)
	}
	rows2, _ := dA.Storage.Scan(storage.KeyRange{Start: []byte("m:ddl:p"), End: []byte("m:ddl:q")}, -1)
	if len(rows2) == 0 {
		t.Fatalf("pending should remain after replay with node offline")
	}
}

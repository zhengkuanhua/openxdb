package main

// BR/PITR 端到端自动化演练测试：复用 runDrill 主流程，在临时数据目录上
// 以较小数据量跑完整「备份→PITR→灾难恢复」闭环，校验全部断言通过。
// 真实 RocksDB + WAL + binlog 链路（无 mock），失败时输出演练报告定位步骤。

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

func TestBRPITRDrillEndToEnd(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil && !errors.Is(err, db.ErrAlreadyInitialized) {
		t.Fatalf("init data dir: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()
	if err := d.StartReplication("127.0.0.1:0"); err != nil {
		t.Fatalf("start replication (binlog): %v", err)
	}

	md, err := runDrill(d.SQL, drillConfig{
		table:     "drill_users",
		backup:    filepath.ToSlash(filepath.Join(dir, "drill.bak")),
		rows:      50,
		pitrRows:  6,
		binlogLSN: d.BinlogLSN,
		startedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("BR/PITR drill failed: %v\n%s", err, md)
	}
}

package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestHAConfigPersistedRestart 复现并防回归 D4.5 Phase4 复测暴露的 NEW-2：
// SET PERSIST ha_epoch_enabled / ha_quorum_enabled 落盘后重启进程，
// 节点必须能正常启动（此前因持久化 ha_* 的 apply 早于 HA 装配，
// 在 db.Open 内部直接报 "requires cluster HA configuration" 退出）。
//
// 三步验证：
//  1. 首次打开后 SET PERSIST 两个开关，确认持久化文件里确有该键；
//  2. 关闭后重新 Open，必须成功（NEW-2 的核心回归点）；
//  3. 重启后开关值必须按持久化配置生效（不是被静默丢弃），
//     且未配置集群的环境下不会因此启用 quorum 仲裁（零回归）。
func TestHAConfigPersistedRestart(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("init dir: %v", err)
	}

	d0, err := Open(dir)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	for _, stmt := range []string{
		"SET PERSIST ha_epoch_enabled = 'on'",
		"SET PERSIST ha_quorum_enabled = 'on'",
	} {
		if _, err := d0.SQL.Execute(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := d0.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	// 前置断言：持久化文件确实写入了 ha_* 键（否则本用例无法覆盖 NEW-2）。
	raw, rerr := os.ReadFile(filepath.Join(dir, PersistedConfigFile))
	if rerr != nil {
		t.Fatalf("read persisted config: %v", rerr)
	}
	if !strings.Contains(string(raw), "ha_epoch_enabled") {
		t.Fatalf("persisted config missing ha_epoch_enabled: %s", raw)
	}

	// NEW-2 核心断言：带持久化 ha_* 的节点重启必须成功。
	d1, err := Open(dir)
	if err != nil {
		t.Fatalf("restart with persisted ha_* must succeed (NEW-2): %v", err)
	}
	defer d1.Close()

	// 持久化值必须真正生效：epoch 开关沿用持久化的 on，quorum 也读到 on。
	if !d1.EpochEnabled() {
		t.Fatalf("persisted ha_epoch_enabled=on not applied after restart")
	}
	if !d1.QuorumEnabled() {
		t.Fatalf("persisted ha_quorum_enabled=on not applied after restart")
	}
	// 零回归：无集群注册表时，开关本身不得让节点进入仲裁写路径；
	// 集群未启动的节点仍应能正常执行本地 SQL。
	if _, err := d1.SQL.Execute("CREATE TABLE persist_ok (id INT)"); err != nil {
		t.Fatalf("post-restart SQL after persisted ha_*: %v", err)
	}
}

// TestHAConfigPersistedRestartIdempotent 持久化 ha_* 的重复重启幂等：
// 连续两次重启都必须成功，避免"首次重置持久化键、二次又炸"的半修复。
func TestHAConfigPersistedRestartIdempotent(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("init dir: %v", err)
	}
	d0, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := d0.SQL.Execute("SET PERSIST ha_epoch_enabled = 'off'"); err != nil {
		t.Fatalf("SET PERSIST: %v", err)
	}
	if err := d0.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i := 0; i < 2; i++ {
		d, err := Open(dir)
		if err != nil {
			t.Fatalf("restart #%d with persisted ha_epoch_enabled=off: %v", i+1, err)
		}
		if d.EpochEnabled() {
			t.Fatalf("restart #%d: ha_epoch_enabled=off not applied", i+1)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("restart #%d close: %v", i+1, err)
		}
	}
}

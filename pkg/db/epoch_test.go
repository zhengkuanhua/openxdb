package db

// P0 HA 脑裂防护（最小防双主）测试：epoch 单调与持久化、升主仲裁、
// 写入 fencing 拒旧主、开关关闭零回归。设计见 docs/HA_BRAIN_SPLIT_DESIGN.md。

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestP0EpochMonotonicPersist epoch 单调发号 + epoch.meta 持久化防回退：
// 连续两次授权单调递增；落盘值可被重新加载作为单调下限；TSO 不可用时
// 以已知最大 epoch+1 兜底，不回退。
func TestP0EpochMonotonicPersist(t *testing.T) {
	d := openTestDB(t)
	if d.Epoch() != 0 {
		t.Fatalf("fresh db epoch = %d, want 0", d.Epoch())
	}
	d.SetEpochEnabled(true)

	e1 := d.acquireEpoch()
	e2 := d.acquireEpoch()
	if e1 == 0 {
		t.Fatalf("first epoch = 0, want > 0")
	}
	if e2 <= e1 {
		t.Fatalf("epoch not monotonic: e2=%d <= e1=%d", e2, e1)
	}

	// 落盘检查：epoch.meta 内容为最新已知最大 epoch（十进制 + \n）。
	b, err := os.ReadFile(d.epochFilePath)
	if err != nil {
		t.Fatalf("read epoch.meta: %v", err)
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || v != e2 {
		t.Fatalf("epoch.meta = %q (parsed %d, err=%v), want %d", b, v, err, e2)
	}

	// 重新加载：持久化值成为单调下限（重启防回退）。
	d2 := &DB{epochFilePath: d.epochFilePath}
	d2.loadEpochMeta()
	if d2.Epoch() != e2 {
		t.Fatalf("reloaded epoch = %d, want %d", d2.Epoch(), e2)
	}

	// TSO 为 nil（不可用）时的兜底发号：以已知最大 epoch+1 前进，不回退。
	e3 := d2.acquireEpoch()
	if e3 != e2+1 {
		t.Fatalf("fallback epoch = %d, want %d", e3, e2+1)
	}
}

// TestP0QuorumGuardRejectsInsufficient quorum 不足拒绝升主 + 开关关闭放行：
// 注册表仅自身或远端不可达时返回 ErrQuorumNotMet；关闭开关后放行（M6 旧行为）。
func TestP0QuorumGuardRejectsInsufficient(t *testing.T) {
	d := openTestDB(t)

	// 默认关闭 → 放行（零回归）。
	if err := d.quorumGuard(); err != nil {
		t.Fatalf("quorum disabled: quorumGuard = %v, want nil", err)
	}

	d.SetQuorumEnabled(true)

	// 注册表仅自身（<2 节点）→ 拒绝。
	if err := d.quorumGuard(); !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("single node quorumGuard = %v, want ErrQuorumNotMet", err)
	}

	// 注册不可达远端：n=2 但 Probe 失败 → 不足（1/2 < quorum 2）。
	d.Cluster.RegisterPeer("node-b", "127.0.0.1:1")
	if err := d.quorumGuard(); !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("unreachable peer quorumGuard = %v, want ErrQuorumNotMet", err)
	}

	// 关闭开关 → 放行。
	d.SetQuorumEnabled(false)
	if err := d.quorumGuard(); err != nil {
		t.Fatalf("quorum disabled again: quorumGuard = %v, want nil", err)
	}
}

// TestP0AllowWriteFencing epoch 拒陈旧主写入：收到更高 epoch 广播后本节点
// 写路径被拒（ErrFenced）；开关关闭 / 未授权主放行；SQL INSERT 同样被拒。
func TestP0AllowWriteFencing(t *testing.T) {
	d := openTestDB(t)

	// 默认关闭 → 放行。
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("disabled AllowWrite = %v, want nil", err)
	}

	d.SetEpochEnabled(true)

	// 未授权主（masterEpoch=0）→ 放行。
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("unauthed AllowWrite = %v, want nil", err)
	}

	// 授权为 master → 放行。
	e := d.acquireEpoch()
	d.setMasterEpoch(e)
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("current master AllowWrite = %v, want nil", err)
	}

	// 收到更高 epoch（其它节点升主广播）→ 陈旧主被拒。
	d.NoteEpoch(e + 10)
	if err := d.AllowWrite(); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale master AllowWrite = %v, want ErrFenced", err)
	}

	// SQL 写路径同样被拒（INSERT 返回错误）。
	if _, err := d.SQL.Execute("CREATE TABLE t (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := d.SQL.Execute("INSERT INTO t (id) VALUES (1)"); err == nil {
		t.Fatalf("INSERT on fenced master: want error, got nil")
	}
}

// TestP0EpochDefaultOnZeroRegression 开关默认开启零破坏（v1.10.0 落地）：
// epoch 默认开启、quorum 默认关闭；未授权主（masterEpoch=0）写路径放行，
// 复制升主、存储与 SQL 写路径均不受影响（M6 既有行为不变）。
func TestP0EpochDefaultOnZeroRegression(t *testing.T) {
	d := openTestDB(t)

	// 默认开关状态：epoch 开启、quorum 关闭。
	if !d.EpochEnabled() || d.QuorumEnabled() {
		t.Fatalf("default switches: epoch=%v quorum=%v, want epoch on / quorum off", d.EpochEnabled(), d.QuorumEnabled())
	}

	// 写路径放行。
	if err := d.AllowWrite(); err != nil {
		t.Fatalf("AllowWrite = %v, want nil", err)
	}

	// 成为 master（复制启动）并直接写存储。
	if err := d.StartReplication(":0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	commitKV(t, d, "k", "v")
	if v, err := d.Storage.Get([]byte("k")); err != nil || string(v) != "v" {
		t.Fatalf("k = %q err=%v, want v", v, err)
	}

	// SQL 写路径放行。
	if _, err := d.SQL.Execute("CREATE TABLE t (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := d.SQL.Execute("INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatalf("INSERT with switches off: %v", err)
	}
}

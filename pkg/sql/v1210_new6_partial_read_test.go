package sql

// NEW-6（v1.21.0）：跨区不可达读显式报错 + allow_partial_read 兼容开关。
//
// 背景：v1.21.0 Phase A 真机复验 §10.4 中停掉持有 region 的节点后，读请求
// 静默回退本地副本，SELECT COUNT(*) 由 54 变 14 而 SQL 层无任何报错。
// 本文件覆盖：默认严格报错（文案含 region / 节点标识）、开关退回旧行为、
// 本地持有与单机模式不误报、配置键注册与取值。

import (
	"errors"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
)

// newNew6Executor 构造"region 归属 node-b、node-b 已 down"的执行器：
// node-a 本机持有本地副本，重指派因 quorum 不足而降级，正是静默降级的历史场景。
func newNew6Executor(t *testing.T) *Executor {
	t.Helper()
	cfg := &fakeCfg{epoch: true, quorum: true}
	e := newP1Executor(&fakeEpochSrc{known: 2},
		&fakeQuorum{err: errors.New("sql: test quorum not met")}, cfg, "node-a")
	e.mgr.RegisterPeer("node-b", "127.0.0.1:1")
	e.mgr.MarkDown("node-b")
	e.router.SetRegions(1, []sharding.RegionInfo{regionOf(1, "node-b", 1)})
	return e
}

// TestNew6RemoteRegionUnavailableErrors 默认（allow_partial_read=off）：
// 归属不可达且无存活持有者 → 显式报错，文案含 region 号与节点标识。
func TestNew6RemoteRegionUnavailableErrors(t *testing.T) {
	e := newNew6Executor(t)
	if e.AllowPartialRead() {
		t.Fatal("allow_partial_read 默认必须为 off")
	}
	owner, err := e.resolveReadOwner(1)
	if err == nil {
		t.Fatalf("跨区不可达读应显式报错，却返回 owner=%q", owner)
	}
	msg := err.Error()
	if !strings.Contains(msg, "remote region unavailable: region 1") {
		t.Fatalf("错误文案缺少 region 标识: %s", msg)
	}
	if !strings.Contains(msg, "node-b") {
		t.Fatalf("错误文案缺少节点标识: %s", msg)
	}
	if !strings.Contains(msg, "127.0.0.1:1") {
		t.Fatalf("错误文案应含对端地址/探测原因: %s", msg)
	}
	if !strings.Contains(msg, "allow_partial_read") {
		t.Fatalf("错误文案应提示兼容开关: %s", msg)
	}
	// 本地持有者不受影响：不应对本机归属的 region 误报。
	if owner, err := e.resolveReadOwner(2); err != nil {
		t.Fatalf("未注册归属的 region 不应报错: owner=%q err=%v", owner, err)
	}
}

// TestNew6AllowPartialReadFallback allow_partial_read=on：
// 退回旧行为，回退本地副本且不返回错误（结果可能不完整）。
func TestNew6AllowPartialReadFallback(t *testing.T) {
	e := newNew6Executor(t)
	e.SetAllowPartialRead(true)
	owner, err := e.resolveReadOwner(1)
	if err != nil {
		t.Fatalf("开启 allow_partial_read 后不应报错: %v", err)
	}
	if owner != "node-a" {
		t.Fatalf("应回退本地副本 owner=node-a，得到 %q", owner)
	}

	// 关闭开关后恢复严格语义。
	e.SetAllowPartialRead(false)
	if _, err := e.resolveReadOwner(1); err == nil {
		t.Fatal("关闭 allow_partial_read 后应重新严格报错")
	}
}

// TestNew6LocalOwnerAndStandaloneNoError 本机持有 / 单机模式不误报（零回归）。
func TestNew6LocalOwnerAndStandaloneNoError(t *testing.T) {
	// 本机即归属：node-a 持有 region 3。
	e := newNew6Executor(t)
	e.router.SetRegions(1, []sharding.RegionInfo{regionOf(3, "node-a", 1)})
	if owner, err := e.resolveReadOwner(3); err != nil || owner != "node-a" {
		t.Fatalf("本机归属不应报错: owner=%q err=%v", owner, err)
	}

	// 单机模式（未装配 Manager）：无跨区概念，直接本地副本。
	solo := newP1Executor(nil, &fakeQuorum{}, &fakeCfg{}, "node-a")
	solo.mgr = nil
	solo.router = sharding.NewRouter()
	if owner, err := solo.resolveReadOwner(3); err != nil || owner != "node-a" {
		t.Fatalf("单机模式不应报错: owner=%q err=%v", owner, err)
	}
}

// TestNew6ConfigKeyRegistered 配置项注册：默认 off、取值解析、非法值报错。
func TestNew6ConfigKeyRegistered(t *testing.T) {
	spec := lookupConfigSpec("allow_partial_read")
	if spec == nil {
		t.Fatal("allow_partial_read 未注册到配置键表")
	}
	if spec.def != "off" {
		t.Fatalf("allow_partial_read 默认值应为 off，得到 %q", spec.def)
	}
	e := newNew6Executor(t)
	if spec.read(e) != "off" {
		t.Fatalf("初始读取应为 off，得到 %q", spec.read(e))
	}
	if err := spec.apply(e, "on"); err != nil || !e.AllowPartialRead() || spec.read(e) != "on" {
		t.Fatalf("apply on 失败: err=%v read=%q", err, spec.read(e))
	}
	if err := spec.apply(e, "off"); err != nil || e.AllowPartialRead() {
		t.Fatalf("apply off 失败: err=%v", err)
	}
	if err := spec.apply(e, "1"); err != nil || !e.AllowPartialRead() {
		t.Fatalf("apply 1 失败: err=%v", err)
	}
	if err := spec.apply(e, "maybe"); err == nil {
		t.Fatal("非法取值应报错")
	}

	// 配置键出现在 SHOW CONFIG 支持的键清单中。
	found := false
	for _, k := range ConfigKeys() {
		if k == "allow_partial_read" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("ConfigKeys() 未包含 allow_partial_read")
	}
}

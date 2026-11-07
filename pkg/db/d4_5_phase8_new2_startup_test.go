package db

// D4.5 Phase8 回归测试：NEW-2 v2 启动期崩溃（persisted.json 含
// ha_quorum_enabled=on 时进程启动即 0xc0000005）的端到端防回归。
//
// 崩溃链：db.Open → LoadPersistedConfig → applyHaQuorumEnabled(on) →
// b1EnsureAsync（起 goroutine）→ EnsureRegionReplicas → holdsRegion →
// regionHoldsLocal → tx.Scan → rocks.Snapshot → C.xdb_snapshot_create(0x0)。
// 进程启动存在"试开-即关-重开"路径（cmd/openxdb 读 TLS 持久化意图），
// 异步 goroutine 在句柄未就绪/已释放时做快照 → 原生访问违例。
//
// 本用例按 on/off × 是否经试开路径 构成 2x2 对照矩阵：
//   - 每个格子都必须启动成功（不崩溃、不报错）；
//   - persisted on 时开关必须真实生效（QuorumEnabled()=true），且启动期被
//     挂起的补齐请求必须在存储就绪时补发（fired=1），即"开关不被栅栏吞掉"；
//   - persisted off 时保持零回归（不触发补齐、fired=0）；
//   - 关闭后补齐调用立即拒绝，不产生任何快照。

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPhase8New2StartupPersistedQuorumMatrix 覆盖 persisted ha_quorum_enabled
// on/off × 启动路径（直接重开 / 试开-关-重开）四格对照。
func TestPhase8New2StartupPersistedQuorumMatrix(t *testing.T) {
	for _, quorum := range []string{"on", "off"} {
		for _, trial := range []bool{false, true} {
			quorum, trial := quorum, trial
			name := fmt.Sprintf("quorum_%s/preflight_%v", quorum, trial)
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				if err := Init(dir); err != nil {
					t.Fatalf("init dir: %v", err)
				}
				d0, err := Open(dir)
				if err != nil {
					t.Fatalf("seed open: %v", err)
				}
				// 与 Phase7 复测一致：epoch 恒 on，仅切换 quorum。
				for _, stmt := range []string{
					"SET PERSIST ha_epoch_enabled = 'on'",
					"SET PERSIST ha_quorum_enabled = '" + quorum + "'",
				} {
					if _, err := d0.SQL.Execute(stmt); err != nil {
						t.Fatalf("%s: %v", stmt, err)
					}
				}
				if err := d0.Close(); err != nil {
					t.Fatalf("seed close: %v", err)
				}
				raw, rerr := os.ReadFile(filepath.Join(dir, PersistedConfigFile))
				if rerr != nil {
					t.Fatalf("read persisted config: %v", rerr)
				}
				if !strings.Contains(string(raw), "ha_quorum_enabled") {
					t.Fatalf("persisted config missing ha_quorum_enabled: %s", raw)
				}

				// 试开路径：复刻 cmd/openxdb 启动时先 Open 读持久化意图再 Close。
				// 这是 Phase7 崩溃的必经前置（句柄被释放后异步补齐仍做快照）。
				if trial {
					dTrial, err := Open(dir)
					if err != nil {
						t.Fatalf("trial open (ha_quorum_enabled=%s) must succeed: %v", quorum, err)
					}
					if ready, closed, _, _ := dTrial.SQL.B1StorageGateState(); !ready || closed {
						t.Fatalf("trial open gate = ready:%v closed:%v, want true/false", ready, closed)
					}
					if err := dTrial.Close(); err != nil {
						t.Fatalf("trial close: %v", err)
					}
					// 关闭后栅栏必须永久关闭，且补齐调用不得再触碰存储。
					if _, closed, _, _ := dTrial.SQL.B1StorageGateState(); !closed {
						t.Fatalf("gate must stay closed after db.Close")
					}
					if n, err := dTrial.SQL.EnsureRegionReplicas(); err != nil || n != 0 {
						t.Fatalf("EnsureRegionReplicas after close = (%d, %v), want (0, nil)", n, err)
					}
				}

				// NEW-2 v2 核心断言：带持久化 ha_quorum_enabled 的节点必须能启动。
				d1, err := Open(dir)
				if err != nil {
					t.Fatalf("restart with persisted ha_quorum_enabled=%s must not crash (NEW-2 v2): %v", quorum, err)
				}
				ready, closed, deferred, fired := d1.SQL.B1StorageGateState()
				if !ready || closed {
					t.Fatalf("gate after open = ready:%v closed:%v, want true/false（Open 返回时必须已就绪）", ready, closed)
				}
				// 开关真实生效：持久化值必须落到运行时开关。
				if got, want := d1.QuorumEnabled(), quorum == "on"; got != want {
					t.Fatalf("QuorumEnabled() = %v, want %v (persisted ha_quorum_enabled=%s)", got, want, quorum)
				}
				if quorum == "on" {
					// 启动期 on 触发的补齐请求被栅栏挂起，就绪时补发一次 → fired=1。
					if fired != 1 {
						t.Fatalf("quorum on: gate fired = %d, want 1（启动期挂起的补齐须在就绪事件补发，否则开关被栅栏吞掉）", fired)
					}
				} else {
					// off 不触发补齐，不得产生任何挂起/补发。
					if fired != 0 || deferred != 0 {
						t.Fatalf("quorum off: fired=%d deferred=%d, want 0/0（零回归）", fired, deferred)
					}
				}
				// 启动后数据面可用（崩溃修复不得影响正常 SQL）。
				if _, err := d1.SQL.Execute("CREATE TABLE p8_startup_ok (id INT, v VARCHAR(32))"); err != nil {
					t.Fatalf("create table after restart: %v", err)
				}
				if _, err := d1.SQL.Execute("INSERT INTO p8_startup_ok VALUES (1, 'phase8')"); err != nil {
					t.Fatalf("insert after restart: %v", err)
				}
				rs, err := d1.SQL.Execute("SELECT v FROM p8_startup_ok WHERE id = 1")
				if err != nil {
					t.Fatalf("select after restart: %v", err)
				}
				if rs == nil || len(rs.Rows) != 1 {
					t.Fatalf("select after restart returned %v rows, want 1", rs)
				}
				// 关闭后补齐必须被拒绝（这是崩溃的直接触发点）。
				if err := d1.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
				if n, err := d1.SQL.EnsureRegionReplicas(); err != nil || n != 0 {
					t.Fatalf("EnsureRegionReplicas after close = (%d, %v), want (0, nil)", n, err)
				}
			})
		}
	}
}

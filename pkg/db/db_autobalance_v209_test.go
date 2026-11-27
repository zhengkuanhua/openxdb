package db_test

// v2.0.9 B2（P2a）自动均衡生产装配回归：集群启动后默认开启自动均衡，且运维可经
// SET auto_balance / SET auto_balance_interval 显式关闭与调周期；关闭后仅保留手动
// BALANCE，行为与 v2.0.8 一致。

import (
	"testing"
	"time"

)

// TestV209AutoBalanceAssembledOnClusterStart 集群装配后自动均衡默认开启且周期为默认值。
func TestV209AutoBalanceAssembledOnClusterStart(t *testing.T) {
	dA, _, _ := m7ThreePlusOneTopology(t)
	enabled, interval := dA.SQL.AutoBalanceStatus()
	if !enabled {
		t.Fatalf("auto balance not assembled after cluster start (want enabled)")
	}
	if interval <= 0 {
		t.Fatalf("auto balance interval = %s, want positive", interval)
	}
}

// TestV209AutoBalanceSwitchAndInterval 覆盖开关与周期入口：
// off -> 关闭（v2.0.8 等价）；interval -> 只改周期不改开关；on -> 重新按新周期开启。
func TestV209AutoBalanceSwitchAndInterval(t *testing.T) {
	dA, _, _ := m7ThreePlusOneTopology(t)

	mustSQL(t, dA, "SET auto_balance=off")
	if enabled, _ := dA.SQL.AutoBalanceStatus(); enabled {
		t.Fatalf("auto balance still enabled after SET auto_balance=off")
	}
	// 关闭状态下仍可调周期（供后续开启使用），且不得把开关打开。
	mustSQL(t, dA, "SET auto_balance_interval=800")
	enabled, interval := dA.SQL.AutoBalanceStatus()
	if enabled {
		t.Fatalf("SET auto_balance_interval re-enabled auto balance")
	}
	if interval != 800*time.Millisecond {
		t.Fatalf("interval = %s, want 800ms", interval)
	}
	mustSQL(t, dA, "SET auto_balance=on")
	enabled, interval = dA.SQL.AutoBalanceStatus()
	if !enabled || interval != 800*time.Millisecond {
		t.Fatalf("after SET auto_balance=on: enabled=%v interval=%s, want true/800ms", enabled, interval)
	}
	// 非法值必须被拒绝。
	if _, err := dA.SQL.Execute("SET auto_balance=maybe"); err == nil {
		t.Fatalf("SET auto_balance=maybe accepted, want error")
	}
	if _, err := dA.SQL.Execute("SET auto_balance_interval=0"); err == nil {
		t.Fatalf("SET auto_balance_interval=0 accepted, want error")
	}
}

// TestV209AutoBalanceOffKeepsManualBalance 关闭自动均衡后手动 BALANCE 仍可用，
// 且默认装配不会因配置缺失把开关覆盖回去（关闭态稳定）。
func TestV209AutoBalanceOffKeepsManualBalance(t *testing.T) {
	dA, dB, _ := m7ThreePlusOneTopology(t)
	mustSQL(t, dA, "SET auto_balance=off")

	if _, err := dA.SQL.Execute("BALANCE"); err != nil {
		t.Fatalf("manual BALANCE after disabling auto balance: %v", err)
	}
	// 关闭态在观察窗口内保持关闭，且期间不再发生自动迁移。
	before := regionCountOnNode(t, dA, dB.Cluster.SelfID)
	time.Sleep(500 * time.Millisecond)
	if enabled, _ := dA.SQL.AutoBalanceStatus(); enabled {
		t.Fatalf("auto balance re-enabled spontaneously")
	}
	if after := regionCountOnNode(t, dA, dB.Cluster.SelfID); after != before {
		t.Fatalf("layout changed while auto balance disabled: %d -> %d", before, after)
	}
}

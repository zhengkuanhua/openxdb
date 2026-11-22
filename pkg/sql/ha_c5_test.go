package sql

// v1.20.0 C5 集群轨写授权统一专项测试（承接 C2 后置的分区拒写 V-A4）。
//
// 验收项（DEV_PLAN_v1.20.0.md C5）：
//   - V-A4 分区拒写：本节点处于分区少数派（MajorityOK=false）时集群轨写被
//     拒（错误码 CLUSTER_MINORITY），分区恢复后写成功；
//   - 开关联动口径：ha_quorum_enabled=on 使"分区拒写"生效，但不联动
//     region 归属三类判定（writeRouteEnforce 仍只读显式位）；
//   - 零回归：开关全关时即使是少数派也照常写。

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// c5QuorumCfg 最小 HAConfigSetter 桩（只关心 quorum 位）。
type c5QuorumCfg struct{ quorum bool }

func (f *c5QuorumCfg) SetEpochEnabled(bool)  {}
func (f *c5QuorumCfg) SetQuorumEnabled(on bool) { f.quorum = on }
func (f *c5QuorumCfg) EpochEnabled() bool    { return false }
func (f *c5QuorumCfg) QuorumEnabled() bool   { return f.quorum }

// newC5Env 构造 5 节点集群视图（quorum=3）+ 可注入探活桩的执行器。
func newC5Env(t *testing.T, prober func(id string) bool) (*Executor, *cluster.Manager) {
	t.Helper()
	e := NewExecutor(nil)
	e.selfID = "node-a"
	m := cluster.NewManager("node-a")
	for _, p := range []string{"node-b", "node-c", "node-d", "node-e"} {
		m.RegisterPeer(p, "127.0.0.1:1")
	}
	m.SetProber(prober)
	e.mgr = m
	e.router = sharding.NewRouter()
	return e, m
}

// c5Ops 构造一条未切分表（隐式 region）的写操作。
func c5Ops() []kvOp {
	return []kvOp{{Key: sharding.EncodeRegionKey(storage.ID(7), []byte("k1")), Value: []byte("v1")}}
}

// TestC5V_A4PartitionMinorityRejectsWrite V-A4：分区少数派拒写 + 恢复后放行。
func TestC5V_A4PartitionMinorityRejectsWrite(t *testing.T) {
	minority := true
	e, m := newC5Env(t, func(string) bool { return !minority })
	e.SetWriteRouteEnforce(true)

	_, _, err := e.groupOps(c5Ops())
	if err == nil {
		t.Fatalf("partition minority must reject cluster track write")
	}
	if code := writeRouteErrCode(err); code != ErrCodeClusterMinority {
		t.Fatalf("want code %s, got %q (err=%v)", ErrCodeClusterMinority, code, err)
	}
	if got := e.PartitionMinorityFencedCount(); got != 1 {
		t.Fatalf("fenced count want 1, got %d", got)
	}

	// 分区恢复：快照刷新后放行。
	minority = false
	m.RefreshPartitionSnapshot()
	local, remote, err := e.groupOps(c5Ops())
	if err != nil {
		t.Fatalf("write after partition recovery must succeed: %v", err)
	}
	if len(local) != 1 || len(remote) != 0 {
		t.Fatalf("recovered write must stay on local path: local=%d remote=%d", len(local), len(remote))
	}
}

// TestC5QuorumLinkageCoversPartitionFenceOnly quorum 联动范围仅限分区拒写。
func TestC5QuorumLinkageCoversPartitionFenceOnly(t *testing.T) {
	e, m := newC5Env(t, func(string) bool { return false })
	cfg := &c5QuorumCfg{}
	e.haCfg = cfg

	if e.partitionWriteFenceOn() {
		t.Fatalf("partition fence must be off by default")
	}
	// 开关全关：即使少数派也照常写（零回归）。
	if _, _, err := e.groupOps(c5Ops()); err != nil {
		t.Fatalf("fence off must not reject write: %v", err)
	}

	cfg.SetQuorumEnabled(true)
	if !e.partitionWriteFenceOn() {
		t.Fatalf("ha_quorum_enabled=on must enable partition fence")
	}
	if e.writeRouteEnforce() {
		t.Fatalf("quorum must NOT link region ownership checks (writeRouteEnforce stays explicit)")
	}

	m.RefreshPartitionSnapshot()
	_, _, err := e.groupOps(c5Ops())
	if code := writeRouteErrCode(err); code != ErrCodeClusterMinority {
		t.Fatalf("quorum on + minority must reject with %s, got %q (err=%v)", ErrCodeClusterMinority, code, err)
	}
}

// TestC5SingleNodeZeroRegression 单机（无远端节点）不触发分区拒写。
func TestC5SingleNodeZeroRegression(t *testing.T) {
	e := NewExecutor(nil)
	e.selfID = "node-a"
	e.mgr = cluster.NewManager("node-a")
	e.router = sharding.NewRouter()
	e.SetWriteRouteEnforce(true)

	if _, _, err := e.groupOps(c5Ops()); err != nil {
		t.Fatalf("single node must not be fenced by partition auth: %v", err)
	}
	if got := e.PartitionMinorityFencedCount(); got != 0 {
		t.Fatalf("single node fenced count want 0, got %d", got)
	}
}

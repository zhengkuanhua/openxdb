package cluster

// v1.20.0 C3：2PC 参与者归属校验专项测试（缺口 B-3，验收 V-B3）。
//
// V-B3 断言：单节点（无集群链路）直调 Server.PrepareTxn，ops 中的键属于其它
// 节点持有的 region 时返回 2PC_NOT_OWNER，且 m:2pc:p:<txid> 暂存未落盘；
// 同时覆盖两条边界：
//   - 开关关闭（提供者不存在或返回 false）时一律放行，参与者 PREPARE 与
//     v1.19.0 逐字一致（零回归）；
//   - 本节点被登记为该 region 副本（m:repl:replicas:<rid>）时允许参与，
//     暂存正常落盘（副本参与面扩展的前提）。

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

// newC3OwnerEnv 构造"region 归属其它节点"的单节点参与者环境：
// 本节点 node-b 持有 region 2 的路由视图，但 owner 为 node-a。
func newC3OwnerEnv(t *testing.T) (*Server, storage.Storage, []TxnOp) {
	t.Helper()
	st := btree.NewKV(32)
	srv := NewServer(st, "node-b")
	router := sharding.NewRouter()
	if err := router.UpsertRegion(sharding.RegionInfo{
		RegionID: 2, StartKey: []byte("a"), EndKey: []byte("z"), Node: "node-a",
	}); err != nil {
		t.Fatalf("seed router: %v", err)
	}
	srv.SetRouter(router)
	ops := []TxnOp{{Key: sharding.EncodeRegionKey(2, []byte("k1")), Value: []byte("1")}}
	return srv, st, ops
}

// TestV1200C3PrepareRejectsNonOwnedRegion V-B3 主断言。
func TestV1200C3PrepareRejectsNonOwnedRegion(t *testing.T) {
	srv, st, ops := newC3OwnerEnv(t)
	srv.SetParticipantOwnershipCheck(func() bool { return true })

	const txID = "tx-c3-vb3-not-owner"
	err := srv.PrepareTxn(txID, 100, ops)
	if err == nil {
		t.Fatalf("prepare for foreign region must fail")
	}
	if !strings.Contains(err.Error(), ErrCode2PCNotOwner) {
		t.Fatalf("want %s, got: %v", ErrCode2PCNotOwner, err)
	}
	// 失败发生在落盘之前：m:2pc:p:<txid> 不得存在。
	if _, gerr := st.Get(prepared2pcKey(txID)); gerr != storage.ErrNotFound {
		t.Fatalf("prepared key must be absent after reject, get err=%v", gerr)
	}
	// 也不得写入任何 m:2pc:* 暂存。
	rows, serr := st.Scan(storage.KeyRange{
		Start: []byte(key2pcPrepared),
		End:   []byte("m:2pc:q"),
	}, -1)
	if serr != nil {
		t.Fatalf("scan prepared keys: %v", serr)
	}
	if len(rows) != 0 {
		t.Fatalf("prepared residue: %d rows", len(rows))
	}
}

// TestV1200C3PrepareOwnerCheckOffRegression 开关关闭时归属校验不生效（零回归）。
func TestV1200C3PrepareOwnerCheckOffRegression(t *testing.T) {
	// 未装配提供者（旧节点装配方式）：放行。
	srv, st, ops := newC3OwnerEnv(t)
	if err := srv.PrepareTxn("tx-c3-off-noprovider", 100, ops); err != nil {
		t.Fatalf("prepare without ownership provider must pass: %v", err)
	}
	if _, err := st.Get(prepared2pcKey("tx-c3-off-noprovider")); err != nil {
		t.Fatalf("prepared key must be written: %v", err)
	}

	// 装配了提供者但开关为 off：同样放行。
	srv2, st2, ops2 := newC3OwnerEnv(t)
	srv2.SetParticipantOwnershipCheck(func() bool { return false })
	if err := srv2.PrepareTxn("tx-c3-off-switch", 100, ops2); err != nil {
		t.Fatalf("prepare with switch off must pass: %v", err)
	}
	if _, err := st2.Get(prepared2pcKey("tx-c3-off-switch")); err != nil {
		t.Fatalf("prepared key must be written when switch off: %v", err)
	}
}

// TestV1200C3PrepareAcceptsDeclaredReplica 本节点登记为副本时允许参与（B-2 前提）。
func TestV1200C3PrepareAcceptsDeclaredReplica(t *testing.T) {
	srv, st, ops := newC3OwnerEnv(t)
	srv.SetParticipantOwnershipCheck(func() bool { return true })

	// 未登记副本 -> 拒绝。
	if err := srv.PrepareTxn("tx-c3-replica-before", 100, ops); err == nil {
		t.Fatalf("prepare before replica registration must fail")
	}
	// 登记 node-b 为 region 2 副本后 -> 放行，暂存落盘。
	if err := PutRegionReplicas(st, 2, []string{"node-b"}); err != nil {
		t.Fatalf("PutRegionReplicas: %v", err)
	}
	reps, err := LoadRegionReplicas(st, 2)
	if err != nil || len(reps) != 1 || reps[0] != "node-b" {
		t.Fatalf("LoadRegionReplicas: %v reps=%v", err, reps)
	}
	if err := srv.PrepareTxn("tx-c3-replica-after", 100, ops); err != nil {
		t.Fatalf("prepare as declared replica must pass: %v", err)
	}
	if _, err := st.Get(prepared2pcKey("tx-c3-replica-after")); err != nil {
		t.Fatalf("prepared key must be written for replica: %v", err)
	}
}

// TestV1200C3ReplicasMetaKey 副本元数据键形态与缺失退化（D-2 MVP）。
func TestV1200C3ReplicasMetaKey(t *testing.T) {
	st := btree.NewKV(32)
	if got := string(ReplicasKey(7)); got != "m:repl:replicas:7" {
		t.Fatalf("replicas key form: %q", got)
	}
	// 缺失 -> (nil, nil)，调用方退化为仅 owner。
	if reps, err := LoadRegionReplicas(st, 7); err != nil || reps != nil {
		t.Fatalf("missing replicas must degrade to nil: %v %v", err, reps)
	}
	if err := PutRegionReplicas(st, 7, []string{"node-c", "node-b", "node-b", ""}); err != nil {
		t.Fatalf("PutRegionReplicas: %v", err)
	}
	reps, err := LoadRegionReplicas(st, 7)
	if err != nil || len(reps) != 2 || !ContainsNode(reps, "node-b") || !ContainsNode(reps, "node-c") {
		t.Fatalf("dedup/sort replicas: %v %v", err, reps)
	}
	// 空集合等价删除。
	if err := PutRegionReplicas(st, 7, nil); err != nil {
		t.Fatalf("PutRegionReplicas empty: %v", err)
	}
	if reps, err := LoadRegionReplicas(st, 7); err != nil || reps != nil {
		t.Fatalf("empty replicas must delete record: %v %v", err, reps)
	}
}

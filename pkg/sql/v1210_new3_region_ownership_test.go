// v1.21.0 NEW-3（P0）：集群轨归属写 fencing 下沉 region 粒度的 SQL 侧回归测试。
//
// 缺陷现场（三机真机 201/202/203，ha_epoch_enabled + ha_quorum_enabled 双开）：
// node-c 因历史误授被权威视图收回归属（cluster_term.meta: own_term>0 / owned=0），
// 而 v1.20.x 的归属 fence 是节点级全局判定，把"目标 region 归属其它节点、本机仅
// 做 2PC 转发"的写一并拦死，且重启后依旧（状态持久化）。
//
// 本文件覆盖两条反向用例（修复要求 4）：
//
//	a) 节点不持有目标 region 时，转发型写不被本地归属 fence 拦截；
//	b) 本节点持有的 region 被权威视图收回后，仍返回 fencing 拒绝（未削弱契约）。
//
// 另覆盖误授根因：notifyLocalTakeover 仅在接管真正落地且本机即该 region owner
// 时才上报归属。
package sql

import (
	"errors"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/tso"
)

// new3Fence 三态假 fence：分别模拟语句级（复制轨）、节点级归属、region 级归属
// 三个入口的返回，用于区分"哪一层拦下了写"。
type new3Fence struct {
	nodeErr      error // AllowWrite（v1.20.x 节点级：含集群轨归属 fence）
	statementErr error // AllowWriteStatement（v1.21.0 语句级：仅复制轨）
	regionErr    error // AllowWriteRegion（v1.21.0 region 级归属）
	regionCalls  []uint64
}

func (f *new3Fence) AllowWrite() error          { return f.nodeErr }
func (f *new3Fence) AllowWriteStatement() error { return f.statementErr }
func (f *new3Fence) AllowWriteRegion(rid uint64) error {
	f.regionCalls = append(f.regionCalls, rid)
	return f.regionErr
}

// legacyFence 仅实现历史 WriteFence 接口（无 region 粒度能力）的旧装配。
type legacyFence struct{ err error }

func (f *legacyFence) AllowWrite() error { return f.err }

// new3Region 构造显式 region 路由条目（不同 rid 使用不同键空间，避免互相覆盖）。
func new3Region(rid storage.ID, node string, regionEpoch uint64) sharding.RegionInfo {
	return sharding.RegionInfo{
		RegionID:    rid,
		StartKey:    []byte{byte('a' + rid)},
		EndKey:      []byte{byte('a' + rid + 1)},
		Node:        node,
		RegionEpoch: regionEpoch,
		Explicit:    true,
	}
}

// TestNew3ForwardWriteNotFencedByNodeOwnership 用例 a：非 owner 节点的转发型写
// 不被归属 fence 拦截。
//   - 语句级入口只做复制轨判定：节点级归属 fence 已收回（nodeErr=ErrFenced 语义）
//     也不再拦截（这是 node-c 全部写被拦的直接成因）；
//   - 目标 region 归属远端节点 → 走转发路径，本地 region 级归属 fence 不参与判定；
//   - 目标 region 归属本机 → region 级 fence 仍生效（见用例 b）。
func TestNew3ForwardWriteNotFencedByNodeOwnership(t *testing.T) {
	errFenced := errors.New("db: write fenced by newer cluster epoch: cluster ownership revoked at term 7 (known term 9)")
	e := newP1Executor(&fakeEpochSrc{}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")
	// 转发型写需要协调者能力（mgr + TSO）才不走硬拒绝；本用例关注的是归属
	// fence 是否误拦，故补齐最小 2PC 转发前提。
	e.ts = tso.New()
	e.mgr.RegisterPeer("node-b", "127.0.0.1:1")
	if err := e.router.UpsertRegion(new3Region(1, "node-a", 3)); err != nil {
		t.Fatalf("upsert local region: %v", err)
	}
	if err := e.router.UpsertRegion(new3Region(2, "node-b", 3)); err != nil {
		t.Fatalf("upsert remote region: %v", err)
	}

	fence := &new3Fence{nodeErr: errFenced}
	e.SetWriteFence(fence)

	// 语句级入口：节点级归属已收回也不拦截转发型写。
	if err := e.writeFenceCheck(); err != nil {
		t.Fatalf("statement-level fence must not apply node ownership fence: %v", err)
	}

	// 归属判定口径：region 2 属远端（可判定且非本机 owner）。
	if owned, known := e.RegionOwnershipForWrite(2); owned || !known {
		t.Fatalf("region 2 ownership = (owned=%v, known=%v), want (false, true)", owned, known)
	}
	if owned, known := e.RegionOwnershipForWrite(1); !owned || !known {
		t.Fatalf("region 1 ownership = (owned=%v, known=%v), want (true, true)", owned, known)
	}

	// 转发型写：不进入本地写批次，也不触发本地 region 级归属 fence。
	local, remote, err := e.groupOps([]kvOp{localOp(2, "k-remote")})
	if err != nil {
		t.Fatalf("forwarded write must not be fenced by local node ownership: %v", err)
	}
	if len(local) != 0 || len(remote["node-b"]) != 1 {
		t.Fatalf("forwarded write routing = (local=%d, remote[node-b]=%d), want (0, 1)", len(local), len(remote["node-b"]))
	}
	if len(fence.regionCalls) != 0 {
		t.Fatalf("region fence calls = %v, want none for forwarded write", fence.regionCalls)
	}

	// 旧装配（仅 AllowWrite）保持节点级判定：零回归，行为与 v1.20.x 一致。
	e.SetWriteFence(&legacyFence{err: errFenced})
	if err := e.writeFenceCheck(); !errors.Is(err, errFenced) {
		t.Fatalf("legacy fence: writeFenceCheck = %v, want the node-level fencing error", err)
	}
}

// TestNew3OwnedRegionRevokedStillFenced 用例 b：本节点持有的 region 被权威视图
// 收回后，写入仍被拒绝（region 粒度下沉不得削弱既有 fencing 强度）。
func TestNew3OwnedRegionRevokedStillFenced(t *testing.T) {
	errFenced := errors.New("db: write fenced by newer cluster epoch: cluster ownership revoked at term 7 (known term 9)")
	e := newP1Executor(&fakeEpochSrc{}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")
	if err := e.router.UpsertRegion(new3Region(1, "node-a", 3)); err != nil {
		t.Fatalf("upsert local region: %v", err)
	}

	fence := &new3Fence{regionErr: errFenced}
	e.SetWriteFence(fence)

	_, _, err := e.groupOps([]kvOp{localOp(1, "k-local")})
	if err == nil {
		t.Fatal("write to locally-owned region must be fenced after authoritative revoke")
	}
	if !strings.Contains(err.Error(), "write to region 1 rejected") || !strings.Contains(err.Error(), errFenced.Error()) {
		t.Fatalf("error = %v, want region-level fencing error carrying the db fencing reason", err)
	}
	if len(fence.regionCalls) != 1 || fence.regionCalls[0] != 1 {
		t.Fatalf("region fence calls = %v, want [1]", fence.regionCalls)
	}

	// 同批多次写同一 region 只判定一次（去重，不放大开销）。
	fence.regionCalls = nil
	_, _, err = e.groupOps([]kvOp{localOp(1, "k1"), localOp(1, "k2"), localOp(1, "k3")})
	if err == nil {
		t.Fatal("write must still be fenced")
	}
	if len(fence.regionCalls) != 1 {
		t.Fatalf("region fence calls = %v, want exactly one per region", fence.regionCalls)
	}
}

// TestNew3RegionOwnershipUnknownFallsBack 归属不可判定时交由节点级判定兜底：
// 隐式单 region（不可判定）与 owner 未注册（不可判定）不得被当成"归属其它
// 节点"而放行，避免口径放宽后出现静默本地写。
func TestNew3RegionOwnershipUnknownFallsBack(t *testing.T) {
	e := newP1Executor(&fakeEpochSrc{}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")
	implicit := sharding.RegionInfo{RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), Node: "node-a"}
	if err := e.router.UpsertRegion(implicit); err != nil {
		t.Fatalf("upsert implicit region: %v", err)
	}
	if owned, known := e.RegionOwnershipForWrite(1); !owned || known {
		t.Fatalf("implicit region ownership = (owned=%v, known=%v), want (true, false)", owned, known)
	}
	if err := e.router.UpsertRegion(new3Region(2, "node-x", 3)); err != nil {
		t.Fatalf("upsert unregistered-owner region: %v", err)
	}
	if _, known := e.RegionOwnershipForWrite(2); known {
		t.Fatal("region owned by unregistered node must be treated as undecidable")
	}

	// 未装配路由视图（裸 Executor）恒不可判定（零回归）。
	bare := &Executor{}
	if owned, known := bare.RegionOwnershipForWrite(1); owned || known {
		t.Fatalf("bare executor ownership = (owned=%v, known=%v), want (false, false)", owned, known)
	}
}

// TestNew3TakeoverNotifyRequiresLocalOwnership 误授根因：只有接管真正落地且本机
// 即该 region 的当前 owner 时才上报归属，避免非 owner 节点被记为"曾持有"。
func TestNew3TakeoverNotifyRequiresLocalOwnership(t *testing.T) {
	e := newP1Executor(&fakeEpochSrc{known: 7}, &fakeQuorum{}, &fakeCfg{epoch: true}, "node-a")

	var calls int
	var gotTerm uint64
	var gotSelf bool
	e.SetClusterOwnershipNotify(func(term uint64, selfOwns, authoritative bool) {
		calls++
		gotTerm, gotSelf = term, selfOwns
	})

	// 目标 region 归属其它节点：接管未在本机落地 → 不上报（NEW-3 误授场景）。
	if err := e.router.UpsertRegion(new3Region(2, "node-b", 4)); err != nil {
		t.Fatalf("upsert remote region: %v", err)
	}
	e.notifyLocalTakeover(2, 7)
	if calls != 0 {
		t.Fatalf("notify calls = %d, want 0 for region owned by another node", calls)
	}

	// 归属本机但 regionEpoch 仍为 0（占位/未完成接管）→ 不上报。
	if err := e.router.UpsertRegion(new3Region(3, "node-a", 0)); err != nil {
		t.Fatalf("upsert zero-epoch region: %v", err)
	}
	e.notifyLocalTakeover(3, 7)
	if calls != 0 {
		t.Fatalf("notify calls = %d, want 0 when regionEpoch is unset", calls)
	}

	// 接管真正落地（归属本机 + 有效 regionEpoch）→ 上报 (term, selfOwns=true)。
	if err := e.router.UpsertRegion(new3Region(1, "node-a", 4)); err != nil {
		t.Fatalf("upsert local region: %v", err)
	}
	e.notifyLocalTakeover(1, 7)
	if calls != 1 || gotTerm != 7 || !gotSelf {
		t.Fatalf("notify = (calls=%d, term=%d, self=%v), want (1, 7, true)", calls, gotTerm, gotSelf)
	}

	// 已有更新纪元的接管记录时，旧纪元不得推进归属。
	e.recordRegionEpochOwner(1, 9, 5, "node-a")
	e.notifyLocalTakeover(1, 7)
	if calls != 1 {
		t.Fatalf("stale-epoch takeover must not notify, calls = %d", calls)
	}
}

package sql

// D4.4 根因修复 B 簇：写路径 region 归属强制路由（最小可行，不做本地降级写）。
//
// 不变量：region 归属为远端节点时，写入只允许经 2PC 协调者转发到权威 owner；
// 协调者能力缺失（未装配管理面 / TSO）必须返回明确错误，绝不静默写本地副本。
// 修复前：groupOps 把远端 op 静默塞入远端组，缺少协调者时整批写入被丢弃或
// 落本地，形成跨节点读分裂（D4.4 复测 GAP C：B 写 id=31 仅本地可见）。

import (
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// TestGroupOpsRejectsRemoteWriteWithoutCoordinator 非 owner 且无协调者路径必须拒绝。
func TestGroupOpsRejectsRemoteWriteWithoutCoordinator(t *testing.T) {
	e := NewExecutor(nil)
	e.selfID = "node-a"
	if err := e.router.UpsertRegion(sharding.RegionInfo{RegionID: 7, StartKey: nil, EndKey: nil}); err != nil {
		t.Fatalf("UpsertRegion: %v", err)
	}
	if err := e.router.SetRegionNode(7, "node-b"); err != nil {
		t.Fatalf("SetRegionNode: %v", err)
	}
	// 远端归属 region 7 的写入
	ops := []kvOp{{Key: sharding.EncodeRegionKey(7, []byte("k1")), Value: []byte("v1")}}
	_, _, err := e.groupOps(ops)
	if err == nil {
		t.Fatalf("remote write without coordinator must be rejected")
	}
	if !strings.Contains(err.Error(), "write to remote region 7") {
		t.Fatalf("error must name the region and rejection: %v", err)
	}
}

// TestGroupOpsLocalOpsUnaffected 本地归属 region 不受写路径加固影响。
func TestGroupOpsLocalOpsUnaffected(t *testing.T) {
	e := NewExecutor(nil)
	e.selfID = "node-a"
	// 未指派归属（空串）按既有语义视作本机；显式指派本机同样本地。
	for _, rid := range []storage.ID{8, 9} {
		if err := e.router.UpsertRegion(sharding.RegionInfo{RegionID: rid, StartKey: nil, EndKey: nil}); err != nil {
			t.Fatalf("UpsertRegion %d: %v", rid, err)
		}
	}
	if err := e.router.SetRegionNode(8, ""); err != nil {
		t.Fatalf("SetRegionNode: %v", err)
	}
	if err := e.router.SetRegionNode(9, "node-a"); err != nil {
		t.Fatalf("SetRegionNode: %v", err)
	}
	ops := []kvOp{
		{Key: sharding.EncodeRegionKey(8, []byte("k1")), Value: []byte("v1")},
		{Key: sharding.EncodeRegionKey(9, []byte("k2")), Value: []byte("v2")},
	}
	local, remote, err := e.groupOps(ops)
	if err != nil {
		t.Fatalf("local ops must not be rejected: %v", err)
	}
	if len(local) != 2 || len(remote) != 0 {
		t.Fatalf("local=%d remote=%d, want 2/0", len(local), len(remote))
	}
}

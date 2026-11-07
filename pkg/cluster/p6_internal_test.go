// P6 HA（NEW-1 修复）：装载期数据分歧可观测性 + 权威归属学习回归测试。
//
// 背景（Phase 4 NEW-1）：重启节点凭旧归属视图继续接受本地写入，多数派同时
// 写同一 region，二次重启后本机侧新写入被静默丢弃。修复要求：物理数据一律
// 不删除，装载冲突必须可观测（计数 + 告警），归属视图以权威源为准收敛。
package cluster

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

// TestP6RegionPushDivergenceCounted 装载前统计"被覆盖行"与"本机残留行"，
// 且残留行物理保留（不静默删除）。
func TestP6RegionPushDivergenceCounted(t *testing.T) {
	st := btree.NewKV(32)
	srv := NewServer(st, "node-b")
	router := sharding.NewRouter()
	if err := router.UpsertRegion(sharding.RegionInfo{
		RegionID: 2, StartKey: []byte("a"), EndKey: []byte("z"), Local: true,
	}); err != nil {
		t.Fatalf("seed router: %v", err)
	}
	srv.SetRouter(router)

	// 本机（重启节点）磁盘上已存在的行：k1 与权威推送同键不同值，k9 仅本机有。
	k1 := sharding.EncodeRegionKey(2, []byte("k1"))
	k9 := sharding.EncodeRegionKey(2, []byte("k9"))
	if err := st.Write(&storage.WriteBatch{Puts: []storage.KVPair{
		{Key: k1, Value: []byte("70")},
		{Key: k9, Value: []byte("local-only")},
	}}); err != nil {
		t.Fatalf("seed storage: %v", err)
	}

	// 权威推送：k1 新值 + k2 新行（不含 k9）。
	resp, err := srv.applyPush(&RegionPushReq{
		SrcID:  "node-c",
		Region: sharding.RegionInfo{RegionID: 2, StartKey: []byte("a"), EndKey: []byte("z"), Node: "node-b"},
		Rows: []QueryRow{
			{Key: k1, Value: []byte("71")},
			{Key: sharding.EncodeRegionKey(2, []byte("k2")), Value: []byte("72")},
		},
		ClaimOwner: true,
	})
	if err != nil {
		t.Fatalf("applyPush: %v", err)
	}
	if resp.Overwritten != 1 {
		t.Fatalf("overwritten rows: want 1, got %d", resp.Overwritten)
	}
	if resp.LocalExtra != 1 {
		t.Fatalf("local extra rows: want 1, got %d", resp.LocalExtra)
	}

	// 残留行 k9 必须物理保留（不静默丢弃）；k1 已被权威值覆盖。
	got, err := st.Get(k9)
	if err != nil || string(got) != "local-only" {
		t.Fatalf("local extra row must be retained, got %q err=%v", got, err)
	}
	got, err = st.Get(k1)
	if err != nil || string(got) != "71" {
		t.Fatalf("pushed row must win for same key, got %q err=%v", got, err)
	}
}

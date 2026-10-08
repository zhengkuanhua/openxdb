package sharding

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// pk 构造内层行键 s{tid}{pkBytes}。
func pkRow(tid uint64, pk uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, pk)
	key := []byte{'s'}
	key = binary.BigEndian.AppendUint64(key, tid)
	return append(key, b...)
}

func TestEncodeDecodeRegionKey(t *testing.T) {
	inner := []byte("s\x00\x00\x00\x00\x00\x00\x00\x01hello")
	rid := storage.ID(42)
	enc := EncodeRegionKey(rid, inner)
	if len(enc) != RegionKeyPrefixLen+len(inner) {
		t.Fatalf("encoded length = %d, want %d", len(enc), RegionKeyPrefixLen+len(inner))
	}
	if enc[0] != 'r' {
		t.Fatalf("prefix byte = %q, want 'r'", enc[0])
	}
	gotRID, gotInner := DecodeRegionKey(enc)
	if gotRID != rid {
		t.Fatalf("decoded region id = %d, want %d", gotRID, rid)
	}
	if !bytes.Equal(gotInner, inner) {
		t.Fatalf("decoded inner = %v, want %v", gotInner, inner)
	}
}

func TestRegionKeyOrdering(t *testing.T) {
	// 同一 region 内键序与 innerKey 序一致；不同 region 前缀隔离。
	innerA := pkRow(1, 10)
	innerB := pkRow(1, 20)
	encA := EncodeRegionKey(7, innerA)
	encB := EncodeRegionKey(7, innerB)
	if bytes.Compare(encA, encB) >= 0 {
		t.Fatalf("region 7 keys not ordered by inner key")
	}
	encOther := EncodeRegionKey(8, innerA)
	// region 7 < region 8（前缀隔离）
	if bytes.Compare(encA, encOther) >= 0 {
		t.Fatalf("region prefix ordering violated")
	}
}

func TestRouterImplicitSingleRegion(t *testing.T) {
	r := NewRouter()
	rs := r.RegionsOf(1)
	if len(rs) != 1 {
		t.Fatalf("implicit regions = %d, want 1", len(rs))
	}
	if rs[0].Explicit {
		t.Fatalf("implicit region should not be explicit")
	}
	if rs[0].RegionID != implicitRegionID(1) {
		t.Fatalf("implicit region id = %d, want %d", rs[0].RegionID, implicitRegionID(1))
	}
	// 隐式单 region 对任意 key 都命中
	rid, err := r.Locate(1, pkRow(1, 123))
	if err != nil {
		t.Fatalf("locate on implicit region: %v", err)
	}
	if rid != implicitRegionID(1) {
		t.Fatalf("locate id = %d, want implicit %d", rid, implicitRegionID(1))
	}
}

func TestRouterSplitAndLocate(t *testing.T) {
	r := NewRouter()
	// 表 1 分 3 段：(-inf, 10) [10, 20) [20, +inf)
	rs := []RegionInfo{
		{RegionID: 1, TableID: 1, StartKey: nil, EndKey: pkRow(1, 10), State: StateActive, Local: true, Explicit: true},
		{RegionID: 2, TableID: 1, StartKey: pkRow(1, 10), EndKey: pkRow(1, 20), State: StateActive, Local: true, Explicit: true},
		{RegionID: 3, TableID: 1, StartKey: pkRow(1, 20), EndKey: nil, State: StateActive, Local: true, Explicit: true},
	}
	r.ReplaceRegions(1, rs, 3)

	cases := []struct {
		pk   uint64
		want storage.ID
	}{
		{5, 1}, {10, 2}, {15, 2}, {19, 2}, {20, 3}, {999, 3},
	}
	for _, c := range cases {
		got, err := r.Locate(1, pkRow(1, c.pk))
		if err != nil {
			t.Fatalf("locate pk=%d: %v", c.pk, err)
		}
		if got != c.want {
			t.Fatalf("locate pk=%d = %d, want %d", c.pk, got, c.want)
		}
	}
	// 其他表不受影响（隐式单 region）
	if rid, err := r.Locate(2, pkRow(2, 1)); err != nil || rid != implicitRegionID(2) {
		t.Fatalf("other table locate = %d err=%v", rid, err)
	}
}

func TestRouterBoundaryErrors(t *testing.T) {
	r := NewRouter()
	// 显式空洞：region A [10,20)，key=25 无 region
	rs := []RegionInfo{
		{RegionID: 1, TableID: 1, StartKey: pkRow(1, 10), EndKey: pkRow(1, 20), State: StateActive, Local: true, Explicit: true},
	}
	r.ReplaceRegions(1, rs, 1)
	// key < 最小 StartKey（空洞开头）
	if _, err := r.Locate(1, pkRow(1, 5)); err != ErrRegionNotFound {
		t.Fatalf("expected ErrRegionNotFound, got %v", err)
	}
	// key >= EndKey（空洞末尾）
	if _, err := r.Locate(1, pkRow(1, 25)); err != ErrRegionNotFound {
		t.Fatalf("expected ErrRegionNotFound, got %v", err)
	}
	// 边界包含/排他：StartKey 含、EndKey 排他
	if rid, err := r.Locate(1, pkRow(1, 10)); err != nil || rid != 1 {
		t.Fatalf("StartKey inclusive violated: rid=%d err=%v", rid, err)
	}
	if _, err := r.Locate(1, pkRow(1, 20)); err != ErrRegionNotFound {
		t.Fatalf("EndKey exclusive violated: err=%v", err)
	}
}

func TestRouterSetDumpAndRemove(t *testing.T) {
	r := NewRouter()
	rs := []RegionInfo{
		{RegionID: 5, TableID: 9, StartKey: nil, EndKey: nil, State: StateActive, Local: true, Explicit: true},
	}
	r.SetRegions(5, rs)
	seq, dump := r.Dump()
	if seq != 5 || len(dump) != 1 {
		t.Fatalf("dump = seq %d, %d regions", seq, len(dump))
	}
	// ReplaceRegions 提升水位
	rs2 := []RegionInfo{
		{RegionID: 6, TableID: 9, StartKey: nil, EndKey: pkRow(9, 50), State: StateActive, Local: true, Explicit: true},
		{RegionID: 7, TableID: 9, StartKey: pkRow(9, 50), EndKey: nil, State: StateActive, Local: true, Explicit: true},
	}
	r.ReplaceRegions(9, rs2, 7)
	if _, dump = r.Dump(); len(dump) != 2 {
		t.Fatalf("after replace dump len = %d, want 2", len(dump))
	}
	// RemoveTable 清空
	r.RemoveTable(9)
	if rs2 = r.RegionsOf(9); len(rs2) != 1 || rs2[0].Explicit {
		t.Fatalf("after remove expected implicit single region, got %+v", rs2)
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	rs := []RegionInfo{
		{RegionID: 1, TableID: 1, StartKey: nil, EndKey: pkRow(1, 10), State: StateActive, Local: true, Explicit: true},
		{RegionID: 2, TableID: 1, StartKey: pkRow(1, 10), EndKey: nil, State: StateActive, Local: true, Explicit: true},
	}
	raw, err := MarshalMeta(2, rs)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	seq, got, err := UnmarshalMeta(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if seq != 2 || len(got) != 2 {
		t.Fatalf("unmarshal = seq %d, %d regions", seq, len(got))
	}
	if !bytes.Equal(got[0].EndKey, pkRow(1, 10)) || got[0].RegionID != 1 {
		t.Fatalf("region 0 mismatch: %+v", got[0])
	}
}

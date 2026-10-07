// Package sharding 提供 M3 分片闭环的接口预留（开发手册 §9，不在 M1/M2 实现）。
//
// M1 Key 编码无 region 前缀；M3 启用时由本包在外层追加前缀
// （EncodeRegionKey 预留），原 M1 编码保持兼容。
// 集中 meta 服务（单点 or etcd）M3 时再设计，M1/M2 不做。
package sharding

import "github.com/zhengkuanhua/openxdb/pkg/storage"

// RegionInfo 一个分片（Region）的元信息：Key 区间 [StartKey, EndKey)。
type RegionInfo struct {
	// RegionID 分片 ID。
	RegionID storage.ID
	// StartKey 区间下界（含）。
	StartKey []byte
	// EndKey 区间上界（不含）。
	EndKey []byte
}

// Router 分片路由接口预留（M3 实现，不在 M1/M2 实现）。
// Locate 按 (tableID, key) 定位所属 Region；Refresh 在 meta 版本变化后刷新路由缓存。
type Router interface {
	// Locate 返回 key 所在分片的 RegionID。
	Locate(tableID storage.ID, key []byte) (storage.ID, error)
	// Refresh 按 meta 版本号刷新路由缓存（分裂/合并后调用）。
	Refresh(metaVersion uint64) error
}

// EncodeRegionKey 预留：在 M1 原始 Key 外层追加 region 前缀。
// M3 启用后所有存取路径经此编码；M1 当前直接使用原始 Key，不调用本函数。
func EncodeRegionKey(regionID storage.ID, key []byte) []byte {
	// M3 实现：regionID(8B) + key。当前仅作编译期占位，M1 不调用。
	out := make([]byte, 0, 8+len(key))
	out = append(out, byte(regionID>>56), byte(regionID>>48), byte(regionID>>40), byte(regionID>>32))
	out = append(out, byte(regionID>>24), byte(regionID>>16), byte(regionID>>8), byte(regionID))
	return append(out, key...)
}

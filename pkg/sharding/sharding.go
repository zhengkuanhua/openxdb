// Package sharding 提供 M3 分片闭环：Region 元数据 / Router 路由 / EncodeRegionKey。
//
// 键空间划分：物理键 = EncodeRegionKey(regionID, innerKey) = 'r' + regionID(8B 大端) + innerKey，
// 其中 innerKey 为 M1 内层键（行键 s{tableID}{pk}、索引键 i{tableID}{indexID}{idxVal}p{pk}）。
// 同一 region 内键序与 innerKey 字节序完全一致；跨 region 以 regionID 前缀隔离，
// 区域边界由 RegionInfo.StartKey（含）/EndKey（不含）定义，保证连续无空洞。
//
// 默认单 region：表未显式分片时，Router 返回隐式整表单 region（ID = implicitRegionID(tableID)），
// 所有存取路径照常带 region 前缀，SQL 行为与未分片完全一致（零回归）。
package sharding

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sort"
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// RegionState 分片状态（M3 单机仅 ACTIVE，LEAVING 为迁移中预留）。
type RegionState string

const (
	// StateActive 正常服务中。
	StateActive RegionState = "ACTIVE"
	// StateLeaving 数据迁移中（预留，单机不触发）。
	StateLeaving RegionState = "LEAVING"
)

// RegionInfo 一个分片（Region）的元信息。
// Key 区间 [StartKey, EndKey)：StartKey 包含、EndKey 排他。
// 边界为内层行键字节（如 s{tableID}{pk}），nil 表示表键空间开放端（表起始/表结束）。
type RegionInfo struct {
	RegionID storage.ID   `json:"region_id"`
	TableID  storage.ID   `json:"table_id"`
	StartKey []byte       `json:"start_key,omitempty"`
	EndKey   []byte       `json:"end_key,omitempty"`
	State    RegionState  `json:"state,omitempty"`
	Local    bool         `json:"local"` // 数据是否物理位于本节点（M3 单机恒 true）
	Explicit bool         `json:"explicit"` // 显式分片标记（隐式单 region 为 false）
	// Node region 归属节点 ID（M4 集群路由）：空字符串 = 本节点（未指派）。
	// 该字段为 M4 新增，M3 既有落盘元数据不含此字段（JSON 反序列化后为空），
	// 语义默认"本节点"，保证未多节点化部署时路由行为零回归。
	Node string `json:"node,omitempty"`
}

// 标准错误。
var (
	// ErrRegionNotFound 路由未命中：key 不属于任何 region（表无显式划分且 key 越界 / 显式空洞）。
	ErrRegionNotFound = errors.New("sharding: no region for key")
)

// RegionKeyPrefixLen region 前缀长度（'r' + 8B regionID）。
const RegionKeyPrefixLen = 9

// implicitRegionID 隐式单 region 的 ID：tableID 高位置 1，与显式 region ID（低位自增）隔离。
func implicitRegionID(tableID storage.ID) storage.ID { return tableID | (1 << 63) }

// EncodeRegionKey 物理键编码：'r' + regionID(8B 大端) + innerKey。
// 同一 region 内键序与 innerKey 序一致；跨 region 以 regionID 前缀隔离。
func EncodeRegionKey(regionID storage.ID, key []byte) []byte {
	out := make([]byte, 0, RegionKeyPrefixLen+len(key))
	out = append(out, 'r')
	out = binary.BigEndian.AppendUint64(out, uint64(regionID))
	return append(out, key...)
}

// DecodeRegionKey 剥离 region 前缀，返回 regionID 与 innerKey。
// 入参必须是 EncodeRegionKey 产出的物理键（长度 >= RegionKeyPrefixLen）。
func DecodeRegionKey(k []byte) (storage.ID, []byte) {
	return storage.ID(binary.BigEndian.Uint64(k[1:9])), k[RegionKeyPrefixLen:]
}

// metaKey 全局分片元数据键（不带 region 前缀，与表目录同层）。
var metaKey = []byte("m:regions")

// MetaRegionsKey 返回分片元数据落盘键（供 cluster/db 等外部包读写同一份元数据）。
func MetaRegionsKey() []byte { return metaKey }

// regionsMeta 落盘格式：seq（显式 region ID 分配水位）+ regions 列表。
type regionsMeta struct {
	Seq     uint64       `json:"seq"`
	Regions []RegionInfo `json:"regions"`
}

// MarshalMeta 序列化路由表（seq 水位 + 全量 region 列表），供事务内落盘 m:regions。
func MarshalMeta(seq uint64, regions []RegionInfo) ([]byte, error) {
	return json.Marshal(regionsMeta{Seq: seq, Regions: regions})
}

// UnmarshalMeta 反序列化 m:regions 载荷。
func UnmarshalMeta(raw []byte) (uint64, []RegionInfo, error) {
	var m regionsMeta
	if err := json.Unmarshal(raw, &m); err != nil {
		return 0, nil, err
	}
	return m.Seq, m.Regions, nil
}

// Router 分片路由：内存 region 表（按 TableID + StartKey 排序）+ ID 分配水位。
// 单机进程内单实例，SQL 执行器与 db.DB 管理入口共享同一指针。
type Router struct {
	mu      sync.RWMutex
	regions []RegionInfo
	seq     uint64
}

// NewRouter 创建空路由表。
func NewRouter() *Router { return &Router{} }

// SetRegions 整体替换路由表（DB.Open 加载落盘元数据时调用）。
func (r *Router) SetRegions(seq uint64, regions []RegionInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq = seq
	r.regions = append([]RegionInfo(nil), regions...)
	r.sortLocked()
}

// Dump 返回路由表副本与 ID 水位（供落盘）。
func (r *Router) Dump() (uint64, []RegionInfo) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RegionInfo, len(r.regions))
	copy(out, r.regions)
	return r.seq, out
}

// RegionsOf 返回表当前 regions：
//   - 表存在显式划分时返回显式 regions（按 StartKey 升序）；
//   - 否则返回隐式整表单 region（ID = implicitRegionID(tableID)，覆盖整表键空间）。
func (r *Router) RegionsOf(tableID storage.ID) []RegionInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var explicit []RegionInfo
	for i := range r.regions {
		if r.regions[i].TableID == tableID {
			explicit = append(explicit, r.regions[i])
		}
	}
	if len(explicit) > 0 {
		return explicit
	}
	return []RegionInfo{{
		RegionID: implicitRegionID(tableID),
		TableID:  tableID,
		State:    StateActive,
		Local:    true,
		Explicit: false,
	}}
}

// Locate 定位 innerKey（完整内层键，如 s{tableID}{pk}）所属 region。
// 无显式划分的表返回隐式单 region；显式表按 StartKey 二分：
//   - 边界语义 [StartKey, EndKey)：StartKey 包含、EndKey 排他，nil = 开放端；
//   - 未命中（key 小于首个 StartKey 或大于等于某 region EndKey）返回 ErrRegionNotFound。
func (r *Router) Locate(tableID storage.ID, key []byte) (storage.ID, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var explicit []RegionInfo
	for i := range r.regions {
		if r.regions[i].TableID == tableID {
			explicit = append(explicit, r.regions[i])
		}
	}
	if len(explicit) == 0 {
		return implicitRegionID(tableID), nil
	}
	// 第一个 StartKey > key 的索引 i；候选为 explicit[i-1]（StartKey <= key）。
	i := sort.Search(len(explicit), func(i int) bool {
		return startLess(key, explicit[i].StartKey)
	})
	if i == 0 {
		return 0, ErrRegionNotFound // key < 最小 StartKey
	}
	rg := explicit[i-1]
	if rg.EndKey != nil && bytes.Compare(key, rg.EndKey) >= 0 {
		return 0, ErrRegionNotFound // key >= EndKey（显式划分出现空洞时触发）
	}
	return rg.RegionID, nil
}

// ReplaceRegions 替换某表的显式 regions（SplitTable 后调用），并推进 ID 水位。
func (r *Router) ReplaceRegions(tableID storage.ID, regions []RegionInfo, seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var kept []RegionInfo
	for i := range r.regions {
		if r.regions[i].TableID != tableID {
			kept = append(kept, r.regions[i])
		}
	}
	kept = append(kept, regions...)
	if seq > r.seq {
		r.seq = seq
	}
	r.regions = kept
	r.sortLocked()
}

// ---- M4 集群路由扩展 ----

// RegionNode 返回 region 归属节点：空字符串 = 本节点（未指派）。
// 隐式单 region（未显式分片）恒为本地。
func (r *Router) RegionNode(regionID storage.ID) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for i := range r.regions {
		if r.regions[i].RegionID == regionID {
			return r.regions[i].Node
		}
	}
	return ""
}

// SetRegionNode 指派 region 归属节点（ASSIGN REGION 管理入口）。
// regionID 必须是已存在的显式 region；隐式单 region 不允许指派。
func (r *Router) SetRegionNode(regionID storage.ID, node string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.regions {
		if r.regions[i].RegionID == regionID {
			r.regions[i].Node = node
			return nil
		}
	}
	return ErrRegionNotFound
}

// FindRegion 按 regionID 查找显式 region 元信息（找不到返回 false）。
func (r *Router) FindRegion(regionID storage.ID) (RegionInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for i := range r.regions {
		if r.regions[i].RegionID == regionID {
			return r.regions[i], true
		}
	}
	return RegionInfo{}, false
}

// UpsertRegion 插入或更新一个显式 region（M4 ASSIGN REGION 时目标节点学习归属）。
// 已存在则更新其 Node/State 等字段；不存在则追加（并按水位推进 ID）。
// 隐式单 region 不作为显式条目插入，返回 ErrRegionNotFound 语义由调用方处理。
func (r *Router) UpsertRegion(rg RegionInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.regions {
		if r.regions[i].RegionID == rg.RegionID {
			r.regions[i] = rg
			return nil
		}
	}
	if uint64(rg.RegionID) > r.seq {
		r.seq = uint64(rg.RegionID)
	}
	r.regions = append(r.regions, rg)
	r.sortLocked()
	return nil
}

// AllRegions 返回全部显式 region 元信息（按 TableID + StartKey 升序）。
func (r *Router) AllRegions() []RegionInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RegionInfo, len(r.regions))
	copy(out, r.regions)
	return out
}

// RemoveTable 删除某表全部 region 元数据（DROP TABLE 时调用）。
func (r *Router) RemoveTable(tableID storage.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var kept []RegionInfo
	for i := range r.regions {
		if r.regions[i].TableID != tableID {
			kept = append(kept, r.regions[i])
		}
	}
	r.regions = kept
}

// sortLocked 按 (TableID, StartKey) 升序排序；nil StartKey 视为表起始（-inf）排最前。
func (r *Router) sortLocked() {
	sort.SliceStable(r.regions, func(i, j int) bool {
		if r.regions[i].TableID != r.regions[j].TableID {
			return r.regions[i].TableID < r.regions[j].TableID
		}
		return startLess(r.regions[i].StartKey, r.regions[j].StartKey)
	})
}

// startLess 字节序比较：nil 视为 -inf（表起始），故 nil < 任何非 nil。
func startLess(a, b []byte) bool {
	if len(a) == 0 {
		return len(b) > 0
	}
	if len(b) == 0 {
		return false
	}
	return bytes.Compare(a, b) < 0
}

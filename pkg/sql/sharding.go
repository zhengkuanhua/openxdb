package sql

// M3 分片集成：Region 元数据落盘 / SplitTable / ListRegions / LocateRegion / 跨 region 扫描。
//
// 物理键 = sharding.EncodeRegionKey(regionID, innerKey)；innerKey 为 M1 内层键
// （行键 s{tableID}{pk}、索引键 i{tableID}{indexID}{idxVal}p{pk}）。
// 分片元数据以全局键 m:regions（不带 region 前缀）与表目录同层落盘，
// 由 SplitTable 在事务内与数据迁移原子提交；DB.Open 时加载回 Router。
//
// 默认单 region：未 split 的表由 Router.RegionsOf 返回隐式整表单 region，
// 所有存取路径照常带 region 前缀，SQL 行为与未分片完全一致（零回归）。

import (
	"bytes"
	"sort"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// metaRegionsKey 全局分片元数据键（不带 region 前缀）。
var metaRegionsKey = []byte("m:regions")

// ---- Router 注入与基础编码 ----

// SetRouter 注入分片路由（sql.New 默认注入空 Router；db.Open 加载落盘元数据后覆盖）。
func (e *Executor) SetRouter(r *sharding.Router) { e.router = r }

// regionsOf 返回表当前 regions（显式或隐式单 region）。
func (e *Executor) regionsOf(meta *TableMeta) []sharding.RegionInfo {
	return e.router.RegionsOf(storage.ID(meta.ID))
}

// locateRow 按主键字节定位 region（未命中返回 sharding.ErrRegionNotFound）。
func (e *Executor) locateRow(meta *TableMeta, pk []byte) (storage.ID, error) {
	return e.router.Locate(storage.ID(meta.ID), EncodeTableKey(meta.ID, pk))
}

// rowKeyAt 编码带 region 前缀的行键。
func (e *Executor) rowKeyAt(meta *TableMeta, rid storage.ID, pk []byte) []byte {
	return sharding.EncodeRegionKey(rid, EncodeTableKey(meta.ID, pk))
}

// indexKeyAt 编码带 region 前缀的索引键。
func (e *Executor) indexKeyAt(meta *TableMeta, rid storage.ID, idxID uint64, idxVal, pk []byte) []byte {
	return sharding.EncodeRegionKey(rid, EncodeIndexKey2(meta.ID, idxID, idxVal, pk))
}

// innerOf 剥离 region 前缀返回内层键。
func innerOf(k []byte) []byte { _, inner := sharding.DecodeRegionKey(k); return inner }

// ridOf 从物理键解出 regionID。
func ridOf(k []byte) storage.ID { rid, _ := sharding.DecodeRegionKey(k); return rid }

// ---- 跨 region 扫描 ----

// rowRanges 表全部行的物理扫描区间（按 regions 展开）。
// 每个 region 覆盖 [r{rid}s{tid}(StartKey), r{rid}s{tid+1}(EndKey))，
// StartKey/EndKey 为内层行键边界（nil = 表起始/表结束）。
func (e *Executor) rowRanges(meta *TableMeta) []storage.KeyRange {
	rs := e.regionsOf(meta)
	out := make([]storage.KeyRange, 0, len(rs))
	for _, rg := range rs {
		// StartKey/EndKey 落盘时已是内层行键（含表前缀 s{tid}），直接拼 region 前缀；
		// nil = 表起始/表结束，分别取 s{tid} 与 s{tid+1} 作为内层界。
		var startInner []byte
		if rg.StartKey == nil {
			startInner = EncodeTableKey(meta.ID, nil)
		} else {
			startInner = rg.StartKey
		}
		var endInner []byte
		if rg.EndKey == nil {
			endInner = EncodeTableKey(meta.ID+1, nil)
		} else {
			endInner = rg.EndKey
		}
		out = append(out, storage.KeyRange{Start: sharding.EncodeRegionKey(rg.RegionID, startInner), End: sharding.EncodeRegionKey(rg.RegionID, endInner)})
	}
	return out
}

// indexTableRanges 某索引整表物理区间（按 regions 展开）。
// 索引键 i{tableID}{indexID}... 所在 region 由写入时行路由决定；
// 对每个 region 扫描 [r{rid}i{tid}{idxID}*, r{rid}i{tid}{idxID+1}*) 覆盖该 region 全部索引键。
func (e *Executor) indexTableRanges(meta *TableMeta, idxID uint64) []storage.KeyRange {
	return e.indexRanges(meta, idxID,
		EncodeIndexKey2(meta.ID, idxID, nil, nil),
		EncodeIndexKey2(meta.ID, idxID+1, nil, nil))
}

// indexRanges 给定索引扫描的内层完整边界 [startInner, endInner) 按 regions 展开的物理区间。
// 对每个 region 均扫描 [r{rid}{startInner}, r{rid}{endInner})，
// 因 region 前缀与内层键前缀固定拼接，展开后覆盖该索引区间在该 region 的全部键。
func (e *Executor) indexRanges(meta *TableMeta, idxID uint64, startInner, endInner []byte) []storage.KeyRange {
	rs := e.regionsOf(meta)
	out := make([]storage.KeyRange, 0, len(rs))
	for _, rg := range rs {
		out = append(out, storage.KeyRange{
			Start: sharding.EncodeRegionKey(rg.RegionID, startInner),
			End:   sharding.EncodeRegionKey(rg.RegionID, endInner),
		})
	}
	return out
}

// scanMerged 对多个物理区间分别扫描，结果按 innerKey 升序稳定合并。
// 跨 region 展开后各 region 键空间隔离，合并排序保证内层键序与单 region 一致。
func (e *Executor) scanMerged(tx txn.Txn, ranges []storage.KeyRange) ([]storage.KVPair, error) {
	var pairs []storage.KVPair
	for _, r := range ranges {
		ps, err := tx.Scan(r, 0)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, ps...)
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		return bytes.Compare(innerOf(pairs[i].Key), innerOf(pairs[j].Key)) < 0
	})
	return pairs, nil
}

// ---- 分片元数据落盘 ----

// loadRegionMeta 从事务读取 m:regions（无分片元数据时返回空）。
func loadRegionMeta(get func(key []byte) ([]byte, error)) (uint64, []sharding.RegionInfo, error) {
	raw, err := get(metaRegionsKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return 0, nil, nil
		}
		return 0, nil, err
	}
	if len(raw) == 0 {
		return 0, nil, nil
	}
	seq, rs, err := sharding.UnmarshalMeta(raw)
	if err != nil {
		return 0, nil, &SQLError{Msg: "corrupted region catalog"}
	}
	return seq, rs, nil
}

// saveRegionMeta 序列化分片元数据（事务内与数据迁移原子提交）。
func saveRegionMeta(e *Executor) ([]byte, error) {
	seq, rs := e.router.Dump()
	return sharding.MarshalMeta(seq, rs)
}

// ---- 分片管理入口 ----

// SplitTable 将表键空间按主键边界划分为多个显式 region（SPLIT 形态）。
// boundaries 为升序、互异的主键字节边界（不含首尾）；划分后 region 数量 = len(boundaries)+1。
// 边界语义：每个 region 区间 [StartKey, EndKey)，StartKey 含、EndKey 排他；
// 首个 region StartKey=nil（表起始），末个 region EndKey=nil（表结束），保证连续无空洞。
// 存量数据（行 + 索引键）在同一事务内按新边界重路由迁移到对应 region 前缀。
func (e *Executor) SplitTable(name string, boundaries [][]byte) error {
	tx, err := e.getTx()
	if err != nil {
		return err
	}
	auto := e.autocommit()
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return err
	}
	meta := findTable(tabs, name)
	if meta == nil {
		return &SQLError{Msg: "table not exists: " + name}
	}
	for i := 1; i < len(boundaries); i++ {
		if bytes.Compare(boundaries[i-1], boundaries[i]) >= 0 {
			return &SQLError{Msg: "split boundaries must be strictly ascending"}
		}
	}
	// 构造新 region 列表（显式 ID 从 seq 水位继续分配）
	seq, _ := e.router.Dump()
	rs := make([]sharding.RegionInfo, 0, len(boundaries)+1)
	var prev []byte // 上一 region 的 EndKey；nil 表示从表起始开始
	for _, b := range boundaries {
		seq++
		rs = append(rs, sharding.RegionInfo{
			RegionID: storage.ID(seq),
			TableID:  storage.ID(meta.ID),
			StartKey: prev,
			EndKey:   EncodeTableKey(meta.ID, b),
			State:    sharding.StateActive,
			Local:    true,
			Explicit: true,
		})
		prev = EncodeTableKey(meta.ID, b)
	}
	seq++
	rs = append(rs, sharding.RegionInfo{
		RegionID: storage.ID(seq),
		TableID:  storage.ID(meta.ID),
		StartKey: prev,
		EndKey:   nil,
		State:    sharding.StateActive,
		Local:    true,
		Explicit: true,
	})
	// 存量数据重路由迁移
	if err := e.remapTable(tx, meta, rs); err != nil {
		return err
	}
	// 更新路由表并落盘（与迁移同事务）
	e.router.ReplaceRegions(storage.ID(meta.ID), rs, seq)
	raw, err := saveRegionMeta(e)
	if err != nil {
		return err
	}
	if err := tx.Put(metaRegionsKey, raw); err != nil {
		return err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// remapTable 将表全部物理行与索引键按新 regions 重路由（写新键、删旧键）。
// 使用临时 Router 视图定位，避免影响主路由；对已经在新 region 的键自动跳过。
func (e *Executor) remapTable(tx txn.Txn, meta *TableMeta, rs []sharding.RegionInfo) error {
	tmp := sharding.NewRouter()
	var seq uint64
	for _, rg := range rs {
		if uint64(rg.RegionID) > seq {
			seq = uint64(rg.RegionID)
		}
	}
	tmp.ReplaceRegions(storage.ID(meta.ID), rs, seq)
	locate := func(inner []byte) (storage.ID, error) { return tmp.Locate(storage.ID(meta.ID), inner) }
	// 行
	rowPairs, err := e.scanMerged(tx, e.rowRanges(meta))
	if err != nil {
		return err
	}
	for _, kv := range rowPairs {
		inner := innerOf(kv.Key)
		newRID, err := locate(inner)
		if err != nil {
			return err
		}
		newKey := sharding.EncodeRegionKey(newRID, inner)
		if bytes.Equal(newKey, kv.Key) {
			continue
		}
		if err := tx.Put(newKey, kv.Value); err != nil {
			return err
		}
		if err := tx.Delete(kv.Key); err != nil {
			return err
		}
	}
	// 索引（若表有索引）
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		idxPairs, err := e.scanMerged(tx, e.indexTableRanges(meta, idx.ID))
		if err != nil {
			return err
		}
		for _, kv := range idxPairs {
			inner := innerOf(kv.Key)
			pk := indexPKSuffix(inner)
			newRID, err := locate(EncodeTableKey(meta.ID, pk))
			if err != nil {
				return err
			}
			newKey := sharding.EncodeRegionKey(newRID, inner)
			if bytes.Equal(newKey, kv.Key) {
				continue
			}
			if err := tx.Put(newKey, kv.Value); err != nil {
				return err
			}
			if err := tx.Delete(kv.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

// ListRegions 返回表当前分片列表：
//   - 显式划分时返回全部显式 region（按 StartKey 升序）；
//   - 未划分时返回隐式整表单 region（Explicit=false，RegionID 高位带标记）。
func (e *Executor) ListRegions(name string) ([]sharding.RegionInfo, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, name)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + name}
	}
	rs := e.regionsOf(meta)
	out := make([]sharding.RegionInfo, len(rs))
	copy(out, rs)
	return out, nil
}

// LocateRegion 按主键字节定位 region（路由未命中返回 sharding.ErrRegionNotFound）。
func (e *Executor) LocateRegion(name string, pk []byte) (storage.ID, error) {
	tx, err := e.getTx()
	if err != nil {
		return 0, err
	}
	auto := e.autocommit()
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return 0, err
	}
	meta := findTable(tabs, name)
	if meta == nil {
		return 0, &SQLError{Msg: "table not exists: " + name}
	}
	return e.locateRow(meta, pk)
}

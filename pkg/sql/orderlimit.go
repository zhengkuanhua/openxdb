package sql

// M12-2 跨节点 ORDER BY / LIMIT 分页（docs/T25_m12_order_limit.md）。
//
// 执行模型（分片本地排序 + coordinator 归并）：
//   - ORDER BY + LIMIT/OFFSET：每个分片（region）本地按排序键排序后取
//     top-(offset+limit) 窗口行（LIMIT 场景即 top-N 下推；OFFSET 场景取足量），
//     coordinator 归并排序后应用 OFFSET/LIMIT 截断；
//   - 无 ORDER BY 仅 LIMIT/OFFSET：各分片按内层物理键序取足量 offset+limit 行，
//     coordinator 按键序合并后截断（结果序 = 内层物理键序，与单机全表序一致）；
//   - 正确性前提：窗口截断必须在 WHERE 过滤之后，故本路径仅限无 WHERE 查询；
//     带 WHERE 的查询走既有全量扫描 + coordinator 过滤排序（正确性优先）。
//   - 单分区 / 未分片表 / 无远端节点：canDistributed* 判定为假，退化为既有
//     scanMergedCluster + finishSelect 本地执行，行为与单机完全一致。
//
// 排序一致性：分片端按列类型比较（cluster.SortSpec），coordinator 端用
// compareVal；INT/DECIMAL 均为数值比较（DECIMAL 同为 scale=4 定点整数），
// TEXT/DATE/BLOB 按字符串/字节序，两端等价。

import (
	"bytes"
	"sort"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// innerLess 按内层物理键比较（等价 scanMergedCluster 的合并序）。
func innerLess(a, b []byte) bool {
	return bytes.Compare(innerOf(a), innerOf(b)) < 0
}

// canDistributedOrder 判定单表查询是否可走分布式排序分页路径：
// 集群启用 + ORDER BY 裸列 + 无聚合/窗口/别名/限定列 + 无 WHERE
// + 至少一个远端 region（否则退化为本地执行）。
func (e *Executor) canDistributedOrder(meta *TableMeta, s *SelectStmt) bool {
	if !e.clusterEnabled() || s.OrderBy == nil || s.OrderBy.Ref != "" {
		return false
	}
	if s.Limit <= 0 && s.Offset <= 0 {
		return false // 无分页需求，走既有全量排序（索引序优化等）
	}
	for _, c := range s.Columns {
		if c.Ref != "" || c.Alias != "" || c.Agg != "" || c.Window != nil {
			return false
		}
	}
	if s.Where != nil && len(s.Where.Conds) > 0 {
		return false // 窗口截断须在 WHERE 过滤后；带 WHERE 走全量兜底
	}
	return e.hasRemoteRegion(meta)
}

// canDistributedTop 判定无 ORDER BY 仅 LIMIT/OFFSET 时是否走"取足量再截断"：
// 集群启用 + 有分页 + 无 WHERE + 至少一个远端 region。
func (e *Executor) canDistributedTop(meta *TableMeta, s *SelectStmt) bool {
	if !e.clusterEnabled() || s.OrderBy != nil || (s.Limit <= 0 && s.Offset <= 0) {
		return false
	}
	if s.Where != nil && len(s.Where.Conds) > 0 {
		return false
	}
	return e.hasRemoteRegion(meta)
}

// hasRemoteRegion 表 rowRanges 覆盖的 region 中是否存在远端归属。
func (e *Executor) hasRemoteRegion(meta *TableMeta) bool {
	for _, r := range e.rowRanges(meta) {
		rid, _ := sharding.DecodeRegionKey(r.Start)
		if !e.regionLocal(rid) {
			return true
		}
	}
	return false
}

// execDistributedOrder 分布式排序分页主路径：
// 各分片本地排序取 top-(offset+limit) 窗口行 → coordinator 归并排序
// → finishSelect 应用 OFFSET/LIMIT 与投影（ordered=true 跳过重复排序）。
func (e *Executor) execDistributedOrder(sess *Session, tx txn.Txn, meta *TableMeta, s *SelectStmt) (*Result, error) {
	bs := e.snapshotTS(sess)
	ob := s.OrderBy
	ci := colIndex(meta, ob.Col)
	if ci < 0 {
		return nil, &SQLError{Msg: "order by column not found: " + ob.Col}
	}
	fetchN := s.Offset + s.Limit // canDistributedOrder 保证 > 0
	spec := &cluster.SortSpec{
		ColIndex: ci,
		Kind:     meta.Columns[ci].Type,
		Desc:     ob.Desc,
		Limit:    fetchN,
	}
	// 收集各分片窗口行（本地 + 远端），coordinator 端统一 decode 为 Value 归并。
	var got [][]Value
	for _, r := range e.rowRanges(meta) {
		rid, _ := sharding.DecodeRegionKey(r.Start)
		var pairs []storage.KVPair
		var err error
		if e.regionLocal(rid) {
			e.incLocal()
			pairs, err = e.scanMergedVisible(tx, []storage.KeyRange{r}, bs)
		} else {
			node := e.resolveRegionOwner(rid)
			if node == e.selfID {
				e.incLocal()
				pairs, err = e.scanMergedVisible(tx, []storage.KeyRange{r}, bs)
			} else {
				e.incRemote()
				rows, rerr := e.mgr.ScanSortedRemote(node, r.Start, r.End, bs, spec, e.txContextID(sess))
				if rerr != nil {
					return nil, rerr
				}
				pairs = make([]storage.KVPair, 0, len(rows))
				for _, row := range rows {
					pairs = append(pairs, storage.KVPair{Key: row.Key, Value: row.Value})
				}
			}
		}
		if err != nil {
			return nil, err
		}
		// 本地分片：decode 后按排序键取 top-(offset+limit)（与远端窗口语义一致）
		shardRows := make([][]Value, 0, len(pairs))
		for _, kv := range pairs {
			row, derr := decodeRow(meta, kv.Value)
			if derr != nil {
				return nil, derr
			}
			shardRows = append(shardRows, row)
		}
		sort.SliceStable(shardRows, func(i, j int) bool {
			cmp := compareVal(shardRows[i][ci], shardRows[j][ci])
			if ob.Desc {
				return cmp > 0
			}
			return cmp < 0
		})
		if len(shardRows) > fetchN {
			shardRows = shardRows[:fetchN]
		}
		got = append(got, shardRows...)
	}
	// coordinator 归并排序：与单机 finishSelect 同序（compareVal + 稳定排序）
	sort.SliceStable(got, func(i, j int) bool {
		cmp := compareVal(got[i][ci], got[j][ci])
		if ob.Desc {
			return cmp > 0
		}
		return cmp < 0
	})
	return e.finishSelect(meta, s, got, true)
}

// scanMergedClusterTop 无 ORDER BY 分页优化：各分片按内层物理键序取足量 n 行，
// coordinator 按键序合并后截断；结果序 = 内层物理键序（与 scanMergedCluster 一致）。
// 仅由 canDistributedTop 调用（无 WHERE 保证键序截断正确）。
func (e *Executor) scanMergedClusterTop(sess *Session, tx txn.Txn, ranges []storage.KeyRange, n int) ([]storage.KVPair, error) {
	bs := e.snapshotTS(sess)
	var out []storage.KVPair
	for _, r := range ranges {
		rid, _ := sharding.DecodeRegionKey(r.Start)
		var pairs []storage.KVPair
		var err error
		if e.regionLocal(rid) {
			e.incLocal()
			pairs, err = e.scanMergedVisible(tx, []storage.KeyRange{r}, bs)
		} else {
			node := e.resolveRegionOwner(rid)
			if node == e.selfID {
				e.incLocal()
				pairs, err = e.scanMergedVisible(tx, []storage.KeyRange{r}, bs)
			} else {
				e.incRemote()
				rows, rerr := e.mgr.ScanRemote(node, r.Start, r.End, n, bs, e.txContextID(sess))
				if rerr != nil {
					return nil, rerr
				}
				pairs = make([]storage.KVPair, 0, len(rows))
				for _, row := range rows {
					pairs = append(pairs, storage.KVPair{Key: row.Key, Value: row.Value})
				}
			}
		}
		if err != nil {
			return nil, err
		}
		// 本地/远端均按内层键序返回，取足量 n 行
		if len(pairs) > n {
			pairs = pairs[:n]
		}
		out = append(out, pairs...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return innerLess(out[i].Key, out[j].Key)
	})
	if len(out) > n {
		out = out[:n]
	}
	return out, nil
}

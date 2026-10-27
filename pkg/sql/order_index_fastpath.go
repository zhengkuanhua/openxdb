package sql

// v1.15.0 W4：ORDER BY 索引顺序扫描 + DISTINCT 免排序（DEV_PLAN_2026Q7）。
//
// 设计原则（语义严格等价，无法保证等价一律回退原路径）：
//   - orderByScanCandidate / orderByIndexCandidate 只做"候选"判定，最终免排序须经
//     rowsSortedBy* 做 O(n) 运行时验证（比较基准统一为 compareVal），验证通过才
//     跳过 sort.SliceStable，否则回退原排序路径；
//   - 主键编码序可能偏离比较序：单列 INT 主键负值（pkBytes 不翻转符号位）、复合
//     主键首列变长类型 2B 长度前缀序偏离字典序。二级索引单列编码（idxBytes）为
//     排序友好编码（INT/DECIMAL 翻转符号位），但为统一语义保证仍以运行时验证兜底；
//   - 分布式 Top 截断路径（scanMergedClusterTop）截断前会丢行、无法事后验证，仅限
//     单列主键 TEXT/DATE/BLOB（单列主键编码无长度前缀，字节序恒等于比较序）且
//     canDistributedOrder 已保证无 WHERE 的场景启用；
//   - Rel 路径（execSelectRel/finishRel）：单物理表（无 JOIN/子查询）+ ORDER BY 裸列
//     升序命中候选时验证后免排序；SELECT DISTINCT 仅在单列裸投影与 ORDER BY 列一致
//     且已有序时做相邻去重，其余场景保守走既有 map 去重路径。

import (
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)


// orderByScanCandidate 判定 ORDER BY 裸列升序是否可能由主键扫描序提供：
// 单列主键（ob.Col == meta.PK）或复合主键首列（ob.Col == meta.PKCols[0]）。
// 仅候选：最终免排序须经 rowsSortedBy* 运行时验证。
func orderByScanCandidate(meta *TableMeta, s *SelectStmt) bool {
	if s == nil || s.OrderBy == nil || s.OrderBy.Desc || s.OrderBy.Ref != "" || meta == nil {
		return false
	}
	if len(meta.PKCols) == 0 {
		return meta.PK != "" && s.OrderBy.Col == meta.PK
	}
	return s.OrderBy.Col == meta.PKCols[0]
}

// orderByIndexCandidate 判定 ORDER BY 裸列升序是否有单列二级索引可提供索引序。
// 仅支持单列索引（idx.Col 非空）：复合索引键为多列拼接编码（变长列带 2B 长度
// 前缀），首列序可能偏离比较序且定位 pk 偏移复杂，保守回退原路径。
func orderByIndexCandidate(meta *TableMeta, s *SelectStmt) *IndexMeta {
	if s == nil || s.OrderBy == nil || s.OrderBy.Desc || s.OrderBy.Ref != "" || meta == nil {
		return nil
	}
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		if len(idx.Cols) == 0 && idx.Col == s.OrderBy.Col {
			return idx
		}
	}
	return nil
}

// orderBySinglePKSafe 判定可安全走"各分片按物理键序取足量"Top 路径：
// ORDER BY 单列主键升序裸列且主键列为 TEXT/DATE/BLOB（单列主键编码无长度前缀，
// 字节序恒等于 compareVal 比较序）。INT/DECIMAL/复合主键编码序可能偏离比较序，
// 截断会丢行，一律保守回退。
func orderBySinglePKSafe(meta *TableMeta, s *SelectStmt) bool {
	if s == nil || s.OrderBy == nil || s.OrderBy.Desc || s.OrderBy.Ref != "" || meta == nil {
		return false
	}
	if len(meta.PKCols) != 0 || meta.PK == "" || meta.PK != s.OrderBy.Col {
		return false
	}
	for _, c := range meta.Columns {
		if c.Name != meta.PK {
			continue
		}
		switch c.Type {
		case "TEXT", "VARCHAR", "CHAR", "DATE", "BLOB":
			return true
		}
		return false
	}
	return false
}

// rowsSortedByValue O(n) 验证 rows 已按 ob 有序（compareVal 比较；相等元素保持
// 既有相对序，与 sort.SliceStable 输出一致，验证通过即可免排序且语义等价）。
func rowsSortedByValue(meta *TableMeta, rows [][]Value, ob *OrderBy) bool {
	if len(rows) < 2 {
		return true
	}
	for i := 1; i < len(rows); i++ {
		a, err := rowValue(meta, rows[i-1], ob.Col)
		if err != nil {
			return false
		}
		b, err := rowValue(meta, rows[i], ob.Col)
		if err != nil {
			return false
		}
		cmp := compareVal(a, b)
		if ob.Desc {
			if cmp < 0 {
				return false
			}
		} else if cmp > 0 {
			return false
		}
	}
	return true
}

// rowsSortedByRel 同上，按 Rel 限定列名取值（Rel 路径）。
func rowsSortedByRel(rel *Rel, rows [][]Value, ob *OrderBy) bool {
	if len(rows) < 2 {
		return true
	}
	qn := qualifiedName(ob.Ref, ob.Col)
	for i := 1; i < len(rows); i++ {
		a, err := relValue(rel, rows[i-1], qn)
		if err != nil {
			return false
		}
		b, err := relValue(rel, rows[i], qn)
		if err != nil {
			return false
		}
		cmp := compareVal(a, b)
		if ob.Desc {
			if cmp < 0 {
				return false
			}
		} else if cmp > 0 {
			return false
		}
	}
	return true
}

// indexScanRowsForOrderBy 普通路径：按 ORDER BY 单列索引全范围扫描 → 批量回表 →
// WHERE 过滤，返回按索引列升序的可见行（finishSelect 内已序验证兜底）。
func (e *Executor) indexScanRowsForOrderBy(sess *Session, tx txn.Txn, meta *TableMeta, idx *IndexMeta, s *SelectStmt) ([][]Value, error) {
	start, end := IndexRange(meta.ID, idx.ID, nil, nil)
	pairs, err := e.scanMergedCluster(sess, tx, e.indexRanges(meta, idx.ID, start, end))
	if err != nil {
		return nil, err
	}
	// 批量回表：先收集全部行键，一次批量读（本地键经 tx.MGet，减少 cgo 往返）；
	// 与 indexLookup 回表方式一致（索引键与行键同 region，按 region 前缀读取）。
	rowKeys := make([][]byte, 0, len(pairs))
	for _, kv := range pairs {
		inner := innerOf(kv.Key)
		pk := indexPKSuffixLen(inner, idxValLenOf(meta, idx))
		rowKeys = append(rowKeys, e.rowKeyAt(meta, ridOf(kv.Key), pk))
	}
	got, err := e.mgetKVCluster(sess, tx, rowKeys)
	if err != nil {
		return nil, err
	}
	rows := make([][]Value, 0, len(pairs))
	for _, kv := range pairs {
		inner := innerOf(kv.Key)
		pk := indexPKSuffixLen(inner, idxValLenOf(meta, idx))
		rk := e.rowKeyAt(meta, ridOf(kv.Key), pk)
		raw, ok := got[string(rk)]
		if !ok {
			continue // 索引脏键（理论上不出现）
		}
		row, err := decodeRow(meta, raw)
		if err != nil {
			return nil, err
		}
		ok, err = matchWhere(meta, row, s.Where)
		if err != nil {
			return nil, err
		}
		if ok {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// relFromIndexScan Rel 路径：按 ORDER BY 单列索引全范围扫描构建 Rel（全列可见行，
// 索引序 = 索引列升序）。仅单物理表、无 JOIN/子查询场景由 execSelectRel 调用；
// 后续 WHERE 过滤保序，ORDER BY 列 = 索引列时 finishRel 免排序（DISTINCT 相邻去重亦受益）。
func (e *Executor) relFromIndexScan(sess *Session, tx txn.Txn, meta *TableMeta, alias string, idx *IndexMeta) (*Rel, error) {
	start, end := IndexRange(meta.ID, idx.ID, nil, nil)
	pairs, err := e.scanMergedCluster(sess, tx, e.indexRanges(meta, idx.ID, start, end))
	if err != nil {
		return nil, err
	}
	rel := &Rel{
		Cols:  make([]string, 0, len(meta.Columns)),
		Types: make([]string, 0, len(meta.Columns)),
		Rows:  make([][]Value, 0, len(pairs)),
	}
	for _, c := range visibleColumns(meta) {
		rel.Cols = append(rel.Cols, alias+"."+c.Name)
		rel.Types = append(rel.Types, c.Type)
	}
	rowKeys := make([][]byte, 0, len(pairs))
	for _, kv := range pairs {
		inner := innerOf(kv.Key)
		pk := indexPKSuffixLen(inner, idxValLenOf(meta, idx))
		rowKeys = append(rowKeys, e.rowKeyAt(meta, ridOf(kv.Key), pk))
	}
	got, err := e.mgetKVCluster(sess, tx, rowKeys)
	if err != nil {
		return nil, err
	}
	for _, kv := range pairs {
		inner := innerOf(kv.Key)
		pk := indexPKSuffixLen(inner, idxValLenOf(meta, idx))
		rk := e.rowKeyAt(meta, ridOf(kv.Key), pk)
		raw, ok := got[string(rk)]
		if !ok {
			continue
		}
		row, err := decodeRow(meta, raw)
		if err != nil {
			return nil, err
		}
		rel.Rows = append(rel.Rows, visibleRow(meta, row))
	}
	return rel, nil
}

package sql

import (
	"bytes"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// fkRef 引用关系（子表 + 外键定义）。
type fkRef struct {
	child *TableMeta
	fk    *ForeignKey
}

// fkReferencing 收集所有引用 parentName 作为父表的外键关系。
func fkReferencing(tabs []*TableMeta, parentName string) []fkRef {
	var refs []fkRef
	for _, t := range tabs {
		if t.Name == parentName {
			continue
		}
		for i := range t.ForeignKeys {
			if t.ForeignKeys[i].RefTable == parentName {
				refs = append(refs, fkRef{child: t, fk: &t.ForeignKeys[i]})
			}
		}
	}
	return refs
}

// validateForeignKeys 建表时校验外键定义：父表存在、列存在且数量一致、
// 引用列为父表主键、列类型兼容（子列类型须与父引用列类型一致）。
func validateForeignKeys(tabs []*TableMeta, meta *TableMeta) error {
	for i := range meta.ForeignKeys {
		fk := &meta.ForeignKeys[i]
		parent := findTable(tabs, fk.RefTable)
		if parent == nil {
			return &SQLError{Msg: "foreign key referenced table not exists: " + fk.RefTable}
		}
		if len(fk.Columns) == 0 || len(fk.Columns) != len(fk.RefColumns) {
			return &SQLError{Msg: "foreign key column count mismatch"}
		}
		// 引用列必须为父表主键（M9 取舍：仅支持引用主键）
		for _, rc := range fk.RefColumns {
			if rc != parent.PK {
				return &SQLError{Msg: "foreign key referenced column must be primary key: " + rc}
			}
		}
		for _, cc := range fk.Columns {
			if colIndex(meta, cc) < 0 {
				return &SQLError{Msg: "unknown column in foreign key: " + cc}
			}
		}
		// 类型兼容：子列类型与父主键列类型一致
		pkType := ""
		for _, c := range parent.Columns {
			if c.Name == parent.PK {
				pkType = c.Type
				break
			}
		}
		for _, cc := range fk.Columns {
			for _, c := range meta.Columns {
				if c.Name == cc && strings.ToUpper(c.Type) != pkType {
					return &SQLError{Msg: "foreign key column type mismatch: " + cc + " (" + c.Type + " vs " + pkType + ")"}
				}
			}
		}
		fk.OnDelete = strings.ToUpper(fk.OnDelete)
		if fk.OnDelete == "" {
			fk.OnDelete = "RESTRICT"
		}
		fk.OnUpdate = strings.ToUpper(fk.OnUpdate)
		if fk.OnUpdate == "" {
			fk.OnUpdate = "RESTRICT"
		}
		// P0-3：SET NULL 要求外键列可空——外键列不得为主键列
		// （主键隐含 NOT NULL，置 NULL 会破坏行键，建表时直接拒绝）。
		if fk.OnDelete == "SET NULL" || fk.OnUpdate == "SET NULL" {
			for _, cc := range fk.Columns {
				if pkHasCol(meta, cc) {
					return &SQLError{Msg: "foreign key SET NULL requires nullable column: " + cc}
				}
			}
		}
	}
	return nil
}

// pkHasCol 判断列是否属于表主键（单列 PK 或复合 PKCols 的一部分）。
func pkHasCol(meta *TableMeta, col string) bool {
	if meta.PK != "" && meta.PK == col {
		return true
	}
	for _, c := range meta.PKCols {
		if c == col {
			return true
		}
	}
	return false
}

// fkRowMatches 判断子行是否引用父行（按外键列值逐对比较）。
func fkRowMatches(parent *TableMeta, parentRow []Value, child *TableMeta, childRow []Value, fk *ForeignKey) bool {
	for i, rc := range fk.RefColumns {
		pv, err := rowValue(parent, parentRow, rc)
		if err != nil {
			return false
		}
		cv, err := rowValue(child, childRow, fk.Columns[i])
		if err != nil {
			return false
		}
		if compareVal(pv, cv) != 0 {
			return false
		}
	}
	return true
}

// checkFKOnRow 子表插入/更新前检查全部外键引用存在性（父表行必须存在且匹配）。
func (e *Executor) checkFKOnRow(sess *Session, tx txn.Txn, tabs []*TableMeta, meta *TableMeta, vals []Value) error {
	for i := range meta.ForeignKeys {
		fk := &meta.ForeignKeys[i]
		parent := findTable(tabs, fk.RefTable)
		if parent == nil {
			return &SQLError{Msg: "foreign key referenced table not exists: " + fk.RefTable}
		}
		if !e.fkParentRowExists(sess, tx, parent, meta, vals, fk) {
			return &SQLError{Msg: "foreign key constraint violated: no matching row in " + fk.RefTable + " for column(s) " + strings.Join(fk.Columns, ",")}
		}
	}
	return nil
}

// fkParentRowExists 读取父表主键行并做完整匹配。
func (e *Executor) fkParentRowExists(sess *Session, tx txn.Txn, parent *TableMeta, child *TableMeta, childRow []Value, fk *ForeignKey) bool {
	// 子表外键列值
	subVals := make([]Value, len(fk.Columns))
	for j, c := range fk.Columns {
		v, err := rowValue(child, childRow, c)
		if err != nil {
			return false
		}
		subVals[j] = v
	}
	pk, err := pkBytes(parent, subVals[0])
	if err != nil {
		return false
	}
	rid, err := e.locateRow(parent, pk)
	if err != nil {
		return false
	}
	raw, err := e.getKVCluster(sess, tx, e.rowKeyAt(parent, rid, pk))
	if err != nil {
		if err == storage.ErrNotFound {
			return false
		}
		return false
	}
	parentRow, err := decodeRow(parent, raw)
	if err != nil {
		return false
	}
	return fkRowMatches(parent, parentRow, child, childRow, fk)
}

// collectFKDeleteOps 删除父表行时的外键联动：扫描引用父表的全部子表行，
// 匹配引用行的按 ON DELETE 处理——CASCADE 收集子行删除 ops（含索引清理）；
// SET NULL 收集子行外键列置 NULL ops（索引键删除 + 行重写，原子同事务）；
// RESTRICT（默认）直接报错阻止删除。
func (e *Executor) collectFKDeleteOps(sess *Session, tx txn.Txn, tabs []*TableMeta, parent *TableMeta, parentRow []Value) ([]kvOp, error) {
	var ops []kvOp
	for _, ref := range fkReferencing(tabs, parent.Name) {
		pairs, err := e.scanMerged(tx, e.rowRanges(ref.child))
		if err != nil {
			return nil, err
		}
		for _, kv := range pairs {
			row, err := decodeRow(ref.child, kv.Value)
			if err != nil {
				return nil, err
			}
			if !fkRowMatches(parent, parentRow, ref.child, row, ref.fk) {
				continue
			}
			if ref.fk.OnDelete == "CASCADE" {
				rid := ridOf(kv.Key)
				pk := pkSuffixOf(ref.child, innerOf(kv.Key))
				dops, err := e.delIndexKeyOps(ref.child, row, pk, rid)
				if err != nil {
					return nil, err
				}
				ops = append(ops, dops...)
				ops = append(ops, kvOp{Key: kv.Key, Delete: true})
				continue
			}
			if ref.fk.OnDelete == "SET NULL" {
				newRow := append([]Value{}, row...)
				for _, c := range ref.fk.Columns {
					ci := colIndex(ref.child, c)
					newRow[ci] = Value{Kind: "NULL"}
				}
				rid := ridOf(kv.Key)
				pk := pkSuffixOf(ref.child, innerOf(kv.Key))
				dops, err := e.delIndexKeyOps(ref.child, row, pk, rid)
				if err != nil {
					return nil, err
				}
				ops = append(ops, dops...)
				raw, err := encodeRow(ref.child, newRow)
				if err != nil {
					return nil, err
				}
				ops = append(ops, kvOp{Key: kv.Key, Value: raw})
				iops, err := e.putIndexKeyOps(sess, tx, ref.child, newRow, pk, rid, nil, nil)
				if err != nil {
					return nil, err
				}
				ops = append(ops, iops...)
				continue
			}
			return nil, &SQLError{Msg: "foreign key constraint violated: " + ref.child.Name + " references " + parent.Name}
		}
	}
	return ops, nil
}

// collectFKUpdateOps 更新父表行且主键值变化时的外键联动：扫描引用父表的全部
// 子表行，匹配旧父键值的按 ON UPDATE 动作处理：
//   - RESTRICT（默认）：存在引用行则报错阻止更新（与 ON DELETE RESTRICT 同语义）；
//   - CASCADE：收集子行更新 ops（外键列改为新父键对应值，含旧索引键删除、
//     行重写、新索引键写入）；若级联改写子行自身主键，递归联动更深层引用；
//   - SET NULL：收集子行外键列置 NULL 的 ops（外键列不可空在建表校验已拒绝）。
//
// 所有 ops 与父行更新同事务原子提交（2PC），跨 region 同样生效。
func (e *Executor) collectFKUpdateOps(sess *Session, tx txn.Txn, tabs []*TableMeta, parent *TableMeta, parentRow, newParentRow []Value) ([]kvOp, error) {
	return e.collectFKUpdateOpsRec(sess, tx, tabs, parent, parentRow, newParentRow, make(map[string]bool))
}

func (e *Executor) collectFKUpdateOpsRec(sess *Session, tx txn.Txn, tabs []*TableMeta, parent *TableMeta, parentRow, newParentRow []Value, visited map[string]bool) ([]kvOp, error) {
	var ops []kvOp
	for _, ref := range fkReferencing(tabs, parent.Name) {
		pairs, err := e.scanMerged(tx, e.rowRanges(ref.child))
		if err != nil {
			return nil, err
		}
		for _, kv := range pairs {
			row, err := decodeRow(ref.child, kv.Value)
			if err != nil {
				return nil, err
			}
			if !fkRowMatches(parent, parentRow, ref.child, row, ref.fk) {
				continue
			}
			rid := ridOf(kv.Key)
			cpk := pkSuffixOf(ref.child, innerOf(kv.Key))
			switch ref.fk.OnUpdate {
			case "CASCADE":
				newRow := append([]Value{}, row...)
				for i, rc := range ref.fk.RefColumns {
					nv, err := rowValue(parent, newParentRow, rc)
					if err != nil {
						return nil, err
					}
					ci := colIndex(ref.child, ref.fk.Columns[i])
					newRow[ci] = nv
				}
				// 子行主键被级联改写 → 递归联动更深层引用（多级外键链）
				ncpk, err := e.pkOf(ref.child, newRow)
				if err != nil {
					return nil, err
				}
				if !bytes.Equal(cpk, ncpk) {
					mark := ref.child.Name + ":" + string(cpk)
					if !visited[mark] {
						visited[mark] = true
						sub, err := e.collectFKUpdateOpsRec(sess, tx, tabs, ref.child, row, newRow, visited)
						if err != nil {
							return nil, err
						}
						ops = append(ops, sub...)
					}
				}
				dops, err := e.delIndexKeyOps(ref.child, row, cpk, rid)
				if err != nil {
					return nil, err
				}
				ops = append(ops, dops...)
				raw, err := encodeRow(ref.child, newRow)
				if err != nil {
					return nil, err
				}
				if bytes.Equal(cpk, ncpk) {
					// 主键未变：原地写行
					ops = append(ops, kvOp{Key: kv.Key, Value: raw})
				} else {
					// 主键被级联改写：删旧行键、按新主键路由写新行键（行与索引同 region）
					ops = append(ops, kvOp{Key: kv.Key, Delete: true})
					nrid, err := e.locateRow(ref.child, ncpk)
					if err != nil {
						return nil, err
					}
					ops = append(ops, kvOp{Key: e.rowKeyAt(ref.child, nrid, ncpk), Value: raw})
					rid = nrid
				}
				iops, err := e.putIndexKeyOps(sess, tx, ref.child, newRow, cpk, rid, nil, nil)
				if err != nil {
					return nil, err
				}
				ops = append(ops, iops...)
			case "SET NULL":
				// 外键列不可空（含为主键）已在建表校验拒绝，此处防御性再查
				for _, c := range ref.fk.Columns {
					if pkHasCol(ref.child, c) {
						return nil, &SQLError{Msg: "foreign key SET NULL requires nullable column: " + c}
					}
				}
				newRow := append([]Value{}, row...)
				for _, c := range ref.fk.Columns {
					ci := colIndex(ref.child, c)
					newRow[ci] = Value{Kind: "NULL"}
				}
				dops, err := e.delIndexKeyOps(ref.child, row, cpk, rid)
				if err != nil {
					return nil, err
				}
				ops = append(ops, dops...)
				raw, err := encodeRow(ref.child, newRow)
				if err != nil {
					return nil, err
				}
				ops = append(ops, kvOp{Key: kv.Key, Value: raw})
				iops, err := e.putIndexKeyOps(sess, tx, ref.child, newRow, cpk, rid, nil, nil)
				if err != nil {
					return nil, err
				}
				ops = append(ops, iops...)
			default: // RESTRICT（含缺省）
				return nil, &SQLError{Msg: "foreign key constraint violated: " + ref.child.Name + " references " + parent.Name}
			}
		}
	}
	return ops, nil
}



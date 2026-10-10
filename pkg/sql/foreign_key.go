package sql

import (
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
	}
	return nil
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
func (e *Executor) checkFKOnRow(tx txn.Txn, tabs []*TableMeta, meta *TableMeta, vals []Value) error {
	for i := range meta.ForeignKeys {
		fk := &meta.ForeignKeys[i]
		parent := findTable(tabs, fk.RefTable)
		if parent == nil {
			return &SQLError{Msg: "foreign key referenced table not exists: " + fk.RefTable}
		}
		if !e.fkParentRowExists(tx, parent, meta, vals, fk) {
			return &SQLError{Msg: "foreign key constraint violated: no matching row in " + fk.RefTable + " for column(s) " + strings.Join(fk.Columns, ",")}
		}
	}
	return nil
}

// fkParentRowExists 读取父表主键行并做完整匹配。
func (e *Executor) fkParentRowExists(tx txn.Txn, parent *TableMeta, child *TableMeta, childRow []Value, fk *ForeignKey) bool {
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
	raw, err := e.getKVCluster(tx, e.rowKeyAt(parent, rid, pk))
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
// 匹配引用行的按 ON DELETE 处理——CASCADE 收集子行删除 ops（含索引清理），
// RESTRICT（默认）直接报错阻止删除。
func (e *Executor) collectFKDeleteOps(tx txn.Txn, tabs []*TableMeta, parent *TableMeta, parentRow []Value) ([]kvOp, error) {
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
			if ref.fk.OnDelete != "CASCADE" {
				return nil, &SQLError{Msg: "foreign key constraint violated: " + ref.child.Name + " references " + parent.Name}
			}
			rid := ridOf(kv.Key)
			pk := pkSuffixOf(ref.child, innerOf(kv.Key))
			dops, err := e.delIndexKeyOps(ref.child, row, pk, rid)
			if err != nil {
				return nil, err
			}
			ops = append(ops, dops...)
			ops = append(ops, kvOp{Key: kv.Key, Delete: true})
		}
	}
	return ops, nil
}

// fkParentReferenced 判断父表 parent 是否仍被任何子表行引用（UPDATE 父表防护）。
func (e *Executor) fkParentReferenced(tx txn.Txn, tabs []*TableMeta, parent *TableMeta, parentRow []Value) (bool, error) {
	for _, ref := range fkReferencing(tabs, parent.Name) {
		pairs, err := e.scanMerged(tx, e.rowRanges(ref.child))
		if err != nil {
			return false, err
		}
		for _, kv := range pairs {
			row, err := decodeRow(ref.child, kv.Value)
			if err != nil {
				return false, err
			}
			if fkRowMatches(parent, parentRow, ref.child, row, ref.fk) {
				return true, nil
			}
		}
	}
	return false, nil
}

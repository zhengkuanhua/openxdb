package sql

import (
	"sort"
	"strings"
)

// M18-A：information_schema 兼容虚拟表（MySQL 风格只读目录视图）。
// 虚拟表不落盘、不进 m:tables 目录，由 execSelect 在解析后按表名
// 动态生成行集；写入（CREATE/INSERT/UPDATE/DELETE）一律拒绝。
//
// 支持的虚拟表：
//   - information_schema.schemata  ：库列表（OpenXDB 单库模型，恒为 default）
//   - information_schema.tables    ：表目录（引擎/类型等常量列）
//   - information_schema.columns   ：列目录（列名/类型/序号/可空/键属性）
//   - information_schema.statistics：索引统计（PK 与二级索引）
//   - information_schema.views     ：视图目录（M9 视图定义）
const infoSchemaPrefix = "information_schema."

// infoSchemaTables 虚拟表 -> 输出列（MySQL 列名子集，保持常见工具查询兼容）。
var infoSchemaTables = map[string][]string{
	"schemata":   {"SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME"},
	"tables":     {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "ROW_COUNT"},
	"columns":    {"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_TYPE", "DATA_TYPE", "IS_NULLABLE", "COLUMN_KEY"},
	"statistics": {"TABLE_NAME", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "NON_UNIQUE"},
	"views":      {"TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION", "SECURITY_TYPE"},
}

// isInfoSchemaName 判断是否为 information_schema 限定名（虚拟表或任意子表），大小写不敏感。
func isInfoSchemaName(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), infoSchemaPrefix)
}

// isInfoSchemaTable 判断 name（schema.table 全名）是否为受支持的虚拟表，大小写不敏感。
func isInfoSchemaTable(name string) bool {
	t := strings.ToLower(strings.TrimPrefix(strings.ToLower(name), infoSchemaPrefix))
	_, ok := infoSchemaTables[t]
	return ok
}

// normalizeTableRef 规范化 FROM/JOIN 表引用：information_schema.* 保留全名；
// 其余 schema.table 按 OpenXDB 单库模型剥去 schema（兼容 default.xxx 写法）。
func normalizeTableRef(name string) string {
	if isInfoSchemaName(name) {
		return name
	}
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// execInfoSchema 生成虚拟目录行集并应用 WHERE 等值过滤与列投影。
// tabs 为真实表目录（nil 时按空目录处理）；s 为原始 SELECT（From 已保证是虚拟表）。
func execInfoSchema(s *SelectStmt, tabs []*TableMeta) (*Result, error) {
	t := strings.ToLower(strings.TrimPrefix(strings.ToLower(s.From), infoSchemaPrefix))
	cols, ok := infoSchemaTables[t]
	if !ok {
		return nil, &SQLError{Msg: "unknown information_schema table: " + t}
	}
	rows := buildInfoSchemaRows(t, tabs)
	// WHERE 等值/比较过滤：列名大小写不敏感，TEXT/INT 按值比较。
	if s.Where != nil {
		filtered := make([][]Value, 0, len(rows))
		for _, row := range rows {
			ok, err := matchInfoSchemaWhere(cols, row, s.Where)
			if err != nil {
				return nil, err
			}
			if ok {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}
	// M18-C：ORDER BY 排序（多列稳定排序；INT 按数值，TEXT 按字符串）。
	if err := orderInfoSchemaRows(rows, cols, s); err != nil {
		return nil, err
	}
	// 列投影：SELECT * 返回全列；否则按选择顺序输出（未知列报错）。
	outCols := cols
	outRows := rows
	if len(s.Columns) > 0 && !(len(s.Columns) == 1 && s.Columns[0].Raw == "*") {
		outCols = make([]string, 0, len(s.Columns))
		idx := make([]int, 0, len(s.Columns))
		for _, c := range s.Columns {
			name := c.Name
			if name == "" {
				name = c.Raw
			}
			name = strings.ToUpper(strings.TrimSpace(name))
			found := -1
			for j, col := range cols {
				if col == name {
					found = j
					break
				}
			}
			if found < 0 {
				return nil, &SQLError{Msg: "unknown column: " + name}
			}
			outCols = append(outCols, cols[found])
			idx = append(idx, found)
		}
		outRows = make([][]Value, 0, len(rows))
		for _, row := range rows {
			nr := make([]Value, len(idx))
			for j, k := range idx {
				nr[j] = row[k]
			}
			outRows = append(outRows, nr)
		}
	}
	return &Result{Columns: outCols, Rows: outRows}, nil
}

// buildInfoSchemaRows 构造虚拟表全量行集。
func buildInfoSchemaRows(t string, tabs []*TableMeta) [][]Value {
	var rows [][]Value
	switch t {
	case "schemata":
		rows = [][]Value{
			{StrVal("default"), StrVal("utf8mb4"), StrVal("utf8mb4_general_ci")},
		}
	case "tables":
		for _, tab := range tabs {
			if tab.Name == "" {
				continue
			}
			rows = append(rows, []Value{
				StrVal("def"),
				StrVal("default"),
				StrVal(tab.Name),
				StrVal("BASE TABLE"),
				StrVal("RocksDB"),
				IntVal(0),
			})
		}
	case "columns":
		for _, tab := range tabs {
			if tab.Name == "" {
				continue
			}
			for i, c := range tab.Columns {
				colType := c.Type
				if colType == "" {
					colType = "TEXT"
				}
				key := ""
				if c.Name == tab.PK {
					key = "PRI"
				}
				nullable := "YES"
				if key == "PRI" {
					nullable = "NO"
				}
				rows = append(rows, []Value{
					StrVal("default"),
					StrVal(tab.Name),
					StrVal(c.Name),
					IntVal(int64(i + 1)),
					StrVal(colType),
					StrVal(colType),
					StrVal(nullable),
					StrVal(key),
				})
			}
		}
	case "statistics":
		for _, tab := range tabs {
			if tab.Name == "" {
				continue
			}
			// 主键行
			rows = append(rows, []Value{
				StrVal(tab.Name), StrVal("PRIMARY"), IntVal(1), StrVal(tab.PK), IntVal(0),
			})
			// 二级索引行（单列索引；SEQ_IN_INDEX 恒 1）
			for _, ix := range tab.Indexes {
				nonUnique := IntVal(1)
				if ix.Unique {
					nonUnique = IntVal(0)
				}
				rows = append(rows, []Value{
					StrVal(tab.Name), StrVal(ix.Name), IntVal(1), StrVal(ix.Col), nonUnique,
				})
			}
		}
	case "views":
		// 视图目录：execSelect 拦截层已把虚拟表查询挡在 expandViews 之外，
		// 此处直接返回空行集（视图信息以 SHOW VIEWS / CREATE VIEW 原文为准）。
		rows = nil
	}
	return rows
}

// matchInfoSchemaWhere 对虚拟表行应用 WHERE（仅支持列名比较；IN/LIKE 等不在信息目录场景展开）。
func matchInfoSchemaWhere(cols []string, row []Value, w *Where) (bool, error) {
	for _, c := range w.Conds {
		name := strings.ToUpper(c.Col)
		idx := -1
		for j, col := range cols {
			if col == name {
				idx = j
				break
			}
		}
		if idx < 0 {
			return false, &SQLError{Msg: "unknown column: " + c.Col}
		}
		if !matchCond(row[idx], c.Op, c.Val) {
			return false, nil
		}
	}
	return true, nil
}

// orderInfoSchemaRows 按 ORDER BY 应用多列稳定排序（列名大小写不敏感；
// INT 按数值比较，其余按字符串比较；DESC 反转；未知排序列报错）。
func orderInfoSchemaRows(rows [][]Value, cols []string, s *SelectStmt) error {
	if len(s.OrderBys) == 0 && s.OrderBy == nil {
		return nil
	}
	obs := s.OrderBys
	if len(obs) == 0 && s.OrderBy != nil {
		obs = []OrderBy{*s.OrderBy}
	}
	type sortKey struct {
		idx  int
		desc bool
	}
	ks := make([]sortKey, 0, len(obs))
	for _, ob := range obs {
		name := strings.ToUpper(strings.TrimSpace(ob.Col))
		found := -1
		for j, col := range cols {
			if col == name {
				found = j
				break
			}
		}
		if found < 0 {
			return &SQLError{Msg: "unknown column: " + ob.Col}
		}
		ks = append(ks, sortKey{idx: found, desc: ob.Desc})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for _, k := range ks {
			cmp := compareInfoValues(rows[i][k.idx], rows[j][k.idx])
			if k.desc {
				cmp = -cmp
			}
			if cmp < 0 {
				return true
			}
			if cmp > 0 {
				return false
			}
		}
		return false
	})
	return nil
}

// compareInfoValues 比较两个虚拟表单元格：INT 按数值，其余按字符串。
func compareInfoValues(a, b Value) int {
	if a.Kind == "INT" && b.Kind == "INT" {
		switch {
		case a.I < b.I:
			return -1
		case a.I > b.I:
			return 1
		}
		return 0
	}
	switch {
	case a.S < b.S:
		return -1
	case a.S > b.S:
		return 1
	}
	return 0
}

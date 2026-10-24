// Package mysqldump 解析 mysqldump 导出的 SQL 文本（M14 生态接入簇子功能一：mysqldump 兼容导入）。
//
// 本包只做文本级解析：把 mysqldump 的 CREATE TABLE / INSERT 语句转换为结构化数据
// （表定义 + 行数据），不依赖 OpenXDB 的 SQL 执行层，由 pkg/sql 的执行入口
// 负责建表与批量灌入。
//
// 兼容的 MySQL 语法子集：
//   - 反引号标识符（含 `` 转义）；
//   - CREATE TABLE 中的列定义、行内/表级 PRIMARY KEY、表级约束（KEY/INDEX/UNIQUE/FOREIGN
//     忽略）、类型参数（VARCHAR(255)/DECIMAL(10,2)/ENUM(...)）、NOT NULL/DEFAULT/
//     AUTO_INCREMENT/COMMENT 等可忽略子句、表级 ENGINE=/DEFAULT CHARSET= 子句；
//   - 多值 INSERT（INSERT INTO t VALUES (...),(...)）、显式列名列表、INSERT IGNORE；
//   - 值字面量：NULL、数字（含负数/小数/科学计数法）、单双引号字符串（反斜杠转义还原）、
//     0x/X'' 十六进制（BLOB）、b''/B'' 位字面量（转十进制）、TRUE/FALSE、
//     字符集引导符（_binary'..' / _utf8mb4'..' 等）；
//   - DROP TABLE IF EXISTS（记录表名，供导入方重建语义）；
//   - SET/LOCK TABLES/UNLOCK TABLES 与 /*!...*/ 版本化注释等辅助语句整体跳过。
package mysqldump

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// Value 表示一行中的一个单元格：NULL 或规范化文本。
// Raw 规则：INT/DECIMAL 为数字原文；DATE 为 YYYY-MM-DD；TEXT 为去转义字符串；
// BLOB 为大写 hex（去掉 0x 前缀）。
type Value struct {
	Null bool
	Raw  string
}

// Column 是解析出的列定义：Name 为去反引号后的列名，
// Type 为 OpenXDB 列类型（INT/TEXT/DATE/DECIMAL/BLOB），Primary 标记主键。
type Column struct {
	Name    string
	Type    string
	Primary bool
}

// Table 是解析出的一张表：定义 + 全部行（行内值的顺序对应 Columns）。
type Table struct {
	Name    string
	Columns []Column
	Rows    [][]Value
}

// Dump 是一次解析的完整结果。
type Dump struct {
	Tables     []*Table // 按语句出现顺序
	Drops      []string // DROP TABLE IF EXISTS 的表名（重建语义）
	Databases  []string // CREATE DATABASE 识别的库名（--databases 模式；OpenXDB 单 default 库，仅记录）
	Statements int      // 有内容语句总数（不含纯注释/空语句）
	Skipped    int      // 跳过的语句数（SET/LOCK/不支持语句）
	BadRows    int      // 容错模式下跳过的坏行数
	Charset    string   // 最近一次 SET NAMES <charset> 识别的字符集（无则空）
}

// Options 控制解析与导入选项（P2 全选项扩展）。
// 除 IgnoreErrors 影响解析容错外，其余字段为 mysqldump 常见选项的语义开关，
// 由导入执行层（pkg/sql）贯通消费；解析层自身对 NoCreateInfo 的 INSERT
// 引用未定义表做宽容跳过，其余选项不影响解析产出。
type Options struct {
	IgnoreErrors bool // true：跳过无法解析的语句/行；false：首个错误即返回

	NoData       bool // --no-data：跳过 INSERT 数据行（只导结构）
	NoCreateInfo bool // --no-create-info：不建表（目标库表须已存在，只导数据）
	// 说明：NoCreateInfo 只作用于导入执行层（跳过 CREATE 执行）；
	// 解析层仍解析 CREATE 结构以校验 INSERT 列序；若 dump 无 CREATE
	// （--no-create-info 导出），INSERT 引用未定义表时宽容跳过。
	CompleteInsert bool // --complete-insert：INSERT 带完整列清单（解析层本就支持列清单）
	ExtendedInsert bool // --extended-insert：多值 INSERT（解析层本就支持多值）
	InsertIgnore   bool // --insert-ignore：主键冲突忽略（导入幂等默认语义）
	Replace        bool // --replace：REPLACE INTO 语义（冲突覆盖旧行）
	HexBlob        bool // --hex-blob：BLOB 列必须以十六进制字面量（大写 hex）解析
	SetNamesCharset string // SET NAMES <charset>：解析时识别的字符集（数据按 UTF-8 处理）
	DropTables     bool // --add-drop-table：DROP TABLE IF EXISTS 先删后建（默认 true 由执行层保证）
}

// ParseFile 解析 mysqldump 导出文件。
func ParseFile(path string, opt Options) (*Dump, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Parse(f, opt)
}

// Parse 从 reader 解析 mysqldump SQL 文本。
func Parse(r io.Reader, opt Options) (*Dump, error) {
	text, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	d := &Dump{}
	statements := splitStatements(string(text))
	d.Statements = len(statements)
	tables := map[string]*Table{}
	for _, st := range statements {
		body := strings.TrimSpace(st.text)
		if body == "" {
			continue
		}
		upper := strings.ToUpper(body)
		switch {
		case strings.HasPrefix(upper, "CREATE TABLE"):
			t, whole, err := parseCreateTable(body)
			if err != nil {
				if opt.IgnoreErrors {
					d.Skipped++
					continue
				}
				return nil, err
			}
			if !whole {
				t.Name = stripTablePrefix(t.Name)
			}
			if old := tables[t.Name]; old != nil {
				// 同名表重复 CREATE（罕见）：以后者为准重建
				old.Columns = t.Columns
				old.Rows = nil
				continue
			}
			tables[t.Name] = t
			d.Tables = append(d.Tables, t)
		case strings.HasPrefix(upper, "INSERT"):
			if opt.NoData {
				// --no-data：跳过全部数据行（只导结构）
				d.Skipped++
				continue
			}
			id, bad, whole, err := parseInsert(body)
			if err != nil {
				if opt.IgnoreErrors {
					d.Skipped++
					continue
				}
				return nil, err
			}
			if !whole {
				id.Table = stripTablePrefix(id.Table)
			}
			d.BadRows += bad
			t := tables[id.Table]
			if t == nil {
				if opt.NoCreateInfo {
					// --no-create-info 导出：dump 不含 CREATE TABLE，
					// 表结构由目标库提供，无法校验列序，宽容跳过该数据段。
					d.Skipped++
					continue
				}
				if opt.IgnoreErrors {
					d.Skipped++
					continue
				}
				return nil, fmt.Errorf("INSERT references undefined table: %s", id.Table)
			}
			if len(id.Columns) > 0 {
				perm := make([]int, len(id.Columns))
				valid := true
				for i, c := range id.Columns {
					idx := -1
					for j, tc := range t.Columns {
						if tc.Name == c {
							idx = j
							break
						}
					}
					if idx < 0 {
						if opt.IgnoreErrors {
							d.BadRows += len(id.Rows)
							valid = false
							break
						}
						return nil, fmt.Errorf("INSERT column not in table %s: %s", t.Name, c)
					}
					perm[i] = idx
				}
				if !valid {
					continue
				}
				for _, row := range id.Rows {
					if len(row) != len(perm) {
						if opt.IgnoreErrors {
							d.BadRows++
							continue
						}
						return nil, fmt.Errorf("INSERT column count mismatch for %s", t.Name)
					}
					reordered := make([]Value, len(t.Columns))
					for i, v := range row {
						reordered[perm[i]] = v
					}
					t.Rows = append(t.Rows, reordered)
				}
			} else {
				if len(id.Rows) > 0 && len(id.Rows[0]) != len(t.Columns) {
					if opt.IgnoreErrors {
						d.BadRows += len(id.Rows)
						continue
					}
					return nil, fmt.Errorf("INSERT column count mismatch for %s: want %d got %d",
						t.Name, len(t.Columns), len(id.Rows[0]))
				}
				t.Rows = append(t.Rows, id.Rows...)
			}
		case strings.HasPrefix(upper, "DROP TABLE"):
			name, whole, err := parseDropTable(body)
			if err != nil {
				if opt.IgnoreErrors {
					d.Skipped++
					continue
				}
				return nil, err
			}
			if !whole {
				name = stripTablePrefix(name)
			}
			d.Drops = append(d.Drops, name)
			if old := tables[name]; old != nil {
				delete(tables, name)
				for i, tt := range d.Tables {
					if tt == old {
						d.Tables = append(d.Tables[:i], d.Tables[i+1:]...)
						break
					}
				}
			}
		default:
			// SET NAMES <charset>：识别并记录字符集（数据按 UTF-8 处理）。
			// CREATE DATABASE：--databases 模式，记录库名（OpenXDB 单 default 库，仅记录）。
			// USE <db>：--databases 模式切换上下文，识别并跳过（表名库前缀已剥离）。
			// 其余辅助语句（LOCK TABLES / UNLOCK TABLES / SET 变量 /
			// 存储过程定义等）整体跳过。
			if cs, ok := parseSetNames(body); ok {
				d.Charset = cs
				d.Skipped++
			} else if db, ok := parseCreateDatabase(body); ok {
				d.Databases = append(d.Databases, db)
				d.Skipped++
			} else if _, ok := parseUseDatabase(body); ok {
				d.Skipped++
			} else {
				d.Skipped++
			}
		}
	}
	return d, nil
}

type insertData struct {
	Table   string
	Columns []string // 显式列名列表；nil 表示按表定义列序
	Rows    [][]Value
}

// splitStatements 把 SQL 文本按顶层分号切分为语句（跳过注释，跟踪引号与转义）。
type statement struct {
	text string
	line int
}

func splitStatements(s string) []statement {
	var out []statement
	var cur strings.Builder
	inStr := byte(0) // 0 | '\'' | '"' | '`'
	inLineComment := false
	inBlockComment := false
	curLine := 1
	line := 1
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\n' {
			line++
		}
		if inLineComment {
			if c == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if c == '*' && i+1 < len(s) && s[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if inStr != 0 {
			cur.WriteByte(c)
			if c == '\\' && inStr != '`' && i+1 < len(s) {
				cur.WriteByte(s[i+1])
				i++
				continue
			}
			if c == inStr {
				if i+1 < len(s) && s[i+1] == inStr {
					cur.WriteByte(s[i+1])
					i++
					continue
				}
				inStr = 0
			}
			continue
		}
		switch {
		case c == '\'' || c == '"' || c == '`':
			inStr = c
			cur.WriteByte(c)
		case c == '-' && i+1 < len(s) && s[i+1] == '-' &&
			(i+2 >= len(s) || s[i+2] == ' ' || s[i+2] == '\t' || s[i+2] == '\r' || s[i+2] == '\n'):
			inLineComment = true
			i++
		case c == '#':
			inLineComment = true
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			inBlockComment = true
			i++
		case c == ';':
			t := strings.TrimSpace(cur.String())
			if t != "" {
				out = append(out, statement{text: t, line: curLine})
			}
			cur.Reset()
			curLine = line
		default:
			cur.WriteByte(c)
		}
	}
	t := strings.TrimSpace(cur.String())
	if t != "" {
		out = append(out, statement{text: t, line: curLine})
	}
	return out
}

// parseIdent 从 s[i] 开始解析标识符（支持反引号包裹与普通标识符），返回去反引号文本。
func parseIdent(s string, i int) (string, int, error) {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	if i >= len(s) {
		return "", i, fmt.Errorf("expected identifier at end")
	}
	if s[i] == '`' {
		j := i + 1
		var sb strings.Builder
		for j < len(s) {
			if s[j] == '`' {
				if j+1 < len(s) && s[j+1] == '`' {
					sb.WriteByte('`')
					j += 2
					continue
				}
				return sb.String(), j + 1, nil
			}
			sb.WriteByte(s[j])
			j++
		}
		return "", i, fmt.Errorf("unterminated backtick identifier")
	}
	j := i
	for j < len(s) {
		c := s[j]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '(' || c == ',' || c == ';' || c == ')' {
			break
		}
		j++
	}
	if j == i {
		return "", i, fmt.Errorf("expected identifier")
	}
	return s[i:j], j, nil
}

// splitTopLevel 返回括号内按顶层逗号分割的段（跟踪嵌套括号与引号）。
func splitTopLevel(s string, start int) ([]string, error) {
	if start >= len(s) || s[start] != '(' {
		return nil, fmt.Errorf("expected (")
	}
	depth := 0
	inStr := byte(0)
	segStart := start + 1
	var segs []string
	for i := start; i < len(s); i++ {
		c := s[i]
		if inStr != 0 {
			if c == '\\' && inStr != '`' && i+1 < len(s) {
				i++
				continue
			}
			if c == inStr {
				if i+1 < len(s) && s[i+1] == inStr {
					i++
					continue
				}
				inStr = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inStr = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				segs = append(segs, s[segStart:i])
				return segs, nil
			}
		case ',':
			if depth == 1 {
				segs = append(segs, s[segStart:i])
				segStart = i + 1
			}
		}
	}
	return nil, fmt.Errorf("unterminated parentheses")
}

// parseDropTable DROP TABLE [IF EXISTS] <name>，返回表名与"整体反引号标识符"标志。
func parseDropTable(s string) (string, bool, error) {
	rest := strings.TrimSpace(s[len("DROP TABLE"):])
	if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS") {
		rest = strings.TrimSpace(rest[len("IF EXISTS"):])
	}
	name, _, whole, err := parseQualifiedName(rest, 0)
	if err != nil {
		return "", false, fmt.Errorf("invalid DROP TABLE: %v", err)
	}
	return name, whole, nil
}

// parseCreateDatabase 识别 "CREATE DATABASE [IF NOT EXISTS] <name>" 并返回库名（去反引号/引号）。
// 可忽略尾部子句：DEFAULT CHARACTER SET / COLLATE / DEFAULT COLLATE 等。
func parseCreateDatabase(s string) (string, bool) {
	const prefix = "CREATE DATABASE"
	if len(s) < len(prefix) {
		return "", false
	}
	rest := strings.TrimSpace(s[len(prefix):])
	if strings.HasPrefix(strings.ToUpper(rest), "IF NOT EXISTS") {
		rest = strings.TrimSpace(rest[len("IF NOT EXISTS"):])
	}
	name, _, err := parseIdent(rest, 0)
	if err != nil {
		return "", false
	}
	return strings.Trim(name, "'\""), true
}

// parseUseDatabase 识别 "USE <name>" 并返回库名（去反引号/引号）。
func parseUseDatabase(s string) (string, bool) {
	const prefix = "USE"
	if len(s) < len(prefix) {
		return "", false
	}
	rest := strings.TrimSpace(s[len(prefix):])
	name, _, err := parseIdent(rest, 0)
	if err != nil {
		return "", false
	}
	return strings.Trim(name, "'\""), true
}

// parseQualifiedName 解析表名，支持 [db.]tbl 与 `db`.`tbl` 限定形式：
// 解析第一段标识符后若紧跟 '.'，继续解析第二段，返回第二段（表名，剥离库前缀）。
// OpenXDB 为单 default 库模型，导入时统一使用裸表名。
// whole 为 true 表示"整体反引号标识符"（首个 token 被反引号包裹且其后无 '.' 限定符），
// 此时点号属于标识符本身（如 `db.tbl`），调用方不应再剥离库前缀。
func parseQualifiedName(s string, i int) (name string, next int, whole bool, err error) {
	if i >= len(s) {
		return "", i, false, fmt.Errorf("expected identifier at end")
	}
	wholeBacktick := s[i] == '`'
	name, j, err := parseIdent(s, i)
	if err != nil {
		return "", i, false, err
	}
	for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
		j++
	}
	if j < len(s) && s[j] == '.' {
		j++
		for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
			j++
		}
		n2, j2, err := parseIdent(s, j)
		if err != nil {
			return "", i, false, err
		}
		return n2, j2, false, nil
	}
	if wholeBacktick {
		return name, j, true, nil
	}
	return name, j, false, nil
}

// stripTablePrefix 剥离表名中的库前缀（`db`.`tbl` / db.tbl → tbl）。
// 仅处理裸标识符形式（parseQualifiedName 已处理反引号限定形式）；
// 单个反引号包裹的整体标识符（如 `db.tbl`，MySQL 允许点号在反引号内）视为表名本身。
func stripTablePrefix(name string) string {
	if strings.HasPrefix(name, "`") {
		return name
	}
	if i := strings.LastIndex(name, "."); i >= 0 {
		return name[i+1:]
	}
	return name
}

// parseSetNames 识别 "SET NAMES <charset>" 语句并返回字符集（去引号）。
// 支持大小写不敏感前缀与带引号字符集（'utf8mb4' / "utf8"）。
func parseSetNames(s string) (string, bool) {
	rest := strings.TrimSpace(s[len("SET"):])
	if !strings.HasPrefix(strings.ToUpper(rest), "NAMES") {
		return "", false
	}
	rest = strings.TrimSpace(rest[len("NAMES"):])
	cs, _, err := parseIdent(rest, 0)
	if err != nil {
		return "", false
	}
	return strings.Trim(cs, "'\""), true
}

// parseCreateTable CREATE TABLE [IF NOT EXISTS] <name> (...)，返回表与"整体反引号标识符"标志。
func parseCreateTable(s string) (*Table, bool, error) {
	rest := strings.TrimSpace(s[len("CREATE TABLE"):])
	if strings.HasPrefix(strings.ToUpper(rest), "IF NOT EXISTS") {
		rest = strings.TrimSpace(rest[len("IF NOT EXISTS"):])
	}
	name, i, whole, err := parseQualifiedName(rest, 0)
	if err != nil {
		return nil, false, fmt.Errorf("CREATE TABLE: %v", err)
	}
	rest = strings.TrimSpace(rest[i:])
	if !strings.HasPrefix(rest, "(") {
		return nil, false, fmt.Errorf("CREATE TABLE %s: expected ( after table name", name)
	}
	segments, err := splitTopLevel(rest, 0)
	if err != nil {
		return nil, false, fmt.Errorf("CREATE TABLE %s: %v", name, err)
	}
	t := &Table{Name: name}
	tablePK := ""
	for _, seg := range segments {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		u := strings.ToUpper(seg)
		switch {
		case strings.HasPrefix(u, "PRIMARY KEY"):
			start := strings.IndexByte(seg, '(')
			if start < 0 {
				return nil, false, fmt.Errorf("CREATE TABLE %s: invalid PRIMARY KEY clause", name)
			}
			cols, _, err := parseParenIdents(seg, start)
			if err != nil {
				return nil, false, fmt.Errorf("CREATE TABLE %s: %v", name, err)
			}
			if len(cols) > 0 {
				tablePK = cols[0]
			}
		case strings.HasPrefix(u, "KEY") || strings.HasPrefix(u, "INDEX") ||
			strings.HasPrefix(u, "UNIQUE") || strings.HasPrefix(u, "FOREIGN") ||
			strings.HasPrefix(u, "CONSTRAINT") || strings.HasPrefix(u, "CHECK"):
			// 普通索引/外键/检查约束：OpenXDB 导入时忽略
		default:
			col, err := parseColumnDef(seg)
			if err != nil {
				return nil, false, fmt.Errorf("CREATE TABLE %s: %v", name, err)
			}
			t.Columns = append(t.Columns, *col)
			if col.Primary {
				tablePK = col.Name
			}
		}
	}
	if len(t.Columns) == 0 {
		return nil, false, fmt.Errorf("CREATE TABLE %s: no columns", name)
	}
	if tablePK != "" {
		for i := range t.Columns {
			if t.Columns[i].Name == tablePK {
				t.Columns[i].Primary = true
			}
		}
	}
	return t, whole, nil
}

// parseParenIdents 解析 (a, b, c) 括号内标识符列表（去反引号），
// 返回右括号后的位置。
func parseParenIdents(s string, start int) ([]string, int, error) {
	segs, err := splitTopLevel(s, start)
	if err != nil {
		return nil, 0, err
	}
	out := make([]string, 0, len(segs))
	for _, seg := range segs {
		name, _, err := parseIdent(seg, 0)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid identifier in list: %q", seg)
		}
		out = append(out, name)
	}
	end, err := matchParenEnd(s, start)
	if err != nil {
		return nil, 0, err
	}
	return out, end, nil
}

// matchParenEnd 返回 s[start] 处左括号匹配的右括号之后的位置。
func matchParenEnd(s string, start int) (int, error) {
	depth := 0
	inStr := byte(0)
	for i := start; i < len(s); i++ {
		c := s[i]
		if inStr != 0 {
			if c == '\\' && inStr != '`' && i+1 < len(s) {
				i++
				continue
			}
			if c == inStr {
				if i+1 < len(s) && s[i+1] == inStr {
					i++
					continue
				}
				inStr = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inStr = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1, nil
			}
		}
	}
	return 0, fmt.Errorf("unterminated parentheses")
}

// parseColumnDef 解析单个列定义段（列名 + 类型 + 可忽略子句）。
func parseColumnDef(seg string) (*Column, error) {
	name, i, err := parseIdent(seg, 0)
	if err != nil {
		return nil, fmt.Errorf("missing column name: %q", seg)
	}
	typTok, _, err := parseIdent(seg, i)
	if err != nil {
		return nil, fmt.Errorf("missing type for column %s", name)
	}
	typ, err := mapType(typTok)
	if err != nil {
		return nil, err
	}
	primary := strings.Contains(strings.ToUpper(seg), "PRIMARY KEY")
	return &Column{Name: name, Type: typ, Primary: primary}, nil
}

// mapType 把 MySQL 列类型映射到 OpenXDB 列类型。
func mapType(t string) (string, error) {
	switch strings.ToUpper(t) {
	case "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
		"BOOL", "BOOLEAN", "SERIAL":
		// SERIAL 是 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE 的 MySQL 别名
		return "INT", nil
	case "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "FIXED", "FLOAT4", "FLOAT8":
		// FLOAT4=FLOAT、FLOAT8=DOUBLE 的 MySQL 别名
		return "DECIMAL", nil
	case "DATE":
		return "DATE", nil
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",
		"ENUM", "SET", "JSON", "DATETIME", "TIMESTAMP", "TIME", "YEAR", "BIT", "GEOMETRY",
		"POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING",
		"MULTIPOLYGON", "GEOMETRYCOLLECTION", "NCHAR", "NVARCHAR":
		return "TEXT", nil
	case "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		return "BLOB", nil
	}
	return "", fmt.Errorf("unsupported MySQL column type: %s", t)
}

// parseInsert INSERT [IGNORE] INTO <name> [(cols)] VALUES (...),(...)。
// 返回插入数据、坏行数与"整体反引号标识符"标志。
func parseInsert(s string) (*insertData, int, bool, error) {
	rest := strings.TrimSpace(s[len("INSERT"):])
	if strings.HasPrefix(strings.ToUpper(rest), "IGNORE") {
		rest = strings.TrimSpace(rest[len("IGNORE"):])
	}
	if !strings.HasPrefix(strings.ToUpper(rest), "INTO") {
		return nil, 0, false, fmt.Errorf("expected INTO after INSERT")
	}
	rest = strings.TrimSpace(rest[len("INTO"):])
	name, i, whole, err := parseQualifiedName(rest, 0)
	if err != nil {
		return nil, 0, false, fmt.Errorf("INSERT: %v", err)
	}
	id := &insertData{Table: name}
	rest = strings.TrimSpace(rest[i:])
	if strings.HasPrefix(rest, "(") {
		cols, end, err := parseParenIdents(rest, 0)
		if err != nil {
			return nil, 0, false, fmt.Errorf("INSERT %s: %v", name, err)
		}
		id.Columns = cols
		rest = strings.TrimSpace(rest[end:])
	}
	u := strings.ToUpper(rest)
	if !strings.HasPrefix(u, "VALUES") && !strings.HasPrefix(u, "VALUE") {
		return nil, 0, false, fmt.Errorf("INSERT %s: expected VALUES", name)
	}
	if strings.HasPrefix(u, "VALUES") {
		rest = strings.TrimSpace(rest[len("VALUES"):])
	} else {
		rest = strings.TrimSpace(rest[len("VALUE"):])
	}
	bad := 0
	for {
		if !strings.HasPrefix(rest, "(") {
			return nil, 0, false, fmt.Errorf("INSERT %s: expected ( in VALUES", name)
		}
		vals, end, err := parseParenValues(rest, 0)
		if err != nil {
			bad++
			rest = skipToComma(rest)
			if rest == "" {
				break
			}
			continue
		}
		id.Rows = append(id.Rows, vals)
		rest = strings.TrimSpace(rest[end:])
		if strings.HasPrefix(rest, ",") {
			rest = strings.TrimSpace(rest[1:])
			continue
		}
		break
	}
	if len(id.Rows) == 0 {
		return nil, 0, false, fmt.Errorf("INSERT %s: no valid rows", name)
	}
	return id, bad, whole, nil
}

// parseParenValues 解析一行元组 (v1, v2, ...) 的值列表。
func parseParenValues(s string, start int) ([]Value, int, error) {
	segs, err := splitTopLevel(s, start)
	if err != nil {
		return nil, 0, err
	}
	out := make([]Value, 0, len(segs))
	for _, seg := range segs {
		v, err := parseValue(seg)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, v)
	}
	// 返回结束位置：从 start 找到匹配的右括号
	depth := 0
	inStr := byte(0)
	end := start + 1
	for i := start; i < len(s); i++ {
		c := s[i]
		if inStr != 0 {
			if c == '\\' && inStr != '`' && i+1 < len(s) {
				i++
				continue
			}
			if c == inStr {
				if i+1 < len(s) && s[i+1] == inStr {
					i++
					continue
				}
				inStr = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inStr = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				end = i + 1
				return out, end, nil
			}
		}
	}
	return nil, 0, fmt.Errorf("unterminated tuple")
}

// skipToComma 在元组解析失败时跳到下一个顶层逗号（容错用），返回剩余文本。
func skipToComma(s string) string {
	depth := 0
	inStr := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inStr != 0 {
			if c == '\\' && inStr != '`' && i+1 < len(s) {
				i++
				continue
			}
			if c == inStr {
				if i+1 < len(s) && s[i+1] == inStr {
					i++
					continue
				}
				inStr = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inStr = c
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				return strings.TrimSpace(s[i+1:])
			}
		}
	}
	return ""
}

// parseValue 解析单个值字面量。
func parseValue(s string) (Value, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Value{}, fmt.Errorf("empty value")
	}
	u := strings.ToUpper(s)
	if u == "NULL" {
		return Value{Null: true}, nil
	}
	if u == "TRUE" {
		return Value{Raw: "1"}, nil
	}
	if u == "FALSE" {
		return Value{Raw: "0"}, nil
	}
	// 字符集引导符：_binary'..' / _utf8mb4'..' 等（mysqldump 导出二进制/多字节列常见）。
	// 引导符与字面量之间不允许空白，剥离后递归解析剩余字面量。
	if s[0] == '_' && len(s) > 2 {
		j := 1
		for j < len(s) && s[j] != '\'' && s[j] != '"' {
			j++
		}
		if j < len(s) && j > 1 {
			return parseValue(s[j:])
		}
	}
	if s[0] == '\'' || s[0] == '"' {
		raw, err := unquoteString(s)
		if err != nil {
			return Value{}, err
		}
		return Value{Raw: raw}, nil
	}
	if len(s) >= 3 && (s[0] == 'X' || s[0] == 'x') && s[1] == '\'' && s[len(s)-1] == '\'' {
		hex := strings.ToUpper(s[2 : len(s)-1])
		for i := 0; i < len(hex); i++ {
			if !isHexChar(hex[i]) {
				return Value{}, fmt.Errorf("invalid hex literal: %s", s)
			}
		}
		return Value{Raw: hex}, nil
	}
	// 位字面量：b'0101' / B'11111111'（BIT 列与数字上下文常用，转十进制）。
	if len(s) >= 3 && (s[0] == 'b' || s[0] == 'B') && s[1] == '\'' && s[len(s)-1] == '\'' {
		bits := s[2 : len(s)-1]
		for i := 0; i < len(bits); i++ {
			if bits[i] != '0' && bits[i] != '1' {
				return Value{}, fmt.Errorf("invalid bit literal: %s", s)
			}
		}
		n := uint64(0)
		if bits != "" {
			v, err := strconv.ParseUint(bits, 2, 64)
			if err != nil {
				return Value{}, fmt.Errorf("invalid bit literal: %s", s)
			}
			n = v
		}
		return Value{Raw: strconv.FormatUint(n, 10)}, nil
	}
	if len(s) >= 2 && (s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
		hex := strings.ToUpper(s[2:])
		for i := 0; i < len(hex); i++ {
			if !isHexChar(hex[i]) {
				return Value{}, fmt.Errorf("invalid hex literal: %s", s)
			}
		}
		return Value{Raw: hex}, nil
	}
	if isNumber(s) {
		return Value{Raw: s}, nil
	}
	return Value{}, fmt.Errorf("unsupported value literal: %s", s)
}

// unquoteString 还原带引号字符串：处理反斜杠转义与引号翻倍。
func unquoteString(s string) (string, error) {
	q := s[0]
	var sb strings.Builder
	i := 1
	for i < len(s) {
		c := s[i]
		if c == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case '0':
				sb.WriteByte(0)
			case 'b':
				sb.WriteByte('\b')
			case 'n':
				sb.WriteByte('\n')
			case 'r':
				sb.WriteByte('\r')
			case 't':
				sb.WriteByte('\t')
			case 'Z':
				sb.WriteByte(26)
			default:
				sb.WriteByte(s[i])
			}
			i++
			continue
		}
		if c == q {
			if i+1 < len(s) && s[i+1] == q {
				sb.WriteByte(q)
				i += 2
				continue
			}
			return sb.String(), nil
		}
		sb.WriteByte(c)
		i++
	}
	return "", fmt.Errorf("unterminated string literal")
}

func isNumber(s string) bool {
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	if i >= len(s) {
		return false
	}
	digits := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
		digits++
	}
	if digits == 0 {
		return false
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	}
	// 科学计数法：1e5 / 1.5E-3 / 2E+06
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		expDigits := 0
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
			expDigits++
		}
		if expDigits == 0 {
			return false
		}
		i = j
	}
	return i == len(s)
}

func isHexChar(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')
}

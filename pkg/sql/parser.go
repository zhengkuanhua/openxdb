package sql

import (
	"strconv"
	"strings"
)

// parser 递归下降解析器（M1 子集，见开发手册 §4.1）。
type parser struct {
	toks []token
	pos  int
}

// Parse 解析单条 SQL 语句（结尾分号可选）。
func Parse(sql string) (Stmt, error) {
	toks, err := lex(sql)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	stmt, err := p.parseStmt()
	if err != nil {
		return nil, err
	}
	// 允许一个可选的收尾分号，然后必须是 EOF
	if p.cur().kind == tokSemicolon {
		p.next()
	}
	if p.cur().kind != tokEOF {
		return nil, errf("unexpected token %q at %d", p.cur().text, p.cur().pos)
	}
	return stmt, nil
}

func (p *parser) cur() token { return p.toks[p.pos] }
func (p *parser) next()      { if p.pos < len(p.toks)-1 { p.pos++ } }

func (p *parser) expect(k tokKind, what string) (token, error) {
	if p.cur().kind != k {
		return token{}, errf("expected %s at %d, got %q", what, p.cur().pos, p.cur().text)
	}
	t := p.cur()
	p.next()
	return t, nil
}

func (p *parser) expectKeyword(kw string) error {
	if p.cur().kind != tokKeyword || p.cur().text != kw {
		return errf("expected keyword %s at %d, got %q", kw, p.cur().pos, p.cur().text)
	}
	p.next()
	return nil
}

func (p *parser) parseStmt() (Stmt, error) {
	if p.cur().kind != tokKeyword {
		return nil, errf("expected statement at %d, got %q", p.cur().pos, p.cur().text)
	}
	switch p.cur().text {
	case "CREATE":
		return p.parseCreate()
	case "DROP":
		return p.parseDrop()
	case "INSERT":
		return p.parseInsert()
	case "SELECT":
		return p.parseSelect()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "BEGIN":
		p.next()
		return &BeginStmt{}, nil
	case "COMMIT":
		p.next()
		return &CommitStmt{}, nil
	case "ROLLBACK":
		p.next()
		return &RollbackStmt{}, nil
	case "EXPORT":
		return p.parseExport()
	case "IMPORT":
		return p.parseImport()
	case "SHOW":
		return p.parseShow()
	case "EXPLAIN":
		return p.parseExplain()
	case "ADD":
		return p.parseAddNode()
	case "ASSIGN":
		return p.parseAssignRegion()
	case "SPLIT":
		return p.parseSplitRegion()
	case "BALANCE":
		p.next()
		return &BalanceStmt{}, nil
	case "BACKUP":
		return p.parseBackup()
	case "RESTORE":
		return p.parseRestore()
	}
	return nil, errf("unsupported statement %q at %d", p.cur().text, p.cur().pos)
}

// parseBackup BACKUP TO '<path>'：生成完整逻辑备份快照文件。
func (p *parser) parseBackup() (Stmt, error) {
	if err := p.expectKeyword("BACKUP"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TO"); err != nil {
		return nil, err
	}
	pathTok, err := p.expect(tokString, "backup file path string")
	if err != nil {
		return nil, err
	}
	return &BackupStmt{Path: pathTok.text}, nil
}

// parseRestore RESTORE FROM '<path>' [TO LSN <n>]：
// 恢复备份快照；可选 TO LSN 从备份点回放 binlog 到指定 LSN（PITR 基础）。
func (p *parser) parseRestore() (Stmt, error) {
	if err := p.expectKeyword("RESTORE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	pathTok, err := p.expect(tokString, "restore file path string")
	if err != nil {
		return nil, err
	}
	s := &RestoreStmt{Path: pathTok.text}
	if p.cur().kind == tokKeyword && p.cur().text == "TO" {
		p.next()
		if err := p.expectKeyword("LSN"); err != nil {
			return nil, err
		}
		lsnTok, err := p.expect(tokNumber, "lsn number")
		if err != nil {
			return nil, err
		}
		lsn, err := strconv.ParseUint(lsnTok.text, 10, 64)
		if err != nil {
			return nil, errf("invalid lsn %q", lsnTok.text)
		}
		s.ToLSN = lsn
		s.HasToLSN = true
	}
	return s, nil
}

// parseAddNode ADD NODE '<addr>'：注册远端节点（地址字符串，如 '127.0.0.1:7791'）。
func (p *parser) parseAddNode() (Stmt, error) {
	if err := p.expectKeyword("ADD"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("NODE"); err != nil {
		return nil, err
	}
	addrTok, err := p.expect(tokString, "node address string")
	if err != nil {
		return nil, err
	}
	return &AddNodeStmt{Addr: addrTok.text}, nil
}

// parseAssignRegion ASSIGN REGION <regionID> TO NODE '<nodeID>'：指派 region 归属节点。
func (p *parser) parseAssignRegion() (Stmt, error) {
	if err := p.expectKeyword("ASSIGN"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("REGION"); err != nil {
		return nil, err
	}
	ridTok, err := p.expect(tokNumber, "region id")
	if err != nil {
		return nil, err
	}
	rid, err := strconv.ParseUint(ridTok.text, 10, 64)
	if err != nil {
		return nil, errf("invalid region id %q", ridTok.text)
	}
	if err := p.expectKeyword("TO"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("NODE"); err != nil {
		return nil, err
	}
	nodeTok, err := p.expect(tokString, "node id string")
	if err != nil {
		return nil, err
	}
	return &AssignRegionStmt{RegionID: uint64(rid), NodeID: nodeTok.text}, nil
}

// parseSplitRegion SPLIT REGION <regionID>：手动分裂 region（沿数据中点）。
func (p *parser) parseSplitRegion() (Stmt, error) {
	if err := p.expectKeyword("SPLIT"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("REGION"); err != nil {
		return nil, err
	}
	ridTok, err := p.expect(tokNumber, "region id")
	if err != nil {
		return nil, err
	}
	rid, err := strconv.ParseUint(ridTok.text, 10, 64)
	if err != nil {
		return nil, errf("invalid region id %q", ridTok.text)
	}
	return &SplitRegionStmt{RegionID: rid}, nil
}

func (p *parser) parseCreate() (Stmt, error) {
	if err := p.expectKeyword("CREATE"); err != nil {
		return nil, err
	}
	switch p.cur().text {
	case "INDEX":
		return p.parseCreateIndex()
	case "TABLE":
		// 已有 TABLE 分支
	default:
		return nil, errf("expected TABLE or INDEX at %d, got %q", p.cur().pos, p.cur().text)
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokLParen, "("); err != nil {
		return nil, err
	}
	stmt := &CreateTableStmt{Name: nameTok.text}
	for {
		col, err := p.expect(tokIdent, "column name")
		if err != nil {
			return nil, err
		}
		typTok, err := p.expect(tokIdent, "column type")
		if err != nil {
			return nil, err
		}
		typ := strings.ToUpper(typTok.text)
		if typ != "INT" && typ != "TEXT" && typ != "STRING" &&
			typ != "DATE" && typ != "DECIMAL" && typ != "BLOB" {
			return nil, errf("unsupported column type %q at %d", typTok.text, typTok.pos)
		}
		if typ == "STRING" {
			typ = "TEXT"
		}
		stmt.Columns = append(stmt.Columns, ColumnDef{Name: col.text, Type: typ})
		switch p.cur().kind {
		case tokComma:
			p.next()
			// 可能是 PRIMARY KEY (col)
			if p.cur().kind == tokKeyword && p.cur().text == "PRIMARY" {
				if err := p.parsePrimaryKey(stmt); err != nil {
					return nil, err
				}
				if _, err := p.expect(tokRParen, ")"); err != nil {
					return nil, err
				}
				return stmt, nil
			}
		case tokRParen:
			p.next()
			if stmt.PK == "" && len(stmt.Columns) > 0 {
				stmt.PK = stmt.Columns[0].Name
			}
			return stmt, nil
		default:
			// 行内主键：id INT PRIMARY KEY（等价于将该列设为主键）
			if p.cur().kind == tokKeyword && p.cur().text == "PRIMARY" {
				if err := p.expectKeyword("PRIMARY"); err != nil {
					return nil, err
				}
				if err := p.expectKeyword("KEY"); err != nil {
					return nil, err
				}
				stmt.PK = col.text
				switch p.cur().kind {
				case tokComma:
					p.next()
					// 行内主键后仍可跟表级 PRIMARY KEY (col)
					if p.cur().kind == tokKeyword && p.cur().text == "PRIMARY" {
						if err := p.parsePrimaryKey(stmt); err != nil {
							return nil, err
						}
						if _, err := p.expect(tokRParen, ")"); err != nil {
							return nil, err
						}
						return stmt, nil
					}
					continue
				case tokRParen:
					p.next()
					return stmt, nil
				default:
					return nil, errf("expected , or ) at %d, got %q", p.cur().pos, p.cur().text)
				}
			}
			return nil, errf("expected , or ) at %d, got %q", p.cur().pos, p.cur().text)
		}
	}
}

func (p *parser) parsePrimaryKey(stmt *CreateTableStmt) error {
	if err := p.expectKeyword("PRIMARY"); err != nil {
		return err
	}
	if err := p.expectKeyword("KEY"); err != nil {
		return err
	}
	if _, err := p.expect(tokLParen, "("); err != nil {
		return err
	}
	pkTok, err := p.expect(tokIdent, "primary key column")
	if err != nil {
		return err
	}
	if _, err := p.expect(tokRParen, ")"); err != nil {
		return err
	}
	// 校验列存在
	found := false
	for _, c := range stmt.Columns {
		if c.Name == pkTok.text {
			found = true
			break
		}
	}
	if !found {
		return errf("primary key column %q not in column list", pkTok.text)
	}
	stmt.PK = pkTok.text
	return nil
}

func (p *parser) parseDrop() (Stmt, error) {
	if err := p.expectKeyword("DROP"); err != nil {
		return nil, err
	}
	switch p.cur().text {
	case "INDEX":
		return p.parseDropIndex()
	case "TABLE":
	default:
		return nil, errf("expected TABLE or INDEX at %d, got %q", p.cur().pos, p.cur().text)
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	s := &DropTableStmt{}
	// DROP TABLE [IF EXISTS] <name>
	if p.cur().kind == tokKeyword && p.cur().text == "IF" {
		if err := p.expectKeyword("IF"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		s.IfExists = true
	}
	nameTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	s.Name = nameTok.text
	return s, nil
}

// parseCreateIndex CREATE INDEX [idx] ON tbl (col)
func (p *parser) parseCreateIndex() (Stmt, error) {
	if err := p.expectKeyword("INDEX"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "index name")
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	tableTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokLParen, "("); err != nil {
		return nil, err
	}
	colTok, err := p.expect(tokIdent, "index column")
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRParen, ")"); err != nil {
		return nil, err
	}
	return &CreateIndexStmt{Name: nameTok.text, Table: tableTok.text, Col: colTok.text}, nil
}

// parseDropIndex DROP INDEX [idx] ON tbl
func (p *parser) parseDropIndex() (Stmt, error) {
	if err := p.expectKeyword("INDEX"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "index name")
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	tableTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	return &DropIndexStmt{Name: nameTok.text, Table: tableTok.text}, nil
}

func (p *parser) parseInsert() (Stmt, error) {
	if err := p.expectKeyword("INSERT"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	stmt := &InsertStmt{Table: nameTok.text}
	// 可选 (col, ...)
	if p.cur().kind == tokLParen {
		p.next()
		for {
			col, err := p.expect(tokIdent, "column name")
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, col.text)
			if p.cur().kind == tokComma {
				p.next()
				continue
			}
			if _, err := p.expect(tokRParen, ")"); err != nil {
				return nil, err
			}
			break
		}
	}
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	for {
		if _, err := p.expect(tokLParen, "("); err != nil {
			return nil, err
		}
		var row []Value
		for {
			v, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			row = append(row, v)
			if p.cur().kind == tokComma {
				p.next()
				continue
			}
			if _, err := p.expect(tokRParen, ")"); err != nil {
				return nil, err
			}
			break
		}
		stmt.Rows = append(stmt.Rows, row)
		if p.cur().kind == tokComma {
			p.next()
			continue
		}
		break
	}
	return stmt, nil
}

func (p *parser) parseSelect() (Stmt, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	stmt := &SelectStmt{Limit: 0}
	for {
		sc, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, sc)
		if p.cur().kind == tokComma {
			p.next()
			continue
		}
		break
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	// FROM：子查询 (SELECT ...) [AS] alias | 表名 [AS] alias
	if p.cur().kind == tokLParen {
		p.next()
		sub, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		subSel, ok := sub.(*SelectStmt)
		if !ok {
			return nil, errf("invalid subquery in FROM")
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return nil, err
		}
		alias, err := p.parseOptAlias()
		if err != nil {
			return nil, err
		}
		if alias == "" {
			return nil, errf("subquery in FROM requires alias")
		}
		stmt.Subquery = subSel
		stmt.SubAlias = alias
	} else {
		nameTok, err := p.expect(tokIdent, "table name")
		if err != nil {
			return nil, err
		}
		stmt.From = nameTok.text
		alias, err := p.parseOptAlias()
		if err != nil {
			return nil, err
		}
		stmt.FromAlias = alias
	}
	// JOIN 子句（可多个）
	for p.cur().kind == tokKeyword && (p.cur().text == "JOIN" || p.cur().text == "INNER" || p.cur().text == "LEFT") {
		jc, err := p.parseJoin()
		if err != nil {
			return nil, err
		}
		stmt.Joins = append(stmt.Joins, jc)
	}
	// WHERE
	if p.cur().kind == tokKeyword && p.cur().text == "WHERE" {
		p.next()
		w, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = &w
	}
	// GROUP BY
	if p.cur().kind == tokKeyword && p.cur().text == "GROUP" {
		if err := p.expectKeyword("GROUP"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			ref, col, err := p.parseColumnRef()
			if err != nil {
				return nil, err
			}
			stmt.GroupBy = append(stmt.GroupBy, qualifiedName(ref, col))
			if p.cur().kind == tokComma {
				p.next()
				continue
			}
			break
		}
	}
	// ORDER BY
	if p.cur().kind == tokKeyword && p.cur().text == "ORDER" {
		if err := p.expectKeyword("ORDER"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		ref, col, err := p.parseColumnRef()
		if err != nil {
			return nil, err
		}
		ob := &OrderBy{Ref: ref, Col: col}
		if p.cur().kind == tokKeyword {
			switch p.cur().text {
			case "ASC":
				p.next()
			case "DESC":
				ob.Desc = true
				p.next()
			}
		}
		stmt.OrderBy = ob
	}
	// LIMIT
	if p.cur().kind == tokKeyword && p.cur().text == "LIMIT" {
		p.next()
		nTok, err := p.expect(tokNumber, "limit number")
		if err != nil {
			return nil, err
		}
		n, err := parseInt10(nTok.text)
		if err != nil {
			return nil, err
		}
		stmt.Limit = int(n)
	}
	return stmt, nil
}

// parseOptAlias 解析可选 [AS] alias；返回别名，无则 ""。
func (p *parser) parseOptAlias() (string, error) {
	if p.cur().kind == tokKeyword && p.cur().text == "AS" {
		p.next()
		tok, err := p.expect(tokIdent, "alias")
		if err != nil {
			return "", err
		}
		return tok.text, nil
	}
	if p.cur().kind == tokIdent {
		a := p.cur().text
		p.next()
		return a, nil
	}
	return "", nil
}

// parseJoin 解析 JOIN 子句：[[INNER|LEFT [OUTER]] JOIN] 右表 [(SELECT...)] [AS] alias [ON conds]。
func (p *parser) parseJoin() (JoinClause, error) {
	var jc JoinClause
	switch p.cur().text {
	case "INNER":
		p.next()
		if err := p.expectKeyword("JOIN"); err != nil {
			return jc, err
		}
		jc.Type = "INNER"
	case "LEFT":
		p.next()
		if p.cur().kind == tokKeyword && p.cur().text == "OUTER" {
			p.next()
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return jc, err
		}
		jc.Type = "LEFT"
	default:
		if err := p.expectKeyword("JOIN"); err != nil {
			return jc, err
		}
		jc.Type = "INNER"
	}
	if p.cur().kind == tokLParen {
		p.next()
		sub, err := p.parseSelect()
		if err != nil {
			return jc, err
		}
		subSel, ok := sub.(*SelectStmt)
		if !ok {
			return jc, errf("invalid subquery in JOIN")
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return jc, err
		}
		jc.Subquery = subSel
		alias, err := p.parseOptAlias()
		if err != nil {
			return jc, err
		}
		if alias == "" {
			return jc, errf("subquery in JOIN requires alias")
		}
		jc.Alias = alias
	} else {
		nameTok, err := p.expect(tokIdent, "join table name")
		if err != nil {
			return jc, err
		}
		jc.Table = nameTok.text
		alias, err := p.parseOptAlias()
		if err != nil {
			return jc, err
		}
		if alias == "" {
			jc.Alias = jc.Table
		} else {
			jc.Alias = alias
		}
	}
	if p.cur().kind == tokKeyword && p.cur().text == "ON" {
		p.next()
		for {
			c, err := p.parseCond()
			if err != nil {
				return jc, err
			}
			jc.On = append(jc.On, c)
			if p.cur().kind == tokKeyword && p.cur().text == "AND" {
				p.next()
				continue
			}
			break
		}
	}
	return jc, nil
}

// parseColumnRef 解析列引用：col | t.col，返回 (限定名, 裸列名)。
func (p *parser) parseColumnRef() (string, string, error) {
	tok, err := p.expect(tokIdent, "column name")
	if err != nil {
		return "", "", err
	}
	if p.cur().kind == tokDot {
		p.next()
		col, err := p.expect(tokIdent, "column name after dot")
		if err != nil {
			return "", "", err
		}
		return tok.text, col.text, nil
	}
	return "", tok.text, nil
}

func qualifiedName(ref, col string) string {
	if ref == "" {
		return col
	}
	return ref + "." + col
}

func (p *parser) parseSelectColumn() (SelectColumn, error) {
	if p.cur().kind == tokStar {
		p.next()
		return SelectColumn{Raw: "*", Name: "*"}, nil
	}
	// CASE WHEN 表达式（P2）
	if p.cur().kind == tokKeyword && p.cur().text == "CASE" {
		cc, err := p.parseCaseWhen()
		if err != nil {
			return SelectColumn{}, err
		}
		sc := SelectColumn{Raw: "CASE", Name: "CASE", Case: cc}
		if err := p.parseSelectAlias(&sc); err != nil {
			return SelectColumn{}, err
		}
		return sc, nil
	}
	if p.cur().kind == tokIdent {
		first := p.cur().text
		p.next()
		// 限定列 t.col
		if p.cur().kind == tokDot {
			p.next()
			colTok, err := p.expect(tokIdent, "column name")
			if err != nil {
				return SelectColumn{}, err
			}
			sc := SelectColumn{Raw: first + "." + colTok.text, Ref: first, Col: colTok.text, Name: first + "." + colTok.text}
			if err := p.parseSelectAlias(&sc); err != nil {
				return SelectColumn{}, err
			}
			return sc, nil
		}
		// 聚合函数：COUNT/SUM/AVG(...)
		if p.cur().kind == tokLParen {
			upper := strings.ToUpper(first)
			if upper != "COUNT" && upper != "SUM" && upper != "AVG" {
				return SelectColumn{}, errf("function %q not supported", first)
			}
			p.next() // (
			if upper == "COUNT" && p.cur().kind == tokStar {
				p.next()
				if _, err := p.expect(tokRParen, ")"); err != nil {
					return SelectColumn{}, err
				}
				sc := SelectColumn{Raw: "COUNT(*)", Agg: "COUNT", Name: "COUNT(*)"}
				if err := p.parseSelectAlias(&sc); err != nil {
					return SelectColumn{}, err
				}
				return sc, nil
			}
			argRef, argCol, err := p.parseColumnRef()
			if err != nil {
				return SelectColumn{}, err
			}
			if _, err := p.expect(tokRParen, ")"); err != nil {
				return SelectColumn{}, err
			}
			sc := SelectColumn{Raw: upper + "(" + qualifiedName(argRef, argCol) + ")", Agg: upper, Ref: argRef, Col: argCol, Name: upper + "(" + qualifiedName(argRef, argCol) + ")"}
			if err := p.parseSelectAlias(&sc); err != nil {
				return SelectColumn{}, err
			}
			return sc, nil
		}
		sc := SelectColumn{Raw: first, Col: first, Name: first}
		if err := p.parseSelectAlias(&sc); err != nil {
			return SelectColumn{}, err
		}
		return sc, nil
	}
	return SelectColumn{}, errf("expected select column at %d, got %q", p.cur().pos, p.cur().text)
}

// parseSelectAlias 解析 SELECT 列可选的 [AS] alias（覆盖输出列名）。
func (p *parser) parseSelectAlias(sc *SelectColumn) error {
	if p.cur().kind == tokKeyword && p.cur().text == "AS" {
		p.next()
		tok, err := p.expect(tokIdent, "alias")
		if err != nil {
			return err
		}
		sc.Alias = tok.text
		sc.Name = tok.text
	}
	return nil
}

func (p *parser) parseWhere() (Where, error) {
	var w Where
	for {
		c, err := p.parseCond()
		if err != nil {
			return w, err
		}
		w.Conds = append(w.Conds, c)
		if p.cur().kind == tokKeyword && p.cur().text == "AND" {
			p.next()
			continue
		}
		break
	}
	return w, nil
}

// parseCond 解析单个条件：左列（或 CASE 表达式）op 右值（支持 BETWEEN/IN/LIKE 与列引用右值）。
func (p *parser) parseCond() (Cond, error) {
	var c Cond
	if p.cur().kind == tokKeyword && p.cur().text == "CASE" {
		cc, err := p.parseCaseWhen()
		if err != nil {
			return c, err
		}
		c.LeftCase = cc
	} else {
		ref, col, err := p.parseColumnRef()
		if err != nil {
			return c, err
		}
		c.Ref, c.Col = ref, col
	}
	switch p.cur().kind {
	case tokEq:
		c.Op = "="
		p.next()
	case tokNe:
		c.Op = "!="
		p.next()
	case tokLt:
		c.Op = "<"
		p.next()
	case tokGt:
		c.Op = ">"
		p.next()
	case tokLe:
		c.Op = "<="
		p.next()
	case tokGe:
		c.Op = ">="
		p.next()
	case tokKeyword:
		switch p.cur().text {
		case "BETWEEN":
			c.Op = "BETWEEN"
			p.next()
			v, err := p.parseValue()
			if err != nil {
				return c, err
			}
			c.Val = v
			if err := p.expectKeyword("AND"); err != nil {
				return c, err
			}
			v2, err := p.parseValue()
			if err != nil {
				return c, err
			}
			c.Val2 = v2
			return c, nil
		case "IN":
			c.Op = "IN"
			p.next()
			if _, err := p.expect(tokLParen, "("); err != nil {
				return c, err
			}
			if p.cur().kind == tokKeyword && p.cur().text == "SELECT" {
				sub, err := p.parseSelect()
				if err != nil {
					return c, err
				}
				subSel, ok := sub.(*SelectStmt)
				if !ok {
					return c, errf("invalid subquery in IN")
				}
				c.Sub = subSel
			} else {
				for {
					v, err := p.parseValue()
					if err != nil {
						return c, err
					}
					c.InList = append(c.InList, v)
					if p.cur().kind == tokComma {
						p.next()
						continue
					}
					break
				}
			}
			if _, err := p.expect(tokRParen, ")"); err != nil {
				return c, err
			}
			return c, nil
		case "LIKE":
			c.Op = "LIKE"
			p.next()
			v, err := p.parseValue()
			if err != nil {
				return c, err
			}
			c.Val = v
			return c, nil
		default:
			return c, errf("expected comparison operator at %d, got %q", p.cur().pos, p.cur().text)
		}
	default:
		return c, errf("expected comparison operator at %d, got %q", p.cur().pos, p.cur().text)
	}
	// 右值：列引用（JOIN ON 场景）或字面量
	if p.cur().kind == tokIdent {
		rref, rcol, err := p.parseColumnRef()
		if err != nil {
			return c, err
		}
		c.RightRef = qualifiedName(rref, rcol)
		return c, nil
	}
	v, err := p.parseValue()
	if err != nil {
		return c, err
	}
	c.Val = v
	return c, nil
}

// parseCaseWhen CASE WHEN <cond> [AND <cond>...] THEN <val> [WHEN ...] [ELSE <val>] END。
// 分支条件复用 parseCond（支持 = != < > <= >= BETWEEN IN LIKE 及嵌套 CASE 左表达式）。
func (p *parser) parseCaseWhen() (*CaseWhenClause, error) {
	if err := p.expectKeyword("CASE"); err != nil {
		return nil, err
	}
	cc := &CaseWhenClause{}
	for {
		if p.cur().kind == tokKeyword && p.cur().text == "WHEN" {
			p.next()
			conds, err := p.parseWhenConds()
			if err != nil {
				return nil, err
			}
			if err := p.expectKeyword("THEN"); err != nil {
				return nil, err
			}
			v, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			cc.Branches = append(cc.Branches, CaseBranch{Conds: conds, Then: v})
			continue
		}
		if p.cur().kind == tokKeyword && p.cur().text == "ELSE" {
			p.next()
			v, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			cc.Else = v
			cc.HasElse = true
			if err := p.expectKeyword("END"); err != nil {
				return nil, err
			}
			return cc, nil
		}
		if p.cur().kind == tokKeyword && p.cur().text == "END" {
			p.next()
			return cc, nil
		}
		return nil, errf("expected WHEN, ELSE or END in CASE at %d, got %q", p.cur().pos, p.cur().text)
	}
}

// parseWhenConds 解析 WHEN 分支条件组（AND 组合，至少一个条件）。
func (p *parser) parseWhenConds() (Where, error) {
	var w Where
	for {
		c, err := p.parseCond()
		if err != nil {
			return w, err
		}
		w.Conds = append(w.Conds, c)
		if p.cur().kind == tokKeyword && p.cur().text == "AND" {
			p.next()
			continue
		}
		break
	}
	return w, nil
}

func (p *parser) parseValue() (Value, error) {
	switch p.cur().kind {
	case tokNumber:
		text := p.cur().text
		p.next()
		if strings.Contains(text, ".") {
			// DECIMAL 字面量（如 12.34 / -0.5）
			scaled, canon, err := parseDecimal(text)
			if err != nil {
				return Value{}, err
			}
			return Value{Kind: "DECIMAL", I: scaled, S: canon}, nil
		}
		n, err := parseInt10(text)
		if err != nil {
			return Value{}, err
		}
		return IntVal(n), nil
	case tokString:
		s := p.cur().text
		p.next()
		return StrVal(s), nil
	}
	return Value{}, errf("expected value at %d, got %q", p.cur().pos, p.cur().text)
}

func (p *parser) parseUpdate() (Stmt, error) {
	if err := p.expectKeyword("UPDATE"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	stmt := &UpdateStmt{Table: nameTok.text}
	for {
		colTok, err := p.expect(tokIdent, "column name")
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokEq, "="); err != nil {
			return nil, err
		}
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		stmt.Sets = append(stmt.Sets, SetItem{Col: colTok.text, Val: v})
		if p.cur().kind == tokComma {
			p.next()
			continue
		}
		break
	}
	if p.cur().kind == tokKeyword && p.cur().text == "WHERE" {
		p.next()
		w, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = &w
	}
	return stmt, nil
}

func (p *parser) parseDelete() (Stmt, error) {
	if err := p.expectKeyword("DELETE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	stmt := &DeleteStmt{Table: nameTok.text}
	if p.cur().kind == tokKeyword && p.cur().text == "WHERE" {
		p.next()
		w, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = &w
	}
	return stmt, nil
}

// peekKind 返回下一 token 类型（不消费）。
func (p *parser) peekKind() tokKind {
	if p.pos+1 < len(p.toks) {
		return p.toks[p.pos+1].kind
	}
	return tokEOF
}

// parseShow SHOW TABLES | SHOW INDEX FROM t | SHOW SLOWQUERIES | SHOW NODES | SHOW REGION ROUTES | SHOW STATS
func (p *parser) parseShow() (Stmt, error) {
	if err := p.expectKeyword("SHOW"); err != nil {
		return nil, err
	}
	switch p.cur().text {
	case "TABLES":
		p.next()
		return &ShowTablesStmt{}, nil
	case "STATS":
		p.next()
		return &ShowStatsStmt{}, nil
	case "SLOWQUERIES":
		p.next()
		return &ShowSlowQueriesStmt{}, nil
	case "NODES":
		p.next()
		return &ShowNodesStmt{}, nil
	case "REGION":
		p.next()
		if err := p.expectKeyword("ROUTES"); err != nil {
			return nil, err
		}
		return &ShowRegionRoutesStmt{}, nil
	case "INDEX":
		p.next()
		if err := p.expectKeyword("FROM"); err != nil {
			return nil, err
		}
		nameTok, err := p.expect(tokIdent, "table name")
		if err != nil {
			return nil, err
		}
		return &ShowIndexStmt{Table: nameTok.text}, nil
	}
	return nil, errf("expected TABLES, INDEX, SLOWQUERIES, NODES or REGION ROUTES after SHOW at %d, got %q", p.cur().pos, p.cur().text)
}

// parseExplain EXPLAIN <SELECT>（不实际执行查询）。
func (p *parser) parseExplain() (Stmt, error) {
	if err := p.expectKeyword("EXPLAIN"); err != nil {
		return nil, err
	}
	if p.cur().kind != tokKeyword || p.cur().text != "SELECT" {
		return nil, errf("expected SELECT after EXPLAIN at %d, got %q", p.cur().pos, p.cur().text)
	}
	sel, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	selStmt, ok := sel.(*SelectStmt)
	if !ok {
		return nil, errf("EXPLAIN requires a SELECT statement")
	}
	return &ExplainStmt{Select: selStmt}, nil
}

// parseExport EXPORT TABLE t [(cols)] TO 'path'
func (p *parser) parseExport() (Stmt, error) {
	if err := p.expectKeyword("EXPORT"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	stmt := &ExportStmt{Table: nameTok.text}
	if p.cur().kind == tokLParen {
		p.next()
		for {
			col, err := p.expect(tokIdent, "column name")
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, col.text)
			if p.cur().kind == tokComma {
				p.next()
				continue
			}
			if _, err := p.expect(tokRParen, ")"); err != nil {
				return nil, err
			}
			break
		}
	}
	if err := p.expectKeyword("TO"); err != nil {
		return nil, err
	}
	pathTok, err := p.expect(tokString, "file path string")
	if err != nil {
		return nil, err
	}
	stmt.Path = pathTok.text
	return stmt, nil
}

// parseImport IMPORT INTO t FROM 'path'
func (p *parser) parseImport() (Stmt, error) {
	if err := p.expectKeyword("IMPORT"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	pathTok, err := p.expect(tokString, "file path string")
	if err != nil {
		return nil, err
	}
	return &ImportStmt{Table: nameTok.text, Path: pathTok.text}, nil
}

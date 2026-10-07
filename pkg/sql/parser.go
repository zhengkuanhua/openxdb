package sql

import (
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
	}
	return nil, errf("unsupported statement %q at %d", p.cur().text, p.cur().pos)
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
		if typ != "INT" && typ != "TEXT" && typ != "STRING" {
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
	nameTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	return &DropTableStmt{Name: nameTok.text}, nil
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
	nameTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	stmt.From = nameTok.text
	// WHERE
	if p.cur().kind == tokKeyword && p.cur().text == "WHERE" {
		p.next()
		w, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = &w
	}
	// ORDER BY
	if p.cur().kind == tokKeyword && p.cur().text == "ORDER" {
		if err := p.expectKeyword("ORDER"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		colTok, err := p.expect(tokIdent, "order by column")
		if err != nil {
			return nil, err
		}
		ob := &OrderBy{Col: colTok.text}
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

func (p *parser) parseSelectColumn() (SelectColumn, error) {
	if p.cur().kind == tokStar {
		p.next()
		return SelectColumn{Raw: "*", Name: "*"}, nil
	}
	if p.cur().kind == tokIdent {
		name := p.cur().text
		p.next()
		// 聚合函数：COUNT/SUM/AVG(...)
		if p.cur().kind == tokLParen {
			upper := strings.ToUpper(name)
			if upper != "COUNT" && upper != "SUM" && upper != "AVG" {
				return SelectColumn{}, errf("function %q not supported", name)
			}
			p.next() // (
			if upper == "COUNT" && p.cur().kind == tokStar {
				p.next()
				if _, err := p.expect(tokRParen, ")"); err != nil {
					return SelectColumn{}, err
				}
				return SelectColumn{Raw: "COUNT(*)", Agg: "COUNT", Name: "COUNT(*)"}, nil
			}
			colTok, err := p.expect(tokIdent, "aggregate column")
			if err != nil {
				return SelectColumn{}, err
			}
			if _, err := p.expect(tokRParen, ")"); err != nil {
				return SelectColumn{}, err
			}
			return SelectColumn{Raw: upper + "(" + colTok.text + ")", Agg: upper, Col: colTok.text, Name: upper + "(" + colTok.text + ")"}, nil
		}
		return SelectColumn{Raw: name, Name: name}, nil
	}
	return SelectColumn{}, errf("expected select column at %d, got %q", p.cur().pos, p.cur().text)
}

func (p *parser) parseWhere() (Where, error) {
	var w Where
	for {
		colTok, err := p.expect(tokIdent, "column name in WHERE")
		if err != nil {
			return w, err
		}
		var op string
		switch p.cur().kind {
		case tokEq:
			op = "="
		case tokNe:
			op = "!="
		case tokLt:
			op = "<"
		case tokGt:
			op = ">"
		case tokLe:
			op = "<="
		case tokGe:
			op = ">="
		default:
			return w, errf("expected comparison operator at %d, got %q", p.cur().pos, p.cur().text)
		}
		p.next()
		v, err := p.parseValue()
		if err != nil {
			return w, err
		}
		w.Conds = append(w.Conds, Cond{Col: colTok.text, Op: op, Val: v})
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
		n, err := parseInt10(p.cur().text)
		p.next()
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

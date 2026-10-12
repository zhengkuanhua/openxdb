package sql

import (
	"strconv"
	"strings"
)

// parser 递归下降解析器（M1 子集，见开发手册 §4.1）。
type parser struct {
	toks []token
	pos  int
	src  string
}

// Parse 解析单条 SQL 语句（结尾分号可选）。
func Parse(sql string) (Stmt, error) {
	toks, err := lex(sql)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks, src: sql}
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
func (p *parser) next() {
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
}

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
	case "REPLACE":
		// M10 UPSERT：REPLACE INTO 语义与 INSERT 同构（冲突时删旧插新）
		return p.parseInsertInto(true)
	case "SELECT":
		return p.parseSelect()
	case "WITH":
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
	case "ALTER":
		return p.parseAlter()
	case "REFRESH":
		// M15 物化视图：REFRESH MATERIALIZED VIEW [CONCURRENTLY] <name>
		return p.parseRefreshMatView()
	case "SET":
		return p.parseSet()
	case "CALL":
		// M15-3：CALL <procedure> [( <arg>, ... )]
		return p.parseCall()
	case "BACKUP":
		return p.parseBackup()
	case "RESTORE":
		return p.parseRestore()
	case "GRANT":
		return p.parseGrant()
	case "REVOKE":
		return p.parseRevoke()
	}
	return nil, errf("unsupported statement %q at %d", p.cur().text, p.cur().pos)
}

// parseSet SET [PERSIST] <name> = <value>：会话变量设置（M9 查询缓存开关等；
// M16-1 起支持 PERSIST，应用后写入持久化配置文件）。
// 值支持标识符（on/off/true/false）、数字（1/0）与字符串字面量。
func (p *parser) parseSet() (Stmt, error) {
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	s := &SetStmt{}
	if p.cur().kind == tokIdent && strings.EqualFold(p.cur().text, "PERSIST") {
		s.Persist = true
		p.next()
	}
	if p.cur().kind != tokIdent {
		return nil, errf("expected variable name at %d, got %q", p.cur().pos, p.cur().text)
	}
	s.Name = strings.ToLower(p.cur().text)
	p.next()
	if _, err := p.expect(tokEq, "="); err != nil {
		return nil, err
	}
	switch p.cur().kind {
	case tokIdent, tokKeyword:
		s.Value = strings.ToLower(p.cur().text)
		p.next()
	case tokNumber:
		s.Value = p.cur().text
		p.next()
	case tokString:
		s.Value = p.cur().text
		p.next()
	default:
		return nil, errf("expected value at %d, got %q", p.cur().pos, p.cur().text)
	}
	return s, nil
}

// parseAlter ALTER TABLE（M9 在线 DDL）：
//
//	ALTER TABLE t ADD COLUMN c TYPE
//	ALTER TABLE t DROP COLUMN c
//	ALTER TABLE t RENAME COLUMN c TO c2
//	ALTER TABLE t RENAME TO t2
//	ALTER TABLE t ADD INDEX i (c)
//	ALTER TABLE t DROP INDEX i
func (p *parser) parseAlter() (Stmt, error) {
	if err := p.expectKeyword("ALTER"); err != nil {
		return nil, err
	}
	// M11：ALTER USER '<name>' IDENTIFIED BY '<pwd>'
	if p.cur().kind == tokKeyword && p.cur().text == "USER" {
		return p.parseAlterUser()
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	tbl, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	if p.cur().kind != tokKeyword {
		return nil, errf("expected ALTER action at %d, got %q", p.cur().pos, p.cur().text)
	}
	s := &AlterTableStmt{Table: tbl.text}
	switch p.cur().text {
	case "ADD":
		p.next()
		if p.cur().kind == tokKeyword && p.cur().text == "COLUMN" {
			p.next()
			col, err := p.expect(tokIdent, "column name")
			if err != nil {
				return nil, err
			}
			typ, err := p.expect(tokIdent, "column type")
			if err != nil {
				return nil, err
			}
			s.Action = "ADD_COLUMN"
			s.Column = ColumnDef{Name: col.text, Type: strings.ToUpper(typ.text)}
			return s, nil
		}
		if p.cur().kind == tokKeyword && p.cur().text == "INDEX" {
			p.next()
			idx, err := p.expect(tokIdent, "index name")
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokLParen, "("); err != nil {
				return nil, err
			}
			col, err := p.expect(tokIdent, "index column")
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokRParen, ")"); err != nil {
				return nil, err
			}
			s.Action = "ADD_INDEX"
			s.Name = idx.text
			s.Col = col.text
			return s, nil
		}
		return nil, errf("expected COLUMN or INDEX after ADD at %d, got %q", p.cur().pos, p.cur().text)
	case "DROP":
		p.next()
		if p.cur().kind == tokKeyword && p.cur().text == "INDEX" {
			p.next()
			idx, err := p.expect(tokIdent, "index name")
			if err != nil {
				return nil, err
			}
			s.Action = "DROP_INDEX"
			s.Name = idx.text
			return s, nil
		}
		if err := p.expectKeyword("COLUMN"); err != nil {
			return nil, err
		}
		col, err := p.expect(tokIdent, "column name")
		if err != nil {
			return nil, err
		}
		s.Action = "DROP_COLUMN"
		s.Col = col.text
		return s, nil
	case "RENAME":
		p.next()
		if p.cur().kind == tokKeyword && p.cur().text == "COLUMN" {
			p.next()
			old, err := p.expect(tokIdent, "column name")
			if err != nil {
				return nil, err
			}
			if err := p.expectKeyword("TO"); err != nil {
				return nil, err
			}
			nw, err := p.expect(tokIdent, "new column name")
			if err != nil {
				return nil, err
			}
			s.Action = "RENAME_COLUMN"
			s.Col = old.text
			s.NewName = nw.text
			return s, nil
		}
		if p.cur().kind == tokKeyword && p.cur().text == "TO" {
			p.next()
			nw, err := p.expect(tokIdent, "new table name")
			if err != nil {
				return nil, err
			}
			s.Action = "RENAME_TABLE"
			s.NewName = nw.text
			return s, nil
		}
		return nil, errf("expected COLUMN or TO after RENAME at %d, got %q", p.cur().pos, p.cur().text)
	}
	return nil, errf("unsupported ALTER action %q at %d", p.cur().text, p.cur().pos)
}

// parseAlterUser ALTER USER '<name>' IDENTIFIED BY '<pwd>'（重置密码）。
func (p *parser) parseAlterUser() (Stmt, error) {
	if err := p.expectKeyword("USER"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokString, "user name")
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("IDENTIFIED"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("BY"); err != nil {
		return nil, err
	}
	pwdTok, err := p.expect(tokString, "password")
	if err != nil {
		return nil, err
	}
	return &AlterUserStmt{Name: nameTok.text, Password: pwdTok.text}, nil
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

// parsePrivList 解析逗号分隔的权限名列表（SELECT/INSERT/UPDATE/DELETE/ALL）。
// 权限 token 为关键字（大写），返回大写权限名切片。
func (p *parser) parsePrivList() ([]string, error) {
	var privs []string
	for {
		if p.cur().kind != tokKeyword {
			return nil, errf("expected privilege at %d, got %q", p.cur().pos, p.cur().text)
		}
		priv := p.cur().text
		switch priv {
		case "SELECT", "INSERT", "UPDATE", "DELETE", "ALL":
			privs = append(privs, priv)
		default:
			return nil, errf("unsupported privilege %q at %d", priv, p.cur().pos)
		}
		p.next()
		if p.cur().kind != tokComma {
			return privs, nil
		}
		p.next()
	}
}

// parseGrant GRANT <priv[,priv...]> ON <table> TO '<user>'。
func (p *parser) parseGrant() (Stmt, error) {
	if err := p.expectKeyword("GRANT"); err != nil {
		return nil, err
	}
	privs, err := p.parsePrivList()
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
	if err := p.expectKeyword("TO"); err != nil {
		return nil, err
	}
	userTok, err := p.expect(tokString, "user name")
	if err != nil {
		return nil, err
	}
	return &GrantStmt{Privileges: privs, Table: tableTok.text, User: userTok.text}, nil
}

// parseRevoke REVOKE <priv[,priv...]> ON <table> FROM '<user>'。
func (p *parser) parseRevoke() (Stmt, error) {
	if err := p.expectKeyword("REVOKE"); err != nil {
		return nil, err
	}
	privs, err := p.parsePrivList()
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
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	userTok, err := p.expect(tokString, "user name")
	if err != nil {
		return nil, err
	}
	return &RevokeStmt{Privileges: privs, Table: tableTok.text, User: userTok.text}, nil
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
	case "UNIQUE":
		// M10 UPSERT：CREATE UNIQUE INDEX（唯一索引，UPSERT 冲突键来源之一）
		return p.parseCreateIndex()
	case "MATERIALIZED":
		// M15：CREATE MATERIALIZED VIEW <name> AS <SELECT>
		return p.parseCreateMatView()
	case "VIEW":
		return p.parseCreateView()
	case "USER":
		// M11：CREATE USER '<name>' IDENTIFIED BY '<pwd>'
		return p.parseCreateUser()
	case "TABLE":
		// 已有 TABLE 分支
	case "TRIGGER":
		// M15-2：CREATE TRIGGER <name> BEFORE|AFTER <event> ON <table> FOR EACH ROW <body>
		return p.parseCreateTrigger()
	case "PROCEDURE":
		// M15-3：CREATE PROCEDURE <name> [( [IN] <param> <TYPE> [, ...] )] BEGIN <body> END
		return p.parseCreateProcedure()
	default:
		return nil, errf("expected TABLE, INDEX, VIEW, MATERIALIZED VIEW, USER, TRIGGER or PROCEDURE at %d, got %q", p.cur().pos, p.cur().text)
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
			// 逗号后可以是表级 PRIMARY KEY (col) 或 FOREIGN KEY 约束
			if p.cur().kind == tokKeyword && (p.cur().text == "PRIMARY" || p.cur().text == "FOREIGN") {
				done, err := p.parseTableLevelTail(stmt)
				if err != nil {
					return nil, err
				}
				if done {
					return stmt, nil
				}
				return nil, errf("expected ) at %d", p.cur().pos)
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
					// 行内主键后仍可跟表级 PRIMARY KEY (col) / FOREIGN KEY 约束
					if p.cur().kind == tokKeyword && (p.cur().text == "PRIMARY" || p.cur().text == "FOREIGN") {
						done, err := p.parseTableLevelTail(stmt)
						if err != nil {
							return nil, err
						}
						if done {
							return stmt, nil
						}
						return nil, errf("expected ) at %d", p.cur().pos)
					}
					continue
				case tokRParen:
					p.next()
					return stmt, nil
				default:
					return nil, errf("expected , or ) at %d, got %q", p.cur().pos, p.cur().text)
				}
			}
			// 列级外键：col TYPE REFERENCES parent (col) [ON DELETE CASCADE|RESTRICT]
			if p.cur().kind == tokKeyword && p.cur().text == "REFERENCES" {
				fk, err := p.parseRefClause(ForeignKey{Columns: []string{col.text}})
				if err != nil {
					return nil, err
				}
				stmt.ForeignKeys = append(stmt.ForeignKeys, fk)
				switch p.cur().kind {
				case tokComma:
					p.next()
					continue
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
			return nil, errf("expected , or ) at %d, got %q", p.cur().pos, p.cur().text)
		}
	}
}

// parseCreateUser CREATE USER '<name>' IDENTIFIED BY '<pwd>'。
func (p *parser) parseCreateUser() (Stmt, error) {
	if err := p.expectKeyword("USER"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokString, "user name")
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("IDENTIFIED"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("BY"); err != nil {
		return nil, err
	}
	pwdTok, err := p.expect(tokString, "password")
	if err != nil {
		return nil, err
	}
	return &CreateUserStmt{Name: nameTok.text, Password: pwdTok.text}, nil
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

// parseTableLevelTail 解析表级约束尾部（逗号分隔的 PRIMARY KEY / FOREIGN KEY）。
// 调用时 cur 位于表级约束首关键字；循环直到消费右括号并返回 done=true。
func (p *parser) parseTableLevelTail(stmt *CreateTableStmt) (bool, error) {
	for {
		if p.cur().kind == tokKeyword && p.cur().text == "PRIMARY" {
			if err := p.parsePrimaryKey(stmt); err != nil {
				return false, err
			}
		} else if p.cur().kind == tokKeyword && p.cur().text == "FOREIGN" {
			fk, err := p.parseForeignKey()
			if err != nil {
				return false, err
			}
			stmt.ForeignKeys = append(stmt.ForeignKeys, fk)
		} else {
			return false, errf("expected PRIMARY KEY or FOREIGN KEY at %d, got %q", p.cur().pos, p.cur().text)
		}
		if p.cur().kind == tokComma {
			p.next()
			continue
		}
		if p.cur().kind == tokRParen {
			p.next()
			return true, nil
		}
		return false, errf("expected , or ) at %d, got %q", p.cur().pos, p.cur().text)
	}
}

// parseForeignKey FOREIGN KEY (col,...) REFERENCES tbl (col,...) [ON DELETE ...]
func (p *parser) parseForeignKey() (ForeignKey, error) {
	var fk ForeignKey
	if err := p.expectKeyword("FOREIGN"); err != nil {
		return fk, err
	}
	if err := p.expectKeyword("KEY"); err != nil {
		return fk, err
	}
	if _, err := p.expect(tokLParen, "("); err != nil {
		return fk, err
	}
	for {
		ct, err := p.expect(tokIdent, "column name")
		if err != nil {
			return fk, err
		}
		fk.Columns = append(fk.Columns, ct.text)
		if p.cur().kind == tokComma {
			p.next()
			continue
		}
		break
	}
	if _, err := p.expect(tokRParen, ")"); err != nil {
		return fk, err
	}
	return p.parseRefClause(fk)
}

// parseRefClause REFERENCES tbl (col,...) [ON DELETE CASCADE|RESTRICT] 公共子句
// （表级 FOREIGN KEY 与列级 REFERENCES 复用）。
func (p *parser) parseRefClause(fk ForeignKey) (ForeignKey, error) {
	if err := p.expectKeyword("REFERENCES"); err != nil {
		return fk, err
	}
	rt, err := p.expect(tokIdent, "referenced table")
	if err != nil {
		return fk, err
	}
	fk.RefTable = rt.text
	if _, err := p.expect(tokLParen, "("); err != nil {
		return fk, err
	}
	for {
		ct, err := p.expect(tokIdent, "referenced column")
		if err != nil {
			return fk, err
		}
		fk.RefColumns = append(fk.RefColumns, ct.text)
		if p.cur().kind == tokComma {
			p.next()
			continue
		}
		break
	}
	if _, err := p.expect(tokRParen, ")"); err != nil {
		return fk, err
	}
	fk.OnDelete = "RESTRICT"
	if p.cur().kind == tokKeyword && p.cur().text == "ON" {
		if err := p.expectKeyword("ON"); err != nil {
			return fk, err
		}
		if err := p.expectKeyword("DELETE"); err != nil {
			return fk, err
		}
		if p.cur().kind != tokKeyword || (p.cur().text != "CASCADE" && p.cur().text != "RESTRICT") {
			return fk, errf("expected CASCADE or RESTRICT at %d, got %q", p.cur().pos, p.cur().text)
		}
		fk.OnDelete = p.cur().text
		p.next()
	}
	return fk, nil
}

func (p *parser) parseDrop() (Stmt, error) {
	if err := p.expectKeyword("DROP"); err != nil {
		return nil, err
	}
	switch p.cur().text {
	case "INDEX":
		return p.parseDropIndex()
	case "MATERIALIZED":
		// M15：DROP MATERIALIZED VIEW [IF EXISTS] <name>
		return p.parseDropMatView()
	case "VIEW":
		return p.parseDropView()
	case "USER":
		// M11 用户管理：DROP USER [IF EXISTS] '<name>'
		return p.parseDropUser()
	case "TRIGGER":
		// M15-2：DROP TRIGGER [IF EXISTS] <name>
		return p.parseDropTrigger()
	case "PROCEDURE":
		// M15-3：DROP PROCEDURE [IF EXISTS] <name>
		return p.parseDropProcedure()
	case "TABLE":
	default:
		return nil, errf("expected TABLE, INDEX, VIEW, MATERIALIZED VIEW, USER, TRIGGER or PROCEDURE at %d, got %q", p.cur().pos, p.cur().text)
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

// parseDropUser DROP USER [IF EXISTS] '<name>'。
func (p *parser) parseDropUser() (Stmt, error) {
	if err := p.expectKeyword("USER"); err != nil {
		return nil, err
	}
	s := &DropUserStmt{}
	// DROP USER [IF EXISTS] <name>
	if p.cur().kind == tokKeyword && p.cur().text == "IF" {
		if err := p.expectKeyword("IF"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		s.IfExists = true
	}
	nameTok, err := p.expect(tokString, "user name")
	if err != nil {
		return nil, err
	}
	s.Name = nameTok.text
	return s, nil
}

// parseCreateView CREATE VIEW <name> AS <SELECT>：收集视图名并解析定义。
func (p *parser) parseCreateView() (Stmt, error) {
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "view name")
	if err != nil {
		return nil, err
	}
	if p.cur().kind != tokKeyword || p.cur().text != "AS" {
		return nil, errf("expected AS at %d, got %q", p.cur().pos, p.cur().text)
	}
	p.next()
	selStmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	sel, ok := selStmt.(*SelectStmt)
	if !ok {
		return nil, errf("expected SELECT after AS at %d", p.cur().pos)
	}
	return &CreateViewStmt{Name: nameTok.text, Select: sel}, nil
}

// parseDropView DROP VIEW [IF EXISTS] <name>。
func (p *parser) parseDropView() (Stmt, error) {
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	s := &DropViewStmt{}
	if p.cur().kind == tokKeyword && p.cur().text == "IF" {
		if err := p.expectKeyword("IF"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		s.IfExists = true
	}
	nameTok, err := p.expect(tokIdent, "view name")
	if err != nil {
		return nil, err
	}
	s.Name = nameTok.text
	return s, nil
}

// remaining 返回从当前位置到语句末尾的原始 SQL 文本（用于截取触发器体）。
func (p *parser) remaining() string {
	if p.pos >= len(p.toks) {
		return ""
	}
	start := p.toks[p.pos].pos
	if start < 0 || start >= len(p.src) {
		return ""
	}
	return p.src[start:]
}

// parseCreateTrigger 解析 CREATE TRIGGER <name> BEFORE|AFTER INSERT|UPDATE|DELETE
// ON <table> FOR EACH ROW <body>（M15-2）。body 为剩余 SQL 原文，由执行层校验。
func (p *parser) parseCreateTrigger() (Stmt, error) {
	if err := p.expectKeyword("TRIGGER"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "trigger name")
	if err != nil {
		return nil, err
	}
	// timing：BEFORE / AFTER（必填）
	var timing string
	if p.cur().kind == tokKeyword && (p.cur().text == "BEFORE" || p.cur().text == "AFTER") {
		timing = p.cur().text
		p.next()
	} else {
		return nil, errf("expected BEFORE or AFTER at %d, got %q", p.cur().pos, p.cur().text)
	}
	// event：INSERT / UPDATE / DELETE（必填）
	var event string
	if p.cur().kind == tokKeyword && (p.cur().text == "INSERT" || p.cur().text == "UPDATE" || p.cur().text == "DELETE") {
		event = p.cur().text
		p.next()
	} else {
		return nil, errf("expected INSERT, UPDATE or DELETE at %d, got %q", p.cur().pos, p.cur().text)
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	tableTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("FOR"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("EACH"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("ROW"); err != nil {
		return nil, err
	}
	body := strings.TrimSpace(p.remaining())
	if body == "" {
		return nil, errf("trigger body required at %d", p.cur().pos)
	}
	body = strings.TrimSuffix(body, ";")
	body = strings.TrimSpace(body)
	// 语法语义：触发器体必须是完整 SQL 语句（稍后由执行层 Parse 复核）。
	if _, err := Parse(body); err != nil {
		return nil, errf("invalid trigger body at %d: %v", nameTok.pos, err)
	}
	// 消耗剩余 token（body 已整体接收）
	p.pos = len(p.toks) - 1
	return &CreateTriggerStmt{Name: nameTok.text, Timing: timing, Event: event, Table: tableTok.text, Body: body}, nil
}

// parseCreateProcedure 解析 CREATE PROCEDURE <name> [( [IN] <param> <TYPE> [, ...] )]
// BEGIN <stmt>; [<stmt>; ...] END（M15-3 存储过程）。
// 参数列表可选；IN 关键字可省略（默认 IN）。过程体为 BEGIN ... END 包裹的
// 一条或多条语句，以原始 SQL 文本保存（内部语句由分号分隔，含参数/变量
// 引用 @name）。收尾 END 取整个语句的最后一个非 EOF token，因此不受过程体
// 内 CASE ... END 表达式影响。
func (p *parser) parseCreateProcedure() (Stmt, error) {
	if err := p.expectKeyword("PROCEDURE"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "procedure name")
	if err != nil {
		return nil, err
	}
	stmt := &CreateProcedureStmt{Name: nameTok.text}
	// 可选参数列表：( [IN] <param> <TYPE> [, ...] )
	if p.cur().kind == tokLParen {
		p.next()
		for {
			if p.cur().kind == tokKeyword && p.cur().text == "IN" {
				p.next()
			}
			paramTok, err := p.expect(tokIdent, "parameter name")
			if err != nil {
				return nil, err
			}
			typTok, err := p.expect(tokIdent, "parameter type")
			if err != nil {
				return nil, err
			}
			typ := strings.ToUpper(typTok.text)
			if typ == "STRING" {
				typ = "TEXT"
			}
			if typ != "INT" && typ != "TEXT" {
				return nil, errf("unsupported parameter type %q at %d (expected INT or TEXT)", typTok.text, typTok.pos)
			}
			stmt.Params = append(stmt.Params, ParamMeta{Name: paramTok.text, Type: typ})
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
	if err := p.expectKeyword("BEGIN"); err != nil {
		return nil, err
	}
	// 过程体：BEGIN 之后到收尾 END（最后一个非 EOF token）之间的原文。
	if p.cur().kind == tokEOF {
		return nil, errf("procedure body required at %d", p.cur().pos)
	}
	bodyStart := p.cur().pos
	last := p.toks[len(p.toks)-2]
	if last.kind != tokKeyword || last.text != "END" {
		return nil, errf("expected keyword END at %d, got %q", last.pos, last.text)
	}
	body := strings.TrimSpace(p.src[bodyStart:last.pos])
	if body == "" {
		return nil, errf("procedure body required at %d", nameTok.pos)
	}
	stmt.Body = body
	p.pos = len(p.toks) - 1
	// 创建期预校验：体可解析（@变量引用以占位字面量代入，运行时再验证）。
	if err := validateProcedureBody(body); err != nil {
		return nil, errf("invalid procedure body at %d: %v", nameTok.pos, err)
	}
	return stmt, nil
}

// parseDropProcedure 解析 DROP PROCEDURE [IF EXISTS] <name>（M15-3）。
func (p *parser) parseDropProcedure() (Stmt, error) {
	if err := p.expectKeyword("PROCEDURE"); err != nil {
		return nil, err
	}
	s := &DropProcedureStmt{}
	if p.cur().kind == tokKeyword && p.cur().text == "IF" {
		if err := p.expectKeyword("IF"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		s.IfExists = true
	}
	nameTok, err := p.expect(tokIdent, "procedure name")
	if err != nil {
		return nil, err
	}
	s.Name = nameTok.text
	return s, nil
}

// parseCall 解析 CALL <name> [( <value> [, <value> ...] )]（M15-3）。
// 无参过程可省略括号；实参列表与过程定义参数一一对应（个数由执行层校验）。
func (p *parser) parseCall() (Stmt, error) {
	if err := p.expectKeyword("CALL"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "procedure name")
	if err != nil {
		return nil, err
	}
	stmt := &CallStmt{Name: nameTok.text}
	if p.cur().kind == tokLParen {
		p.next()
		if p.cur().kind != tokRParen {
			for {
				v, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				stmt.Args = append(stmt.Args, v)
				if p.cur().kind == tokComma {
					p.next()
					continue
				}
				break
			}
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return nil, err
		}
	}
	return stmt, nil
}

// parseDropTrigger 解析 DROP TRIGGER [IF EXISTS] <name>（M15-2）。
func (p *parser) parseDropTrigger() (Stmt, error) {
	if err := p.expectKeyword("TRIGGER"); err != nil {
		return nil, err
	}
	s := &DropTriggerStmt{}
	if p.cur().kind == tokKeyword && p.cur().text == "IF" {
		if err := p.expectKeyword("IF"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		s.IfExists = true
	}
	nameTok, err := p.expect(tokIdent, "trigger name")
	if err != nil {
		return nil, err
	}
	s.Name = nameTok.text
	return s, nil
}

// parseCreateMatView 解析 CREATE MATERIALIZED VIEW <name> AS <SELECT>（M15）。
func (p *parser) parseCreateMatView() (Stmt, error) {
	if err := p.expectKeyword("MATERIALIZED"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "materialized view name")
	if err != nil {
		return nil, err
	}
	if p.cur().kind != tokKeyword || p.cur().text != "AS" {
		return nil, errf("expected AS at %d, got %q", p.cur().pos, p.cur().text)
	}
	p.next()
	selStmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	sel, ok := selStmt.(*SelectStmt)
	if !ok {
		return nil, errf("expected SELECT after AS at %d", p.cur().pos)
	}
	return &CreateMatViewStmt{Name: nameTok.text, Select: sel}, nil
}

// parseDropMatView 解析 DROP MATERIALIZED VIEW [IF EXISTS] <name>（M15）。
func (p *parser) parseDropMatView() (Stmt, error) {
	if err := p.expectKeyword("MATERIALIZED"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	s := &DropMatViewStmt{}
	if p.cur().kind == tokKeyword && p.cur().text == "IF" {
		if err := p.expectKeyword("IF"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		s.IfExists = true
	}
	nameTok, err := p.expect(tokIdent, "materialized view name")
	if err != nil {
		return nil, err
	}
	s.Name = nameTok.text
	return s, nil
}

// parseRefreshMatView 解析 REFRESH MATERIALIZED VIEW <name> [CONCURRENTLY]（M15）。
// CONCURRENTLY 为兼容语法，语义忽略（实现始终全量重算）。
func (p *parser) parseRefreshMatView() (Stmt, error) {
	if err := p.expectKeyword("REFRESH"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("MATERIALIZED"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	s := &RefreshMatViewStmt{}
	// CONCURRENTLY 兼容标准位置（名称前）：REFRESH MATERIALIZED VIEW CONCURRENTLY <name>
	if p.cur().kind == tokKeyword && p.cur().text == "CONCURRENTLY" {
		p.next()
		s.Concurrently = true
	}
	nameTok, err := p.expect(tokIdent, "materialized view name")
	if err != nil {
		return nil, err
	}
	s.Name = nameTok.text
	// 亦兼容名称后的 CONCURRENTLY 写法
	if p.cur().kind == tokKeyword && p.cur().text == "CONCURRENTLY" {
		p.next()
		s.Concurrently = true
	}
	return s, nil
}

// parseCreateIndex CREATE [UNIQUE] INDEX [idx] ON tbl (col)
func (p *parser) parseCreateIndex() (Stmt, error) {
	unique := false
	if p.cur().kind == tokKeyword && p.cur().text == "UNIQUE" {
		p.next()
		unique = true
	}
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
	return &CreateIndexStmt{Name: nameTok.text, Table: tableTok.text, Col: colTok.text, Unique: unique}, nil
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
	return p.parseInsertInto(false)
}

// parseInsertInto 解析 INSERT [INTO] / REPLACE [INTO] 的公共部分；
// replace=true 时按 REPLACE INTO 语义（冲突删旧插新，不支持 ON DUPLICATE 子句）。
func (p *parser) parseInsertInto(replace bool) (Stmt, error) {
	if replace {
		if err := p.expectKeyword("REPLACE"); err != nil {
			return nil, err
		}
	} else {
		if err := p.expectKeyword("INSERT"); err != nil {
			return nil, err
		}
	}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tokIdent, "table name")
	if err != nil {
		return nil, err
	}
	stmt := &InsertStmt{Table: nameTok.text, Replace: replace}
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
	// M10 UPSERT：INSERT ... ON DUPLICATE KEY UPDATE col=val[, ...]
	// （REPLACE 无此子句；无唯一/主键冲突时等价普通 INSERT）
	if !replace && p.cur().kind == tokKeyword && p.cur().text == "ON" {
		p.next()
		if err := p.expectKeyword("DUPLICATE"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("KEY"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("UPDATE"); err != nil {
			return nil, err
		}
		sets, err := p.parseSetItems()
		if err != nil {
			return nil, err
		}
		stmt.OnDup = &OnDuplicateUpdate{Sets: sets}
	}
	return stmt, nil
}

// parseSetItems 解析逗号分隔的 SET 赋值项列表（col=value）。
func (p *parser) parseSetItems() ([]SetItem, error) {
	var sets []SetItem
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
		sets = append(sets, SetItem{Col: colTok.text, Val: v})
		if p.cur().kind == tokComma {
			p.next()
			continue
		}
		break
	}
	return sets, nil
}

func (p *parser) parseSelect() (Stmt, error) {
	stmt := &SelectStmt{Limit: 0}
	// M10 CTE：WITH name AS (SELECT ...) [, ...] 前缀（可嵌套，子查询 FROM 亦可带 WITH）
	if p.cur().kind == tokKeyword && p.cur().text == "WITH" {
		p.next()
		for {
			nameTok, err := p.expect(tokIdent, "CTE name")
			if err != nil {
				return nil, err
			}
			if err := p.expectKeyword("AS"); err != nil {
				return nil, err
			}
			if _, err := p.expect(tokLParen, "("); err != nil {
				return nil, err
			}
			sub, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			subSel, ok := sub.(*SelectStmt)
			if !ok {
				return nil, errf("invalid CTE definition")
			}
			if _, err := p.expect(tokRParen, ")"); err != nil {
				return nil, err
			}
			stmt.CTEs = append(stmt.CTEs, CTE{Name: nameTok.text, Select: subSel})
			if p.cur().kind == tokComma {
				p.next()
				continue
			}
			break
		}
	}
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
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
	// LIMIT [OFFSET n]（M12-2 支持 OFFSET 分页：LIMIT n OFFSET m）
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
		if p.cur().kind == tokKeyword && p.cur().text == "OFFSET" {
			p.next()
			mTok, err := p.expect(tokNumber, "offset number")
			if err != nil {
				return nil, err
			}
			m, err := parseInt10(mTok.text)
			if err != nil {
				return nil, err
			}
			stmt.Offset = int(m)
		}
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
	// 聚合/窗口函数只允许出现在 SELECT 列表；WHERE/GROUP BY/ORDER BY 等位置显式报错
	if p.cur().kind == tokLParen {
		switch strings.ToUpper(tok.text) {
		case "COUNT", "SUM", "AVG", "ROW_NUMBER", "RANK", "DENSE_RANK":
			return "", "", errf("function %s() only allowed in SELECT list", tok.text)
		}
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
		// 聚合/窗口函数：COUNT/SUM/AVG/ROW_NUMBER/RANK/DENSE_RANK(...)
		if p.cur().kind == tokLParen {
			upper := strings.ToUpper(first)
			if upper != "COUNT" && upper != "SUM" && upper != "AVG" && upper != "ROW_NUMBER" && upper != "RANK" && upper != "DENSE_RANK" {
				return SelectColumn{}, errf("function %q not supported", first)
			}
			// 无参排名窗口函数：ROW_NUMBER() / RANK() / DENSE_RANK()，必须带 OVER
			if upper == "ROW_NUMBER" || upper == "RANK" || upper == "DENSE_RANK" {
				p.next() // (
				if _, err := p.expect(tokRParen, ")"); err != nil {
					return SelectColumn{}, err
				}
				sc := SelectColumn{Raw: upper + "()", Agg: upper, Name: upper + "()"}
				if p.cur().kind == tokKeyword && p.cur().text == "OVER" {
					if err := p.parseWindowSpec(&sc); err != nil {
						return SelectColumn{}, err
					}
				} else {
					return SelectColumn{}, errf("window function %s() requires OVER clause", upper)
				}
				if err := p.parseSelectAlias(&sc); err != nil {
					return SelectColumn{}, err
				}
				return sc, nil
			}
			p.next() // (
			if upper == "COUNT" && p.cur().kind == tokStar {
				p.next()
				if _, err := p.expect(tokRParen, ")"); err != nil {
					return SelectColumn{}, err
				}
				sc := SelectColumn{Raw: "COUNT(*)", Agg: "COUNT", Name: "COUNT(*)"}
				if p.cur().kind == tokKeyword && p.cur().text == "OVER" {
					if err := p.parseWindowSpec(&sc); err != nil {
						return SelectColumn{}, err
					}
				}
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
			if p.cur().kind == tokKeyword && p.cur().text == "OVER" {
				if err := p.parseWindowSpec(&sc); err != nil {
					return SelectColumn{}, err
				}
			}
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

// parseWindowSpec 解析 OVER 子句（M10 窗口函数）：
// OVER ( [PARTITION BY col [, col ...]] [ORDER BY col [ASC|DESC]] )
// 无显式框架，默认为 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW。
func (p *parser) parseWindowSpec(sc *SelectColumn) error {
	if err := p.expectKeyword("OVER"); err != nil {
		return err
	}
	if p.cur().kind != tokLParen {
		return errf("expected ( after OVER at %d, got %q", p.cur().pos, p.cur().text)
	}
	p.next()
	ws := &WindowSpec{}
	if p.cur().kind == tokKeyword && p.cur().text == "PARTITION" {
		if err := p.expectKeyword("PARTITION"); err != nil {
			return err
		}
		if err := p.expectKeyword("BY"); err != nil {
			return err
		}
		for {
			ref, col, err := p.parseColumnRef()
			if err != nil {
				return err
			}
			ws.PartitionBy = append(ws.PartitionBy, qualifiedName(ref, col))
			if p.cur().kind == tokComma {
				p.next()
				continue
			}
			break
		}
	}
	if p.cur().kind == tokKeyword && p.cur().text == "ORDER" {
		if err := p.expectKeyword("ORDER"); err != nil {
			return err
		}
		if err := p.expectKeyword("BY"); err != nil {
			return err
		}
		ref, col, err := p.parseColumnRef()
		if err != nil {
			return err
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
		ws.OrderBy = ob
	}
	if _, err := p.expect(tokRParen, ")"); err != nil {
		return err
	}
	sc.Window = ws
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
	// M12-3 分布式子查询：EXISTS (SELECT ...) 独立条件（无左操作数）。
	if p.cur().kind == tokKeyword && p.cur().text == "EXISTS" {
		p.next()
		if _, err := p.expect(tokLParen, "("); err != nil {
			return c, err
		}
		if p.cur().kind != tokKeyword || p.cur().text != "SELECT" {
			return c, errf("expected SELECT in EXISTS at %d, got %q", p.cur().pos, p.cur().text)
		}
		sub, err := p.parseSelect()
		if err != nil {
			return c, err
		}
		subSel, ok := sub.(*SelectStmt)
		if !ok {
			return c, errf("invalid subquery in EXISTS")
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return c, err
		}
		c.Op = "EXISTS"
		c.Sub = subSel
		return c, nil
	}
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
	// 右值：标量子查询 (SELECT ...) / 列引用（JOIN ON 场景）/ 字面量
	// M12-3 标量子查询：比较运算符右侧出现 "(" 且后随 SELECT 时解析为子查询。
	if p.cur().kind == tokLParen && p.pos+1 < len(p.toks) &&
		p.toks[p.pos+1].kind == tokKeyword && p.toks[p.pos+1].text == "SELECT" {
		p.next() // 吃掉 (
		sub, err := p.parseSelect()
		if err != nil {
			return c, err
		}
		subSel, ok := sub.(*SelectStmt)
		if !ok {
			return c, errf("invalid subquery in scalar comparison")
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return c, err
		}
		c.Sub = subSel
		return c, nil
	}
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
	sets, err := p.parseSetItems()
	if err != nil {
		return nil, err
	}
	stmt.Sets = sets
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
	case "VIEWS":
		p.next()
		return &ShowViewsStmt{}, nil
	case "TRIGGERS":
		// M15-2：SHOW TRIGGERS [FROM <table>]
		p.next()
		table := ""
		if p.cur().kind == tokKeyword && p.cur().text == "FROM" {
			p.next()
			nameTok, err := p.expect(tokIdent, "table name")
			if err != nil {
				return nil, err
			}
			table = nameTok.text
		}
		return &ShowTriggersStmt{Table: table}, nil
	case "PROCEDURES":
		// M15-3：SHOW PROCEDURES
		p.next()
		return &ShowProceduresStmt{}, nil
	}
	// SHOW USERS：USERS 不在关键字表（避免与表名 users 冲突），按标识符文本匹配。
	if p.cur().kind == tokIdent && strings.EqualFold(p.cur().text, "USERS") {
		p.next()
		return &ShowUsersStmt{}, nil
	}
	// M16-1：SHOW CONFIG / SHOW PERSISTED CONFIG（CONFIG/PERSISTED 不在关键字表，
	// 按标识符文本匹配，避免与表名 config 冲突）。
	if p.cur().kind == tokIdent && strings.EqualFold(p.cur().text, "CONFIG") {
		p.next()
		return &ShowConfigStmt{}, nil
	}
	if p.cur().kind == tokIdent && strings.EqualFold(p.cur().text, "PERSISTED") {
		p.next()
		if !(p.cur().kind == tokIdent && strings.EqualFold(p.cur().text, "CONFIG")) {
			return nil, errf("expected keyword CONFIG at %d, got %q", p.pos, p.cur().text)
		}
		p.next()
		return &ShowConfigStmt{Persisted: true}, nil
	}
	// SHOW BINLOG EVENTS [FROM <lsn>] [LIMIT <n>]：BINLOG/EVENTS 不在关键字表。
	if p.cur().kind == tokIdent && strings.EqualFold(p.cur().text, "BINLOG") {
		p.next()
		if !(p.cur().kind == tokIdent && strings.EqualFold(p.cur().text, "EVENTS")) {
			return nil, errf("expected keyword EVENTS at %d, got %q", p.pos, p.cur().text)
		}
		p.next()
		stmt := &ShowBinlogEventsStmt{}
		if p.cur().kind == tokKeyword && p.cur().text == "FROM" {
			p.next()
			lsnTok, err := p.expect(tokNumber, "lsn number")
			if err != nil {
				return nil, err
			}
			lsn, err := strconv.ParseUint(lsnTok.text, 10, 64)
			if err != nil {
				return nil, errf("invalid lsn %q", lsnTok.text)
			}
			stmt.From = &lsn
		}
		if p.cur().kind == tokKeyword && p.cur().text == "LIMIT" {
			p.next()
			nTok, err := p.expect(tokNumber, "limit number")
			if err != nil {
				return nil, err
			}
			n, err := strconv.Atoi(nTok.text)
			if err != nil {
				return nil, errf("invalid limit %q", nTok.text)
			}
			stmt.Limit = n
		}
		return stmt, nil
	}
	return nil, errf("expected TABLES, INDEX, SLOWQUERIES, NODES, REGION ROUTES, VIEWS or USERS after SHOW at %d, got %q", p.cur().pos, p.cur().text)
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

// parseImport IMPORT MYSQLDUMP FROM 'path' [BATCH n] [IGNORE ERRORS] 或 IMPORT INTO t FROM 'path' [BATCH n] [IGNORE ERRORS]
func (p *parser) parseImport() (Stmt, error) {
	if err := p.expectKeyword("IMPORT"); err != nil {
		return nil, err
	}
	stmt := &ImportStmt{}
	// M14：IMPORT MYSQLDUMP FROM 'path' —— mysqldump 兼容导入（解析 CREATE TABLE/INSERT 灌入）
	if p.cur().kind == tokIdent && strings.EqualFold(p.cur().text, "MYSQLDUMP") {
		p.next()
		stmt.Format = "MYSQLDUMP"
		if err := p.expectKeyword("FROM"); err != nil {
			return nil, err
		}
		pathTok, err := p.expect(tokString, "file path string")
		if err != nil {
			return nil, err
		}
		stmt.Path = pathTok.text
	} else {
		// 旧语法：IMPORT INTO t FROM 'path'
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
		stmt.Table = nameTok.text
		stmt.Path = pathTok.text
	}
	// 可选后缀：BATCH <n>（逐批提交行数）/ IGNORE ERRORS（坏行计数跳过）
	for p.cur().kind == tokIdent {
		switch strings.ToUpper(p.cur().text) {
		case "BATCH":
			p.next()
			nTok, err := p.expect(tokNumber, "batch row count")
			if err != nil {
				return nil, err
			}
			n, err := strconv.ParseInt(nTok.text, 10, 64)
			if err != nil || n <= 0 {
				return nil, &SQLError{Msg: "invalid BATCH count: " + nTok.text}
			}
			stmt.Batch = int(n)
		case "IGNORE":
			p.next()
			if p.cur().kind != tokIdent || !strings.EqualFold(p.cur().text, "ERRORS") {
				return nil, &SQLError{Msg: "expected keyword ERRORS after IGNORE"}
			}
			p.next()
			stmt.IgnoreErrors = true
		default:
			return nil, &SQLError{Msg: "unexpected token after IMPORT: " + p.cur().text}
		}
	}
	return stmt, nil
}

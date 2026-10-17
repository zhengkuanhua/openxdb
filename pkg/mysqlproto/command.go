package mysqlproto

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// MySQL 命令常量。
const (
	comQuit             = 0x01
	comInitDB           = 0x02
	comQuery            = 0x03
	comFieldList        = 0x04
	comPing             = 0x0e
	comChangeUser       = 0x11
	comStmtPrepare      = 0x16
	comStmtExecute      = 0x17
	comStmtSendLongData = 0x18
	comStmtClose        = 0x19
	comStmtReset        = 0x1a
	comSetOption        = 0x1b
)

// mysqlConn 单个 MySQL 协议连接：持有独立 SQL 会话与预处理语句表。
type mysqlConn struct {
	pc     *packetConn
	engine *sql.Engine
	user   string
	sess   *sql.Session

	stmts      map[uint32]string // stmt id → SQL 文本
	stmtTypes  map[uint32][]byte  // stmt id → PREPARE 时登记的参数类型（每参数 2 字节）
	nextStmtID uint32
	inTx       bool
	autoCommit bool // MySQL 兼容：SET autocommit=0 后进入隐式事务模式
}

// serveCommands 进入命令循环，直到连接关闭或致命错误。
// 返回值决定调用方是否打印错误日志。
func (c *mysqlConn) serveCommands() error {
	for {
		payload, err := c.pc.readPacket()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if len(payload) == 0 {
			continue
		}
		cmd := payload[0]
		switch cmd {
		case comQuit:
			return nil
		case comPing:
			if err := c.pc.writePacket(okPacket(0, 0, c.status())); err != nil {
				return err
			}
		case comInitDB:
			// OpenXDB 无 schema 概念：接受并忽略（SELECT database() 等由 SQL 层处理）。
			if err := c.pc.writePacket(okPacket(0, 0, c.status())); err != nil {
				return err
			}
		case comQuery:
			if err := c.handleQuery(string(payload[1:])); err != nil {
				return err
			}
		case comFieldList:
			// 空列定义列表（客户端少见依赖该命令）。
			if err := c.pc.writePacket(eofPacket(c.status())); err != nil {
				return err
			}
		case comStmtPrepare:
			if err := c.handleStmtPrepare(string(payload[1:])); err != nil {
				return err
			}
		case comStmtExecute:
			if err := c.handleStmtExecute(payload[1:]); err != nil {
				return err
			}
		case comStmtClose:
			c.handleStmtClose(payload[1:])
		case comStmtReset:
			c.handleStmtReset(payload[1:])
		case comStmtSendLongData:
			// 长数据暂不支持：忽略（后续 EXECUTE 不带该参数）。
		case comSetOption:
			if err := c.pc.writePacket(eofPacket(c.status())); err != nil {
				return err
			}
		case comChangeUser:
			if err := c.pc.writePacket(errPacket(fmt.Errorf("OpenXDB: COM_CHANGE_USER not supported"))); err != nil {
				return err
			}
		default:
			if err := c.pc.writePacket(errPacket(fmt.Errorf("OpenXDB: unknown command 0x%02x", cmd))); err != nil {
				return err
			}
		}
	}
}

// execTrace 执行 SQL；OPENXDB_DEBUG_WIRE 开启时打印 SQL 与耗时，用于协议联调排障。
func (c *mysqlConn) execTrace(sqlText string) (*sql.Result, error) {
	if os.Getenv("OPENXDB_DEBUG_WIRE") != "" {
		startT := time.Now()
		res, err := c.engine.ExecuteAsSession(sqlText, c.user, c.sess)
		log.Printf("[openxdb-mysql] QUERY %s cost=%v err=%v", sqlText, time.Since(startT), err)
		return res, err
	}
	return c.engine.ExecuteAsSession(sqlText, c.user, c.sess)
}

// handleQuery 执行 COM_QUERY：有列则文本结果集，否则 OK 包。
func (c *mysqlConn) handleQuery(query string) error {
	// Connector/J 连接初始化探测：SELECT @@session.xxx AS xxx, ...
	// lexer 不支持 @@ 双 @ 系统变量，直接拦截返回固定兼容值（T41）。
	// Connector/J 初始化探测：只读状态查询返回 0/0，其余系统变量探测返回固定 18 列。
	if isTxReadOnlyProbe(query) {
		return c.writeTxReadOnlyProbeResult()
	}
	if isSysVarProbe(query) {
		return c.writeSysVarProbeResult()
	}
	// Connector/J 初始化还发 "select @@version_comment limit 1"，单列探测，固定兼容值响应。
	if isVersionCommentProbe(query) {
		return c.writeVersionCommentResult()
	}
	// Connector/J 连接初始化阶段的无害 SET（字符集/autocommit/sql_mode 等），
	// OpenXDB 内部统一 UTF-8 且事务语义自洽，直接 OK（T41）。
	if isConnInitSet(query) {
		if v, ok := parseAutoCommit(query); ok {
			c.autoCommit = v == 1
			// autocommit 从 0 切回 1：MySQL 语义为提交当前隐式事务。
			if c.autoCommit && c.inTx {
				if _, err := c.engine.ExecuteAsSession("COMMIT", c.user, c.sess); err == nil {
					c.inTx = false
				}
			}
		}
		return c.pc.writePacket(okPacket(0, 0, c.status()))
	}
	// MySQL autocommit=0 隐式事务：DML/DDL 前自动开启会话事务。
	if !c.autoCommit && !c.inTx && isImplicitTxTrigger(query) {
		if _, err := c.engine.ExecuteAsSession("BEGIN", c.user, c.sess); err != nil {
			return c.pc.writePacket(errPacket(err))
		}
		c.inTx = true
	}
	res, err := c.execTrace(query)
	if err != nil {
		// 调试：解析/执行失败时打印原始 SQL，便于定位驱动发送的异常语句。
		log.Printf("[openxdb-mysql] query failed: %v; sql=%q", err, query)
		// autocommit=0 下 COMMIT/ROLLBACK 无活动事务时幂等成功（MySQL 语义）。
		if !c.autoCommit && isTxnControlStmt(query) && strings.Contains(err.Error(), "no active transaction") {
			return c.pc.writePacket(okPacket(0, 0, c.status()))
		}
		return c.pc.writePacket(errPacket(err))
	}
	c.syncTxnFlag(query)
	if res == nil {
		return c.pc.writePacket(okPacket(0, 0, c.status()))
	}
	if len(res.Columns) > 0 {
		rows := make([][][]byte, 0, len(res.Rows))
		for _, r := range res.Rows {
			row := make([][]byte, 0, len(r))
			for _, v := range r {
				row = append(row, []byte(v.String()))
			}
			rows = append(rows, row)
		}
		return c.writeResultSet(res.Columns, rows)
	}
	return c.pc.writePacket(okPacket(uint64(res.AffectedRows), 0, c.status()))
}

// handleStmtPrepare COM_STMT_PREPARE：登记 SQL 文本并返回 PREPARE OK。
func (c *mysqlConn) handleStmtPrepare(sqlText string) error {
	c.nextStmtID++
	id := c.nextStmtID
	numParams := countPlaceholders(sqlText)
	c.stmts[id] = sqlText
	// PREPARE OK 参数列统一声明 VAR_STRING（0xfd）；登记类型供 EXECUTE 沿用。
	types := make([]byte, 0, numParams*2)
	for i := 0; i < numParams; i++ {
		types = append(types, mysqlTypeVarString, 0x00)
	}
	c.stmtTypes[id] = types
	return c.writePrepareOK(id, 0, numParams)
}

// handleStmtClose COM_STMT_CLOSE：删除本地登记，无响应。
func (c *mysqlConn) handleStmtClose(p []byte) {
	if len(p) < 4 {
		return
	}
	id := binary.LittleEndian.Uint32(p[0:4])
	delete(c.stmts, id)
}

// handleStmtReset COM_STMT_RESET：重置语句（本地无状态，直接 OK）。
func (c *mysqlConn) handleStmtReset(p []byte) error {
	if len(p) < 4 {
		return nil
	}
	_ = binary.LittleEndian.Uint32(p[0:4])
	return c.pc.writePacket(okPacket(0, 0, c.status()))
}

// handleStmtExecute COM_STMT_EXECUTE：解析二进制参数、展开占位符并执行。
func (c *mysqlConn) handleStmtExecute(p []byte) error {
	if len(p) < 9 {
		return c.pc.writePacket(errPacket(fmt.Errorf("OpenXDB: malformed COM_STMT_EXECUTE")))
	}
	id := binary.LittleEndian.Uint32(p[0:4])
	sqlText, ok := c.stmts[id]
	if !ok {
		return c.pc.writePacket(errPacket(fmt.Errorf("OpenXDB: statement %d not found", id)))
	}
	if os.Getenv("OPENXDB_DEBUG_WIRE") != "" {
		fmt.Printf("[wire] EXECUTE id=%d len=%d hex=%x\n", id, len(p), p)
	}
	off := 9 // stmt_id 4 + flags 1 + iteration_count 4
	numParams := countPlaceholders(sqlText)
	if numParams > 0 {
		if off+(numParams+7)/8+1 > len(p) {
			return c.pc.writePacket(errPacket(fmt.Errorf("OpenXDB: malformed COM_STMT_EXECUTE params")))
		}
		nullBitmap := p[off : off+(numParams+7)/8]
		off += (numParams + 7) / 8
		newBound := p[off]
		off++
		var types []byte
		if newBound&0x01 != 0 {
			// 客户端重发了参数类型：记录之，供后续 EXECUTE 复用。
			if off+numParams*2 > len(p) {
				return c.pc.writePacket(errPacket(fmt.Errorf("OpenXDB: malformed COM_STMT_EXECUTE types")))
			}
			types = p[off : off+numParams*2]
			c.stmtTypes[id] = types
			off += numParams * 2
		} else {
			// 未重发类型：沿用上次客户端发送的类型。
			types = c.stmtTypes[id]
			if len(types) < numParams*2 {
				return c.pc.writePacket(errPacket(fmt.Errorf("OpenXDB: statement %d types not registered", id)))
			}
		}
		args, err := parseBinaryParams(p, off, numParams, nullBitmap, types)
		if err != nil {
			if os.Getenv("OPENXDB_DEBUG_WIRE") != "" {
				fmt.Printf("[wire] EXECUTE FAIL id=%d sql=%s hex=%x err=%v\n", id, sqlText, p, err)
			}
			return c.pc.writePacket(errPacket(err))
		}
		sqlText = expandPlaceholders(sqlText, args)
	}
	// MySQL autocommit=0 隐式事务：DML/DDL 前自动开启会话事务。
	if !c.autoCommit && !c.inTx && isImplicitTxTrigger(sqlText) {
		if _, err := c.engine.ExecuteAsSession("BEGIN", c.user, c.sess); err != nil {
			return c.pc.writePacket(errPacket(err))
		}
		c.inTx = true
	}
	res, err := c.execTrace(sqlText)
	if err != nil {
		// autocommit=0 下 COMMIT/ROLLBACK 无活动事务时幂等成功（MySQL 语义）。
		if !c.autoCommit && isTxnControlStmt(sqlText) && strings.Contains(err.Error(), "no active transaction") {
			return c.pc.writePacket(okPacket(0, 0, c.status()))
		}
		return c.pc.writePacket(errPacket(err))
	}
	c.syncTxnFlag(sqlText)
	if res == nil {
		return c.pc.writePacket(okPacket(0, 0, c.status()))
	}
	if len(res.Columns) > 0 {
		return c.writeBinaryResultSet(res.Columns, res.Rows)
	}
	return c.pc.writePacket(okPacket(uint64(res.AffectedRows), 0, c.status()))
}

// ---- 参数解析与占位符展开 ----

// parseBinaryParams 解析 COM_STMT_EXECUTE 二进制参数值为 SQL 文本字面量。
func parseBinaryParams(p []byte, off, n int, nullBitmap, types []byte) ([]string, error) {
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if nullBitmap[i/8]&(1<<(uint(i)%8)) != 0 {
			args = append(args, "NULL")
			continue
		}
		typ := types[i*2]
		unsigned := types[i*2+1] == 0x80
		switch {
		case typ == 1 || typ == 2 || typ == 3 || typ == 8 || typ == 9 || typ == 13 || typ == 16:
			// TINYINT/SMALLINT/INT/LONGLONG/MEDIUMINT/YEAR/BIT
			size := 1
			switch typ {
			case 1, 16:
				size = 1
			case 2, 13:
				size = 2
			case 3, 9:
				size = 4
			case 8:
				size = 8
			}
			if off+size > len(p) {
				return nil, fmt.Errorf("OpenXDB: param %d truncated", i)
			}
			var v int64
			if size == 8 {
				v = int64(binary.LittleEndian.Uint64(p[off : off+8]))
			} else {
				// 小端低位在前：原字节放低 size 字节，高位补零。
				b := make([]byte, 8)
				copy(b, p[off:off+size])
				v = int64(binary.LittleEndian.Uint64(b))
				// 符号扩展
				if !unsigned && size < 8 && p[off+size-1]&0x80 != 0 {
					v -= 1 << (size * 8)
				}
			}
			off += size
			if unsigned && v < 0 {
				args = append(args, fmt.Sprintf("%d", uint64(v)))
			} else {
				args = append(args, fmt.Sprintf("%d", v))
			}
		case typ == 4 || typ == 5:
			// FLOAT/DOUBLE
			size := 4
			if typ == 5 {
				size = 8
			}
			if off+size > len(p) {
				return nil, fmt.Errorf("OpenXDB: param %d truncated", i)
			}
			if size == 4 {
				args = append(args, fmt.Sprintf("%g", float64(math.Float32frombits(binary.LittleEndian.Uint32(p[off:off+4])))))
			} else {
				args = append(args, fmt.Sprintf("%g", math.Float64frombits(binary.LittleEndian.Uint64(p[off:off+8]))))
			}
			off += size
		case typ == 15 || typ == 245 || typ == 246 || typ == 252 || typ == 253 || typ == 254:
			// STRING/VARCHAR/BLOB/DECIMAL：lenenc 字符串。
			b, n2, err := readLenencBytes(p, off)
			if err != nil {
				return nil, fmt.Errorf("OpenXDB: param %d string truncated", i)
			}
			off = n2
			args = append(args, quoteString(string(b)))
		case typ == 10 || typ == 7 || typ == 12:
			// DATE/DATETIME/TIMESTAMP：二进制日期结构。
			lit, n2, err := parseBinaryDate(p, off, int(typ))
			if err != nil {
				return nil, err
			}
			off = n2
			args = append(args, lit)
		default:
			return nil, fmt.Errorf("OpenXDB: unsupported param type %d", typ)
		}
	}
	return args, nil
}

// parseBinaryDate 解析二进制日期结构为 SQL 字面量。
// 长度字段：0（date=0）；4（date）；7（datetime/timestamp 无微秒）；11（含微秒）。
func parseBinaryDate(p []byte, off, typ int) (string, int, error) {
	if off >= len(p) {
		return "", off, fmt.Errorf("OpenXDB: date param truncated")
	}
	length := int(p[off])
	off++
	if length == 0 {
		return "NULL", off, nil
	}
	if off+length > len(p) {
		return "", off, fmt.Errorf("OpenXDB: date param truncated")
	}
	if typ == 10 { // DATE
		year := int(p[off]) | int(p[off+1])<<8
		month := int(p[off+2])
		day := int(p[off+3])
		return fmt.Sprintf("'%04d-%02d-%02d'", year, month, day), off + 4, nil
	}
	// DATETIME/TIMESTAMP
	year := int(p[off]) | int(p[off+1])<<8
	month := int(p[off+2])
	day := int(p[off+3])
	hour := int(p[off+4])
	min := int(p[off+5])
	sec := int(p[off+6])
	s := fmt.Sprintf("'%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec)
	if length == 11 {
		micro := int(p[off+7]) | int(p[off+8])<<8 | int(p[off+9])<<16 | int(p[off+10])<<24
		s += fmt.Sprintf(".%06d", micro)
	}
	return s + "'", off + length, nil
}

// quoteString 转义字符串为 SQL 单引号字面量。
func quoteString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)
	return "'" + s + "'"
}

// countPlaceholders 统计 SQL 中引号外的 ? 数量（单引号字符串内跳过，” 转义支持）。
func countPlaceholders(sqlText string) int {
	n := 0
	inQuote := false
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		if inQuote {
			if ch == '\\' {
				i++
				continue
			}
			if ch == '\'' {
				if i+1 < len(sqlText) && sqlText[i+1] == '\'' {
					i++
					continue
				}
				inQuote = false
			}
			continue
		}
		switch ch {
		case '\'':
			inQuote = true
		case '?':
			n++
		}
	}
	return n
}

// expandPlaceholders 将 SQL 中引号外的 ? 依次替换为参数字面量。
func expandPlaceholders(sqlText string, args []string) string {
	if len(args) == 0 {
		return sqlText
	}
	var sb strings.Builder
	argIdx := 0
	inQuote := false
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		if inQuote {
			sb.WriteByte(ch)
			if ch == '\\' && i+1 < len(sqlText) {
				i++
				sb.WriteByte(sqlText[i])
				continue
			}
			if ch == '\'' {
				if i+1 < len(sqlText) && sqlText[i+1] == '\'' {
					i++
					sb.WriteByte(sqlText[i])
					continue
				}
				inQuote = false
			}
			continue
		}
		switch ch {
		case '\'':
			inQuote = true
			sb.WriteByte(ch)
		case '?':
			if argIdx < len(args) {
				sb.WriteString(args[argIdx])
				argIdx++
			} else {
				sb.WriteByte('?')
			}
		default:
			sb.WriteByte(ch)
		}
	}
	return sb.String()
}

// isSysVarProbe 判断是否为 Connector/J 连接初始化的系统变量探测查询：
// 形如 SELECT @@session.xxx AS xxx, ...（可能带 /* mysql-connector... */ 注释前缀）。
// 这类查询仅用于驱动探测服务器变量，OpenXDB 用固定兼容值响应。
func isSysVarProbe(query string) bool {
	up := strings.ToUpper(query)
	// 跳过前导块注释（如 Connector/J 版本注释）。
	for {
		idx := strings.Index(up, "/*")
		if idx != 0 {
			break
		}
		end := strings.Index(up[idx+2:], "*/")
		if end < 0 {
			return false
		}
		up = strings.TrimSpace(up[idx+2+end+2:])
	}
	up = strings.ReplaceAll(up, " ", "")
	return strings.HasPrefix(up, "SELECT@@SESSION.") || strings.HasPrefix(up, "SELECT@@GLOBAL.")
}

// isVersionCommentProbe 判断是否为 Connector/J 初始化发的 "select @@version_comment limit 1"。
func isVersionCommentProbe(query string) bool {
	up := strings.ToUpper(strings.ReplaceAll(query, " ", ""))
	return strings.HasPrefix(up, "SELECT@@VERSION_COMMENT")
}

// writeVersionCommentResult 响应 @@version_comment 探测（单列单行）。
func (c *mysqlConn) writeVersionCommentResult() error {
	return c.writeResultSet([]string{"version_comment"}, [][][]byte{{[]byte("OpenXDB v1.0.0 (RocksDB)")}})
}

// connInitSetKeys Connector/J 连接初始化阶段可能发送的无害 SET 变量白名单。
// 命中即视为连接级初始化，直接 OK；其余 SET 仍走 SQL 引擎（如 query_cache 等）。
var connInitSetKeys = []string{
	"NAMES", "CHARACTER_SET_CLIENT", "CHARACTER_SET_CONNECTION",
	"CHARACTER_SET_RESULTS", "CHARACTER_SET_SERVER", "COLLATION_CONNECTION",
	"COLLATION_SERVER", "AUTOCOMMIT", "SQL_MODE", "TIME_ZONE",
	"TX_ISOLATION", "TRANSACTION_ISOLATION", "WAIT_TIMEOUT",
	"INTERACTIVE_TIMEOUT", "NET_WRITE_TIMEOUT", "NET_READ_TIMEOUT",
	"MAX_ALLOWED_PACKET", "LOWER_CASE_TABLE_NAMES", "FOREIGN_KEY_CHECKS",
	"UNIQUE_CHECKS", "INNODB_STRICT_MODE", "SQL_SAFE_UPDATES",
	"INIT_CONNECT", "GROUP_CONCAT_MAX_LEN", "OPTIMIZER_SWITCH",
	"SQL_NOTES", "SQL_WARNINGS", "SESSION_TRACK_SCHEMA",
	"SESSION_TRACK_STATE_CHANGE", "SESSION_TRACK_TRANSACTION_INFO",
}

// isConnInitSet 判断是否为 Connector/J 连接初始化阶段的无害 SET 语句。
func isConnInitSet(query string) bool {
	up := strings.ToUpper(strings.TrimSpace(query))
	if !strings.HasPrefix(up, "SET ") {
		return false
	}
	rest := strings.TrimSpace(up[4:])
	// SET NAMES <charset> 无等号，直接命中。
	if strings.HasPrefix(rest, "NAMES ") || rest == "NAMES" {
		return true
	}
	// 支持逗号分隔多赋值（如 SET autocommit=1, sql_mode='...'）：逐个判断。
	for _, item := range strings.Split(rest, ",") {
		item = strings.TrimSpace(item)
		name := item
		if eq := strings.IndexByte(item, '='); eq >= 0 {
			name = strings.TrimSpace(item[:eq])
		}
		found := false
		for _, k := range connInitSetKeys {
			if name == k {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// txReadOnlyProbeColumns 只读状态探测查询的列（Connector/J 8.0.13 发送
// SELECT @@session.transaction_read_only AS transaction_read_only, @@session.tx_read_only AS tx_read_only）。
var txReadOnlyProbeColumns = []string{"transaction_read_only", "tx_read_only"}

// parseAutoCommit 解析 SET autocommit=<0|1>（容忍空格与大小写），返回 (值, 是否命中)。
func parseAutoCommit(query string) (int, bool) {
	up := strings.ToUpper(strings.TrimSpace(query))
	if !strings.HasPrefix(up, "SET ") {
		return 0, false
	}
	rest := strings.TrimSpace(up[4:])
	// 仅处理单赋值；多赋值里的 autocommit 由 isConnInitSet 吞掉，此处无法精确解析。
	if strings.Contains(rest, ",") {
		for _, item := range strings.Split(rest, ",") {
			item = strings.TrimSpace(item)
			if v, ok := parseSingleAutoCommit(item); ok {
				return v, ok
			}
		}
		return 0, false
	}
	return parseSingleAutoCommit(rest)
}

// parseSingleAutoCommit 解析单个 autocommit=0/1 赋值项。
func parseSingleAutoCommit(item string) (int, bool) {
	name := item
	if eq := strings.IndexByte(item, '='); eq >= 0 {
		name = strings.TrimSpace(item[:eq])
	}
	if name != "AUTOCOMMIT" {
		return 0, false
	}
	if eq := strings.IndexByte(item, '='); eq >= 0 {
		val := strings.TrimSpace(item[eq+1:])
		if val == "0" {
			return 0, true
		}
		if val == "1" {
			return 1, true
		}
	}
	return 0, false
}

// isTxnControlStmt 判断是否为事务控制语句（BEGIN/START TRANSACTION/COMMIT/ROLLBACK/END）。
func isTxnControlStmt(query string) bool {
	up := strings.ToUpper(strings.TrimSpace(query))
	for _, p := range []string{"BEGIN", "START TRANSACTION", "COMMIT", "ROLLBACK", "END"} {
		if up == p || strings.HasPrefix(up, p+" ") || strings.HasPrefix(up, p+"\t") || strings.HasPrefix(up, p+"\r") || strings.HasPrefix(up, p+"\n") {
			return true
		}
	}
	return false
}

// isTxnBegin 判断语句是否为开启事务的控制语句。
func isTxnBegin(query string) bool {
	up := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(up, "BEGIN ") || up == "BEGIN" || strings.HasPrefix(up, "START TRANSACTION")
}

// isImplicitTxTrigger 判断语句是否为需要隐式事务的写语句（MySQL autocommit=0 语义）。
func isImplicitTxTrigger(query string) bool {
	up := strings.ToUpper(strings.TrimSpace(query))
	for {
		// 跳过前导注释（/* */ 或 -- 或 #）。
		if strings.HasPrefix(up, "/*") {
			if end := strings.Index(up[2:], "*/"); end >= 0 {
				up = strings.TrimSpace(up[2+end+2:])
				continue
			}
			return false
		}
		if strings.HasPrefix(up, "--") || strings.HasPrefix(up, "#") {
			return false
		}
		break
	}
	for _, p := range []string{"INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME", "LOAD"} {
		if strings.HasPrefix(up, p) && (len(up) == len(p) || up[len(p)] == ' ' || up[len(p)] == '\t') {
			return true
		}
	}
	return false
}

// syncTxnFlag 语句执行成功后按类型同步协议层事务标志（executor 内部已维护 sess.inTx）。
func (c *mysqlConn) syncTxnFlag(query string) {
	if isTxnControlStmt(query) {
		if isTxnBegin(query) {
			c.inTx = true
		} else {
			c.inTx = false
		}
	}
}

// isTxReadOnlyProbe 判断是否为只读状态探测查询（跳过前导注释）。
func isTxReadOnlyProbe(query string) bool {
	up := strings.ToUpper(strings.TrimSpace(query))
	for {
		idx := strings.Index(up, "/*")
		if idx != 0 {
			break
		}
		end := strings.Index(up[idx+2:], "*/")
		if end < 0 {
			return false
		}
		up = strings.TrimSpace(up[idx+2+end+2:])
	}
	up = strings.ReplaceAll(up, " ", "")
	if !strings.HasPrefix(up, "SELECT@@SESSION.") && !strings.HasPrefix(up, "SELECT@@GLOBAL.") {
		return false
	}
	return strings.Contains(up, "TRANSACTION_READ_ONLY") || strings.Contains(up, "TX_READ_ONLY")
}

// writeTxReadOnlyProbeResult 写出只读状态探测结果：transaction_read_only=0, tx_read_only=0。
func (c *mysqlConn) writeTxReadOnlyProbeResult() error {
	row := [][]byte{[]byte("0"), []byte("0")}
	return c.writeResultSet(txReadOnlyProbeColumns, [][][]byte{row})
}

// sysVarProbeColumns 系统变量探测查询的固定列（与 Connector/J 8.0.13 初始化查询对应）。
var sysVarProbeColumns = []string{
	"auto_increment_increment", "character_set_client", "character_set_connection",
	"character_set_results", "character_set_server", "collation_server",
	"collation_connection", "init_connect", "interactive_timeout", "license",
	"lower_case_table_names", "max_allowed_packet", "net_write_timeout",
	"sql_mode", "system_time_zone", "time_zone", "transaction_isolation",
	"wait_timeout",
}

// writeSysVarProbeResult 写出系统变量探测的固定结果集（单行）。
func (c *mysqlConn) writeSysVarProbeResult() error {
	row := [][]byte{
		[]byte("1"), []byte("utf8mb4"), []byte("utf8mb4"), []byte("utf8mb4"),
		[]byte("utf8mb4"), []byte("utf8mb4_general_ci"), []byte("utf8mb4_general_ci"),
		[]byte(""), []byte("28800"), []byte("Apache-2.0"), []byte("0"),
		[]byte("4194304"), []byte("60"), []byte("STRICT_TRANS_TABLES"),
		[]byte("CST"), []byte("SYSTEM"), []byte("REPEATABLE-READ"), []byte("28800"),
	}
	return c.writeResultSet(sysVarProbeColumns, [][][]byte{row})
}

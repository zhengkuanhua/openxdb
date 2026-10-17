package mysqlproto

import (
	"encoding/binary"
	"fmt"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// MySQL 类型常量（column type）。
const (
	mysqlTypeLong       = 3
	mysqlTypeDate       = 10
	mysqlTypeLongLong   = 8
	mysqlTypeVarString  = 253
	mysqlTypeBlob       = 252
	mysqlTypeNewDecimal = 246
)

// charsetID utf8mb4（列字符集声明用）。
const charsetUTF8MB4 = 33

// okPacket 构造 OK 包（0x00 header）。
// affectedRows / lastInsertID 按 lenenc 编码；status 默认 autocommit。
func okPacket(affectedRows, lastInsertID uint64, status uint16) []byte {
	p := make([]byte, 0, 16)
	p = append(p, 0x00)
	p = appendLenencInt(p, affectedRows)
	p = appendLenencInt(p, lastInsertID)
	p = append(p, byte(status), byte(status>>8))
	p = append(p, 0x00, 0x00) // warnings
	return p
}

// errPacket 构造 ERR 包（0xff header）：SQLSTATE 'HY000'，message 为错误文本。
func errPacket(err error) []byte {
	msg := err.Error()
	p := make([]byte, 0, len(msg)+16)
	p = append(p, 0xff)
	var code [2]byte
	binary.LittleEndian.PutUint16(code[:], 1045) // ER_ACCESS_DENIED / 通用错误
	p = append(p, code[:]...)
	p = append(p, '#')
	p = append(p, "HY000"...)
	p = append(p, msg...)
	return p
}

// eofPacket 构造经典 EOF 包（0xfe header）。
func eofPacket(status uint16) []byte {
	p := make([]byte, 0, 5)
	p = append(p, 0xfe)
	p = append(p, 0x00, 0x00) // warnings
	p = append(p, byte(status), byte(status>>8))
	return p
}

// columnDefinition 构造 ColumnDefinition41。
func columnDefinition(name string, typ byte, charset uint16, length uint32, flags uint16, decimals byte) []byte {
	p := make([]byte, 0, len(name)+32)
	p = appendLenencBytes(p, []byte("def")) // catalog
	p = appendLenencBytes(p, nil)           // schema
	p = appendLenencBytes(p, nil)           // table
	p = appendLenencBytes(p, nil)           // org_table
	p = appendLenencBytes(p, []byte(name))  // name
	p = appendLenencBytes(p, nil)           // org_name
	p = append(p, 0x0c)                     // length of fixed fields
	var cs [2]byte
	binary.LittleEndian.PutUint16(cs[:], charset)
	p = append(p, cs[:]...)
	var cl [4]byte
	binary.LittleEndian.PutUint32(cl[:], length)
	p = append(p, cl[:]...)
	p = append(p, typ)
	var fl [2]byte
	binary.LittleEndian.PutUint16(fl[:], flags)
	p = append(p, fl[:]...)
	p = append(p, decimals)
	p = append(p, 0x00, 0x00) // filler
	return p
}

// textRow 构造文本协议数据行（每列 lenenc 字节串）。
func textRow(cols [][]byte) []byte {
	p := make([]byte, 0, 16)
	for _, c := range cols {
		p = appendLenencBytes(p, c)
	}
	return p
}

// binaryRow 构造二进制协议数据行（COM_STMT_EXECUTE 结果集行）。
// 包结构：header(0x00) + null bitmap((n+7+2)/8 字节) + 各非 NULL 列 lenenc 值。
// 第 i 列（i 从 0 起）为 NULL 时置位包内第 i+2 bit（含 header 字节起算）。
func binaryRow(values []sql.Value) []byte {
	p := make([]byte, 0, 16)
	// bitfield 首字节即 0x00 header，其后为 null bitmap。
	bitfield := make([]byte, 1+(len(values)+7+2)/8)
	for i, v := range values {
		if v.Kind == "NULL" {
			bitfield[(i+2)/8] |= 1 << (uint(i+2) % 8)
		}
	}
	p = append(p, bitfield...)
	for _, v := range values {
		if v.Kind == "NULL" {
			continue
		}
		p = appendLenencBytes(p, []byte(v.String()))
	}
	return p
}

// writeBinaryResultSet 写出 EXECUTE 二进制结果集：列数 → 每列定义 → EOF → 二进制行 → EOF。
func (c *mysqlConn) writeBinaryResultSet(columns []string, rows [][]sql.Value) error {
	p := make([]byte, 0, 4)
	p = appendLenencInt(p, uint64(len(columns)))
	if err := c.pc.writePacket(p); err != nil {
		return err
	}
	for _, col := range columns {
		if err := c.pc.writePacket(columnDefinition(col, mysqlTypeVarString, charsetUTF8MB4, 1024, 0, 0)); err != nil {
			return err
		}
	}
	if err := c.pc.writePacket(eofPacket(c.status())); err != nil {
		return err
	}
	for _, row := range rows {
		if err := c.pc.writePacket(binaryRow(row)); err != nil {
			return err
		}
	}
	return c.pc.writePacket(eofPacket(c.status()))
}

// writeResultSet 写出文本结果集：列数 → 每列定义 → EOF → 每行 → EOF。
// columns 为列名；rows 为每行各列已编码字节。
func (c *mysqlConn) writeResultSet(columns []string, rows [][][]byte) error {
	p := make([]byte, 0, 4)
	p = appendLenencInt(p, uint64(len(columns)))
	if err := c.pc.writePacket(p); err != nil {
		return err
	}
	for _, col := range columns {
		// 列类型推断：INT 由上层传入（OpenXDB Value 无列类型元数据，
		// 统一按字符串声明为 VAR_STRING，客户端仍可正确展示）。
		if err := c.pc.writePacket(columnDefinition(col, mysqlTypeVarString, charsetUTF8MB4, 1024, 0, 0)); err != nil {
			return err
		}
	}
	if err := c.pc.writePacket(eofPacket(c.status())); err != nil {
		return err
	}
	for _, row := range rows {
		if err := c.pc.writePacket(textRow(row)); err != nil {
			return err
		}
	}
	return c.pc.writePacket(eofPacket(c.status()))
}

// writePrepareOK 构造并写出 COM_STMT_PREPARE_OK（0x00 header）。
// numColumns / numParams 非零时附带对应列定义 + EOF。
func (c *mysqlConn) writePrepareOK(stmtID uint32, numColumns, numParams int) error {
	p := make([]byte, 0, 16)
	p = append(p, 0x00)
	var id [4]byte
	binary.LittleEndian.PutUint32(id[:], stmtID)
	p = append(p, id[:]...)
	p = append(p, byte(numColumns), byte(numColumns>>8))
	p = append(p, byte(numParams), byte(numParams>>8))
	p = append(p, 0x00)       // reserved
	p = append(p, 0x00, 0x00) // warnings
	if err := c.pc.writePacket(p); err != nil {
		return err
	}
	// 参数列定义（名称未知，用 "?" 占位）。
	for i := 0; i < numParams; i++ {
		if err := c.pc.writePacket(columnDefinition("?", mysqlTypeVarString, charsetUTF8MB4, 1024, 0, 0)); err != nil {
			return err
		}
	}
	if numParams > 0 {
		if err := c.pc.writePacket(eofPacket(c.status())); err != nil {
			return err
		}
	}
	// 结果列定义（PREPARE 阶段无法预知，输出空列集 + EOF 亦可被客户端接受；
	// 客户端在首次 EXECUTE 时通过真实结果集获取列信息）。
	if numColumns > 0 {
		// 占位空列：大多数驱动不依赖 PREPARE 时的列定义（EXECUTE 结果集为准）。
	}
	return nil
}

// status 返回当前连接状态标志（autocommit 恒真，显式事务时置 IN_TRANSACTION）。
func (c *mysqlConn) status() uint16 {
	if c.inTx {
		return statusAutocommit | statusInTransaction
	}
	return statusAutocommit
}

// notImplemented 返回未实现命令的 ERR 包（错误码 1235 ER_NOT_SUPPORTED_YET）。
func notImplemented(name string) []byte {
	msg := fmt.Sprintf("OpenXDB: command %s not supported yet", name)
	return errPacket(fmt.Errorf("%s", msg))
}

package mysqlproto

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// ---- 纯函数单元测试 ----

func TestCountPlaceholders(t *testing.T) {
	cases := []struct {
		sql  string
		want int
	}{
		{"SELECT * FROM t WHERE id = ?", 1},
		{"SELECT ? + ?", 2},
		{"SELECT 'a?b' AS x", 0},
		{"SELECT 'it''s ?' AS x", 0},
		{"SELECT 'a' , ?, 'b?c', ?", 2},
		{"SELECT 1", 0},
	}
	for _, c := range cases {
		if got := countPlaceholders(c.sql); got != c.want {
			t.Errorf("countPlaceholders(%q) = %d, want %d", c.sql, got, c.want)
		}
	}
}

func TestExpandPlaceholders(t *testing.T) {
	got := expandPlaceholders("SELECT * FROM t WHERE id = ? AND name = ?", []string{"7", "'alice'"})
	want := "SELECT * FROM t WHERE id = 7 AND name = 'alice'"
	if got != want {
		t.Errorf("expand = %q, want %q", got, want)
	}
}

func TestQuoteString(t *testing.T) {
	if got := quoteString("a'b\\c"); got != "'a''b\\\\c'" {
		t.Errorf("quoteString = %q", got)
	}
}

// ---- 集成测试：迷你 MySQL 客户端往返 ----

type testClient struct {
	t  *testing.T
	pc *packetConn
}

func dialClient(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testClient{t: t, pc: &packetConn{rw: conn}}
}

func (c *testClient) readGreeting() (scramble []byte) {
	c.t.Helper()
	p, err := c.pc.readPacket()
	if err != nil {
		c.t.Fatalf("read greeting: %v", err)
	}
	if p[0] != 0x0a {
		c.t.Fatalf("greeting protocol = %d, want 10", p[0])
	}
	off := 1
	for p[off] != 0 {
		off++
	}
	off++    // version
	off += 4 // conn id
	scramble = append(scramble, p[off:off+8]...)
	off += 9             // part1 + filler
	off += 2 + 1 + 2 + 2 // caps low + charset + status + caps high
	off += 1             // auth data len
	off += 10            // reserved
	scramble = append(scramble, p[off:off+12]...)
	off += 13 // part2 + terminator
	if off >= len(p) || p[off] == 0 {
		return scramble
	}
	return scramble
}

func (c *testClient) sendHandshake(user, password string) {
	c.t.Helper()
	var caps uint32 = capProtocol41 | capSecureConnection | capPluginAuth | capPluginAuthLenenc | capConnectWithDB | capTransactions
	p := make([]byte, 0, 64)
	var cb [4]byte
	binary.LittleEndian.PutUint32(cb[:], caps)
	p = append(p, cb[:]...)
	var mp [4]byte
	binary.LittleEndian.PutUint32(mp[:], 1<<24)
	p = append(p, mp[:]...)
	p = append(p, 0x21) // utf8mb4
	p = append(p, make([]byte, 23)...)
	p = append(p, user...)
	p = append(p, 0)
	// auth response：fast-auth 32 字节（无法被服务端校验，触发 full auth）。
	p = appendLenencBytes(p, []byte(password))
	p = append(p, 0) // 无数据库名
	p = append(p, authPlugin...)
	p = append(p, 0)
	if err := c.pc.writePacket(p); err != nil {
		c.t.Fatalf("send handshake: %v", err)
	}
}

func (c *testClient) expectOK() {
	c.t.Helper()
	p, err := c.pc.readPacket()
	if err != nil {
		c.t.Fatalf("read ok: %v", err)
	}
	if p[0] != 0x00 {
		c.t.Fatalf("expected OK (0x00), got 0x%02x payload=%q", p[0], p)
	}
}

func (c *testClient) command(b byte, arg []byte) []byte {
	c.t.Helper()
	p := make([]byte, 0, len(arg)+1)
	p = append(p, b)
	p = append(p, arg...)
	if err := c.pc.writePacket(p); err != nil {
		c.t.Fatalf("send command 0x%02x: %v", b, err)
	}
	resp, err := c.pc.readPacket()
	if err != nil {
		c.t.Fatalf("read resp: %v", err)
	}
	return resp
}

// readBinaryResultSetFrom 解析二进制结果集（COM_STMT_EXECUTE）：列定义同文本协议，
// 行按 null bitmap（前 2 bit 保留）+ lenenc 列值解析。返回列名与行。
func (c *testClient) readBinaryResultSetFrom(first []byte) (cols []string, rows [][]string) {
	c.t.Helper()
	n, _, err := readLenencInt(first, 0)
	if err != nil {
		c.t.Fatalf("parse col count: %v", err)
	}
	for i := 0; i < int(n); i++ {
		cp, err := c.pc.readPacket()
		if err != nil {
			c.t.Fatalf("read col def: %v", err)
		}
		_, off, err := readLenencBytes(cp, 0) // catalog
		if err != nil {
			c.t.Fatalf("col parse: %v (payload %x)", err, cp)
		}
		for j := 0; j < 3; j++ {
			_, off, err = readLenencBytes(cp, off)
			if err != nil {
				c.t.Fatalf("col parse: %v (payload %x)", err, cp)
			}
		}
		name, _, err := readLenencBytes(cp, off)
		if err != nil {
			c.t.Fatalf("col name: %v", err)
		}
		cols = append(cols, string(name))
	}
	if _, err := c.pc.readPacket(); err != nil { // EOF
		c.t.Fatalf("read col EOF: %v", err)
	}
	return cols, c.readBinaryRows(cols)
}

// readBinaryRows 读取 EXECUTE 二进制结果集行（列定义/EOF 已由调用方消费）。
func (c *testClient) readBinaryRows(cols []string) (rows [][]string) {
	for {
		rp, err := c.pc.readPacket()
		if err != nil {
			c.t.Fatalf("read bin row: %v", err)
		}
		if rp[0] == 0xfe && len(rp) < 9 { // EOF 包
			break
		}
		row := make([]string, len(cols))
		// 包结构：header(0x00) + null bitmap((n+7+2)/8 字节) + 列值。
		off := 1 + (len(cols)+7+2)/8
		for i := 0; i < len(cols); i++ {
			if rp[(i+2)/8]&(1<<(uint(i+2)%8)) != 0 {
				row[i] = "<NULL>"
				continue
			}
			b, n2, err := readLenencBytes(rp, off)
			if err != nil {
				c.t.Fatalf("bin row parse: %v (payload %x)", err, rp)
			}
			row[i] = string(b)
			off = n2
		}
		rows = append(rows, row)
	}
	return rows
}

// readResultSetFrom 解析文本结果集：first 为已读入的列数包；返回列名与行。
func (c *testClient) readResultSetFrom(first []byte) (cols []string, rows [][]string) {
	c.t.Helper()
	n, _, err := readLenencInt(first, 0)
	if err != nil {
		c.t.Fatalf("parse col count: %v", err)
	}
	for i := 0; i < int(n); i++ {
		cp, err := c.pc.readPacket()
		if err != nil {
			c.t.Fatalf("read col def: %v", err)
		}
		// 跳过 catalog/schema/table/org_table，取 name。
		_, off, err := readLenencBytes(cp, 0) // catalog
		if err != nil {
			c.t.Fatalf("col parse: %v (payload %x)", err, cp)
		}
		for j := 0; j < 3; j++ {
			_, off, err = readLenencBytes(cp, off)
			if err != nil {
				c.t.Fatalf("col parse: %v (payload %x)", err, cp)
			}
		}
		name, _, err := readLenencBytes(cp, off)
		if err != nil {
			c.t.Fatalf("col name: %v", err)
		}
		cols = append(cols, string(name))
	}
	if _, err := c.pc.readPacket(); err != nil { // EOF
		c.t.Fatalf("read col EOF: %v", err)
	}
	for {
		rp, err := c.pc.readPacket()
		if err != nil {
			c.t.Fatalf("read row: %v", err)
		}
		if rp[0] == 0xfe && len(rp) < 9 { // EOF 包
			break
		}
		row := make([]string, 0, len(cols))
		off := 0
		for i := 0; i < len(cols); i++ {
			v, n2, err := readLenencBytes(rp, off)
			if err != nil {
				c.t.Fatalf("row cell: %v", err)
			}
			row = append(row, string(v))
			off = n2
		}
		rows = append(rows, row)
	}
	return cols, rows
}

func (c *testClient) queryResult(t *testing.T, q string) (cols []string, rows [][]string) {
	t.Helper()
	resp := c.command(comQuery, []byte(q))
	switch resp[0] {
	case 0xff:
		t.Fatalf("query %q failed: %s", q, resp)
	case 0x00:
		return nil, nil
	}
	// resp 即列数包（无 OK header），继续读列定义与行。
	cols, rows = c.readResultSetFrom(resp)
	return cols, rows
}

func TestMySQLWireRoundtrip(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	t.Cleanup(func() { d.Close() })

	srv := New(d.SQL)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ServeListener(ln)
	addr := ln.Addr().String()

	c := dialClient(t, addr)
	scramble := c.readGreeting()
	if len(scramble) != 20 {
		t.Fatalf("scramble len = %d, want 20", len(scramble))
	}
	_ = scramble
	c.sendHandshake("root", "x")
	c.expectOK() // 免鉴权模式直接 OK

	// CREATE TABLE
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")))
	// INSERT
	ins := c.command(comQuery, []byte("INSERT INTO t VALUES (1, 'alice')"))
	if ins[0] != 0x00 {
		t.Fatalf("insert not ok: %q", ins)
	}
	affected, _, err := readLenencInt(ins, 1)
	if err != nil || affected != 1 {
		t.Fatalf("insert affected = %d, want 1", affected)
	}
	// SELECT
	cols, rows := c.queryResult(t, "SELECT * FROM t")
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "v" {
		t.Fatalf("cols = %v", cols)
	}
	if len(rows) != 1 || rows[0][0] != "1" || rows[0][1] != "alice" {
		t.Fatalf("rows = %v", rows)
	}
	// COM_PING
	c.expectOKFrom(c.command(comPing, nil))
	// COM_STMT_PREPARE
	prep := c.command(comStmtPrepare, []byte("SELECT * FROM t WHERE id = ?"))
	if prep[0] != 0x00 {
		t.Fatalf("prepare not ok: %q", prep)
	}
	stmtID := binary.LittleEndian.Uint32(prep[1:5])
	numCols := binary.LittleEndian.Uint16(prep[5:7])
	numParams := binary.LittleEndian.Uint16(prep[7:9])
	if numParams != 1 {
		t.Fatalf("prepare params = %d, want 1", numParams)
	}
	_ = numCols
	// 参数列定义 + EOF
	if _, err := c.pc.readPacket(); err != nil {
		t.Fatalf("param col: %v", err)
	}
	c.expectEOF()
	// COM_STMT_EXECUTE：参数 id=1（INT=3）
	ep := make([]byte, 0, 32)
	var sid [4]byte
	binary.LittleEndian.PutUint32(sid[:], stmtID)
	ep = append(ep, sid[:]...)
	ep = append(ep, 0) // flags
	var iter [4]byte
	binary.LittleEndian.PutUint32(iter[:], 1)
	ep = append(ep, iter[:]...)
	ep = append(ep, 0x00) // null bitmap（1 参数 = 1 字节，非 NULL）
	ep = append(ep, 0x01) // new_params_bound_flag
	ep = append(ep, 3, 0) // MYSQL_TYPE_LONG
	var pv [4]byte
	binary.LittleEndian.PutUint32(pv[:], 1)
	ep = append(ep, pv[:]...)
	resp := c.command(comStmtExecute, ep)
	if resp[0] == 0xff {
		t.Fatalf("stmt execute failed: %s", resp)
	}
	var cols2 []string
	var rows2 [][]string
	if resp[0] != 0x00 {
		cols2, rows2 = c.readBinaryResultSetFrom(resp)
	}
	_ = cols2
	if len(rows2) != 1 || rows2[0][0] != "1" || rows2[0][1] != "alice" {
		t.Fatalf("stmt rows = %v", rows2)
	}
	// COM_STMT_CLOSE（无响应）
	if err := c.pc.writePacket(append([]byte{comStmtClose}, sid[:]...)); err != nil {
		t.Fatalf("stmt close: %v", err)
	}
	// COM_QUIT
	if err := c.pc.writePacket([]byte{comQuit}); err != nil {
		t.Fatalf("quit: %v", err)
	}
}

// expectOKFrom 断言响应为 OK 包。
func (c *testClient) expectOKFrom(p []byte) {
	c.t.Helper()
	if p[0] != 0x00 {
		c.t.Fatalf("expected OK, got 0x%02x payload=%q", p[0], p)
	}
}

func (c *testClient) expectEOF() {
	c.t.Helper()
	p, err := c.pc.readPacket()
	if err != nil {
		c.t.Fatalf("read eof: %v", err)
	}
	if p[0] != 0xfe || len(p) != 5 {
		c.t.Fatalf("expected EOF, got %q", p)
	}
}

// 防呆：io 包确保本文件引用有效（readPacket 返回 io.EOF 场景）。
var _ = io.EOF

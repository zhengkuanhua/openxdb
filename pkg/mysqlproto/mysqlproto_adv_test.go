package mysqlproto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"net"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// ---- COM_CHANGE_USER：负载解析单元测试 ----

func TestParseChangeUserPayload(t *testing.T) {
	p := make([]byte, 0, 64)
	p = append(p, "newuser"...)
	p = append(p, 0)
	p = appendLenencBytes(p, []byte("newpass"))
	p = append(p, 0) // db 空
	p = append(p, 0x21)
	p = append(p, authPlugin...)
	p = append(p, 0)

	user, authResp, db, charset, plugin, err := parseChangeUserPayload(p)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if user != "newuser" {
		t.Errorf("user = %q, want newuser", user)
	}
	if string(authResp) != "newpass" {
		t.Errorf("authResp = %q, want newpass", authResp)
	}
	if db != "" || charset != 0x21 || plugin != authPlugin {
		t.Errorf("db=%q charset=%d plugin=%q", db, charset, plugin)
	}
}

// ---- 集成测试辅助：完整认证往返（兼容免鉴权直返 OK 与 verify 模式 full auth）----

// startTestServer 启动一个 MySQL 协议服务器并建立已认证的测试连接。
func startTestServer(t *testing.T, verify verifyFn) (*testClient, string) {
	t.Helper()
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
	if verify != nil {
		srv.SetVerify(verify)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ServeListener(ln)
	c := dialClient(t, ln.Addr().String())
	c.readGreeting()
	c.sendHandshake("root", "x")
	resp := c.completeAuth("x")
	if resp[0] != 0x00 {
		t.Fatalf("handshake auth failed: %q", resp)
	}
	return c, ln.Addr().String()
}

// completeAuth 处理认证往返：若服务端发起 full auth（AuthMoreData 0x03），
// 则请求 RSA 公钥并加密发送明文密码；免鉴权模式下首个包即 OK。
func (c *testClient) completeAuth(password string) []byte {
	c.t.Helper()
	resp, err := c.pc.readPacket()
	if err != nil {
		c.t.Fatalf("read auth response: %v", err)
	}
	if resp[0] == 0x01 && len(resp) >= 2 && resp[1] == 0x03 {
		if err := c.pc.writePacket([]byte{0x02}); err != nil {
			c.t.Fatalf("request pubkey: %v", err)
		}
		kp, err := c.pc.readPacket()
		if err != nil || len(kp) < 2 || kp[0] != 0x01 {
			c.t.Fatalf("read pubkey: %v payload=%x", err, kp)
		}
		der := kp[1:]
		if der[len(der)-1] == 0 {
			der = der[:len(der)-1]
		}
		blk, _ := pem.Decode(der)
		if blk == nil {
			c.t.Fatalf("pubkey not PEM: %q", der)
		}
		pubAny, err := x509.ParsePKIXPublicKey(blk.Bytes)
		if err != nil {
			c.t.Fatalf("parse pubkey: %v", err)
		}
		pub, ok := pubAny.(*rsa.PublicKey)
		if !ok {
			c.t.Fatalf("unexpected pubkey type %T", pubAny)
		}
		ct, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, []byte(password), nil)
		if err != nil {
			c.t.Fatalf("rsa encrypt: %v", err)
		}
		if err := c.pc.writePacket(ct); err != nil {
			c.t.Fatalf("send encrypted password: %v", err)
		}
		resp, err = c.pc.readPacket()
		if err != nil {
			c.t.Fatalf("read auth result: %v", err)
		}
	}
	return resp
}

// changeUser 发送 COM_CHANGE_USER（user + authResp + db + charset + plugin）。
func (c *testClient) changeUser(user, password string) []byte {
	c.t.Helper()
	p := make([]byte, 0, 64)
	p = append(p, user...)
	p = append(p, 0)
	p = appendLenencBytes(p, []byte(password))
	p = append(p, 0) // 不切换数据库
	p = append(p, 0x21)
	p = append(p, authPlugin...)
	p = append(p, 0)
	if err := c.pc.writePacket(append([]byte{comChangeUser}, p...)); err != nil {
		c.t.Fatalf("send change user: %v", err)
	}
	return c.completeAuth(password)
}

// sendLongData 发送 COM_STMT_SEND_LONG_DATA（无响应）。
func (c *testClient) sendLongData(stmtID uint32, paramID uint16, data []byte) {
	c.t.Helper()
	p := []byte{comStmtSendLongData}
	var sid [4]byte
	binary.LittleEndian.PutUint32(sid[:], stmtID)
	p = append(p, sid[:]...)
	var pid [2]byte
	binary.LittleEndian.PutUint16(pid[:], paramID)
	p = append(p, pid[:]...)
	p = append(p, data...)
	if err := c.pc.writePacket(p); err != nil {
		c.t.Fatalf("send long data: %v", err)
	}
}

// ---- COM_CHANGE_USER：免鉴权模式集成测试 ----

func TestCOMChangeUserNoAuth(t *testing.T) {
	c, _ := startTestServer(t, nil)
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t VALUES (1, 'alice')")))

	prep := c.command(comStmtPrepare, []byte("SELECT * FROM t WHERE id = ?"))
	if prep[0] != 0x00 {
		t.Fatalf("prepare failed: %q", prep)
	}
	stmtID := binary.LittleEndian.Uint32(prep[1:5])
	if _, err := c.pc.readPacket(); err != nil { // 参数列定义
		t.Fatalf("param col: %v", err)
	}
	c.expectEOF()

	// 切换用户：免鉴权直接 OK。
	resp := c.changeUser("newuser", "newpass")
	if resp[0] != 0x00 {
		t.Fatalf("change user not ok: %q", resp)
	}

	// MySQL 语义：切换后预处理语句被清空，旧 stmtID 执行应报错。
	ep := make([]byte, 0, 32)
	var sid [4]byte
	binary.LittleEndian.PutUint32(sid[:], stmtID)
	ep = append(ep, sid[:]...)
	ep = append(ep, 0) // flags
	var iter [4]byte
	binary.LittleEndian.PutUint32(iter[:], 1)
	ep = append(ep, iter[:]...)
	ep = append(ep, 0x00) // null bitmap
	ep = append(ep, 0x01) // new_params_bound_flag
	ep = append(ep, 3, 0) // MYSQL_TYPE_LONG
	var pv [4]byte
	binary.LittleEndian.PutUint32(pv[:], 1)
	ep = append(ep, pv[:]...)
	eresp := c.command(comStmtExecute, ep)
	if eresp[0] != 0xff {
		t.Fatalf("execute after change user should fail, got %q", eresp)
	}

	// 新会话可用（newuser 无表权限，改用无表查询验证连接与 SQL 通路）。
	cols, rows := c.queryResult(t, "SELECT 1")
	if len(rows) != 1 || rows[0][0] != "1" {
		t.Fatalf("rows after change user = %v (cols %v)", rows, cols)
	}
}

// ---- COM_CHANGE_USER：verify 模式集成测试（成功重置 / 失败保持）----

func TestCOMChangeUserWithVerify(t *testing.T) {
	c, _ := startTestServer(t, func(user, password string) bool {
		return (user == "root" && password == "x") ||
			(user == "gooduser" && password == "goodpass")
	})
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t VALUES (1, 'alice')")))

	prep := c.command(comStmtPrepare, []byte("SELECT * FROM t WHERE id = ?"))
	stmtID := binary.LittleEndian.Uint32(prep[1:5])
	if _, err := c.pc.readPacket(); err != nil {
		t.Fatalf("param col: %v", err)
	}
	c.expectEOF()

	// 失败切换：返回 ERR，原用户与预处理语句保持不变。
	bad := c.changeUser("baduser", "badpass")
	if bad[0] != 0xff {
		t.Fatalf("bad change user should fail, got %q", bad)
	}
	ep := make([]byte, 0, 32)
	var sid [4]byte
	binary.LittleEndian.PutUint32(sid[:], stmtID)
	ep = append(ep, sid[:]...)
	ep = append(ep, 0)
	var iter [4]byte
	binary.LittleEndian.PutUint32(iter[:], 1)
	ep = append(ep, iter[:]...)
	ep = append(ep, 0x00)
	ep = append(ep, 0x01)
	ep = append(ep, 3, 0)
	var pv [4]byte
	binary.LittleEndian.PutUint32(pv[:], 1)
	ep = append(ep, pv[:]...)
	eresp := c.command(comStmtExecute, ep)
	if eresp[0] == 0xff {
		t.Fatalf("execute after failed change user should still work, got %q", eresp)
	}
	// 消费完整结果集，避免残留包污染后续 change user 响应读取。
	c.readBinaryResultSetFrom(eresp)
	// 成功切换：OK，旧语句被清空。
	ok := c.changeUser("gooduser", "goodpass")
	if ok[0] != 0x00 {
		t.Fatalf("good change user failed: %q", ok)
	}
	eresp = c.command(comStmtExecute, ep)
	if eresp[0] != 0xff {
		t.Fatalf("execute after successful change user should fail, got %q", eresp)
	}
}

// ---- COM_STMT_SEND_LONG_DATA：分块累积 + EXECUTE 集成测试 ----

func TestCOMStmtSendLongData(t *testing.T) {
	c, _ := startTestServer(t, nil)
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t2 (a TEXT, b TEXT)")))

	prep := c.command(comStmtPrepare, []byte("INSERT INTO t2 VALUES (?, ?)"))
	if prep[0] != 0x00 {
		t.Fatalf("prepare failed: %q", prep)
	}
	stmtID := binary.LittleEndian.Uint32(prep[1:5])
	if binary.LittleEndian.Uint16(prep[7:9]) != 2 {
		t.Fatalf("prepare params = %d, want 2", binary.LittleEndian.Uint16(prep[7:9]))
	}
	for i := 0; i < 2; i++ {
		if _, err := c.pc.readPacket(); err != nil { // 参数列定义
			t.Fatalf("param col: %v", err)
		}
	}
	c.expectEOF()

	// 分块发送长参数：param0 累积 "hello " + "world"。
	c.sendLongData(stmtID, 0, []byte("hello "))
	c.sendLongData(stmtID, 0, []byte("world"))

	// EXECUTE：param0 使用长数据（值不发送）；param1 普通字符串 "x"。
	ep := make([]byte, 0, 64)
	var sid [4]byte
	binary.LittleEndian.PutUint32(sid[:], stmtID)
	ep = append(ep, sid[:]...)
	ep = append(ep, 0)
	var iter [4]byte
	binary.LittleEndian.PutUint32(iter[:], 1)
	ep = append(ep, iter[:]...)
	ep = append(ep, 0x00) // null bitmap（2 参数均非 NULL）
	ep = append(ep, 0x01) // new_params_bound_flag
	ep = append(ep, 252, 0, 253, 0) // MYSQL_TYPE_BLOB, MYSQL_TYPE_VAR_STRING
	ep = appendLenencBytes(ep, []byte("x"))
	eresp := c.command(comStmtExecute, ep)
	if eresp[0] != 0x00 {
		t.Fatalf("execute with long data failed: %q", eresp)
	}

	cols, rows := c.queryResult(t, "SELECT * FROM t2")
	if len(rows) != 1 || rows[0][0] != "hello world" || rows[0][1] != "x" {
		t.Fatalf("rows = %v (cols %v)", rows, cols)
	}
}

// ---- 扩展参数类型：TIME / NEWDATE / DECIMAL / BLOB 变体 / NULL ----

func TestStmtExecuteExtendedParamTypes(t *testing.T) {
	c, _ := startTestServer(t, nil)
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t3 (a TEXT, b TEXT, c TEXT, d TEXT, e TEXT)")))

	prep := c.command(comStmtPrepare, []byte("INSERT INTO t3 VALUES (?, ?, ?, ?, ?)"))
	if prep[0] != 0x00 {
		t.Fatalf("prepare failed: %q", prep)
	}
	stmtID := binary.LittleEndian.Uint32(prep[1:5])
	if binary.LittleEndian.Uint16(prep[7:9]) != 5 {
		t.Fatalf("prepare params = %d, want 5", binary.LittleEndian.Uint16(prep[7:9]))
	}
	for i := 0; i < 5; i++ {
		if _, err := c.pc.readPacket(); err != nil {
			t.Fatalf("param col: %v", err)
		}
	}
	c.expectEOF()

	// TIME：length=8 + days=0 + 10:20:30
	timeVal := []byte{8, 0, 0, 0, 0, 10, 20, 30}
	// NEWDATE：length=4 + 2026-10-22（year 小端 2 字节）
	dateVal := []byte{4, 0xea, 0x07, 10, 22}

	ep := make([]byte, 0, 128)
	var sid [4]byte
	binary.LittleEndian.PutUint32(sid[:], stmtID)
	ep = append(ep, sid[:]...)
	ep = append(ep, 0)
	var iter [4]byte
	binary.LittleEndian.PutUint32(iter[:], 1)
	ep = append(ep, iter[:]...)
	ep = append(ep, 0x00) // null bitmap（5 参数均非 NULL）
	ep = append(ep, 0x01) // new_params_bound_flag
	// 类型：TIME(11) NEWDATE(14) DECIMAL(0) TINY_BLOB(249) NULL(6)
	ep = append(ep, 11, 0, 14, 0, 0, 0, 249, 0, 6, 0)
	ep = append(ep, timeVal...)
	ep = append(ep, dateVal...)
	ep = appendLenencBytes(ep, []byte("12.34"))
	ep = appendLenencBytes(ep, []byte("blobval"))
	eresp := c.command(comStmtExecute, ep)
	if eresp[0] != 0x00 {
		t.Fatalf("execute extended types failed: %q", eresp)
	}

	cols, rows := c.queryResult(t, "SELECT * FROM t3")
	if len(rows) != 1 {
		t.Fatalf("rows = %v", rows)
	}
	want := []string{"10:20:30", "2026-10-22", "12.34", "blobval", ""}
	for i := 0; i < 5; i++ {
		if rows[0][i] != want[i] {
			t.Errorf("col %d = %q, want %q (full row %v, cols %v)", i, rows[0][i], want[i], rows[0], cols)
		}
	}
}

// 防呆：time 包确保本文件引用有效（readGreeting 超时场景）。
var _ = time.Second

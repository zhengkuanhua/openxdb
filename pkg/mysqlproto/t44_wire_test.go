package mysqlproto

// v1.6.0 P1-2（T44）测试覆盖：COM_REGISTER_SLAVE 合法/截断、COM_BINLOG_DUMP
// 全量 / 从中间 LSN / 轮询新事件 / 未启用报错。事件包为 OpenXDB 专有外壳
// （type=99 FORMAT / type=100 ROWS / type=101 XID，见 binlog_dump.go）。

import (
	"encoding/binary"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// newBinlogServer 装配真实引擎（启用复制）并返回 server 地址与已握手客户端。
func newBinlogServer(t *testing.T) (*db.DB, string, *testClient) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.StartReplication("127.0.0.1:0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	srv := New(d.SQL)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ServeListener(ln)
	addr := ln.Addr().String()
	c := dialClient(t, addr)
	scramble := c.readGreeting()
	_ = scramble
	c.sendHandshake("root", "x")
	c.expectOK()
	return d, addr, c
}

// closeClient 关闭客户端连接（释放 binlog 轮询 goroutine 持有的文件句柄）。
func closeClient(c *testClient) {
	c.t.Helper()
	if conn, ok := c.pc.rw.(net.Conn); ok {
		_ = conn.Close()
	}
}

// pkUint64 将事件主键字节解码为 uint64（INT 主键为 8B 大端）。
func pkUint64(pk []byte) uint64 {
	if len(pk) != 8 {
		return ^uint64(0)
	}
	return binary.BigEndian.Uint64(pk)
}

// sendBinlogDump 发送 COM_BINLOG_DUMP（position 起流式推送）。
func sendBinlogDump(c *testClient, pos uint32) {
	c.t.Helper()
	p := make([]byte, 0, 32)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], pos)
	p = append(p, b[:]...)
	p = append(p, 0, 0) // flags
	binary.LittleEndian.PutUint32(b[:], 1)
	p = append(p, b[:]...) // server-id
	p = append(p, "openxdb-binlog"...)
	p = append(p, 0)
	if err := c.pc.writePacket(append([]byte{comBinlogDump}, p...)); err != nil {
		c.t.Fatalf("send binlog dump: %v", err)
	}
}

// readBinlogEvent 读取一个 binlog 事件包（19B 外壳 + body）。
func readBinlogEvent(c *testClient) (typ byte, logPos uint32, body []byte) {
	c.t.Helper()
	p, err := c.pc.readPacket()
	if err != nil {
		c.t.Fatalf("read binlog event: %v", err)
	}
	if len(p) < 19 {
		c.t.Fatalf("binlog event too short: %x", p)
	}
	return p[4], binary.LittleEndian.Uint32(p[13:17]), p[19:]
}

// parseFormatBody 解析 FORMAT 事件体：magic | filename NUL | start_lsn | last_lsn。
func parseFormatBody(b []byte) (startLSN, lastLSN uint64) {
	if len(b) < 7+16 {
		return 0, 0
	}
	off := 7
	for off < len(b) && b[off] != 0 {
		off++
	}
	off++ // NUL
	if off+16 > len(b) {
		return 0, 0
	}
	startLSN = binary.LittleEndian.Uint64(b[off : off+8])
	lastLSN = binary.LittleEndian.Uint64(b[off+8 : off+16])
	return
}

// parseRowsBody 解析 ROWS 事件体。
func parseRowsBody(b []byte) (lsn, commit, sver uint64, kind byte, tableID uint64, isIndex byte, table, pk, raw []byte) {
	if len(b) < 34 {
		return 0, 0, 0, 0, 0, 0, nil, nil, nil
	}
	lsn = binary.LittleEndian.Uint64(b[0:8])
	commit = binary.LittleEndian.Uint64(b[8:16])
	sver = binary.LittleEndian.Uint64(b[16:24])
	kind = b[24]
	tableID = binary.LittleEndian.Uint64(b[25:33])
	isIndex = b[33]
	off := 34
	var err error
	if table, off, err = readLenencBytes(b, off); err != nil {
		return 0, 0, 0, 0, 0, 0, nil, nil, nil
	}
	if pk, off, err = readLenencBytes(b, off); err != nil {
		return 0, 0, 0, 0, 0, 0, nil, nil, nil
	}
	if raw, _, err = readLenencBytes(b, off); err != nil {
		return 0, 0, 0, 0, 0, 0, nil, nil, nil
	}
	return
}

// TestT44RegisterSlave COM_REGISTER_SLAVE：合法负载 OK；截断负载 ERR。
func TestT44RegisterSlave(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	srv := New(d.SQL)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ServeListener(ln)
	c := dialClient(t, ln.Addr().String())
	scramble := c.readGreeting()
	_ = scramble
	c.sendHandshake("root", "x")
	c.expectOK()

	// 合法负载：server-id + hostname/user/password 三段 + 尾部 port/rank/master-id。
	payload := make([]byte, 0, 32)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], 12345)
	payload = append(payload, b[:]...)
	payload = append(payload, byte(len("host1")))
	payload = append(payload, "host1"...)
	payload = append(payload, byte(len("rep")))
	payload = append(payload, "rep"...)
	payload = append(payload, byte(len("pw")))
	payload = append(payload, "pw"...)
	var w [2]byte
	binary.LittleEndian.PutUint16(w[:], 3306)
	payload = append(payload, w[:]...)
	binary.LittleEndian.PutUint32(b[:], 1)
	payload = append(payload, b[:]...)
	binary.LittleEndian.PutUint32(b[:], 2)
	payload = append(payload, b[:]...)
	c.expectOKFrom(c.command(comRegisterSlave, payload))

	// 截断负载（不足 server-id 4B）→ ERR。
	resp := c.command(comRegisterSlave, []byte{0x01, 0x02})
	if resp[0] != 0xff {
		t.Fatalf("malformed register slave: expected ERR, got 0x%02x payload=%q", resp[0], resp)
	}
	if !strings.Contains(string(resp[1:]), "malformed") {
		t.Fatalf("malformed register slave msg = %q", resp[1:])
	}
}

// TestT44BinlogDumpFull COM_BINLOG_DUMP 全量：FORMAT + 逐记录 ROWS + XID，
// 事件体还原 LSN/表名/主键/类型，log_pos 取事件 LSN。
func TestT44BinlogDumpFull(t *testing.T) {
	_, _, c := newBinlogServer(t)
	t.Cleanup(func() { closeClient(c) })
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t VALUES (1,'alice')")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t VALUES (2,'bob')")))
	c.expectOKFrom(c.command(comQuery, []byte("DELETE FROM t WHERE id=1")))

	sendBinlogDump(c, 0)
	// FORMAT：magic 正确、start=0、last>0。
	typ, logPos, body := readBinlogEvent(c)
	if typ != evOpenXDBFormat {
		t.Fatalf("first event type = %d, want FORMAT(%d)", typ, evOpenXDBFormat)
	}
	if string(body[:7]) != "OPENXDB" {
		t.Fatalf("format magic = %q", body[:7])
	}
	startLSN, lastLSN := parseFormatBody(body)
	if startLSN != 0 || lastLSN == 0 {
		t.Fatalf("format start=%d last=%d, want start=0 last>0", startLSN, lastLSN)
	}
	_ = logPos

	// 3 条记录事件：PUT(1)、PUT(2)、DELETE(1)，各带 XID 提交边界。
	wantPK := []uint64{1, 2, 1}
	wantKind := []byte{1, 1, 2}
	for i := 0; i < 3; i++ {
		typ, lp, rb := readBinlogEvent(c)
		if typ != evOpenXDBRows {
			t.Fatalf("event %d type = %d, want ROWS(%d)", i, typ, evOpenXDBRows)
		}
		lsn, commit, _, kind, _, isIndex, table, pk, raw := parseRowsBody(rb)
		if lsn == 0 || commit == 0 {
			t.Fatalf("rows %d lsn=%d commit=%d", i, lsn, commit)
		}
		if kind != wantKind[i] {
			t.Fatalf("rows %d kind = %d, want %d", i, kind, wantKind[i])
		}
		if isIndex != 0 {
			t.Fatalf("rows %d is_index = %d, want 0", i, isIndex)
		}
		if string(table) != "t" {
			t.Fatalf("rows %d table = %q, want t", i, table)
		}
		if pkUint64(pk) != wantPK[i] {
			t.Fatalf("rows %d pk = %x, want %d", i, pk, wantPK[i])
		}
		if i < 2 && len(raw) == 0 {
			t.Fatalf("rows %d PUT raw value empty", i)
		}
		if i == 2 && len(raw) != 0 {
			t.Fatalf("rows %d DELETE raw value should be empty, got %x", i, raw)
		}
		if uint64(lp) != lsn {
			t.Fatalf("rows %d log_pos = %d, want lsn %d", i, lp, lsn)
		}
		// XID 提交边界紧随其后。
		typ, _, xb := readBinlogEvent(c)
		if typ != evOpenXDBXID {
			t.Fatalf("rows %d tail type = %d, want XID(%d)", i, typ, evOpenXDBXID)
		}
		if binary.LittleEndian.Uint64(xb[:8]) != commit {
			t.Fatalf("rows %d XID commit = %d, want %d", i, binary.LittleEndian.Uint64(xb[:8]), commit)
		}
	}
}

// TestT44BinlogDumpFrom 从中间 LSN dump 只收到后续事件；越过末尾仅 FORMAT。
func TestT44BinlogDumpFrom(t *testing.T) {
	_, addr, c := newBinlogServer(t)
	t.Cleanup(func() { closeClient(c) })
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t VALUES (1,'alice')")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t VALUES (2,'bob')")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t VALUES (3,'carol')")))

	_, rows := c.queryResult(t, "SHOW BINLOG EVENTS")
	if len(rows) != 3 {
		t.Fatalf("binlog events = %d, want 3", len(rows))
	}
	secondLSN, err := strconv.ParseUint(rows[1][0], 10, 64)
	if err != nil {
		t.Fatalf("parse second lsn: %v", err)
	}
	lastLSN, err := strconv.ParseUint(rows[2][0], 10, 64)
	if err != nil {
		t.Fatalf("parse last lsn: %v", err)
	}

	// FROM 中间 LSN：只收到第 2、3 条记录。
	sendBinlogDump(c, uint32(secondLSN))
	typ, _, body := readBinlogEvent(c)
	if typ != evOpenXDBFormat {
		t.Fatalf("first type = %d, want FORMAT", typ)
	}
	startLSN, _ := parseFormatBody(body)
	if startLSN != secondLSN {
		t.Fatalf("format start = %d, want %d", startLSN, secondLSN)
	}
	for i := 0; i < 2; i++ {
		typ, _, rb := readBinlogEvent(c)
		if typ != evOpenXDBRows {
			t.Fatalf("tail event %d type = %d, want ROWS", i, typ)
		}
		_, _, _, _, _, _, _, pk, _ := parseRowsBody(rb)
		wantPK := uint64(2)
		if i == 1 {
			wantPK = 3
		}
		if pkUint64(pk) != wantPK {
			t.Fatalf("from-middle pk = %x, want %d", pk, wantPK)
		}
		if typ, _, _ = readBinlogEvent(c); typ != evOpenXDBXID {
			t.Fatalf("from-middle tail %d type = %d, want XID", i, typ)
		}
	}

	// 越过末尾（lastLSN+1）：新连接只收 FORMAT，随后轮询等待（不读）。
	c2 := dialClient(t, addr)
	t.Cleanup(func() { closeClient(c2) })
	scramble := c2.readGreeting()
	_ = scramble
	c2.sendHandshake("root", "x")
	c2.expectOK()
	sendBinlogDump(c2, uint32(lastLSN+1))
	typ, _, body = readBinlogEvent(c2)
	if typ != evOpenXDBFormat {
		t.Fatalf("past-end first type = %d, want FORMAT", typ)
	}
	startLSN, _ = parseFormatBody(body)
	if startLSN != lastLSN+1 {
		t.Fatalf("past-end format start = %d, want %d", startLSN, lastLSN+1)
	}
}

// TestT44BinlogDumpPolling 读完文件尾后轮询：新写入事件在轮询间隔内到达。
func TestT44BinlogDumpPolling(t *testing.T) {
	_, addr, c := newBinlogServer(t)
	t.Cleanup(func() { closeClient(c) })
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t VALUES (1,'alice')")))

	sendBinlogDump(c, 0)
	// 读齐 FORMAT + ROWS(1) + XID，此时位于文件尾。
	typ, _, _ := readBinlogEvent(c)
	if typ != evOpenXDBFormat {
		t.Fatalf("first type = %d, want FORMAT", typ)
	}
	typ, _, _ = readBinlogEvent(c)
	if typ != evOpenXDBRows {
		t.Fatalf("second type = %d, want ROWS", typ)
	}
	if typ, _, _ = readBinlogEvent(c); typ != evOpenXDBXID {
		t.Fatalf("third type = %d, want XID", typ)
	}

	// 追加新行：binlog dump 连接被流式推送独占，需用第二连接写入；
	// 随后读操作设 5s 上限（轮询间隔 200ms，远超余量）。
	c2 := dialClient(t, addr)
	t.Cleanup(func() { closeClient(c2) })
	scramble := c2.readGreeting()
	_ = scramble
	c2.sendHandshake("root", "x")
	c2.expectOK()
	c2.expectOKFrom(c2.command(comQuery, []byte("INSERT INTO t VALUES (2,'bob')")))
	if conn, ok := c.pc.rw.(net.Conn); ok {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	}
	typ, _, rb := readBinlogEvent(c)
	if typ != evOpenXDBRows {
		t.Fatalf("polled type = %d, want ROWS", typ)
	}
	_, _, _, _, _, _, _, pk, raw := parseRowsBody(rb)
	if pkUint64(pk) != 2 || len(raw) == 0 {
		t.Fatalf("polled rows pk=%x raw=%x", pk, raw)
	}
	if typ, _, _ = readBinlogEvent(c); typ != evOpenXDBXID {
		t.Fatalf("polled tail type = %d, want XID", typ)
	}
}

// TestT44BinlogDumpNotEnabled 未启用复制（未 StartReplication）→ ERR。
func TestT44BinlogDumpNotEnabled(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	srv := New(d.SQL)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ServeListener(ln)
	c := dialClient(t, ln.Addr().String())
	scramble := c.readGreeting()
	_ = scramble
	c.sendHandshake("root", "x")
	c.expectOK()

	sendBinlogDump(c, 0)
	resp, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("read dump resp: %v", err)
	}
	if resp[0] != 0xff {
		t.Fatalf("expected ERR for not-enabled binlog, got 0x%02x payload=%q", resp[0], resp)
	}
	if !strings.Contains(string(resp[1:]), "binlog") {
		t.Fatalf("not-enabled msg = %q, want mention binlog", resp[1:])
	}
}

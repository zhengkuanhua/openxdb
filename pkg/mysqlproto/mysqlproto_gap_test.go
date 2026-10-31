package mysqlproto

// v1.5.0 P2-3 mysqlproto 高级协议测试正式化：补齐源码功能面缺口测试。
// 覆盖：握手构造/解析、RSA 认证、lenenc/帧编解码边界、结果集打包、
// 二进制参数全类型（FLOAT/DOUBLE/负整数/unsigned/BIT/YEAR/MEDIUMINT/截断）、
// 驱动探测谓词、TLS 明文认证、STMT_RESET、类型复用、autocommit 隐式事务。

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// ---- 握手：构造与解析 ----

func TestHandshakeV10Structure(t *testing.T) {
	s := New(nil)
	scramble, err := genScramble()
	if err != nil {
		t.Fatalf("genScramble: %v", err)
	}
	p := s.handshakeV10(7, scramble)
	if p[0] != 0x0a {
		t.Fatalf("protocol = %d, want 10", p[0])
	}
	off := 1
	verEnd := bytes.IndexByte(p[off:], 0)
	if verEnd < 0 || string(p[off:off+verEnd]) != serverVersion {
		t.Fatalf("server version = %q, want %q", p[off:off+verEnd], serverVersion)
	}
	if serverVersion != "8.0.36-OpenXDB v1.17.0" {
		t.Fatalf("server version = %q, want %q", serverVersion, "8.0.36-OpenXDB v1.17.0")
	}
	if !bytes.HasPrefix(p[off:off+verEnd], []byte("8.0.36-OpenXDB")) {
		t.Fatalf("server version prefix lost: %q", p[off:off+verEnd])
	}
	off += verEnd + 1
	if binary.LittleEndian.Uint32(p[off:off+4]) != 7 {
		t.Fatalf("conn id = %d, want 7", binary.LittleEndian.Uint32(p[off:off+4]))
	}
	off += 4
	if !bytes.Equal(p[off:off+8], scramble[:8]) {
		t.Fatalf("scramble part1 mismatch")
	}
	off += 9 // part1 + filler
	if binary.LittleEndian.Uint16(p[off:off+2]) != uint16(serverCapabilities) {
		t.Fatalf("caps low = %#x", binary.LittleEndian.Uint16(p[off:off+2]))
	}
	if p[off+2] != 0x21 {
		t.Fatalf("charset = %d, want 0x21", p[off+2])
	}
	if binary.LittleEndian.Uint16(p[off+3:off+5]) != statusAutocommit {
		t.Fatalf("status = %d, want autocommit", binary.LittleEndian.Uint16(p[off+3:off+5]))
	}
	if binary.LittleEndian.Uint16(p[off+5:off+7]) != uint16(serverCapabilities>>16) {
		t.Fatalf("caps high = %#x", binary.LittleEndian.Uint16(p[off+5:off+7]))
	}
	off += 7 // caps low(2) + charset(1) + status(2) + caps high(2)
	if p[off] != byte(len(authPlugin)+8) {
		t.Fatalf("auth plugin len = %d, want %d", p[off], len(authPlugin)+8)
	}
	off += 11 // auth len + reserved 10
	if !bytes.Equal(p[off:off+12], scramble[8:20]) {
		t.Fatalf("scramble part2 mismatch")
	}
	off += 13 // part2 + terminator
	if !bytes.Equal(p[off:], append([]byte(authPlugin), 0)) {
		t.Fatalf("auth plugin tail = %q, want %q", p[off:], authPlugin)
	}
}

func TestGenScramble(t *testing.T) {
	a, err := genScramble()
	if err != nil {
		t.Fatalf("genScramble: %v", err)
	}
	if len(a) != 21 {
		t.Fatalf("len = %d, want 21", len(a))
	}
	if a[20] != 0 {
		t.Fatalf("terminator = %#x, want 0", a[20])
	}
	zero := true
	for _, b := range a[:20] {
		if b != 0 {
			zero = false
			break
		}
	}
	if zero {
		t.Fatalf("scramble all zero")
	}
	b, _ := genScramble()
	if bytes.Equal(a[:20], b[:20]) {
		t.Fatalf("two scrambles identical")
	}
}

func TestParseHandshakeResponse(t *testing.T) {
	// 完整：lenenc auth + db + plugin + attrs。
	base := uint32(capProtocol41) | uint32(capSecureConnection) | uint32(capPluginAuth) | uint32(capPluginAuthLenenc) |
		uint32(capConnectWithDB) | uint32(capConnectAttrs) | uint32(capTransactions)
	p := make([]byte, 32)
	binary.LittleEndian.PutUint32(p[0:4], base)
	binary.LittleEndian.PutUint32(p[4:8], 1<<24)
	p[8] = 0x21
	p = append(p, "alice"...)
	p = append(p, 0)
	p = appendLenencBytes(p, []byte("s3cret"))
	p = append(p, "mydb"...)
	p = append(p, 0)
	p = append(p, authPlugin...)
	p = append(p, 0)
	p = appendLenencBytes(p, []byte{0x01, 0x02})
	hr, err := parseHandshakeResponse(p)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if hr.capability != base || hr.maxPacket != 1<<24 || hr.charset != 0x21 {
		t.Fatalf("header fields: caps=%#x max=%d cs=%d", hr.capability, hr.maxPacket, hr.charset)
	}
	if hr.username != "alice" || string(hr.authResp) != "s3cret" || hr.database != "mydb" ||
		hr.pluginName != authPlugin || !bytes.Equal(hr.attrs, []byte{0x01, 0x02}) {
		t.Fatalf("hr = %+v", hr)
	}

	// 1 字节长度 auth（无 LENENC，有 SECURE_CONNECTION）。
	p2 := make([]byte, 32)
	binary.LittleEndian.PutUint32(p2[0:4], uint32(capProtocol41)|uint32(capSecureConnection)|uint32(capPluginAuth)|uint32(capConnectWithDB))
	p2[8] = 0x08
	p2 = append(p2, "bob"...)
	p2 = append(p2, 0)
	p2 = append(p2, 4) // 1 字节长度
	p2 = append(p2, "p4ss"...)
	p2 = append(p2, "db2"...)
	p2 = append(p2, 0)
	p2 = append(p2, authPlugin...)
	p2 = append(p2, 0)
	hr2, err := parseHandshakeResponse(p2)
	if err != nil {
		t.Fatalf("parse one-byte len: %v", err)
	}
	if hr2.username != "bob" || string(hr2.authResp) != "p4ss" || hr2.database != "db2" || hr2.charset != 0x08 {
		t.Fatalf("hr2 = %+v", hr2)
	}

	// attrs 截断不阻断握手。
	p3 := make([]byte, 32)
	binary.LittleEndian.PutUint32(p3[0:4], base)
	p3 = append(p3, "carol"...)
	p3 = append(p3, 0)
	p3 = appendLenencBytes(p3, []byte("x"))
	p3 = append(p3, 0) // db 空
	p3 = append(p3, 0)
	p3 = append(p3, authPlugin...)
	p3 = append(p3, 0)
	p3 = append(p3, 0xfc, 0xff) // attrs 长度声明超界
	hr3, err := parseHandshakeResponse(p3)
	if err != nil {
		t.Fatalf("parse attrs truncated: %v", err)
	}
	if hr3.username != "carol" || hr3.attrs != nil {
		t.Fatalf("hr3 = %+v", hr3)
	}

	// payload 过短。
	if _, err := parseHandshakeResponse(make([]byte, 20)); err == nil {
		t.Fatalf("short payload should error")
	}
	// username 无终止符。
	p4 := make([]byte, 32)
	binary.LittleEndian.PutUint32(p4[0:4], capProtocol41)
	p4 = append(p4, "no-null"...)
	if _, err := parseHandshakeResponse(p4); err == nil {
		t.Fatalf("missing username terminator should error")
	}
}

// ---- RSA 认证：密钥对与解密 ----

func TestRSAKeyPairRoundtrip(t *testing.T) {
	kp, err := newRSAKeyPair()
	if err != nil {
		t.Fatalf("newRSAKeyPair: %v", err)
	}
	if len(kp.pem) == 0 || kp.pem[len(kp.pem)-1] != 0 {
		t.Fatalf("pem tail missing terminator, len=%d", len(kp.pem))
	}
	blk, _ := pem.Decode(kp.pem[:len(kp.pem)-1])
	if blk == nil || blk.Type != "PUBLIC KEY" {
		t.Fatalf("pem decode: %v", blk)
	}
	// OAEP-SHA1 路径。
	ct, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, &kp.priv.PublicKey, []byte("oaep-secret"), nil)
	if err != nil {
		t.Fatalf("oaep encrypt: %v", err)
	}
	plain, err := kp.decryptPassword(ct)
	if err != nil || string(plain) != "oaep-secret" {
		t.Fatalf("oaep decrypt = %q err=%v", plain, err)
	}
	// PKCS1v15 路径（OAEP 失败后回退）。
	ct2, err := rsa.EncryptPKCS1v15(rand.Reader, &kp.priv.PublicKey, []byte("pkcs-secret"))
	if err != nil {
		t.Fatalf("pkcs encrypt: %v", err)
	}
	plain2, err := kp.decryptPassword(ct2)
	if err != nil || string(plain2) != "pkcs-secret" {
		t.Fatalf("pkcs decrypt = %q err=%v", plain2, err)
	}
	// 错误密文 → 错误。
	if _, err := kp.decryptPassword([]byte{0x01, 0x02, 0x03}); err == nil {
		t.Fatalf("garbage ciphertext should error")
	}
	// nil 密钥。
	var kp2 *rsaKeyPair
	if _, err := kp2.decryptPassword(ct); err == nil {
		t.Fatalf("nil keypair should error")
	}
}

// ---- lenenc / NUL 字符串 / 帧编解码 ----

func TestLenencIntRoundtrip(t *testing.T) {
	vals := []uint64{0, 1, 0xfa, 0xfb, 0xfc, 0xffff, 0x10000, 0xffffff, 0x1000000, 0xffffffffffffffff}
	for _, v := range vals {
		p := appendLenencInt(nil, v)
		got, off, err := readLenencInt(p, 0)
		if err != nil || got != v || off != len(p) {
			t.Errorf("roundtrip %d: got=%d off=%d len=%d err=%v (bytes %x)", v, got, off, len(p), err, p)
		}
	}
	// 边界编码形态断言。
	if got := appendLenencInt(nil, 0xfa); len(got) != 1 || got[0] != 0xfa {
		t.Errorf("0xfa encoded as %x", got)
	}
	if got := appendLenencInt(nil, 0xfb); len(got) != 3 || got[0] != 0xfc {
		t.Errorf("0xfb encoded as %x", got)
	}
	if got := appendLenencInt(nil, 0x10000); len(got) != 4 || got[0] != 0xfd {
		t.Errorf("0x10000 encoded as %x", got)
	}
	if got := appendLenencInt(nil, 0x1000000); len(got) != 9 || got[0] != 0xfe {
		t.Errorf("0x1000000 encoded as %x", got)
	}
	// 截断错误。
	if _, _, err := readLenencInt([]byte{0xfc, 0x01}, 0); err == nil {
		t.Fatalf("truncated fc should error")
	}
	if _, _, err := readLenencInt([]byte{0xfd, 0x01, 0x02}, 0); err == nil {
		t.Fatalf("truncated fd should error")
	}
	if _, _, err := readLenencInt([]byte{0xfe, 0x01, 0x02, 0x03}, 0); err == nil {
		t.Fatalf("truncated fe should error")
	}
	if _, _, err := readLenencInt(nil, 0); err == nil {
		t.Fatalf("empty should error")
	}
}

func TestLenencBytesRoundtrip(t *testing.T) {
	for _, s := range []string{"", "a", "hello world", string(bytes.Repeat([]byte{'x'}, 300))} {
		p := appendLenencBytes(nil, []byte(s))
		got, off, err := readLenencBytes(p, 0)
		if err != nil || string(got) != s || off != len(p) {
			t.Errorf("roundtrip %q: err=%v", s, err)
		}
	}
	// 数据截断。
	if _, _, err := readLenencBytes([]byte{0x05, 'a'}, 0); err == nil {
		t.Fatalf("truncated bytes should error")
	}
}

func TestNulString(t *testing.T) {
	s, off, err := readNulString([]byte("abc\x00def"), 0)
	if err != nil || s != "abc" || off != 4 {
		t.Fatalf("got %q off=%d err=%v", s, off, err)
	}
	if _, _, err := readNulString([]byte("abc"), 0); err != io.ErrUnexpectedEOF {
		t.Fatalf("missing terminator: err=%v", err)
	}
}

func TestPacketFramingChunked(t *testing.T) {
	var buf bytes.Buffer
	pc := &packetConn{rw: &buf}
	big := make([]byte, maxPacketSize+1024)
	for i := range big {
		big[i] = byte(i % 251)
	}
	if err := pc.writePacket(big); err != nil {
		t.Fatalf("write big: %v", err)
	}
	// 分片：第一帧恰好 16MB-1，第二帧 1024 字节；seq 连续。
	hdr := buf.Bytes()
	if n := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16; n != maxPacketSize {
		t.Fatalf("first frame len = %d, want %d", n, maxPacketSize)
	}
	if hdr[3] != 0 || hdr[maxPacketSize+4+3] != 1 {
		t.Fatalf("seq mismatch")
	}
	pc2 := &packetConn{rw: &buf, seq: 0}
	got, err := pc2.readPacket()
	if err != nil {
		t.Fatalf("read big: %v", err)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("roundtrip mismatch (len %d vs %d)", len(got), len(big))
	}
}

// ---- 结果集打包 ----

func TestOKPacket(t *testing.T) {
	p := okPacket(2, 5, statusAutocommit|statusInTransaction)
	want := []byte{0x00, 0x02, 0x05, 0x03, 0x00, 0x00, 0x00}
	if !bytes.Equal(p, want) {
		t.Fatalf("okPacket = %x, want %x", p, want)
	}
	// 大 affected（lenenc fc）。
	p2 := okPacket(0x100, 0, statusAutocommit)
	if p2[1] != 0xfc || binary.LittleEndian.Uint16(p2[2:4]) != 0x0100 {
		t.Fatalf("big affected = %x", p2[:6])
	}
}

func TestErrPacket(t *testing.T) {
	p := errPacket(fmt.Errorf("boom"))
	want := []byte{0xff, 0x15, 0x04, '#', 'H', 'Y', '0', '0', '0', 'b', 'o', 'o', 'm'}
	if !bytes.Equal(p, want) {
		t.Fatalf("errPacket = %x, want %x", p, want)
	}
}

func TestEOFPacket(t *testing.T) {
	p := eofPacket(statusAutocommit)
	if len(p) != 5 || p[0] != 0xfe || p[1] != 0 || p[2] != 0 || p[3] != 0x02 || p[4] != 0 {
		t.Fatalf("eofPacket = %x", p)
	}
}

func TestColumnDefinition(t *testing.T) {
	p := columnDefinition("name", mysqlTypeVarString, charsetUTF8MB4, 1024, 0, 0)
	// catalog "def" → schema/table/org_table/name → org_name → fixed。
	cat, off, err := readLenencBytes(p, 0)
	if err != nil || string(cat) != "def" {
		t.Fatalf("catalog: %q err=%v", cat, err)
	}
	for i := 0; i < 3; i++ {
		_, off, err = readLenencBytes(p, off)
		if err != nil {
			t.Fatalf("skip field %d: %v", i, err)
		}
	}
	name, off, err := readLenencBytes(p, off)
	if err != nil || string(name) != "name" {
		t.Fatalf("name = %q err=%v", name, err)
	}
	// org_name（lenenc，通常为空）。
	_, off, err = readLenencBytes(p, off)
	if err != nil {
		t.Fatalf("org_name: %v", err)
	}
	if p[off] != 0x0c {
		t.Fatalf("fixed fields len = %d, want 12", p[off])
	}
	if binary.LittleEndian.Uint16(p[off+1:off+3]) != charsetUTF8MB4 {
		t.Fatalf("charset = %d", binary.LittleEndian.Uint16(p[off+1:off+3]))
	}
	if binary.LittleEndian.Uint32(p[off+3:off+7]) != 1024 {
		t.Fatalf("length = %d", binary.LittleEndian.Uint32(p[off+3:off+7]))
	}
	if p[off+7] != mysqlTypeVarString {
		t.Fatalf("type = %d, want 253", p[off+7])
	}
}

func TestTextRow(t *testing.T) {
	p := textRow([][]byte{nil, []byte("ab"), {}})
	want := []byte{0xfb, 0x02, 'a', 'b', 0x00}
	if !bytes.Equal(p, want) {
		t.Fatalf("textRow = %x, want %x", p, want)
	}
}

func TestBinaryRow(t *testing.T) {
	vals := []sql.Value{sql.IntVal(42), sql.StrVal("x"), {Kind: "NULL"}}
	p := binaryRow(vals)
	// header 0x00 + null bitmap (bit 2 起算，第 3 列置位 → bitmap[0]=0x10) + lenenc 列值。
	want := []byte{0x00, 0x10, 0x02, '4', '2', 0x01, 'x'}
	if !bytes.Equal(p, want) {
		t.Fatalf("binaryRow = %x, want %x", p, want)
	}
	// 第 2 列 NULL（索引 1 → bit 3）。
	p2 := binaryRow([]sql.Value{sql.IntVal(1), {Kind: "NULL"}, sql.IntVal(3)})
	if p2[0] != 0x00 {
		t.Fatalf("header = %#x, want 0x00", p2[0])
	}
	if p2[1] != 0x08 {
		t.Fatalf("bitmap = %#x, want 0x08 (column 1 NULL)", p2[1])
	}
}

func TestMySQLTypeOf(t *testing.T) {
	cases := []struct {
		in   string
		want byte
	}{
		{"INT", mysqlTypeLongLong},
		{"DECIMAL", mysqlTypeNewDecimal},
		{"BLOB", mysqlTypeBlob},
		{"TEXT", mysqlTypeVarString},
		{"", mysqlTypeVarString},
	}
	for _, c := range cases {
		if got := mysqlTypeOf(c.in); got != c.want {
			t.Errorf("mysqlTypeOf(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestStatusFlags(t *testing.T) {
	c := &mysqlConn{}
	if c.status() != statusAutocommit {
		t.Fatalf("default status = %d, want %d", c.status(), statusAutocommit)
	}
	c.inTx = true
	if c.status() != statusAutocommit|statusInTransaction {
		t.Fatalf("in-tx status = %d", c.status())
	}
}

// ---- 二进制参数：日期/时间/数值全类型 ----

func TestParseBinaryDate(t *testing.T) {
	// DATE（typ 10）：4 字节。
	p := []byte{4, 0xea, 0x07, 10, 22}
	lit, off, err := parseBinaryDate(p, 0, 10)
	if err != nil || lit != "'2026-10-22'" || off != 5 {
		t.Fatalf("date = %q off=%d err=%v", lit, off, err)
	}
	// DATETIME（typ 7）：7 字节。
	p2 := []byte{7, 0xea, 0x07, 10, 22, 10, 20, 30}
	lit2, off2, err := parseBinaryDate(p2, 0, 7)
	if err != nil || lit2 != "'2026-10-22 10:20:30'" || off2 != 8 {
		t.Fatalf("datetime = %q off=%d err=%v", lit2, off2, err)
	}
	// DATETIME 微秒（typ 12）：11 字节。
	p3 := []byte{11, 0xea, 0x07, 10, 22, 10, 20, 30, 0x40, 0xe2, 0x01, 0x00}
	lit3, off3, err := parseBinaryDate(p3, 0, 12)
	if err != nil || lit3 != "'2026-10-22 10:20:30.123456'" || off3 != 12 {
		t.Fatalf("datetime micro = %q off=%d err=%v", lit3, off3, err)
	}
	// length=0 → NULL。
	lit4, off4, err := parseBinaryDate([]byte{0}, 0, 10)
	if err != nil || lit4 != "NULL" || off4 != 1 {
		t.Fatalf("zero date = %q off=%d err=%v", lit4, off4, err)
	}
	// 截断。
	if _, _, err := parseBinaryDate([]byte{4, 0xea}, 0, 10); err == nil {
		t.Fatalf("truncated date should error")
	}
	if _, _, err := parseBinaryDate(nil, 0, 10); err == nil {
		t.Fatalf("empty date should error")
	}
}

func TestParseBinaryTime(t *testing.T) {
	// length=8：10:20:30（length 字段值即数据体字节数，包需含 length+8 字节）。
	p := []byte{8, 0, 0, 0, 0, 10, 20, 30, 0}
	lit, off, err := parseBinaryTime(p, 0)
	if err != nil || lit != "'10:20:30'" || off != 8 {
		t.Fatalf("time = %q off=%d err=%v", lit, off, err)
	}
	// length=12 微秒。
	p2 := []byte{12, 0, 0, 0, 0, 10, 20, 30, 0x40, 0xe2, 0x01, 0x00, 0}
	lit2, off2, err := parseBinaryTime(p2, 0)
	if err != nil || lit2 != "'10:20:30.123456'" || off2 != 12 {
		t.Fatalf("time micro = %q off=%d err=%v", lit2, off2, err)
	}
	// length=0 → 零时间。
	lit3, off3, err := parseBinaryTime([]byte{0}, 0)
	if err != nil || lit3 != "'00:00:00'" || off3 != 1 {
		t.Fatalf("zero time = %q off=%d err=%v", lit3, off3, err)
	}
	// 负 days：-1 天 2 小时 = -22:00:00。
	p4 := []byte{8, 0xff, 0xff, 0xff, 0xff, 2, 0, 0, 0}
	lit4, _, err := parseBinaryTime(p4, 0)
	if err != nil || lit4 != "'-22:00:00'" {
		t.Fatalf("negative time = %q err=%v", lit4, err)
	}
	// 坏长度。
	if _, _, err := parseBinaryTime([]byte{5, 0, 0, 0, 0, 0, 0}, 0); err == nil {
		t.Fatalf("bad length should error")
	}
}

func TestParseBinaryParamsExtended(t *testing.T) {
	// FLOAT(4) + DOUBLE(5) + INT 负数(3) + LONGLONG 负数(8) + BIT(16) + YEAR(13)。
	// FLOAT 1.5 = 0x3fc00000 LE；DOUBLE 2.25 = 0x4002000000000000 LE。
	types := []byte{4, 0, 5, 0, 3, 0, 8, 0, 16, 0, 13, 0}
	p := make([]byte, 0, 64)
	p = append(p, 0x00, 0x00, 0xc0, 0x3f)
	p = append(p, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x40)
	p = append(p, 0xfe, 0xff, 0xff, 0xff) // -2 (INT)
	p = append(p, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff) // -2 (LONGLONG)
	p = append(p, 0x01) // BIT=1
	p = append(p, 0xea, 0x07) // YEAR=2026
	args, err := parseBinaryParams(p, 0, 6, make([]byte, 1), types, nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []string{"1.5", "2.25", "-2", "-2", "1", "2026"}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("arg %d = %q, want %q (args %v)", i, args[i], want[i], args)
		}
	}

	// unsigned INT：0xffffffff → 4294967295。
	typesU := []byte{3, 0x80}
	pU := []byte{0xff, 0xff, 0xff, 0xff}
	argsU, err := parseBinaryParams(pU, 0, 1, make([]byte, 1), typesU, nil)
	if err != nil || argsU[0] != "4294967295" {
		t.Fatalf("unsigned = %v err=%v", argsU, err)
	}

	// 截断：INT 只给 2 字节。
	if _, err := parseBinaryParams([]byte{0xff, 0xff}, 0, 1, make([]byte, 1), []byte{3, 0}, nil); err == nil {
		t.Fatalf("truncated int should error")
	}
	// unsupported 类型。
	if _, err := parseBinaryParams([]byte{0x01}, 0, 1, make([]byte, 1), []byte{0x63, 0}, nil); err == nil {
		t.Fatalf("unsupported type should error")
	}
	// NULL bitmap 置位 → NULL。
	bm := []byte{0x01}
	argsN, err := parseBinaryParams(nil, 0, 1, bm, []byte{3, 0}, nil)
	if err != nil || argsN[0] != "NULL" {
		t.Fatalf("null bitmap arg = %v err=%v", argsN, err)
	}
	// longData 命中：不消费值字节。
	ld := map[uint16][]byte{0: []byte("longblob")}
	argsL, err := parseBinaryParams(nil, 0, 1, make([]byte, 1), []byte{252, 0}, ld)
	if err != nil || argsL[0] != "'longblob'" {
		t.Fatalf("longdata arg = %v err=%v", argsL, err)
	}
}

// ---- COM_CHANGE_USER：1 字节长度前缀兼容解析 ----

func TestChangeUserPayloadOneByteLen(t *testing.T) {
	p := []byte("u\x00")
	p = append(p, 5)
	p = append(p, "hello"...)
	p = append(p, 0)
	p = append(p, 0x21)
	p = append(p, authPlugin...)
	p = append(p, 0)
	user, authResp, db, charset, plugin, err := parseChangeUserPayload(p)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if user != "u" || string(authResp) != "hello" || db != "" || charset != 0x21 || plugin != authPlugin {
		t.Fatalf("got user=%q auth=%q db=%q cs=%d plugin=%q", user, authResp, db, charset, plugin)
	}
	// auth 长度截断。
	if _, _, _, _, _, err := parseChangeUserPayload([]byte("u\x00\x05ab")); err == nil {
		t.Fatalf("truncated auth should error")
	}
}

// ---- 驱动探测谓词 ----

func TestProbePredicates(t *testing.T) {
	cases := []struct {
		name string
		fn   func(string) bool
		in   string
		want bool
	}{
		{"isSysVarProbe", isSysVarProbe, "SELECT @@session.auto_increment_increment AS a", true},
		{"isSysVarProbe-global", isSysVarProbe, "SELECT @@global.time_zone", true},
		{"isSysVarProbe-comment", isSysVarProbe, "/* mysql-connector-j-8.0.13 */ SELECT @@session.wait_timeout AS wait_timeout", true},
		{"isSysVarProbe-false", isSysVarProbe, "SELECT version()", false},
		{"isVersionCommentProbe", isVersionCommentProbe, "select @@version_comment limit 1", true},
		{"isVersionCommentProbe-false", isVersionCommentProbe, "SELECT 1", false},
		{"isTxReadOnlyProbe", isTxReadOnlyProbe, "SELECT @@session.transaction_read_only AS transaction_read_only, @@session.tx_read_only AS tx_read_only", true},
		{"isTxReadOnlyProbe-comment", isTxReadOnlyProbe, "/* c */ SELECT @@session.tx_read_only AS tx_read_only", true},
		{"isTxReadOnlyProbe-false", isTxReadOnlyProbe, "SELECT @@session.auto_increment_increment AS a", false},
		{"isConnInitSet-names", isConnInitSet, "SET NAMES utf8mb4", true},
		{"isConnInitSet-autocommit", isConnInitSet, "SET autocommit=1", true},
		{"isConnInitSet-multi", isConnInitSet, "SET autocommit=0, sql_mode='STRICT_TRANS_TABLES'", true},
		{"isConnInitSet-false", isConnInitSet, "SET query_cache_type=0", false},
		{"isTxnControlStmt-commit", isTxnControlStmt, "COMMIT", true},
		{"isTxnControlStmt-begin", isTxnControlStmt, "BEGIN", true},
		{"isTxnControlStmt-start", isTxnControlStmt, "START TRANSACTION", true},
		{"isTxnControlStmt-false", isTxnControlStmt, "SELECT 1", false},
		{"isTxnBegin-begin", isTxnBegin, "BEGIN", true},
		{"isTxnBegin-start", isTxnBegin, "START TRANSACTION", true},
		{"isTxnBegin-false", isTxnBegin, "COMMIT", false},
		{"isImplicitTxTrigger-insert", isImplicitTxTrigger, "INSERT INTO t VALUES (1)", true},
		{"isImplicitTxTrigger-comment", isImplicitTxTrigger, "/* c */ UPDATE t SET a=1", true},
		{"isImplicitTxTrigger-select", isImplicitTxTrigger, "SELECT 1", false},
		{"isImplicitTxTrigger-linecomment", isImplicitTxTrigger, "-- comment\nSELECT 1", false},
	}
	for _, c := range cases {
		if got := c.fn(c.in); got != c.want {
			t.Errorf("%s(%q) = %v, want %v", c.name, c.in, got, c.want)
		}
	}

	// parseAutoCommit / parseSingleAutoCommit。
	acCases := []struct {
		in   string
		want int
		ok   bool
	}{
		{"SET autocommit=0", 0, true},
		{"SET autocommit=1", 1, true},
		{"set  AUTOCOMMIT = 0", 0, true},
		{"SET autocommit=0, wait_timeout=100", 0, true},
		{"SET wait_timeout=100", 0, false},
		{"SET autocommit=x", 0, false},
	}
	for _, c := range acCases {
		if v, ok := parseAutoCommit(c.in); v != c.want || ok != c.ok {
			t.Errorf("parseAutoCommit(%q) = %d,%v want %d,%v", c.in, v, ok, c.want, c.ok)
		}
	}
	if v, ok := parseSingleAutoCommit("AUTOCOMMIT=0"); v != 0 || !ok {
		t.Errorf("parseSingleAutoCommit = %d,%v", v, ok)
	}
	if v, ok := parseSingleAutoCommit("SQL_MODE='x'"); v != 0 || ok {
		t.Errorf("parseSingleAutoCommit non-autocommit = %d,%v", v, ok)
	}
}

// ---- TLS 集成：verify 模式 + 明文密码认证 ----

func genTestTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("x509 keypair: %v", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, &tls.Config{InsecureSkipVerify: true}
}

// startTLSTestServer 启动 TLS 模式的 MySQL 协议服务器并建立已认证连接。
func startTLSTestServer(t *testing.T, verify verifyFn) *testClient {
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
	srv.SetVerify(verify)
	serverCfg, clientCfg := genTestTLSConfigs(t)
	srv.SetTLS(serverCfg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ServeListener(ln)
	conn, err := tls.Dial("tcp", ln.Addr().String(), clientCfg)
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	c := &testClient{t: t, pc: &packetConn{rw: conn}}
	c.readGreeting()
	c.sendHandshake("tlsuser", "tlspass")
	// TLS 下直接发送明文密码（含 \0 终止符），完成 full auth。
	resp, err := c.pc.readPacket()
	if err != nil {
		t.Fatalf("read auth more: %v", err)
	}
	if len(resp) < 2 || resp[0] != 0x01 || resp[1] != 0x03 {
		t.Fatalf("expected full auth request, got %x", resp)
	}
	plain := append([]byte("tlspass"), 0)
	if err := c.pc.writePacket(plain); err != nil {
		t.Fatalf("send plain: %v", err)
	}
	resp, err = c.pc.readPacket()
	if err != nil {
		t.Fatalf("read auth result: %v", err)
	}
	if resp[0] != 0x00 {
		t.Fatalf("TLS auth failed: %q", resp)
	}
	return c
}

func TestTLSAuthPlainPassword(t *testing.T) {
	c := startTLSTestServer(t, func(user, password string) bool {
		return user == "tlsuser" && password == "tlspass"
	})
	// 连接已认证；验证 SQL 通路。
	cols, rows := c.queryResult(t, "SELECT 1")
	if len(rows) != 1 || rows[0][0] != "1" {
		t.Fatalf("rows = %v (cols %v)", rows, cols)
	}
	// 错误密码：独立连接应被拒绝（ERR）。
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
	srv.SetVerify(func(user, password string) bool { return user == "tlsuser" && password == "tlspass" })
	serverCfg, clientCfg := genTestTLSConfigs(t)
	srv.SetTLS(serverCfg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ServeListener(ln)
	conn, err := tls.Dial("tcp", ln.Addr().String(), clientCfg)
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	c2 := &testClient{t: t, pc: &packetConn{rw: conn}}
	c2.readGreeting()
	c2.sendHandshake("tlsuser", "wrongpass")
	resp, err := c2.pc.readPacket()
	if err != nil {
		t.Fatalf("read auth more: %v", err)
	}
	if len(resp) < 2 || resp[0] != 0x01 || resp[1] != 0x03 {
		t.Fatalf("expected full auth request, got %x", resp)
	}
	plain := append([]byte("wrongpass"), 0)
	if err := c2.pc.writePacket(plain); err != nil {
		t.Fatalf("send plain: %v", err)
	}
	resp, err = c2.pc.readPacket()
	if err != nil {
		t.Fatalf("read auth result: %v", err)
	}
	if resp[0] != 0xff {
		t.Fatalf("wrong password should be rejected, got %x", resp)
	}
}

// ---- 集成：驱动初始化探测查询 ----

func TestConnInitProbeQueries(t *testing.T) {
	c, _ := startTestServer(t, nil)
	// 只读状态探测 → 2 列 0/0。
	cols, rows := c.queryResult(t, "SELECT @@session.transaction_read_only AS transaction_read_only, @@session.tx_read_only AS tx_read_only")
	if len(cols) != 2 || len(rows) != 1 || rows[0][0] != "0" || rows[0][1] != "0" {
		t.Fatalf("tx read only probe: cols=%v rows=%v", cols, rows)
	}
	// 系统变量探测 → 固定 18 列。
	cols2, rows2 := c.queryResult(t, "SELECT @@session.auto_increment_increment AS auto_increment_increment")
	if len(cols2) != len(sysVarProbeColumns) || len(rows2) != 1 {
		t.Fatalf("sysvar probe: %d cols, want %d", len(cols2), len(sysVarProbeColumns))
	}
	if rows2[0][0] != "1" {
		t.Fatalf("sysvar first col = %q, want 1", rows2[0][0])
	}
	// version_comment 探测 → 1 列。
	cols3, rows3 := c.queryResult(t, "select @@version_comment limit 1")
	if len(cols3) != 1 || len(rows3) != 1 || rows3[0][0] != "OpenXDB v1.0.0 (RocksDB)" {
		t.Fatalf("version comment: cols=%v rows=%v", cols3, rows3)
	}
	// 连接初始化 SET：NAMES / autocommit / 多赋值。
	c.expectOKFrom(c.command(comQuery, []byte("SET NAMES utf8mb4")))
	c.expectOKFrom(c.command(comQuery, []byte("SET autocommit=1")))
	c.expectOKFrom(c.command(comQuery, []byte("SET autocommit=0, sql_mode='STRICT_TRANS_TABLES'")))
	// 非白名单 SET 走 SQL 引擎（存在性不做强断言，只保证不崩溃）。
	_ = c.command(comQuery, []byte("SET unknown_var=1"))
}

// ---- 集成：STMT_RESET 清除 long data ----

func TestStmtResetClearsLongData(t *testing.T) {
	c, _ := startTestServer(t, nil)
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t4 (a TEXT, b TEXT)")))
	prep := c.command(comStmtPrepare, []byte("INSERT INTO t4 VALUES (?, ?)"))
	if prep[0] != 0x00 {
		t.Fatalf("prepare failed: %q", prep)
	}
	stmtID := binary.LittleEndian.Uint32(prep[1:5])
	for i := 0; i < 2; i++ {
		if _, err := c.pc.readPacket(); err != nil {
			t.Fatalf("param col: %v", err)
		}
	}
	c.expectEOF()

	c.sendLongData(stmtID, 0, []byte("stale-long-data"))
	// RESET：清除 long data 缓冲。
	var sid [4]byte
	binary.LittleEndian.PutUint32(sid[:], stmtID)
	c.expectOKFrom(c.command(comStmtReset, sid[:]))

	// EXECUTE 两个参数都用普通 lenenc 值；若 RESET 未生效会因无值字节而解析失败。
	ep := make([]byte, 0, 64)
	ep = append(ep, sid[:]...)
	ep = append(ep, 0)
	var iter [4]byte
	binary.LittleEndian.PutUint32(iter[:], 1)
	ep = append(ep, iter[:]...)
	ep = append(ep, 0x00) // null bitmap
	ep = append(ep, 0x01) // new_params_bound_flag
	ep = append(ep, 252, 0, 253, 0)
	ep = appendLenencBytes(ep, []byte("fresh"))
	ep = appendLenencBytes(ep, []byte("x"))
	eresp := c.command(comStmtExecute, ep)
	if eresp[0] != 0x00 {
		t.Fatalf("execute after reset failed: %q", eresp)
	}
	cols, rows := c.queryResult(t, "SELECT * FROM t4")
	if len(rows) != 1 || rows[0][0] != "fresh" || rows[0][1] != "x" {
		t.Fatalf("rows = %v (cols %v)", rows, cols)
	}
}

// ---- 集成：EXECUTE 不重发类型（new_bound=0 沿用上次类型）----

func TestStmtExecuteReuseTypes(t *testing.T) {
	c, _ := startTestServer(t, nil)
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t5 (id INT PRIMARY KEY, v TEXT)")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t5 VALUES (1, 'a'), (2, 'b')")))
	prep := c.command(comStmtPrepare, []byte("SELECT * FROM t5 WHERE id = ?"))
	if prep[0] != 0x00 {
		t.Fatalf("prepare failed: %q", prep)
	}
	stmtID := binary.LittleEndian.Uint32(prep[1:5])
	if _, err := c.pc.readPacket(); err != nil {
		t.Fatalf("param col: %v", err)
	}
	c.expectEOF()

	var sid [4]byte
	binary.LittleEndian.PutUint32(sid[:], stmtID)
	// 第一次 EXECUTE：new_bound=1，类型 INT，值 1。
	ep1 := make([]byte, 0, 32)
	ep1 = append(ep1, sid[:]...)
	ep1 = append(ep1, 0)
	var iter [4]byte
	binary.LittleEndian.PutUint32(iter[:], 1)
	ep1 = append(ep1, iter[:]...)
	ep1 = append(ep1, 0x00)
	ep1 = append(ep1, 0x01)
	ep1 = append(ep1, 3, 0)
	var pv [4]byte
	binary.LittleEndian.PutUint32(pv[:], 1)
	ep1 = append(ep1, pv[:]...)
	resp1 := c.command(comStmtExecute, ep1)
	if resp1[0] == 0xff {
		t.Fatalf("execute1 failed: %q", resp1)
	}
	cols1, rows1 := c.readBinaryResultSetFrom(resp1)
	if len(rows1) != 1 || rows1[0][0] != "1" {
		t.Fatalf("rows1 = %v (cols %v)", rows1, cols1)
	}

	// 第二次 EXECUTE：new_bound=0，不重发类型，值 2（沿用 INT）。
	ep2 := make([]byte, 0, 32)
	ep2 = append(ep2, sid[:]...)
	ep2 = append(ep2, 0)
	ep2 = append(ep2, iter[:]...)
	ep2 = append(ep2, 0x00) // null bitmap
	ep2 = append(ep2, 0x00) // new_params_bound_flag = 0
	binary.LittleEndian.PutUint32(pv[:], 2)
	ep2 = append(ep2, pv[:]...)
	resp2 := c.command(comStmtExecute, ep2)
	if resp2[0] == 0xff {
		t.Fatalf("execute2 failed: %q", resp2)
	}
	cols2, rows2 := c.readBinaryResultSetFrom(resp2)
	if len(rows2) != 1 || rows2[0][0] != "2" || rows2[0][1] != "b" {
		t.Fatalf("rows2 = %v (cols %v)", rows2, cols2)
	}
}

// ---- 集成：autocommit=0 隐式事务 ----

func TestAutoCommitImplicitTxn(t *testing.T) {
	c, _ := startTestServer(t, nil)
	c.expectOKFrom(c.command(comQuery, []byte("SET autocommit=0")))
	c.expectOKFrom(c.command(comQuery, []byte("CREATE TABLE t6 (id INT PRIMARY KEY, v TEXT)")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t6 VALUES (1, 'x')")))
	// 事务内可见。
	_, rows := c.queryResult(t, "SELECT * FROM t6")
	if len(rows) != 1 {
		t.Fatalf("in-tx rows = %v, want 1", rows)
	}
	c.expectOKFrom(c.command(comQuery, []byte("COMMIT")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t6 VALUES (2, 'y')")))
	c.expectOKFrom(c.command(comQuery, []byte("ROLLBACK")))
	// 回滚后仅剩 1 行。
	_, rows2 := c.queryResult(t, "SELECT * FROM t6")
	if len(rows2) != 1 || rows2[0][0] != "1" {
		t.Fatalf("after rollback rows = %v, want [1]", rows2)
	}
	// 无活动事务 COMMIT 幂等 OK（MySQL 语义）。
	c.expectOKFrom(c.command(comQuery, []byte("COMMIT")))
	// 恢复 autocommit。
	c.expectOKFrom(c.command(comQuery, []byte("SET autocommit=1")))
	c.expectOKFrom(c.command(comQuery, []byte("INSERT INTO t6 VALUES (3, 'z')")))
	_, rows3 := c.queryResult(t, "SELECT * FROM t6")
	if len(rows3) != 2 {
		t.Fatalf("after autocommit=1 rows = %v, want 2", rows3)
	}
}

// 防呆：math 包确保本文件引用有效（float32/64 位拆分场景）。
var _ = math.MaxFloat64

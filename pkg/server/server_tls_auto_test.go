package server

// T36 商用档：AllowLegacy 明文/TLS 并存（AutoListener）与安全默认（纯 TLS）测试。

import (
	"bufio"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// TestAutoListenerMixedPlainAndTLS：SetTLSLegacy(true) 后同一端口明文与 TLS 连接并存，
// 行协议往返均正常。
func TestAutoListenerMixedPlainAndTLS(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	s.SetTLS(serverTLSCfg(t))
	s.SetTLSLegacy(true)

	addr := waitServeTCPAddr(t, s)

	// 明文分支
	plain, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("plain dial: %v", err)
	}
	defer plain.Close()
	_ = plain.SetDeadline(time.Now().Add(5 * time.Second))
	plainRd := bufio.NewReader(plain)
	if got := roundtrip(t, plain, plainRd, "PING"); got != "PONG" {
		t.Fatalf("plain PING over mixed listener: want PONG, got %q", got)
	}
	if got := roundtrip(t, plain, plainRd, "SET autokey autoval"); got != "OK" {
		t.Fatalf("plain SET: %q", got)
	}
	if got := roundtrip(t, plain, plainRd, "GET autokey"); got != "autoval" {
		t.Fatalf("plain GET: want autoval, got %q", got)
	}

	// TLS 分支（自签名 Insecure 客户端）
	tconn, err := tls.Dial("tcp", addr, InsecureTLSConfig())
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer tconn.Close()
	_ = tconn.SetDeadline(time.Now().Add(5 * time.Second))
	tlsRd := bufio.NewReader(tconn)
	if got := roundtrip(t, tconn, tlsRd, "PING"); got != "PONG" {
		t.Fatalf("TLS PING over mixed listener: want PONG, got %q", got)
	}
	if got := roundtrip(t, tconn, tlsRd, "SET tlskey tlsval"); got != "OK" {
		t.Fatalf("TLS SET: %q", got)
	}
	if got := roundtrip(t, tconn, tlsRd, "GET tlskey"); got != "tlsval" {
		t.Fatalf("TLS GET: want tlsval, got %q", got)
	}
}

// TestTLSOnlyRejectsPlainClient：未开 AllowLegacy 时纯 TLS 监听拒绝明文客户端
// （明文首字节不是 TLS ClientHello，服务端握手失败即断开）。
func TestTLSOnlyRejectsPlainClient(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	s.SetTLS(serverTLSCfg(t)) // 默认 tlsAllowLegacy=false → 纯 TLS

	addr := waitServeTCPAddr(t, s)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("plain dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := conn.Write([]byte("PING\n")); err != nil {
		t.Fatalf("write PING: %v", err)
	}
	buf := make([]byte, 128)
	n, err := conn.Read(buf)
	if err == nil {
		t.Fatalf("expected TLS handshake failure (EOF/error), got %q", string(buf[:n]))
	}
	// 允许 TLS alert 或 EOF，但绝不应当返回 PONG
	if n > 0 && string(buf[:n]) == "PONG" {
		t.Fatalf("plaintext client unexpectedly got PONG from TLS-only listener")
	}
}

// TestAutoConnSecureFlag：AutoListener 按首字节识别后，明文连接 Secure()=false、
// TLS 连接 Secure()=true（会话安全标记依据）。
func TestAutoConnSecureFlag(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer raw.Close()
	auto := NewAutoListener(raw, serverTLSCfg(t))

	// 明文分支
	plain, err := net.Dial("tcp", raw.Addr().String())
	if err != nil {
		t.Fatalf("plain dial: %v", err)
	}
	if _, err := plain.Write([]byte("PING\n")); err != nil {
		t.Fatalf("plain write: %v", err)
	}
	pc, err := auto.Accept()
	if err != nil {
		t.Fatalf("accept plain: %v", err)
	}
	ac, ok := pc.(*autoConn)
	if !ok {
		t.Fatalf("expected *autoConn, got %T", pc)
	}
	if ac.Secure() {
		t.Fatalf("plaintext connection must report secure=false")
	}
	plain.Close()

	// TLS 分支（客户端先发起 ClientHello）：这里不跑完整 TLS 握手
	// （服务端未进入 REPL 循环、不主动握手），只发 TLS 记录头
	// （ContentType=0x16 handshake）验证 AutoListener 按首字节识别。
	tc, err := net.Dial("tcp", raw.Addr().String())
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	if _, err := tc.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x05}); err != nil {
		t.Fatalf("write clienthello head: %v", err)
	}
	tc2, err := auto.Accept()
	if err != nil {
		t.Fatalf("accept tls: %v", err)
	}
	ac2, ok := tc2.(*autoConn)
	if !ok {
		t.Fatalf("expected *autoConn, got %T", tc2)
	}
	if !ac2.Secure() {
		t.Fatalf("TLS connection must report secure=true")
	}
	tc.Close()
}

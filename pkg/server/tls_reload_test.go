package server

// T36 商用档增强：证书热轮换。覆盖 AutoListener.SetConfig 原子切换与
// Server.ReloadTLS 文件级重载（旧连接不中断、新连接使用新证书、失败保留旧配置）。

import (
	"bufio"
	"crypto/tls"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// serverTLSConfigFromFiles 由 cert/key 文件构建服务端 TLS 配置。
func serverTLSConfigFromFiles(t *testing.T, certFile, keyFile string) *tls.Config {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}

// tlsDialPing 建立 TLS 连接并执行 PING，返回连接（供后续复用）与响应。
func tlsDialPing(t *testing.T, addr string, cfg *tls.Config) (net.Conn, string) {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	rd := bufio.NewReader(conn)
	if got := roundtrip(t, conn, rd, "PING"); got != "PONG" {
		conn.Close()
		t.Fatalf("TLS PING: want PONG, got %q", got)
	}
	return conn, ""
}

// serveAutoConn 循环 Accept 并为每条连接提供 PING→PONG 应答（测试用简易服务端）。
func serveAutoConn(t *testing.T, auto *AutoListener) {
	t.Helper()
	for {
		c, err := auto.Accept()
		if err != nil {
			return // 监听器关闭
		}
		go func(conn net.Conn) {
			defer conn.Close()
			rd := bufio.NewReader(conn)
			for {
				line, err := rd.ReadString('\n')
				if err != nil {
					return
				}
				cmd := line
				if len(cmd) > 0 && cmd[len(cmd)-1] == '\n' {
					cmd = cmd[:len(cmd)-1]
				}
				if cmd == "PING" {
					_, _ = conn.Write([]byte("PONG\n"))
				} else if cmd != "" {
					_, _ = conn.Write([]byte("OK\n"))
				}
			}
		}(c)
	}
}

// TestAutoListenerConfigRotate：SetConfig 原子切换后，新连接使用新证书
// （CAFileTLSConfig 校验），已建立连接继续以旧配置服务。
func TestAutoListenerConfigRotate(t *testing.T) {
	certA, keyA := genTestCert(t, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil)
	certB, keyB := genTestCert(t, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil)

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer raw.Close()
	addr := raw.Addr().String()
	auto := NewAutoListener(raw, serverTLSConfigFromFiles(t, certA, keyA))
	go serveAutoConn(t, auto)

	// 连接1：证书A 配置下建立，保持连接。
	conn1, _ := tlsDialPing(t, addr, InsecureTLSConfig())
	defer conn1.Close()
	rd1 := bufio.NewReader(conn1)

	// 轮换到证书B。
	auto.SetConfig(serverTLSConfigFromFiles(t, certB, keyB))

	// 连接2：以证书B 为信任根校验握手成功；以证书A 为信任根校验必须失败。
	if _, err := tls.Dial("tcp", addr, mustCACfg(t, certB, "127.0.0.1")); err != nil {
		t.Fatalf("new conn with cert B must verify OK: %v", err)
	}
	if _, err := tls.Dial("tcp", addr, mustCACfg(t, certA, "127.0.0.1")); err == nil {
		t.Fatalf("new conn with cert A must be rejected after rotate")
	}

	// 旧连接1 仍可继续使用（沿用连接建立时的证书A 配置）。
	_ = conn1.SetDeadline(time.Now().Add(5 * time.Second))
	if got := roundtrip(t, conn1, rd1, "PING"); got != "PONG" {
		t.Fatalf("old conn after rotate: want PONG, got %q", got)
	}
}

// TestServerReloadTLS：Server.ReloadTLS 文件级热轮换（AllowLegacy 模式）：
// 新连接以新证书校验成功、旧证书校验失败；明文分支不受影响；
// 重载失败保留旧配置且服务不中断。
func TestServerReloadTLS(t *testing.T) {
	certA, keyA := genTestCert(t, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil)
	certB, keyB := genTestCert(t, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil)

	s, closeFn := newTestServer(t)
	defer closeFn()
	s.SetTLS(serverTLSConfigFromFiles(t, certA, keyA))
	s.SetTLSLegacy(true)
	addr := waitServeTCPAddr(t, s)

	// 轮换前：新连接以证书A 校验成功、证书B 校验失败。
	if _, err := tls.Dial("tcp", addr, mustCACfg(t, certA, "127.0.0.1")); err != nil {
		t.Fatalf("pre-reload conn with cert A must verify OK: %v", err)
	}
	if _, err := tls.Dial("tcp", addr, mustCACfg(t, certB, "127.0.0.1")); err == nil {
		t.Fatalf("pre-reload conn with cert B must be rejected")
	}

	// ReloadTLS 切到证书B。
	if err := s.ReloadTLS(certB, keyB); err != nil {
		t.Fatalf("ReloadTLS: %v", err)
	}
	if _, err := tls.Dial("tcp", addr, mustCACfg(t, certB, "127.0.0.1")); err != nil {
		t.Fatalf("post-reload conn with cert B must verify OK: %v", err)
	}
	if _, err := tls.Dial("tcp", addr, mustCACfg(t, certA, "127.0.0.1")); err == nil {
		t.Fatalf("post-reload conn with cert A must be rejected")
	}

	// 明文分支不受影响。
	plain, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("plain dial: %v", err)
	}
	defer plain.Close()
	_ = plain.SetDeadline(time.Now().Add(5 * time.Second))
	plainRd := bufio.NewReader(plain)
	if got := roundtrip(t, plain, plainRd, "PING"); got != "PONG" {
		t.Fatalf("plain PING after reload: want PONG, got %q", got)
	}

	// 重载失败：路径不存在 → 报错，旧配置（证书B）保留且服务可用。
	bad := filepath.Join(t.TempDir(), "missing.pem")
	if err := s.ReloadTLS(bad, bad); err == nil {
		t.Fatalf("ReloadTLS with missing files must fail")
	}
	if _, err := tls.Dial("tcp", addr, mustCACfg(t, certB, "127.0.0.1")); err != nil {
		t.Fatalf("conn after failed reload must still use cert B: %v", err)
	}
}

// mustCACfg 以 caCert 为信任根构建 verify-ca 客户端配置（自签名证书用自身作根）。
func mustCACfg(t *testing.T, caCert, serverName string) *tls.Config {
	t.Helper()
	cfg, err := CAFileTLSConfig(caCert, serverName)
	if err != nil {
		t.Fatalf("CAFileTLSConfig: %v", err)
	}
	return cfg
}

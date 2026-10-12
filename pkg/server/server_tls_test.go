package server

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// genSelfSignedCert 生成开发/测试用自签名 RSA 证书（P0 TLS），返回
// certFile / keyFile 路径（位于 t.TempDir()，不进入仓库）。
func genSelfSignedCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "openxdb-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("x509.CreateCertificate: %v", err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

// serverTLSCfg 由 genSelfSignedCert 产物构造服务端 *tls.Config。
func serverTLSCfg(t *testing.T) *tls.Config {
	t.Helper()
	certFile, keyFile := genSelfSignedCert(t)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}

// waitServeTCPAddr 在 goroutine 中运行 s.ServeTCP("127.0.0.1:0")，轮询
// 监听器就绪后返回实际监听地址（tls.Listen 在 ServeListener 内记录 s.ln）。
func waitServeTCPAddr(t *testing.T, s *Server) string {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- s.ServeTCP("127.0.0.1:0") }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.lnMu.Lock()
		ln := s.ln
		s.lnMu.Unlock()
		if ln != nil {
			return ln.Addr().String()
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-errCh:
		t.Fatalf("ServeTCP returned early: %v", err)
	default:
	}
	t.Fatalf("ServeTCP listener did not become ready")
	return ""
}

// roundtrip 通过已有连接完成一行命令往返。
func roundtrip(t *testing.T, conn net.Conn, rd *bufio.Reader, line string) string {
	t.Helper()
	if _, err := conn.Write([]byte(line + "\n")); err != nil {
		t.Fatalf("write %q: %v", line, err)
	}
	resp, err := rd.ReadString('\n')
	if err != nil {
		t.Fatalf("read after %q: %v", line, err)
	}
	return strings.TrimSpace(resp)
}

// TestServeTCPWithTLSAndClientDial：TLS 监听 + 自签名客户端 PING/SET/GET/QUIT。
func TestServeTCPWithTLSAndClientDial(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	s.SetTLS(serverTLSCfg(t))

	addr := waitServeTCPAddr(t, s)
	conn, err := tls.Dial("tcp", addr, InsecureTLSConfig())
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	rd := bufio.NewReader(conn)

	if got := roundtrip(t, conn, rd, "PING"); got != "PONG" {
		t.Fatalf("TLS PING: want PONG, got %q", got)
	}
	if got := roundtrip(t, conn, rd, "SET tlskey tlsval"); got != "OK" {
		t.Fatalf("TLS SET: %q", got)
	}
	if got := roundtrip(t, conn, rd, "GET tlskey"); got != "tlsval" {
		t.Fatalf("TLS GET: want tlsval, got %q", got)
	}
	if got := roundtrip(t, conn, rd, "QUIT"); got != "BYE" {
		t.Fatalf("TLS QUIT: want BYE, got %q", got)
	}
}

// TestDialPlainRoundtrip：server.Dial(nil) 明文连接基线（TLS 未配置时
// 行为与旧版 net.Dial 一致，无回归）。
func TestDialPlainRoundtrip(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()

	addr := waitServeTCPAddr(t, s) // 未 SetTLS → 明文监听
	conn, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("plain dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	rd := bufio.NewReader(conn)

	if got := roundtrip(t, conn, rd, "PING"); got != "PONG" {
		t.Fatalf("plain PING: want PONG, got %q", got)
	}
	if got := roundtrip(t, conn, rd, "SET plainkey plainval"); got != "OK" {
		t.Fatalf("plain SET: %q", got)
	}
	if got := roundtrip(t, conn, rd, "GET plainkey"); got != "plainval" {
		t.Fatalf("plain GET: want plainval, got %q", got)
	}
}

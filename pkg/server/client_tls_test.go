package server

// T36 商用档：客户端证书校验（VerifyTLSConfig verify-full / CAFileTLSConfig verify-ca）。
// 覆盖合法证书、自签名、过期证书、主机名不匹配、CA 链不匹配。

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
	"testing"
	"time"
)

// genTestCert 生成自签名 RSA 证书（可自定义有效期与 DNS SAN），返回 cert/key 路径。
func genTestCert(t *testing.T, notBefore, notAfter time.Time, dnsNames []string) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "openxdb-test"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dnsNames,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("x509.CreateCertificate: %v", err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
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

// TestVerifyTLSConfigRequiresServerName：verify-full 必须提供 serverName。
func TestVerifyTLSConfigRequiresServerName(t *testing.T) {
	certFile, _ := genTestCert(t, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil)
	if _, err := VerifyTLSConfig(certFile, ""); err == nil {
		t.Fatalf("VerifyTLSConfig with empty serverName must fail")
	}
}

// TestVerifyTLSConfigValidCert：合法证书 + 匹配主机名 → 握手与行协议成功。
func TestVerifyTLSConfigValidCert(t *testing.T) {
	certFile, keyFile := genTestCert(t, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	s, closeFn := newTestServer(t)
	defer closeFn()
	s.SetTLS(&tls.Config{Certificates: []tls.Certificate{cert}})
	addr := waitServeTCPAddr(t, s)

	cfg, err := VerifyTLSConfig(certFile, "127.0.0.1")
	if err != nil {
		t.Fatalf("VerifyTLSConfig: %v", err)
	}
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		t.Fatalf("verified tls dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if got := roundtrip(t, conn, bufio.NewReader(conn), "PING"); got != "PONG" {
		t.Fatalf("verified TLS PING: want PONG, got %q", got)
	}
}

// TestVerifyTLSConfigHostnameMismatch：证书合法但主机名不匹配 → 握手失败。
func TestVerifyTLSConfigHostnameMismatch(t *testing.T) {
	certFile, keyFile := genTestCert(t, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	s, closeFn := newTestServer(t)
	defer closeFn()
	s.SetTLS(&tls.Config{Certificates: []tls.Certificate{cert}})
	addr := waitServeTCPAddr(t, s)

	cfg, err := VerifyTLSConfig(certFile, "wrong.example.com")
	if err != nil {
		t.Fatalf("VerifyTLSConfig: %v", err)
	}
	conn, err := tls.Dial("tcp", addr, cfg)
	if err == nil {
		conn.Close()
		t.Fatalf("hostname mismatch must fail TLS handshake")
	}
}

// TestVerifyTLSConfigExpiredCert：证书已过期 → 握手失败。
func TestVerifyTLSConfigExpiredCert(t *testing.T) {
	certFile, keyFile := genTestCert(t, time.Now().Add(-48*time.Hour), time.Now().Add(-24*time.Hour), nil)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	s, closeFn := newTestServer(t)
	defer closeFn()
	s.SetTLS(&tls.Config{Certificates: []tls.Certificate{cert}})
	addr := waitServeTCPAddr(t, s)

	cfg, err := VerifyTLSConfig(certFile, "127.0.0.1")
	if err != nil {
		t.Fatalf("VerifyTLSConfig: %v", err)
	}
	conn, err := tls.Dial("tcp", addr, cfg)
	if err == nil {
		conn.Close()
		t.Fatalf("expired certificate must fail TLS handshake")
	}
}

// TestCAFileTLSConfigChainTrust：verify-ca 信任对应 CA；不匹配 CA 拒绝。
func TestCAFileTLSConfigChainTrust(t *testing.T) {
	certFile, keyFile := genTestCert(t, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil)
	otherCert, otherKey := genTestCert(t, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil)
	_ = otherKey

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	s, closeFn := newTestServer(t)
	defer closeFn()
	s.SetTLS(&tls.Config{Certificates: []tls.Certificate{cert}})
	addr := waitServeTCPAddr(t, s)

	// 匹配 CA → 成功
	cfg, err := CAFileTLSConfig(certFile, "")
	if err != nil {
		t.Fatalf("CAFileTLSConfig: %v", err)
	}
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		t.Fatalf("trusted CA dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if got := roundtrip(t, conn, bufio.NewReader(conn), "PING"); got != "PONG" {
		t.Fatalf("trusted CA PING: want PONG, got %q", got)
	}
	conn.Close()

	// 不匹配 CA → 握手失败（证书链不受信任）
	cfg2, err := CAFileTLSConfig(otherCert, "")
	if err != nil {
		t.Fatalf("CAFileTLSConfig(other): %v", err)
	}
	conn2, err := tls.Dial("tcp", addr, cfg2)
	if err == nil {
		conn2.Close()
		t.Fatalf("untrusted CA must fail TLS handshake")
	}
}

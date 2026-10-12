package main

import (
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

// genCertPair 生成自签名证书（main 包测试专用，写 t.TempDir()）。
func genCertPair(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "openxdb-cli-test"},
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
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

// TestLoadTLSConfigNone：未提供 TLS 选项 → (nil, nil)，保持明文监听。
func TestLoadTLSConfigNone(t *testing.T) {
	cfg, err := loadTLSConfig([]string{"--data-dir", "x", "--port", "7788"})
	if err != nil {
		t.Fatalf("loadTLSConfig: %v", err)
	}
	if cfg != nil {
		t.Fatalf("cfg = %+v, want nil", cfg)
	}
}

// TestLoadTLSConfigOnlyCert：仅 --tls-cert → 报错（必须成对提供）。
func TestLoadTLSConfigOnlyCert(t *testing.T) {
	_, err := loadTLSConfig([]string{"--tls-cert", "cert.pem"})
	if err == nil || !strings.Contains(err.Error(), "must provide both") {
		t.Fatalf("want must provide both error, got %v", err)
	}
}

// TestLoadTLSConfigOnlyKey：仅 --tls-key → 报错（必须成对提供）。
func TestLoadTLSConfigOnlyKey(t *testing.T) {
	_, err := loadTLSConfig([]string{"--tls-key", "key.pem"})
	if err == nil || !strings.Contains(err.Error(), "must provide both") {
		t.Fatalf("want must provide both error, got %v", err)
	}
}

// TestLoadTLSConfigBadFiles：成对提供但证书文件无效 → 报错。
func TestLoadTLSConfigBadFiles(t *testing.T) {
	_, err := loadTLSConfig([]string{"--tls-cert", "nope.pem", "--tls-key", "nope.key"})
	if err == nil || !strings.Contains(err.Error(), "load TLS cert/key") {
		t.Fatalf("want load TLS cert/key error, got %v", err)
	}
}

// TestLoadTLSConfigValid：成对提供有效自签名证书 → 返回非 nil tls.Config。
func TestLoadTLSConfigValid(t *testing.T) {
	certFile, keyFile := genCertPair(t)
	cfg, err := loadTLSConfig([]string{"--tls-cert", certFile, "--tls-key", keyFile})
	if err != nil {
		t.Fatalf("loadTLSConfig: %v", err)
	}
	if cfg == nil {
		t.Fatalf("cfg = nil, want non-nil")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("certificates = %d, want 1", len(cfg.Certificates))
	}
	// 证书可被服务端 tls.Listen 使用（冒烟级验证）。
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer ln.Close()
}

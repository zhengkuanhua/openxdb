package replication

import (
	"context"
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

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// genSelfSignedCertTLS 生成自签名证书（P0 TLS 复制端口测试），返回
// certFile/keyFile（t.TempDir()，不入仓库）。
func genSelfSignedCertTLS(t *testing.T) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "openxdb-repl-test"},
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
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

// TestMasterTLSFollowerTLSAppliedValue：TLS 复制端口 + TLS 从节点完成一条
// binlog 复制并校验落地值，验证 P0 TLS 复制链路端到端可用。
func TestMasterTLSFollowerTLSAppliedValue(t *testing.T) {
	certFile, keyFile := genSelfSignedCertTLS(t)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	serverCfg := &tls.Config{Certificates: []tls.Certificate{cert}}

	path, _ := mkBinlog(t)
	m, err := NewMasterTLS(path, "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatalf("NewMasterTLS: %v", err)
	}
	defer m.Close()
	if err := m.Serve(); err != nil {
		t.Fatalf("Serve: %v", err)
	}

	store := newBtree()
	f := NewFollower(store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.StartTLS(ctx, m.Addr(), &tls.Config{InsecureSkipVerify: true})

	if err := m.Append(&BinlogEntry{Batch: &storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("tk"), Value: []byte("tv")}}}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	waitApplied(t, f, 1)
	got, err := store.Get([]byte("tk"))
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if string(got) != "tv" {
		t.Fatalf("replicated value = %q, want %q", string(got), "tv")
	}
}

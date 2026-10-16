// gen_cert.go — T36 商用档：自签名证书生成子命令（openxdb gen-cert）。
//
// 仅使用 Go 标准库（crypto/x509 / crypto/ecdsa / encoding/pem），无第三方依赖。
// 生成 ECDSA P-256 自签名证书：
//   - Subject CN=--cn（默认 localhost），SAN=DNS:localhost, IP:127.0.0.1（+::1）
//   - NotBefore=now-1h（容忍时钟偏差），NotAfter=now+--days（默认 365）
//   - 私钥 PKCS#8 PEM；证书 X.509 PEM
//   - 已存在文件不覆盖（安全）；--force 覆盖
// 配合 start --tls-cert/--tls-key 使用；商用环境请替换为正式 CA 签发证书。
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"strconv"
	"time"
)

func cmdGenCert(args []string) error {
	certPath, _ := flagValue(args, "--cert")
	keyPath, _ := flagValue(args, "--key")
	outDir := ""
	if p, ok := flagValue(args, "--out-dir"); ok {
		outDir = p
	}
	cn := "localhost"
	if v, ok := flagValue(args, "--cn"); ok {
		cn = v
	}
	days := 365
	if v, ok := flagValue(args, "--days"); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 3650 {
			return errors.New("invalid --days value: " + v + " (expected integer 1..3650)")
		}
		days = n
	}
	force := false
	for _, a := range args {
		if a == "--force" {
			force = true
		}
	}
	// 默认输出：out-dir 下 tls-cert.pem / tls-key.pem。
	if certPath == "" {
		certPath = "tls-cert.pem"
	}
	if keyPath == "" {
		keyPath = "tls-key.pem"
	}
	if outDir != "" {
		certPath = outDir + string(os.PathSeparator) + certPath
		keyPath = outDir + string(os.PathSeparator) + keyPath
	}
	if !force {
		for _, p := range []string{certPath, keyPath} {
			if _, err := os.Stat(p); err == nil {
				return fmt.Errorf("refusing to overwrite existing file: %s (use --force to override)", p)
			}
		}
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate ECDSA key: %w", err)
	}
	now := time.Now()
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return fmt.Errorf("generate serial: %w", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(time.Duration(days) * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		DNSNames:              []string{"localhost", cn},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("create certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal private key: %w", err)
	}
	if err := writePEMFile(certPath, "CERTIFICATE", der, force); err != nil {
		return err
	}
	if err := writePEMFile(keyPath, "PRIVATE KEY", keyDER, force); err != nil {
		return err
	}
	fmt.Printf("generated self-signed TLS certificate (ECDSA P-256, CN=%s, %d days):\n", cn, days)
	fmt.Printf("  cert: %s\n", certPath)
	fmt.Printf("  key:  %s\n", keyPath)
	fmt.Printf("usage: openxdb start --data-dir <dir> --tls-cert %s --tls-key %s\n", certPath, keyPath)
	fmt.Printf("       (or SET PERSIST tls_cert/tls_key/tls_allow_legacy for restart-effective config)\n")
	return nil
}

// writePEMFile 写 PEM 文件；非 force 且文件已存在时报错不覆盖。
func writePEMFile(path, blockType string, der []byte, force bool) error {
	if !force {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("refusing to overwrite existing file: %s (use --force to override)", path)
		}
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		return fmt.Errorf("encode pem %s: %w", path, err)
	}
	return nil
}

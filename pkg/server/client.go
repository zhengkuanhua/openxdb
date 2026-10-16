package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"
)

// 本文件提供行协议 TCP 客户端连接能力（P0 TLS）：与服务端 ServeTCP 配套，
// 供测试工具/外部客户端复用。tlsCfg 为 nil 时走明文连接（默认行为），
// 非 nil 时走 TLS 连接（客户端侧由调用方决定证书校验策略）。

// DialTimeout 建立到行协议服务的客户端连接：tlsCfg == nil 时明文 TCP，
// 否则 TLS 加密连接。允许调用方传入 InsecureSkipVerify（自签名证书/内网
// 测试场景）或指定 ServerName 做标准校验。
func Dial(addr string, tlsCfg *tls.Config) (net.Conn, error) {
	return DialTimeout(addr, tlsCfg, 5*time.Second)
}

// DialTimeout 与 Dial 等价，额外指定连接建立超时。
func DialTimeout(addr string, tlsCfg *tls.Config, timeout time.Duration) (net.Conn, error) {
	d := &net.Dialer{Timeout: timeout}
	if tlsCfg == nil {
		return d.Dial("tcp", addr)
	}
	return tls.DialWithDialer(d, "tcp", addr, tlsCfg)
}

// InsecureTLSConfig 返回适合内网测试/自签名证书的客户端 TLS 配置：
// 不校验服务器证书链与服务器名（InsecureSkipVerify）。生产环境应改为
// 校验 CA 与 ServerName 的标准配置。
func InsecureTLSConfig() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true}
}

// VerifyTLSConfig 返回商用校验的客户端 TLS 配置（T36 verify-full 语义）：
// 以 caPath（PEM CA 证书）为信任根校验服务器证书链，并以 serverName
// 校验证书主机名（SAN/CN）。证书过期、链不可信、主机名不匹配均握手失败。
// serverName 必填（verify-full 必须校验主机名）；仅校验链用 CAFileTLSConfig。
func VerifyTLSConfig(caPath, serverName string) (*tls.Config, error) {
	if serverName == "" {
		return nil, fmt.Errorf("client: VerifyTLSConfig requires non-empty serverName")
	}
	roots, err := loadCACertPool(caPath)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		RootCAs:    roots,
		ServerName: serverName,
	}, nil
}

// CAFileTLSConfig 返回仅校验证书链、不校验主机名的客户端 TLS 配置
// （T36 verify-ca 语义）：以 caPath 为信任根校验链；serverName 传空时
// 不校验主机名（适合证书 SAN 与连接地址不一致的内网场景）。
func CAFileTLSConfig(caPath, serverName string) (*tls.Config, error) {
	roots, err := loadCACertPool(caPath)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		RootCAs:    roots,
		ServerName: serverName,
		// Go 的 TLS 客户端默认在 ServerName 非空时校验主机名；传空则
		// 仅校验链。此函数保留 serverName 参数供调用方选择 verify-full。
	}, nil
}

// loadCACertPool 读取 PEM CA 证书文件并构建信任根池。
func loadCACertPool(caPath string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("client: read CA file %s: %w", caPath, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("client: no valid CA certificates in %s", caPath)
	}
	return roots, nil
}

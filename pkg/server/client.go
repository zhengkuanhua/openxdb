package server

import (
	"crypto/tls"
	"net"
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

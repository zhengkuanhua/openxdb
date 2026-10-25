package server

// T36 商用档：AutoTLS 监听器（AllowLegacy 语义）。
//
// 同一端口自动识别 TLS 与明文连接：Accept 后缓冲读取首字节，
// 0x16 为 TLS 记录层 ContentType=handshake（TLS ClientHello）→ 以
// tls.Server 包装为加密连接；否则按明文连接处理。行协议首命令均为
// ASCII 字母（PING/AUTH/HELP/SQL…），首字节不可能为 0x16，探测无歧义。
//
// 安全默认：仅当服务端显式开启 AllowLegacy（--tls-allow-legacy 或
// SET PERSIST tls_allow_legacy=on）时 ServeTCP 使用本监听器；默认配置
// 证书后为纯 TLS 监听，明文连接 TLS 握手失败即断开。

import (
	"bufio"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"time"
)

// NewAutoListener 包装底层监听器：peek 首字节识别 TLS ClientHello 并
// 决定连接类型（tls.Server 或明文）。config 为服务端 TLS 配置（非 nil）。
func NewAutoListener(ln net.Listener, config *tls.Config) *AutoListener {
	l := &AutoListener{Listener: ln}
	l.SetConfig(config)
	return l
}

// AutoListener 在同一端口提供明文与 TLS 并存监听（T36 AllowLegacy）。
// config 经 SetConfig 原子替换，支持证书热轮换：新 Accept 的连接使用
// 最新配置，已建立连接继续使用其握手时的配置直至断开。
type AutoListener struct {
	net.Listener
	mu     sync.RWMutex
	config *tls.Config
}

// SetConfig 原子替换监听层 TLS 配置（T36 证书热轮换）：仅影响后续
// Accept 建立的连接；已建立连接不受影响。
func (l *AutoListener) SetConfig(config *tls.Config) {
	l.mu.Lock()
	l.config = config
	l.mu.Unlock()
}

// currentConfig 返回当前 TLS 配置（Accept 时读取）。
func (l *AutoListener) currentConfig() *tls.Config {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.config
}

// Accept 接受一条连接并按首字节判定类型；判定失败（对端提前关闭等）
// 关闭连接并返回原始连接（上层协议循环读到 EOF 后正常退出）。
func (l *AutoListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return l.wrap(c), nil
}

func (l *AutoListener) wrap(c net.Conn) net.Conn {
	br := bufio.NewReader(c)
	head, err := br.Peek(1)
	if err != nil {
		// 对端未发数据即关闭：关闭连接，上层按空连接处理。
		_ = c.Close()
		return c
	}
	if head[0] == 0x16 {
		// TLS ClientHello：包装为 TLS 连接（握手在首次读写时完成）。
		// 关键：tls.Server 必须基于 bufferedConn，否则 Peek 已缓冲的
		// ClientHello 首字节从底层 socket 读不到，握手死锁。
		bc := &bufferedConn{Conn: c, br: br}
		tc := tls.Server(bc, l.currentConfig())
		return &autoConn{Conn: bc, tls: tc, secure: true}
	}
	// 明文连接：保持原样（secure=false），已缓冲字节经 bufferedConn 消费。
	return &autoConn{Conn: &bufferedConn{Conn: c, br: br}, secure: false}
}

// bufferedConn 包装 net.Conn：所有读取经 bufio.Reader（消费 peek 缓冲），
// 使 TLS 与明文分支共享同一字节流，不丢首字节。
type bufferedConn struct {
	net.Conn
	br *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.br.Read(p)
}

// autoConn 自动判定后的连接：tls 非 nil 时读写走 TLS 通道；Secure 报告
// 连接是否加密（会话审计标记用）。
type autoConn struct {
	net.Conn // bufferedConn（明文分支直接读；TLS 分支经 tls 包装）
	tls      *tls.Conn
	secure   bool
}

// Read 转发到 TLS 或明文通道。
func (c *autoConn) Read(p []byte) (int, error) {
	if c.tls != nil {
		return c.tls.Read(p)
	}
	return c.Conn.Read(p)
}

// Write 转发到 TLS 或明文通道。
func (c *autoConn) Write(p []byte) (int, error) {
	if c.tls != nil {
		return c.tls.Write(p)
	}
	return c.Conn.Write(p)
}

// Secure 报告连接是否加密（T36 会话安全标记）。
func (c *autoConn) Secure() bool { return c.secure }

// Close 关闭底层连接与 TLS 通道。
func (c *autoConn) Close() error {
	if c.tls != nil {
		if err := c.tls.Close(); err != nil {
			return err
		}
	}
	return c.Conn.Close()
}

// LocalAddr / RemoteAddr 委托底层连接。
func (c *autoConn) LocalAddr() net.Addr  { return c.Conn.LocalAddr() }
func (c *autoConn) RemoteAddr() net.Addr { return c.Conn.RemoteAddr() }

// SetDeadline 同步设置 TLS 与底层连接超时。
func (c *autoConn) SetDeadline(t time.Time) error {
	if c.tls != nil {
		if err := c.tls.SetDeadline(t); err != nil {
			return err
		}
	}
	return c.Conn.SetDeadline(t)
}

// SetReadDeadline 同步设置读超时。
func (c *autoConn) SetReadDeadline(t time.Time) error {
	if c.tls != nil {
		if err := c.tls.SetReadDeadline(t); err != nil {
			return err
		}
	}
	return c.Conn.SetReadDeadline(t)
}

// SetWriteDeadline 同步设置写超时。
func (c *autoConn) SetWriteDeadline(t time.Time) error {
	if c.tls != nil {
		if err := c.tls.SetWriteDeadline(t); err != nil {
			return err
		}
	}
	return c.Conn.SetWriteDeadline(t)
}

// Ensure io 包引用（未来扩展用）。
var _ io.ReadWriter = (*autoConn)(nil)

package mysqlproto

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// MySQLServer MySQL wire protocol 服务端：与行协议（pkg/server）独立监听，
// 共享同一 SQL 引擎与会话模型。
type MySQLServer struct {
	engine *sql.Engine

	// tlsConfig 非 nil 时监听层强制 TLS（客户端需 ssl-mode=REQUIRED）；
	// 明文连接走 RSA 公钥加密密码路径，同样安全。
	tlsConfig *tls.Config

	// verify 认证校验（user, password → 是否通过）。
	// 未设置时放行所有连接（与行协议默认无鉴权一致，执行身份 root）。
	verify verifyFn

	rsa *rsaKeyPair

	connID atomic.Uint32

	// Logf 连接级日志出口（nil 时静默）。
	Logf func(format string, a ...any)
}

// New 创建 MySQL 协议服务器。
func New(engine *sql.Engine) *MySQLServer {
	s := &MySQLServer{engine: engine}
	if rsa, err := newRSAKeyPair(); err == nil {
		s.rsa = rsa
	}
	return s
}

// SetVerify 装配认证校验函数（main 注入 Engine.AuthenticateUser）。
func (s *MySQLServer) SetVerify(fn verifyFn) { s.verify = fn }

// SetTLS 装配监听层 TLS 配置。
func (s *MySQLServer) SetTLS(cfg *tls.Config) { s.tlsConfig = cfg }

// Serve 监听 addr 并提供 MySQL 协议服务，直至监听器关闭。
func (s *MySQLServer) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.ServeListener(ln)
}

// ServeListener 在既有监听器上提供 MySQL 协议服务（测试用随机端口）。
func (s *MySQLServer) ServeListener(ln net.Listener) error {
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

// handleConn 处理单个 MySQL 连接：握手 → 认证 → 命令循环。
func (s *MySQLServer) handleConn(conn net.Conn) {
	defer conn.Close()
	pc := &packetConn{rw: conn}
	id := s.connID.Add(1)
	scramble, err := genScramble()
	if err != nil {
		return
	}
	if err := pc.writePacket(s.handshakeV10(id, scramble)); err != nil {
		return
	}
	payload, err := pc.readPacket()
	if err != nil {
		return
	}
	hr, err := parseHandshakeResponse(payload)
	if err != nil {
		_ = pc.writePacket(errPacket(err))
		return
	}
	if hr.capability&capProtocol41 == 0 {
		_ = pc.writePacket(errPacket(fmt.Errorf("OpenXDB: client must support protocol 4.1+")))
		return
	}
	user := "root"
	if s.verify != nil {
		if err := s.authenticate(&mysqlConn{pc: pc}, hr); err != nil {
			_ = pc.writePacket(errPacket(err))
			if s.Logf != nil {
				s.Logf("mysql auth failed: user=%s err=%v", hr.username, err)
			}
			return
		}
		user = hr.username
		if err := pc.writePacket(okPacket(0, 0, statusAutocommit)); err != nil {
			return
		}
	} else {
		if err := pc.writePacket(okPacket(0, 0, statusAutocommit)); err != nil {
			return
		}
	}
	c := &mysqlConn{
		pc:     pc,
		engine: s.engine,
		user:   user,
		sess:   s.engine.NewSession(),
		stmts:     map[uint32]string{},
		stmtTypes: map[uint32][]byte{},
		autoCommit: true,
	}
	if err := c.serveCommands(); err != nil {
		if s.Logf != nil {
			s.Logf("mysql conn %d closed: %v", id, err)
		}
	}
}

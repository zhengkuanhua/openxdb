package mysqlproto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// caching_sha2_password 认证流程：
//
//	握手声明插件为 caching_sha2_password。客户端 fast-auth 需要服务端保存
//	无盐 SHA-256(password)，而引擎用户目录为加盐 SHA-256（不可推导），故
//	fast-auth 一律判定不可验证 → 发送 AuthMoreData(0x03) 要求 full auth。
//	full auth 两条路径（按客户端能力自动选择）：
//	  1. TLS 连接：客户端直接发送明文密码 → Engine.AuthenticateUser 校验；
//	  2. 明文连接：客户端请求 RSA 公钥（0x02）→ 服务端下发 PEM 公钥 →
//	     客户端 RSA 加密密码 → 服务端解密校验。
//	两种填充（OAEP-SHA1 / PKCS1v15）均尝试，兼容各客户端实现差异。

// sha256 在 fast-auth 计算中需要，但服务端不持有原始密码哈希；
// 此处保留 sha1 import 避免误用（native 协议不支持时提示清晰错误）。

// rsaKeyPair 每次服务启动生成一次临时 RSA 密钥（2048 位）。
type rsaKeyPair struct {
	priv *rsa.PrivateKey
	pem  []byte // PEM 公钥（含结尾 0）
}

// newRSAKeyPair 生成 RSA 密钥对并编码公钥。
func newRSAKeyPair() (*rsaKeyPair, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	pemBytes = append(pemBytes, 0) // MySQL 协议公钥包以 0x00 结尾
	return &rsaKeyPair{priv: priv, pem: pemBytes}, nil
}

// decryptPassword 解密客户端 RSA 加密的明文密码（先试 OAEP-SHA1，再试 PKCS1v15）。
func (k *rsaKeyPair) decryptPassword(ct []byte) ([]byte, error) {
	if k == nil || k.priv == nil {
		return nil, fmt.Errorf("mysqlproto: no RSA key configured")
	}
	plain, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, k.priv, ct, nil)
	if err == nil {
		return plain, nil
	}
	plain, err = rsa.DecryptPKCS1v15(rand.Reader, k.priv, ct)
	if err != nil {
		return nil, fmt.Errorf("mysqlproto: RSA decrypt password failed")
	}
	return plain, nil
}

// authenticate 完成 caching_sha2_password 认证：解析 auth response，
// 走 full auth 并调用 verifier 校验明文密码。返回错误时调用方发送 ERR。
//
// verify 由装配方注入（main 传入 Engine.AuthenticateUser 或免鉴权放行）。
func (s *MySQLServer) authenticate(c *mysqlConn, hr *handshakeResponse) error {
	// fast-auth 无法校验：一律请求 full authentication。
	if err := c.pc.writePacket([]byte{0x01, 0x03}); err != nil {
		return err
	}
	// 客户端响应：0x02 = 请求公钥；其它 = TLS 下的明文密码。
	resp, err := c.pc.readPacket()
	if err != nil {
		return err
	}
	var plain []byte
	if len(resp) == 1 && resp[0] == 0x02 {
		// 明文连接：下发 RSA 公钥。
		if s.rsa == nil {
			return fmt.Errorf("mysqlproto: RSA public key not available")
		}
		pkt := make([]byte, 0, len(s.rsa.pem)+1)
		pkt = append(pkt, 0x01) // AuthMoreData
		pkt = append(pkt, s.rsa.pem...)
		if err := c.pc.writePacket(pkt); err != nil {
			return err
		}
		ct, err := c.pc.readPacket()
		if err != nil {
			return err
		}
		plain, err = s.rsa.decryptPassword(ct)
		if err != nil {
			return err
		}
	} else {
		// TLS 或客户端已直接发送明文密码（可能以 0x00 结尾）。
		plain = resp
	}
	// 去除尾部 0x00（明文密码终止符）。
	if n := len(plain); n > 0 && plain[n-1] == 0 {
		plain = plain[:n-1]
	}
	if !s.verify(hr.username, string(plain)) {
		return fmt.Errorf("access denied for user '%s'", hr.username)
	}
	return nil
}

// verifyFn 认证校验函数签名：用户 + 明文密码 → 是否通过。
type verifyFn func(user, password string) bool

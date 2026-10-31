package mysqlproto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"net"
	"time"
)

// Client 轻量 MySQL wire 协议客户端（D7.5 / G2：oxsql 交互 CLI 的 Dial 基座）。
// 实现 caching_sha2_password full-auth（明文连接 RSA 公钥路径）与
// COM_QUERY 文本结果集解析，覆盖 OpenXDB --mysql-port 服务端能力子集。
type Client struct {
	conn          net.Conn
	pc            *packetConn
	serverVersion string // 握手包中的服务端版本串（G1 冒烟断言用）
}

// Dial 连接 addr 并完成握手认证；失败时关闭底层连接。
func Dial(addr, user, password string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	c := &Client{conn: conn, pc: &packetConn{rw: conn}}
	if err := c.handshake(user, password); err != nil {
		conn.Close()
		return nil, err
	}
	return c, nil
}

// ServerVersion 返回握手包中的服务端版本串（如 "8.0.36-OpenXDB v1.17.0"）。
func (c *Client) ServerVersion() string { return c.serverVersion }

// Close 关闭连接。
func (c *Client) Close() error { return c.conn.Close() }

// handshake 读取 greeting → 发送握手响应 → 完成认证。
func (c *Client) handshake(user, password string) error {
	p, err := c.pc.readPacket()
	if err != nil {
		return err
	}
	if len(p) == 0 || p[0] != 0x0a {
		return fmt.Errorf("oxsql: unexpected greeting protocol byte 0x%02x", p[0])
	}
	// 解析 server version（greeting[1:0x00]）。
	off := 1
	for off < len(p) && p[off] != 0 {
		off++
	}
	c.serverVersion = string(p[1:off])
	// 提取 20B auth data（8B part1 + 12B part2，偏移与 readGreeting 一致）。
	off++
	off += 4 // conn id
	if off+8+1 > len(p) {
		return fmt.Errorf("oxsql: truncated greeting")
	}
	auth := append([]byte{}, p[off:off+8]...)
	off += 8 + 1 // part1 + filler
	off += 2 + 1 + 2 + 2 // caps low + charset + status + caps high
	off += 1             // auth data len
	off += 10            // reserved
	if off+12 > len(p) {
		return fmt.Errorf("oxsql: truncated greeting auth data")
	}
	auth = append(auth, p[off:off+12]...)
	off += 12
	_ = off

	if err := c.sendHandshake(user, password); err != nil {
		return err
	}
	return c.completeAuth(password)
}

// sendHandshake 发送 Protocol 4.1 握手响应（caps 与服务端 serverCaps 兼容）。
func (c *Client) sendHandshake(user, password string) error {
	var caps uint32 = capProtocol41 | capSecureConnection | capPluginAuth |
		capPluginAuthLenenc | capConnectWithDB | capTransactions
	p := make([]byte, 0, 64)
	var cb [4]byte
	binary.LittleEndian.PutUint32(cb[:], caps)
	p = append(p, cb[:]...)
	var mp [4]byte
	binary.LittleEndian.PutUint32(mp[:], 1<<24)
	p = append(p, mp[:]...)
	p = append(p, 0x21) // utf8mb4
	p = append(p, make([]byte, 23)...)
	p = append(p, user...)
	p = append(p, 0)
	p = appendLenencBytes(p, []byte(password))
	p = append(p, 0) // 无数据库名
	p = append(p, authPlugin...)
	p = append(p, 0)
	return c.pc.writePacket(p)
}

// completeAuth 处理认证阶段响应：OK / ERR / AuthMoreData(0x03)→RSA full auth。
func (c *Client) completeAuth(password string) error {
	resp, err := c.pc.readPacket()
	if err != nil {
		return err
	}
	if len(resp) > 0 && resp[0] == 0x00 {
		return nil // OK
	}
	if len(resp) > 0 && resp[0] == 0xff {
		return fmt.Errorf("oxsql: auth failed: %s", errMessage(resp))
	}
	if len(resp) >= 2 && resp[0] == 0x01 && resp[1] == 0x03 {
		// 请求 RSA 公钥（明文连接路径）。
		if err := c.pc.writePacket([]byte{0x02}); err != nil {
			return err
		}
		pub, err := c.pc.readPacket()
		if err != nil {
			return err
		}
		if len(pub) < 2 || pub[0] != 0x01 {
			return fmt.Errorf("oxsql: unexpected public key packet 0x%02x", pub[0])
		}
		block, _ := pem.Decode(pub[1:])
		if block == nil {
			return fmt.Errorf("oxsql: invalid RSA public key PEM")
		}
		pk, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return err
		}
		rsaPub, ok := pk.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("oxsql: server public key is not RSA")
		}
		ct, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, rsaPub, []byte(password), nil)
		if err != nil {
			return err
		}
		if err := c.pc.writePacket(ct); err != nil {
			return err
		}
		final, err := c.pc.readPacket()
		if err != nil {
			return err
		}
		if len(final) > 0 && final[0] == 0x00 {
			return nil
		}
		if len(final) > 0 && final[0] == 0xff {
			return fmt.Errorf("oxsql: auth failed: %s", errMessage(final))
		}
		return fmt.Errorf("oxsql: unexpected auth final packet 0x%02x", final[0])
	}
	return fmt.Errorf("oxsql: unexpected auth response 0x%02x", resp[0])
}

// Query 执行 COM_QUERY 并解析文本结果集；无结果集（OK）时 cols/rows 为空。
func (c *Client) Query(q string) (cols []string, rows [][]string, err error) {
	p := make([]byte, 0, len(q)+1)
	p = append(p, comQuery)
	p = append(p, q...)
	if err := c.pc.writePacket(p); err != nil {
		return nil, nil, err
	}
	resp, err := c.pc.readPacket()
	if err != nil {
		return nil, nil, err
	}
	switch resp[0] {
	case 0xff:
		return nil, nil, fmt.Errorf("oxsql: %s", errMessage(resp))
	case 0x00:
		return nil, nil, nil // OK（无结果集）
	}
	n, _, err := readLenencInt(resp, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("oxsql: parse column count: %v", err)
	}
	for i := 0; i < int(n); i++ {
		cp, err := c.pc.readPacket()
		if err != nil {
			return nil, nil, fmt.Errorf("oxsql: read column definition: %v", err)
		}
		_, off, err := readLenencBytes(cp, 0) // catalog
		if err != nil {
			return nil, nil, fmt.Errorf("oxsql: parse column: %v", err)
		}
		for j := 0; j < 3; j++ {
			_, off, err = readLenencBytes(cp, off) // schema/table/org_table
			if err != nil {
				return nil, nil, fmt.Errorf("oxsql: parse column: %v", err)
			}
		}
		name, _, err := readLenencBytes(cp, off)
		if err != nil {
			return nil, nil, fmt.Errorf("oxsql: parse column name: %v", err)
		}
		cols = append(cols, string(name))
	}
	if _, err := c.pc.readPacket(); err != nil { // 列定义后 EOF
		return nil, nil, fmt.Errorf("oxsql: read column EOF: %v", err)
	}
	for {
		rp, err := c.pc.readPacket()
		if err != nil {
			return nil, nil, fmt.Errorf("oxsql: read row: %v", err)
		}
		if rp[0] == 0xfe && len(rp) < 9 { // EOF 包
			break
		}
		row := make([]string, 0, len(cols))
		off := 0
		for i := 0; i < len(cols); i++ {
			v, n2, err := readLenencBytes(rp, off)
			if err != nil {
				return nil, nil, fmt.Errorf("oxsql: row cell: %v", err)
			}
			row = append(row, string(v))
			off = n2
		}
		rows = append(rows, row)
	}
	return cols, rows, nil
}

// errMessage 从 ERR 包提取错误消息（跳过 errcode 2B + sqlstate 6B）。
func errMessage(p []byte) string {
	off := 1
	if len(p) >= 3 {
		off += 2 // errcode
	}
	if len(p) >= 9 && p[off] == '#' {
		off += 6 // sqlstate（可选）
	}
	if off < len(p) {
		return string(p[off:])
	}
	return "unknown server error"
}

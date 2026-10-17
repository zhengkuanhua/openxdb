// Package mysqlproto 实现 MySQL wire protocol（协议版本 10）服务端。
//
// 设计目标（评估建议：wire protocol 优先于 TPC-C 合规）：
//   - 与现有逐行文本协议（pkg/server）双协议共存：同一二进制、独立监听端口
//     （--mysql-port，默认 3307），行协议端口行为零回归。
//   - 认证插件 caching_sha2_password（MySQL 8 默认）：复用引擎用户目录
//     （Engine.AuthenticateUser 明文校验），支持 TLS 明文路径与 RSA 公钥
//     加密路径；无法做 fast-auth（引擎不存无盐 SHA-256），自动回退 full auth。
//   - 命令层：COM_QUERY（文本结果集）、COM_STMT_PREPARE/EXECUTE/CLOSE/RESET
//     （桥接引擎 M18 占位符展开）、COM_PING/QUIT/INIT_DB/FIELD_LIST。
//   - 文本结果集：ColumnDefinition41 + EOF 经典协议（不设 CLIENT_DEPRECATE_EOF）。
//
// 包内不依赖 pkg/server，避免循环引用；由 cmd/openxdb 装配两者。

package mysqlproto

import (
	"encoding/binary"
	"fmt"
	"io"
)

// maxPacketSize 单包最大字节数（16MB，MySQL 协议上限）。
const maxPacketSize = 16*1024*1024 - 1

// packetConn 提供 MySQL 协议帧读写：3 字节小端长度 + 1 字节序列号。
type packetConn struct {
	rw  io.ReadWriter
	seq uint8
}

// writePacket 写入一个协议包（自动分片；序列号连续递增）。
func (c *packetConn) writePacket(payload []byte) error {
	for {
		var chunk []byte
		if len(payload) > maxPacketSize {
			chunk = payload[:maxPacketSize]
			payload = payload[maxPacketSize:]
		} else {
			chunk = payload
			payload = nil
		}
		hdr := make([]byte, 4)
		hdr[0] = byte(len(chunk))
		hdr[1] = byte(len(chunk) >> 8)
		hdr[2] = byte(len(chunk) >> 16)
		hdr[3] = c.seq
		if _, err := c.rw.Write(hdr); err != nil {
			return err
		}
		if len(chunk) > 0 {
			if _, err := c.rw.Write(chunk); err != nil {
				return err
			}
		}
		c.seq++
		if len(payload) == 0 {
			return nil
		}
	}
}

// readPacket 读取一个协议包（自动重组分片），返回 payload（不含 4 字节头）。
func (c *packetConn) readPacket() ([]byte, error) {
	var payload []byte
	for {
		hdr := make([]byte, 4)
		if _, err := io.ReadFull(c.rw, hdr); err != nil {
			return nil, err
		}
		n := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
		if n > 0 {
			chunk := make([]byte, n)
			if _, err := io.ReadFull(c.rw, chunk); err != nil {
				return nil, err
			}
			payload = append(payload, chunk...)
		}
		c.seq = hdr[3] + 1
		if n < maxPacketSize {
			return payload, nil
		}
		// 恰好 16MB-1：可能还有后续分片，继续读。
		if n != maxPacketSize {
			return nil, fmt.Errorf("mysqlproto: invalid packet length %d", n)
		}
	}
}

// ---- 长度编码整数（lenenc） ----

// readLenencInt 解析长度编码整数。
func readLenencInt(p []byte, off int) (uint64, int, error) {
	if off >= len(p) {
		return 0, off, io.ErrUnexpectedEOF
	}
	b := p[off]
	off++
	switch {
	case b < 0xfb:
		return uint64(b), off, nil
	case b == 0xfb:
		return 0, off, nil // NULL 标记（本包不用于整数场景，调用方自行处理）
	case b == 0xfc:
		if off+2 > len(p) {
			return 0, off, io.ErrUnexpectedEOF
		}
		return uint64(binary.LittleEndian.Uint16(p[off:])), off + 2, nil
	case b == 0xfd:
		if off+3 > len(p) {
			return 0, off, io.ErrUnexpectedEOF
		}
		v := uint64(p[off]) | uint64(p[off+1])<<8 | uint64(p[off+2])<<16
		return v, off + 3, nil
	default: // 0xfe
		if off+8 > len(p) {
			return 0, off, io.ErrUnexpectedEOF
		}
		return binary.LittleEndian.Uint64(p[off:]), off + 8, nil
	}
}

// appendLenencInt 追加长度编码整数。
func appendLenencInt(dst []byte, v uint64) []byte {
	switch {
	case v < 0xfb:
		return append(dst, byte(v))
	case v <= 0xffff:
		dst = append(dst, 0xfc, byte(v), byte(v>>8))
		return dst
	case v <= 0xffffff:
		dst = append(dst, 0xfd, byte(v), byte(v>>8), byte(v>>16))
		return dst
	default:
		dst = append(dst, 0xfe)
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], v)
		return append(dst, buf[:]...)
	}
}

// readLenencBytes 读取长度编码字符串。
func readLenencBytes(p []byte, off int) ([]byte, int, error) {
	n, off, err := readLenencInt(p, off)
	if err != nil {
		return nil, off, err
	}
	if off+int(n) > len(p) {
		return nil, off, io.ErrUnexpectedEOF
	}
	return p[off : off+int(n)], off + int(n), nil
}

// appendLenencBytes 追加长度编码字符串。
func appendLenencBytes(dst, b []byte) []byte {
	dst = appendLenencInt(dst, uint64(len(b)))
	return append(dst, b...)
}

// readNulString 读取以 0x00 结尾的字符串（含结尾 0）。
func readNulString(p []byte, off int) (string, int, error) {
	for i := off; i < len(p); i++ {
		if p[i] == 0 {
			return string(p[off:i]), i + 1, nil
		}
	}
	return "", off, io.ErrUnexpectedEOF
}

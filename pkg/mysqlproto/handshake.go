package mysqlproto

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
)

// MySQL 能力标志（capability flags，协议 10）。
const (
	capLongPassword     = 0x00000001
	capFoundRows        = 0x00000002
	capLongFlag         = 0x00000004
	capConnectWithDB    = 0x00000008
	capNoSchema         = 0x00000010
	capCompress         = 0x00000020
	capLocalFiles       = 0x00000080
	capProtocol41       = 0x00000200
	capTransactions     = 0x00002000
	capSecureConnection = 0x00008000
	capMultiStatements  = 0x00010000
	capMultiResults     = 0x00020000
	capPSMultiResults   = 0x00040000
	capPluginAuth       = 0x00080000
	capConnectAttrs     = 0x00100000
	capPluginAuthLenenc = 0x00200000
	capSessionTrack     = 0x00800000
	capDeprecateEOF     = 0x01000000
	capSSL              = 0x00000800
	capInteractive      = 0x00000400
	capIgnoreSpace      = 0x00000100
	capIgnoreSigpipe    = 0x00001000
	capReserved         = 0x00004000
	capCanHandleExpired = 0x00400000
	capSSLVerifyCert    = 0x40000000
	capRememberOptions  = 0x80000000
)

// serverCapabilities 服务器声明的能力：经典 EOF 协议（不设 DEPRECATE_EOF），
// 支持事务标志（CLIENT_TRANSACTIONS，autocommit 状态位）与预处理语句多结果。
var serverCapabilities uint32 = capLongPassword | capFoundRows | capLongFlag |
	capConnectWithDB | capProtocol41 | capTransactions | capSecureConnection |
	capMultiResults | capPSMultiResults | capPluginAuth | capPluginAuthLenenc

// serverStatus 常量。
const (
	statusAutocommit       = 0x0002
	statusInTransaction    = 0x0001
	statusMoreResults      = 0x0008 // SERVER_MORE_RESULTS_EXISTS：多语句结果流中仍有后续结果
)

// authPlugin 当前认证插件（MySQL 8 默认 caching_sha2_password）。
const authPlugin = "caching_sha2_password"

// OpenxdbVersion OpenXDB 自身发布版本号（与发版版本常量同源注入）。
const OpenxdbVersion = "v1.17.0"

// serverVersion 服务器版本串：前缀 8.0.36-OpenXDB 保持 MySQL 兼容
// （mysql CLI 可识别），OpenXDB 自身版本部分与 OpenxdbVersion 同源拼接。
const serverVersion = "8.0.36-OpenXDB " + OpenxdbVersion

// handshakeV10 构造服务端初始握手包（Server Greeting，协议 10）。
// scramble 为 20 字节随机串（auth-plugin-data，含结尾 0）。
func (s *MySQLServer) handshakeV10(connID uint32, scramble []byte) []byte {
	p := make([]byte, 0, 128)
	p = append(p, 0x0a) // protocol version
	p = append(p, serverVersion...)
	p = append(p, 0)
	var id [4]byte
	binary.LittleEndian.PutUint32(id[:], connID)
	p = append(p, id[:]...)
	p = append(p, scramble[:8]...) // auth-plugin-data-part-1
	p = append(p, 0)               // filler
	var caps [4]byte
	binary.LittleEndian.PutUint32(caps[:], serverCapabilities)
	p = append(p, caps[0], caps[1])                                  // capability flags lower
	p = append(p, 0x21)                                              // character set: utf8mb4
	p = append(p, byte(statusAutocommit), byte(statusAutocommit>>8)) // status
	p = append(p, caps[2], caps[3])                                  // capability flags upper
	p = append(p, byte(len(authPlugin)+8))                           // auth plugin data len (21)
	p = append(p, make([]byte, 10)...)                               // reserved
	// auth-plugin-data-part-2：12 字节 + 结尾 0，补齐 20 字节 scramble。
	p = append(p, scramble[8:20]...)
	p = append(p, 0)
	p = append(p, authPlugin...)
	p = append(p, 0)
	return p
}

// genScramble 生成 20 字节随机挑战串（含末尾 0，共 21 字节）。
func genScramble() ([]byte, error) {
	b := make([]byte, 21)
	if _, err := rand.Read(b[:20]); err != nil {
		return nil, err
	}
	b[20] = 0
	return b, nil
}

// handshakeResponse 解析客户端握手响应（HandshakeResponse41）。
type handshakeResponse struct {
	capability uint32
	maxPacket  uint32
	charset    byte
	username   string
	authResp   []byte
	database   string
	pluginName string
	attrs      []byte
}

// parseHandshakeResponse 解析握手响应包。payload 为包体（不含帧头）。
func parseHandshakeResponse(payload []byte) (*handshakeResponse, error) {
	if len(payload) < 32 {
		return nil, fmt.Errorf("mysqlproto: handshake response too short (%d)", len(payload))
	}
	hr := &handshakeResponse{
		capability: binary.LittleEndian.Uint32(payload[0:4]),
		maxPacket:  binary.LittleEndian.Uint32(payload[4:8]),
		charset:    payload[8],
	}
	off := 32 // 跳过 capability 4 + max packet 4 + charset 1 + reserved 23
	user, n, err := readNulString(payload, off)
	if err != nil {
		return nil, fmt.Errorf("mysqlproto: parse username: %w", err)
	}
	hr.username = user
	off = n
	// auth response：CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA 时 lenenc，否则 1 字节长度。
	if hr.capability&capPluginAuthLenenc != 0 {
		var resp []byte
		resp, off, err = readLenencBytes(payload, off)
		if err != nil {
			return nil, fmt.Errorf("mysqlproto: parse auth response: %w", err)
		}
		hr.authResp = resp
	} else if hr.capability&capSecureConnection != 0 {
		if off >= len(payload) {
			return nil, io.ErrUnexpectedEOF
		}
		n := int(payload[off])
		off++
		if off+n > len(payload) {
			return nil, io.ErrUnexpectedEOF
		}
		hr.authResp = payload[off : off+n]
		off += n
	}
	if hr.capability&capConnectWithDB != 0 {
		db, n2, err := readNulString(payload, off)
		if err != nil {
			return nil, fmt.Errorf("mysqlproto: parse database: %w", err)
		}
		hr.database = db
		off = n2
	}
	if hr.capability&capPluginAuth != 0 {
		plugin, n2, err := readNulString(payload, off)
		if err != nil {
			return nil, fmt.Errorf("mysqlproto: parse auth plugin: %w", err)
		}
		hr.pluginName = plugin
		off = n2
	}
	if hr.capability&capConnectAttrs != 0 {
		attrs, _, err := readLenencBytes(payload, off)
		if err != nil {
			// 客户端属性解析失败不阻断握手。
			attrs = nil
		}
		hr.attrs = attrs
	}
	return hr, nil
}

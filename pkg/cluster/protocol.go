// Package cluster 提供 M4 多节点真分布式第一簇的节点拓扑与跨节点查询转发。
//
// 设计要点：
//   - 帧格式与 pkg/replication 完全一致：len(4B 大端) + type(1B) + payload，
//     避免无谓新协议；PING/PONG（type 5/6，payload = ts 8B UnixMilli）语义
//     直接复用 M2 心跳协议；
//   - 节点管理与查询转发使用新帧类型（16-19），payload 为 JSON（[]byte 自动
//     base64 编码，跨端一致）；
//   - 查询转发只传"物理键区间"，远端按区间直扫本机存储并原样返回
//     {Key, Value} 字节对，不感知行结构，避免 cluster 与 sql 包循环依赖。
package cluster

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// 帧类型：1-6 为 M2 复制协议占用（HELLO/HELLO_OK/BINLOG/ACK/PING/PONG）；
// M4 节点链路从 16 开始，避免与 M2 语义冲突。
const (
	msgPing       byte = 5 // 复用 M2：PING（ts 8B UnixMilli）
	msgPong       byte = 6 // 复用 M2：PONG（ts 8B UnixMilli）
	msgNodeHello  byte = 16
	msgNodeHelloOK byte = 17
	msgQueryReq   byte = 18
	msgQueryResp  byte = 19
)

const (
	protoMagic   = "OPENXDBND"
	protoVersion = 1
)

// HelloPayload NODE_HELLO 载荷：声明本节点身份。
type HelloPayload struct {
	NodeID  string `json:"node_id"`
	Addr    string `json:"addr"`
	Version int    `json:"version"`
}

// HelloOKPayload NODE_HELLO_OK 载荷：对端确认并回执自身 NodeID。
type HelloOKPayload struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
}

// QueryOp 查询转发操作类型。
type QueryOp string

const (
	OpScan QueryOp = "SCAN" // 区间扫描 [Start, End)
	OpGet  QueryOp = "GET"  // 点查
)

// QueryReq QUERY_REQ 载荷。
// Start/End 为带 region 前缀的物理键（JSON 序列化时自动 base64）。
type QueryReq struct {
	Op    QueryOp `json:"op"`
	Start []byte  `json:"start,omitempty"`
	End   []byte  `json:"end,omitempty"`
	Limit int     `json:"limit,omitempty"`
}

// QueryRow 远端返回的一行：物理键 + 原始行值字节（不感知行结构）。
type QueryRow struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// QueryResp QUERY_RESP 载荷：Err 非空表示失败；Op=GET 时 Row 单行。
type QueryResp struct {
	Err  string     `json:"err,omitempty"`
	Row  *QueryRow  `json:"row,omitempty"`
	Rows []QueryRow `json:"rows,omitempty"`
}

// errNotNotFound 远端点查未命中的哨兵（QueryResp.Err = "not_found"）。
const remoteNotFound = "not_found"

// writeFrame 写帧：len(4B 大端) + type(1B) + payload（与 pkg/replication 同格式）。
func writeFrame(w io.Writer, typ byte, payload []byte) error {
	head := make([]byte, 5)
	binary.BigEndian.PutUint32(head[:4], uint32(len(payload)))
	head[4] = typ
	if _, err := w.Write(head); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// readFrame 读帧：返回类型与载荷。
func readFrame(r io.Reader) (byte, []byte, error) {
	head := make([]byte, 5)
	if _, err := io.ReadFull(r, head); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(head[:4])
	if n > 64<<20 { // 64MB 帧上限，防御异常连接
		return 0, nil, errors.New("cluster: frame too large")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return head[4], payload, nil
}

func marshalJSON(v interface{}) ([]byte, error) { return json.Marshal(v) }

func unmarshalJSON(raw []byte, v interface{}) error {
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("cluster: bad payload: %w", err)
	}
	return nil
}

// pingPayload 构造 PING 载荷（ts 8B UnixMilli，与 M2 心跳一致）。
func pingPayload(ts int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(ts))
	return b
}

// parseTs 解析 PING/PONG 载荷时间戳。
func parseTs(payload []byte) (int64, error) {
	if len(payload) != 8 {
		return 0, errors.New("cluster: bad heartbeat payload")
	}
	return int64(binary.BigEndian.Uint64(payload)), nil
}

// dialTimeout 节点链路连接超时。
const dialTimeout = 3 * time.Second

func dial(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, dialTimeout)
}

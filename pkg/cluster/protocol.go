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
	msgPing        byte = 5 // 复用 M2：PING（ts 8B UnixMilli）
	msgPong        byte = 6 // 复用 M2：PONG（ts 8B UnixMilli）
	msgNodeHello   byte = 16
	msgNodeHelloOK byte = 17
	msgQueryReq    byte = 18
	msgQueryResp   byte = 19
	// v1.19.0 远端区间聚合：多区间单次往返 + 区间流式分块回传。
	// 编号 66/67 避开既有占用段（20-21 REGION_PUSH、22-27 2PC、28-29 TSO、
	// 30-31 DDL、32-33 注册表、40-45 备份恢复、50 epoch、60-61 全量对齐、
	// 70-71 TSO HA）。既有 QUERY_REQ/QUERY_RESP 语义与帧格式完全不变；
	// 灰度开关关闭时不会发出新帧，老路径零回归（未知帧对端直接断开）。
	msgQueryMultiReq  byte = 66
	msgQueryMultiResp byte = 67
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
// BeginTS 为发起方快照版本（M8 分布式快照隔离）：远端按该版本做
// MVCC 过滤，begin_ts=0 表示不过滤（旧路径/单机零回归）。
// TxID 为发起方显式事务 ID（M18-A 事务上下文）：随帧传播到目标节点，
// 供参与者识别写请求所属会话事务；空串表示无显式事务上下文（零回归）。
type QueryReq struct {
	Op      QueryOp `json:"op"`
	Start   []byte  `json:"start,omitempty"`
	End     []byte  `json:"end,omitempty"`
	Limit   int     `json:"limit,omitempty"`
	BeginTS uint64  `json:"begin_ts,omitempty"`
	TxID    string  `json:"tx_id,omitempty"`
	// Sort 非空时（M12-2 跨节点 ORDER BY/LIMIT 分页）：远端扫描完成后
	// 按 Sort 提取第 ColIndex 列排序，再截取 [Offset, Offset+Limit) 窗口返回。
	// 仅影响 OpScan，OpGet 忽略。
	Sort *SortSpec `json:"sort,omitempty"`
}

// SortSpec 远端排序窗口下推描述。
// Kind 与 sql 包列类型一致（INT/DECIMAL/TEXT/DATE/BLOB）；远端不感知表结构，
// 仅需按行 JSON 数组的第 ColIndex 列提取排序键；DECIMAL 按 Scale 位定点整数比较。
type SortSpec struct {
	ColIndex int    `json:"col_index"`
	Kind     string `json:"kind"`
	Desc     bool   `json:"desc,omitempty"`
	Scale    int    `json:"scale,omitempty"`
	Offset   int    `json:"offset,omitempty"` // 排序后窗口起点（0 = 从头）
	Limit    int    `json:"limit,omitempty"`  // 窗口行数（<=0 = 全量）
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

// QueryRange 多区间批量扫描中的单个区间（v1.19.0 远端区间聚合）。
// Start/End 为带 region 前缀的物理键；Limit<=0 表示不限行数；Sort 非空时
// 该区间按 M12-2 排序窗口语义在归属节点本地排序后截窗（与 QUERY_REQ 一致）。
type QueryRange struct {
	Start []byte    `json:"start,omitempty"`
	End   []byte    `json:"end,omitempty"`
	Limit int       `json:"limit,omitempty"`
	Sort  *SortSpec `json:"sort,omitempty"`
}

// MultiQueryReq QUERY_MULTI_REQ 载荷：一次往返携带多个物理区间。
// 同一归属节点的多个区间合并为一次请求（原 QUERY_REQ 为每区间一次往返），
// 把跨节点多区间读的往返次数从 O(区间数) 降为 O(归属节点数)。
type MultiQueryReq struct {
	Op      QueryOp      `json:"op"` // 目前支持 OpScan（多区间区间扫描）
	Ranges  []QueryRange `json:"ranges"`
	BeginTS uint64       `json:"begin_ts,omitempty"`
	TxID    string       `json:"tx_id,omitempty"`
}

// MultiQueryChunk QUERY_MULTI_RESP 载荷：按区间流式回传的分块。
// Index 为对应请求区间下标；Rows 为本次分块携带的行；Err 非空表示该区间
// 执行失败（服务端随即以 Done=true 结束本次响应）；Done 表示全部区间结束。
type MultiQueryChunk struct {
	Index int        `json:"index,omitempty"`
	Rows  []QueryRow `json:"rows,omitempty"`
	Err   string     `json:"err,omitempty"`
	Done  bool       `json:"done,omitempty"`
}

// multiChunkBytes 单个流式分块的行载荷字节预算（服务端按此切块，避免超大
// 结果集单帧逼近 64MB 帧上限；JSON base64 后约放大 1.33 倍，仍留足余量）。
const multiChunkBytes = 4 << 20

// multiChunkRows 单个流式分块的行数上限（小行场景避免块过大）。
const multiChunkRows = 512

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

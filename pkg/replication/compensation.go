package replication

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// maxCompensateBatch 单个补偿帧内最大条目数（防单帧过大）。
const maxCompensateBatch = 512

// PushCompensation 一次性补偿推送（阶段二回迁自动化）：
// 陈旧主把崩溃前未复制 binlog 段 entries 作为补偿批次回灌给新主 masterAddr。
// 独立一次性连接（不占用从端 dump 流）：RoleCompensate 握手后逐批发送
// msgCompensate，每批等待 msgAck 确认再继续；返回新主 helloOK 通告的主端位点。
// tlsCfg 非 nil 时使用 TLS 连接（与 StartFollowerTLS 同语义）。
// 失败时调用方应走全量对齐兜底路径（region push）。
func PushCompensation(ctx context.Context, masterAddr string, entries []*BinlogEntry, tlsCfg *tls.Config) (storage.LSN, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	var conn net.Conn
	var err error
	if tlsCfg != nil {
		conn, err = tls.DialWithDialer(dialer, "tcp", masterAddr, tlsCfg)
	} else {
		conn, err = dialer.Dial("tcp", masterAddr)
	}
	if err != nil {
		return 0, fmt.Errorf("replication: compensation dial: %w", err)
	}
	defer func() { _ = conn.Close() }() // 补偿连接用完即释放，不常驻

	_ = conn.SetDeadline(time.Now().Add(30 * time.Second)) // 整体超时兜底
	defer func() { _ = conn.SetDeadline(time.Time{}) }()

	if err := writeFrame(conn, msgHello, encodeHello(RoleCompensate, 0)); err != nil {
		return 0, fmt.Errorf("replication: compensation hello: %w", err)
	}
	typ, payload, err := readFrame(conn)
	if err != nil || typ != msgHelloOK {
		return 0, fmt.Errorf("replication: compensation hello_ok: frame=%d err=%v", typ, err)
	}
	last, _, err := decodeHelloOK(payload)
	if err != nil {
		return 0, fmt.Errorf("replication: compensation hello_ok decode: %w", err)
	}
	// capability 附加帧（P0 版本协商；新主端固定跟随 helloOK 发送）。
	if _, _, err := readFrame(conn); err != nil {
		return 0, fmt.Errorf("replication: compensation capability: %w", err)
	}
	for len(entries) > 0 {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("replication: compensation canceled: %w", err)
		}
		n := maxCompensateBatch
		if len(entries) < n {
			n = len(entries)
		}
		batch := entries[:n]
		start := batch[0].LSN
		frame, perr := encodeCompensation(start, batch)
		if perr != nil {
			return 0, fmt.Errorf("replication: compensation encode: %w", perr)
		}
		if werr := writeFrame(conn, msgCompensate, frame); werr != nil {
			return 0, fmt.Errorf("replication: compensation send: %w", werr)
		}
		typ, ackPayload, rerr := readFrame(conn)
		if rerr != nil || typ != msgAck {
			return 0, fmt.Errorf("replication: compensation ack: frame=%d err=%v", typ, rerr)
		}
		if _, derr := decodeAck(ackPayload); derr != nil {
			return 0, fmt.Errorf("replication: compensation ack decode: %w", derr)
		}
		entries = entries[n:]
	}
	return last, nil
}

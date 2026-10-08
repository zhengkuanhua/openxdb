package replication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// 复制协议（TCP，二进制帧）：
//
//	帧 = len(4B, BE) + type(1B) + payload(len 字节)；len 为 payload 长度。
//
//	帧类型：
//	  HELLO    (1)  Slave → Master：握手（magic+version+role+fromLSN）
//	  HELLO_OK (2)  Master → Slave：接受（version+lastLSN）
//	  BINLOG   (3)  Master → Slave：一条复制日志（binlog 记录编码）
//	  ACK      (4)  Slave → Master：已应用位点确认
//	  PING     (5)  双向：心跳探测（发送时间戳 ms）
//	  PONG     (6)  双向：心跳应答（原样回传时间戳）
const (
	protoMagic   = "OPENXDBRPL"
	protoVersion = 1

	msgHello   byte = 1
	msgHelloOK byte = 2
	msgBinlog  byte = 3
	msgAck     byte = 4
	msgPing    byte = 5
	msgPong    byte = 6

	maxFrameBody = 1 << 30
)

// hello payload: magic(10B) + version(2B) + role(1B) + fromLSN(8B)
func encodeHello(role ReplicaRole, from storage.LSN) []byte {
	b := make([]byte, 0, len(protoMagic)+2+1+8)
	b = append(b, protoMagic...)
	b = binary.BigEndian.AppendUint16(b, protoVersion)
	b = append(b, byte(role))
	b = binary.BigEndian.AppendUint64(b, uint64(from))
	return b
}

func decodeHello(payload []byte) (ReplicaRole, storage.LSN, error) {
	if len(payload) != len(protoMagic)+2+1+8 {
		return 0, 0, ErrCorrupt
	}
	if string(payload[:len(protoMagic)]) != protoMagic {
		return 0, 0, errors.New("replication: hello magic mismatch")
	}
	if binary.BigEndian.Uint16(payload[len(protoMagic):len(protoMagic)+2]) != protoVersion {
		return 0, 0, errors.New("replication: hello version mismatch")
	}
	role := ReplicaRole(payload[len(protoMagic)+2])
	from := storage.LSN(binary.BigEndian.Uint64(payload[len(protoMagic)+3:]))
	return role, from, nil
}

// helloOK payload: version(2B) + lastLSN(8B)
func encodeHelloOK(last storage.LSN) []byte {
	b := make([]byte, 0, 2+8)
	b = binary.BigEndian.AppendUint16(b, protoVersion)
	b = binary.BigEndian.AppendUint64(b, uint64(last))
	return b
}

func decodeHelloOK(payload []byte) (storage.LSN, error) {
	if len(payload) != 2+8 {
		return 0, ErrCorrupt
	}
	if binary.BigEndian.Uint16(payload[0:2]) != protoVersion {
		return 0, errors.New("replication: hello_ok version mismatch")
	}
	return storage.LSN(binary.BigEndian.Uint64(payload[2:10])), nil
}

// ack payload: lsn(8B)
func encodeAck(lsn storage.LSN) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(lsn))
	return b
}

func decodeAck(payload []byte) (storage.LSN, error) {
	if len(payload) != 8 {
		return 0, ErrCorrupt
	}
	return storage.LSN(binary.BigEndian.Uint64(payload)), nil
}

// ping/pong payload: ts(8B, UnixMilli)
func encodePing(ts int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(ts))
	return b
}

func decodePing(payload []byte) (int64, error) {
	if len(payload) != 8 {
		return 0, ErrCorrupt
	}
	return int64(binary.BigEndian.Uint64(payload)), nil
}

func writeFrame(w io.Writer, typ byte, payload []byte) error {
	if len(payload) > maxFrameBody {
		return errors.New("replication: frame too large")
	}
	var head [5]byte
	binary.BigEndian.PutUint32(head[0:4], uint32(len(payload)))
	head[4] = typ
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func readFrame(r io.Reader) (byte, []byte, error) {
	var head [5]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	bodyLen := binary.BigEndian.Uint32(head[0:4])
	if bodyLen > maxFrameBody {
		return 0, nil, ErrCorrupt
	}
	payload := make([]byte, bodyLen)
	if bodyLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, err
		}
	}
	return head[4], payload, nil
}

// decodeBinFramePayload 将 BINLOG 帧 payload 解码为复制日志条目。
func decodeBinFramePayload(payload []byte) (*BinlogEntry, error) {
	return decodeBinRecord(bytes.NewReader(payload))
}

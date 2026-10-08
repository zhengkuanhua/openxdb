package replication

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// FollowerStatus 从节点状态快照（查询/观测用）。
type FollowerStatus struct {
	Role       ReplicaRole
	MasterAddr string
	State      string // idle | connecting | streaming | offline
	AppliedLSN storage.LSN
	Connected  bool
	LastError  string
}

// Follower 从端复制器：连接主节点、握手（携带本地续传位点）、接收 binlog 幂等
// 应用到本地存储、周期 ACK、心跳应答。断线/超时后 Start 返回并标记 offline，
// 重连时以 AppliedLSN+1 为起始位点续传（同一实例内存位点；重启需另行恢复）。
type Follower struct {
	st storage.Storage

	mu        sync.Mutex
	applied   storage.LSN
	state     string
	masterAddr string
	lastErr   string
	conn      net.Conn

	// HeartbeatInterval 预留心跳发送间隔（当前以回 PONG 应答主端 PING）。
	HeartbeatInterval time.Duration
	// ReadTimeout 接收帧超时：超过视为主节点失联，Start 返回。
	ReadTimeout time.Duration
}

// NewFollower 创建从端复制器。
func NewFollower(st storage.Storage) *Follower {
	return &Follower{
		st:                st,
		state:             "idle",
		HeartbeatInterval: 2 * time.Second,
		ReadTimeout:       10 * time.Second,
	}
}

// ApplyBinlog 幂等应用一条复制日志：
//   - e.LSN <= applied：已应用过，跳过（幂等）；
//   - e.LSN == applied+1：正常顺序，应用并推进；
//   - e.LSN > applied+1：位点跳号（流缺口），返回 ErrGap 拒绝。
func (f *Follower) ApplyBinlog(e *BinlogEntry) error {
	if e == nil || e.Batch == nil {
		return errors.New("replication: nil entry or batch")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if e.LSN <= f.applied {
		return nil
	}
	if e.LSN > f.applied+1 {
		return fmt.Errorf("%w: want %d, got %d", ErrGap, f.applied+1, e.LSN)
	}
	if err := f.st.Write(e.Batch); err != nil {
		return err
	}
	f.applied = e.LSN
	return nil
}

// AppliedLSN 返回已应用位点（幂等/续传基准）。
func (f *Follower) AppliedLSN() storage.LSN {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied
}

// Status 返回从节点状态快照。
func (f *Follower) Status() FollowerStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return FollowerStatus{
		Role:       RoleSlave,
		MasterAddr: f.masterAddr,
		State:      f.state,
		AppliedLSN: f.applied,
		Connected:  f.conn != nil,
		LastError:  f.lastErr,
	}
}

// Start 连接主节点并进入复制循环（阻塞直到 ctx 取消、连接断开或主节点失联）。
// 起始位点 = 当前 AppliedLSN+1，实现断线重连续传。
func (f *Follower) Start(ctx context.Context, masterAddr string) error {
	conn, err := net.DialTimeout("tcp", masterAddr, 5*time.Second)
	if err != nil {
		f.setOffline(masterAddr, err.Error())
		return err
	}
	f.mu.Lock()
	f.conn = conn
	f.masterAddr = masterAddr
	f.state = "connecting"
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.state = "offline"
		f.conn = nil
		f.mu.Unlock()
		_ = conn.Close()
	}()

	// 握手：携带续传位点。
	from := f.AppliedLSN() + 1
	if err := writeFrame(conn, msgHello, encodeHello(RoleSlave, from)); err != nil {
		f.setOffline(masterAddr, err.Error())
		return err
	}
	typ, payload, err := readFrame(conn)
	if err != nil {
		f.setOffline(masterAddr, err.Error())
		return err
	}
	if typ != msgHelloOK {
		f.setOffline(masterAddr, "unexpected handshake response")
		return errors.New("replication: unexpected handshake response")
	}
	if _, err := decodeHelloOK(payload); err != nil {
		f.setOffline(masterAddr, err.Error())
		return err
	}
	f.setStateStreaming(masterAddr)

	// 复制循环。
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(f.ReadTimeout))
		typ, payload, err := readFrame(conn)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				ferr := errors.New("replication: master heartbeat timeout")
				f.setOffline(masterAddr, ferr.Error())
				return ferr
			}
			f.setOffline(masterAddr, err.Error())
			return err
		}
		_ = conn.SetReadDeadline(time.Time{})
		switch typ {
		case msgBinlog:
			e, derr := decodeBinFramePayload(payload)
			if derr != nil {
				f.setOffline(masterAddr, derr.Error())
				return derr
			}
			if aerr := f.ApplyBinlog(e); aerr != nil {
				f.setOffline(masterAddr, aerr.Error())
				return aerr
			}
			if werr := writeFrame(conn, msgAck, encodeAck(e.LSN)); werr != nil {
				f.setOffline(masterAddr, werr.Error())
				return werr
			}
		case msgPing:
			ts, derr := decodePing(payload)
			if derr != nil {
				f.setOffline(masterAddr, derr.Error())
				return derr
			}
			if werr := writeFrame(conn, msgPong, encodePing(ts)); werr != nil {
				f.setOffline(masterAddr, werr.Error())
				return werr
			}
		default:
			// HELLO_OK / ACK 等在从端无处理，忽略。
		}
	}
}

// Stop 断开当前复制连接（Start 将返回并标记 offline）。
func (f *Follower) Stop() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn != nil {
		_ = f.conn.Close()
	}
	return nil
}

func (f *Follower) setOffline(addr, errMsg string) {
	f.mu.Lock()
	f.state = "offline"
	f.masterAddr = addr
	f.lastErr = errMsg
	f.mu.Unlock()
}

func (f *Follower) setStateStreaming(addr string) {
	f.mu.Lock()
	f.state = "streaming"
	f.masterAddr = addr
	f.lastErr = ""
	f.mu.Unlock()
}

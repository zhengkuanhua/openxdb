package replication

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// FollowerStatus 从节点状态快照（查询/观测用）。
type FollowerStatus struct {
	Role          ReplicaRole
	MasterAddr    string
	State         string // idle | connecting | streaming | offline
	AppliedLSN    storage.LSN
	LastMasterLSN storage.LSN // 主端通告/收到的最新 binlog 位点（RPO 审计基准）
	Connected     bool
	LastError     string
}

// Follower 从端复制器：连接主节点、握手（携带本地续传位点）、接收 binlog 幂等
// 应用到本地存储、周期 ACK、心跳应答。断线/超时后 Start 返回并标记 offline，
// 重连时以 AppliedLSN+1 为起始位点续传（同一实例内存位点；重启需另行恢复）。
type Follower struct {
	st storage.Storage

	mu        sync.Mutex
	applied   storage.LSN
	// lastMasterLSN 主端 binlog 最新位点（v1.6.0 阶段一 RPO 审计）：握手
	// helloOK 通告与每个 msgBinlog 时取 max 更新；RpoGap 以它为基准。
	lastMasterLSN storage.LSN
	state         string
	masterAddr    string
	lastErr       string
	conn          net.Conn

	// HeartbeatInterval 预留心跳发送间隔（当前以回 PONG 应答主端 PING）。
	HeartbeatInterval time.Duration
	// ReadTimeout 接收帧超时：超过视为主节点失联，Start 返回。
	ReadTimeout time.Duration

	// peerCapFlags 握手后主端 CAPABILITY 帧声明的能力位（P0 HA 版本协商）。
	// 旧主不发 CAPABILITY 帧时保持 0，表示无 epoch/fencing 语义。
	peerCapFlags byte
	// savedTLS 最近一次复制连接使用的 TLS 配置（PullCatchup 追平复用；nil=明文）。
	savedTLS *tls.Config
}

// catchupDialTimeout PullCatchup 追平连接拨号超时（旧主可达性短探测）。
const catchupDialTimeout = 500 * time.Millisecond

// PeerCapability 返回握手协商出的对端（主节点）能力位：
// 0 = 旧协议（无 CAPABILITY 帧）；非 0 含 capEpochFencing 位表示对端
// 声明支持 clusterEpoch/fencing 语义。供 db 层决定是否启用 fencing 校验。
func (f *Follower) PeerCapability() byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peerCapFlags
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

// ReadBarrier 等待复制应用到达稳定边界：阻塞直至当前进行中的 ApplyBinlog
// 批次完整写入存储后返回（D6 备库常驻读，replica SQL 读路径在创建快照前
// 调用）。配合 Storage.Write 原子批次与快照隔离，保证读可见性以
// AppliedLSN 为界：不会读到进行中的半个批次，也不会越过已应用位点；
// 屏障只等待当前批次（Mutex 短临界区），不阻塞后续复制应用，读与复制
// 应用保持并发，锁序单向（f.mu → storage）无死锁。
func (f *Follower) ReadBarrier() {
	f.mu.Lock()
	f.mu.Unlock()
}

// LastMasterLSN 返回主端 binlog 最新位点（RPO 审计基准，v1.6.0 阶段一）：
// 握手 helloOK 通告与每个 msgBinlog 时取 max 更新；从未连接或旧主无位点
// 通告时为 0。
func (f *Follower) LastMasterLSN() storage.LSN {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastMasterLSN
}

// RpoGap 返回复制落后缺口（RPO 审计，v1.6.0 阶段一）：
// lastMasterLSN - appliedLSN；已知主端位点不高于已应用位点时记 0。
func (f *Follower) RpoGap() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastMasterLSN <= f.applied {
		return 0
	}
	return uint64(f.lastMasterLSN - f.applied)
}

// recordMasterLSN 记录主端 binlog 最新位点（取 max，幂等；锁内调用方自持锁）。
func (f *Follower) recordMasterLSN(lsn storage.LSN) {
	if lsn > f.lastMasterLSN {
		f.lastMasterLSN = lsn
	}
}

// Status 返回从节点状态快照。
func (f *Follower) Status() FollowerStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return FollowerStatus{
		Role:          RoleSlave,
		MasterAddr:    f.masterAddr,
		State:         f.state,
		AppliedLSN:    f.applied,
		LastMasterLSN: f.lastMasterLSN,
		Connected:     f.conn != nil,
		LastError:     f.lastErr,
	}
}

// Start 连接主节点并进入复制循环（阻塞直到 ctx 取消、连接断开或主节点失联）。
// 起始位点 = 当前 AppliedLSN+1，实现断线重连续传。复制端口为明文连接。
func (f *Follower) Start(ctx context.Context, masterAddr string) error {
	return f.StartTLS(ctx, masterAddr, nil)
}

// StartTLS 与 Start 等价，但以 TLS 加密连接主节点复制端口（P0 TLS，可选）：
// tlsCfg 非 nil 时使用 tls.DialWithDialer（自签名/内网场景可传
// InsecureSkipVerify）；nil 保持明文。
func (f *Follower) StartTLS(ctx context.Context, masterAddr string, tlsCfg *tls.Config) error {
	var conn net.Conn
	var err error
	if tlsCfg != nil {
		d := &net.Dialer{Timeout: 5 * time.Second}
		conn, err = tls.DialWithDialer(d, "tcp", masterAddr, tlsCfg)
	} else {
		conn, err = net.DialTimeout("tcp", masterAddr, 5*time.Second)
	}
	if err != nil {
		f.setOffline(masterAddr, err.Error())
		return err
	}
	f.mu.Lock()
	f.conn = conn
	f.masterAddr = masterAddr
	f.state = "connecting"
	f.savedTLS = tlsCfg
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
	// 阶段一 RPO 审计：握手通告记录主端 binlog 最新位点（旧主无位点=0）。
	last, _, err := decodeHelloOK(payload)
	if err != nil {
		f.setOffline(masterAddr, err.Error())
		return err
	}
	f.mu.Lock()
	f.recordMasterLSN(last)
	f.mu.Unlock()
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
			// 阶段一 RPO 审计：收到 binlog 即记录主端最新位点（max 语义）。
			f.mu.Lock()
			f.recordMasterLSN(e.LSN)
			f.mu.Unlock()
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
		case msgCapability:
			// P0 HA 版本协商：记录主端能力位（epoch/fencing 语义）。
			flags, derr := decodeCapability(payload)
			if derr != nil {
				f.setOffline(masterAddr, derr.Error())
				return derr
			}
			f.mu.Lock()
			f.peerCapFlags = flags
			f.mu.Unlock()
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

// PullCatchup 一次性追平复制（阶段一 RPO 补偿）：连接 masterAddr，从
// AppliedLSN+1 续传，追平到主端通告位点（握手 helloOK lastLSN；每个
// msgBinlog 时取 max 更新）或 ctx 截止即停；已应用条目保留（幂等）。
// 使用独立一次性连接，不写入 f.conn，可与主复制循环并发安全共用幂等
// 应用（ApplyBinlog 锁内写存储）；TLS 配置复用最近一次 StartTLS 的
// savedTLS（nil=明文）。返回追平后的 AppliedLSN（未追平时返回截止前
// 进度与 ctx/网络错误，调用方按需决定 promote 策略）。
func (f *Follower) PullCatchup(ctx context.Context, masterAddr string) (storage.LSN, error) {
	tlsCfg := func() *tls.Config {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.savedTLS
	}()
	var conn net.Conn
	var err error
	d := &net.Dialer{Timeout: catchupDialTimeout}
	if tlsCfg != nil {
		conn, err = tls.DialWithDialer(d, "tcp", masterAddr, tlsCfg)
	} else {
		conn, err = d.Dial("tcp", masterAddr)
	}
	if err != nil {
		return f.AppliedLSN(), fmt.Errorf("replication: catchup dial %s: %w", masterAddr, err)
	}
	defer func() { _ = conn.Close() }()

	from := f.AppliedLSN() + 1
	if err := writeFrame(conn, msgHello, encodeHello(RoleSlave, from)); err != nil {
		return f.AppliedLSN(), fmt.Errorf("replication: catchup hello: %w", err)
	}
	typ, payload, err := readFrame(conn)
	if err != nil {
		return f.AppliedLSN(), fmt.Errorf("replication: catchup handshake: %w", err)
	}
	if typ != msgHelloOK {
		return f.AppliedLSN(), errors.New("replication: catchup unexpected handshake response")
	}
	last, _, err := decodeHelloOK(payload)
	if err != nil {
		return f.AppliedLSN(), fmt.Errorf("replication: catchup decode hello_ok: %w", err)
	}
	f.mu.Lock()
	f.recordMasterLSN(last)
	f.mu.Unlock()

	for f.AppliedLSN() < f.LastMasterLSN() {
		if err := ctx.Err(); err != nil {
			return f.AppliedLSN(), err
		}
		if dl, ok := ctx.Deadline(); ok {
			_ = conn.SetReadDeadline(dl)
		} else {
			_ = conn.SetReadDeadline(time.Now().Add(f.ReadTimeout))
		}
		typ, payload, err := readFrame(conn)
		if err != nil {
			if ctx.Err() != nil {
				return f.AppliedLSN(), ctx.Err()
			}
			return f.AppliedLSN(), err
		}
		_ = conn.SetReadDeadline(time.Time{})
		switch typ {
		case msgBinlog:
			e, derr := decodeBinFramePayload(payload)
			if derr != nil {
				return f.AppliedLSN(), derr
			}
			f.mu.Lock()
			f.recordMasterLSN(e.LSN)
			f.mu.Unlock()
			if aerr := f.ApplyBinlog(e); aerr != nil {
				return f.AppliedLSN(), aerr
			}
			if werr := writeFrame(conn, msgAck, encodeAck(e.LSN)); werr != nil {
				return f.AppliedLSN(), werr
			}
		case msgPing:
			ts, derr := decodePing(payload)
			if derr != nil {
				return f.AppliedLSN(), derr
			}
			if werr := writeFrame(conn, msgPong, encodePing(ts)); werr != nil {
				return f.AppliedLSN(), werr
			}
		case msgCapability:
			// 一次性追平连接忽略能力协商（不覆盖主复制循环的 peerCapFlags）。
		default:
			// HELLO_OK / ACK 等在从端无处理，忽略。
		}
	}
	return f.AppliedLSN(), nil
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

package replication

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// SlaveStatus 从节点状态快照（查询/观测用）。
type SlaveStatus struct {
	NodeID   string
	Remote   string
	Online   bool
	LastAck  storage.LSN
	LastSeen time.Time
}

// MasterReplicator 主端复制器：落盘 binlog + 监听复制端口 + 按位点推送 + 心跳。
type MasterReplicator struct {
	store BinlogStore
	ln    net.Listener

	// tlsConfig 非 nil 时复制端口使用 tls.Listen 加密监听（P0 TLS，可选）。
	tlsConfig *tls.Config

	mu       sync.Mutex
	nextNode uint64
	clients  map[string]*slaveConn // 在线从节点
	known    map[string]*slaveInfo // 全部（含离线，保留最后位点）
	closed   bool
	wake     chan struct{} // Append 后唤醒空闲推送循环

	// HeartbeatInterval 心跳发送间隔；HeartbeatTimeout 未收到任何帧即判离线。
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration

	// T33 同步/半同步复制（M20）状态：模式、半同步参数、ACK 等待与降级记录。
	mode       ReplMode        // 当前复制模式（默认 ModeAsync，M2 兼容）
	semiAckN   int             // 半同步最少确认副本数（默认 1）
	ackTimeout time.Duration   // ACK 等待超时（默认 10s）
	degraded   bool            // 半同步当前降级异步标志
	degLSN     storage.LSN     // 最近一次降级位点
	degAt      time.Time       // 最近一次降级时间
	degCount   int64           // 累计降级次数
	lastWaitMs int64           // 最近一次 WaitAck 实际等待毫秒
	waiting    bool            // 当前是否有提交在等待 ACK

	// compensateFn 补偿帧应用回调（阶段二回迁）：nil 表示不接受补偿。
	// 由上层（DB.StartReplicationTLS）装配，收到补偿批次时按序 apply 到本地
	// 存储并追加自身 binlog，使其他从端可同步到补偿数据。
	compensateFn func(entries []*BinlogEntry) error
}

type slaveConn struct {
	conn     net.Conn
	info     *slaveInfo
	lastRecv time.Time
}

type slaveInfo struct {
	nodeID  string
	remote  string
	online  bool
	lastAck storage.LSN
	lastSeen time.Time
}

// NewMaster 创建主端复制器：打开 binlog 并监听复制端口（addr 支持 ":0" 随机端口，
// 实际地址用 Addr() 获取）。Serve() 启动服务。复制端口为明文（旧行为）。
func NewMaster(binlogPath, addr string) (*MasterReplicator, error) {
	return NewMasterTLS(binlogPath, addr, nil)
}

// NewMasterTLS 创建主端复制器（P0 TLS，可选）：tlsCfg 非 nil 时复制端口
// 使用 tls.Listen 加密监听（从节点须以 TLS 连接）；nil 保持明文。
func NewMasterTLS(binlogPath, addr string, tlsCfg *tls.Config) (*MasterReplicator, error) {
	store, err := OpenBinlog(binlogPath)
	if err != nil {
		return nil, fmt.Errorf("replication: open binlog: %w", err)
	}
	var ln net.Listener
	if tlsCfg != nil {
		ln, err = tls.Listen("tcp", addr, tlsCfg)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("replication: listen %s: %w", addr, err)
	}
	return &MasterReplicator{
		store:             store,
		ln:                ln,
		tlsConfig:         tlsCfg,
		clients:           make(map[string]*slaveConn),
		known:             make(map[string]*slaveInfo),
		wake:              make(chan struct{}, 1),
		HeartbeatInterval: 2 * time.Second,
		HeartbeatTimeout:  10 * time.Second,
		mode:              ModeAsync,
		semiAckN:          DefaultSemiAckN,
		ackTimeout:        DefaultAckTimeout,
	}, nil
}

// Addr 返回复制监听地址（随机端口时返回实际端口）。
func (m *MasterReplicator) Addr() string { return m.ln.Addr().String() }

// BinlogPath 返回 binlog 落盘文件路径。
func (m *MasterReplicator) BinlogPath() string { return m.store.(*binlogStore).path }

// SetCompensationHandler 装配补偿帧应用回调（阶段二回迁）。nil 清除回调。
// 回调负责把补偿批次 apply 到本地存储并追加自身 binlog（幂等 PUT/DELETE）。
func (m *MasterReplicator) SetCompensationHandler(fn func(entries []*BinlogEntry) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.compensateFn = fn
}

// Serve 启动接受循环与心跳循环（异步）。
func (m *MasterReplicator) Serve() error {
	go m.acceptLoop()
	go m.heartbeatLoop()
	return nil
}

func (m *MasterReplicator) acceptLoop() {
	for {
		conn, err := m.ln.Accept()
		if err != nil {
			if m.isClosed() {
				return
			}
			continue
		}
		go m.handleSlave(conn)
	}
}

// handleSlave 处理一个从节点连接：握手 → 注册 → 读循环（ACK/PONG）+ 推送循环。
func (m *MasterReplicator) handleSlave(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second)) // 握手超时
	typ, payload, err := readFrame(conn)
	if err != nil || typ != msgHello {
		_ = conn.Close()
		return
	}
	role, from, err := decodeHello(payload)
	if err != nil {
		_ = conn.Close()
		return
	}
	// 阶段二回迁：陈旧主以 RoleCompensate 身份发补偿帧回灌，不注册为从端。
	if role == RoleCompensate {
		m.handleCompensation(conn)
		return
	}
	if role != RoleSlave {
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{}) // 清除握手超时

	m.mu.Lock()
	m.nextNode++
	nodeID := fmt.Sprintf("slave-%d", m.nextNode)
	now := time.Now()
	info := &slaveInfo{nodeID: nodeID, remote: conn.RemoteAddr().String(), online: true, lastSeen: now}
	m.known[nodeID] = info
	sc := &slaveConn{conn: conn, info: info, lastRecv: now}
	m.clients[nodeID] = sc
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		info.online = false
		delete(m.clients, nodeID)
		m.mu.Unlock()
		_ = conn.Close()
	}()

	if err := writeFrame(conn, msgHelloOK, encodeHelloOK(m.store.LastLSN(), m.ReplicationMode())); err != nil {
		return
	}
	// P0 HA：协议能力协商——HELLO_OK 后追加 CAPABILITY 帧（版本+能力位）。
	// 声明本协议支持 epoch/fencing 语义；旧节点忽略未知帧，模式（async/
	// semi-sync/sync）仍由 HELLO_OK 的 mode 字段表达，复制协议本身不变。
	if err := writeFrame(conn, msgCapability, encodeCapability(capEpochFencing)); err != nil {
		return
	}
	go m.readLoop(sc)
	m.pushLoop(sc, from)
}

// handleCompensation 处理一条补偿连接（阶段二回迁）：陈旧主以
// RoleCompensate 身份握手后发送 msgCompensate 补偿帧，逐批 apply+追加
// binlog 并回 ACK；连接关闭（EOF/断开）即结束，不注册从端、不推送 binlog。
// compensateFn 为 nil（旧版主端未装配）时拒绝处理直接断开。
func (m *MasterReplicator) handleCompensation(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second)) // 握手后仍留超时防呆连接
	if err := writeFrame(conn, msgHelloOK, encodeHelloOK(m.store.LastLSN(), m.ReplicationMode())); err != nil {
		return
	}
	if err := writeFrame(conn, msgCapability, encodeCapability(capEpochFencing)); err != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Time{}) // 清除握手超时
	for {
		typ, payload, err := readFrame(conn)
		if err != nil {
			return // EOF / 断开：补偿批次发送完毕
		}
		if typ != msgCompensate {
			continue // 忽略非补偿帧（补偿连接不允许其他上行帧）
		}
		_, entries, derr := decodeCompensation(payload)
		if derr != nil {
			return
		}
		m.mu.Lock()
		fn := m.compensateFn
		m.mu.Unlock()
		if fn == nil {
			return // 未装配补偿回调：拒绝本次补偿（发送端应走全量对齐兜底）
		}
		if aerr := fn(entries); aerr != nil {
			return
		}
		var last storage.LSN
		if len(entries) > 0 {
			last = entries[len(entries)-1].LSN
		}
		if werr := writeFrame(conn, msgAck, encodeAck(last)); werr != nil {
			return
		}
	}
}

// readLoop 读取从节点上行帧：ACK 推进位点，PONG 维持存活标记。
func (m *MasterReplicator) readLoop(sc *slaveConn) {
	for {
		typ, payload, err := readFrame(sc.conn)
		if err != nil {
			_ = sc.conn.Close()
			return
		}
		now := time.Now()
		m.mu.Lock()
		sc.lastRecv = now
		sc.info.lastSeen = now
		switch typ {
		case msgAck:
			if lsn, derr := decodeAck(payload); derr == nil && lsn > sc.info.lastAck {
				sc.info.lastAck = lsn
			}
		case msgPong:
			// 心跳应答：仅更新存活时间
		}
		m.mu.Unlock()
	}
}

// pushLoop 从 from 起流式推送 binlog；空闲时按心跳间隔发 PING。
func (m *MasterReplicator) pushLoop(sc *slaveConn, from storage.LSN) {
	conn := sc.conn
	cur := from
	for {
		err := m.store.ReadFrom(cur, func(e *BinlogEntry) error {
			data, derr := encodeBinRecord(e)
			if derr != nil {
				return derr
			}
			if werr := writeFrame(conn, msgBinlog, data); werr != nil {
				return werr
			}
			cur = e.LSN + 1
			return nil
		})
		if err != nil {
			if !errors.Is(err, ErrClosed) {
				_ = conn.Close()
			}
			return
		}
		if cur <= m.store.LastLSN() {
			continue // 推送期间有新条目，立即继续
		}
		select {
		case <-m.wake:
		case <-time.After(m.HeartbeatInterval):
			if werr := writeFrame(conn, msgPing, encodePing(time.Now().UnixMilli())); werr != nil {
				_ = conn.Close()
				return
			}
		}
		if m.isClosed() {
			return
		}
	}
}

// heartbeatLoop 定期检查从节点最后收到帧的时间，超时判离线并断开。
func (m *MasterReplicator) heartbeatLoop() {
	t := time.NewTicker(m.HeartbeatInterval)
	defer t.Stop()
	for range t.C {
		m.mu.Lock()
		now := time.Now()
		var victims []net.Conn
		for _, sc := range m.clients {
			if now.Sub(sc.lastRecv) > m.HeartbeatTimeout {
				sc.info.online = false
				victims = append(victims, sc.conn)
			}
		}
		m.mu.Unlock()
		for _, c := range victims {
			_ = c.Close()
		}
		if m.isClosed() {
			return
		}
	}
}

// Append 落盘 binlog 并唤醒推送（异步）。
func (m *MasterReplicator) Append(entry *BinlogEntry) error {
	if err := m.store.Append(entry); err != nil {
		return err
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return nil
}

// Pull 从指定位点拉取 binlog 条目（程序化读取，顺序）。
func (m *MasterReplicator) Pull(from storage.LSN) ([]*BinlogEntry, error) {
	var out []*BinlogEntry
	err := m.store.ReadFrom(from, func(e *BinlogEntry) error {
		out = append(out, e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Ack 记录从节点已确认位点（离线信息保留，供续传观测）。
func (m *MasterReplicator) Ack(node string, lsn storage.LSN) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if info := m.known[node]; info != nil && lsn > info.lastAck {
		info.lastAck = lsn
	}
	return nil
}

// LastLSN 返回当前 binlog 最大位点。
func (m *MasterReplicator) LastLSN() storage.LSN { return m.store.LastLSN() }

// SlaveStatus 返回全部已知从节点状态（在线 + 离线）。
func (m *MasterReplicator) SlaveStatus() []SlaveStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SlaveStatus, 0, len(m.known))
	for _, info := range m.known {
		out = append(out, SlaveStatus{
			NodeID:   info.nodeID,
			Remote:   info.remote,
			Online:   info.online,
			LastAck:  info.lastAck,
			LastSeen: info.lastSeen,
		})
	}
	return out
}

func (m *MasterReplicator) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// Close 关闭监听、全部从节点连接与 binlog。
func (m *MasterReplicator) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	conns := make([]net.Conn, 0, len(m.clients))
	for _, sc := range m.clients {
		conns = append(conns, sc.conn)
	}
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	if m.ln != nil {
		_ = m.ln.Close()
	}
	return m.store.Close()
}

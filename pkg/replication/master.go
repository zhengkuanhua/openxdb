package replication

import (
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

	mu       sync.Mutex
	nextNode uint64
	clients  map[string]*slaveConn // 在线从节点
	known    map[string]*slaveInfo // 全部（含离线，保留最后位点）
	closed   bool
	wake     chan struct{} // Append 后唤醒空闲推送循环

	// HeartbeatInterval 心跳发送间隔；HeartbeatTimeout 未收到任何帧即判离线。
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
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
// 实际地址用 Addr() 获取）。Serve() 启动服务。
func NewMaster(binlogPath, addr string) (*MasterReplicator, error) {
	store, err := OpenBinlog(binlogPath)
	if err != nil {
		return nil, fmt.Errorf("replication: open binlog: %w", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("replication: listen %s: %w", addr, err)
	}
	return &MasterReplicator{
		store:             store,
		ln:                ln,
		clients:           make(map[string]*slaveConn),
		known:             make(map[string]*slaveInfo),
		wake:              make(chan struct{}, 1),
		HeartbeatInterval: 2 * time.Second,
		HeartbeatTimeout:  10 * time.Second,
	}, nil
}

// Addr 返回复制监听地址（随机端口时返回实际端口）。
func (m *MasterReplicator) Addr() string { return m.ln.Addr().String() }

// BinlogPath 返回 binlog 落盘文件路径。
func (m *MasterReplicator) BinlogPath() string { return m.store.(*binlogStore).path }

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
	if err != nil || role != RoleSlave {
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

	if err := writeFrame(conn, msgHelloOK, encodeHelloOK(m.store.LastLSN())); err != nil {
		return
	}
	go m.readLoop(sc)
	m.pushLoop(sc, from)
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

package cluster

// M4 节点拓扑：NodeInfo / 节点注册表 / 集群管理器。
//
// 形态决策（文档 docs/T13_m4_multinode.md）：
//   - 节点注册表 = 内存 Registry + 全局键 m:nodes 落盘（与 m:regions 同层）；
//     ADD NODE 时先完成 TCP 握手（NODE_HELLO/NODE_HELLO_OK 双向身份确认），
//     再写入内存与落盘；DB.Open 时从 m:nodes 反序列化恢复；
//   - 本节点身份 node_id 由 db.Init 写入 openxdb.conf（node_id = node-<8hex>），
//     未初始化该字段的旧数据目录以数据目录短哈希兜底，保证多节点部署可区分；
//   - 存活检测复用 M2 PING/PONG 心跳协议（ts 8B UnixMilli），
//     由 Manager.StartHeartbeat 周期探测远端节点，超时标记 DOWN；
//   - 查询转发走 QUERY_REQ/QUERY_RESP（物理键区间直扫，见 client.go）。

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"
)

// NodeRole 节点角色（M4 第一簇仅数据节点；协调/元数据服务下簇引入）。
type NodeRole string

const (
	// RoleData 数据节点：持有 region 数据并提供查询转发服务。
	RoleData NodeRole = "DATA"
)

// NodeState 节点存活状态。
type NodeState string

const (
	// StateUp 节点在线（心跳正常 / 本节点）。
	StateUp NodeState = "UP"
	// StateDown 节点离线（心跳超时）。
	StateDown NodeState = "DOWN"
)

// NodeInfo 集群节点元信息。
type NodeInfo struct {
	ID       string    `json:"id"`
	Addr     string    `json:"addr"`
	Role     NodeRole  `json:"role"`
	State    NodeState `json:"state"`
	LastSeen int64     `json:"last_seen"` // UnixMilli；0 = 未知
}

// MetaNodesKey 节点注册表落盘全局键（不带 region 前缀，与 m:regions 同层）。
func MetaNodesKey() []byte { return []byte("m:nodes") }

// MarshalNodes 序列化节点注册表。
func MarshalNodes(nodes []NodeInfo) ([]byte, error) {
	return json.Marshal(nodes)
}

// UnmarshalNodes 反序列化 m:nodes 载荷。
func UnmarshalNodes(raw []byte) ([]NodeInfo, error) {
	var nodes []NodeInfo
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

// 标准错误。
var (
	// ErrNodeExists 节点已注册。
	ErrNodeExists = errors.New("cluster: node already registered")
	// ErrNodeNotFound 节点未注册。
	ErrNodeNotFound = errors.New("cluster: node not registered")
	// ErrNodeDown 节点已离线（转发目标不可达）。
	ErrNodeDown = errors.New("cluster: node is down")
	// ErrClusterNotEnabled 未启用集群（未注册任何远端节点）。
	ErrClusterNotEnabled = errors.New("cluster: not enabled (no remote nodes)")
)

// Manager 集群管理器：节点注册表 + 心跳 + 查询转发客户端。
type Manager struct {
	SelfID  string
	SelfAddr string

	mu    sync.RWMutex
	nodes map[string]*NodeInfo

	clients    map[string]*Client // nodeID -> 复用客户端（惰性建立）
	heartbeats *heartbeatLoop
	hbMu       sync.Mutex
	stopped    bool
}

// NewManager 创建集群管理器（selfID 必须非空）。
func NewManager(selfID string) *Manager {
	if selfID == "" {
		selfID = "node-local"
	}
	return &Manager{
		SelfID:  selfID,
		nodes:   map[string]*NodeInfo{},
		clients: map[string]*Client{},
	}
}

// SelfNode 返回本节点信息。
func (m *Manager) SelfNode() NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if n, ok := m.nodes[m.SelfID]; ok {
		return *n
	}
	return NodeInfo{ID: m.SelfID, Addr: m.SelfAddr, Role: RoleData, State: StateUp}
}

// RegisterSelf 注册本节点（db.Open 初始化时调用，不落盘 m:nodes——本节点无需注册到自身）。
func (m *Manager) RegisterSelf(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SelfAddr = addr
	m.nodes[m.SelfID] = &NodeInfo{
		ID: m.SelfID, Addr: addr, Role: RoleData, State: StateUp,
		LastSeen: time.Now().UnixMilli(),
	}
}

// AddNode 注册远端节点：先 TCP 握手（NODE_HELLO）确认对端身份，再写入注册表。
// 返回对端自报 NodeID（注册名 = 对端 NodeID；重复注册报错）。
func (m *Manager) AddNode(addr string) (string, error) {
	hello, err := m.helloRemote(addr)
	if err != nil {
		return "", err
	}
	if hello.NodeID == "" {
		return "", errors.New("cluster: remote node returned empty id")
	}
	if hello.NodeID == m.SelfID {
		return "", errors.New("cluster: cannot add self node")
	}
	m.mu.Lock()
	if _, ok := m.nodes[hello.NodeID]; ok {
		m.mu.Unlock()
		return "", ErrNodeExists
	}
	m.nodes[hello.NodeID] = &NodeInfo{
		ID: hello.NodeID, Addr: addr, Role: RoleData, State: StateUp,
		LastSeen: time.Now().UnixMilli(),
	}
	m.mu.Unlock()
	return hello.NodeID, nil
}

// SetNodes 整体替换注册表（db.Open 从 m:nodes 恢复时调用；保持本节点条目存在）。
func (m *Manager) SetNodes(nodes []NodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = map[string]*NodeInfo{}
	for i := range nodes {
		n := nodes[i]
		m.nodes[n.ID] = &n
	}
	if _, ok := m.nodes[m.SelfID]; !ok {
		m.nodes[m.SelfID] = &NodeInfo{ID: m.SelfID, Addr: m.SelfAddr, Role: RoleData, State: StateUp, LastSeen: time.Now().UnixMilli()}
	}
}

// ListNodes 返回注册表快照（含本节点，按 ID 排序）。
func (m *Manager) ListNodes() []NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// GetNode 返回节点信息。
func (m *Manager) GetNode(id string) (NodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[id]
	if !ok {
		return NodeInfo{}, false
	}
	return *n, true
}

// NodeAddr 返回节点链路地址（未注册报错）。
func (m *Manager) NodeAddr(id string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[id]
	if !ok {
		return "", ErrNodeNotFound
	}
	return n.Addr, nil
}

// HasRemoteNodes 是否存在已注册的远端节点（用于集群能力判断）。
func (m *Manager) HasRemoteNodes() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id := range m.nodes {
		if id != m.SelfID {
			return true
		}
	}
	return false
}

// RegisterPeer 注册对端节点（双向握手：收到 NODE_HELLO 时调用）。
// 已存在则刷新地址与状态；本节点自身忽略。
func (m *Manager) RegisterPeer(id, addr string) {
	if id == "" || id == m.SelfID {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[id]; ok {
		n.Addr = addr
		n.State = StateUp
		if t := time.Now().UnixMilli(); t > n.LastSeen {
			n.LastSeen = t
		}
		return
	}
	m.nodes[id] = &NodeInfo{ID: id, Addr: addr, Role: RoleData, State: StateUp, LastSeen: time.Now().UnixMilli()}
}

// MarkHeartbeat 记录对端心跳响应时间（更新 LastSeen 并置 UP）。
func (m *Manager) MarkHeartbeat(id string, ts int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[id]; ok {
		n.State = StateUp
		if ts > n.LastSeen {
			n.LastSeen = ts
		}
	}
}

// MarkDown 标记节点离线。
func (m *Manager) MarkDown(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[id]; ok {
		n.State = StateDown
	}
}

// ensureRemote 校验转发目标存在且在线。
func (m *Manager) ensureRemote(id string) (*NodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[id]
	if !ok {
		return nil, ErrNodeNotFound
	}
	if id == m.SelfID {
		return n, nil
	}
	if n.State == StateDown {
		return nil, ErrNodeDown
	}
	return n, nil
}

// ---- 查询转发（client.go 实现底层会话） ----

// ScanRemote 将物理区间 [start, end) 转发到归属节点执行，返回远端行。
func (m *Manager) ScanRemote(nodeID string, start, end []byte, limit int) ([]QueryRow, error) {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return nil, err
	}
	return m.clientFor(n.ID).Scan(start, end, limit)
}

// GetRemote 将点查转发到归属节点执行；未命中返回 storage.ErrNotFound。
func (m *Manager) GetRemote(nodeID string, key []byte) ([]byte, error) {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return nil, err
	}
	return m.clientFor(n.ID).Get(key)
}

// helloRemote 与远端完成节点握手（连接 + NODE_HELLO -> NODE_HELLO_OK）。
func (m *Manager) helloRemote(addr string) (*HelloOKPayload, error) {
	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	payload, err := marshalJSON(HelloPayload{NodeID: m.SelfID, Addr: m.SelfAddr, Version: protoVersion})
	if err != nil {
		return nil, err
	}
	if err := writeFrame(conn, msgNodeHello, payload); err != nil {
		return nil, err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return nil, err
	}
	if typ != msgNodeHelloOK {
		return nil, errors.New("cluster: unexpected handshake reply")
	}
	var ok HelloOKPayload
	if err := unmarshalJSON(raw, &ok); err != nil {
		return nil, err
	}
	return &ok, nil
}

// clientFor 惰性获取（复用）到某节点的客户端会话。
func (m *Manager) clientFor(nodeID string) *Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.clients[nodeID]; ok {
		return c
	}
	c := &Client{nodeID: nodeID, mgr: m}
	m.clients[nodeID] = c
	return c
}

// ---- 心跳循环（heartbeat.go 实现） ----

// StartHeartbeat 启动心跳循环：周期对每个远端节点 PING，
// 超过 timeout 未收到 PONG 的节点标记 DOWN。interval<=0 时默认 2s，timeout<=0 默认 6s。
// 返回后可调用 Stop 停止。
func (m *Manager) StartHeartbeat(interval, timeout time.Duration) {
	m.hbMu.Lock()
	defer m.hbMu.Unlock()
	if m.heartbeats != nil {
		return
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if timeout <= 0 {
		timeout = 6 * time.Second
	}
	m.heartbeats = newHeartbeatLoop(m, interval, timeout)
	m.heartbeats.start()
}

// Stop 停止心跳循环并关闭全部复用连接。
func (m *Manager) Stop() {
	m.hbMu.Lock()
	if m.heartbeats != nil {
		m.heartbeats.stop()
		m.heartbeats = nil
	}
	m.hbMu.Unlock()
	m.mu.Lock()
	for id, c := range m.clients {
		c.close()
		delete(m.clients, id)
	}
	m.mu.Unlock()
}

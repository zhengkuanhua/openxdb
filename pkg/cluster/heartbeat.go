package cluster

// 心跳与存活检测：复用 M2 PING/PONG 帧协议（type 5/6，ts 8B UnixMilli）。
// 每周期对每个已注册远端节点发送 PING；收到 PONG 更新 LastSeen 并置 UP；
// 距离最近一次 PONG 超过 timeout 的节点标记 DOWN（查询转发时拒绝路由）。

import (
	"sync"
	"time"
)

// heartbeatLoop 周期探测远端节点存活的循环。
type heartbeatLoop struct {
	mgr     *Manager
	interval time.Duration
	timeout  time.Duration
	stopCh   chan struct{}
	once     sync.Once
}

func newHeartbeatLoop(m *Manager, interval, timeout time.Duration) *heartbeatLoop {
	return &heartbeatLoop{mgr: m, interval: interval, timeout: timeout, stopCh: make(chan struct{})}
}

func (h *heartbeatLoop) start() { go h.loop() }

func (h *heartbeatLoop) stop() { h.once.Do(func() { close(h.stopCh) }) }

func (h *heartbeatLoop) loop() {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			h.tick()
		}
	}
}

func (h *heartbeatLoop) tick() {
	now := time.Now().UnixMilli()
	for _, n := range h.mgr.ListNodes() {
		if n.ID == h.mgr.SelfID {
			continue
		}
		h.pingNode(n, now)
	}
	// 离线判定：LastSeen 距今超过 timeout
	for _, n := range h.mgr.ListNodes() {
		if n.ID == h.mgr.SelfID {
			continue
		}
		if n.State == StateDown {
			continue
		}
		// 启动竞态防护：本进程内从未成功收到过该节点 PONG 时，
		// 不将其判为 DOWN（避免集群滚动启动时误触发 region 自动重指派）。
		if !h.mgr.EverUp(n.ID) {
			continue
		}
		if now-n.LastSeen > h.timeout.Milliseconds() {
			h.mgr.MarkDown(n.ID)
			// M6：UP->DOWN 首次转换时通知 SQL 层触发 region 自动重指派/读 failover
			h.mgr.notifyDown(n.ID)
		}
	}
}

// pingNode 对单节点发送 PING 并处理 PONG。
func (h *heartbeatLoop) pingNode(n NodeInfo, now int64) {
	conn, err := dial(n.Addr)
	if err != nil {
		return // 连接失败交给超时离线判定
	}
	defer conn.Close()
	if err := writeFrame(conn, msgPing, pingPayload(now)); err != nil {
		return
	}
	typ, payload, err := readFrame(conn)
	if err != nil {
		return
	}
	if typ != msgPong {
		return
	}
	ts, err := parseTs(payload)
	if err != nil {
		return
	}
	h.mgr.MarkHeartbeat(n.ID, ts)
}

// Probe 对单个注册节点做一次 PING/PONG 探活（P0 HA 升主多数派仲裁用）。
// 可达返回 true；地址缺失/拨号失败/超时/非法响应返回 false。不修改节点状态。
func (m *Manager) Probe(id string) bool {
	addr, err := m.NodeAddr(id)
	if err != nil {
		return false
	}
	conn, err := dial(addr)
	if err != nil {
		return false
	}
	defer conn.Close()
	if err := writeFrame(conn, msgPing, pingPayload(time.Now().UnixMilli())); err != nil {
		return false
	}
	typ, payload, err := readFrame(conn)
	if err != nil {
		return false
	}
	if typ != msgPong {
		return false
	}
	if _, err := parseTs(payload); err != nil {
		return false
	}
	return true
}

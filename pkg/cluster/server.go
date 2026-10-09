package cluster

// 节点链路服务端：监听集群端口，处理节点间协议帧。
//   - NODE_HELLO -> NODE_HELLO_OK：双向身份握手（本节点回执自身 NodeID/Addr）；
//   - QUERY_REQ -> QUERY_RESP：按物理键区间直扫本机存储并原样返回字节行；
//   - PING -> PONG：复用 M2 心跳协议（存活检测）。
//
// 查询执行直接走存储层（storage.Storage 的 Scan/Get），不经过 SQL 执行器：
// 转发只传"物理键区间"，远端无需感知表结构；单行点查/区间扫描均为只读，
// 本簇不做跨节点一致性快照（文档说明留待下簇）。

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// Server 集群节点服务。
type Server struct {
	SelfID  string
	SelfAddr string
	st      storage.Storage
	// router 分片路由（ASSIGN REGION 数据装载时学习 region 归属）。
	// 由 db.StartCluster 注入，与 SQL 执行器共享同一指针。
	router *sharding.Router
	// mgr 集群管理器（双向握手时注册对端节点）。由 db.StartCluster 注入。
	mgr *Manager

	ln net.Listener
	wg sync.WaitGroup
	// idleTimeout 空闲连接保活上限（读超时防御，默认 30s）。
	idleTimeout time.Duration
	closed      chan struct{}
	once        sync.Once
}

// NewServer 创建节点服务（selfID 为对端可见的节点身份）。
func NewServer(st storage.Storage, selfID string) *Server {
	return &Server{
		SelfID:     selfID,
		st:         st,
		idleTimeout: 30 * time.Second,
		closed:     make(chan struct{}),
	}
}

// SetAddr 记录本节点对外地址（供 HELLO_OK 回执）。
func (s *Server) SetAddr(addr string) { s.SelfAddr = addr }

// SetRouter 注入分片路由（供 REGION_PUSH 数据装载更新本地路由视图）。
func (s *Server) SetRouter(r *sharding.Router) { s.router = r }

// SetManager 注入集群管理器（供 NODE_HELLO 双向注册对端节点）。
func (s *Server) SetManager(m *Manager) { s.mgr = m }

// Serve 在已有 listener 上提供服务，返回实际监听地址（127.0.0.1:0 场景获取端口）。
func (s *Server) Serve(ln net.Listener) (string, error) {
	s.ln = ln
	addr := ln.Addr().String()
	if s.SelfAddr == "" {
		s.SelfAddr = addr
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return addr, nil
}

// ListenAndServe 监听 addr 并启动服务，返回实际监听地址。
func (s *Server) ListenAndServe(addr string) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	return s.Serve(ln)
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			s.handleConn(conn)
		}()
	}
}

// Close 关闭服务。
func (s *Server) Close() error {
	s.once.Do(func() { close(s.closed) })
	if s.ln != nil {
		s.ln.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	// 每帧读超时防御：长时间无数据视为异常连接
	for {
		conn.SetReadDeadline(time.Now().Add(s.idleTimeout))
		typ, payload, err := readFrame(conn)
		if err != nil {
			return
		}
		switch typ {
		case msgNodeHello:
			s.handleHello(conn, payload)
		case msgQueryReq:
			s.handleQuery(conn, payload)
		case msgRegionPush:
			s.handlePut(conn, payload)
		case msgPing:
			// 复用 M2 心跳：原样回 ts
			writeFrame(conn, msgPong, payload)
		default:
			return // 未知帧：断开
		}
	}
}

// handleHello 处理节点握手：双向注册对端，回执自身 NodeID/Addr。
func (s *Server) handleHello(conn net.Conn, payload []byte) {
	var hello HelloPayload
	if err := unmarshalJSON(payload, &hello); err != nil {
		return
	}
	// 双向注册：发起方加入本机注册表（含 m:nodes 落盘，重启后仍可路由到对方）。
	// 握手是低频操作，落盘失败不阻断回执（内存注册已生效，转发可用）。
	if s.mgr != nil && hello.NodeID != "" && hello.NodeID != s.SelfID {
		s.mgr.RegisterPeer(hello.NodeID, hello.Addr)
		s.persistNodes()
	}
	resp, err := marshalJSON(HelloOKPayload{NodeID: s.SelfID, Addr: s.SelfAddr})
	if err != nil {
		return
	}
	writeFrame(conn, msgNodeHelloOK, resp)
}

// persistNodes 将节点注册表落盘 m:nodes（双向握手触发，与 execAddNode 落盘同一键）。
func (s *Server) persistNodes() {
	if s.st == nil || s.mgr == nil {
		return
	}
	raw, err := MarshalNodes(s.mgr.ListNodes())
	if err != nil {
		return
	}
	batch := &storage.WriteBatch{}
	batch.Puts = append(batch.Puts, storage.KVPair{Key: MetaNodesKey(), Value: raw})
	_ = s.st.Write(batch)
}

// handleQuery 执行转发查询（只读：Scan/Get）。
func (s *Server) handleQuery(conn net.Conn, payload []byte) {
	var req QueryReq
	if err := unmarshalJSON(payload, &req); err != nil {
		writeFrame(conn, msgQueryResp, mustJSON(QueryResp{Err: "bad request"}))
		return
	}
	var resp QueryResp
	switch req.Op {
	case OpGet:
		v, err := s.st.Get(req.Start)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				resp = QueryResp{Err: remoteNotFound}
			} else {
				resp = QueryResp{Err: err.Error()}
			}
		} else {
			resp = QueryResp{Row: &QueryRow{Key: req.Start, Value: v}}
		}
	case OpScan:
		rows, err := s.scanRange(req.Start, req.End, req.Limit)
		if err != nil {
			resp = QueryResp{Err: err.Error()}
		} else {
			resp = QueryResp{Rows: rows}
		}
	default:
		resp = QueryResp{Err: "unknown op"}
	}
	writeFrame(conn, msgQueryResp, mustJSON(resp))
}

// handlePut 处理 REGION_PUSH：同一 WriteBatch 内写入全量行 + 学习路由 + 落盘 m:regions。
func (s *Server) handlePut(conn net.Conn, payload []byte) {
	var req RegionPushReq
	if err := unmarshalJSON(payload, &req); err != nil {
		writeFrame(conn, msgRegionPushOK, mustJSON(RegionPushResp{Err: "bad push request"}))
		return
	}
	resp, err := s.applyPush(&req)
	if err != nil {
		writeFrame(conn, msgRegionPushOK, mustJSON(RegionPushResp{Err: err.Error()}))
		return
	}
	writeFrame(conn, msgRegionPushOK, mustJSON(resp))
}

// applyPush 执行数据装载：目标节点将 region 数据与路由原子写入本机。
// region 元信息以"本节点"视图落盘（Node=""、Local=true），
// 数据写入物理键空间原样保留（含 region 前缀），覆盖式装载幂等。
func (s *Server) applyPush(req *RegionPushReq) (RegionPushResp, error) {
	if s.router == nil {
		return RegionPushResp{}, errors.New("cluster: router not injected")
	}
	batch := &storage.WriteBatch{}
	for _, row := range req.Rows {
		batch.Puts = append(batch.Puts, storage.KVPair{Key: row.Key, Value: row.Value})
	}
	// 学习源节点路由视图（第一簇约定：源视图为权威）：
	// 其余 region 按其归属记录（Node 非空直接学习；Node=="" 表示源节点本地持有，
	// 目标节点需补记为 req.SrcID，保证转发可路由）。
	// 本次接收的 region 单独处理为本地（见下）。
	for _, route := range req.Routes {
		if route.RegionID == req.Region.RegionID {
			continue // 本次接收 region 稍后单独落本地
		}
		if route.Node == "" {
			route.Node = req.SrcID
			route.Local = false
		}
		if err := s.router.UpsertRegion(route); err != nil {
			return RegionPushResp{}, err
		}
	}
	// 学习路由（本地视图）
	rg := req.Region
	rg.Node = ""
	rg.Local = true
	rg.State = sharding.StateActive
	if err := s.router.UpsertRegion(rg); err != nil {
		return RegionPushResp{}, err
	}
	// 落盘 m:regions（路由表变更随数据同批次原子提交）
	seq, regions := s.router.Dump()
	metaRaw, err := sharding.MarshalMeta(seq, regions)
	if err != nil {
		return RegionPushResp{}, err
	}
	batch.Puts = append(batch.Puts, storage.KVPair{Key: sharding.MetaRegionsKey(), Value: metaRaw})
	if err := s.st.Write(batch); err != nil {
		return RegionPushResp{}, err
	}
	return RegionPushResp{Rows: len(req.Rows)}, nil
}

// scanRange 区间扫描 [start, end)；end 为空表示无上界。
func (s *Server) scanRange(start, end []byte, limit int) ([]QueryRow, error) {
	rows, err := s.st.Scan(storage.KeyRange{Start: start, End: end}, limit)
	if err != nil {
		return nil, err
	}
	out := make([]QueryRow, 0, len(rows))
	for _, kv := range rows {
		out = append(out, QueryRow{Key: kv.Key, Value: kv.Value})
	}
	return out, nil
}

// mustJSON 序列化响应（handleQuery 内错误已由上层处理，此处不返回错误）。
func mustJSON(v interface{}) []byte {
	b, _ := marshalJSON(v)
	return b
}

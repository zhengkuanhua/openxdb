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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/tso"
)

// Server 集群节点服务。
type Server struct {
	SelfID   string
	SelfAddr string
	st       storage.Storage
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

	// 2PC participant state (M5): pending2pc caches prepared ops;
	// durable marker keys live under m:2pc:* (see txn2pc.go).
	mu2pc      sync.Mutex
	pending2pc map[string][]TxnOp
	// prepareFail 参与者 prepare 阶段故障注入（测试专用；nil = 正常路径）。
	prepareFail func(txID string) error
	// commitFail 参与者 commit 阶段故障注入（测试专用；nil = 正常路径）。
	commitFail func(txID string) error
	// tso 本地/全局时间戳源（M8）：nil = 未装配（旧路径，不做版本过滤）。
	// 中心化部署下本节点即为发号权威，TSO_REQ 由其批量发放。
	tso tso.Source
}

// SetTSO 注入时间戳源（M8：db.StartCluster 装配本地发号器）。
// nil 表示不启用快照隔离（单机旧路径零回归）。
func (s *Server) SetTSO(src tso.Source) { s.tso = src }

// SetPrepareFail 设置参与者 prepare 阶段故障钩子（仅测试使用；传 nil 恢复）。
func (s *Server) SetPrepareFail(fn func(txID string) error) {
	s.mu2pc.Lock()
	defer s.mu2pc.Unlock()
	s.prepareFail = fn
}

// SetCommitFail 注入参与者 commit 阶段故障（测试专用；nil = 正常路径）。
func (s *Server) SetCommitFail(fn func(txID string) error) {
	s.mu2pc.Lock()
	defer s.mu2pc.Unlock()
	s.commitFail = fn
}

// NewServer 创建节点服务（selfID 为对端可见的节点身份）。
func NewServer(st storage.Storage, selfID string) *Server {
	return &Server{
		SelfID:      selfID,
		st:          st,
		idleTimeout: 30 * time.Second,
		closed:      make(chan struct{}),
		pending2pc:  map[string][]TxnOp{},
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
		case msg2pcPrepareReq:
			s.handle2pcPrepare(conn, payload)
		case msg2pcCommitReq:
			s.handle2pcCommit(conn, payload)
		case msg2pcAbortReq:
			s.handle2pcAbort(conn, payload)
		case msgTsoReq:
			s.handleTso(conn, payload)
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
		rows, err := s.scanRange(req.Start, req.End, req.Limit, req.BeginTS, req.Sort)
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
// beginTS>0 时逐行做快照版本过滤（M8 分布式快照隔离）。
// spec 非空时（M12-2）：过滤后按 spec 提取排序键本地排序，再截取
// [Offset, Offset+Limit) 窗口返回；Limit<=0 表示窗口无上界。
func (s *Server) scanRange(start, end []byte, limit int, beginTS uint64, spec *SortSpec) ([]QueryRow, error) {
	rows, err := s.st.Scan(storage.KeyRange{Start: start, End: end}, limit)
	if err != nil {
		return nil, err
	}
	out := make([]QueryRow, 0, len(rows))
	for _, kv := range rows {
		if beginTS > 0 {
			visible, verr := VersionVisible(s.st.Scan, kv.Key, beginTS)
			if verr != nil {
				return nil, verr
			}
			if !visible {
				continue
			}
		}
		out = append(out, QueryRow{Key: kv.Key, Value: kv.Value})
	}
	if spec == nil {
		return out, nil
	}
	// M12-2 本地排序 + 窗口截取：coordinator 只需归并已有序的窗口行。
	sort.SliceStable(out, func(i, j int) bool {
		return sortRowLess(out[i], out[j], spec)
	})
	if spec.Offset > 0 {
		if spec.Offset >= len(out) {
			out = out[:0]
		} else {
			out = out[spec.Offset:]
		}
	}
	if spec.Limit > 0 && len(out) > spec.Limit {
		out = out[:spec.Limit]
	}
	return out, nil
}

// sortRowLess 按 SortSpec 比较两行：提取第 ColIndex 列排序键后按类型比较。
// INT/DECIMAL 按数值（DECIMAL 按 Scale 位定点整数）；TEXT/DATE/BLOB 按字符串。
func sortRowLess(a, b QueryRow, spec *SortSpec) bool {
	ka, ea := sortRowKey(a, spec)
	kb, eb := sortRowKey(b, spec)
	if ea != nil {
		return false
	}
	if eb != nil {
		return true
	}
	var cmp int
	if spec.Kind == "INT" || spec.Kind == "DECIMAL" {
		if ka.i < kb.i {
			cmp = -1
		} else if ka.i > kb.i {
			cmp = 1
		}
	} else {
		cmp = strings.Compare(ka.s, kb.s)
	}
	if spec.Desc {
		return cmp > 0
	}
	return cmp < 0
}

// sortKeyVal 排序键（数值或字符串二选一）。
type sortKeyVal struct {
	i int64
	s string
}

// sortRowKey 从行 JSON 数组提取排序键。
// INT：JSON number → int64；DECIMAL：规范化定点串 → scaled int64；
// 其它（TEXT/DATE/BLOB）：字符串原样。
func sortRowKey(row QueryRow, spec *SortSpec) (sortKeyVal, error) {
	var arr []interface{}
	if err := unmarshalJSON(row.Value, &arr); err != nil {
		return sortKeyVal{}, err
	}
	if spec.ColIndex < 0 || spec.ColIndex >= len(arr) {
		return sortKeyVal{}, errors.New("cluster: sort col index out of range")
	}
	switch spec.Kind {
	case "INT":
		f, ok := arr[spec.ColIndex].(float64)
		if !ok {
			return sortKeyVal{}, errors.New("cluster: INT sort key not numeric")
		}
		return sortKeyVal{i: int64(f)}, nil
	case "DECIMAL":
		s, ok := arr[spec.ColIndex].(string)
		if !ok {
			return sortKeyVal{}, errors.New("cluster: DECIMAL sort key not string")
		}
		scaled, err := decSortScaled(s, spec.Scale)
		if err != nil {
			return sortKeyVal{}, err
		}
		return sortKeyVal{i: scaled}, nil
	default:
		s, _ := arr[spec.ColIndex].(string)
		return sortKeyVal{s: s}, nil
	}
}

// decSortScaled 解析定点十进制串为缩放整数（scale 位小数，如 4 位 "12.3400" → 123400）。
// 与 sql 包 decCanonical/parseDecimal 的定点语义保持一致，供远端排序精确比较。
func decSortScaled(s string, scale int) (int64, error) {
	neg := false
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		neg = s[i] == '-'
		i++
	}
	intPart := int64(0)
	intDigits := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		intPart = intPart*10 + int64(s[i]-'0')
		intDigits++
		i++
	}
	if intDigits == 0 {
		return 0, errors.New("cluster: invalid DECIMAL: " + s)
	}
	frac := int64(0)
	fracDigits := 0
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			frac = frac*10 + int64(s[i]-'0')
			fracDigits++
			i++
		}
	}
	if i != len(s) {
		return 0, errors.New("cluster: invalid DECIMAL: " + s)
	}
	for fracDigits < scale {
		frac *= 10
		fracDigits++
	}
	if fracDigits > scale {
		return 0, errors.New("cluster: DECIMAL too many fractional digits: " + s)
	}
	unit := int64(1)
	for j := 0; j < scale; j++ {
		unit *= 10
	}
	scaled := intPart*unit + frac
	if neg {
		scaled = -scaled
	}
	return scaled, nil
}

// mustJSON 序列化响应（handleQuery 内错误已由上层处理，此处不返回错误）。
func mustJSON(v interface{}) []byte {
	b, _ := marshalJSON(v)
	return b
}

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
	"log"
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
	// ddlApply 节点侧 DDL 元数据 apply 回调（T34）：由 db.StartCluster 装配
	// sql.Executor.ApplyRemoteDDL；nil = 未装配（不接收目录变更）。
	ddlApply func(req DDLPushReq) error
	// backupPrepare 节点侧 T35 备份候选点回调（db.StartCluster 装配
	// sql.Executor.RemoteBackupPrepare）；nil = 未装配（回执 Err）。
	backupPrepare func(req BackupPrepareReq) (BackupPrepareResp, error)
	// backupDo 节点侧 T35 分布式备份执行回调（db.StartCluster 装配
	// sql.Executor.RemoteBackupDo）；nil = 未装配（回执 Err）。
	backupDo func(req BackupDoReq) (BackupDoResp, error)
	// restoreDo 节点侧 T35 分布式恢复执行回调（db.StartCluster 装配
	// sql.Executor.RemoteRestoreDo）；nil = 未装配（回执 Err）。
	restoreDo func(req RestoreDoReq) (RestoreDoResp, error)
	// epochNotify 节点侧 P0 HA epoch 广播接收回调（db.StartCluster 装配
	// db.DB.NoteEpoch）；nil = 未装配（忽略 EPOCH_NOTIFY，旧节点零回归）。
	epochNotify func(epoch uint64)
	// regionEpochCheck 节点侧 P1 HA 路由消息 epoch 校验回调（db.StartCluster 装配
	// sql.Executor.CheckRegionPushEpoch）；nil = 未装配（跳过校验，旧节点/关闭开关零回归）。
	// 签名：clusterEpoch=消息级全局纪元，regionEpoch=该 region owner 变更纪元，rid=region ID；
	// 返回 err 表示消息陈旧，接收端拒绝更新路由并记日志。
	regionEpochCheck func(clusterEpoch, regionEpoch uint64, rid storage.ID) error
	// fullAlignFn 节点侧 v1.7.0 全量对齐回调（db.StartCluster 装配
	// sql.Executor.PushAllRegionsToNode）；nil = 未装配（FULLALIGN_REQ 回执 Err）。
	// 签名：dest=推送目的地 NodeID（请求方自身，旧主）。
	fullAlignFn func(dest string) error
	// routeSyncFn 节点侧权威路由视图同步回调（db.StartCluster 装配
	// sql.Executor.PushRouteViewToNode）；nil = 未装配（FULLALIGN_REQ{RoutesOnly} 回执 Err）。
	// 签名：dest=路由视图接收方 NodeID（重启节点自身）。
	// D4.4 修复：重启节点主动拉取权威 region 归属视图，不依赖对端 UP 事件检测。
	routeSyncFn func(dest string) error
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

// SetRegionEpochChecker 注入 P1 路由消息 epoch 校验回调（db.StartCluster 装配
// sql.Executor.CheckRegionPushEpoch）；传 nil 恢复零回归（跳过校验）。
func (s *Server) SetRegionEpochChecker(fn func(clusterEpoch, regionEpoch uint64, rid storage.ID) error) {
	s.regionEpochCheck = fn
}

// checkPushEpoch 校验 P1 路由消息 epoch 新鲜度：
//   - 校验器未装配（旧节点）或消息不带 clusterEpoch（=0：关闭开关/ASSIGN 旧路径）
//     → 放行（版本协商防呆 + 零回归）；
//   - 否则交由装配的回调判定（陈旧 → 返回错误，接收方拒绝更新路由并记日志）。
func (s *Server) checkPushEpoch(clusterEpoch, regionEpoch uint64, rid storage.ID) error {
	if s.regionEpochCheck == nil || clusterEpoch == 0 {
		return nil
	}
	return s.regionEpochCheck(clusterEpoch, regionEpoch, rid)
}

// SetManager 注入集群管理器（供 NODE_HELLO 双向注册对端节点）。
func (s *Server) SetManager(m *Manager) { s.mgr = m }

// SetFullAlignHandler 注入 v1.7.0 全量对齐回调（db.StartCluster 装配
// sql.Executor.PushAllRegionsToNode）；传 nil 恢复未装配（FULLALIGN_REQ 回执 Err）。
func (s *Server) SetFullAlignHandler(fn func(dest string) error) { s.fullAlignFn = fn }

// SetRouteSyncHandler 注入权威路由视图同步回调（db.StartCluster 装配
// sql.Executor.PushRouteViewToNode）；传 nil 恢复未装配（FULLALIGN_REQ{RoutesOnly} 回执 Err）。
// D4.4 修复：重启节点经此路径主动从集群权威源刷新 region 归属视图。
func (s *Server) SetRouteSyncHandler(fn func(dest string) error) { s.routeSyncFn = fn }

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
		case msgRegistrySync:
			s.handleRegistrySync(conn, payload)
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
		case msgDDLPush:
			s.handleDDLPush(conn, payload)
		case msgBackupPrepare:
			s.handleBackupPrepare(conn, payload)
		case msgBackupDo:
			s.handleBackupDo(conn, payload)
		case msgRestoreDo:
			s.handleRestoreDo(conn, payload)
		case msgEpochNotify:
			// P0 HA：epoch 广播（升主授权通知；无回执，单向尽力而为）
			s.handleEpoch(conn, payload)
		case msgFullAlignReq:
			// v1.7.0：全量对齐请求（回迁降级兜底；目标节点推送全部 region 后回执）
			s.handleFullAlign(conn, payload)
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

// handleRegistrySync 处理 REGISTRY_SYNC（G2 修复）：合并源节点注册表快照并落盘 m:nodes。
// 幂等：RegisterPeer 对已存在节点刷新地址与状态；本节点自身条目忽略。
func (s *Server) handleRegistrySync(conn net.Conn, payload []byte) {
	var req RegistrySyncReq
	if err := unmarshalJSON(payload, &req); err != nil {
		writeFrame(conn, msgRegistrySyncOK, mustJSON(RegistrySyncResp{Err: "bad registry sync request"}))
		return
	}
	if s.mgr != nil {
		s.mgr.RegisterPeers(req.Nodes)
		// 注册表落盘：重启后仍可路由到全部节点（失败不阻断回执，内存已生效）
		s.persistNodes()
	}
	writeFrame(conn, msgRegistrySyncOK, mustJSON(RegistrySyncResp{}))
}

// applyPush 执行数据装载：目标节点将 region 数据与路由原子写入本机。
// region 元信息以"本节点"视图落盘（Node=""、Local=true），
// 数据写入物理键空间原样保留（含 region 前缀），覆盖式装载幂等。
// SyncOnly=true 时降级为纯路由同步（applyRouteSync），不写数据。
func (s *Server) applyPush(req *RegionPushReq) (RegionPushResp, error) {
	if s.router == nil {
		return RegionPushResp{}, errors.New("cluster: router not injected")
	}
	if req.SyncOnly {
		return s.applyRouteSync(req)
	}
	// P1 epoch 前置校验（写数据/学路由前）：消息带 clusterEpoch 且本机已装配校验器时，
	// 校验本次接收 region 与路由视图条目的 epoch 新鲜度；陈旧消息整批拒绝
	// （不写数据、不学路由），防分区双端各自装载覆盖。
	if err := s.checkPushEpoch(req.ClusterEpoch, req.Region.RegionEpoch, req.Region.RegionID); err != nil {
		return RegionPushResp{}, err
	}
	for _, route := range req.Routes {
		if err := s.checkPushEpoch(req.ClusterEpoch, route.RegionEpoch, route.RegionID); err != nil {
			return RegionPushResp{}, err
		}
	}
	batch := &storage.WriteBatch{}
	for _, row := range req.Rows {
		batch.Puts = append(batch.Puts, storage.KVPair{Key: row.Key, Value: row.Value})
	}
	// 学习源节点路由视图（第一簇约定：源视图为权威）：
	// 其余 region 按其归属记录：Node 非空直接学习；Node=="" 表示源节点未持有
	// （源侧已用 regionHoldsLocal 仅把本地持有者补记 selfID），目标节点不得无条件
	// 补记 req.SrcID——仅本机真正持有数据的 region 保持本地视图，未持有者保持
	// 未指派，避免 reintroduce region 漂移。
	// 本次接收的 region 单独处理为本地（见下）。
	for _, route := range req.Routes {
		if route.RegionID == req.Region.RegionID {
			continue // 本次接收 region 稍后单独落本地
		}
		if route.Node == "" {
			holds, err := s.regionHoldsLocal(route)
			if err != nil {
				return RegionPushResp{}, err
			}
			if holds {
				// 本机物理持有该 region：保持本地视图（Node=="" 即本节点）
				route.Node = ""
				route.Local = true
			} else if cur, ok := s.router.FindRegion(route.RegionID); ok && cur.Node != "" {
				continue // 本机不持有且已有明确远端归属：保留原归属，不覆盖
			} else {
				// 本机不持有且无明确归属：保持未指派，不补记 SrcID
				route.Node = ""
				route.Local = false
			}
		}
		route = s.normalizeRouteOwner(route)
		if err := s.router.UpsertRegion(route); err != nil {
			return RegionPushResp{}, err
		}
	}
	// 学习本次接收 region 的归属（D4.4 修复：不再无条件落本地）：
	//   - ClaimOwner=true（ASSIGN/迁移/回迁装载）：目标成为 owner，落本地视图；
	//   - Region.Node 为空（旧节点/单机 region 未显式化）：沿用"未指派即本节点"语义；
	//   - Region.Node == 本节点：源视图已确认本节点为 owner，落本地视图；
	//   - 其余（Region.Node 为第三方或源节点）：本机仅持副本，保持远端归属与 local=false，
	//     避免全量对齐/重启刷新把他人 region 误判为本地，形成孤立读写视图（GAP A/C 根因）。
	rg := req.Region
	if req.ClaimOwner {
		rg.Node = s.SelfID // 归属声明：目标节点成为该 region owner
	}
	rg = s.normalizeRouteOwner(rg)
	rg.State = sharding.StateActive
	if err := s.router.UpsertRegion(rg); err != nil {
		return RegionPushResp{}, err
	}
	// 权威视图收敛（D4.4 次要缺陷修复）：全量装载携带源节点完整路由视图时，
	// 清理"权威视图中不存在、且区间已被现役 region 完全接管"的本地历史 region。
	pruned, retained := 0, 0
	if req.Authoritative {
		var cerr error
		pruned, retained, cerr = s.convergeRouteView(req.Routes, req.Region.RegionID)
		if cerr != nil {
			return RegionPushResp{}, cerr
		}
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
	return RegionPushResp{Rows: len(req.Rows), Pruned: pruned, RetainedStale: retained}, nil
}

// applyRouteSync 纯路由同步（SyncOnly=true）：路由变更全网广播的接收端。
// 1) 先删除 DeleteIDs 路由（SPLIT 广播移除旧 region，幂等）；
// 2) 按源权威视图学习 Routes：Node 非空直接学习；Node=="" 沿用持有校验语义
//    （本机持有→保持本地视图；不持有且已有远端归属→保留原归属；否则未指派）；
// 3) 权威全量同步（Authoritative=true，重启刷新路径）时收敛本地 region 集合：
//    清理权威视图不存在、区间已被现役 region 完全取代的历史残留（D4.4 修复）；
// 4) 落盘 m:regions。
// 不写数据、不把 req.Region 落本地（本机未持有该 region，不得标记为本地）。
func (s *Server) applyRouteSync(req *RegionPushReq) (RegionPushResp, error) {
	if s.router == nil {
		return RegionPushResp{}, errors.New("cluster: router not injected")
	}
	for _, rid := range req.DeleteIDs {
		if err := s.router.DeleteRegion(rid); err != nil {
			return RegionPushResp{}, err
		}
	}
	for _, route := range req.Routes {
		// P1 epoch 校验：陈旧条目跳过（不覆盖本地路由）并记日志；其余条目继续，
		// 路由广播为尽力而为，单条陈旧不阻断整体同步（防旧分区广播覆盖新视图）。
		if err := s.checkPushEpoch(req.ClusterEpoch, route.RegionEpoch, route.RegionID); err != nil {
			log.Printf("cluster: %s: stale route sync rejected rid=%d clusterEpoch=%d regionEpoch=%d: %v", s.SelfID, route.RegionID, req.ClusterEpoch, route.RegionEpoch, err)
			continue
		}
		if route.Node == "" {
			holds, err := s.regionHoldsLocal(route)
			if err != nil {
				return RegionPushResp{}, err
			}
			if holds {
				// 本机物理持有该 region：保持本地视图（Node=="" 即本节点）
				route.Node = ""
				route.Local = true
			} else if cur, ok := s.router.FindRegion(route.RegionID); ok && cur.Node != "" {
				continue // 本机不持有且已有明确远端归属：保留原归属，不覆盖
			} else {
				// 本机不持有且无明确归属：保持未指派，不补记 SrcID
				route.Node = ""
				route.Local = false
			}
		}
		route = s.normalizeRouteOwner(route)
		if err := s.router.UpsertRegion(route); err != nil {
			return RegionPushResp{}, err
		}
	}
	// 权威视图收敛（D4.4 次要缺陷修复）：重启节点拉取权威视图后，本地集合必须与
	// 权威视图收敛一致——否则历史 region（如与现役 region 区间完全重叠的 rid=3）
	// 会继续以 local=true 出现在 SHOW REGION ROUTES。增量广播（Authoritative=false）
	// 只含本次变更条目，严禁做收敛删除。
	pruned, retained := 0, 0
	if req.Authoritative {
		var cerr error
		pruned, retained, cerr = s.convergeRouteView(req.Routes, 0)
		if cerr != nil {
			return RegionPushResp{}, cerr
		}
	}
	seq, regions := s.router.Dump()
	metaRaw, err := sharding.MarshalMeta(seq, regions)
	if err != nil {
		return RegionPushResp{}, err
	}
	batch := &storage.WriteBatch{}
	batch.Puts = append(batch.Puts, storage.KVPair{Key: sharding.MetaRegionsKey(), Value: metaRaw})
	if err := s.st.Write(batch); err != nil {
		return RegionPushResp{}, err
	}
	return RegionPushResp{Pruned: pruned, RetainedStale: retained}, nil
}

// convergeRouteView 以权威写入视图收敛本地 region 集合（D4.4 次要缺陷修复）。
//
// 缺陷：路由视图同步此前只做 upsert 学习 + DeleteIDs 显式删除，从不清理"权威
// 视图中并不存在"的本地记录。节点重启后 m:regions 里 SPLIT/迁移前的历史 region
// （实测 rid=3 与现役 rid=7 区间完全重叠）被重新装载并标 local=true，
// SHOW REGION ROUTES 因此出现权威视图没有的 region，视图与权威不收敛。
//
// 清理判定唯一依据 sharding.Router.PruneSupersededRegions：仅当某本地 region
// 的 RegionID 不在权威视图、且其键区间被权威视图中同表 region 完全覆盖时才移除。
// keepID 非 0 时表示本次数据装载的 region，必须保留：若它不在权威视图内（权威
// 视图残缺/装载与视图不同步）则整批放弃收敛，宁可不清理也不误删现役 region。
//
// 物理数据一律不动：本方法只改路由视图。被移除的 region 若本机仍持有物理键空间，
// 计入 retained 并打告警日志，物理数据保留待人工对账清理，绝不自动删除数据。
func (s *Server) convergeRouteView(authoritative []sharding.RegionInfo, keepID storage.ID) (pruned, retained int, err error) {
	if len(authoritative) == 0 {
		return 0, 0, nil
	}
	if keepID != 0 {
		found := false
		for i := range authoritative {
			if authoritative[i].RegionID == keepID {
				found = true
				break
			}
		}
		if !found {
			log.Printf("cluster: %s: skip route view convergence: authoritative view misses incoming region rid=%d", s.SelfID, keepID)
			return 0, 0, nil
		}
	}
	removed := s.router.PruneSupersededRegions(authoritative)
	for _, rg := range removed {
		holds, herr := s.regionHoldsLocal(rg)
		if herr != nil {
			return len(removed), retained, herr
		}
		if holds {
			retained++
			log.Printf("cluster: %s: route view converged: superseded region rid=%d table=%d [%x,%x) removed from view (local flag cleared); physical keys retained on this node, manual GC required",
				s.SelfID, rg.RegionID, rg.TableID, rg.StartKey, rg.EndKey)
			continue
		}
		log.Printf("cluster: %s: route view converged: superseded region rid=%d table=%d [%x,%x) removed from view (no local data)",
			s.SelfID, rg.RegionID, rg.TableID, rg.StartKey, rg.EndKey)
	}
	return len(removed), retained, nil
}

// normalizeRouteOwner 依据权威归属归一 Local 标记（D4.4 修复）。
//
// 集群内 region 归属只有一个事实源：RegionInfo.Node。Local 必须由 Node 推导，
// 否则会出现"owner 是本节点但 local=false"（写路径按 owner 判定会静默写本地，
// 形成分叉写）或"owner 是远端但 local=true"（孤立读视图，重启节点看不到
// 他人 region）两类矛盾视图，正是 D4.4 GAP A/C 的未闭合根因。
//   - Node 为空或等于本节点 → Node 归一为空（"未指派即本节点"既有语义）+ Local=true；
//   - Node 为远端 → Local=false，保留远端归属。
func (s *Server) normalizeRouteOwner(rg sharding.RegionInfo) sharding.RegionInfo {
	if rg.Node == "" || rg.Node == s.SelfID {
		rg.Node = ""
		rg.Local = true
		return rg
	}
	rg.Local = false
	return rg
}

// regionHoldsLocal 判断本机物理键空间 [EncodeRegionKey(rid,StartKey), EncodeRegionKey(rid,EndKey))
// 是否存在至少一个键（主表或索引行）。用于接收远端路由视图时对 Node=="" 条目做持有校验：
// 与 pkg/sql 的 regionHoldsLocal 同语义（cluster 包不反向依赖 sql 包，故本地等价实现），
// 仅本机真正持有数据的 region 才有资格保持本地视图，未持有者保持未指派。
func (s *Server) regionHoldsLocal(rg sharding.RegionInfo) (bool, error) {
	start := sharding.EncodeRegionKey(rg.RegionID, rg.StartKey)
	end := sharding.EncodeRegionKey(rg.RegionID, rg.EndKey)
	if rg.EndKey == nil {
		// 开放上界：半开区间覆盖全部 r{rid} 物理键（与 execAssignRegion 一致）
		end = sharding.EncodeRegionKey(rg.RegionID+1, nil)
	}
	kvs, err := s.st.Scan(storage.KeyRange{Start: start, End: end}, 1)
	if err != nil {
		return false, err
	}
	return len(kvs) > 0, nil
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

package cluster

// M4 数据装载协议：ASSIGN REGION 时把 region 全量物理键数据推送到目标节点。
//
// 帧：REGION_PUSH(type 20) -> REGION_PUSH_OK(type 21)。
// 载荷：RegionPushReq{Region 元信息 + Rows(物理键字节行)}。
// 目标节点收到后：
//   - 在同一 WriteBatch 内写入全部行 + 更新路由表（UpsertRegion，Node=""/Local=true）
//     + 落盘 m:regions（保证数据与路由原子可见）；
//   - 回 REGION_PUSH_OK{Err, Rows}。
//
// 只用于"数据装载"（下簇分布式写事务之前的第一簇形态），非在线迁移协议。

import (
	"errors"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

const (
	msgRegionPush     byte = 20
	msgRegionPushOK   byte = 21
	msgRegistrySync   byte = 32 // 避开 2PC(22-27)/TSO(28-29)/DDL(30-31)
	msgRegistrySyncOK byte = 33
)

// RegionPushReq REGION_PUSH 载荷：region 元信息 + 全量物理键数据 + 源节点路由视图。
// Routes 携带源节点当前完整路由表快照（含显式化的本地归属），
// 目标节点据此学习"其余 region 归属哪个节点"，保证双节点路由视图完整、
// 任一端发起的跨节点查询都能正确展开（第一簇约定：源视图为权威）。
type RegionPushReq struct {
	SrcID     string                `json:"src_id,omitempty"` // 源节点 NodeID（补记 Node=="" 归属用）
	Region    sharding.RegionInfo   `json:"region"`
	Rows      []QueryRow            `json:"rows"`
	Routes    []sharding.RegionInfo `json:"routes,omitempty"`
	SyncOnly  bool                  `json:"sync_only,omitempty"` // true=纯路由同步：不写数据、不把 Region 落本地
	// ClaimOwner 本次接收 region 的归属声明（D4.4 修复）：
	// true = 目标节点成为该 region owner（ASSIGN/迁移/回迁装载，Region.Node 通常即目标）；
	// false（默认）= 副本对齐语义，接收端按 Region.Node 权威归属落视图，
	// 不得把 Region 无条件判为本地，避免全量对齐把第三方 region 误标 local=true。
	ClaimOwner bool `json:"claim_owner,omitempty"`
	DeleteIDs []storage.ID          `json:"delete_ids,omitempty"` // SPLIT 广播：接收端先删除这些 region 路由
	// ClusterEpoch 消息级全局纪元（P1 HA 防路由分裂）：由发起方在 quorum 授权后
	// 递增取号，接收端校验本消息不落后于已见值；0 = 未启用/旧节点消息（跳过校验）。
	// region 级 owner 变更纪元随 Region.RegionEpoch / Routes[i].RegionEpoch 携带。
	ClusterEpoch uint64 `json:"cluster_epoch,omitempty"`
	// Authoritative 标记 Routes 是否为源节点的"全量权威路由视图"（D4.4 收敛修复）：
	// true = Routes 即源节点完整视图，接收端在学习后清理"已被现役 region 取代"的
	// 本地历史 region 记录（见 sharding.Router.PruneSupersededRegions）；
	// false（默认）= 增量广播（只含本次变更条目），接收端仅 upsert，不得做收敛删除。
	Authoritative bool `json:"authoritative,omitempty"`
}

// RegionPushResp REGION_PUSH_OK 载荷。
type RegionPushResp struct {
	Err  string `json:"err,omitempty"`
	Rows int    `json:"rows"`
	// Pruned 本次权威视图同步中被判定为历史残留、已从本地路由视图移除的 region 数
	// （仅路由记录移除，物理键不动）。
	Pruned int `json:"pruned,omitempty"`
	// RetainedStale 上述被移除记录中，本机物理仍持有其键空间数据的条数：
	// 视图已收敛，但物理数据保留待人工清理/对账（不自动删除数据）。
	RetainedStale int `json:"retained_stale,omitempty"`
	// Overwritten / LocalExtra（NEW-1 可观测性）：本次装载覆盖的本机已有行数
	// （同键不同值）与推送未覆盖、物理保留的本机残留行数（不删除）。
	Overwritten int `json:"overwritten,omitempty"`
	LocalExtra  int `json:"local_extra,omitempty"`
}

// PushRegion 将 region 数据装载到目标节点（ASSIGN REGION 远端指派路径）。
// 目标节点会学习路由（region 归属本地）并原子落盘数据+路由；routes 为源节点
// 路由视图快照，目标节点据此补齐其余 region 的归属视图。
// P1 前兼容路径：消息不带 clusterEpoch（0），接收端跳过 epoch 校验（零回归）。
func (m *Manager) PushRegion(nodeID string, rg sharding.RegionInfo, rows []QueryRow, routes []sharding.RegionInfo, srcID string) error {
	return m.pushRegionEpoch(nodeID, rg, rows, routes, srcID, 0)
}

// PushRegionReplica 以副本语义把 region 数据装载到目标节点（D4.4 修复）：
// 声明 Region.Node 的权威归属，接收端保持该归属（本机仅持副本，local=false），
// 仅当 Region.Node 为空或等于目标节点时目标才成为 owner。用于重启节点/新节点
// 的全量对齐与路由同步，避免把第三方 region 误判为本地形成孤立读视图。
func (m *Manager) PushRegionReplica(nodeID string, rg sharding.RegionInfo, rows []QueryRow, routes []sharding.RegionInfo, srcID string) error {
	return m.pushRegionOwner(nodeID, rg, rows, routes, srcID, 0, false)
}

// PushRegionEpoch P1 数据装载：在 PushRegion 基础上携带 clusterEpoch（发起方
// quorum 授权后取号），接收端校验 epoch 新鲜度后才更新路由，防分区双端重指派。
func (m *Manager) PushRegionEpoch(nodeID string, rg sharding.RegionInfo, rows []QueryRow, routes []sharding.RegionInfo, srcID string, clusterEpoch uint64) error {
	return m.pushRegionEpoch(nodeID, rg, rows, routes, srcID, clusterEpoch)
}

// pushRegionEpoch 短连接发送 REGION_PUSH 并读取 REGION_PUSH_OK（claim 语义：目标成为 owner）。
func (m *Manager) pushRegionEpoch(nodeID string, rg sharding.RegionInfo, rows []QueryRow, routes []sharding.RegionInfo, srcID string, clusterEpoch uint64) error {
	return m.pushRegionOwner(nodeID, rg, rows, routes, srcID, clusterEpoch, true)
}

// pushRegionOwner 带归属声明地发送 REGION_PUSH：claim=true 目标成为 owner，
// claim=false 接收端按 Region.Node 权威归属落视图（副本对齐）。
func (m *Manager) pushRegionOwner(nodeID string, rg sharding.RegionInfo, rows []QueryRow, routes []sharding.RegionInfo, srcID string, clusterEpoch uint64, claim bool) error {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return err
	}
	return m.clientFor(n.ID).pushRegion(rg, rows, routes, srcID, clusterEpoch, claim)
}

// pushRegion 短连接发送 REGION_PUSH 并读取 REGION_PUSH_OK。
func (c *Client) pushRegion(rg sharding.RegionInfo, rows []QueryRow, routes []sharding.RegionInfo, srcID string, clusterEpoch uint64, claim bool) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	// Routes 为非空时即源节点完整路由表快照（调用方均传 AllRegions 全量视图），
	// 因此标记为权威视图：接收端学习后收敛清理被取代的历史 region 记录（D4.4）。
	payload, err := marshalJSON(RegionPushReq{SrcID: srcID, Region: rg, Rows: rows, Routes: routes, ClusterEpoch: clusterEpoch, ClaimOwner: claim, Authoritative: len(routes) > 0})
	if err != nil {
		return err
	}
	if err := writeFrame(conn, msgRegionPush, payload); err != nil {
		return err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return err
	}
	if typ != msgRegionPushOK {
		return errors.New("cluster: unexpected push reply")
	}
	var resp RegionPushResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return err
	}
	if resp.Err != "" {
		return errors.New("cluster: push failed: " + resp.Err)
	}
	return nil
}

// BroadcastRegions 将路由视图增量同步到所有已注册远端节点（不含本节点）。
// 路由变更（ASSIGN/SPLIT/failover）后调用，使各节点视图收敛一致；
// deleteIDs 用于 SPLIT 广播：接收端先删除旧 region 路由再学习新划分。
// 广播失败返回首个错误（尽力而为，单节点失败不阻断其余节点同步）。
// P1 前兼容路径：消息不带 clusterEpoch（0），接收端跳过 epoch 校验（零回归）。
func (m *Manager) BroadcastRegions(routes []sharding.RegionInfo, deleteIDs []storage.ID, srcID string) error {
	return m.broadcastRegionsEpoch(routes, deleteIDs, srcID, 0)
}

// BroadcastRegionsEpoch P1 路由广播：在 BroadcastRegions 基础上携带 clusterEpoch，
// 接收端校验 epoch 新鲜度后才更新路由，防分区双端各自广播覆盖对方视图。
func (m *Manager) BroadcastRegionsEpoch(routes []sharding.RegionInfo, deleteIDs []storage.ID, srcID string, clusterEpoch uint64) error {
	return m.broadcastRegionsEpoch(routes, deleteIDs, srcID, clusterEpoch)
}

// broadcastRegionsEpoch 路由广播实现：逐节点发送带 epoch 的纯路由同步。
func (m *Manager) broadcastRegionsEpoch(routes []sharding.RegionInfo, deleteIDs []storage.ID, srcID string, clusterEpoch uint64) error {
	var firstErr error
	for _, n := range m.ListNodes() {
		if n.ID == m.SelfID {
			continue
		}
		if err := m.clientFor(n.ID).syncRegionRoutes(routes, deleteIDs, srcID, clusterEpoch, false); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// SyncRouteViewTo 把源节点权威路由视图（纯路由，不带数据行）同步给 target 节点
//（D4.4 修复：重启节点主动刷新 region 归属视图的服务端下发实现）。
// 与 BroadcastRegionsEpoch 的差别是单播到指定节点，不向全网扩散。
func (m *Manager) SyncRouteViewTo(target string, routes []sharding.RegionInfo, srcID string) error {
	return m.SyncRouteViewToEpoch(target, routes, srcID, 0)
}

// SyncRouteViewToEpoch 带 clusterEpoch 的权威路由视图同步（P9）：
// 与 SyncRouteViewTo 相同，但把源节点已知的集群纪元随消息下发，接收端据此
// 推进本机集群轨归属纪元（cluster term）——term=0 表示源节点无纪元信息
// （旧节点/单机），接收端按"仅学习归属、不推进纪元"处理。
func (m *Manager) SyncRouteViewToEpoch(target string, routes []sharding.RegionInfo, srcID string, clusterEpoch uint64) error {
	n, err := m.ensureRemote(target)
	if err != nil {
		return err
	}
	return m.clientFor(n.ID).syncRegionRoutes(routes, nil, srcID, clusterEpoch, true)
}

// syncRegionRoutes 短连接发送 REGION_PUSH(SyncOnly=true) 并读取 REGION_PUSH_OK。
// 只同步路由视图（Routes 为源节点权威视图；DeleteIDs 先删后学），
// 不携带数据、不把 req.Region 落本地，接收端仅更新路由表并落盘 m:regions。
// authoritative=true 表示 routes 是源节点全量权威视图（重启刷新路径），接收端
// 学完后需清理被现役 region 取代的本地历史 region；增量广播必须传 false。
func (c *Client) syncRegionRoutes(routes []sharding.RegionInfo, deleteIDs []storage.ID, srcID string, clusterEpoch uint64, authoritative bool) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	payload, err := marshalJSON(RegionPushReq{SrcID: srcID, SyncOnly: true, DeleteIDs: deleteIDs, Routes: routes, ClusterEpoch: clusterEpoch, Authoritative: authoritative})
	if err != nil {
		return err
	}
	if err := writeFrame(conn, msgRegionPush, payload); err != nil {
		return err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return err
	}
	if typ != msgRegionPushOK {
		return errors.New("cluster: unexpected route sync reply")
	}
	var resp RegionPushResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return err
	}
	if resp.Err != "" {
		return errors.New("cluster: route sync failed: " + resp.Err)
	}
	return nil
}

// RegistrySyncReq REGISTRY_SYNC 载荷：源节点注册表全量快照（G2 修复）。
// 接收端合并 Nodes 到本机注册表并落盘 m:nodes，使拓扑各节点注册表收敛一致。
type RegistrySyncReq struct {
	SrcID string     `json:"src_id,omitempty"`
	Nodes []NodeInfo `json:"nodes,omitempty"`
}

// RegistrySyncResp REGISTRY_SYNC_OK 载荷。
type RegistrySyncResp struct {
	Err string `json:"err,omitempty"`
}

// syncRegistry 短连接发送 REGISTRY_SYNC 并读取 REGISTRY_SYNC_OK。
// 注册表广播为尽力而为：单节点失败不阻断其余节点同步（由 BroadcastRegistry 汇总）。
func (c *Client) syncRegistry(nodes []NodeInfo, srcID string) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	payload, err := marshalJSON(RegistrySyncReq{SrcID: srcID, Nodes: nodes})
	if err != nil {
		return err
	}
	if err := writeFrame(conn, msgRegistrySync, payload); err != nil {
		return err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return err
	}
	if typ != msgRegistrySyncOK {
		return errors.New("cluster: unexpected registry sync reply")
	}
	var resp RegistrySyncResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return err
	}
	if resp.Err != "" {
		return errors.New("cluster: registry sync failed: " + resp.Err)
	}
	return nil
}

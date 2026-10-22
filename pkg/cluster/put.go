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
	msgRegionPush   byte = 20
	msgRegionPushOK byte = 21
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
	DeleteIDs []storage.ID          `json:"delete_ids,omitempty"` // SPLIT 广播：接收端先删除这些 region 路由
}

// RegionPushResp REGION_PUSH_OK 载荷。
type RegionPushResp struct {
	Err  string `json:"err,omitempty"`
	Rows int    `json:"rows"`
}

// PushRegion 将 region 数据装载到目标节点（ASSIGN REGION 远端指派路径）。
// 目标节点会学习路由（region 归属本地）并原子落盘数据+路由；routes 为源节点
// 路由视图快照，目标节点据此补齐其余 region 的归属视图。
func (m *Manager) PushRegion(nodeID string, rg sharding.RegionInfo, rows []QueryRow, routes []sharding.RegionInfo, srcID string) error {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return err
	}
	return m.clientFor(n.ID).pushRegion(rg, rows, routes, srcID)
}

// pushRegion 短连接发送 REGION_PUSH 并读取 REGION_PUSH_OK。
func (c *Client) pushRegion(rg sharding.RegionInfo, rows []QueryRow, routes []sharding.RegionInfo, srcID string) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	payload, err := marshalJSON(RegionPushReq{SrcID: srcID, Region: rg, Rows: rows, Routes: routes})
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
func (m *Manager) BroadcastRegions(routes []sharding.RegionInfo, deleteIDs []storage.ID, srcID string) error {
	var firstErr error
	for _, n := range m.ListNodes() {
		if n.ID == m.SelfID {
			continue
		}
		if err := m.clientFor(n.ID).syncRegionRoutes(routes, deleteIDs, srcID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// syncRegionRoutes 短连接发送 REGION_PUSH(SyncOnly=true) 并读取 REGION_PUSH_OK。
// 只同步路由视图（Routes 为源节点权威视图；DeleteIDs 先删后学），
// 不携带数据、不把 req.Region 落本地，接收端仅更新路由表并落盘 m:regions。
func (c *Client) syncRegionRoutes(routes []sharding.RegionInfo, deleteIDs []storage.ID, srcID string) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	payload, err := marshalJSON(RegionPushReq{SrcID: srcID, SyncOnly: true, DeleteIDs: deleteIDs, Routes: routes})
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

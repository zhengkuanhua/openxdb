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
	SrcID  string                `json:"src_id,omitempty"` // 源节点 NodeID（补记 Node=="" 归属用）
	Region sharding.RegionInfo   `json:"region"`
	Rows   []QueryRow            `json:"rows"`
	Routes []sharding.RegionInfo `json:"routes,omitempty"`
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

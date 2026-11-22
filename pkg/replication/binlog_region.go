package replication

// v1.20.0 C4：持续复制 region 化 —— binlog 侧按 region 维度读取增量。
//
// 本文件只做"读"：在既有的顺序 binlog（M2 复制轨，DEV_PLAN D-1 决策复用）
// 之上，按条目携带的 RegionID 过滤出属于目标 region 的增量段，供上层
// （pkg/db 的 region 增量复制循环）推送给影子副本。
//
// 兼容语义（DEV_PLAN §4 红线 3）：
//   - 条目 RegionID == 目标 region：命中；
//   - 条目 RegionID == 0：v1 旧文件中的条目，或跨越多个 region 的整批写入，
//     一律按"整批应用"候选返回，由调用方按物理键前缀自行裁剪/整批应用；
//   - 其余 region 的条目直接跳过。

import (
	"errors"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ErrRegionReadUnsupported 底层 binlog 实现不支持按 region 读取增量。
var ErrRegionReadUnsupported = errors.New("replication: region binlog read unsupported")

// errStopRegionRead 内部哨兵：达到 limit 后中止顺序扫描（不是错误）。
var errStopRegionRead = errors.New("replication: stop region read")

// RegionLogReader 可选能力接口：按 region 维度读取 binlog 增量。
// BinlogStore 的既有实现（含测试替身）无须实现本接口——调用方以类型断言
// 探测，未实现时返回 ErrRegionReadUnsupported，保持向后兼容。
type RegionLogReader interface {
	ReadRegion(from storage.LSN, regionID storage.ID, limit int) ([]*BinlogEntry, error)
}

// ReadRegion 读取 LSN>=from 且与 regionID 相关的条目（顺序、最多 limit 条）。
// regionID==0 表示"不按 region 过滤"，等价于整批读取（兼容路径）。
func (b *binlogStore) ReadRegion(from storage.LSN, regionID storage.ID, limit int) ([]*BinlogEntry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	out := make([]*BinlogEntry, 0, 16)
	err := b.replay(func(e *BinlogEntry) error {
		if e.LSN < from {
			return nil
		}
		if regionID != 0 && e.RegionID != 0 && e.RegionID != regionID {
			return nil
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			return errStopRegionRead
		}
		return nil
	})
	if err != nil && err != errStopRegionRead {
		return nil, err
	}
	return out, nil
}

// ReadRegionFrom 按 region 维度读取本主端 binlog 增量（C4 持续复制入口）。
// from 为副本已应用位点 + 1；limit<=0 表示不限制条数。
func (m *MasterReplicator) ReadRegionFrom(from storage.LSN, regionID storage.ID, limit int) ([]*BinlogEntry, error) {
	if m == nil || m.store == nil {
		return nil, ErrRegionReadUnsupported
	}
	r, ok := m.store.(RegionLogReader)
	if !ok {
		return nil, ErrRegionReadUnsupported
	}
	return r.ReadRegion(from, regionID, limit)
}

// BinlogVersion 返回 binlog 文件的实际记录格式版本（1=无 region，2=带 region）。
// 供运维巡检与兼容性断言使用。
func (b *binlogStore) BinlogVersion() uint16 { return b.recordVer() }

// BinlogVersion 返回主端 binlog 文件的记录格式版本（见 binlogStore.BinlogVersion）。
func (m *MasterReplicator) BinlogVersion() uint16 {
	if m == nil || m.store == nil {
		return 0
	}
	if v, ok := m.store.(interface{ BinlogVersion() uint16 }); ok {
		return v.BinlogVersion()
	}
	return binVersion
}

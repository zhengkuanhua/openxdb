package replication

// v1.20.0 C4：持续复制 region 化 —— binlog 侧 region 维度（写侧）。
//
// 本文件只做"判别"：从一个待落盘的 WriteBatch 中推导出它所属的 region。
// 目的：让 binlog 条目携带 region 维度，供影子副本按 region 拉取增量。
//
// 兼容语义（DEV_PLAN §4 红线 3）：
//   - 单 region 批次（绝大多数 DML 提交）：RegionID = 该 region；
//   - 跨 region / 混合元数据的批次（2PC 参与者提交、DDL、SPLIT 等）：
//     RegionID = 0，表示"整批"，由下游按物理键前缀自行裁剪，旧条目同样
//     RegionID = 0，二者语义一致，因此新 follower 读旧文件无需特殊分支。
//
// 为什么不做"按 region 拆成多条条目"：拆分会改变既有链路的条目数量，与
// §4 红线 5（既有测试断言不得修改）冲突；且复制帧仍保持整批回放语义，
// region 维度只在落盘条目与 region 增量读取上生效。

import (
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// RegionOfBatch 推导批次所属 region。
//
// 返回 0 表示该批次不能归属到单一 region（跨 region 或含非 region 物理键），
// 调用方按"整批"处理；返回非 0 表示批次内全部物理键都属于同一 region。
// 空批次返回 0（无归属，保持旧语义）。
func RegionOfBatch(batch *storage.WriteBatch) storage.ID {
	if batch == nil {
		return 0
	}
	var rid storage.ID
	seen := false
	for _, p := range batch.Puts {
		r, ok := regionOfKey(p.Key)
		if !ok {
			return 0
		}
		if !seen {
			rid, seen = r, true
			continue
		}
		if r != rid {
			return 0
		}
	}
	for _, k := range batch.Deletes {
		r, ok := regionOfKey(k)
		if !ok {
			return 0
		}
		if !seen {
			rid, seen = r, true
			continue
		}
		if r != rid {
			return 0
		}
	}
	if !seen {
		return 0
	}
	return rid
}

// regionOfKey 解析物理键的 region 前缀；非 region 键返回 ok=false。
func regionOfKey(k []byte) (storage.ID, bool) {
	if len(k) < sharding.RegionKeyPrefixLen {
		return 0, false
	}
	if k[0] != 'r' {
		return 0, false
	}
	rid, _ := sharding.DecodeRegionKey(k)
	return rid, true
}

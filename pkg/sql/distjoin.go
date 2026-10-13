package sql

// M12 分布式 JOIN：跨分片/跨节点 JOIN 的路由与合并层。
//
// 设计要点：
//   - 路由条件（planJoinRoute）：右表为物理表、ON 全部为等值（=），且至少一条
//     等值条件引用右表主键列（两种书写方向均可：a.x = b.pk 或 b.pk = a.x），
//     驱动侧对应列可在 left Rel 中解析到。
//   - 执行路径（routedJoin）：驱动表仍由 scanMergedCluster 跨 region/跨节点合并
//     得到全量左行；对每个左行按右表主键编码路由点查（getKVCluster 本地直读 /
//     远端转发），命中后 decodeRow 并按完整 ON 条件（matchJoinOn）校验合并，
//     未命中或条件不满足时 INNER 丢弃 / LEFT 补空。每左行至多命中 1 行
//     （右主键唯一），语义与 joinRels 全量拼接完全一致。
//   - 退化与兜底：单分区（隐式单 region）时路由点查即本地点查，行为与单机 JOIN
//     一致；驱动值类型与右主键编码不兼容（如 INT 主键对 TEXT 驱动值）时返回
//     errJoinUnroutable，由调用方回退 joinRels 全量内存 JOIN，保证与单机语义
//     零偏差。子查询右表、非等值 ON、无主键等值条件的 JOIN 一律走 joinRels。

import (
	"errors"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// errJoinUnroutable 驱动行键值类型与右表主键编码不兼容，JOIN 无法按主键路由。
// 调用方捕获后应回退 joinRels 全量拼接，保证结果与单机一致。
var errJoinUnroutable = errors.New("join: routed key type incompatible with right PK encoding")

// joinRoutePlan 一条可路由 JOIN 的静态计划。
type joinRoutePlan struct {
	// pkOnIdx ON 条件中引用右表主键的等值条件索引（其余 AND 条件由 matchJoinOn 校验）。
	pkOnIdx int
	// leftIdx 驱动左行中与右主键等值的列索引。
	leftIdx int
}

// planJoinRoute 判定 JOIN 是否可走分布式路由路径，并产出路由计划。
// 判定失败（返回 ok=false）时调用方应走 joinRels 全量 JOIN。
func planJoinRoute(left *Rel, jc *JoinClause, rmeta *TableMeta) (joinRoutePlan, bool) {
	if rmeta == nil || rmeta.PK == "" || len(jc.On) == 0 {
		return joinRoutePlan{}, false
	}
	// 右表列名集（别名限定，与 relFromScan 输出一致）。
	alias := orAlias(jc.Table, jc.Alias)
	right := &Rel{Cols: make([]string, 0, len(rmeta.Columns))}
	for _, c := range rmeta.Columns {
		right.Cols = append(right.Cols, alias+"."+c.Name)
	}
	// 仅等值条件可路由；任一非等值 ON 条件回退全量 JOIN。
	for _, c := range jc.On {
		if c.Op != "=" {
			return joinRoutePlan{}, false
		}
	}
	// 寻找引用右表主键的等值条件（两种书写方向）。
	for i, c := range jc.On {
		// 方向 1：右主键在右操作数侧，形如 a.x = b.pk。
		if c.RightRef != "" && baseColName(c.RightRef) == rmeta.PK && relColIndex(right, c.RightRef) >= 0 {
			if li := relColIndex(left, qualifiedName(c.Ref, c.Col)); li >= 0 {
				return joinRoutePlan{pkOnIdx: i, leftIdx: li}, true
			}
		}
		// 方向 2：右主键在左操作数侧，形如 b.pk = a.x。
		rightName := qualifiedName(c.Ref, c.Col)
		if baseColName(rightName) == rmeta.PK && relColIndex(right, rightName) >= 0 {
			if c.RightRef != "" {
				if li := relColIndex(left, c.RightRef); li >= 0 {
					return joinRoutePlan{pkOnIdx: i, leftIdx: li}, true
				}
			}
		}
	}
	return joinRoutePlan{}, false
}

// joinRel 统一 JOIN 入口：满足分布式路由条件时走 routedJoin（路由点查 + 合并），
// 否则回退 joinRels（单机全量内存 JOIN，行为与 M1 一致）。
func (e *Executor) joinRel(sess *Session, tx txn.Txn, tabs []*TableMeta, left, right *Rel, jc *JoinClause) (*Rel, error) {
	if jc.Subquery == nil && len(jc.On) > 0 {
		meta := findTable(tabs, jc.Table)
		if plan, ok := planJoinRoute(left, jc, meta); ok {
			rel, err := e.routedJoin(sess, tx, left, right, meta, jc, plan)
			if err == errJoinUnroutable {
				// 键值类型与右主键编码不兼容 → 回退全量 JOIN 保证语义一致。
				return joinRels(left, right, jc)
			}
			return rel, err
		}
	}
	return joinRels(left, right, jc)
}

// routedJoin 分布式路由 JOIN：驱动左行按右表主键路由点查并合并。
// 每左行至多命中 1 个右行（右主键唯一）；未命中 / 条件不满足时
// INNER 丢弃该行、LEFT 以空值补右列，语义与 joinRels 一致。
func (e *Executor) routedJoin(sess *Session, tx txn.Txn, left, right *Rel, rmeta *TableMeta, jc *JoinClause, plan joinRoutePlan) (*Rel, error) {
	out := &Rel{Cols: append(append([]string{}, left.Cols...), right.Cols...)}
	for _, lr := range left.Rows {
		lv := lr[plan.leftIdx]
		if !joinKeyCompatible(rmeta, lv) {
			return nil, errJoinUnroutable
		}
		pk, err := pkBytes(rmeta, lv)
		if err != nil {
			return nil, err
		}
		// 按分区键路由：驱动值编码右表主键 → locate 所属 region → 编码带 region
		// 前缀的物理行键（与 INSERT 写入键一致）→ 本地直读 / 远端转发点查。
		rid, err := e.locateRow(rmeta, pk)
		if err != nil {
			return nil, err
		}
		key := e.rowKeyAt(rmeta, rid, pk)
		raw, err := e.getKVCluster(sess, tx, key)
		if err != nil {
			if err == storage.ErrNotFound {
				if jc.Type == "LEFT" {
					out.Rows = append(out.Rows, leftJoinNullRow(left, right, lr))
				}
				continue
			}
			return nil, err
		}
		rr, err := decodeRow(rmeta, raw)
		if err != nil {
			return nil, err
		}
		ok, err := matchJoinOn(left, lr, right, rr, jc.On)
		if err != nil {
			return nil, err
		}
		if !ok {
			if jc.Type == "LEFT" {
				out.Rows = append(out.Rows, leftJoinNullRow(left, right, lr))
			}
			continue
		}
		merged := make([]Value, 0, len(lr)+len(rr))
		merged = append(merged, lr...)
		merged = append(merged, rr...)
		out.Rows = append(out.Rows, merged)
	}
	return out, nil
}

// leftJoinNullRow 构造 LEFT JOIN 未匹配行的补空输出（右列全部 TEXT 空串）。
func leftJoinNullRow(left, right *Rel, lr []Value) []Value {
	merged := make([]Value, 0, len(lr)+len(right.Cols))
	merged = append(merged, lr...)
	for range right.Cols {
		merged = append(merged, Value{Kind: "TEXT", S: ""})
	}
	return merged
}

// joinKeyCompatible 驱动键值 Kind 与右表主键列编码是否兼容（编码结果与写入一致）。
// 不兼容时路由点查无法命中而全量 JOIN 可能匹配，必须回退以保证语义一致：
//   - INT 主键仅接受 INT（DECIMAL/TEXT 的 pkBytes 编码不同）；
//   - DECIMAL 主键仅接受 DECIMAL；
//   - BLOB 主键接受 TEXT/BLOB（一律 hex 解码）；
//   - TEXT/DATE 主键接受除 BLOB 外任意 Kind（pkBytes 按 S 原字节编码，与写入一致）。
func joinKeyCompatible(meta *TableMeta, v Value) bool {
	pkType := ""
	for _, c := range meta.Columns {
		if c.Name == meta.PK {
			pkType = c.Type
			break
		}
	}
	switch pkType {
	case "INT":
		return v.Kind == "INT"
	case "DECIMAL":
		return v.Kind == "DECIMAL"
	case "BLOB":
		return v.Kind == "TEXT" || v.Kind == "BLOB"
	default: // TEXT / DATE
		return v.Kind != "BLOB"
	}
}

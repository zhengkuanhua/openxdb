package sql

// v2.0.8 Q1 第一层：多行 INSERT "duplicate primary key" 误报诊断探针。
//
// 现象（v2.0.7 真机复测）：节点 B 入口执行多行 INSERT 报 duplicate primary key，
// 同批其它行却查得到；换新主键组合又全部成功；从其它入口执行同结构语句亦成功。
// 说明误报依赖"入口节点 + 特定已占键组合"，本地三进程矩阵无法复现。
//
// 该误报的唯一产生点是冲突判定读到"存在"：读路径在 region 归属本机时直读本地
// 键空间（regionLocal），在归属节点 down 且 allow_partial_read=on 时回退本地
// 副本。两种情形下若本机残留了迁移后的旧副本，就会读到"幽灵行"并据此报冲突，
// 而真实归属节点上并无该键。
//
// 本探针在每次抛出 duplicate primary key 前输出一行结构化现场（region、键 hex、
// 视图归属、自身是否归属、region 是否显式、读路径来源），使真机复现时可直接
// 判定误报机制，而不必反复插桩。正常路径零输出、零开销。

import (
	"log"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// dupDiagSource 冲突判定的读路径来源。
type dupDiagSource string

const (
	// dupDiagInsertRow insertRowOps 主键预检（逐行插入路径）。
	dupDiagInsertRow dupDiagSource = "insertRowOps"
	// dupDiagUpsertHook insertRowUpsertOpsHook 统一冲突判定（多行/UPSERT 路径）。
	dupDiagUpsertHook dupDiagSource = "insertRowUpsertOpsHook"
)

// logDupConflictDiag 冲突报错前输出判定现场（每语句冲突至多一次，INFO 级）。
// router 未装配（离线装配/单机测试）时输出 view_owner=""，不影响判定本身。
func (e *Executor) logDupConflictDiag(src dupDiagSource, meta *TableMeta, rid storage.ID, key []byte) {
	if e == nil || meta == nil {
		return
	}
	viewOwner := ""
	explicit := false
	if e.router != nil {
		viewOwner = e.router.RegionNode(rid)
		if rg, ok := e.router.FindRegion(rid); ok {
			explicit = rg.Explicit
		}
	}
	log.Printf("sql: dup-conflict diag src=%s table=%s rid=%d key=%x self=%q view_owner=%q owner_unassigned=%t self_is_owner=%t region_explicit=%t region_local=%t",
		src, meta.Name, rid, key, e.selfID, viewOwner, viewOwner == "", viewOwner == e.selfID, explicit, e.regionLocal(rid))
}

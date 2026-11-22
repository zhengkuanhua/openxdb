package sql

// v1.20.0 C4：持续复制 region 化 —— HA 配置项挂接。
//
// 新开关 ha_replica_region_sync（默认 off）：
//   on  —— owner 侧把提交写入落成带 region 维度的增量日志，影子副本按 region
//          持续跟随 owner 写入并推进 m:repl:region:<rid>:lsn；
//   off —— 完全不做上述任何事（不建日志文件、不起协程、不写位点键），
//          旧路径（静态一次性 bootstrap）行为不变。
//
// 与既有开关的关系：本开关不联动 ha_quorum_enabled / ha_write_route_enforce /
// ha_2pc_replica_quorum，需要显式开启（DEV_PLAN §4 红线 2）。
// 副本集合仍由 ha_replica_enabled 的 bootstrap 建立，本开关只接管"之后的新写入"。

import (
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
)

// RegionSyncSwitch region 增量复制开关（由 *db.DB 实现）。
type RegionSyncSwitch interface {
	SetRegionSyncEnabled(on bool) error
	RegionSyncEnabled() bool
}

// haRegionSyncSpec 构造 ha_replica_region_sync 配置项。
func haRegionSyncSpec() *configSpec {
	return &configSpec{
		key:  "ha_replica_region_sync",
		desc: "持续复制 region 化：影子副本跟随 owner 增量写入（默认 off）",
		def:  "off",
		apply: func(e *Executor, v string) error {
			return applyHaRegionSync(e, v)
		},
		read: func(e *Executor) string {
			return boolValStr(haRegionSyncOn(e))
		},
	}
}

// applyHaRegionSync 解析并应用 ha_replica_region_sync。
func applyHaRegionSync(e *Executor, v string) error {
	on, err := haParseBool(v)
	if err != nil {
		return &SQLError{Msg: "invalid value for ha_replica_region_sync: " + v + " (expected on/off)"}
	}
	if e.haCfg == nil {
		return &SQLError{Msg: "ha_replica_region_sync requires cluster HA configuration (start cluster first)"}
	}
	sw, ok := e.haCfg.(RegionSyncSwitch)
	if !ok {
		return &SQLError{Msg: "ha_replica_region_sync unsupported by this build (no region sync switch)"}
	}
	if err := sw.SetRegionSyncEnabled(on); err != nil {
		return &SQLError{Msg: "ha_replica_region_sync apply failed: " + err.Error()}
	}
	return nil
}

// haRegionSyncOn 读取当前开关状态（未装配时视为 off）。
func haRegionSyncOn(e *Executor) bool {
	if e.haCfg == nil {
		return false
	}
	sw, ok := e.haCfg.(RegionSyncSwitch)
	if !ok {
		return false
	}
	return sw.RegionSyncEnabled()
}

// Regions 返回本机路由视图中的全部 region（C4 跟随循环遍历目标）。
// 空路由表时 Router 以隐式单 region 兜底，未分片行为零回归。
func (e *Engine) Regions() []sharding.RegionInfo {
	if e == nil || e.ex == nil || e.ex.Router() == nil {
		return nil
	}
	return e.ex.Router().AllRegions()
}

// init 注册配置项。文件名字典序在 ha_config.go 之后，故 initHAConfigSpecs
// 已执行完毕，此处追加即位于 SHOW CONFIG 列表末尾（顺序稳定可断言）。
func init() {
	ensureHAConfigSpecs()
	configSpecs = append(configSpecs, haRegionSyncSpec())
}

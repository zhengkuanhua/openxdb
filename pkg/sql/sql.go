package sql

import (
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/tso"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// Engine SQL 引擎入口：Parse + Execute 一条语句。
// 协议层/CLI 通过 Engine.Execute 处理 `SQL ...` 命令或 SQL 关键字语句。
type Engine struct {
	ex *Executor
}

// New 创建 SQL 引擎。
func New(tm txn.TxnManager) *Engine {
	return &Engine{ex: NewExecutor(tm)}
}

// LoadSharding 加载分片路由表（db.Open 读取落盘 m:regions 后调用）。
// 空路由表时 Router 以隐式单 region 兜底，未分片行为零回归。
func (e *Engine) LoadSharding(seq uint64, regions []sharding.RegionInfo) {
	r := sharding.NewRouter()
	r.SetRegions(seq, regions)
	e.ex.SetRouter(r)
}

// SplitTable 按主键边界将表拆分为多个显式 region（M3 分片管理入口）。
func (e *Engine) SplitTable(name string, boundaries [][]byte) error {
	return e.ex.SplitTable(name, boundaries)
}

// ListRegions 查看表当前分片（显式划分或隐式单 region）。
func (e *Engine) ListRegions(name string) ([]sharding.RegionInfo, error) {
	return e.ex.ListRegions(name)
}

// LocateRegion 按主键字节定位所属 region（路由未命中返回 sharding.ErrRegionNotFound）。
func (e *Engine) LocateRegion(name string, pk []byte) (storage.ID, error) {
	return e.ex.LocateRegion(name, pk)
}

// ExecutorRouter 返回执行器共享的路由表指针（供 db.StartCluster 注入节点服务端）。
func (e *Engine) ExecutorRouter() *sharding.Router { return e.ex.Router() }

// SetCluster 注入 M4 集群管理器与本节点 ID（供 db.Open 装配，节点注册/转发/路由展开）。
func (e *Engine) SetCluster(mgr *cluster.Manager, selfID string) { e.ex.SetCluster(mgr, selfID) }

// SetTSO 注入 M8 全局时间戳源（供 db.Open 装配；nil = 未启用快照隔离，
// 单机旧路径零回归）。
func (e *Engine) SetTSO(src tso.Source) { e.ex.SetTSO(src) }

// FailoverDownNode 触发节点离线后的 region 自动重指派（M6 心跳回调装配）。
func (e *Engine) FailoverDownNode(downID string) (int, error) { return e.ex.FailoverDownNode(downID) }

// SetAutoSplit 配置 M7 自动分裂：enabled 开启后，写路径累计水位并检查本地 region
// 行数，超过 rowThreshold 自动沿数据中点一分为二（rowThreshold<=0 表示未开启）。
func (e *Engine) SetAutoSplit(enabled bool, rowThreshold int64) {
	e.ex.setAutoSplit(enabled, rowThreshold)
}

// SetAutoBalance 配置 M7 自动均衡：enabled 开启后按 interval 周期执行均衡调度
// （interval<=0 使用默认 3s）；关闭时停止循环。
func (e *Engine) SetAutoBalance(enabled bool, interval time.Duration) {
	e.ex.setAutoBalance(enabled, interval)
}

// StopAutoBalance 停止自动均衡循环（db.Close 装配，避免 Close 竞态）。
func (e *Engine) StopAutoBalance() { e.ex.stopAutoBalance() }

// SetBackupEnv 注入备份/恢复环境（M8(BR)，db.Open 装配）。
// st 为引擎级 Storage 引用（BACKUP 一致快照 / RESTORE 物理写 / PITR 回放）；
// binlogPath 为数据目录 binlog 文件路径（可空字符串表示未启用）。
func (e *Engine) SetBackupEnv(st storage.Storage, binlogPath string) {
	e.ex.SetBackupEnv(st, binlogPath)
}

// SplitRegionNow 手动触发 region 分裂（函数式入口，兼容旧手动 SPLIT）。
func (e *Engine) SplitRegionNow(regionID uint64) error { return e.ex.splitRegionByID(regionID) }

// BalanceNow 手动触发一次负载均衡（迁移过载本地 region 到最闲存活节点）。
func (e *Engine) BalanceNow() (*Result, error) { return e.ex.execBalance(&BalanceStmt{}) }

// Execute 解析并执行一条 SQL 语句。
func (e *Engine) Execute(stmt string) (*Result, error) {
	ast, err := Parse(stmt)
	if err != nil {
		return nil, err
	}
	return e.ex.ExecuteText(ast, stmt)
}

// SetSlowThreshold 设置慢查询阈值（毫秒，<=0 关闭记录），透传至执行器。
func (e *Engine) SetSlowThreshold(ms int64) { e.ex.SetSlowThreshold(ms) }

// SetStatsSources 注入运维统计源（T20）：连接数来源与 binlog 位点来源，
// 供 SHOW STATS 输出实时指标；不注入时对应指标为 0。
func (e *Engine) SetStatsSources(connSource func() int64, binlogLSN func() (uint64, error)) {
	e.ex.SetStatsSources(connSource, binlogLSN)
}

// SlowQueries 返回慢查询记录副本（供协议层展示或测试断言）。
func (e *Engine) SlowQueries() []SlowQueryRecord { return e.ex.SlowQueries() }

// IsSQL 判断一行命令是否以 SQL 关键字开头（供协议层路由）。
func IsSQL(line string) bool {
	trimmed := strings.TrimLeft(line, " \t\r\n")
	for _, kw := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "BEGIN", "COMMIT", "ROLLBACK", "EXPORT", "IMPORT", "SHOW", "EXPLAIN", "SPLIT", "BALANCE", "BACKUP", "RESTORE", "ALTER", "SET"} {
		if strings.HasPrefix(strings.ToUpper(trimmed), kw+" ") ||
			strings.EqualFold(trimmed, kw) {
			return true
		}
	}
	return false
}

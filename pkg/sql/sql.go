package sql

import (
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
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

// SlowQueries 返回慢查询记录副本（供协议层展示或测试断言）。
func (e *Engine) SlowQueries() []SlowQueryRecord { return e.ex.SlowQueries() }

// IsSQL 判断一行命令是否以 SQL 关键字开头（供协议层路由）。
func IsSQL(line string) bool {
	trimmed := strings.TrimLeft(line, " \t\r\n")
	for _, kw := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "BEGIN", "COMMIT", "ROLLBACK", "EXPORT", "IMPORT", "SHOW", "EXPLAIN"} {
		if strings.HasPrefix(strings.ToUpper(trimmed), kw+" ") ||
			strings.EqualFold(trimmed, kw) {
			return true
		}
	}
	return false
}

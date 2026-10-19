package sql

import (
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/metrics"
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

// ApplyRemoteDDL 节点侧应用远端 DDL 目录变更（T34 跨节点 DDL 广播接收端）。
func (e *Engine) ApplyRemoteDDL(req cluster.DDLPushReq) error { return e.ex.ApplyRemoteDDL(req) }

// ReplayPendingDDL 重放待同步的 DDL 广播（启动/运维/测试）。
func (e *Engine) ReplayPendingDDL() (int, error) { return e.ex.ReplayPendingDDL() }

// RemoteBackupPrepare 节点侧 T35 备份候选点回调（db.StartCluster 装配到 cluster.Server）。
func (e *Engine) RemoteBackupPrepare(req cluster.BackupPrepareReq) (cluster.BackupPrepareResp, error) {
	return e.ex.RemoteBackupPrepare(req)
}

// RemoteBackupDo 节点侧 T35 分布式备份执行回调（对齐 GlobalLSN 写本地备份）。
func (e *Engine) RemoteBackupDo(req cluster.BackupDoReq) (cluster.BackupDoResp, error) {
	return e.ex.RemoteBackupDo(req)
}

// RemoteRestoreDo 节点侧 T35 分布式恢复执行回调（恢复本地备份并 PITR 到 GlobalLSN）。
func (e *Engine) RemoteRestoreDo(req cluster.RestoreDoReq) (cluster.RestoreDoResp, error) {
	return e.ex.RemoteRestoreDo(req)
}

// SetReplicationModeSetter 装配 T33 复制模式控制器（db.Open 调用，转发至 Executor）。
func (e *Engine) SetReplicationModeSetter(c ReplicationModeSetter) { e.ex.SetReplicationModeSetter(c) }

// AuthenticateUser 校验用户名与密码（M11 预留：server 层连接鉴权握手与执行期权限校验用）。
// 用户不存在或密码不匹配返回 (false, nil)；存储层错误原样返回。密码只做哈希比对，不落明文。
func (e *Engine) AuthenticateUser(name, password string) (bool, error) {
	return e.ex.authenticateUser(name, password)
}

// HasPrivilege 查询用户对某表是否拥有指定权限（M11 预留：执行期权限校验用）。
// op 为 "SELECT"/"INSERT"/"UPDATE"/"DELETE"；用户或表无授权记录返回 false。
// 存储层错误原样返回；未知 op 返回错误。
func (e *Engine) HasPrivilege(user, table, op string) (bool, error) {
	return e.ex.hasPrivilege(user, table, op)
}

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

// StartCoordReplay 启动 2PC 后台补提交扫描循环（db.StartCluster 在
// RecoverCoordinated2PC 之后装配）：周期幂等重放 committing 状态协调者
// 记录，收尾"不确定窗口"未完成的远端 COMMIT。
func (e *Engine) StartCoordReplay(interval time.Duration) { e.ex.StartCoordReplay(interval) }

// StopCoordReplay 停止后台补提交循环（db.Close 装配，避免关闭竞态）。
func (e *Engine) StopCoordReplay() { e.ex.StopCoordReplay() }

// SetBackupEnv 注入备份/恢复环境（M8(BR)，db.Open 装配）。
// st 为引擎级 Storage 引用（BACKUP 一致快照 / RESTORE 物理写 / PITR 回放）；
// binlogPath 为数据目录 binlog 文件路径（可空字符串表示未启用）。
func (e *Engine) SetBackupEnv(st storage.Storage, binlogPath string) {
	e.ex.SetBackupEnv(st, binlogPath)
}

// SetBackupScheduleConfig 装配备份调度配置（BR 方向②，cmd/main start 装配）。
// 全量覆盖：intervalSec 周期秒（0=关闭）、keep 保留份数（0=不清理）、
// dir 备份输出目录、remoteURL 远程 HTTP PUT 目标（空=不上传）。
func (e *Engine) SetBackupScheduleConfig(cfg BackupScheduleConfig) {
	e.ex.SetBackupScheduleConfig(cfg)
}

// StartBackupScheduler 启动备份调度循环（cmd/main start 装配后调用；
// 与 db 启动流程衔接，确保 BACKUP 环境已注入）。
func (e *Engine) StartBackupScheduler() { e.ex.StartBackupScheduler() }

// StopBackupScheduler 停止备份调度循环（db 关闭流程装配，避免关闭竞态）。
func (e *Engine) StopBackupScheduler() { e.ex.StopBackupScheduler() }

// BackupScheduleStatus 返回备份调度状态快照（SHOW BACKUP STATUS 数据源）。
func (e *Engine) BackupScheduleStatus() BackupScheduleStatus { return e.ex.BackupScheduleStatus() }

// T37 商用档：监控告警（规则与阈值配置）Engine 装配。
// 规则评估引擎以 metrics.Registry.Snapshot 为数据源（与 Prometheus 同源），
// 通知出口复用 logx.DefaultAlerts（内存缓冲 + 文件 + webhook）。cmd/main
// start 在 SetMetrics 后调用 StartAlertRules；db 关闭流程调用 StopAlertRules。

// StartAlertRules 启动告警规则评估循环（cmd/main start 装配后调用；幂等）。
func (e *Engine) StartAlertRules() { e.ex.StartAlertRules() }

// StopAlertRules 停止告警规则评估循环（db 关闭流程装配，避免关闭竞态）。
func (e *Engine) StopAlertRules() { e.ex.StopAlertRules() }

// SetAlertRulesEnabled 设置规则告警总开关（SET alert_rules_enabled 数据源）。
func (e *Engine) SetAlertRulesEnabled(on bool) { e.ex.SetAlertRulesEnabled(on) }

// AlertRulesEnabled 返回规则告警总开关。
func (e *Engine) AlertRulesEnabled() bool { return e.ex.AlertRulesEnabled() }

// SetAlertEvalInterval 设置评估周期秒（SET alert_eval_interval 数据源）。
func (e *Engine) SetAlertEvalInterval(sec int64) { e.ex.SetAlertEvalInterval(sec) }

// AlertEvalInterval 返回评估周期秒。
func (e *Engine) AlertEvalInterval() int64 { return e.ex.AlertEvalInterval() }

// SetAlertRulesJSON 解析并应用规则 JSON（SET alert_rules 数据源；非法返回错误）。
func (e *Engine) SetAlertRulesJSON(s string) error { return e.ex.SetAlertRulesJSON(s) }

// AlertRulesJSON 返回当前规则集 JSON（SHOW CONFIG 数据源）。
func (e *Engine) AlertRulesJSON() string { return e.ex.AlertRulesJSON() }

// AlertRules 返回规则状态快照（SHOW ALERT RULES 数据源）。
func (e *Engine) AlertRules() []RuleStatus { return e.ex.AlertRules() }

// EvaluateAlertRulesNow 立即执行一轮规则评估（测试/运维手动触发）。
func (e *Engine) EvaluateAlertRulesNow() { e.ex.EvaluateAlertRulesNow() }

// BackupNow 手动立即执行一次备份（运维/测试接口），返回备份文件路径。
// 正在执行时返回错误；结果同时记录到 SHOW BACKUP STATUS 最近状态。
func (e *Engine) BackupNow() (string, error) { return e.ex.BackupNow() }

// SplitRegionNow 手动触发 region 分裂（函数式入口，兼容旧手动 SPLIT）。
func (e *Engine) SplitRegionNow(regionID uint64) error { return e.ex.splitRegionByID(regionID) }

// BalanceNow 手动触发一次负载均衡（迁移过载本地 region 到最闲存活节点）。
func (e *Engine) BalanceNow() (*Result, error) {
	return e.ex.execBalance(e.ex.globalSess, &BalanceStmt{})
}

// Execute 解析并执行一条 SQL 语句。
// 以超级用户（root）身份执行，向后兼容未接入鉴权的调用方（CLI/旧驱动）。
func (e *Engine) Execute(stmt string) (*Result, error) {
	return e.ExecuteAs(stmt, SuperUser)
}

// NewSession 创建独立 SQL 会话（服务端每 TCP 连接一个，见 pkg/server）。
// 会话承载独立显式事务状态（BEGIN/COMMIT/ROLLBACK）与语句快照，多连接
// 可并行执行互不干扰；同一 Session 不得被多个 goroutine 并发使用
// （连接内串行语义）。旧 API（Execute/ExecuteAs）仍使用内置全局会话，
// 行为与旧版完全一致。
func (e *Engine) NewSession() *Session { return e.ex.NewSession() }

// ExecuteAsSession 解析并以指定用户身份、在指定独立会话上执行一条 SQL
// 语句：显式事务状态与语句快照属于该会话，跨连接并行安全。会话由
// NewSession 创建；多行/多语句需同一 Session 串行调用。
func (e *Engine) ExecuteAsSession(stmt, user string, sess *Session) (*Result, error) {
	ast, err := Parse(stmt)
	if err != nil {
		return nil, err
	}
	return e.ex.ExecuteTextAsSession(ast, stmt, user, sess)
}

// ExecuteAs 解析并以指定用户身份执行一条 SQL 语句：执行前经权限校验
// （管理语句仅 root；DML 需对应表级授权；SHOW/EXPLAIN/SET/事务控制放行）。
// 空用户（降级 guest）视为无任何授权。校验规则详见 authz.go 与
// docs/T24_m11_auth.md。
func (e *Engine) ExecuteAs(stmt, user string) (*Result, error) {
	ast, err := Parse(stmt)
	if err != nil {
		return nil, err
	}
	return e.ex.ExecuteTextAs(ast, stmt, user)
}

// SetSlowThreshold 设置慢查询阈值（毫秒，<=0 关闭记录），透传至执行器。
// 运行时调用即时生效；也可经 SET slow_query_threshold = <ms> 配置。
func (e *Engine) SetSlowThreshold(ms int64) { e.ex.SetSlowThreshold(ms) }

// SlowQueryThreshold 返回当前慢查询阈值（毫秒），0 表示已禁用记录。
func (e *Engine) SlowQueryThreshold() int64 { return e.ex.SlowQueryThreshold() }

// SetSlowLog 配置慢查询落盘（M13）：path 为日志文件路径（如 data/slow.log），
// enabled=false 或 path 空串关闭落盘。变更即时生效，无需重启。
func (e *Engine) SetSlowLog(path string, enabled bool) { e.ex.SetSlowLog(path, enabled) }

// SetPersistedConfigPath 装配持久化配置文件路径（M16-1，db.Open 注入
// data/persisted.json；空=未启用持久化，SET PERSIST 报错）。
func (e *Engine) SetPersistedConfigPath(path string) { e.ex.SetPersistedConfigPath(path) }

// LoadPersistedConfig 加载持久化配置并合并到内存（M16-1，db.Open 启动时调用）。
// 优先级：内存（SET / SET PERSIST 立即生效）> 持久化（重启合并）> 默认值；
// 非法键 / 非法值 / 损坏文件返回明确错误，绝不静默忽略。
func (e *Engine) LoadPersistedConfig() error { return e.ex.LoadPersistedConfig() }

// PersistedSlowLogIntent 返回持久化 slow_query_log 的启动意图
// （1=on、0=off、-1=无记录），供服务装配慢日志路径时尊重持久化开关。
func (e *Engine) PersistedSlowLogIntent() int { return e.ex.PersistedSlowLogIntent() }

// PersistedTLSConfig 返回持久化 TLS 启动意图（T36）：证书/私钥路径与
// AllowLegacy 开关（cert/key 均空表示无持久化 TLS），供服务装配监听层。
func (e *Engine) PersistedTLSConfig() (cert, key string, allowLegacy bool) {
	return e.ex.PersistedTLSConfig()
}

// SlowLogEnabled 返回当前慢查询落盘开关状态。
func (e *Engine) SlowLogEnabled() bool { return e.ex.SlowLogEnabled() }

// SlowLogPath 返回当前慢查询落盘路径（关闭时为空串）。
func (e *Engine) SlowLogPath() string { return e.ex.SlowLogPath() }

// SetSlowQueryMax 设置内存慢查询记录保留条数（>=1 生效；默认 64）。
func (e *Engine) SetSlowQueryMax(n int) { e.ex.SetSlowQueryMax(n) }

// ShutdownTimeout 返回优雅停机超时（M16-2，默认 30s；可由
// SET [PERSIST] shutdown_timeout 热更新）。供服务端 SHUTDOWN/SIGINT 触发
// drain 时读取当前生效超时。
func (e *Engine) ShutdownTimeout() time.Duration {
	return time.Duration(e.ex.ShutdownTimeoutSec()) * time.Second
}

// SetStatsSources 注入运维统计源（T20）：连接数来源与 binlog 位点来源，
// 供 SHOW STATS 输出实时指标；不注入时对应指标为 0。
func (e *Engine) SetStatsSources(connSource func() int64, binlogLSN func() (uint64, error)) {
	e.ex.SetStatsSources(connSource, binlogLSN)
}

// SetMetrics 注入指标注册表（M13 metrics 导出；nil 关闭 SQL/事务/节点计数）。
func (e *Engine) SetMetrics(m *metrics.Registry) { e.ex.SetMetrics(m) }

// MetricsRegionStats 聚合当前路由表 region 分片状态（M13 metrics 采集源）。
func (e *Engine) MetricsRegionStats() metrics.RegionStats { return e.ex.MetricsRegionStats() }

// SlowQueries 返回慢查询记录副本（供协议层展示或测试断言）。
func (e *Engine) SlowQueries() []SlowQueryRecord { return e.ex.SlowQueries() }

// ClusterNodes 返回节点注册表快照（存活/角色，按 ID 排序；未装配集群返回空）。
func (e *Engine) ClusterNodes() []cluster.NodeInfo {
	if e.ex.mgr == nil {
		return nil
	}
	return e.ex.mgr.ListNodes()
}

// ClusterSelfNode 返回本节点信息（未装配集群返回 false；STATUS 单机兜底用）。
func (e *Engine) ClusterSelfNode() (cluster.NodeInfo, bool) {
	if e.ex.mgr == nil {
		return cluster.NodeInfo{}, false
	}
	return e.ex.mgr.SelfNode(), true
}

// IsSQL 判断一行命令是否以 SQL 关键字开头（供协议层路由）。
func IsSQL(line string) bool {
	trimmed := strings.TrimLeft(line, " \t\r\n")
	for _, kw := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "GRANT", "REVOKE", "BEGIN", "COMMIT", "ROLLBACK", "EXPORT", "IMPORT", "SHOW", "EXPLAIN", "SPLIT", "BALANCE", "BACKUP", "RESTORE", "ALTER", "SET", "REFRESH", "USE", "CALL"} {
		if strings.HasPrefix(strings.ToUpper(trimmed), kw+" ") ||
			strings.EqualFold(trimmed, kw) {
			return true
		}
	}
	return false
}

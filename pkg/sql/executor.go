package sql

import (
	"bytes"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/metrics"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/tso"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// Executor SQL 执行器（开发手册 §4.2 算子最小集）。
// 依赖：TxnManager（本地 ACID + 快照隔离）；DDL/DML 默认以单语句隐式事务提交；
// BEGIN 后进入会话级显式事务，后续语句复用同一事务直至 COMMIT/ROLLBACK。
type Executor struct {
	tm txn.TxnManager
	mu sync.Mutex
	// T33 复制模式控制器（nil = 未装配，SET replication_* 返回明确错误）。
	// 由 db.Open 注入（避免 sql 包依赖 db 包）；实现见 ReplicationModeSetter。
	replCtl ReplicationModeSetter
	// P0 HA：写入 fencing 检查（nil = 未装配，写路径零开销放行）。
	// 由 db.Open 注入（db.DB.AllowWrite）；实现见 WriteFence。
	writeFence WriteFence
	// D6：备库常驻读只读强制（replica 模式装配；默认 false 主端零回归）。
	// SetReplicaReadOnly 开启后，写语句（isWriteStmt 白名单）在
	// ExecuteTextAsSession 入口被拒绝；提升为主节点时可热切换置 false。
	roMu      sync.RWMutex
	replicaRO bool
	// P0 HA：ha_epoch_enabled / ha_quorum_enabled 开关节流控制器
	// （nil = 未装配，SET ha_* 返回明确错误）；实现见 HAConfigSetter。
	haCfg HAConfigSetter
	// P1 HA：clusterEpoch 取号源与 quorum 仲裁器（nil = 未装配，M6 旧行为零回归）。
	// epochSrc 供 failoverRegion/CheckRegionPushEpoch 取新号与对照；
	// quorumArb 供 FailoverDownNode 前置多数派确认（不足则降级只读）。
	epochSrc  EpochSource
	quorumArb QuorumArbiter
	// P1 HA：已重指派 region 的 epoch 快照（regionEpoch 大者胜）。
	// FailoverDownNode 重指派成功后写入（随路由持久化），resolveRegionOwner
	// 读取 failover 路由时对照，陈旧 failover 条目视为无效。
	p1Mu    sync.Mutex
	p1Epoch map[storage.ID]p1RegionEpoch
	// P1 HA：可注入持有校验钩子（nil = 真实键空间扫描，见 ha_p1.go）。
	holdsChecker holdsChecker
	// P2 HA：陈旧路由消息冲突计数（分区恢复合并冲突标记，见 ha_p2.go）。
	p2Mu           sync.Mutex
	regionConflict uint64
	// 全局会话（旧单会话 API 使用：Engine.Execute/ExecuteAs、CLI、既有测试）。
	// 服务端多连接场景使用 NewSession 创建独立会话，显式事务状态互不干扰。
	globalSess *Session
	// v1.8.0 触发器递归深度上限：允许的嵌套触发层数（默认 1：最外层 DML +
	// 1 层链式不同触发器；更深层与直接/同键递归被抑制）。配置化可热更新。
	triggerMaxDepth int
	// M3 分片路由（sql.New 注入默认 Router；db.Open 加载落盘元数据后覆盖）
	router *sharding.Router
	// M4 集群管理器（nil = 单机模式，转发路径零触发）。
	// db.Open 装配后注入；selfID 供"本节点"归属判断。
	mgr    *cluster.Manager
	selfID string
	// M8 分布式快照隔离：ts 为全局时间戳源（nil = 未装配，旧路径零回归）。
	// 快照版本与会话状态见 Session（显式事务 curBeginTS / autocommit
	// 语句 stmtBeginTS）。
	ts tso.Source
	// P2-13 Serializable 提交冲突检测注册表（NewExecutor 初始化；
	// 仅 Serializable 显式事务使用，其他路径零触发）。
	serial *serialRegistry
	// M6 故障转移状态：自动重指派互斥 + 已处理 down 节点去重。
	failoverMu   sync.Mutex
	failoverDone map[string]bool
	// M7 自动分裂：配置（开启 + 行数阈值）+ 各表写计数水位。
	splitMu  sync.Mutex
	splitCfg splitConfig
	splitAcc map[uint64]int64
	// M7 自动均衡：周期循环控制（并发触发互斥见 balanceOnce）。
	balanceMu    sync.Mutex
	balanceStop  chan struct{}
	balanceDone  chan struct{}
	balanceOnceN int64 // 已执行均衡轮数（测试观测用）
	// P2/M13：慢查询配置与记录（内存 + 落盘）。
	// slowThreshold 毫秒阈值；<=0 表示不记录（默认 100ms）。
	// slowQueries 内存环形记录（保留最近 slowQueryMax 条，可 SHOW 查询）；
	// slowLog 落盘器（db/cmd 装配路径与开关后写入 data/slow.log）。
	slowThreshold int64
	slowQueries   []SlowQueryRecord
	slowQueryMax  int // 内存保留条数（默认 slowQueryMaxDefault，可 SetSlowQueryMax 调整）
	slowLog       *SlowLog
	// v1.14.0 P0：只读 SELECT 并行开关（服务端 REPLSession 批内 autocommit
	// 只读 SELECT 并发执行 + 按序重组 + 批尾 flush）。默认开启；关闭回退
	// v1.13.0 逐条串行执行行为。经 e.mu 保护读写，热路径读开销可忽略。
	roParallel bool
	// v1.16.0 C6：autocommit 只读语句共享快照开关。开启时连续 autocommit
	// 只读 SELECT 复用同一临时只读事务（RocksDB 快照），免逐语句
	// tm.Begin/Snapshot/Release 固定开销（延迟隐藏）；关闭回退逐语句独立
	// 快照（原语义）。仅 autocommit 只读 SELECT 生效，显式事务边界与错误
	// 传播语义不变。经 e.mu 保护读写（与 roParallel 同锁）。
	snapshotShare bool
	// v1.16.0 C6：连接内 pipeline 条件 flush 开关。服务端 REPLSession 串行
	// 路径开启时，通道仍有后续命令则合并响应 flush（隐藏逐条 flush 延迟）；
	// 通道空/读侧关闭/语句出错时立即 flush（保持逐条交互与错误传播语义）。
	// 关闭回退 v1.14.0 每命令 flush 行为。经 e.mu 保护读写。
	pipelineFlush bool
	// v1.17.0 D1.1：显式事务 UPDATE 快照刷新锁外化开关。开启时显式事务内
	// 对已持锁行（事务内先前语句已取行锁/表锁）跳过 refreshWriteSnapshot：
	// 行锁持有期间前序事务不可能修改该行，刷新无新增信息，RocksDB 快照
	// 重建与 TSO 取号从每写语句 1 次降为每新锁定行 1 次；仅新锁定行仍锁内
	// 刷新（读到前序提交，保持 M18/M19 读改写不丢更新语义）。autocommit
	// 路径不受影响（无持锁行，恒刷新）。关闭回退 v1.16.0 每次写语句刷新。
	// 经 e.mu 保护读写（与 roParallel/snapshotShare 同锁）。
	snapshotLockExternal bool
	// v1.14.0 P1：WAL 组提交定时刷盘窗口（毫秒，0=关闭回退被动攒批）。
	// 经 SetGroupCommitWindow 同步透传底层 WAL；SHOW CONFIG 读取同锁保护。
	groupCommitWindowMs float64
	// v1.17.0 D1.3：WAL 组提交主动定时窗口开关。开启时 leader 无并发等待
	// 者也等待 groupCommitWindow（收集稀疏到达的 Append 合并 fsync，降低
	// 低并发写路径 fsync 频率）；关闭回退 v1.16.0 C1 单等待者免窗口（仅
	// 存在 follower 时等待，无固定窗口延迟）。默认 false（零回归）。
	// 经 SetGroupCommitActiveWindow 同步透传底层 WAL。
	activeGroupCommitWindow bool
	// v1.17.0 D1.2：autocommit 写 pipeline 开关。开启时服务端 REPLSession
	// 收集连续 autocommit 写语句批，executor 层 Commit 走 defer sync（批内
	// 跳过 WAL fsync，批尾 FlushWritePipeline 一次性 SyncUpTo 覆盖整批，把
	// N 次 fsync 合并为 1 次），且窗口内 RocksDB 自身 WAL 被禁用（OpenXDB
	// WAL 为唯一持久化通道，杜绝幽灵数据窗口）。仅 autocommit 写路径生效，
	// 显式事务与错误传播语义不变。默认 false（零回归）。经 e.mu 保护读写。
	writePipeline bool
	// v1.16.0：WAL 自动 checkpoint 控制器（nil = 未装配，SET wal_* 返回
	// 明确错误，DB 行为零回归）。由 db.Open 注入（sql 包不依赖 db 包）。
	chkCtl CheckpointSetter
	// M17-1 审计日志：auditEnabled 总开关（默认开，安全默认）；
	// auditThreshold 慢审计阈值毫秒（<=0 不标记 slow）；auditRecords 内存环
	// 缓冲（保留最近 auditMax 条，SHOW AUDITLOG 查询）；auditLog 落盘器
	// （db/cmd 装配路径与开关后写入 data/audit.log）。
	auditEnabled   bool
	auditThreshold int64
	auditRecords   []AuditRecord
	auditMax       int
	auditID        int64
	auditLog       *AuditLog
	// M8(BR)：备份/恢复环境（db.Open 装配注入）。
	// st 为引擎级 Storage 引用：BACKUP 基于一致快照导出、RESTORE 物理写与 PITR 回放；
	// binlogPath 为数据目录 binlog 文件路径（TO LSN 回放读取，M2 pkg/replication）。
	st         storage.Storage
	binlogPath string
	// AUTO_INCREMENT 内存计数器：key=表名\x00列名 → 下一个待分配值。
	// 首次遇到某表列时从现有数据扫 max+1 初始化，之后单调递增（回滚/失败
	// 跳号，与 MySQL 语义一致；重启后重新扫 max 恢复）。
	autoIncMu   sync.Mutex
	autoIncNext map[string]int64
	// T20：运维指标注入源（nil = 未装配，SHOW STATS 输出 0）。
	// connSource 返回当前活跃 TCP 连接数（server 层会话计数）；
	// binlogLSN 返回当前 binlog 最大位点（db 层注入，读取只读）。
	connSource func() int64
	binlogLSN  func() (uint64, error)
	// M9：查询结果缓存（SELECT 结果以规范化 SQL 为 key；写语句后全量失效）。
	qc *QueryCache
	// M13：metrics 导出注册表（nil = 未装配，计数零触发）。
	m *metrics.Registry
	// M16-2：优雅停机超时（秒）。SHUTDOWN/SIGINT 触发 drain 的最长等待时间，
	// 超时未排空强制退出（非 0 退出码 + 告警日志）。可经
	// SET [PERSIST] shutdown_timeout 热更新，默认 30 秒。
	shutdownTimeoutSec int64
	// M16-1：在线配置热更新（config.go）。
	// cfgPath 持久化配置文件路径（db.Open 装配 data/persisted.json；空=未启用持久化）；
	// persistedCfg 已持久化键→值（SHOW PERSISTED CONFIG / SHOW CONFIG 来源 persisted）；
	// sessionCfg 本进程 SET（非 PERSIST）覆盖过的键→值（SHOW CONFIG 来源 session）。
	cfgPath      string
	cfgMu        sync.Mutex
	persistedCfg map[string]string
	sessionCfg   map[string]string
	// BR 方向②扩展：备份调度器（周期自动 BACKUP + 保留份数 + 状态接口，
	// backup_schedule.go）。NewExecutor 初始化；config.go 的 backup_* 键
	// 热更新、cmd/main 启动装配与 SHOW BACKUP STATUS 均读写此调度器。
	backupSched *BackupScheduler
	// T37 商用档：告警规则评估引擎（指标阈值规则 + 状态机 + 通知出口，
	// alert_rules.go）。NewExecutor 初始化；config.go 的 alert_rules* 键
	// 热更新、cmd/main 启动装配与 SHOW ALERT RULES / SHOW ALERT STATUS
	// 均读写此引擎。评估数据源为 metrics.Registry.Snapshot（e.m，同 Prometheus）。
	alertRules      *AlertRulesEngine
	coordReplayStop chan struct{}
	coordReplayDone chan struct{}
	// P2-12 备份导出分片并行扫描并发度：BACKUP ... CONCURRENCY k 设置；
	// <=0 表示未显式指定，backupCore 使用默认自动并发度
	// （min(GOMAXPROCS, 4)，见 backup.go）。
	backupConcurrency int
	// v1.9.0 物化视图增量刷新：依赖表集合缓存（DML 钩子 O(1) 判定是否记录
	// 变更键）。nil = 未构建，tableTrackedByMVs 惰性从 m:matviews 目录加载；
	// create/drop 物化视图后重建。
	mvDepMu  sync.RWMutex
	mvDepSet map[string]bool
}

// splitConfig M7 自动分裂配置。
type splitConfig struct {
	enabled      bool
	rowThreshold int64 // 行数阈值；<=0 视为未开启
}

func NewExecutor(tm txn.TxnManager) *Executor {
	e := &Executor{
		tm:            tm,
		globalSess:    &Session{prepared: map[string]string{}, userVars: map[string]Value{}, pendingRemote: map[string][]kvOp{}},
		serial:        newSerialRegistry(), // P2-13：Serializable 提交冲突检测注册表
		router:        sharding.NewRouter(),
		failoverDone:  map[string]bool{},
		p1Epoch:       map[storage.ID]p1RegionEpoch{},
		splitAcc:      map[uint64]int64{},
		qc:            NewQueryCache(),
		// v1.14.0 P0：只读 SELECT 并行默认开启（SET ro_parallel=off 回退串行）。
		roParallel:    true,
		// v1.16.0 C6：autocommit 只读共享快照 / 连接内 pipeline 条件 flush
		// 默认开启（SET snapshot_share=off / pipeline_flush=off 回退逐语句
		// 独立快照 / 逐命令 flush 原语义）。
		snapshotShare: true,
		pipelineFlush: true,
		// v1.17.0 D1.1：显式事务 UPDATE 快照刷新锁外化默认开启（SET
		// snapshot_lock_external=off 回退 v1.16.0 每次写语句锁内刷新）。
		snapshotLockExternal: true,
		// v1.14.0 P1：WAL 组提交定时刷盘窗口默认 0.5ms（SET group_commit_window_ms=0 回退被动攒批）。
		groupCommitWindowMs: groupCommitWindowMsDefault,
		slowThreshold: slowThresholdDefault, // M13：默认 100ms
		slowQueryMax:  slowQueryMaxDefault,
		slowLog:       NewSlowLog(),
		// v1.8.0：触发器递归深度默认 1（允许 1 层链式，禁止直接/更深递归）。
		triggerMaxDepth: 1,
		// M17-1：审计默认开启（安全默认）+ 内存环 1000 条；落盘器懒创建。
		auditEnabled: true,
		auditMax:     auditMaxDefault,
		// M16-1：在线配置热更新（空 map 保证 SHOW / PERSIST 零 nil 访问）。
		persistedCfg: map[string]string{},
		sessionCfg:   map[string]string{},
		// M16-2：优雅停机默认超时 30s（可 SET shutdown_timeout 热更新）。
		shutdownTimeoutSec: shutdownTimeoutDefaultSec,
	}
	// BR 方向②：备份调度器单点装配（需引用 Executor 本身执行 BACKUP，
	// 故构造后单独初始化；db.Close 停止调度亦依赖此字段非 nil）。
	e.initBackupScheduler()
	// T37 商用档：告警规则评估引擎单点装配（需引用 Executor 取指标快照，
	// 故构造后单独初始化；cmd/main 启动/停止评估循环亦依赖此字段非 nil）。
	e.initAlertRulesEngine()
	// v1.14.0 P1：与底层 WAL 默认窗口保持一致（wal_impl 默认 500µs）。
	e.SetGroupCommitWindow(time.Duration(groupCommitWindowMsDefault * float64(time.Millisecond)))
	return e
}

// newExecutor 为 NewExecutor 的辅助装配，见 NewExecutor。

// NewExecutorWithScheduler 保持 NewExecutor 单点装配：调度器需引用 Executor
// 本身（执行 BACKUP），故在结构体构造后单独初始化。
func (e *Executor) initBackupScheduler() {
	if e.backupSched == nil {
		e.backupSched = newBackupScheduler(e)
	}
}

// SetROParallel 设置只读 SELECT 并行开关（v1.14.0 P0）：批内 autocommit
// 只读 SELECT 并发执行 + 按序重组。关闭回退 v1.13.0 逐条串行执行行为。
// 运行时调用即时生效（SET ro_parallel=on|off 热切换）。
func (e *Executor) SetROParallel(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.roParallel = enabled
}

// ROParallel 返回只读 SELECT 并行开关当前状态（服务端 REPLSession 热路径
// 判定是否启用并行批收集）。
func (e *Executor) ROParallel() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.roParallel
}

// SetSnapshotShare 设置 autocommit 只读语句共享快照开关（v1.16.0 C6）：
// 开启时连续 autocommit 只读 SELECT 复用同一临时只读事务（RocksDB 快照），
// 免逐语句 Begin/Snapshot/Release 固定开销；关闭回退逐语句独立快照（原
// 语义）。运行时调用即时生效（SET snapshot_share=on|off 热切换）。显式
// 事务边界与错误传播语义不受影响。
func (e *Executor) SetSnapshotShare(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.snapshotShare = enabled
}

// SnapshotShare 返回 autocommit 只读语句共享快照开关当前状态（SHOW
// CONFIG 与测试观测用）。
func (e *Executor) SnapshotShare() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotShare
}

// SetPipelineFlush 设置连接内 pipeline 条件 flush 开关（v1.16.0 C6）：
// 开启时服务端 REPLSession 串行路径在通道仍有后续命令时合并响应 flush
// （延迟隐藏）；关闭回退 v1.14.0 每命令 flush 行为。运行时调用即时生效
// （SET pipeline_flush=on|off 热切换）。
func (e *Executor) SetPipelineFlush(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pipelineFlush = enabled
}

// PipelineFlush 返回连接内 pipeline 条件 flush 开关当前状态（服务端
// REPLSession 热路径判定与 SHOW CONFIG 用）。
func (e *Executor) PipelineFlush() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pipelineFlush
}

// SetSnapshotLockExternal 设置显式事务 UPDATE 快照刷新锁外化开关
// （v1.17.0 D1.1）：开启时显式事务内对已持锁行跳过 refreshWriteSnapshot
// （行锁持有期间前序事务不可能修改该行，刷新无新增信息），仅新锁定行
// 锁内刷新；关闭回退 v1.16.0 每次写语句锁内刷新。运行时调用即时生效
// （SET snapshot_lock_external=on|off 热切换）。autocommit 路径不受影响。
func (e *Executor) SetSnapshotLockExternal(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.snapshotLockExternal = enabled
}

// SnapshotLockExternal 返回显式事务 UPDATE 快照刷新锁外化开关当前状态
// （execUpdate 热路径判定与 SHOW CONFIG 用）。
func (e *Executor) SnapshotLockExternal() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotLockExternal
}

// SetGroupCommitWindow 设置 WAL 组提交定时刷盘窗口（v1.14.0 P1），
// 透传至事务管理器底层 WAL；d<=0 关闭定时窗口（回退 v1.13.0 被动攒批）。
// 经 SET group_commit_window_ms 热切换即时生效。
func (e *Executor) SetGroupCommitWindow(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.groupCommitWindowMs = float64(d) / float64(time.Millisecond)
	// tm 可能为 nil（测试/未装配场景）：与 writeFence 等注入点同模式，零开销放行。
	if e.tm != nil {
		e.tm.SetGroupCommitWindow(d)
	}
}

// SetGroupCommitActiveWindow 设置 WAL 组提交主动定时窗口开关（v1.17.0
// D1.3），透传至事务管理器底层 WAL。开启时 leader 无并发等待者也等待
// 组提交窗口（收集稀疏 Append 合并 fsync）；关闭回退 v1.16.0 C1 单等待
// 者免窗口（零回归）。经 SET group_commit_active_window 热切换即时生效。
func (e *Executor) SetGroupCommitActiveWindow(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.activeGroupCommitWindow = enabled
	if e.tm != nil {
		e.tm.SetActiveWindow(enabled)
	}
}

// GroupCommitActiveWindow 返回 WAL 组提交主动定时窗口开关当前状态
// （v1.17.0 D1.3；SHOW CONFIG 与测试观测用）。
func (e *Executor) GroupCommitActiveWindow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.activeGroupCommitWindow
}

// SetWritePipeline 设置 autocommit 写 pipeline 开关（v1.17.0 D1.2）。
// 开启时：写批内事务（executor 逐事务置 defer 标记）Commit 跳过 WAL
// fsync，服务端 REPLSession 批尾 FlushWritePipeline 统一刷盘；存储禁用
// 自身 WAL（OpenXDB WAL 为唯一持久化通道，杜绝幽灵数据窗口）；关闭时
// 全部还原（零回归）。事务级 defer 由 exec2pcWrite 依据会话标记逐事务
// 置位，其余连接/事务不受影响（各自 SyncUpTo 水位推进会顺带覆盖更早
// defer 事务的 WAL，持久化只提前不延后）。
func (e *Executor) SetWritePipeline(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.writePipeline = enabled
	if e.st != nil {
		if nw, ok := e.st.(interface{ SetNoWAL(bool) }); ok {
			nw.SetNoWAL(enabled)
		}
	}
}

// WritePipeline 返回 autocommit 写 pipeline 开关当前状态（v1.17.0 D1.2；
// SHOW CONFIG 与测试观测用）。
func (e *Executor) WritePipeline() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.writePipeline
}

// FlushWritePipeline 统一刷盘写批内延迟持久化的事务（v1.17.0 D1.2）：
// 对 deferredLSN 水位执行一次 WAL SyncUpTo 覆盖整批。幂等；批尾必须在
// 响应写回前调用（保证客户端 ack 前全部事务已持久化）。返回底层刷盘错误。
func (e *Executor) FlushWritePipeline() error {
	if e.tm == nil {
		return nil
	}
	return e.tm.FlushDeferredSync()
}

// groupCommitWindowMsText 返回 WAL 组提交定时刷盘窗口当前值（毫秒，0=已关闭）。
// 供 SHOW CONFIG 展示；线程安全（随 SetGroupCommitWindow 同锁保护）。
func (e *Executor) groupCommitWindowMsText() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return strconv.FormatFloat(e.groupCommitWindowMs, 'f', 3, 64)
}

// SetSlowThreshold 设置慢查询阈值（毫秒，<=0 关闭记录）。运行时调用即时生效。
func (e *Executor) SetSlowThreshold(ms int64) { e.slowThreshold = ms }

// SlowQueryThreshold 返回当前慢查询阈值（毫秒），0 表示已禁用记录。
func (e *Executor) SlowQueryThreshold() int64 { return e.slowThreshold }

// SetSlowLog 配置慢查询落盘（M13）：path 为日志文件路径，enabled=false 或
// path 空串关闭落盘。配置变更对后续记录即时生效，无需重启。
func (e *Executor) SetSlowLog(path string, enabled bool) {
	if e.slowLog == nil {
		e.slowLog = NewSlowLog()
	}
	e.slowLog.Configure(path, enabled)
}

// SlowLogEnabled 返回当前慢查询落盘开关状态（SHOW STATS 等展示用）。
func (e *Executor) SlowLogEnabled() bool {
	return e.slowLog != nil && e.slowLog.Enabled()
}

// SlowLogPath 返回当前慢查询落盘路径（关闭时为空串）。
func (e *Executor) SlowLogPath() string {
	if e.slowLog == nil {
		return ""
	}
	return e.slowLog.Path()
}

// SetSlowQueryMax 设置内存慢查询记录保留条数（>=1 生效；默认 slowQueryMaxDefault）。
// 调整即时生效，不影响已保留记录。
func (e *Executor) SetSlowQueryMax(n int) {
	if n < 1 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.slowQueryMax = n
	if len(e.slowQueries) > n {
		e.slowQueries = e.slowQueries[len(e.slowQueries)-n:]
	}
}

// SetStatsSources 装配 SHOW STATS 指标注入源（T20，cmd/openxdb start 时注入）：
// conn 返回活跃连接数（server 层会话计数）；binlog 返回 binlog 最大位点（db 层）。
func (e *Executor) SetStatsSources(conn func() int64, binlog func() (uint64, error)) {
	e.connSource = conn
	e.binlogLSN = binlog
}

// SetMetrics 注入指标注册表（M13 metrics 导出；nil 关闭计数）。
// T37 起同时把注册表快照源注入告警规则评估引擎（评估数据与 Prometheus
// /metrics 同源；nil 时规则引擎不评估）。
func (e *Executor) SetMetrics(m *metrics.Registry) {
	e.m = m
	if e.alertRules != nil {
		if m != nil {
			e.alertRules.snapshot = m.Snapshot
		} else {
			e.alertRules.snapshot = nil
		}
	}
}

// incLocal / incRemote 数据访问本地/远端转发计数（M13 metrics）。
func (e *Executor) incLocal() {
	if e.m != nil {
		e.m.IncLocal()
	}
}
func (e *Executor) incRemote() {
	if e.m != nil {
		e.m.IncRemote()
	}
}

// MetricsRegionStats 聚合当前路由表的 region 分片状态指标（M13 metrics 采集源）。
// 口径与 T20 SHOW STATS 一致：仅统计显式路由表（隐式单 region 不落盘）。
func (e *Executor) MetricsRegionStats() metrics.RegionStats {
	regions := e.router.AllRegions()
	st := metrics.RegionStats{ByState: map[string]int64{}}
	for _, rg := range regions {
		st.Total++
		st.ByState[string(rg.State)]++
		if rg.Node == "" || rg.Node == e.selfID {
			st.Local++
		} else {
			st.Remote++
		}
	}
	return st
}

// SlowQueries 返回当前慢查询记录副本（SHOW SLOWQUERIES 与协议层读取共用）。
func (e *Executor) SlowQueries() []SlowQueryRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]SlowQueryRecord, len(e.slowQueries))
	copy(out, e.slowQueries)
	return out
}

// recordSlow 记录一条慢查询（内存保留最近 slowQueryMax 条 + 落盘）。
// 调用方已通过阈值判定（低开销：此路径之外不做任何格式化）。
func (e *Executor) recordSlow(r SlowQueryRecord) {
	e.mu.Lock()
	if len(e.slowQueries) >= e.slowQueryMax {
		e.slowQueries = e.slowQueries[1:]
	}
	e.slowQueries = append(e.slowQueries, r)
	e.mu.Unlock()
	if e.m != nil {
		e.m.IncSlow()
	}
	// 落盘在内存锁外执行，慢查询低频 + 落盘器独立互斥，互不阻塞热路径。
	if e.slowLog != nil {
		e.slowLog.Write(r)
	}
}

// execTimed 执行一条语句并统计耗时（毫秒）：先取开始时间，执行结束后
// 若超过阈值（含执行失败，Err 记录错误信息）才构造记录与格式化，正常语句零开销。
func (e *Executor) execTimed(sess *Session, user, sql string, stmt Stmt, fn func() (*Result, error)) (*Result, error) {
	startMs := timeNowMs()
	startAt := timeNow()
	res, err := fn()
	ms := timeNowMs() - startMs
	if e.m != nil {
		e.m.IncSQL()
	}
	if e.slowThreshold > 0 && ms >= e.slowThreshold {
		e.recordSlow(SlowQueryRecord{
			SQL:      sql,
			User:     user,
			StartAt:  startAt,
			Duration: ms,
			Rows:     rowsOf(res),
			Err:      errString(err),
		})
	}
	// M17 审计埋点：DML/DDL/PRIVILEGE 语句记录审计事件（SELECT/SHOW 等
	// 非审计语句跳过；总开关关闭零开销；失败语句 success=false + Err）。
	if event, obj, ok := auditEventOf(stmt); ok {
		e.recordAudit(AuditRecord{
			User:       user,
			Event:      event,
			Object:     obj,
			Detail:     sql,
			Success:    err == nil,
			Err:        errString(err),
			DurationMs: ms,
			Slow:       e.auditThreshold > 0 && ms >= e.auditThreshold,
		})
	}
	return res, err
}

// rowsOf 提取结果行数：SELECT 返回行数；DML 影响行数；无结果返回 0。
func rowsOf(res *Result) int64 {
	if res == nil {
		return 0
	}
	if res.AffectedRows > 0 {
		return int64(res.AffectedRows)
	}
	return int64(len(res.Rows))
}

// errString 错误信息文本（nil 为空串）。
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Execute 执行一条解析后的语句；SELECT 等语句统计耗时并按阈值记录慢查询。
// 以超级用户（root）身份执行，向后兼容未接入鉴权的调用方。
func (e *Executor) Execute(stmt Stmt) (*Result, error) {
	return e.ExecuteTextAs(stmt, fmt.Sprintf("%T", stmt), SuperUser)
}

// ExecuteText 同 Execute，但慢查询记录使用原始 SQL 文本（Engine 层传入原文）。
// 以超级用户（root）身份执行，向后兼容未接入鉴权的调用方。
func (e *Executor) ExecuteText(stmt Stmt, sqlText string) (*Result, error) {
	return e.ExecuteTextAs(stmt, sqlText, SuperUser)
}

// ExecuteTextAs 同 ExecuteText，但以指定用户身份执行：执行前经权限校验。
// 非 root 用户执行管理语句或无表级权限的 DML 时返回权限拒绝错误；
// 降级 guest（user 为空串）同样受控（见 authz.go）。
//
// 使用全局会话（globalSess）执行：与旧版行为完全一致，适用于 CLI、
// 既有测试与单会话调用方。服务端多连接场景请使用 ExecuteTextAsSession。
func (e *Executor) ExecuteTextAs(stmt Stmt, sqlText, user string) (*Result, error) {
	return e.ExecuteTextAsSession(stmt, sqlText, user, e.globalSess)
}

// ExecuteTextAsSession 同 ExecuteTextAs，但以独立会话执行：显式事务
// 状态（BEGIN/COMMIT/ROLLBACK）与语句快照属于指定会话，多连接可并行
// 执行互不干扰。会话对象由 Executor.NewSession 创建。
func (e *Executor) ExecuteTextAsSession(stmt Stmt, sqlText, user string, sess *Session) (*Result, error) {
	// D6.2：replica 只读强制——写语句（isWriteStmt 白名单）在 replica
	// 模式下一律拒绝，只读查询/事务控制/配置语句放行（仅 replica 模式
	// 装配，主端 start 默认关闭零回归）。批量并行读路径与所有直调入口
	// 最终均经本方法，单点拦截全覆盖。
	if e.isReplicaRO() && isWriteStmt(stmt) {
		return nil, fmt.Errorf("read-only replica: write statement rejected")
	}
	if err := e.checkPrivilege(stmt, user); err != nil {
		return nil, err
	}
	return e.execTimed(sess, user, sqlText, stmt, func() (*Result, error) {
		// M9 查询缓存：仅 autocommit 下的 SELECT 参与缓存（显式事务内
		// 依赖事务快照，结果不缓存也不查缓存，保持快照隔离语义）。
		if ss, ok := stmt.(*SelectStmt); ok && e.qc.Enabled() && e.autocommit(sess) {
			key := NormalizeSQL(sqlText)
			if res, hit := e.qc.Get(key); hit {
				return res, nil
			}
			res, err := e.execStmt(sess, ss)
			if err == nil {
				e.qc.Put(key, res)
			}
			return res, err
		}
		res, err := e.execStmt(sess, stmt)
		if err == nil && isWriteStmt(stmt) {
			e.qc.Invalidate()
		}
		return res, err
	})
}

// SetReplicaReadOnly 设置/关闭 D6 备库只读强制：开启后所有经
// ExecuteTextAsSession 的写语句（isWriteStmt 白名单）被拒绝。仅 replica
// 模式（cmdReplica）装配，主端 start 默认 false 零回归；提升为主节点
// 时置 false 可热切换（执行路径每次语句经 RWMutex 读锁检查）。
func (e *Executor) SetReplicaReadOnly(ro bool) {
	e.roMu.Lock()
	e.replicaRO = ro
	e.roMu.Unlock()
}

// IsReplicaReadOnly 返回 D6 备库只读强制是否开启（状态观测/汇报用）。
func (e *Executor) IsReplicaReadOnly() bool {
	e.roMu.RLock()
	defer e.roMu.RUnlock()
	return e.replicaRO
}

// isReplicaRO 执行路径只读强制检查（RWMutex 读锁，成本可忽略）。
func (e *Executor) isReplicaRO() bool {
	e.roMu.RLock()
	defer e.roMu.RUnlock()
	return e.replicaRO
}

// isWriteStmt 判断语句是否修改存储可见状态：DML 与 DDL（含备份恢复、
// region 运维写操作）成功后需失效查询缓存；只读查询/事务控制语句不失效。
func isWriteStmt(stmt Stmt) bool {
	switch stmt.(type) {
	case *CreateTableStmt, *DropTableStmt, *CreateIndexStmt, *DropIndexStmt,
		*AlterTableStmt, *InsertStmt, *UpdateStmt, *DeleteStmt, *ImportStmt,
		*AssignRegionStmt, *SplitRegionStmt, *BalanceStmt, *BackupStmt, *RestoreStmt,
		*GrantStmt, *RevokeStmt, *CreateRoleStmt, *DropRoleStmt:
		return true
	}
	return false
}

// execStmt 语句分发（不计时/不记录慢查询，供 ExecuteText 包装）。
func (e *Executor) execStmt(sess *Session, stmt Stmt) (*Result, error) {
	// C6 顶层 SELECT 标记：仅 execStmt 分发的 SelectStmt 为顶层只读语句，
	// 允许进入共享快照窗口；内部触发的 SELECT（子查询/物化视图重算/
	// 过程触发器等）不经此处，保持 false，禁止开窗/复用窗口。
	_, isTopSelect := stmt.(*SelectStmt)
	sess.mu.Lock()
	sess.stmtTopLevel = isTopSelect
	sess.mu.Unlock()
	// M8：每条语句独立快照（显式事务内不重置，复用 BEGIN 快照）。
	// v1.16.0 C6：可共享 autocommit 只读 SELECT 保持共享窗口快照；
	// 其余语句关闭窗口恢复独立快照。
	e.resetStmtSnapshot(sess, stmt)
	switch s := stmt.(type) {
	case *CreateTableStmt:
		return e.execCreateTable(sess, s)
	case *DropTableStmt:
		return e.execDropTable(sess, s)
	case *CreateIndexStmt:
		return e.execCreateIndex(sess, s)
	case *DropIndexStmt:
		return e.execDropIndex(sess, s)
	case *AlterTableStmt:
		return e.execAlterTable(sess, s)
	case *InsertStmt:
		return e.execInsert(sess, s)
	case *SelectStmt:
		return e.execSelect(sess, s)
	case *UpdateStmt:
		return e.execUpdate(sess, s)
	case *DeleteStmt:
		return e.execDelete(sess, s)
	case *BeginStmt:
		if err := e.beginTx(sess); err != nil {
			return nil, err
		}
		return &Result{}, nil
	case *CommitStmt:
		if err := e.commitTx(sess); err != nil {
			return nil, err
		}
		return &Result{}, nil
	case *RollbackStmt:
		if err := e.rollbackTx(sess); err != nil {
			return nil, err
		}
		return &Result{}, nil
	case *ExportStmt:
		return e.execExport(sess, s)
	case *ImportStmt:
		return e.execImport(sess, s)
	case *PrepareStmt:
		return e.execPrepare(sess, s)
	case *ExecuteStmt:
		return e.execExecute(sess, s)
	case *DeallocateStmt:
		return e.execDeallocate(sess, s)
	case *CreateViewStmt:
		return e.execCreateView(sess, s)
	case *DropViewStmt:
		return e.execDropView(sess, s)
	case *ShowViewsStmt:
		return e.execShowViews(sess)
	case *CreateMatViewStmt:
		return e.execCreateMatView(sess, s)
	case *RefreshMatViewStmt:
		return e.execRefreshMatView(sess, s)
	case *DropMatViewStmt:
		return e.execDropMatView(sess, s)
	case *CreateTriggerStmt:
		return e.execCreateTrigger(sess, s)
	case *DropTriggerStmt:
		return e.execDropTrigger(sess, s)
	case *ShowTriggersStmt:
		return e.execShowTriggers(sess, s)
	case *CreateProcedureStmt:
		return e.execCreateProcedure(sess, s)
	case *DropProcedureStmt:
		return e.execDropProcedure(sess, s)
	case *ShowProceduresStmt:
		return e.execShowProcedures(sess, s)
	case *CallStmt:
		return e.execCall(sess, s)
	case *CreateUserStmt:
		return e.execCreateUser(sess, s)
	case *DropUserStmt:
		return e.execDropUser(sess, s)
	case *AlterUserStmt:
		return e.execAlterUser(sess, s)
	case *ShowUsersStmt:
		return e.execShowUsers(sess)
	case *ShowTablesStmt:
		return e.execShowTables(sess)
	case *ShowDatabasesStmt:
		return e.execShowDatabases(sess)
	case *CreateDatabaseStmt:
		return e.execCreateDatabase(sess, s)
	case *UseStmt:
		return e.execUse(sess, s)
	case *DropDatabaseStmt:
		return e.execDropDatabase(sess, s)
	case *ShowIndexStmt:
		return e.execShowIndex(sess, s)
	case *ShowCreateStmt:
		return e.execShowCreate(sess, s)
	case *ShowSlowQueriesStmt:
		return e.execShowSlowQueries(sess, s)
	case *ExplainStmt:
		return e.execExplain(sess, s)
	case *AddNodeStmt:
		return e.execAddNode(sess, s)
	case *ShowNodesStmt:
		return e.execShowNodes(sess)
	case *ShowRegionRoutesStmt:
		return e.execShowRegionRoutes(sess)
	case *ShowStatsStmt:
		return e.execShowStats(sess)
	case *ShowConfigStmt:
		return e.execShowConfig(sess, s)
	case *ShowBackupStatusStmt:
		return e.execShowBackupStatus(sess, s)
	case *ShowAlertsStmt:
		return e.execShowAlerts(sess, s)
	case *ShowAlertStatusStmt:
		return e.execShowAlertStatus(sess, s)
	case *ShowAlertRulesStmt:
		return e.execShowAlertRules(sess, s)
	case *ShowBinlogEventsStmt:
		return e.execShowBinlogEvents(sess, s)
	case *ShowMasterStatusStmt:
		return e.execShowMasterStatus(sess)
	case *SetStmt:
		return e.execSet(sess, s)
	case *AssignRegionStmt:
		return e.execAssignRegion(sess, s)
	case *SplitRegionStmt:
		return e.execSplitRegion(sess, s)
	case *BalanceStmt:
		return e.execBalance(sess, s)
	case *BackupStmt:
		return e.execBackup(sess, s)
	case *RestoreStmt:
		return e.execRestore(sess, s)
	case *GrantStmt:
		return e.execGrantDispatch(sess, s)
	case *RevokeStmt:
		return e.execRevokeDispatch(sess, s)
	case *CreateRoleStmt:
		return e.execCreateRole(sess, s)
	case *DropRoleStmt:
		return e.execDropRole(sess, s)
	case *ShowRolesStmt:
		return e.execShowRoles(sess, s)
	case *ShowGrantsStmt:
		return e.execShowGrants(sess, s)
	}
	return nil, ErrUnsupported
}

// SetBackupEnv 注入备份/恢复环境（db.Open 装配）。
// st 为引擎级 Storage 引用；binlogPath 为数据目录 binlog 文件路径（可空字符串表示未启用）。
func (e *Executor) SetBackupEnv(st storage.Storage, binlogPath string) {
	e.st = st
	e.binlogPath = binlogPath
}

// ---- 会话级显式事务 ----

// takeRowLocks 获取写路径行锁/表锁并返回语句级解锁函数与"是否实际获取
// 新锁"标记（v1.17.0 D1.1：acquired=false 表示 keys 全部已持锁——显式
// 事务内重复写同一行/同表，未新获锁，调用方据此跳过快照刷新）。
//   - autocommit：锁随语句结束释放（defer 生效，2PC 提交已完成），
//     acquired 恒为 true（每语句独立新锁）；
//   - 显式事务：锁挂到会话由 COMMIT/ROLLBACK 统一释放（releaseHeldLocks），
//     覆盖"读旧值→提交"全临界区，防止后事务在前事务未提交时读旧值覆盖写
//     （并发显式事务丢更新）；同 key 防重入，事务内多次写同一行不自锁，
//     此时 acquired=false。
func (e *Executor) takeRowLocks(sess *Session, keys ...string) (func(), bool) {
	if !sess.inTx {
		return e.lockRowKeys(keys...), true
	}
	sess.mu.Lock()
	if sess.heldLockKeys == nil {
		sess.heldLockKeys = make(map[string]bool)
	}
	need := make([]string, 0, len(keys))
	for _, k := range keys {
		if !sess.heldLockKeys[k] {
			sess.heldLockKeys[k] = true
			need = append(need, k)
		}
	}
	sess.mu.Unlock()
	if len(need) == 0 {
		return func() {}, false
	}
	unlock := e.lockRowKeys(need...)
	sess.mu.Lock()
	sess.heldLocks = append(sess.heldLocks, unlock)
	sess.mu.Unlock()
	return func() {}, true // 显式事务：语句结束不释放，事务结束时统一释放
}

// releaseHeldLocks 释放显式事务持有的写锁（事务提交/回滚完成后调用，
// 保证等待者拿锁后能看到已提交数据）。
func (e *Executor) releaseHeldLocks(sess *Session) {
	sess.mu.Lock()
	locks := sess.heldLocks
	sess.heldLocks = nil
	sess.heldLockKeys = nil
	sess.mu.Unlock()
	for _, u := range locks {
		u()
	}
}

// refreshWriteSnapshot 锁内"当前读"快照刷新：
//   - 刷新引擎快照（RefreshSnapshot，读到最新已提交视图）；
//   - 显式事务同时把事务快照号推进到权威序列当前号（Reset+Get），
//     保证 VersionVisible 不再把前序已提交版本判为不可见。
//
// 后拿锁事务由此读到前序事务提交的值，读改写不丢更新。
func (e *Executor) refreshWriteSnapshot(sess *Session, tx txn.Txn) error {
	if r, ok := tx.(interface{ RefreshSnapshot() error }); ok {
		if err := r.RefreshSnapshot(); err != nil {
			return err
		}
	}
	if !sess.inTx || e.ts == nil {
		return nil
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	e.ts.Reset()
	ts, err := e.ts.Get()
	if err != nil {
		return err
	}
	sess.curBeginTS = ts
	return nil
}

// beginTx 开启会话事务（已开启时报错）。
func (e *Executor) beginTx(sess *Session) error {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.inTx {
		return &SQLError{Msg: "transaction already started"}
	}
	// M8 分布式快照隔离：BEGIN 时向 TSO 取全局 begin_ts 作为事务快照。
	// 未装配 TSO（单机旧路径）时 begin_ts=0，行为与 M7 完全一致。
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	sess.curTx = tx
	sess.inTx = true
	sess.curTxID = e.newTxID()
	// M18：事务快照号延迟到首条读语句（锁内/执行路径）时取。
	// BEGIN 阶段不再取号：隐式/显式事务并发时，取号顺序与行锁获取顺序
	// 可能错位，导致后拿锁事务拿到更小 beginTS、读不到前序提交而覆盖写（丢更新）。
	sess.curBeginTS = 0
	// P0-1：防御清空上次事务遗留的 2PC 暂存（正常 COMMIT/ROLLBACK 已清空；
	// 此处兜底异常残留，避免跨事务串扰）。
	sess.pendingLocal = nil
	sess.pendingRemote = map[string][]kvOp{}
	// P2-13：事务隔离级别：默认会话级（SET SESSION TRANSACTION
	// ISOLATION LEVEL）；语句级设置（SET TRANSACTION ISOLATION LEVEL）
	// 仅影响下一事务，BEGIN 时消费并复位。
	iso := sess.isolation
	if sess.nextTxIsoSet {
		iso = sess.nextTxIso
		sess.nextTxIsoSet = false
	}
	sess.txIso = iso
	if iso == IsoSerializable {
		if st, ok := tx.(interface {
			EnableSerializable()
			ReadSet() map[string]bool
			WriteSet() map[string]bool
		}); ok {
			st.EnableSerializable()
			e.serial.register(tx.ID(), st.ReadSet(), st.WriteSet())
		}
	}
	return nil
}

// commitTx 提交会话事务（未开启时报错）。
func (e *Executor) commitTx(sess *Session) error {
	sess.mu.Lock()
	if !sess.inTx {
		sess.mu.Unlock()
		return &SQLError{Msg: "no active transaction"}
	}
	tx := sess.curTx
	// P0-1：锁内取出事务快照号与远端 2PC 暂存标记（后续 execExplicitCommit2PC
	// 在会话复位后执行，不能依赖会话字段）。
	beginTS := sess.curBeginTS
	hasRemote := len(sess.pendingRemote) > 0
	// P2-13：锁内取出 Serializable 检查所需信息（事务 ID / 隔离级别），
	// 提交后会话复位（txIso 复位为默认，事务结束即失效）。
	txID := tx.ID()
	txIso := sess.txIso
	sess.txIso = IsoSnapshot
	sess.inTx = false
	sess.curTx = nil
	sess.curBeginTS = 0
	sess.curTxID = ""
	sess.mu.Unlock()
	if e.ts != nil {
		// 事务结束后刷新 TSO 缓存：后续新事务/新语句取权威当前序列
		// 的新鲜号，避免读到残余预取批次中的滞后时间戳。
		e.ts.Reset()
	}
	// P2-13：Serializable 提交前冲突检测（storage.ErrConflict 语义）。
	// 冲突视为提交失败：回滚事务、释放行锁并返回错误（事务未生效）。
	if txIso == IsoSerializable {
		if st, ok := tx.(interface {
			ReadSet() map[string]bool
			WriteSet() map[string]bool
		}); ok {
			extra := e.serialExtraWriteKeys(sess)
			extraRead := e.serialRemoteReadKeys(sess)
			if err := e.serial.checkAndCommit(txID, st.ReadSet(), st.WriteSet(), extra, extraRead); err != nil {
				_ = tx.Rollback()
				e.releaseHeldLocks(sess)
				return err
			}
		}
	}
	var err error
	if hasRemote {
		// P0-1：显式事务跨 region 写统一走 2PC 提交（延迟 PREPARE→COMMIT，
		// 协调者记录/版本记录/后台重放与 autocommit 2PC 同一状态机）。
		err = e.execExplicitCommit2PC(sess, tx, beginTS)
	} else {
		err = tx.Commit()
	}
	// 事务已关闭（无论成败），清空累积的 2PC 暂存，避免残留跨事务串扰。
	e.clearPendingExplicit(sess)
	if err != nil {
		e.releaseHeldLocks(sess)
		return err
	}
	// 提交完成后释放写锁：等待者拿锁后读到已提交数据，读改写不丢更新。
	e.releaseHeldLocks(sess)
	if e.m != nil {
		e.m.IncCommit()
	}
	return nil
}

// serialExtraWriteKeys 收集 2PC 远端暂存写的 key（P2-13）：远端写的行锁
// 由目标节点持有，协调者侧 registry 无法感知，需显式纳入已提交写集，
// 保证后续 Serializable 事务读到这些 key 的旧值时能触发冲突。
func (e *Executor) serialExtraWriteKeys(sess *Session) map[string]bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	extra := map[string]bool{}
	for _, ops := range sess.pendingRemote {
		for _, op := range ops {
			extra[string(op.Key)] = true
		}
	}
	return extra
}

// serialRemoteReadKeys 取走 Serializable 分布式远端读集（提交检测用，
// 取走后清零避免跨事务串扰）。
func (e *Executor) serialRemoteReadKeys(sess *Session) map[string]bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	extra := sess.serialRemoteReads
	sess.serialRemoteReads = nil
	return extra
}

// rollbackTx 回滚会话事务（未开启时报错）。
func (e *Executor) rollbackTx(sess *Session) error {
	sess.mu.Lock()
	if !sess.inTx {
		sess.mu.Unlock()
		return &SQLError{Msg: "no active transaction"}
	}
	tx := sess.curTx
	// P2-13：事务结束（回滚）复位隔离级别并注销 Serializable 注册。
	sess.txIso = IsoSnapshot
	sess.inTx = false
	sess.curTx = nil
	sess.curBeginTS = 0
	sess.curTxID = ""
	// P0-1：远端写仅暂存未 PREPARE（PREPARE 发生在 COMMIT 时），
	// 回滚直接丢弃，无需向参与者发 ABORT；本地写随 tx.Rollback 丢弃。
	sess.pendingLocal = nil
	sess.pendingRemote = map[string][]kvOp{}
	sess.serialRemoteReads = nil
	sess.mu.Unlock()
	if e.ts != nil {
		e.ts.Reset()
	}
	e.serial.unregister(tx.ID())
	if err := tx.Rollback(); err != nil {
		e.releaseHeldLocks(sess)
		return err
	}
	// 回滚完成后释放写锁（等待者拿锁后仍读旧值，不破坏读改写正确性）。
	e.releaseHeldLocks(sess)
	if e.m != nil {
		e.m.IncRollback()
	}
	return nil
}

// resetSessionTxState 复位会话事务状态（T42：INSERT INTO ... SELECT 在
// autocommit 下临时提升的显式事务，其事务句柄已由 exec2pcWrite 提交或
// defer Rollback 处理，此处仅清理会话残留，避免 inTx/curTx 泄漏影响后续语句）。
func (e *Executor) resetSessionTxState(sess *Session) {
	sess.mu.Lock()
	// P2-13：异常复位时注销 Serializable 注册（已提交路径已注销）。
	if sess.inTx && sess.curTx != nil {
		e.serial.unregister(sess.curTx.ID())
	}
	sess.txIso = IsoSnapshot
	sess.inTx = false
	sess.curTx = nil
	sess.curBeginTS = 0
	sess.curTxID = ""
	// P0-1：同步清空显式事务 2PC 暂存（异常路径复位时防御）。
	sess.pendingLocal = nil
	sess.pendingRemote = map[string][]kvOp{}
	// P2-13：异常复位清空 Serializable 远端读集累积。
	sess.serialRemoteReads = nil
	// v1.16.0 C6：异常复位一并关闭共享快照窗口（释放窗口事务/快照）。
	sess.closeSharedROTxLocked()
	sess.mu.Unlock()
	// 临时提升事务若持过写锁（如 INSERT...SELECT 场景），一并释放避免泄漏。
	e.releaseHeldLocks(sess)
	if e.ts != nil {
		e.ts.Reset()
	}
}

// SetTSO 注入全局时间戳源（M8：db.Open/StartCluster 装配）。
// nil 表示不启用快照隔离（单机旧路径零回归）。
func (e *Executor) SetTSO(src tso.Source) {
	// e.ts 仅启动装配期调用（db.Open/StartCluster），装配后不变；
	// 读取侧（snapshotTS/tsoGet/backup/2pc）均无锁直读，这里同样不加锁。
	e.ts = src
	// globalSess 字段由会话锁保护（与语句路径一致）。
	e.globalSess.mu.Lock()
	e.globalSess.stmtBeginTS = 0
	// v1.16.0 C6：TSO 重装配时关闭共享快照窗口（旧快照版本与新 TSO
	// 语义不兼容，避免快照/取号不一致）。
	e.globalSess.closeSharedROTxLocked()
	e.globalSess.mu.Unlock()
}

// tsoGet 从装配的时间戳源取号；未装配返回 0（旧路径）。
func (e *Executor) tsoGet() (uint64, error) {
	ts := e.ts // e.ts 装配后不变，无锁直读
	if ts == nil {
		return 0, nil
	}
	return ts.Get()
}

// txContextID 返回当前语句所属会话事务 ID（M18-A 事务上下文）：
// 显式事务内返回 BEGIN 时生成的事务 ID；autocommit 返回空串（不携带）。
func (e *Executor) txContextID(sess *Session) string {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.inTx {
		return sess.curTxID
	}
	return ""
}

// snapshotTS 返回当前读快照版本：
//   - 未装配 TSO：0（不过滤，旧路径零回归）；
//   - 显式事务：BEGIN 时获取的 curBeginTS；
//   - autocommit 语句：首次读时惰性获取并缓存 stmtBeginTS（每条语句独立）。
//
// 取号失败（如远程 TSO 不可达）退回 0：读不过滤、尽力而为，不阻塞语句。
func (e *Executor) snapshotTS(sess *Session) uint64 {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if e.ts == nil {
		return 0
	}
	if sess.inTx {
		// M18：事务首条读语句时取快照号；Reset 丢弃残余预取批次，
		// 保证取到权威序列当前新鲜号（与行锁顺序一致）。
		if sess.curBeginTS == 0 {
			e.ts.Reset()
			ts, err := e.ts.Get()
			if err != nil {
				return 0
			}
			sess.curBeginTS = ts
		}
		return sess.curBeginTS
	}
	if sess.stmtBeginTS == 0 {
		ts, err := e.ts.Get()
		if err != nil {
			return 0
		}
		sess.stmtBeginTS = ts
	}
	return sess.stmtBeginTS
}

// resetStmtSnapshot 在每条语句执行入口重置 autocommit 语句快照，
// 保证每条语句拥有独立快照（显式事务内不重置）。
//
// v1.16.0 C6（snapshot_share）：语句为可共享 autocommit 只读 SELECT 时——
// 共享窗口（sharedROTx）已开且窗口期间无对外可见提交（全局提交序列
// 未变）则保持 stmtBeginTS（复用窗口快照）；窗口已过期（期间其他连接
// 写提交/2PC 参与方提交）或未开则关闭并重置为 0（窗口起点，本语句独立
// 取号后由 execSelect 挂载新窗口）。其余语句（写语句/事务控制/不可共享
// SELECT）关闭共享窗口并重置，恢复逐语句独立快照原语义。
func (e *Executor) resetStmtSnapshot(sess *Session, stmt Stmt) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.inTx {
		return
	}
	if e.snapshotShare && e.stmtROShareableLocked(sess, stmt) {
		if sess.sharedROTx == nil {
			sess.stmtBeginTS = 0 // 窗口起点：本语句独立取号
			return
		}
		// 窗口已开：窗口期间若发生对外可见提交则窗口过期，关闭并重置，
		// 本语句重新独立取号（保持 autocommit 写后读语义）。
		if sess.sharedROSeq != e.commitSeqLocked() {
			sess.closeSharedROTxLocked()
			sess.stmtBeginTS = 0
		}
		return // 窗口未过期：保持 stmtBeginTS（共享快照）
	}
	sess.closeSharedROTxLocked()
	sess.stmtBeginTS = 0
}

// commitSeqLocked 读取全局写提交序列（C6 窗口过期检测）。优先存储级
// （storage.WriteSeqProvider：本地事务提交、2PC 参与方提交、恢复重放等
// 一切经 Write 落盘的可见写入都计数）；存储不支持时回退事务级
// （txn.CommitSeqProvider）；均未实现（测试 mock）返回 0：窗口复用比对
// 恒等，即退化为纯共享窗口（与无对外提交场景等价，mock 只读测试语义
// 一致）。调用方须持 sess.mu（窗口字段与提交序列一致性由会话锁串行化）。
func (e *Executor) commitSeqLocked() uint64 {
	if p, ok := e.st.(storage.WriteSeqProvider); ok {
		return p.WriteSeq()
	}
	if p, ok := e.tm.(txn.CommitSeqProvider); ok {
		return p.CommitSeq()
	}
	return 0
}

// stmtROShareableLocked 判定 autocommit 语句是否可进入 C6 只读共享快照
// 窗口（调用方须持 sess.mu）。判定保守对齐 C3 批判定（roBatchEligible）：
// 仅纯只读 SELECT，排除读取用户变量（@var）、forcedTx 挂载窗口、
// Serializable 隔离、非顶层 SELECT（内部子查询/物化视图重算等）；
// C3 批接口并行段（batchExec）禁止共享，保持批内
// "每条独立临时事务 + 独立快照"语义。
func (e *Executor) stmtROShareableLocked(sess *Session, stmt Stmt) bool {
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		return false
	}
	if !sess.stmtTopLevel {
		return false
	}
	if sess.forcedTx != nil || sess.batchExec {
		return false
	}
	if sess.txIso == IsoSerializable {
		return false
	}
	return !selectHasUserVar(sel)
}

// stmtROShareable 为 stmtROShareableLocked 的加锁版本（execSelect 无锁
// 上下文调用）。
func (e *Executor) stmtROShareable(sess *Session, stmt Stmt) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return e.stmtROShareableLocked(sess, stmt)
}

// enterSharedROWindow 将 autocommit 只读语句挂载/复用共享快照窗口：
// 窗口已开（sharedROTx 非空）直接返回窗口事务（快照共享）；未开则将
// 本语句新建的临时事务挂载为窗口事务（快照由窗口持有，语句结束不释放，
// 由下一条共享语句复用或非共享语句/错误关闭）。挂载时记录当前全局提交
// 序列，供后续语句比对检测窗口期间的外部提交。
func (e *Executor) enterSharedROWindow(sess *Session, tx txn.Txn) (txn.Txn, error) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.sharedROTx != nil {
		if tx != nil && tx != sess.sharedROTx {
			_ = tx.Rollback() // 防御竞态：本语句新建事务未被挂载，释放
		}
		return sess.sharedROTx, nil
	}
	sess.sharedROTx = tx
	sess.sharedROSeq = e.commitSeqLocked()
	return tx, nil
}

// exitSharedROWindow 关闭共享快照窗口并释放窗口事务（RocksDB 快照）。
// 仅在共享语句出错时调用（成功路径保持窗口供后续语句复用）。窗口关闭
// 后 stmtBeginTS 归零，下一条只读语句重新独立取号（错误传播语义不变）。
func (e *Executor) exitSharedROWindow(sess *Session) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.closeSharedROTxLocked()
	sess.stmtBeginTS = 0
}

// isSharedROTx 判定 tx 是否为当前会话共享窗口事务（防御：嵌套不可共享
// 语句触达窗口事务时不得 Rollback——窗口由外层语句管理，只读执行语义
// 一致）。
func (e *Executor) isSharedROTx(sess *Session, tx txn.Txn) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return tx != nil && tx == sess.sharedROTx
}

// getTx 获取当前语句的事务：会话事务内复用 curTx；否则新建临时事务。
// forcedTx 非空时（v1.9.0 物化视图刷新全量重算窗口）优先透传该事务，
// 使内部 SELECT 复用当前刷新事务读取同事务内已写入的依赖 MV 增量。
// v1.16.0 C6：本方法供非 execSelect 调用点（DDL/写语句/ListRegions 等
// 元数据辅助函数）使用，始终返回独立临时事务，不触碰共享快照窗口——
// 窗口复用仅限 execSelect 的 getTxForSelect，保证窗口事务生命周期由
// execSelect 独占管理（辅助函数 auto defer Rollback 不会误结束窗口）。
func (e *Executor) getTx(sess *Session) (txn.Txn, error) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.forcedTx != nil {
		return sess.forcedTx, nil
	}
	if sess.inTx {
		return sess.curTx, nil
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	// v1.17.0 D1.2：autocommit 写 pipeline 批内语句创建临时事务后置事务级
	// defer 标记（会话级标记由服务端 REPLSession 写批收集期置位）。持
	// sess.mu 锁内读取字段；事务在批尾统一 FlushWritePipeline 刷盘。
	if sess.deferWrite {
		tx.SetDeferSync(true)
	}
	return tx, nil
}

// getTxForSelect 获取 SELECT 语句的事务（仅 execSelect 调用）：显式事务/
// forcedTx 语义与 getTx 一致；autocommit 场景优先复用 C6 共享快照窗口
// 事务（窗口已开且未过期：窗口期间无对外可见提交），过期则关闭窗口由
// 本语句新建独立事务（下一条共享语句挂载新窗口），未开则新建临时事务。
func (e *Executor) getTxForSelect(sess *Session) (txn.Txn, error) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.forcedTx != nil {
		return sess.forcedTx, nil
	}
	if sess.inTx {
		return sess.curTx, nil
	}
	if sess.sharedROTx != nil {
		if sess.sharedROSeq != e.commitSeqLocked() {
			// 窗口过期：窗口期间发生了对外可见提交（其他连接/2PC 参与方），
			// 关闭窗口，本语句新建独立快照（保持写后读语义）。
			sess.closeSharedROTxLocked()
		} else {
			return sess.sharedROTx, nil
		}
	}
	return e.tm.Begin()
}

// autocommit 判断当前语句是否以隐式事务提交（不在显式事务中时为 true）。
// forcedTx 挂载窗口视为事务内（仅透传读取，不自动提交/回滚）。
func (e *Executor) autocommit(sess *Session) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.forcedTx != nil {
		return false
	}
	return !sess.inTx
}

// ---- DDL ----

func (e *Executor) execCreateTable(sess *Session, s *CreateTableStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	// M18-A：information_schema 为只读虚拟目录，禁止建表。
	if isInfoSchemaName(s.Name) {
		return nil, &SQLError{Msg: "cannot create table in information_schema"}
	}
	// T39：表名支持 db.tbl 库前缀，解析归属库并校验库存在；s.Name 归一为表名。
	var db, tbl string
	{
		var err error
		db, tbl, err = e.resolveTableDB(sess, tx, s.Name)
		if err != nil {
			return nil, err
		}
	}
	s.Name = tbl
	if findTable(tabs, s.Name) != nil {
		// T41：CREATE TABLE IF NOT EXISTS——表已存在时静默跳过
		if s.IfNotExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "table already exists: " + s.Name}
	}
	// T41：CTAS——CREATE TABLE <name> AS SELECT ...（原子建表+插数）
	if s.Select != nil {
		return e.execCreateTableAsSelect(sess, s, tabs)
	}
	if s.PK == "" && len(s.Columns) > 0 {
		s.PK = s.Columns[0].Name
	}
	meta := &TableMeta{ID: nextTableID(tabs), Name: s.Name, DB: db, Columns: s.Columns, PK: s.PK, PKCols: s.PKCols, ForeignKeys: s.ForeignKeys, Indexes: s.Indexes}
	// M9 外键：建表时校验引用关系（父表存在/列存在/引用主键/类型兼容）
	if err := validateForeignKeys(tabs, meta); err != nil {
		return nil, err
	}
	tabs = append(tabs, meta)
	raw, err := saveTables(tabs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		e.broadcastDDL(cluster.DDLOpCreate, meta, 0, "")
	}
	return &Result{AffectedRows: 0}, nil
}

// execCreateTableAsSelect 执行 CREATE TABLE <name> AS SELECT（T41 的 CTAS）。
// 流程：事务内执行 SELECT 取快照 → 由结果列名与值推断列类型（复用物化视图
// 推断：INT/DECIMAL/DATE/BLOB/TEXT，空结果集全 TEXT）→ 注册新表（无显式主键
// 时按项目惯例默认首列为 PK）→ 逐行编码写入（行键路由与普通 INSERT 一致）→
// 同一事务原子提交：建表与插数要么全部生效要么全部回滚。
func (e *Executor) execCreateTableAsSelect(sess *Session, s *CreateTableStmt, tabs []*TableMeta) (*Result, error) {
	if s.Select == nil {
		return nil, &SQLError{Msg: "CREATE TABLE AS requires SELECT"}
	}
	needTx := !sess.inTx
	if needTx {
		if err := e.beginTx(sess); err != nil {
			return nil, err
		}
		defer func() {
			if sess.inTx {
				_ = e.rollbackTx(sess)
			}
		}()
	}
	tx := sess.curTx
	db, _, err := e.resolveTableDB(sess, tx, s.Name)
	if err != nil {
		return nil, err
	}
	res, err := e.execSelect(sess, s.Select)
	if err != nil {
		return nil, err
	}
	cols, err := matViewColumnNames(res.Columns)
	if err != nil {
		return nil, err
	}
	defs, normRows, err := inferMatViewSchema(cols, res.Rows)
	if err != nil {
		return nil, err
	}
	meta := &TableMeta{
		ID:      nextTableID(tabs),
		Name:    s.Name,
		DB:      db,
		Columns: defs,
	}
	if len(defs) > 0 {
		meta.PK = defs[0].Name // 未指定主键：按项目惯例默认首列
	}
	tabs = append(tabs, meta)
	raw, err := saveTables(tabs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	n, err := e.writeCtasRows(tx, meta, normRows)
	if err != nil {
		return nil, err
	}
	if needTx {
		if err := e.commitTx(sess); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: int(n)}, nil
}

// writeCtasRows 把 CTAS 结果行写入新表：主键为默认首列（项目惯例），
// 行键路由与普通 INSERT 一致（locateRow + rowKeyAt + encodeRow）。
func (e *Executor) writeCtasRows(tx txn.Txn, meta *TableMeta, rows [][]Value) (int, error) {
	for _, row := range rows {
		if len(row) != len(meta.Columns) {
			return 0, &SQLError{Msg: "corrupted CTAS result"}
		}
		raw, err := encodeRow(meta, row)
		if err != nil {
			return 0, err
		}
		pk, err := pkBytes(meta, row[0])
		if err != nil {
			return 0, err
		}
		rid, err := e.locateRow(meta, pk)
		if err != nil {
			return 0, err
		}
		key := e.rowKeyAt(meta, rid, pk)
		if err := tx.Put(key, raw); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}

func (e *Executor) execDropTable(sess *Session, s *DropTableStmt) (*Result, error) {
	// M18-C：information_schema 为只读虚拟目录，禁止 DROP 虚拟表。
	if isInfoSchemaName(s.Name) {
		return nil, &SQLError{Msg: "cannot drop table in information_schema"}
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	// T39：DROP TABLE 表名支持 db.tbl 库前缀；s.Name 归一为表名。
	{
		var tbl string
		var err error
		_, tbl, err = e.resolveTableDB(sess, tx, s.Name)
		if err != nil {
			return nil, err
		}
		s.Name = tbl
	}
	meta := findTable(tabs, s.Name)
	if meta == nil {
		if s.IfExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "table not exists: " + s.Name}
	}
	// M9 外键：DROP 表时外键联动清理——被删表自身的外键定义随目录移除自然消失；
	// 若被删表作为父表被其它表引用，则联动移除这些子表上指向它的外键定义，
	// 避免元数据悬空引用（子表数据保留，仅解除约束关系）。
	for _, t := range tabs {
		if t.Name == s.Name {
			continue
		}
		var kept []ForeignKey
		for _, fk := range t.ForeignKeys {
			if fk.RefTable == s.Name {
				continue
			}
			kept = append(kept, fk)
		}
		t.ForeignKeys = kept
	}
	// 删除表内所有行与索引键（跨 region 展开）
	pairs, err := e.scanMerged(tx, e.rowRanges(meta))
	if err != nil {
		return nil, err
	}
	for _, kv := range pairs {
		if err := tx.Delete(kv.Key); err != nil {
			return nil, err
		}
	}
	if len(meta.Indexes) > 0 {
		var ipairs []storage.KVPair
		for i := range meta.Indexes {
			ps, err := e.scanMerged(tx, e.indexTableRanges(meta, meta.Indexes[i].ID))
			if err != nil {
				return nil, err
			}
			ipairs = append(ipairs, ps...)
		}
		for _, kv := range ipairs {
			if err := tx.Delete(kv.Key); err != nil {
				return nil, err
			}
		}
	}
	// 从目录移除
	var kept []*TableMeta
	for _, t := range tabs {
		if t.Name != s.Name {
			kept = append(kept, t)
		}
	}
	raw, err := saveTables(kept)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	// 清理分片元数据（DROP TABLE 移除该表全部 region 记录）
	e.router.RemoveTable(storage.ID(meta.ID))
	rraw, err := saveRegionMeta(e)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaRegionsKey, rraw); err != nil {
		return nil, err
	}
	// M17 联动：DROP TABLE 一并清理所有用户对该表的表级权限（m:grants）、
	// 列级权限（m:colgrants）与角色内表授权（m:roles）。
	grants, err := loadGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	removeTableGrants(grants, s.Name)
	graw, err := saveGrants(grants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(grantsMetaKey, graw); err != nil {
		return nil, err
	}
	colGrants, err := loadColGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	roles, err := loadRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	removeTablePrivileges(colGrants, roles, s.Name)
	craw, err := saveColGrants(colGrants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(colGrantsMetaKey, craw); err != nil {
		return nil, err
	}
	rraw2, err := saveRoles(roles)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(rolesMetaKey, rraw2); err != nil {
		return nil, err
	}
	// v1.9.0：表 DROP 属结构变更，无法用变更键集合表示，标记依赖该表的
	// 物化视图全部失效（下次 REFRESH 走全量路径）；与 DROP 同事务原子落盘。
	if err := e.markMVDirtyOverflow(tx, s.Name); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		e.broadcastDDL(cluster.DDLOpDrop, meta, meta.ID, meta.Name)
	}
	return &Result{AffectedRows: len(pairs)}, nil
}

// ---- M11：用户管理 ----

// execCreateUser CREATE USER '<name>' IDENTIFIED BY '<pwd>'。
// 密码以 SHA-256+salt 哈希落库（user.go），绝不保存明文；重复用户报错。
func (e *Executor) execCreateUser(sess *Session, s *CreateUserStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findUser(users, s.Name) != nil {
		// T41：CREATE USER IF NOT EXISTS——用户已存在时静默跳过
		if s.IfNotExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "user already exists: " + s.Name}
	}
	saltHash, err := hashPassword(s.Password)
	if err != nil {
		return nil, err
	}
	users = append(users, &UserMeta{Name: s.Name, SaltHash: saltHash})
	raw, err := saveUsers(users)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(usersMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execDropUser DROP USER [IF EXISTS] '<name>'。
// 用户不存在时：IF EXISTS 静默成功，否则报错。
func (e *Executor) execDropUser(sess *Session, s *DropUserStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findUser(users, s.Name) == nil {
		if s.IfExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "user not exists: " + s.Name}
	}
	var kept []*UserMeta
	for _, u := range users {
		if u.Name != s.Name {
			kept = append(kept, u)
		}
	}
	raw, err := saveUsers(kept)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(usersMetaKey, raw); err != nil {
		return nil, err
	}
	// M17 联动：DROP USER 一并清理该用户的全部权限记录
	// （表级 m:grants / 库级 m:dbgrants / 列级 m:colgrants / 角色映射 m:userroles）。
	grants, err := loadGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	dbGrants, err := loadDbGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	colGrants, err := loadColGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	userRoles, err := loadUserRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	removeUserPrivileges(grants, dbGrants, colGrants, userRoles, s.Name)
	graw, err := saveGrants(grants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(grantsMetaKey, graw); err != nil {
		return nil, err
	}
	dbraw, err := saveDbGrants(dbGrants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(dbGrantsMetaKey, dbraw); err != nil {
		return nil, err
	}
	craw, err := saveColGrants(colGrants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(colGrantsMetaKey, craw); err != nil {
		return nil, err
	}
	urraw, err := saveUserRoles(userRoles)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(userRolesMetaKey, urraw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execAlterUser ALTER USER '<name>' IDENTIFIED BY '<pwd>'（重置密码）。
func (e *Executor) execAlterUser(sess *Session, s *AlterUserStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	u := findUser(users, s.Name)
	if u == nil {
		return nil, &SQLError{Msg: "user not exists: " + s.Name}
	}
	saltHash, err := hashPassword(s.Password)
	if err != nil {
		return nil, err
	}
	u.SaltHash = saltHash
	raw, err := saveUsers(users)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(usersMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execShowUsers SHOW USERS：列出全部用户（按名称排序）。
func (e *Executor) execShowUsers(sess *Session) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })
	res := &Result{Columns: []string{"user"}}
	for _, u := range users {
		res.Rows = append(res.Rows, []Value{StrVal(u.Name)})
	}
	return res, nil
}

// execCreateIndex 建二级索引：元数据登记 + 对现有数据回填索引键。
func (e *Executor) execCreateIndex(sess *Session, s *CreateIndexStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	if findIndex(meta, s.Name) != nil {
		// T41：CREATE INDEX IF NOT EXISTS——索引已存在时静默跳过
		if s.IfNotExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "index already exists: " + s.Name}
	}
	if colIndex(meta, s.Col) < 0 {
		return nil, &SQLError{Msg: "unknown column: " + s.Col}
	}
	idx := IndexMeta{ID: nextIndexID(meta), Name: s.Name, Col: s.Col, Unique: s.Unique}
	meta.Indexes = append(meta.Indexes, idx)
	// 回填：全表扫描逐行写索引键（meta.Indexes 已含新索引；跨 region 展开）；
	// M10 UPSERT：UNIQUE 索引回填时做磁盘 + 语句内 pending 双重唯一性检查（重复数据建索引报错）
	pairs, err := e.scanMerged(tx, e.rowRanges(meta))
	if err != nil {
		return nil, err
	}
	var pending [][]Value
	for _, kv := range pairs {
		row, err := decodeRow(meta, kv.Value)
		if err != nil {
			return nil, err
		}
		pk := pkSuffixOf(meta, innerOf(kv.Key))
		if err := e.putIndexKeys(sess, tx, meta, row, pk, ridOf(kv.Key), &pending); err != nil {
			return nil, err
		}
		pending = append(pending, row)
	}
	raw, err := saveTables(tabs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: len(pairs)}, nil
}

// execDropIndex 删索引：元数据移除 + 删除全部索引键。
func (e *Executor) execDropIndex(sess *Session, s *DropIndexStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	idx := findIndex(meta, s.Name)
	if idx == nil {
		return nil, &SQLError{Msg: "index not exists: " + s.Name}
	}
	// 仅删该索引区间 [i{tid}{idxID}, i{tid}{idxID+1})（跨 region 展开）
	pairs, err := e.scanMerged(tx, e.indexRanges(meta, idx.ID,
		EncodeIndexKey2(meta.ID, idx.ID, nil, nil),
		EncodeIndexKey2(meta.ID, idx.ID+1, nil, nil)))
	if err != nil {
		return nil, err
	}
	for _, kv := range pairs {
		if err := tx.Delete(kv.Key); err != nil {
			return nil, err
		}
	}
	var kept []IndexMeta
	for _, i := range meta.Indexes {
		if i.ID != idx.ID {
			kept = append(kept, i)
		}
	}
	meta.Indexes = kept
	raw, err := saveTables(tabs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: len(pairs)}, nil
}

// ---- M9 在线 DDL ----

// ddlCommit 保存表目录元数据并把在线 DDL 的数据重写键操作统一提交：
// metaKey 为全局元数据键（不含 region 前缀），直接写入事务缓冲；
// 数据行 ops 在 autocommit 下走 2PC（单机零触发）+ tx.Commit() 落 WAL，
// 显式事务内本地应用由用户 COMMIT 统一落盘。任一环节失败整体回滚，
// 保证崩溃安全。
func (e *Executor) ddlCommit(sess *Session, tx txn.Txn, tabs []*TableMeta, ops []kvOp, auto bool) error {
	raw, err := saveTables(tabs)
	if err != nil {
		return err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return err
	}
	if auto {
		if len(ops) == 0 {
			return tx.Commit()
		}
		if err := e.exec2pcWrite(sess, tx, ops); err != nil {
			return err
		}
	} else {
		// P0-1：显式事务内跨 region 写支持——本地写立即进入会话事务缓冲，
		// 远端写暂存（语句成功后再挂载，COMMIT 统一 2PC 提交）。
		local, remote, err := e.splitExplicitOps(sess, tx, ops)
		if err != nil {
			return err
		}
		e.stagePendingExplicit(sess, local, remote)
	}
	return nil
}

// execAlterTable 在线 DDL：ALTER TABLE 六种动作（ADD/DROP/RENAME COLUMN、
// RENAME TABLE、ADD/DROP INDEX）。整个操作在单个事务内完成：
// 元数据变更 + 数据重写要么全部生效要么全部回滚；Commit 时经 WAL 落盘，
// 崩溃后 WAL 重放保持目录与数据一致（SHOW TABLES / SHOW INDEX 同步可见）。
func (e *Executor) execAlterTable(sess *Session, s *AlterTableStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	// T39：ALTER TABLE 表名支持 db.tbl 库前缀；s.Table 归一为表名。
	{
		var tbl string
		var err error
		_, tbl, err = e.resolveTableDB(sess, tx, s.Table)
		if err != nil {
			return nil, err
		}
		s.Table = tbl
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	// v1.9.0：ALTER 属结构变更，变更键无法表达，标记依赖该表的物化视图
	// 全量失效（与 ALTER 同事务原子落盘；回滚时一并回滚）。
	if err := e.markMVDirtyOverflow(tx, s.Table); err != nil {
		return nil, err
	}
	affected := 0
	switch s.Action {
	case "ADD_COLUMN":
		if colIndex(meta, s.Column.Name) >= 0 {
			return nil, &SQLError{Msg: "column already exists: " + s.Column.Name}
		}
		// 旧列清单（decodeRow 依赖列数，新增列前先固化旧列布局）
		old := *meta
		old.Columns = append([]ColumnDef(nil), meta.Columns...)
		meta.Columns = append(meta.Columns, s.Column)
		pairs, err := e.scanMerged(tx, e.rowRanges(meta))
		if err != nil {
			return nil, err
		}
		var ops []kvOp
		for _, kv := range pairs {
			row, err := decodeRow(&old, kv.Value)
			if err != nil {
				return nil, err
			}
			// 已有行回填默认值（INT=0 / TEXT='' / DATE=1970-01-01 / DECIMAL=0.0000 / BLOB=''）
			row = append(row, zeroValue(s.Column.Type))
			raw, err := encodeRow(meta, row)
			if err != nil {
				return nil, err
			}
			ops = append(ops, kvOp{Key: kv.Key, Value: raw})
		}
		affected = len(pairs)
		if err := e.ddlCommit(sess, tx, tabs, ops, auto); err != nil {
			return nil, err
		}
		if auto {
			e.broadcastDDL(cluster.DDLOpAlter, meta, 0, "")
		}
		return &Result{AffectedRows: affected}, nil
	case "DROP_COLUMN":
		if colIndex(meta, s.Col) < 0 {
			return nil, &SQLError{Msg: "unknown column: " + s.Col}
		}
		if meta.PK == s.Col {
			return nil, &SQLError{Msg: "cannot drop primary key column: " + s.Col}
		}
		old := *meta
		old.Columns = append([]ColumnDef(nil), meta.Columns...)
		old.Indexes = append([]IndexMeta(nil), meta.Indexes...)
		// 从新列布局中移除被删列（行重写后 encodeRow 按新布局编码）
		cols := make([]ColumnDef, 0, len(meta.Columns)-1)
		for _, c := range meta.Columns {
			if c.Name != s.Col {
				cols = append(cols, c)
			}
		}
		meta.Columns = cols
		// 删除列需同步移除引用该列的相关索引（索引键 + 元数据）
		var kept []IndexMeta
		for _, in := range meta.Indexes {
			if in.Col == s.Col {
				pairs, err := e.scanMerged(tx, e.indexRanges(meta, in.ID,
					EncodeIndexKey2(meta.ID, in.ID, nil, nil),
					EncodeIndexKey2(meta.ID, in.ID+1, nil, nil)))
				if err != nil {
					return nil, err
				}
				for _, kv := range pairs {
					if err := tx.Delete(kv.Key); err != nil {
						return nil, err
					}
				}
				affected += len(pairs)
				continue
			}
			kept = append(kept, in)
		}
		meta.Indexes = kept
		idx := colIndex(&old, s.Col)
		pairs, err := e.scanMerged(tx, e.rowRanges(meta))
		if err != nil {
			return nil, err
		}
		var ops []kvOp
		for _, kv := range pairs {
			row, err := decodeRow(&old, kv.Value)
			if err != nil {
				return nil, err
			}
			row = append(row[:idx], row[idx+1:]...)
			raw, err := encodeRow(meta, row)
			if err != nil {
				return nil, err
			}
			ops = append(ops, kvOp{Key: kv.Key, Value: raw})
		}
		affected += len(pairs)
		if err := e.ddlCommit(sess, tx, tabs, ops, auto); err != nil {
			return nil, err
		}
		if auto {
			e.broadcastDDL(cluster.DDLOpAlter, meta, 0, "")
		}
		return &Result{AffectedRows: affected}, nil
	case "RENAME_COLUMN":
		if colIndex(meta, s.Col) < 0 {
			return nil, &SQLError{Msg: "unknown column: " + s.Col}
		}
		if colIndex(meta, s.NewName) >= 0 {
			return nil, &SQLError{Msg: "column already exists: " + s.NewName}
		}
		for i := range meta.Columns {
			if meta.Columns[i].Name == s.Col {
				meta.Columns[i].Name = s.NewName
				break
			}
		}
		if meta.PK == s.Col {
			meta.PK = s.NewName
		}
		for i := range meta.Indexes {
			if meta.Indexes[i].Col == s.Col {
				meta.Indexes[i].Col = s.NewName
			}
		}
		if err := e.ddlCommit(sess, tx, tabs, nil, auto); err != nil {
			return nil, err
		}
		if auto {
			e.broadcastDDL(cluster.DDLOpAlter, meta, 0, "")
		}
		return &Result{AffectedRows: 0}, nil
	case "RENAME_TABLE":
		if findTable(tabs, s.NewName) != nil {
			return nil, &SQLError{Msg: "table already exists: " + s.NewName}
		}
		meta.Name = s.NewName
		if err := e.ddlCommit(sess, tx, tabs, nil, auto); err != nil {
			return nil, err
		}
		if auto {
			e.broadcastDDL(cluster.DDLOpAlter, meta, 0, "")
		}
		return &Result{AffectedRows: 0}, nil
	case "MODIFY_COLUMN":
		idx := colIndex(meta, s.Col)
		if idx < 0 {
			return nil, &SQLError{Msg: "unknown column: " + s.Col}
		}
		// 主键 / 索引列类型变更会破坏行键与索引键编码，拒绝修改（提示先删索引）。
		if meta.PK == s.Col {
			return nil, &SQLError{Msg: "cannot modify primary key column: " + s.Col}
		}
		for _, in := range meta.Indexes {
			if in.Col == s.Col {
				return nil, &SQLError{Msg: "cannot modify indexed column: " + s.Col + " (drop index first)"}
			}
		}
		if meta.Columns[idx].Type == s.NewType && meta.Columns[idx].Scale == s.Column.Scale {
			// 同类型同精度幂等：无行重写
			return &Result{AffectedRows: 0}, nil
		}
		// 旧列布局解码（行字节按旧类型编码），转换后按新布局编码重写。
		old := *meta
		old.Columns = append([]ColumnDef(nil), meta.Columns...)
		old.Indexes = append([]IndexMeta(nil), meta.Indexes...)
		meta.Columns[idx].Type = s.NewType
		// v1.12.0 高精度：DECIMAL 声明 scale 同步更新（同类型改精度走行重写）。
		if s.Column.Scale > 0 {
			meta.Columns[idx].Scale = s.Column.Scale
		}
		pairs, err := e.scanMerged(tx, e.rowRanges(meta))
		if err != nil {
			return nil, err
		}
		var ops []kvOp
		for _, kv := range pairs {
			row, err := decodeRow(&old, kv.Value)
			if err != nil {
				return nil, err
			}
			// 宽松转换，失败回填目标类型零值（如 TEXT→INT 非数字 → 0）
			row[idx] = convertColValue(row[idx], s.NewType, s.Column.Scale)
			raw, err := encodeRow(meta, row)
			if err != nil {
				return nil, err
			}
			ops = append(ops, kvOp{Key: kv.Key, Value: raw})
		}
		affected = len(pairs)
		if err := e.ddlCommit(sess, tx, tabs, ops, auto); err != nil {
			return nil, err
		}
		if auto {
			e.broadcastDDL(cluster.DDLOpAlter, meta, 0, "")
		}
		return &Result{AffectedRows: affected}, nil
	case "ADD_INDEX":
		if findIndex(meta, s.Name) != nil {
			return nil, &SQLError{Msg: "index already exists: " + s.Name}
		}
		if colIndex(meta, s.Col) < 0 {
			return nil, &SQLError{Msg: "unknown column: " + s.Col}
		}
		idx := IndexMeta{ID: nextIndexID(meta), Name: s.Name, Col: s.Col}
		meta.Indexes = append(meta.Indexes, idx)
		// 回填：全表扫描逐行写索引键（跨 region 展开）；
		// M10 UPSERT：UNIQUE 索引回填时做磁盘 + 语句内 pending 双重唯一性检查（重复数据建索引报错）
		pairs, err := e.scanMerged(tx, e.rowRanges(meta))
		if err != nil {
			return nil, err
		}
		var pending [][]Value
		for _, kv := range pairs {
			row, err := decodeRow(meta, kv.Value)
			if err != nil {
				return nil, err
			}
			pk := pkSuffixOf(meta, innerOf(kv.Key))
			if err := e.putIndexKeys(sess, tx, meta, row, pk, ridOf(kv.Key), &pending); err != nil {
				return nil, err
			}
			pending = append(pending, row)
		}
		raw, err := saveTables(tabs)
		if err != nil {
			return nil, err
		}
		if err := tx.Put(metaKey, raw); err != nil {
			return nil, err
		}
		if auto {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			e.broadcastDDL(cluster.DDLOpAlter, meta, 0, "")
		}
		return &Result{AffectedRows: len(pairs)}, nil
	case "DROP_INDEX":
		idx := findIndex(meta, s.Name)
		if idx == nil {
			return nil, &SQLError{Msg: "index not exists: " + s.Name}
		}
		pairs, err := e.scanMerged(tx, e.indexRanges(meta, idx.ID,
			EncodeIndexKey2(meta.ID, idx.ID, nil, nil),
			EncodeIndexKey2(meta.ID, idx.ID+1, nil, nil)))
		if err != nil {
			return nil, err
		}
		for _, kv := range pairs {
			if err := tx.Delete(kv.Key); err != nil {
				return nil, err
			}
		}
		var kept []IndexMeta
		for _, i := range meta.Indexes {
			if i.ID != idx.ID {
				kept = append(kept, i)
			}
		}
		meta.Indexes = kept
		raw, err := saveTables(tabs)
		if err != nil {
			return nil, err
		}
		if err := tx.Put(metaKey, raw); err != nil {
			return nil, err
		}
		if auto {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			e.broadcastDDL(cluster.DDLOpAlter, meta, 0, "")
		}
		return &Result{AffectedRows: len(pairs)}, nil
	}
	return nil, errf("unsupported ALTER action %q", s.Action)
}

// ---- DML ----

func (e *Executor) execInsert(sess *Session, s *InsertStmt) (*Result, error) {
	// P0 HA：写入 fencing 检查（陈旧主 epoch 被拒；开关关闭时放行）。
	if err := e.writeFenceCheck(); err != nil {
		return nil, err
	}
	// P0-1：显式事务跨 region 写暂存（本地/远端分组；语句成功路径末尾挂载）。
	var explLocal []kvOp
	var explRemote map[string][]kvOp
	// M18-C：information_schema 为只读虚拟目录，禁止 INSERT。
	if isInfoSchemaName(s.Table) {
		return nil, &SQLError{Msg: "cannot insert into information_schema"}
	}
	auto := e.autocommit(sess)
	// v1.8.0 同事务原子性：autocommit 下若该表该事件存在触发器，临时提升
	// 会话事务，使触发器体写入（经显式路径 applyOps 入同一 curTx）随主 DML
	// 一并提交或回滚；T42 INSERT...SELECT 的读改写同事务需求一并由该提升
	// 满足。函数返回前无论成败复位会话状态，避免提交/回滚后残留 inTx/curTx
	// 影响后续语句。
	if auto && !sess.inTx && (s.Select != nil || e.anyTriggerOn(sess, s.Table, "INSERT")) {
		if err := e.beginTx(sess); err != nil {
			return nil, err
		}
		defer e.resetSessionTxState(sess)
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	// T39：INSERT 表名支持 db.tbl 库前缀；s.Table 归一为表名。
	{
		var tbl string
		var err error
		_, tbl, err = e.resolveTableDB(sess, tx, s.Table)
		if err != nil {
			return nil, err
		}
		s.Table = tbl
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	affected := 0
	var ops []kvOp
	// v1.8.0 行级触发器：语句前探测存在行级触发器时逐行收集 NEW 行快照，
	// 供 FOR EACH ROW 触发器按行物化；无行级触发器时零开销（nil）。
	var rowSnaps []triggerRow
	if e.anyRowTrigger(sess, s.Table) {
		rowSnaps = make([]triggerRow, 0)
	}
	// v1.9.0 物化视图变更跟踪：表被 MV 依赖时逐行收集受影响行主键，
	// 语句成功后合并进 dirty 日志（同事务原子落盘）；无依赖时零开销。
	var dirtyPks [][]byte
	tracked, terr := e.tableTrackedByMVs(tx, s.Table)
	if terr != nil {
		return nil, terr
	}
	// M10 UPSERT：语句内已计划写入的最终行（含 REPLACE 删旧插新后的新行、
	// ON DUPLICATE 更新后的目标行），用于语句内主键/唯一键冲突判定。
	var pending [][]Value
	if s.Select != nil {
		// T42：先完整执行 SELECT 取快照（目标表与源表相同时先读后写，快照语义），
		// 再逐行灌入目标表——复用 insertRowUpsertOps 与 VALUES 路径完全一致
		// （类型 coerced / 主键与唯一索引冲突 / ON DUPLICATE 更新 / REPLACE 删旧插新 /
		// 索引维护 / 外键检查 / pending 语句内重复判定）。
		res, err := e.execSelect(sess, s.Select)
		if err != nil {
			return nil, err
		}
		for _, row := range res.Rows {
			rops, aff, err := e.insertRowUpsertOps(sess, tx, tabs, meta, s.Columns, row, s.OnDup, s.Replace, &pending)
			if err != nil {
				return nil, err
			}
			if aff > 0 {
				affected += aff
				ops = append(ops, rops...)
				if rowSnaps != nil {
					snap, serr := e.buildRow(meta, s.Columns, row, tx)
					if serr != nil {
						return nil, serr
					}
					rowSnaps = append(rowSnaps, triggerRow{meta: meta, newRow: snap})
				}
				if tracked {
					vals, verr := e.buildRow(meta, s.Columns, row, tx)
					if verr != nil {
						return nil, verr
					}
					pk, perr := e.pkOf(meta, vals)
					if perr != nil {
						return nil, perr
					}
					dirtyPks = append(dirtyPks, pk)
				}
			}
		}
	} else {
		for _, row := range s.Rows {
			rops, aff, err := e.insertRowUpsertOps(sess, tx, tabs, meta, s.Columns, row, s.OnDup, s.Replace, &pending)
			if err != nil {
				return nil, err
			}
			if aff > 0 {
				affected += aff
				ops = append(ops, rops...)
				if rowSnaps != nil {
					snap, serr := e.buildRow(meta, s.Columns, row, tx)
					if serr != nil {
						return nil, serr
					}
					rowSnaps = append(rowSnaps, triggerRow{meta: meta, newRow: snap})
				}
				if tracked {
					vals, verr := e.buildRow(meta, s.Columns, row, tx)
					if verr != nil {
						return nil, verr
					}
					pk, perr := e.pkOf(meta, vals)
					if perr != nil {
						return nil, perr
					}
					dirtyPks = append(dirtyPks, pk)
				}
			}
		}
	}
	// M15-2/v1.8.0：BEFORE 触发器（写入提交前执行；失败则整个 DML 随事务回滚）。
	// 行级触发器按行快照逐行触发；语句级保持一次触发。
	if affected > 0 {
		if err := e.fireTriggers(sess, tx, "BEFORE", s.Table, "INSERT", rowSnaps); err != nil {
			return nil, err
		}
	}
	// v1.8.0：AFTER 触发器移入主事务提交前执行（同事务原子性）——体写入随
	// 主 DML 一并提交/回滚；任一侧失败主 DML 整体回滚（旧语义为体独立事务、
	// 主 DML 失败不回滚体写，已按 v1.8.0 设计变更）。
	if affected > 0 {
		if err := e.fireTriggers(sess, tx, "AFTER", s.Table, "INSERT", rowSnaps); err != nil {
			return nil, err
		}
	}
	// v1.9.0：记录物化视图变更键（同事务原子落盘；触发器等体写入的后续
	// DML 会各自记录自己的依赖表，无需在此展开）。
	if tracked && len(dirtyPks) > 0 {
		if err := e.recordMVDirtyKeys(tx, s.Table, dirtyPks); err != nil {
			return nil, err
		}
	}
	if auto {
		// autocommit：本地 + 远端统一走 2PC 提交（单机零触发）
		if err := e.exec2pcWrite(sess, tx, ops); err != nil {
			// T42：临时提升的会话事务已被 exec2pcWrite 处理（提交或半途失败），
			// 立即复位会话事务状态，避免残留 inTx/curTx 影响后续语句。
			e.resetSessionTxState(sess)
			return nil, err
		}
		// T42：临时提升的会话事务已提交，立即复位会话事务状态，
		// 避免后续 maybeAutoSplit / AFTER 触发器通过 getTx 拿到已提交的 curTx。
		e.resetSessionTxState(sess)
	} else {
		// P0-1：显式事务内跨 region 写支持——本地写立即进入会话事务缓冲，
		// 远端写暂存（语句成功后再挂载，COMMIT 统一 2PC 提交）。
		explLocal, explRemote, err = e.splitExplicitOps(sess, tx, ops)
		if err != nil {
			return nil, err
		}
	}
	// M7 自动分裂：写提交后按配置检查本地 region 数据规模（显式事务内不触发，
	// 分裂为独立事务，避免与用户事务交叠）。
	if err := e.maybeAutoSplit(meta); err != nil {
		return nil, err
	}
	// P0-1：语句成功，挂载显式事务跨 region 写暂存（本地累积供恢复重放与
	// 版本记录，远端按节点累积，COMMIT 统一 2PC；ROLLBACK 直接丢弃）。
	e.stagePendingExplicit(sess, explLocal, explRemote)
	return &Result{AffectedRows: affected}, nil
}

// insertRowOps 单行插入的"物理操作列表"版本：不直接写事务，
// 返回该行所需的全部键操作（行 Put + 索引键 Put），供调用方本地应用
// 或 2PC 分组提交（M5）。dupSkip 语义同 insertRow。
// M9 外键：插入前校验引用完整性（父表行存在且类型匹配）。
func (e *Executor) insertRowOps(sess *Session, tx txn.Txn, tabs []*TableMeta, meta *TableMeta, cols []string, row []Value, dupSkip bool) ([]kvOp, bool, error) {
	vals, err := e.buildRow(meta, cols, row, tx)
	if err != nil {
		return nil, false, err
	}
	if err := e.checkFKOnRow(sess, tx, tabs, meta, vals); err != nil {
		return nil, false, err
	}
	pk, err := e.pkOf(meta, vals)
	if err != nil {
		return nil, false, err
	}
	// 主键唯一性约束（ACID-C）：按 region 路由定位（远端 region 经
	// getKVCluster 转发读唯一性，保证跨节点不重复）
	rid, err := e.locateRow(meta, pk)
	if err != nil {
		return nil, false, err
	}
	key := e.rowKeyAt(meta, rid, pk)
	if _, err := e.getKVCluster(sess, tx, key); err == nil {
		if dupSkip {
			return nil, false, nil
		}
		return nil, false, &SQLError{Msg: "duplicate primary key: " + meta.Name}
	} else if err != storage.ErrNotFound {
		return nil, false, err
	}
	raw, err := encodeRow(meta, vals)
	if err != nil {
		return nil, false, err
	}
	ops := []kvOp{{Key: key, Value: raw}}
	idxOps, err := e.putIndexKeyOps(sess, tx, meta, vals, pk, rid, nil, nil)
	if err != nil {
		return nil, false, err
	}
	ops = append(ops, idxOps...)
	return ops, true, nil
}

// insertRowUpsertOps M10 UPSERT 单行处理：
//   - 普通 INSERT：主键/唯一索引冲突报错；
//   - INSERT ... ON DUPLICATE KEY UPDATE：冲突时更新冲突行（affected=2），无冲突插入（affected=1）；
//   - REPLACE INTO：冲突时删除所有冲突行后插入新行（affected=删除数+1），无冲突插入（affected=1）。
//
// pending 记录本语句已计划的最终行（用于语句内主键/唯一键重复判定）。
func (e *Executor) insertRowUpsertOps(sess *Session, tx txn.Txn, tabs []*TableMeta, meta *TableMeta, cols []string, row []Value, onDup *OnDuplicateUpdate, replace bool, pending *[][]Value) ([]kvOp, int, error) {
	vals, err := e.buildRow(meta, cols, row, tx)
	if err != nil {
		return nil, 0, err
	}
	if err := e.checkFKOnRow(sess, tx, tabs, meta, vals); err != nil {
		return nil, 0, err
	}
	pk, err := e.pkOf(meta, vals)
	if err != nil {
		return nil, 0, err
	}
	rid, err := e.locateRow(meta, pk)
	if err != nil {
		return nil, 0, err
	}
	key := e.rowKeyAt(meta, rid, pk)
	diskExists := false
	if _, err := e.getKVCluster(sess, tx, key); err == nil {
		diskExists = true
	} else if err != storage.ErrNotFound {
		return nil, 0, err
	}
	// 收集全部冲突行（主键 + 唯一索引，磁盘 + pending，去重）
	conflicts, err := e.upsertConflicts(sess, tx, meta, vals, pk, diskExists, *pending)
	if err != nil {
		return nil, 0, err
	}
	if len(conflicts) == 0 {
		// 无冲突：普通插入
		raw, err := encodeRow(meta, vals)
		if err != nil {
			return nil, 0, err
		}
		ops := []kvOp{{Key: key, Value: raw}}
		idxOps, err := e.putIndexKeyOps(sess, tx, meta, vals, pk, rid, pending, nil)
		if err != nil {
			return nil, 0, err
		}
		ops = append(ops, idxOps...)
		*pending = append(*pending, vals)
		return ops, 1, nil
	}
	if onDup != nil {
		// 更新冲突行（主键冲突优先于唯一索引冲突）
		target := conflicts[0]
		for _, cp := range conflicts {
			if bytes.Equal(cp, pk) {
				target = cp
				break
			}
		}
		return e.onDupUpdateOps(sess, tx, tabs, meta, vals, pk, rid, target, diskExists, onDup, pending)
	}
	if replace {
		return e.replaceRowOps(sess, tx, tabs, meta, vals, pk, rid, conflicts, diskExists, pending)
	}
	// 普通 INSERT：报冲突错误
	if diskExists || bytes.Equal(conflicts[0], pk) {
		return nil, 0, &SQLError{Msg: "duplicate primary key: " + meta.Name}
	}
	return nil, 0, &SQLError{Msg: "duplicate unique key on " + meta.Name}
}

// upsertConflicts 收集与待插入行冲突的所有既有行 pk（去重）：
// 磁盘主键冲突、磁盘各唯一索引冲突、pending 主键/唯一冲突。
// 返回按发现顺序；普通 INSERT 报错时主键冲突优先。
func (e *Executor) upsertConflicts(sess *Session, tx txn.Txn, meta *TableMeta, vals []Value, pk []byte, diskExists bool, pending [][]Value) ([][]byte, error) {
	seen := map[string]bool{}
	var out [][]byte
	add := func(cpk []byte) {
		if cpk == nil {
			return
		}
		k := string(cpk)
		if seen[k] {
			return
		}
		seen[k] = true
		out = append(out, cpk)
	}
	if diskExists {
		add(pk)
	}
	// 磁盘唯一索引冲突
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		if !idx.Unique {
			continue
		}
		val, err := indexColValue(meta, idx, vals)
		if err != nil {
			return nil, err
		}
		start := EncodeIndexKey2(meta.ID, idx.ID, val, nil)
		end := EncodeIndexKey2(meta.ID, idx.ID, append(append([]byte{}, val...), 0xff), nil)
		pairs, err := e.scanMergedCluster(sess, tx, e.indexRanges(meta, idx.ID, start, end))
		if err != nil {
			return nil, err
		}
		for _, kv := range pairs {
			epk := indexPKSuffixLen(innerOf(kv.Key), len(val))
			if !bytes.Equal(epk, pk) {
				add(epk)
			}
		}
	}
	// pending 主键冲突
	for _, prow := range pending {
		ppk, err := e.pkOf(meta, prow)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(ppk, pk) {
			add(ppk)
			break
		}
	}
	// pending 唯一索引冲突
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		if !idx.Unique {
			continue
		}
		val, err := indexColValue(meta, idx, vals)
		if err != nil {
			return nil, err
		}
		for _, prow := range pending {
			ppk, err := e.pkOf(meta, prow)
			if err != nil {
				return nil, err
			}
			if bytes.Equal(ppk, pk) {
				continue
			}
			pv, err := indexColValue(meta, idx, prow)
			if err != nil {
				return nil, err
			}
			if bytes.Equal(val, pv) {
				add(ppk)
			}
		}
	}
	return out, nil
}

// onDupUpdateOps ON DUPLICATE KEY UPDATE：更新冲突行（不改变其主键身份）。
// SET 应用在冲突行上（MySQL 语义：待插入值被丢弃，仅更新既有行）；主键列禁止修改。
func (e *Executor) onDupUpdateOps(sess *Session, tx txn.Txn, tabs []*TableMeta, meta *TableMeta, newVals []Value, newPK []byte, newRid storage.ID, targetPK []byte, diskExists bool, onDup *OnDuplicateUpdate, pending *[][]Value) ([]kvOp, int, error) {
	for _, set := range onDup.Sets {
		if set.Col == meta.PK {
			return nil, 0, &SQLError{Msg: "cannot update primary key column in ON DUPLICATE KEY UPDATE: " + set.Col}
		}
	}
	// 定位冲突行（磁盘或 pending）
	target, rid, key, onDisk, err := e.upsertTargetRow(sess, tx, meta, newPK, newRid, targetPK, diskExists, *pending)
	if err != nil {
		return nil, 0, err
	}
	// 应用 SET 到冲突行
	updated := append([]Value{}, target...)
	for _, set := range onDup.Sets {
		idx := colIndex(meta, set.Col)
		if idx < 0 {
			return nil, 0, &SQLError{Msg: "unknown column: " + set.Col}
		}
		var v Value
		if set.Expr != nil {
			v, err = evalSetExpr(meta, newVals, set.Expr)
			if err != nil {
				return nil, 0, err
			}
		} else {
			v = set.Val
		}
		v, err = coerceValueScaled(meta.Columns[idx].Type, set.Col, meta.Columns[idx].Scale, v)
		if err != nil {
			return nil, 0, err
		}
		updated[idx] = v
	}
	if err := e.checkFKOnRow(sess, tx, tabs, meta, updated); err != nil {
		return nil, 0, err
	}
	// P0-3：ON DUPLICATE KEY UPDATE 禁止改主键列（见上方校验），主键不变则
	// 子表引用不受影响，被引用行更新非主键列直接放行，无需外键联动。
	var ops []kvOp
	if onDisk {
		dops, err := e.delIndexKeyOps(meta, target, targetPK, rid)
		if err != nil {
			return nil, 0, err
		}
		ops = append(ops, dops...)
	}
	raw, err := encodeRow(meta, updated)
	if err != nil {
		return nil, 0, err
	}
	ops = append(ops, kvOp{Key: key, Value: raw})
	iops, err := e.putIndexKeyOps(sess, tx, meta, updated, targetPK, rid, pending, nil)
	if err != nil {
		return nil, 0, err
	}
	ops = append(ops, iops...)
	e.replacePending(pending, meta, targetPK, updated)
	return ops, 2, nil
}

// replaceRowOps REPLACE INTO：删除全部冲突行（含外键 CASCADE 联动）后插入新行，
// 删除与插入在同一个提交内原子完成。
func (e *Executor) replaceRowOps(sess *Session, tx txn.Txn, tabs []*TableMeta, meta *TableMeta, newVals []Value, newPK []byte, newRid storage.ID, conflicts [][]byte, diskExists bool, pending *[][]Value) ([]kvOp, int, error) {
	var ops []kvOp
	deleted := 0
	// 待删除 pk 集合：REPLACE 插入时唯一性检查跳过这些行
	ignore := make(map[string]bool, len(conflicts))
	for _, cp := range conflicts {
		ignore[string(cp)] = true
	}
	for _, cp := range conflicts {
		target, rid, key, onDisk, err := e.upsertTargetRow(sess, tx, meta, newPK, newRid, cp, diskExists && bytes.Equal(cp, newPK), *pending)
		if err != nil {
			return nil, 0, err
		}
		if onDisk {
			// 外键联动删除（CASCADE 子行等）
			fops, err := e.collectFKDeleteOps(sess, tx, tabs, meta, target)
			if err != nil {
				return nil, 0, err
			}
			ops = append(ops, fops...)
			dops, err := e.delIndexKeyOps(meta, target, cp, rid)
			if err != nil {
				return nil, 0, err
			}
			ops = append(ops, dops...)
			// 主键冲突时跳过行键 Delete：后续 Put 覆盖同一行键，
			// 而存储层 WriteBatch 先 Put 后 Delete，同 key Delete 会覆盖 Put 导致行丢失。
			if !bytes.Equal(cp, newPK) {
				ops = append(ops, kvOp{Key: key, Delete: true})
			}
		}
		deleted++
		e.dropPending(pending, meta, cp)
	}
	// 插入新行
	raw, err := encodeRow(meta, newVals)
	if err != nil {
		return nil, 0, err
	}
	ops = append(ops, kvOp{Key: e.rowKeyAt(meta, newRid, newPK), Value: raw})
	iops, err := e.putIndexKeyOps(sess, tx, meta, newVals, newPK, newRid, pending, ignore)
	if err != nil {
		return nil, 0, err
	}
	ops = append(ops, iops...)
	e.replacePending(pending, meta, newPK, newVals)
	return ops, deleted + 1, nil
}

// upsertTargetRow 定位冲突行：优先 pending（语句内最终状态），否则磁盘行。
func (e *Executor) upsertTargetRow(sess *Session, tx txn.Txn, meta *TableMeta, newPK []byte, newRid storage.ID, targetPK []byte, diskExists bool, pending [][]Value) ([]Value, storage.ID, []byte, bool, error) {
	if bytes.Equal(targetPK, newPK) {
		if diskExists {
			raw, err := e.getKVCluster(sess, tx, e.rowKeyAt(meta, newRid, newPK))
			if err != nil {
				return nil, 0, nil, false, err
			}
			row, err := decodeRow(meta, raw)
			if err != nil {
				return nil, 0, nil, false, err
			}
			return row, newRid, e.rowKeyAt(meta, newRid, newPK), true, nil
		}
		// pending 主键冲突：取 pending 行（磁盘无此行，本语句将写入）
		row := e.pendingRowByPK(meta, pending, newPK)
		if row == nil {
			return nil, 0, nil, false, &SQLError{Msg: "internal: pending row not found for upsert"}
		}
		return row, newRid, e.rowKeyAt(meta, newRid, newPK), false, nil
	}
	// 唯一索引冲突：目标行为冲突行（pk=targetPK）
	row := e.pendingRowByPK(meta, pending, targetPK)
	if row != nil {
		tr, err := e.locateRow(meta, targetPK)
		if err != nil {
			return nil, 0, nil, false, err
		}
		return row, tr, e.rowKeyAt(meta, tr, targetPK), false, nil
	}
	tr, err := e.locateRow(meta, targetPK)
	if err != nil {
		return nil, 0, nil, false, err
	}
	raw, err := e.getKVCluster(sess, tx, e.rowKeyAt(meta, tr, targetPK))
	if err != nil {
		return nil, 0, nil, false, err
	}
	row, err = decodeRow(meta, raw)
	if err != nil {
		return nil, 0, nil, false, err
	}
	return row, tr, e.rowKeyAt(meta, tr, targetPK), true, nil
}

// pendingRowByPK 在语句内 pending 行中按主键查找。
func (e *Executor) pendingRowByPK(meta *TableMeta, pending [][]Value, pk []byte) []Value {
	for _, row := range pending {
		ppk, err := e.pkOf(meta, row)
		if err == nil && bytes.Equal(ppk, pk) {
			return row
		}
	}
	return nil
}

// replacePending 用 newRow 替换 pending 中同主键行；不存在则追加。
func (e *Executor) replacePending(pending *[][]Value, meta *TableMeta, pk []byte, newRow []Value) {
	for i, row := range *pending {
		ppk, err := e.pkOf(meta, row)
		if err == nil && bytes.Equal(ppk, pk) {
			(*pending)[i] = newRow
			return
		}
	}
	*pending = append(*pending, newRow)
}

// dropPending 从 pending 中移除指定主键的行（该行最终状态不存在）。
func (e *Executor) dropPending(pending *[][]Value, meta *TableMeta, pk []byte) {
	out := (*pending)[:0]
	for _, row := range *pending {
		ppk, err := e.pkOf(meta, row)
		if err == nil && bytes.Equal(ppk, pk) {
			continue
		}
		out = append(out, row)
	}
	*pending = out
}

// insertRow 单行插入（类型强制 + 主键唯一性 + 行写入 + 索引维护）。
// dupSkip=true 时主键已存在返回 (false, nil)（导入语义跳过）；
// dupSkip=false 时主键已存在返回错误（INSERT 唯一性约束）。
func (e *Executor) insertRow(sess *Session, tx txn.Txn, tabs []*TableMeta, meta *TableMeta, cols []string, row []Value, dupSkip bool) (bool, error) {
	ops, ok, err := e.insertRowOps(sess, tx, tabs, meta, cols, row, dupSkip)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if err := applyOps(tx, ops); err != nil {
		return false, err
	}
	return true, nil
}

// resolveWhereVars 将 WHERE 条件值中的 @var 用户变量替换为会话值（M18-C）。
// 变量未设置时报错；CASE 分支等值节点同样处理。
func (e *Executor) resolveWhereVars(sess *Session, w *Where) error {
	if w == nil {
		return nil
	}
	for i := range w.Conds {
		c := &w.Conds[i]
		if c.Val.Kind == "VAR" {
			v, ok := sess.userVars[c.Val.S]
			if !ok {
				return &SQLError{Msg: "user variable not set: " + c.Val.S}
			}
			c.Val = v
		}
		if c.LeftCase != nil {
			for j := range c.LeftCase.Branches {
				b := &c.LeftCase.Branches[j]
				for k := range b.Conds.Conds {
					if b.Conds.Conds[k].Val.Kind == "VAR" {
						v, ok := sess.userVars[b.Conds.Conds[k].Val.S]
						if !ok {
							return &SQLError{Msg: "user variable not set: " + b.Conds.Conds[k].Val.S}
						}
						b.Conds.Conds[k].Val = v
					}
				}
			}
		}
	}
	return nil
}

// serverVersionString 与 pkg/mysqlproto/handshake.go 的 serverVersion 保持一致
// （D7.2 同源约束：sql 包与 mysqlproto 存在依赖环，无法直接引用，改动需两处同步）。
const serverVersionString = "8.0.36-OpenXDB v1.17.0"

// evalScalarFunc 求值无参标量内置函数（D7.2：VERSION()）。
func evalScalarFunc(name string) (Value, error) {
	switch name {
	case "VERSION":
		return Value{Kind: "TEXT", S: serverVersionString}, nil
	}
	return Value{}, errf("unsupported scalar function %q", name)
}

func (e *Executor) execSelect(sess *Session, s *SelectStmt) (res *Result, err error) {
	// 无 FROM 常量投影：SELECT 1 / SELECT 'x' / SELECT 1+2 / SELECT @var（M18-C / T45）。
	// 兼容 mysql CLI 的 SELECT 1、Connector/J 连接探测等无表查询。
	// 注意：FROM 为子查询时 s.From 为空但 s.Subquery 非空，必须走正常执行路径。
	if s.From == "" && s.Subquery == nil {
		cols := make([]string, 0, len(s.Columns))
		row := make([]Value, 0, len(s.Columns))
		for _, sc := range s.Columns {
			name := sc.Name
			if sc.Alias != "" {
				name = sc.Alias
			}
			cols = append(cols, name)
			var v Value
			var err error
			switch {
			case sc.Var != "":
				// M18-C：用户变量取值（未设置报错）。
				var ok bool
				v, ok = sess.userVars[sc.Var]
				if !ok {
					return nil, &SQLError{Msg: "user variable not set: " + sc.Var}
				}
			case sc.Expr != nil:
				if hasColRef(sc.Expr) {
					return nil, &SQLError{Msg: "column reference without FROM: " + sc.Expr.Raw}
				}
				v, err = evalExprForRow(nil, nil, sc.Expr)
				if err != nil {
					return nil, err
				}
			case sc.Case != nil:
				if caseHasColRef(sc.Case) {
					return nil, &SQLError{Msg: "column reference in CASE without FROM"}
				}
				v, err = e.evalCaseRel(sess, nil, nil, nil, sc.Case)
				if err != nil {
					return nil, err
				}
			case sc.Func != "":
				// D7.2：无 FROM 标量函数（VERSION()）直接求值。
				v, err = evalScalarFunc(sc.Func)
				if err != nil {
					return nil, err
				}
			case sc.Agg != "":
				// P1-10：无 FROM 的聚合/窗口函数区分报错（窗口函数必须带 OVER 且需要行上下文）。
				if sc.Window != nil {
					return nil, &SQLError{Msg: "window function without FROM: " + sc.Raw}
				}
				return nil, &SQLError{Msg: "aggregate function without FROM: " + sc.Raw}
			case sc.Ref != "" || sc.Col != "":
				// P1-10：无 FROM 的纯列引用（SELECT t.col / SELECT col）给出语义化报错，
				// 不再落入笼统 unsupported constant select column。
				return nil, &SQLError{Msg: "column reference without FROM: " + sc.Raw}
			default:
				return nil, &SQLError{Msg: "unsupported select column: " + sc.Raw}
			}
			row = append(row, v)
		}
		return &Result{Columns: cols, Rows: [][]Value{row}}, nil
	}
	tx, err := e.getTxForSelect(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		// v1.16.0 C6：autocommit 只读 SELECT 共享快照窗口（snapshot_share）。
		// 可共享语句：窗口未开时挂载本语句事务（快照由窗口持有），窗口已开
		// 时复用窗口事务（共享快照），成功路径不关闭窗口（供连续只读语句
		// 复用，延迟隐藏）；出错时关闭窗口并释放（错误传播语义不变）。
		// 其余 autocommit 语句：独立临时事务 + 语句结束 Rollback（原语义）。
		if e.snapshotShare && e.stmtROShareable(sess, s) {
			tx, err = e.enterSharedROWindow(sess, tx)
			if err != nil {
				return nil, err
			}
			defer func() {
				if err != nil {
					e.exitSharedROWindow(sess)
				}
			}()
		} else if !e.isSharedROTx(sess, tx) {
			defer tx.Rollback()
		}
		// 防御：嵌套不可共享语句触达窗口事务（isSharedROTx=true）时不
		// Rollback、不新建——窗口由外层语句管理，只读执行语义一致。
	}
	// M18-C：WHERE 中的 @var 用户变量在查询执行前替换为会话值（未设置报错）。
	if err := e.resolveWhereVars(sess, s.Where); err != nil {
		return nil, err
	}
	// T39：普通 schema.table 限定名解析归属库并校验库存在（信息表保留全名）。
	if strings.Contains(s.From, "."+infoSchemaPrefix) {
		// 兼容 default.information_schema.tables 等带库前缀的信息表写法：剥去库前缀。
		s.From = s.From[strings.Index(s.From, "."+infoSchemaPrefix)+1:]
	} else if !isInfoSchemaName(s.From) {
		_, tbl, err := e.resolveTableDB(sess, tx, s.From)
		if err != nil {
			return nil, err
		}
		s.From = tbl
	}
	// M18-A：information_schema 虚拟表（只读目录视图），拦截在 CTE/视图展开之前。
	if isInfoSchemaName(s.From) {
		tabs, err := loadTables(tx.Get)
		if err != nil {
			return nil, err
		}
		return execInfoSchema(s, tabs)
	}
	// v1.15.0 W2：COUNT(*)/COUNT(1) 快速路径（无 WHERE/GROUP/HAVING/ORDER/
	// DISTINCT/JOIN/窗口/OFFSET 且事务支持 RangeCounter 时逐 region 计数求和，
	// 避免全表物化 + 逐行计数；条件不满足一律回退原路径）。
	if res, ok, err := e.tryCountFastPath(sess, tx, s); err != nil {
		return nil, err
	} else if ok {
		return res, nil
	}
	// M10 CTE：执行前展开语句级 WITH 子句（引用 CTE 名替换为定义子查询），
	// 先于视图展开执行（语句内作用域优先）；循环引用在此阶段报错。
	if err := e.expandCTEs(s); err != nil {
		return nil, err
	}
	// M9 视图：执行前将 FROM/JOIN 中引用的视图展开为其定义（子查询），
	// 展开过程带栈式循环引用防护。
	if err := e.expandViews(tx, s); err != nil {
		return nil, err
	}
	// P0 增强路径：JOIN / GROUP BY / FROM 子查询 / BETWEEN / IN / 限定列引用
	if hasAdvancedSelect(s) {
		return e.execSelectRel(sess, tx, s)
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.From)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.From}
	}
	var rows [][]Value
	ordered := false // 结果是否已按 ORDER BY 升序（索引序）
	// 主键等值/前缀快速路径：WHERE 命中主键列等值前缀（单列或复合前缀）
	// → 精确点查 Get / 有序前缀 Seek，避免全表 JSON 扫描（TPC-C 复合主键关键路径）。
	if plan, perr := e.pkCondPlan(meta, s.Where, nil); perr != nil {
		return nil, perr
	} else if plan != nil {
		if plan.full {
			dbgLog("pkcond execSelect hit full=%v rangeCol=%q lo=%v hi=%v prefixlen=%d", plan.full, plan.rangeCol, plan.rangeLo, plan.rangeHi, len(plan.prefix))
			rid, err := e.locateRow(meta, plan.prefix)
			if err != nil {
				return nil, err
			}
			raw, err := e.getKVCluster(sess, tx, e.rowKeyAt(meta, rid, plan.prefix))
			if err == nil {
				row, err := decodeRow(meta, raw)
				if err != nil {
					return nil, err
				}
				rows = [][]Value{row}
			} else if err != storage.ErrNotFound {
				return nil, err
			}
		} else if plan.rangeCol != "" && (plan.rangeLo != nil || plan.rangeHi != nil) {
			// 主键前缀 + 末列字面量范围：走有序区间 Seek，避免按前缀扫整段
			// （TPC-C STOCK_LEVEL bmsql_order_line 统计的关键路径）。
			pairs, err := e.scanMergedCluster(sess, tx, e.pkRangeRanges(meta, plan))
			if err != nil {
				return nil, err
			}
			rows = make([][]Value, 0, len(pairs))
			for _, kv := range pairs {
				row, err := decodeRow(meta, kv.Value)
				if err != nil {
					return nil, err
				}
				ok, err := matchWhere(meta, row, s.Where)
				if err != nil {
					return nil, err
				}
				if ok {
					rows = append(rows, row)
				}
			}
		} else {
			// 前缀等值：有序 Seek 前缀区间（结果行少，仍需 WHERE 过滤确认边界）
			pr := e.pkPrefixRanges(meta, plan.prefix)
			dbgLog("pkprefix-prefix2244 table=%s nrange=%d start0=%x end0=%x", meta.Name, len(pr), pr[0].Start, pr[0].End)
			pairs, err := e.scanMergedCluster(sess, tx, pr)
			if err != nil {
				return nil, err
			}
			dbgLog("pkprefix-prefix2244 table=%s got=%d", meta.Name, len(pairs))
			rows = make([][]Value, 0, len(pairs))
			for _, kv := range pairs {
				row, err := decodeRow(meta, kv.Value)
				if err != nil {
					return nil, err
				}
				ok, err := matchWhere(meta, row, s.Where)
				if err != nil {
					return nil, err
				}
				if ok {
					rows = append(rows, row)
				}
			}
		}
		return e.finishSelect(meta, s, rows, ordered)
	}
	// 二级索引优化：WHERE 命中索引列等值/范围
	if rows, ordered, err = e.indexLookup(sess, tx, meta, s); err != nil {
		return nil, err
	}
	if rows == nil {
		// M12-2 分布式排序分页捷径（无 WHERE：窗口截断正确性前提）
		if e.canDistributedOrder(meta, s) {
			// v1.15.0 W4：ORDER BY 单列主键（TEXT/DATE/BLOB，编码序恒=比较序）时
			// 各分片按物理键序取足量即可免排序（无 WHERE 已由 canDistributedOrder
			// 保证；截断前丢行无法事后验证，故仅限静态安全类型）。
			if orderBySinglePKSafe(meta, s) {
				pairs, err := e.scanMergedClusterTop(sess, tx, e.rowRanges(meta), s.Offset+s.Limit)
				if err != nil {
					return nil, err
				}
				rows = make([][]Value, 0, len(pairs))
				for _, kv := range pairs {
					row, err := decodeRow(meta, kv.Value)
					if err != nil {
						return nil, err
					}
					rows = append(rows, row)
				}
				return e.finishSelect(meta, s, rows, true)
			}
			return e.execDistributedOrder(sess, tx, meta, s)
		}
		// v1.15.0 W4：ORDER BY 二级索引顺序扫描（无 WHERE 索引条件时按索引序全范围
		// 扫描回表；索引序 = 索引列升序，finishSelect 内已序验证兜底）。
		if idx := orderByIndexCandidate(meta, s); idx != nil {
			rows, err = e.indexScanRowsForOrderBy(sess, tx, meta, idx, s)
			if err != nil {
				return nil, err
			}
			ordered = true
		} else {
			// 全表扫描 + 过滤（跨 region/跨节点展开后按内层键序合并，等价单 region 全表序）
			var pairs []storage.KVPair
			var err error
			if e.canDistributedTop(meta, s) {
				// M12-2 无 ORDER BY 仅 LIMIT/OFFSET：各分片取足量再截断
				pairs, err = e.scanMergedClusterTop(sess, tx, e.rowRanges(meta), s.Offset+s.Limit)
			} else {
				pairs, err = e.scanMergedCluster(sess, tx, e.rowRanges(meta))
			}
			if err != nil {
				return nil, err
			}
			rows = make([][]Value, 0, len(pairs))
			for _, kv := range pairs {
				row, err := decodeRow(meta, kv.Value)
				if err != nil {
					return nil, err
				}
				ok, err := matchWhere(meta, row, s.Where)
				if err != nil {
					return nil, err
				}
				if ok {
					rows = append(rows, row)
				}
			}
		}
	}
	dbgLog("fullscan where-cols=%v", func() []string {
		if s.Where == nil {
			return nil
		}
		out := make([]string, 0, len(s.Where.Conds))
		for i := range s.Where.Conds {
			c := &s.Where.Conds[i]
			out = append(out, c.Col+" "+c.Op+" "+c.Val.Kind)
		}
		return out
	}())
	return e.finishSelect(meta, s, rows, ordered)
}

// indexLookup 用二级索引满足 WHERE（等值/范围）与 ORDER BY。
// 返回 rows=nil 表示无可用索引条件，应回退全表扫描。
func (e *Executor) indexLookup(sess *Session, tx txn.Txn, meta *TableMeta, s *SelectStmt) ([][]Value, bool, error) {
	if s.Where == nil || len(meta.Indexes) == 0 {
		return nil, false, nil
	}
	// 取第一个命中索引列的条件（非 !=）
	var hit *Cond
	var idx *IndexMeta
	for i := range s.Where.Conds {
		c := &s.Where.Conds[i]
		if c.Op == "!=" {
			continue
		}
		for j := range meta.Indexes {
			if meta.Indexes[j].Col == c.Col {
				hit = c
				idx = &meta.Indexes[j]
				break
			}
		}
		if hit != nil {
			break
		}
	}
	if hit == nil {
		return nil, false, nil
	}
	// 索引列为 BLOB 时，TEXT 字面量按 hex 解码为 BLOB 值再编码（与存储键一致）。
	hv := hit.Val
	if ci := colIndex(meta, idx.Col); ci >= 0 && meta.Columns[ci].Type == "BLOB" && hv.Kind == "TEXT" {
		raw, err := decodeHex(strings.ToUpper(hv.S))
		if err != nil {
			return nil, false, err
		}
		hv = Value{Kind: "BLOB", S: encodeHex(raw)}
	}
	val, err := idxBytes(hv)
	if err != nil {
		return nil, false, err
	}
	// 构造扫描边界；非等值操作符要求值类型与列类型一致（比较语义）
	var start, end []byte
	prefix := func(v []byte) []byte { return EncodeIndexKey2(meta.ID, idx.ID, v, nil) }
	switch hit.Op {
	case "=":
		start = prefix(val)
		end = prefix(append(append([]byte{}, val...), 0xff))
	case ">=":
		start = prefix(val)
		end = EncodeIndexKey2(meta.ID+1, 0, nil, nil)
	case ">":
		start = prefix(append(append([]byte{}, val...), 0xff))
		end = EncodeIndexKey2(meta.ID+1, 0, nil, nil)
	case "<":
		start = EncodeIndexKey2(meta.ID, idx.ID, nil, nil)
		end = prefix(val)
	case "<=":
		start = EncodeIndexKey2(meta.ID, idx.ID, nil, nil)
		end = prefix(append(append([]byte{}, val...), 0xff))
	default:
		return nil, false, nil
	}
	pairs, err := e.scanMergedCluster(sess, tx, e.indexRanges(meta, idx.ID, start, end))
	if err != nil {
		return nil, false, err
	}
	rows := make([][]Value, 0, len(pairs))
	// 批量回表：先收集全部行键，一次批量读（本地键经 tx.MGet，减少 cgo 往返），
	// 远端键由 mgetKVCluster 逐条转发；与逐行 getKVCluster 语义等价。
	rowKeys := make([][]byte, 0, len(pairs))
	for _, kv := range pairs {
		inner := innerOf(kv.Key)
		pk := indexPKSuffixLen(inner, len(val))
		// 回表：索引键与行键同 region（写入时同路由），按 region 前缀读取（跨节点经转发）
		rowKeys = append(rowKeys, e.rowKeyAt(meta, ridOf(kv.Key), pk))
	}
	got, err := e.mgetKVCluster(sess, tx, rowKeys)
	if err != nil {
		return nil, false, err
	}
	for _, kv := range pairs {
		inner := innerOf(kv.Key)
		pk := indexPKSuffixLen(inner, len(val))
		rk := e.rowKeyAt(meta, ridOf(kv.Key), pk)
		raw, ok := got[string(rk)]
		if !ok {
			continue // 索引脏键（理论上不出现）
		}
		row, err := decodeRow(meta, raw)
		if err != nil {
			return nil, false, err
		}
		ok, err = matchWhere(meta, row, s.Where)
		if err != nil {
			return nil, false, err
		}
		if ok {
			rows = append(rows, row)
		}
	}
	// 索引序 = 该索引列升序；ORDER BY 同列升序时无需再排序
	ordered := s.OrderBy != nil && s.OrderBy.Col == idx.Col && !s.OrderBy.Desc
	return rows, ordered, nil
}

// finishSelect 聚合 / 排序 / 限行 / 投影。
// ordered=true 表示 rows 已按 ORDER BY 列升序（来自索引序），跳过重复排序。
func (e *Executor) finishSelect(meta *TableMeta, s *SelectStmt, rows [][]Value, ordered bool) (*Result, error) {
	// 排序（v1.15.0 W4：ORDER BY 索引顺序扫描免排序）
	// 命中主键/单列索引候选时先做 O(n) 已序验证（比较基准 compareVal），验证通过
	// 才免排序；ordered=true（既有索引序/分布式归并序）也统一验证兜底——单列主键
	// INT 负值、复合主键/索引首列长度前缀序等编码序可能偏离比较序，验证失败一律
	// 回退原排序路径，保证语义严格等价。
	if s.OrderBy != nil {
		if ordered {
			if !rowsSortedByValue(meta, rows, s.OrderBy) {
				ordered = false
			}
		} else if orderByScanCandidate(meta, s) || orderByIndexCandidate(meta, s) != nil {
			if rowsSortedByValue(meta, rows, s.OrderBy) {
				ordered = true
			}
		}
		if !ordered {
			ob := s.OrderBy
			sort.SliceStable(rows, func(i, j int) bool {
				a, _ := rowValue(meta, rows[i], ob.Col)
				b, _ := rowValue(meta, rows[j], ob.Col)
				cmp := compareVal(a, b)
				if ob.Desc {
					return cmp > 0
				}
				return cmp < 0
			})
		}
	}
	// LIMIT / OFFSET（M12-2：OFFSET 在排序后截断，与 LIMIT 同一语义位置）
	if s.Offset > 0 {
		if s.Offset >= len(rows) {
			rows = rows[:0]
		} else {
			rows = rows[s.Offset:]
		}
	}
	if s.Limit > 0 && len(rows) > s.Limit {
		rows = rows[:s.Limit]
	}
	// 聚合
	hasAgg := false
	hasWindow := false
	for _, c := range s.Columns {
		if c.Window != nil {
			hasWindow = true
			continue
		}
		if c.Agg != "" {
			hasAgg = true
		}
	}
	if hasWindow {
		// 窗口函数与全局聚合混用不支持
		if hasAgg {
			return nil, &SQLError{Msg: "window function cannot be mixed with aggregate without GROUP BY"}
		}
		return e.finishSelectWindow(meta, s, rows)
	}
	if hasAgg {
		return e.doAgg(meta, s, rows)
	}
	// 投影
	res := &Result{}
	for _, c := range s.Columns {
		if c.Name == "*" {
			for _, col := range visibleColumns(meta) {
				res.Columns = append(res.Columns, col.Name)
			}
		} else {
			res.Columns = append(res.Columns, c.Name)
		}
	}
	for _, row := range rows {
		var out []Value
		for _, c := range s.Columns {
			if c.Name == "*" {
				out = append(out, visibleRow(meta, row)...)
			} else if c.Case != nil {
				v, err := evalCaseM1(meta, row, c.Case)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			} else if c.Func != "" {
				// D7.2：标量函数列（VERSION()）不依赖行上下文。
				v, err := evalScalarFunc(c.Func)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			} else {
				v, err := rowValue(meta, row, c.Name)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
		}
		res.Rows = append(res.Rows, out)
	}
	return res, nil
}

// finishSelectWindow 窗口函数投影（单表/meta 路径）：普通列逐行取值，窗口列由 computeWindows 计算。
func (e *Executor) finishSelectWindow(meta *TableMeta, s *SelectStmt, rows [][]Value) (*Result, error) {
	res := &Result{}
	for _, c := range s.Columns {
		if c.Name == "*" {
			for _, col := range visibleColumns(meta) {
				res.Columns = append(res.Columns, col.Name)
			}
		} else {
			res.Columns = append(res.Columns, c.Name)
		}
	}
	var winCols []*SelectColumn
	for i := range s.Columns {
		if s.Columns[i].Window != nil {
			winCols = append(winCols, &s.Columns[i])
		}
	}
	cols := make([]string, 0, len(res.Columns))
	colTypes := make([]string, 0, len(res.Columns))
	for _, col := range visibleColumns(meta) {
		cols = append(cols, col.Name)
		colTypes = append(colTypes, col.Type)
	}
	rel := &Rel{Cols: cols, Rows: rows, Types: colTypes}
	winVals, err := computeWindows(rows, func(row []Value, col string) (Value, error) {
		return rowValue(meta, row, col)
	}, func(row []Value, e *Expr) (Value, error) {
		return evalExprForRow(rel, row, e)
	}, winCols)
	if err != nil {
		return nil, err
	}
	for ri, row := range rows {
		var out []Value
		wi := 0
		for _, c := range s.Columns {
			if c.Name == "*" {
				out = append(out, visibleRow(meta, row)...)
			} else if c.Window != nil {
				out = append(out, winVals[ri][wi])
				wi++
			} else if c.Case != nil {
				v, err := evalCaseM1(meta, row, c.Case)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			} else {
				v, err := rowValue(meta, row, c.Name)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
		}
		res.Rows = append(res.Rows, out)
	}
	return res, nil
}

func (e *Executor) doAgg(meta *TableMeta, s *SelectStmt, rows [][]Value) (*Result, error) {
	res := &Result{}
	out := make([]Value, 0, len(s.Columns))
	for _, c := range s.Columns {
		res.Columns = append(res.Columns, c.Name)
		res.ColTypes = append(res.ColTypes, aggResultType(meta, nil, rows, &c))
		res.ColNullable = append(res.ColNullable, c.Agg != "COUNT" && len(rows) == 0)
		v, err := aggColumn(meta, rows, &c)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	res.Rows = append(res.Rows, out)
	return res, nil
}

// aggResultType 推断聚合列的结果类型："INT"/"DECIMAL"/"TEXT"/"BLOB"/""（未知）。
// meta 非 nil 时优先查列定义（单表路径）；否则从首行操作数 Kind 推断（JOIN/子查询路径）。
func aggResultType(meta *TableMeta, rel *Rel, rows [][]Value, c *SelectColumn) string {
	if c.Agg == "COUNT" {
		return "INT"
	}
	if meta != nil {
		for _, m := range meta.Columns {
			if m.Name == c.Col {
				return m.Type
			}
		}
	}
	// meta 为空（JOIN/子查询/高级路径）或列不在 meta 时，优先从 Rel.Types 兜底
	//（Rel 构造点已从物理表元数据/子查询 ColTypes 填充列类型，空结果集同样可用）。
	if rel != nil && len(rel.Types) == len(rel.Cols) {
		for i, cn := range rel.Cols {
			if baseColName(cn) == baseColName(c.Col) && rel.Types[i] != "" {
				return rel.Types[i]
			}
		}
	}
	if len(rows) > 0 {
		if v, err := aggOperand(rel, rows[0], c); err == nil {
			switch v.Kind {
			case "INT":
				return "INT"
			case "DECIMAL":
				return "DECIMAL"
			case "BLOB":
				return "BLOB"
			case "TEXT":
				return "TEXT"
			}
		}
	}
	return ""
}

// aggColumn 计算单列聚合值（COUNT/SUM/AVG/MAX/MIN；DECIMAL 保持精度，INT 走 int64）。
// P0-4：COUNT(col/expr) 仅计非 NULL、支持 DISTINCT；SUM/AVG/MAX/MIN 跳过 NULL 并支持 DISTINCT。
func aggColumn(meta *TableMeta, rows [][]Value, c *SelectColumn) (Value, error) {
	if c.Agg == "COUNT" {
		// COUNT(*)：无目标列时按行数计数。
		if c.Col == "" {
			return IntVal(int64(len(rows))), nil
		}
		seen := make(map[string]bool)
		var cnt int64
		for _, r := range rows {
			v, err := rowValue(meta, r, c.Col)
			if err != nil {
				return Value{}, err
			}
			if v.Kind == "NULL" {
				continue
			}
			if c.Distinct {
				k := v.String()
				if seen[k] {
					continue
				}
				seen[k] = true
			}
			cnt++
		}
		return IntVal(cnt), nil
	}
	// MAX/MIN/SUM/AVG：收集非 NULL 操作数（DISTINCT 去重）。
	vals := make([]Value, 0, len(rows))
	seen := make(map[string]bool)
	for _, r := range rows {
		v, err := rowValue(meta, r, c.Col)
		if err != nil {
			return Value{}, err
		}
		if v.Kind == "NULL" {
			continue
		}
		if c.Distinct {
			k := v.String()
			if seen[k] {
				continue
			}
			seen[k] = true
		}
		vals = append(vals, v)
	}
	switch c.Agg {
	case "MAX", "MIN":
		if len(vals) == 0 {
			return Value{Kind: "NULL"}, nil
		}
		best := vals[0]
		for _, v := range vals[1:] {
			cmp := compareVal(v, best)
			if (c.Agg == "MAX" && cmp > 0) || (c.Agg == "MIN" && cmp < 0) {
				best = v
			}
		}
		return best, nil
	case "SUM", "AVG":
		if len(vals) == 0 {
			// 空表/空组 SUM/AVG 返回 NULL（SQL 标准：COUNT=0，SUM/AVG=NULL），与 MAX/MIN 一致。
			return Value{Kind: "NULL"}, nil
		}
		dec := false
		var isum, dsum int64
		for _, v := range vals {
			switch v.Kind {
			case "INT":
				isum += v.I
			case "DECIMAL":
				dec = true
				dsum += v.I
			default:
				return Value{}, &SQLError{Msg: "aggregate on non-numeric column: " + c.Name}
			}
		}
		if dec {
			// v1.12.0 高精度：取首个 DECIMAL 操作数的小数位作为结果 scale（同列同 scale）。
			ds := decScale
			for _, v := range vals {
				if v.Kind == "DECIMAL" {
					ds = decDS(v)
					break
				}
			}
			if c.Agg == "SUM" {
				return decValScaled(dsum, ds), nil
			}
			return decValScaled(dsum/int64(len(vals)), ds), nil
		}
		if c.Agg == "SUM" {
			return IntVal(isum), nil
		}
		return IntVal(isum / int64(len(vals))), nil
	}
	return Value{}, &SQLError{Msg: "unknown aggregate: " + c.Agg}
}

// ---- P0 SELECT 增强引擎：JOIN / GROUP BY / FROM 子查询 / BETWEEN / IN ----

// Rel 表示一个"关系"：列名序列 + 行集 + 列类型序列（与 Cols 对齐，空串=未知）。
// 单表列名为 alias.col（限定形式）；子查询列名为 subAlias.col（子查询输出列名）。
type Rel struct {
	Cols  []string
	Rows  [][]Value
	Types []string
}

// hasAdvancedSelect 判断 SELECT 是否走增强路径（保留旧单表路径的索引优化与既有行为）。
func hasAdvancedSelect(s *SelectStmt) bool {
	if s.Subquery != nil || len(s.Joins) > 0 || len(s.GroupBy) > 0 || s.Distinct || s.Having != nil {
		return true
	}
	if s.Where != nil {
		for _, c := range s.Where.Conds {
			if c.Ref != "" || c.Op == "BETWEEN" || c.Op == "IN" || c.RightRef != "" || c.Sub != nil || c.LeftExpr != nil || c.RightExpr != nil {
				return true
			}
		}
	}
	if s.OrderBy != nil && s.OrderBy.Ref != "" {
		return true
	}
	for _, c := range s.Columns {
		if c.Ref != "" || c.Alias != "" || c.Expr != nil || c.Agg == "MAX" || c.Agg == "MIN" {
			return true
		}
	}
	return false
}

// orAlias 返回有效表别名：显式别名优先，否则表名。
func orAlias(table, alias string) string {
	if alias == "" {
		return table
	}
	return alias
}

// baseColName 取输出列名的裸名（去掉限定前缀最后一段）。
func baseColName(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// ---- W3 列投影下推：SELECT 所需列收集 ----

// collectExprCols 递归收集算术表达式树中的列引用（裸列名）。
func collectExprCols(e *Expr, set map[string]bool) {
	if e == nil {
		return
	}
	if e.Op == "" {
		if e.Val == nil && e.Col != "" {
			set[e.Col] = true
		}
		return
	}
	collectExprCols(e.Left, set)
	collectExprCols(e.Right, set)
}

// collectCaseCols 收集 CASE WHEN 分支条件中的列引用。
func collectCaseCols(cc *CaseWhenClause, set map[string]bool) {
	if cc == nil {
		return
	}
	for i := range cc.Branches {
		for j := range cc.Branches[i].Conds.Conds {
			collectCondCols(&cc.Branches[i].Conds.Conds[j], set)
		}
	}
}

// collectCondCols 收集过滤条件（WHERE/HAVING/CASE WHEN）中的列引用。
// IN 子查询（c.Sub）内部列由子查询自身执行，不收集。
func collectCondCols(c *Cond, set map[string]bool) {
	if c.Col != "" {
		set[c.Col] = true
	}
	if c.RightRef != "" {
		set[baseColName(c.RightRef)] = true
	}
	for _, tl := range c.TupleLeft {
		if tl != "" {
			set[baseColName(tl)] = true
		}
	}
	if c.LeftCase != nil {
		collectCaseCols(c.LeftCase, set)
	}
	if c.LeftExpr != nil {
		collectExprCols(c.LeftExpr, set)
	}
	if c.RightExpr != nil {
		collectExprCols(c.RightExpr, set)
	}
	if c.LeftAgg != nil {
		collectSelectColRefs(c.LeftAgg, set)
	}
}

// collectSelectColRefs 收集单个 SELECT 列的列引用（聚合/窗口参数、表达式、
// CASE、裸列）。返回 false 表示含无法下推形态（星号等），调用方须回退全列。
func collectSelectColRefs(c *SelectColumn, set map[string]bool) bool {
	if c.Name == "*" {
		return false
	}
	if c.Var != "" {
		return true // 用户变量：无表列引用
	}
	if c.Window != nil {
		for _, p := range c.Window.PartitionBy {
			if p != "" {
				set[baseColName(p)] = true
			}
		}
		if c.Window.OrderBy != nil && c.Window.OrderBy.Col != "" {
			set[c.Window.OrderBy.Col] = true
		}
		for _, ob := range c.Window.OrderBys {
			if ob.Col != "" {
				set[ob.Col] = true
			}
		}
	}
	if c.Case != nil {
		collectCaseCols(c.Case, set)
	}
	if c.Expr != nil {
		collectExprCols(c.Expr, set)
	}
	if c.Col != "" {
		set[c.Col] = true
	}
	return true
}

// collectSelectCols 收集 SELECT 语句执行所需的全部表列（裸列名去重）。
// 覆盖：投影列、WHERE、HAVING、GROUP BY、ORDER BY、窗口分区/排序/聚合参数、
// 表达式/CASE/聚合参数。返回 (列集合, 是否可下推)；false 时调用方必须全列解码。
func collectSelectCols(s *SelectStmt) (map[string]bool, bool) {
	set := make(map[string]bool)
	for i := range s.Columns {
		if !collectSelectColRefs(&s.Columns[i], set) {
			return nil, false
		}
	}
	if s.Where != nil {
		for i := range s.Where.Conds {
			collectCondCols(&s.Where.Conds[i], set)
		}
	}
	if s.Having != nil {
		for i := range s.Having.Conds {
			collectCondCols(&s.Having.Conds[i], set)
		}
	}
	for _, g := range s.GroupBy {
		if g != "" {
			set[baseColName(g)] = true
		}
	}
	if s.OrderBy != nil && s.OrderBy.Col != "" {
		set[s.OrderBy.Col] = true
	}
	for _, ob := range s.OrderBys {
		if ob.Col != "" {
			set[ob.Col] = true
		}
	}
	return set, true
}

// selectNeeded 计算 SELECT 的投影下推列（裸列名列表，按表可见列序）。
// 返回 nil 表示回退全列：星号/JOIN/子查询/无引用/引用覆盖全部可见列等场景。
// 不存在的列引用不进入 needed（由下游 relValue 报 unknown column，保持报错语义）。
func (e *Executor) selectNeeded(meta *TableMeta, s *SelectStmt) []string {
	// JOIN 场景保守回退全列（左右列定位复杂，保证语义等价优先）。
	if len(s.Joins) > 0 || s.Subquery != nil {
		return nil
	}
	colSet, ok := collectSelectCols(s)
	if !ok || len(colSet) == 0 {
		return nil
	}
	vis := visibleColumns(meta)
	needed := make([]string, 0, len(vis))
	coveredAll := true
	for _, c := range vis {
		if colSet[c.Name] {
			needed = append(needed, c.Name)
		} else {
			coveredAll = false
		}
	}
	if coveredAll {
		return nil // 全部可见列都要：与全列等价，无收益
	}
	return needed
}

// relFromTable 解析 FROM / JOIN 右侧关系：物理表或子查询派生表。
// needed 为投影下推列（裸列名，nil/空 = 全列解码），仅主查询 FROM 单表路径传入。
func (e *Executor) relFromTable(sess *Session, tx txn.Txn, tabs []*TableMeta, table, alias string, sub *SelectStmt, subAlias string, needed []string) (*Rel, error) {
	if sub != nil {
		subRes, err := e.execSelect(sess, sub)
		if err != nil {
			return nil, err
		}
		rel := &Rel{Cols: make([]string, 0, len(subRes.Columns)), Rows: subRes.Rows, Types: make([]string, 0, len(subRes.Columns))}
		for i, c := range subRes.Columns {
			// 派生表列名 = 子查询别名 + 子查询输出列裸名（去掉内部限定前缀）
			rel.Cols = append(rel.Cols, subAlias+"."+baseColName(c))
			if i < len(subRes.ColTypes) {
				rel.Types = append(rel.Types, subRes.ColTypes[i])
			} else {
				rel.Types = append(rel.Types, "")
			}
		}
		return rel, nil
	}
	meta := findTable(tabs, table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + table}
	}
	return e.relFromScan(sess, tx, meta, alias, needed)
}

// relFromScan 全表扫描生成 Rel（列名限定为 alias.col；跨 region/跨节点展开）。
// needed 非空时启用 W3 列投影下推：仅解码所需列并仅保留这些列到 rel
// （未投影列跳过类型还原；Cols/Rows/Types 与 needed 对齐，语义与全列等价）。
func (e *Executor) relFromScan(sess *Session, tx txn.Txn, meta *TableMeta, alias string, needed []string) (*Rel, error) {
	pairs, err := e.scanMergedCluster(sess, tx, e.rowRanges(meta))
	if err != nil {
		return nil, err
	}
	vis := visibleColumns(meta)
	if len(needed) > 0 && len(needed) < len(vis) {
		// W3：投影下推——仅解码 needed 列。
		idxs := make([]int, 0, len(needed))   // meta.Columns 索引（decode 用）
		visIdx := make([]int, 0, len(needed)) // visibleColumns 索引（rel 对齐用）
		for _, n := range needed {
			mi := colIndex(meta, n)
			if mi < 0 {
				continue // 引用列不存在：保持既有语义，交给下游报 unknown column
			}
			vi := -1
			for i, c := range vis {
				if c.Name == n {
					vi = i
					break
				}
			}
			if vi < 0 {
				continue // 隐藏列（__mv_*）不进结果 rel，语义同全列路径
			}
			idxs = append(idxs, mi)
			visIdx = append(visIdx, vi)
		}
		if len(idxs) > 0 {
			rel := &Rel{Cols: make([]string, 0, len(idxs)), Rows: make([][]Value, 0, len(pairs)), Types: make([]string, 0, len(idxs))}
			for _, vi := range visIdx {
				rel.Cols = append(rel.Cols, alias+"."+vis[vi].Name)
				rel.Types = append(rel.Types, vis[vi].Type)
			}
			for _, kv := range pairs {
				row, err := decodeRowSelected(meta, kv.Value, idxs)
				if err != nil {
					return nil, err
				}
				rel.Rows = append(rel.Rows, row)
			}
			dbgLog("projpush table=%s cols=%d/%d pairs=%d", meta.Name, len(idxs), len(vis), len(rel.Rows))
			return rel, nil
		}
	}
	// 全列路径（原逻辑）
	rel := &Rel{Cols: make([]string, 0, len(vis)), Rows: make([][]Value, 0, len(pairs)), Types: make([]string, 0, len(vis))}
	for _, c := range vis {
		rel.Cols = append(rel.Cols, alias+"."+c.Name)
		rel.Types = append(rel.Types, c.Type)
	}
	for _, kv := range pairs {
		row, err := decodeRow(meta, kv.Value)
		if err != nil {
			return nil, err
		}
		rel.Rows = append(rel.Rows, visibleRow(meta, row))
	}
	dbgLog("pkprefix table=%s pairs=%d rows=%d", meta.Name, len(pairs), len(rel.Rows))
	return rel, nil
}

// joinRels 拼接左右 Rel：INNER 仅保留匹配行；LEFT 保留左行并以空串值补右列。
func joinRels(left, right *Rel, jc *JoinClause) (*Rel, error) {
	out := &Rel{Cols: append(append([]string{}, left.Cols...), right.Cols...), Types: append(append([]string{}, left.Types...), right.Types...)}
	for _, lr := range left.Rows {
		matched := 0
		for _, rr := range right.Rows {
			ok, err := matchJoinOn(left, lr, right, rr, jc.On)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			merged := make([]Value, 0, len(lr)+len(rr))
			merged = append(merged, lr...)
			merged = append(merged, rr...)
			out.Rows = append(out.Rows, merged)
			matched++
		}
		if matched == 0 && jc.Type == "LEFT" {
			merged := make([]Value, 0, len(lr)+len(right.Cols))
			merged = append(merged, lr...)
			for range right.Cols {
				merged = append(merged, Value{Kind: "TEXT", S: ""})
			}
			out.Rows = append(out.Rows, merged)
		}
	}
	return out, nil
}

// matchJoinOn 求值 ON 条件（AND 组合；左右列引用）。
// 每端列先在 left 中查找、再在 right 中查找，支持任意一侧列作左操作数。
func matchJoinOn(left *Rel, lr []Value, right *Rel, rr []Value, conds []Cond) (bool, error) {
	for _, c := range conds {
		aName := qualifiedName(c.Ref, c.Col)
		var a Value
		if i := relColIndex(left, aName); i >= 0 {
			a = lr[i]
		} else if i := relColIndex(right, aName); i >= 0 {
			a = rr[i]
		} else {
			return false, &SQLError{Msg: "unknown column: " + aName}
		}
		var b Value
		if c.RightRef != "" {
			if i := relColIndex(right, c.RightRef); i >= 0 {
				b = rr[i]
			} else if i := relColIndex(left, c.RightRef); i >= 0 {
				b = lr[i]
			} else {
				return false, &SQLError{Msg: "unknown column: " + c.RightRef}
			}
		} else {
			b = c.Val
		}
		if !matchCond(a, c.Op, b) {
			return false, nil
		}
	}
	return true, nil
}

// relColIndex 列定位：限定名精确匹配；裸名匹配裸列或唯一限定后缀（多义返回 -1）。
func relColIndex(rel *Rel, name string) int {
	for i, c := range rel.Cols {
		if c == name {
			return i
		}
	}
	if !strings.Contains(name, ".") {
		found := -1
		for i, c := range rel.Cols {
			if c == name || strings.HasSuffix(c, "."+name) {
				if found >= 0 {
					return -1 // 歧义
				}
				found = i
			}
		}
		return found
	}
	return -1
}

// relValue 在 Rel 行中取值。
func relValue(rel *Rel, row []Value, name string) (Value, error) {
	idx := relColIndex(rel, name)
	if idx < 0 {
		return Value{}, &SQLError{Msg: "unknown column: " + name}
	}
	return row[idx], nil
}

// evalExprForRow 在 Rel 行上求值算术表达式（T40）。
// 支持叶子字面量（INT/DECIMAL/TEXT）、叶子列引用（限定/裸）与二元算术 + - * / %、
// 一元负号（NEG）。
// hasColRef 判断 Expr 树中是否存在列引用（非字面量叶子），用于无 FROM 常量投影守卫。
func hasColRef(e *Expr) bool {
	if e == nil {
		return false
	}
	if e.Op == "" {
		return e.Val == nil && (e.Col != "" || e.Ref != "")
	}
	return hasColRef(e.Left) || hasColRef(e.Right)
}

// caseHasColRef 判断 CASE 分支条件中是否存在列引用/子查询（无 FROM 常量投影时不可用）。
func caseHasColRef(cc *CaseWhenClause) bool {
	if cc == nil {
		return false
	}
	for _, b := range cc.Branches {
		for _, c := range b.Conds.Conds {
			if c.Col != "" || c.RightRef != "" || c.Sub != nil {
				return true
			}
		}
	}
	return false
}

func evalExprForRow(rel *Rel, row []Value, e *Expr) (Value, error) {
	if e == nil {
		return Value{}, &SQLError{Msg: "empty expression"}
	}
	switch e.Op {
	case "":
		if e.Val != nil {
			return *e.Val, nil
		}
		return relValue(rel, row, qualifiedName(e.Ref, e.Col))
	case "NEG":
		v, err := evalExprForRow(rel, row, e.Right)
		if err != nil {
			return Value{}, err
		}
		switch v.Kind {
		case "INT":
			return IntVal(-v.I), nil
		case "DECIMAL":
			return decValScaled(-v.I, decDS(v)), nil
		case "NULL":
			// P1-10：SQL 语义 -NULL = NULL。
			return v, nil
		default:
			return Value{}, &SQLError{Msg: "cannot apply unary minus to " + v.Kind + " value: " + v.String()}
		}
	}
	l, err := evalExprForRow(rel, row, e.Left)
	if err != nil {
		return Value{}, err
	}
	r, err := evalExprForRow(rel, row, e.Right)
	if err != nil {
		return Value{}, err
	}
	return evalArith(l, r, e.Op)
}

// evalSetExpr 基于表元数据与当前行对 UPDATE SET 赋值表达式求值。
// 与 evalExprForRow 等价，但列索引直接取自 TableMeta（无 Rel 对象场景）。
func evalSetExpr(meta *TableMeta, row []Value, e *Expr) (Value, error) {
	if e == nil {
		return Value{}, &SQLError{Msg: "empty expression"}
	}
	switch e.Op {
	case "":
		if e.Val != nil {
			return *e.Val, nil
		}
		idx := colIndex(meta, e.Col)
		if idx < 0 {
			return Value{}, &SQLError{Msg: "unknown column: " + e.Col}
		}
		return row[idx], nil
	case "NEG":
		v, err := evalSetExpr(meta, row, e.Right)
		if err != nil {
			return Value{}, err
		}
		switch v.Kind {
		case "INT":
			return IntVal(-v.I), nil
		case "DECIMAL":
			return decValScaled(-v.I, decDS(v)), nil
		case "NULL":
			// P1-10：SQL 语义 -NULL = NULL。
			return v, nil
		default:
			return Value{}, &SQLError{Msg: "cannot apply unary minus to " + v.Kind + " value: " + v.String()}
		}
	}
	l, err := evalSetExpr(meta, row, e.Left)
	if err != nil {
		return Value{}, err
	}
	r, err := evalSetExpr(meta, row, e.Right)
	if err != nil {
		return Value{}, err
	}
	return evalArith(l, r, e.Op)
}

// evalArith 计算二元算术（T40）。
// 类型规则：INT op INT → INT（除法除外，INT/INT → DECIMAL；取模为 INT）；
// 任一操作数为 DECIMAL → DECIMAL（定点，小数位取两操作数较大 scale，v1.12.0 高精度）；
// TEXT/DATE/BLOB 参与算术报错。
// 内部统一用 big.Int 计算防溢出；除零报错。
func evalArith(a, b Value, op string) (Value, error) {
	if !isNumericKind(a.Kind) || !isNumericKind(b.Kind) {
		return Value{}, &SQLError{Msg: "arithmetic on non-numeric operand: " + a.Kind + " and " + b.Kind}
	}
	switch op {
	case "+", "-", "*", "/", "%":
	default:
		return Value{}, errf("unsupported arithmetic operator %q", op)
	}
	dec := a.Kind == "DECIMAL" || b.Kind == "DECIMAL"
	if !dec {
		ab := new(big.Int).SetInt64(a.I)
		bb := new(big.Int).SetInt64(b.I)
		if op == "/" || op == "%" {
			if bb.Sign() == 0 {
				return Value{}, &SQLError{Msg: "division by zero"}
			}
		}
		switch op {
		case "+":
			ab.Add(ab, bb)
		case "-":
			ab.Sub(ab, bb)
		case "*":
			ab.Mul(ab, bb)
		case "/":
			ab.Mul(ab, big.NewInt(decUnit))
			ab.Quo(ab, bb)
			if !ab.IsInt64() {
				return Value{}, &SQLError{Msg: "DECIMAL overflow in division"}
			}
			return decVal(ab.Int64()), nil
		case "%":
			ab.Rem(ab, bb)
		}
		if !ab.IsInt64() {
			return Value{}, &SQLError{Msg: "INT overflow in arithmetic"}
		}
		return IntVal(ab.Int64()), nil
	}
	// DECIMAL 路径：统一为缩放整数（INT 放大 10^scale 对齐；scale 取两操作数较大值）。
	dsA, dsB := decDS(a), decDS(b)
	ds := dsA
	if dsB > ds {
		ds = dsB
	}
	unit := big.NewInt(pow10Int64(ds))
	as := new(big.Int).SetInt64(a.I)
	if a.Kind == "INT" {
		as.Mul(as, unit)
	} else if dsA < ds {
		as.Mul(as, big.NewInt(pow10Int64(ds-dsA)))
	}
	bs := new(big.Int).SetInt64(b.I)
	if b.Kind == "INT" {
		bs.Mul(bs, unit)
	} else if dsB < ds {
		bs.Mul(bs, big.NewInt(pow10Int64(ds-dsB)))
	}
	if op == "/" || op == "%" {
		if bs.Sign() == 0 {
			return Value{}, &SQLError{Msg: "division by zero"}
		}
	}
	switch op {
	case "+":
		as.Add(as, bs)
	case "-":
		as.Sub(as, bs)
	case "*":
		as.Mul(as, bs)
		as.Quo(as, unit) // scaled*scaled/10^ds → scaled
	case "/":
		as.Mul(as, unit)
		as.Quo(as, bs)
	case "%":
		as.Rem(as, bs)
	}
	if !as.IsInt64() {
		return Value{}, &SQLError{Msg: "DECIMAL overflow in arithmetic"}
	}
	return decValScaled(as.Int64(), ds), nil
}

// tryCountFastPath 尝试走 COUNT(*)/COUNT(1) 快速路径（v1.15.0 W2）：
// 仅当单物理表 + 单列 COUNT + 无 WHERE/GROUP BY/HAVING/ORDER BY/DISTINCT/
// JOIN/窗口/OFFSET，且当前事务实现 storage.RangeCounter 时，按 meta.rowRanges
// 逐 region CountRange 求和，返回单行单列结果（列名/类型与既有 COUNT 路径一致，
// 避免全表物化 + 逐行计数）。
// 任一条件不满足（含 COUNT(列名)、COUNT(DISTINCT ...)、COUNT(NULL)、
// 事务不支持 RangeCounter）返回 ok=false，由调用方回退原全量物化路径。
func (e *Executor) tryCountFastPath(sess *Session, tx txn.Txn, s *SelectStmt) (*Result, bool, error) {
	if s.Subquery != nil || s.SubAlias != "" || len(s.Joins) != 0 || s.Distinct ||
		s.Where != nil || s.Having != nil || len(s.GroupBy) != 0 || s.Offset > 0 ||
		s.OrderBy != nil || len(s.OrderBys) != 0 || len(s.Columns) != 1 {
		return nil, false, nil
	}
	c := &s.Columns[0]
	if c.Agg != "COUNT" || c.Distinct || c.Window != nil {
		return nil, false, nil
	}
	// 参数必须为 COUNT(*)（Col=="" 且 Expr==nil）或 COUNT(1)/COUNT('x')
	// 等纯字面量（Col=="" 且 Expr 为字面量叶子）；COUNT(NULL) 恒为 0，
	// 不能走快速路径（旧路径逐行 COUNT 的语义）。
	if c.Col != "" || (c.Expr != nil && (!c.Expr.isLiteral() || c.Expr.Val == nil || c.Expr.Val.Kind == "NULL")) {
		return nil, false, nil
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, false, err
	}
	meta := findTable(tabs, s.From)
	if meta == nil {
		return nil, false, nil
	}
	rc, ok := tx.(storage.RangeCounter)
	if !ok {
		return nil, false, nil
	}
	var total int64
	for _, rng := range e.rowRanges(meta) {
		n, err := rc.CountRange(rng, 0)
		if err != nil {
			return nil, false, err
		}
		total += int64(n)
	}
	res := &Result{
		Columns:     []string{c.Name},
		ColTypes:    []string{"INT"},
		ColNullable: []bool{false},
		Rows:        [][]Value{{IntVal(total)}},
	}
	res.Rows = applyOffsetLimit(res.Rows, 0, s.Limit)
	return res, true, nil
}

// execSelectRel 增强路径主流程：FROM（表/子查询）→ JOIN → WHERE → 聚合/分组 → 排序/投影。
func (e *Executor) execSelectRel(sess *Session, tx txn.Txn, s *SelectStmt) (*Result, error) {
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	// M12-2：单物理表（无子查询/JOIN/GROUP BY）+ 裸列 ORDER BY 分页时，
	// 走分布式排序捷径（各分片排序窗口 + coordinator 归并）；否则全量 Rel 路径。
	if s.Subquery == nil && len(s.Joins) == 0 && len(s.GroupBy) == 0 {
		if meta := findTable(tabs, s.From); meta != nil && e.canDistributedOrder(meta, s) {
			return e.execDistributedOrder(sess, tx, meta, s)
		}
	}
	alias := orAlias(s.From, s.FromAlias)
	var rel *Rel
	// IN 子查询物化缓存：pkCondPlan 前缀后主键列 IN 需要命中值做逐值点查；
	// WHERE 过滤阶段复用同一缓存避免重复执行子查询。
	var subCache map[*Cond]subqueryResult
	// 主键等值/前缀优化：单物理表（无 JOIN）且 WHERE 命中主键等值前缀时，
	// 直接以点查/有序前缀 Seek 读取候选行集构建 Rel，替代先全表物化再过滤
	// （避免 10 万行 order_line/stock 全表白扫）。
	// 允许 WHERE 含子查询：前缀只收窄候选行，子查询/其余条件仍由后续
	// matchWhere 逐行评估（TPC-C STOCK_LEVEL 外层 s_w_id 前缀 + IN 子查询关键路径）。
	if len(s.Joins) == 0 {
		if meta := findTable(tabs, s.From); meta != nil {
			// 先物化 IN 子查询（rel 为空时路由模式回退，后续 WHERE 过滤阶段再完整物化）
			if s.Where != nil && len(s.Where.Conds) > 0 {
				subCache, err = e.precomputeSubqueries(sess, tx, tabs, nil, s.Where)
				if err != nil {
					return nil, err
				}
			}
			if plan, perr := e.pkCondPlan(meta, s.Where, subCache); perr != nil {
				return nil, perr
			} else if plan != nil && len(plan.inVals) > 0 && plan.inCol != "" {
				// 前缀 + 下一主键列 IN（字面量/子查询物化值）：逐值前缀点查，
				// 消除全表扫描（TPC-C STOCK_LEVEL：s_w_id 前缀 + s_i_id IN(21值) → 21 次 seek）。
				rel = &Rel{Cols: make([]string, 0, len(meta.Columns)), Rows: make([][]Value, 0, len(plan.inVals)), Types: make([]string, 0, len(meta.Columns))}
				for _, c := range visibleColumns(meta) {
					rel.Cols = append(rel.Cols, alias+"."+c.Name)
					rel.Types = append(rel.Types, c.Type)
				}
				for _, iv := range plan.inVals {
					inPrefix, err := pkPrefixBytes(meta, append(append([]Value{}, plan.prefixVals...), iv))
					if err != nil {
						return nil, err
					}
					pairs, err := e.scanMergedCluster(sess, tx, e.pkPrefixRanges(meta, inPrefix))
					if err != nil {
						return nil, err
					}
					for _, kv := range pairs {
						row, err := decodeRow(meta, kv.Value)
						if err != nil {
							return nil, err
						}
						ok, err := e.matchWhereRelCached(sess, tx, rel, row, s.Where, subCache)
						if err != nil {
							return nil, err
						}
						if ok {
							rel.Rows = append(rel.Rows, visibleRow(meta, row))
						}
					}
				}
			} else if plan != nil {
				var pairs []storage.KVPair
				if plan.full {
					rid, err := e.locateRow(meta, plan.prefix)
					if err != nil {
						return nil, err
					}
					key := e.rowKeyAt(meta, rid, plan.prefix)
					raw, err := e.getKVCluster(sess, tx, key)
					if err == nil {
						pairs = []storage.KVPair{{Key: key, Value: raw}}
					} else if err != storage.ErrNotFound {
						return nil, err
					}
				} else {
					pairs, err = e.scanMergedCluster(sess, tx, e.pkPrefixRanges(meta, plan.prefix))
					if err != nil {
						return nil, err
					}
				}
				rel = &Rel{Cols: make([]string, 0, len(meta.Columns)), Rows: make([][]Value, 0, len(pairs)), Types: make([]string, 0, len(meta.Columns))}
				for _, c := range visibleColumns(meta) {
					rel.Cols = append(rel.Cols, alias+"."+c.Name)
					rel.Types = append(rel.Types, c.Type)
				}
				for _, kv := range pairs {
					row, err := decodeRow(meta, kv.Value)
					if err != nil {
						return nil, err
					}
					rel.Rows = append(rel.Rows, visibleRow(meta, row))
				}
			} else if plan != nil && plan.rangeCol != "" && (plan.rangeLo != nil || plan.rangeHi != nil) {
				// 主键前缀 + 末列字面量范围：有序区间 Seek（与 2244 同语义）。
				pairs, err := e.scanMergedCluster(sess, tx, e.pkRangeRanges(meta, plan))
				if err != nil {
					return nil, err
				}
				rel = &Rel{Cols: make([]string, 0, len(meta.Columns)), Rows: make([][]Value, 0, len(pairs)), Types: make([]string, 0, len(meta.Columns))}
				for _, c := range visibleColumns(meta) {
					rel.Cols = append(rel.Cols, alias+"."+c.Name)
					rel.Types = append(rel.Types, c.Type)
				}
				for _, kv := range pairs {
					row, err := decodeRow(meta, kv.Value)
					if err != nil {
						return nil, err
					}
					ok, err := matchWhere(meta, row, s.Where)
					if err != nil {
						return nil, err
					}
					if ok {
						rel.Rows = append(rel.Rows, visibleRow(meta, row))
					}
				}
			} else if plan != nil {
				// 主键前缀等值：有序 Seek 前缀区间（TPC-C STOCK_LEVEL 外层 stock
				// 按 s_w_id 前缀裁剪 10 万→1 万行，避免 relFromTable 全表物化）。
				pr := e.pkPrefixRanges(meta, plan.prefix)
				dbgLog("pkprefix-prefix3063 table=%s nrange=%d start0=%x end0=%x", meta.Name, len(pr), pr[0].Start, pr[0].End)
				pairs, err := e.scanMergedCluster(sess, tx, pr)
				if err != nil {
					return nil, err
				}
				dbgLog("pkprefix-prefix3063 table=%s got=%d", meta.Name, len(pairs))
				rel = &Rel{Cols: make([]string, 0, len(meta.Columns)), Rows: make([][]Value, 0, len(pairs)), Types: make([]string, 0, len(meta.Columns))}
				for _, c := range visibleColumns(meta) {
					rel.Cols = append(rel.Cols, alias+"."+c.Name)
					rel.Types = append(rel.Types, c.Type)
				}
				for _, kv := range pairs {
					row, err := decodeRow(meta, kv.Value)
					if err != nil {
						return nil, err
					}
					ok, err := matchWhere(meta, row, s.Where)
					if err != nil {
						return nil, err
					}
					if ok {
						rel.Rows = append(rel.Rows, visibleRow(meta, row))
					}
				}
			}
		}
	}
	// v1.15.0 W4：ORDER BY 二级索引顺序扫描——单物理表（无 JOIN/子查询）且 ORDER BY
	// 裸列升序有单列索引时，按索引序全范围扫描构建 Rel（索引序=索引列升序），
	// 后续 WHERE 过滤保序，finishRel 免排序；DISTINCT 相邻去重亦受益。
	// 主键前缀等值/范围（pkCondPlan）已在上方优先，此分支仅兜底无主键条件场景。
	if rel == nil && s.Subquery == nil && len(s.Joins) == 0 && s.OrderBy != nil {
		if m := findTable(tabs, s.From); m != nil {
			if idx := orderByIndexCandidate(m, s); idx != nil {
				rel, err = e.relFromIndexScan(sess, tx, m, alias, idx)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if rel == nil {
		// W3 列投影下推：单物理表（无 JOIN/子查询）时计算所需列，
		// 让 relFromScan 按需解码投影列；其余场景传 nil 走全列解码。
		var needed []string
		if s.Subquery == nil && len(s.Joins) == 0 {
			if m := findTable(tabs, s.From); m != nil {
				needed = e.selectNeeded(m, s)
			}
		}
		rel, err = e.relFromTable(sess, tx, tabs, s.From, alias, s.Subquery, s.SubAlias, needed)
		if err != nil {
			return nil, err
		}
	}
	for i := range s.Joins {
		jc := &s.Joins[i]
		// M12-4：物理表右端且 ON 满足主键点查/前缀范围路由时，跳过右表全量物化，
		// 仅以列定义进入路由 JOIN（路由按区间/点查从存储扫描，避免 order_line
		// 30 万行全表物化）；路由执行时类型不兼容回退在 joinRel 内补物化。
		if jc.Subquery == nil && len(jc.On) > 0 {
			if meta := findTable(tabs, jc.Table); meta != nil {
				if _, ok := planJoinRoute(rel, jc, meta); ok {
					rel, err = e.joinRel(sess, tx, tabs, rel, joinRightSchema(meta, orAlias(jc.Table, jc.Alias)), jc)
					if err != nil {
						return nil, err
					}
					continue
				}
				if _, ok := planJoinRange(rel, jc, meta); ok {
					rel, err = e.joinRel(sess, tx, tabs, rel, joinRightSchema(meta, orAlias(jc.Table, jc.Alias)), jc)
					if err != nil {
						return nil, err
					}
					continue
				}
			}
		}
		right, err := e.relFromTable(sess, tx, tabs, jc.Table, orAlias(jc.Table, jc.Alias), jc.Subquery, jc.Alias, nil)
		if err != nil {
			return nil, err
		}
		// M12：右表物理表且 ON 含右主键等值条件时走分布式路由 JOIN（跨 region/节点
		// 路由点查 + 合并）；否则回退 joinRels 全量拼接，行为与单机一致。
		rel, err = e.joinRel(sess, tx, tabs, rel, right, jc)
		if err != nil {
			return nil, err
		}
	}
	// WHERE 过滤（M12-3：子查询条件预计算一次，行级过滤复用缓存）
	if s.Where != nil && len(s.Where.Conds) > 0 {
		cache := subCache
		if cache == nil {
			cache, err = e.precomputeSubqueries(sess, tx, tabs, rel, s.Where)
			if err != nil {
				return nil, err
			}
		}
		filtered := make([][]Value, 0, len(rel.Rows))
		for _, r := range rel.Rows {
			ok, err := e.matchWhereRelCached(sess, tx, rel, r, s.Where, cache)
			if err != nil {
				return nil, err
			}
			if ok {
				filtered = append(filtered, r)
			}
		}
		rel.Rows = filtered
	}
	hasAgg := false
	hasWindow := false
	for _, c := range s.Columns {
		if c.Window != nil {
			hasWindow = true
			continue
		}
		if c.Agg != "" {
			hasAgg = true
		}
	}
	if len(s.GroupBy) > 0 {
		// P1-10：窗口函数与 GROUP BY 组合合法（窗口在分组聚合结果上计算），
		// 由 groupRel 统一投影并在分组结果行上计算窗口值。
		return e.groupRel(sess, tx, s, rel)
	}
	if hasAgg && hasWindow {
		return nil, &SQLError{Msg: "window function cannot be mixed with aggregate without GROUP BY"}
	}
	if hasAgg {
		return e.aggRel(sess, tx, s, rel)
	}
	// v1.15.0 W4：ORDER BY 索引顺序扫描免排序（Rel 路径）——单物理表（无 JOIN/
	// 子查询）+ ORDER BY 裸列升序命中主键/单列索引候选时，运行时验证扫描序已
	// 按 ORDER BY 列有序（比较基准 compareVal），通过则 finishRel 免排序，
	// 且 DISTINCT 可走相邻去重；验证失败保持 ordered=false 回退原排序路径。
	obOrdered := false
	if s.OrderBy != nil && s.Subquery == nil && len(s.Joins) == 0 {
		if m := findTable(tabs, s.From); m != nil {
			if (orderByScanCandidate(m, s) || orderByIndexCandidate(m, s) != nil) && rowsSortedByRel(rel, rel.Rows, s.OrderBy) {
				obOrdered = true
			}
		}
	}
	return e.finishRel(sess, tx, s, rel, rel.Rows, obOrdered)
}

// matchWhereRel 对 Rel 行执行 WHERE（支持限定列/右列引用/BETWEEN/IN/IN 子查询）。
// 子查询条件走 matchCondRel 动态执行（CASE 分支等无缓存场景兜底）。
func (e *Executor) matchWhereRel(sess *Session, tx txn.Txn, rel *Rel, row []Value, w *Where) (bool, error) {
	return e.matchWhereRelCached(sess, tx, rel, row, w, nil)
}

func (e *Executor) matchCondRel(sess *Session, tx txn.Txn, rel *Rel, row []Value, c *Cond) (bool, error) {
	return e.matchCondRelCached(sess, tx, rel, row, c, nil)
}

// evalCaseRel Rel 行级 CASE 求值（分支条件走 matchWhereRel，支持嵌套/限定列）。
func (e *Executor) evalCaseRel(sess *Session, tx txn.Txn, rel *Rel, row []Value, cc *CaseWhenClause) (Value, error) {
	for i := range cc.Branches {
		ok, err := e.matchWhereRel(sess, tx, rel, row, &cc.Branches[i].Conds)
		if err != nil {
			return Value{}, err
		}
		if ok {
			return cc.Branches[i].Then, nil
		}
	}
	if cc.HasElse {
		return cc.Else, nil
	}
	return StrVal(""), nil
}

// inSubquery 执行 IN 子查询并判断包含（取子查询第一列）。
func (e *Executor) inSubquery(sess *Session, tx txn.Txn, v Value, sub *SelectStmt) (bool, error) {
	res, err := e.execSelect(sess, sub)
	if err != nil {
		return false, err
	}
	for _, r := range res.Rows {
		if len(r) == 0 {
			continue
		}
		if compareVal(v, r[0]) == 0 {
			return true, nil
		}
	}
	return false, nil
}

// aggOperand 取聚合参数的逐行操作数：算术表达式（T40）或列引用（限定/裸）。
func aggOperand(rel *Rel, row []Value, c *SelectColumn) (Value, error) {
	if c.Expr != nil {
		return evalExprForRow(rel, row, c.Expr)
	}
	return relValue(rel, row, qualifiedName(c.Ref, c.Col))
}

// aggValues 计算单列聚合值（COUNT/SUM/AVG/MAX/MIN）。
// P0-4：支持表达式参数与 DISTINCT；COUNT(*) 计全部行，COUNT(expr/col) 仅计非 NULL；
// MAX/MIN/SUM/AVG 跳过 NULL 操作数；DISTINCT 先对非 NULL 操作数去重。
func aggValues(rel *Rel, rows [][]Value, c *SelectColumn) (Value, error) {
	if c.Agg == "COUNT" {
		// COUNT(*)：无列引用也无表达式时按行数计数。
		if c.Col == "" && c.Expr == nil {
			return IntVal(int64(len(rows))), nil
		}
		seen := make(map[string]bool)
		var cnt int64
		for _, r := range rows {
			v, err := aggOperand(rel, r, c)
			if err != nil {
				return Value{}, err
			}
			if v.Kind == "NULL" {
				continue
			}
			if c.Distinct {
				k := v.String()
				if seen[k] {
					continue
				}
				seen[k] = true
			}
			cnt++
		}
		return IntVal(cnt), nil
	}
	// MAX/MIN/SUM/AVG：收集非 NULL 操作数（DISTINCT 去重）。
	vals := make([]Value, 0, len(rows))
	seen := make(map[string]bool)
	for _, r := range rows {
		v, err := aggOperand(rel, r, c)
		if err != nil {
			return Value{}, err
		}
		if v.Kind == "NULL" {
			continue
		}
		if c.Distinct {
			k := v.String()
			if seen[k] {
				continue
			}
			seen[k] = true
		}
		vals = append(vals, v)
	}
	switch c.Agg {
	case "MAX", "MIN":
		if len(vals) == 0 {
			return Value{Kind: "NULL"}, nil
		}
		best := vals[0]
		for _, v := range vals[1:] {
			cmp := compareVal(v, best)
			if (c.Agg == "MAX" && cmp > 0) || (c.Agg == "MIN" && cmp < 0) {
				best = v
			}
		}
		return best, nil
	case "SUM", "AVG":
		// INT：int64 求和；DECIMAL：scaled int64 求和后保持精度。
		if len(vals) == 0 {
			// 空表/空组 SUM/AVG 返回 NULL（SQL 标准：COUNT=0，SUM/AVG=NULL），与 MAX/MIN 一致。
			return Value{Kind: "NULL"}, nil
		}
		dec := false
		var isum int64
		var dsum int64
		for _, v := range vals {
			switch v.Kind {
			case "INT":
				isum += v.I
			case "DECIMAL":
				dec = true
				dsum += v.I
			default:
				return Value{}, &SQLError{Msg: "aggregate on non-numeric column: " + c.Name}
			}
		}
		if dec {
			// v1.12.0 高精度：取首个 DECIMAL 操作数的小数位作为结果 scale（同列同 scale）。
			ds := decScale
			for _, v := range vals {
				if v.Kind == "DECIMAL" {
					ds = decDS(v)
					break
				}
			}
			if c.Agg == "SUM" {
				return decValScaled(dsum, ds), nil
			}
			return decValScaled(dsum/int64(len(vals)), ds), nil
		}
		if c.Agg == "SUM" {
			return IntVal(isum), nil
		}
		return IntVal(isum / int64(len(vals))), nil
	}
	return Value{}, errf("unsupported aggregate %q (supported: COUNT, SUM, AVG, MAX, MIN)", c.Agg)
}

// computeWindows 计算所有窗口列在 rows 上的逐行值（M10 窗口函数）。
// 返回 winVals[i][j] = 第 i 行、第 j 个窗口列的值；winCols 须按 SELECT 列表顺序传入。
// exprEval 用于窗口聚合参数为算术表达式（T40，如 SUM(k+1) OVER ()）；可为 nil。
func computeWindows(rows [][]Value, getter func(row []Value, col string) (Value, error), exprEval func(row []Value, e *Expr) (Value, error), winCols []*SelectColumn) ([][]Value, error) {
	n := len(rows)
	out := make([][]Value, n)
	for i := range out {
		out[i] = make([]Value, len(winCols))
	}
	for j, c := range winCols {
		vals, err := computeWindowCol(rows, getter, exprEval, c)
		if err != nil {
			return nil, err
		}
		for i := range vals {
			out[i][j] = vals[i]
		}
	}
	return out, nil
}

// computeWindowCol 计算单个窗口列在 rows 上的逐行值。
// 默认框架 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW（逐行累计）；
// ROW_NUMBER/RANK/DENSE_RANK 按窗口 ORDER BY 键分配，无 ORDER BY 时保持行序。
func computeWindowCol(rows [][]Value, getter func(row []Value, col string) (Value, error), exprEval func(row []Value, e *Expr) (Value, error), c *SelectColumn) ([]Value, error) {
	ws := c.Window
	vals := make([]Value, len(rows))
	// 分区：按 PARTITION BY 列取值拼键分组；无分区时整个结果集为一组
	parts := make(map[string][]int)
	var order []string
	for i, r := range rows {
		var kb strings.Builder
		for _, p := range ws.PartitionBy {
			v, err := getter(r, p)
			if err != nil {
				return nil, err
			}
			kb.WriteString(v.String())
			kb.WriteByte(0xff)
		}
		k := kb.String()
		if _, ok := parts[k]; !ok {
			parts[k] = []int{}
			order = append(order, k)
		}
		parts[k] = append(parts[k], i)
	}
	for _, k := range order {
		idxs := parts[k]
		// 组内稳定排序：按窗口 ORDER BY 键（支持多列任意 ASC/DESC 组合）；
		// 无 ORDER BY 保持原行序。
		sorted := make([]int, len(idxs))
		copy(sorted, idxs)
		orbs := ws.OrderBys
		if len(orbs) == 0 && ws.OrderBy != nil {
			orbs = []OrderBy{*ws.OrderBy}
		}
		// keyMap：单键（RANGE 帧按首列键值域，parser 已拦截 RANGE 多列）；
		// keyMapMulti：多键（排序与 rank 断点）。
		keyMap := make(map[int]Value, len(idxs))
		keyMapMulti := make(map[int][]Value, len(idxs))
		if len(orbs) > 0 {
			for _, oi := range idxs {
				var ks []Value
				for _, ob := range orbs {
					qname := qualifiedName(ob.Ref, ob.Col)
					v, err := getter(rows[oi], qname)
					if err != nil {
						return nil, err
					}
					ks = append(ks, v)
				}
				keyMapMulti[oi] = ks
				keyMap[oi] = ks[0]
			}
			sort.SliceStable(sorted, func(a, b int) bool {
				ka := keyMapMulti[sorted[a]]
				kb := keyMapMulti[sorted[b]]
				for i, ob := range orbs {
					cmp := compareVal(ka[i], kb[i])
					if cmp != 0 {
						if ob.Desc {
							return cmp > 0
						}
						return cmp < 0
					}
				}
				return false
			})
		}
		// 组内逐行窗口值：rank 为 RANK（带 gap），dense 为 DENSE_RANK（连续编号）
		rank := 1
		dense := 1
		for t, oi := range sorted {
			if t > 0 && len(orbs) > 0 {
				ka := keyMapMulti[sorted[t]]
				kb := keyMapMulti[sorted[t-1]]
				diff := false
				for i := range ka {
					if compareVal(ka[i], kb[i]) != 0 {
						diff = true
						break
					}
				}
				if diff {
					rank = t + 1
					dense++
				}
			}
			var v Value
			switch c.Agg {
			case "ROW_NUMBER":
				v = IntVal(int64(t + 1))
			case "RANK":
				v = IntVal(int64(rank))
			case "DENSE_RANK":
				v = IntVal(int64(dense))
			case "COUNT", "MAX", "MIN", "SUM", "AVG":
				// P0-5：窗口帧内聚合（显式 ROWS/RANGE 帧；无帧时默认
				// ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW）。
				span, err := windowFrameSpan(ws, sorted, t, keyMap, oi)
				if err != nil {
					return nil, err
				}
				v, err = aggWindowSpan(sorted, span, rows, getter, exprEval, c)
				if err != nil {
					return nil, err
				}
			default:
				return nil, errf("unsupported window function %q (supported: ROW_NUMBER, RANK, DENSE_RANK, COUNT, MAX, MIN, SUM, AVG)", c.Agg)
			}
			vals[oi] = v
		}
	}
	return vals, nil
}

// windowFrameSpan 计算排序后位置 t 的窗口帧跨度（P0-5）。
// 返回帧内 sorted 位置索引列表（按排序序升序）；空帧返回 nil。
// 无显式帧：默认 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW。
// ROWS 帧：按物理行窗口；RANGE 帧：按 ORDER BY 键值域窗口（无 ORDER BY 时整分区）。
func windowFrameSpan(ws *WindowSpec, sorted []int, t int, keyMap map[int]Value, curRow int) ([]int, error) {
	if ws.Frame == nil {
		return intRange(0, t), nil
	}
	if ws.Frame.Type == "ROWS" {
		s, e := windowFrameRowsBounds(ws.Frame, len(sorted), t)
		if e < s {
			return nil, nil // 空帧
		}
		return intRange(s, e), nil
	}
	// RANGE 帧
	if ws.OrderBy == nil {
		return intRange(0, len(sorted)-1), nil
	}
	cur := keyMap[curRow]
	lo, hi, err := windowFrameKeyBounds(ws.Frame, cur)
	if err != nil {
		return nil, err
	}
	var span []int
	for j := 0; j < len(sorted); j++ {
		if keyInFrame(keyMap[sorted[j]], cur, lo, hi) {
			span = append(span, j)
		}
	}
	return span, nil
}

// intRange 返回 [s, e] 闭区间的整数序列。
func intRange(s, e int) []int {
	out := make([]int, 0, e-s+1)
	for i := s; i <= e; i++ {
		out = append(out, i)
	}
	return out
}

// windowFrameRowsBounds 计算 ROWS 帧在排序后位置 t 的行闭区间 [s, e]（空帧时 s > e）。
// 边界含 UNBOUNDED PRECEDING/FOLLOWING、CURRENT ROW、n PRECEDING/FOLLOWING。
func windowFrameRowsBounds(f *WindowFrame, n, t int) (int, int) {
	s := 0
	e := n - 1
	switch f.Start.Kind {
	case "UNBOUNDED_PRECEDING":
		s = 0
	case "PRECEDING":
		s = t - int(f.Start.Offset)
		if s < 0 {
			s = 0
		}
	case "CURRENT_ROW":
		s = t
	case "FOLLOWING":
		s = t + int(f.Start.Offset)
		if s >= n {
			s = n // 空帧标记（start > end）
		}
	}
	switch f.End.Kind {
	case "UNBOUNDED_FOLLOWING":
		e = n - 1
	case "FOLLOWING":
		e = t + int(f.End.Offset)
		if e >= n {
			e = n - 1
		}
	case "CURRENT_ROW":
		e = t
	case "PRECEDING":
		e = t - int(f.End.Offset)
		if e < 0 {
			e = -1 // 空帧标记（end < start）
		}
	}
	return s, e
}

// windowFrameKeyBounds 返回 RANGE 帧在键值 cur 下的值域边界（lo/hi 为 nil 表示无界）。
func windowFrameKeyBounds(f *WindowFrame, cur Value) (*Value, *Value, error) {
	var lo, hi *Value
	switch f.Start.Kind {
	case "UNBOUNDED_PRECEDING":
		lo = nil
	case "PRECEDING":
		v, err := rangeOffsetVal(cur, f.Start.Offset, false)
		if err != nil {
			return nil, nil, err
		}
		lo = &v
	case "CURRENT_ROW":
		v := cur
		lo = &v
	case "FOLLOWING":
		v, err := rangeOffsetVal(cur, f.Start.Offset, true)
		if err != nil {
			return nil, nil, err
		}
		lo = &v
	}
	switch f.End.Kind {
	case "UNBOUNDED_FOLLOWING":
		hi = nil
	case "FOLLOWING":
		v, err := rangeOffsetVal(cur, f.End.Offset, true)
		if err != nil {
			return nil, nil, err
		}
		hi = &v
	case "CURRENT_ROW":
		v := cur
		hi = &v
	case "PRECEDING":
		v, err := rangeOffsetVal(cur, f.End.Offset, false)
		if err != nil {
			return nil, nil, err
		}
		hi = &v
	}
	return lo, hi, nil
}

// rangeOffsetVal 对数值键加减偏移（RANGE n PRECEDING/FOLLOWING 值域）。
func rangeOffsetVal(cur Value, offset int64, following bool) (Value, error) {
	switch cur.Kind {
	case "INT":
		if following {
			return IntVal(cur.I + offset), nil
		}
		return IntVal(cur.I - offset), nil
	case "DECIMAL":
		ds := decDS(cur)
		if following {
			return decValScaled(cur.I+offset*pow10Int64(ds), ds), nil
		}
		return decValScaled(cur.I-offset*pow10Int64(ds), ds), nil
	}
	return Value{}, &SQLError{Msg: "RANGE frame with offset requires numeric ORDER BY key"}
}

// keyInFrame 判断排序键 k 是否位于 RANGE 帧值域（以当前行键 cur 为基准）。
// NULL 键仅与 NULL 键同行同帧（等值 NULL 语义）；非 NULL 键帧内排除 NULL。
func keyInFrame(k, cur Value, lo, hi *Value) bool {
	if k.Kind == "NULL" || cur.Kind == "NULL" {
		return k.Kind == "NULL" && cur.Kind == "NULL"
	}
	if lo != nil && compareVal(k, *lo) < 0 {
		return false
	}
	if hi != nil && compareVal(k, *hi) > 0 {
		return false
	}
	return true
}

// aggWindowSpan 在窗口帧内（span 为 sorted 位置索引）计算聚合值（P0-5）。
// 语义对齐全局聚合：COUNT(*) 计全部帧内行、COUNT(expr/col) 跳过 NULL；
// MAX/MIN/SUM/AVG 跳过 NULL 操作数；空帧：COUNT=0，其余为 NULL。
func aggWindowSpan(sorted []int, span []int, rows [][]Value, getter func(row []Value, col string) (Value, error), exprEval func(row []Value, e *Expr) (Value, error), c *SelectColumn) (Value, error) {
	if c.Agg == "COUNT" {
		if c.Col == "" && c.Expr == nil {
			return IntVal(int64(len(span))), nil
		}
		var cnt int64
		for _, j := range span {
			av, err := winOperand(rows[sorted[j]], getter, exprEval, c)
			if err != nil {
				return Value{}, err
			}
			if av.Kind == "NULL" {
				continue
			}
			cnt++
		}
		return IntVal(cnt), nil
	}
	if len(span) == 0 {
		return Value{Kind: "NULL"}, nil
	}
	var vals []Value
	for _, j := range span {
		av, err := winOperand(rows[sorted[j]], getter, exprEval, c)
		if err != nil {
			return Value{}, err
		}
		if av.Kind == "NULL" {
			continue
		}
		vals = append(vals, av)
	}
	switch c.Agg {
	case "MAX", "MIN":
		if len(vals) == 0 {
			return Value{Kind: "NULL"}, nil
		}
		best := vals[0]
		for _, v := range vals[1:] {
			cmp := compareVal(v, best)
			if (c.Agg == "MAX" && cmp > 0) || (c.Agg == "MIN" && cmp < 0) {
				best = v
			}
		}
		return best, nil
	case "SUM", "AVG":
		if len(vals) == 0 {
			return Value{Kind: "NULL"}, nil
		}
		dec := false
		var isum, dsum int64
		for _, v := range vals {
			switch v.Kind {
			case "INT":
				isum += v.I
			case "DECIMAL":
				dec = true
				dsum += v.I
			default:
				return Value{}, &SQLError{Msg: "window aggregate on non-numeric column: " + c.Name}
			}
		}
		if c.Agg == "SUM" {
			if dec {
				return decVal(dsum), nil
			}
			return IntVal(isum), nil
		}
		if dec {
			return decVal(dsum / int64(len(vals))), nil
		}
		return IntVal(isum / int64(len(vals))), nil
	}
	return Value{}, errf("unsupported window function %q", c.Agg)
}

// winOperand 取窗口聚合操作数：表达式参数优先，否则取列值。
func winOperand(row []Value, getter func(row []Value, col string) (Value, error), exprEval func(row []Value, e *Expr) (Value, error), c *SelectColumn) (Value, error) {
	if c.Expr != nil {
		if exprEval == nil {
			return Value{}, errf("window expression requires expression evaluator: " + c.Name)
		}
		return exprEval(row, c.Expr)
	}
	return getter(row, c.Col)
}
func (e *Executor) aggRel(sess *Session, tx txn.Txn, s *SelectStmt, rel *Rel) (*Result, error) {
	res := &Result{}
	out := make([]Value, 0, len(s.Columns))
	for _, c := range s.Columns {
		res.Columns = append(res.Columns, c.Name)
		res.ColTypes = append(res.ColTypes, aggResultType(nil, rel, rel.Rows, &c))
		res.ColNullable = append(res.ColNullable, c.Agg != "COUNT" && len(rel.Rows) == 0)
		v, err := aggValues(rel, rel.Rows, &c)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	res.Rows = append(res.Rows, out)
	// P0-4：无 GROUP BY 的全局聚合 HAVING 过滤（条件引用聚合列/聚合别名，如 HAVING COUNT(*) > 0）。
	if s.Having != nil {
		hrel := &Rel{Cols: append([]string{}, res.Columns...)}
		ok := true
		for _, c := range s.Having.Conds {
			m, err := e.havingMatch(sess, tx, rel, rel.Rows, hrel, out, &c)
			if err != nil {
				return nil, err
			}
			if !m {
				ok = false
				break
			}
		}
		if !ok {
			res.Rows = res.Rows[:0]
		}
	}
	// T40 遗留：全局聚合结果同样应用 LIMIT/OFFSET（先 OFFSET 跳过、再 LIMIT 截断；LIMIT<=0 不截断）。
	res.Rows = applyOffsetLimit(res.Rows, s.Offset, s.Limit)
	return res, nil
}

// applyOffsetLimit 对结果行应用 LIMIT/OFFSET，语义与 finishRel 截断一致：
// 先 OFFSET 跳过、再 LIMIT 截断；LIMIT<=0 不截断。
func applyOffsetLimit(rows [][]Value, offset, limit int) [][]Value {
	if offset > 0 {
		if offset >= len(rows) {
			return rows[:0]
		}
		rows = rows[offset:]
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

// groupRel GROUP BY 分组聚合：每组一行；非聚合列必须属于分组键，取组内首行值。
func (e *Executor) groupRel(sess *Session, tx txn.Txn, s *SelectStmt, rel *Rel) (*Result, error) {
	keys := make([]int, len(s.GroupBy))
	for i, g := range s.GroupBy {
		keys[i] = relColIndex(rel, g)
		if keys[i] < 0 {
			return nil, &SQLError{Msg: "unknown column in GROUP BY: " + g}
		}
	}
	type grp struct{ rows [][]Value }
	groups := make(map[string]*grp)
	var order []string
	for _, r := range rel.Rows {
		var kb strings.Builder
		for _, ki := range keys {
			kb.WriteString(r[ki].String())
			kb.WriteByte(0xff)
		}
		k := kb.String()
		if _, ok := groups[k]; !ok {
			groups[k] = &grp{}
			order = append(order, k)
		}
		groups[k].rows = append(groups[k].rows, r)
	}
	res := &Result{}
	// P0-4：每组分组键列值（与 res.Rows 平行），供 HAVING 表达式/ORDER BY 引用未选列时使用。
	groupKeyVals := make([][]Value, 0, len(order))
	// P1-10：GROUP BY + 窗口函数组合。窗口列先占位，分组循环结束后
	// 在分组结果行（输出行）上计算窗口值（分区/排序/聚合引用输出列名）。
	var winCols []*SelectColumn
	winIdx := make(map[int]int) // 输出列位置 -> winCols 索引
	for i := range s.Columns {
		c := &s.Columns[i]
		res.Columns = append(res.Columns, c.Name)
		if c.Window != nil {
			// 窗口聚合参数为算术表达式时无法在分组输出行上求值（原始行已折叠），给出语义化报错。
			if c.Expr != nil {
				return nil, &SQLError{Msg: "window expression not supported with GROUP BY: " + c.Raw}
			}
			winCols = append(winCols, c)
			winIdx[len(res.Columns)-1] = len(winCols) - 1
			switch c.Agg {
			case "ROW_NUMBER", "RANK", "DENSE_RANK", "COUNT":
				res.ColTypes = append(res.ColTypes, "INT")
			case "SUM", "AVG", "MAX", "MIN":
				res.ColTypes = append(res.ColTypes, aggResultType(nil, rel, rel.Rows, c))
			default:
				res.ColTypes = append(res.ColTypes, "")
			}
			res.ColNullable = append(res.ColNullable, false)
			continue
		}
		if c.Agg != "" {
			// 分组聚合每组至少一行，结果不可空；类型从 rel 首行推断。
			res.ColTypes = append(res.ColTypes, aggResultType(nil, rel, rel.Rows, c))
		} else {
			res.ColTypes = append(res.ColTypes, "")
		}
		res.ColNullable = append(res.ColNullable, false)
	}
	for _, k := range order {
		g := groups[k]
		out := make([]Value, 0, len(s.Columns))
		for i := range s.Columns {
			c := &s.Columns[i]
			if c.Window != nil {
				// 窗口列占位，待全部分组行生成后统一计算。
				out = append(out, Value{Kind: "NULL"})
				continue
			}
			if c.Case != nil {
				v, err := e.evalCaseRel(sess, tx, rel, g.rows[0], c.Case)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
				continue
			}
			if c.Agg == "" {
				if c.Expr != nil {
					// T40：GROUP BY 下表达式投影列取组内首行求值（表达式列名含原始文本）。
					v, err := evalExprForRow(rel, g.rows[0], c.Expr)
					if err != nil {
						return nil, err
					}
					out = append(out, v)
					continue
				}
				if !inGroupBy(s.GroupBy, qualifiedName(c.Ref, c.Col)) {
					return nil, &SQLError{Msg: "column not in GROUP BY: " + c.Name}
				}
				idx := relColIndex(rel, qualifiedName(c.Ref, c.Col))
				if idx < 0 {
					return nil, &SQLError{Msg: "unknown column: " + c.Name}
				}
				out = append(out, g.rows[0][idx])
				continue
			}
			v, err := aggValues(rel, g.rows, c)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		// P0-4：收集分组键列值（与 res.Rows 平行，供 HAVING/ORDER BY 引用未选列）。
		gkv := make([]Value, len(keys))
		for i, ki := range keys {
			gkv[i] = g.rows[0][ki]
		}
		groupKeyVals = append(groupKeyVals, gkv)
		res.Rows = append(res.Rows, out)
	}
	// P1-10：窗口列在分组结果行上统一计算（分区/排序/聚合参数按输出列名解析），
	// 计算完成后替换占位值；LIMIT/OFFSET 在窗口计算之后应用，与 MySQL 语义一致。
	if len(winCols) > 0 {
		colIdx := make(map[string]int, len(res.Columns))
		for i, cn := range res.Columns {
			if _, ok := colIdx[cn]; !ok {
				colIdx[cn] = i
			}
		}
		getter := func(row []Value, col string) (Value, error) {
			idx, ok := colIdx[col]
			if !ok {
				return Value{}, &SQLError{Msg: "unknown column in window: " + col}
			}
			return row[idx], nil
		}
		winVals, err := computeWindows(res.Rows, getter, nil, winCols)
		if err != nil {
			return nil, err
		}
		for ri := range res.Rows {
			for ci, wj := range winIdx {
				res.Rows[ri][ci] = winVals[ri][wj]
			}
		}
	}
	// P0-4：HAVING 过滤分组结果（在窗口计算之后），随后 ORDER BY 排序，最后 LIMIT/OFFSET。
	if s.Having != nil {
		hrel := &Rel{Cols: append(append([]string{}, res.Columns...), s.GroupBy...)}
		kept := res.Rows[:0]
		keptKeys := groupKeyVals[:0]
		for ri, row := range res.Rows {
			ok := true
			for _, c := range s.Having.Conds {
				m, err := e.havingMatch(sess, tx, rel, groups[order[ri]].rows, hrel, append(row, groupKeyVals[ri]...), &c)
				if err != nil {
					return nil, err
				}
				if !m {
					ok = false
					break
				}
			}
			if ok {
				kept = append(kept, row)
				keptKeys = append(keptKeys, groupKeyVals[ri])
			}
		}
		res.Rows = kept
		groupKeyVals = keptKeys
	}
	if err := sortGroupResult(res, s, groupKeyVals); err != nil {
		return nil, err
	}
	// T40 遗留：GROUP BY 分组完成后同样应用 LIMIT/OFFSET（先 OFFSET 跳过、再 LIMIT 截断；LIMIT<=0 不截断）。
	res.Rows = applyOffsetLimit(res.Rows, s.Offset, s.Limit)
	return res, nil
}

// havingMatch 求值 HAVING 单条件。左操作数可为聚合调用（LeftAgg，在组内原始行上聚合）、
// 算术表达式或分组列（在 hrel/hrow 上求值）；右值/运算符与 matchCond 一致。
// hrel 为虚拟关系：输出列 + GROUP BY 键列；hrow = 输出行 + 分组键值行。
func (e *Executor) havingMatch(sess *Session, tx txn.Txn, rel *Rel, groupRows [][]Value, hrel *Rel, hrow []Value, c *Cond) (bool, error) {
	if c.Op == "EXISTS" {
		return false, &SQLError{Msg: "EXISTS not supported in HAVING"}
	}
	var a Value
	var err error
	if c.LeftAgg != nil {
		a, err = aggValues(rel, groupRows, c.LeftAgg)
		if err != nil {
			return false, err
		}
	} else if c.LeftExpr != nil {
		a, err = evalExprForRow(hrel, hrow, c.LeftExpr)
		if err != nil {
			return false, err
		}
	} else {
		q := qualifiedName(c.Ref, c.Col)
		a, err = relValue(hrel, hrow, q)
		if err != nil {
			return false, &SQLError{Msg: "unknown column in HAVING: " + q}
		}
	}
	if c.Sub != nil {
		return false, &SQLError{Msg: "subquery in HAVING not supported"}
	}
	switch c.Op {
	case "=", "!=", "<", ">", "<=", ">=":
		if c.RightRef != "" {
			b, err := relValue(hrel, hrow, c.RightRef)
			if err != nil {
				return false, err
			}
			return matchCond(a, c.Op, b), nil
		}
		if c.RightExpr != nil {
			b, err := evalExprForRow(hrel, hrow, c.RightExpr)
			if err != nil {
				return false, err
			}
			return matchCond(a, c.Op, b), nil
		}
		return matchCond(a, c.Op, c.Val), nil
	case "BETWEEN":
		return compareVal(a, c.Val) >= 0 && compareVal(a, c.Val2) <= 0, nil
	case "LIKE":
		return likeMatch(a.String(), c.Val.String()), nil
	case "IN":
		for _, v := range c.InList {
			if compareVal(a, v) == 0 {
				return true, nil
			}
		}
		return false, nil
	}
	return false, &SQLError{Msg: "unsupported op in HAVING: " + c.Op}
}

// sortGroupResult 对 GROUP BY 分组结果排序（P0-4）。
// 排序键先按输出列名解析，未选中的分组键列回落到 groupKeyVals；
// 保持稳定排序（同键行维持分组首见顺序）。
func sortGroupResult(res *Result, s *SelectStmt, groupKeyVals [][]Value) error {
	if len(s.OrderBys) == 0 && s.OrderBy == nil {
		return nil
	}
	obs := s.OrderBys
	if len(obs) == 0 && s.OrderBy != nil {
		obs = []OrderBy{*s.OrderBy}
	}
	colIdx := make(map[string]int, len(res.Columns))
	for i, cn := range res.Columns {
		if _, ok := colIdx[cn]; !ok {
			colIdx[cn] = i
		}
	}
	groupIdx := make(map[string]int, len(s.GroupBy)*2)
	for i, g := range s.GroupBy {
		if _, ok := groupIdx[g]; !ok {
			groupIdx[g] = i
		}
		base := baseColName(g)
		if _, ok := groupIdx[base]; !ok {
			groupIdx[base] = i
		}
	}
	type sortKey struct {
		idx  int
		desc bool
	}
	keys := make([]sortKey, 0, len(obs))
	for _, ob := range obs {
		q := qualifiedName(ob.Ref, ob.Col)
		if i, ok := colIdx[q]; ok {
			keys = append(keys, sortKey{idx: i, desc: ob.Desc})
			continue
		}
		if i, ok := groupIdx[q]; ok {
			keys = append(keys, sortKey{idx: -(i + 1), desc: ob.Desc}) // 负数索引 = 分组键列
			continue
		}
		// 兼容：ORDER BY 裸名但输出列为限定名（或反之）。
		base := baseColName(q)
		if i, ok := colIdx[base]; ok {
			keys = append(keys, sortKey{idx: i, desc: ob.Desc})
			continue
		}
		if i, ok := groupIdx[base]; ok {
			keys = append(keys, sortKey{idx: -(i + 1), desc: ob.Desc})
			continue
		}
		return &SQLError{Msg: "unknown column in ORDER BY: " + q}
	}
	less := func(ri, rj int) bool {
		for _, k := range keys {
			var a, b Value
			if k.idx >= 0 {
				a, b = res.Rows[ri][k.idx], res.Rows[rj][k.idx]
			} else {
				gi := -k.idx - 1
				a, b = groupKeyVals[ri][gi], groupKeyVals[rj][gi]
			}
			cmp := compareVal(a, b)
			if cmp != 0 {
				if k.desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	}
	sort.SliceStable(res.Rows, less)
	return nil
}

// inGroupBy 判断限定列名是否在 GROUP BY 列表（限定/裸名兼容）。
func inGroupBy(groups []string, qname string) bool {
	for _, g := range groups {
		if g == qname {
			return true
		}
		if !strings.Contains(qname, ".") {
			if g == qname || strings.HasSuffix(g, "."+qname) {
				return true
			}
		} else if strings.Contains(g, ".") && g == qname {
			return true
		}
	}
	return false
}

// finishRel 排序 / LIMIT / 投影（非聚合场景）。
// SELECT * 输出列名为限定名（t.col），避免多表重名歧义。
func (e *Executor) finishRel(sess *Session, tx txn.Txn, s *SelectStmt, rel *Rel, rows [][]Value, ordered bool) (*Result, error) {
	// P0-4：无 GROUP BY/聚合的 HAVING 等价于行过滤（仅支持列/表达式条件；聚合调用报错）。
	if s.Having != nil {
		var filtered [][]Value
		for _, r := range rows {
			ok := true
			for _, c := range s.Having.Conds {
				if c.LeftAgg != nil {
					return nil, &SQLError{Msg: "aggregate in HAVING requires GROUP BY or aggregate select"}
				}
				m, err := e.havingMatch(sess, tx, rel, nil, rel, r, &c)
				if err != nil {
					return nil, err
				}
				if !m {
					ok = false
					break
				}
			}
			if ok {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}
	// 排序（v1.15.0 W4：ordered=true 表示 execSelectRel 已验证扫描序=ORDER BY 序，
	// 跳过全量内存排序；验证失败或未命中候选时保持 false 走原排序路径）。
	if s.OrderBy != nil && !ordered {
		ob := s.OrderBy
		sort.SliceStable(rows, func(i, j int) bool {
			a, _ := relValue(rel, rows[i], qualifiedName(ob.Ref, ob.Col))
			b, _ := relValue(rel, rows[j], qualifiedName(ob.Ref, ob.Col))
			cmp := compareVal(a, b)
			if ob.Desc {
				return cmp > 0
			}
			return cmp < 0
		})
	}
	res := &Result{}
	for _, c := range s.Columns {
		if c.Name == "*" {
			for _, col := range rel.Cols {
				res.Columns = append(res.Columns, col)
			}
		} else {
			res.Columns = append(res.Columns, c.Name)
		}
	}
	var winCols []*SelectColumn
	hasWindow := false
	for i := range s.Columns {
		if s.Columns[i].Window != nil {
			winCols = append(winCols, &s.Columns[i])
			hasWindow = true
		}
	}
	var winVals [][]Value
	if hasWindow {
		var err error
		winVals, err = computeWindows(rows, func(row []Value, col string) (Value, error) {
			return relValue(rel, row, col)
		}, func(row []Value, e *Expr) (Value, error) {
			return evalExprForRow(rel, row, e)
		}, winCols)
		if err != nil {
			return nil, err
		}
	}
	// 逐行投影（T40：投影列可为算术表达式 c.Expr），保持结果集行序。
	projAll := make([][]Value, len(rows))
	for ri, row := range rows {
		var out []Value
		wi := 0
		for _, c := range s.Columns {
			if c.Name == "*" {
				out = append(out, row...)
			} else if c.Window != nil {
				out = append(out, winVals[ri][wi])
				wi++
			} else if c.Case != nil {
				v, err := e.evalCaseRel(sess, tx, rel, row, c.Case)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			} else if c.Expr != nil {
				v, err := evalExprForRow(rel, row, c.Expr)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			} else if c.Func != "" {
				// D7.2：标量函数列（VERSION()）不依赖行上下文。
				v, err := evalScalarFunc(c.Func)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			} else {
				v, err := relValue(rel, row, qualifiedName(c.Ref, c.Col))
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
		}
		projAll[ri] = out
	}
	// SELECT DISTINCT（T40）：对投影后全行去重，保持首次出现顺序。
	// v1.15.0 W4：DISTINCT 免排序——仅当 ordered（行序已按 ORDER BY 列有序）且
	// 单列裸投影与 ORDER BY 列一致时，去重键维度 = 有序维度，相邻去重即可
	// （保留首现，与无序 map 首现语义等价）；多列/表达式/星号投影时行序未必按
	// 去重键有序，相邻去重会漏删重复项，保守走既有 map 路径。
	keep := make([]int, len(projAll))
	for i := range keep {
		keep[i] = i
	}
	if s.Distinct {
		keep = keep[:0]
		adj := false
		if ordered && s.OrderBy != nil && len(s.Columns) == 1 {
			c := &s.Columns[0]
			if c.Name == "*" || c.Expr != nil || c.Case != nil || c.Window != nil || c.Agg != "" || c.Alias != "" || c.Var != "" {
				adj = false
			} else if c.Col == s.OrderBy.Col && c.Ref == s.OrderBy.Ref {
				adj = true
			}
		}
		if adj {
			var prev string
			first := true
			for i, row := range projAll {
				var kb strings.Builder
				for _, v := range row {
					kb.WriteString(v.String())
					kb.WriteByte(0xff)
				}
				key := kb.String()
				if !first && key == prev {
					continue
				}
				first = false
				prev = key
				keep = append(keep, i)
			}
		} else {
			seen := make(map[string]struct{}, len(projAll))
			for i, row := range projAll {
				var kb strings.Builder
				for _, v := range row {
					kb.WriteString(v.String())
					kb.WriteByte(0xff)
				}
				key := kb.String()
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				keep = append(keep, i)
			}
		}
	}
	// LIMIT / OFFSET（M12-2；DISTINCT 时作用于去重后的结果行）
	if s.Offset > 0 {
		if s.Offset >= len(keep) {
			keep = keep[:0]
		} else {
			keep = keep[s.Offset:]
		}
	}
	if s.Limit > 0 && len(keep) > s.Limit {
		keep = keep[:s.Limit]
	}
	for _, ri := range keep {
		res.Rows = append(res.Rows, projAll[ri])
	}
	return res, nil
}

func (e *Executor) execUpdate(sess *Session, s *UpdateStmt) (*Result, error) {
	// P0 HA：写入 fencing 检查（陈旧主 epoch 被拒；开关关闭时放行）。
	if err := e.writeFenceCheck(); err != nil {
		return nil, err
	}
	// P0-1：显式事务跨 region 写暂存（本地/远端分组；语句成功路径末尾挂载）。
	var explLocal []kvOp
	var explRemote map[string][]kvOp
	auto := e.autocommit(sess)
	// v1.8.0 同事务原子性：autocommit 下该表该事件存在触发器时临时提升会话
	// 事务，使触发器体写入随主 DML 一并提交或回滚；函数返回前复位会话状态。
	if auto && !sess.inTx && e.anyTriggerOn(sess, s.Table, "UPDATE") {
		if err := e.beginTx(sess); err != nil {
			return nil, err
		}
		defer e.resetSessionTxState(sess)
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	// T39：UPDATE 表名支持 db.tbl 库前缀；s.Table 归一为表名。
	{
		var tbl string
		var err error
		_, tbl, err = e.resolveTableDB(sess, tx, s.Table)
		if err != nil {
			return nil, err
		}
		s.Table = tbl
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	// 主键等值/前缀快速路径：WHERE 命中主键列等值前缀（单列或复合前缀）
	// → 精确点查 Get / 有序前缀 Seek，避免全表扫描（与 execSelect 对称）。
	var pairs []storage.KVPair
	if plan, perr := e.pkCondPlan(meta, s.Where, nil); perr != nil {
		return nil, perr
	} else if plan != nil {
		if plan.full {
			rid, err := e.locateRow(meta, plan.prefix)
			if err != nil {
				return nil, err
			}
			key := e.rowKeyAt(meta, rid, plan.prefix)
			// M17：跨节点读改写串行化——读旧值前持锁；显式事务锁随事务
			// COMMIT/ROLLBACK 释放（takeRowLocks），autocommit 随语句结束释放。
			dbgLog("UPD prelock sess.inTx=%v stmtBeginTS=%d", sess.inTx, sess.stmtBeginTS)
			dbgLog("UPD full path key=%q plan.full=%v", string(key), plan.full)
			unlockFull, acquiredFull := e.takeRowLocks(sess, string(key))
			defer unlockFull()
			// M18+/M19 修复：锁内当前读刷新（引擎快照 + 事务快照号），后拿锁
			// 事务读到前序已提交值（autocommit 单语句与显式事务写语句均生效）。
			// v1.17.0 D1.1：显式事务已持锁行跳过刷新（acquiredFull=false 且
			// snapshotLockExternal 开启时）；新锁定行仍锁内刷新。开关关闭回退
			// v1.16.0 每次写语句刷新（auto || sess.inTx 恒真条件）。
			if auto || (sess.inTx && (e.SnapshotLockExternal() || acquiredFull)) {
				if err := e.refreshWriteSnapshot(sess, tx); err != nil {
					return nil, err
				}
			}
			raw, err := e.getKVCluster(sess, tx, key)
			if err == nil {
				pairs = []storage.KVPair{{Key: key, Value: raw}}
			} else if err != storage.ErrNotFound {
				return nil, err
			}
		} else {
			// 前缀多行无法预知行 key，按表级哨兵锁串行化（低频路径）。
			unlockPrefix, acquiredPrefix := e.takeRowLocks(sess, "__tbl__:"+meta.Name)
			defer unlockPrefix()
			if auto || (sess.inTx && (e.SnapshotLockExternal() || acquiredPrefix)) {
				if err := e.refreshWriteSnapshot(sess, tx); err != nil {
					return nil, err
				}
			}
			pairs, err = e.scanMergedCluster(sess, tx, e.pkPrefixRanges(meta, plan.prefix))
			if err != nil {
				return nil, err
			}
		}
	}
	if pairs == nil {
		// 全表扫描 + 过滤（跨 region 展开后按内层键序合并）；同样持表级哨兵锁。
		dbgLog("UPD fullscan path tbl=%s", meta.Name)
		unlockScan, acquiredScan := e.takeRowLocks(sess, "__tbl__:"+meta.Name)
		defer unlockScan()
		if auto || (sess.inTx && (e.SnapshotLockExternal() || acquiredScan)) {
			if err := e.refreshWriteSnapshot(sess, tx); err != nil {
				return nil, err
			}
		}
		pairs, err = e.scanMergedCluster(sess, tx, e.rowRanges(meta))
		if err != nil {
			return nil, err
		}
	}
	affected := 0
	var ops []kvOp
	// v1.8.0 行级触发器：存在行级触发器时逐行收集 OLD/NEW 行快照，
	// 供 FOR EACH ROW 触发器按行物化；无行级触发器时零开销（nil）。
	var rowSnaps []triggerRow
	if e.anyRowTrigger(sess, s.Table) {
		rowSnaps = make([]triggerRow, 0)
	}
	// v1.9.0 物化视图变更跟踪：表被 MV 依赖时逐行收集受影响行主键
	//（主键变化时新旧键都失效），语句成功后合并进 dirty 日志。
	var dirtyPks [][]byte
	tracked, terr := e.tableTrackedByMVs(tx, s.Table)
	if terr != nil {
		return nil, terr
	}
	// M10 UPSERT：语句内已计划更新的最终行（唯一索引语句内冲突判定）
	var pending [][]Value
	for _, kv := range pairs {
		row, err := decodeRow(meta, kv.Value)
		if err != nil {
			return nil, err
		}
		ok, err := matchWhere(meta, row, s.Where)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		rid := ridOf(kv.Key)
		pk := pkSuffixOf(meta, innerOf(kv.Key))
		// P0-3：保留旧行副本，供主键变化时的 ON UPDATE 级联匹配子表引用。
		oldRow := append([]Value{}, row...)
		// 维护索引：先删旧索引键（收集 ops，暂不落事务缓冲）
		dops, err := e.delIndexKeyOps(meta, row, pk, rid)
		if err != nil {
			return nil, err
		}
		ops = append(ops, dops...)
		for _, set := range s.Sets {
			idx := colIndex(meta, set.Col)
			if idx < 0 {
				return nil, &SQLError{Msg: "unknown column: " + set.Col}
			}
			var v Value
			if set.Expr != nil {
				// 表达式赋值（col = col + ? 等）：基于当前行求值后再做列类型转换。
				v, err = evalSetExpr(meta, row, set.Expr)
				if err != nil {
					return nil, err
				}
			} else {
				v = set.Val
			}
			v, err = coerceValueScaled(meta.Columns[idx].Type, set.Col, meta.Columns[idx].Scale, v)
			if err != nil {
				return nil, err
			}
			row[idx] = v
		}
		// P0-3 外键：主键列值实际变化时按 ON UPDATE 动作联动引用子表
		// （RESTRICT 报错阻止 / CASCADE 同步子行外键值 / SET NULL 置空）。
		// 主键未变（仅改非主键列）不影响子表引用，直接放行。
		newPK, err := e.pkOf(meta, row)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(pk, newPK) {
			fops, err := e.collectFKUpdateOps(sess, tx, tabs, meta, oldRow, row)
			if err != nil {
				return nil, err
			}
			ops = append(ops, fops...)
		}
		// 更新后行须满足自身引用完整性。
		if err := e.checkFKOnRow(sess, tx, tabs, meta, row); err != nil {
			return nil, err
		}
		raw, err := encodeRow(meta, row)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(pk, newPK) {
			// 主键未变：原地写行
			ops = append(ops, kvOp{Key: kv.Key, Value: raw})
		} else {
			// 主键变化：删旧行键、按新主键路由写新行键（行与索引同 region）
			ops = append(ops, kvOp{Key: kv.Key, Delete: true})
			rid, err = e.locateRow(meta, newPK)
			if err != nil {
				return nil, err
			}
			ops = append(ops, kvOp{Key: e.rowKeyAt(meta, rid, newPK), Value: raw})
		}
		// 记录语句内最终行（用于唯一索引语句内冲突判定）
		e.dropPending(&pending, meta, pk)
		e.replacePending(&pending, meta, pk, row)
		// 写新索引键（收集 ops；唯一索引做磁盘 + pending 双重冲突检查）
		iops, err := e.putIndexKeyOps(sess, tx, meta, row, pk, rid, &pending, nil)
		if err != nil {
			return nil, err
		}
		ops = append(ops, iops...)
		affected++
		if rowSnaps != nil {
			rowSnaps = append(rowSnaps, triggerRow{meta: meta, oldRow: oldRow, newRow: append([]Value{}, row...)})
		}
		if tracked {
			dirtyPks = append(dirtyPks, pk)
			if !bytes.Equal(pk, newPK) {
				dirtyPks = append(dirtyPks, newPK)
			}
		}
	}
	// M15-2/v1.8.0：BEFORE 触发器（写入提交前执行；失败则整个 DML 随事务回滚）。
	if affected > 0 {
		if err := e.fireTriggers(sess, tx, "BEFORE", s.Table, "UPDATE", rowSnaps); err != nil {
			return nil, err
		}
	}
	// v1.8.0：AFTER 触发器移入主事务提交前执行（同事务原子性，见 execInsert）。
	if affected > 0 {
		if err := e.fireTriggers(sess, tx, "AFTER", s.Table, "UPDATE", rowSnaps); err != nil {
			return nil, err
		}
	}
	// v1.9.0：记录物化视图变更键（同事务原子落盘）。
	if tracked && len(dirtyPks) > 0 {
		if err := e.recordMVDirtyKeys(tx, s.Table, dirtyPks); err != nil {
			return nil, err
		}
	}
	if auto {
		if err := e.exec2pcWrite(sess, tx, ops); err != nil {
			return nil, err
		}
	} else {
		// P0-1：显式事务内跨 region 写支持——本地写立即进入会话事务缓冲，
		// 远端写暂存（语句成功后再挂载，COMMIT 统一 2PC 提交）。
		explLocal, explRemote, err = e.splitExplicitOps(sess, tx, ops)
		if err != nil {
			return nil, err
		}
	}
	// M7 自动分裂：写提交后检查本地 region 数据规模（同 execInsert）。
	if err := e.maybeAutoSplit(meta); err != nil {
		return nil, err
	}
	// P0-1：语句成功，挂载显式事务跨 region 写暂存（本地累积供恢复重放与
	// 版本记录，远端按节点累积，COMMIT 统一 2PC；ROLLBACK 直接丢弃）。
	e.stagePendingExplicit(sess, explLocal, explRemote)
	return &Result{AffectedRows: affected}, nil
}

func (e *Executor) execDelete(sess *Session, s *DeleteStmt) (*Result, error) {
	// P0 HA：写入 fencing 检查（陈旧主 epoch 被拒；开关关闭时放行）。
	if err := e.writeFenceCheck(); err != nil {
		return nil, err
	}
	// P0-1：显式事务跨 region 写暂存（本地/远端分组；语句成功路径末尾挂载）。
	var explLocal []kvOp
	var explRemote map[string][]kvOp
	auto := e.autocommit(sess)
	// v1.8.0 同事务原子性：autocommit 下该表该事件存在触发器时临时提升会话
	// 事务，使触发器体写入随主 DML 一并提交或回滚；函数返回前复位会话状态。
	if auto && !sess.inTx && e.anyTriggerOn(sess, s.Table, "DELETE") {
		if err := e.beginTx(sess); err != nil {
			return nil, err
		}
		defer e.resetSessionTxState(sess)
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	// T39：DELETE 表名支持 db.tbl 库前缀；s.Table 归一为表名。
	{
		var tbl string
		var err error
		_, tbl, err = e.resolveTableDB(sess, tx, s.Table)
		if err != nil {
			return nil, err
		}
		s.Table = tbl
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	// 主键等值/前缀快速路径：WHERE 命中主键列等值前缀（单列或复合前缀）
	// → 精确点查 Get / 有序前缀 Seek，避免全表扫描（与 execSelect/execUpdate 对称）。
	var pairs []storage.KVPair
	if plan, perr := e.pkCondPlan(meta, s.Where, nil); perr != nil {
		return nil, perr
	} else if plan != nil {
		if plan.full {
			rid, err := e.locateRow(meta, plan.prefix)
			if err != nil {
				return nil, err
			}
			key := e.rowKeyAt(meta, rid, plan.prefix)
			raw, err := e.getKVCluster(sess, tx, key)
			if err == nil {
				pairs = []storage.KVPair{{Key: key, Value: raw}}
			} else if err != storage.ErrNotFound {
				return nil, err
			}
		} else {
			pairs, err = e.scanMergedCluster(sess, tx, e.pkPrefixRanges(meta, plan.prefix))
			if err != nil {
				return nil, err
			}
		}
	}
	if pairs == nil {
		// 全表扫描 + 过滤（跨 region 展开后按内层键序合并）
		pairs, err = e.scanMergedCluster(sess, tx, e.rowRanges(meta))
		if err != nil {
			return nil, err
		}
	}
	affected := 0
	var ops []kvOp
	// v1.8.0 行级触发器：存在行级触发器时逐行收集 OLD 行快照，
	// 供 FOR EACH ROW 触发器按行物化；无行级触发器时零开销（nil）。
	var rowSnaps []triggerRow
	if e.anyRowTrigger(sess, s.Table) {
		rowSnaps = make([]triggerRow, 0)
	}
	// v1.9.0 物化视图变更跟踪：表被 MV 依赖时逐行收集受影响行主键，
	// 语句成功后合并进 dirty 日志。
	var dirtyPks [][]byte
	tracked, terr := e.tableTrackedByMVs(tx, s.Table)
	if terr != nil {
		return nil, terr
	}
	for _, kv := range pairs {
		row, err := decodeRow(meta, kv.Value)
		if err != nil {
			return nil, err
		}
		ok, err := matchWhere(meta, row, s.Where)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		rid := ridOf(kv.Key)
		pk := pkSuffixOf(meta, innerOf(kv.Key))
		dops, err := e.delIndexKeyOps(meta, row, pkSuffixOf(meta, innerOf(kv.Key)), rid)
		if err != nil {
			return nil, err
		}
		ops = append(ops, dops...)
		// M9/P0-3 外键：删除父表行时检查子表引用——RESTRICT 阻止删除，
		// CASCADE 级联收集子行删除操作（含其索引清理），SET NULL 置空外键列。
		fkops, err := e.collectFKDeleteOps(sess, tx, tabs, meta, row)
		if err != nil {
			return nil, err
		}
		ops = append(ops, fkops...)
		ops = append(ops, kvOp{Key: kv.Key, Delete: true})
		affected++
		if rowSnaps != nil {
			rowSnaps = append(rowSnaps, triggerRow{meta: meta, oldRow: row})
		}
		if tracked {
			dirtyPks = append(dirtyPks, pk)
		}
	}
	// M15-2/v1.8.0：BEFORE 触发器（写入提交前执行；失败则整个 DML 随事务回滚）。
	if affected > 0 {
		if err := e.fireTriggers(sess, tx, "BEFORE", s.Table, "DELETE", rowSnaps); err != nil {
			return nil, err
		}
	}
	// v1.8.0：AFTER 触发器移入主事务提交前执行（同事务原子性，见 execInsert）。
	if affected > 0 {
		if err := e.fireTriggers(sess, tx, "AFTER", s.Table, "DELETE", rowSnaps); err != nil {
			return nil, err
		}
	}
	// v1.9.0：记录物化视图变更键（同事务原子落盘）。
	if tracked && len(dirtyPks) > 0 {
		if err := e.recordMVDirtyKeys(tx, s.Table, dirtyPks); err != nil {
			return nil, err
		}
	}
	if auto {
		if err := e.exec2pcWrite(sess, tx, ops); err != nil {
			return nil, err
		}
	} else {
		// P0-1：显式事务内跨 region 写支持——本地写立即进入会话事务缓冲，
		// 远端写暂存（语句成功后再挂载，COMMIT 统一 2PC 提交）。
		explLocal, explRemote, err = e.splitExplicitOps(sess, tx, ops)
		if err != nil {
			return nil, err
		}
	}
	// M7 自动分裂：写提交后检查本地 region 数据规模（同 execInsert）。
	if err := e.maybeAutoSplit(meta); err != nil {
		return nil, err
	}
	// P0-1：语句成功，挂载显式事务跨 region 写暂存（本地累积供恢复重放与
	// 版本记录，远端按节点累积，COMMIT 统一 2PC；ROLLBACK 直接丢弃）。
	e.stagePendingExplicit(sess, explLocal, explRemote)
	return &Result{AffectedRows: affected}, nil
}

// ---- 辅助 ----

func (e *Executor) buildRow(meta *TableMeta, cols []string, vals []Value, tx txn.Txn) ([]Value, error) {
	if os.Getenv("OPENXDB_BR_DEBUG") != "" {
		fmt.Printf("[br-debug] table=%s cols=%v nvals=%d\n", meta.Name, cols, len(vals))
	}
	if len(cols) == 0 {
		if len(vals) != len(meta.Columns) {
			return nil, &SQLError{Msg: "column count mismatch"}
		}
		out := make([]Value, len(vals))
		copy(out, vals)
		return e.coerceRow(meta, out)
	}
	if len(cols) != len(vals) {
		return nil, &SQLError{Msg: "column count mismatch"}
	}
	out := make([]Value, len(meta.Columns))
	filled := make([]bool, len(meta.Columns))
	for i, c := range cols {
		idx := colIndex(meta, c)
		if idx < 0 {
			return nil, &SQLError{Msg: "unknown column: " + c}
		}
		out[idx] = vals[i]
		filled[idx] = true
	}
	for i, f := range filled {
		if !f {
			// AUTO_INCREMENT 列缺省时分配自增值（MySQL 语义，TPC-C history.hist_id
			// 依赖此行为）；普通列用类型默认值（INT→0、DECIMAL→0、TEXT→''、
			// DATE→当前时间、BLOB→空字节）。
			if meta.Columns[i].AutoIncrement {
				id, err := e.autoIncValue(meta, meta.Columns[i].Name, tx)
				if err != nil {
					return nil, err
				}
				out[i] = IntVal(id)
			} else {
				out[i] = defaultColumnValue(meta.Columns[i])
			}
		}
	}
	return e.coerceRow(meta, out)
}

// autoIncValue 分配 AUTO_INCREMENT 列的下一个值。首次遇到该表列时扫现有数据
// 取 max+1 作为初值，此后从内存计数器单调递增（并发安全）。
func (e *Executor) autoIncValue(meta *TableMeta, col string, tx txn.Txn) (int64, error) {
	key := meta.Name + "\x00" + col
	e.autoIncMu.Lock()
	defer e.autoIncMu.Unlock()
	if e.autoIncNext == nil {
		e.autoIncNext = make(map[string]int64)
	}
	if next, ok := e.autoIncNext[key]; ok {
		e.autoIncNext[key] = next + 1
		return next, nil
	}
	var max int64
	if tx != nil {
		pairs, err := e.scanMerged(tx, e.rowRanges(meta))
		if err != nil {
			return 0, err
		}
		idx := colIndex(meta, col)
		for _, kv := range pairs {
			row, err := decodeRow(meta, kv.Value)
			if err != nil {
				return 0, err
			}
			if v := row[idx]; v.Kind == "INT" && v.I > max {
				max = v.I
			}
		}
	}
	next := max + 1
	e.autoIncNext[key] = next + 1
	return next, nil
}

// defaultColumnValue 返回列类型的 MySQL 兼容默认值。
func defaultColumnValue(c ColumnDef) Value {
	switch c.Type {
	case "INT":
		return Value{Kind: "INT", I: 0}
	case "DECIMAL":
		return Value{Kind: "DECIMAL", I: 0, S: "0"}
	case "DATE":
		return Value{Kind: "DATE", S: time.Now().Format("2006-01-02 15:04:05")}
	case "BLOB":
		return StrVal("")
	default: // TEXT
		return StrVal("")
	}
}

// coerceRow 按列类型做值类型强制（支持 INT/TEXT/DATE/DECIMAL/BLOB）。
func (e *Executor) coerceRow(meta *TableMeta, vals []Value) ([]Value, error) {
	for i, c := range meta.Columns {
		v, err := coerceValueScaled(c.Type, c.Name, c.Scale, vals[i])
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	return vals, nil
}

// pkOf 从行值提取主键字节（INT 定长 / TEXT 原字节；复合主键按 PKCols 多列编码）。
// pkPlan 主键等值前缀计划：prefix 为主键字节前缀；full=true 表示覆盖全部主键列，
// 此时 prefix 即完整行主键，可精确点查。
type pkPlan struct {
	prefix     []byte
	prefixVals []Value // 等值前缀列值（用于 IN 逐值前缀扩展）
	full       bool
	rangeCol   string
	rangeLo    *Value
	rangeHi    *Value
	loIncl     bool
	hiIncl     bool
	inCol      string  // 前缀后下一主键列的 IN 条件（字面量或子查询物化值）
	inVals     []Value // IN 命中值列表
}

// pkCondPlan 分析 WHERE：若包含按主键列顺序排列的连续等值前缀条件，
// 返回主键字节前缀与是否覆盖全部主键列（full=true 可精确点查）。
// 仅限简单 Col=字面量（无限定名/右列引用/CASE/表达式/元组），返回 nil 表示
// 无可用主键等值条件（回退全表/索引路径）。
// normalizePKVal 按列类型把字符串字面量归一化为数值（INT/DECIMAL 列 + STR 字面量），
// 保证主键前缀/范围/IN 编码与行键编码一致；否则字符串 "1" 会按 TEXT 编码（2B 长度前缀）
// 导致 seek 落空、命中 0 行（TPC-C 绑定参数以字符串下发时全表正确、点查全空）。
func normalizePKVal(meta *TableMeta, col string, v Value) Value {
	if v.Kind != "TEXT" {
		return v
	}
	for _, c := range meta.Columns {
		if c.Name != col {
			continue
		}
		switch c.Type {
		case "INT":
			if i, err := strconv.ParseInt(strings.TrimSpace(v.S), 10, 64); err == nil {
				return Value{Kind: "INT", I: i, S: v.S}
			}
		case "DECIMAL":
			if i, err := strconv.ParseInt(strings.TrimSpace(v.S), 10, 64); err == nil {
				return Value{Kind: "DECIMAL", I: i, S: v.S}
			}
		}
		return v
	}
	return v
}

// pkColTypeIn 判断主键列类型是否属于给定 kinds（INT/DECIMAL 等值编码可保序）。
func pkColTypeIn(meta *TableMeta, col string, kinds ...string) bool {
	for _, c := range meta.Columns {
		if c.Name != col {
			continue
		}
		for _, k := range kinds {
			if c.Type == k {
				return true
			}
		}
		return false
	}
	return false
}

func (e *Executor) pkCondPlan(meta *TableMeta, w *Where, subCache map[*Cond]subqueryResult) (*pkPlan, error) {
	if w == nil || len(w.Conds) == 0 {
		return nil, nil
	}
	for i := range w.Conds {
		c := &w.Conds[i]
		dbgLog("pkcond cond[%d] col=%q op=%q ref=%q rightref=%q val=%v", i, c.Col, c.Op, c.Ref, c.RightRef, c.Val)
	}
	var pkCols []string
	if len(meta.PKCols) > 0 {
		pkCols = meta.PKCols
	} else {
		if meta.PK == "" {
			return nil, nil
		}
		pkCols = []string{meta.PK}
	}
	vals := make(map[string]Value, len(pkCols))
	for i := range w.Conds {
		c := &w.Conds[i]
		if c.Op != "=" || c.Sub != nil || c.Ref != "" || c.RightRef != "" || c.LeftCase != nil || c.LeftExpr != nil || c.RightExpr != nil || len(c.TupleLeft) > 0 {
			continue
		}
		for _, pc := range pkCols {
			if c.Col == pc {
				vals[pc] = normalizePKVal(meta, pc, c.Val)
				break
			}
		}
	}
	// 必须是 PKCols 的连续前缀（第 0..k-1 列全部有等值条件）才可安全走有序范围
	prefixVals := make([]Value, 0, len(pkCols))
	for _, pc := range pkCols {
		v, ok := vals[pc]
		if !ok {
			break
		}
		prefixVals = append(prefixVals, v)
	}
	if len(prefixVals) == 0 {
		return nil, nil
	}
	pb, err := pkPrefixBytes(meta, prefixVals)
	if err != nil {
		return nil, err
	}
	plan := &pkPlan{prefix: pb, prefixVals: prefixVals, full: len(prefixVals) == len(pkCols)}
	// 前缀后下一主键列的 IN 条件（字面量或子查询物化值）：逐值前缀点查，
	// 消除全表扫描（TPC-C STOCK_LEVEL：s_w_id 前缀 + s_i_id IN 子查询 → 21 次 seek）。
	if len(prefixVals) < len(pkCols) {
		rc := pkCols[len(prefixVals)]
		for i := range w.Conds {
			c := &w.Conds[i]
			if c.Col != rc || c.Ref != "" || c.RightRef != "" || c.LeftCase != nil || c.LeftExpr != nil || c.RightExpr != nil || len(c.TupleLeft) > 0 || c.Op != "IN" {
				continue
			}
			if c.Sub == nil && len(c.InList) > 0 {
				nv := make([]Value, 0, len(c.InList))
				for _, iv := range c.InList {
					nv = append(nv, normalizePKVal(meta, rc, iv))
				}
				plan.inCol, plan.inVals = rc, nv
			} else if c.Sub != nil && subCache != nil {
				if r, ok := subCache[c]; ok && r.inSet != nil && len(r.inSet.vals) > 0 {
					plan.inCol, plan.inVals = rc, r.inSet.vals
				}
			}
		}
	}
	// 前缀后下一主键列的字面量范围条件：>= / > / <= / <（INT/DECIMAL 才能保序编码）
	if len(prefixVals) < len(pkCols) {
		rc := pkCols[len(prefixVals)]
		if pkColTypeIn(meta, rc, "INT", "DECIMAL") {
			for i := range w.Conds {
				c := &w.Conds[i]
				if c.Col != rc || c.Sub != nil || c.Ref != "" || c.RightRef != "" || c.LeftCase != nil || c.LeftExpr != nil || c.RightExpr != nil || len(c.TupleLeft) > 0 {
					continue
				}
				switch c.Op {
				case ">=":
					v := normalizePKVal(meta, rc, c.Val)
					plan.rangeCol, plan.rangeLo, plan.loIncl = rc, &v, true
				case ">":
					v := normalizePKVal(meta, rc, c.Val)
					plan.rangeCol, plan.rangeLo, plan.loIncl = rc, &v, false
				case "<=":
					v := normalizePKVal(meta, rc, c.Val)
					plan.rangeCol, plan.rangeHi, plan.hiIncl = rc, &v, true
				case "<":
					v := normalizePKVal(meta, rc, c.Val)
					plan.rangeCol, plan.rangeHi, plan.hiIncl = rc, &v, false
				}
			}
		}
	}
	dbgLog("pkcond plan full=%v rangeCol=%q lo=%v hi=%v prefixlen=%d", plan.full, plan.rangeCol, plan.rangeLo, plan.rangeHi, len(plan.prefix))
	return plan, nil
}

func (e *Executor) pkOf(meta *TableMeta, vals []Value) ([]byte, error) {
	if len(meta.PKCols) > 0 {
		return pkBytesMulti(meta, vals)
	}
	for i, c := range meta.Columns {
		if c.Name == meta.PK {
			return pkBytes(meta, vals[i])
		}
	}
	return nil, &SQLError{Msg: "table has no primary key: " + meta.Name}
}

// pkSuffixOf 从行键 s{tableID:8B}{pk} 提取主键后缀。
func pkSuffixOf(meta *TableMeta, rowKey []byte) []byte {
	return rowKey[1+8:]
}

// putIndexKeyOps 为一行产出其所有索引键 Put 操作（值 = 存在标记），
// 不直接写事务；供本地 applyOps 或 2PC 分组提交（M5）使用。
// rid 为行所在 region；索引键与行键同 region（保证回表同前缀）。
// M10 UPSERT：UNIQUE 索引写入前做唯一性检查（磁盘 + 语句内 pending 双通道）；
// ignore 为本次语句将删除的行 pk 集合（REPLACE 删旧插新时跳过被删行的旧键），
// 可为 nil。
func (e *Executor) putIndexKeyOps(sess *Session, tx txn.Txn, meta *TableMeta, row []Value, pk []byte, rid storage.ID, pending *[][]Value, ignore map[string]bool) ([]kvOp, error) {
	ops := make([]kvOp, 0, len(meta.Indexes))
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		val, err := indexColValue(meta, idx, row)
		if err != nil {
			return nil, err
		}
		if idx.Unique {
			// 磁盘唯一性：扫描同值索引键区间，键归属行必须与当前行相同
			start := EncodeIndexKey2(meta.ID, idx.ID, val, nil)
			end := EncodeIndexKey2(meta.ID, idx.ID, append(append([]byte{}, val...), 0xff), nil)
			pairs, err := e.scanMergedCluster(sess, tx, e.indexRanges(meta, idx.ID, start, end))
			if err != nil {
				return nil, err
			}
			for _, kv := range pairs {
				epk := indexPKSuffixLen(innerOf(kv.Key), len(val))
				if bytes.Equal(epk, pk) {
					continue
				}
				if ignore != nil && ignore[string(epk)] {
					continue
				}
				return nil, &SQLError{Msg: "duplicate unique key: " + idx.Name + " on " + meta.Name + "." + idx.Col}
			}
			// 语句内 pending 唯一性：本语句前序已计划写入的最终行
			if pending != nil {
				for _, prow := range *pending {
					ppk, err := e.pkOf(meta, prow)
					if err != nil {
						return nil, err
					}
					if bytes.Equal(ppk, pk) {
						continue
					}
					if ignore != nil && ignore[string(ppk)] {
						continue
					}
					pv, err := indexColValue(meta, idx, prow)
					if err != nil {
						return nil, err
					}
					if bytes.Equal(val, pv) {
						return nil, &SQLError{Msg: "duplicate unique key: " + idx.Name + " on " + meta.Name + "." + idx.Col}
					}
				}
			}
		}
		key := EncodeIndexKey2(meta.ID, idx.ID, val, pk)
		ops = append(ops, kvOp{Key: sharding.EncodeRegionKey(rid, key), Value: []byte{1}})
	}
	return ops, nil
}

// delIndexKeyOps 为一行产出其所有索引键 Delete 操作（rid 语义同
// putIndexKeyOps）。
func (e *Executor) delIndexKeyOps(meta *TableMeta, row []Value, pk []byte, rid storage.ID) ([]kvOp, error) {
	ops := make([]kvOp, 0, len(meta.Indexes))
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		val, err := indexColValue(meta, idx, row)
		if err != nil {
			return nil, err
		}
		key := EncodeIndexKey2(meta.ID, idx.ID, val, pk)
		ops = append(ops, kvOp{Key: sharding.EncodeRegionKey(rid, key), Delete: true})
	}
	return ops, nil
}

// putIndexKeys 为一行写入其所有索引键（值 = 存在标记）。
// rid 为行所在 region；索引键与行键同 region（保证回表同前缀）。
// pending 为语句内已计划写入的最终行（唯一索引回填/更新时做语句内唯一性检查），可为 nil。
func (e *Executor) putIndexKeys(sess *Session, tx txn.Txn, meta *TableMeta, row []Value, pk []byte, rid storage.ID, pending *[][]Value) error {
	ops, err := e.putIndexKeyOps(sess, tx, meta, row, pk, rid, pending, nil)
	if err != nil {
		return err
	}
	return applyOps(tx, ops)
}

// delIndexKeys 删除一行在所有索引上的键（rid 语义同 putIndexKeys）。
func (e *Executor) delIndexKeys(tx txn.Txn, meta *TableMeta, row []Value, pk []byte, rid storage.ID) error {
	ops, err := e.delIndexKeyOps(meta, row, pk, rid)
	if err != nil {
		return err
	}
	return applyOps(tx, ops)
}

// matchWhere 行级条件匹配（AND 组合；无 WHERE 恒真）。
// 支持普通列条件与 CASE 左表达式条件（P2）。
func matchWhere(meta *TableMeta, row []Value, w *Where) (bool, error) {
	if w == nil || len(w.Conds) == 0 {
		return true, nil
	}
	for _, c := range w.Conds {
		if c.LeftCase != nil {
			lv, err := evalCaseM1(meta, row, c.LeftCase)
			if err != nil {
				return false, err
			}
			if c.Op == "LIKE" {
				if !likeMatch(lv.String(), c.Val.String()) {
					return false, nil
				}
				continue
			}
			if !matchCond(lv, c.Op, c.Val) {
				return false, nil
			}
			continue
		}
		if c.Op == "TUPLE_IN" {
			// 行值 IN：(col1, col2) IN ((v1,v2),(v3,v4))——逐列取行值后与每个元组逐列比较。
			hit := false
			for _, tup := range c.TupleVals {
				eq := true
				for k, lc := range c.TupleLeft {
					lvv, err := rowValue(meta, row, lc)
					if err != nil {
						return false, err
					}
					if k >= len(tup) || compareVal(lvv, tup[k]) != 0 {
						eq = false
						break
					}
				}
				if eq {
					hit = true
					break
				}
			}
			if !hit {
				return false, nil
			}
			continue
		}
		v, err := rowValue(meta, row, c.Col)
		if err != nil {
			return false, err
		}
		switch c.Op {
		case "LIKE":
			if !likeMatch(v.String(), c.Val.String()) {
				return false, nil
			}
		case "BETWEEN":
			if compareVal(v, c.Val) < 0 || compareVal(v, c.Val2) > 0 {
				return false, nil
			}
		case "IN":
			if c.Sub != nil {
				return false, nil // 无 tx 上下文，子查询 IN 不在行级过滤中匹配
			}
			hit := false
			for _, iv := range c.InList {
				if compareVal(v, iv) == 0 {
					hit = true
					break
				}
			}
			if !hit {
				return false, nil
			}
		default:
			if !matchCond(v, c.Op, c.Val) {
				return false, nil
			}
		}
	}
	return true, nil
}

func matchCond(a Value, op string, b Value) bool {
	cmp := compareVal(a, b)
	switch op {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case "<":
		return cmp < 0
	case ">":
		return cmp > 0
	case "<=":
		return cmp <= 0
	case ">=":
		return cmp >= 0
	}
	return false
}

// compareVal 值比较：INT/DECIMAL 数值序（跨类型按数值），TEXT/DATE 字节序，
// BLOB 解码 hex 后按原始字节比较（TEXT 侧若为合法 hex 亦解码，宽松匹配）；其余按字符串字典序。
func compareVal(a, b Value) int {
	if a.Kind == "DECIMAL" || b.Kind == "DECIMAL" {
		return compareDec(a, b)
	}
	if a.Kind == "INT" && b.Kind == "INT" {
		if a.I < b.I {
			return -1
		}
		if a.I > b.I {
			return 1
		}
		return 0
	}
	if a.Kind == "BLOB" || b.Kind == "BLOB" {
		return bytes.Compare(blobBytes(a), blobBytes(b))
	}
	as := a.String()
	bs := b.String()
	return bytes.Compare([]byte(as), []byte(bs))
}

// blobBytes 返回值的字节形态：BLOB 解码 hex；TEXT 若为合法 hex 亦解码，否则原字符串字节。
func blobBytes(v Value) []byte {
	s := strings.ToUpper(v.S)
	if b, err := decodeHex(s); err == nil {
		return b
	}
	return []byte(v.S)
}

// compareDec DECIMAL 与 INT/DECIMAL 数值比较（v1.12.0：跨 scale 对齐比较）。
func compareDec(a, b Value) int {
	if a.Kind == "DECIMAL" && b.Kind == "DECIMAL" {
		dsA, dsB := decDS(a), decDS(b)
		ds := dsA
		if dsB > ds {
			ds = dsB
		}
		av := a.I * pow10Int64(ds-dsA)
		bv := b.I * pow10Int64(ds-dsB)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	if a.Kind == "DECIMAL" {
		return compareDecScaledInt(a.I, decDS(a), b.I)
	}
	return -compareDecScaledInt(b.I, decDS(b), a.I)
}

func colIndex(meta *TableMeta, name string) int {
	for i, c := range meta.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [24]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// ---- P2/M13：慢查询与运维语句执行 ----

// slowThresholdDefault 默认慢查询阈值（毫秒，M13 默认 100ms）。
const slowThresholdDefault int64 = 100

// groupCommitWindowMsDefault v1.14.0 P1 默认 WAL 组提交定时刷盘窗口（毫秒）。
// 0.5ms 与 wal_impl.defaultGroupCommitWindow(500µs) 保持一致。
const groupCommitWindowMsDefault = 0.5

// slowQueryMaxDefault 内存慢查询记录默认上限（可 SetSlowQueryMax 调整）。
const slowQueryMaxDefault = 64

// shutdownTimeoutDefaultSec M16-2 优雅停机默认超时（秒）。
const shutdownTimeoutDefaultSec int64 = 30

// SetShutdownTimeoutSec 设置优雅停机超时（秒，M16-2）。<=0 视为默认 30s。
// 运行时调用即时生效（SET/SET PERSIST shutdown_timeout 的 apply 入口）。
func (e *Executor) SetShutdownTimeoutSec(sec int64) {
	if sec < 1 {
		sec = shutdownTimeoutDefaultSec
	}
	e.shutdownTimeoutSec = sec
}

// ShutdownTimeoutSec 返回当前优雅停机超时（秒，M16-2）。
func (e *Executor) ShutdownTimeoutSec() int64 { return e.shutdownTimeoutSec }

// timeNowMs 当前 Unix 毫秒。
func timeNowMs() int64 {
	return time.Now().UnixMilli()
}

// timeNow 本地时间文本 YYYY-MM-DD HH:MM:SS。
func timeNow() string {
	now := time.Now()
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d",
		now.Year(), int(now.Month()), now.Day(), now.Hour(), now.Minute(), now.Second())
}

// execCreateView CREATE VIEW：校验重名（表/视图）、定义引用的表/视图存在、
// 循环引用防护（含间接环），随后持久化视图目录。
func (e *Executor) execCreateView(sess *Session, s *CreateViewStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	views, err := loadViews(tx.Get)
	if err != nil {
		return nil, err
	}
	if findTable(tabs, s.Name) != nil {
		return nil, &SQLError{Msg: "view name conflicts with table: " + s.Name}
	}
	if findView(views, s.Name) != nil {
		// T41：CREATE VIEW IF NOT EXISTS——视图已存在时静默跳过
		if s.IfNotExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "view already exists: " + s.Name}
	}
	if s.Select == nil {
		return nil, &SQLError{Msg: "view definition missing"}
	}
	// 循环引用静态防护：直接引用自身或经已有视图链间接引用自身均拒绝
	// （先于存在性检查，保证自引用报循环错误而非 table not exists）
	if err := checkViewCycle(views, s.Name, s.Select); err != nil {
		return nil, err
	}
	// 定义引用的每个名字必须是真实表或已存在视图
	for _, n := range viewRefNames(s.Select) {
		if findTable(tabs, n) == nil && findView(views, n) == nil {
			return nil, &SQLError{Msg: "table not exists: " + n}
		}
	}
	views = append(views, &ViewMeta{Name: s.Name, Select: s.Select})
	raw, err := saveViews(views)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(viewsKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{}, nil
}

// execDropView DROP VIEW [IF EXISTS]：被其它视图引用的视图禁止删除（RESTRICT）。
func (e *Executor) execDropView(sess *Session, s *DropViewStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	views, err := loadViews(tx.Get)
	if err != nil {
		return nil, err
	}
	if findView(views, s.Name) == nil {
		if s.IfExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "view not exists: " + s.Name}
	}
	// 被其它视图引用时禁止删除，避免悬空引用
	for _, v := range views {
		if v.Name == s.Name {
			continue
		}
		for _, n := range viewRefNames(v.Select) {
			if n == s.Name {
				return nil, &SQLError{Msg: "cannot drop view " + s.Name + ": referenced by view " + v.Name}
			}
		}
	}
	var kept []*ViewMeta
	for _, v := range views {
		if v.Name != s.Name {
			kept = append(kept, v)
		}
	}
	raw, err := saveViews(kept)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(viewsKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{}, nil
}

// execShowViews SHOW VIEWS：列出全部视图名与定义摘要。
func (e *Executor) execShowViews(sess *Session) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	views, err := loadViews(tx.Get)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []string{"view", "definition"}}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	for _, v := range views {
		res.Rows = append(res.Rows, []Value{
			StrVal(v.Name),
			StrVal(viewSummary(v.Select)),
		})
	}
	return res, nil
}

// expandViews 将 SELECT 中 FROM / JOIN 引用的视图展开为定义（子查询）。
// 展开采用"定义副本"（JSON 深拷贝），避免共享 AST 污染；
// stack 记录当前展开链上的视图名，遇到重复即判定循环引用。
// ---- M10 CTE ----

// expandCTEs 展开语句级 CTE（WITH 子句）：将 FROM/JOIN 中引用 CTE 名的
// 表替换为 CTE 定义子查询（深拷贝副本）。支持单/多 CTE、CTE 引用 CTE
// （兄弟互引）、与视图互引（视图展开先于本层执行时由 execSelect 递归处理）。
// 循环引用（CTE 直接或间接引用自身）报 cyclic CTE reference。
func (e *Executor) expandCTEs(s *SelectStmt) error {
	return e.expandCTEsInner(make(map[string]*SelectStmt), s, nil)
}

// expandCTEsInner 递归展开：先注册本层 CTE（语句内作用域，同名覆盖），
// 再替换 FROM/JOIN 引用并带栈递归展开 CTE 定义，检测循环引用。
func (e *Executor) expandCTEsInner(ctes map[string]*SelectStmt, s *SelectStmt, stack []string) error {
	if s == nil {
		return nil
	}
	for _, c := range s.CTEs {
		ctes[c.Name] = c.Select
	}
	if s.From != "" {
		if def, ok := ctes[s.From]; ok {
			if cyclicCTE(stack, s.From) {
				return &SQLError{Msg: "cyclic CTE reference: " + s.From}
			}
			sub, err := cloneSelect(def)
			if err != nil {
				return err
			}
			tbl := s.From
			s.Subquery = sub
			s.SubAlias = orAlias(tbl, s.FromAlias)
			s.From = ""
			s.FromAlias = ""
			if err := e.expandCTEsInner(ctes, sub, append(stack, tbl)); err != nil {
				return err
			}
		}
	}
	if s.Subquery != nil {
		if err := e.expandCTEsInner(ctes, s.Subquery, stack); err != nil {
			return err
		}
	}
	for i := range s.Joins {
		jc := &s.Joins[i]
		if jc.Subquery == nil && jc.Table != "" {
			if def, ok := ctes[jc.Table]; ok {
				if cyclicCTE(stack, jc.Table) {
					return &SQLError{Msg: "cyclic CTE reference: " + jc.Table}
				}
				sub, err := cloneSelect(def)
				if err != nil {
					return err
				}
				tbl := jc.Table
				jc.Subquery = sub
				jc.Table = ""
				if err := e.expandCTEsInner(ctes, sub, append(stack, tbl)); err != nil {
					return err
				}
			}
		} else if jc.Subquery != nil {
			if err := e.expandCTEsInner(ctes, jc.Subquery, stack); err != nil {
				return err
			}
		}
	}
	// 本层 CTE 定义展开（stack 推进以检测直接/间接循环引用）
	for i := range s.CTEs {
		c := &s.CTEs[i]
		if err := e.expandCTEsInner(ctes, c.Select, append(stack, c.Name)); err != nil {
			return err
		}
	}
	return nil
}

// cyclicCTE 判断 CTE 名是否已在展开栈中（即形成循环）。
func cyclicCTE(stack []string, name string) bool {
	for _, n := range stack {
		if n == name {
			return true
		}
	}
	return false
}

func (e *Executor) expandViews(tx txn.Txn, s *SelectStmt) error {
	if s == nil {
		return nil
	}
	views, err := loadViews(tx.Get)
	if err != nil {
		return err
	}
	return e.expandViewsInner(views, s, nil)
}

func (e *Executor) expandViewsInner(views []*ViewMeta, s *SelectStmt, stack []string) error {
	if s == nil {
		return nil
	}
	if s.From != "" {
		if v := findView(views, s.From); v != nil {
			if cyclicView(stack, v.Name) {
				return &SQLError{Msg: "cyclic view reference: " + v.Name}
			}
			sub, err := cloneSelect(v.Select)
			if err != nil {
				return err
			}
			s.Subquery = sub
			s.SubAlias = s.From
			s.From = ""
			if err := e.expandViewsInner(views, sub, append(stack, v.Name)); err != nil {
				return err
			}
		}
	}
	if s.Subquery != nil {
		if err := e.expandViewsInner(views, s.Subquery, stack); err != nil {
			return err
		}
	}
	for i := range s.Joins {
		jc := &s.Joins[i]
		if jc.Subquery == nil && jc.Table != "" {
			if v := findView(views, jc.Table); v != nil {
				if cyclicView(stack, v.Name) {
					return &SQLError{Msg: "cyclic view reference: " + v.Name}
				}
				sub, err := cloneSelect(v.Select)
				if err != nil {
					return err
				}
				jc.Subquery = sub
				jc.Table = ""
				if err := e.expandViewsInner(views, sub, append(stack, v.Name)); err != nil {
					return err
				}
			}
		} else if jc.Subquery != nil {
			if err := e.expandViewsInner(views, jc.Subquery, stack); err != nil {
				return err
			}
		}
	}
	return nil
}

// cyclicView 判断 view 名是否已在展开栈中（即形成循环）。
func cyclicView(stack []string, name string) bool {
	for _, n := range stack {
		if n == name {
			return true
		}
	}
	return false
}

// execShowTables SHOW TABLES：列出全部表（表名 / 列定义 / 主键 / 索引概要）。
func (e *Executor) execShowTables(sess *Session) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []string{"table", "columns", "pk", "indexes"}}
	// T39：SHOW TABLES 仅列出会话当前库（USE 切换）中的表；空/默认归 default。
	cur := sess.CurrentDB
	if cur == "" {
		cur = defaultDB
	}
	sort.Slice(tabs, func(i, j int) bool { return tabs[i].Name < tabs[j].Name })
	for _, t := range tabs {
		if tableDB(t) != cur {
			continue
		}
		cols := make([]string, len(t.Columns))
		for i, c := range t.Columns {
			cols[i] = c.Name + ":" + c.Type
		}
		idx := make([]string, len(t.Indexes))
		for i, in := range t.Indexes {
			idx[i] = in.Name + "(" + in.Col + ")"
		}
		res.Rows = append(res.Rows, []Value{
			StrVal(t.Name),
			StrVal(strings.Join(cols, ", ")),
			StrVal(t.PK),
			StrVal(strings.Join(idx, ", ")),
		})
	}
	return res, nil
}

// execCreateDatabase CREATE DATABASE [IF NOT EXISTS] <name>（T39）。
// 多库元数据仅登记库目录（m:databases），不触及表目录；default 库恒存在不可重复创建。
// 与用户目录一致：直接写库目录键并经 WAL 原子持久化（autocommit 提交 / 显式事务延迟提交）。
func (e *Executor) execCreateDatabase(sess *Session, s *CreateDatabaseStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	if err := normalizeDBName(s.Name); err != nil {
		return nil, err
	}
	dbs, err := loadDatabases(tx.Get)
	if err != nil {
		return nil, err
	}
	// default 库恒存在（单库模型兼容入口），不允许重复登记。
	exists := s.Name == defaultDB || findDatabase(dbs, s.Name)
	if exists {
		if s.IfNotExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "database already exists: " + s.Name}
	}
	dbs = append(dbs, s.Name)
	raw, err := saveDatabases(dbs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(dbKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execUse USE <db>（T39）：切换会话级默认库；default 恒存在。
func (e *Executor) execUse(sess *Session, s *UseStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	dbs, err := loadDatabases(tx.Get)
	if err != nil {
		return nil, err
	}
	if s.Name != defaultDB && !findDatabase(dbs, s.Name) {
		return nil, &SQLError{Msg: "database not exists: " + s.Name}
	}
	sess.CurrentDB = s.Name
	return &Result{}, nil
}

// execDropDatabase DROP DATABASE [IF EXISTS] <name>（T39）：级联删除库内所有表。
// 删除行/索引键/外键联动清理，与 execDropTable 同一套删除语义；default 库禁止删除。
func (e *Executor) execDropDatabase(sess *Session, s *DropDatabaseStmt) (*Result, error) {
	if s.Name == defaultDB {
		return nil, &SQLError{Msg: "cannot drop default database"}
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	dbs, err := loadDatabases(tx.Get)
	if err != nil {
		return nil, err
	}
	if !findDatabase(dbs, s.Name) {
		if s.IfExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "database not exists: " + s.Name}
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	// 级联删除：归属该库的全部表（行/索引键/目录）。
	var removed int
	var kept []*TableMeta
	for _, t := range tabs {
		if tableDB(t) != s.Name {
			kept = append(kept, t)
			continue
		}
		removed++
		// M9 外键联动清理：被删表作为 RefTable 时移除引用。
		for _, ot := range tabs {
			if ot.Name == t.Name && tableDB(ot) == tableDB(t) {
				continue
			}
			var nfk []ForeignKey
			for _, fk := range ot.ForeignKeys {
				if fk.RefTable == t.Name {
					continue
				}
				nfk = append(nfk, fk)
			}
			ot.ForeignKeys = nfk
		}
		pairs, err := e.scanMerged(tx, e.rowRanges(t))
		if err != nil {
			return nil, err
		}
		for _, kv := range pairs {
			if err := tx.Delete(kv.Key); err != nil {
				return nil, err
			}
		}
		if len(t.Indexes) > 0 {
			var ipairs []storage.KVPair
			for i := range t.Indexes {
				ps, err := e.scanMerged(tx, e.indexTableRanges(t, t.Indexes[i].ID))
				if err != nil {
					return nil, err
				}
				ipairs = append(ipairs, ps...)
			}
			for _, kv := range ipairs {
				if err := tx.Delete(kv.Key); err != nil {
					return nil, err
				}
			}
		}
	}
	// 移除库目录。
	var ndbs []string
	for _, d := range dbs {
		if d != s.Name {
			ndbs = append(ndbs, d)
		}
	}
	raw, err := saveDatabases(ndbs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(dbKey, raw); err != nil {
		return nil, err
	}
	raw, err = saveTables(kept)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: removed}, nil
}

// resolveTableDB 解析表引用（可带 db.tbl 库前缀）：返回归属库与表名，并校验库存在。
// 无前缀时归属会话当前库；default 恒存在不校验；information_schema.* 保留全名由调用方拦截。
func (e *Executor) resolveTableDB(sess *Session, tx txn.Txn, name string) (string, string, error) {
	db := sess.CurrentDB
	if db == "" {
		db = defaultDB
	}
	tbl := name
	if i := strings.IndexByte(name, '.'); i >= 0 {
		db = name[:i]
		tbl = name[i+1:]
		if strings.Contains(tbl, ".") {
			return "", "", &SQLError{Msg: "invalid table name: " + name}
		}
	}
	if db != defaultDB {
		dbs, err := loadDatabases(tx.Get)
		if err != nil {
			return "", "", err
		}
		if !findDatabase(dbs, db) {
			return "", "", &SQLError{Msg: "database not exists: " + db}
		}
	}
	return db, tbl, nil
}

// tableDB 表归属库（空 = default，兼容存量数据）。
func tableDB(t *TableMeta) string {
	if t.DB == "" {
		return defaultDB
	}
	return t.DB
}

// execShowDatabases SHOW DATABASES（T39）：默认库 default 前置 + 已创建库排序可见。
func (e *Executor) execShowDatabases(sess *Session) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	dbs, err := loadDatabases(tx.Get)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []string{"database"}}
	for _, d := range allDatabases(dbs) {
		res.Rows = append(res.Rows, []Value{StrVal(d)})
	}
	return res, nil
}

// execShowIndex SHOW INDEX FROM t：列出指定表全部二级索引。
func (e *Executor) execShowIndex(sess *Session, s *ShowIndexStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	res := &Result{Columns: []string{"table", "index", "column", "id", "unique"}}
	for _, in := range meta.Indexes {
		res.Rows = append(res.Rows, []Value{
			StrVal(meta.Name),
			StrVal(in.Name),
			StrVal(in.Col),
			IntVal(int64(in.ID)),
			StrVal("NO"),
		})
	}
	return res, nil
}

// execShowCreate SHOW CREATE TABLE t（D7.4）：基于表存储元数据重建 CREATE TABLE
// 语句（列定义/类型精度/自增/主键/二级索引/外键），供 mysqldump 与第三方工具读取。
// 输出列与 MySQL 一致：Table | Create Table。
func (e *Executor) execShowCreate(sess *Session, s *ShowCreateStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Table}
	}
	res := &Result{Columns: []string{"Table", "Create Table"}}
	res.Rows = append(res.Rows, []Value{StrVal(meta.Name), StrVal(buildCreateTableSQL(meta))})
	return res, nil
}

// buildCreateTableSQL 基于 TableMeta 重建 CREATE TABLE 文本（D7.4）。
// 存储元数据为唯一事实源：类型经 parser 归一化（INTEGER→INT、VARCHAR/CHAR→TEXT、
// TIMESTAMP→DATE、STRING→TEXT），精度参数保留在 Params 中原样回放。
func buildCreateTableSQL(meta *TableMeta) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE `" + meta.Name + "` (\n")
	lines := make([]string, 0, len(meta.Columns)+len(meta.Indexes)+len(meta.ForeignKeys)+1)
	for _, c := range meta.Columns {
		line := "  `" + c.Name + "` " + c.Type
		if len(c.Params) > 0 {
			ps := make([]string, len(c.Params))
			for i, p := range c.Params {
				ps[i] = strconv.Itoa(p)
			}
			line += "(" + strings.Join(ps, ",") + ")"
		}
		if c.AutoIncrement {
			line += " AUTO_INCREMENT"
		}
		lines = append(lines, line)
	}
	// 主键：复合主键 PKCols 优先，单列回退 PK。
	if len(meta.PKCols) > 0 {
		lines = append(lines, "  PRIMARY KEY (`"+strings.Join(meta.PKCols, "`,`")+"`)")
	} else if meta.PK != "" {
		lines = append(lines, "  PRIMARY KEY (`"+meta.PK+"`)")
	}
	// 二级索引（含 UNIQUE）。
	for _, in := range meta.Indexes {
		cols := in.Cols
		if len(cols) == 0 {
			cols = []string{in.Col}
		}
		kind := "KEY"
		if in.Unique {
			kind = "UNIQUE KEY"
		}
		lines = append(lines, "  "+kind+" `"+in.Name+"` (`"+strings.Join(cols, "`,`")+"`)")
	}
	// 外键（约束名以列名派生，元数据未持久化命名）。
	for _, fk := range meta.ForeignKeys {
		line := "  CONSTRAINT `fk_" + strings.Join(fk.Columns, "_") + "` FOREIGN KEY (`" +
			strings.Join(fk.Columns, "`,`") + "`) REFERENCES `" + fk.RefTable + "` (`" +
			strings.Join(fk.RefColumns, "`,`") + "`)"
		if fk.OnDelete != "" {
			line += " ON DELETE " + fk.OnDelete
		}
		if fk.OnUpdate != "" {
			line += " ON UPDATE " + fk.OnUpdate
		}
		lines = append(lines, line)
	}
	b.WriteString(strings.Join(lines, ",\n"))
	b.WriteString("\n)")
	return b.String()
}

// execShowSlowQueries SHOW SLOWQUERIES [SUMMARY [TOP <n>]]：
// 无 SUMMARY 查看内存慢查询记录（新->旧）；
// SUMMARY 输出按阈值区间汇总 + TOP N 最慢明细报表（D3.2 slowlog 闭环）。
// M13 列：time | user | duration_ms | rows | error | sql。
func (e *Executor) execShowSlowQueries(sess *Session, s *ShowSlowQueriesStmt) (*Result, error) {
	if s.Summary {
		return e.execShowSlowSummary(s.TopN)
	}
	recs := e.SlowQueries()
	res := &Result{Columns: []string{"time", "user", "duration_ms", "rows", "error", "sql"}}
	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		res.Rows = append(res.Rows, []Value{
			StrVal(r.StartAt),
			StrVal(r.User),
			IntVal(r.Duration),
			IntVal(r.Rows),
			StrVal(r.Err),
			StrVal(r.SQL),
		})
	}
	return res, nil
}

// slowBucketBounds 慢查询阈值区间上界（毫秒，左闭右开；最后一桶 >= 上界）。
// D3.2：SHOW SLOWQUERIES SUMMARY 按区间汇总记录分布。
var slowBucketBounds = []int64{1, 10, 100, 500, 1000, 5000}

// slowBucketLabel 返回区间标签："[1,10)" / "[5000,+)" 等。
func slowBucketLabel(lo, hi int64, last bool) string {
	if last {
		return "[" + itoa64(lo) + ",+)"
	}
	return "[" + itoa64(lo) + "," + itoa64(hi) + ")"
}

func itoa64(n int64) string { return strconv.FormatInt(n, 10) }

// execShowSlowSummary SHOW SLOWQUERIES SUMMARY [TOP <n>]：
// 按阈值区间汇总慢查询记录 + TOP N 最慢明细报表（D3.2 slowlog 闭环）。
// 输出 metric|value 键值表（与 SHOW STATS 同风格）：
//
//	summary.threshold_ms           当前慢查询阈值
//	summary.records                记录总数
//	summary.avg_ms / summary.max_ms 全部记录的平均/最大耗时
//	bucket.[lo,hi) / bucket.[lo,+) count=N avg_ms=X max_ms=Y  各区间汇总
//	top.<rank>                     TOP N 最慢明细（time|user|duration_ms|rows|error|sql）
//
// TOP N 默认 10（TopN<=0），上限 100（超出截断）。
func (e *Executor) execShowSlowSummary(topN int) (*Result, error) {
	if topN <= 0 {
		topN = 10
	}
	if topN > 100 {
		topN = 100
	}
	recs := e.SlowQueries()
	res := &Result{Columns: []string{"metric", "value"}}
	appendKV := func(k string, v Value) {
		res.Rows = append(res.Rows, []Value{StrVal(k), v})
	}
	appendKV("summary.threshold_ms", IntVal(e.SlowQueryThreshold()))
	appendKV("summary.records", IntVal(int64(len(recs))))
	if len(recs) == 0 {
		return res, nil
	}
	var sum int64
	var maxMs int64
	for _, r := range recs {
		sum += r.Duration
		if r.Duration > maxMs {
			maxMs = r.Duration
		}
	}
	appendKV("summary.avg_ms", IntVal(sum/int64(len(recs))))
	appendKV("summary.max_ms", IntVal(maxMs))
	// 阈值区间桶汇总（记录为慢查询即 >= 阈值，通常 >= 1ms；<1ms 桶恒 0 不输出）
	buckets := make([]int64, len(slowBucketBounds)+1)
	bucketSum := make([]int64, len(slowBucketBounds)+1)
	bucketMax := make([]int64, len(slowBucketBounds)+1)
	for _, r := range recs {
		idx := len(buckets) - 1 // 默认最后一桶（>= 最后上界）
		for i, hi := range slowBucketBounds {
			if r.Duration < hi {
				idx = i
				break
			}
		}
		buckets[idx]++
		bucketSum[idx] += r.Duration
		if r.Duration > bucketMax[idx] {
			bucketMax[idx] = r.Duration
		}
	}
	for i := range buckets {
		if buckets[i] == 0 {
			continue
		}
		var lo int64
		if i > 0 {
			lo = slowBucketBounds[i-1]
		}
		last := i == len(buckets)-1
		label := slowBucketLabel(lo, slowBucketBounds[i], last)
		appendKV("bucket."+label, StrVal(fmt.Sprintf("count=%d avg_ms=%d max_ms=%d",
			buckets[i], bucketSum[i]/buckets[i], bucketMax[i])))
	}
	// TOP N 最慢明细（耗时降序，稳定排序）
	order := make([]int, len(recs))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return recs[order[a]].Duration > recs[order[b]].Duration })
	if topN > len(order) {
		topN = len(order)
	}
	for rank, idx := range order[:topN] {
		r := recs[idx]
		appendKV(fmt.Sprintf("top.%d", rank+1), StrVal(fmt.Sprintf("%s|%s|%dms|rows=%d|%s|%s",
			r.StartAt, r.User, r.Duration, r.Rows, r.Err, r.SQL)))
	}
	return res, nil
}

// execSet SET [PERSIST] <name> = <value>：会话/服务变量设置（M9 查询缓存开关；
// M13 慢查询阈值与落盘开关，运行时变更即时生效；M16-1 起 PERSIST 应用后写入
// 持久化配置文件，重启仍生效）。校验与应用统一走配置注册表（config.go），
// 非法键报 unknown session variable，非法值/类型按配置项给出明确报错。
func (e *Executor) execSet(sess *Session, s *SetStmt) (*Result, error) {
	// M18-C：SET @var = value 用户变量赋值（会话级）。
	if s.Var != "" {
		sess.userVars[s.Var] = s.VarVal
		return &Result{Columns: []string{"variable", "value"}}, nil
	}
	// P2-13：SET [SESSION] TRANSACTION ISOLATION LEVEL <level>。
	// SESSION 级改会话默认（新事务生效）；语句级仅影响下一事务（BEGIN 消费）。
	if s.IsoSet {
		if s.IsoScope == "SESSION" {
			sess.isolation = s.IsoLevel
		} else {
			sess.nextTxIso = s.IsoLevel
			sess.nextTxIsoSet = true
		}
		return &Result{Columns: []string{"variable", "value"}}, nil
	}
	spec := lookupConfigSpec(s.Name)
	if spec == nil {
		return nil, &SQLError{Msg: "unknown session variable: " + s.Name}
	}
	// T36：persistOnly 键（tls_cert/tls_key/tls_allow_legacy）为启动级配置，
	// 监听层在启动装配无法热更新；普通 SET 明确报错，仅 SET PERSIST 落盘。
	if spec.persistOnly && !s.Persist {
		return nil, &SQLError{Msg: s.Name + " 为启动级配置，仅支持 SET PERSIST（重启生效）"}
	}
	if err := spec.apply(e, s.Value); err != nil {
		return nil, err
	}
	if s.Persist {
		// SET PERSIST：应用成功后写入持久化配置（校验失败不落盘）。
		if err := e.persistConfigValue(s.Name, s.Value); err != nil {
			return nil, &SQLError{Msg: "failed to persist config " + s.Name + ": " + err.Error()}
		}
	} else {
		// 仅当前会话/进程生效：记录来源，重启后由持久化或默认值接管。
		e.cfgMu.Lock()
		e.sessionCfg[s.Name] = s.Value
		e.cfgMu.Unlock()
	}
	return &Result{Columns: []string{"variable", "value"}}, nil
}

// maskQuestionMarks 将 SQL 文本中引号外的 ? 替换为 0（用于 PREPARE 语法校验，
// 与 expandPrepared 的扫描规则一致：单引号字符串内不替换）。
func maskQuestionMarks(sqlText string) string {
	bs := []byte(sqlText)
	inStr := false
	for i := 0; i < len(bs); i++ {
		switch bs[i] {
		case '\'':
			inStr = !inStr
		case '?':
			if !inStr {
				bs[i] = '0'
			}
		}
	}
	return string(bs)
}

// execPrepare PREPARE <name> FROM '<sql-text>'（M18）：编译期即校验语法，
// 通过后登记会话级 prepared 缓存；同名覆盖。不持有事务副作用。
func (e *Executor) execPrepare(sess *Session, s *PrepareStmt) (*Result, error) {
	// PREPARE 文本允许 ? 占位符：校验前将引号外的 ? 掩码为数字 0（结构位置等价），
	// 保持普通 Parse 拒绝裸 ? 的语义不变（占位符仅存在于 PREPARE 文本产物）。
	if _, err := Parse(maskQuestionMarks(s.SQL)); err != nil {
		return nil, &SQLError{Msg: "prepared statement syntax error: " + err.Error()}
	}
	sess.prepared[s.Name] = s.SQL
	return &Result{Columns: []string{"prepared", "sql"}}, nil
}

// execExecute EXECUTE <name> [USING @a, @b]（M18）：取回登记 SQL，按序
// 替换 ? 占位符（@var 取会话用户变量，字面量原样），再走完整解析+执行。
func (e *Executor) execExecute(sess *Session, s *ExecuteStmt) (*Result, error) {
	sqlText, ok := sess.prepared[s.Name]
	if !ok {
		return nil, &SQLError{Msg: "prepared statement not found: " + s.Name}
	}
	expanded, err := e.expandPrepared(sess, sqlText, s.Args)
	if err != nil {
		return nil, err
	}
	inner, err := Parse(expanded)
	if err != nil {
		return nil, &SQLError{Msg: "prepared statement expansion error: " + err.Error()}
	}
	return e.execStmt(sess, inner)
}

// expandPrepared 将 prepared SQL 中的 ? 占位符按参数列表顺序替换为 SQL 文本。
func (e *Executor) expandPrepared(sess *Session, sqlText string, args []string) (string, error) {
	var sb strings.Builder
	argIdx := 0
	placeholders := 0
	i := 0
	for i < len(sqlText) {
		c := sqlText[i]
		switch c {
		case '\'':
			// 字符串字面量整体跳过（不误替换引号内的 ?）
			sb.WriteByte(c)
			j := i + 1
			for j < len(sqlText) {
				if sqlText[j] == '\'' {
					if j+1 < len(sqlText) && sqlText[j+1] == '\'' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			sb.WriteString(sqlText[i+1 : j])
			i = j
			continue
		case '?':
			placeholders++
			if argIdx >= len(args) {
				return "", &SQLError{Msg: fmt.Sprintf("prepared statement expects %d argument(s), got %d", placeholders, len(args))}
			}
			rep, err := e.resolvePreparedArg(sess, args[argIdx])
			if err != nil {
				return "", err
			}
			sb.WriteString(rep)
			argIdx++
			i++
			continue
		}
		sb.WriteByte(c)
		i++
	}
	if argIdx != len(args) {
		return "", &SQLError{Msg: fmt.Sprintf("prepared statement got %d argument(s), expects %d", len(args), placeholders)}
	}
	return sb.String(), nil
}

// resolvePreparedArg 解析单个 EXECUTE 参数：@var 查会话用户变量；数字/字符串字面量原样。
func (e *Executor) resolvePreparedArg(sess *Session, arg string) (string, error) {
	if strings.HasPrefix(arg, "@") {
		v, ok := sess.userVars[arg]
		if !ok {
			return "", &SQLError{Msg: "user variable not set: " + arg}
		}
		if v.Kind == "INT" {
			return strconv.FormatInt(v.I, 10), nil
		}
		return "'" + strings.ReplaceAll(v.S, "'", "''") + "'", nil
	}
	// 字面量：数字原样；字符串补引号并转义
	if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") {
		return "'" + strings.ReplaceAll(arg[1:len(arg)-1], "'", "''") + "'", nil
	}
	return arg, nil
}

// execDeallocate DEALLOCATE PREPARE <name>（M18）：移除会话级登记。
func (e *Executor) execDeallocate(sess *Session, s *DeallocateStmt) (*Result, error) {
	if _, ok := sess.prepared[s.Name]; !ok {
		return nil, &SQLError{Msg: "prepared statement not found: " + s.Name}
	}
	delete(sess.prepared, s.Name)
	return &Result{Columns: []string{"deallocated"}}, nil
}

// boolVal 布尔值转 SQL 文本值（SHOW STATS 等输出用）。
func boolVal(b bool) Value {
	if b {
		return StrVal("on")
	}
	return StrVal("off")
}

// execShowStats SHOW STATS：输出服务器运行指标（T20 运维监控）。
// 覆盖：活跃连接数（server 会话计数）、表数量、region 数量、
// 慢查询数量、binlog 最大位点。注入源未装配时输出 0。
func (e *Executor) execShowStats(sess *Session) (*Result, error) {
	res := &Result{Columns: []string{"metric", "value"}}
	appendKV := func(k string, v Value) {
		res.Rows = append(res.Rows, []Value{StrVal(k), v})
	}
	if e.connSource != nil {
		appendKV("connection_count", IntVal(e.connSource()))
	} else {
		appendKV("connection_count", IntVal(0))
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	appendKV("table_count", IntVal(int64(len(tabs))))
	appendKV("region_count", IntVal(int64(len(e.router.AllRegions()))))
	appendKV("slow_query_count", IntVal(int64(len(e.SlowQueries()))))
	lsn := uint64(0)
	if e.binlogLSN != nil {
		if n, err := e.binlogLSN(); err == nil {
			lsn = n
		}
	}
	appendKV("binlog_lsn", IntVal(int64(lsn)))
	// M9 查询缓存：开关 + 命中/未命中/失效次数。
	qcOn, qcHits, qcMisses, qcInvals := e.qc.Stats()
	appendKV("query_cache_enabled", boolVal(qcOn))
	appendKV("query_cache_hits", IntVal(qcHits))
	appendKV("query_cache_misses", IntVal(qcMisses))
	appendKV("query_cache_invalidations", IntVal(qcInvals))
	return res, nil
}

// execExplain EXPLAIN <SELECT>：输出执行计划文本（不实际执行查询）。
func (e *Executor) execExplain(sess *Session, s *ExplainStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	meta := findTable(tabs, s.Select.From)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + s.Select.From}
	}
	// D3.1：行数估算（RangeCounter 真实计数 / 启发式常量），叠加到节点级耗时估算
	tableRows, err := e.tableRowEstimate(tx, meta)
	if err != nil {
		return nil, err
	}
	lines := e.planOfWithStats(s.Select, meta, tableRows)
	res := &Result{Columns: []string{"plan"}}
	for _, ln := range lines {
		res.Rows = append(res.Rows, []Value{StrVal(ln)})
	}
	return res, nil
}

// tableRowEstimate 估算表当前行数（D3.1 EXPLAIN 行数估算）：
// 事务实现 storage.RangeCounter 时按 region 范围真实计数（与 COUNT 快速路径同源）；
// 否则回退启发式常量（EXPLAIN 不实际执行查询，仅诊断参考）。
func (e *Executor) tableRowEstimate(tx txn.Txn, meta *TableMeta) (int64, error) {
	rc, ok := tx.(storage.RangeCounter)
	if !ok {
		return explainEstimateDefaultRows, nil
	}
	var total int64
	for _, rng := range e.rowRanges(meta) {
		n, err := rc.CountRange(rng, 0)
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return total, nil
}

// explainEstimateDefaultRows RangeCounter 不可用时的表行数启发式估算常量（D3.1）。
const explainEstimateDefaultRows = 1000

// planOf 构建 SELECT 执行计划文本（保持历史输出，不含节点级统计）。
func (e *Executor) planOf(s *SelectStmt, meta *TableMeta) []string {
	return e.planLines(s, meta, explainEstimateDefaultRows)
}

// planOfWithStats 在 planOf 基础上追加节点级行数估算与耗时估算（D3.1）：
// 每个算子行末尾追加 "rows=N est_ms=X.XX" 字段（输出向后兼容追加字段，
// 原有列与文本前缀不变，旧解析器不受影响）。tableRows 为表当前行数估算
// （<0 时回退启发式常量）。行数与耗时均为启发式估算，非真实执行度量。
func (e *Executor) planOfWithStats(s *SelectStmt, meta *TableMeta, tableRows int64) []string {
	return e.planLines(s, meta, tableRows)
}

// planLines 构建执行计划文本；tableRows>=0 时追加节点级 rows/est_ms 统计。
func (e *Executor) planLines(s *SelectStmt, meta *TableMeta, tableRows int64) []string {
	if tableRows < 0 {
		tableRows = explainEstimateDefaultRows
	}
	var lines []string
	prevRows := tableRows
	if len(s.Joins) > 0 {
		// 嵌套循环连接：驱动表 + 每张连接表行数估算
		rows := tableRows * int64(len(s.Joins)+1)
		lines = append(lines, fmt.Sprintf("access: nested-loop join (%s + %d table(s)) rows=%d est_ms=%.2f",
			s.From, len(s.Joins), rows, 0.5+0.01*float64(rows)))
		prevRows = rows
	} else if s.Subquery != nil {
		lines = append(lines, fmt.Sprintf("access: subquery scan (FROM (%s)) rows=%d est_ms=%.2f",
			s.SubAlias, prevRows, 0.3+0.001*float64(prevRows)))
	} else {
		access := e.accessOf(s, meta)
		rows, est := e.estimateAccess(s, meta, tableRows)
		lines = append(lines, fmt.Sprintf("access: %s rows=%d est_ms=%.2f", access, rows, est))
		prevRows = rows
	}
	// 过滤算子
	if s.Where != nil && len(s.Where.Conds) > 0 {
		rows := prevRows / 2
		if rows < 1 {
			rows = 1
		}
		lines = append(lines, fmt.Sprintf("filter: %s rows=%d est_ms=%.2f",
			e.whereSummary(s.Where), rows, 0.02+0.0005*float64(prevRows)))
		prevRows = rows
	}
	// JOIN ON 条件
	for i := range s.Joins {
		if len(s.Joins[i].On) > 0 {
			lines = append(lines, fmt.Sprintf("join%d on: %s rows=%d est_ms=%.2f",
				i+1, e.whereSummary(&Where{Conds: s.Joins[i].On}), prevRows, 0.05+0.0005*float64(prevRows)))
		}
	}
	if len(s.GroupBy) > 0 {
		rows := prevRows / 4
		if rows < 1 {
			rows = 1
		}
		lines = append(lines, fmt.Sprintf("group: %s rows=%d est_ms=%.2f",
			strings.Join(s.GroupBy, ", "), rows, 0.05+0.001*float64(prevRows)))
		prevRows = rows
	}
	hasAgg := false
	for _, c := range s.Columns {
		if c.Agg != "" {
			hasAgg = true
			break
		}
	}
	if hasAgg {
		if len(s.GroupBy) == 0 {
			prevRows = 1
		}
		lines = append(lines, fmt.Sprintf("aggregate: yes rows=%d est_ms=%.2f",
			prevRows, 0.02+0.0005*float64(prevRows)))
	}
	if s.OrderBy != nil {
		lines = append(lines, fmt.Sprintf("sort: %s %s rows=%d est_ms=%.2f",
			s.OrderBy.Col, orderDir(s.OrderBy.Desc), prevRows, 0.05+0.005*float64(prevRows)))
	}
	if s.Limit > 0 {
		rows := prevRows
		if int64(s.Limit) < rows {
			rows = int64(s.Limit)
		}
		lines = append(lines, fmt.Sprintf("limit: %d rows=%d est_ms=%.2f", s.Limit, rows, 0.005))
		prevRows = rows
	}
	if s.Offset > 0 {
		rows := prevRows - int64(s.Offset)
		if rows < 0 {
			rows = 0
		}
		lines = append(lines, fmt.Sprintf("offset: %d rows=%d est_ms=%.2f", s.Offset, rows, 0.005))
		prevRows = rows
	}
	lines = append(lines, fmt.Sprintf("projection: %s rows=%d est_ms=%.2f",
		e.projSummary(s), prevRows, 0.01+0.0002*float64(prevRows)))
	return lines
}

// estimateAccess 访问路径行数与耗时估算（D3.1，启发式，单位 ms）：
// 判定与 accessOf 一致——点查/索引等值 rows=1；索引范围按表行数 1/3；
// 全表扫描为表行数。
func (e *Executor) estimateAccess(s *SelectStmt, meta *TableMeta, tableRows int64) (rows int64, estMs float64) {
	if s.Where != nil && len(s.Where.Conds) == 1 {
		c := s.Where.Conds[0]
		if c.Col == meta.PK && c.Op == "=" && c.LeftCase == nil {
			return 1, 0.05
		}
	}
	if s.Where != nil {
		for i := range s.Where.Conds {
			c := &s.Where.Conds[i]
			if c.Op == "!=" || c.Op == "LIKE" || c.LeftCase != nil {
				continue
			}
			for j := range meta.Indexes {
				if meta.Indexes[j].Col == c.Col {
					if c.Op == "=" || c.Op == "IN" {
						return 1, 0.10
					}
					r := tableRows / 3
					if r < 1 {
						r = 1
					}
					return r, 0.20 + 0.003*float64(r)
				}
			}
		}
	}
	return tableRows, 0.30 + 0.001*float64(tableRows)
}

// accessOf 判断单表访问路径：点查 / 索引扫描 / 全表扫描。
func (e *Executor) accessOf(s *SelectStmt, meta *TableMeta) string {
	if s.Where != nil && len(s.Where.Conds) == 1 {
		c := s.Where.Conds[0]
		if c.Col == meta.PK && c.Op == "=" && c.LeftCase == nil {
			return "point lookup on PK (" + meta.PK + ")"
		}
	}
	if s.Where != nil {
		for i := range s.Where.Conds {
			c := &s.Where.Conds[i]
			if c.Op == "!=" || c.Op == "LIKE" || c.LeftCase != nil {
				continue
			}
			for j := range meta.Indexes {
				if meta.Indexes[j].Col == c.Col {
					return "index scan on " + meta.Indexes[j].Name + "(" + meta.Indexes[j].Col + ")"
				}
			}
		}
	}
	return "table scan (full)"
}

// whereSummary 条件摘要（列名/算子/值；含 LIKE 与 CASE）。
func (e *Executor) whereSummary(w *Where) string {
	var parts []string
	for i := range w.Conds {
		c := &w.Conds[i]
		left := c.Col
		if c.LeftCase != nil {
			left = "CASE(...)"
		}
		switch c.Op {
		case "BETWEEN":
			parts = append(parts, left+" BETWEEN "+c.Val.String()+" AND "+c.Val2.String())
		case "IN":
			parts = append(parts, left+" IN (...)")
		case "LIKE":
			parts = append(parts, left+" LIKE '"+c.Val.String()+"'")
		default:
			parts = append(parts, left+" "+c.Op+" "+c.Val.String())
		}
	}
	return strings.Join(parts, " AND ")
}

// projSummary SELECT 投影列摘要。
func (e *Executor) projSummary(s *SelectStmt) string {
	var parts []string
	for _, c := range s.Columns {
		if c.Case != nil {
			parts = append(parts, "CASE(...)")
			continue
		}
		parts = append(parts, c.Name)
	}
	return strings.Join(parts, ", ")
}

func orderDir(desc bool) string {
	if desc {
		return "DESC"
	}
	return "ASC"
}

// ---- P2：LIKE 匹配 ----

// likeMatch 通配符匹配：% 匹配任意多字符（含 0），_ 匹配单字符；逐字节匹配，大小写敏感。
// 采用经典两指针回溯算法，无递归开销。
func likeMatch(s, pat string) bool {
	sr := []byte(s)
	pr := []byte(pat)
	si, pi := 0, 0
	star, mark := -1, 0
	for si < len(sr) {
		if pi < len(pr) && (pr[pi] == '_' || pr[pi] == sr[si]) {
			si++
			pi++
		} else if pi < len(pr) && pr[pi] == '%' {
			star = pi
			mark = si
			pi++
		} else if star >= 0 {
			pi = star + 1
			mark++
			si = mark
		} else {
			return false
		}
	}
	for pi < len(pr) && pr[pi] == '%' {
		pi++
	}
	return pi == len(pr)
}

// ---- P2：CASE WHEN 求值 ----

// evalCaseM1 行级 CASE 求值（M1 路径；CASE 内条件按 AND 组合匹配）。
func evalCaseM1(meta *TableMeta, row []Value, cc *CaseWhenClause) (Value, error) {
	for i := range cc.Branches {
		ok, err := matchWhere(meta, row, &cc.Branches[i].Conds)
		if err != nil {
			return Value{}, err
		}
		if ok {
			return cc.Branches[i].Then, nil
		}
	}
	if cc.HasElse {
		return cc.Else, nil
	}
	return StrVal(""), nil
}

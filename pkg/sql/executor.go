package sql

import (
	"bytes"
	"fmt"
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
	tm    txn.TxnManager
	mu    sync.Mutex
	curTx txn.Txn
	inTx  bool
	// M3 分片路由（sql.New 注入默认 Router；db.Open 加载落盘元数据后覆盖）
	router *sharding.Router
	// M4 集群管理器（nil = 单机模式，转发路径零触发）。
	// db.Open 装配后注入；selfID 供"本节点"归属判断。
	mgr    *cluster.Manager
	selfID string
	// M8 分布式快照隔离：ts 为全局时间戳源（nil = 未装配，旧路径零回归）。
	// curBeginTS 为显式事务 BEGIN 时获取的快照版本；stmtBeginTS 为
	// autocommit 语句首次读时惰性获取的语句快照（每条语句独立）。
	ts          tso.Source
	curBeginTS  uint64
	stmtBeginTS uint64
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
	// M8(BR)：备份/恢复环境（db.Open 装配注入）。
	// st 为引擎级 Storage 引用：BACKUP 基于一致快照导出、RESTORE 物理写与 PITR 回放；
	// binlogPath 为数据目录 binlog 文件路径（TO LSN 回放读取，M2 pkg/replication）。
	st         storage.Storage
	binlogPath string
	// T20：运维指标注入源（nil = 未装配，SHOW STATS 输出 0）。
	// connSource 返回当前活跃 TCP 连接数（server 层会话计数）；
	// binlogLSN 返回当前 binlog 最大位点（db 层注入，读取只读）。
	connSource func() int64
	binlogLSN  func() (uint64, error)
	// M9：查询结果缓存（SELECT 结果以规范化 SQL 为 key；写语句后全量失效）。
	qc *QueryCache
	// M13：metrics 导出注册表（nil = 未装配，计数零触发）。
	m *metrics.Registry
}

// splitConfig M7 自动分裂配置。
type splitConfig struct {
	enabled      bool
	rowThreshold int64 // 行数阈值；<=0 视为未开启
}

func NewExecutor(tm txn.TxnManager) *Executor {
	return &Executor{
		tm:            tm,
		router:        sharding.NewRouter(),
		failoverDone:  map[string]bool{},
		splitAcc:      map[uint64]int64{},
		qc:            NewQueryCache(),
		slowThreshold: slowThresholdDefault, // M13：默认 100ms
		slowQueryMax:  slowQueryMaxDefault,
		slowLog:       NewSlowLog(),
	}
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
func (e *Executor) SetMetrics(m *metrics.Registry) { e.m = m }

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
func (e *Executor) execTimed(user, sql string, fn func() (*Result, error)) (*Result, error) {
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
func (e *Executor) ExecuteTextAs(stmt Stmt, sqlText, user string) (*Result, error) {
	if err := e.checkPrivilege(stmt, user); err != nil {
		return nil, err
	}
	return e.execTimed(user, sqlText, func() (*Result, error) {
		// M9 查询缓存：仅 autocommit 下的 SELECT 参与缓存（显式事务内
		// 依赖事务快照，结果不缓存也不查缓存，保持快照隔离语义）。
		if ss, ok := stmt.(*SelectStmt); ok && e.qc.Enabled() && e.autocommit() {
			key := NormalizeSQL(sqlText)
			if res, hit := e.qc.Get(key); hit {
				return res, nil
			}
			res, err := e.execStmt(ss)
			if err == nil {
				e.qc.Put(key, res)
			}
			return res, err
		}
		res, err := e.execStmt(stmt)
		if err == nil && isWriteStmt(stmt) {
			e.qc.Invalidate()
		}
		return res, err
	})
}

// isWriteStmt 判断语句是否修改存储可见状态：DML 与 DDL（含备份恢复、
// region 运维写操作）成功后需失效查询缓存；只读查询/事务控制语句不失效。
func isWriteStmt(stmt Stmt) bool {
	switch stmt.(type) {
	case *CreateTableStmt, *DropTableStmt, *CreateIndexStmt, *DropIndexStmt,
		*AlterTableStmt, *InsertStmt, *UpdateStmt, *DeleteStmt, *ImportStmt,
		*AssignRegionStmt, *SplitRegionStmt, *BalanceStmt, *BackupStmt, *RestoreStmt,
		*GrantStmt, *RevokeStmt:
		return true
	}
	return false
}

// execStmt 语句分发（不计时/不记录慢查询，供 ExecuteText 包装）。
func (e *Executor) execStmt(stmt Stmt) (*Result, error) {
	// M8：每条语句独立快照（显式事务内不重置，复用 BEGIN 快照）。
	e.resetStmtSnapshot()
	switch s := stmt.(type) {
	case *CreateTableStmt:
		return e.execCreateTable(s)
	case *DropTableStmt:
		return e.execDropTable(s)
	case *CreateIndexStmt:
		return e.execCreateIndex(s)
	case *DropIndexStmt:
		return e.execDropIndex(s)
	case *AlterTableStmt:
		return e.execAlterTable(s)
	case *InsertStmt:
		return e.execInsert(s)
	case *SelectStmt:
		return e.execSelect(s)
	case *UpdateStmt:
		return e.execUpdate(s)
	case *DeleteStmt:
		return e.execDelete(s)
	case *BeginStmt:
		if err := e.beginTx(); err != nil {
			return nil, err
		}
		return &Result{}, nil
	case *CommitStmt:
		if err := e.commitTx(); err != nil {
			return nil, err
		}
		return &Result{}, nil
	case *RollbackStmt:
		if err := e.rollbackTx(); err != nil {
			return nil, err
		}
		return &Result{}, nil
	case *ExportStmt:
		return e.execExport(s)
	case *ImportStmt:
		return e.execImport(s)
	case *CreateViewStmt:
		return e.execCreateView(s)
	case *DropViewStmt:
		return e.execDropView(s)
	case *ShowViewsStmt:
		return e.execShowViews()
	case *CreateUserStmt:
		return e.execCreateUser(s)
	case *DropUserStmt:
		return e.execDropUser(s)
	case *AlterUserStmt:
		return e.execAlterUser(s)
	case *ShowUsersStmt:
		return e.execShowUsers()
	case *ShowTablesStmt:
		return e.execShowTables()
	case *ShowIndexStmt:
		return e.execShowIndex(s)
	case *ShowSlowQueriesStmt:
		return e.execShowSlowQueries()
	case *ExplainStmt:
		return e.execExplain(s)
	case *AddNodeStmt:
		return e.execAddNode(s)
	case *ShowNodesStmt:
		return e.execShowNodes()
	case *ShowRegionRoutesStmt:
		return e.execShowRegionRoutes()
	case *ShowStatsStmt:
		return e.execShowStats()
	case *SetStmt:
		return e.execSet(s)
	case *AssignRegionStmt:
		return e.execAssignRegion(s)
	case *SplitRegionStmt:
		return e.execSplitRegion(s)
	case *BalanceStmt:
		return e.execBalance(s)
	case *BackupStmt:
		return e.execBackup(s)
	case *RestoreStmt:
		return e.execRestore(s)
	case *GrantStmt:
		return e.execGrant(s)
	case *RevokeStmt:
		return e.execRevoke(s)
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

// beginTx 开启会话事务（已开启时报错）。
func (e *Executor) beginTx() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.inTx {
		return &SQLError{Msg: "transaction already started"}
	}
	// M8 分布式快照隔离：BEGIN 时向 TSO 取全局 begin_ts 作为事务快照。
	// 未装配 TSO（单机旧路径）时 begin_ts=0，行为与 M7 完全一致。
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	e.curTx = tx
	e.inTx = true
	if e.ts != nil {
		// 事务起点刷新 TSO 缓存：丢弃残余预取批次，取权威节点当前序列
		// 的新鲜号作为快照，避免批量缓存让 begin_ts 滞后于已提交事务。
		e.ts.Reset()
		ts, terr := e.ts.Get()
		if terr != nil {
			tx.Rollback()
			return &SQLError{Msg: fmt.Sprintf("BEGIN_FAILED: tso unavailable: %v", terr)}
		}
		e.curBeginTS = ts
	} else {
		e.curBeginTS = 0
	}
	return nil
}

// commitTx 提交会话事务（未开启时报错）。
func (e *Executor) commitTx() error {
	e.mu.Lock()
	if !e.inTx {
		e.mu.Unlock()
		return &SQLError{Msg: "no active transaction"}
	}
	tx := e.curTx
	e.inTx = false
	e.curTx = nil
	e.curBeginTS = 0
	e.mu.Unlock()
	if e.ts != nil {
		// 事务结束后刷新 TSO 缓存：后续新事务/新语句取权威当前序列
		// 的新鲜号，避免读到残余预取批次中的滞后时间戳。
		e.ts.Reset()
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if e.m != nil {
		e.m.IncCommit()
	}
	return nil
}

// rollbackTx 回滚会话事务（未开启时报错）。
func (e *Executor) rollbackTx() error {
	e.mu.Lock()
	if !e.inTx {
		e.mu.Unlock()
		return &SQLError{Msg: "no active transaction"}
	}
	tx := e.curTx
	e.inTx = false
	e.curTx = nil
	e.curBeginTS = 0
	e.mu.Unlock()
	if e.ts != nil {
		e.ts.Reset()
	}
	if err := tx.Rollback(); err != nil {
		return err
	}
	if e.m != nil {
		e.m.IncRollback()
	}
	return nil
}

// SetTSO 注入全局时间戳源（M8：db.Open/StartCluster 装配）。
// nil 表示不启用快照隔离（单机旧路径零回归）。
func (e *Executor) SetTSO(src tso.Source) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ts = src
	e.stmtBeginTS = 0
}

// tsoGet 从装配的时间戳源取号；未装配返回 0（旧路径）。
func (e *Executor) tsoGet() (uint64, error) {
	e.mu.Lock()
	ts := e.ts
	e.mu.Unlock()
	if ts == nil {
		return 0, nil
	}
	return ts.Get()
}

// snapshotTS 返回当前读快照版本：
//   - 未装配 TSO：0（不过滤，旧路径零回归）；
//   - 显式事务：BEGIN 时获取的 curBeginTS；
//   - autocommit 语句：首次读时惰性获取并缓存 stmtBeginTS（每条语句独立）。
//
// 取号失败（如远程 TSO 不可达）退回 0：读不过滤、尽力而为，不阻塞语句。
func (e *Executor) snapshotTS() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ts == nil {
		return 0
	}
	if e.inTx {
		return e.curBeginTS
	}
	if e.stmtBeginTS == 0 {
		ts, err := e.ts.Get()
		if err != nil {
			return 0
		}
		e.stmtBeginTS = ts
	}
	return e.stmtBeginTS
}

// resetStmtSnapshot 在每条语句执行入口重置 autocommit 语句快照，
// 保证每条语句拥有独立快照（显式事务内不重置）。
func (e *Executor) resetStmtSnapshot() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.inTx {
		e.stmtBeginTS = 0
	}
}

// getTx 获取当前语句的事务：会话事务内复用 curTx；否则新建临时事务。
func (e *Executor) getTx() (txn.Txn, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.inTx {
		return e.curTx, nil
	}
	return e.tm.Begin()
}

// autocommit 判断当前语句是否以隐式事务提交（不在显式事务中时为 true）。
func (e *Executor) autocommit() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.inTx
}

// ---- DDL ----

func (e *Executor) execCreateTable(s *CreateTableStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	if findTable(tabs, s.Name) != nil {
		return nil, &SQLError{Msg: "table already exists: " + s.Name}
	}
	if s.PK == "" && len(s.Columns) > 0 {
		s.PK = s.Columns[0].Name
	}
	meta := &TableMeta{ID: nextTableID(tabs), Name: s.Name, Columns: s.Columns, PK: s.PK, ForeignKeys: s.ForeignKeys}
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
	}
	return &Result{AffectedRows: 0}, nil
}

func (e *Executor) execDropTable(s *DropTableStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
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
	// M11 联动：DROP TABLE 一并清理所有用户对该表的权限记录（m:grants）。
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
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: len(pairs)}, nil
}

// ---- M11：用户管理 ----

// execCreateUser CREATE USER '<name>' IDENTIFIED BY '<pwd>'。
// 密码以 SHA-256+salt 哈希落库（user.go），绝不保存明文；重复用户报错。
func (e *Executor) execCreateUser(s *CreateUserStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
	if auto {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findUser(users, s.Name) != nil {
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
func (e *Executor) execDropUser(s *DropUserStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
	// M11 联动：DROP USER 一并清理该用户的全部权限记录（m:grants）。
	grants, err := loadGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	removeUserGrants(grants, s.Name)
	graw, err := saveGrants(grants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(grantsMetaKey, graw); err != nil {
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
func (e *Executor) execAlterUser(s *AlterUserStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
func (e *Executor) execShowUsers() (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	if e.autocommit() {
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
func (e *Executor) execCreateIndex(s *CreateIndexStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
		if err := e.putIndexKeys(tx, meta, row, pk, ridOf(kv.Key), &pending); err != nil {
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
func (e *Executor) execDropIndex(s *DropIndexStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
func (e *Executor) ddlCommit(tx txn.Txn, tabs []*TableMeta, ops []kvOp, auto bool) error {
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
		if err := e.exec2pcWrite(tx, ops); err != nil {
			return err
		}
	} else {
		if e.hasRemoteOps(ops) {
			return &SQLError{Msg: "distributed write inside explicit transaction not supported in M5 (autocommit 2PC only)"}
		}
		if err := applyOps(tx, ops); err != nil {
			return err
		}
	}
	return nil
}

// execAlterTable 在线 DDL：ALTER TABLE 六种动作（ADD/DROP/RENAME COLUMN、
// RENAME TABLE、ADD/DROP INDEX）。整个操作在单个事务内完成：
// 元数据变更 + 数据重写要么全部生效要么全部回滚；Commit 时经 WAL 落盘，
// 崩溃后 WAL 重放保持目录与数据一致（SHOW TABLES / SHOW INDEX 同步可见）。
func (e *Executor) execAlterTable(s *AlterTableStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
		if err := e.ddlCommit(tx, tabs, ops, auto); err != nil {
			return nil, err
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
		if err := e.ddlCommit(tx, tabs, ops, auto); err != nil {
			return nil, err
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
		if err := e.ddlCommit(tx, tabs, nil, auto); err != nil {
			return nil, err
		}
		return &Result{AffectedRows: 0}, nil
	case "RENAME_TABLE":
		if findTable(tabs, s.NewName) != nil {
			return nil, &SQLError{Msg: "table already exists: " + s.NewName}
		}
		meta.Name = s.NewName
		if err := e.ddlCommit(tx, tabs, nil, auto); err != nil {
			return nil, err
		}
		return &Result{AffectedRows: 0}, nil
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
			if err := e.putIndexKeys(tx, meta, row, pk, ridOf(kv.Key), &pending); err != nil {
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
		}
		return &Result{AffectedRows: len(pairs)}, nil
	}
	return nil, errf("unsupported ALTER action %q", s.Action)
}

// ---- DML ----

func (e *Executor) execInsert(s *InsertStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
	affected := 0
	var ops []kvOp
	// M10 UPSERT：语句内已计划写入的最终行（含 REPLACE 删旧插新后的新行、
	// ON DUPLICATE 更新后的目标行），用于语句内主键/唯一键冲突判定。
	var pending [][]Value
	for _, row := range s.Rows {
		rops, aff, err := e.insertRowUpsertOps(tx, tabs, meta, s.Columns, row, s.OnDup, s.Replace, &pending)
		if err != nil {
			return nil, err
		}
		if aff > 0 {
			affected += aff
			ops = append(ops, rops...)
		}
	}
	if auto {
		// autocommit：本地 + 远端统一走 2PC 提交（单机零触发）
		if err := e.exec2pcWrite(tx, ops); err != nil {
			return nil, err
		}
	} else {
		// 显式会话事务：仅支持本地 region 写（远端写 M5 返回明确错误）
		if e.hasRemoteOps(ops) {
			return nil, &SQLError{Msg: "distributed write inside explicit transaction not supported in M5 (autocommit 2PC only)"}
		}
		if err := applyOps(tx, ops); err != nil {
			return nil, err
		}
	}
	// M7 自动分裂：写提交后按配置检查本地 region 数据规模（显式事务内不触发，
	// 分裂为独立事务，避免与用户事务交叠）。
	if err := e.maybeAutoSplit(meta); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: affected}, nil
}

// insertRowOps 单行插入的"物理操作列表"版本：不直接写事务，
// 返回该行所需的全部键操作（行 Put + 索引键 Put），供调用方本地应用
// 或 2PC 分组提交（M5）。dupSkip 语义同 insertRow。
// M9 外键：插入前校验引用完整性（父表行存在且类型匹配）。
func (e *Executor) insertRowOps(tx txn.Txn, tabs []*TableMeta, meta *TableMeta, cols []string, row []Value, dupSkip bool) ([]kvOp, bool, error) {
	vals, err := e.buildRow(meta, cols, row)
	if err != nil {
		return nil, false, err
	}
	if err := e.checkFKOnRow(tx, tabs, meta, vals); err != nil {
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
	if _, err := e.getKVCluster(tx, key); err == nil {
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
	idxOps, err := e.putIndexKeyOps(tx, meta, vals, pk, rid, nil, nil)
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
func (e *Executor) insertRowUpsertOps(tx txn.Txn, tabs []*TableMeta, meta *TableMeta, cols []string, row []Value, onDup *OnDuplicateUpdate, replace bool, pending *[][]Value) ([]kvOp, int, error) {
	vals, err := e.buildRow(meta, cols, row)
	if err != nil {
		return nil, 0, err
	}
	if err := e.checkFKOnRow(tx, tabs, meta, vals); err != nil {
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
	if _, err := e.getKVCluster(tx, key); err == nil {
		diskExists = true
	} else if err != storage.ErrNotFound {
		return nil, 0, err
	}
	// 收集全部冲突行（主键 + 唯一索引，磁盘 + pending，去重）
	conflicts, err := e.upsertConflicts(tx, meta, vals, pk, diskExists, *pending)
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
		idxOps, err := e.putIndexKeyOps(tx, meta, vals, pk, rid, pending, nil)
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
		return e.onDupUpdateOps(tx, tabs, meta, vals, pk, rid, target, diskExists, onDup, pending)
	}
	if replace {
		return e.replaceRowOps(tx, tabs, meta, vals, pk, rid, conflicts, diskExists, pending)
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
func (e *Executor) upsertConflicts(tx txn.Txn, meta *TableMeta, vals []Value, pk []byte, diskExists bool, pending [][]Value) ([][]byte, error) {
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
		ci := colIndex(meta, idx.Col)
		val, err := idxBytes(vals[ci])
		if err != nil {
			return nil, err
		}
		start := EncodeIndexKey2(meta.ID, idx.ID, val, nil)
		end := EncodeIndexKey2(meta.ID, idx.ID, append(append([]byte{}, val...), 0xff), nil)
		pairs, err := e.scanMergedCluster(tx, e.indexRanges(meta, idx.ID, start, end))
		if err != nil {
			return nil, err
		}
		for _, kv := range pairs {
			epk := indexPKSuffix(innerOf(kv.Key))
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
		ci := colIndex(meta, idx.Col)
		val, err := idxBytes(vals[ci])
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
			pv, err := idxBytes(prow[ci])
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
func (e *Executor) onDupUpdateOps(tx txn.Txn, tabs []*TableMeta, meta *TableMeta, newVals []Value, newPK []byte, newRid storage.ID, targetPK []byte, diskExists bool, onDup *OnDuplicateUpdate, pending *[][]Value) ([]kvOp, int, error) {
	for _, set := range onDup.Sets {
		if set.Col == meta.PK {
			return nil, 0, &SQLError{Msg: "cannot update primary key column in ON DUPLICATE KEY UPDATE: " + set.Col}
		}
	}
	// 定位冲突行（磁盘或 pending）
	target, rid, key, onDisk, err := e.upsertTargetRow(tx, meta, newPK, newRid, targetPK, diskExists, *pending)
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
		v, err := coerceValue(meta.Columns[idx].Type, set.Col, set.Val)
		if err != nil {
			return nil, 0, err
		}
		updated[idx] = v
	}
	if err := e.checkFKOnRow(tx, tabs, meta, updated); err != nil {
		return nil, 0, err
	}
	// 被更新行若被外键引用（作为父表）则禁止（ON UPDATE RESTRICT 语义）
	referenced, err := e.fkParentReferenced(tx, tabs, meta, target)
	if err != nil {
		return nil, 0, err
	}
	if referenced {
		return nil, 0, &SQLError{Msg: "foreign key constraint violated: " + meta.Name + " is referenced by child table (ON UPDATE not supported)"}
	}
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
	iops, err := e.putIndexKeyOps(tx, meta, updated, targetPK, rid, pending, nil)
	if err != nil {
		return nil, 0, err
	}
	ops = append(ops, iops...)
	e.replacePending(pending, meta, targetPK, updated)
	return ops, 2, nil
}

// replaceRowOps REPLACE INTO：删除全部冲突行（含外键 CASCADE 联动）后插入新行，
// 删除与插入在同一个提交内原子完成。
func (e *Executor) replaceRowOps(tx txn.Txn, tabs []*TableMeta, meta *TableMeta, newVals []Value, newPK []byte, newRid storage.ID, conflicts [][]byte, diskExists bool, pending *[][]Value) ([]kvOp, int, error) {
	var ops []kvOp
	deleted := 0
	// 待删除 pk 集合：REPLACE 插入时唯一性检查跳过这些行
	ignore := make(map[string]bool, len(conflicts))
	for _, cp := range conflicts {
		ignore[string(cp)] = true
	}
	for _, cp := range conflicts {
		target, rid, key, onDisk, err := e.upsertTargetRow(tx, meta, newPK, newRid, cp, diskExists && bytes.Equal(cp, newPK), *pending)
		if err != nil {
			return nil, 0, err
		}
		if onDisk {
			// 外键联动删除（CASCADE 子行等）
			fops, err := e.collectFKDeleteOps(tx, tabs, meta, target)
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
	iops, err := e.putIndexKeyOps(tx, meta, newVals, newPK, newRid, pending, ignore)
	if err != nil {
		return nil, 0, err
	}
	ops = append(ops, iops...)
	e.replacePending(pending, meta, newPK, newVals)
	return ops, deleted + 1, nil
}

// upsertTargetRow 定位冲突行：优先 pending（语句内最终状态），否则磁盘行。
func (e *Executor) upsertTargetRow(tx txn.Txn, meta *TableMeta, newPK []byte, newRid storage.ID, targetPK []byte, diskExists bool, pending [][]Value) ([]Value, storage.ID, []byte, bool, error) {
	if bytes.Equal(targetPK, newPK) {
		if diskExists {
			raw, err := e.getKVCluster(tx, e.rowKeyAt(meta, newRid, newPK))
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
	raw, err := e.getKVCluster(tx, e.rowKeyAt(meta, tr, targetPK))
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
func (e *Executor) insertRow(tx txn.Txn, tabs []*TableMeta, meta *TableMeta, cols []string, row []Value, dupSkip bool) (bool, error) {
	ops, ok, err := e.insertRowOps(tx, tabs, meta, cols, row, dupSkip)
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

func (e *Executor) execSelect(s *SelectStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
	if auto {
		defer tx.Rollback()
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
		return e.execSelectRel(tx, s)
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
	// 点查优化：WHERE 仅含主键等值 → 直接 Get
	if s.Where != nil && len(s.Where.Conds) == 1 {
		c := s.Where.Conds[0]
		if c.Col == meta.PK && c.Op == "=" {
			pk, err := pkBytes(meta, c.Val)
			if err != nil {
				return nil, err
			}
			rid, err := e.locateRow(meta, pk)
			if err != nil {
				return nil, err
			}
			raw, err := e.getKVCluster(tx, e.rowKeyAt(meta, rid, pk))
			if err == nil {
				row, err := decodeRow(meta, raw)
				if err != nil {
					return nil, err
				}
				rows = [][]Value{row}
			} else if err != storage.ErrNotFound {
				return nil, err
			}
			return e.finishSelect(meta, s, rows, ordered)
		}
	}
	// 二级索引优化：WHERE 命中索引列等值/范围
	if rows, ordered, err = e.indexLookup(tx, meta, s); err != nil {
		return nil, err
	}
	if rows == nil {
		// M12-2 分布式排序分页捷径（无 WHERE：窗口截断正确性前提）
		if e.canDistributedOrder(meta, s) {
			return e.execDistributedOrder(tx, meta, s)
		}
		// 全表扫描 + 过滤（跨 region/跨节点展开后按内层键序合并，等价单 region 全表序）
		var pairs []storage.KVPair
		var err error
		if e.canDistributedTop(meta, s) {
			// M12-2 无 ORDER BY 仅 LIMIT/OFFSET：各分片取足量再截断
			pairs, err = e.scanMergedClusterTop(tx, e.rowRanges(meta), s.Offset+s.Limit)
		} else {
			pairs, err = e.scanMergedCluster(tx, e.rowRanges(meta))
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
	return e.finishSelect(meta, s, rows, ordered)
}

// indexLookup 用二级索引满足 WHERE（等值/范围）与 ORDER BY。
// 返回 rows=nil 表示无可用索引条件，应回退全表扫描。
func (e *Executor) indexLookup(tx txn.Txn, meta *TableMeta, s *SelectStmt) ([][]Value, bool, error) {
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
	pairs, err := e.scanMergedCluster(tx, e.indexRanges(meta, idx.ID, start, end))
	if err != nil {
		return nil, false, err
	}
	rows := make([][]Value, 0, len(pairs))
	for _, kv := range pairs {
		inner := innerOf(kv.Key)
		pk := indexPKSuffix(inner)
		// 回表：索引键与行键同 region（写入时同路由），按 region 前缀读取（跨节点经转发）
		raw, err := e.getKVCluster(tx, e.rowKeyAt(meta, ridOf(kv.Key), pk))
		if err != nil {
			if err == storage.ErrNotFound {
				continue // 索引脏键（理论上不出现）
			}
			return nil, false, err
		}
		row, err := decodeRow(meta, raw)
		if err != nil {
			return nil, false, err
		}
		ok, err := matchWhere(meta, row, s.Where)
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
	// 排序
	if s.OrderBy != nil && !ordered {
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
			for _, col := range meta.Columns {
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
				out = append(out, row...)
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

// finishSelectWindow 窗口函数投影（单表/meta 路径）：普通列逐行取值，窗口列由 computeWindows 计算。
func (e *Executor) finishSelectWindow(meta *TableMeta, s *SelectStmt, rows [][]Value) (*Result, error) {
	res := &Result{}
	for _, c := range s.Columns {
		if c.Name == "*" {
			for _, col := range meta.Columns {
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
	winVals, err := computeWindows(rows, func(row []Value, col string) (Value, error) {
		return rowValue(meta, row, col)
	}, winCols)
	if err != nil {
		return nil, err
	}
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
		v, err := aggColumn(meta, rows, &c)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	res.Rows = append(res.Rows, out)
	return res, nil
}

// aggColumn 计算单列聚合值（COUNT/SUM/AVG；DECIMAL 保持精度，INT 走 int64）。
func aggColumn(meta *TableMeta, rows [][]Value, c *SelectColumn) (Value, error) {
	colType := ""
	for _, m := range meta.Columns {
		if m.Name == c.Col {
			colType = m.Type
			break
		}
	}
	switch c.Agg {
	case "COUNT":
		return IntVal(int64(len(rows))), nil
	case "SUM", "AVG":
		dec := false
		var isum, dsum int64
		for _, r := range rows {
			v, err := rowValue(meta, r, c.Col)
			if err != nil {
				return Value{}, err
			}
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
			if c.Agg == "SUM" {
				return decVal(dsum), nil
			}
			if len(rows) > 0 {
				return decVal(dsum / int64(len(rows))), nil
			}
			return decVal(0), nil
		}
		if len(rows) == 0 {
			if colType == "DECIMAL" {
				return decVal(0), nil
			}
			return IntVal(0), nil
		}
		if c.Agg == "SUM" {
			return IntVal(isum), nil
		}
		return IntVal(isum / int64(len(rows))), nil
	}
	return Value{}, &SQLError{Msg: "unknown aggregate: " + c.Agg}
}

// ---- P0 SELECT 增强引擎：JOIN / GROUP BY / FROM 子查询 / BETWEEN / IN ----

// Rel 表示一个"关系"：列名序列 + 行集。
// 单表列名为 alias.col（限定形式）；子查询列名为 subAlias.col（子查询输出列名）。
type Rel struct {
	Cols []string
	Rows [][]Value
}

// hasAdvancedSelect 判断 SELECT 是否走增强路径（保留旧单表路径的索引优化与既有行为）。
func hasAdvancedSelect(s *SelectStmt) bool {
	if s.Subquery != nil || len(s.Joins) > 0 || len(s.GroupBy) > 0 {
		return true
	}
	if s.Where != nil {
		for _, c := range s.Where.Conds {
			if c.Ref != "" || c.Op == "BETWEEN" || c.Op == "IN" || c.RightRef != "" || c.Sub != nil {
				return true
			}
		}
	}
	if s.OrderBy != nil && s.OrderBy.Ref != "" {
		return true
	}
	for _, c := range s.Columns {
		if c.Ref != "" || c.Alias != "" {
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

// relFromTable 解析 FROM / JOIN 右侧关系：物理表或子查询派生表。
func (e *Executor) relFromTable(tx txn.Txn, tabs []*TableMeta, table, alias string, sub *SelectStmt, subAlias string) (*Rel, error) {
	if sub != nil {
		subRes, err := e.execSelect(sub)
		if err != nil {
			return nil, err
		}
		rel := &Rel{Cols: make([]string, 0, len(subRes.Columns)), Rows: subRes.Rows}
		for _, c := range subRes.Columns {
			// 派生表列名 = 子查询别名 + 子查询输出列裸名（去掉内部限定前缀）
			rel.Cols = append(rel.Cols, subAlias+"."+baseColName(c))
		}
		return rel, nil
	}
	meta := findTable(tabs, table)
	if meta == nil {
		return nil, &SQLError{Msg: "table not exists: " + table}
	}
	return e.relFromScan(tx, meta, alias)
}

// relFromScan 全表扫描生成 Rel（列名限定为 alias.col；跨 region/跨节点展开）。
func (e *Executor) relFromScan(tx txn.Txn, meta *TableMeta, alias string) (*Rel, error) {
	pairs, err := e.scanMergedCluster(tx, e.rowRanges(meta))
	if err != nil {
		return nil, err
	}
	rel := &Rel{Cols: make([]string, 0, len(meta.Columns)), Rows: make([][]Value, 0, len(pairs))}
	for _, c := range meta.Columns {
		rel.Cols = append(rel.Cols, alias+"."+c.Name)
	}
	for _, kv := range pairs {
		row, err := decodeRow(meta, kv.Value)
		if err != nil {
			return nil, err
		}
		rel.Rows = append(rel.Rows, row)
	}
	return rel, nil
}

// joinRels 拼接左右 Rel：INNER 仅保留匹配行；LEFT 保留左行并以空串值补右列。
func joinRels(left, right *Rel, jc *JoinClause) (*Rel, error) {
	out := &Rel{Cols: append(append([]string{}, left.Cols...), right.Cols...)}
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

// execSelectRel 增强路径主流程：FROM（表/子查询）→ JOIN → WHERE → 聚合/分组 → 排序/投影。
func (e *Executor) execSelectRel(tx txn.Txn, s *SelectStmt) (*Result, error) {
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	// M12-2：单物理表（无子查询/JOIN/GROUP BY）+ 裸列 ORDER BY 分页时，
	// 走分布式排序捷径（各分片排序窗口 + coordinator 归并）；否则全量 Rel 路径。
	if s.Subquery == nil && len(s.Joins) == 0 && len(s.GroupBy) == 0 {
		if meta := findTable(tabs, s.From); meta != nil && e.canDistributedOrder(meta, s) {
			return e.execDistributedOrder(tx, meta, s)
		}
	}
	rel, err := e.relFromTable(tx, tabs, s.From, orAlias(s.From, s.FromAlias), s.Subquery, s.SubAlias)
	if err != nil {
		return nil, err
	}
	for i := range s.Joins {
		jc := &s.Joins[i]
		right, err := e.relFromTable(tx, tabs, jc.Table, orAlias(jc.Table, jc.Alias), jc.Subquery, jc.Alias)
		if err != nil {
			return nil, err
		}
		// M12：右表物理表且 ON 含右主键等值条件时走分布式路由 JOIN（跨 region/节点
		// 路由点查 + 合并）；否则回退 joinRels 全量拼接，行为与单机一致。
		rel, err = e.joinRel(tx, tabs, rel, right, jc)
		if err != nil {
			return nil, err
		}
	}
	// WHERE 过滤（M12-3：子查询条件预计算一次，行级过滤复用缓存）
	if s.Where != nil && len(s.Where.Conds) > 0 {
		cache, err := e.precomputeSubqueries(tx, tabs, rel, s.Where)
		if err != nil {
			return nil, err
		}
		filtered := make([][]Value, 0, len(rel.Rows))
		for _, r := range rel.Rows {
			ok, err := e.matchWhereRelCached(tx, rel, r, s.Where, cache)
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
		if hasWindow {
			return nil, &SQLError{Msg: "window function not supported with GROUP BY"}
		}
		return e.groupRel(tx, s, rel)
	}
	if hasAgg && hasWindow {
		return nil, &SQLError{Msg: "window function cannot be mixed with aggregate without GROUP BY"}
	}
	if hasAgg {
		return e.aggRel(s, rel)
	}
	return e.finishRel(tx, s, rel, rel.Rows)
}

// matchWhereRel 对 Rel 行执行 WHERE（支持限定列/右列引用/BETWEEN/IN/IN 子查询）。
// 子查询条件走 matchCondRel 动态执行（CASE 分支等无缓存场景兜底）。
func (e *Executor) matchWhereRel(tx txn.Txn, rel *Rel, row []Value, w *Where) (bool, error) {
	return e.matchWhereRelCached(tx, rel, row, w, nil)
}

func (e *Executor) matchCondRel(tx txn.Txn, rel *Rel, row []Value, c *Cond) (bool, error) {
	return e.matchCondRelCached(tx, rel, row, c, nil)
}

// evalCaseRel Rel 行级 CASE 求值（分支条件走 matchWhereRel，支持嵌套/限定列）。
func (e *Executor) evalCaseRel(tx txn.Txn, rel *Rel, row []Value, cc *CaseWhenClause) (Value, error) {
	for i := range cc.Branches {
		ok, err := e.matchWhereRel(tx, rel, row, &cc.Branches[i].Conds)
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
func (e *Executor) inSubquery(tx txn.Txn, v Value, sub *SelectStmt) (bool, error) {
	res, err := e.execSelect(sub)
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

// aggValues 计算单列聚合值（COUNT/SUM/AVG）。
func aggValues(rel *Rel, rows [][]Value, c *SelectColumn) (Value, error) {
	switch c.Agg {
	case "COUNT":
		return IntVal(int64(len(rows))), nil
	case "SUM", "AVG":
		// INT：int64 求和；DECIMAL：scaled int64 求和后保持精度。
		dec := false
		var isum int64
		var dsum int64
		for _, r := range rows {
			v, err := relValue(rel, r, qualifiedName(c.Ref, c.Col))
			if err != nil {
				return Value{}, err
			}
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
			if c.Agg == "SUM" {
				return decVal(dsum), nil
			}
			if len(rows) > 0 {
				return decVal(dsum / int64(len(rows))), nil
			}
			return decVal(0), nil
		}
		if c.Agg == "SUM" {
			return IntVal(isum), nil
		}
		if len(rows) > 0 {
			return IntVal(isum / int64(len(rows))), nil
		}
		return IntVal(0), nil
	}
	return Value{}, errf("unsupported aggregate %q", c.Agg)
}

// computeWindows 计算所有窗口列在 rows 上的逐行值（M10 窗口函数）。
// 返回 winVals[i][j] = 第 i 行、第 j 个窗口列的值；winCols 须按 SELECT 列表顺序传入。
func computeWindows(rows [][]Value, getter func(row []Value, col string) (Value, error), winCols []*SelectColumn) ([][]Value, error) {
	n := len(rows)
	out := make([][]Value, n)
	for i := range out {
		out[i] = make([]Value, len(winCols))
	}
	for j, c := range winCols {
		vals, err := computeWindowCol(rows, getter, c)
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
func computeWindowCol(rows [][]Value, getter func(row []Value, col string) (Value, error), c *SelectColumn) ([]Value, error) {
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
		// 组内稳定排序：按窗口 ORDER BY 键；无 ORDER BY 保持原行序
		sorted := make([]int, len(idxs))
		copy(sorted, idxs)
		keyMap := make(map[int]Value, len(idxs))
		if ws.OrderBy != nil {
			qname := qualifiedName(ws.OrderBy.Ref, ws.OrderBy.Col)
			for _, oi := range idxs {
				v, err := getter(rows[oi], qname)
				if err != nil {
					return nil, err
				}
				keyMap[oi] = v
			}
			sort.SliceStable(sorted, func(a, b int) bool {
				cmp := compareVal(keyMap[sorted[a]], keyMap[sorted[b]])
				if ws.OrderBy.Desc {
					return cmp > 0
				}
				return cmp < 0
			})
		}
		// 组内逐行窗口值：rank 为 RANK（带 gap），dense 为 DENSE_RANK（连续编号）
		rank := 1
		dense := 1
		var isum, dsum int64
		dec := false
		cnt := 0
		for t, oi := range sorted {
			if t > 0 && ws.OrderBy != nil {
				if compareVal(keyMap[sorted[t]], keyMap[sorted[t-1]]) != 0 {
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
			case "COUNT":
				cnt++
				v = IntVal(int64(cnt))
			case "SUM", "AVG":
				av, err := getter(rows[oi], c.Col)
				if err != nil {
					return nil, err
				}
				switch av.Kind {
				case "INT":
					isum += av.I
				case "DECIMAL":
					dec = true
					dsum += av.I
				default:
					return nil, &SQLError{Msg: "window aggregate on non-numeric column: " + c.Name}
				}
				cnt++
				if c.Agg == "SUM" {
					if dec {
						v = decVal(dsum)
					} else {
						v = IntVal(isum)
					}
				} else if dec {
					v = decVal(dsum / int64(cnt))
				} else {
					v = IntVal(isum / int64(cnt))
				}
			default:
				return nil, errf("unsupported window function %q", c.Agg)
			}
			vals[oi] = v
		}
	}
	return vals, nil
}

// aggRel 全局聚合（无 GROUP BY）。
func (e *Executor) aggRel(s *SelectStmt, rel *Rel) (*Result, error) {
	res := &Result{}
	out := make([]Value, 0, len(s.Columns))
	for _, c := range s.Columns {
		res.Columns = append(res.Columns, c.Name)
		v, err := aggValues(rel, rel.Rows, &c)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	res.Rows = append(res.Rows, out)
	return res, nil
}

// groupRel GROUP BY 分组聚合：每组一行；非聚合列必须属于分组键，取组内首行值。
func (e *Executor) groupRel(tx txn.Txn, s *SelectStmt, rel *Rel) (*Result, error) {
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
	for _, c := range s.Columns {
		res.Columns = append(res.Columns, c.Name)
	}
	for _, k := range order {
		g := groups[k]
		out := make([]Value, 0, len(s.Columns))
		for _, c := range s.Columns {
			if c.Case != nil {
				v, err := e.evalCaseRel(tx, rel, g.rows[0], c.Case)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
				continue
			}
			if c.Agg == "" {
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
			v, err := aggValues(rel, g.rows, &c)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		res.Rows = append(res.Rows, out)
	}
	return res, nil
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
func (e *Executor) finishRel(tx txn.Txn, s *SelectStmt, rel *Rel, rows [][]Value) (*Result, error) {
	if s.OrderBy != nil {
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
	// LIMIT / OFFSET（M12-2）
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
		}, winCols)
		if err != nil {
			return nil, err
		}
	}
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
				v, err := e.evalCaseRel(tx, rel, row, c.Case)
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
		res.Rows = append(res.Rows, out)
	}
	return res, nil
}

func (e *Executor) execUpdate(s *UpdateStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
	// 全表扫描 + 过滤（跨 region 展开后按内层键序合并）
	pairs, err := e.scanMergedCluster(tx, e.rowRanges(meta))
	if err != nil {
		return nil, err
	}
	affected := 0
	var ops []kvOp
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
		// 维护索引：先删旧索引键（收集 ops，暂不落事务缓冲）
		dops, err := e.delIndexKeyOps(meta, row, pk, rid)
		if err != nil {
			return nil, err
		}
		ops = append(ops, dops...)
		// M9 外键：父表被引用行禁止更新（ON UPDATE 动作未支持，RESTRICT 语义）。
		// 基于旧行判断（SET 修改前），防止把被引用的主键改走。
		referenced, err := e.fkParentReferenced(tx, tabs, meta, row)
		if err != nil {
			return nil, err
		}
		if referenced {
			return nil, &SQLError{Msg: "foreign key constraint violated: " + meta.Name + " is referenced by child table (ON UPDATE not supported)"}
		}
		for _, set := range s.Sets {
			idx := colIndex(meta, set.Col)
			if idx < 0 {
				return nil, &SQLError{Msg: "unknown column: " + set.Col}
			}
			v, err := coerceValue(meta.Columns[idx].Type, set.Col, set.Val)
			if err != nil {
				return nil, err
			}
			row[idx] = v
		}
		// 更新后行须满足自身引用完整性。
		if err := e.checkFKOnRow(tx, tabs, meta, row); err != nil {
			return nil, err
		}
		raw, err := encodeRow(meta, row)
		if err != nil {
			return nil, err
		}
		ops = append(ops, kvOp{Key: kv.Key, Value: raw})
		// 记录语句内最终行（用于唯一索引语句内冲突判定）
		e.dropPending(&pending, meta, pk)
		e.replacePending(&pending, meta, pk, row)
		// 写新索引键（收集 ops；唯一索引做磁盘 + pending 双重冲突检查）
		iops, err := e.putIndexKeyOps(tx, meta, row, pk, rid, &pending, nil)
		if err != nil {
			return nil, err
		}
		ops = append(ops, iops...)
		affected++
	}
	if auto {
		if err := e.exec2pcWrite(tx, ops); err != nil {
			return nil, err
		}
	} else {
		if e.hasRemoteOps(ops) {
			return nil, &SQLError{Msg: "distributed write inside explicit transaction not supported in M5 (autocommit 2PC only)"}
		}
		if err := applyOps(tx, ops); err != nil {
			return nil, err
		}
	}
	// M7 自动分裂：写提交后检查本地 region 数据规模（同 execInsert）。
	if err := e.maybeAutoSplit(meta); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: affected}, nil
}

func (e *Executor) execDelete(s *DeleteStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
	// 全表扫描 + 过滤（跨 region 展开后按内层键序合并）
	pairs, err := e.scanMergedCluster(tx, e.rowRanges(meta))
	if err != nil {
		return nil, err
	}
	affected := 0
	var ops []kvOp
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
		dops, err := e.delIndexKeyOps(meta, row, pkSuffixOf(meta, innerOf(kv.Key)), rid)
		if err != nil {
			return nil, err
		}
		ops = append(ops, dops...)
		// M9 外键：删除父表行时检查子表引用——RESTRICT 阻止删除，
		// CASCADE 级联收集子行删除操作（含其索引清理）。
		fkops, err := e.collectFKDeleteOps(tx, tabs, meta, row)
		if err != nil {
			return nil, err
		}
		ops = append(ops, fkops...)
		ops = append(ops, kvOp{Key: kv.Key, Delete: true})
		affected++
	}
	if auto {
		if err := e.exec2pcWrite(tx, ops); err != nil {
			return nil, err
		}
	} else {
		if e.hasRemoteOps(ops) {
			return nil, &SQLError{Msg: "distributed write inside explicit transaction not supported in M5 (autocommit 2PC only)"}
		}
		if err := applyOps(tx, ops); err != nil {
			return nil, err
		}
	}
	// M7 自动分裂：写提交后检查本地 region 数据规模（同 execInsert）。
	if err := e.maybeAutoSplit(meta); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: affected}, nil
}

// ---- 辅助 ----

func (e *Executor) buildRow(meta *TableMeta, cols []string, vals []Value) ([]Value, error) {
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
			return nil, &SQLError{Msg: "missing value for column: " + meta.Columns[i].Name}
		}
	}
	return e.coerceRow(meta, out)
}

// coerceRow 按列类型做值类型强制（支持 INT/TEXT/DATE/DECIMAL/BLOB）。
func (e *Executor) coerceRow(meta *TableMeta, vals []Value) ([]Value, error) {
	for i, c := range meta.Columns {
		v, err := coerceValue(c.Type, c.Name, vals[i])
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	return vals, nil
}

// pkOf 从行值提取主键字节（INT 定长 / TEXT 原字节）。
func (e *Executor) pkOf(meta *TableMeta, vals []Value) ([]byte, error) {
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
func (e *Executor) putIndexKeyOps(tx txn.Txn, meta *TableMeta, row []Value, pk []byte, rid storage.ID, pending *[][]Value, ignore map[string]bool) ([]kvOp, error) {
	ops := make([]kvOp, 0, len(meta.Indexes))
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		ci := colIndex(meta, idx.Col)
		val, err := idxBytes(row[ci])
		if err != nil {
			return nil, err
		}
		if idx.Unique {
			// 磁盘唯一性：扫描同值索引键区间，键归属行必须与当前行相同
			start := EncodeIndexKey2(meta.ID, idx.ID, val, nil)
			end := EncodeIndexKey2(meta.ID, idx.ID, append(append([]byte{}, val...), 0xff), nil)
			pairs, err := e.scanMergedCluster(tx, e.indexRanges(meta, idx.ID, start, end))
			if err != nil {
				return nil, err
			}
			for _, kv := range pairs {
				epk := indexPKSuffix(innerOf(kv.Key))
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
					pv, err := idxBytes(prow[ci])
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
		ci := colIndex(meta, idx.Col)
		val, err := idxBytes(row[ci])
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
func (e *Executor) putIndexKeys(tx txn.Txn, meta *TableMeta, row []Value, pk []byte, rid storage.ID, pending *[][]Value) error {
	ops, err := e.putIndexKeyOps(tx, meta, row, pk, rid, pending, nil)
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

// compareDec DECIMAL 与 INT/DECIMAL 数值比较。
func compareDec(a, b Value) int {
	av, aDec := decScaledOf(a)
	bv, bDec := decScaledOf(b)
	if aDec && bDec {
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	if aDec {
		return compareDecInt(av, bv)
	}
	return -compareDecInt(bv, av)
}

// decScaledOf 返回值的 scale=4 缩放表示（INT 直接放大；DECIMAL 取自身）。
func decScaledOf(v Value) (int64, bool) {
	if v.Kind == "DECIMAL" {
		return v.I, true
	}
	return v.I * decUnit, false
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

// slowQueryMaxDefault 内存慢查询记录默认上限（可 SetSlowQueryMax 调整）。
const slowQueryMaxDefault = 64

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
func (e *Executor) execCreateView(s *CreateViewStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
func (e *Executor) execDropView(s *DropViewStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	auto := e.autocommit()
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
func (e *Executor) execShowViews() (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	if e.autocommit() {
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
func (e *Executor) execShowTables() (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	if e.autocommit() {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []string{"table", "columns", "pk", "indexes"}}
	sort.Slice(tabs, func(i, j int) bool { return tabs[i].Name < tabs[j].Name })
	for _, t := range tabs {
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

// execShowIndex SHOW INDEX FROM t：列出指定表全部二级索引。
func (e *Executor) execShowIndex(s *ShowIndexStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	if e.autocommit() {
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

// execShowSlowQueries SHOW SLOWQUERIES：查看内存慢查询记录（新->旧）。
// M13 列：time | user | duration_ms | rows | error | sql。
func (e *Executor) execShowSlowQueries() (*Result, error) {
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

// execSet SET <name> = <value>：会话/服务变量设置（M9 查询缓存开关；
// M13 慢查询阈值与落盘开关，运行时变更即时生效）。
func (e *Executor) execSet(s *SetStmt) (*Result, error) {
	switch s.Name {
	case "query_cache":
		on := false
		switch s.Value {
		case "on", "true", "1":
			on = true
		case "off", "false", "0":
			on = false
		default:
			return nil, &SQLError{Msg: "invalid value for query_cache: " + s.Value + " (expected on/off/1/0)"}
		}
		e.qc.SetEnabled(on)
		return &Result{Columns: []string{"variable", "value"}}, nil
	case "slow_query_threshold":
		// M13：毫秒阈值，<=0 关闭记录。即时生效。
		ms, err := strconv.ParseInt(s.Value, 10, 64)
		if err != nil {
			return nil, &SQLError{Msg: "invalid value for slow_query_threshold: " + s.Value + " (expected integer ms)"}
		}
		e.SetSlowThreshold(ms)
		return &Result{Columns: []string{"variable", "value"}}, nil
	case "slow_query_log":
		// M13：慢查询落盘开关 on/off（路径由服务启动配置，见 SetSlowLog）。即时生效。
		on := false
		switch s.Value {
		case "on", "true", "1":
			on = true
		case "off", "false", "0":
			on = false
		default:
			return nil, &SQLError{Msg: "invalid value for slow_query_log: " + s.Value + " (expected on/off/1/0)"}
		}
		if e.slowLog != nil {
			e.slowLog.Configure(e.slowLog.Path(), on)
		}
		return &Result{Columns: []string{"variable", "value"}}, nil
	default:
		return nil, &SQLError{Msg: "unknown session variable: " + s.Name}
	}
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
func (e *Executor) execShowStats() (*Result, error) {
	res := &Result{Columns: []string{"metric", "value"}}
	appendKV := func(k string, v Value) {
		res.Rows = append(res.Rows, []Value{StrVal(k), v})
	}
	if e.connSource != nil {
		appendKV("connection_count", IntVal(e.connSource()))
	} else {
		appendKV("connection_count", IntVal(0))
	}
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	if e.autocommit() {
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
func (e *Executor) execExplain(s *ExplainStmt) (*Result, error) {
	tx, err := e.getTx()
	if err != nil {
		return nil, err
	}
	if e.autocommit() {
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
	lines := e.planOf(s.Select, meta)
	res := &Result{Columns: []string{"plan"}}
	for _, ln := range lines {
		res.Rows = append(res.Rows, []Value{StrVal(ln)})
	}
	return res, nil
}

// planOf 构建 SELECT 执行计划文本（M1 单表形态；增强形态给出对应算子提示）。
func (e *Executor) planOf(s *SelectStmt, meta *TableMeta) []string {
	var lines []string
	if len(s.Joins) > 0 {
		lines = append(lines, "access: nested-loop join ("+s.From+" + "+strconv.Itoa(len(s.Joins))+" table(s))")
	} else if s.Subquery != nil {
		lines = append(lines, "access: subquery scan (FROM ("+s.SubAlias+"))")
	} else {
		lines = append(lines, "access: "+e.accessOf(s, meta))
	}
	// 过滤算子
	if s.Where != nil && len(s.Where.Conds) > 0 {
		lines = append(lines, "filter: "+e.whereSummary(s.Where))
	}
	// JOIN ON 条件
	for i := range s.Joins {
		if len(s.Joins[i].On) > 0 {
			lines = append(lines, fmt.Sprintf("join%d on: %s", i+1, e.whereSummary(&Where{Conds: s.Joins[i].On})))
		}
	}
	if len(s.GroupBy) > 0 {
		lines = append(lines, "group: "+strings.Join(s.GroupBy, ", "))
	}
	hasAgg := false
	for _, c := range s.Columns {
		if c.Agg != "" {
			hasAgg = true
			break
		}
	}
	if hasAgg {
		lines = append(lines, "aggregate: yes")
	}
	if s.OrderBy != nil {
		lines = append(lines, "sort: "+s.OrderBy.Col+" "+orderDir(s.OrderBy.Desc))
	}
	if s.Limit > 0 {
		lines = append(lines, "limit: "+strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		lines = append(lines, "offset: "+strconv.Itoa(s.Offset))
	}
	lines = append(lines, "projection: "+e.projSummary(s))
	return lines
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

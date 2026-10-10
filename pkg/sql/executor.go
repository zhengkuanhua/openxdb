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
	// P2：慢查询配置与记录（内存）
	slowThreshold int64 // 毫秒阈值；<=0 表示不记录（默认 1000ms）
	slowQueries   []SlowQueryRecord
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
}

// splitConfig M7 自动分裂配置。
type splitConfig struct {
	enabled      bool
	rowThreshold int64 // 行数阈值；<=0 视为未开启
}

func NewExecutor(tm txn.TxnManager) *Executor {
	return &Executor{tm: tm, router: sharding.NewRouter(), failoverDone: map[string]bool{}, splitAcc: map[uint64]int64{}, qc: NewQueryCache()}
}

// SetSlowThreshold 设置慢查询阈值（毫秒，<=0 关闭记录）。
func (e *Executor) SetSlowThreshold(ms int64) { e.slowThreshold = ms }

// SetStatsSources 装配 SHOW STATS 指标注入源（T20，cmd/openxdb start 时注入）：
// conn 返回活跃连接数（server 层会话计数）；binlog 返回 binlog 最大位点（db 层）。
func (e *Executor) SetStatsSources(conn func() int64, binlog func() (uint64, error)) {
	e.connSource = conn
	e.binlogLSN = binlog
}

// SlowQueries 返回当前慢查询记录副本（SHOW SLOWQUERIES 与协议层读取共用）。
func (e *Executor) SlowQueries() []SlowQueryRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]SlowQueryRecord, len(e.slowQueries))
	copy(out, e.slowQueries)
	return out
}

// recordSlow 记录一条慢查询（保留最近 slowQueryMax 条）。
func (e *Executor) recordSlow(sql string, ms int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.slowQueries) >= slowQueryMax {
		e.slowQueries = e.slowQueries[1:]
	}
	e.slowQueries = append(e.slowQueries, SlowQueryRecord{
		SQL:      sql,
		Duration: ms,
		At:       timeNow(),
	})
}

// execTimed 执行一条语句并统计耗时（毫秒），超阈值记录慢查询。
// 若记录慢查询自身失败则原样返回执行错误。
func (e *Executor) execTimed(sql string, fn func() (*Result, error)) (*Result, error) {
	start := timeNowMs()
	res, err := fn()
	ms := timeNowMs() - start
	if err == nil && e.slowThreshold > 0 && ms >= e.slowThreshold {
		e.recordSlow(sql, ms)
	}
	return res, err
}

// Execute 执行一条解析后的语句；SELECT 等语句统计耗时并按阈值记录慢查询。
func (e *Executor) Execute(stmt Stmt) (*Result, error) {
	return e.ExecuteText(stmt, fmt.Sprintf("%T", stmt))
}

// ExecuteText 同 Execute，但慢查询记录使用原始 SQL 文本（Engine 层传入原文）。
func (e *Executor) ExecuteText(stmt Stmt, sqlText string) (*Result, error) {
	return e.execTimed(sqlText, func() (*Result, error) {
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
		*AssignRegionStmt, *SplitRegionStmt, *BalanceStmt, *BackupStmt, *RestoreStmt:
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
	return tx.Commit()
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
	return tx.Rollback()
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
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: len(pairs)}, nil
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
	idx := IndexMeta{ID: nextIndexID(meta), Name: s.Name, Col: s.Col}
	meta.Indexes = append(meta.Indexes, idx)
	// 回填：全表扫描逐行写索引键（meta.Indexes 已含新索引；跨 region 展开）
	pairs, err := e.scanMerged(tx, e.rowRanges(meta))
	if err != nil {
		return nil, err
	}
	for _, kv := range pairs {
		row, err := decodeRow(meta, kv.Value)
		if err != nil {
			return nil, err
		}
		pk := pkSuffixOf(meta, innerOf(kv.Key))
		if err := e.putIndexKeys(tx, meta, row, pk, ridOf(kv.Key)); err != nil {
			return nil, err
		}
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
		// 回填：全表扫描逐行写索引键（跨 region 展开）
		pairs, err := e.scanMerged(tx, e.rowRanges(meta))
		if err != nil {
			return nil, err
		}
		for _, kv := range pairs {
			row, err := decodeRow(meta, kv.Value)
			if err != nil {
				return nil, err
			}
			pk := pkSuffixOf(meta, innerOf(kv.Key))
			if err := e.putIndexKeys(tx, meta, row, pk, ridOf(kv.Key)); err != nil {
				return nil, err
			}
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
	for _, row := range s.Rows {
		rops, ok, err := e.insertRowOps(tx, tabs, meta, s.Columns, row, false)
		if err != nil {
			return nil, err
		}
		if ok {
			affected++
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
	idxOps, err := e.putIndexKeyOps(meta, vals, pk, rid)
	if err != nil {
		return nil, false, err
	}
	ops = append(ops, idxOps...)
	return ops, true, nil
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
		// 全表扫描 + 过滤（跨 region/跨节点展开后按内层键序合并，等价单 region 全表序）
		pairs, err := e.scanMergedCluster(tx, e.rowRanges(meta))
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
	// LIMIT
	if s.Limit > 0 && len(rows) > s.Limit {
		rows = rows[:s.Limit]
	}
	// 聚合
	hasAgg := false
	for _, c := range s.Columns {
		if c.Agg != "" {
			hasAgg = true
			break
		}
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
		rel, err = joinRels(rel, right, jc)
		if err != nil {
			return nil, err
		}
	}
	// WHERE 过滤
	if s.Where != nil && len(s.Where.Conds) > 0 {
		filtered := make([][]Value, 0, len(rel.Rows))
		for _, r := range rel.Rows {
			ok, err := e.matchWhereRel(tx, rel, r, s.Where)
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
	for _, c := range s.Columns {
		if c.Agg != "" {
			hasAgg = true
			break
		}
	}
	if len(s.GroupBy) > 0 {
		return e.groupRel(tx, s, rel)
	}
	if hasAgg {
		return e.aggRel(s, rel)
	}
	return e.finishRel(tx, s, rel, rel.Rows)
}

// matchWhereRel 对 Rel 行执行 WHERE（支持限定列/右列引用/BETWEEN/IN/IN 子查询）。
func (e *Executor) matchWhereRel(tx txn.Txn, rel *Rel, row []Value, w *Where) (bool, error) {
	if w == nil || len(w.Conds) == 0 {
		return true, nil
	}
	for _, c := range w.Conds {
		ok, err := e.matchCondRel(tx, rel, row, &c)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (e *Executor) matchCondRel(tx txn.Txn, rel *Rel, row []Value, c *Cond) (bool, error) {
	var a Value
	var err error
	if c.LeftCase != nil {
		a, err = e.evalCaseRel(tx, rel, row, c.LeftCase)
		if err != nil {
			return false, err
		}
	} else {
		a, err = relValue(rel, row, qualifiedName(c.Ref, c.Col))
		if err != nil {
			return false, err
		}
	}
	switch c.Op {
	case "=", "!=", "<", ">", "<=", ">=":
		if c.RightRef != "" {
			b, err := relValue(rel, row, c.RightRef)
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
		if c.Sub != nil {
			return e.inSubquery(tx, a, c.Sub)
		}
		for _, v := range c.InList {
			if compareVal(a, v) == 0 {
				return true, nil
			}
		}
		return false, nil
	}
	return false, errf("unsupported op %q", c.Op)
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
	for _, row := range rows {
		var out []Value
		for _, c := range s.Columns {
			if c.Name == "*" {
				out = append(out, row...)
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
		// 写新索引键（收集 ops）
		iops, err := e.putIndexKeyOps(meta, row, pk, rid)
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
func (e *Executor) putIndexKeyOps(meta *TableMeta, row []Value, pk []byte, rid storage.ID) ([]kvOp, error) {
	ops := make([]kvOp, 0, len(meta.Indexes))
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		ci := colIndex(meta, idx.Col)
		val, err := idxBytes(row[ci])
		if err != nil {
			return nil, err
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
func (e *Executor) putIndexKeys(tx txn.Txn, meta *TableMeta, row []Value, pk []byte, rid storage.ID) error {
	ops, err := e.putIndexKeyOps(meta, row, pk, rid)
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

// ---- P2：运维语句执行 ----

// slowQueryMax 内存慢查询记录上限。
const slowQueryMax = 64

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
func (e *Executor) execShowSlowQueries() (*Result, error) {
	recs := e.SlowQueries()
	res := &Result{Columns: []string{"time", "duration_ms", "sql"}}
	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		res.Rows = append(res.Rows, []Value{
			StrVal(r.At),
			IntVal(r.Duration),
			StrVal(r.SQL),
		})
	}
	return res, nil
}

// execSet SET <name> = <value>：会话变量设置（M9 查询缓存开关）。
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

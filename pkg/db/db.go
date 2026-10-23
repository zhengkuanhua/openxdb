// Package db 提供 OpenXDB 数据目录与数据库句柄（M1 T5，开发手册 §3.7）。
// 数据目录布局：
//
//	<data-dir>/
//	  openxdb.conf  配置文件（引擎、版本、创建时间）
//	  data/         RocksDB 数据目录
//	  wal.log       WAL 日志（首次 Open 时创建）
//
// DB.Open 完成 存储 + WAL + 事务层 的装配，并在返回前执行崩溃恢复（WAL 重放）。
package db

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/logx"
	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/rocksdb"
	"github.com/zhengkuanhua/openxdb/pkg/tso"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
	"github.com/zhengkuanhua/openxdb/pkg/wal"
)

const (
	// ConfigFile 数据目录配置文件。
	ConfigFile = "openxdb.conf"
	// DataSubDir RocksDB 数据子目录。
	DataSubDir = "data"
	// WALFile WAL 日志文件名。
	WALFile = "wal.log"
	// BinlogFile 复制日志（binlog）文件名。
	BinlogFile = "binlog.log"
	// PersistedConfigFile 在线配置热更新持久化文件（M16-1，SET PERSIST 落盘，
	// 重启由 Open 加载合并：内存 > 持久化 > 默认值）。
	PersistedConfigFile = "persisted.json"
	// EpochMetaFile P0 HA epoch 持久化文件（记录已知最大 clusterEpoch，
	// 重启加载作为单调下限，防回退）。
	EpochMetaFile = "epoch.meta"
	// DefaultEngine 当前存储引擎。
	DefaultEngine = "rocksdb"
)

// 标准错误。
var (
	// ErrAlreadyInitialized 数据目录已初始化（再次 Init）。
	ErrAlreadyInitialized = errors.New("db: data directory already initialized")
	// ErrNotInitialized 数据目录未初始化（Open 前须 Init）。
	ErrNotInitialized = errors.New("db: data directory not initialized")
	// ErrReplicationStarted 主节点复制已启动。
	ErrReplicationStarted = errors.New("db: replication already started")
	// ErrFollowerStarted 从节点复制已启动。
	ErrFollowerStarted = errors.New("db: follower already started")
)

// DB 已装配的数据库句柄：存储 / WAL / 事务管理器 / SQL 引擎 一致关闭。
type DB struct {
	Dir     string
	Storage storage.Storage
	WAL     wal.WAL
	Txn     txn.TxnManager
	SQL     *sql.Engine

	// M2 复制组件（未启用时为 nil）。
	Replicator *replication.MasterReplicator // 主节点复制器（StartReplication 后非 nil）
	Follower   *replication.Follower         // 从节点复制器（StartFollower 后非 nil）

	// M4 集群组件（Open 即创建管理器；StartCluster 后服务端监听节点链路）。
	Cluster    *cluster.Manager // 节点注册表 + 心跳 + 查询转发
	ClusterSrv *cluster.Server  // 节点链路服务（StartCluster 后非 nil）

	// M8 全局时间戳源（Open 创建本地发号器并注入 SQL 引擎；StartCluster
	// 注入节点服务，作为中心化发号权威服务 TSO_REQ）。
	TSO tso.Source

	// M6 主从自动故障转移（EnableAutoFailover 后非零；StopAutoFailover 置零）。
	failoverStop chan struct{}
	failoverMu   sync.Mutex

	// P0 HA 脑裂防护（最小防双主）：clusterEpoch 与升主仲裁。
	// epoch 复用 TSO 单调发号 + 本地持久化兜底防回退；ha_epoch_enabled /
	// ha_quorum_enabled 默认关闭，关闭时以上字段全部保持零值/空，零回归。
	epochMu       sync.Mutex
	epoch         uint64 // 本节点已知全局最大 epoch（授权与广播的并集）
	masterEpoch   uint64 // 本节点当前 master 授权 epoch（0 = 未授权主）
	epochEnabled  bool   // ha_epoch_enabled：epoch 授权与写入 fencing（默认 off）
	quorumEnabled bool   // ha_quorum_enabled：升主多数派仲裁（默认 off）
	epochFilePath string // EpochMetaFile 持久化路径（Open 时装配）

	// P2 HA：分区恢复合并（复制轨收敛 + 数据冲突标记）。
	// NoteEpoch 检测到本节点为陈旧主（masterEpoch < 广播 epoch）时自动
	// 降级只读并停止复制；写路径被 fencing 拒绝计入冲突标记。开关关闭
	// 时以上字段保持零值/空，零回归。
	readOnly     bool   // 陈旧主降级后的只读标记（分区恢复收敛）
	conflictCount uint64 // 数据冲突标记：陈旧主写尝试被拒 / 恢复合并降级计数
	demoteHook   func() // 降级观察回调（测试/上层通知用，nil 忽略）
}

// Init 初始化数据目录：创建 data/ 子目录与 openxdb.conf。
// 已初始化时返回 ErrAlreadyInitialized（不重复覆盖配置）。
func Init(dir string) error {
	if dir == "" {
		return fmt.Errorf("db: empty data dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("db: create data dir: %w", err)
	}
	confPath := filepath.Join(dir, ConfigFile)
	if _, err := os.Stat(confPath); err == nil {
		return ErrAlreadyInitialized
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("db: stat config: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, DataSubDir), 0o755); err != nil {
		return fmt.Errorf("db: create data subdir: %w", err)
	}
	conf := fmt.Sprintf(`# OpenXDB data directory config
engine = %s
node_id = %s
created_at = %s
`, DefaultEngine, clusterNodeID(dir), time.Now().Format(time.RFC3339))
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		return fmt.Errorf("db: write config: %w", err)
	}
	return nil
}

// Open 打开数据目录：装配 RocksDB + WAL + 事务管理器，并完成崩溃恢复。
// 目录未 Init 时返回 ErrNotInitialized。
func Open(dir string) (*DB, error) {
	if dir == "" {
		return nil, fmt.Errorf("db: empty data dir")
	}
	confPath := filepath.Join(dir, ConfigFile)
	b, err := os.ReadFile(confPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotInitialized
		}
		return nil, fmt.Errorf("db: read config: %w", err)
	}
	if !strings.Contains(string(b), "engine = "+DefaultEngine) {
		return nil, fmt.Errorf("db: unsupported engine in %s", confPath)
	}

	// 首次 Open 时 data/ 为空目录，需 createIfMissing=true 初始化引擎文件；
	// 后续 Open 引擎文件已存在，true 亦无害（不会覆盖已有数据）。
	st, err := rocksdb.Open(filepath.Join(dir, DataSubDir), true)
	if err != nil {
		logx.DefaultAlerts.Alert("storage", "storage_open_failed", fmt.Sprintf("open storage in %s: %v", dir, err))
		return nil, fmt.Errorf("db: open storage: %w", err)
	}
	w, err := wal.Open(filepath.Join(dir, WALFile))
	if err != nil {
		logx.DefaultAlerts.Alert("wal", "wal_open_failed", fmt.Sprintf("open wal in %s: %v", dir, err))
		_ = st.Close()
		return nil, fmt.Errorf("db: open wal: %w", err)
	}
	// M2 生产接线：默认启用 WAL 组提交（P0-4）。
	// Append 仅写缓冲并快速分配 LSN，持久化由 txn 提交路径的 SyncUpTo 兜底，
	// 多事务并发提交时合并 fsync；语义与逐条 fsync 等价（崩溃恢复由
	// SyncUpTo/Close 保证），显著降低高并发写入的 fsync 开销。
	w.SetGroupCommit(true)
	tm, err := txn.New(st, w)
	if err != nil {
		logx.DefaultAlerts.Alert("storage", "txn_init_failed", fmt.Sprintf("new txn manager: %v", err))
		_ = w.Close()
		_ = st.Close()
		return nil, fmt.Errorf("db: new txn manager: %w", err)
	}
	if err := tm.Recover(); err != nil {
		logx.DefaultAlerts.Alert("wal", "wal_recover_failed", fmt.Sprintf("recover wal: %v", err))
		_ = tm.Close()
		_ = st.Close()
		return nil, fmt.Errorf("db: recover: %w", err)
	}
	// M3：加载落盘分片元数据到 Router（无分片元数据时为空路由表，Router 隐式单 region 兜底）。
	var seq uint64
	var regions []sharding.RegionInfo
	if raw, err := st.Get([]byte("m:regions")); err != nil {
		if err != storage.ErrNotFound {
			_ = tm.Close()
			_ = st.Close()
			return nil, fmt.Errorf("db: load region catalog: %w", err)
		}
	} else if len(raw) > 0 {
		seq, regions, err = sharding.UnmarshalMeta(raw)
		if err != nil {
			_ = tm.Close()
			_ = st.Close()
			return nil, fmt.Errorf("db: corrupt region catalog: %w", err)
		}
	}
	eng := sql.New(tm)
	eng.LoadSharding(seq, regions)
	// M4：恢复节点注册表（m:nodes）并创建集群管理器；selfAddr 由 StartCluster 填充。
	mgr := cluster.NewManager(clusterNodeID(dir))
	if raw, err := st.Get(cluster.MetaNodesKey()); err != nil {
		if err != storage.ErrNotFound {
			_ = tm.Close()
			_ = st.Close()
			return nil, fmt.Errorf("db: load node catalog: %w", err)
		}
	} else if len(raw) > 0 {
		nodes, err := cluster.UnmarshalNodes(raw)
		if err != nil {
			_ = tm.Close()
			_ = st.Close()
			return nil, fmt.Errorf("db: corrupt node catalog: %w", err)
		}
		mgr.SetNodes(nodes)
	}
	mgr.RegisterSelf("")
	eng.SetCluster(mgr, mgr.SelfID)
	// M8：本地时间戳源（中心化发号权威；单机模式退化为本地自增，零回归）。
	ts := tso.New()
	eng.SetTSO(ts)
	// M8(BR)：注入备份/恢复环境（引擎级 Storage 引用 + 数据目录 binlog 路径）。
	eng.SetBackupEnv(st, filepath.Join(dir, BinlogFile))
	// M16-1：在线配置热更新——装配持久化配置文件（data 目录 persisted.json）
	// 并加载合并（优先级：内存 > 持久化 > 默认值）。文件缺失 = 首次启动空表；
	// 损坏/非法键/非法值明确报错，绝不静默忽略。
	eng.SetPersistedConfigPath(filepath.Join(dir, PersistedConfigFile))
	if err := eng.LoadPersistedConfig(); err != nil {
		_ = tm.Close()
		_ = st.Close()
		return nil, fmt.Errorf("db: load persisted config: %w", err)
	}
	db := &DB{Dir: dir, Storage: st, WAL: w, Txn: tm, SQL: eng, Cluster: mgr, TSO: ts}
	// P0 HA：加载 epoch.meta（已知最大 clusterEpoch 单调下限，防回退）。
	db.epochFilePath = filepath.Join(dir, EpochMetaFile)
	db.loadEpochMeta()
	// T33：装配复制模式控制器（SET replication_* 热切换入口）。
	eng.SetReplicationModeSetter(db)
	// P0 HA：装配写入 fencing 检查（epoch 校验拒陈旧主写路径；开关关闭零开销）。
	eng.SetWriteFence(db)
	// P0 HA：装配 epoch/quorum 开关节流控制器（SET ha_* 热切换入口）。
	eng.SetHAConfigSetter(db)
	// P1 HA：装配 clusterEpoch 取号源与 quorum 仲裁器（FailoverDownNode 前置
	// 多数派确认 + failoverRegion epoch 授权；开关关闭零回归）。
	eng.SetEpochSource(db)
	eng.SetQuorumArbiter(db)
	return db, nil
}

// SetTSONode 将本节点时间戳源切换为指向 nodeID 节点的远程 TSO 客户端
// （中心化部署：所有非中心节点调用本方法指向同一发号节点，保证全局单调
// 可比；单机 / 未配置时保持本地发号，零回归）。batch<=0 时使用默认批次。
func (d *DB) SetTSONode(nodeID string, batch uint64) error {
	src := cluster.NewTSOClient(d.Cluster, nodeID, batch)
	d.TSO = src
	d.SQL.SetTSO(src)
	if d.ClusterSrv != nil {
		d.ClusterSrv.SetTSO(src)
	}
	return nil
}

// StartCluster 启动 M4 集群节点链路：监听 addr 提供节点握手/查询转发/数据装载，
// 并启动心跳循环检测远端节点存活。addr 支持 ":0" 随机端口，返回实际监听地址。
func (d *DB) StartCluster(addr string) (string, error) {
	if d.ClusterSrv != nil {
		return "", errors.New("db: cluster already started")
	}
	srv := cluster.NewServer(d.Storage, d.Cluster.SelfID)
	srv.SetRouter(d.SQL.ExecutorRouter())
	srv.SetManager(d.Cluster)
	// T34：装配节点侧 DDL 元数据 apply（跨节点 DDL 广播的接收端）。
	srv.SetDDLApply(d.SQL.ApplyRemoteDDL)
	// T35：装配节点侧分布式备份/恢复回调（备份候选点、对齐备份、对齐恢复）。
	srv.SetBackupPrepare(d.SQL.RemoteBackupPrepare)
	srv.SetBackupDo(d.SQL.RemoteBackupDo)
	srv.SetRestoreDo(d.SQL.RemoteRestoreDo)
	// M8：注入中心化时间戳源（TSO_REQ 由本节点批量发放）。
	srv.SetTSO(d.TSO)
	// P0 HA：装配 epoch 广播接收回调（升主节点广播新 epoch 时更新本节点
	// 已知最大 epoch，作为陈旧主写入 fencing 依据；开关关闭时无副作用）。
	srv.SetEpochNotify(d.NoteEpoch)
	// P1 HA：装配路由消息 epoch 校验回调（REGION_PUSH/路由广播接收端校验
	// clusterEpoch+regionEpoch 新鲜度，拒绝陈旧消息；开关关闭/旧节点零回归）。
	srv.SetRegionEpochChecker(d.SQL.CheckRegionPushEpoch)
	// M5：2PC 崩溃恢复——清理未决 prepare（未决事务回滚，数据从未写入）
	// 与历史幂等标记（仅运行期有效），保证重启后参与者状态干净可重入。
	if err := srv.Recover2PC(); err != nil {
		return "", fmt.Errorf("db: recover 2pc: %w", err)
	}
	// M8：协调者崩溃恢复——依据持久化 CoordRecord 决策 commit/abort 并
	// 驱动参与者幂等收尾（prepared/committing -> commit；preparing/aborted -> abort）。
	if err := srv.RecoverCoordinated2PC(); err != nil {
		return "", fmt.Errorf("db: recover coordinated 2pc: %w", err)
	}
	realAddr, err := srv.ListenAndServe(addr)
	if err != nil {
		return "", fmt.Errorf("db: serve cluster: %w", err)
	}
	srv.SetAddr(realAddr)
	d.Cluster.RegisterSelf(realAddr)
	d.ClusterSrv = srv
	// 心跳：周期 PING 远端节点，超时标记 DOWN（复用 M2 帧协议）。
	// M6：节点离线后自动触发 region 重指派（SQL 层读 failover 依赖新路由）。
	d.Cluster.SetOnDown(func(nodeID string) { _, _ = d.SQL.FailoverDownNode(nodeID) })
	d.Cluster.StartHeartbeat(2*time.Second, 6*time.Second)
	return realAddr, nil
}

// clusterNodeID 读取或生成节点 ID：优先 openxdb.conf 的 node_id，
// 未初始化该字段的旧数据目录以数据目录短哈希兜底（保证多节点部署可区分）。
func clusterNodeID(dir string) string {
	confPath := filepath.Join(dir, ConfigFile)
	if b, err := os.ReadFile(confPath); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "node_id = ") {
				if id := strings.TrimPrefix(line, "node_id = "); id != "" {
					return id
				}
			}
		}
	}
	h := crc32.ChecksumIEEE([]byte(filepath.Clean(dir)))
	return fmt.Sprintf("node-%08x", h)
}

// StartReplication 启动主节点复制（M2）：创建 binlog 并监听复制端口 addr，
// 事务提交成功后同步落 binlog、按位点异步推送从节点。addr 支持 ":0" 随机端口。
// 复制端口为明文（旧行为）。
func (d *DB) StartReplication(addr string) error {
	return d.StartReplicationTLS(addr, nil)
}

// StartReplicationTLS 与 StartReplication 等价，但复制端口以 TLS 加密监听
// （P0 TLS，可选）：tlsCfg 非 nil 时从节点须以 TLS 连接；nil 保持明文。
func (d *DB) StartReplicationTLS(addr string, tlsCfg *tls.Config) error {
	if d.Replicator != nil {
		return ErrReplicationStarted
	}
	r, err := replication.NewMasterTLS(filepath.Join(d.Dir, BinlogFile), addr, tlsCfg)
	if err != nil {
		return fmt.Errorf("db: new replicator: %w", err)
	}
	if err := r.Serve(); err != nil {
		_ = r.Close()
		return fmt.Errorf("db: serve replicator: %w", err)
	}
	// 事务提交成功后按提交顺序同步写 binlog；错误仅记录，不改变单机提交语义。
	d.Txn.SetCommitHook(func(lsn storage.LSN, seq uint64, batch *storage.WriteBatch) error {
		return r.Append(&replication.BinlogEntry{CommitLSN: lsn, Batch: batch, SchemaVer: 0})
	})
	// T33 提交后置确认钩子（锁外）：按当前复制模式等待副本 ACK。
	// async 模式立即返回（M2 兼容零开销）；sync/semi-sync 由复制器内部处理。
	d.Txn.SetCommitWaitHook(func(lsn storage.LSN, seq uint64) error {
		return r.WaitAck(context.Background(), lsn)
	})
	// P0 HA：epoch 授权（仅 ha_epoch_enabled 开启时）。成为 master 即取新
	// clusterEpoch 并广播到集群；此后收到更高 epoch 广播（其它节点升主）
	// 时本节点写路径将被 fencing 拒绝，防双主。
	if d.epochEnabled {
		e := d.acquireEpoch()
		d.setMasterEpoch(e)
		d.broadcastEpoch(e)
	}
	d.Replicator = r
	return nil
}

// SetReplicationMode 热切换主端复制模式（T33）。
// mode 取值 replication.ModeAsync / ModeSemiSync / ModeSync；ackN 为半同步
// 最少确认副本数（async 忽略）；timeout 为 ACK 等待超时（<=0 用默认 10s）。
func (d *DB) SetReplicationMode(mode replication.ReplMode, ackN int, timeout time.Duration) error {
	if d.Replicator == nil {
		return replication.ErrReplicationNotStarted
	}
	return d.Replicator.SetReplicationMode(mode, ackN, timeout)
}

// ReplicationSyncStatus 返回主端复制同步状态（T33）。
func (d *DB) ReplicationSyncStatus() replication.SyncStatus {
	if d.Replicator == nil {
		return replication.SyncStatus{Mode: replication.ModeAsync}
	}
	return d.Replicator.SyncStatus()
}

// SetMode 实现 sql.ReplicationModeSetter（T33）：字符串模式 + 毫秒超时转内部调用。
func (d *DB) SetMode(mode string, ackN int, timeoutMs int64) error {
	m, err := replication.ParseReplMode(mode)
	if err != nil {
		return err
	}
	return d.SetReplicationMode(m, ackN, time.Duration(timeoutMs)*time.Millisecond)
}

// ModeStatus 实现 sql.ReplicationModeSetter（T33）。
func (d *DB) ModeStatus() string { return d.ReplicationSyncStatus().Mode.String() }

// AckStatus 实现 sql.ReplicationModeSetter（T33）：半同步最少确认副本数。
func (d *DB) AckStatus() int {
	st := d.ReplicationSyncStatus()
	if st.Mode != replication.ModeSemiSync {
		return 1
	}
	return st.SemiAckN
}

// TimeoutStatus 实现 sql.ReplicationModeSetter（T33）：ACK 等待超时毫秒。
func (d *DB) TimeoutStatus() int64 { return d.ReplicationSyncStatus().Timeout.Milliseconds() }

// StartFollower 以从节点身份连接主节点 masterAddr 并开始异步复制（M2）。
// 起始位点 = 当前已应用位点 + 1（断线重连可续传）；状态用 FollowerStatus 查询。
// 复制连接为明文（旧行为）。
func (d *DB) StartFollower(masterAddr string) error {
	return d.StartFollowerTLS(masterAddr, nil)
}

// StartFollowerTLS 与 StartFollower 等价，但以 TLS 加密连接主节点复制端口
// （P0 TLS，可选）：tlsCfg 非 nil 时使用 TLS（自签名/内网可传
// InsecureSkipVerify）；nil 保持明文。
func (d *DB) StartFollowerTLS(masterAddr string, tlsCfg *tls.Config) error {
	if d.Follower != nil {
		return ErrFollowerStarted
	}
	f := replication.NewFollower(d.Storage)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		_ = f.StartTLS(ctx, masterAddr, tlsCfg) // 错误进入 FollowerStatus.LastError
	}()
	d.Follower = f
	return nil
}

// ReplicationStatus 返回主节点复制监听地址与从节点列表（未启用复制时 address 为空）。
func (d *DB) ReplicationStatus() (address string, slaves []replication.SlaveStatus) {
	if d.Replicator == nil {
		return "", nil
	}
	return d.Replicator.Addr(), d.Replicator.SlaveStatus()
}

// BinlogLSN 返回当前 binlog 最大位点（T20 运维监控，只读不创建）。
// 未启用复制时读取数据目录 binlog 文件（存在即返回位点，不存在返回 0, nil）。
func (d *DB) BinlogLSN() (uint64, error) {
	path := filepath.Join(d.Dir, BinlogFile)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	lsn, err := replication.PeekLastLSN(path)
	return uint64(lsn), err
}

// Close 关闭事务管理器、存储与 WAL（顺序与 Open 相反）。
func (d *DB) Close() error {
	var errs []error
	if d.Cluster != nil {
		d.Cluster.Stop() // 停止心跳并关闭复用连接
	}
	if d.ClusterSrv != nil {
		if err := d.ClusterSrv.Close(); err != nil {
			errs = append(errs, fmt.Errorf("db: close cluster: %w", err))
		}
		d.ClusterSrv = nil
	}
	// M6：先停止自动故障转移监控，避免 Close 过程中误触发 promote。
	d.StopAutoFailover()
	// M7：停止自动均衡循环，避免 Close 过程中误触发 region 迁移。
	if d.SQL != nil {
		d.SQL.StopAutoBalance()
	}
	// BR 方向②：停止备份调度循环，避免 Close 过程中误触发 BACKUP
	//（备份依赖存储快照，存储关闭前必须停）。
	if d.SQL != nil {
		d.SQL.StopBackupScheduler()
	}
	// T37：停止告警规则评估循环，避免 Close 过程中并发取指标快照/写通知
	//（评估依赖 metrics 注册表与 logx 出口，存储关闭前必须停）。
	if d.SQL != nil {
		d.SQL.StopAlertRules()
	}
	// P0：停止 2PC 后台补提交扫描，避免 Close 过程中并发读写存储/网络。
	if d.SQL != nil {
		d.SQL.StopCoordReplay()
	}
	if d.Follower != nil {
		if err := d.Follower.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("db: close follower: %w", err))
		}
		d.Follower = nil
	}
	if d.Replicator != nil {
		if err := d.Replicator.Close(); err != nil {
			errs = append(errs, fmt.Errorf("db: close replicator: %w", err))
		}
		d.Replicator = nil
	}
	if d.Txn != nil {
		if err := d.Txn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("db: close txn: %w", err))
		}
	}
	if d.WAL != nil {
		if err := d.WAL.Close(); err != nil {
			errs = append(errs, fmt.Errorf("db: close wal: %w", err))
		}
	}
	if d.Storage != nil {
		if err := d.Storage.Close(); err != nil {
			errs = append(errs, fmt.Errorf("db: close storage: %w", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("db: close errors: %v", errs)
	}
	return nil
}

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
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/rocksdb"
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
		return nil, fmt.Errorf("db: open storage: %w", err)
	}
	w, err := wal.Open(filepath.Join(dir, WALFile))
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("db: open wal: %w", err)
	}
	tm, err := txn.New(st, w)
	if err != nil {
		_ = w.Close()
		_ = st.Close()
		return nil, fmt.Errorf("db: new txn manager: %w", err)
	}
	if err := tm.Recover(); err != nil {
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
	return &DB{Dir: dir, Storage: st, WAL: w, Txn: tm, SQL: eng, Cluster: mgr}, nil
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
	// M5：2PC 崩溃恢复——清理未决 prepare（未决事务回滚，数据从未写入）
	// 与历史幂等标记（仅运行期有效），保证重启后参与者状态干净可重入。
	if err := srv.Recover2PC(); err != nil {
		return "", fmt.Errorf("db: recover 2pc: %w", err)
	}
	realAddr, err := srv.ListenAndServe(addr)
	if err != nil {
		return "", fmt.Errorf("db: serve cluster: %w", err)
	}
	srv.SetAddr(realAddr)
	d.Cluster.RegisterSelf(realAddr)
	d.ClusterSrv = srv
	// 心跳：周期 PING 远端节点，超时标记 DOWN（复用 M2 帧协议）。
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
func (d *DB) StartReplication(addr string) error {
	if d.Replicator != nil {
		return ErrReplicationStarted
	}
	r, err := replication.NewMaster(filepath.Join(d.Dir, BinlogFile), addr)
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
	d.Replicator = r
	return nil
}

// StartFollower 以从节点身份连接主节点 masterAddr 并开始异步复制（M2）。
// 起始位点 = 当前已应用位点 + 1（断线重连可续传）；状态用 FollowerStatus 查询。
func (d *DB) StartFollower(masterAddr string) error {
	if d.Follower != nil {
		return ErrFollowerStarted
	}
	f := replication.NewFollower(d.Storage)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		_ = f.Start(ctx, masterAddr) // 错误进入 FollowerStatus.LastError
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

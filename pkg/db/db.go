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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	// DefaultEngine 当前存储引擎。
	DefaultEngine = "rocksdb"
)

// 标准错误。
var (
	// ErrAlreadyInitialized 数据目录已初始化（再次 Init）。
	ErrAlreadyInitialized = errors.New("db: data directory already initialized")
	// ErrNotInitialized 数据目录未初始化（Open 前须 Init）。
	ErrNotInitialized = errors.New("db: data directory not initialized")
)

// DB 已装配的数据库句柄：存储 / WAL / 事务管理器 / SQL 引擎 一致关闭。
type DB struct {
	Dir     string
	Storage storage.Storage
	WAL     wal.WAL
	Txn     txn.TxnManager
	SQL     *sql.Engine
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
created_at = %s
`, DefaultEngine, time.Now().Format(time.RFC3339))
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
	return &DB{Dir: dir, Storage: st, WAL: w, Txn: tm, SQL: sql.New(tm)}, nil
}

// Close 关闭事务管理器、存储与 WAL（顺序与 Open 相反）。
func (d *DB) Close() error {
	var errs []error
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

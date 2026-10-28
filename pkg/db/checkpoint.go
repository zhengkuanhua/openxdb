package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// WAL 自动 checkpoint 默认参数（与 pkg/sql/config.go 中默认值保持一致）。
const (
	defaultCheckpointInterval = 60 * time.Second
	defaultCheckpointMaxMB    = 64
)

// CheckpointStats checkpoint 运行统计（观测/测试辅助）。
type CheckpointStats struct {
	Runs          int64       // 累计检查次数
	Truncations   int64       // 累计实际截断次数
	LastTruncated storage.LSN // 最近一次截断位点（safe+1，即保留起点）
	LastRun       string      // 最近一次检查时间（2006-01-02 15:04:05）
	LastError     string      // 最近一次错误（无错误为空）
}

// checkpointController 周期执行 WAL 自动 checkpoint：在「存储层已应用」且
// 「WAL 已 fsync」的双重安全位点处调用 WAL.Truncate 截断已持久化记录，
// 使 wal.log 不再随提交单调增长（v1.16.0）。
//
// 安全位点推导：
//   - WAL.LastSyncedLSN()：已 fsync 落盘的最大 LSN。截断只删 <= 该位点的
//     记录，避免删除尚未落盘（崩溃即丢）的记录。
//   - txn.AppliedLSN()：已成功应用到存储的最大提交 LSN。截断只删 <= 该
//     位点的记录，避免删除已落盘但尚未应用到存储、崩溃恢复仍需重放的记录。
//   - 两者取小得到 safe：safe 及之前的记录既已落盘又已应用，删除后崩溃
//     恢复结果与截断前严格一致。
//
// 截断前对实现 storage.Flusher 的存储（RocksDB）执行一次 Flush，把已应用
// 数据落 SST，双保险保证截断位点数据不依赖被截记录。
type checkpointController struct {
	db *DB

	mu       sync.Mutex
	enabled  bool
	interval time.Duration
	maxMB    int64 // >0：WAL 超过该大小（MB）才截断；<=0：每次检查都截断

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
	wakeCh    chan struct{} // 配置热更新唤醒（非阻塞缓冲 1，丢信号无害：下次迭代重读）
	stats     CheckpointStats
}

func newCheckpointController(d *DB) *checkpointController {
	return &checkpointController{
		db:       d,
		enabled:  true,
		interval: defaultCheckpointInterval,
		maxMB:    defaultCheckpointMaxMB,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
		wakeCh:   make(chan struct{}, 1),
	}
}

// Start 启动后台检查协程（幂等；DB.Open 装配后调用）。
func (cc *checkpointController) Start() {
	cc.startOnce.Do(func() {
		go cc.loop()
	})
}

// Stop 停止后台检查协程并等待退出（幂等；DB.Close 最先调用，保证后续
// 关闭 WAL/Txn/Storage 时无并发访问）。
func (cc *checkpointController) Stop() {
	cc.stopOnce.Do(func() {
		close(cc.stopCh)
		<-cc.doneCh
	})
}

// loop 周期检查协程：每 interval 唤醒一次；interval 热更新经 wakeCh 立即
// 打断当前等待并重读，保证 SET wal_checkpoint_interval_ms 即时生效。
func (cc *checkpointController) loop() {
	defer close(cc.doneCh)
	for {
		cc.mu.Lock()
		interval := cc.interval
		enabled := cc.enabled
		cc.mu.Unlock()
		if interval <= 0 {
			interval = defaultCheckpointInterval
		}
		timer := time.NewTimer(interval)
		select {
		case <-cc.stopCh:
			timer.Stop()
			return
		case <-cc.wakeCh:
			timer.Stop()
			continue
		case <-timer.C:
			if enabled {
				cc.checkpointOnce()
			}
		}
	}
}

// wake 通知 loop 配置已变更（丢信号无害：下次迭代仍会重读配置）。
func (cc *checkpointController) wake() {
	select {
	case cc.wakeCh <- struct{}{}:
	default:
	}
}

// checkpointOnce 执行一次 checkpoint 判定与截断。返回是否发生了截断（测试辅助）。
func (cc *checkpointController) checkpointOnce() bool {
	cc.mu.Lock()
	cc.stats.Runs++
	cc.stats.LastRun = time.Now().Format("2006-01-02 15:04:05")
	cc.mu.Unlock()

	d := cc.db
	// 1) WAL 已 fsync 位点：只可截断 <= lastSynced 的记录。
	synced := d.WAL.LastSyncedLSN()
	if synced == 0 {
		return false
	}
	// 2) 存储层已应用位点：只可截断 <= appliedLSN 的记录。
	var applied storage.LSN
	if p, ok := d.Txn.(txn.AppliedLSNProvider); ok {
		applied = p.AppliedLSN()
	}
	if applied == 0 {
		return false
	}
	// 安全位点：双重约束取小。
	safe := synced
	if applied < safe {
		safe = applied
	}
	if safe == 0 {
		return false
	}
	// 3) 大小阈值：默认仅当 WAL 超过 maxMB 才截断（防止低负载下频繁重建
	// 文件的元数据开销）；maxMB<=0 表示每次检查都截断（周期强制收敛）。
	cc.mu.Lock()
	maxMB := cc.maxMB
	cc.mu.Unlock()
	if maxMB > 0 {
		sz, err := walFileSize(d.Dir)
		if err == nil && sz < maxMB*1024*1024 {
			return false
		}
	}
	// 4) 可选存储层 flush：保证位点数据已落 SST（RocksDB memtable flush）。
	if f, ok := d.Storage.(storage.Flusher); ok {
		if err := f.Flush(); err != nil {
			cc.recordError(err)
			return false
		}
	}
	// 5) 截断：保留 > safe（safe 及之前记录已安全，可全部删除）。
	truncAt := safe + 1
	if err := d.WAL.Truncate(truncAt); err != nil {
		cc.recordError(err)
		return false
	}
	cc.mu.Lock()
	cc.stats.Truncations++
	cc.stats.LastTruncated = truncAt
	cc.stats.LastError = ""
	cc.mu.Unlock()
	return true
}

// SetCheckpointEnabled 热更新总开关（SET wal_auto_checkpoint 透传）。
func (cc *checkpointController) SetCheckpointEnabled(on bool) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.enabled = on
	cc.wake()
}

// SetCheckpointInterval 热更新检查周期（毫秒；ms<=0 保持现值）。
// SET wal_checkpoint_interval_ms 透传；经 wakeCh 立即打断当前等待生效。
func (cc *checkpointController) SetCheckpointInterval(ms int64) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if ms > 0 {
		cc.interval = time.Duration(ms) * time.Millisecond
		cc.wake()
	}
}

// SetCheckpointMaxMB 热更新大小阈值（MB；mb<0 保持现值；0=仅按周期截断）。
// SET wal_checkpoint_max_mb 透传。
func (cc *checkpointController) SetCheckpointMaxMB(mb int64) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.maxMB = mb
	cc.wake()
}

// CheckpointConfig 返回当前配置文本（SHOW CONFIG / 状态展示用）。
func (cc *checkpointController) CheckpointConfig() (enabled, interval, maxMB string) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.enabled {
		enabled = "on"
	} else {
		enabled = "off"
	}
	interval = fmt.Sprintf("%d", cc.interval.Milliseconds())
	maxMB = fmt.Sprintf("%d", cc.maxMB)
	return
}

// Stats 返回运行统计快照（观测/测试辅助）。
func (cc *checkpointController) Stats() CheckpointStats {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.stats
}

func (cc *checkpointController) recordError(err error) {
	cc.mu.Lock()
	cc.stats.LastError = err.Error()
	cc.mu.Unlock()
}

// walFileSize 返回数据目录 wal.log 当前字节数。
func walFileSize(dir string) (int64, error) {
	fi, err := os.Stat(filepath.Join(dir, WALFile))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

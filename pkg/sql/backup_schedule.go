package sql

// BR 方向②扩展：备份调度 + 远程备份。
//
// 备份调度（周期自动 BACKUP + 保留份数 + 状态接口）：
//   - 配置经现有热更新体系（config.go）暴露四个键：
//       backup_interval   调度周期秒；正整数开启周期备份，0 关闭（默认 0）
//       backup_keep       保留份数；N>0 时仅保留最近 N 份备份，0 不清理（默认 0）
//       backup_dir        备份输出目录（cmd/main 默认 <data>/backups）
//       backup_remote_url 远程 HTTP 目标（可选，备份完成后 PUT 上传）
//   - 启动装配：cmd/openxdb start 解析 --backup-interval/--backup-keep/
//     --backup-dir/--backup-remote-url 后经 Engine.SetBackupScheduleConfig
//     写入调度器，再 StartBackupScheduler 启动周期循环；运行期可用
//     SET [PERSIST] backup_* 热更新（SHOW CONFIG / SHOW PERSISTED CONFIG 可见）。
//   - 状态接口：SHOW BACKUP STATUS 输出调度配置与最近执行结果；
//     Engine.BackupNow 手动立即执行一次（测试/运维）。
//
// 远程备份（HTTP PUT 上传，至少一种远程目标 + 恢复路径）：
//   - 调度备份成功后若配置 backup_remote_url，将备份文件 PUT 到该 URL
//     （远端应为 WebDAV 类或自定义 HTTP 端点，按 URL 语义接收 PUT/GET）。
//   - 恢复路径：openxdb backup pull --url <url> --out <local.bak>
//     下载到本地后执行 RESTORE FROM '<local.bak>' [TO LSN <n>] 恢复。
//   - 手动推送：openxdb backup push --file <local.bak> --url <url>。
//
// 保留份数轮转说明：backup_keep>0 时调度器会删除 backup_dir 下最旧的
// openxdb_backup_*.bak 文件，仅保留最近 N 份；默认 0 不删除任何文件。

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// 调度备份文件命名：openxdb_backup_<20060102_150405>.bak（文件名时间戳
// 前缀可字典序排序，供保留份数轮转按新旧裁剪）。
const (
	backupSchedFilePrefix = "openxdb_backup_"
	backupSchedFileSuffix = ".bak"
	// backupSchedTick 调度循环检查节拍（间隔可热更新，固定细粒度轮询）。
	backupSchedTick = 500 * time.Millisecond
)

// BackupScheduleConfig 备份调度配置快照（装配 / 热更新 / 状态展示共用）。
type BackupScheduleConfig struct {
	IntervalSec int64  // 调度周期秒；0=关闭
	Keep        int64  // 保留份数；0=不清理旧备份
	Dir         string // 备份输出目录（空=未配置，到点执行报错并记录）
	RemoteURL   string // 远程 HTTP PUT 目标 URL；空=不上传
}

// BackupScheduleStatus 备份调度状态快照（SHOW BACKUP STATUS 输出来源）。
type BackupScheduleStatus struct {
	Config     BackupScheduleConfig
	Enabled    bool   // IntervalSec>0（周期备份已开启）
	Running    bool   // 正在执行备份（单飞防重入）
	NextRunAt  string // 下次计划执行时间（未开启为空串）
	LastRunAt  string // 最近一次执行时间
	LastFile   string // 最近一次备份文件路径
	LastRemote string // 最近一次远程上传结果：ok / failed: <err> / 空=未配置
	RunCount   int64  // 已成功生成备份文件次数
	LastError  string // 最近一次执行错误（备份失败或保留轮转失败）
}

// BackupScheduler 周期备份调度器：单 goroutine 循环，配置变更经 Update
// 唤醒重算下次执行时间；到点单飞执行 BACKUP → 远程上传 → 保留轮转。
type BackupScheduler struct {
	e *Executor

	mu         sync.Mutex
	cfg        BackupScheduleConfig
	running    bool
	lastRunAt  time.Time
	nextRunAt  time.Time
	lastFile   string
	lastRemote string
	runCount   int64
	lastErr    string

	started bool
	stopCh  chan struct{}
	wakeCh  chan struct{}
	doneCh  chan struct{}
}

// newBackupScheduler 创建调度器（NewExecutor 装配；e 用于执行 BACKUP）。
func newBackupScheduler(e *Executor) *BackupScheduler {
	return &BackupScheduler{e: e}
}

// Update 全量更新调度配置并重算下次执行时间（配置热更新入口）。
func (bs *BackupScheduler) Update(cfg BackupScheduleConfig) {
	bs.mu.Lock()
	bs.cfg = cfg
	bs.recomputeLocked(time.Now())
	bs.mu.Unlock()
	bs.wake()
}

// Config 返回当前配置快照。
func (bs *BackupScheduler) Config() BackupScheduleConfig {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return bs.cfg
}

// Status 返回当前状态快照。
func (bs *BackupScheduler) Status() BackupScheduleStatus {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	st := BackupScheduleStatus{
		Config:     bs.cfg,
		Enabled:    bs.cfg.IntervalSec > 0,
		Running:    bs.running,
		LastFile:   bs.lastFile,
		LastRemote: bs.lastRemote,
		RunCount:   bs.runCount,
		LastError:  bs.lastErr,
	}
	if !bs.nextRunAt.IsZero() {
		st.NextRunAt = bs.nextRunAt.Format("2006-01-02 15:04:05")
	}
	if !bs.lastRunAt.IsZero() {
		st.LastRunAt = bs.lastRunAt.Format("2006-01-02 15:04:05")
	}
	return st
}

// Start 启动调度循环（幂等；Stop 后可再次 Start）。
func (bs *BackupScheduler) Start() {
	bs.mu.Lock()
	if bs.started {
		bs.mu.Unlock()
		return
	}
	bs.started = true
	bs.stopCh = make(chan struct{})
	bs.wakeCh = make(chan struct{}, 1)
	bs.doneCh = make(chan struct{})
	bs.mu.Unlock()
	go bs.loop()
}

// Stop 停止调度循环并等待退出（幂等）。
func (bs *BackupScheduler) Stop() {
	bs.mu.Lock()
	if !bs.started {
		bs.mu.Unlock()
		return
	}
	bs.started = false
	ch := bs.stopCh
	done := bs.doneCh
	bs.mu.Unlock()
	close(ch)
	<-done
}

// wake 唤醒调度循环立即重算（channel 容量 1，合并突发多次更新）。
func (bs *BackupScheduler) wake() {
	bs.mu.Lock()
	ch := bs.wakeCh
	bs.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// loop 调度主循环：固定节拍轮询 + 配置更新唤醒。
func (bs *BackupScheduler) loop() {
	tick := time.NewTicker(backupSchedTick)
	defer tick.Stop()
	defer close(bs.doneCh)
	for {
		select {
		case <-bs.stopCh:
			return
		case <-bs.wakeCh:
			bs.maybeRun()
		case <-tick.C:
			bs.maybeRun()
		}
	}
}

// maybeRun 检查是否到点：未开启 / 已到点未到 / 正在执行 均跳过。
func (bs *BackupScheduler) maybeRun() {
	bs.mu.Lock()
	if !bs.started || bs.cfg.IntervalSec <= 0 || bs.running {
		bs.mu.Unlock()
		return
	}
	if time.Now().Before(bs.nextRunAt) {
		bs.mu.Unlock()
		return
	}
	bs.running = true
	bs.mu.Unlock()

	bs.runBackup()

	bs.mu.Lock()
	bs.running = false
	bs.mu.Unlock()
}

// runOnce 手动立即执行一次备份（Engine.BackupNow；正在执行时返回错误）。
// 同步返回备份文件路径与错误（错误同时记录到状态 last_error）。
func (bs *BackupScheduler) runOnce() (string, error) {
	bs.mu.Lock()
	if bs.running {
		bs.mu.Unlock()
		return "", errors.New("backup already running")
	}
	bs.running = true
	bs.mu.Unlock()
	defer func() {
		bs.mu.Lock()
		bs.running = false
		bs.mu.Unlock()
	}()
	return bs.runBackup()
}

// runBackup 执行一次调度备份：BACKUP TO → 远程上传 → 保留轮转 → 记录状态。
func (bs *BackupScheduler) runBackup() (string, error) {
	cfg := bs.Config()
	if cfg.Dir == "" {
		err := errors.New("backup dir not configured")
		bs.recordRun("", err, time.Now())
		return "", err
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		bs.recordRun("", fmt.Errorf("create backup dir: %w", err), time.Now())
		return "", err
	}
	path := filepath.Join(cfg.Dir, fmt.Sprintf("%s%s%s",
		backupSchedFilePrefix, time.Now().Format("20060102_150405"), backupSchedFileSuffix))
	// 直接调用 execBackup（同 BACKUP TO '<path>' 语句，复用一致快照导出逻辑）。
	if _, err := bs.e.execBackup(bs.e.globalSess, &BackupStmt{Path: path}); err != nil {
		bs.recordRun("", err, time.Now())
		return "", err
	}
	// 远程上传：失败仅记录 last_remote，不视为备份失败（本地文件仍有效）。
	if cfg.RemoteURL != "" {
		if uerr := UploadBackupFile(path, cfg.RemoteURL); uerr != nil {
			bs.mu.Lock()
			bs.lastRemote = "failed: " + uerr.Error()
			bs.mu.Unlock()
		} else {
			bs.mu.Lock()
			bs.lastRemote = "ok"
			bs.mu.Unlock()
		}
	}
	// 保留份数轮转：keep>0 时删除最旧多余备份（仅在 backup_dir 内按前缀匹配）。
	var retainErr error
	if cfg.Keep > 0 {
		if _, rerr := retainBackups(cfg.Dir, cfg.Keep); rerr != nil {
			retainErr = fmt.Errorf("retain: %w", rerr)
		}
	}
	bs.recordRun(path, retainErr, time.Now())
	return path, retainErr
}

// recordRun 更新最近执行状态并重算下次执行时间（at 为本次执行完成时刻）。
func (bs *BackupScheduler) recordRun(path string, err error, at time.Time) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	bs.lastRunAt = at
	bs.lastFile = path
	bs.runCount++
	if err != nil {
		bs.lastErr = err.Error()
	} else {
		bs.lastErr = ""
	}
	bs.recomputeLocked(at)
}

// recomputeLocked 依据当前配置与最近执行时间重算下次执行时间。
// 未开启（interval<=0）清空计划；lastRunAt 为零（从未执行）则从 now 起算；
// 已过期的计划收敛到 now（下个节拍立即触发）。
func (bs *BackupScheduler) recomputeLocked(now time.Time) {
	if bs.cfg.IntervalSec <= 0 {
		bs.nextRunAt = time.Time{}
		return
	}
	if bs.lastRunAt.IsZero() {
		bs.nextRunAt = now.Add(time.Duration(bs.cfg.IntervalSec) * time.Second)
		return
	}
	bs.nextRunAt = bs.lastRunAt.Add(time.Duration(bs.cfg.IntervalSec) * time.Second)
	if bs.nextRunAt.Before(now) {
		bs.nextRunAt = now
	}
}

// retainBackups 保留 backup_dir 下最近 keep 份调度备份，删除更旧的
// openxdb_backup_*.bak 文件（文件名时间戳前缀字典序 = 新旧序）。
func retainBackups(dir string, keep int64) ([]string, error) {
	if keep <= 0 {
		return nil, nil
	}
	matches, err := filepath.Glob(filepath.Join(dir, backupSchedFilePrefix+"*"+backupSchedFileSuffix))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	if int64(len(matches)) <= keep {
		return nil, nil
	}
	removed := matches[:int64(len(matches))-keep]
	for _, p := range removed {
		if err := os.Remove(p); err != nil {
			return nil, fmt.Errorf("remove %s: %w", p, err)
		}
	}
	return removed, nil
}

// UploadBackupFile 将备份文件通过 HTTP PUT 上传到远程目标（远程备份）。
// 远端应为 WebDAV 类或自定义 HTTP 端点：PUT <url> 接收文件字节，2xx 视为成功。
func UploadBackupFile(path, url string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	req, err := http.NewRequest(http.MethodPut, url, f)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("remote backup upload failed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

// DownloadBackupFile 从远程目标下载备份文件到本地（远程恢复路径）。
// 远端应支持 GET <url> 返回文件字节；写入本地前先落 .tmp 再改名，避免半写。
func DownloadBackupFile(url, out string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("remote backup download failed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	tmp := out + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if _, err := os.Stat(out); err == nil {
		if err := os.Remove(out); err != nil {
			return err
		}
	}
	return os.Rename(tmp, out)
}

// ---- Executor 挂载 ----

// SetBackupScheduleConfig 装配备份调度配置（cmd/main 启动装配；运行期
// SET PERSIST backup_* 经 config.go 热更新到同一调度器）。
func (e *Executor) SetBackupScheduleConfig(cfg BackupScheduleConfig) {
	e.backupSched.Update(cfg)
}

// StartBackupScheduler 启动备份调度循环（cmd/main start 装配后调用）。
func (e *Executor) StartBackupScheduler() { e.backupSched.Start() }

// StopBackupScheduler 停止备份调度循环（db 关闭流程装配，避免关闭竞态）。
func (e *Executor) StopBackupScheduler() { e.backupSched.Stop() }

// BackupScheduleStatus 返回备份调度状态快照（SHOW BACKUP STATUS 数据源）。
func (e *Executor) BackupScheduleStatus() BackupScheduleStatus { return e.backupSched.Status() }

// BackupNow 手动立即执行一次备份，返回备份文件路径（运维/测试接口）。
func (e *Executor) BackupNow() (string, error) { return e.backupSched.runOnce() }

// execShowBackupStatus SHOW BACKUP STATUS：输出调度配置与最近执行状态
// （与 SHOW CONFIG 同风格：variable/value 两列）。
func (e *Executor) execShowBackupStatus(sess *Session, s *ShowBackupStatusStmt) (*Result, error) {
	st := e.backupSched.Status()
	c := st.Config
	return &Result{
		Columns: []string{"variable", "value"},
		Rows: [][]Value{
			{StrVal("enabled"), StrVal(boolValStr(st.Enabled))},
			{StrVal("interval_sec"), StrVal(fmt.Sprintf("%d", c.IntervalSec))},
			{StrVal("keep"), StrVal(fmt.Sprintf("%d", c.Keep))},
			{StrVal("dir"), StrVal(c.Dir)},
			{StrVal("remote_url"), StrVal(c.RemoteURL)},
			{StrVal("running"), StrVal(boolValStr(st.Running))},
			{StrVal("next_run"), StrVal(st.NextRunAt)},
			{StrVal("last_run"), StrVal(st.LastRunAt)},
			{StrVal("last_file"), StrVal(st.LastFile)},
			{StrVal("last_remote"), StrVal(st.LastRemote)},
			{StrVal("run_count"), StrVal(fmt.Sprintf("%d", st.RunCount))},
			{StrVal("last_error"), StrVal(st.LastError)},
		},
	}, nil
}

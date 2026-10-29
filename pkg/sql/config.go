package sql

// M16-1 在线配置热更新：配置注册表与持久化（pkg/sql 新文件）。
//
// 支持键均为真实存在的运行时配置（源码 grep 确认，禁止虚构）：
//   - query_cache           查询结果缓存开关（M9，query_cache.go）
//   - ro_parallel           只读 SELECT 并行开关（v1.14.0 P0，executor.go roParallel）
//   - snapshot_share        autocommit 只读语句共享快照开关（v1.16.0 C6，executor.go）
//   - pipeline_flush        连接内 pipeline 条件 flush 开关（v1.16.0 C6，server.go REPLSession）
//   - snapshot_lock_external 显式事务 UPDATE 快照刷新锁外化开关（v1.17.0 D1.1，executor.go）
//   - write_pipeline        autocommit 写 pipeline 开关（v1.17.0 D1.2，server.go REPLSession）
//   - group_commit_window_ms WAL 组提交定时刷盘窗口毫秒（v1.14.0 P1，wal_impl.go）
//   - slow_query_threshold  慢查询阈值毫秒（M13，executor.go slowThreshold）
//   - slow_query_log        慢查询落盘开关（M13，slowlog.go）
//   - shutdown_timeout      优雅停机超时秒（M16-2，executor.go shutdownTimeoutSec）
//
// 持久化文件：数据目录下 persisted.json（db.Open 装配路径后由 Engine
// LoadPersistedConfig 加载合并）。优先级：内存（SET / SET PERSIST 立即生效）
// > 持久化（重启合并）> 默认值。非法键 / 非法值 / 类型错误均返回明确报错。

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/logx"
)

// configSpec 一个可热更新的配置项：校验与应用（apply）以及当前值读取（read）。
// apply 完全复用 M9/M13 原有 SET 语义与报错文案，保证现有 SET 行为零回归。
// persistOnly 标记启动级配置（T36 tls_*）：仅允许 SET PERSIST 落盘，
// 普通 SET 直接报错；实际生效在启动装配（restart-effective）。
type configSpec struct {
	key         string
	desc        string
	def         string // 默认值（SHOW CONFIG 来源 default 时展示）
	persistOnly bool   // T36：true = 仅 SET PERSIST（启动级配置，重启生效）
	apply       func(e *Executor, v string) error
	read        func(e *Executor) string
}

// configSpecs 支持的配置键注册表（顺序即 SHOW CONFIG 输出顺序）。
var configSpecs = []*configSpec{
	{
		key:  "query_cache",
		desc: "查询结果缓存开关（M9）：on/off/1/0，on 开启缓存",
		def:  "off",
		apply: func(e *Executor, v string) error {
			on := false
			switch v {
			case "on", "true", "1":
				on = true
			case "off", "false", "0":
				on = false
			default:
				return &SQLError{Msg: "invalid value for query_cache: " + v + " (expected on/off/1/0)"}
			}
			e.qc.SetEnabled(on)
			return nil
		},
		read: func(e *Executor) string { return boolValStr(e.qc.Enabled()) },
	},
	{
		key:  "ro_parallel",
		desc: "只读 SELECT 并行开关（v1.14.0 P0）：on/off/1/0，批内 autocommit 只读 SELECT 并发执行+按序重组，默认 on",
		def:  "on",
		apply: func(e *Executor, v string) error {
			on := false
			switch v {
			case "on", "true", "1":
				on = true
			case "off", "false", "0":
				on = false
			default:
				return &SQLError{Msg: "invalid value for ro_parallel: " + v + " (expected on/off/1/0)"}
			}
			e.SetROParallel(on)
			return nil
		},
		read: func(e *Executor) string { return boolValStr(e.ROParallel()) },
	},
	{
		key:  "snapshot_share",
		desc: "autocommit 只读语句共享快照开关（v1.16.0 C6）：on/off/1/0，连续 autocommit 只读 SELECT 复用同一快照（延迟隐藏），默认 on；off 回退逐语句独立快照",
		def:  "on",
		apply: func(e *Executor, v string) error {
			on := false
			switch v {
			case "on", "true", "1":
				on = true
			case "off", "false", "0":
				on = false
			default:
				return &SQLError{Msg: "invalid value for snapshot_share: " + v + " (expected on/off/1/0)"}
			}
			e.SetSnapshotShare(on)
			return nil
		},
		read: func(e *Executor) string { return boolValStr(e.SnapshotShare()) },
	},
	{
		key:  "pipeline_flush",
		desc: "连接内 pipeline 条件 flush 开关（v1.16.0 C6）：on/off/1/0，REPL 串行路径通道仍有后续命令时合并响应 flush（延迟隐藏），默认 on；off 回退逐命令 flush",
		def:  "on",
		apply: func(e *Executor, v string) error {
			on := false
			switch v {
			case "on", "true", "1":
				on = true
			case "off", "false", "0":
				on = false
			default:
				return &SQLError{Msg: "invalid value for pipeline_flush: " + v + " (expected on/off/1/0)"}
			}
			e.SetPipelineFlush(on)
			return nil
		},
		read: func(e *Executor) string { return boolValStr(e.PipelineFlush()) },
	},
	{
		key:  "snapshot_lock_external",
		desc: "显式事务 UPDATE 快照刷新锁外化开关（v1.17.0 D1.1）：on/off/1/0，显式事务内已持锁行跳过 refreshWriteSnapshot（仅新锁定行锁内刷新），默认 on；off 回退 v1.16.0 每次写语句刷新",
		def:  "on",
		apply: func(e *Executor, v string) error {
			on := false
			switch v {
			case "on", "true", "1":
				on = true
			case "off", "false", "0":
				on = false
			default:
				return &SQLError{Msg: "invalid value for snapshot_lock_external: " + v + " (expected on/off/1/0)"}
			}
			e.SetSnapshotLockExternal(on)
			return nil
		},
		read: func(e *Executor) string { return boolValStr(e.SnapshotLockExternal()) },
	},
	{
		key:  "group_commit_window_ms",
		desc: "WAL 组提交定时刷盘窗口毫秒（v1.14.0 P1）：0.1~1ms 建议区间，0 关闭定时窗口回退被动攒批，默认 0.5",
		def:  "0.5",
		apply: func(e *Executor, v string) error {
			ms, err := strconv.ParseFloat(v, 64)
			if err != nil || ms < 0 {
				return &SQLError{Msg: "invalid value for group_commit_window_ms: " + v + " (expected non-negative number of ms, 0=disabled)"}
			}
			e.SetGroupCommitWindow(time.Duration(ms * float64(time.Millisecond)))
			return nil
		},
		read: func(e *Executor) string { return e.groupCommitWindowMsText() },
	},
	{
		key:  "write_pipeline",
		desc: "autocommit 写 pipeline 开关（v1.17.0 D1.2）：on/off/1/0，开启时服务端 REPLSession 收集连续 autocommit 写 DML 为批、批尾统一 WAL 刷盘（合并 Commit fsync，写路径吞吐提升），默认 off（零回归，与 v1.16.0 逐语句 SyncUpTo 一致）",
		def:  "off",
		apply: func(e *Executor, v string) error {
			on := false
			switch v {
			case "on", "true", "1":
				on = true
			case "off", "false", "0":
				on = false
			default:
				return &SQLError{Msg: "invalid value for write_pipeline: " + v + " (expected on/off/1/0)"}
			}
			e.SetWritePipeline(on)
			return nil
		},
		read: func(e *Executor) string { return boolValStr(e.WritePipeline()) },
	},
	{
		key:  "slow_query_threshold",
		desc: "慢查询阈值毫秒（M13）：整数，<=0 关闭记录，默认 100",
		def:  "100",
		apply: func(e *Executor, v string) error {
			ms, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return &SQLError{Msg: "invalid value for slow_query_threshold: " + v + " (expected integer ms)"}
			}
			e.SetSlowThreshold(ms)
			return nil
		},
		read: func(e *Executor) string { return strconv.FormatInt(e.SlowQueryThreshold(), 10) },
	},
	{
		key:  "slow_query_log",
		desc: "慢查询落盘开关（M13）：on/off/1/0；重启生效依赖启动装配的日志路径",
		def:  "off",
		apply: func(e *Executor, v string) error {
			on := false
			switch v {
			case "on", "true", "1":
				on = true
			case "off", "false", "0":
				on = false
			default:
				return &SQLError{Msg: "invalid value for slow_query_log: " + v + " (expected on/off/1/0)"}
			}
			// 落盘依赖日志路径：路径未装配时无法开启（保持关闭，安全默认），
			// 不报错——服务装配路径后按持久化 intent 恢复（PersistedSlowLogIntent）。
			if on && (e.slowLog == nil || e.slowLog.Path() == "") {
				return nil
			}
			if e.slowLog != nil {
				e.slowLog.Configure(e.slowLog.Path(), on)
			}
			return nil
		},
		read: func(e *Executor) string { return boolValStr(e.SlowLogEnabled()) },
	},
	{
		key:  "shutdown_timeout",
		desc: "优雅停机超时秒数（M16-2）：正整数；SHUTDOWN/SIGINT drain 超过则强制退出",
		def:  "30",
		apply: func(e *Executor, v string) error {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 1 {
				return &SQLError{Msg: "invalid value for shutdown_timeout: " + v + " (expected positive integer seconds)"}
			}
			e.SetShutdownTimeoutSec(n)
			return nil
		},
		read: func(e *Executor) string { return strconv.FormatInt(e.ShutdownTimeoutSec(), 10) },
	},
	// BR 方向②扩展：备份调度（周期自动 BACKUP + 保留份数 + 状态接口）与远程备份。
	// backup_interval 正整数开启周期备份，0 关闭；backup_keep 保留份数（0=不清理）；
	// backup_dir 备份输出目录；backup_remote_url 远程 HTTP PUT 目标（空=不上传）。
	// 热更新即时生效：调度器重算下次执行时间；SHOW BACKUP STATUS 查看状态。
	{
		key:  "backup_interval",
		desc: "备份调度周期秒（BR 方向②）：正整数开启周期 BACKUP，0 关闭；配合 backup_dir/backup_keep/backup_remote_url",
		def:  "0",
		apply: func(e *Executor, v string) error {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return &SQLError{Msg: "invalid value for backup_interval: " + v + " (expected non-negative integer seconds)"}
			}
			cfg := e.backupSched.Config()
			cfg.IntervalSec = n
			e.backupSched.Update(cfg)
			return nil
		},
		read: func(e *Executor) string { return strconv.FormatInt(e.backupSched.Config().IntervalSec, 10) },
	},
	{
		key:  "backup_keep",
		desc: "备份保留份数（BR 方向②）：N>0 仅保留最近 N 份调度备份，0 不清理",
		def:  "0",
		apply: func(e *Executor, v string) error {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return &SQLError{Msg: "invalid value for backup_keep: " + v + " (expected non-negative integer)"}
			}
			cfg := e.backupSched.Config()
			cfg.Keep = n
			e.backupSched.Update(cfg)
			return nil
		},
		read: func(e *Executor) string { return strconv.FormatInt(e.backupSched.Config().Keep, 10) },
	},
	{
		key:  "backup_dir",
		desc: "备份输出目录（BR 方向②）：周期备份写入目录；空=未配置",
		def:  "",
		apply: func(e *Executor, v string) error {
			cfg := e.backupSched.Config()
			cfg.Dir = v
			e.backupSched.Update(cfg)
			return nil
		},
		read: func(e *Executor) string { return e.backupSched.Config().Dir },
	},
	{
		key:  "backup_remote_url",
		desc: "远程备份 HTTP PUT 目标（BR 方向②）：备份完成后上传；空=不上传",
		def:  "",
		apply: func(e *Executor, v string) error {
			if v != "" && !(strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://")) {
				return &SQLError{Msg: "invalid value for backup_remote_url: " + v + " (expected http(s) URL or empty)"}
			}
			cfg := e.backupSched.Config()
			cfg.RemoteURL = v
			e.backupSched.Update(cfg)
			return nil
		},
		read: func(e *Executor) string { return e.backupSched.Config().RemoteURL },
	},
	// 结构化日志与告警体系：日志级别动态调整 + 关键模块告警（文件/webhook 出口）。
	// log_level 控制 JSON 行日志输出级别（debug/info/warn/error），即时生效；
	// alert_enabled 总开关（off 时告警仅入内存缓冲 + Error 日志），alert_file 为
	// JSON 行告警文件路径，alert_webhook 为 HTTP POST 目标，alert_keep 为内存
	// 保留最近告警条数。SHOW ALERTS / SHOW ALERT STATUS 查看。
	{
		key:  "log_level",
		desc: "结构化日志级别（日志告警体系）：debug/info/warn/error，即时生效",
		def:  "info",
		apply: func(e *Executor, v string) error {
			lv, err := logx.ParseLevel(v)
			if err != nil {
				return &SQLError{Msg: "invalid value for log_level: " + v + " (expected debug/info/warn/error)"}
			}
			logx.Default.SetLevel(lv)
			return nil
		},
		read: func(e *Executor) string { return logx.Default.Level().String() },
	},
	{
		key:  "alert_enabled",
		desc: "告警总开关（日志告警体系）：on/off/1/0；off 时告警仅入内存缓冲与 Error 日志",
		def:  "off",
		apply: func(e *Executor, v string) error {
			on := false
			switch v {
			case "on", "true", "1":
				on = true
			case "off", "false", "0":
				on = false
			default:
				return &SQLError{Msg: "invalid value for alert_enabled: " + v + " (expected on/off/1/0)"}
			}
			cfg := logx.DefaultAlerts.Config()
			cfg.Enabled = on
			logx.DefaultAlerts.Configure(cfg)
			return nil
		},
		read: func(e *Executor) string { return boolValStr(logx.DefaultAlerts.Config().Enabled) },
	},
	{
		key:  "alert_file",
		desc: "告警 JSON 行文件路径（日志告警体系）：告警追加写入；空=不落盘",
		def:  "",
		apply: func(e *Executor, v string) error {
			cfg := logx.DefaultAlerts.Config()
			cfg.File = v
			logx.DefaultAlerts.Configure(cfg)
			return nil
		},
		read: func(e *Executor) string { return logx.DefaultAlerts.Config().File },
	},
	{
		key:  "alert_webhook",
		desc: "告警 HTTP webhook 目标（日志告警体系）：POST JSON；空=不推送",
		def:  "",
		apply: func(e *Executor, v string) error {
			if v != "" && !(strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://")) {
				return &SQLError{Msg: "invalid value for alert_webhook: " + v + " (expected http(s) URL or empty)"}
			}
			cfg := logx.DefaultAlerts.Config()
			cfg.Webhook = v
			logx.DefaultAlerts.Configure(cfg)
			return nil
		},
		read: func(e *Executor) string { return logx.DefaultAlerts.Config().Webhook },
	},
	{
		key:  "alert_keep",
		desc: "告警内存保留条数（日志告警体系）：最近 N 条；0 使用默认 100",
		def:  "100",
		apply: func(e *Executor, v string) error {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return &SQLError{Msg: "invalid value for alert_keep: " + v + " (expected non-negative integer)"}
			}
			cfg := logx.DefaultAlerts.Config()
			cfg.Keep = int(n)
			logx.DefaultAlerts.Configure(cfg)
			return nil
		},
		read: func(e *Executor) string { return strconv.Itoa(logx.DefaultAlerts.Config().Keep) },
	},
	// T37 商用档：监控告警规则（指标阈值 + 持续时长 + 严重级别）配置。
	// alert_rules_enabled 为规则评估总开关；alert_eval_interval 为评估周期秒；
	// alert_rules 为规则 JSON 数组（SET/PERSIST 均可，非法规则整体报错）。
	// 评估引擎数据源与 Prometheus 指标同源（metrics.Registry.Snapshot）。
	{
		key:  "alert_rules_enabled",
		desc: "告警规则总开关（T37 监控告警）：on/off/1/0；off 时规则不评估不通知",
		def:  "off",
		apply: func(e *Executor, v string) error {
			on := false
			switch v {
			case "on", "true", "1":
				on = true
			case "off", "false", "0":
				on = false
			default:
				return &SQLError{Msg: "invalid value for alert_rules_enabled: " + v + " (expected on/off/1/0)"}
			}
			e.SetAlertRulesEnabled(on)
			return nil
		},
		read: func(e *Executor) string { return boolValStr(e.AlertRulesEnabled()) },
	},
	{
		key:  "alert_eval_interval",
		desc: "告警规则评估周期秒（T37 监控告警）：1-3600，即时生效",
		def:  "10",
		apply: func(e *Executor, v string) error {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 1 || n > 3600 {
				return &SQLError{Msg: "invalid value for alert_eval_interval: " + v + " (expected 1-3600)"}
			}
			e.SetAlertEvalInterval(n)
			return nil
		},
		read: func(e *Executor) string { return strconv.FormatInt(e.AlertEvalInterval(), 10) },
	},
	{
		key:  "alert_rules",
		desc: "告警规则 JSON 数组（T37 监控告警）：[{id,metric,op,threshold,duration_sec,severity,enabled}]；空=清空规则",
		def:  "",
		apply: func(e *Executor, v string) error {
			if err := e.SetAlertRulesJSON(v); err != nil {
				return &SQLError{Msg: "invalid value for alert_rules: " + err.Error()}
			}
			return nil
		},
		read: func(e *Executor) string { return e.AlertRulesJSON() },
	},
	// T36 商用档：连接层 TLS 启动级配置（persistOnly）。
	// 仅允许 SET PERSIST 落盘（监听层在启动装配，无法热更新）；普通 SET 由
	// executor.execSet 明确报错。启动装配优先级：命令行 > 持久化 intent > 默认（无 TLS）。
	// tls_allow_legacy 安全默认 off（纯 TLS）；显式 on 时同端口明文/TLS 并存。
	{
		key:         "tls_cert",
		desc:        "TLS 服务端证书路径（T36，启动级）：SET PERSIST 落盘重启生效，配合 tls_key",
		def:         "",
		persistOnly: true,
		apply:       func(e *Executor, v string) error { return nil },
		read: func(e *Executor) string { return e.configPersistedValue("tls_cert", "") },
	},
	{
		key:         "tls_key",
		desc:        "TLS 服务端私钥路径（T36，启动级）：SET PERSIST 落盘重启生效，配合 tls_cert",
		def:         "",
		persistOnly: true,
		apply:       func(e *Executor, v string) error { return nil },
		read: func(e *Executor) string { return e.configPersistedValue("tls_key", "") },
	},
	{
		key:         "tls_allow_legacy",
		desc:        "TLS 同端口明文降级开关（T36，启动级）：on/off/1/0；on=允许明文会话并存，off=纯 TLS（安全默认）",
		def:         "off",
		persistOnly: true,
		apply: func(e *Executor, v string) error {
			switch v {
			case "on", "true", "1", "off", "false", "0":
				return nil
			default:
				return &SQLError{Msg: "invalid value for tls_allow_legacy: " + v + " (expected on/off/1/0)"}
			}
		},
		read: func(e *Executor) string {
			return e.configPersistedValue("tls_allow_legacy", "off")
		},
	},
}

// configPersistedValue 返回配置键当前生效值：内存 SET/SET PERSIST（sessionCfg）优先，
// 其次持久化（persistedCfg），最后给定默认值。仅用于 persistOnly 键的 SHOW CONFIG 展示。
// 注意：不得引用 configSpecs/lookupConfigSpec（否则构成包级初始化循环）。
func (e *Executor) configPersistedValue(key, def string) string {
	e.cfgMu.Lock()
	defer e.cfgMu.Unlock()
	if v, ok := e.sessionCfg[key]; ok {
		return v
	}
	if v, ok := e.persistedCfg[key]; ok {
		return v
	}
	return def
}

// lookupConfigSpec 按键查找配置项；未知键返回 nil（对应 unknown session variable）。
func lookupConfigSpec(key string) *configSpec {
	for _, s := range configSpecs {
		if s.key == key {
			return s
		}
	}
	return nil
}

// boolValStr 布尔值配置文本（SHOW CONFIG 展示用，与 boolVal 同口径）。
func boolValStr(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// persistConfigFile 持久化配置文件内容：配置键 → 原始值文本。
type persistConfigFile map[string]string

// loadPersistedConfigFile 读取持久化配置；文件不存在返回空表（首次启动）。
func loadPersistedConfigFile(path string) (persistConfigFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return persistConfigFile{}, nil
		}
		return nil, err
	}
	m := persistConfigFile{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("persisted config corrupt: %w", err)
	}
	return m, nil
}

// savePersistedConfigFile 写持久化配置（临时文件 + 改名，避免半写损坏）。
func savePersistedConfigFile(path string, m persistConfigFile) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// SetPersistedConfigPath 装配持久化配置文件路径（db.Open 注入 data/persisted.json；
// 空串 = 未启用持久化，SET PERSIST 报错）。
func (e *Executor) SetPersistedConfigPath(path string) {
	e.cfgMu.Lock()
	defer e.cfgMu.Unlock()
	e.cfgPath = path
}

// PersistedConfigPath 返回当前持久化配置文件路径（未装配为空串）。
func (e *Executor) PersistedConfigPath() string {
	e.cfgMu.Lock()
	defer e.cfgMu.Unlock()
	return e.cfgPath
}

// LoadPersistedConfig 加载持久化配置并合并到内存（db.Open 启动时调用）：
// 逐项校验并应用，非法键 / 非法值 / 损坏文件返回明确错误，绝不静默忽略。
// 优先级：持久化覆盖默认值；之后的内存 SET / SET PERSIST 再覆盖持久化值。
func (e *Executor) LoadPersistedConfig() error {
	e.cfgMu.Lock()
	path := e.cfgPath
	e.cfgMu.Unlock()
	if path == "" {
		return nil
	}
	m, err := loadPersistedConfigFile(path)
	if err != nil {
		return err
	}
	// 应用前先全量校验，任一非法即整体失败（防部分生效）。
	for k, v := range m {
		spec := lookupConfigSpec(k)
		if spec == nil {
			return fmt.Errorf("persisted config contains unknown key %q", k)
		}
		if err := spec.apply(e, v); err != nil {
			return fmt.Errorf("persisted config %s=%q invalid: %w", k, v, err)
		}
	}
	e.cfgMu.Lock()
	e.persistedCfg = m
	e.cfgMu.Unlock()
	return nil
}

// PersistedSlowLogIntent 返回持久化配置中 slow_query_log 的启动意图：
// 1 = 持久化为 on；0 = 持久化为 off；-1 = 无持久化记录（供服务装配日志路径时尊重）。
func (e *Executor) PersistedSlowLogIntent() int {
	e.cfgMu.Lock()
	defer e.cfgMu.Unlock()
	v, ok := e.persistedCfg["slow_query_log"]
	if !ok {
		return -1
	}
	switch v {
	case "on", "true", "1":
		return 1
	default:
		return 0
	}
}

// PersistedTLSConfig 返回持久化 TLS 启动意图（T36）：证书/私钥路径与
// 明文降级开关。cmd/openxdb 在 db.Open（已加载 persisted.json）后调用，
// 与命令行参数合并装配监听层（命令行优先）。allowLegacy 默认 off（纯 TLS，
// 安全默认）；持久化未配置 tls_allow_legacy 时返回 false。
func (e *Executor) PersistedTLSConfig() (cert, key string, allowLegacy bool) {
	e.cfgMu.Lock()
	defer e.cfgMu.Unlock()
	cert = e.persistedCfg["tls_cert"]
	key = e.persistedCfg["tls_key"]
	if v, ok := e.persistedCfg["tls_allow_legacy"]; ok {
		switch v {
		case "on", "true", "1":
			allowLegacy = true
		default:
			allowLegacy = false
		}
	}
	return cert, key, allowLegacy
}

// persistConfigValue 应用后写入持久化配置：校验已在 apply 阶段完成，
// 这里仅更新内存视图并落盘。落盘失败返回错误（SET PERSIST 明确报错）。
func (e *Executor) persistConfigValue(key, value string) error {
	e.cfgMu.Lock()
	defer e.cfgMu.Unlock()
	if e.cfgPath == "" {
		return fmt.Errorf("persisted config not available (no config file path)")
	}
	e.persistedCfg[key] = value
	if err := savePersistedConfigFile(e.cfgPath, e.persistedCfg); err != nil {
		return err
	}
	return nil
}

// execShowConfig SHOW CONFIG / SHOW PERSISTED CONFIG（M16-1）。
// SHOW CONFIG：当前生效配置，含来源（default / persisted / session）；
// SHOW PERSISTED CONFIG：仅已持久化到配置文件的配置。
func (e *Executor) execShowConfig(sess *Session, s *ShowConfigStmt) (*Result, error) {
	if s.Persisted {
		e.cfgMu.Lock()
		cfg := e.persistedCfg
		e.cfgMu.Unlock()
		res := &Result{Columns: []string{"variable", "value"}}
		// 稳定输出：按注册表顺序过滤已持久化键。
		for _, spec := range configSpecs {
			if v, ok := cfg[spec.key]; ok {
				res.Rows = append(res.Rows, []Value{StrVal(spec.key), StrVal(v)})
			}
		}
		return res, nil
	}
	res := &Result{Columns: []string{"variable", "value", "source"}}
	for _, spec := range configSpecs {
		source := "default"
		e.cfgMu.Lock()
		if _, ok := e.sessionCfg[spec.key]; ok {
			source = "session"
		} else if _, ok := e.persistedCfg[spec.key]; ok {
			source = "persisted"
		}
		e.cfgMu.Unlock()
		res.Rows = append(res.Rows, []Value{StrVal(spec.key), StrVal(spec.read(e)), StrVal(source)})
	}
	return res, nil
}

// ConfigKeys 返回 configSpecs 注册表全部配置键（配置文件 [sql] 段白名单来源）。
func ConfigKeys() []string {
	keys := make([]string, 0, len(configSpecs))
	for _, spec := range configSpecs {
		keys = append(keys, spec.key)
	}
	return keys
}

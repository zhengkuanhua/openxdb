package sql

// M16-1 在线配置热更新：配置注册表与持久化（pkg/sql 新文件）。
//
// 支持键均为真实存在的运行时配置（源码 grep 确认，禁止虚构）：
//   - query_cache           查询结果缓存开关（M9，query_cache.go）
//   - slow_query_threshold  慢查询阈值毫秒（M13，executor.go slowThreshold）
//   - slow_query_log        慢查询落盘开关（M13，slowlog.go）
//
// 持久化文件：数据目录下 persisted.json（db.Open 装配路径后由 Engine
// LoadPersistedConfig 加载合并）。优先级：内存（SET / SET PERSIST 立即生效）
// > 持久化（重启合并）> 默认值。非法键 / 非法值 / 类型错误均返回明确报错。

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

// configSpec 一个可热更新的配置项：校验与应用（apply）以及当前值读取（read）。
// apply 完全复用 M9/M13 原有 SET 语义与报错文案，保证现有 SET 行为零回归。
type configSpec struct {
	key   string
	desc  string
	def   string // 默认值（SHOW CONFIG 来源 default 时展示）
	apply func(e *Executor, v string) error
	read  func(e *Executor) string
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
func (e *Executor) execShowConfig(s *ShowConfigStmt) (*Result, error) {
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

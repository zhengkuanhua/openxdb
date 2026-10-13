// Package configfile 提供 OpenXDB 统一配置文件（INI 风格）的解析与校验。
//
// 选型：沿用仓库现有 openxdb.conf（db.Init 生成）的 `key = value` + `#` 注释
// 风格，支持分段：
//
//	# 示例配置见仓库 docs/openxdb.conf.example
//	[server]
//	port = 7788            # cmd start 启动参数默认值（命令行 flag 可覆盖）
//	data_dir = ./data
//	log_level = info
//	alert_enabled = off
//
//	[sql]
//	query_cache = off      # SQL 引擎 SET PERSIST 配置项（与 SHOW CONFIG 键一致）
//	slow_query_threshold = 100
//	backup_interval = 0
//
// 未知段 / 未知键 / 非法值一律拒绝（config-check 与启动时校验），不静默忽略。
// 零第三方依赖，仅标准库。
package configfile

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Section 一个配置段（保留键出现顺序）。
type Section struct {
	Key   string
	Vals  map[string]string
	Order []string
}

// File 解析后的配置文件（段按出现顺序）。
type File struct {
	Sections []*Section
	index    map[string]*Section
}

// Section 按名称取段（大小写不敏感）；不存在返回 nil。
func (f *File) Section(name string) *Section {
	return f.index[strings.ToLower(strings.TrimSpace(name))]
}

// Get 取某段某键的值；未设置返回 ("", false)。
func (f *File) Get(section, key string) (string, bool) {
	sec := f.Section(section)
	if sec == nil {
		return "", false
	}
	v, ok := sec.Vals[strings.ToLower(strings.TrimSpace(key))]
	return v, ok
}

// Load 读取并解析配置文件。
func Load(path string) (*File, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	f, err := Parse(string(b))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return f, nil
}

// Parse 解析配置文件文本（UTF-8）。支持：
//   - 空行 / # 或 ; 注释行
//   - [section] 段头（段名大小写不敏感）
//   - key = value（key 大小写不敏感；value 允许用单/双引号包裹）
//
// 键值出现在任何段之前视为格式错误；空文件视为格式错误。
func Parse(text string) (*File, error) {
	f := &File{index: map[string]*Section{}}
	cur := ""
	ln := 0
	for _, raw := range strings.Split(text, "\n") {
		ln++
		line := strings.TrimSpace(strings.TrimSuffix(raw, "\r"))
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") {
				return nil, fmt.Errorf("line %d: malformed section header %q", ln, line)
			}
			name := strings.ToLower(strings.TrimSpace(line[1 : len(line)-1]))
			if name == "" {
				return nil, fmt.Errorf("line %d: empty section name", ln)
			}
			cur = name
			if f.Section(cur) == nil {
				sec := &Section{Key: cur, Vals: map[string]string{}}
				f.Sections = append(f.Sections, sec)
				f.index[cur] = sec
			}
			continue
		}
		if cur == "" {
			return nil, fmt.Errorf("line %d: key=value outside any section", ln)
		}
		eq := strings.Index(line, "=")
		if eq < 0 {
			return nil, fmt.Errorf("line %d: expected key=value, got %q", ln, line)
		}
		key := strings.ToLower(strings.TrimSpace(line[:eq]))
		if key == "" {
			return nil, fmt.Errorf("line %d: empty key", ln)
		}
		val := strings.TrimSpace(line[eq+1:])
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		sec := f.Section(cur)
		if _, dup := sec.Vals[key]; !dup {
			sec.Order = append(sec.Order, key)
		}
		sec.Vals[key] = val
	}
	if len(f.Sections) == 0 {
		return nil, fmt.Errorf("empty config file (expected [server]/[sql] sections)")
	}
	return f, nil
}

// Validate 校验未知段与未知键。known 为段名(小写)→键集合(小写)。
// 返回错误列表；nil 表示通过。默认拒绝未知段/键（不静默忽略）。
func Validate(f *File, known map[string]map[string]bool) []string {
	var errs []string
	for _, sec := range f.Sections {
		keys, ok := known[sec.Key]
		if !ok {
			errs = append(errs, fmt.Sprintf("unknown section [%s]", sec.Key))
			continue
		}
		for _, k := range sec.Order {
			if !keys[k] {
				errs = append(errs, fmt.Sprintf("[%s] unknown key %q", sec.Key, k))
			}
		}
	}
	return errs
}

// boolOK 判断布尔文本合法（on/off/true/false/1/0）。
func boolOK(v string) bool {
	switch v {
	case "on", "off", "true", "false", "1", "0":
		return true
	}
	return false
}

// ValidateServerValues 校验 [server] 段的值格式（端口/秒数/保留数整数、
// 布尔开关、http(s) URL、日志级别）。仅校验已出现的键，返回错误列表。
func ValidateServerValues(f *File) []string {
	var errs []string
	sec := f.Section("server")
	if sec == nil {
		return errs
	}
	intKeys := map[string]bool{
		"port": true, "replica_port": true, "metrics_port": true,
		"shutdown_timeout": true, "backup_interval": true, "backup_keep": true,
		"alert_keep": true,
	}
	portKeys := map[string]bool{"port": true}
	// 可选监听端口（replica_port/metrics_port）：0 表示关闭，允许 0..65535。
	optionalPortKeys := map[string]bool{"replica_port": true, "metrics_port": true}
	nonNegKeys := map[string]bool{"shutdown_timeout": true, "backup_interval": true, "backup_keep": true, "alert_keep": true}
	urlKeys := map[string]bool{"backup_remote_url": true, "alert_webhook": true}
	boolKeys := map[string]bool{"alert_enabled": true}
	levelKeys := map[string]bool{"log_level": true}
	for _, k := range sec.Order {
		v := sec.Vals[k]
		if intKeys[k] {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errs = append(errs, fmt.Sprintf("[server] %s=%q not an integer", k, v))
				continue
			}
			if portKeys[k] && (n < 1 || n > 65535) {
				errs = append(errs, fmt.Sprintf("[server] %s=%d out of range 1..65535", k, n))
			}
			if optionalPortKeys[k] && (n < 0 || n > 65535) {
				errs = append(errs, fmt.Sprintf("[server] %s=%d out of range 0..65535 (0=disabled)", k, n))
			}
			if nonNegKeys[k] && n < 0 {
				errs = append(errs, fmt.Sprintf("[server] %s=%d must be non-negative", k, n))
			}
		}
		if urlKeys[k] && v != "" && !(strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://")) {
			errs = append(errs, fmt.Sprintf("[server] %s=%q not an http(s) URL", k, v))
		}
		if boolKeys[k] && !boolOK(v) {
			errs = append(errs, fmt.Sprintf("[server] %s=%q not on/off/true/false/1/0", k, v))
		}
		if levelKeys[k] {
			switch v {
			case "debug", "info", "warn", "warning", "error":
			default:
				errs = append(errs, fmt.Sprintf("[server] %s=%q not debug/info/warn/error", k, v))
			}
		}
	}
	return errs
}

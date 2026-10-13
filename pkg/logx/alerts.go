package logx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// AlertConfig 告警配置（可热更新：SET alert_enabled/alert_file/alert_webhook/
// alert_keep 或 openxdb start --alert-* / 配置文件的 [server] alert_*）。
type AlertConfig struct {
	Enabled bool   // 总开关：false 时告警仅入内存缓冲 + 结构化日志，不落盘不推送
	File    string // 告警 JSON 行文件路径；空 = 不落盘
	Webhook string // HTTP(S) webhook POST 目标 URL；空 = 不推送
	Keep    int    // 内存保留最近告警条数；<=0 使用默认 100
}

// Alert 一条告警记录（时间/级别/模块/事件/消息）。
type Alert struct {
	Time   string `json:"time"`
	Level  string `json:"level"`
	Module string `json:"module"`
	Event  string `json:"event"`
	Msg    string `json:"msg"`
}

// AlertManager 告警管理器：内存环形缓冲 + 可选 JSON 行文件落盘 + HTTP webhook
// 推送。任何告警都会同步写一条 Error 级结构化日志（保证无告警配置也可观测），
// webhook 推送为异步（失败仅记录日志，不阻塞主流程）。
type AlertManager struct {
	mu      sync.RWMutex
	enabled bool
	file    string
	webhook string
	keep    int
	ring    []Alert
	fileH   *os.File
	client  *http.Client
}

// NewAlertManager 创建告警管理器（默认 keep=100、关闭）。
func NewAlertManager() *AlertManager {
	return &AlertManager{
		keep:   100,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

// Configure 应用告警配置（热更新）。告警文件打开失败时降级为仅内存/日志并
// 记录结构化日志，不阻塞配置生效。
func (m *AlertManager) Configure(cfg AlertConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cfg.Keep <= 0 {
		cfg.Keep = 100
	}
	m.enabled = cfg.Enabled
	m.webhook = cfg.Webhook
	m.keep = cfg.Keep
	if cfg.File == m.file {
		return
	}
	if m.fileH != nil {
		_ = m.fileH.Close()
		m.fileH = nil
	}
	m.file = cfg.File
	if cfg.File == "" {
		return
	}
	f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		m.file = ""
		Default.Error("alert", "file_open_failed", fmt.Sprintf("open alert file: %v", err), "file", cfg.File)
		return
	}
	m.fileH = f
}

// Config 返回当前告警配置快照。
func (m *AlertManager) Config() AlertConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return AlertConfig{Enabled: m.enabled, File: m.file, Webhook: m.webhook, Keep: m.keep}
}

// Alert 记录一条告警（level 固定 error）。module 为关键模块名
// （wal/storage/network/backup/sql 等），event 为事件名，msg 为人类可读描述。
func (m *AlertManager) Alert(module, event, msg string) {
	a := Alert{
		Time:   time.Now().Format("2006-01-02T15:04:05.000Z07:00"),
		Level:  "error",
		Module: module,
		Event:  event,
		Msg:    msg,
	}
	m.mu.Lock()
	m.ring = append(m.ring, a)
	if m.keep > 0 && len(m.ring) > m.keep {
		m.ring = m.ring[len(m.ring)-m.keep:]
	}
	enabled, fileH, webhook := m.enabled, m.fileH, m.webhook
	m.mu.Unlock()

	Default.Error(module, event, msg) // 结构化日志兜底

	if !enabled {
		return
	}
	if fileH != nil {
		b, _ := json.Marshal(a)
		_, _ = fileH.Write(append(b, '\n'))
	}
	if webhook != "" {
		go m.push(webhook, a)
	}
}

// push 异步推送 webhook（HTTP POST，JSON body）。
func (m *AlertManager) push(url string, a Alert) {
	b, _ := json.Marshal(a)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		Default.Error("alert", "webhook_failed", fmt.Sprintf("push webhook: %v", err), "url", url)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		Default.Error("alert", "webhook_failed", fmt.Sprintf("push webhook: HTTP %d", resp.StatusCode), "url", url)
	}
}

// Recent 返回最近告警（按时间先后；SHOW ALERTS 数据源）。
func (m *AlertManager) Recent() []Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Alert, len(m.ring))
	copy(out, m.ring)
	return out
}

// Close 关闭告警文件句柄（服务停机装配）。
func (m *AlertManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fileH != nil {
		err := m.fileH.Close()
		m.fileH = nil
		return err
	}
	return nil
}

// DefaultAlerts 全局告警管理器（服务启动时 Configure）。
var DefaultAlerts = NewAlertManager()

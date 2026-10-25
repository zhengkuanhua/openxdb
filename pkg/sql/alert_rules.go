package sql

// T37 商用档：监控告警（告警规则与阈值配置）。
//
// 在已有可观测性（M13 metrics.Registry.Snapshot / STATUS / SHOW STATS）之上
// 提供"指标阈值规则"型主动告警：规则定义（阈值、持续时长、严重级别）、
// 评估引擎（定时评估指标快照、状态机 NORMAL/ALERT/RESOLVED、持续时长防抖）、
// 通知出口（复用 logx.DefaultAlerts：内存环形缓冲 + 文件 + webhook，与事件
// 告警同源 SHOW ALERTS）、管理语句（SHOW ALERT RULES / SHOW ALERT STATUS /
// SET alert_rules* 热更新与 SET PERSIST 持久化）。
//
// 设计文档：docs/T37_commercial_alerting.md。
// 评估数据源与 Prometheus 指标同源（metrics.Registry.Snapshot()），不另造口径。

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/logx"
	"github.com/zhengkuanhua/openxdb/pkg/metrics"
)

// AlertRule 一条告警规则：以指标快照字段为输入，超过阈值并持续 duration 秒
// 触发告警；severity 为严重级别（info/warn/critical）。webhook 为规则级回调
// URL（可选）：触发/恢复时异步 POST JSON 载荷，与 logx 全局通知并存。
type AlertRule struct {
	ID          string  `json:"id"`           // 规则唯一标识
	Metric      string  `json:"metric"`       // 指标名（见 alertMetricValue）
	Op          string  `json:"op"`           // 比较运算符：> >= < <=
	Threshold   float64 `json:"threshold"`    // 阈值（非负）
	DurationSec int64   `json:"duration_sec"` // 越界持续秒数；0 = 立即触发
	Severity    string  `json:"severity"`     // info / warn / critical
	Enabled     bool    `json:"enabled"`      // 规则开关
	Webhook     string  `json:"webhook,omitempty"` // 规则回调 URL（http/https，可选）
}

// 规则状态机（NORMAL/ALERT/RESOLVED）。
type ruleState string

const (
	ruleStateNormal   ruleState = "NORMAL"
	ruleStateAlert    ruleState = "ALERT"
	ruleStateResolved ruleState = "RESOLVED"
)

// ruleRuntime 单规则运行时状态。
type ruleRuntime struct {
	state          ruleState
	violatingSince time.Time // NORMAL 下首次越界时刻；未越界为零值
	lastValue      float64   // 最近一次评估值（SHOW ALERT RULES 展示）
	hasValue       bool      // 是否已评估过（避免未评估显示 0 误导）
}

// alertRulesConfig 评估引擎配置快照。
type alertRulesConfig struct {
	Enabled      bool  // 总开关（alert_rules_enabled）
	EvalInterval int64 // 评估周期秒（alert_eval_interval，1-3600）
	Rules        []AlertRule
}

// AlertRulesEngine 告警规则评估引擎：定时评估指标快照并驱动状态机，越界持续
// duration 触发告警、恢复后发送一次性恢复通知。装配模式与 BackupScheduler
// 一致（NewExecutor 初始化；Engine.StartAlertRules 启动循环）。
type AlertRulesEngine struct {
	e *Executor

	mu   sync.Mutex
	cfg  alertRulesConfig
	rt   map[string]*ruleRuntime
	last time.Time // 最近一次评估时刻（节拍轮询判断周期）

	started bool
	stopCh  chan struct{}
	wakeCh  chan struct{}
	doneCh  chan struct{}

	now      func() time.Time                       // 测试注入时钟
	notifier func(level, module, event, msg string) // 通知出口（默认 logx.DefaultAlerts.Notify）
	snapshot func() metrics.Snapshot                // 指标快照源（默认 e.m.Snapshot，e.m 非 nil）

	webhookClient *http.Client // 规则级 webhook 回调客户端（2s 超时，尽力而为）
}

// alertRulesTick 评估循环检查节拍（固定细粒度轮询，周期由 alert_eval_interval
// 控制，与 backupScheduler 同模式）。
const alertRulesTick = 500 * time.Millisecond

// newAlertRulesEngine 创建评估引擎（NewExecutor 装配；e 用于取指标快照）。
func newAlertRulesEngine(e *Executor) *AlertRulesEngine {
	return &AlertRulesEngine{
		e:        e,
		cfg:      alertRulesConfig{EvalInterval: 10}, // T37：默认周期与 configSpecs 一致
		rt:       map[string]*ruleRuntime{},
		now:      time.Now,
		notifier: logx.DefaultAlerts.Notify,
		webhookClient: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

// Start 启动评估循环（幂等；Stop 后可再次 Start）。
func (ar *AlertRulesEngine) Start() {
	ar.mu.Lock()
	if ar.started {
		ar.mu.Unlock()
		return
	}
	ar.started = true
	ar.stopCh = make(chan struct{})
	ar.wakeCh = make(chan struct{}, 1)
	ar.doneCh = make(chan struct{})
	ar.mu.Unlock()
	go ar.loop()
}

// Stop 停止评估循环并等待退出（幂等）。
func (ar *AlertRulesEngine) Stop() {
	ar.mu.Lock()
	if !ar.started {
		ar.mu.Unlock()
		return
	}
	ar.started = false
	ch := ar.stopCh
	done := ar.doneCh
	ar.mu.Unlock()
	close(ch)
	<-done
}

// wake 唤醒循环立即评估（channel 容量 1，合并突发多次更新）。
func (ar *AlertRulesEngine) wake() {
	ar.mu.Lock()
	ch := ar.wakeCh
	ar.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// loop 评估主循环：固定节拍轮询 + 配置更新唤醒。
func (ar *AlertRulesEngine) loop() {
	tick := time.NewTicker(alertRulesTick)
	defer tick.Stop()
	defer close(ar.doneCh)
	for {
		select {
		case <-ar.stopCh:
			return
		case <-ar.wakeCh:
			ar.maybeEval()
		case <-tick.C:
			ar.maybeEval()
		}
	}
}

// maybeEval 检查是否到评估周期：总开关关闭 / 未装配指标 / 未到周期 均跳过。
func (ar *AlertRulesEngine) maybeEval() {
	ar.mu.Lock()
	enabled := ar.cfg.Enabled
	interval := ar.cfg.EvalInterval
	last := ar.last
	now := ar.now()
	ar.mu.Unlock()
	if !enabled || interval <= 0 {
		return
	}
	if !last.IsZero() && now.Sub(last) < time.Duration(interval)*time.Second {
		return
	}
	ar.mu.Lock()
	ar.last = now
	ar.mu.Unlock()
	if ar.snapshot == nil {
		return
	}
	ar.Evaluate(ar.snapshot(), now)
}

// SetEnabled 设置总开关（SET alert_rules_enabled，即时生效）。
func (ar *AlertRulesEngine) SetEnabled(on bool) {
	ar.mu.Lock()
	ar.cfg.Enabled = on
	ar.mu.Unlock()
	ar.wake()
}

// Enabled 返回总开关状态。
func (ar *AlertRulesEngine) Enabled() bool {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	return ar.cfg.Enabled
}

// SetEvalInterval 设置评估周期秒（1-3600；非法值忽略，保持原值）。
func (ar *AlertRulesEngine) SetEvalInterval(sec int64) {
	if sec < 1 || sec > 3600 {
		return
	}
	ar.mu.Lock()
	ar.cfg.EvalInterval = sec
	ar.mu.Unlock()
	ar.wake()
}

// EvalInterval 返回评估周期秒。
func (ar *AlertRulesEngine) EvalInterval() int64 {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	return ar.cfg.EvalInterval
}

// SetRules 全量替换规则集（SET alert_rules；调用方保证已通过 parseAlertRulesJSON
// 校验）。规则状态随替换重建（同名规则重新走 NORMAL 计时）。
func (ar *AlertRulesEngine) SetRules(rules []AlertRule) {
	ar.mu.Lock()
	ar.cfg.Rules = rules
	ar.rt = map[string]*ruleRuntime{}
	ar.last = time.Time{}
	ar.mu.Unlock()
	ar.wake()
}

// Config 返回配置快照（SHOW ALERT STATUS 数据源）。
func (ar *AlertRulesEngine) Config() alertRulesConfig {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	return ar.cfg
}

// Evaluate 执行一轮评估：逐条 enabled 规则取指标值、比较阈值并驱动状态机。
// 状态转移通知经 notifier 出口（默认 logx.DefaultAlerts.Notify，进入与事件
// 告警相同的文件/webhook/内存缓冲）。
func (ar *AlertRulesEngine) Evaluate(snap metrics.Snapshot, now time.Time) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	for i := range ar.cfg.Rules {
		r := &ar.cfg.Rules[i]
		if !r.Enabled {
			continue
		}
		v, ok := alertMetricValue(snap, r.Metric)
		if !ok {
			continue // 指标不可用（未装配采集源），跳过本轮
		}
		st := ar.rt[r.ID]
		if st == nil {
			st = &ruleRuntime{state: ruleStateNormal}
			ar.rt[r.ID] = st
		}
		st.lastValue = v
		st.hasValue = true
		violating := alertCompare(v, r.Op, r.Threshold)
		ar.advanceLocked(r, st, violating, now)
	}
}

// advanceLocked 按状态机推进单规则（调用方持锁）。
//
//	NORMAL+越界：记录首次越界；持续 >= duration 转 ALERT 并触发通知。
//	NORMAL+未越界：清零首次越界。
//	ALERT+越界：保持 ALERT（不重复通知）。
//	ALERT+未越界：转 RESOLVED 并发一次性恢复通知（info）。
//	RESOLVED+越界：回 NORMAL 重新计时 duration（防抖）。
//	RESOLVED+未越界：保持 RESOLVED（不重复恢复通知）。
func (ar *AlertRulesEngine) advanceLocked(r *AlertRule, st *ruleRuntime, violating bool, now time.Time) {
	switch st.state {
	case ruleStateNormal:
		if !violating {
			st.violatingSince = time.Time{}
			return
		}
		if st.violatingSince.IsZero() {
			st.violatingSince = now
		}
		if r.DurationSec <= 0 || now.Sub(st.violatingSince) >= time.Duration(r.DurationSec)*time.Second {
			st.state = ruleStateAlert
			ar.notifyLocked(r, st, "rule_triggered", r.Severity)
		}
	case ruleStateAlert:
		if violating {
			return
		}
		st.state = ruleStateResolved
		ar.notifyLocked(r, st, "rule_resolved", "info")
	case ruleStateResolved:
		if !violating {
			return
		}
		st.state = ruleStateNormal
		st.violatingSince = now
	}
}

// notifyLocked 发出一条规则告警/恢复通知（调用方持锁；msg 含规则上下文）。
// 除 notifier 出口外，若规则配置了 webhook 回调 URL，同步构造 JSON 载荷并
// 异步 POST（2s 超时，尽力而为，失败静默不影响评估循环）。
func (ar *AlertRulesEngine) notifyLocked(r *AlertRule, st *ruleRuntime, event, level string) {
	msg := fmt.Sprintf("rule=%s metric=%s op=%s threshold=%s current=%s duration=%ds severity=%s state=%s",
		r.ID, r.Metric, r.Op, strconv.FormatFloat(r.Threshold, 'g', -1, 64),
		strconv.FormatFloat(st.lastValue, 'g', -1, 64), r.DurationSec, r.Severity, st.state)
	if ar.notifier != nil {
		ar.notifier(level, "alert-rules", event, msg)
	}
	if r.Webhook != "" && ar.webhookClient != nil {
		go ar.pushRuleWebhook(r.Webhook, ar.ruleWebhookBody(r, st, event, level))
	}
}

// ruleWebhookBody 构造规则回调 JSON 载荷（调用方持锁，字段读取安全；
// goroutine 只持有已序列化的 body，不接触规则/运行时对象）。
func (ar *AlertRulesEngine) ruleWebhookBody(r *AlertRule, st *ruleRuntime, event, level string) []byte {
	type ruleInfo struct {
		ID          string  `json:"id"`
		Metric      string  `json:"metric"`
		Op          string  `json:"op"`
		Threshold   float64 `json:"threshold"`
		DurationSec int64   `json:"duration_sec"`
		Severity    string  `json:"severity"`
	}
	payload := struct {
		Event       string   `json:"event"`
		Level       string   `json:"level"`
		State       string   `json:"state"`
		MetricValue float64  `json:"metric_value"`
		Rule        ruleInfo `json:"rule"`
		Time        string   `json:"time"`
	}{
		Event:       event,
		Level:       level,
		State:       string(st.state),
		MetricValue: st.lastValue,
		Rule: ruleInfo{
			ID: r.ID, Metric: r.Metric, Op: r.Op, Threshold: r.Threshold,
			DurationSec: r.DurationSec, Severity: r.Severity,
		},
		Time: ar.now().Format(time.RFC3339),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		// Marshal 失败仅发生于字段不可序列化（结构体字段均为基础类型，理论不可达）。
		return []byte(`{"event":"` + event + `","level":"` + level + `","error":"marshal"}`)
	}
	return b
}

// pushRuleWebhook 异步发送规则回调（非阻塞；失败静默，规则引擎不因此降级）。
func (ar *AlertRulesEngine) pushRuleWebhook(url string, body []byte) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := ar.webhookClient.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

// RuleStatus SHOW ALERT RULES 单行（规则 + 运行时状态 + 最近评估值）。
type RuleStatus struct {
	AlertRule
	State     string
	LastValue float64
	HasValue  bool
}

// Status 返回全部规则状态快照（SHOW ALERT RULES 数据源）。
func (ar *AlertRulesEngine) Status() []RuleStatus {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	out := make([]RuleStatus, 0, len(ar.cfg.Rules))
	for i := range ar.cfg.Rules {
		r := ar.cfg.Rules[i]
		rs := RuleStatus{AlertRule: r, State: string(ruleStateNormal)}
		if st := ar.rt[r.ID]; st != nil {
			rs.State = string(st.state)
			rs.LastValue = st.lastValue
			rs.HasValue = st.hasValue
		}
		out = append(out, rs)
	}
	return out
}

// supportedAlertMetrics 支持的指标名 → 快照取值函数（与 metrics.Snapshot 同源）。
var supportedAlertMetrics = map[string]func(s metrics.Snapshot) (float64, bool){
	"sql_total":        func(s metrics.Snapshot) (float64, bool) { return float64(s.SQLTotal), true },
	"kv_total":         func(s metrics.Snapshot) (float64, bool) { return float64(s.KVTotal), true },
	"sql_qps":          func(s metrics.Snapshot) (float64, bool) { return float64(s.SQLQPS), true },
	"kv_qps":           func(s metrics.Snapshot) (float64, bool) { return float64(s.KVQPS), true },
	"active_conns":     func(s metrics.Snapshot) (float64, bool) { return float64(s.ActiveConns), true },
	"slow_query_total": func(s metrics.Snapshot) (float64, bool) { return float64(s.SlowQueryTotal), true },
	"storage_bytes":    func(s metrics.Snapshot) (float64, bool) { return float64(s.StorageBytes), true },
	"txn_commit":       func(s metrics.Snapshot) (float64, bool) { return float64(s.TxnCommit), true },
	"txn_rollback":     func(s metrics.Snapshot) (float64, bool) { return float64(s.TxnRollback), true },
	"local_total":      func(s metrics.Snapshot) (float64, bool) { return float64(s.LocalTotal), true },
	"remote_total":     func(s metrics.Snapshot) (float64, bool) { return float64(s.RemoteTotal), true },
	"region_total":     func(s metrics.Snapshot) (float64, bool) { return float64(s.Regions.Total), true },
	"region_local":     func(s metrics.Snapshot) (float64, bool) { return float64(s.Regions.Local), true },
	"region_remote":    func(s metrics.Snapshot) (float64, bool) { return float64(s.Regions.Remote), true },
}

// alertMetricValue 取指标值；未知指标 / 采集源未装配（0 但标记不可用）返回
// ok=false。注：累计型计数 0 是合法值，是否可用以快照自身字段为准；active_conns
// 等注入源未装配时快照为 0，仍视为可用（阈值可命中 0 场景）。
func alertMetricValue(s metrics.Snapshot, name string) (float64, bool) {
	fn, ok := supportedAlertMetrics[name]
	if !ok {
		return 0, false
	}
	return fn(s)
}

// alertCompare 阈值比较（op: > >= < <=；非法 op 返回 false）。
func alertCompare(v float64, op string, threshold float64) bool {
	switch op {
	case ">":
		return v > threshold
	case ">=":
		return v >= threshold
	case "<":
		return v < threshold
	case "<=":
		return v <= threshold
	}
	return false
}

// validSeverity 校验严重级别。
func validSeverity(s string) bool {
	switch s {
	case "info", "warn", "critical":
		return true
	}
	return false
}

// parseAlertRulesJSON 解析并校验规则 JSON 数组文本（SET alert_rules 与持久化
// 加载共用）。空串返回 nil（清空规则）。任一规则非法即整体报错（防部分生效）。
func parseAlertRulesJSON(s string) ([]AlertRule, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var rules []AlertRule
	if err := json.Unmarshal([]byte(s), &rules); err != nil {
		return nil, fmt.Errorf("invalid alert_rules JSON: %w", err)
	}
	seen := map[string]bool{}
	for i := range rules {
		r := &rules[i]
		if strings.TrimSpace(r.ID) == "" {
			return nil, fmt.Errorf("alert rule #%d: id is required", i+1)
		}
		if seen[r.ID] {
			return nil, fmt.Errorf("alert rule id %q duplicated", r.ID)
		}
		seen[r.ID] = true
		if _, ok := supportedAlertMetrics[r.Metric]; !ok {
			return nil, fmt.Errorf("alert rule %q: unsupported metric %q", r.ID, r.Metric)
		}
		switch r.Op {
		case ">", ">=", "<", "<=":
		default:
			return nil, fmt.Errorf("alert rule %q: invalid op %q (expected > >= < <=)", r.ID, r.Op)
		}
		if r.Threshold < 0 {
			return nil, fmt.Errorf("alert rule %q: threshold must be non-negative", r.ID)
		}
		if r.DurationSec < 0 {
			return nil, fmt.Errorf("alert rule %q: duration_sec must be non-negative", r.ID)
		}
		if !validSeverity(r.Severity) {
			return nil, fmt.Errorf("alert rule %q: invalid severity %q (expected info/warn/critical)", r.ID, r.Severity)
		}
		if r.Webhook != "" && !strings.HasPrefix(r.Webhook, "http://") && !strings.HasPrefix(r.Webhook, "https://") {
			return nil, fmt.Errorf("alert rule %q: invalid webhook URL %q (expected http:// or https://)", r.ID, r.Webhook)
		}
	}
	return rules, nil
}

// ---- Executor 装配与 SQL 接口 ----

// initAlertRulesEngine 初始化评估引擎（NewExecutor 构造后调用；Engine 装配与
// 关闭依赖该字段非 nil）。
func (e *Executor) initAlertRulesEngine() {
	if e.alertRules == nil {
		e.alertRules = newAlertRulesEngine(e)
	}
}

// SetAlertRulesEnabled 设置规则告警总开关（SET alert_rules_enabled，即时生效）。
func (e *Executor) SetAlertRulesEnabled(on bool) { e.alertRules.SetEnabled(on) }

// AlertRulesEnabled 返回规则告警总开关。
func (e *Executor) AlertRulesEnabled() bool { return e.alertRules.Enabled() }

// SetAlertEvalInterval 设置评估周期秒（SET alert_eval_interval，即时生效）。
func (e *Executor) SetAlertEvalInterval(sec int64) { e.alertRules.SetEvalInterval(sec) }

// AlertEvalInterval 返回评估周期秒。
func (e *Executor) AlertEvalInterval() int64 { return e.alertRules.EvalInterval() }

// SetAlertRulesJSON 解析并应用规则 JSON（SET alert_rules；非法规则返回错误，
// 当前规则集保持不变）。
func (e *Executor) SetAlertRulesJSON(s string) error {
	rules, err := parseAlertRulesJSON(s)
	if err != nil {
		return err
	}
	e.alertRules.SetRules(rules)
	return nil
}

// AlertRulesJSON 返回当前规则集 JSON 文本（SHOW CONFIG 展示用）。
func (e *Executor) AlertRulesJSON() string {
	cfg := e.alertRules.Config()
	if len(cfg.Rules) == 0 {
		return ""
	}
	b, err := json.Marshal(cfg.Rules)
	if err != nil {
		return ""
	}
	return string(b)
}

// AlertRulesStatus 返回规则告警状态摘要（enabled/interval/规则数）。
func (e *Executor) AlertRulesStatus() (enabled bool, interval int64, count int) {
	cfg := e.alertRules.Config()
	return cfg.Enabled, cfg.EvalInterval, len(cfg.Rules)
}

// AlertRules 返回规则状态快照（SHOW ALERT RULES 数据源）。
func (e *Executor) AlertRules() []RuleStatus { return e.alertRules.Status() }

// StartAlertRules 启动规则评估循环（cmd/main start 装配；幂等）。
func (e *Executor) StartAlertRules() { e.alertRules.Start() }

// StopAlertRules 停止规则评估循环（cmd/main 关闭装配；幂等）。
func (e *Executor) StopAlertRules() { e.alertRules.Stop() }

// EvaluateAlertRulesNow 立即执行一轮评估（测试与运维手动触发用；总开关关闭或
// 未装配指标时为空操作）。
func (e *Executor) EvaluateAlertRulesNow() {
	ar := e.alertRules
	ar.mu.Lock()
	enabled := ar.cfg.Enabled
	ar.mu.Unlock()
	if !enabled {
		return
	}
	if ar.snapshot == nil {
		return
	}
	ar.Evaluate(ar.snapshot(), ar.now())
}

// execShowAlertRules SHOW ALERT RULES：列出规则与运行时状态
// （id/metric/op/threshold/duration_sec/severity/enabled/state/last_value）。
func (e *Executor) execShowAlertRules(sess *Session, s *ShowAlertRulesStmt) (*Result, error) {
	res := &Result{Columns: []string{"id", "metric", "op", "threshold", "duration_sec", "severity", "enabled", "state", "last_value"}}
	for _, rs := range e.AlertRules() {
		lastVal := ""
		if rs.HasValue {
			lastVal = strconv.FormatFloat(rs.LastValue, 'g', -1, 64)
		}
		res.Rows = append(res.Rows, []Value{
			StrVal(rs.ID), StrVal(rs.Metric), StrVal(rs.Op),
			StrVal(strconv.FormatFloat(rs.Threshold, 'g', -1, 64)),
			StrVal(strconv.FormatInt(rs.DurationSec, 10)),
			StrVal(rs.Severity), StrVal(boolValStr(rs.Enabled)),
			StrVal(rs.State), StrVal(lastVal),
		})
	}
	return res, nil
}

package sql

// T37 v1.11.0 增强：规则级 webhook 回调测试。
// 验证：触发/恢复时异步 POST JSON 载荷、与 logx notifier 并存、非法 URL 拒绝、
// 未配置 webhook 的规则不产生回调请求。

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/metrics"
)

// webhookRecorder 记录回调请求体（channel 供测试同步等待）。
type webhookRecorder struct {
	ch chan string
}

func (w *webhookRecorder) handler() http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if ct := req.Header.Get("Content-Type"); ct != "application/json" {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}
		body, _ := io.ReadAll(req.Body)
		w.ch <- string(body)
		rw.WriteHeader(http.StatusOK)
	}
}

func waitWebhook(t *testing.T, rec *webhookRecorder) string {
	t.Helper()
	select {
	case body := <-rec.ch:
		return body
	case <-time.After(2 * time.Second):
		t.Fatalf("webhook callback not received within timeout")
		return ""
	}
}

// TestAlertRuleWebhookTriggerAndResolved 规则触发与恢复各推一次回调，
// 且与 notifier 出口并存。
func TestAlertRuleWebhookTriggerAndResolved(t *testing.T) {
	rec := &webhookRecorder{ch: make(chan string, 4)}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	ar, _, events := newTestAlertEngine(t)
	rules := []AlertRule{
		{
			ID: "web-conns", Metric: "active_conns", Op: ">", Threshold: 3,
			DurationSec: 0, Severity: "warn", Webhook: srv.URL,
		},
	}
	setTestRules(ar, rules...)

	// 越界 → ALERT，回调 rule_triggered。
	ar.Evaluate(metrics.Snapshot{ActiveConns: 5}, ar.now())
	body := waitWebhook(t, rec)
	var got struct {
		Event       string  `json:"event"`
		Level       string  `json:"level"`
		State       string  `json:"state"`
		MetricValue float64 `json:"metric_value"`
		Rule        struct {
			ID      string `json:"id"`
			Metric  string `json:"metric"`
			Severity string `json:"severity"`
		} `json:"rule"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("unmarshal webhook body: %v (body=%s)", err, body)
	}
	if got.Event != "rule_triggered" || got.Level != "warn" || got.State != "ALERT" {
		t.Fatalf("trigger payload mismatch: %+v", got)
	}
	if got.Rule.ID != "web-conns" || got.Rule.Metric != "active_conns" || got.MetricValue != 5 {
		t.Fatalf("trigger payload rule/current mismatch: %+v", got)
	}
	if len(*events) != 1 || !strings.Contains((*events)[0], "rule_triggered") {
		t.Fatalf("notifier must also receive trigger, got %v", *events)
	}

	// 恢复 → RESOLVED，回调 rule_resolved（info）。
	ar.Evaluate(metrics.Snapshot{ActiveConns: 1}, ar.now())
	body = waitWebhook(t, rec)
	var got2 struct {
		Event string `json:"event"`
		Level string `json:"level"`
	}
	if err := json.Unmarshal([]byte(body), &got2); err != nil {
		t.Fatalf("unmarshal resolved webhook body: %v (body=%s)", err, body)
	}
	if got2.Event != "rule_resolved" || got2.Level != "info" {
		t.Fatalf("resolved payload mismatch: %+v", got2)
	}
	if len(*events) != 2 || !strings.Contains((*events)[1], "rule_resolved") {
		t.Fatalf("notifier must also receive resolved, got %v", *events)
	}
}

// TestAlertRuleWebhookNoCallbackWhenUnset 未配置 webhook 的规则不产生回调请求。
func TestAlertRuleWebhookNoCallbackWhenUnset(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		atomic.AddInt32(&hits, 1)
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ar, _, _ := newTestAlertEngine(t)
	setTestRules(ar, AlertRule{
		ID: "plain", Metric: "active_conns", Op: ">", Threshold: 3,
		DurationSec: 0, Severity: "warn",
	})
	ar.Evaluate(metrics.Snapshot{ActiveConns: 9}, ar.now())
	time.Sleep(100 * time.Millisecond)
	if n := atomic.LoadInt32(&hits); n != 0 {
		t.Fatalf("unexpected webhook hits: %d", n)
	}
}

// TestAlertRuleWebhookParseRejectInvalidURL 非法 webhook URL 整体拒绝。
func TestAlertRuleWebhookParseRejectInvalidURL(t *testing.T) {
	js := `[{"id":"bad","metric":"active_conns","op":">","threshold":1,"duration_sec":0,"severity":"warn","enabled":true,"webhook":"ftp://x"}]`
	if _, err := parseAlertRulesJSON(js); err == nil {
		t.Fatalf("parse must reject non-http webhook URL")
	}
	js2 := `[{"id":"bad2","metric":"active_conns","op":">","threshold":1,"duration_sec":0,"severity":"warn","enabled":true,"webhook":"not a url"}]`
	if _, err := parseAlertRulesJSON(js2); err == nil {
		t.Fatalf("parse must reject malformed webhook URL")
	}
}

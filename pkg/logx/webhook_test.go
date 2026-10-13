package logx

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newTestHook 启动本地 HTTP 服务接收 webhook POST，解析为 Alert 后送入 ch。
func newTestHook(t *testing.T, ch chan Alert) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var a Alert
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			t.Errorf("decode webhook body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		select {
		case ch <- a:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

var _ = time.Second

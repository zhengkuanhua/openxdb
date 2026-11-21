package cluster

// v1.20.0 C1：2PC 参与者提交接入事务提交钩子的单元测试。
// 覆盖：未装配钩子时零回归（直写存储）、装配钩子后批次原样交付回调、
// 回调错误上抛且可重试、幂等重放不重复触发钩子。

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

// newC1Server 构造仅含存储与 2PC 状态的参与者 Server（无需监听）。
func newC1Server(t *testing.T) *Server {
	t.Helper()
	return NewServer(btree.NewKV(32), "node-a")
}

// kv 读取存储键值，不存在返回 (nil, false)。
func kv(t *testing.T, st storage.Storage, key string) ([]byte, bool) {
	t.Helper()
	v, err := st.Get([]byte(key))
	if err == storage.ErrNotFound {
		return nil, false
	}
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	return v, true
}

// TestParticipantCommitUnassembledZeroRegression 未装配 participantCommit 时
// commit2pc 仍直接写存储：数据可见、committed 标记落盘、pending 清理，
// 与改动前行为完全一致。
func TestParticipantCommitUnassembledZeroRegression(t *testing.T) {
	srv := newC1Server(t)
	ops := []TxnOp{{Key: []byte("k1"), Value: []byte("v1")}}
	if err := srv.prepare2pc("tx-old", 100, ops); err != nil {
		t.Fatalf("prepare2pc: %v", err)
	}
	if err := srv.commit2pc("tx-old", 100); err != nil {
		t.Fatalf("commit2pc: %v", err)
	}
	if v, ok := kv(t, srv.st, "k1"); !ok || string(v) != "v1" {
		t.Fatalf("k1 = %q ok=%v, want v1", v, ok)
	}
	if _, ok := kv(t, srv.st, string(committed2pcKey("tx-old"))); !ok {
		t.Fatalf("committed marker missing (old path must still write it)")
	}
	if _, ok := kv(t, srv.st, string(prepared2pcKey("tx-old"))); ok {
		t.Fatalf("prepared marker must be deleted on commit")
	}
	if len(srv.pending2pc) != 0 {
		t.Fatalf("pending2pc not cleaned: %v", srv.pending2pc)
	}
}

// TestParticipantCommitHookReceivesBatch 装配钩子后提交批次（数据键 +
// 版本记录 + 2PC 元键）原样交给回调，且 Server 自身不再直写存储。
func TestParticipantCommitHookReceivesBatch(t *testing.T) {
	srv := newC1Server(t)
	var gotTxID string
	var gotTS uint64
	var gotBatch *storage.WriteBatch
	calls := 0
	srv.SetParticipantCommit(func(txID string, commitTS uint64, batch *storage.WriteBatch) error {
		calls++
		gotTxID, gotTS, gotBatch = txID, commitTS, batch
		return nil
	})
	ops := []TxnOp{
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k9"), Delete: true},
	}
	if err := srv.prepare2pc("tx-hook", 200, ops); err != nil {
		t.Fatalf("prepare2pc: %v", err)
	}
	if err := srv.commit2pc("tx-hook", 200); err != nil {
		t.Fatalf("commit2pc: %v", err)
	}
	if calls != 1 || gotTxID != "tx-hook" || gotTS != 200 {
		t.Fatalf("hook args: calls=%d txID=%q ts=%d", calls, gotTxID, gotTS)
	}
	if gotBatch == nil {
		t.Fatalf("hook batch nil")
	}
	// 数据键：Put k2=v2；2PC 元键：Put committed 标记、Delete prepared 标记。
	var hasPut, hasCommitted, hasPreparedDelete, hasVersion bool
	for _, p := range gotBatch.Puts {
		switch {
		case bytes.Equal(p.Key, []byte("k2")):
			hasPut = string(p.Value) == "v2"
		case bytes.Equal(p.Key, committed2pcKey("tx-hook")):
			hasCommitted = true
		case bytes.HasPrefix(p.Key, []byte("m:ver:")):
			hasVersion = true
		}
	}
	for _, k := range gotBatch.Deletes {
		if bytes.Equal(k, prepared2pcKey("tx-hook")) {
			hasPreparedDelete = true
		}
		if bytes.Equal(k, []byte("k9")) && hasPut {
			// 业务删除键也在批次内（此处仅记录，不置位）
		}
	}
	if !hasPut || !hasCommitted || !hasPreparedDelete {
		t.Fatalf("batch missing expected entries: put=%v committed=%v preparedDelete=%v", hasPut, hasCommitted, hasPreparedDelete)
	}
	if !hasVersion {
		t.Fatalf("batch missing version record puts")
	}
	// 装配钩子后由回调负责落库，Server 不再直写本地存储。
	if _, ok := kv(t, srv.st, "k2"); ok {
		t.Fatalf("server must not write storage directly when hook assembled")
	}
	if len(srv.pending2pc) != 0 {
		t.Fatalf("pending2pc not cleaned after hook success")
	}
}

// TestParticipantCommitHookErrorPropagatesAndRetries 回调失败时 commit2pc
// 原样返回错误、保留 pending（协调者重试可重放）；改为直写后重试成功。
func TestParticipantCommitHookErrorPropagatesAndRetries(t *testing.T) {
	srv := newC1Server(t)
	boom := errors.New("hook boom")
	srv.SetParticipantCommit(func(string, uint64, *storage.WriteBatch) error { return boom })
	ops := []TxnOp{{Key: []byte("k3"), Value: []byte("v3")}}
	if err := srv.prepare2pc("tx-err", 300, ops); err != nil {
		t.Fatalf("prepare2pc: %v", err)
	}
	err := srv.commit2pc("tx-err", 300)
	if err == nil || !strings.Contains(err.Error(), "hook boom") {
		t.Fatalf("commit2pc err = %v, want hook error", err)
	}
	if _, ok := kv(t, srv.st, string(committed2pcKey("tx-err"))); ok {
		t.Fatalf("committed marker must not be written when hook fails")
	}
	if len(srv.pending2pc) != 1 {
		t.Fatalf("pending2pc must be kept for retry, got %v", srv.pending2pc)
	}
	// 重试：撤下钩子走旧直写路径，数据与标记补齐。
	srv.SetParticipantCommit(nil)
	if err := srv.commit2pc("tx-err", 300); err != nil {
		t.Fatalf("retry commit2pc: %v", err)
	}
	if v, ok := kv(t, srv.st, "k3"); !ok || string(v) != "v3" {
		t.Fatalf("k3 = %q ok=%v after retry", v, ok)
	}
}

// TestParticipantCommitIdempotentReplaySkipsHook 幂等重放（重复 COMMIT）
// 不重复触发回调，不产生重复副作用。
func TestParticipantCommitIdempotentReplaySkipsHook(t *testing.T) {
	srv := newC1Server(t)
	calls := 0
	// 钩子模拟 db 层真实实现：批次最终落到本地同一存储（含 committed 标记），
	// 幂等重放依赖该标记早退。
	srv.SetParticipantCommit(func(txID string, commitTS uint64, batch *storage.WriteBatch) error {
		calls++
		return srv.st.Write(batch)
	})
	ops := []TxnOp{{Key: []byte("k4"), Value: []byte("v4")}}
	if err := srv.prepare2pc("tx-idem", 400, ops); err != nil {
		t.Fatalf("prepare2pc: %v", err)
	}
	if err := srv.commit2pc("tx-idem", 400); err != nil {
		t.Fatalf("commit2pc: %v", err)
	}
	// 第二次 COMMIT：内存已 committed 标记，早退不调用钩子。
	srv.pending2pc["tx-idem"] = ops
	if err := srv.commit2pc("tx-idem", 400); err != nil {
		t.Fatalf("replay commit2pc: %v", err)
	}
	if calls != 1 {
		t.Fatalf("hook calls = %d, want 1 (idempotent replay must skip hook)", calls)
	}
}

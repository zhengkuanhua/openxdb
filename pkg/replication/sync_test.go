package replication

// T33 同步/半同步复制单元测试（M20）。
// 覆盖：模式解析与校验、WaitAck 各模式（async 零阻塞 / semi-sync 确认与
// 降级 / sync 全确认与超时报错 / 无在线副本拒绝伪同步）。

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ---- 模式解析与参数校验 ----

func TestParseReplMode(t *testing.T) {
	cases := []struct {
		in   string
		want ReplMode
		err  bool
	}{
		{"async", ModeAsync, false},
		{"", ModeAsync, false},
		{"ASYNC", ModeAsync, false},
		{"semi-sync", ModeSemiSync, false},
		{"semisync", ModeSemiSync, false},
		{"semi_sync", ModeSemiSync, false},
		{"sync", ModeSync, false},
		{"SYNC", ModeSync, false},
		{"bogus", ModeAsync, true},
	}
	for _, c := range cases {
		got, err := ParseReplMode(c.in)
		if c.err && err == nil {
			t.Fatalf("ParseReplMode(%q): want error, got nil", c.in)
		}
		if !c.err && (err != nil || got != c.want) {
			t.Fatalf("ParseReplMode(%q) = %v, %v; want %v", c.in, got, err, c.want)
		}
	}
	if ModeAsync.String() != "async" || ModeSemiSync.String() != "semi-sync" || ModeSync.String() != "sync" {
		t.Fatalf("unexpected mode strings: %s/%s/%s", ModeAsync, ModeSemiSync, ModeSync)
	}
}

func TestSetReplicationModeValidation(t *testing.T) {
	m := newMaster(t)
	if err := m.SetReplicationMode(ReplMode(99), 1, time.Second); err == nil {
		t.Fatalf("invalid mode: want error")
	}
	if err := m.SetReplicationMode(ModeSemiSync, 0, time.Second); err == nil {
		t.Fatalf("ackN=0: want error")
	}
	if err := m.SetReplicationMode(ModeSemiSync, 1, -time.Second); err == nil {
		t.Fatalf("negative timeout: want error")
	}
	// 合法切换：semi-sync 参数生效；切回 async 后参数重置为默认
	if err := m.SetReplicationMode(ModeSemiSync, 3, 250*time.Millisecond); err != nil {
		t.Fatalf("set semi-sync: %v", err)
	}
	st := m.SyncStatus()
	if st.Mode != ModeSemiSync || st.SemiAckN != 3 || st.Timeout != 250*time.Millisecond {
		t.Fatalf("semi-sync status = %+v", st)
	}
	if err := m.SetReplicationMode(ModeAsync, 1, 0); err != nil {
		t.Fatalf("set async: %v", err)
	}
	st = m.SyncStatus()
	if st.Mode != ModeAsync || st.SemiAckN != DefaultSemiAckN || st.Timeout != DefaultAckTimeout {
		t.Fatalf("async status = %+v", st)
	}
}

// ---- WaitAck 基础行为 ----

// 裸客户端：只握手不回任何帧（不回 ACK 也不回 PONG），模拟"在线但不确认"。
func dialSilentSlave(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := writeFrame(conn, msgHello, encodeHello(RoleSlave, 0)); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	// 等 HELLO_OK（握手完成即被 master 视为在线）
	if _, _, err := readFrame(conn); err != nil {
		t.Fatalf("read hello_ok: %v", err)
	}
	return conn
}

func TestWaitAckAsyncNoBlock(t *testing.T) {
	m := newMaster(t)
	lsn := mustAppend(t, m, "k", "v")
	if err := m.WaitAck(context.Background(), lsn); err != nil {
		t.Fatalf("async WaitAck: %v", err)
	}
	if st := m.SyncStatus(); st.Waiting || st.LastWaitMs != 0 {
		t.Fatalf("async should not wait, status=%+v", st)
	}
}

func TestWaitAckSyncAllOnlineConfirmed(t *testing.T) {
	m := newMaster(t)
	m.HeartbeatInterval = 10 * time.Second
	f := NewFollower(newBtree())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Start(ctx, m.Addr())
	waitSlave(t, m, true, 0)

	if err := m.SetReplicationMode(ModeSync, 1, 5*time.Second); err != nil {
		t.Fatalf("set sync: %v", err)
	}
	lsn := mustAppend(t, m, "a", "1")
	if err := m.WaitAck(context.Background(), lsn); err != nil {
		t.Fatalf("sync WaitAck with live slave: %v", err)
	}
	st := m.SyncStatus()
	if st.Degraded || st.Waiting {
		t.Fatalf("sync should not degrade, status=%+v", st)
	}
	waitApplied(t, f, lsn)
}

func TestWaitAckSyncNoSlaveRejected(t *testing.T) {
	m := newMaster(t)
	if err := m.SetReplicationMode(ModeSync, 1, time.Second); err != nil {
		t.Fatalf("set sync: %v", err)
	}
	lsn := mustAppend(t, m, "a", "1")
	if err := m.WaitAck(context.Background(), lsn); err != ErrNoSlave {
		t.Fatalf("sync no slave: want ErrNoSlave, got %v", err)
	}
}

func TestWaitAckSyncTimeout(t *testing.T) {
	m := newMaster(t)
	m.HeartbeatInterval = 10 * time.Second
	dialSilentSlave(t, m.Addr())
	// 等 master 将裸客户端标记在线
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := m.SyncStatus()
		if st.OnlineSlaves >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if m.SyncStatus().OnlineSlaves < 1 {
		t.Fatalf("silent slave not registered online")
	}
	if err := m.SetReplicationMode(ModeSync, 1, 150*time.Millisecond); err != nil {
		t.Fatalf("set sync: %v", err)
	}
	lsn := mustAppend(t, m, "a", "1")
	if err := m.WaitAck(context.Background(), lsn); err != ErrAckTimeout {
		t.Fatalf("sync timeout: want ErrAckTimeout, got %v", err)
	}
}

func TestWaitAckSemiSyncConfirmed(t *testing.T) {
	m := newMaster(t)
	m.HeartbeatInterval = 10 * time.Second
	f := NewFollower(newBtree())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Start(ctx, m.Addr())
	waitSlave(t, m, true, 0)

	if err := m.SetReplicationMode(ModeSemiSync, 1, 5*time.Second); err != nil {
		t.Fatalf("set semi-sync: %v", err)
	}
	lsn := mustAppend(t, m, "a", "1")
	if err := m.WaitAck(context.Background(), lsn); err != nil {
		t.Fatalf("semi-sync WaitAck: %v", err)
	}
	st := m.SyncStatus()
	if st.Degraded {
		t.Fatalf("semi-sync with ack should not degrade, status=%+v", st)
	}
	waitApplied(t, f, lsn)
}

func TestWaitAckSemiSyncNoSlaveDegrades(t *testing.T) {
	m := newMaster(t)
	if err := m.SetReplicationMode(ModeSemiSync, 1, time.Second); err != nil {
		t.Fatalf("set semi-sync: %v", err)
	}
	lsn := mustAppend(t, m, "a", "1")
	if err := m.WaitAck(context.Background(), lsn); err != nil {
		t.Fatalf("semi-sync no slave should degrade to async: %v", err)
	}
	st := m.SyncStatus()
	if !st.Degraded || st.DegradedCount != 1 || st.DegradedLSN != lsn {
		t.Fatalf("degrade status = %+v", st)
	}
}

func TestWaitAckSemiSyncTimeoutDegrades(t *testing.T) {
	m := newMaster(t)
	m.HeartbeatInterval = 10 * time.Second
	dialSilentSlave(t, m.Addr())
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m.SyncStatus().OnlineSlaves >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := m.SetReplicationMode(ModeSemiSync, 1, 150*time.Millisecond); err != nil {
		t.Fatalf("set semi-sync: %v", err)
	}
	lsn := mustAppend(t, m, "a", "1")
	if err := m.WaitAck(context.Background(), lsn); err != nil {
		t.Fatalf("semi-sync timeout should degrade (commit ok): %v", err)
	}
	st := m.SyncStatus()
	if !st.Degraded || st.DegradedCount != 1 {
		t.Fatalf("timeout degrade status = %+v", st)
	}
}

func TestWaitAckSemiSyncAckN(t *testing.T) {
	m := newMaster(t)
	m.HeartbeatInterval = 10 * time.Second
	f1 := NewFollower(newBtree())
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	go f1.Start(ctx1, m.Addr())
	waitSlave(t, m, true, 0)

	// ackN=2 但只有 1 个在线副本：semi-sync 取 min(2, 1)=1，应立即确认不降级。
	if err := m.SetReplicationMode(ModeSemiSync, 2, time.Second); err != nil {
		t.Fatalf("set semi-sync: %v", err)
	}
	lsn := mustAppend(t, m, "a", "1")
	if err := m.WaitAck(context.Background(), lsn); err != nil {
		t.Fatalf("semi-sync ackN>online should still confirm: %v", err)
	}
	if st := m.SyncStatus(); st.Degraded {
		t.Fatalf("should not degrade when online>=1, status=%+v", st)
	}
	_ = f1
}

func TestWaitAckModeSwitchDuringWait(t *testing.T) {
	m := newMaster(t)
	if err := m.SetReplicationMode(ModeSync, 1, time.Second); err != nil {
		t.Fatalf("set sync: %v", err)
	}
	lsn := mustAppend(t, m, "a", "1")
	// 异步 goroutine 等待（无在线副本会立刻返回 ErrNoSlave，此处测的不是这个）；
	// 切换回 async 后再等待必须立即返回。
	if err := m.SetReplicationMode(ModeAsync, 1, 0); err != nil {
		t.Fatalf("set async: %v", err)
	}
	if err := m.WaitAck(context.Background(), lsn); err != nil {
		t.Fatalf("async after switch: %v", err)
	}
}

// ---- 测试辅助 ----

func mustAppend(t *testing.T, m *MasterReplicator, key, val string) storage.LSN {
	t.Helper()
	before := m.LastLSN()
	if err := m.Append(&BinlogEntry{Batch: &storage.WriteBatch{
		Puts: []storage.KVPair{{Key: []byte(key), Value: []byte(val)}},
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	return before + 1
}

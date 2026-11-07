package sql

// D4.5 Phase8 回归测试：B1 副本补齐的启动期存储就绪栅栏（NEW-2 v2 修复）。
//
// 覆盖点：
//  1. 栅栏默认放行（直接 sql.New 构造的引擎零回归）；
//  2. 启动期（MarkStorageStarting 之后、MarkStorageReady 之前）补齐请求一律
//     挂起，既不执行也不触碰存储；
//  3. 就绪事件（MarkStorageReady）打开栅栏并补发启动期挂起请求一次，
//     保证持久化 ha_quorum_enabled=on 的开关真实生效；
//  4. 关闭后（MarkStorageClosed）永久拒绝，且不产生新的挂起请求；
//  5. 关闭态下直接调用 EnsureRegionReplicas 立即返回，不产生任何快照/扫描。

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
)

// p8HAConfigStub 最小 HAConfigSetter 桩：仅用于放大 quorum 开关，令 b1Enabled()
// 为真，从而真正走到栅栏判定分支。
type p8HAConfigStub struct {
	quorum bool
}

func (s *p8HAConfigStub) SetEpochEnabled(on bool) {}

func (s *p8HAConfigStub) SetQuorumEnabled(on bool) { s.quorum = on }

func (s *p8HAConfigStub) EpochEnabled() bool { return true }

func (s *p8HAConfigStub) QuorumEnabled() bool { return s.quorum }

// TestPhase8B1StorageGateLifecycle 校验栅栏状态机全生命周期。
func TestPhase8B1StorageGateLifecycle(t *testing.T) {
	e := NewExecutor(nil)

	// 1. 默认放行。
	if ready, closed, deferred, fired := e.B1StorageGateState(); !ready || closed || deferred != 0 || fired != 0 {
		t.Fatalf("default gate state = ready:%v closed:%v deferred:%d fired:%d, want true/false/0/0",
			ready, closed, deferred, fired)
	}
	if !e.b1EnsureAllowed() {
		t.Fatalf("default gate must allow ensure (零回归：直接 sql.New 构造需放行)")
	}

	// 2. 启动期：拒绝并登记挂起。
	e.MarkStorageStarting()
	if ready, closed, _, _ := e.B1StorageGateState(); ready || closed {
		t.Fatalf("startup gate state = ready:%v closed:%v, want false/false", ready, closed)
	}
	if e.b1EnsureAllowed() {
		t.Fatalf("startup gate must refuse ensure (存储句柄尚未就绪)")
	}
	if _, _, deferred, _ := e.B1StorageGateState(); deferred != 1 {
		t.Fatalf("startup deferred = %d, want 1 (启动期请求须挂起待补发)", deferred)
	}

	// 3. 就绪事件：放行 + 补发一次。
	e.MarkStorageReady()
	ready, closed, deferred, fired := e.B1StorageGateState()
	if !ready || closed {
		t.Fatalf("ready gate state = ready:%v closed:%v, want true/false", ready, closed)
	}
	if deferred != 0 || fired != 1 {
		t.Fatalf("after ready: deferred=%d fired=%d, want 0/1 (就绪须补发启动期挂起请求)", deferred, fired)
	}
	if !e.b1EnsureAllowed() {
		t.Fatalf("ready gate must allow ensure")
	}

	// 4. 关闭：永久拒绝且不新增挂起。
	e.MarkStorageClosed()
	ready, closed, deferred, fired = e.B1StorageGateState()
	if ready || !closed {
		t.Fatalf("closed gate state = ready:%v closed:%v, want false/true", ready, closed)
	}
	if e.b1EnsureAllowed() {
		t.Fatalf("closed gate must refuse ensure (句柄已释放，禁止快照)")
	}
	if _, _, deferred, fired2 := e.B1StorageGateState(); deferred != 0 || fired2 != fired {
		t.Fatalf("closed gate must not record deferred/fired: deferred=%d fired=%d", deferred, fired2)
	}
	if !e.b1StorageClosed() {
		t.Fatalf("b1StorageClosed() = false after MarkStorageClosed")
	}
	// 幂等：重复关闭不得复活栅栏。
	e.MarkStorageClosed()
	if ready, closed, _, _ := e.B1StorageGateState(); ready || !closed {
		t.Fatalf("repeated close must stay closed: ready:%v closed:%v", ready, closed)
	}
}

// TestPhase8EnsureBlockedBeforeStorageReady 校验 quorum 开启时，存储未就绪 /
// 已关闭两个时点上的补齐调用均"零副作用返回"——这正是 Phase7 复测崩溃栈
// （b1EnsureAsync → EnsureRegionReplicas → holdsRegion → Snapshot(0x0)）的
// 触发前置条件，修复后必须在进入扫描前就被拦下。
func TestPhase8EnsureBlockedBeforeStorageReady(t *testing.T) {
	e := NewExecutor(nil)
	e.SetHAConfigSetter(&p8HAConfigStub{quorum: true})
	e.mgr = cluster.NewManager("solo-node")
	if !e.b1Enabled() {
		t.Fatalf("precondition: b1Enabled() must be true with quorum on")
	}

	// 启动期：补齐请求被挂起，返回 0 且无 error（不是失败，而是尚未就绪）。
	e.MarkStorageStarting()
	n, err := e.EnsureRegionReplicas()
	if err != nil || n != 0 {
		t.Fatalf("startup EnsureRegionReplicas = (%d, %v), want (0, nil)", n, err)
	}
	if _, _, deferred, fired := e.B1StorageGateState(); deferred != 1 || fired != 0 {
		t.Fatalf("startup deferred/fired = %d/%d, want 1/0", deferred, fired)
	}

	// 就绪后：放行（单节点存活数 < 2，仍应安全返回 0）。
	e.MarkStorageReady()
	if n, err := e.EnsureRegionReplicas(); err != nil || n != 0 {
		t.Fatalf("ready EnsureRegionReplicas = (%d, %v), want (0, nil)", n, err)
	}

	// 关闭后：立即返回，不再触碰存储。
	e.MarkStorageClosed()
	if n, err := e.EnsureRegionReplicas(); err != nil || n != 0 {
		t.Fatalf("closed EnsureRegionReplicas = (%d, %v), want (0, nil)", n, err)
	}
	if _, _, deferred, _ := e.B1StorageGateState(); deferred != 0 {
		t.Fatalf("closed gate must not record deferred, got %d", deferred)
	}
}

package cluster

import "testing"

// B2 TSO HA：同 epoch 双主仲裁的单元测试。
//
// 端到端测试（pkg/db/tso_ha_test.go）覆盖"主宕机 → 自动切换 → 无回退"，
// 但两侧几乎同时被授予领导权（同 epoch 双主）属于时序竞态，端到端难以稳定
// 复现，故在状态机层直接验证仲裁规则：
//   - 同 epoch 且双方都是主：节点 ID 字典序小者降级，大者保留；
//   - 更高 epoch 到达：无条件降级并接受 last 提升；
//   - 自身广播：不得自降级（否则新主上位即失效）。
func TestB2TSOFenceSameEpochArbitration(t *testing.T) {
	newLeaderState := func(self string) *tsoHAState {
		return &tsoHAState{selfID: self, epoch: 7, leader: true, last: 100, reserved: 9000}
	}

	t.Run("small id steps down on same epoch", func(t *testing.T) {
		st := newLeaderState("node-a")
		st.fence(7, 100, "node-b")
		if st.leader {
			t.Fatalf("node-a 收到 node-b 同 epoch 广播后应降级，实际仍为主")
		}
		if st.epoch != 7 {
			t.Fatalf("同 epoch 仲裁不应改写 epoch，得到 %d", st.epoch)
		}
	})

	t.Run("large id keeps leadership on same epoch", func(t *testing.T) {
		st := newLeaderState("node-c")
		st.fence(7, 100, "node-a")
		if !st.leader {
			t.Fatalf("node-c 收到 node-a 同 epoch 广播应保留主位，实际被降级")
		}
	})

	t.Run("higher epoch always steps down", func(t *testing.T) {
		st := newLeaderState("node-z")
		st.fence(8, 420, "node-a")
		if st.leader {
			t.Fatalf("更高 epoch 到达后仍为主")
		}
		if st.epoch != 8 {
			t.Fatalf("epoch 应推进到 8，得到 %d", st.epoch)
		}
		if st.last < 420 {
			t.Fatalf("last 应提升到 420 之上，得到 %d", st.last)
		}
	})

	t.Run("self broadcast does not step down", func(t *testing.T) {
		st := newLeaderState("node-b")
		st.fence(7, 100, "node-b")
		if !st.leader {
			t.Fatalf("收到自身广播不应降级")
		}
	})

	t.Run("same epoch last still raised", func(t *testing.T) {
		st := newLeaderState("node-c")
		st.fence(7, 777, "node-a")
		if st.last < 777 {
			t.Fatalf("同 epoch 广播也应提升 last 到 777 之上，得到 %d", st.last)
		}
	})
}

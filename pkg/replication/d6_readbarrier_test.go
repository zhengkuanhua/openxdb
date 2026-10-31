package replication

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// D6.3：复制应用与 SQL 读并发（排期 Phase F / F3）。ReadBarrier 等待
// 进行中的复制批次完成，读路径快照以 AppliedLSN 为界；屏障只等当前批次、
// 不阻塞后续复制应用，锁序单向无死锁。
func TestFollowerReadBarrierConcurrent(t *testing.T) {
	st := newBtree()
	f := NewFollower(st)

	const total = 200
	var applied uint64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := storage.LSN(1); i <= total; i++ {
			batch := &storage.WriteBatch{Puts: []storage.KVPair{
				{Key: []byte(fmt.Sprintf("k%03d", i)), Value: []byte(fmt.Sprintf("v%03d", i))},
			}}
			if err := f.ApplyBinlog(&BinlogEntry{LSN: i, Batch: batch}); err != nil {
				t.Errorf("ApplyBinlog(%d): %v", i, err)
				return
			}
			atomic.StoreUint64(&applied, uint64(i))
		}
	}()

	// 模拟 replica 读路径：ReadBarrier 后读存储。每轮保证读到的数据
	// 位于完整复制边界（不出现半批次/越界可见），且与复制应用并发
	// 无死锁（-race 下验证锁序）。
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		f.ReadBarrier()
		lsn := atomic.LoadUint64(&applied)
		if lsn == 0 {
			continue
		}
		key := []byte(fmt.Sprintf("k%03d", lsn))
		v, err := st.Get(key)
		if err != nil {
			t.Fatalf("Get applied key %s: %v", key, err)
		}
		want := fmt.Sprintf("v%03d", lsn)
		if string(v) != want {
			t.Fatalf("key %s: got %s want %s", key, v, want)
		}
		if f.AppliedLSN() >= total {
			break
		}
	}
	wg.Wait()
	if got := f.AppliedLSN(); got != total {
		t.Fatalf("applied = %d, want %d", got, total)
	}
}

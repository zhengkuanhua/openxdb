// E1b 存储引擎写读基准（B+Tree 对照版，M0-lite 实验）
// 与 E1a(e1_bench.cpp, RocksDB) 同口径：100 万 key / 批量 1000 / 随机种子 42/7
// 运行: go run ./tests/bench/e1_btree_bench.go [keys]
package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

func makeKey(i uint64) []byte { return []byte(fmt.Sprintf("%016x", i)) }

func makeValue(i uint64) []byte {
	return []byte(fmt.Sprintf("value-%016x-%016x", i, i*2654435761))
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

func main() {
	n := uint64(1000000)
	if len(os.Args) > 1 {
		if v, err := strconv.ParseUint(os.Args[1], 10, 64); err == nil {
			n = v
		}
	}

	kv := btree.NewKV(16)

	// 1) 顺序写（批量 1000）
	{
		t0 := time.Now()
		var batch *storage.WriteBatch
		for i := uint64(0); i < n; i++ {
			if batch == nil {
				batch = &storage.WriteBatch{}
			}
			batch.Puts = append(batch.Puts, storage.KVPair{Key: makeKey(i), Value: makeValue(i)})
			if len(batch.Puts) >= 1000 {
				if err := kv.Write(batch); err != nil {
					panic(err)
				}
				batch = nil
			}
		}
		if batch != nil {
			kv.Write(batch)
		}
		d := time.Since(t0)
		fmt.Printf("seq_write: %d keys, %d ms, %.0f TPS\n", n, ms(d), float64(n)*1000.0/float64(ms(d)))
	}

	// 2) 随机写（批量 1000，种子 42 同 E1a）
	{
		rng := newRng(42)
		t0 := time.Now()
		var batch *storage.WriteBatch
		for i := uint64(0); i < n; i++ {
			k := rng() % n
			if batch == nil {
				batch = &storage.WriteBatch{}
			}
			batch.Puts = append(batch.Puts, storage.KVPair{Key: makeKey(k), Value: makeValue(k)})
			if len(batch.Puts) >= 1000 {
				if err := kv.Write(batch); err != nil {
					panic(err)
				}
				batch = nil
			}
		}
		if batch != nil {
			kv.Write(batch)
		}
		d := time.Since(t0)
		fmt.Printf("rand_write: %d keys, %d ms, %.0f TPS\n", n, ms(d), float64(n)*1000.0/float64(ms(d)))
	}

	// 3) 随机点查（记录 P99 延迟，种子 7 同 E1a）
	{
		rng := newRng(7)
		reads := n
		lat := make([]int64, reads)
		t0 := time.Now()
		for i := uint64(0); i < reads; i++ {
			k := rng() % n
			s1 := time.Now()
			if _, err := kv.Get(makeKey(k)); err != nil && err != storage.ErrNotFound {
				panic(err)
			}
			s2 := time.Now()
			lat[i] = s2.Sub(s1).Microseconds()
		}
		d := time.Since(t0)
		sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
		p99 := lat[reads*99/100]
		fmt.Printf("rand_get: %d reads, %d ms, %.0f QPS, P99 %d us\n", reads, ms(d), float64(reads)*1000.0/float64(ms(d)), p99)
	}

	fmt.Println("E1B BENCH DONE")
}

// newRng 返回一个取值为 64 位伪随机数的闭包（线性同余，与 C++ mt19937_64 不完全一致，口径以文档说明为准）。
func newRng(seed uint64) func() uint64 {
	s := seed
	return func() uint64 {
		s = s*6364136223846793005 + 1442695040888963407
		return s
	}
}

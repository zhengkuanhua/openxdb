package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/btree"
)

func key(i int) []byte { return []byte(fmt.Sprintf("%08d", i)) }

type op struct {
	kind int // 0/1 put, 2 del
	k    int
	v    string
}

func main() {
	kv := btree.NewKV(16)
	rng := rand.New(rand.NewSource(42))
	ref := map[int]string{}
	ops := []op{}
	step := 0
	for step < 5000 {
		k := rng.Intn(2000)
		kind := rng.Intn(3)
		o := op{kind: kind, k: k}
		if kind == 2 {
			_ = kv.Write(&storage.WriteBatch{Deletes: [][]byte{key(k)}})
			delete(ref, k)
		} else {
			o.v = fmt.Sprintf("v%d", rng.Intn(1000))
			_ = kv.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: key(k), Value: []byte(o.v)}}})
			ref[k] = o.v
		}
		ops = append(ops, o)
		step++
		if step%50 == 0 {
			pairs, _ := kv.Scan(storage.KeyRange{}, 0)
			seen := map[string]bool{}
			bad := ""
			for i, p := range pairs {
				s := string(p.Key)
				if seen[s] {
					bad = fmt.Sprintf("duplicate at %d: %s", i, s)
					break
				}
				seen[s] = true
				if i > 0 && string(pairs[i-1].Key) >= s {
					bad = fmt.Sprintf("order at %d: %s >= %s", i, pairs[i-1].Key, s)
					break
				}
			}
			missing := 0
			for kk := range ref {
				if !seen[string(key(kk))] {
					missing++
				}
			}
			if bad != "" || missing > 0 || len(pairs) != len(ref) {
				fmt.Printf("BROKEN at step=%d pairs=%d ref=%d missing=%d %s\n", step, len(pairs), len(ref), missing, bad)
				start := step - 40
				if start < 0 {
					start = 0
				}
				for i := start; i < step; i++ {
					o := ops[i]
					kd := "PUT "
					if o.kind == 2 {
						kd = "DEL "
					}
					fmt.Printf("  %4d %s k=%d v=%s\n", i+1, kd, o.k, o.v)
				}
				os.Exit(1)
			}
		}
	}
	fmt.Println("OK all steps")
}

package btree

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestReproLocate 复现随机序列破坏点，dump 破坏后整树与受影响路径。
func TestReproLocate(t *testing.T) {
	tr := New(16)
	rng := rand.New(rand.NewSource(42))
	ops := []dbgOp{}
	for step := 0; step < 5000; step++ {
		k := []byte(fmt.Sprintf("%08d", rng.Intn(2000)))
		op := dbgOp{del: rng.Intn(3) == 2, k: k}
		if op.del {
			tr.del(k)
		} else {
			tr.put(k, []byte("v"))
		}
		ops = append(ops, op)
		if err := tr.Validate(); err != nil {
			fmt.Printf("BROKEN step=%d op=%s %s: %v\n", step+1, map[bool]string{true: "DEL", false: "PUT"}[op.del], op.k, err)
			for i := step - 5; i <= step; i++ {
				o := ops[i]
				kd := "PUT"
				if o.del {
					kd = "DEL"
				}
				fmt.Printf("  %4d %s %s\n", i+1, kd, o.k)
			}
			fmt.Println("--- TREE after broken step ---")
			var out []string
			dumpTree(tr.root, 0, &out)
			for _, l := range out {
				fmt.Println(l)
			}
			fmt.Println("--- PATH of broken key on tree (pre-op impossible; dump get path) ---")
			out = nil
			dumpPath(tr.root, op.k, 0, &out)
			for _, l := range out {
				fmt.Println(l)
			}
			t.FailNow()
		}
	}
}

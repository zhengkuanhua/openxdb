package btree

import (
	"fmt"
	"math/rand"
	"testing"
)

type dbgOp struct {
	del bool
	k   []byte
}

func dumpTree(n *node, depth int, out *[]string) {
	if n == nil {
		return
	}
	ind := ""
	for i := 0; i < depth; i++ {
		ind += "  "
	}
	*out = append(*out, fmt.Sprintf("%s[%s] keys=%v", ind, map[bool]string{true: "L", false: "I"}[n.leaf], n.keys))
	for _, kd := range n.kids {
		dumpTree(kd, depth+1, out)
	}
}

func dumpPath(n *node, key []byte, depth int, out *[]string) {
	ind := ""
	for i := 0; i < depth; i++ {
		ind += "  "
	}
	*out = append(*out, fmt.Sprintf("%s[%s] keys=%v kids=%d", ind, map[bool]string{true: "L", false: "I"}[n.leaf], n.keys, len(n.kids)))
	if n.leaf {
		return
	}
	i := search(n.keys, key)
	sep := "nil"
	if i < len(n.keys) {
		sep = string(n.keys[i])
	}
	*out = append(*out, fmt.Sprintf("%s -> kid %d (keys[%d]=%s)", ind, i, i, sep))
	dumpPath(n.kids[i], key, depth+1, out)
}

// TestDebugLocate 临时调试：逐步执行随机操作并每步 Validate 定位破坏点。
func TestDebugLocate(t *testing.T) {
	tr := New(16)
	rng := rand.New(rand.NewSource(42))
	ops := []dbgOp{}
	var savedRoot *node
	for step := 0; step < 5000; step++ {
		k := []byte(fmt.Sprintf("%08d", rng.Intn(2000)))
		op := dbgOp{del: rng.Intn(3) == 2, k: k}
		if op.del {
			tr.del(k)
		} else {
			tr.put(k, []byte("v"))
		}
		ops = append(ops, op)
		if step == 1678 {
			savedRoot = cloneNode(tr.root)
		}
		if err := tr.Validate(); err != nil {
			fmt.Printf("BROKEN step=%d op=%s %s: %v\n", step+1, map[bool]string{true: "DEL", false: "PUT"}[op.del], op.k, err)
			for i := step - 8; i <= step; i++ {
				o := ops[i]
				kd := "PUT"
				if o.del {
					kd = "DEL"
				}
				fmt.Printf("  %4d %s %s\n", i+1, kd, o.k)
			}
			fmt.Println("--- TREE at step 1678 (saved, before broken step) ---")
			var out []string
			dumpTree(savedRoot, 0, &out)
			for _, l := range out {
				fmt.Println(l)
			}
			fmt.Println("--- PATH of put(00000395) on saved tree ---")
			out = nil
			dumpPath(savedRoot, []byte("00000395"), 0, &out)
			for _, l := range out {
				fmt.Println(l)
			}
			fmt.Println("--- REPLAY put(00000395) on saved tree + Validate ---")
			replay := &Tree{root: cloneNode(savedRoot), order: 16}
			replay.put([]byte("00000395"), []byte("v"))
			if err := replay.Validate(); err != nil {
				fmt.Println("replay ALSO BROKEN:", err)
			} else {
				fmt.Println("replay OK")
			}
			var out2 []string
			dumpTree(replay.root, 0, &out2)
			for _, l := range out2 {
				fmt.Println(l)
			}
			t.FailNow()
		}
	}
}

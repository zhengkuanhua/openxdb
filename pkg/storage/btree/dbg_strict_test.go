package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// maxKeyOf 返回子树最大键（变体 B：最右叶子最后一个键）。
func maxKeyOf(n *node) []byte {
	if n.leaf {
		return n.keys[len(n.keys)-1]
	}
	return maxKeyOf(n.kids[len(n.kids)-1])
}

// validateStrict 在标准 Validate 基础上追加变体 B 检查：
// 内部节点 keys[i] 必须等于 kids[i] 子树的最大键。
func validateStrict(t *Tree) error {
	if t.root == nil {
		return nil
	}
	var walk func(n *node) error
	walk = func(n *node) error {
		if !n.leaf {
			for i := range n.keys {
				mk := maxKeyOf(n.kids[i])
				if !bytes.Equal(n.keys[i], mk) {
					return fmt.Errorf("variant-B: keys[%d]=%s != kids[%d].max=%s", i, n.keys[i], i, mk)
				}
			}
			for _, kd := range n.kids {
				if err := walk(kd); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(t.root)
}

// TestStrictLocate 用增强校验定位最早破坏步骤。
func TestStrictLocate(t *testing.T) {
	tr := New(16)
	rng := rand.New(rand.NewSource(42))
	ops := []dbgOp{}
	for step := 0; step < 5000; step++ {
		k := []byte(fmt.Sprintf("%08d", rng.Intn(2000)))
		op := dbgOp{del: rng.Intn(3) == 2, k: k}
		pre := &Tree{root: cloneNode(tr.root), order: tr.order}
		if op.del {
			tr.del(k)
		} else {
			tr.put(k, []byte("v"))
		}
		ops = append(ops, op)
		if err := validateStrict(tr); err != nil {
			fmt.Printf("STRICT-BROKEN step=%d op=%s %s: %v\n", step+1, map[bool]string{true: "DEL", false: "PUT"}[op.del], op.k, err)
			for i := step - 8; i <= step; i++ {
				o := ops[i]
				kd := "PUT"
				if o.del {
					kd = "DEL"
				}
				fmt.Printf("  %4d %s %s\n", i+1, kd, o.k)
			}
			fmt.Println("=== PRE-OP TREE ===")
			var out []string
			dumpTree(pre.root, 0, &out)
			for _, l := range out {
				fmt.Println(l)
			}
			fmt.Println("=== POST-OP TREE ===")
			out = nil
			dumpTree(tr.root, 0, &out)
			for _, l := range out {
				fmt.Println(l)
			}
			t.FailNow()
		}
	}
}

// Package btree 提供内存 B+Tree 存储实现（E1b 对照实验；M1 Storage 接口备选）。
//
// 设计要点：
//   - 分支因子 order（默认 32）：内部节点最多 order 个孩子、order-1 个键，
//     叶子节点最多 order-1 个键；除根外键数下界 minKeys = order/2。
//   - 插入采用 top-down 分裂（下降路径遇满节点先分裂），无需回溯；
//   - 删除采用 bottom-up 递归（借位/合并），根下溢时直接下沉；
//   - 全节点持 RWMutex，Write 加写锁、Get/Scan/Snapshot 加读锁；
//   - Snapshot 为整树深拷贝（MVP 简化，E1b 基准不涉及快照路径）。
package btree

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// DefaultOrder 默认分支因子。
const DefaultOrder = 32

// node B+Tree 节点：叶子存 keys+vals；内部存 keys+kids（len(kids)=len(keys)+1）。
type node struct {
	leaf bool
	keys [][]byte
	vals [][]byte
	kids []*node
}

// Tree 内存 B+Tree。
type Tree struct {
	order int
	root  *node
}

// New 创建一棵空树。
func New(order int) *Tree {
	if order < 4 {
		order = DefaultOrder
	}
	return &Tree{order: order}
}

// maxKeys 节点键数上限（叶与内部一致：order-1）。
func (t *Tree) maxKeys() int { return t.order - 1 }

// minKeys 非根节点键数下界（B+Tree 标准：ceil(order/2)-1，order 偶数时 = order/2-1）。
func (t *Tree) minKeys() int { return (t.order - 1) / 2 }

// search 返回第一个 >= key 的索引（内部下降用；叶子插入位用）。
func search(keys [][]byte, key []byte) int {
	i := sort.Search(len(keys), func(j int) bool { return bytes.Compare(keys[j], key) >= 0 })
	return i
}

// insertAt / deleteAt 切片辅助。
func insertAt(s [][]byte, i int, v []byte) [][]byte {
	s = append(s, nil)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}

func deleteAt(s [][]byte, i int) [][]byte {
	return append(s[:i], s[i+1:]...)
}

// splitFull 将满节点 child 分裂为 (up, left, right)：
// 叶子：up=keys[mid]（作为分隔键副本保留在左叶子末尾），左右为 [0,mid+1) / [mid+1,)；
// 内部：up=keys[mid] 提到父，左 keys[:mid]+kids[:mid+1]，右 keys[mid+1:]+kids[mid+1:]。
// 注意：左右节点必须深拷贝键值，避免与 child 共享底层数组导致后续写入互相覆盖。
func splitFull(child *node) (up []byte, left, right *node) {
	mid := len(child.keys) / 2
	up = child.keys[mid]
	if child.leaf {
		left = &node{leaf: true, keys: cloneSlice(child.keys[:mid+1]), vals: cloneSlice(child.vals[:mid+1])}
		right = &node{leaf: true, keys: cloneSlice(child.keys[mid+1:]), vals: cloneSlice(child.vals[mid+1:])}
	} else {
		left = &node{leaf: false, keys: cloneSlice(child.keys[:mid]), kids: cloneKids(child.kids[:mid+1])}
		right = &node{leaf: false, keys: cloneSlice(child.keys[mid+1:]), kids: cloneKids(child.kids[mid+1:])}
	}
	return up, left, right
}

func cloneSlice(s [][]byte) [][]byte {
	out := make([][]byte, len(s))
	copy(out, s)
	return out
}

func cloneKids(s []*node) []*node {
	out := make([]*node, len(s))
	copy(out, s)
	return out
}

// put 插入或更新键值对（键不可变语义：等值覆盖）。
func (t *Tree) put(key, val []byte) {
	if t.root == nil {
		t.root = &node{leaf: true}
	}
	if len(t.root.keys) >= t.maxKeys() {
		up, l, r := splitFull(t.root)
		t.root = &node{leaf: false, keys: [][]byte{up}, kids: []*node{l, r}}
	}
	t.insert(t.root, key, val)
}

// insert 递归下降插入；调用前保证沿途节点未满。
func (t *Tree) insert(n *node, key, val []byte) {
	if n.leaf {
		i := search(n.keys, key)
		if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
			n.vals[i] = val
			return
		}
		n.keys = insertAt(n.keys, i, key)
		n.vals = insertAt(n.vals, i, val)
		return
	}
	i := search(n.keys, key)
	if len(n.kids[i].keys) >= t.maxKeys() {
		up, l, r := splitFull(n.kids[i])
		pos := search(n.keys, up)
		n.keys = insertAt(n.keys, pos, up)
		n.kids[pos] = l
		n.kids = append(n.kids, nil)
		copy(n.kids[pos+2:], n.kids[pos+1:])
		n.kids[pos+1] = r
		// 分裂后 up 副本位于左孩子 l 末尾：key==up 必须留在 l（走覆盖），
		// 只有 key>up 才下降右孩子 r。
		if bytes.Compare(key, up) > 0 {
			i++
		}
	}
	t.insert(n.kids[i], key, val)
}

// get 查询键值；不存在返回 false。
func (t *Tree) get(key []byte) ([]byte, bool) {
	if t.root == nil {
		return nil, false
	}
	n := t.root
	for {
		i := search(n.keys, key)
		if n.leaf {
			if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
				return n.vals[i], true
			}
			return nil, false
		}
		n = n.kids[i]
	}
}

// del 删除键；返回是否删除。
func (t *Tree) del(key []byte) bool {
	if t.root == nil {
		return false
	}
	ok := t.remove(t.root, key)
	if !t.root.leaf && len(t.root.keys) == 0 {
		t.root = t.root.kids[0]
	}
	return ok
}

// Validate 自检树结构不变量（调试与健壮性用途）：
//   - 各节点键严格升序；
//   - 内部节点分隔键与子树键序一致（左子树所有键 < keys[i]，右子树所有键 >= keys[i]；
//     叶子含键语义下分隔键 = 左子树最大键，故左子树键 <= keys[i]）；
//   - 全树叶子遍历键全局唯一。
func (t *Tree) Validate() error {
	if t.root == nil {
		return nil
	}
	var last []byte
	first := true
	var walk func(n *node, lo, hi []byte) error
	walk = func(n *node, lo, hi []byte) error {
		for i, k := range n.keys {
			if i > 0 && bytes.Compare(n.keys[i-1], k) >= 0 {
				return fmt.Errorf("node keys not strictly ordered at %d", i)
			}
			if lo != nil && bytes.Compare(k, lo) < 0 {
				return fmt.Errorf("key %s below lo %s", k, lo)
			}
			if hi != nil && bytes.Compare(k, hi) > 0 {
				return fmt.Errorf("key %s above hi %s", k, hi)
			}
		}
		if !n.leaf {
			if len(n.kids) != len(n.keys)+1 {
				return fmt.Errorf("internal node kids=%d keys=%d", len(n.kids), len(n.keys))
			}
			for i, kd := range n.kids {
				klo, khi := lo, hi
				if i > 0 {
					klo = n.keys[i-1]
				}
				if i < len(n.keys) {
					khi = n.keys[i]
				}
				if err := walk(kd, klo, khi); err != nil {
					return err
				}
			}
		} else {
			for _, k := range n.keys {
				if !first && bytes.Compare(last, k) >= 0 {
					return fmt.Errorf("global duplicate/order: %s after %s", k, last)
				}
				last, first = k, false
			}
		}
		return nil
	}
	if err := walk(t.root, nil, nil); err != nil {
		return err
	}
	return nil
}

// remove 从子树删除键，返回是否找到。调用后子树可能下溢（由上层修复）。
func (t *Tree) remove(n *node, key []byte) bool {
	if n.leaf {
		i := search(n.keys, key)
		if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
			n.keys = deleteAt(n.keys, i)
			n.vals = deleteAt(n.vals, i)
			return true
		}
		return false
	}
	i := search(n.keys, key)
	ok := t.remove(n.kids[i], key)
	// 变体 B：删除键后先同步分隔键——若 key 是 kids[i] 的旧最大键（即 n.keys[i]），
	// 删除后 kids[i] 最大键变小，须先把 n.keys[i] 更新为 kids[i] 的新最大键，
	// 再交由下方 rebalance 基于正确分隔键做借位/合并。
	// 注意：i == len(n.keys)（最右子树）没有对应分隔键，其最大键存于上层父分隔键，
	// 交由上层递归按同一规则传播更新。
	if ok && i < len(n.keys) && bytes.Equal(n.keys[i], key) && len(n.kids[i].keys) > 0 {
		n.keys[i] = maxKey(n.kids[i])
	}
	if len(n.kids[i].keys) < t.minKeys() {
		t.rebalance(n, i)
	}
	return ok
}

// maxKey 返回子树的最大键（变体 B 语义：沿最右子树链下探到叶子末键）。
func maxKey(n *node) []byte {
	for !n.leaf {
		n = n.kids[len(n.kids)-1]
	}
	return n.keys[len(n.keys)-1]
}

// rebalance 修复 n.kids[i] 下溢：先借左/右兄弟，否则合并。
func (t *Tree) rebalance(n *node, i int) {
	if i > 0 && len(n.kids[i-1].keys) > t.minKeys() {
		t.borrowLeft(n, i)
		return
	}
	if i+1 < len(n.kids) && len(n.kids[i+1].keys) > t.minKeys() {
		t.borrowRight(n, i)
		return
	}
	if i > 0 {
		t.merge(n, i-1, i)
	} else {
		t.merge(n, i, i+1)
	}
}

// borrowLeft 从 n.kids[i-1] 借一个键给 n.kids[i]。
// 不变式（变体 B）：分隔键 = 左子树最大键（副本保留在左子树）。
func (t *Tree) borrowLeft(n *node, i int) {
	c, left := n.kids[i], n.kids[i-1]
	li := len(left.keys) - 1
	if c.leaf {
		// 叶子：c 前插 left 最大键；分隔键更新为 left 新最大键。
		c.keys = insertAt(c.keys, 0, left.keys[li])
		c.vals = insertAt(c.vals, 0, left.vals[li])
		left.keys = deleteAt(left.keys, li)
		left.vals = deleteAt(left.vals, li)
		n.keys[i-1] = left.keys[li-1]
	} else {
		// 内部：借 left 最右子树 kids[li+1]（其最大键 = 父键 n.keys[i-1]），
		// c 前插父键作为新 keys[0]；left 删最右子树与旧最大键 keys[li]。
		// 删除后 left 新最大键 = 旧 keys[li]（变体 B 关系 keys[li] == kids[li].max），
		// 因此分隔键应取旧 keys[li]，而非删除后的 keys[li-1]。
		newSep := left.keys[li]
		c.kids = append([]*node{left.kids[li+1]}, c.kids...)
		c.keys = insertAt(c.keys, 0, n.keys[i-1])
		left.kids = left.kids[:li+1]
		left.keys = deleteAt(left.keys, li)
		n.keys[i-1] = newSep
	}
}

// borrowRight 从 n.kids[i+1] 借一个键给 n.kids[i]。
// 不变式（变体 B）：分隔键 = 左子树最大键（副本保留在左子树）。
func (t *Tree) borrowRight(n *node, i int) {
	c, right := n.kids[i], n.kids[i+1]
	if c.leaf {
		// 叶子：c 追加 right 最小键；分隔键更新为被借键（= c 新最大键）。
		borrowed := right.keys[0]
		c.keys = append(c.keys, borrowed)
		c.vals = append(c.vals, right.vals[0])
		right.keys = deleteAt(right.keys, 0)
		right.vals = deleteAt(right.vals, 0)
		n.keys[i] = borrowed
	} else {
		// 内部：借 right 最左子树 kids[0]（其最大键 = right.keys[0]），
		// c 追加被借键；分隔键更新为被借键（= c 新最大键）。
		borrowed := right.keys[0]
		c.keys = append(c.keys, borrowed)
		c.kids = append(c.kids, right.kids[0])
		right.kids = deleteKidsAt(right.kids, 0)
		right.keys = deleteAt(right.keys, 0)
		n.keys[i] = borrowed
	}
}

// merge 合并 n.kids[a] 与 n.kids[b]（a<b，分隔键 n.keys[a]），删除后者。
func (t *Tree) merge(n *node, a, b int) {
	l, r := n.kids[a], n.kids[b]
	if l.leaf {
		l.keys = append(l.keys, r.keys...)
		l.vals = append(l.vals, r.vals...)
	} else {
		// 变体 B：l.keys 末尾是 kids[l_last-1].max，不含 l 整体最大键；
		// l 整体最大键 = n.keys[a]（父分隔键副本，此时已由 remove 更新为
		// kids[a] 的最新最大键），须插在 l 与 r 之间。
		l.keys = append(l.keys, n.keys[a])
		l.keys = append(l.keys, r.keys...)
		l.kids = append(l.kids, r.kids...)
	}
	if b < len(n.keys) {
		// 合并节点最大键 = 原 kids[b] 最大键 = n.keys[b]：将其设为位置 a 的分隔键，
		// 再删除位置 b 的旧键。
		n.keys[a] = n.keys[b]
		n.keys = deleteAt(n.keys, b)
	} else {
		// b 是最右子树（无对应分隔键）：合并节点最大键存于上层父分隔键，直接删旧分隔键。
		n.keys = deleteAt(n.keys, a)
	}
	n.kids = append(n.kids[:b], n.kids[b+1:]...)
}

func deleteKidsAt(s []*node, i int) []*node {
	return append(s[:i], s[i+1:]...)
}

// collect 中序遍历收集 [start,end) 区间键值对；limit<=0 不限制。
func (t *Tree) collect(start, end []byte, limit int) []storage.KVPair {
	var out []storage.KVPair
	if t.root == nil {
		return out
	}
	seen := 0
	walk := func(n *node) bool {
		if !n.leaf {
			return true
		}
		for i, k := range n.keys {
			if start != nil && bytes.Compare(k, start) < 0 {
				continue
			}
			if end != nil && bytes.Compare(k, end) >= 0 {
				return false
			}
			out = append(out, storage.KVPair{Key: k, Value: n.vals[i]})
			seen++
			if limit > 0 && seen >= limit {
				return false
			}
		}
		return true
	}
	// 从根下降定位到 start 所在叶子，再顺序遍历（利用叶子链表可避免扫描左子树；
	// MVP 直接全树中序，深度有限、基准读场景无碍）。
	var rec func(n *node) bool
	rec = func(n *node) bool {
		if !n.leaf {
			for _, kd := range n.kids {
				if !rec(kd) {
					return false
				}
			}
			return true
		}
		return walk(n)
	}
	rec(t.root)
	return out
}

// ---- Storage 接口实现 ----

// KV 实现 storage.Storage 的内存 B+Tree 存储。
// 线程安全：内部 RWMutex 保护。
type KV struct {
	tree *Tree
	mu   sync.RWMutex
}

func NewKV(order int) *KV { return &KV{tree: New(order)} }

// Get 实现 storage.Storage。
func (kv *KV) Get(key []byte) ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.tree.get(key)
	if !ok {
		return nil, storage.ErrNotFound
	}
	return v, nil
}

// Scan 实现 storage.Storage。
func (kv *KV) Scan(r storage.KeyRange, limit int) ([]storage.KVPair, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.tree.collect(r.Start, r.End, limit), nil
}

// Write 实现 storage.Storage（原子应用 WriteBatch）。
func (kv *KV) Write(batch *storage.WriteBatch) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, p := range batch.Puts {
		kv.tree.put(p.Key, p.Value)
	}
	for _, k := range batch.Deletes {
		kv.tree.del(k)
	}
	return nil
}

// Snapshot 实现 storage.Storage（整树深拷贝，MVP 简化）。
func (kv *KV) Snapshot() (storage.Snapshot, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return &snap{root: cloneNode(kv.tree.root)}, nil
}

// Close 实现 storage.Storage（内存实现无需释放）。
func (kv *KV) Close() error { return nil }

// snap 一致性快照：独立深拷贝树，与原树互不影响。
type snap struct {
	root *node
}

func cloneNode(n *node) *node {
	if n == nil {
		return nil
	}
	c := &node{leaf: n.leaf}
	c.keys = append([][]byte(nil), n.keys...)
	c.vals = append([][]byte(nil), n.vals...)
	for _, k := range n.kids {
		c.kids = append(c.kids, cloneNode(k))
	}
	return c
}

func (s *snap) Get(key []byte) ([]byte, error) {
	if s.root == nil {
		return nil, storage.ErrNotFound
	}
	n := s.root
	for {
		i := search(n.keys, key)
		if n.leaf {
			if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
				return n.vals[i], nil
			}
			return nil, storage.ErrNotFound
		}
		n = n.kids[i]
	}
}

func (s *snap) Scan(r storage.KeyRange, limit int) ([]storage.KVPair, error) {
	var out []storage.KVPair
	seen := 0
	var rec func(n *node) bool
	rec = func(n *node) bool {
		if n == nil {
			return true
		}
		if !n.leaf {
			for _, k := range n.kids {
				if !rec(k) {
					return false
				}
			}
			return true
		}
		for i, k := range n.keys {
			if r.Start != nil && bytes.Compare(k, r.Start) < 0 {
				continue
			}
			if r.End != nil && bytes.Compare(k, r.End) >= 0 {
				return false
			}
			out = append(out, storage.KVPair{Key: k, Value: n.vals[i]})
			seen++
			if limit > 0 && seen >= limit {
				return false
			}
		}
		return true
	}
	rec(s.root)
	return out, nil
}

func (s *snap) Release() {}

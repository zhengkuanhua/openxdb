package sql

import (
	"strings"
	"sync"
)

// M9 子功能 2：查询缓存（pkg/sql 新文件）。
//
// 以规范化 SQL 文本为 key 缓存 SELECT 结果（列 + 行），autocommit 下
// 命中直接返回副本（快照隔离语义：显式事务内不参与缓存）；写语句
// （INSERT/UPDATE/DELETE 与 DDL）执行成功后全量失效，保证缓存与存储
// 一致（安全优先，不做表级细粒度跟踪）。开关通过
// `SET query_cache = on|off` 控制，统计通过 SHOW STATS 输出。

// queryCacheEntry 缓存条目：规范化 SQL → 查询结果（列 + 行）。
type queryCacheEntry struct {
	cols []string
	rows [][]Value
}

// QueryCache 查询结果缓存（线程安全）。
type QueryCache struct {
	mu      sync.Mutex
	enabled bool
	entries map[string]*queryCacheEntry
	hits    int64
	misses  int64
	invals  int64
}

// NewQueryCache 创建默认关闭的查询缓存。
func NewQueryCache() *QueryCache {
	return &QueryCache{entries: make(map[string]*queryCacheEntry)}
}

// SetEnabled 开关缓存；关闭时清空条目（避免陈旧结果残留）。
func (q *QueryCache) SetEnabled(on bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enabled = on
	if !on {
		q.entries = make(map[string]*queryCacheEntry)
	}
}

// Enabled 查询缓存是否开启。
func (q *QueryCache) Enabled() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.enabled
}

// Get 命中返回结果的深拷贝（防调用方修改污染缓存）；未命中计 miss。
func (q *QueryCache) Get(key string) (*Result, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.enabled {
		return nil, false
	}
	ent, ok := q.entries[key]
	if !ok {
		q.misses++
		return nil, false
	}
	q.hits++
	rows := make([][]Value, len(ent.rows))
	for i, r := range ent.rows {
		rows[i] = append([]Value(nil), r...)
	}
	return &Result{Columns: append([]string(nil), ent.cols...), Rows: rows}, true
}

// Put 缓存一个 SELECT 结果（仅缓存带结果集列的结果，DML 结果不缓存）。
func (q *QueryCache) Put(key string, res *Result) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.enabled || len(res.Columns) == 0 {
		return
	}
	rows := make([][]Value, len(res.Rows))
	for i, r := range res.Rows {
		rows[i] = append([]Value(nil), r...)
	}
	q.entries[key] = &queryCacheEntry{
		cols: append([]string(nil), res.Columns...),
		rows: rows,
	}
}

// Invalidate 写操作后全量失效缓存（计数 + 清空）。
func (q *QueryCache) Invalidate() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.enabled {
		return
	}
	q.invals++
	q.entries = make(map[string]*queryCacheEntry)
}

// Stats 缓存运行状态：开关 + 命中/未命中/失效次数。
func (q *QueryCache) Stats() (enabled bool, hits, misses, invals int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.enabled, q.hits, q.misses, q.invals
}

// NormalizeSQL 规范化 SQL 文本作为缓存 key：去首尾空白与尾分号、
// 统一小写、折叠连续空白。同语义不同写法的查询共享同一缓存条目。
func NormalizeSQL(sql string) string {
	s := strings.TrimSpace(sql)
	s = strings.TrimSuffix(s, ";")
	var b strings.Builder
	inSpace := false
	for _, ch := range s {
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			if !inSpace && b.Len() > 0 {
				b.WriteByte(' ')
				inSpace = true
			}
			continue
		}
		b.WriteRune(ch)
		inSpace = false
	}
	return strings.ToLower(b.String())
}

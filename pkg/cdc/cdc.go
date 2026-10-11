// Package cdc 变更数据捕获（Change Data Capture，M14 子功能二）：
// 把主节点 binlog 暴露为可订阅的增量变更流，供外部数据管道 / 增量同步消费，
// 与 M14 子功能一 mysqldump 兼容导入形成迁移闭环（导入后可增量订阅后续变更）。
//
// 设计要点：
//   - 拉取模式：Subscriber.Fetch(from, limit) 按 LSN 增量读取 binlog 事件，
//     返回下一消费位点 nextLSN；客户端把 nextLSN 持久化到 OffsetStore，
//     重启后从该位点续读（LSN 单调递增，重连续读无重复无遗漏）。
//   - 事件模型：把 binlog 物理键解码为表/行级变更（TableID / PK / Kind），
//     RawValue 保持 encodeRow 原始 JSON 字节，由上层结合表元数据解码列值；
//     元数据键（m:...）不产出事件（非用户数据）。
//   - 并发读保护：通过 replication.BinlogReader 独立只读句柄 + 尾部容忍
//     扫描，不持有 binlog 写路径锁，与主写 / 复制推送并发共存。
package cdc

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ChangeKind 变更类型。
type ChangeKind uint8

const (
	// KindPut Upsert（写入/更新；binlog 不区分 insert 与 update）。
	KindPut ChangeKind = iota
	// KindDelete 删除。
	KindDelete
)

func (k ChangeKind) String() string {
	if k == KindDelete {
		return "DELETE"
	}
	return "PUT"
}

// Event 一条表/行级变更事件。
type Event struct {
	LSN       storage.LSN // binlog 位点（幂等 / 续读基准）
	CommitLSN storage.LSN // 源 WAL 提交点
	SchemaVer uint64
	Kind      ChangeKind
	TableID   uint64 // 表 ID（行键 / 索引键解码）
	PK        []byte // 主键字节（索引键为 'p' 分隔符后的主键后缀）
	IsIndex   bool   // 二级索引键变更（跟随行变更，消费方通常忽略）
	RawKey    []byte // 物理键（含 region 前缀）
	RawValue  []byte // Put 行值（encodeRow JSON）；Delete 为空
}

// 逻辑键类型（见 pkg/sql/meta.go：行键 's'、索引键 'i'；元数据键 'm'）。
const (
	logicKeyRow   = 's'
	logicKeyIndex = 'i'
)

// errStop 内部哨兵：Fetch 达到 limit 时中止扫描（正常返回）。
// 复用 replication.ErrStopFetch 使 BinlogReader.ScanFrom 识别为正常停止。
var errStop = replication.ErrStopFetch

// Decode 把一条 binlog 记录解码为变更事件列表。
// 元数据键（m:...）与无法识别的键不产出事件；顺序保持 binlog 内 put/delete 顺序。
func Decode(entry *replication.BinlogEntry) []Event {
	if entry == nil || entry.Batch == nil {
		return nil
	}
	var evs []Event
	for _, p := range entry.Batch.Puts {
		ev := decodeKey(p.Key, KindPut)
		if ev == nil {
			continue
		}
		ev.LSN, ev.CommitLSN, ev.SchemaVer = entry.LSN, entry.CommitLSN, entry.SchemaVer
		ev.RawKey, ev.RawValue = p.Key, p.Value
		evs = append(evs, *ev)
	}
	for _, k := range entry.Batch.Deletes {
		ev := decodeKey(k, KindDelete)
		if ev == nil {
			continue
		}
		ev.LSN, ev.CommitLSN, ev.SchemaVer = entry.LSN, entry.CommitLSN, entry.SchemaVer
		ev.RawKey = k
		evs = append(evs, *ev)
	}
	return evs
}

// decodeKey 解析物理键为表/行变更；返回 nil 表示元数据键或无法识别（跳过）。
func decodeKey(key []byte, kind ChangeKind) *Event {
	if len(key) == 0 {
		return nil
	}
	inner := key
	if key[0] == 'r' {
		if len(key) < sharding.RegionKeyPrefixLen {
			return nil
		}
		inner = key[sharding.RegionKeyPrefixLen:]
	}
	if len(inner) == 0 {
		return nil
	}
	switch inner[0] {
	case logicKeyRow:
		// 's'{tableID:8B}{pk}
		if len(inner) < 1+8 {
			return nil
		}
		return &Event{Kind: kind, TableID: binary.BigEndian.Uint64(inner[1:9]), PK: inner[9:]}
	case logicKeyIndex:
		// 'i'{tableID:8B}{indexID:8B}{idxVal}p{pk}
		if len(inner) < 1+8 {
			return nil
		}
		ev := &Event{Kind: kind, TableID: binary.BigEndian.Uint64(inner[1:9]), IsIndex: true}
		if i := bytes.LastIndexByte(inner, 'p'); i >= 0 && i+1 < len(inner) {
			ev.PK = inner[i+1:]
		}
		return ev
	default:
		// 'm' 元数据键及其它：非用户数据，跳过。
		return nil
	}
}

// Subscriber binlog 订阅会话（拉取模式）。
// 会话持有一个独立只读句柄；Fetch 内部加锁，支持单会话顺序拉取。
// Close 后再次 Fetch 返回 replication.ErrClosed。
type Subscriber struct {
	mu     sync.Mutex
	reader *replication.BinlogReader
	closed bool
}

// NewSubscriber 打开 binlog 只读订阅会话。
// 文件不存在时返回 os.ErrNotExist 原样错误（不创建）。
func NewSubscriber(path string) (*Subscriber, error) {
	r, err := replication.OpenBinlogRead(path)
	if err != nil {
		return nil, err
	}
	return &Subscriber{reader: r}, nil
}

// Fetch 从 from（含）开始增量读取至多 limit 条事件（limit<=0 表示一次读完
// 当前文件全部），返回事件列表与下一消费位点 nextLSN：
//   - 达到 limit：nextLSN = 最后返回事件的 LSN + 1；
//   - 未达 limit（读到文件尾）：nextLSN = 文件尾位点 + 1（无新事件时保持
//     max(next, from)，即 from 超前于文件时不倒退）；
//
// 客户端应把 nextLSN 保存到 OffsetStore，下次以该值续读；LSN 单调递增，
// 重连续读不会重复也不会遗漏。
func (s *Subscriber) Fetch(from storage.LSN, limit int) ([]*Event, storage.LSN, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, replication.ErrClosed
	}
	var events []*Event
	lastCollected := from - 1 // 无事件时表示 from 之前均已消费
	tail, err := s.reader.ScanFrom(from, func(e *replication.BinlogEntry) error {
		for _, ev := range Decode(e) {
			events = append(events, &ev)
			lastCollected = e.LSN
		}
		if limit > 0 && len(events) >= limit {
			return errStop
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if limit > 0 && len(events) >= limit {
		return events, lastCollected + 1, nil
	}
	next := tail + 1
	if next < from {
		next = from
	}
	return events, next, nil
}

// Close 关闭订阅会话。
func (s *Subscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.reader.Close()
}

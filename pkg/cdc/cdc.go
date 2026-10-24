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
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ErrTimeout 阻塞订阅超时哨兵：Blocking=true 且 BlockTimeout>0 时，
// 在等待期内无新事件到达，返回 ErrTimeout（此前已收到的事件仍随返回值带出）。
var ErrTimeout = errors.New("cdc: blocking fetch timeout")

// pollInterval 阻塞订阅的轮询间隔（binlog 为 append-only 落盘文件，反复扫描尾部）。
const pollInterval = 50 * time.Millisecond

// EventFormat 控制事件负载格式（P2-14 binlog 全选项）。
type EventFormat int

const (
	// FormatFullRow 完整行负载：保留 RawValue（encodeRow JSON）。
	FormatFullRow EventFormat = iota
	// FormatPKOnly 仅主键负载：RawValue 置空，消费方只取 Kind/PK（节省带宽）。
	FormatPKOnly
)

// KindFilter 变更类型过滤（P2-14）。
// 零值（两者都 false）表示不过滤（等价 nil）。
type KindFilter struct {
	Put    bool // 收 PUT 事件
	Delete bool // 收 DELETE 事件
}

func (f *KindFilter) match(k ChangeKind) bool {
	if f == nil {
		return true
	}
	switch k {
	case KindPut:
		return f.Put
	case KindDelete:
		return f.Delete
	}
	return false
}

// TableFilter 按表名/库名过滤事件（P2-14）。
// Entries 任一规则命中即通过；空 Entries 表示不过滤。
// 规则支持：
//   - "tbl"：精确表名（匹配 resolve 结果 "db.tbl" 的后半段或裸表名）；
//   - "db.tbl"：精确 库.表；
//   - "db.*"：库下全部表；
//   - "*" 或 "*.*"：全部表；
//   - 十进制数字：按表 ID 精确匹配（未配置 resolver 时也可用）。
type TableFilter struct {
	Entries []string
}

func (f *TableFilter) match(id uint64, resolve func(uint64) (string, bool)) bool {
	if f == nil || len(f.Entries) == 0 {
		return true
	}
	var name string
	if resolve != nil {
		if n, ok := resolve(id); ok {
			name = n
		}
	}
	for _, e := range f.Entries {
		if e == "*" || e == "*.*" {
			return true
		}
		if n, err := strconv.ParseUint(e, 10, 64); err == nil && n == id {
			return true
		}
		if name == "" {
			continue
		}
		if matchTableRule(e, name) {
			return true
		}
	}
	return false
}

// matchTableRule 判断规则是否命中全名（"db.tbl" 或 "tbl"）。
func matchTableRule(rule, name string) bool {
	if rule == name {
		return true
	}
	if strings.HasSuffix(rule, ".*") {
		// "db.*"：库前缀 + 任意表
		return strings.HasPrefix(name, strings.TrimSuffix(rule, "*"))
	}
	if !strings.Contains(rule, ".") {
		// 裸表名规则：匹配 "db.tbl" 的后半段
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			return name[i+1:] == rule
		}
	}
	return false
}

// Filter 订阅过滤与行为选项（P2-14 binlog 全选项）。
// 各字段均为可选项；零值 = 不施加对应限制或使用 Fetch 的默认参数。
type Filter struct {
	// TableFilter 表/库过滤；nil = 全部。
	TableFilter *TableFilter
	// KindFilter 变更类型过滤；nil = Put+Delete 都收。
	KindFilter *KindFilter
	// FromPosition 起始位点；>0 时覆盖 Fetch 的 from 参数。
	FromPosition storage.LSN
	// MaxEvents 返回事件上限；>0 时覆盖 Fetch 的 limit 参数（取两者更小值）。
	MaxEvents int
	// Blocking 阻塞订阅：无新事件时轮询等待而非立即返回。
	Blocking bool
	// BlockTimeout 阻塞等待总超时（毫秒）；<=0 表示无限等待。
	// 超时且无事件返回 ErrTimeout；超时前已有事件则正常返回（含已收事件）。
	BlockTimeout int64
	// EventFormat 事件负载格式（默认 FormatFullRow）。
	EventFormat EventFormat
}

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
	mu      sync.Mutex
	reader  *replication.BinlogReader
	closed  bool
	resolve func(uint64) (string, bool) // 表 ID → "db.tbl"（TableFilter 按名过滤用）
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

// SetTableResolver 设置表 ID → "库.表" 名解析回调（TableFilter 按表名/库名过滤时需要）。
// 回调返回 ok=false 表示未知表（此时按名规则不命中，仅 "*"/数字 ID 规则可命中）。
func (s *Subscriber) SetTableResolver(fn func(uint64) (string, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolve = fn
}

// Fetch 从 from（含）开始增量读取至多 limit 条事件（limit<=0 表示一次读完
// 当前文件全部），返回事件列表与下一消费位点 nextLSN：
//   - 达到 limit：nextLSN = 最后返回事件的 LSN + 1；
//   - 未达 limit（读到文件尾）：nextLSN = 文件尾位点 + 1（无新事件时保持
//     max(next, from)，即 from 超前于文件时不倒退）；
//
// 客户端应把 nextLSN 保存到 OffsetStore，下次以该值续读；LSN 单调递增，
// 重连续读不会重复也不会遗漏。
// 等价 FetchFiltered(from, limit, Filter{})：不带过滤与阻塞行为。
func (s *Subscriber) Fetch(from storage.LSN, limit int) ([]*Event, storage.LSN, error) {
	return s.FetchFiltered(from, limit, Filter{})
}

// FetchFiltered 同 Fetch，但应用 P2-14 全选项（表过滤 / 类型过滤 / 起始位点 /
// 上限 / 阻塞订阅 / 负载格式）。Filter 内 FromPosition/MaxEvents 为 0 时
// 回退使用 from/limit 参数；两者均非 0 时取更小更严的值。
func (s *Subscriber) FetchFiltered(from storage.LSN, limit int, f Filter) ([]*Event, storage.LSN, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, replication.ErrClosed
	}
	scanFrom := from
	if f.FromPosition > 0 {
		scanFrom = f.FromPosition
	}
	max := limit
	if f.MaxEvents > 0 {
		if max <= 0 || f.MaxEvents < max {
			max = f.MaxEvents
		}
	}
	var events []*Event
	lastCollected := scanFrom - 1 // 无事件时表示 scanFrom 之前均已消费
	var deadline time.Time
	if f.Blocking && f.BlockTimeout > 0 {
		deadline = time.Now().Add(time.Duration(f.BlockTimeout) * time.Millisecond)
	}
	for {
		tail, err := s.reader.ScanFrom(scanFrom, func(e *replication.BinlogEntry) error {
			for _, ev := range Decode(e) {
				if !f.TableFilter.match(ev.TableID, s.resolve) {
					continue
				}
				if !f.KindFilter.match(ev.Kind) {
					continue
				}
				if f.EventFormat == FormatPKOnly {
					ev.RawValue = nil
				}
				events = append(events, &ev)
				lastCollected = e.LSN
			}
			if max > 0 && len(events) >= max {
				return errStop
			}
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
		if max > 0 && len(events) >= max {
			return events, lastCollected + 1, nil
		}
		if !f.Blocking {
			next := tail + 1
			if next < scanFrom {
				next = scanFrom
			}
			return events, next, nil
		}
		// 阻塞订阅：未达上限且无更多新事件，轮询等待。
		if !deadline.IsZero() && time.Now().After(deadline) {
			next := tail + 1
			if next < scanFrom {
				next = scanFrom
			}
			if len(events) > 0 {
				// 超时前已收到部分事件：正常返回（不报超时）
				return events, next, nil
			}
			return events, next, ErrTimeout
		}
		time.Sleep(pollInterval)
	}
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

// E4 稳定性/持久化压测负载程序（第 2 批 E4）。
//
// 连续运行 sysbench/TPC-C 风格 OLTP 混合负载（默认 8:2、8 连接、每事务 4 操作），
// 每 60s 输出一个区间的 TPS / P50 / P95 / P99 / 错误数，用于观察长时间运行
// 是否存在性能劣化（TPS 下行 / 延迟上行 / 错误数增长）。
// 运行结束后打印累计插入行数，供 check 模式（-expect）校验持久化完整性。
//
// 用法（仓库根目录，需 RocksDB CGO 环境）：
//
//	go run ./tests/bench/stability -mode load -dir <data_dir> -minutes 6 [-conns 8] [-rows 100000] [-ratio 8:2]
//	go run ./tests/bench/stability -mode check -dir <data_dir> -expect <期望行数>
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

var (
	mode    = flag.String("mode", "load", "load=压测负载 | check=重启校验")
	minutes = flag.Int("minutes", 6, "load 模式：压测时长（分钟，每 60s 一个区间）")
	conns   = flag.Int("conns", 8, "load 模式：并发连接数")
	rows    = flag.Int("rows", 100000, "load 模式：预装载行数")
	ratio   = flag.String("ratio", "8:2", "load 模式：读写比 8:2 或 5:5")
	batch   = 1000
)

type segStats struct {
	mu      sync.Mutex
	txns    int
	errs    int
	reads   int64
	inserts int64
	updates int64
	lats    []int64
}

func (s *segStats) addErr()    { s.mu.Lock(); s.errs++; s.mu.Unlock() }
func (s *segStats) addRead()   { atomic.AddInt64(&s.reads, 1) }
func (s *segStats) addInsert() { atomic.AddInt64(&s.inserts, 1) }
func (s *segStats) addUpdate() { atomic.AddInt64(&s.updates, 1) }
func (s *segStats) addTxn(d time.Duration) {
	s.mu.Lock()
	s.txns++
	s.lats = append(s.lats, d.Microseconds())
	s.mu.Unlock()
}

var (
	segMu    sync.Mutex
	segs     []*segStats // 每 60s 一个区间
	inserted int64       // 全局已插入操作数（跨区间累计）
)

func segAt(idx int64) *segStats {
	segMu.Lock()
	defer segMu.Unlock()
	for int64(len(segs)) <= idx {
		segs = append(segs, &segStats{})
	}
	return segs[idx]
}

func pct(lats []int64, p float64) int64 {
	if len(lats) == 0 {
		return 0
	}
	return lats[int(float64(len(lats)-1)*p)]
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func newRng(seed uint64) func() uint64 {
	var state uint64 = seed
	return func() uint64 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return state
	}
}

func runLoad(e *sql.Engine, duration time.Duration, connsN, rowsN int, readP float64, interval time.Duration) {
	deadline := time.Now().Add(duration)
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < connsN; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sess := e.NewSession()
			rng := newRng(20261014 + uint64(w))
			for time.Now().Before(deadline) {
				idx := int64(time.Since(start) / interval)
				s := segAt(idx)
				t0 := time.Now()
				if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
					s.addErr()
					continue
				}
				for j := 0; j < 4; j++ {
					op := rng() % 1000
					if uint64(readP*1000) > op {
						// 读：点查或范围查（表尾窗口，实际读取行数 <= 100）
						maxID := int64(rowsN) + atomic.LoadInt64(&inserted)
						if rng()%2 == 0 {
							k := rng() % uint64(maxInt64(maxID, 1))
							if _, err := e.ExecuteAsSession(fmt.Sprintf("SELECT * FROM oltp_t WHERE id = %d", k), sql.SuperUser, sess); err != nil {
								s.addErr()
							} else {
								s.addRead()
							}
						} else if maxID > 101 {
							lo := maxInt64(maxID-500, 0)
							a := lo + int64(rng()%uint64(maxInt64(maxID-100-lo, 1)))
							if _, err := e.ExecuteAsSession(fmt.Sprintf("SELECT * FROM oltp_t WHERE id >= %d AND id < %d", a, a+100), sql.SuperUser, sess); err != nil {
								s.addErr()
							} else {
								s.addRead()
							}
						}
					} else {
						// 写：插入新行或更新已有行
						if rng()%2 == 0 {
							id := atomic.AddInt64(&inserted, 1) + int64(rowsN) - 1
							if _, err := e.ExecuteAsSession(fmt.Sprintf("INSERT INTO oltp_t VALUES (%d, 'oltp-%08d', %d)", id, id, id%1000), sql.SuperUser, sess); err != nil {
								s.addErr()
							} else {
								s.addInsert()
							}
						} else {
							maxID := int64(rowsN) + atomic.LoadInt64(&inserted)
							k := rng() % uint64(maxInt64(maxID, 1))
							if _, err := e.ExecuteAsSession(fmt.Sprintf("UPDATE oltp_t SET score = %d WHERE id = %d", k%1000+1, k), sql.SuperUser, sess); err != nil {
								s.addErr()
							} else {
								s.addUpdate()
							}
						}
					}
				}
				if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
					s.addErr()
					continue
				}
				s.addTxn(time.Since(t0))
			}
		}(w)
	}
	wg.Wait()

	segMu.Lock()
	defer segMu.Unlock()
	fmt.Printf("E4 区间趋势（每 %v，%d 连接，读写比 %s，总 %v）：\n", interval, connsN, *ratio, duration)
	fmt.Println("区间 | 事务数 | TPS | P50(us) | P95(us) | P99(us) | 平均(us) | 错误 | 读/插/改")
	for i, s := range segs {
		s.mu.Lock()
		sort.Slice(s.lats, func(a, b int) bool { return s.lats[a] < s.lats[b] })
		rate := 0.0
		if d := int64(interval / time.Millisecond); d > 0 {
			rate = float64(s.txns) * 1000.0 / float64(d)
		}
		avg := int64(0)
		if len(s.lats) > 0 {
			var sum int64
			for _, v := range s.lats {
				sum += v
			}
			avg = sum / int64(len(s.lats))
		}
		fmt.Printf("%d | %d | %.1f | %d | %d | %d | %d | %d | %d/%d/%d\n",
			i+1, s.txns, rate, pct(s.lats, 0.50), pct(s.lats, 0.95), pct(s.lats, 0.99),
			avg, s.errs, s.reads, s.inserts, s.updates)
		s.mu.Unlock()
	}
	fmt.Printf("E4 累计：插入=%d 期望行数=%d\n", inserted, int64(rowsN)+inserted)
	fmt.Println("E4 LOAD DONE")
}

func main() {
	flag.Parse()
	switch *mode {
	case "check":
		runCheckImpl()
		return
	case "load":
	default:
		fmt.Fprintf(os.Stderr, "mode 必须是 load 或 check: %s\n", *mode)
		os.Exit(2)
	}
	if *dir == "" || *minutes <= 0 {
		fmt.Fprintln(os.Stderr, "mode=load 需要 -dir 与 -minutes")
		os.Exit(2)
	}
	if err := db.Init(*dir); err != nil && !errors.Is(err, db.ErrAlreadyInitialized) {
		fmt.Fprintf(os.Stderr, "init data dir: %v\n", err)
		os.Exit(2)
	}
	d, err := db.Open(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(2)
	}
	defer d.Close()
	e := d.SQL
	_, _ = e.Execute("DROP TABLE oltp_t")
	if _, err := e.Execute("CREATE TABLE oltp_t (id INT, name TEXT, score INT, PRIMARY KEY (id))"); err != nil {
		fmt.Fprintf(os.Stderr, "create oltp table: %v\n", err)
		os.Exit(2)
	}
	loadStmts := (*rows + batch - 1) / batch
	loadStart := time.Now()
	for s := 0; s < loadStmts; s++ {
		start := s * batch
		rowsNow := minInt(batch, *rows-start)
		var sb strings.Builder
		sb.WriteString("INSERT INTO oltp_t VALUES ")
		for j := 0; j < rowsNow; j++ {
			id := start + j
			if j > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "(%d, 'load-%08d', %d)", id, id, id%1000)
		}
		if _, err := e.Execute(sb.String()); err != nil {
			fmt.Fprintf(os.Stderr, "oltp load %d: %v\n", start, err)
			os.Exit(2)
		}
	}
	loadMs := time.Since(loadStart).Milliseconds()
	fmt.Printf("E4 预装载 %d 行: %d ms (%.0f 行/秒)\n", *rows, loadMs,
		float64(*rows)*1000.0/float64(maxInt64(loadMs, 1)))
	if _, err := e.Execute("CREATE INDEX idx_oltp_id ON oltp_t (id)"); err != nil {
		fmt.Fprintf(os.Stderr, "create oltp index: %v\n", err)
		os.Exit(2)
	}
	readP := 0.8
	switch *ratio {
	case "8:2":
		readP = 0.8
	case "5:5":
		readP = 0.5
	default:
		fmt.Fprintf(os.Stderr, "ratio 必须是 8:2 或 5:5: %s\n", *ratio)
		os.Exit(2)
	}
	fmt.Printf("E4 stability load: minutes=%d conns=%d rows=%d ratio=%s go=%s\n",
		*minutes, *conns, *rows, *ratio, runtime.Version())
	runLoad(e, time.Duration(*minutes)*time.Minute, *conns, *rows, readP, time.Minute)
}

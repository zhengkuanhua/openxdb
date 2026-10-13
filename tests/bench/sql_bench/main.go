// SQL 层性能基准（P0-5 上线前置项）：产出可发布的读写吞吐/延迟证据。
//
// 与 tests/bench/e1_btree_bench.go（引擎层微基准）风格对齐：独立 main 程序、
// 参数可配置、LCG 随机种子、输出 TPS/QPS 与延迟分位数。
//
// 覆盖 SQL 层典型场景：
//
//	A. 单点写入（INSERT，逐条 autocommit）
//	B. 单点查询（主键 SELECT，随机点查）
//	C. 范围扫描（WHERE id >= a AND id < b）
//	D. 混合事务（BEGIN / 10x INSERT + 5x SELECT / COMMIT）
//	E. 引擎层对照（同一 RocksDB 的批量写与点查，展示 SQL 层叠加开销）
//
// 运行（仓库根目录，需 RocksDB CGO 环境，见 scripts/build.ps1）：
//
//	go run ./tests/bench/sql_bench -n 100000 -dir sql_bench_data -out tests/bench/docs/sql_bench_result.md
//
// 输出：控制台表格 + Markdown 结果文件（QPS/TPS、平均/P50/P95/P99、总耗时）。
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

var (
	n   = flag.Int("n", 100000, "数据量（行数/操作数，默认 100000）")
	dir = flag.String("dir", "sql_bench_data", "基准数据目录（自动 init，可复用；干净数据请换新目录）")
	out = flag.String("out", "tests/bench/docs/sql_bench_result.md", "结果 Markdown 输出路径")
	multi = flag.Int("multi", 100, "F 场景多行导入：单条 INSERT 的 VALUES 行数（默认 100）")
)

// opStat 收集单场景操作耗时（微秒）并输出统计。
type opStat struct {
	label string
	count int
	total time.Duration
	lats  []int64
}

func newOpStat(label string, count int) *opStat {
	return &opStat{label: label, count: count, lats: make([]int64, 0, count)}
}

func (s *opStat) add(d time.Duration) {
	s.lats = append(s.lats, d.Microseconds())
	s.total += d
}

// pct 返回已排序延迟的分位数（us）。
func (s *opStat) pct(p float64) int64 {
	if len(s.lats) == 0 {
		return 0
	}
	return s.lats[int(float64(len(s.lats)-1)*p)]
}

// row 生成控制台行与 Markdown 行共用的统计文本。
func (s *opStat) row(unit string) string {
	sort.Slice(s.lats, func(a, b int) bool { return s.lats[a] < s.lats[b] })
	rate := float64(s.count) * 1000.0 / float64(s.total.Milliseconds())
	return fmt.Sprintf("%s | %d | %s | %.1f %s | %d us | %d us | %d us | %d us",
		s.label, s.count, s.total.Round(time.Millisecond), rate, unit,
		s.total.Microseconds()/maxInt64(int64(s.count), 1), s.pct(0.50), s.pct(0.95), s.pct(0.99))
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func main() {
	flag.Parse()
	if *n <= 0 {
		fmt.Fprintln(os.Stderr, "n must be > 0")
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

	// 准备表：旧目录残留表先 DROP（忽略不存在错误）。
	_, _ = e.Execute("DROP TABLE bench")
	if _, err := e.Execute("CREATE TABLE bench (id INT, name TEXT, score INT, PRIMARY KEY (id))"); err != nil {
		fmt.Fprintf(os.Stderr, "create table: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("OpenXDB SQL layer benchmark: n=%d, dir=%s, go=%s\n", *n, *dir, runtime.Version())

	// A. 单点写入（INSERT，逐条 autocommit：解析+事务+WAL+存储 全链路）
	ins := newOpStat("A.单点写入 INSERT", *n)
	for i := 0; i < *n; i++ {
		t0 := time.Now()
		if _, err := e.Execute(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name-%08d', %d)", i, i, i%1000)); err != nil {
			fmt.Fprintf(os.Stderr, "insert %d: %v\n", i, err)
			os.Exit(2)
		}
		ins.add(time.Since(t0))
	}

	// B. 单点查询（主键 SELECT，随机点查，种子 42）
	get := newOpStat("B.单点查询 SELECT", *n)
	rng := newRng(42)
	for i := 0; i < *n; i++ {
		k := int(rng() % uint64(*n))
		t0 := time.Now()
		res, err := e.Execute(fmt.Sprintf("SELECT * FROM bench WHERE id = %d", k))
		if err != nil || len(res.Rows) != 1 {
			fmt.Fprintf(os.Stderr, "select id=%d: rows=%d err=%v\n", k, len(res.Rows), err)
			os.Exit(2)
		}
		get.add(time.Since(t0))
	}

	// D. 混合事务（BEGIN / 10x INSERT + 5x SELECT / COMMIT；事务数 = n/10）
	txnCount := *n / 10
	txn := newOpStat("D.混合事务 TPS", txnCount)
	rng = newRng(99)
	base := *n
	for t := 0; t < txnCount; t++ {
		t0 := time.Now()
		if _, err := e.Execute("BEGIN"); err != nil {
			fmt.Fprintf(os.Stderr, "begin: %v\n", err)
			os.Exit(2)
		}
		for j := 0; j < 10; j++ {
			id := base + t*10 + j
			if _, err := e.Execute(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'txn-%08d', %d)", id, id, id%1000)); err != nil {
				fmt.Fprintf(os.Stderr, "txn insert %d: %v\n", id, err)
				os.Exit(2)
			}
		}
		for j := 0; j < 5; j++ {
			k := int(rng() % uint64(*n))
			if _, err := e.Execute(fmt.Sprintf("SELECT id FROM bench WHERE id = %d", k)); err != nil {
				fmt.Fprintf(os.Stderr, "txn select %d: %v\n", k, err)
				os.Exit(2)
			}
		}
		if _, err := e.Execute("COMMIT"); err != nil {
			fmt.Fprintf(os.Stderr, "commit: %v\n", err)
			os.Exit(2)
		}
		txn.add(time.Since(t0))
	}

	// C. 范围扫描（WHERE 范围，每次返回 100 行）。
	// 现状：SQL 层主键（PK）不在 Indexes（仅二级索引），WHERE id >= a 范围条件回退全表扫描 +
	// 逐行过滤，成本随数据量线性放大；C1 如实呈现该现状（固定 3 次控制基准时长）。
	// C2：对 id 建立二级索引后，范围条件走 indexLookup（单边界 [a, 表尾) + 回表），
	// 起点取表尾前 500 行窗口使实际读取行数 ≈ 范围大小，固定 100 次。
	c1 := newOpStat("C1.范围扫描(无索引)", 3)
	rng = newRng(7)
	for i := 0; i < 3; i++ {
		a := int(rng() % uint64(2**n-100))
		t0 := time.Now()
		res, err := e.Execute(fmt.Sprintf("SELECT * FROM bench WHERE id >= %d AND id < %d", a, a+100))
		if err != nil || len(res.Rows) != 100 {
			fmt.Fprintf(os.Stderr, "c1 scan a=%d: rows=%d err=%v\n", a, len(res.Rows), err)
			os.Exit(2)
		}
		c1.add(time.Since(t0))
	}

	if _, err := e.Execute("CREATE INDEX idx_bench_id ON bench (id)"); err != nil {
		fmt.Fprintf(os.Stderr, "create index: %v\n", err)
		os.Exit(2)
	}
	c2 := newOpStat("C2.范围扫描(二级索引)", 100)
	rng = newRng(7)
	for i := 0; i < 100; i++ {
		a := 2**n - 100 - int(rng()%400)
		t0 := time.Now()
		res, err := e.Execute(fmt.Sprintf("SELECT * FROM bench WHERE id >= %d AND id < %d", a, a+100))
		if err != nil || len(res.Rows) != 100 {
			fmt.Fprintf(os.Stderr, "c2 scan a=%d: rows=%d err=%v\n", a, len(res.Rows), err)
			os.Exit(2)
		}
		c2.add(time.Since(t0))
	}

	// E. 引擎层对照（同一 RocksDB：批量写 / 点查，展示 SQL 层叠加开销口径）
	engWrite := newOpStat("E1.引擎批量写", *n)
	rng = newRng(42)
	var batch *storage.WriteBatch
	flush := func() error {
		if batch == nil {
			return nil
		}
		err := d.Storage.Write(batch)
		batch = nil
		return err
	}
	for i := 0; i < *n; i++ {
		k := rng() % uint64(*n)
		if batch == nil {
			batch = &storage.WriteBatch{}
		}
		t0 := time.Now()
		batch.Puts = append(batch.Puts, storage.KVPair{Key: engineKey(k), Value: []byte(fmt.Sprintf("value-%016x", k))})
		if len(batch.Puts) >= 1000 {
			if err := flush(); err != nil {
				fmt.Fprintf(os.Stderr, "engine write: %v\n", err)
				os.Exit(2)
			}
		}
		engWrite.add(time.Since(t0))
	}
	if err := flush(); err != nil {
		fmt.Fprintf(os.Stderr, "engine write: %v\n", err)
		os.Exit(2)
	}

	engGet := newOpStat("E2.引擎点查", *n)
	rng = newRng(7)
	for i := 0; i < *n; i++ {
		k := rng() % uint64(*n)
		t0 := time.Now()
		if _, err := d.Storage.Get(engineKey(k)); err != nil && err != storage.ErrNotFound {
			fmt.Fprintf(os.Stderr, "engine get: %v\n", err)
			os.Exit(2)
		}
		engGet.add(time.Since(t0))
	}

	// F. 多行 VALUES 批量导入（单条 INSERT 多行，全语句单次提交）。
	// 总行数 = n，语句数 = n/multi；统计口径为行/秒，直接对比 A 场景（逐行单条）。
	// id 使用独立区间 [2n+txnCount*10, ...) 避免与 A/D 场景冲突。
	batchRows := *multi
	if batchRows <= 0 {
		batchRows = 100
	}
	stmtCount := (*n + batchRows - 1) / batchRows
	multiImport := newOpStat(fmt.Sprintf("F.多行导入INSERT(%d行/语句)", batchRows), stmtCount)
	rng = newRng(1234)
	startID := 2**n + txnCount*10
	for i := 0; i < *n; i += batchRows {
		rows := batchRows
		if *n-i < batchRows {
			rows = *n - i
		}
		var sb strings.Builder
		sb.WriteString("INSERT INTO bench VALUES ")
		for j := 0; j < rows; j++ {
			id := startID + i + j
			if j > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "(%d, 'multi-%08d', %d)", id, id, id%1000)
		}
		t0 := time.Now()
		if _, err := e.Execute(sb.String()); err != nil {
			fmt.Fprintf(os.Stderr, "multi insert batch@%d: %v\n", i, err)
			os.Exit(2)
		}
		multiImport.add(time.Since(t0))
	}

	// 控制台输出
	header := fmt.Sprintf("%-24s | %-8s | %-12s | %-9s | %-8s | %-6s | %-6s | %-6s",
		"场景", "操作数", "总耗时", "速率", "平均", "P50", "P95", "P99")
	sep := strings.Repeat("-", 100)
	fmt.Println(sep)
	fmt.Println(header)
	fmt.Println(sep)
	for _, s := range []*opStat{ins, get, c1, c2, txn, engWrite, engGet, multiImport} {
		unit := "QPS"
		if s == txn || s == engWrite {
			unit = "TPS"
		}
		fmt.Println(s.row(unit))
	}
	fmt.Println(sep)

	// Markdown 结果文件
	var b strings.Builder
	b.WriteString("# OpenXDB SQL 层性能基准结果（P0-5）\n\n")
	b.WriteString(fmt.Sprintf("- 运行时间：%s\n", time.Now().Format("2006-01-02 15:04:05")))
	b.WriteString(fmt.Sprintf("- 环境：%s / %s，go %s\n", runtime.GOOS, runtime.GOARCH, runtime.Version()))
	b.WriteString(fmt.Sprintf("- 参数：n=%d, data-dir=%s\n", *n, *dir))
	b.WriteString(fmt.Sprintf("- 引擎：RocksDB 11.8.1 + WAL（真实 db.Open 路径，含 SQL 解析/事务/WAL/存储全链路）\n\n"))
	b.WriteString("## 场景汇总\n\n")
	b.WriteString("| 场景 | 操作数 | 总耗时 | 速率 | 平均 | P50 | P95 | P99 |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, s := range []*opStat{ins, get, c1, c2, txn, engWrite, engGet, multiImport} {
		unit := "QPS"
		if s == txn || s == engWrite {
			unit = "TPS"
		}
		b.WriteString("| " + s.row(unit) + " |\n")
	}
	b.WriteString("\n## 场景说明\n\n")
	b.WriteString("- **A.单点写入 INSERT**：逐条 `INSERT INTO bench VALUES (...)` autocommit，代表单条写入全链路（解析→事务→WAL→存储）。\n")
	b.WriteString("- **B.单点查询 SELECT**：随机主键点查 `SELECT * FROM bench WHERE id = k`，行数 1。\n")
	b.WriteString("- **C1.范围扫描(无索引)**：`WHERE id >= a AND id < a+100`，每次返回 100 行。SQL 层主键不在二级索引集合（Indexes），主键范围条件回退全表扫描+逐行过滤，成本随数据量线性放大；固定 3 次控制基准时长。\n")
	b.WriteString("- **C2.范围扫描(二级索引)**：先 `CREATE INDEX idx_bench_id ON bench (id)`，同一查询走二级索引范围读（单边界 `[a, 表尾)` + 回表）；起点取表尾前 500 行窗口使实际读取行数≈范围大小，固定 100 次。\n")
	b.WriteString("- **D.混合事务 TPS**：`BEGIN` + 10 条 INSERT + 5 条 SELECT + `COMMIT`，以事务为单位统计吞吐（TPS）与延迟。\n")
	b.WriteString("- **E1/E2 引擎层对照**：同一 RocksDB 实例直接批量写（batch=1000）与随机点查（绕过 SQL 层），口径为原始 KV（key 前缀 `e:`，不经过表编码），用于展示 SQL 层叠加开销。\n")
	b.WriteString(fmt.Sprintf("- **F.多行导入 INSERT**：单条 `INSERT INTO bench VALUES (...),(...),...` 一次写入 %d 行，全语句聚合为同一事务批次单次提交（autocommit 2PC），统计口径为行/秒，直接对比 A 场景（逐行单条）。\n\n", batchRows))
	b.WriteString("## 解读\n\n")
	ov := float64(ins.total.Microseconds()) / float64(maxInt64(engWrite.total.Microseconds(), 1))
	b.WriteString(fmt.Sprintf("- SQL 单点写入 vs 引擎批量写：SQL 层逐条写入（含解析+事务+WAL 单条提交）耗时为引擎批量写（batch=1000）的 **%.1fx**；单条写入吞吐受 WAL 提交粒度与逐条解析主导。\n", ov))
	ovGet := float64(get.total.Microseconds()) / float64(maxInt64(engGet.total.Microseconds(), 1))
	b.WriteString(fmt.Sprintf("- SQL 单点查询 vs 引擎点查：SQL 点查耗时为引擎点查的 **%.1fx**（含 SQL 解析、类型解码与结果组装）。\n", ovGet))
	ovMulti := float64(ins.total.Microseconds()) / float64(maxInt64(multiImport.total.Microseconds(), 1))
	b.WriteString(fmt.Sprintf("- 多行批量 vs 逐行单条：F 场景（每语句 %d 行，同一事务批次单次提交）行速率为 A 场景的 **%.1fx**（A 单行 %.1f 行/秒；F 语句 %.1f 语句/秒 ≈ %.0f 行/秒），逐条 WAL 提交粒度与逐行解释开销被批量摊薄。\n", batchRows, ovMulti, float64(ins.count)*1000.0/float64(maxInt64(ins.total.Milliseconds(), 1)), float64(multiImport.count)*1000.0/float64(maxInt64(multiImport.total.Milliseconds(), 1)), float64(multiImport.count)*1000.0/float64(maxInt64(multiImport.total.Milliseconds(), 1))*float64(batchRows)))
	b.WriteString("- 延迟均以微秒（us）计；速率以 QPS（查询/秒）或 TPS（写入/事务 每秒）计。\n")
	b.WriteString("- 本基准运行于本机 CGO（RocksDB）环境，绝对数值与机器相关；相对开销（SQL vs 引擎）更具可比性。\n")

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir out: %v\n", err)
		os.Exit(2)
	}
	if err := os.WriteFile(*out, []byte(b.String()), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write out: %v\n", err)
		os.Exit(2)
	}
	fmt.Println("result written:", *out)
	fmt.Println("SQL_BENCH DONE")
}

func engineKey(v uint64) []byte { return []byte(fmt.Sprintf("e:%016x", v)) }

// newRng 返回 LCG 伪随机闭包（与 tests/bench/e1_btree_bench.go 同风格）。
func newRng(seed uint64) func() uint64 {
	s := seed
	return func() uint64 {
		s = s*6364136223846793005 + 1442695040888963407
		return s
	}
}

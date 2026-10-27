// C5 cgo 扫描拷贝优化专项基准（v1.16.0 P2）。
//
// 与 tests/bench/sql_bench（OLTP 混压）不同，本程序聚焦单语句专项压测，
// 用于对比 C5 前后 bridge.cc 扫描路径的收益：
//
//	COUNT(*) 全表      —— SELECT COUNT(*) FROM c5_t（无 WHERE，走 COUNT 快速路径，
//	                        bridge 层只迭代 key 不拷贝 value）
//	范围扫描           —— SELECT * FROM c5_t WHERE id >= a AND id < b（大范围物化，
//	                        xdb_scan/xscan_item 拷贝链主战场）
//	ORDER BY           —— SELECT * FROM c5_t ORDER BY score（无索引列，全表物化+排序，
//	                        纯 Scan 主导）
//	SUM                —— SELECT SUM(score) FROM c5_t（全表物化+聚合，需要 value）
//
// 运行（仓库根目录，需 RocksDB CGO 环境，见 scripts/build.ps1）：
//
//	go run ./tests/bench/c5_sql_bench -rows 500000 -dir tests/bench/c5_sql_bench_data \
//	    -out docs/experiments/C5_sql_bench_result.md
//
// 输出：控制台表格 + Markdown 结果文件（每语句平均耗时 / 中位 / QPS）。
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

var (
	rows   = flag.Int("rows", 500000, "表行数（默认 500000）")
	dir    = flag.String("dir", "tests/bench/c5_sql_bench_data", "基准数据目录（自动 init；干净数据请换新目录）")
	out    = flag.String("out", "docs/experiments/C5_sql_bench_result.md", "结果 Markdown 输出路径")
	rounds = flag.Int("rounds", 7, "每语句计时轮数（首轮预热，取剩余轮统计）")
	multi  = flag.Int("multi", 100, "单条 INSERT 的 VALUES 行数")
	skipLoad = flag.Bool("skip-load", false, "跳过装载（需目录已装载 c5_t），用于同数据同状态公平对比基线/优化版")
)

type benchCase struct {
	name string
	sql  string
	desc string
}

func main() {
	flag.Parse()

	fmt.Printf("[c5] rows=%d dir=%s rounds=%d\n", *rows, *dir, *rounds)

	// 打开/初始化数据库（复用 sql_bench 同一 db.Open 路径）
	if err := db.Init(*dir); err != nil && err != db.ErrAlreadyInitialized {
		fmt.Fprintf(os.Stderr, "db init: %v\n", err)
		os.Exit(1)
	}
	d, err := db.Open(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db open: %v\n", err)
		os.Exit(1)
	}
	e := d.SQL
	if *skipLoad {
		// 同数据同状态对比：目录必须已装载 c5_t，跳过 DROP/CREATE/INSERT。
		fmt.Printf("[c5] skip-load: reuse existing c5_t in %s\n", *dir)
	} else {
		// 建表并装载
		if _, err := e.Execute("DROP TABLE IF EXISTS c5_t"); err != nil {
			fmt.Fprintf(os.Stderr, "drop: %v\n", err)
			os.Exit(1)
		}
		if _, err := e.Execute("CREATE TABLE c5_t (id INT, name TEXT, score INT, PRIMARY KEY (id))"); err != nil {
			fmt.Fprintf(os.Stderr, "create table: %v\n", err)
			os.Exit(1)
		}
		loadStart := time.Now()
		sess := e.NewSession()
		if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
			fmt.Fprintf(os.Stderr, "begin: %v\n", err)
			os.Exit(1)
		}
		const batchRows = 100 // 每事务行数
		for i := 0; i < *rows; i += batchRows {
			end := i + batchRows
			if end > *rows {
				end = *rows
			}
			var sb strings.Builder
			sb.WriteString("INSERT INTO c5_t VALUES ")
			for j := i; j < end; j++ {
				if j > i {
					sb.WriteString(",")
				}
				fmt.Fprintf(&sb, "(%d, 'name-%08d', %d)", j, j, j%1000)
			}
			if _, err := e.ExecuteAsSession(sb.String(), sql.SuperUser, sess); err != nil {
				fmt.Fprintf(os.Stderr, "insert batch %d..%d: %v\n", i, end, err)
				os.Exit(1)
			}
		}
		if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
			fmt.Fprintf(os.Stderr, "commit: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("[c5] loaded %d rows in %v\n", *rows, time.Since(loadStart))
	}

	// 单语句用例
	half := *rows / 2
	cases := []benchCase{
		{"COUNT(*)", fmt.Sprintf("SELECT COUNT(*) FROM c5_t"), "全表计数（COUNT 快速路径，免值拷贝）"},
		{"range-scan", fmt.Sprintf("SELECT * FROM c5_t WHERE id >= 0 AND id < %d", *rows), "全表范围扫描（Scan 拷贝链）"},
		{"range-half", fmt.Sprintf("SELECT * FROM c5_t WHERE id >= %d AND id < %d", half, *rows), "半表范围扫描"},
		{"order-by", "SELECT * FROM c5_t ORDER BY score", "全表 ORDER BY（无索引列，物化排序）"},
		{"sum", "SELECT SUM(score) FROM c5_t", "全表 SUM（物化聚合，需 value）"},
	}

	type stat struct {
		lats []time.Duration
	}
	stats := make(map[string]*stat)
	for _, c := range cases {
		st := &stat{}
		for r := 0; r < *rounds; r++ {
			start := time.Now()
			if _, err := e.Execute(c.sql); err != nil {
				fmt.Fprintf(os.Stderr, "[%s] exec: %v\n", c.name, err)
				os.Exit(1)
			}
			st.lats = append(st.lats, time.Since(start))
		}
		stats[c.name] = st
	}

	// 统计（去掉首轮预热）
	type row struct {
		name, desc  string
		avg, med    time.Duration
		p95, qps    float64
	}
	var outRows []row
	for _, c := range cases {
		st := stats[c.name]
		samples := st.lats[1:]
		if len(samples) == 0 {
			samples = st.lats
		}
		sorted := append([]time.Duration(nil), samples...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		var total time.Duration
		for _, d := range samples {
			total += d
		}
		avg := total / time.Duration(len(samples))
		med := sorted[len(sorted)/2]
		p95 := sorted[max(0, int(float64(len(sorted))*0.95)-1)]
		qps := float64(1) / avg.Seconds()
		outRows = append(outRows, row{c.name, c.desc, avg, med, float64(p95), qps})
	}

	// 控制台
	fmt.Println()
	fmt.Printf("%-12s %14s %14s %14s %12s\n", "case", "avg(ms)", "med(ms)", "p95(ms)", "qps")
	for _, r := range outRows {
		fmt.Printf("%-12s %14.3f %14.3f %14.3f %12.1f\n", r.name,
			float64(r.avg)/1e6, float64(r.med)/1e6, r.p95/1e6, r.qps)
	}

	// Markdown 落盘
	if *out != "" {
		var sb strings.Builder
		sb.WriteString("# C5 单语句专项压测（cgo 扫描拷贝优化）\n\n")
		fmt.Fprintf(&sb, "- 表：c5_t(id INT, name TEXT, score INT, PRIMARY KEY(id))，共 %d 行\n", *rows)
		fmt.Fprintf(&sb, "- 环境：Windows + Go %s + CGO + RocksDB\n", goVersion())
		fmt.Fprintf(&sb, "- 轮数：%d（首轮预热，统计其余）\n\n", *rounds)
		sb.WriteString("| case | 说明 | avg(ms) | med(ms) | p95(ms) | QPS |\n")
		sb.WriteString("|------|------|--------:|--------:|--------:|----:|\n")
		for _, r := range outRows {
			fmt.Fprintf(&sb, "| %s | %s | %.3f | %.3f | %.3f | %.1f |\n", r.name, r.desc,
				float64(r.avg)/1e6, float64(r.med)/1e6, r.p95/1e6, r.qps)
		}
		abs, err := filepath.Abs(*out)
		if err == nil {
			*out = abs
		}
		if err := os.WriteFile(*out, []byte(sb.String()), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "write out: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\n[c5] result written: %s\n", *out)
	}
}

func goVersion() string {
	return "1.27.1"
}

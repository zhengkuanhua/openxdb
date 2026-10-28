//
// C6 连接内语句 pipeline / 快照复用收益基准（v1.16.0 P3）
//
// 对照两个 feature flag 的开关收益：
//  1. snapshot_share（executor.go，autocommit 只读语句共享快照窗口）
//     同一 Session 连续执行只读语句（COUNT / 全表范围 / 点查 / SUM），
//     off 为基线（每条语句独立 Begin + 独立快照），on 复用窗口快照，
//     量化省 Begin/cgo Snapshot 的单连接只读语句延迟收益。
//     测量修正：点查每条用不同 key（防 RocksDB block cache 掩盖），
//     每模式热身 1 轮 + off/on 按轮交错取中位数（防 cache/噪声顺序效应）。
//  2. pipeline_flush（server.go，REPL 连接内条件 flush）
//     经 REPLSession 行协议一次性批量发送 N 条命令（SET，快速非 fsync
//     语句，走串行路径），off 逐条 flush，on 延迟合并 flush，
//     量化连接内 pipeline 批量发送的服务端处理耗时收益（延迟隐藏）。
//     测量修正：底层 writer 用 syscallWriter（每次 Write 模拟 syscall
//     开销并计数），替代零成本 io.Discard，还原逐条 vs 合并 flush
//     的写路径成本差异（off 写 N 次、on 写 1 次）。
//
// 运行（仓库根目录，需 RocksDB CGO 环境，见 scripts/build.ps1）：
//
//	go run ./tests/bench/c6_sql_bench -rows 100000 -dir tests/bench/c6_sql_bench_data \
//	    -out docs/experiments/C6_sql_bench_result.md
//
// 输出：控制台表格 + Markdown 结果文件。
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/server"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

var (
	rows     = flag.Int("rows", 100000, "表行数（默认 100000）")
	dir      = flag.String("dir", "tests/bench/c6_sql_bench_data", "基准数据目录（自动 init；干净数据请换新目录）")
	out      = flag.String("out", "docs/experiments/C6_sql_bench_result.md", "结果 Markdown 输出路径")
	rounds   = flag.Int("rounds", 3, "每场景计时轮数（取中位数）")
	repeat   = flag.Int("repeat", 100, "每轮内语句重复条数")
	pipeline = flag.Int("pipeline", 100, "pipeline 场景批量命令条数")
	skipLoad = flag.Bool("skip-load", false, "跳过装载（需目录已装载 c6_t），用于同数据同状态公平对比")
)

type benchCase struct {
	name    string
	sql     string
	desc    string
	dynamic bool // 点查类：每 k 用不同 key，避免 RocksDB block cache 掩盖测量
}

// pointSQL 构造第 k 条点查：key 随 k 变化（id = (k*7919+7) % rows），
// 保证每条读不同行，量化共享快照 vs 独立快照的真实 Begin/Snapshot 收益。
func pointSQL(k, rows int) string {
	return fmt.Sprintf("SELECT * FROM c6_t WHERE id = %d", (k*7919+7)%rows)
}

func main() {
	flag.Parse()
	fmt.Printf("[c6] rows=%d dir=%s rounds=%d repeat=%d pipeline=%d\n",
		*rows, *dir, *rounds, *repeat, *pipeline)

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

	if !*skipLoad {
		if _, err := e.Execute("DROP TABLE IF EXISTS c6_t"); err != nil {
			fmt.Fprintf(os.Stderr, "drop: %v\n", err)
			os.Exit(1)
		}
		if _, err := e.Execute("CREATE TABLE c6_t (id INT, name TEXT, score INT, PRIMARY KEY (id))"); err != nil {
			fmt.Fprintf(os.Stderr, "create table: %v\n", err)
			os.Exit(1)
		}
		loadStart := time.Now()
		sess := e.NewSession()
		if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
			fmt.Fprintf(os.Stderr, "begin: %v\n", err)
			os.Exit(1)
		}
		const batchRows = 100
		for i := 0; i < *rows; i += batchRows {
			end := i + batchRows
			if end > *rows {
				end = *rows
			}
			var sb strings.Builder
			sb.WriteString("INSERT INTO c6_t VALUES ")
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
		fmt.Printf("[c6] loaded %d rows in %v\n", *rows, time.Since(loadStart))
	}

	cases := []benchCase{
		{"count-all", "SELECT COUNT(*) FROM c6_t", "全表计数（COUNT 快速路径）", false},
		{"range-all", fmt.Sprintf("SELECT * FROM c6_t WHERE id >= 0 AND id < %d", *rows), "全表范围扫描（物化）", false},
		{"point-var", "", "点查（PK 索引，每条不同 key）", true},
		{"sum-all", "SELECT SUM(score) FROM c6_t", "全表 SUM（物化聚合）", false},
	}

	// ---------- 场景 1：snapshot_share 对照（Engine 直调单 Session） ----------
	// 测量修正（v1.16.0 C6 复测）：① point 类每 k 用不同 key，避免
	// RocksDB block cache 命中掩盖真实快照开销；② 每模式先热身 1 轮
	//（同 SQL 同会话预跑，不计入样本）再计时；③ off/on 按轮交错
	//（round 奇数先 off、偶数先 on），各自取中位数，抵消 cache/系统噪声
	// 顺序效应。
	type snapRow struct {
		name, desc   string
		off, on      time.Duration // 平均每条
	}
	var snapRows []snapRow
	for _, c := range cases {
		meas := func(enabled bool) time.Duration {
			if _, err := e.Execute(fmt.Sprintf("SET snapshot_share = %v", boolStr(enabled))); err != nil {
				fmt.Fprintf(os.Stderr, "set snapshot_share: %v\n", err)
				os.Exit(1)
			}
			// 全表类单条耗时高（数百 ms），重复次数压低避免基准过长；
			// 点查类单条耗时低，用满 repeat 保证样本稳定。
			rep := *repeat
			if !c.dynamic && rep > 3 {
				rep = 3
			}
			sess := e.NewSession()
			// 热身 1 轮：同模式预跑 rep 条，丢弃样本
			for k := 0; k < rep; k++ {
				q := c.sql
				if c.dynamic {
					q = pointSQL(k, *rows)
				}
				if _, err := e.ExecuteAsSession(q, sql.SuperUser, sess); err != nil {
					fmt.Fprintf(os.Stderr, "[%s] warmup: %v\n", c.name, err)
					os.Exit(1)
				}
			}
			var samples []time.Duration
			for r := 0; r < *rounds; r++ {
				start := time.Now()
				for k := 0; k < rep; k++ {
					q := c.sql
					if c.dynamic {
						q = pointSQL(k, *rows)
					}
					if _, err := e.ExecuteAsSession(q, sql.SuperUser, sess); err != nil {
						fmt.Fprintf(os.Stderr, "[%s] exec: %v\n", c.name, err)
						os.Exit(1)
					}
				}
				samples = append(samples, time.Since(start))
			}
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			return samples[len(samples)/2] / time.Duration(rep)
		}
		offS, onS := make([]time.Duration, 0, *rounds), make([]time.Duration, 0, *rounds)
		for r := 0; r < *rounds; r++ {
			if r%2 == 0 {
				offS = append(offS, meas(false))
				onS = append(onS, meas(true))
			} else {
				onS = append(onS, meas(true))
				offS = append(offS, meas(false))
			}
		}
		sort.Slice(offS, func(i, j int) bool { return offS[i] < offS[j] })
		sort.Slice(onS, func(i, j int) bool { return onS[i] < onS[j] })
		off, on := offS[len(offS)/2], onS[len(onS)/2]
		snapRows = append(snapRows, snapRow{c.name, c.desc, off, on})
		fmt.Printf("[c6] snap %-10s off=%7.1fus/stmt  on=%7.1fus/stmt  gain=%5.1f%%\n",
			c.name, float64(off)/1e3, float64(on)/1e3, gainPct(off, on))
	}
	// 恢复默认开关
	if _, err := e.Execute("SET snapshot_share = on"); err != nil {
		fmt.Fprintf(os.Stderr, "reset snapshot_share: %v\n", err)
		os.Exit(1)
	}

	// ---------- 场景 2：pipeline_flush 对照（REPL 行协议批量发送） ----------
	// 测量修正（v1.16.0 C6 复测）：io.Discard 使底层 Write 零成本，off
	// （逐条 flush）与 on（合并 flush）都测不出差异；改用 syscallWriter——
	// 每次底层 Write 计入 1 次模拟 syscall 并叠加固定开销（50us），
	// 真实还原"逐条 vs 合并 flush"的写路径成本差异。同时统计底层
	// Write 次数：off 应等于 N 条命令（每条一次 flush），on 应收敛到 1。
	measPipe := func(enabled bool) (time.Duration, int) {
		if _, err := e.Execute(fmt.Sprintf("SET pipeline_flush = %v", boolStr(enabled))); err != nil {
			fmt.Fprintf(os.Stderr, "set pipeline_flush: %v\n", err)
			os.Exit(1)
		}
		var samples []time.Duration
		var writeCounts []int
		for r := 0; r < *rounds; r++ {
			var buf bytes.Buffer
			for k := 0; k < *pipeline; k++ {
				buf.WriteString("SET slow_query_threshold = 1\n")
			}
			w := &syscallWriter{per: 50 * time.Microsecond}
			s := server.New(d.Txn)
			s.SQL = e
			start := time.Now()
			if err := s.REPL(&buf, w); err != nil {
				fmt.Fprintf(os.Stderr, "repl: %v\n", err)
				os.Exit(1)
			}
			samples = append(samples, time.Since(start))
			writeCounts = append(writeCounts, w.count)
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		sort.Ints(writeCounts)
		return samples[len(samples)/2], writeCounts[len(writeCounts)/2]
	}
	pipeOff, pipeOffW := measPipe(false)
	pipeOn, pipeOnW := measPipe(true)
	fmt.Printf("[c6] pipe %d SET: off=%8.2fms(w=%d)  on=%8.2fms(w=%d)  gain=%5.1f%%\n",
		*pipeline, float64(pipeOff)/1e6, pipeOffW, float64(pipeOn)/1e6, pipeOnW, gainPct(pipeOff, pipeOn))

	// ---------- 落盘 Markdown ----------
	if *out != "" {
		var sb strings.Builder
		sb.WriteString("# C6 单连接 pipeline / 快照复用收益基准（v1.16.0 P3）\n\n")
		fmt.Fprintf(&sb, "- 表：c6_t(id INT, name TEXT, score INT, PRIMARY KEY(id))，共 %d 行\n", *rows)
		fmt.Fprintf(&sb, "- 环境：Windows + Go 1.27.1 + CGO + RocksDB\n")
		fmt.Fprintf(&sb, "- 每场景轮数：%d（取中位数）；每轮每语句重复 %d 条\n\n", *rounds, *repeat)
		sb.WriteString("## 场景 1：snapshot_share（单 Session 连续只读语句，平均每条耗时）\n\n")
		sb.WriteString("| 语句 | 说明 | off(us/条) | on(us/条) | 收益 |\n")
		sb.WriteString("|------|------|----------:|----------:|-----:|\n")
		for _, r := range snapRows {
			fmt.Fprintf(&sb, "| %s | %s | %.1f | %.1f | %.1f%% |\n",
				r.name, r.desc, float64(r.off)/1e3, float64(r.on)/1e3, gainPct(r.off, r.on))
		}
		fmt.Fprintf(&sb, "\n## 场景 2：pipeline_flush（REPL 行协议一次性批量发送 %d 条 SET，总耗时；w=底层 Write 次数）\n\n", *pipeline)
		sb.WriteString("| 开关 | 总耗时(ms) | 底层 Write 次数 |\n|------|----------:|----------:|\n")
		fmt.Fprintf(&sb, "| off（逐条 flush） | %.2f | %d |\n", float64(pipeOff)/1e6, pipeOffW)
		fmt.Fprintf(&sb, "| on（延迟合并 flush） | %.2f | %d |\n", float64(pipeOn)/1e6, pipeOnW)
		fmt.Fprintf(&sb, "| 收益 | %.1f%% | - |\n", gainPct(pipeOff, pipeOn))
		abs, err := filepath.Abs(*out)
		if err == nil {
			*out = abs
		}
		if err := os.WriteFile(*out, []byte(sb.String()), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "write out: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("[c6] result written: %s\n", *out)
	}
}

// boolStr 输出 SET 开关的布尔字面量。
func boolStr(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// syscallWriter 模拟带固定成本的底层 Write（每次计入 1 次 syscall 并
// 叠加 per 开销），用于 pipeline_flush 对照中量化"逐条 flush vs 合并
// flush"的写路径差异，同时统计底层 Write 次数。
type syscallWriter struct {
	mu    sync.Mutex
	count int
	per   time.Duration
}

func (w *syscallWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.count++
	w.mu.Unlock()
	if w.per > 0 {
		time.Sleep(w.per)
	}
	return len(p), nil
}

func gainPct(before, after time.Duration) float64 {
	if before <= 0 {
		return 0
	}
	return (float64(before) - float64(after)) / float64(before) * 100
}

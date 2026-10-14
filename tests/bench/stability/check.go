// 稳定性/持久化验证辅助程序（第 2 批 E4）：
// 打开 sql_bench 初始化并压测过的数据目录，验证进程重启后数据完整可查：
//  1. COUNT(*) 与期望行数（预装载行 + OLTP 阶段实际插入行）一致；
//  2. 抽样主键点查全部命中（重启后数据可读）；
//  3. 退出码 0=通过，非 0=失败（供脚本/CI 判定）。
//
// 用法（仓库根目录，需 RocksDB CGO 环境）：
//
//	go run ./tests/bench/stability -dir <data_dir> -expect <期望行数>
//
// 说明：期望行数取自 E4 压测运行日志中的「插入=%d」（OLTP 阶段插入行），
// 期望行数 = oltp-rows + 插入行。0 表示仅报告不判定。
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

var (
	dir    = flag.String("dir", "", "数据目录（sql_bench 初始化过且已完成压测）")
	expect = flag.Int64("expect", 0, "期望 oltp_t 行数（预装载 + OLTP 插入），0=仅报告")
)

func runCheckImpl() {
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "dir must be set")
		os.Exit(2)
	}

	d, err := db.Open(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(2)
	}
	defer d.Close()

	// 1) 全表计数：验证数据完整落盘且重启后可见。
	res, err := d.SQL.Execute("SELECT COUNT(*) FROM oltp_t")
	if err != nil {
		fmt.Fprintf(os.Stderr, "count: %v\n", err)
		os.Exit(2)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		fmt.Fprintf(os.Stderr, "count result shape: rows=%d cols=%d\n", len(res.Rows), len(res.Rows[0]))
		os.Exit(2)
	}
	actual := res.Rows[0][0].I
	fmt.Printf("COUNT(*) = %d\n", actual)

	// 2) 抽样点查 5 个键：重启后随机读可命中。
	probes := []int64{0, 1, 7, 9999, 123456}
	miss := 0
	for _, k := range probes {
		r, err := d.SQL.Execute(fmt.Sprintf("SELECT id, name, score FROM oltp_t WHERE id = %d", k))
		if err != nil {
			fmt.Fprintf(os.Stderr, "probe id=%d: %v\n", k, err)
			miss++
			continue
		}
		if len(r.Rows) != 1 {
			fmt.Printf("probe id=%d: MISS (rows=%d)\n", k, len(r.Rows))
			miss++
			continue
		}
		fmt.Printf("probe id=%d: HIT (name=%s score=%d)\n", k, r.Rows[0][1].S, r.Rows[0][2].I)
	}
	if miss > 0 {
		fmt.Printf("RESTART CHECK FAIL: %d/5 probes missed\n", miss)
		os.Exit(1)
	}

	// 3) 期望行数判定。
	if *expect > 0 {
		if actual == *expect {
			fmt.Printf("RESTART CHECK PASS: count=%d == expect=%d\n", actual, *expect)
		} else {
			fmt.Printf("RESTART CHECK FAIL: count=%d != expect=%d\n", actual, *expect)
			os.Exit(1)
		}
	} else {
		fmt.Println("RESTART CHECK REPORT ONLY: count reported, expect=0 skipped")
	}
	fmt.Println("STABILITY CHECK DONE")
}

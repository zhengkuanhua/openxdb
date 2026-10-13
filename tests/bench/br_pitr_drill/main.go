// BR/PITR 端到端自动化演练（方向② P0 收尾）：一键复现「备份→恢复」闭环（含数据校验）。
//
// 与 tests/bench/sql_bench（独立 main 程序、参数可配置、输出 Markdown 结果）风格对齐；
// 直接走真实 db.Open 路径（RocksDB + WAL + binlog），不 mock。
//
// 演练场景（同一数据目录顺序执行）：
//
//	A. 建表+二级索引，写入阶段1数据（-rows 行）。
//	B. 全量备份 BACKUP TO '<path>'，校验备份行数与备份点 LSN。
//	C. PITR：备份点后逐条写入阶段2数据（-pitrRows 行）并记录每条提交后 LSN，
//	   RESTORE FROM '<path>' TO LSN <k 条处 LSN> 精确回放，校验行集 = 阶段1 + 前 k 条；
//	   并校验 TO LSN = 备份点 LSN 时等于备份快照。
//	D. BR 全量闭环：模拟灾难（DROP TABLE）→ RESTORE FROM '<path>'（无 TO LSN），
//	   校验表结构、行数、主键抽样、二级索引可用性、备份点后数据不存在。
//
// 任一步校验失败立即报错并以非零退出码结束（脚本可据此判定演练失败）。
// 输出：控制台摘要 + Markdown 结果文件（-out，默认 tests/bench/docs/br_pitr_drill_result.md）。
//
// 运行（仓库根目录，需 RocksDB CGO 环境，见 scripts/build.ps1）：
//
//	go run ./tests/bench/br_pitr_drill -dir br_pitr_data -out tests/bench/docs/br_pitr_drill_result.md
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

var (
	dir       = flag.String("dir", "br_pitr_data", "演练数据目录（自动 init；复用旧目录时自动 DROP 演练表，保持干净基线）")
	backup    = flag.String("backup", "", "备份文件路径（默认 <dir>/drill_snapshot.bak）")
	out       = flag.String("out", "tests/bench/docs/br_pitr_drill_result.md", "演练结果 Markdown 输出路径（留空则不写文件）")
	rows      = flag.Int("rows", 1000, "阶段1 数据行数（全量备份时的快照行数）")
	pitrRows  = flag.Int("pitr-rows", 30, "PITR 阶段在备份点后逐条写入的行数")
	tableName = "drill_users"
)

func main() {
	flag.Parse()
	if *rows <= 0 || *pitrRows <= 0 {
		fmt.Fprintln(os.Stderr, "rows and pitr-rows must be > 0")
		os.Exit(2)
	}

	// 数据目录：自动 init（可复用），演练表先 DROP 保证干净基线。
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

	// PITR 依赖 binlog：启用主节点复制（监听 0 端口，仅生成 binlog 文件）。
	if err := d.StartReplication("127.0.0.1:0"); err != nil {
		fmt.Fprintf(os.Stderr, "start replication (binlog): %v\n", err)
		os.Exit(2)
	}
	e := d.SQL

	backupPath := *backup
	if backupPath == "" {
		backupPath = filepath.ToSlash(filepath.Join(*dir, "drill_snapshot.bak"))
	}

	md, err := runDrill(e, drillConfig{
		table:     tableName,
		backup:    backupPath,
		rows:      *rows,
		pitrRows:  *pitrRows,
		startedAt: time.Now(),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "BR/PITR drill FAILED:", err)
		os.Exit(1)
	}

	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir out: %v\n", err)
			os.Exit(2)
		}
		if err := os.WriteFile(*out, []byte(md), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write out: %v\n", err)
			os.Exit(2)
		}
		fmt.Println("result written:", *out)
	}
	fmt.Println("BR_PITR_DRILL DONE")
}

// drillConfig 演练参数。
type drillConfig struct {
	table     string
	backup    string
	rows      int
	pitrRows  int
	binlogLSN func() (uint64, error) // 当前 binlog 最大位点（PITR 回放边界）
	startedAt time.Time
}

// runDrill 执行完整演练闭环；任一步校验失败返回 error（含失败点描述）。
// 供 main 与 tests/bench/br_pitr_drill/drill_test.go 复用（测试传临时目录与较小数据量）。
func runDrill(e *sql.Engine, cfg drillConfig) (string, error) {
	var report strings.Builder
	step := func(name string) { fmt.Fprintf(&report, "- %s\n", name) }
	fail := func(name string, err error) (string, error) {
		fmt.Fprintf(&report, "  ✗ FAIL: %v\n", err)
		return report.String(), fmt.Errorf("%s: %w", name, err)
	}

	report.WriteString(fmt.Sprintf("# OpenXDB BR / PITR 自动化演练结果\n\n"))
	report.WriteString(fmt.Sprintf("- 运行时间：%s\n", time.Now().Format("2006-01-02 15:04:05")))
	report.WriteString(fmt.Sprintf("- 环境：%s / %s，go %s\n", runtime.GOOS, runtime.GOARCH, runtime.Version()))
	report.WriteString(fmt.Sprintf("- 参数：rows=%d, pitr-rows=%d, table=%s, backup=%s\n", cfg.rows, cfg.pitrRows, cfg.table, cfg.backup))
	report.WriteString(fmt.Sprintf("- 链路：RocksDB + WAL + binlog（真实 db.Open 路径），全量备份=一致快照逻辑备份，PITR=备份快照+binlog 回放\n\n"))

	// ---- A. 准备：建表 + 阶段1数据 ----
	report.WriteString("## A. 准备（建表 + 阶段1数据）\n\n")
	_, _ = e.Execute("DROP TABLE " + cfg.table) // 复用目录时清基线，不存在可忽略
	if _, err := e.Execute(fmt.Sprintf("CREATE TABLE %s (id INT, name TEXT, age INT, PRIMARY KEY (id))", cfg.table)); err != nil {
		return fail("create table", err)
	}
	if _, err := e.Execute(fmt.Sprintf("CREATE INDEX idx_%s_age ON %s (age)", cfg.table, cfg.table)); err != nil {
		return fail("create index", err)
	}
	for i := 1; i <= cfg.rows; i++ {
		if _, err := e.Execute(fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES (%d, 'u%d', %d)", cfg.table, i, i, i)); err != nil {
			return fail("stage1 insert", err)
		}
	}
	step(fmt.Sprintf("建表+索引完成，阶段1 写入 %d 行（id 1..%d，age=i 唯一）", cfg.rows, cfg.rows))

	// ---- B. 全量备份 ----
	report.WriteString("\n## B. 全量备份（BACKUP TO）\n\n")
	backRes, err := e.Execute("BACKUP TO '" + cfg.backup + "'")
	if err != nil {
		return fail("backup", err)
	}
	if len(backRes.Rows) != 1 {
		return fail("backup result", fmt.Errorf("expect 1 row, got %d", len(backRes.Rows)))
	}
	backupRows := backRes.Rows[0][2].I
	backupLSN := backRes.Rows[0][3].I
	if backupRows != int64(cfg.rows) {
		return fail("backup rows", fmt.Errorf("expect %d, got %d", cfg.rows, backupRows))
	}
	if backupLSN <= 0 {
		return fail("backup lsn", fmt.Errorf("expect > 0, got %d", backupLSN))
	}
	step(fmt.Sprintf("备份文件：%s（tables=%d rows=%d backup_lsn=%d）", cfg.backup, backRes.Rows[0][1].I, backupRows, backupLSN))

	// ---- C. PITR：备份点后逐条写入 + TO LSN 精确回放 ----
	report.WriteString("\n## C. PITR（RESTORE ... TO LSN 精确回放）\n\n")
	lsns := make([]uint64, 0, cfg.pitrRows)
	base := int64(cfg.rows)
	for i := 1; i <= cfg.pitrRows; i++ {
		id := base + int64(i)
		if _, err := e.Execute(fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES (%d, 'pitr-%d', %d)", cfg.table, id, id, id)); err != nil {
			return fail("pitr insert", err)
		}
		lsn, err := cfg.binlogLSN()
		if err != nil {
			return fail("pitr binlog lsn", err)
		}
		lsns = append(lsns, lsn)
	}
	step(fmt.Sprintf("备份点后逐条写入 %d 行（id %d..%d），记录每条提交后 LSN", cfg.pitrRows, base+1, base+int64(cfg.pitrRows)))

	// 校验点：覆盖中间与末尾，确保回放"部分记录"与"全部记录"都精确。
	checkpoints := []int{}
	for _, k := range []int{1, 2, 3} {
		if cfg.pitrRows/k >= 2 && cfg.pitrRows/k < cfg.pitrRows {
			checkpoints = append(checkpoints, cfg.pitrRows/k)
		}
	}
	checkpoints = append(checkpoints, cfg.pitrRows)
	seen := map[int]bool{}
	for _, k := range checkpoints {
		if !seen[k] {
			seen[k] = true
			toLSN := lsns[k-1]
			if _, err := e.Execute(fmt.Sprintf("RESTORE FROM '%s' TO LSN %d", cfg.backup, toLSN)); err != nil {
				return fail(fmt.Sprintf("pitr restore to LSN %d", toLSN), err)
			}
			if err := checkCount(e, cfg.table, base+int64(k)); err != nil {
				return fail(fmt.Sprintf("pitr checkpoint k=%d (LSN %d)", k, toLSN), err)
			}
			if err := checkID(e, cfg.table, base+int64(k), true); err != nil {
				return fail(fmt.Sprintf("pitr checkpoint k=%d missing last row", k), err)
			}
			if err := checkID(e, cfg.table, base+int64(k)+1, false); err != nil {
				return fail(fmt.Sprintf("pitr checkpoint k=%d leaked row beyond LSN", k), err)
			}
			step(fmt.Sprintf("TO LSN %d（回放前 %d 条）：行数=%d（阶段1 %d + 回放 %d），末行存在，下一行不存在 ✓", toLSN, k, base+int64(k), cfg.rows, k))
		}
	}
	// TO LSN = 备份点：等于备份快照（只含阶段1）。
	if _, err := e.Execute(fmt.Sprintf("RESTORE FROM '%s' TO LSN %d", cfg.backup, backupLSN)); err != nil {
		return fail("pitr restore to backup lsn", err)
	}
	if err := checkCount(e, cfg.table, int64(cfg.rows)); err != nil {
		return fail("pitr to backup lsn count", err)
	}
	if err := checkID(e, cfg.table, base+1, false); err != nil {
		return fail("pitr to backup lsn leaked row", err)
	}
	step(fmt.Sprintf("TO LSN %d（=备份点）：行数=%d，无备份点后数据 ✓", backupLSN, cfg.rows))

	// ---- D. BR 全量闭环：灾难恢复 ----
	report.WriteString("\n## D. BR 全量闭环（灾难恢复 RESTORE FROM）\n\n")
	// 灾难：备份点后写入若干新数据并 DROP 表（模拟误删/损坏）。
	for i := 1; i <= cfg.pitrRows; i++ {
		id := base + int64(i)
		if _, err := e.Execute(fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES (%d, 'post-%d', %d)", cfg.table, id, id, id)); err != nil {
			return fail("disaster insert", err)
		}
	}
	if _, err := e.Execute("DROP TABLE " + cfg.table); err != nil {
		return fail("disaster drop table", err)
	}
	step(fmt.Sprintf("模拟灾难：备份点后写入 %d 行并 DROP TABLE %s", cfg.pitrRows, cfg.table))

	if _, err := e.Execute("RESTORE FROM '" + cfg.backup + "'"); err != nil {
		return fail("restore", err)
	}
	if err := checkCount(e, cfg.table, int64(cfg.rows)); err != nil {
		return fail("restore count", err)
	}
	if err := checkID(e, cfg.table, 1, true); err != nil {
		return fail("restore first row", err)
	}
	if err := checkID(e, cfg.table, int64(cfg.rows), true); err != nil {
		return fail("restore last row", err)
	}
	if err := checkID(e, cfg.table, base+1, false); err != nil {
		return fail("restore leaked post-backup row", err)
	}
	// 二级索引可用性：age=i 唯一，恢复后索引键应重建可用。
	if err := checkIndex(e, cfg.table, int64(cfg.rows), int64(cfg.rows)); err != nil {
		return fail("restore index query", err)
	}
	step(fmt.Sprintf("RESTORE 完成：行数=%d，首末主键存在，备份点后数据不存在，二级索引查询命中 ✓", cfg.rows))

	report.WriteString("\n## 结论\n\n")
	report.WriteString("- 全量备份（BR）：一致快照逻辑备份可完整还原表结构/数据/二级索引，幂等覆盖恢复（先 DROP 再建）。\n")
	report.WriteString("- 时间点恢复（PITR）：备份快照 + binlog 回放，TO LSN 精确控制回放边界（部分/全部/备份点本身），行集逐级精确对应。\n")
	report.WriteString(fmt.Sprintf("- 演练通过：全部 %d 项断言成功，总耗时 %s。\n", countSteps(report.String()), time.Since(cfg.startedAt).Round(time.Millisecond)))
	return report.String(), nil
}

func countSteps(s string) int {
	return strings.Count(s, " ✓")
}

// checkCount 断言表行数。
func checkCount(e *sql.Engine, table string, want int64) error {
	res, err := e.Execute(fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	if err != nil {
		return err
	}
	if len(res.Rows) != 1 || res.Rows[0][0].I != want {
		return fmt.Errorf("COUNT(*)=%v, want %d", rowVal(res), want)
	}
	return nil
}

// checkID 断言主键 id 存在（want=true）或不存在（want=false）。
func checkID(e *sql.Engine, table string, id int64, want bool) error {
	res, err := e.Execute(fmt.Sprintf("SELECT id FROM %s WHERE id = %d", table, id))
	if err != nil {
		return err
	}
	got := len(res.Rows) == 1
	if got != want {
		if want {
			return fmt.Errorf("row id=%d missing", id)
		}
		return fmt.Errorf("row id=%d unexpectedly present", id)
	}
	return nil
}

// checkIndex 断言二级索引可用：SELECT id FROM t WHERE age = ageVal 精确命中 id。
func checkIndex(e *sql.Engine, table string, ageVal, wantID int64) error {
	res, err := e.Execute(fmt.Sprintf("SELECT id FROM %s WHERE age = %d", table, ageVal))
	if err != nil {
		return err
	}
	if len(res.Rows) != 1 || res.Rows[0][0].I != wantID {
		return fmt.Errorf("index query age=%d: rows=%v, want id=%d", ageVal, rowVal(res), wantID)
	}
	return nil
}

func rowVal(res *sql.Result) string {
	parts := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts = append(parts, fmt.Sprintf("%v", r))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

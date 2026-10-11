// M14 生态接入：openxdb import —— mysqldump 兼容导入。
//
// 复用 pkg/sql 已实现的 IMPORT MYSQLDUMP 能力（pkg/import 解析 + pkg/sql 执行器），
// 把 mysqldump 生成的 SQL dump 文件解析并灌入数据目录对应数据库，输出统计。
// 与 M14 子功能二 binlog 订阅形成迁移闭环：先 import 全量存量，再订阅增量变更。
//
// 用法：openxdb import --data-dir <dir> --dump <file> [--batch <n>] [--ignore-errors]
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// hasFlag 判断 args 中是否出现无值开关（如 --ignore-errors）。
func hasFlag(args []string, name string) bool {
	for _, a := range args {
		if a == name {
			return true
		}
	}
	return false
}

// cmdImport 执行 mysqldump 兼容导入。
func cmdImport(args []string) error {
	dir, err := needDataDir(args)
	if err != nil {
		return err
	}
	dump, ok := flagValue(args, "--dump")
	if !ok || dump == "" {
		return errors.New("missing --dump <file>")
	}
	if fi, err := os.Stat(dump); err != nil {
		return fmt.Errorf("dump file: %w", err)
	} else if fi.IsDir() {
		return errors.New("--dump must be a file: " + dump)
	}

	stmt := "IMPORT MYSQLDUMP FROM '" + dump + "'"
	if b, ok := flagValue(args, "--batch"); ok && b != "" {
		n, err := strconv.Atoi(b)
		if err != nil || n <= 0 {
			return errors.New("invalid --batch value: " + b + " (正整数)")
		}
		stmt += " BATCH " + strconv.Itoa(n)
	}
	if hasFlag(args, "--ignore-errors") {
		stmt += " IGNORE ERRORS"
	}

	d, err := db.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	res, err := d.SQL.Execute(stmt)
	if err != nil {
		return err
	}
	// IMPORT MYSQLDUMP 返回单行统计：tables / inserted / skipped / errors / batches。
	if len(res.Rows) != 1 || len(res.Columns) != 5 {
		return fmt.Errorf("unexpected import result shape: cols=%v rows=%d", res.Columns, len(res.Rows))
	}
	nums := make([]int64, 5)
	for i, c := range res.Columns {
		if res.Rows[0][i].Kind != "INT" {
			return fmt.Errorf("import stat %s not int: %+v", c, res.Rows[0][i])
		}
		nums[i] = res.Rows[0][i].I
	}
	fmt.Printf("imported %s -> tables=%d inserted=%d skipped=%d errors=%d batches=%d\n",
		dump, nums[0], nums[1], nums[2], nums[3], nums[4])
	return nil
}

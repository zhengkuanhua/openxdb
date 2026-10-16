// tmp_mini: 临时最小复现程序（定位 G 场景崩溃点，非基准产物）。
package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

func main() {
	dir := "tests/bench/tmp_mini_data"
	if err := db.Init(dir); err != nil && !errors.Is(err, db.ErrAlreadyInitialized) {
		fmt.Fprintf(os.Stderr, "init: %v\n", err)
		os.Exit(2)
	}
	d, err := db.Open(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(2)
	}
	defer d.Close()
	e := d.SQL
	if _, err := e.Execute("CREATE TABLE oltp_t (id INT, name TEXT, score INT, PRIMARY KEY (id))"); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			fmt.Fprintf(os.Stderr, "create: %v\n", err)
			os.Exit(2)
		}
	}
	// 清空可能残留数据（重建语义：删旧表后重建）
	if _, err := e.Execute("DROP TABLE oltp_t"); err == nil {
		if _, err := e.Execute("CREATE TABLE oltp_t (id INT, name TEXT, score INT, PRIMARY KEY (id))"); err != nil {
			fmt.Fprintf(os.Stderr, "recreate: %v\n", err)
			os.Exit(2)
		}
	}
	// 预装载 10000 行（多行 100/条）
	for i := 0; i < 10000; i += 100 {
		vals := "(" + fmt.Sprintf("%d,'n-%d',%d", i, i, i%1000) + ")"
		for j := 1; j < 100; j++ {
			vals += ",(" + fmt.Sprintf("%d,'n-%d',%d", i+j, i+j, (i+j)%1000) + ")"
		}
		if _, err := e.Execute("INSERT INTO oltp_t VALUES " + vals); err != nil {
			fmt.Fprintf(os.Stderr, "preload %d: %v\n", i, err)
			os.Exit(2)
		}
	}
	fmt.Println("preload done")

	// 步骤 1: BEGIN + COMMIT 空事务
	sess := e.NewSession()
	if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
		fmt.Println("begin err:", err)
		os.Exit(2)
	}
	if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
		fmt.Println("commit err:", err)
		os.Exit(2)
	}
	fmt.Println("step1 begin/commit ok")

	// 步骤 2: BEGIN + SELECT 点查 + COMMIT
	if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
		fmt.Println("begin err:", err)
		os.Exit(2)
	}
	if _, err := e.ExecuteAsSession("SELECT * FROM oltp_t WHERE id = 5", sql.SuperUser, sess); err != nil {
		fmt.Println("sel err:", err)
		os.Exit(2)
	}
	if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
		fmt.Println("commit err:", err)
		os.Exit(2)
	}
	fmt.Println("step2 select ok")

	// 步骤 3: BEGIN + INSERT + COMMIT
	if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
		fmt.Println("begin err:", err)
		os.Exit(2)
	}
	if _, err := e.ExecuteAsSession("INSERT INTO oltp_t VALUES (20000, 'x', 1)", sql.SuperUser, sess); err != nil {
		fmt.Println("ins err:", err)
		os.Exit(2)
	}
	if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
		fmt.Println("commit err:", err)
		os.Exit(2)
	}
	fmt.Println("step3 insert ok")

	// 步骤 4: BEGIN + UPDATE + COMMIT
	if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
		fmt.Println("begin err:", err)
		os.Exit(2)
	}
	if _, err := e.ExecuteAsSession("UPDATE oltp_t SET score = 7 WHERE id = 5", sql.SuperUser, sess); err != nil {
		fmt.Println("upd err:", err)
		os.Exit(2)
	}
	if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
		fmt.Println("commit err:", err)
		os.Exit(2)
	}
	fmt.Println("step4 update ok")

	// 步骤 5: BEGIN + 范围查 + COMMIT
	if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
		fmt.Println("begin err:", err)
		os.Exit(2)
	}
	if _, err := e.ExecuteAsSession("SELECT * FROM oltp_t WHERE id >= 9000 AND id < 9100", sql.SuperUser, sess); err != nil {
		fmt.Println("scan err:", err)
		os.Exit(2)
	}
	if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
		fmt.Println("commit err:", err)
		os.Exit(2)
	}
	fmt.Println("step5 scan ok")

	// 步骤 6: 循环混合事务 50 次
	t0 := time.Now()
	for i := 0; i < 50; i++ {
		if _, err := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess); err != nil {
			fmt.Println("loop begin err:", i, err)
			os.Exit(2)
		}
		if _, err := e.ExecuteAsSession(fmt.Sprintf("SELECT * FROM oltp_t WHERE id = %d", i%10000), sql.SuperUser, sess); err != nil {
			fmt.Println("loop sel err:", i, err)
			os.Exit(2)
		}
		if _, err := e.ExecuteAsSession(fmt.Sprintf("INSERT INTO oltp_t VALUES (%d, 'x', 1)", 30000+i), sql.SuperUser, sess); err != nil {
			fmt.Println("loop ins err:", i, err)
			os.Exit(2)
		}
		if _, err := e.ExecuteAsSession(fmt.Sprintf("UPDATE oltp_t SET score = 3 WHERE id = %d", i%10000), sql.SuperUser, sess); err != nil {
			fmt.Println("loop upd err:", i, err)
			os.Exit(2)
		}
		if _, err := e.ExecuteAsSession(fmt.Sprintf("SELECT * FROM oltp_t WHERE id >= %d AND id < %d", 9500, 9600), sql.SuperUser, sess); err != nil {
			fmt.Println("loop scan err:", i, err)
			os.Exit(2)
		}
		if _, err := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess); err != nil {
			fmt.Println("loop commit err:", i, err)
			os.Exit(2)
		}
	}
	fmt.Println("step6 loop done in", time.Since(t0))
	fmt.Println("MINI ALL OK")
}

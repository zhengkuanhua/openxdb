// oxsql —— OpenXDB 交互式命令行客户端（D7.5 / G2）。
//
// 通过 MySQL wire 协议连接 OpenXDB --mysql-port 服务端口，提供品牌提示符
// oxsql> 的 REPL（第三方 mysql CLI 提示符固定在客户端侧，无法定制）。
//
// 用法：
//
//	oxsql [-h 127.0.0.1] [-P 3306] [-u root] [-p] [-c "SQL"]
//	  -c 指定单条 SQL 执行后退出（脚本 / 冒烟测试友好）；
//	  无 -c 时进入交互模式：逐条执行 SQL，\q / exit / quit 退出。
//
// 示例：
//
//	oxsql -h 127.0.0.1 -P 3306 -u root -c "select version()"
//	oxsql -P 3306
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/mysqlproto"
)

const prompt = "oxsql> "

func main() {
	host := flag.String("h", "127.0.0.1", "server host")
	port := flag.Int("P", 3306, "server MySQL wire port")
	user := flag.String("u", "root", "user name")
	askPass := flag.Bool("p", false, "prompt for password")
	once := flag.String("c", "", "run single SQL and exit")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: oxsql [-h host] [-P port] [-u user] [-p] [-c SQL]\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	password := ""
	if *askPass {
		fmt.Fprint(os.Stderr, "Enter password: ")
		b, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		password = strings.TrimRight(b, "\r\n")
	}

	addr := fmt.Sprintf("%s:%d", *host, *port)
	c, err := mysqlproto.Dial(addr, *user, password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "oxsql: connect %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Printf("Connected to %s, server version: %s\n", addr, c.ServerVersion())

	if *once != "" {
		if err := runSQL(c, *once); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	repl(c)
}

// repl 交互循环：打印 oxsql> 提示符，逐条执行 SQL。
func repl(c *mysqlproto.Client) {
	sc := bufio.NewScanner(os.Stdin)
	fmt.Print(prompt)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			fmt.Print(prompt)
			continue
		}
		switch strings.ToLower(line) {
		case "\\q", "exit", "quit":
			return
		}
		if err := runSQL(c, line); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		fmt.Print(prompt)
	}
}

// runSQL 执行单条 SQL 并打印结果集 / OK 信息。
func runSQL(c *mysqlproto.Client, sql string) error {
	cols, rows, err := c.Query(sql)
	if err != nil {
		return err
	}
	if cols == nil {
		fmt.Println("Query OK")
		return nil
	}
	fmt.Println(strings.Join(cols, "\t"))
	for _, r := range rows {
		fmt.Println(strings.Join(r, "\t"))
	}
	fmt.Printf("%d row(s) in set\n", len(rows))
	return nil
}

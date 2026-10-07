// Command openxdb 是 OpenXDB 数据库的入口命令（M1，v2.0-FP）。
// 子命令：version / init / repl / start。
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/server"
)

const version = "v0.1.0-alpha"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "version":
		fmt.Printf("OpenXDB %s (M1 single-node)\n", version)
		return
	case "init":
		err = cmdInit(os.Args[2:])
	case "repl":
		err = cmdREPL(os.Args[2:])
	case "start":
		err = cmdStart(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "openxdb:", err)
		os.Exit(1)
	}
}

// flagValue 从 args 中取 --name <value>，找不到返回 ("", false)。
func flagValue(args []string, name string) (string, bool) {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == name {
			return args[i+1], true
		}
	}
	return "", false
}

func needDataDir(args []string) (string, error) {
	dir, ok := flagValue(args, "--data-dir")
	if !ok || dir == "" {
		return "", errors.New("missing --data-dir <dir>")
	}
	return dir, nil
}

func cmdInit(args []string) error {
	dir, err := needDataDir(args)
	if err != nil {
		return err
	}
	if err := db.Init(dir); err != nil {
		if errors.Is(err, db.ErrAlreadyInitialized) {
			return errors.New("data directory already initialized: " + dir)
		}
		return err
	}
	fmt.Printf("initialized OpenXDB data directory: %s\n", dir)
	return nil
}

func cmdREPL(args []string) error {
	dir, err := needDataDir(args)
	if err != nil {
		return err
	}
	d, err := db.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	fmt.Printf("OpenXDB %s REPL (data dir: %s), type HELP for commands.\n", version, dir)
	s := server.New(d.Txn)
	s.SQL = d.SQL
	return s.REPL(os.Stdin, os.Stdout)
}

func cmdStart(args []string) error {
	dir, err := needDataDir(args)
	if err != nil {
		return err
	}
	port := 7788
	if p, ok := flagValue(args, "--port"); ok {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid --port value: " + p)
		}
		port = n
	}
	d, err := db.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("OpenXDB %s serving on %s (data dir: %s)\n", version, addr, dir)
	s := server.New(d.Txn)
	s.SQL = d.SQL
	return s.ServeTCP(addr)
}

func usage() {
	fmt.Println(`Usage: openxdb <command> [options]

Commands:
  version                   打印版本
  init    --data-dir <dir>    初始化数据目录
  repl    --data-dir <dir>    本地交互式命令行
  start   --data-dir <dir>    启动 TCP 服务
          [--port <n>]        监听端口（默认 7788）`)
}

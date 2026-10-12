// Command openxdb 是 OpenXDB 数据库的入口命令（M1，v2.0-FP）。
// 子命令：version / init / repl / start。
package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/metrics"
	"github.com/zhengkuanhua/openxdb/pkg/server"
)

const version = "v0.20.0-alpha"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "version":
		err = cmdVersion(os.Args[2:])
	case "preflight":
		err = cmdPreflight(os.Args[2:])
	case "init":
		err = cmdInit(os.Args[2:])
	case "repl":
		err = cmdREPL(os.Args[2:])
	case "start":
		err = cmdStart(os.Args[2:])
	case "replica":
		err = cmdReplica(os.Args[2:])
	case "doctor":
		err = cmdDoctor(os.Args[2:])
	case "stats":
		err = cmdStats(os.Args[2:])
	case "status":
		err = cmdStatus(os.Args[2:])
	case "import":
		err = cmdImport(os.Args[2:])
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
	// M16-2：--shutdown-timeout 命令行覆盖停机超时（秒），优先级高于
	// 持久化配置 shutdown_timeout；不传时由 SQL 引擎按 内存>持久化>默认 取。
	shutdownTimeoutFlag := int64(-1)
	if p, ok := flagValue(args, "--shutdown-timeout"); ok {
		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 {
			return errors.New("invalid --shutdown-timeout value: " + p)
		}
		shutdownTimeoutFlag = int64(n)
	}
	d, err := db.Open(dir)
	if err != nil {
		return err
	}
	shutdownClosed := false
	defer func() {
		if !shutdownClosed {
			_ = d.Close()
		}
	}()
	// M2：主节点复制（可选）。--replica-port 非 0 时监听复制端口并生成 binlog。
	if rp, ok := flagValue(args, "--replica-port"); ok {
		n, err := strconv.Atoi(rp)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid --replica-port value: " + rp)
		}
		if err := d.StartReplication(fmt.Sprintf(":%d", n)); err != nil {
			return err
		}
		fmt.Printf("OpenXDB %s replication listening on :%d (binlog: %s)\n", version, n, db.BinlogFile)
	}
	// M4：集群节点链路（可选）。--cluster-addr 非空时监听节点握手/转发端口。
	if ca, ok := flagValue(args, "--cluster-addr"); ok && ca != "" {
		realAddr, err := d.StartCluster(ca)
		if err != nil {
			return err
		}
		fmt.Printf("OpenXDB %s cluster node %s listening on %s (node id: %s)\n",
			version, d.Cluster.SelfID, realAddr, d.Cluster.SelfID)
	}
	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("OpenXDB %s serving on %s (data dir: %s)\n", version, addr, dir)
	s := server.New(d.Txn)
	s.SQL = d.SQL
	// M11 生产接线（P0-3）：连接鉴权装配。
	// --auth 启用 TCP 连接握手鉴权（AUTH <user> <password>）；未指定时保持
	// 旧行为（无鉴权，会话默认超级用户 root），向后兼容。
	// --auth-allow-legacy 允许无凭证旧客户端以降级 guest 身份继续使用连接
	// （默认 false = 强制鉴权，未鉴权连接除 AUTH/PING/HELP/QUIT 外一律拒绝）。
	// root 凭据：--root-password <pwd> 优先，其次环境变量 OPENXDB_ROOT_PASSWORD；
	// 启用鉴权且用户表尚无 root 时自动执行 CREATE USER 'root' 完成引导初始化。
	authOn := false
	for _, a := range args {
		if a == "--auth" {
			authOn = true
		}
	}
	allowLegacy := false
	for _, a := range args {
		if a == "--auth-allow-legacy" {
			allowLegacy = true
		}
	}
	if authOn {
		rootPwd := ""
		if p, ok := flagValue(args, "--root-password"); ok {
			rootPwd = p
		} else {
			rootPwd = os.Getenv("OPENXDB_ROOT_PASSWORD")
		}
		if rootPwd == "" {
			fmt.Fprintln(os.Stderr, "openxdb: WARN --auth enabled without --root-password/OPENXDB_ROOT_PASSWORD: 仅可通过既有 SQL 用户完成 AUTH")
		} else {
			okAuth, err := d.SQL.AuthenticateUser("root", rootPwd)
			if err != nil {
				return fmt.Errorf("auth: check root user: %w", err)
			}
			if !okAuth {
				esc := strings.ReplaceAll(rootPwd, "'", "''")
				if _, err := d.SQL.Execute("CREATE USER 'root' IDENTIFIED BY '" + esc + "'"); err != nil {
					return fmt.Errorf("auth: bootstrap root user: %w", err)
				}
				fmt.Printf("auth: bootstrapped root user (password via --root-password/OPENXDB_ROOT_PASSWORD)\n")
			}
		}
		s.SetAuth(&server.AuthConfig{Engine: d.SQL, AllowLegacy: allowLegacy})
		fmt.Printf("authentication enabled (allow-legacy=%v)\n", allowLegacy)
	}
	// T20：注入运维统计源（连接数 / binlog 位点），供 SHOW STATS 输出实时指标。
	d.SQL.SetStatsSources(s.ActiveConns, d.BinlogLSN)
	// M13/M16-1：慢查询落盘装配（默认 data/slow.log，阈值 100ms 可由 SET 运行时调）。
	// 开关尊重持久化配置（SET PERSIST slow_query_log 的 intent；无记录时默认开启）。
	slowLogPath := filepath.Join(dir, "slow.log")
	slowLogOn := true
	if intent := d.SQL.PersistedSlowLogIntent(); intent >= 0 {
		slowLogOn = intent == 1
	}
	d.SQL.SetSlowLog(slowLogPath, slowLogOn)
	fmt.Printf("slow query log: %s (threshold %dms, use SHOW SLOWQUERIES)\n", slowLogPath, d.SQL.SlowQueryThreshold())
	// M13：metrics 导出装配（METRICS/STATUS/HTTP /metrics 共享注册表；
	// 注入连接数/慢查询阈值/存储大小/region 分布实时采集源）。
	m := metrics.New()
	m.SetConnSource(s.ActiveConns)
	m.SetSlowThresholdSource(d.SQL.SlowQueryThreshold)
	m.SetStorageBytesSource(func() int64 { return dirSize(dir) })
	m.SetRegionStatsSource(d.SQL.MetricsRegionStats)
	s.SetMetrics(m)
	d.SQL.SetMetrics(m)
	if mp, ok := flagValue(args, "--metrics-port"); ok {
		n, err := strconv.Atoi(mp)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid --metrics-port value: " + mp)
		}
		if n > 0 {
			go func() {
				_ = http.ListenAndServe(fmt.Sprintf(":%d", n), m.Handler())
			}()
			fmt.Printf("metrics endpoint: http://127.0.0.1:%d/metrics\n", n)
		}
	}
	// ---- M16-2 优雅停机装配 ----
	// 停机流程：拒绝新连接/新事务 → drain 进行中事务（超时强制）→
	// flush/关闭存储（txn/WAL/storage 由 db.Close 按序处理，退出码 0/1）。
	s.SetShutdownHook(func() error {
		shutdownClosed = true
		return d.Close()
	})
	// 停机超时来源：命令行 > SQL 引擎（内存>持久化>默认 30s）。
	s.ShutdownTimeout = func() time.Duration {
		if shutdownTimeoutFlag > 0 {
			return time.Duration(shutdownTimeoutFlag) * time.Second
		}
		return d.SQL.ShutdownTimeout()
	}
	s.Logf = log.Printf
	// SIGINT/SIGTERM 走同一优雅停机流程（默认超时，正常退出码 0，
	// 超时强制退出码 1）。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		code := runSignalLoop(sigCh, s, d.SQL.ShutdownTimeout)
		if code != 0 {
			fmt.Fprintf(os.Stderr, "openxdb: graceful shutdown forced exit (drain timeout)\n")
		}
		os.Exit(code)
	}()
	fmt.Printf("OpenXDB %s serving on %s (data dir: %s), Ctrl-C to stop.\n", version, addr, dir)
	if err := s.ServeTCP(addr); err != nil {
		return err
	}
	// ServeTCP 返回（SHUTDOWN 命令触发停机后监听器关闭）→ 等待收尾。
	s.WaitShutdown()
	if s.ForceExited() {
		return errors.New("graceful shutdown timed out, forced exit")
	}
	return nil
}

// runSignalLoop 等待 SIGINT/SIGTERM 并触发优雅停机（M16-2），返回进程
// 退出码：0=正常停机（drain 完成，存储已关闭），1=超时强制退出。
// sigCh 为 nil 时（测试路径）调用方已自行触发 GracefulShutdown。
func runSignalLoop(sigCh <-chan os.Signal, s *server.Server, timeout func() time.Duration) int {
	<-sigCh
	if err := s.GracefulShutdown(timeout()); err != nil {
		return 1
	}
	return 0
}

// cmdReplica 以从节点身份启动：连接主节点复制端口并异步应用 binlog。
func cmdReplica(args []string) error {
	dir, err := needDataDir(args)
	if err != nil {
		return err
	}
	master, ok := flagValue(args, "--master")
	if !ok || master == "" {
		return errors.New("missing --master <host:port>")
	}
	d, err := db.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.StartFollower(master); err != nil {
		return err
	}
	fmt.Printf("OpenXDB %s replica of %s (data dir: %s), Ctrl-C to stop.\n", version, master, dir)
	select {} // 阻塞等待；StartFollower 在后台 goroutine 持续复制
}

func usage() {
	fmt.Println(`Usage: openxdb <command> [options]

Commands:
  version                   打印版本（发行/构建/commit/数据格式版本）
  preflight --data-dir <dir> 升级前健康检查（PREFLIGHT CHECK，M16-3）
          [--target-version <ver>] 目标版本提示（升级兼容判断参考）
          [--json]            输出 JSON 报告（退出码：0=PASS 2=FAIL）
  init    --data-dir <dir>    初始化数据目录
  repl    --data-dir <dir>    本地交互式命令行
  start   --data-dir <dir>    启动 TCP 服务
          [--port <n>]        监听端口（默认 7788）
          [--replica-port <n>] 主节点复制端口（启用 M2 复制并生成 binlog）
          [--cluster-addr <addr>] 集群节点链路地址（启用 M4 节点握手/转发）
          [--metrics-port <n>] Prometheus /metrics 端口（默认 7789，0 禁用）
          [--shutdown-timeout <sec>] 优雅停机 drain 超时（默认取配置 shutdown_timeout，30s）
                              服务内发 SHUTDOWN [GRACEFUL] 或 Ctrl-C/SIGTERM 触发
          [--auth]            启用连接握手鉴权（AUTH <user> <password>）
          [--auth-allow-legacy] --auth 时允许无凭证客户端降级 guest（默认强制鉴权）
          [--root-password <pwd>] root 引导密码（无 root 用户时自动 CREATE USER；亦可经环境变量 OPENXDB_ROOT_PASSWORD 传入）
  replica --data-dir <dir> --master <host:port>
                             以从节点身份连接主节点并异步复制
  doctor  --data-dir <dir>   数据目录/本地服务健康巡检
          [--addr <host:port>] 在线服务检查（端口连通、慢查询等）
          [--json]            输出 JSON 报告（退出码：0=通过 1=警告 2=错误）
  stats   [--addr <host:port>] 拉取服务运行指标（默认 127.0.0.1:7788）
          [--interval <sec>]  二次采样计算 binlog 增长（默认单次）
          [--json]            输出 JSON 指标
  metrics [--addr <host:port>] 拉取 Prometheus 文本指标（默认 127.0.0.1:7788）
  status  [--addr <host:port>] 拉取聚合状态快照（默认 127.0.0.1:7788）
          [--json]            输出 JSON 状态快照
  import  --data-dir <dir> --dump <file>
                              M14 生态接入：mysqldump 兼容导入
          [--batch <n>]       每批行数提交一次事务（默认单原子事务）
          [--ignore-errors]   坏行/建表/执行错误跳过继续`)
}

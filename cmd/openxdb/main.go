// Command openxdb 是 OpenXDB 数据库的入口命令（M1，v2.0-FP）。
// 子命令：version / init / repl / start。
package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/configfile"
	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/logx"
	"github.com/zhengkuanhua/openxdb/pkg/metrics"
	"github.com/zhengkuanhua/openxdb/pkg/mysqlproto"
	"github.com/zhengkuanhua/openxdb/pkg/server"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
	"github.com/zhengkuanhua/openxdb/pkg/storage/rocksdb"
)

const version = "v1.0.0"

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
	case "config-check":
		err = cmdConfigCheck(os.Args[2:])
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
	case "backup":
		err = cmdBackup(os.Args[2:])
	case "gen-cert":
		err = cmdGenCert(os.Args[2:])
	case "metrics":
		err = cmdMetrics(os.Args[2:])
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

// loadTLSConfig 解析 --tls-cert/--tls-key 并加载服务端 TLS 配置（P0/T36）：
//   - 两者都未提供 → 返回 (nil, nil)，保持明文监听（向后兼容）；
//   - 仅提供一个 → 返回错误（必须成对提供）；
//   - 两者都提供 → 加载 X509 证书密钥对，校验证书有效期（T36：过期/未生效
//     拒绝启动，商用合规），返回非 nil *tls.Config。
func loadTLSConfig(args []string) (*tls.Config, error) {
	tlsCert, hasCert := flagValue(args, "--tls-cert")
	tlsKey, hasKey := flagValue(args, "--tls-key")
	if hasCert != hasKey {
		if hasCert {
			return nil, errors.New("--tls-cert provided but --tls-key missing (must provide both)")
		}
		return nil, errors.New("--tls-key provided but --tls-cert missing (must provide both)")
	}
	if !hasCert {
		return nil, nil
	}
	return loadTLSConfigFrom(tlsCert, tlsKey)
}

// loadTLSConfigFrom 从指定证书/私钥文件加载服务端 TLS 配置（T36）：
// 加载 X509 证书密钥对并校验有效期（NotBefore/NotAfter 覆盖当前时间），
// 过期或未生效返回明确错误拒绝启动（商用合规：过期证书不得静默运行）。
func loadTLSConfigFrom(tlsCert, tlsKey string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(tlsCert, tlsKey)
	if err != nil {
		return nil, fmt.Errorf("load TLS cert/key: %w", err)
	}
	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("TLS cert %s: empty certificate chain", tlsCert)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("TLS cert %s: parse certificate: %w", tlsCert, err)
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) {
		return nil, fmt.Errorf("TLS certificate %s not yet valid (NotBefore %s, now %s)",
			tlsCert, leaf.NotBefore.Format(time.RFC3339), now.Format(time.RFC3339))
	}
	if now.After(leaf.NotAfter) {
		return nil, fmt.Errorf("TLS certificate %s expired (NotAfter %s, now %s)",
			tlsCert, leaf.NotAfter.Format(time.RFC3339), now.Format(time.RFC3339))
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}

func needDataDir(args []string) (string, error) {
	dir, ok := flagValue(args, "--data-dir")
	if !ok || dir == "" {
		return "", errors.New("missing --data-dir <dir>")
	}
	return dir, nil
}

// serverKeys [server] 段白名单：启动参数 + 结构化日志/告警默认值。
// 存储/网络/TLS/备份等现有配置项归 [sql] 段（白名单来自 sql.ConfigKeys）。
var serverKeys = []string{
	"data_dir", "port", "replica_port", "metrics_port", "mysql_port", "shutdown_timeout",
	"cluster_addr", "log_level", "alert_enabled", "alert_file", "alert_webhook", "alert_keep",
}

// loadStartConfig 加载并校验统一配置文件（--config <path>，INI 风格）。
// 未提供 --config 时返回 (nil, nil)；校验失败（未知段/键、值格式非法）直接报错。
func loadStartConfig(args []string) (*configfile.File, error) {
	p, ok := flagValue(args, "--config")
	if !ok || p == "" {
		return nil, nil
	}
	f, err := configfile.Load(p)
	if err != nil {
		return nil, err
	}
	known := map[string]map[string]bool{
		"server": {},
		"sql":    {},
	}
	for _, k := range serverKeys {
		known["server"][k] = true
	}
	for _, k := range sql.ConfigKeys() {
		known["sql"][k] = true
	}
	if errs := configfile.Validate(f, known); len(errs) > 0 {
		return nil, errors.New("config " + p + " invalid: " + strings.Join(errs, "; "))
	}
	if errs := configfile.ValidateServerValues(f); len(errs) > 0 {
		return nil, errors.New("config " + p + " invalid: " + strings.Join(errs, "; "))
	}
	return f, nil
}

// cmdConfigCheck 校验配置文件：解析、未知段/键、服务端值格式，并回显有效配置。
func cmdConfigCheck(args []string) error {
	p, ok := flagValue(args, "--config")
	if !ok || p == "" {
		return errors.New("config-check requires --config <path>")
	}
	f, err := loadStartConfig(args)
	if err != nil {
		return err
	}
	fmt.Printf("config OK: %s\n", p)
	for _, name := range []string{"server", "sql"} {
		sec := f.Section(name)
		if sec == nil || len(sec.Order) == 0 {
			continue
		}
		fmt.Printf("[%s]\n", name)
		for _, k := range sec.Order {
			fmt.Printf("  %s = %s\n", k, sec.Vals[k])
		}
	}
	return nil
}

// escSQL 转义 SQL 字符串字面量（单引号翻倍），用于 SET 装配。
func escSQL(s string) string { return strings.ReplaceAll(s, "'", "''") }

// applySQLFileConfig 将配置文件 [sql] 段逐键通过 SET 装配到 SQL 引擎（内存级）。
// 优先级：持久化配置（persisted.json）之后、命令行参数之前。
func applySQLFileConfig(e *sql.Engine, f *configfile.File) error {
	sec := f.Section("sql")
	if sec == nil {
		return nil
	}
	for _, k := range sec.Order {
		if _, err := e.Execute("SET " + k + " = '" + escSQL(sec.Vals[k]) + "'"); err != nil {
			return fmt.Errorf("config file [sql] %s=%s: %w", k, sec.Vals[k], err)
		}
	}
	return nil
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
	// 统一配置文件（--config <path>，INI 风格）：集中管理启动参数与现有配置项；
	// 命令行参数优先级高于文件配置（CLI flag 覆盖 [server]/[sql] 段同名键）。
	cfgFile, err := loadStartConfig(args)
	if err != nil {
		return err
	}
	dir := ""
	if p, ok := flagValue(args, "--data-dir"); ok && p != "" {
		dir = p
	}
	if cfgFile != nil {
		if v, ok := cfgFile.Get("server", "data_dir"); ok && dir == "" {
			dir = v
		}
	}
	if dir == "" {
		return errors.New("missing --data-dir <dir> or [server] data_dir in config file")
	}
	port := 7788
	if cfgFile != nil {
		if v, ok := cfgFile.Get("server", "port"); ok {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > 65535 {
				return errors.New("invalid [server] port value: " + v)
			}
			port = n
		}
	}
	if p, ok := flagValue(args, "--port"); ok {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid --port value: " + p)
		}
		port = n
	}
	replicaPort := 0
	if cfgFile != nil {
		if v, ok := cfgFile.Get("server", "replica_port"); ok {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 || n > 65535 {
				return errors.New("invalid [server] replica_port value: " + v)
			}
			replicaPort = n
		}
	}
	if p, ok := flagValue(args, "--replica-port"); ok {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid --replica-port value: " + p)
		}
		replicaPort = n
	}
	metricsPort := 0
	if cfgFile != nil {
		if v, ok := cfgFile.Get("server", "metrics_port"); ok {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 || n > 65535 {
				return errors.New("invalid [server] metrics_port value: " + v)
			}
			metricsPort = n
		}
	}
	if p, ok := flagValue(args, "--metrics-port"); ok {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid --metrics-port value: " + p)
		}
		metricsPort = n
	}
	// MySQL wire protocol 兼容端口（可选）：默认 0=关闭，保持既有行协议行为；
	// 配置 [server] mysql_port 或 --mysql-port <n> 开启后，MySQL 客户端
	//（mysql CLI / go-sql-driver / JDBC 等）可直接连接，共享同一 SQL 引擎。
	mysqlPort := 0
	if cfgFile != nil {
		if v, ok := cfgFile.Get("server", "mysql_port"); ok {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 || n > 65535 {
				return errors.New("invalid [server] mysql_port value: " + v)
			}
			mysqlPort = n
		}
	}
	if p, ok := flagValue(args, "--mysql-port"); ok {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid --mysql-port value: " + p)
		}
		mysqlPort = n
	}
	// M16-2：--shutdown-timeout 命令行覆盖停机超时（秒），优先级高于
	// 持久化配置 shutdown_timeout；不传时由 SQL 引擎按 内存>持久化>默认 取。
	shutdownTimeoutFlag := int64(-1)
	if cfgFile != nil {
		if v, ok := cfgFile.Get("server", "shutdown_timeout"); ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n <= 0 {
				return errors.New("invalid [server] shutdown_timeout value: " + v + " (expected positive integer seconds)")
			}
			shutdownTimeoutFlag = n
		}
	}
	if p, ok := flagValue(args, "--shutdown-timeout"); ok {
		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 {
			return errors.New("invalid --shutdown-timeout value: " + p)
		}
		shutdownTimeoutFlag = int64(n)
	}
	clusterAddr := ""
	if cfgFile != nil {
		if v, ok := cfgFile.Get("server", "cluster_addr"); ok {
			clusterAddr = v
		}
	}
	if ca, ok := flagValue(args, "--cluster-addr"); ok {
		clusterAddr = ca
	}
	// M8：中心化 TSO 发号节点（可选）。--tso-node <nodeID> 或 [server] tso_node
	// 非空时启动后把本节点时间戳源切换为指向该节点的远程 TSO 客户端（需集群
	// 模式且该节点已在注册表；取号时懒解析地址，未注册前不报错）。
	tsoNode := ""
	if cfgFile != nil {
		if v, ok := cfgFile.Get("server", "tso_node"); ok {
			tsoNode = v
		}
	}
	if tn, ok := flagValue(args, "--tso-node"); ok {
		tsoNode = tn
	}
	// 结构化日志级别与告警配置（[server] 段默认值）：db.Open 前的失败路径亦使用；
	// [sql] 段与 CLI 覆盖在 db.Open 后装配（见下）。
	if cfgFile != nil {
		if v, ok := cfgFile.Get("server", "log_level"); ok {
			lv, lerr := logx.ParseLevel(v)
			if lerr != nil {
				return errors.New("invalid [server] log_level value: " + v)
			}
			logx.Default.SetLevel(lv)
		}
		ac := logx.DefaultAlerts.Config()
		if v, ok := cfgFile.Get("server", "alert_enabled"); ok {
			b, berr := strconv.ParseBool(v)
			if berr != nil {
				return errors.New("invalid [server] alert_enabled value: " + v + " (expected true/false/1/0)")
			}
			ac.Enabled = b
		}
		if v, ok := cfgFile.Get("server", "alert_file"); ok {
			ac.File = v
		}
		if v, ok := cfgFile.Get("server", "alert_webhook"); ok {
			ac.Webhook = v
		}
		if v, ok := cfgFile.Get("server", "alert_keep"); ok {
			n, aerr := strconv.Atoi(v)
			if aerr != nil || n < 1 {
				return errors.New("invalid [server] alert_keep value: " + v + " (expected positive integer)")
			}
			ac.Keep = n
		}
		logx.DefaultAlerts.Configure(ac)
	}
	// P0/T36 TLS：主服务监听层加密（可选）。--tls-cert 与 --tls-key 必须同时提供，
	// 仅提供一个即报错；两者都提供时加载证书并用 TLS 监听；否则回退持久化
	// TLS intent（SET PERSIST tls_cert/tls_key，重启生效）；仍无则明文监听。
	// 命令行参数优先级高于持久化配置。复制端口（--replica-port）复用同一配置。
	tlsCfg, err := loadTLSConfig(args)
	if err != nil {
		return err
	}
	tlsAllowLegacy := hasFlag(args, "--tls-allow-legacy")
	if tlsCfg == nil {
		d0, derr := db.Open(dir)
		if derr != nil {
			return derr
		}
		pCert, pKey, pLegacy := d0.SQL.PersistedTLSConfig()
		if pCert != "" && pKey != "" {
			pc, lerr := loadTLSConfigFrom(pCert, pKey)
			if lerr != nil {
				_ = d0.Close()
				return lerr
			}
			tlsCfg = pc
			if !hasFlag(args, "--tls-allow-legacy") {
				tlsAllowLegacy = pLegacy
			}
		}
		_ = d0.Close()
	}
	d, err := db.Open(dir)
	if err != nil {
		return err
	}
	// 配置文件 [sql] 段：SET 装配到 SQL 引擎（内存级，覆盖持久化默认值）。
	// 优先级：持久化配置 < 配置文件 [sql] 段 < 命令行参数。
	if cfgFile != nil {
		if err := applySQLFileConfig(d.SQL, cfgFile); err != nil {
			_ = d.Close()
			return err
		}
	}
	// 命令行参数覆盖（最高优先级）：日志级别与告警开关/出口。
	if p, ok := flagValue(args, "--log-level"); ok {
		if _, err := d.SQL.Execute("SET log_level = '" + escSQL(p) + "'"); err != nil {
			_ = d.Close()
			return err
		}
	}
	if p, ok := flagValue(args, "--alert-enabled"); ok {
		if _, err := d.SQL.Execute("SET alert_enabled = '" + escSQL(p) + "'"); err != nil {
			_ = d.Close()
			return err
		}
	}
	if p, ok := flagValue(args, "--alert-file"); ok {
		if _, err := d.SQL.Execute("SET alert_file = '" + escSQL(p) + "'"); err != nil {
			_ = d.Close()
			return err
		}
	}
	if p, ok := flagValue(args, "--alert-webhook"); ok {
		if _, err := d.SQL.Execute("SET alert_webhook = '" + escSQL(p) + "'"); err != nil {
			_ = d.Close()
			return err
		}
	}
	if p, ok := flagValue(args, "--alert-keep"); ok {
		if _, err := d.SQL.Execute("SET alert_keep = '" + escSQL(p) + "'"); err != nil {
			_ = d.Close()
			return err
		}
	}
	shutdownClosed := false
	defer func() {
		if !shutdownClosed {
			_ = d.Close()
		}
	}()
	// M2：主节点复制（可选）。--replica-port 或配置文件 [server] replica_port
	// 非 0 时监听复制端口并生成 binlog；已配置 TLS 时复制端口同样加密监听。
	if replicaPort > 0 {
		if err := d.StartReplicationTLS(fmt.Sprintf(":%d", replicaPort), tlsCfg); err != nil {
			return err
		}
		fmt.Printf("OpenXDB %s replication listening on :%d (binlog: %s)\n", version, replicaPort, db.BinlogFile)
	}
	// M4：集群节点链路（可选）。--cluster-addr 或配置文件 [server] cluster_addr
	// 非空时监听节点握手/转发端口。
	if clusterAddr != "" {
		realAddr, err := d.StartCluster(clusterAddr)
		if err != nil {
			return err
		}
		fmt.Printf("OpenXDB %s cluster node %s listening on %s (node id: %s)\n",
			version, d.Cluster.SelfID, realAddr, d.Cluster.SelfID)
	}
	// M8：中心化 TSO（可选）。--tso-node 指向全局发号节点，其余节点挂载远程
	// TSO 客户端（懒解析地址，未注册前首次取号返回错误，ADD NODE 后自动可用）。
	if tsoNode != "" {
		if err := d.SetTSONode(tsoNode, 0); err != nil {
			return err
		}
		fmt.Printf("OpenXDB %s TSO source -> node %s (batch %d)\n", version, tsoNode, 64)
	}
	addr := fmt.Sprintf(":%d", port)
	s := server.New(d.Txn)
	s.SQL = d.SQL
	// P0/T36 TLS：装配监听层加密（--tls-cert/--tls-key 或持久化 tls_cert/tls_key
	// 生效时启用；否则 s.tlsConfig == nil，ServeTCP 保持明文监听）。
	// --tls-allow-legacy / 持久化 tls_allow_legacy=on 开启同端口明文降级并存。
	s.SetTLS(tlsCfg)
	if tlsCfg != nil {
		s.SetTLSLegacy(tlsAllowLegacy)
		if tlsAllowLegacy {
			fmt.Printf("TLS enabled (cert/key loaded, legacy plaintext allowed on same port)\n")
		} else {
			fmt.Printf("TLS enabled (cert/key loaded, plaintext rejected)\n")
		}
	}
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
	// BR 方向②：备份调度装配。CLI flag 显式提供时覆盖持久化配置
	//（SET PERSIST backup_*）；未提供时按 持久化>默认 生效。
	// --backup-interval 秒（0=关闭）开启周期自动 BACKUP；--backup-keep 保留份数
	//（0=不清理）；--backup-dir 输出目录（默认 <data>/backups）；--backup-remote-url
	// 远程 HTTP PUT 上传目标（可选，配合 openxdb backup push/pull 传输）。
	bsCfg := d.SQL.BackupScheduleStatus().Config
	backupDir := bsCfg.Dir
	if p, ok := flagValue(args, "--backup-dir"); ok && p != "" {
		backupDir = p
	}
	if backupDir == "" {
		backupDir = filepath.Join(dir, "backups")
	}
	if p, ok := flagValue(args, "--backup-interval"); ok {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return errors.New("invalid --backup-interval value: " + p + " (expected non-negative integer seconds)")
		}
		bsCfg.IntervalSec = int64(n)
	}
	if p, ok := flagValue(args, "--backup-keep"); ok {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return errors.New("invalid --backup-keep value: " + p + " (expected non-negative integer)")
		}
		bsCfg.Keep = int64(n)
	}
	if p, ok := flagValue(args, "--backup-remote-url"); ok {
		if p != "" && !strings.HasPrefix(p, "http://") && !strings.HasPrefix(p, "https://") {
			return errors.New("invalid --backup-remote-url value: " + p + " (expected http(s) URL or empty)")
		}
		bsCfg.RemoteURL = p
	}
	bsCfg.Dir = backupDir
	d.SQL.SetBackupScheduleConfig(bsCfg)
	if bsCfg.IntervalSec > 0 {
		d.SQL.StartBackupScheduler()
		remoteTxt := "none"
		if bsCfg.RemoteURL != "" {
			remoteTxt = bsCfg.RemoteURL
		}
		fmt.Printf("backup scheduler: every %ds -> %s (keep %d, remote %s, use SHOW BACKUP STATUS)\n",
			bsCfg.IntervalSec, bsCfg.Dir, bsCfg.Keep, remoteTxt)
	} else {
		fmt.Printf("backup scheduler: disabled (use --backup-interval <sec> / SET PERSIST backup_interval)\n")
	}
	// M13：metrics 导出装配（METRICS/STATUS/HTTP /metrics 共享注册表；
	// 注入连接数/慢查询阈值/存储大小/region 分布实时采集源）。
	m := metrics.New()
	m.SetConnSource(s.ActiveConns)
	m.SetSlowThresholdSource(d.SQL.SlowQueryThreshold)
	m.SetStorageBytesSource(func() int64 { return dirSize(dir) })
	m.SetRegionStatsSource(d.SQL.MetricsRegionStats)
	// RocksDB 内部统计采集源：LSM/SST/MemTable/Compaction 等指标对接监控看板；
	// 非 RocksDB 引擎时返回空快照（全 0），不影响整体渲染。
	m.SetRocksStatsSource(func() metrics.RocksStats {
		rs, err := rocksdb.StatsOf(d.Storage)
		if err != nil {
			return metrics.RocksStats{}
		}
		return metrics.RocksStats{
			NumLevels:             rs.NumLevels,
			NumFilesAtLevel:       rs.NumFilesAtLevel,
			CurSizeAllMemTables:   rs.CurSizeAllMemTables,
			SizeAllMemTables:      rs.SizeAllMemTables,
			CompactionPending:     rs.CompactionPending,
			NumRunningCompactions: rs.NumRunningCompactions,
			NumRunningFlushes:     rs.NumRunningFlushes,
			LiveSSTFilesSize:      rs.LiveSSTFilesSize,
			NumLiveVersions:       rs.NumLiveVersions,
			EstimateNumKeys:       rs.EstimateNumKeys,
		}
	})
	s.SetMetrics(m)
	d.SQL.SetMetrics(m)
	// T37：监控告警规则评估引擎装配（指标阈值 + 持续时长 + 严重级别；
	// 数据源与 Prometheus /metrics 同源 metrics.Registry）。启动评估循环，
	// 规则通过 SET/SET PERSIST alert_rules* 配置，SHOW ALERT RULES 查看。
	d.SQL.StartAlertRules()
	fmt.Printf("alert rules engine: started (use SET alert_rules_enabled=on, SHOW ALERT RULES)\n")
	if metricsPort > 0 {
		go func() {
			_ = http.ListenAndServe(fmt.Sprintf(":%d", metricsPort), m.Handler())
		}()
		fmt.Printf("metrics endpoint: http://127.0.0.1:%d/metrics\n", metricsPort)
	}
	// MySQL wire protocol 兼容监听（可选）：--mysql-port / [server] mysql_port>0
	// 时启用。共享同一 SQL 引擎；鉴权映射到既有 AUTH 模型（--auth 开启时用
	// Engine.AuthenticateUser 校验，未开启时放行且执行身份为 root）。TLS 配置
	// 与主服务同源（--tls-cert/--tls-key 或持久化 intent），明文连接则走
	// caching_sha2_password RSA 公钥加密路径。
	if mysqlPort > 0 {
		ms := mysqlproto.New(d.SQL)
		if authOn {
			ms.SetVerify(func(user, pass string) bool {
				ok, err := d.SQL.AuthenticateUser(user, pass)
				return err == nil && ok
			})
		}
		if tlsCfg != nil {
			ms.SetTLS(tlsCfg)
		}
		ms.Logf = func(format string, a ...any) {
			logx.Default.Info("mysqlproto", "mysql_log", fmt.Sprintf(format, a...))
		}
		mysqlAddr := fmt.Sprintf(":%d", mysqlPort)
		go func() {
			if err := ms.Serve(mysqlAddr); err != nil {
				fmt.Fprintf(os.Stderr, "openxdb: mysql protocol listener: %v\n", err)
			}
		}()
		proto := "plaintext"
		if tlsCfg != nil {
			proto = "TLS"
		}
		fmt.Printf("mysql wire protocol listening on %s (%s, auth=%v)\n", mysqlAddr, proto, authOn)
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
	// 结构化日志：server 内部日志统一走 logx JSON 行（module=server），
	// 输出至 stderr；级别受 [server]/SET log_level 动态控制。
	s.Logf = func(format string, a ...any) {
		logx.Default.Info("server", "server_log", fmt.Sprintf(format, a...))
	}
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

// cmdBackup 远程备份传输子命令：push 上传本地备份文件到远程目标
// （HTTP PUT，配合 --backup-remote-url 的 WebDAV/自定义端点）；
// pull 从远程目标下载备份到本地，再通过 RESTORE FROM '<path>' 恢复。
func cmdBackup(args []string) error {
	if len(args) < 1 {
		return errors.New("backup: missing subcommand (push|pull), see usage")
	}
	switch args[0] {
	case "push":
		path, ok := flagValue(args[1:], "--file")
		if !ok || path == "" {
			return errors.New("backup push: missing --file <path>")
		}
		url, ok := flagValue(args[1:], "--url")
		if !ok || url == "" {
			return errors.New("backup push: missing --url <http(s) url>")
		}
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			return errors.New("backup push: --url must be http(s) URL")
		}
		if err := sql.UploadBackupFile(path, url); err != nil {
			return fmt.Errorf("backup push: %w", err)
		}
		fmt.Printf("backup pushed: %s -> %s\n", path, url)
		return nil
	case "pull":
		url, ok := flagValue(args[1:], "--url")
		if !ok || url == "" {
			return errors.New("backup pull: missing --url <http(s) url>")
		}
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			return errors.New("backup pull: --url must be http(s) URL")
		}
		out, ok := flagValue(args[1:], "--out")
		if !ok || out == "" {
			return errors.New("backup pull: missing --out <path>")
		}
		if err := sql.DownloadBackupFile(url, out); err != nil {
			return fmt.Errorf("backup pull: %w", err)
		}
		fmt.Printf("backup pulled: %s -> %s (RESTORE FROM '<path>' TO 恢复)\n", url, out)
		return nil
	default:
		return errors.New("backup: unknown subcommand '" + args[0] + "' (expected push|pull)")
	}
}

// cmdReplica 以从节点身份启动：连接主节点复制端口并异步应用 binlog。
// 默认明文连接；--replica-tls 时以 TLS 连接（自签名/内网测试不校验服务器名）；
// --replica-tls-ca <path> 时以 TLS 连接并校验 CA 链与主机名（T36 verify-full，
// 商用默认；--master 主机名须与证书 CN/SAN 匹配）。
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
	var tlsCfg *tls.Config
	if caPath, hasCA := flagValue(args, "--replica-tls-ca"); hasCA {
		// verify-full：主机名取 --master 的 host 部分（--master 可为 host:port）。
		host := master
		if h, _, err := net.SplitHostPort(master); err == nil {
			host = h
		}
		tlsCfg, err = server.VerifyTLSConfig(caPath, host)
		if err != nil {
			return err
		}
	} else {
		for _, a := range args {
			if a == "--replica-tls" {
				tlsCfg = server.InsecureTLSConfig()
			}
		}
	}
	if err := d.StartFollowerTLS(master, tlsCfg); err != nil {
		return err
	}
	if tlsCfg != nil {
		fmt.Printf("OpenXDB %s replica of %s (TLS, data dir: %s), Ctrl-C to stop.\n", version, master, dir)
	} else {
		fmt.Printf("OpenXDB %s replica of %s (data dir: %s), Ctrl-C to stop.\n", version, master, dir)
	}
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
          [--tls-cert <path> --tls-key <path>] 启用监听层 TLS 加密
                               （两者必须同时提供；仅提供一个报错。
                               提供后主服务与 --replica-port 复制端口均加密监听；
                               未提供时回退持久化 tls_cert/tls_key（SET PERSIST）；
                               均未配置保持明文，向后兼容）
          [--tls-allow-legacy] --tls-cert 生效时允许同端口明文/TLS 并存
                               （默认纯 TLS，明文连接拒绝）
          [--replica-port <n>] 主节点复制端口（启用 M2 复制并生成 binlog）
          [--cluster-addr <addr>] 集群节点链路地址（启用 M4 节点握手/转发）
          [--metrics-port <n>] Prometheus /metrics 端口（默认 7789，0 禁用）
          [--mysql-port <n>]  MySQL wire protocol 兼容监听端口（默认 0=关闭；
                              开启后 mysql CLI / go-sql-driver 等可直接连接）
          [--shutdown-timeout <sec>] 优雅停机 drain 超时（默认取配置 shutdown_timeout，30s）
                              服务内发 SHUTDOWN [GRACEFUL] 或 Ctrl-C/SIGTERM 触发
          [--auth]            启用连接握手鉴权（AUTH <user> <password>）
          [--auth-allow-legacy] --auth 时允许无凭证客户端降级 guest（默认强制鉴权）
          [--root-password <pwd>] root 引导密码（无 root 用户时自动 CREATE USER；亦可经环境变量 OPENXDB_ROOT_PASSWORD 传入）
  gen-cert [--cert <path>] [--key <path>] [--out-dir <dir>]
                              生成自签名 TLS 证书（T36；ECDSA P-256，默认 cert.pem/key.pem）
          [--days <n>]        证书有效期天数（默认 365）
          [--cn <name>]       Subject CN 与 SAN（默认 localhost）
          [--force]           覆盖已存在文件（默认拒绝覆盖）
  backup push --file <path> --url <http(s) url>
                              将本地备份文件上传到远程目标
  backup pull --url <http(s) url> --out <path>
                              从远程目标下载备份到本地（再 RESTORE FROM 恢复）
  replica --data-dir <dir> --master <host:port>
                             以从节点身份连接主节点并异步复制
          [--replica-tls]     以 TLS 加密连接主节点复制端口（自签名/内网测试不校验服务器名）
          [--replica-tls-ca <path>] 以 TLS 连接并校验 CA 链与主机名（T36 verify-full）
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

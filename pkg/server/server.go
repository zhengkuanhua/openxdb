// Package server 实现 OpenXDB 行协议与 CLI 服务端（M1 T5，开发手册 §3.6）。
//
// 协议为逐行文本命令（大小写不敏感，空白分隔）：
//
//	PING              → PONG
//	HELP              → 命令列表
//	GET <key>         → <value>（不存在返回 ERR not found）
//	SET <key> <val>   → OK（value 允许含空格）
//	DEL <key>         → OK
//	BEGIN             → OK（开启显式事务）
//	COMMIT            → OK（提交显式事务）
//	ROLLBACK          → OK（回滚显式事务）
//	QUIT              → BYE（仅结束当前 REPL / 连接）
//
// 未处于显式事务时，SET/DEL/GET 自动以隐式事务执行：写操作立即提交（持久化），
// 读操作持有 BEGIN 时刻快照，保证单条命令内部一致性。
// 显式事务内，GET 遵循 read-your-writes + 快照隔离。
package server

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/metrics"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// Server 协议执行器：线程安全（内部串行化命令处理）。
type Server struct {
	Txn txn.TxnManager
	SQL *sql.Engine // 非 nil 时支持 SQL 语句（T3 集成）

	// Auth 非 nil 时启用连接握手鉴权（M11 子功能 3）：客户端连接需先
	// AUTH <user> <password>；未鉴权会话按 AllowLegacy 降级策略处理。
	// nil 时保持旧行为（无鉴权，会话默认超级用户 root），向后兼容。
	Auth *AuthConfig

	mu  sync.Mutex
	cur txn.Txn // 当前显式事务；nil 表示未开启

	// T20：活跃 TCP 连接会话计数（ServeListener 每个连接进入 +1、退出 -1），
	// 供 SHOW STATS / openxdb stats 拉取连接数指标。
	conns int64

	// M13：metrics 导出注册表（nil = 未装配，METRICS 返回 ERR not enabled）。
	m *metrics.Registry

	// ---- M16-2 优雅停机状态机 ----
	// state：0=运行，1=停机中（拒绝新连接/新事务，drain 进行中事务），
	// 2=已关闭（GracefulShutdown 完成，存储已 flush/关闭）。
	state int32
	// active 正在锁内执行命令数（含被拒命令）：drain 判断无进行中命令的依据。
	active int64
	// forced 标记超时强制退出（GracefulShutdown 超时置位，调用方据此决定退出码）。
	forced int32
	// ln 当前监听器（ServeListener 记录，GracefulShutdown 关闭以拒绝新连接）。
	lnMu sync.Mutex
	ln   net.Listener
	// shutdownHook 在 drain 完成后调用（main 注入 db.Close：flush WAL/rocksdb/
	// 持久化配置后关闭存储）。nil 时仅完成状态机（测试/纯逻辑场景）。
	shutdownHook func() error
	// shutdownDone 在 GracefulShutdown 完成（含超时强制路径）时关闭，
	// 供 ServeTCP 返回后 WaitShutdown 阻塞等待停机流程收尾。
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	// ShutdownTimeout 返回停机 drain 超时（main 注入 SQL 引擎配置；
	// nil = 默认 30s）。
	ShutdownTimeout func() time.Duration
	// Logf 停机过程日志输出（main 注入 log.Printf；nil = 静默）。
	Logf func(format string, args ...any)

	// tlsConfig 非 nil 时 ServeTCP 使用 tls.Listen 提供加密监听（P0 TLS）；
	// nil 保持明文监听（默认行为，向后兼容）。经 SetTLS 装配。
	tlsConfig *tls.Config
}

// AuthConfig 连接鉴权配置：经 SetAuth 装配到 Server。
type AuthConfig struct {
	// Engine 用于 AUTH 握手时校验用户名/密码（AuthenticateUser），必填。
	Engine *sql.Engine
	// AllowLegacy 允许无凭证旧客户端（T19 JDBC/Python 驱动，README 明示
	// no server-side auth handshake）以降级 guest 身份继续使用连接：
	// 未鉴权会话的 SQL 语句经执行期权限校验（guest 无任何授权，管理语句与
	// DML 一律拒绝），KV 命令（GET/SET/DEL/BEGIN/COMMIT/ROLLBACK）照常可用。
	// false 时未鉴权连接除 AUTH/PING/HELP/QUIT 外一律拒绝并返回
	// "auth required"。默认 true（向后兼容）。
	AllowLegacy bool
}

// session 连接会话状态：user 为当前会话用户；authed 标记是否已通过 AUTH 握手。
type session struct {
	user   string
	authed bool
}

// New 创建协议执行器。
func New(tm txn.TxnManager) *Server {
	return &Server{Txn: tm, shutdownDone: make(chan struct{})}
}

// SetTLS 装配监听层 TLS 配置（P0 TLS）。传入 nil 恢复明文监听（默认行为，
// 向后兼容）；非 nil 时 ServeTCP 使用 tls.Listen 提供加密监听，客户端须
// 以 TLS 连接（pkg/server 的 Dial/InsecureTLSConfig 可配合内网自签名测试）。
func (s *Server) SetTLS(cfg *tls.Config) { s.tlsConfig = cfg }

// ActiveConns 返回当前活跃 TCP 连接数（T20 会话计数）。
func (s *Server) ActiveConns() int64 { return atomic.LoadInt64(&s.conns) }

// SetAuth 启用连接鉴权配置（M11 子功能 3）。传入 nil 可恢复无鉴权旧行为。
func (s *Server) SetAuth(cfg *AuthConfig) { s.Auth = cfg }

// SetMetrics 注入指标注册表（M13 metrics 导出；nil 关闭计数与 METRICS 命令）。
func (s *Server) SetMetrics(m *metrics.Registry) { s.m = m }

// incKV / incCommit / incRollback M13 metrics：KV 命令与 KV 事务计数。
func (s *Server) incKV() {
	if s.m != nil {
		s.m.IncKV()
	}
}
func (s *Server) incCommit() {
	if s.m != nil {
		s.m.IncCommit()
	}
}
func (s *Server) incRollback() {
	if s.m != nil {
		s.m.IncRollback()
	}
}

// defaultSession 返回当前模式的默认会话：无鉴权（Auth==nil）时会话为超级
// 用户 root（旧行为）；启用鉴权时为未鉴权 guest（user 为空串，等待 AUTH）。
func (s *Server) defaultSession() *session {
	if s.Auth == nil {
		return &session{user: sql.SuperUser, authed: true}
	}
	return &session{user: "", authed: false}
}

// Handle 处理一行命令，返回响应文本（成功时不含 ERR 前缀）。
// 使用无鉴权模式的默认会话：兼容旧调用方（CLI REPL / 既有测试）。
// 鉴权场景（多命令保持会话状态）请使用 HandleSession。
func (s *Server) Handle(line string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handleLocked(line, s.defaultSession())
}

// HandleSession 处理一行命令，携带连接会话状态（AUTH 握手/降级后的用户）。
// 与 REPLSession 配合用于 TCP 连接：每连接一个 session，跨命令保持。
func (s *Server) HandleSession(line string, sess *session) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// M16-2：正在执行命令计数（drain 判断无进行中命令的依据）。
	atomic.AddInt64(&s.active, 1)
	defer atomic.AddInt64(&s.active, -1)
	return s.handleLocked(line, sess)
}

func (s *Server) handleLocked(line string, sess *session) (string, error) {
	line = strings.TrimSpace(strings.TrimPrefix(line, "\ufeff")) // 容忍 Windows 控制台 BOM
	if line == "" {
		return "", nil
	}
	toks := strings.Fields(line)
	cmd := strings.ToUpper(toks[0])
	// M11 连接握手鉴权：Auth 启用且会话未鉴权时，仅放行 AUTH/PING/HELP/QUIT；
	// 其他命令按 AllowLegacy 拒绝（返回 auth required）或降级为 guest 放行
	// （SQL 层权限校验会拒绝管理语句与无授权 DML）。
	if s.Auth != nil && !sess.authed {
		switch cmd {
		case "AUTH":
			return s.doAuth(toks, sess)
		case "PING":
			return "PONG", nil
		case "HELP":
			return "PING AUTH GET SET DEL BEGIN COMMIT ROLLBACK QUIT", nil
		case "QUIT":
			return "BYE", nil
		default:
			if !s.Auth.AllowLegacy {
				return "", errors.New("ERR auth required: send AUTH <user> <password> first")
			}
			sess.user = "" // 降级 guest：无任何表级/管理权限
		}
	}
	// M16-2 停机中：拒绝新连接/新事务；仅放行事务终态命令（COMMIT/ROLLBACK）
	// 与 QUIT/PING/HELP/SHUTDOWN，让进行中显式事务在 drain 窗口内收尾。
	if atomic.LoadInt32(&s.state) != 0 {
		switch cmd {
		case "COMMIT", "ROLLBACK", "QUIT", "PING", "HELP", "SHUTDOWN":
		default:
			return "", errors.New("ERR server shutting down: rejecting new connections/transactions")
		}
	}
	switch cmd {
	case "PING":
		return "PONG", nil
	case "HELP":
		return "PING GET SET DEL BEGIN COMMIT ROLLBACK SHUTDOWN METRICS STATUS QUIT", nil
	case "QUIT":
		return "BYE", nil
	case "SHUTDOWN":
		return s.doShutdown(toks, sess)
	case "AUTH":
		if s.Auth == nil {
			return "", errors.New("ERR auth unavailable: connection auth not enabled")
		}
		return s.doAuth(toks, sess)
	case "GET":
		if len(toks) < 2 {
			return "", errors.New("ERR syntax: GET <key>")
		}
		s.incKV()
		return s.doGet([]byte(toks[1]))
	case "SET":
		// SQL SET 变量赋值（M13：slow_query_threshold / slow_query_log 等）优先
		// 路由到 SQL 引擎：形式为 SET <name> = <value> 或 SET <name>=<value>。
		// 其余 SET <key> <value...> 保持 KV 语义（向后兼容）。
		if s.SQL != nil && len(toks) >= 2 && isSQLSetAssign(toks[1:]) {
			return s.doSQL(line, sess)
		}
		if len(toks) < 3 {
			return "", errors.New("ERR syntax: SET <key> <value>")
		}
		s.incKV()
		val := strings.Join(toks[2:], " ")
		return s.doWrite(func(t txn.Txn) error { return t.Put([]byte(toks[1]), []byte(val)) })
	case "DEL":
		if len(toks) < 2 {
			return "", errors.New("ERR syntax: DEL <key>")
		}
		s.incKV()
		return s.doWrite(func(t txn.Txn) error { return t.Delete([]byte(toks[1])) })
	case "METRICS":
		// M13 metrics 导出：返回 Prometheus 文本格式指标（服务装配后可用）。
		if s.m == nil {
			return "", errors.New("ERR metrics not enabled")
		}
		return s.m.Render(), nil
	case "STATUS":
		// M13 子功能三：聚合状态快照（STATUS / STATUS JSON），只读不阻塞。
		return s.doStatus(toks)
	case "BEGIN":
		// SQL 引擎已挂载时，事务命令优先路由到 SQL 会话事务（executor 自维护
		// curTx/curBeginTS）；否则走 KV 层显式事务（GET/SET/DEL 可见）。
		// 事务命令成功统一回显 OK（SQL 引擎的 BEGIN 无列结果不直接透出）。
		if s.SQL != nil && sql.IsSQL(line) {
			if _, err := s.doSQL(line, sess); err != nil {
				return "", err
			}
			return "OK", nil
		}
		if s.cur != nil {
			return "", errors.New("ERR transaction already active")
		}
		t, err := s.Txn.Begin()
		if err != nil {
			return "", fmt.Errorf("ERR begin: %v", err)
		}
		s.cur = t
		return "OK", nil
	case "COMMIT":
		if s.SQL != nil && sql.IsSQL(line) {
			if _, err := s.doSQL(line, sess); err != nil {
				return "", err
			}
			return "OK", nil
		}
		if s.cur == nil {
			return "", errors.New("ERR no active transaction")
		}
		t := s.cur
		s.cur = nil
		if err := t.Commit(); err != nil {
			return "", fmt.Errorf("ERR commit: %v", err)
		}
		s.incCommit()
		return "OK", nil
	case "ROLLBACK":
		if s.SQL != nil && sql.IsSQL(line) {
			if _, err := s.doSQL(line, sess); err != nil {
				return "", err
			}
			return "OK", nil
		}
		if s.cur == nil {
			return "", errors.New("ERR no active transaction")
		}
		t := s.cur
		s.cur = nil
		if err := t.Rollback(); err != nil {
			return "", fmt.Errorf("ERR rollback: %v", err)
		}
		s.incRollback()
		return "OK", nil
	default:
		// SQL 语句路由（T3）：SQL 引擎已挂载且行首为 SQL 关键字
		if s.SQL != nil && sql.IsSQL(line) {
			return s.doSQL(line, sess)
		}
		return "", fmt.Errorf("ERR unknown command %q (try HELP)", cmd)
	}
}

// doAuth 处理连接握手命令 AUTH <user> <password>：经 Engine.AuthenticateUser
// 校验用户名与密码。成功返回 "AUTH OK" 并将会话切换为对应用户；失败返回
// 明确错误并保持未鉴权（允许重试）。密码支持含空格（取剩余 token 拼接）。
func (s *Server) doAuth(toks []string, sess *session) (string, error) {
	if len(toks) < 3 {
		return "", errors.New("ERR syntax: AUTH <user> <password>")
	}
	if s.Auth == nil || s.Auth.Engine == nil {
		return "", errors.New("ERR auth unavailable: connection auth not enabled")
	}
	user := toks[1]
	password := strings.Join(toks[2:], " ")
	ok, err := s.Auth.Engine.AuthenticateUser(user, password)
	if err != nil {
		return "", fmt.Errorf("ERR auth error: %v", err)
	}
	if !ok {
		return "", errors.New("ERR auth failed: invalid user or password")
	}
	sess.user = user
	sess.authed = true
	return "AUTH OK", nil
}

// isSQLSetAssign 判断 SET 行是否为 SQL 变量赋值形式：
// "SET <name> = <value>"（toks 从 name 开始）或 "SET <name>=<value>"。
func isSQLSetAssign(toks []string) bool {
	if len(toks) == 0 {
		return false
	}
	if strings.Contains(toks[0], "=") {
		return true
	}
	return len(toks) >= 2 && toks[1] == "="
}

// doSQL 执行一条 SQL 语句并格式化输出（以会话用户身份执行，触发执行期
// 权限校验：管理语句仅 root，DML 需对应表级授权，见 pkg/sql/authz.go）。
func (s *Server) doSQL(line string, sess *session) (string, error) {
	res, err := s.SQL.ExecuteAs(line, sess.user)
	if err != nil {
		return "", fmt.Errorf("ERR sql: %v", err)
	}
	return formatResult(res), nil
}

// formatResult 将 SQL 结果渲染为文本表格 / 影响行数。
func formatResult(res *sql.Result) string {
	if len(res.Columns) == 0 {
		return fmt.Sprintf("(%d rows affected)", res.AffectedRows)
	}
	// 每列宽度 = max(列名, 最大单元格)
	widths := make([]int, len(res.Columns))
	for i, c := range res.Columns {
		if len(c) > widths[i] {
			widths[i] = len(c)
		}
	}
	for _, r := range res.Rows {
		for i, v := range r {
			s := v.String()
			if len(s) > widths[i] {
				widths[i] = len(s)
			}
		}
	}
	var sb strings.Builder
	writeRow := func(cells []string) {
		for i, c := range cells {
			if i > 0 {
				sb.WriteString(" | ")
			}
			sb.WriteString(c)
			for j := len(c); j < widths[i]; j++ {
				sb.WriteByte(' ')
			}
		}
		sb.WriteString("\n")
	}
	header := make([]string, len(res.Columns))
	for i, c := range res.Columns {
		header[i] = c
	}
	writeRow(header)
	// 分隔线
	sep := make([]string, len(widths))
	for i, w := range widths {
		sep[i] = strings.Repeat("-", w)
	}
	writeRow(sep)
	for _, r := range res.Rows {
		cells := make([]string, len(r))
		for i, v := range r {
			cells[i] = v.String()
		}
		writeRow(cells)
	}
	sb.WriteString(fmt.Sprintf("(%d rows)", len(res.Rows)))
	return sb.String()
}

// doGet 读取：显式事务走事务快照+读己写；否则隐式事务（Begin→Get→Commit）。
func (s *Server) doGet(key []byte) (string, error) {
	if s.cur != nil {
		v, err := s.cur.Get(key)
		return formatGet(v, err)
	}
	t, err := s.Txn.Begin()
	if err != nil {
		return "", fmt.Errorf("ERR begin: %v", err)
	}
	defer func() { _ = t.Rollback() }() // 只读事务直接放弃，不写 WAL
	v, err := t.Get(key)
	return formatGet(v, err)
}

// doWrite 写入：显式事务写缓冲；否则隐式事务立即提交。
func (s *Server) doWrite(mut func(t txn.Txn) error) (string, error) {
	if s.cur != nil {
		if err := mut(s.cur); err != nil {
			return "", fmt.Errorf("ERR write: %v", err)
		}
		return "OK", nil
	}
	t, err := s.Txn.Begin()
	if err != nil {
		return "", fmt.Errorf("ERR begin: %v", err)
	}
	if err := mut(t); err != nil {
		_ = t.Rollback()
		return "", fmt.Errorf("ERR write: %v", err)
	}
	if err := t.Commit(); err != nil {
		return "", fmt.Errorf("ERR commit: %v", err)
	}
	return "OK", nil
}

func formatGet(v []byte, err error) (string, error) {
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return "", errors.New("ERR not found")
		}
		return "", fmt.Errorf("ERR get: %v", err)
	}
	return string(v), nil
}

// REPL 交互循环：逐行读入命令直到 EOF 或 QUIT。
// 使用无鉴权模式的默认会话（本地 CLI 场景，Auth 未启用时即 root）。
func (s *Server) REPL(r io.Reader, w io.Writer) error {
	return s.REPLSession(r, w, s.defaultSession())
}

// REPLSession 交互循环：逐行读入命令直到 EOF 或 QUIT，携带连接会话
// （AUTH 握手/降级状态跨命令保持）。TCP 连接每连接一个 session。
func (s *Server) REPLSession(r io.Reader, w io.Writer, sess *session) error {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		resp, err := s.HandleSession(sc.Text(), sess)
		if err != nil {
			resp = err.Error()
		}
		if resp == "" {
			continue
		}
		if _, werr := fmt.Fprintln(w, resp); werr != nil {
			return werr
		}
		if resp == "BYE" {
			return nil
		}
	}
	return sc.Err()
}

// ServeTCP 监听 addr 并启动 TCP 服务。已配置 TLS（SetTLS 非 nil）时使用
// tls.Listen 提供加密监听；未配置时保持明文监听（默认行为）。
func (s *Server) ServeTCP(addr string) error {
	var ln net.Listener
	var err error
	if s.tlsConfig != nil {
		ln, err = tls.Listen("tcp", addr, s.tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", addr, err)
	}
	defer ln.Close()
	return s.ServeListener(ln)
}

// ServeListener 在既有监听器上提供 TCP 服务：每连接独立协程跑 REPL 循环
// （命令处理由 Server 锁串行化，保证多连接下事务安全）。
func (s *Server) ServeListener(ln net.Listener) error {
	// M16-2：记录当前监听器，GracefulShutdown 时关闭以拒绝新连接。
	s.lnMu.Lock()
	s.ln = ln
	s.lnMu.Unlock()
	defer func() {
		s.lnMu.Lock()
		if s.ln == ln {
			s.ln = nil
		}
		s.lnMu.Unlock()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				// M16-2：GracefulShutdown 主动关闭监听器（拒绝新连接），
				// 属正常退出路径；退出码由调用方据 ForceExited 判定。
				return nil
			}
			return fmt.Errorf("server: accept: %w", err)
		}
		go s.serveConn(conn)
	}
}

// serveConn 单个 TCP 连接的处理协程：进入时连接计数 +1，退出时 -1。
func (s *Server) serveConn(c net.Conn) {
	defer c.Close()
	atomic.AddInt64(&s.conns, 1)
	defer atomic.AddInt64(&s.conns, -1)
	// 每连接独立会话：Auth 启用时默认未鉴权 guest，需先 AUTH；
	// Auth 未启用时直接以超级用户 root 进入（旧行为）。
	_ = s.REPLSession(c, c, s.defaultSession())
}

// ---- M16-2 优雅停机 ----

// ErrShutdownTimeout 优雅停机超时强制退出错误：drain 窗口内仍有进行中事务
// 未收尾，存储已强制关闭（数据完整，可 reopen），调用方应使用非 0 退出码。
var ErrShutdownTimeout = errors.New("server: graceful shutdown timed out")

// SetShutdownHook 注入 drain 完成后执行的关闭钩子（main 装配 db.Close：
// 按序停止 txn/WAL/storage，flush RocksDB 与持久化配置）。
func (s *Server) SetShutdownHook(hook func() error) { s.shutdownHook = hook }

// ForceExited 返回停机是否因超时强制退出（main 据此决定退出码 0/1）。
func (s *Server) ForceExited() bool { return atomic.LoadInt32(&s.forced) != 0 }

// ShuttingDown 返回是否已进入停机流程（拒绝新连接/新事务）。
func (s *Server) ShuttingDown() bool { return atomic.LoadInt32(&s.state) != 0 }

// WaitShutdown 阻塞直到优雅停机流程完成（含超时强制路径）。
// 未触发过停机时永不返回：仅 ServeTCP 返回后确认停机收尾时调用。
func (s *Server) WaitShutdown() { <-s.shutdownDone }

// logf 输出停机过程日志（未装配 Logf 时静默，测试可注入收集器断言）。
func (s *Server) logf(format string, args ...any) {
	if s.Logf != nil {
		s.Logf(format, args...)
	}
}

// shutdownTimeout 返回当前生效停机超时：优先 ShutdownTimeout 注入源
// （main 装配 SQL 引擎 shutdown_timeout 配置），否则默认 30s。
func (s *Server) shutdownTimeout() time.Duration {
	if s.ShutdownTimeout != nil {
		if d := s.ShutdownTimeout(); d > 0 {
			return d
		}
	}
	return 30 * time.Second
}

// doShutdown 处理 SHUTDOWN [GRACEFUL] 命令（M16-2）：进入优雅停机流程。
// 管理操作仅 root（鉴权模式）；Auth 未启用时默认 root（旧行为）。
// 停机在独立协程执行，避免占用命令串行化锁；命令返回后本连接后续命令
// 被拒绝（仅 QUIT/事务终态放行）。
func (s *Server) doShutdown(toks []string, sess *session) (string, error) {
	if s.Auth != nil && sess.user != sql.SuperUser {
		return "", errors.New("ERR permission denied: SHUTDOWN requires root")
	}
	if len(toks) > 1 && strings.ToUpper(toks[1]) != "GRACEFUL" {
		return "", errors.New("ERR syntax: SHUTDOWN [GRACEFUL]")
	}
	if atomic.LoadInt32(&s.state) != 0 {
		return "", errors.New("ERR already shutting down")
	}
	go func() {
		if err := s.GracefulShutdown(s.shutdownTimeout()); err != nil {
			s.logf("graceful shutdown: error: %v", err)
		}
	}()
	return "OK: graceful shutdown started", nil
}

// drained 判断 drain 是否完成：无正在执行命令（active==0 且锁空闲）且
// 无未收尾显式事务。锁被占用时视为未排空（避免阻塞轮询导致超时判断延迟）。
func (s *Server) drained() bool {
	if atomic.LoadInt64(&s.active) != 0 {
		return false
	}
	if !s.mu.TryLock() {
		return false
	}
	defer s.mu.Unlock()
	return s.cur == nil
}

// GracefulShutdown 执行优雅停机（M16-2）：拒绝新连接/新事务 → drain 进行中
// 事务（超时强制）→ flush/关闭存储。SHUTDOWN 命令与 SIGINT/SIGTERM 共用
// 此流程。
// 正常路径返回 nil（调用方退出码 0）；超时强制路径返回 ErrShutdownTimeout
// （调用方退出码非 0），但存储仍会完成关闭，保证 reopen 数据完整。
func (s *Server) GracefulShutdown(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&s.state, 0, 1) {
		return errors.New("server: already shutting down")
	}
	if timeout <= 0 {
		timeout = s.shutdownTimeout()
	}
	s.logf("graceful shutdown: start, drain timeout=%s", timeout)
	// 1) 拒绝新连接：关闭监听器；已建连会话在命令入口拒绝新事务。
	s.closeListener()
	s.logf("graceful shutdown: rejecting new connections")
	deadline := time.Now().Add(timeout)
	for {
		if s.drained() {
			break
		}
		if time.Now().After(deadline) {
			atomic.StoreInt32(&s.forced, 1)
			s.mu.Lock()
			hasTxn := s.cur != nil
			s.mu.Unlock()
			s.logf("graceful shutdown: TIMEOUT after %s, force exit (active cmds=%d, active conns=%d, explicit txn=%v)", timeout, atomic.LoadInt64(&s.active), atomic.LoadInt64(&s.conns), hasTxn)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	atomic.StoreInt32(&s.state, 2)
	// 2) flush/关闭存储（txn/WAL/storage 由 db.Close 按序处理，持久化配置已落盘）。
	if s.shutdownHook != nil {
		if err := s.shutdownHook(); err != nil {
			s.logf("graceful shutdown: close storage error: %v", err)
			s.shutdownOnce.Do(func() { close(s.shutdownDone) })
			return err
		}
	}
	s.shutdownOnce.Do(func() { close(s.shutdownDone) })
	if atomic.LoadInt32(&s.forced) != 0 {
		s.logf("graceful shutdown: forced exit (exit code 1)")
		return ErrShutdownTimeout
	}
	s.logf("graceful shutdown: drained, storage closed (exit code 0)")
	return nil
}

// closeListener 关闭当前监听器（拒绝新 TCP 连接）。
func (s *Server) closeListener() {
	s.lnMu.Lock()
	ln := s.ln
	s.lnMu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
}

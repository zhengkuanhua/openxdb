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
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

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
	return &Server{Txn: tm}
}

// ActiveConns 返回当前活跃 TCP 连接数（T20 会话计数）。
func (s *Server) ActiveConns() int64 { return atomic.LoadInt64(&s.conns) }

// SetAuth 启用连接鉴权配置（M11 子功能 3）。传入 nil 可恢复无鉴权旧行为。
func (s *Server) SetAuth(cfg *AuthConfig) { s.Auth = cfg }

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
	switch cmd {
	case "PING":
		return "PONG", nil
	case "HELP":
		return "PING GET SET DEL BEGIN COMMIT ROLLBACK QUIT", nil
	case "QUIT":
		return "BYE", nil
	case "AUTH":
		if s.Auth == nil {
			return "", errors.New("ERR auth unavailable: connection auth not enabled")
		}
		return s.doAuth(toks, sess)
	case "GET":
		if len(toks) < 2 {
			return "", errors.New("ERR syntax: GET <key>")
		}
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
		val := strings.Join(toks[2:], " ")
		return s.doWrite(func(t txn.Txn) error { return t.Put([]byte(toks[1]), []byte(val)) })
	case "DEL":
		if len(toks) < 2 {
			return "", errors.New("ERR syntax: DEL <key>")
		}
		return s.doWrite(func(t txn.Txn) error { return t.Delete([]byte(toks[1])) })
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

// ServeTCP 监听 addr 并启动 TCP 服务。
func (s *Server) ServeTCP(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", addr, err)
	}
	defer ln.Close()
	return s.ServeListener(ln)
}

// ServeListener 在既有监听器上提供 TCP 服务：每连接独立协程跑 REPL 循环
// （命令处理由 Server 锁串行化，保证多连接下事务安全）。
func (s *Server) ServeListener(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("server: accept: %w", err)
		}
		go func(c net.Conn) {
			defer c.Close()
			atomic.AddInt64(&s.conns, 1)
			defer atomic.AddInt64(&s.conns, -1)
			// 每连接独立会话：Auth 启用时默认未鉴权 guest，需先 AUTH；
			// Auth 未启用时直接以超级用户 root 进入（旧行为）。
			_ = s.REPLSession(c, c, s.defaultSession())
		}(conn)
	}
}

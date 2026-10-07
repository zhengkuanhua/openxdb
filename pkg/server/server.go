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

	"github.com/openxdb/openxdb/pkg/sql"
	"github.com/openxdb/openxdb/pkg/storage"
	"github.com/openxdb/openxdb/pkg/txn"
)

// Server 协议执行器：线程安全（内部串行化命令处理）。
type Server struct {
	Txn txn.TxnManager
	SQL *sql.Engine // 非 nil 时支持 SQL 语句（T3 集成）

	mu  sync.Mutex
	cur txn.Txn // 当前显式事务；nil 表示未开启
}

// New 创建协议执行器。
func New(tm txn.TxnManager) *Server {
	return &Server{Txn: tm}
}

// Handle 处理一行命令，返回响应文本（成功时不含 ERR 前缀）。
func (s *Server) Handle(line string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handleLocked(line)
}

func (s *Server) handleLocked(line string) (string, error) {
	line = strings.TrimSpace(strings.TrimPrefix(line, "\ufeff")) // 容忍 Windows 控制台 BOM
	if line == "" {
		return "", nil
	}
	toks := strings.Fields(line)
	cmd := strings.ToUpper(toks[0])
	switch cmd {
	case "PING":
		return "PONG", nil
	case "HELP":
		return "PING GET SET DEL BEGIN COMMIT ROLLBACK QUIT", nil
	case "QUIT":
		return "BYE", nil
	case "GET":
		if len(toks) < 2 {
			return "", errors.New("ERR syntax: GET <key>")
		}
		return s.doGet([]byte(toks[1]))
	case "SET":
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
			return s.doSQL(line)
		}
		return "", fmt.Errorf("ERR unknown command %q (try HELP)", cmd)
	}
}

// doSQL 执行一条 SQL 语句并格式化输出。
func (s *Server) doSQL(line string) (string, error) {
	res, err := s.SQL.Execute(line)
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
func (s *Server) REPL(r io.Reader, w io.Writer) error {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		resp, err := s.Handle(sc.Text())
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
			_ = s.REPL(c, c)
		}(conn)
	}
}

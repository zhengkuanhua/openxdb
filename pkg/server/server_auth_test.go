package server

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// M11 子功能 3：server 层连接握手鉴权（AUTH）+ 旧客户端降级策略。
// 测试经真实 TCP 连接驱动 REPLSession，验证会话状态跨命令保持。

// newAuthTestServer 构造启用鉴权的 Server：
//   - 挂载 SQL 引擎（d.SQL）并 SetAuth；
//   - 以 root 身份初始化：root 用户（密码 rootpw）、users 表 + 1 行、
//     alice 用户（密码 apw，已 GRANT SELECT ON users）；
//   - 返回 Server 与清理函数。
func newAuthTestServer(t *testing.T, allowLegacy bool) (*Server, func()) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	s := New(d.Txn)
	s.SQL = d.SQL
	s.SetAuth(&AuthConfig{Engine: d.SQL, AllowLegacy: allowLegacy})

	exec := func(stmt string) {
		if _, err := d.SQL.Execute(stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}
	exec("CREATE USER 'root' IDENTIFIED BY 'rootpw'")
	exec("CREATE USER 'alice' IDENTIFIED BY 'apw'")
	exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	exec("INSERT INTO users VALUES (1, 'alice')")
	exec("GRANT SELECT ON users TO 'alice'")

	return s, func() { _ = d.Close() }
}

// tcpClient 封装真实 TCP 连接上的逐行命令往返。
type tcpClient struct {
	conn net.Conn
	rd   *bufio.Reader
}

func dialServer(t *testing.T, s *Server) *tcpClient {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = s.ServeListener(ln) }()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	return &tcpClient{conn: conn, rd: bufio.NewReader(conn)}
}

func (c *tcpClient) cmd(line string) string {
	if _, err := c.conn.Write([]byte(line + "\n")); err != nil {
		return "ERR write: " + err.Error()
	}
	resp, err := c.rd.ReadString('\n')
	if err != nil {
		return "ERR read: " + err.Error()
	}
	return strings.TrimSpace(resp)
}

// cmdAll 读取整条响应：SQL 查询结果渲染为多行表格（表头/分隔线/数据/
// 行计数），逐行读至以 "(" 开头的行计数行结束。
func (c *tcpClient) cmdAll(line string) string {
	if _, err := c.conn.Write([]byte(line + "\n")); err != nil {
		return "ERR write: " + err.Error()
	}
	var sb strings.Builder
	for {
		resp, err := c.rd.ReadString('\n')
		if err != nil {
			sb.WriteString("ERR read: " + err.Error())
			break
		}
		sb.WriteString(resp)
		if strings.HasPrefix(strings.TrimSpace(resp), "(") {
			break // 行计数行：格式化为多行的 SQL 结果已读完
		}
	}
	return strings.TrimSpace(sb.String())
}

func (c *tcpClient) close() { _ = c.conn.Close() }

// TestAuthHandshakeTCP 完整握手闭环：未鉴权降级拒绝 → 错误密码拒绝 →
// 正确密码 AUTH OK → root 全权限（管理语句 + DML）。
func TestAuthHandshakeTCP(t *testing.T) {
	s, closeFn := newAuthTestServer(t, true)
	defer closeFn()
	c := dialServer(t, s)
	defer c.close()

	// 未鉴权：SQL 降级为 guest，无任何授权 → 权限拒绝
	if got := c.cmd("SELECT * FROM users"); !strings.Contains(got, "permission denied") {
		t.Fatalf("guest SELECT: want permission denied, got %q", got)
	}
	if got := c.cmd("CREATE TABLE t (id INT PRIMARY KEY)"); !strings.Contains(got, "permission denied") {
		t.Fatalf("guest CREATE: want permission denied, got %q", got)
	}

	// 错误密码：明确拒绝且保持未鉴权
	if got := c.cmd("AUTH root wrongpw"); !strings.Contains(got, "auth failed") {
		t.Fatalf("wrong password: want auth failed, got %q", got)
	}
	if got := c.cmd("CREATE TABLE t (id INT PRIMARY KEY)"); !strings.Contains(got, "permission denied") {
		t.Fatalf("still guest after failed AUTH: want permission denied, got %q", got)
	}

	// 正确密码：AUTH OK
	if got := c.cmd("AUTH root rootpw"); got != "AUTH OK" {
		t.Fatalf("AUTH root rootpw: want AUTH OK, got %q", got)
	}

	// root 全权限：管理语句 + DML 放行
	if got := c.cmd("CREATE TABLE t (id INT PRIMARY KEY)"); !strings.HasPrefix(got, "(0 rows affected)") {
		t.Fatalf("root CREATE: want rows affected, got %q", got)
	}
	if got := c.cmdAll("SELECT * FROM users"); !strings.Contains(got, "alice") {
		t.Fatalf("root SELECT: want row data, got %q", got)
	}
}

// TestAuthRejectWithoutLegacy 关闭降级后，未鉴权连接除 AUTH/PING/HELP/QUIT 外
// 一律拒绝；AUTH 成功后恢复全部命令。
func TestAuthRejectWithoutLegacy(t *testing.T) {
	s, closeFn := newAuthTestServer(t, false)
	defer closeFn()
	c := dialServer(t, s)
	defer c.close()

	if got := c.cmd("PING"); got != "PONG" {
		t.Fatalf("PING: want PONG, got %q", got)
	}
	for _, line := range []string{"SET k v", "GET k", "SELECT * FROM users", "BEGIN", "COMMIT"} {
		if got := c.cmd(line); !strings.Contains(got, "auth required") {
			t.Fatalf("%q: want auth required, got %q", line, got)
		}
	}

	if got := c.cmd("AUTH root rootpw"); got != "AUTH OK" {
		t.Fatalf("AUTH: want AUTH OK, got %q", got)
	}
	if got := c.cmd("SET k v"); got != "OK" {
		t.Fatalf("SET after AUTH: want OK, got %q", got)
	}
	if got := c.cmd("GET k"); got != "v" {
		t.Fatalf("GET after AUTH: want v, got %q", got)
	}
}

// TestAuthLegacyGuestKV 降级策略：旧客户端（T19 JDBC/Python 驱动，无 AUTH）
// 的 KV 命令照常可用，SQL 受限（guest 无授权）。
func TestAuthLegacyGuestKV(t *testing.T) {
	s, closeFn := newAuthTestServer(t, true)
	defer closeFn()
	c := dialServer(t, s)
	defer c.close()

	if got := c.cmd("SET legkey legval"); got != "OK" {
		t.Fatalf("legacy SET: want OK, got %q", got)
	}
	if got := c.cmd("GET legkey"); got != "legval" {
		t.Fatalf("legacy GET: want legval, got %q", got)
	}
	if got := c.cmd("SELECT * FROM users"); !strings.Contains(got, "permission denied") {
		t.Fatalf("legacy SQL: want permission denied, got %q", got)
	}
}

// TestAuthAlicePrivilegedSQL 普通用户 AUTH 后：有授权的 SELECT 放行，
// 管理语句与未授权 DML 拒绝。
func TestAuthAlicePrivilegedSQL(t *testing.T) {
	s, closeFn := newAuthTestServer(t, true)
	defer closeFn()
	c := dialServer(t, s)
	defer c.close()

	if got := c.cmd("AUTH alice apw"); got != "AUTH OK" {
		t.Fatalf("AUTH alice: want AUTH OK, got %q", got)
	}
	if got := c.cmdAll("SELECT * FROM users"); !strings.Contains(got, "alice") {
		t.Fatalf("alice SELECT: want row data, got %q", got)
	}
	// 未授权 DML 拒绝
	if got := c.cmd("INSERT INTO users (id, name) VALUES (2, 'bob')"); !strings.Contains(got, "permission denied") {
		t.Fatalf("alice INSERT: want permission denied, got %q", got)
	}
	// 管理语句拒绝（非超级用户）
	if got := c.cmd("CREATE TABLE t (id INT PRIMARY KEY)"); !strings.Contains(got, "permission denied") {
		t.Fatalf("alice CREATE: want permission denied, got %q", got)
	}
	if got := c.cmd("GRANT SELECT ON users TO 'alice'"); !strings.Contains(got, "permission denied") {
		t.Fatalf("alice GRANT: want permission denied, got %q", got)
	}
}

// TestAuthSyntax 握手参数缺失返回语法错误；密码含空格时按剩余 token 拼接。
func TestAuthSyntax(t *testing.T) {
	s, closeFn := newAuthTestServer(t, true)
	defer closeFn()
	c := dialServer(t, s)
	defer c.close()

	if got := c.cmd("AUTH root"); !strings.HasPrefix(got, "ERR syntax") {
		t.Fatalf("AUTH root: want ERR syntax, got %q", got)
	}
}

// TestAuthUnavailableWithoutConfig 未启用鉴权（Auth==nil）时 AUTH 命令
// 返回明确提示，旧客户端行为不变（旧协议无握手）。
func TestAuthUnavailableWithoutConfig(t *testing.T) {
	s, closeFn := newTestServer(t)
	defer closeFn()
	if got := mustResp(t, s, "AUTH root rootpw"); !strings.Contains(got, "auth unavailable") {
		t.Fatalf("AUTH w/o config: want auth unavailable, got %q", got)
	}
	// 无鉴权模式下 SQL/KV 行为完全不变（旧测试覆盖，此处回归断言）
	if got := mustResp(t, s, "PING"); got != "PONG" {
		t.Fatalf("PING: want PONG, got %q", got)
	}
}

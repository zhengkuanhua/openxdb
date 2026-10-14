// M17-1 SQL 审计日志（audit log）：
//
// 覆盖连接登录/登出（CONNECT / DISCONNECT / AUTH_FAILED）、数据操作（DML：
// INSERT/UPDATE/DELETE）、结构变更（DDL：CREATE/DROP/ALTER/IMPORT/EXPORT/
// BACKUP/RESTORE/集群运维）与权限变更（PRIVILEGE：CREATE/DROP/ALTER USER、
// GRANT/REVOKE、CREATE/DROP ROLE）四类事件。
//
// 配置（SET [PERSIST] <key> = <value>，见 config.go 注册表）：
//   - audit_log            审计总开关（on/off，默认 on，安全默认）
//   - audit_log_file       审计落盘路径（空=不落盘；设置路径即开启落盘）
//   - audit_log_threshold  慢审计阈值毫秒（>0 时超过阈值的事件标记 slow）
//
// 存储：内存环缓冲（最近 auditMax 条，SHOW AUDITLOG [LIMIT n] 查询，倒序
// 返回）+ JSON 行落盘（一行一条，写失败静默忽略，不阻塞 SQL 执行）。
// 埋点：统一挂载于 ExecuteTextAsSession（checkPrivilege 拒绝分支与 execTimed
// 末尾），连接类事件由 server 层 AUTH 握手/连接关闭时通过
// RecordAuditConnect / RecordAuditDisconnect 记录，不散落各 exec* 函数。

package sql

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"
)

// 审计事件分类（AuditRecord.Event）。
const (
	AuditConnect    = "CONNECT"     // 连接鉴权成功
	AuditDisconnect = "DISCONNECT"  // 连接关闭（已鉴权会话）
	AuditAuthFailed = "AUTH_FAILED" // 连接鉴权失败
	AuditDML        = "DML"         // 数据操作：INSERT/UPDATE/DELETE
	AuditDDL        = "DDL"         // 结构/运维变更：CREATE/DROP/ALTER/IMPORT/EXPORT/BACKUP/RESTORE/集群
	AuditPrivilege  = "PRIVILEGE"   // 权限变更：用户/角色/授权
)

// auditMaxDefault 内存审计记录保留条数上限（环缓冲满则丢弃最旧）。
const auditMaxDefault = 1000

// AuditRecord 一条审计记录。
// Time 为本地时间文本（YYYY-MM-DDTHH:MM:SS.mmm）；Detail 为原始 SQL 文本
// （连接事件为固定说明）；Err 记录执行失败原因（成功为空串）。
type AuditRecord struct {
	ID         int64  `json:"id"`
	Time       string `json:"time"`
	User       string `json:"user"`
	Event      string `json:"event"`
	Object     string `json:"object"`
	Detail     string `json:"detail"`
	Success    bool   `json:"success"`
	Err        string `json:"err"`
	DurationMs int64  `json:"duration_ms"`
	Slow       bool   `json:"slow"`
}

// AuditLog 审计落盘器：JSON 一行一条（encoding/json 转义引号/换行，格式
// 稳定可机器解析），低频打开追加写，写失败静默忽略（审计不阻塞执行）。
// 与 SlowLog（slowlog.go）同构，按 M13 落盘惯例实现。
type AuditLog struct {
	mu      sync.Mutex
	path    string
	enabled bool
	f       *os.File
}

// NewAuditLog 创建审计落盘器（初始关闭，路径由 db/cmd 装配或 SET 热更新）。
func NewAuditLog() *AuditLog { return &AuditLog{} }

// Configure 配置审计落盘：path 为日志文件路径；enabled=false 或 path 空串
// 关闭落盘。配置变更对后续记录即时生效，无需重启。
func (a *AuditLog) Configure(path string, enabled bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f != nil {
		_ = a.f.Close()
		a.f = nil
	}
	a.path = path
	a.enabled = enabled && path != ""
	if a.enabled {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			// 路径不可写时保持关闭（不报错：服务装配路径后按 intent 恢复）。
			a.enabled = false
			return
		}
		a.f = f
	}
}

// Enabled 返回当前审计落盘开关状态。
func (a *AuditLog) Enabled() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.enabled
}

// Path 返回当前审计落盘路径（关闭时为空串）。
func (a *AuditLog) Path() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.path
}

// Write 追加一条 JSON 行；关闭或写失败时静默忽略。
func (a *AuditLog) Write(r AuditRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.enabled || a.f == nil {
		return
	}
	b, err := json.Marshal(r)
	if err != nil {
		return
	}
	if _, err := a.f.Write(append(b, '\n')); err != nil {
		// 写失败静默忽略：审计不阻塞 SQL 执行；下次写入仍会重试。
		return
	}
}

// ---- Executor 审计配置与记录 ----

// SetAuditEnabled 设置审计总开关（SET audit_log 热更新）。
func (e *Executor) SetAuditEnabled(enabled bool) { e.auditEnabled = enabled }

// AuditEnabled 返回审计总开关状态。
func (e *Executor) AuditEnabled() bool { return e.auditEnabled }

// SetAuditThreshold 设置慢审计阈值（毫秒，<=0 不标记 slow）。
func (e *Executor) SetAuditThreshold(ms int64) { e.auditThreshold = ms }

// AuditThreshold 返回当前慢审计阈值（毫秒）。
func (e *Executor) AuditThreshold() int64 { return e.auditThreshold }

// SetAuditLog 配置审计落盘（M17-1）：path 为日志文件路径，enabled=false 或
// path 空串关闭落盘。配置变更对后续记录即时生效。
func (e *Executor) SetAuditLog(path string, enabled bool) {
	if e.auditLog == nil {
		e.auditLog = NewAuditLog()
	}
	e.auditLog.Configure(path, enabled)
}

// AuditLogEnabled 返回当前审计落盘开关状态。
func (e *Executor) AuditLogEnabled() bool {
	return e.auditLog != nil && e.auditLog.Enabled()
}

// AuditLogPath 返回当前审计落盘路径（关闭时为空串）。
func (e *Executor) AuditLogPath() string {
	if e.auditLog == nil {
		return ""
	}
	return e.auditLog.Path()
}

// AuditRecords 返回内存审计记录副本（环缓冲内容，新->旧；供测试/外部只读）。
func (e *Executor) AuditRecords() []AuditRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]AuditRecord, len(e.auditRecords))
	copy(out, e.auditRecords)
	return out
}

// recordAudit 记录一条审计事件：总开关关闭时零开销返回（不分配不格式化）；
// 事件入内存环（满则丢弃最旧），随后在内存锁外追加落盘（低频 + 落盘器
// 独立互斥，互不阻塞热路径，与 recordSlow 同构）。
func (e *Executor) recordAudit(r AuditRecord) {
	if !e.auditEnabled {
		return
	}
	if r.Time == "" {
		r.Time = time.Now().Format("2006-01-02T15:04:05.000")
	}
	e.mu.Lock()
	e.auditID++
	r.ID = e.auditID
	e.auditRecords = append(e.auditRecords, r)
	if len(e.auditRecords) > e.auditMax {
		e.auditRecords = e.auditRecords[len(e.auditRecords)-e.auditMax:]
	}
	e.mu.Unlock()
	if e.auditLog != nil {
		e.auditLog.Write(r)
	}
}

// RecordAuditConnect 记录连接事件（server 层 AUTH 握手调用）：success=true
// 记录 CONNECT，false 记录 AUTH_FAILED。总开关关闭时静默。
func (e *Executor) RecordAuditConnect(user string, success bool) {
	event := AuditConnect
	if !success {
		event = AuditAuthFailed
	}
	e.recordAudit(AuditRecord{
		User:    user,
		Event:   event,
		Object:  "connection",
		Detail:  "AUTH",
		Success: success,
	})
}

// RecordAuditDisconnect 记录登出事件（server 层连接关闭时调用，仅已鉴权会话）。
func (e *Executor) RecordAuditDisconnect(user string) {
	e.recordAudit(AuditRecord{
		User:    user,
		Event:   AuditDisconnect,
		Object:  "connection",
		Detail:  "connection closed",
		Success: true,
	})
}

// auditEventOf 返回语句的审计事件分类与目标对象；非审计语句（SELECT、
// SHOW、事务控制等）返回 ok=false，调用方零开销跳过。
func auditEventOf(stmt Stmt) (event, object string, ok bool) {
	switch s := stmt.(type) {
	// DML：数据操作
	case *InsertStmt:
		return AuditDML, s.Table, true
	case *UpdateStmt:
		return AuditDML, s.Table, true
	case *DeleteStmt:
		return AuditDML, s.Table, true
	// DDL：结构 / 运维变更
	case *CreateTableStmt:
		return AuditDDL, s.Name, true
	case *DropTableStmt:
		return AuditDDL, s.Name, true
	case *CreateIndexStmt:
		return AuditDDL, s.Table + "." + s.Name, true
	case *DropIndexStmt:
		return AuditDDL, s.Table + "." + s.Name, true
	case *AlterTableStmt:
		return AuditDDL, s.Table, true
	case *CreateViewStmt:
		return AuditDDL, s.Name, true
	case *DropViewStmt:
		return AuditDDL, s.Name, true
	case *CreateMatViewStmt:
		return AuditDDL, s.Name, true
	case *RefreshMatViewStmt:
		return AuditDDL, s.Name, true
	case *DropMatViewStmt:
		return AuditDDL, s.Name, true
	case *CreateTriggerStmt:
		return AuditDDL, s.Table + "." + s.Name, true
	case *DropTriggerStmt:
		return AuditDDL, s.Name, true
	case *CreateProcedureStmt:
		return AuditDDL, s.Name, true
	case *DropProcedureStmt:
		return AuditDDL, s.Name, true
	case *ExportStmt:
		return AuditDDL, s.Table, true
	case *ImportStmt:
		return AuditDDL, s.Table, true
	case *BackupStmt:
		return AuditDDL, "backup:" + s.Path, true
	case *RestoreStmt:
		return AuditDDL, "restore:" + s.Path, true
	case *AddNodeStmt:
		return AuditDDL, "node:" + s.Addr, true
	case *SplitRegionStmt:
		return AuditDDL, "region:" + u64toa(s.RegionID), true
	case *AssignRegionStmt:
		return AuditDDL, "region:" + u64toa(s.RegionID) + "->node:" + s.NodeID, true
	case *BalanceStmt:
		return AuditDDL, "cluster", true
	// PRIVILEGE：用户 / 角色 / 授权变更
	case *CreateUserStmt:
		return AuditPrivilege, "user:" + s.Name, true
	case *DropUserStmt:
		return AuditPrivilege, "user:" + s.Name, true
	case *AlterUserStmt:
		return AuditPrivilege, "user:" + s.Name, true
	case *GrantStmt:
		return AuditPrivilege, grantTarget(s), true
	case *RevokeStmt:
		return AuditPrivilege, revokeTarget(s), true
	case *CreateRoleStmt:
		return AuditPrivilege, "role:" + s.Name, true
	case *DropRoleStmt:
		return AuditPrivilege, "role:" + s.Name, true
	}
	return "", "", false
}

// grantTarget / revokeTarget 构造授权/回收审计对象描述（库级/列级/角色/表级）。
func grantTarget(s *GrantStmt) string {
	switch s.Scope {
	case "DATABASE":
		return "database:" + s.Database
	case "COLUMN":
		return s.Table + "(" + strings.Join(s.Columns, ",") + ")"
	case "ROLE":
		return "role:" + s.Role
	default:
		return s.Table
	}
}

func revokeTarget(s *RevokeStmt) string {
	switch s.Scope {
	case "DATABASE":
		return "database:" + s.Database
	case "COLUMN":
		return s.Table + "(" + strings.Join(s.Columns, ",") + ")"
	case "ROLE":
		return "role:" + s.Role
	default:
		return s.Table
	}
}

// execShowAuditLog SHOW AUDITLOG [LIMIT n]：查看内存审计记录（新->旧）。
// n<=0 或未提供时返回全部内存记录。仅 root 可执行（authz.go 管理语句白名单）。
func (e *Executor) execShowAuditLog(sess *Session, s *ShowAuditLogStmt) (*Result, error) {
	recs := e.AuditRecords()
	n := len(recs)
	if s.Limit > 0 && s.Limit < n {
		n = s.Limit
	}
	res := &Result{Columns: []string{"id", "time", "user", "event", "object", "success", "duration_ms", "slow", "error", "detail"}}
	for i := len(recs) - 1; i >= len(recs)-n; i-- {
		r := recs[i]
		res.Rows = append(res.Rows, []Value{
			IntVal(r.ID),
			StrVal(r.Time),
			StrVal(r.User),
			StrVal(r.Event),
			StrVal(r.Object),
			StrVal(boolYesNo(r.Success)),
			IntVal(r.DurationMs),
			StrVal(boolYesNo(r.Slow)),
			StrVal(r.Err),
			StrVal(r.Detail),
		})
	}
	return res, nil
}

// boolYesNo 布尔值转 "yes"/"no" 文本（SHOW AUDITLOG 列展示用）。
func boolYesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// u64toa 无符号整数转十进制文本（审计对象描述用）。
func u64toa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

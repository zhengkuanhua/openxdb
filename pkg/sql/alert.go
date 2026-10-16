package sql

// 结构化日志与告警体系（pkg/sql 层接口）：SHOW ALERTS / SHOW ALERT STATUS
// 执行与 Engine 导出方法。告警管理器与结构化日志器位于 pkg/logx（全局实例），
// 埋点分布：备份调度（backup_schedule.go）、网络服务（server.go 经 db 装配）、
// 存储/WAL（db.Open 失败路径）等关键模块异常。

import (
	"strconv"

	"github.com/zhengkuanhua/openxdb/pkg/logx"
)

// execShowAlerts SHOW ALERTS：列出最近告警记录（时间/级别/模块/事件/消息）。
func (e *Executor) execShowAlerts(sess *Session, s *ShowAlertsStmt) (*Result, error) {
	alerts := logx.DefaultAlerts.Recent()
	res := &Result{Columns: []string{"time", "level", "module", "event", "msg"}}
	for _, a := range alerts {
		res.Rows = append(res.Rows, []Value{
			StrVal(a.Time), StrVal(a.Level), StrVal(a.Module), StrVal(a.Event), StrVal(a.Msg),
		})
	}
	return res, nil
}

// execShowAlertStatus SHOW ALERT STATUS：查看告警开关与出口配置
// （variable/value 两列，与 SHOW BACKUP STATUS 同风格）。T37 起追加
// 规则告警状态行（alert_rules_enabled / alert_eval_interval / alert_rule_count）。
func (e *Executor) execShowAlertStatus(sess *Session, s *ShowAlertStatusStmt) (*Result, error) {
	c := logx.DefaultAlerts.Config()
	rulesOn, evalInterval, ruleCount := e.AlertRulesStatus()
	return &Result{
		Columns: []string{"variable", "value"},
		Rows: [][]Value{
			{StrVal("enabled"), StrVal(boolValStr(c.Enabled))},
			{StrVal("file"), StrVal(c.File)},
			{StrVal("webhook"), StrVal(c.Webhook)},
			{StrVal("keep"), StrVal(itoa(int64(c.Keep)))},
			{StrVal("level"), StrVal(logx.Default.Level().String())},
			{StrVal("alert_rules_enabled"), StrVal(boolValStr(rulesOn))},
			{StrVal("alert_eval_interval"), StrVal(strconv.FormatInt(evalInterval, 10))},
			{StrVal("alert_rule_count"), StrVal(itoa(int64(ruleCount)))},
		},
	}, nil
}

// SetLogLevel 动态调整结构化日志级别（cmd/main start --log-level 装配）。
func (e *Engine) SetLogLevel(lv logx.Level) { logx.Default.SetLevel(lv) }

// LogLevel 返回当前结构化日志级别。
func (e *Engine) LogLevel() logx.Level { return logx.Default.Level() }

// SetAlertConfig 装配告警配置（cmd/main start --alert-* 装配）。
func (e *Engine) SetAlertConfig(cfg logx.AlertConfig) { logx.DefaultAlerts.Configure(cfg) }

// AlertConfig 返回当前告警配置。
func (e *Engine) AlertConfig() logx.AlertConfig { return logx.DefaultAlerts.Config() }

// RecentAlerts 返回最近告警记录（SHOW ALERTS 数据源）。
func (e *Engine) RecentAlerts() []logx.Alert { return logx.DefaultAlerts.Recent() }

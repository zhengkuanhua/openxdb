package sql

import (
	"github.com/zhengkuanhua/openxdb/pkg/cdc"
)

// execShowMasterStatus SHOW MASTER STATUS（v1.6.0 P1-2 复制协议位点入口）：
// 返回 binlog 文件名与当前写位点，供 MySQL 复制 / 订阅工具取位点后发起
// COM_BINLOG_DUMP。Position 语义 = 最后 LSN + 1（下一写位点），与
// COM_BINLOG_DUMP 的 position 参数一致（从该 LSN 起读）。
//
// 输出列（对齐 MySQL SHOW MASTER STATUS 五列）：
//   - File：openxdb-binlog（单文件 append-only，固定名）；
//   - Position：最后 LSN + 1；
//   - Binlog_Do_DB / Binlog_Ignore_DB / Executed_Gtid_Set：OpenXDB 无对应能力，恒空串。
func (e *Executor) execShowMasterStatus(sess *Session) (*Result, error) {
	if e.binlogPath == "" {
		return nil, &SQLError{Msg: "binlog not enabled"}
	}
	sub, err := cdc.NewSubscriber(e.binlogPath)
	if err != nil {
		return nil, &SQLError{Msg: "open binlog: " + err.Error()}
	}
	defer sub.Close()
	// Fetch(0,0) 一次读完当前文件，next = 文件尾位点 + 1 = 下一写位点。
	_, next, err := sub.Fetch(0, 0)
	if err != nil {
		return nil, &SQLError{Msg: "read binlog: " + err.Error()}
	}
	res := &Result{Columns: []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}}
	res.Rows = append(res.Rows, []Value{
		StrVal("openxdb-binlog"),
		IntVal(int64(next)),
		StrVal(""),
		StrVal(""),
		StrVal(""),
	})
	return res, nil
}

// binlogWireInfo 返回复制协议（COM_BINLOG_DUMP）装配所需信息：
// binlog 文件路径（空 = 未启用）与表目录（tableID → 表名/列，未知表 ID 回退
// #table-<id> 显示）。st 未装配（未走 db.Open 路径）时返回空表目录。
func (e *Executor) binlogWireInfo() (string, []TableMeta, error) {
	if e.binlogPath == "" {
		return "", nil, nil
	}
	var tabs []*TableMeta
	if e.st != nil {
		var err error
		tabs, err = loadTables(e.st.Get)
		if err != nil {
			return "", nil, err
		}
	}
	out := make([]TableMeta, 0, len(tabs))
	for _, t := range tabs {
		out = append(out, *t)
	}
	return e.binlogPath, out, nil
}

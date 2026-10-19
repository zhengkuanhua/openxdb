package sql

import (
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// Session 会话对象：承载会话级显式事务状态与快照版本。
//
// 并发模型：服务端（pkg/server）每 TCP 连接创建一个 Session，连接内
// 串行执行、跨连接并行；SQL 层 autocommit 语句不使用显式事务状态，
// 各自构造独立临时事务，天然可并发。
//
// 兼容性：旧 API（Engine.Execute / ExecuteAs 等单会话调用方，含 CLI、
// 既有测试与 sql_bench）使用 Executor 内置的全局会话 globalSess，
// 行为与旧版完全一致（显式事务状态全局共享，无并发隔离要求）。
type Session struct {
	mu sync.Mutex

	// 显式事务状态（BEGIN 后置位，COMMIT/ROLLBACK 复位）。
	inTx  bool
	curTx txn.Txn
	// M8 快照隔离版本：curBeginTS 为显式事务 BEGIN 时获取的快照版本；
	// stmtBeginTS 为 autocommit 语句首次读时惰性获取的语句快照版本
	// （每条语句独立，显式事务内不重置）。
	curBeginTS  uint64
	stmtBeginTS uint64
	// M18-A 显式事务 ID：BEGIN 时生成，COMMIT/ROLLBACK 清零；
	// 随 QUERY_REQ 等集群帧跨节点传播（事务上下文），供参与者识别
	// 写请求所属会话事务；空串表示无显式事务上下文（零回归）。
	curTxID string
	// M19 显式事务写锁：heldLocks 为事务内写语句（UPDATE）持有的行锁/表锁
	// 解锁闭包，COMMIT/ROLLBACK 统一释放，覆盖"读旧值→提交"全临界区，
	// 防止后事务在前事务未提交时读旧值覆盖写（丢更新）；heldLockKeys 记录
	// 已持有的 key 防重入（事务内多次写同一行不自锁）。
	heldLocks    []func()
	heldLockKeys map[string]bool
	// 递归防护深度：triggerDepth 防触发器无限递归；
	// procDepth 防存储过程嵌套展开。
	triggerDepth int
	procDepth    int
	// M18：prepared statement（名称→SQL 文本，会话级）。
	prepared map[string]string
	// M18：用户变量（@name→值，会话级；SET @a=... 写入，SELECT @a 读取）。
	userVars map[string]Value
}

// NewSession 创建独立会话。服务端每 TCP 连接调用一次；
// 同一 Session 不应被多个 goroutine 同时使用（连接内串行语义）。
func (e *Executor) NewSession() *Session {
	return &Session{
		prepared: map[string]string{},
		userVars: map[string]Value{},
	}
}

package sql

// v2.0.5 建表成功后主动补隐式整表 region 归属（根治"首写必失败"）。
//
// 背景（v2.0.4 遗留）：隐式整表 region 归属只在权威路由视图同步通道
// （REGION_PUSH SyncOnly）派生下发，建表路径不登记。于是新建未切分表后，
// 集群各节点本地路由视图都没有该表条目：
//   - 纯转发节点写入时 routeUnassigned → 拒绝一次，靠 decideWriteRoute 的
//     自愈拉视图 + 业务重试才成功（真机表现为"首写必失败、重试成功"）；
//   - 持有节点写入同样先失败一次。
//
// 本文件在 DDL autocommit 提交并广播成功后补一次归属登记：
//  ① 本机登记：只对"本次新建的那张表"按确定性 owner 规则写内存映射
//     （Router.SetImplicitRegionOwner），纯内存、不落盘、不开事务、不遍历表目录；
//  ② 对端推送：异步延迟后向在线对端推一次权威路由视图（PushRouteViewToNode，
//     载荷含 implicit routes），使其余节点同步骤学到归属；
//  ③ 两条路径都在建表事务之外，且推送带冷却去抖，绝不阻塞建表语句返回。
//
// 【v2.0.5 时序纪律（实测教训）】原实现在建表返回路径上做"本机自学"时是遍历
// 表目录推导的（implicitRoutesForViewSync 内部 tm.Begin + loadTables），等价于在
// DDL 紧邻窗口插入一次事务快照：pkg/db 的 D4.5 B1 单副本可恢复用例随即出现
// 时序回归（12 次基线全过 → 改动后 9 次 3 败，B1 ensure count=0、
// "region has no live copy"，32s 超时）。这正是 v2.0.4 结论的复现：
// DDL 路径上新增任何事务都会打乱其后的副本维护时序。故本层严格约束：
//   - 建表返回路径只允许纯内存写入与 goroutine 派生，禁止 tm.Begin/遍历表目录；
//   - 视图生成（含 implicitRoutesForViewSync 的目录遍历）与归属登记只允许发生在
//     异步 goroutine 中，建表语句返回路径上零副作用。
//
// 【v2.0.5 归属口径与用例约束】隐式整表 region 归"在线节点中 ID 字典序最小者"。
// 因此"建表节点 ≠ 归属节点"时，切分前的写入经 2PC 转发到归属节点落盘；此后
// SplitTable 把显式 region 归给切分发起节点，就会形成"region 在 A、数据在 B"，
// B1 判 lost。pkg/db 的播种用例已改为"先切分、后写入"以避免依赖该归属
// （见 d4_4_route_view_test.go seedThreeRegions）。

import (
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// implicitPushCooldown 对端推送冷却窗口：批量建表（含脚本灌表）只推一轮，
// 避免把管理面打成广播风暴；错过的表由下一次建表/视图刷新自愈覆盖。
const implicitPushCooldown = 200 * time.Millisecond

// learnImplicitRouteLocal 登记本机对该表隐式整表 region 的归属认知。
// 只登记本次新建的这一张表：纯内存映射，不开事务、不遍历表目录、不落盘。
// 单机（无远端节点）不派生、不改语义；已切分表不在登记范围内。
// 由 notifyImplicitRouteView 在异步补推 goroutine 内调用（不在建表返回路径上）。
func (e *Executor) learnImplicitRouteLocal(tableID uint64) {
	if e.router == nil || tableID == 0 {
		return
	}
	tid := storage.ID(tableID)
	// 已切分表：隐式整表 region 已不存在，以分片视图为准，绝不登记。
	if e.router.HasStoredRegions(tid) {
		return
	}
	owner := e.implicitRouteOwner()
	if owner == "" {
		return
	}
	e.router.SetImplicitRegionOwner(tid, owner)
}

// notifyImplicitRouteView 建表成功后补隐式归属：异步 goroutine 内先登记本机认知，
// 再向在线对端推一次权威路由视图。建表语句返回路径上只做一次轻量状态判断（不触盘、
// 不开事务、不加数据面锁），实际学习与 RPC 都在 goroutine 中完成。
// 调用方须已释放 DDL 锁（broadcastDDL / commitTx 返回之后），且不得在事务内调用。
// tableID 为本次新建表的表 ID。
func (e *Executor) notifyImplicitRouteView(tableID uint64) {
	if e.mgr == nil || !e.mgr.HasRemoteNodes() || e.router == nil {
		return
	}
	// 测试注入：替代真实对端推送，避免用例里发起网络 RPC；学习同步执行便于断言。
	if e.implicitPushHook != nil {
		e.learnImplicitRouteLocal(tableID)
		e.implicitPushHook()
		return
	}
	e.implicitPushMu.Lock()
	if e.implicitPushBusy || (!e.implicitPushAt.IsZero() && time.Since(e.implicitPushAt) < implicitPushCooldown) {
		e.implicitPushMu.Unlock()
		return
	}
	e.implicitPushAt = time.Now()
	e.implicitPushBusy = true
	e.implicitPushMu.Unlock()
	mgr, selfID := e.mgr, e.selfID
	go func() {
		defer func() {
			e.implicitPushMu.Lock()
			e.implicitPushBusy = false
			e.implicitPushMu.Unlock()
		}()
		e.learnImplicitRouteLocal(tableID)
		for _, n := range mgr.ListNodes() {
			if n.ID == selfID || mgr.IsDown(n.ID) {
				continue
			}
			// 尽力而为：单节点失败不阻断其余节点，也不影响建表语句结果；
			// 未收敛的表由下次建表或写路径自愈（multinode.go）兜底。
			_ = e.PushRouteViewToNode(n.ID)
		}
	}()
}

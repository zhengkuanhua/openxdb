# Changelog

OpenXDB 版本变更记录。

> 版本线说明：历史版本以仓库 README Status 行、docs/ 设计文档头部标注与 git 提交记录为准。早期部分中间 -alpha 版本未单独打标（git 与文档均无显式版本号），按里程碑区间归纳，版本号不做臆造。

## Unreleased

**Added**
- **TSO 租约兜底与失效切换（v2.1.0 H1 / NEW-5，P0，默认关闭）**：v1.21.0 三机真机复验 §10.4 确认 TSO 仍是写路径硬单点——停掉发号节点后全集群写失败（`tso unavailable`），恢复需人工改 `--tso-node`。本次在既有 epoch 选主之上补齐"何时可以安全换人"的判定：发号权由组内**多数派授予的租约**决定（`TSO_LEASE_VOTE` / `TSO_LEASE_RENEW` 两帧），主失联或续约未获多数派即自行降级、到期拒绝发号，由客户端按回执 `retry_after_ms` 等待旧租约到期后重新拉票，选出新主接续发号，无需人工介入。
  - **安全不变量**（均配单测）：①号段单调不减——接任方以"调用方已观测最大戳 ∪ 投票回执中各副本 `last`"为水位抬升号段窗口，首批号严格越过旧主已发序列（即使接任客户端刚启动、`maxSeen=0`）；②跨节点不重号——同纪元一票制 + 租约窗口取"拉票发起时刻+TTL"（⊂ 各副本授予窗口），双主窗口为零；③租约判断只用本地时长（`now` 可注入，`time.Now()` 自带单调读数），不比较跨节点绝对时间，不依赖单点时钟；④分区少数派拿不到多数派即无法接任。
  - **开关与兼容**：`--tso-lease` / `[server] tso_lease`（默认 **off**，未真机验收前不启用）+ `--tso-lease-ttl` / `[server] tso_lease_ttl`（默认 10s，续约 TTL/3）；需配合 `--tso-ha-group`，未配置组时启动报错。关闭状态下发号权仍由 v1.21.0 epoch 选主单独决定，行为与 v1.21.0 完全一致；`--tso-node` 单点部署不受影响。
  - **不破坏既有预取**：NEW-5 低水位预取、`maxSeen` 单调护栏、`acceptLocked` 回退拒绝全部保持原样，租约只接管"选主等待策略"，不改号段消费路径。
  - 改动文件：`pkg/cluster/tso_lease.go`、`pkg/cluster/tso_lease_client.go`（新增）、`pkg/cluster/tso_ha.go`、`pkg/cluster/tso_ha_client.go`、`pkg/cluster/server.go`、`pkg/db/tso_ha.go`、`cmd/openxdb/main.go`、`pkg/configfile/configfile.go`；单测 `pkg/cluster/tso_lease_test.go`、`cmd/openxdb/main_tso_lease_test.go`。

## v2.0.1 (2026-11-24)

**版本口径**
- **发行版本号由 v2.0.0 调整为 v2.0.1（补丁发布）**：本版交付 H1 TSO 租约失效切换的同纪元选举死锁修复（提交 `2459d64`），相对 v2.0.0 仅含该缺陷修复与配套活性单测，不含新 SQL 语法、不新增或改动默认开关、不动存储引擎与读路径；`MySQLServerPrefix`（`8.0.36-OpenXDB `）兼容契约不变，mysql CLI / JDBC 仍识别为 MySQL 8.0.x。

**Fixed**
- **H1 租约失效切换同纪元选举死锁（P0）**：`runElect` 竞选失败只回滚 `seenLeader` / `seenUntil`，未回滚 `votedEpoch` / `votedFor` 且不推进纪元，导致同纪元内各候选自投互斥、租约永远拿不到多数派（实测报错 `lease quorum not reached (1/2) (epoch=4)`），主节点停摆后集群无法自动换人。修复：① `runElect` 失败完整回滚 `voted*`，并将纪元推进到 `max(seen, voted)+1`；② `vote()` 同纪元拒票改为带回观测纪元，不再自旋；③ `electOnceLocked` / `electLeaseLocked` 失败推进纪元并做毫秒级短退避重试（上限 5 次）。
  - 改动文件：`pkg/cluster/tso_lease.go`、`pkg/cluster/tso_ha_client.go`、`pkg/cluster/tso_lease_client.go`；新增 `pkg/cluster/tso_lease_liveness_test.go`（同纪元碰撞、并发竞选、串行重试、租约过期重拉票 4 组活性用例，18 项断言）。
  - 真机复验：三机 v2.0.0 + `tso_lease=on`，双向终止 leader 各一轮，累计写入 220 条全部成功，零 `lease quorum` 错误。
  - 开关状态不变：`--tso-lease` / `[server] tso_lease` 仍默认 **off**，关闭时行为与 v2.0.0 完全一致。

## v2.0.0 (2026-11-23)

**版本口径**
- **发行版本号由 v1.21.0 升格为 v2.0.0（发布口径，无新增功能改动）**：本次仅调整 `pkg/version` 版本常量与发布文档口径，代码实现与此前 v1.21.0 提交 `5c98877` 等价；`MySQLServerPrefix`（`8.0.36-OpenXDB `）保持不变的兼容契约继续有效，mysql CLI / JDBC 仍识别为 MySQL 8.0.x。升格理由为集群轨（2PC / 持续复制 / 写授权 / TSO / 跨区读）能力首次以正式大版本对外交付，并按双端 Release 附带二进制与 `SHA256SUMS`。

**Fixed**
- **版本常量漂移修正，三机版本串统一为 v1.21.0（版本一致性，P1）**：`pkg/version/version.go` 的 `const Version` 仍停留在 `v1.20.1`，而实机二进制已含 v1.21.0 全部功能（8c3a4f4），导致启动横幅、`openxdb version`、握手包与 `@@version_comment` / `select version()` 四处口径统一显示旧串，运维易误判为未部署新版本。修复将常量改为 `v1.21.0` 并同步 `ServerVersion` 注释；`MySQLServerPrefix`（`8.0.36-OpenXDB `）按兼容契约**保持不变**，确保 mysql CLI / JDBC 仍识别为 MySQL 8.0.x。重新静态编译（md5 `3efe118c3eb284d8747bcde64266a63f`，链接期使用 `librocksdb.a` + `libzstd.a`，无运行时 libzstd 依赖），并按 node-a → node-b → node-c 滚动优雅重启。真机复验 PASS：三节点 `select version()` 均为 `8.0.36-OpenXDB v1.21.0`，`SHOW NODES` 三节点全 UP，只读计数跨节点一致（`v1210_t1`=103、`v120_t1`=56、`t121_t2`=1），分别由 node-a / node-c 发起的写入三节点均可读回。
- **归属写 fencing 下沉 region 粒度并收口接管误授（40bcc19，NEW-3，P0）**：修复 owner 节点因历史误授标记被**节点级** fence 永久拦截所有写（含转发型写）、重启不恢复的 P0 缺陷。根因为三条叠加——`cluster_term.meta` 持久化了误授记录（`owned=false` 却被判为曾持有）、`notifyLocalTakeover` 恒传 `selfOwns=true`（任何走接管路径的节点都被记为曾持有目标 region）、归属 fence 做成节点级全局判定（`!clusterOwned` 即拒写，单个 region 的误授放大为本节点全部写被拦）。修复将归属 fence 下沉到 region 粒度：仅当本节点当前持有目标 region 时才做归属一致性 / epoch 校验，目标 owner 非本机的转发型写放行，归属不可判定时退回节点级保守拒写；`notifyLocalTakeover` 改为携带 region id，并加 `takeoverLandedLocally` 前置判定（路由快照该 region 的 `Node` 为本机、`regionEpoch` 非零、历史接管纪元不大于本次）后才上报归属。新增 `v1210_new3_region_fence_test.go`、`v1210_new3_region_ownership_test.go`；P9 契约测试全绿，集群轨授权不写 `masterEpoch`。真机三机复验 PASS（201 / 202 / 203 三入口各写 3 行，三端行数与行集合一致，26 → 29）。
- **复制轨 fencing 增加 `clusterOwned` 短路，修复语句级写入口误拒回归（df7ad73，NEW-3，P0）**：真机滚动重启复验暴露 owner 节点历史降级未复位（`readOnly=true`）且 `d.epoch > masterEpoch` 时，语句级入口 `AllowWriteStatement` 只走复制轨判定、缺少 `clusterOwned` 短路，导致合法 owner 写入恒报 `demoted stale master is read-only`。修复把短路收敛在公共函数 `copyTrackWriteFenceLocked` 开头（`if d.clusterOwned { return nil }`），`AllowWrite` / `AllowWriteStatement` / `AllowWriteRegion` 三个入口共用同一口径；`clusterOwned=false` 时保留原有 `ErrFenced` 语义，归属收回 / 重启待确认仍由归属 fence 拒写。新增 `v1210_new3_copy_track_shortcircuit_test.go`（`TestNew3CopyTrackFenceSkippedWhenClusterOwned` / `TestNew3CopyTrackFenceKeptWhenNotClusterOwned`）。

**Added**
- **TSO 号段低水位预取与抖动容忍（NEW-5）**：`HATSOClient` 与单点 `tsoClient` 由「缓存耗尽才拉批」升级为「低水位异步预取 + 池内兜底」——剩余量低于 `batch/4`（最小 1）时后台补充拉取，拉取失败按短退避重试且**不使当前取号失败**，仅在池真正耗尽时返回错误；任何补充响应仍须严格大于 `maxSeen`（回退保护），否则丢弃并按拉取失败计。新增观测字段 `prefetchFails`。`tso.Source` 接口不变。测试 `pkg/cluster/v1210_new5_prefetch_test.go`（低水位触发、抖动吸收、`maxSeen` 回归保护、退避、HA 侧预取、并发取号不重复、接口不变）。

**Changed**
- **跨区不可达读默认显式报错（NEW-6，行为变更）**：归属节点不可达且重指派后无存活持有者时，读路径（`resolveReadOwner`）不再静默回退本地副本返回部分结果，改为返回含 region 与节点标识的错误（如 `remote region unavailable: region 3 (node-b: dial tcp ...)`）；分布式排序、`ORDER BY ... LIMIT` 合并等路径同步收口。存量部署可设 `--allow-partial-read` / `[server] allow_partial_read=on` / `SET allow_partial_read=on` 退回旧行为（本地副本尽力服务并打 WARN）。测试 `pkg/sql/v1210_new6_partial_read_test.go`（不可达报错、兼容开关回退、本地 owner 与单机零回归、配置键注册）。

## v1.20.1 (2026-11-22)

**Fixed**
- **归属视图分裂收敛（3bf395d，P0）**：`selfOwnsAny()` 由「`Node == ""` 即视为本机归属」收紧为「`Node == SelfID`」或「`Node == "" && Local == true`」，修复真机 T3 复验暴露的分区恢复后 node-a 与 node-c 同时 `self_owns=true`（双写风险）；新增 `reconcileOwnerView()` 在 `applyPush` / `applyRouteSync` 落盘 `m:regions` 前收敛「归属他人却标本地」的矛盾条目（只清内存 `Local` 标记、不触碰物理数据），并新增 `ownerConflicts` 累计计数与 `OwnerViewConflicts()` 只读口径。测试 `pkg/cluster/v1201_owner_view_split_test.go`（未指派不持有不自认 owner / 未指派但持有仍归属 / 冲突收敛且计数 +1）。
- **归属视图落盘归一（f160387，P0）**：本地 `ASSIGN REGION` 只改 `Node` 不触碰 `Local`，复制轨推来的 route 可能残留 `local=true`，使跨 region 2PC 用例对 route `local` 标记的断言偶发失败。新增 `normalizeRegionOwnerView()` 于 `persistRegions` 落盘前按 `Node` 统一归一 `Local`（归属远端置 `false`，本机归属与未指派不变，幂等）。测试 `pkg/sql/v1201_owner_view_normalize_test.go`。

**Changed**
- **测试稳定性加固（仅测试侧，不改产品行为）**：新增 `testDir()` 替代 `tb.TempDir()`，目录清理放入独立 goroutine 限时 5s——消除 Windows `DELETE_PENDING`（`os.RemoveAll` 阻塞在 `NtSetInformationFile`）导致的整包随机挂死；新增 `safeCloseDB()`，`db.Close` 限时 30s，规避 rocksdb 后台 compaction/flush 线程 join 偶发长时间阻塞拖死全量测试。`pkg/sql` 全部 15 个测试文件已切换。

**Docs**
- `KNOWN_LIMITATIONS.md` 新增 §3.6 归属视图收敛与写授权口径（不变量、收敛口径、`OwnerViewConflicts()` 排障建议）；`DEV_PLAN_v1.20.1.md` 回填 P0-1 / P0-2 完成态与实测证据（计划文档仍为本地 untracked）。

**Known**
- `pkg/db` 全量耗时在 80s~330s 间波动（`TestM13SlowQueryPublic` 等含后台 IO 的用例串行叠加），属慢而非挂；产品侧 `DB::Close` 在 Windows 下偶发长时间阻塞的根因（后台线程 join 与 WAL checkpoint 循环竞争）未在代码层消除，随 v1.21.0 写轨生产化统一处理。
- `NEW-1` 陈旧主写拒绝 / `NEW-2` 重启矩阵代码级已就绪，三独立 IP 真机复跑仍为欠账，与 T3 真分区 fencing 一并归入 v1.21.0 真机收口范围。

## v1.20.0 (2026-11-22)

**Added**
- **C1 2PC 参与者提交接入提交钩子（989503c，P0）**：`pkg/cluster/txn2pc.go` 参与者侧 COMMIT 不再直接 `st.Write(batch)`，改走 `db.Txn` 提交钩子路径，使 2PC 远端提交同样产生 binlog（此前绕过钩子导致远端写无 binlog）。开关 `ha_2pc_participant_hook` 默认 `off`，关闭时退回直写路径；测试 `pkg/db/v1200_c1_participant_hook_test.go`（V-B1）。
- **C2 写路径强制路由闭环（e386d47，P1）**：新增开关 `ha_write_route_enforce`（默认 `off`），以 `routeDecision` 四态判定取代死代码 `checkWritable`（已删除）；显式分区的未指派场景不再默认本地，集群模式下孤立视图被显式拒绝。测试 `pkg/sql/write_route_enforce_test.go`（V-A1/A2/A3/A5）。
- **C3 2PC 参与面扩展（7588810，P2）**：参与者集合纳入 region 副本（副本集合落独立元数据键 `m:repl:replicas:<rid>`，不改 `sharding.RegionInfo`），参与者 PREPARE 校验自身 region 归属并做副本 quorum 判定；开关默认 `off`，关闭时参与者集合退化为仅 owner，语义与 v1.19.0 逐字一致。测试见 `pkg/cluster/v1200_c3_participant_owner_test.go` 等（V-B2/B3）。
- **C4 持续复制 region 化（aeecbca，P3）**：binlog 记录携带 region 维度，影子副本由「静态一次性 bootstrap」改为持续跟随 owner 写入（新增 `pkg/db/ha_region_sync.go`、`pkg/replication/binlog_region.go`）；旧格式 binlog 向下兼容。V-C1~V-C6 通过（V-C7 为静态基线核对，非运行期用例）。
- **C5 集群轨写授权统一（e63c6c0，P4）**：新增 `pkg/cluster/partition_snapshot.go` 分区状态快照——由心跳 watcher 每秒采样刷新，写路径最多一个调用者触发真实采样、其余立即复用旧快照（`stale-while-refresh`，TTL 2s），彻底消除「逐语句真实 PING 远端」带来的 P95 延迟绑定点；新增 `pkg/sql/ha_c5.go` 节点级分区写授权（错误码 `CLUSTER_MINORITY`），在 `groupOps` 中先于 region 归属校验生效；`majorityProbe`（P3 语句级 fence）改为优先读快照，快照不可用时回退仲裁器同步判定。原 C2 后置的 V-A4「分区场景拒写」由此交付。
- **测试**：`pkg/sql/ha_c5_test.go`（分区少数派拒写与恢复放行、quorum 仅联动分区拒写不联动归属判定、单机零回归）、`pkg/cluster/partition_snapshot_test.go`（采样刷新与 TTL 复用、未采样返回 unknown、watcher 刷新后快照可见）。
- **C6 发版收尾**：`pkg/version` 版本常量升级至 `v1.20.0`，CLI 版本输出、MySQL 握手版本串与 `select version()` 三者同源；`RELEASE_NOTES` / `README` / `KNOWN_LIMITATIONS` / `docs/index.md` 文档口径同步。

**Changed**
- `CheckPartitionRecovery` 改为经 `RefreshPartitionSnapshot` 刷新快照，由 watcher 周期采样承担探活，取代逐语句真实探活。
- 分区拒写由 `ha_write_route_enforce=on` **或** `ha_quorum_enabled=on` 触发；而 region 归属的三类判定（LOCAL / NOT_ASSIGNED / NOT_LOCAL）仍只读显式位，不随 quorum 联动——既有 quorum 用例的集群轨基线写保持放行，避免误拒导致的零回归破坏。
- 拒绝计数复用 P3 fence 计数器（`PartitionMinorityFencedCount`），不新增独立统计口径。

**Docs**
- `DEV_PLAN_v1.20.0.md` 回填 C1~C5 完成态（含提交号）与 §8 本轮实测结论；本轮计划文档仍为本地 untracked 文件，不入库。

**Known**
- `ha_quorum` 仍默认关闭（决策点 D-5 未解除）；D-3「C5 后再议默认 on」本轮不执行，待真机证据后单独评估。
- 环境级验收（T3 真分区 fencing、NEW-1 陈旧主写拒绝、NEW-2 重启矩阵，三独立 IP 真机）记为欠账，不阻塞本版发版；阻塞项仅代码级验收（C1~C5 全部用例）。
- 已定位的既有 flaky：`pkg/db` 的 `TestV1200C4TwoPCWriteEntersRegionRepl` 偶发失败（单跑与二轮全量均 PASS）；`pkg/sql` 全量随机 hang 经与 HEAD 基线对照确认与 v1.20.0 改动无关。

## v1.19.0 (2026-11-20)

**Added**
- **远端区间聚合 + 远端 Scan 流式协议（`remote_range_agg`，默认 off）**：跨 region 多区间读由「逐区间独立 `QUERY_REQ` 往返」合并为「按归属节点聚合的单次 `QUERY_MULTI` 往返」。协议新增帧 `msgQueryMultiReq` / `msgQueryMultiResp`（编号 66/67，避开既有 REGION_PUSH 20-21、2PC 22-27、TSO 28-29、DDL 30-31、注册表 32-33、备份恢复 40-45、epoch 50、全量对齐 60-61、TSO HA 70-71）；新增 `QueryRange` / `MultiQueryReq` / `MultiQueryChunk`，服务端按区间复用既有 `scanRange`（含 `beginTS` 版本过滤与排序截窗）后以 512 行 / 4MB 预算分块流式回传，`Done` 收尾、任一区间出错即错误前置。客户端新增 `Client.ScanMulti` 与 `Manager.ScanMultiRemote`。
- **执行器侧接线**：`pkg/sql` 的 `scanMergedCluster` 在开关开启时分流至新增 `scanMergedClusterAgg`——按 `sharding.DecodeRegionKey` 分类，本地区间一次 `scanMergedVisible` 合并扫描，同归属节点的远端区间按节点分组后单次取回，结果按内层键全局稳定排序，与旧路径逐行等价；保留 M6 读 failover（`resolveRegionOwner`）、P3 READY 前置（`checkRemoteReady`）、P2-13 Serializable 远端读集登记语义。
- **独立 feature flag**：`remote_range_agg` 四级透传（`configSpec` 注册默认 off → `Engine` → `Executor.SetRemoteRangeAgg` / `RemoteRangeAgg` 原子读写 → `scanMergedCluster` 分流），关闭时完全走 v1.18.0 逐区间路径，不发新帧、零回归可灰度回退。
- **测试**：`pkg/cluster/query_multi_test.go`——同节点多区间单次往返且与逐区间 `ScanRemote` 逐行等价、超 512 行分块流式、空区间与未知节点错误、服务端排序截窗语义；`pkg/sql/remote_range_agg_test.go`——同节点两远端区间往返 2 → 1、本地+远端混合结果行序与内容等价、全本地区间零远端往返、目标节点未就绪整体拒绝且不发起往返。
- **存储过程异常处理**：新增 `DECLARE {CONTINUE|EXIT} [<name>] HANDLER FOR <cond>[, ...] <stmt>`，条件支持 `SQLSTATE '<值>'`、数字错误码、`SQLEXCEPTION`（非 01/02 类）、`SQLWARNING`（01 类）、`NOT FOUND`（02 类）；处理器按「最近声明优先」匹配，随块退出自动出栈（`procRunner.handlers` 栈 + 块级 `depth` 截断），`CONTINUE` 继续执行出错语句之后的语句、`EXIT` 退出当前块。游标取尽（`02000`）纳入 `NOT FOUND` 捕获范围，错误文本不变。新增 `SIGNAL SQLSTATE '<值>' [SET MESSAGE_TEXT|MYSQL_ERRNO|SQLSTATE = ...]` 与 `RESIGNAL [SET ...]`（无参重抛当前错误，处理器执行期间禁止自捕获）。无 SQLSTATE 的过程错误统一归入 `HY000` 供 `SQLEXCEPTION` 匹配。
- **存储过程动态 SQL**：新增 `PREPARE <stmt> FROM '<文本>'|@var`、`EXECUTE <stmt> [USING <expr>[, ...]]`、`DEALLOCATE PREPARE <stmt>`。`?` 占位符按出现顺序以实参字面量绑定（个数不符报 `argument count mismatch`），绑定结果走标准 `Parse` + `execStmt` 路径，并对语句类型做过程体白名单校验（如 `SHOW` 等过程体不支持的语句不允许经动态 SQL 绕过）。预处理语句表为单次 CALL 调用级（`procRunner.prepared`），不跨调用泄漏。
- **存储过程嵌套 `BEGIN…END` 块**：过程体内 `BEGIN … END` 可任意层嵌套，块内 `DECLARE` 的局部变量、游标、处理器均建立独立嵌套作用域——进入块时压栈、退出块时按进入前快照整体出栈（同名变量在块内遮蔽、块退出后恢复外层值；块内声明的游标与处理器不再对外层可见，块外引用报 `unknown procedure variable` / 未声明游标等既有错误文本）。块内 `EXIT` 仅退出当前块（不影响外层控制流），`ITERATE` / 循环语句在嵌套块中语义不变。
- **命名条件 `DECLARE <name> CONDITION FOR ...`**：支持 `SQLSTATE '<值>'` 与数字错误码两种定义形式；命名条件按块作用域登记（块内声明仅块内可见），可在 `DECLARE ... HANDLER FOR <name>`、`HANDLER FOR CONDITION <name>`、`SIGNAL <name>`、`RESIGNAL <name>` 中按名引用，匹配时与直接书写 `SQLSTATE` / 错误码完全等价。未声明或已出作用域的条件引用在创建期或执行期报 `unknown condition in ...`；`CONDITION FOR SQLEXCEPTION` 等不支持的形态报 `CONDITION FOR requires SQLSTATE`，非法状态值报 `invalid SQLSTATE`。
- **`GET DIAGNOSTICS`**：支持 `GET [CURRENT] DIAGNOSTICS [CONDITION <n> | CONDITION <@var>] <@var> = <item>[, ...]`，诊断目标覆盖 `ROW_COUNT`（最近一条 DML 的影响行数）与 `CONDITION <n>` 条件信息项 `RETURNED_SQLSTATE` / `MYSQL_ERRNO` / `MESSAGE_TEXT`（诊断区仅保留最近一条，序号须为 1）。越界序号、`ROW_COUNT` 与 `CONDITION` 混用、无前置条件时读取条件信息项均报明确错误。
- **HANDLER 体支持多语句复合块**：`DECLARE {CONTINUE|EXIT} HANDLER FOR <cond> BEGIN <stmt>; ... END` 可含块内声明、条件分支与任意嵌套块，处理器执行期与普通过程体共用同一解释器路径。
- **缺陷修正**：过程内变量值带入普通 SQL 语句时按 SQL 标准转义单引号（`''` 表示一个字面引号）。此前含单引号的字符串变量（如 `GET DIAGNOSTICS ... MESSAGE_TEXT` 取到的错误文本 `SQLSTATE '45000': boom`）代入后会破坏重建后的语句并报 `expected )`，现可正常执行。
- **测试**：`pkg/sql/proc_handler_dynsql_test.go`（12 例）——CONTINUE/EXIT 语义、游标 NOT FOUND、嵌套块出栈、SQLEXCEPTION/SQLWARNING/错误码匹配、SIGNAL 未捕获冒泡与过程体中止、RESIGNAL 外层接管、PREPARE 字面量与 @var、EXECUTE USING 绑定、未定义语句 / 参数个数不匹配 / 空文本 / 白名单拒绝、创建期非法 SQLSTATE / 缺条件 / 缺语句体 / 不支持的 HANDLER 条件；`pkg/sql/proc_nested_block_test.go`（8 例）——嵌套块变量作用域与遮蔽恢复、块内 `EXIT` 只退本块、块内游标 FETCH 写外层变量、块内处理器随块出栈（外层未捕获错误上抛）、嵌套块内 `ITERATE`、三层嵌套、`BEGIN` 未闭合的创建期报错；`pkg/sql/proc_condition_diag_test.go`（9 例）——命名条件在 `HANDLER FOR <name>` / `HANDLER FOR CONDITION <name>` / `SIGNAL <name>` / `RESIGNAL <name>` 中的匹配与块作用域、`GET DIAGNOSTICS` 的 `ROW_COUNT` 与三项条件信息项、HANDLER 复合块体、以及各类创建期/执行期错误路径。
- **触发器体复合块 `BEGIN…END`**：`CREATE TRIGGER ... FOR EACH ROW BEGIN <stmt>; ... END` 的触发器体可写任意语句数的复合块，内部复用 v1.19.0 已落地的过程体解释器（`parseProcedureBody` + `procRunner`），由此自动获得块级作用域 `DECLARE`（变量 / 游标 / 命名条件 `CONDITION`）、`IF` / `LOOP` / `WHILE` / `REPEAT` / `CASE`、`DECLARE {CONTINUE|EXIT} HANDLER`（含 `BEGIN…END` 复合处理器体）/ `SIGNAL` / `RESIGNAL`、`CALL` 存储过程与 `PREPARE` / `EXECUTE` / `DEALLOCATE PREPARE` 动态 SQL；行上下文以 `procRunner.rowRefs` 注入，表达式按 `new.<col>` / `old.<col>` 环境键求值。体首 token 非 `BEGIN` 时仍走 v1.8.0 单语句路径（`Parse` + `NEW`/`OLD` 物化 + `execStmt`），执行行为与错误文案完全不变。
- **`BEFORE INSERT` / `BEFORE UPDATE` 的 `SET NEW.<col> = <expr>` 回写**：BEFORE 行级触发器可在体内（单语句体或复合块体）对 `NEW.<col>` 赋值，赋值按行上下文累积，该行全部 BEFORE 触发器执行完毕后统一写回待写入行切片（`applyToRow` 按 `TableMeta.Columns` 对齐），后续触发器与落盘读到的是回写后的值；列名大小写不敏感、以表实际列名落键。`OLD.<col>` 只读，`SET OLD.<col> = ...` 在执行期拒绝；`SET NEW.<不存在列>` 在创建期即报 `unknown column in NEW row`；`AFTER` 触发器与语句级触发器体内引用 `NEW`/`OLD` 仍按事件语义报错。
- **触发器执行顺序 `FOLLOWS` / `PRECEDES`**：`CREATE TRIGGER` 支持在 `FOR EACH ROW` 之后声明 `FOLLOWS <trigger>` / `PRECEDES <trigger>`（可叠加多个），触发时对同表同事件同时机的命中集做稳定拓扑排序（`orderTriggers`），无依赖约束的触发器保持创建顺序（与 v1.8.0 一致），依赖指向已不存在或非同组触发器名时忽略该依赖、成环时按创建顺序兜底。创建期校验：同一触发器不得同时声明 `FOLLOWS` 与 `PRECEDES`、不得引用自身、被依赖触发器须已存在且同表同事件同时机，违反均在建表期报错。
- **测试**：`pkg/sql/trigger_body_test.go`（13 例）——复合块体的 `DECLARE`+`IF`、`WHILE` 累加、`HANDLER`+`SIGNAL` 捕获、`CALL` 存储过程、`PREPARE`/`EXECUTE` 动态 SQL；`BEFORE INSERT`/`BEFORE UPDATE` 的 `SET NEW` 回写与 `AFTER` 触发器读到回写值；`FOLLOWS` / `PRECEDES` / 无依赖创建顺序兜底三组执行顺序断言（以自增序号列保证结果有序可比）；顺序与体语义错误路径（互斥声明、自引用、依赖不存在、依赖非同表同事件同时机、复合块体语法错误、`OLD` 只读、未知列、语句级体引用 `NEW` 被拒）；既有单语句体路径零回归断言。
- **事务保存点 `SAVEPOINT` / `ROLLBACK TO` / `RELEASE SAVEPOINT`**：新增事务子语句族，语法对齐 MySQL `SAVEPOINT <name>`、`ROLLBACK [WORK] TO [SAVEPOINT] <name>`、`RELEASE SAVEPOINT <name>`（关键字大小写不敏感，`ROLLBACK TO` 的 `WORK` 与 `SAVEPOINT` 修饰词均可省略）。`pkg/txn` 新增 `SavepointTxn` 接口与 `ErrSavepointNotFound`：`txnImpl` 在保存点标记处记录有序操作日志 `ops` 的序位（`savepointMark` 栈），`ROLLBACK TO` 用标记前的前缀重建写批次 `batch *storage.WriteBatch` 并原地重建 `writeSet`（回滚后写入不进提交、读己写 dirty 路径同步回到标记点状态），`Commit`/`Rollback` 结束后清空全部保存点。语义对齐 MySQL：重名保存点覆盖旧标记、`ROLLBACK TO` 不删除该点（可重复回同一标记点）、`RELEASE SAVEPOINT` 级联删除该点及其后所有点（不撤销已写入数据）、未知名报 `savepoint <name> does not exist`、未开启事务报既有文案 `no active transaction`、支持任意层嵌套。SQL 层新增 `SavepointStmt` / `RollbackToSavepointStmt` / `ReleaseSavepointStmt` 三个 AST 与对应解析分支（`parseSavepoint` / `parseRollback` / `parseReleaseSavepoint`），`Engine.IsSQL` 关键字同步；执行器侧保存点状态以会话栈 `spPending` 维护，同步记录 2PC 暂存水位（`pendingLocal` 长度 + `pendingRemote` 各节点长度），`ROLLBACK TO` 时本地与跨节点写集按同一水位前缀截断，`COMMIT`/`ROLLBACK`/会话复位统一清空。**分布式 2PC 场景策略：本地保存点生效、跨节点写集按写集前缀丢弃**——远端参与者的 `PREPARE` 延迟到 `COMMIT` 阶段，`ROLLBACK TO` 时远端不存在未提交状态，按前缀截断即等价于远端丢弃，无需参与者交互且不产生语义不一致（策略与理由记入 `KNOWN_LIMITATIONS` §1.5）。
- **测试**：`pkg/txn/txn_savepoint_test.go`（9 例）——三步基本流程（`TestTxnSavepointBasicFlow`）、回滚后 dirty 读己写回到标记点状态（`TestTxnSavepointRestoresDirtyState`）、多层嵌套逐层回滚（`TestTxnSavepointNested`）、重名覆盖（`TestTxnSavepointDuplicateNameOverwrites`）、`ROLLBACK TO` 可重复回（`TestTxnSavepointRollbackRepeatable`）、`RELEASE` 级联删除（`TestTxnSavepointReleaseCascade`）、未知名与未开事务报错（`TestTxnSavepointErrors`）、写集前缀重建（`TestTxnSavepointWriteSetRewind`）、回滚至空后提交（`TestTxnSavepointEmptyAfterRollbackCommit`）；`pkg/sql/savepoint_test.go`（10 例）——SQL 层三步基本流程与读己写（`TestSavepointSQLBasicFlow`）、语法变体与大小写不敏感（`TestSavepointSQLSyntaxVariants`）、未开事务报错（`TestSavepointSQLNoActiveTransaction`）、未知名报错（`TestSavepointSQLUnknownName`）、多层嵌套（`TestSavepointSQLNested`）、重名覆盖（`TestSavepointSQLDuplicateNameOverwrite`）、可重复回（`TestSavepointSQLRollbackRepeatable`）、`RELEASE` 级联删除（`TestSavepointSQLReleaseCascade`）、`COMMIT`/`ROLLBACK` 后保存点失效（`TestSavepointSQLCommitClears`）、回滚后再写入仍可正常提交（`TestSavepointSQLWriteAfterRollbackCommit`）。
- **递归 CTE（`WITH RECURSIVE`）**：新增 `WITH RECURSIVE <name> [(<col>, ...)] AS (<anchor> UNION [ALL] <recursive>) [, ...] <SELECT>` 语法，支持可选列名列表、可选 `ALL`（`UNION` 去重 / `UNION ALL` 保留重复）、同一 `WITH` 内与非递归 CTE 混用（后定义可引用先定义，递归 CTE 的 anchor 可引用前置普通 CTE，普通 CTE 引用递归 CTE 时按作用域取物化结果），递归 CTE 可被外层 `FROM` / `JOIN` 右端 / 子查询引用。实现上**非递归 CTE 继续走 v1.10.0 的宏展开路径（`expandCTEs` 深拷贝替换表名），代码零改动、行为零回归**，仅 `RECURSIVE` 定义走新增物化 worktable 路径（`pkg/sql/cte_recursive.go`）：执行 anchor 得初始行集 → 每轮把上一轮新增行（delta）作为递归项自引用表的输入反复执行 → 无新增行即收敛；`UNION` 按整行（各列值序列化）去重、`UNION ALL` 不去重。物化结果以语句级作用域栈（`Session.recCTEPush/recCTEPop/recCTELookup`，内层遮蔽外层、语句结束出栈）注入执行器，`execSelectRel` 的 FROM 主表与 JOIN 右端命中时直接取物化结果（按别名重限定列名），并强制该语句走既有 Rel 管线（过滤/JOIN/聚合/排序）以复用全部既有语义。递归项与 anchor 的读仍走既有物理表扫描 / `scanMergedCluster` 路径，**不绕过 region 路由与可见性过滤**。
- **递归 CTE 限制与配置**：新增配置项 `recursive_cte_max_depth`（默认 1000，对齐 MySQL `cte_max_recursion_depth`），支持 `SET recursive_cte_max_depth = <n>` 热切换与 `SHOW` 查看，取值非正数报 `invalid value for recursive_cte_max_depth`；达到上限报 `recursive CTE <name> exceeded maximum recursion depth <n>`，**不静默截断结果**。递归项校验（MySQL 口径）：自引用缺失报 `must be referenced in the FROM clause of the recursive term`、同一递归项内重复引用报 `must not be referenced more than once`、出现在子查询/派生表内报 `is not allowed inside a subquery of the recursive term`、作为聚合/窗口函数参数报 `is not allowed as an aggregate function argument in the recursive term`；递归项列数与 anchor（或声明列名列表）不一致报 `column count mismatch`；未声明 `RECURSIVE` 却写 `UNION` 的定义报 `requires WITH RECURSIVE`，未声明 `RECURSIVE` 的循环引用仍报既有文案 `cyclic CTE reference`。
- **缺陷修正**：`JOIN ... ON <cond>` 之后紧跟外层 `)` 时（典型如 CTE 定义体、派生表收尾）解析器会把该括号当作 `ON (cond)` 的闭合符吞掉，导致随后报 `expected )`。现改为仅在 `ON` 子句自身打开过包裹括号时才消费结尾 `)`（`parseJoin` 的 `parenBudget`），带括号的 `ON (a.x = b.x)` 写法行为不变。
- **测试**：`pkg/sql/cte_recursive_test.go`（12 例）——1..N 递推序列与输出列名（`TestRecursiveCTESequence`）、多列列名列表与列间递推（`TestRecursiveCTEMultiColumnList`）、树形父子表遍历且递归 CTE 作 JOIN 右端（`TestRecursiveCTETreeTraversal`）、菱形图下 `UNION` 去重与 `UNION ALL` 保留重复（`TestRecursiveCTEUnionDedupVsUnionAll`）、环形图 `UNION` 去重收敛（`TestRecursiveCTEUnionCycleTerminates`）、深度上限默认值/`SET` 热切换/超限报错/非法取值（`TestRecursiveCTEDepthLimit`）、递归项与列名列表列数不匹配（`TestRecursiveCTEColumnCountMismatch`）、递归引用非法位置四类（缺失 FROM 引用、重复引用、子查询内、聚合参数内，`TestRecursiveCTEInvalidReferencePosition`）、与普通 CTE 三种混用形态（`TestRecursiveCTEMixedWithPlainCTE`）、anchor 空集不启动迭代（`TestRecursiveCTEEmptyAnchor`）、作用域不跨语句泄漏（`TestRecursiveCTEScopeNotLeaked`）、非递归 CTE 既有行为零回归（链式引用 / `cyclic CTE reference` / 未声明 `RECURSIVE` 的 `UNION` 报错 / 未定义表仍报错，`TestRecursiveCTENonRecursiveUnchanged`）。
- **SQL JSON 函数族（D6）**：新增 8 个 JSON 标量函数——`JSON_VALID`、`JSON_EXTRACT`、`JSON_UNQUOTE`、`JSON_TYPE`、`JSON_LENGTH`、`JSON_OBJECT`、`JSON_ARRAY`、`JSON_SET`（函数名大小写不敏感）。实现（`pkg/sql/json_func.go`）自带保序 JSON 文档模型（object 保留键序、number 保留原始字面量），解析用 `encoding/json` + `UseNumber` 并校验尾部无多余内容；输出为 MySQL 紧凑风格（分隔符 `", "` / `": "`，转义 `" \\ \n \r \t \b \f` 及控制字符 `\uXXXX`）。路径语法支持根 `$`、成员 `.key`、引号成员 `$."key with space"`、数组下标 `[n]`，`JSON_EXTRACT` 支持多路径并按路径序输出数组（缺失路径以 `null` 占位）；`JSON_SET` 为 copy-on-write 深拷贝写入，支持替换既有键、追加缺失键、数组末尾追加、根替换与多路径顺序写入，中间层级为标量或数组下标越界时明确报错。`JSON_TYPE` 返回 `OBJECT`/`ARRAY`/`STRING`/`INTEGER`/`DOUBLE`/`BOOLEAN`/`NULL`，`JSON_LENGTH` 对标量返回 1。
- **表达式函数节点**：`ast.Expr` 新增 `Fn string` / `Args []*Expr`（不改变既有 `Op`/`Left`/`Right`/`Ref`/`Col`/`Val` 语义；`isPlainColumn()`/`isLiteral()` 显式排除函数节点，避免被误判为列引用），解析层在 `parseExprPrimary` 与 `parseSelectColumn` 注册 JSON 函数名后由通用 `parseExpr` 生成 Fn 节点，因而支持任意位置出现——投影列（含算术与 `AS` 别名组合）、`WHERE` 条件、`UPDATE SET` 赋值、`CASE` 分支及函数实参嵌套。求值接入三条路径：投影与 `WHERE` 走 `evalExprForRow`、`UPDATE SET` 走 `evalSetExpr`，并同步 `collectExprCols`/`hasColRef`/`joinExprResolvable`/`exprHasCTECol`/`exprHasUserVar`/`replaceTrigRefExpr`/`collectTrigRefsExpr` 的实参遍历，保证列依赖收集、分布式 JOIN 可解析判定、递归 CTE 列引用、用户变量批量化与触发器 `NEW.`/`OLD.` 替换对函数实参同样生效；嵌套调用的 JSON 结果按 JSON 值而非字符串参与构造（如 `JSON_ARRAY(JSON_ARRAY(1))` → `[[1]]`）。
- **结果类型与报错口径**：所有 JSON 函数仅使用既有 `Value` 的 `TEXT`/`INT`/`NULL` 类型，不引入新 `Kind`，协议层零改动；非法 JSON 文本报 `invalid JSON text`（携带函数名与实参位次）、非法路径报 `invalid JSON path` 系列（含空成员名 `empty member name`、引号成员未闭合 `unterminated quoted member`、缺 `]` 等）、实参个数不符报 `incorrect parameter count` / `wrong number of arguments`，均不静默返回 `NULL`；`JSON_VALID` 例外——对任意（含非法）文本返回 `1`/`0`；`NULL` 实参按 SQL 语义传播为 `NULL`。
- **测试**：`pkg/sql/json_func_test.go`（15 例）——`TestJSONValid`（合法 1 / 非法 0 / 尾部垃圾 / 空串 / `NULL` 传播 / 参数个数）、`TestJSONExtract`（成员、嵌套、数组下标、引号成员、根路径、数字字面量保真、缺失路径 `NULL`、多路径数组）、`TestJSONUnquote`（去引号、非 JSON 文本原样、`NULL`）、`TestJSONType`（7 种类型）、`TestJSONLength`（对象/数组/空数组/标量/带路径/缺失路径）、`TestJSONObject`（类型映射、`NULL` 值、`NULL` 键报错、奇数实参报错）、`TestJSONArray`（混合类型、空数组、嵌套）、`TestJSONSet`（替换、新增键、数组替换/末尾追加/越界报错、`NULL` 值、根替换、多路径、标量中间层报错）、`TestJSONNestedCalls`（三层嵌套）、`TestJSONColumnRef`（列引用投影、`AS` 别名、`WHERE` 过滤）、`TestJSONWithAggregateAndCase`（`COUNT` + `JSON_VALID`、算术组合、`CASE` 分支）、`TestJSONUpdateSet`（`UPDATE SET` 内函数与列实参）、`TestJSONInvalidInput`（非法 JSON / 路径 / 非字符串路径）、`TestJSONSelectNoFrom`（无 `FROM` 常量投影）、`TestJSONFuncNotSupportedName`（未注册函数名报错口径不变）。
- **文档口径同步**：`KNOWN_LIMITATIONS` §1.3 存储过程/触发器受限子集边界更新（异常处理、动态 SQL、嵌套 `BEGIN…END` 块、`CONDITION` 命名条件、`GET DIAGNOSTICS`、HANDLER 复合块体、触发器体复合块、`SET NEW` 回写、`FOLLOWS`/`PRECEDES` 均已支持，剩余边界改为诊断区其余条件信息项、`DECLARE ... CONDITION FOR` 引用已声明条件、游标 `FOR UPDATE` 等）；新增 §1.5「事务保存点（`SAVEPOINT`）语义与 2PC 边界」，写明 MySQL 兼容语义、跨节点 2PC 所选「本地保存点生效 + 跨节点写集按写集前缀丢弃」策略及其理由（远端 `PREPARE` 延迟到 `COMMIT`，截断即等价丢弃），并列出会话内存态、崩溃恢复不按保存点粒度、不改变事务快照读视图等边界，§3.1 跨节点写章节同步指向；`README` 能力表存储过程行、触发器行与事务行同步（事务行新增保存点能力）；同日同步递归 CTE 口径——新增 §1.6「递归 CTE（`WITH RECURSIVE`）边界」，写明物化 worktable 迭代语义、`recursive_cte_max_depth` 默认 1000 与 `SET` 热切换、递归项受限位置与列数校验、`UNION` 去重成本与深递归/分片读的轮数量级边界，`README` 能力表 SQL 行补充递归 CTE 能力；同日同步 SQL JSON 函数族口径——新增 §1.7「SQL JSON 函数族边界」，写明支持的 8 个函数与路径子集、非法 JSON/非法路径一律报错而 `JSON_VALID` 返回 0/1 的口径、单遍解析与深拷贝写入的性能量级、以及大文档与高频路径解析的开销建议，`README` 能力表 SQL 行补充 JSON 函数能力。
- **可更新视图（A1）**：`CREATE VIEW` 时静态判定可更新性，并把「视图列名 → 基表列名」映射或不可更新原因随视图目录持久化；`INSERT` / `UPDATE` / `DELETE` 目标为可更新视图时按映射重写为对基表的写操作。判定条件为单表 `FROM`、无 `DISTINCT` / `GROUP BY` / `HAVING` / 聚合 / 窗口函数 / CTE / `LIMIT` / `OFFSET`、投影列全部为可逆裸列引用（允许 `t.col` 与 `AS` 别名，`SELECT *` 按基表列序恒等展开）。
- **视图元数据扩展**：`ViewMeta` 新增 `Updatable` / `NotUpdatableReason` / `BaseTable` / `ColumnMap` / `ColumnOrder`（均 `omitempty`）——旧视图目录（无新字段）反序列化后按不可更新处理，不改写既有目录结构、不影响旧视图读写。
- **写操作重写**：`INSERT` 列清单按映射落到基表列（省略列清单时按视图列顺序，视图未暴露的基表列走类型默认值）；`UPDATE` 的 `SET` 列必须落在视图列集合内并映射到基表列；`WHERE`（含列对列比较的右值列引用）与 `ON DUPLICATE KEY UPDATE` 中的视图列名（含算术语境内）一并重写为基表列名，视图未暴露的列统一报 `view <name> has no column: <col>`；写目标改为基表后权限校验按基表判定（重写发生在 `checkPrivilege` 之前）。
- **写重写下沉层**：可更新视图 DML 重写下沉到 `execStmt` 分发到 `execInsert` / `execUpdate` / `execDelete` 之前，存储过程体、动态 SQL（`EXECUTE`）与触发器体语句内的视图写与顶层一致；语句对象携带幂等标记，顶层 `ExecuteTextAsSession` 与下沉层的重复进入直接复用首次结论（不二次映射、不重复报错、不重复读视图目录），顶层「先重写、再权限校验」的既有顺序不变。
- **报错口径**：不可更新视图的写操作报 `view <name> is not updatable: <reason>`（`reason` 为建表期记录的具体条款，如 `aggregate function in definition`、`JOIN in definition`、`base object is a view: <v>`）；目标既非表也非视图仍保持既有 `table not exists: <name>`；视图定义尾部的 `WITH CHECK OPTION` 自 A2 起按语义实现（见下方 A2 条目）。
- **测试**：`pkg/sql/updatable_view_test.go`（17 例）——经视图 `INSERT`（含无列清单与未暴露列默认值）、`UPDATE`（含别名列与 `SET col = col + 1`）、`DELETE`（条件与全量）、`SELECT *` 视图、`INSERT ... SELECT` 与 `ON DUPLICATE KEY UPDATE` 重写、九类不可更新定义的原因口径、嵌套视图基表、视图不存在口径、`WITH CHECK OPTION` 拒绝、写权限按基表判定、显式事务内写入回滚、存储过程体内的 `INSERT` / `UPDATE` / `DELETE` 经视图落基表（含过程内写不可更新视图的口径）、`WHERE` 右侧列引用重写与未暴露列两侧同口径、重复执行与视图重建后的幂等无二次映射。
- **文档口径同步**：`KNOWN_LIMITATIONS` §1.8 可更新视图边界（判定条款、列映射随 `ALTER TABLE` 陈旧的后果、无 `CHECK OPTION` 时视图不可见行可被改写的影响与建议、每语句一次视图目录读取的开销），`README` 能力表 SQL 行补充可更新视图能力。
- **WITH CHECK OPTION（A2）**：`CREATE VIEW ... WITH [CASCADED|LOCAL] CHECK OPTION` 语法落地（`WITH` 后可省略写法，省略时按 `CASCADED` 记录；单独出现 `CHECK OPTION` 缺 `WITH` 仍按语法错误拒绝）。建视图时把定义 `WHERE` 规范化为「基表裸列名」条件副本并随视图目录持久化，`INSERT` / `UPDATE` 经视图写入时对新行求值，不满足报 `CHECK OPTION failed '<view>'` 并整条语句回滚；`DELETE` 不受限、不校验。求值对象为写入后最终行值——`INSERT` 含视图未暴露基表列的类型默认取值，`UPDATE` 为 `SET` 生效后的整行，`WHERE` 中 `t.col` 限定引用与 `AS` 别名引用在规范化阶段统一剥离为基表裸列名。
- **视图元数据扩展（A2）**：`ViewMeta` 新增 `CheckOption` / `CheckCond`（均 `omitempty`）——旧视图目录（无新字段）反序列化后按无 `CHECK OPTION` 处理，不改写既有字段与 JSON 结构。不可更新视图带 `CHECK OPTION` 在建表期即报 `view <name> is not updatable: <reason>`（不落库半成品视图）。**`LOCAL` 与 `CASCADED` 本版本语义等价**：可更新性判定要求基表为真实表（不支持视图套视图），不存在级联检查的下层视图，两者仅按写法原样记录。`ON DUPLICATE KEY UPDATE` 命中冲突后的更新分支与 `REPLACE` 的冲突替换分支不施加校验（仅普通插入与 `UPDATE` 产生的新行求解条件）。
- **校验挂载点**：`viewCheckPlan` 由 DML 重写阶段（`rewriteUpdatableViewDML`，含顶层与 `execStmt` 下沉层）挂到 `InsertStmt` / `UpdateStmt` 上，`execInsert` 在冲突判定后、写集收集处对普通插入行校验（`insertRowUpsertOpsHook` 的 `buildRow` 与 BEFORE 触发器回写之后，保证校验的是最终落盘值），`execUpdate` 在 `SET` 表达式应用后、外键校验前校验；两处均只产出待应用写集、语句末尾统一提交，故校验失败天然整条回滚。存储过程体、动态 SQL 与触发器体内的视图写经同一下沉层，约束与顶层一致。
- **测试**：`pkg/sql/check_option_test.go`（13 例）——`INSERT` 越界拒绝与拒绝后基表零残留、多行 `INSERT` 任一行越界整条回滚、未暴露列按默认值参与求值、`UPDATE` 改出边界拒绝与改回边界内放行、多行 `UPDATE` 整条回滚、`DELETE` 不受限、无 `CHECK OPTION` 视图不校验、`LOCAL` / `CASCADED` / 裸写法等价、限定列名与 `AS` 别名列引用、复合条件（`AND` / 比较组合）、无 `WHERE` 定义恒通过、不可更新视图带 `CHECK OPTION` 建表期拒绝、存储过程体内的写入约束。
- **文档口径同步（A2）**：`KNOWN_LIMITATIONS` §1.8 改写为 A2 语义（`CHECK OPTION` 支持范围、`LOCAL` / `CASCADED` 等价理由、整语句回滚粒度、`ON DUPLICATE KEY UPDATE` 更新分支不校验的差异、校验条件随 `ALTER TABLE` 陈旧的口径与建议），`README` 能力表 SQL 行同步。

**D7 发版收尾（2026-11-20 完成本地准备）**

- **版本常量单点化**：新增零依赖包 `pkg/version`（`Version` / `MySQLServerPrefix` / `ServerVersion`），`cmd/openxdb/main.go` 的版本常量、`pkg/mysqlproto` 的握手版本串与 `pkg/sql` 的 `select version()` 全部取自同一常量，消除 `main.go` 与 `handshake.go` 两处独立版本常量，发版只需改 `pkg/version.Version` 一处。
- **版本串漂移修正**：握手版本串与 `select version()` 此前各自硬编码且停留在 `v1.17.0`（落后版本线两轮），本次随单点化一并修正为 `8.0.36-OpenXDB v1.19.0`；相关测试改为对 `pkg/version` 常量的同源断言，避免下次发版再次漂移。
- **文档同步**：`RELEASE_NOTES.md` 整体改写为 v1.19.0 口径；`README.md` 版本口径升级 v1.19.0 并补版本状态条目；`KNOWN_LIMITATIONS.md` 版本口径升级 v1.19.0（§1.3 / §1.4 / §1.5 / §1.7 / §1.8 既有的 v1.19.0 能力条目保留）；`docs/index.md` 头部版本口径同步。
- **回归验证**：`go build ./...`、`go vet ./...`、`go test ./...` 全通过（33 个包：22 个 ok、11 个无测试文件，无 FAIL）。`gofmt` 仅在仓库工作区 CRLF 换行约定下会把全部 CRLF 文件列出，按 LF 归一化后本次改动文件无格式差异；`pkg/server/status.go` 存在一处历史遗留对齐差异，非本次引入，未纳入本轮改动。
- **tag 与远端核验**：annotated tag `v1.19.0` 与 gitee / origin 双远端推送及三方 SHA 比对由主链在本地改动落库后执行。

## v1.18.0 (2026-11-17)

v1.18.0（分布式纵深：跨节点数据分布 + HA quorum 灰度）按 DEV_PLAN_v1.18.0 推进并完成 D2–D7：D2/D3/D4/D5/D6 已收口，D7 发版收尾于 2026-11-17 完成本地准备（版本常量、文档同步、回归验证），2026-11-18 完成 annotated tag 创建与双远端核验。

**Added**

- **D2 会话锁重试收口**：v1.17.0 回退后在干净库重建复测，16 连接 read 17.12 TPS（≥ 14.22，+44.6%）、read_write 8 连接 13.58 TPS 无回归，errs=0。
- **D3 跨节点数据分布策略**：显式 REGION 指派（隐式 region 显式化 + ASSIGN REGION）三节点分布落地；`SHOW REGION ROUTES` 可见指派结果，跨节点点查可见性、COUNT/SUM 跨节点聚合不丢不重，非并发全链路验证通过；数据分布操作指南成文。
- **D4 HA quorum 灰度全链路演练**（D4.1–D4.5）：`ha.quorumEnabled=true` 三节点多数派评估、宕机 failover（读 failover 自动重指派 + 数据可读）、脑裂与 epoch fencing 拒绝旧主写、节点恢复回迁与路由收敛四项演练完成；灰度报告与 `HA_BRAIN_SPLIT_DESIGN` §5.5 结论更新（quorum 仍默认关闭，结论见报告）。
- **D6.1 块缓存调优（负收益，不采纳）**：块缓存调至 256MB 实测 -1.6%，已回退；确认 SQL 路径瓶颈为 cgo 往返（`runtime.cgocall` flat 63.75%）而非引擎缓存容量。
- **D6.2 批量点查合并**：`routedJoin`（routed JOIN 回表）与 `evalInSubqueryRouted`（IN 子查询）由逐行/逐值点查改为一次 `mgetKVCluster` 批量读，保持左行原序、LEFT JOIN 补空、未命中跳过与路由错误前置返回语义；N=64 基准 33.3ms → 0.52ms（点查往返 64 次 → 1 次），验证用例断言 `Get=0` / `MGet=1`。
- **D6.3 Snapshot 批量 Scan 评估**：核对确认快照号语句级/事务级复用、多区间单次合并扫描、版本可见性批量过滤均已具备，无新增代码；跨 region 远端区间聚合（多 `QUERY_REQ` 合并）转 v1.19.0 与写路径 2PC 一并处理。
- **D5 Linux E5 受控复测**：D5.1 在 RHEL 9.7 目标机（2 vCPU / Go 1.22.12 / RocksDB 11.8.1 CGO，源码 HEAD `dac6843`）以 `scripts/build.sh --rocks-src/--rocks-build` 构建并安装为 `/usr/local/bin/openxdb`，bench 依赖（仓库内 `drivers/python`）对齐，Linux 从零部署链路打通；D5.2 与 Windows 同口径采集（同脚本、8 连接、每轮 prepare 重置、3 轮取中位数）——OLTP read / write / read-write **8.17 / 8.13 / 8.10 TPS**（QPS 114.33 / 32.53 / 145.80），TPC-C 5 仓 tpmC **749.50** / tpm_total 1684.50，14 组采集全部 `errs=0` / `reconnects=0`。对比 Windows v1.17.0 基线（14.15 / 13.40 / 12.60 TPS，tpmC 774.50）：OLTP 达 57.7%–64.3%，TPC-C 达 96.8%；差异主因为 CPU 配额（2 vCPU vs 16 逻辑核），非代码路径——同机 threads=2 与 threads=8 的 read TPS 同为约 8.2 而 p50 延迟差约 4 倍（0.24 s → 0.95 s），表明 2 vCPU 下吞吐已饱和、8 连接属超配口径。同步确认"每轮 prepare 重置"口径的必要性（不重置连跑 TPC-C tpmC 衰减 565.00 → 230.50 → 220.00）及 `bench_tpcc.py` 主键起点限制（库内已有历史订单时重跑触发 duplicate primary key: history / order_line）。详见 `docs/experiments/D5_linux_e5_controlled_retest.md`。

**D7 发版收尾（2026-11-17 完成本地准备；2026-11-18 完成 tag 与双远端核验）**

- **版本口径统一**：`cmd/openxdb/main.go` 版本常量升级 `v1.18.0`（握手版本串 `8.0.36-OpenXDB v1.18.0` 与 `select version()` 由该常量同源注入，自动联动）；`cmd/oxsql` 交互提示符由 `oxsql>` 改为 `openxdb>`（头部注释与 REPL 注释同步，命令名仍为 `oxsql`）。
- **文档同步**：`RELEASE_NOTES.md` 整体改写为 v1.18.0 口径（覆盖 D2/D3/D4/D5/D6 + D7 收尾）；`README.md` 版本口径升级 v1.18.0 并补 v1.18.0 特性亮点行与版本状态条目；`KNOWN_LIMITATIONS.md` 版本口径升级 v1.18.0（§2.1 / §3.4 既有的 v1.18.0 结论保留）。
- **回归验证**：`go build ./...` 与 `go vet ./...` 全通过，无回归。
- **tag 与远端核验（2026-11-18 完成）**：annotated tag `v1.18.0` 打在 `302b8a2`（v1.18.0 收尾提交），gitee 与 origin（GitHub443）双远端推送成功；`git rev-parse refs/tags/v1.18.0^{}` 本地与 `git ls-remote` 双端 deref 结果均为 `302b8a2f013be4f61edfa75cca0221d76d4d72c4`，tag 对象 SHA 双端一致。tag 注释内已留痕 D4/D5/D6 变更摘要与三项真机验证欠账说明。
- **工作区清理（2026-11-18 完成）**：删除 `tests/bench/c5_sql_bench/result_*.md` 12 份基准散落结果、`tests/bench/tmp_c6_smoke_out.md`、两个空中间目录（`bench_data/d45`、`tmp_mini/c6_slowcheck`）与三个 bench 数据目录（`e2_data_1m`、`rocksdb/e2_fsync_run1m`、`rocksdb/e2_fsync_run2m_b1000`，各 68.2MB，共约 204.6MB，均由脚本可重建）。`DEV_PLAN_v1.17.0.md`、`DEV_PLAN_v1.18.0.md`、`docs/DEV_PLAN_2026Q6/Q7/Q8.md` 按项目约定保留为 untracked 排期文档。
- **口径裁定（A6）**：T3 / NEW-1 / NEW-2 真机复验按 `docs/D4_5_ha_quorum_gray_report.md` §8.2 执行——**v1.18.0 发版前不阻塞，v1.19.0 前必须完成**。打 `v1.18.0` tag 仅表示版本节点落定，**不改变**该三项欠账状态，也不构成 `ha_quorum` 可开启的依据；`ha_quorum` 维持默认关闭，三项真机验证完成前不得在文档中改写为 PASS。

## v1.17.0 (2026-10-30)

v1.17.0（显式事务写路径优化 + 可观测性/稳定性补强）正式发布。按 DEV_PLAN_v1.17.0 完成 D1 三项写路径改造（D2 会话锁调度精细化因并发伸缩验收未达标整体回退）、D3 可观测性与诊断闭环、D4 并发规模与稳定性验证，并经 D5 发版前 E5 受控复测，将 write 吞吐从"贴近目标线"拉到"稳定余量"（QPS +50%）；全仓回归全绿。

**Added**

- **D1.1 UPDATE 快照刷新锁外化**：显式事务 UPDATE 的锁内快照刷新移到锁外（对齐 C2 Begin 锁外化同型改造），降低写语句延迟；`snapshot_lock_external` 独立 flag，失败可回退锁内刷新。
- **D1.2 autocommit 写 pipeline 深化**：写语句接入连接内 pipeline（原 pipeline 仅覆盖读路径），减少端侧逐条往返；`write_pipeline` 独立 flag 默认 off，可热切换回退逐条执行。
- **D1.3 WAL 组提交主动定时窗口**：`group_commit_active_window`（默认 off 回退 v1.16.0 C1 单等待者免窗口）在写密集下主动定时刷新合并 fsync，降低 fsync 次数与写放大；不影响 WAL 重放 / 崩溃恢复 / 复制 2PC 顺序。
- **D3.1 EXPLAIN 节点级耗时/行数估算**：执行计划 planLines 追加 `rows=N est_ms=X.XX` 节点级耗时与行数估算，真实行数走存储层 CountRange 计数，输出向后兼容追加字段，旧解析器不受影响。
- **D3.2 slowlog 阈值区间 TOP N 汇总**：`SHOW SLOWQUERIES SUMMARY` 按阈值区间汇总 TOP N（降序稳定排序 + bucket 区间聚合），配合 `SET slow_query_threshold` 热配置形成诊断闭环。
- **D4.1 并发规模长稳验证**：8 连接写密集 30min 长稳——errs=0、goroutines 恒 2、heap 42MB、wal.log 受控（45.6MB）、崩溃恢复 check COUNT 严格等价（2355864）。
- **D4.2 bench 工具参数化**：`bench_oltp.py --rows` CLI 化、`run_e5.ps1 -OltpRows`、`load.go` 追加 goroutine/heap 观测，便于复测与 CI。
- **E5 受控复测（发版最终）**（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read / write / read-write **14.15 / 13.40 / 12.60 TPS**（QPS 198.10 / 53.60 / 226.80，errs=0 / reconnects=0），较 v1.16.0（10.37 / 8.33 / 8.15 TPS，QPS 145.13 / 33.33 / 146.70）**write QPS +61%**、read-write TPS +55%、read 无回归 +36%；TPC-C 5 仓 tpmC **774.50** / tpm_total 1746.50（较 v1.16.0 的 254.00 / 581.00 提升约 **3.0 倍**）。**达成 v1.17.0 验收目标：OLTP TPS read ≥11（14.15）/ write ≥9（13.40）/ rw ≥8.5（12.60），QPS read ≥145（198.10）/ write ≥40（53.60）/ rw ≥146（226.80），TPC-C tpmC ≥300（774.50），errs=0 / reconnects=0**。详见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。

- **D6.1 replica SQL 常驻监听**：`openxdb replica --mysql-port <port>` 支持 MySQL wire 常驻读监听（复用主端装配路径，支持 `--auth` / `--tls-cert` / `--tls-key`），mysql CLI / oxsql 可直连备库常驻查询。
- **D6.2 replica 只读强制**：备库写语句按 `isWriteStmt` 白名单统一拒绝（错误 `read-only replica: write statement rejected`），仅 replica 模式开放，主端读写零回归。
- **D6.3 复制应用与 SQL 读并发**：以 `AppliedLSN` 为界，读快照创建前经 `ReadBarrier` 短临界区对齐复制应用批次；存储原子批次 + 快照隔离保证无半批次可见，读不阻塞复制应用、无死锁；未采用「读/复制全互斥」回退方案（并发方案经 `-race` 与专项回归验证通过）。
- **D6.4 专项验证与文档**：6 项验收全通过——备库常驻查询、写语句拒绝且数据不受污染、滞后窗口读断言（30 轮 lagOk/worstGap=0）、崩溃恢复二次重启追平且顺序一致、`go build ./...` 与 `go test ./...` 全绿、主端读写无回归；验证脚本与说明见 [docs/experiments/D6_replica_resident_read.md](docs/experiments/D6_replica_resident_read.md) 与 [docs/experiments/run_d6_replica_validate.ps1](docs/experiments/run_d6_replica_validate.ps1)。
- **D7.1 MySQL 握手版本串**：`serverVersion` 由硬编码 `8.0.36-OpenXDB` 改为与 v1.17.0 版本常量同源注入，mysql CLI 登录握手显示 `8.0.36-OpenXDB v1.17.0`（前缀保持 mysql CLI 版本格式识别兼容）。
- **D7.2 version() 函数**：SQL 函数注册表登记 `version`，`select version();` 返回 `8.0.36-OpenXDB v1.17.0`（与握手版本同源）。
- **D7.3 SHOW INDEXES 复数兼容**：`parseShow` 同时接受 `INDEX` / `INDEXES`，复用单数实现，输出等价；验收修复大小写敏感（`show indexes` 小写未识别），`parseShow` switch 归一化为 `strings.ToUpper` 并新增大小写等价单测。
- **D7.4 SHOW CREATE TABLE**：`parseShow` 新增 `CREATE` 分支 + `ShowCreateStmt`，executor 基于表存储元数据重建 CREATE TABLE（列定义/类型/主键/二级索引/约束），供 mysqldump 与第三方工具使用。
- **D7.5 oxsql> 客户端提示符**：自研交互 CLI `cmd/oxsql` 提示符落地为 `oxsql>`（仓库无 `mysql>` 实现，`mysql>` 为第三方 mysql CLI 客户端侧固定）。
- **D7.6 专项验证与文档**：5 项验收全通过（G1 握手版本串、G3 `version()`、G4 `SHOW CREATE TABLE` 可重建 schema、G5 `SHOW INDEXES` 与 `SHOW INDEX` 四种大小写组合等价、G2 `oxsql>` 提示符生效），全仓 `go test ./...` 全绿；验证脚本与说明见 [docs/experiments/D7_mysql_compat_validate.md](docs/experiments/D7_mysql_compat_validate.md) 与 [docs/experiments/run_d7_mysql_compat_validate.ps1](docs/experiments/run_d7_mysql_compat_validate.ps1)。

**回退记录（D2）**

- **D2 会话锁与调度精细化已回退**：D2.1 session lock granularity split（d7660f5）与 D2.2 read-parallel in write txn（442c4cb）因并发伸缩验收未达标（D2.3 复测 16 连接 read TPS 10.80 较 8 连接 11.85 为 -8.9%、read-write 10.28 较基线 12.40 为 -17.1%，未达"16 连接较 8 连接提升 ≥20%"验收目标）而整体回退，恢复 D1 基线行为（等价 cdd62e7）；D2.3 并发伸缩 bench 数据与实验文档（e1ba367）保留作为记录。回退后全仓回归全绿（`go test ./... -count=1 -p 1`），E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read / write / read-write **12.75 / 12.63 / 10.97 TPS**（QPS 178.50 / 50.53 / 197.40，errs=0 / reconnects=0），TPC-C 5 仓 tpmC **709.00** / tpm_total 1568.00。**D1 指标复测仍达标：write QPS 中位 ≥40（50.53）、TPS write ≥9（12.63）、tpmC ≥300（709.00），read / read-write 无回归，errs=0 / reconnects=0**。

**边界声明**

- TPS/QPS 受全局会话锁与单机端侧往返制约，数值为**单机端侧功能基准**，非生产级吞吐承诺；write 模式已脱离吞吐下限（QPS 中位 53.60，三轮最低 52.87 仍高于 v1.16.0 中位 33.33）。
- 所有优化均有独立 feature flag 可灰度回退；与 v1.16.0 兼容、可回退。

## v1.16.0 (2026-10-28)

v1.16.0（SQL 执行内核并发模型优化 + WAL 持久化增强）正式发布。按 DEV_PLAN_2026Q8 完成 C1-C7 六项内核并发优化与 E5 受控复测，并落地 WAL 自动 checkpoint / truncate 持久化收敛；全仓回归全绿。

**Added**

- **C1 WAL 组提交增强**：单等待者免 0.5ms 窗口 + 批量刷盘，写事务 COMMIT 延迟显著下降；`group_commit_window_ms` 可回退。
- **C2 Begin 快照创建锁外化**：快照创建移至锁外，ID / 序号仍单调；点查 / autocommit 密集场景 Begin 锁竞争消除。
- **C3 ro_parallel 下沉 executor + 显式事务只读并行**：只读并行覆盖 REPL 批与 autocommit 直调场景，显式事务内纯只读语句并行执行（MVCC 快照语义）；`ro_parallel` 默认 on 可回退。
- **C4 提交应用流水线化 / 组提交**：阶段 3 按 seq 排队改写为注册聚合，WAL 重放结果严格等价、复制 / 2PC 顺序不变。
- **C5 cgo 扫描拷贝优化**：arena 单块 xscan + 两趟 do_scan + 零拷贝 xscan_item（unsafe 边界专项测试）；全表 / 范围读加速。
- **C6 连接内语句 pipeline / 快照复用**：autocommit 顶层纯只读 SELECT 共享快照 + 连接内 pipeline flush；`snapshot_share` / `pipeline_flush` 独立 flag 默认开。
- **WAL 自动 checkpoint / truncate**：新增安全位点模型（safe = min(LastSyncedLSN, AppliedLSN)，截断前 RocksDB Flush 落 SST），后台周期检查自动截断，长期运行 wal.log 大小受控；配置 `wal_auto_checkpoint`（默认 on）/ `wal_checkpoint_interval_ms`（60000）/ `wal_checkpoint_max_mb`（64）；新增 LastSyncedLSN / AppliedLSN 位点基础设施与 storage.Flusher 接口。
- **C7 E5 受控复测**（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read / write / read-write **10.37 / 8.33 / 8.15 TPS**（QPS 145.13 / 33.33 / 146.70，errs=0 / reconnects=0），较 v1.15.0（0.38 / 0.40 / 0.40 TPS）TPS 提升约 **20-27 倍**、QPS 提升约 **22-27 倍**，达成验收目标（TPS≥1.5、QPS≥30）；TPC-C 5 仓 tpmC **254.00** / tpm_total 581.00（较 v1.15.0 81.50 / 170.50 提升约 3.1-3.4 倍）。详见 [docs/experiments/C7_e5_controlled_retest.md](docs/experiments/C7_e5_controlled_retest.md)。

**边界声明**

- TPS/QPS 受全局会话锁与单机端侧往返制约，数值为**单机端侧功能基准**，非生产级吞吐承诺；write 模式为吞吐下限（QPS 中位 33.33）。
- 所有优化均有独立 feature flag 可灰度回退；与 C1-C6 兼容、可回退。

## v1.15.0 (2026-10-27)

v1.15.0（SQL 执行层单语句路径优化）落地四项优化：W1 Scan 快路径、W2 COUNT 快速路径、W3 列投影下推、W4 ORDER BY 索引顺序扫描免排序 + DISTINCT 相邻去重免排序。

**Added**

- **W1 Scan 快路径**：txn 无未提交写时快照直透，跳过全量物化 + 重建排序。
- **W2 COUNT(*)/COUNT(1) 快速路径**：无 WHERE/GROUP 等时走存储层 RangeCounter 逐 region 计数求和。
- **W3 SELECT 列投影下推**：按需解码列，JOIN / 子查询 / 星号保守全列回退。
- **W4 ORDER BY 索引顺序扫描免排序 + DISTINCT 相邻去重免排序**：单列二级索引 / 主键 + 运行时 O(n) 已序验证。
- **E5 受控复测**（3 轮取中位数，8 连接）：OLTP read 0.38 / write 0.40 / read-write 0.40 TPS（与 v1.14.0 持平、无回归）；TPC-C tpmC 81.50。单语句专项：COUNT(*) 全表 580ms → **5.18ms**（约 103-112 倍）、全表扫描多列 613.51ms（约 1.27-1.47 倍）、全表扫描单列 532.33ms（约 1.47-1.69 倍）、ORDER BY 主键 / DISTINCT 免排序生效（详见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)）。

## v1.6.0 ~ v1.14.0（里程碑区间摘要）

介于 v1.5.0 与 v1.15.0 之间的正式版本按里程碑区间归纳（细节见 README 版本状态节）：

- **v1.6.0 阶段一/二（2026-10-24）**：ALTER TABLE 列级操作补全（MODIFY COLUMN）、CREATE DATABASE / 库级管理（`db.tbl` 前缀）、多列窗口扩展、binlog / mysqldump 全选项、存储过程受限子集补齐（DECLARE/游标/OUT 参数）、HA 回迁自动化。
- **v1.7.0（2026-10-24）**：HA 全量对齐兜底联动；空表 AVG 语义转正。
- **v1.8.0（2026-10-24）**：触发器行级语义（FOR EACH ROW、NEW/OLD、体回滚与递归策略）。
- **v1.9.0（2026-10-24）**：物化视图增量刷新（CONCURRENTLY 快照交换、依赖链管理）。
- **v1.10.0（2026-10-25）**：HA 脑裂防护生产化（ha.epochEnabled 默认开启）。
- **v1.11.0（2026-10-25）**：商业生态增强与文档对齐（Parquet 多 RowGroup、告警 webhook、TLS 证书热轮换）。
- **v1.12.0（2026-10-25）**：性能与精度优化（bufio 读覆层 + COM_QUERY 多语句、DECIMAL 按列 scale、IMPORT BATCH 强化、WAL 组提交默认开启，E5 tpmC 13.5→41.0）。
- **v1.13.0（2026-10-25）**：服务端多连接并发处理 + E5 pipeline 化，TPC-C tpmC 41.00→78.00。
- **v1.14.0（2026-10-25）**：只读 SELECT 并行（ro_parallel）+ WAL 组提交定时窗口，E5 tpmC 78.00→85.50。

## v1.5.0-dev (2026-10-23)

v1.5.0（协议与吞吐）第 1 项「客户端 pipeline 协议」先行启动落地。服务端 `REPLSession` 本就逐行按序处理，改造集中在驱动层。

**Added**

- **Python 驱动流水线批量执行**：`OpenXDBProtocol.pipeline()` 一次性连续发送多条命令并按序批量读取响应；`Cursor.pipeline()` / `Connection.pipeline()` 暴露 `PipelineResult`（`ok` / `affected` / `table` / `error` 四类结果）；复用既有单条响应边界解析，与 `command()` 行为完全一致。
- **JDBC 驱动批量执行**：`OpenXDBStatement.addBatch()` / `executeBatch()` / `executeLargeBatch()` 由 `SQLFeatureNotSupportedException` 升级为真实流水线实现（底层 `OpenXDBProtocol.pipeline` 一次写出多行并按序读回），失败语句抛 `BatchUpdateException` 且保留已成功语句更新计数。
- **服务端回归测试**：`pkg/server/server_pipeline_test.go` 新增 `TestREPLSessionConsecutiveLinesInOrder`，覆盖连续多行混合语句（DDL/DML/查询/非法语句交错）按序执行、错误不中断、表格回复边界。
- **吞吐基准**：Python 侧 `tests/test_pipeline.py` 与 JDBC 侧 `OpenXDBPipelineTest.java` 内置批量 vs 逐条对比基准（本地实测约 1.2–1.3 倍提升）。

## v1.0.0 (2026-10-16)

正式发布版，自研单机/分布式关系型数据库内核（Go + RocksDB）功能闭环收尾。

**Added**

- **中英文产品手册**：新增 [docs/product_manual_zh.md](docs/product_manual_zh.md)（中文版）与 [docs/product_manual_en.md](docs/product_manual_en.md)（英文版），11 章节对应结构，覆盖架构、SQL、复制/分片/集群、运维、性能基准、已知限制与 FAQ。
- **发布收尾文档**：新增 CHANGELOG.md、[RELEASE_NOTES.md](RELEASE_NOTES.md)、[KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md)，README 版本号定稿 v1.0.0 并补充文档导航。
- **docs 站点骨架**：新增 [mkdocs.yml](mkdocs.yml) 与 [docs/index.md](docs/index.md)，提供 MkDocs 文档站点入口。
- **版本定稿**：`cmd/openxdb` 发行版本常量更新为 v1.0.0，与 README Status 行保持一致。

**Milestones covered（随历史版本交付，本版本汇总确认）**

- M1–M21、T1–T42、E1–E5 全部里程碑：存储/WAL/事务/SQL 子集/二级索引、复制、分片、多节点集群、2PC 分布式写、HA、自动分裂与负载均衡、分布式事务（TSO + 快照隔离）、备份恢复与 PITR、客户端驱动（JDBC/Python）、运维工具链、SQL 解析修复、M9–M16 功能/兼容/可观测/生态/运维簇、M17 审计与 RBAC、M18 MySQL 兼容（prepared statement + information_schema）、M19 生态导出（JSON/Parquet）、M20 分布式纵深（同步/半同步复制、跨节点 DDL、分布式备份一致性）、M21 商业加固（TLS、监控告警、真实用户验证）、E1–E5 基准与稳定性实验。

## v0.30.0-alpha

- **T42 INSERT INTO SELECT**：`INSERT [INTO] table [(cols)] SELECT` / `REPLACE INTO ... SELECT`，列清单可选、全 SELECT 源兼容（表达式/别名/聚合/DISTINCT/WHERE/GROUP BY/ORDER BY/LIMIT/OFFSET）、原子事务与回滚、自插入快照语义。见 [docs/T42_insert_select.md](docs/T42_insert_select.md)。
- **中英文产品手册**：docs/product_manual_zh.md 与 docs/product_manual_en.md（本版本内新增）。

## v0.29.0-alpha

- **T41 DDL 兼容簇**：CREATE 系列 IF NOT EXISTS（TABLE/VIEW/MATERIALIZED VIEW/INDEX/USER/ROLE/TRIGGER/PROCEDURE 幂等跳过）+ CREATE TABLE AS SELECT（表达式/聚合/DISTINCT/别名、结果驱动列类型推断、原子创建+插入+回滚）。见 [docs/T41_ddl_compat_cluster.md](docs/T41_ddl_compat_cluster.md)。

## v0.28.0-alpha

- **T40 SQL 能力簇**：MAX/MIN 聚合（全局/GROUP BY/window、空表返回 0）、算术表达式（+ - * / %、优先级、一元负号、除零报错）、SELECT DISTINCT（单/多列、与 WHERE/ORDER BY/LIMIT/聚合组合）。见 [docs/T40_sql_capability_cluster.md](docs/T40_sql_capability_cluster.md)。

## v0.27.0-alpha

- **T38 真实用户验证**：端到端商业场景验证（订单/库存/用户中心业务 schema、事务 ACID、查询分析、运维生态脚本、Python 驱动 30+ 场景全绿）。见 [docs/T38_commercial_real_user_validation.md](docs/T38_commercial_real_user_validation.md)。
- **T39 SQL 能力差距评估**：SQL 能力差距盘点与排期。见 [docs/T39_sql_capability_gap_assessment.md](docs/T39_sql_capability_gap_assessment.md)。
- **E5 标准基准对齐**：标准 sysbench/TPC-C 因非 wire 协议不可直接运行，改用 Python PEP 249 驱动等价实测：OLTP read 1.73 TPS / write 0.93 / read-write 1.32（8 线程 × 60s），TPC-C 5 仓 tpmC 13.50 / TPM 32.50（8 线程 × 120s），0 错误。见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。

## v0.26.0-alpha

- **T37 监控与告警**：告警规则模型（metric/op/threshold/duration_sec/severity，JSON 持久化）+ 评估引擎（NORMAL/ALERT/RESOLVED 状态机 + 持续时长去抖）+ logx 告警通道（环缓冲/文件/webhook）+ SHOW ALERT RULES / STATUS / ALERTS。见 [docs/T37_commercial_alerting.md](docs/T37_commercial_alerting.md)。

## v0.25.0-alpha

- **T36 TLS 合规**：连接层 TLS 加密与证书管理（默认仅 TLS 监听、`--tls-allow-legacy` 明文/TLS 共存、会话级 secure 标记、verify-full/verify-ca 客户端配置、`openxdb gen-cert` 自签名证书生成器、JDBC/Python 驱动 TLS 参数）。见 [docs/T36_commercial_tls.md](docs/T36_commercial_tls.md)。

## v0.24.0-alpha

- **T35 分布式备份一致性**：协调者四阶段多节点全局一致备份（PREPARE 收集 LastLSN/TSO → GlobalLSN=max(all LSN) → DO 以 global_lsn 对齐快照 → manifest 一致性标记）+ 分布式 RESTORE 恢复到全局时间点，产物格式向后兼容。见 [docs/T35_m20_dist_backup_consistency.md](docs/T35_m20_dist_backup_consistency.md)。

## v0.23.0-alpha

- **T34 跨节点 DDL**：DDL 元数据经 DDL_PUSH 帧广播到全部在线节点、按 TableID 幂等收敛、离线/失败记 pending 并在下次广播前或 ReplayPendingDDL 重放，单机零触发兼容。见 [docs/T34_m20_cross_node_ddl.md](docs/T34_m20_cross_node_ddl.md)。

## v0.22.0-alpha

- **T33 同步/半同步复制**：异步（M2）默认兼容 + sync/semi-sync 两种增强模式（WaitAck 在事务锁外确认、ACK 超时/无从降级异步、SET replication_mode/ack/timeout 热切换）。见 [docs/T33_m20_sync_semi_sync_replication.md](docs/T33_m20_sync_semi_sync_replication.md)。

## v0.21.0-alpha

- **M18 MySQL 兼容簇**：prepared statement（PREPARE/EXECUTE/DEALLOCATE + `?` 占位符 + `@var` 用户变量、会话级缓存）+ information_schema 只读虚拟视图（schemata/tables/columns/statistics）。见 [docs/T31_m18_mysql_compat.md](docs/T31_m18_mysql_compat.md)。
- **M19 生态导出**：EXPORT TABLE FORMAT JSON（JSON Lines）+ FORMAT PARQUET（自包含极简 Parquet v1 writer，Thrift Compact 编码、单 row group、PLAIN/UNCOMPRESSED）。见 [docs/T32_m19_ecosystem_export.md](docs/T32_m19_ecosystem_export.md)。

## v0.20.0-alpha

- **T29 M16 运维簇**：热配置重载（SET PERSIST/SHOW CONFIG、persisted.json 原子写、重启生效）+ 优雅关闭（SHUTDOWN [GRACEFUL]、drain 活跃事务）+ 升级辅助（openxdb preflight 检查、rolling_upgrade.ps1）。见 [docs/T29_m16_hot_config.md](docs/T29_m16_hot_config.md)、[docs/T29_m16_graceful_shutdown.md](docs/T29_m16_graceful_shutdown.md)、[docs/T29_m16_upgrade.md](docs/T29_m16_upgrade.md)。
- **T30 配置与告警日志链路**：统一配置文件与结构化日志告警。见 [docs/T30_config_log_alert.md](docs/T30_config_log_alert.md)。

## v0.19.0-alpha

- **T28 M15 SQL 功能簇（完成）**：物化视图（shadow-table 物化 + REFRESH [CONCURRENTLY] + DROP ... RESTRICT）+ 触发器 + 存储过程。见 [docs/T28_m15_matview.md](docs/T28_m15_matview.md)、[docs/T28_m15_trigger.md](docs/T28_m15_trigger.md)、[docs/T28_m15_procedure.md](docs/T28_m15_procedure.md)。

## v0.18.0-alpha

- **T27 M14 生态接入簇（推进）**：mysqldump 兼容导入（IMPORT MYSQLDUMP）+ binlog 订阅 CDC（pkg/cdc Subscriber pull-based）+ 工具链闭环（openxdb import CLI、examples/binlog_subscriber）。见 [docs/T27_m14_mysqldump_import.md](docs/T27_m14_mysqldump_import.md)、[docs/T27_m14_binlog_sub.md](docs/T27_m14_binlog_sub.md)、[docs/T27_m14_toolchain.md](docs/T27_m14_toolchain.md)。
- **T28 M15 SQL 功能簇（推进）**：物化视图/触发器/存储过程开始落地。

## v0.16.0-alpha

- **T26 M13 可观测簇（推进）**：慢查询日志（SHOW SLOWQUERIES / SET slow_query_threshold，环缓冲 + 持久化）。见 [docs/T26_m13_slowlog.md](docs/T26_m13_slowlog.md)。

## v0.14.0-alpha

- **T23 M10 SQL 兼容簇（推进）**：CTE（WITH 链/环保护）+ upsert（ON DUPLICATE KEY UPDATE + REPLACE INTO）。见 [docs/T23_m10_cte.md](docs/T23_m10_cte.md)、[docs/T23_m10_upsert.md](docs/T23_m10_upsert.md)。
- **T24 M11 认证与权限簇**：用户管理（SHA-256+salt）+ GRANT/REVOKE 权限模型 + 连接认证与权限校验。见 [docs/T24_m11_user.md](docs/T24_m11_user.md)、[docs/T24_m11_grant.md](docs/T24_m11_grant.md)、[docs/T24_m11_auth.md](docs/T24_m11_auth.md)。
- **T25 M12 分布式 SQL 簇**：分布式 join + 跨节点 ORDER BY/LIMIT + 分布式子查询。见 [docs/T25_m12_join.md](docs/T25_m12_join.md)、[docs/T25_m12_order_limit.md](docs/T25_m12_order_limit.md)、[docs/T25_m12_subquery.md](docs/T25_m12_subquery.md)。
- **T26 M13 可观测簇（推进）**：Prometheus 指标导出 + 状态视图。见 [docs/T26_m13_metrics.md](docs/T26_m13_metrics.md)、[docs/T26_m13_status.md](docs/T26_m13_status.md)。
- **T27 M14 生态接入簇（推进）**：binlog 订阅 CDC 方向。见 [docs/T27_m14_binlog_sub.md](docs/T27_m14_binlog_sub.md)。

## v0.13.0-alpha

- **T22 M9 SQL 功能簇**：在线 DDL（ALTER TABLE）+ 查询缓存 + 批量导入 + 外键（ON DELETE CASCADE|RESTRICT）+ 视图（CREATE/DROP VIEW、定义展开、环保护）。见 [docs/T22_m9_online_ddl.md](docs/T22_m9_online_ddl.md)、[docs/T22_m9_query_cache.md](docs/T22_m9_query_cache.md)、[docs/T22_m9_bulk_import.md](docs/T22_m9_bulk_import.md)、[docs/T22_m9_foreign_key.md](docs/T22_m9_foreign_key.md)、[docs/T22_m9_view.md](docs/T22_m9_view.md)。
- **T23 M10 SQL 兼容簇（推进）**：窗口函数（ROW_NUMBER/RANK/DENSE_RANK + SUM/AVG/COUNT，PARTITION BY/ORDER BY）。见 [docs/T23_m10_window.md](docs/T23_m10_window.md)。

## v0.12.0-alpha

- **T21 SQL 解析修复**：DROP TABLE IF EXISTS（幂等删除）+ 内联列级 PRIMARY KEY（`id INT PRIMARY KEY`）。见 [docs/T21_sql_parser_fixes.md](docs/T21_sql_parser_fixes.md)。

## v0.11.0-alpha

- **T20 运维工具链**：`openxdb doctor` 健康巡检（数据目录/WAL/RocksDB/元数据/日志 LSN/端口/慢查询，退出码 0/1/2）+ `openxdb stats` 实时指标（连接/表/region/慢查询/日志 LSN）+ 一键部署脚本（deploy.ps1 / deploy.sh）。见 [docs/T20_ops_toolchain.md](docs/T20_ops_toolchain.md)。

## v0.9.0-alpha ~ v0.10.0-alpha（里程碑区间，版本未逐版留档）

- **T18 备份与恢复**：一致性快照逻辑备份（单文件、version + CRC32）+ 幂等恢复（drop-then-recreate）+ LSN 基 PITR（RESTORE ... TO LSN）。见 [docs/T18_backup_restore.md](docs/T18_backup_restore.md)。
- **T19 客户端驱动**：JDBC 驱动（jdbc:openxdb://、META-INF/services 自动注册）+ Python 驱动（PEP 249 connection/cursor/protocol、qmark paramstyle），包装 pkg/server TCP 协议。见 [docs/T19_client_drivers.md](docs/T19_client_drivers.md)。

## v0.8.0-alpha

- **T17 M8 分布式事务增强**：全局 TSO（64 位 ms+logical 布局、批量发放、事务边界 Reset）+ 分布式快照隔离（版本记录 m:ver + begin_ts/commit_ts 全局过滤）+ 2PC 恢复加固（CoordRecord 持久化、重启后 commit/abort 决策）。见 [docs/T17_m8_dist_txn.md](docs/T17_m8_dist_txn.md)。

## v0.7.0-alpha

- **T16 M7 region 自动分裂与负载均衡**：写路径自动分裂（DML 计数水印 → 行数检查 → 数据中点分裂）+ BALANCE/周期触发负载均衡（最空闲节点、热 region 在线迁移）。见 [docs/T16_m7_split_balance.md](docs/T16_m7_split_balance.md)。

## v0.6.0-alpha

- **T15 M6 高可用与故障转移**：心跳超时节点下线检测 + OnDown 回调、region 自动重分配（副本扫描 + REGION_PUSH）、跨节点读故障转移、复制链路主从自动提升。见 [docs/T15_m6_ha.md](docs/T15_m6_ha.md)。

## v0.2.0-alpha ~ v0.5.0-alpha（里程碑区间，版本未逐版留档）

- **B2 写路径组提交**：批提交/批量 fsync（group commit / batch fsync）。见 [docs/B2_group_commit.md](docs/B2_group_commit.md)。
- **P0 SQL 增强**：JOIN / GROUP BY / FROM+IN 子查询 / BETWEEN / 会话事务（BEGIN/COMMIT/ROLLBACK）。见 [docs/T8_p0_sql_enhancements.md](docs/T8_p0_sql_enhancements.md)。
- **P1 CSV 类型**：CSV 导入导出 + DATE/DECIMAL/BLOB 类型。见 [docs/T9_p1_csv_types.md](docs/T9_p1_csv_types.md)。
- **P2 运维语句与表达式**：SHOW TABLES / SHOW INDEX / EXPLAIN / 慢查询 + LIKE / CASE WHEN。见 [docs/T10_p2_ops_expr.md](docs/T10_p2_ops_expr.md)。
- **M2 复制**：binlog + 复制器 + 从节点 + 心跳（异步主从）。见 [docs/T11_m2_replication.md](docs/T11_m2_replication.md)。
- **M3 分片**：RegionInfo 元数据 + Router + region 前缀物理键 + 跨 region 查询。见 [docs/T12_m3_sharding.md](docs/T12_m3_sharding.md)。
- **M4 多节点集群**：节点拓扑 + 握手/存活 + 集群路由表 + 跨节点 SELECT 转发/合并。见 [docs/T13_m4_multinode.md](docs/T13_m4_multinode.md)。
- **M5 分布式写路径 / 2PC**：跨节点事务两阶段提交（协调者/参与者、prepare 持久化、幂等 commit/abort、崩溃恢复、单机零触发）。见 [docs/T14_m5_2pc.md](docs/T14_m5_2pc.md)。
- **E1–E4 基准与稳定性**：RocksDB 写基准（E1a）/B+Tree 对比（E1b）、fsync 同步写基准（E2，batch=1000 时 sync 747k TPS seq / 198k rand）、SQL OLTP 端到端基准（E3，cgo MultiGet 修复后 256.3 TPS 8:2 / 605.4 TPS 5:5）、长时间稳定性与持久性（E4，6 分钟混合负载 307-385 TPS 无退化、重启 COUNT(*) 一致）。见 [docs/experiments/](docs/experiments/)。

## v0.1.0-alpha

- **M1 基础内核**：RocksDB cgo 桥存储层（+ 纯 Go 内存 B+Tree 参考实现）、追加式 WAL 与重放恢复、快照隔离本地事务、SQL 子集与二级索引、CLI/REPL 与 TCP 服务。见 [docs/T1_rocksdb_storage_impl.md](docs/T1_rocksdb_storage_impl.md)、[docs/T2_wal_impl.md](docs/T2_wal_impl.md)、[docs/T3_sql_layer.md](docs/T3_sql_layer.md)、[docs/T3_txn_impl.md](docs/T3_txn_impl.md)、[docs/T5_cli_impl.md](docs/T5_cli_impl.md)、[docs/T6_index_layer.md](docs/T6_index_layer.md)。
- **设计决策与早期文档**：ACID 笔记（[docs/T4_acid_notes.md](docs/T4_acid_notes.md)）、M2/M3 接口预留（[docs/T7_m2m3_reservations.md](docs/T7_m2m3_reservations.md)）、引擎选型 ADR（[docs/W4_adr_engine_selection.md](docs/W4_adr_engine_selection.md)）、博客 [docs/blog_why_openxdb_rocksdb.md](docs/blog_why_openxdb_rocksdb.md)。
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

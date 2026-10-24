---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_ecb33a0fcebd11f1b37d525400248c00
    ReservedCode1: 5z9qmx4MEZ1ddSPDMwi14UdkWCEl+G0JMaAQ/9qXlQkKXdzi6sZBySxBiwpT5L2mivOLadw8eypJZXgjWM+kQdHRwG8GiUwH/JIRHeUGl47fd0mWODA7at+zmcxPweqi4dAl9J+uJkXciDqGvnN+fM0RbPMNmL9Hmh8pRFHfa2V0XQlxt5RlwsdWEd0=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_ecb33a0fcebd11f1b37d525400248c00
    ReservedCode2: 5z9qmx4MEZ1ddSPDMwi14UdkWCEl+G0JMaAQ/9qXlQkKXdzi6sZBySxBiwpT5L2mivOLadw8eypJZXgjWM+kQdHRwG8GiUwH/JIRHeUGl47fd0mWODA7at+zmcxPweqi4dAl9J+uJkXciDqGvnN+fM0RbPMNmL9Hmh8pRFHfa2V0XQlxt5RlwsdWEd0=
---

---
# OpenXDB 阶段计划 v1.4.0 - v1.6.0
> 文档。版本号：DEV_PLAN_2026Q5
> 计划周期：2026-10-23 ~ 2026-12-31（约 10 周）
> 适用版本：v1.4.0 / v1.5.0 / v1.6.0

## 1. 文档说明与版本更新概览
Q4（v1.1.0-v1.3.0）排期与 HA 脑裂防护 P0/P1/P2 已全部落地并推送双端（gitee + origin GitHub）。v1.4.0 工程债四项已于 2026-10-23 全部完成并推送双端（最新 commit 397421c）。本计划将剩余需求落定正式排期：工程债清理、性能验证遗留、KNOWN_LIMITATIONS 能力补齐与 HA 后续规划评估。

**版本更新原则**
- 每个版本一个主题，禁止多主题并行讨论；
- 工程债/稳定性优先，协议与吞吐次之，SQL/兼容能力补齐最后；
- 严格基于排期文档推进，排期外既有基线失败不越界修改。

## 2. 排期汇总
| 版本 | 主题 | 周期 | 说明 |
|---|---|---|---|
| v1.4.0 | 工程债与稳定性 | 3 周（10.23 - 11.12）| waitEverUp 修复提交、fixregions 补测试、D 混合事务复测 |
| v1.5.0 | 协议与吞吐 | 3 周（11.13 - 12.03）| 客户端 pipeline、行协议边界修正 |
| v1.6.0 | SQL/兼容补齐 | 4 周（12.04 - 12.31）| SQL 子集、MySQL wire 扩展、存储过程受限子集 |

## 3. 详细排期

### 3.1 v1.4.0（工程债与稳定性，2026-10-23 ~ 2026-11-12）
| # | 需求项 | 优先等级 | 预估周期 | 依据与说明 |
|---|---|---|---|---|
| 1 | waitEverUp 修复提交 | P1 | 0.5 周 | pkg/cluster/cluster_test.go 与 pkg/db/db_m7_test.go 工作区改动已就绪，补跑 TestHeartbeatDown / TestM7 回归后提交 |
| 2 | cmd/fixregions 补测试 | P1 | 1 周 | 修复工具当前零测试覆盖，补诊断命令单元测试 |
| 3 | D 混合事务 c4 -14.5% 复测 | P0 | 1 周 | P0-1 锁消除后未在稳定环境重跑，确认性能收益回正 |
| 4 | .gitignore 补 joinrng_debug.log | P2 | 0.5 天 | 防调试日志再污染工作区状态 |

**执行状态（2026-10-23）：四项全部完成并推送双端**
- 第 1 项：waitEverUp 提交 `c77bdab`（心跳竞态防护 + 路由视图契约，回归 pkg/cluster / pkg/db 全 PASS）
- 第 2 项：cmd/fixregions 入库 `b7796fc`（实际已有 main_test.go 13 用例，真实缺口为整目录未纳入版本库，已 git add 入库）
- 第 3 项：D 混合事务复测完成——c4 中位数 346.6 TPS（基线 30.6，+1032.5%），-14.5% 损耗确认回正；复测期间发现并临时关闭 distjoin.go dbgLog / txn2pc.go dbgBVV 两处热路径调试残留
- 第 4 项：根目录清理 joinrng_debug.log（103MB）与 openxdb.exe，.gitignore 补 joinrng_debug.log 规则，提交 `397421c`

**推荐下一步**：v1.5.0 正式窗口（11.13）前约 3 周空档，建议提前启动第 1 项「客户端 pipeline 协议」（当前吞吐第一矛盾）。

### 3.2 v1.5.0（协议与吞吐，2026-11-13 ~ 2026-12-03）
| # | 需求项 | 优先等级 | 预估周期 | 依据与说明 |
|---|---|---|---|---|
| 1 | 客户端 pipeline 协议 | P1 | 2 周 | 当前逐语句往返是吞吐瓶颈，驱动层流水线化 |
| 2 | 行协议换行 / NULL 空串二义 | P1 | 1 周 | 修正协议边界，保证导出导入往返一致性 |
| 3 | mysqlproto 高级协议测试正式化 | P2 | 1 周 | mysqlproto_adv_test.go 等排期外未跟踪代码纳入版本库并补测试 |

### 3.3 v1.6.0（SQL/兼容补齐，2026-12-04 ~ 2026-12-31）
| # | 需求项 | 优先等级 | 预估周期 | 依据与说明 |
|---|---|---|---|---|
| 1 | SQL 子集：ALTER 列级 / CREATE DATABASE / 多列窗口 | P1 | 2 周 | 按 T39/T40 SQL 能力缺口逐项补齐 |
| 2 | MySQL wire：binlog / mysqldump 全选项 | P1 | 2 周 | 生态兼容增强（T27_m14 / T31_m18 相关） |
| 3 | 存储过程受限子集 | P2 | 2 周 | 扩展面较大，按 T28_m15 预留推进 |
| 4 | HA 后续：async 丢 RPO 补偿 / 回迁自动化 | P2 | 2 周 | 设计草案评估后立项；region 多副本 / TSO 高可用 / 跨机房多活已明确不做 |

**执行状态（2026-10-24）：第 1 项完成并推送双端；第 2 项 T44 测试落地**
- 第 1 项：SQL 子集三项——ALTER 列级类型变更（`ALTER TABLE t MODIFY [COLUMN] c TYPE`，行数据重写 + 非法值零值回填、同事务原子提交）、CREATE DATABASE [IF NOT EXISTS] 多库目录（`db.go` 新增 `m:databases` 目录键 + SHOW DATABASES 同步可见，default 恒存前置）、窗口 ORDER BY 多列（`OVER (ORDER BY col1, col2 [ASC|DESC] ...)` 任意组合，RANGE 帧仍限单列）；新增 `pkg/sql/t39_sql_subset_test.go` 13 用例，`go build ./...` 与 `go test ./pkg/sql/` 全绿（基线 TestM6DownNodeReadFailover / TestP1DecimalAgg 排期外不修改）
- 第 2 项：T44 复制协议测试正式化（此前 binlog_dump.go / p1_2_wire.go 已入库、测试未跟踪）——mysqlproto 侧新增 t44_wire_test.go 5 用例（COM_REGISTER_SLAVE 合法/截断、COM_BINLOG_DUMP 全量逐记录/中间 LSN/轮询新事件/未启用报错）；sql 侧 4 用例（SHOW MASTER STATUS 位点语义、整库 dump 导入、Position 往返一致）；import 解析层 2 用例（数据库级 dump、`db.tbl` 整体反引号保留）。同步修复两个缺陷：① 整体反引号标识符 `\`db.tbl\`` 被 stripTablePrefix 误剥离点号前缀（parseQualifiedName 增加 whole 标志，CREATE/INSERT/DROP 按需剥离）；② binlog dump 轮询不感知客户端断开导致订阅句柄常驻（文件尾探活读超时，连接关闭即退出释放）。三包全量测试通过
- 第 4 项：阶段二回迁自动化完成——新增补偿帧 msgCompensate（RoleCompensate 独立连接回灌旧主未复制段，新主 apply+追加自身 binlog）、旧主识别新主 epoch 后重置基线以全新 follower 追平、binlog 损坏走全量对齐兜底入口判定；docs/T43_ha_rpo_rejoin.md 同步标注落地，新增 pkg/db/db_rejoin_test.go 三用例，replication/sql 全绿（db 仅基线 TestM6DownNodeReadFailover 排期外不修改）

## 4. 基线失败与边界约束
- TestM6DownNodeReadFailover：双节点心跳时序竞态（滚动启动误判 DOWN），排期外不修改。
- TestP1DecimalAgg：空表 AVG 断言期望 0.0000 与实现返回 NULL 的分歧（实现符合 SQL 标准），排期外不修改。
- 未跟踪代码保留不删：cmd/fixregions/、mysqlproto_adv_test.go、dup_repro_test.go、p1_10_edge_test.go。
- 调试残留 / 构建产物（*.exe、*.log、*_out.txt 等）一律不得纳入提交。
*（内容由AI生成，仅供参考）*

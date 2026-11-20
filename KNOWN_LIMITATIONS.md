---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_fff9e8edd40011f180d5525400248c00
    ReservedCode1: nRLYRIDP/1GeKXFMzy+MkIeUkaOQy4C2TM8wea/j3bgFbddNNJt5q2nbcNbUtzqSASvzNtcPcWXJ4ZzrkqkvWQDHtJFRP7Kh+QVrKtP1qN6vu/M0NVhvssZ6dBppNPAFMQT6A/vK0Nmvm8iIe6AUMGdwV4tTxq2+yaZJif9rSk17Rxbo+6FYYH4hzxU=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_fff9e8edd40011f180d5525400248c00
    ReservedCode2: nRLYRIDP/1GeKXFMzy+MkIeUkaOQy4C2TM8wea/j3bgFbddNNJt5q2nbcNbUtzqSASvzNtcPcWXJ4ZzrkqkvWQDHtJFRP7Kh+QVrKtP1qN6vu/M0NVhvssZ6dBppNPAFMQT6A/vK0Nmvm8iIe6AUMGdwV4tTxq2+yaZJif9rSk17Rxbo+6FYYH4hzxU=
---

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_4a29eb5dcdd111f1b307525400393706
    ReservedCode1: YtWrJY5X9ga7JOaUVWHvfrtfOs6/DmVKYPZqRM7TZBswg+vqfvgKAQs+bWEhiopH81vKtcBqQ57DUYMz1eThDRPUTeI2yo/d6EvqsEpz6ClNMNoRWRohmub6O4nTXhn/Nd+75LfhDIA1kKzyCPyGEKIj/3BEp51ofE10DcjBXlKsfk7uOxETjyzcNYU=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_4a29eb5dcdd111f1b307525400393706
    ReservedCode2: YtWrJY5X9ga7JOaUVWHvfrtfOs6/DmVKYPZqRM7TZBswg+vqfvgKAQs+bWEhiopH81vKtcBqQ57DUYMz1eThDRPUTeI2yo/d6EvqsEpz6ClNMNoRWRohmub6O4nTXhn/Nd+75LfhDIA1kKzyCPyGEKIj/3BEp51ofE10DcjBXlKsfk7uOxETjyzcNYU=
---

---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_7e7b432cc91211f196f9525400248c00
    ReservedCode1: WhCXMgXbRb2HXDCj2bcNX2HOXocawQTWgIJKMrlGyVTEGQlHZiqnGXR5CqWo9lsxfNyvmLAan4sBjGWpRFGU21li/NsYOjAWLe/ahPQ7/ke6iG7/wORZSuFIYkFtRYQACl0R6NKX89zDYe35sAdwxT9ujKvixLBO/2Z+w6pexlriOw8NcMDc0cJtHLA=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_7e7b432cc91211f196f9525400248c00
    ReservedCode2: WhCXMgXbRb2HXDCj2bcNX2HOXocawQTWgIJKMrlGyVTEGQlHZiqnGXR5CqWo9lsxfNyvmLAan4sBjGWpRFGU21li/NsYOjAWLe/ahPQ7/ke6iG7/wORZSuFIYkFtRYQACl0R6NKX89zDYe35sAdwxT9ujKvixLBO/2Z+w6pexlriOw8NcMDc0cJtHLA=
---

# OpenXDB Known Limitations

版本：v1.19.0（2026-11-20）

本文档列出 OpenXDB v1.19.0 的已知限制与边界。每条目给出限制描述、影响与建议，帮助用户在选型、部署与使用时做出合理判断。详细功能范围请以 [docs/product_manual_zh.md](docs/product_manual_zh.md)（第 4 章 SQL 参考）与设计文档 docs/ 为准。

---

## 1. SQL 能力边界

### 1.1 SQL 为子集（非完整标准）

- **限制描述**：OpenXDB 实现的是 SQL 子集，并非完整 SQL 标准。当前 `HAVING` 子句、聚合表达式参数（如 `SUM(a+b)`）、`COUNT(DISTINCT expr)` 及 `GROUP BY` 后 `ORDER BY`/`LIMIT` 已支持（v1.2.0，详见 docs/T41_sql_having_groupby.md）；v1.6.0 起补齐 `ALTER TABLE ... MODIFY COLUMN` 列级类型变更、`CREATE DATABASE [IF NOT EXISTS]` / `DROP DATABASE` / `SHOW DATABASES` / `USE <db>` 多库目录与 `db.tbl` 前缀（详见 docs/T43_sql_database_alter_window）；窗口函数支持多列 `ORDER BY`（RANGE 帧仍限首列键域）；DDL/DML/查询能力以产品手册第 4 章 SQL 参考为准。
- **影响**：迁移自 MySQL/PostgreSQL 等系统的存量 SQL 可能需要改写；依赖完整窗口（如多列窗口排序列）与子查询等高级分析场景仍受限。
- **建议**：迁移前对照 SQL 参考做语法兼容性评审；复杂分析需求可先用 `EXPLAIN` 与子查询/视图组合验证等价改写。

### 1.2 MySQL wire protocol 兼容为子集（--mysql-port）

- **限制描述**：OpenXDB 默认使用自研二进制 TCP 协议（`pkg/server`）；自 v1.1.0 起可通过 `openxdb start --mysql-port <port>` 启用 **MySQL wire protocol 兼容监听**（`pkg/mysqlproto`），支持 MySQL 官方客户端、go-sql-driver/mysql、JDBC（MySQL 方言）驱动直接连接，覆盖常用查询/DML/预处理语句（含 COM_CHANGE_USER、COM_STMT_SEND_LONG_DATA、常用参数类型映射）等能力。协议兼容为常用能力子集；v1.6.0 起已支持 binlog 复制（COM_REGISTER_SLAVE / COM_BINLOG_DUMP 位点订阅）与 mysqldump 常用选项/整库导入（T44），更外围的 MySQL 生态工具链场景仍不受支持。
- **影响**：MySQL 生态监控、备份、管理工具链中依赖更新协议扩展（如 GTID、半同步原生协议、复制过滤）的场景仍不可直接复用；E5 基准采用 Python PEP 249 驱动等价实测，而非标准 sysbench/TPC-C 工具直连。
- **建议**：标准 MySQL 客户端与驱动场景使用 `openxdb start --mysql-port <port>` 直连；需要完整生态工具链的场景请使用驱动包装层或适配层，或使用内置 REPL（`openxdb repl`）与随仓库交付的 JDBC / Python 驱动。
- **v1.17.0 D7 更新**：补齐 MySQL 兼容缺口五项——① 握手版本串由硬编码 `8.0.36-OpenXDB` 改为与 v1.17.0 版本常量同源注入，mysql CLI 登录显示 `8.0.36-OpenXDB v1.17.0`（G1）；② `select version();` 返回 `8.0.36-OpenXDB v1.17.0`（G3）；③ `SHOW CREATE TABLE <t>` 基于存储元数据重建可执行 schema（列定义/类型/主键/二级索引/约束）（G4）；④ `SHOW INDEXES` 与 `SHOW INDEX` 等价（含大小写归一化）（G5）；⑤ 自研交互 CLI 提示符 `oxsql>` 生效（G2）。边界：握手/`version()` 版本号与版本常量同源、发版需同步更新；`oxsql>` 为自研 CLI 提示符，第三方 mysql CLI 的 `mysql>` 属客户端侧展示不受影响；五项均为读路径/客户端体验改动，不触碰事务、复制与 WAL 语义，全仓回归全绿。详见 [docs/experiments/D7_mysql_compat_validate.md](docs/experiments/D7_mysql_compat_validate.md)。
- **v1.19.0 D7 更新（版本常量单点化与漂移修正）**：新增零依赖包 `pkg/version`（`Version` / `MySQLServerPrefix` / `ServerVersion`），`cmd/openxdb` 版本输出、`pkg/mysqlproto` 握手版本串与 `pkg/sql` 的 `select version()` 全部取自同一常量，消除 `cmd/openxdb/main.go` 与 `pkg/mysqlproto/handshake.go` 两处独立版本常量。同时修正历史漂移：握手串与 `select version()` 此前各自硬编码且停留在 `v1.17.0`（落后版本线两轮，未随 v1.18.0 发版更新），现均为 `8.0.36-OpenXDB v1.19.0`。边界：后续发版只需修改 `pkg/version.Version` 一处，握手串与 `select version()` 自动联动；客户端若硬编码校验过 `8.0.36-OpenXDB v1.17.0` 需相应调整。

### 1.3 存储过程/触发器为受限子集

- **限制描述**：存储过程与触发器覆盖设计文档（T28）所定义的语法与语义：v1.2.0 补齐 IF/循环/CASE/递归深度上限，v1.6.0 补齐 DECLARE/DEFAULT、IF/ELSIF/ELSE、WHILE/LOOP/FOR/REPEAT、游标（OPEN/FETCH/CLOSE）与 OUT/INOUT 参数回写；v1.8.0 补齐触发器行级语义（FOR EACH ROW、NEW/OLD 行引用、体回滚递归策略），v1.19.0 补齐触发器体 `BEGIN…END` 复合块（复用过程体解释器，自动获得块作用域、DECLARE、控制流、异常处理与动态 SQL）、`BEFORE INSERT/UPDATE` 的 `SET NEW.<col> = <expr>` 行回写、`FOLLOWS`/`PRECEDES` 执行顺序声明；同一版本亦补齐存储过程异常处理（`DECLARE {CONTINUE|EXIT} HANDLER FOR SQLSTATE / 错误码 / SQLEXCEPTION / SQLWARNING / NOT FOUND`、`SIGNAL`、`RESIGNAL`）与动态 SQL（`PREPARE ... FROM`、`EXECUTE ... USING`、`DEALLOCATE PREPARE`），并补齐嵌套 `BEGIN…END` 复合语句块（块内 `DECLARE` 变量/游标/处理器的嵌套作用域与块退出出栈）、`DECLARE <name> CONDITION FOR SQLSTATE '<值>' / <错误码>` 命名条件（可在 `HANDLER FOR` / `SIGNAL` / `RESIGNAL` 按名引用）、`GET DIAGNOSTICS`（`ROW_COUNT` 与 `CONDITION <n>` 的 `RETURNED_SQLSTATE` / `MYSQL_ERRNO` / `MESSAGE_TEXT`）、HANDLER 体多语句复合块。仍不支持完整 SQL/PL 能力（如 `GET DIAGNOSTICS` 的其余条件信息项、`DECLARE ... CONDITION FOR` 引用已声明条件、游标 `FOR UPDATE` / 可更新游标、`SIGNAL` 之外的 `RAISE` 等方言语法）。
- **影响**：复杂业务逻辑若依赖完整 PL/SQL 需要改写为应用层逻辑或其他等价实现；动态 SQL 仅覆盖过程体已支持的语句类型（如 `SHOW` 等语句经 `EXECUTE` 会被拒绝），且 `HANDLER` 匹配按「最近声明优先」、不做同块重复条件声明校验；诊断区仅保留最近一条语句的条件信息（`CONDITION <n>` 序号仅支持 1）。触发器体复合块与过程体共用同一解释器，故可用语句范围与存储过程一致；`SET NEW.<col>` 仅在 `BEFORE INSERT` / `BEFORE UPDATE` 行级触发器内生效，`OLD` 侧始终只读；`FOLLOWS` / `PRECEDES` 仅表达同表同事件同时机触发器组内的相对次序，跨表或跨事件无排序语义，成环与指向组外触发器名的依赖被忽略并按创建顺序兜底。
- **建议**：业务逻辑优先在应用层实现；确需数据库内逻辑时，使用文档内已支持的 CREATE PROCEDURE / TRIGGER 语法子集；异常处理建议把 `EXIT HANDLER` 放在过程顶层做统一兜底，`CONTINUE HANDLER` 限定在需要跳过失败的语句附近，需读取错误详情时在处理器体内首条语句使用 `GET DIAGNOSTICS`；触发器若需改写待写入值，把赋值逻辑集中在 `BEFORE` 触发器内，`AFTER` 触发器只做观测与落痕，并用 `FOLLOWS` 显式声明有先后依赖的触发器，避免依赖隐式创建顺序。

### 1.4 行协议与类型精度

- **限制描述**：行协议单元格边界自 v1.5.0 起已消除 NULL/空串二义：NULL 渲染为 `\N` 字面量、空字符串渲染为空单元格；单元格内换行/回车/反斜杠由服务端转义为 `\n` / `\r` / `\\` 并在驱动端解码还原，文本参数通过 `E'...'` 转义字符串字面量发送（详见 docs/T19_client_drivers.md §2.2/§5）。DECIMAL 自 v1.12.0 起支持按列声明 scale 的可配置精度：内部值对象携带小数位（DS）字段，跨 scale 比较/舍入按对齐规则处理，行协议与 JSON 导出按声明 scale 保留小数位，不再强制 4 位规范化。
- **影响**：高精度 DECIMAL（>4 位小数）按列精度直接存取，无需整数单位换算；未声明 scale 时沿用默认 4 位；超列 scale 的导入值按拒绝策略处理（宽松模式跳过该行）。
- **建议**：金融等要求高精度的场景在 DDL 中显式声明 DECIMAL scale（如 `DECIMAL(18,6)`）；文本/NULL/空串语义边界保持不变。

### 1.5 事务保存点（`SAVEPOINT`）语义与 2PC 边界

- **限制描述**：v1.19.0 落地 `SAVEPOINT <name>`、`ROLLBACK [WORK] TO [SAVEPOINT] <name>`、`RELEASE SAVEPOINT <name>` 三条语句（关键字大小写不敏感），语义对齐 MySQL：同一事务内重名 `SAVEPOINT` 覆盖旧标记；`ROLLBACK TO` 只回退标记点之后的本事务写入、不删除该保存点（可重复回到同一点）；`RELEASE SAVEPOINT` 删除该点及其之后所有保存点但不撤销已写入数据；回滚到已释放/不存在的名报 `savepoint <name> does not exist`；`COMMIT` / `ROLLBACK` 结束事务时清空全部保存点；未开启事务时执行三条语句均报既有文案 `no active transaction`；支持任意层嵌套。实现上保存点标记的是事务有序操作日志（`ops`）的序位，`ROLLBACK TO` 用标记前的日志前缀重建写批次与写集，因此回滚后读己写（dirty 读路径）同步回到标记点状态。**跨节点 2PC 场景采用「本地保存点生效 + 跨节点写集按写集前缀丢弃」策略**（非显式拒绝）。
- **影响**：显式事务内的语句级局部回退（如应用侧 SAVEPOINT 包裹的乐观重试、存储过程内局部补救）可直接使用保存点，无需整事务回滚重做。2PC 路径下 `ROLLBACK TO` 的有效范围限于本节点已应用写入与已暂存的跨节点写集：远端参与者只在 `COMMIT` 阶段才收到 `PREPARE`，`ROLLBACK TO` 时远端不存在任何未提交状态，故按写集前缀截断即等价于远端丢弃，保持本地与远端写集一致，不产生语义漂移。边界：保存点状态仅存于单次事务会话内存中，不写入 WAL/协调者记录，进程崩溃后不做保存点级恢复（崩溃恢复以整事务为粒度）；会话级代理/连接池切换会话后保存点不跨会话可见；保存点不改变事务快照隔离的读视图（回滚不重建快照，仍读事务开始时的可见版本）。
- **建议**：把保存点用于「语句级可重试的局部失败」而不是「事务级进度管理」；跨节点写事务中若需回退到某个业务阶段，仍应显式设计重新执行的写集，不要依赖保存点保存跨节点中间态；长时间运行的事务中保存点数量与嵌套深度应受控（每个保存点保留一份操作日志序位，`ROLLBACK TO` 的重建成本与标记点后的日志长度相关）。

### 1.6 递归 CTE（`WITH RECURSIVE`）边界

- **限制描述**：v1.19.0 落地 `WITH RECURSIVE <name> [(<col>, ...)] AS (<anchor> UNION [ALL] <recursive>) [, ...] <SELECT>`，语义对齐 MySQL：支持可选列名列表、`UNION`（整行去重）/ `UNION ALL`（保留重复）、同一 `WITH` 内与非递归 CTE 混用（后定义可引用先定义，anchor 可引用前置普通 CTE），递归 CTE 可被外层 `FROM` / `JOIN` 右端 / 子查询引用。实现上**非递归 CTE 仍走原有宏展开路径（按定义深拷贝并替换表名），零改动零回归**；只有声明 `RECURSIVE` 的定义改走物化 worktable：先执行 anchor 得到初始行集，随后每轮把上一轮新增行作为递归项中自引用表的输入反复执行，直到某轮无新增行（收敛）或达到深度上限。深度上限配置项为 `recursive_cte_max_depth`（默认 1000，对齐 MySQL `cte_max_recursion_depth`），支持 `SET recursive_cte_max_depth = <n>` 热切换与 `SHOW` 查看，超限报 `recursive CTE <name> exceeded maximum recursion depth <n>`，不做静默截断。递归项受限：自引用必须且只能出现一次、且必须位于递归项的 `FROM` 主表或 `JOIN` 右端表位置；不得出现在子查询 / `FROM` 派生表内，不得作为聚合或窗口函数参数；递归项列数须与 anchor 输出列（及声明的列名列表）一致。未声明 `RECURSIVE` 却使用 `UNION` 的定义报 `requires WITH RECURSIVE`；未声明 `RECURSIVE` 的循环引用保持既有 `cyclic CTE reference` 文案。
- **影响**：递归结果为语句级物化快照，行数即物化行数；`UNION` 去重需按整行建立去重集合，内存开销与结果行数线性相关（宽表 / 大文本列值成本更高），`UNION ALL` 无去重开销但环形数据不会自动收敛，必须以 `WHERE` 谓词或深度上限终止。递归项每轮完整执行一次（含 `JOIN` / 过滤），轮数与结果深度同阶，深递归耗时随轮数线性增长。递归项与 anchor 的读沿用既有物理表扫描与 `scanMergedCluster` 路径，不绕过 region 路由与可见性过滤，因此分片（跨节点）场景下每轮迭代都要完成一次分布式读，网络往返次数与轮数同阶。物化结果为语句内临时表，不落盘、不参与其它语句的读己写交叉校验；暂不支持 `WITH` 与 `INSERT` / `UPDATE` / `DELETE` 的组合（如 `INSERT ... WITH RECURSIVE ...`）、递归项内的聚合 / 窗口 / `DISTINCT` / `GROUP BY` 分析语义，以及多递归 CTE 之间的相互自引用（互相递归）。
- **建议**：用递归 CTE 处理层数可控的层级数据（组织树、BOM、分类路径）与序列生成，并在递归项中显式给出终止谓词（如 `WHERE n < 100`、`WHERE depth < 6`），不要把收敛完全寄托于 `UNION` 去重或深度上限；深递归 / 大结果集场景优先在递归项中前置过滤以减少每轮参与行数；分片表上的递归查询应把 anchor 的起点行限制为少量键（避免每轮扫描大量 region），必要时先用非递归 CTE 或子查询把候选键收敛到单 region 再递归展开。

### 1.7 SQL JSON 函数族边界

- **限制描述**：v1.19.0 落地 8 个 JSON 标量函数——`JSON_VALID`、`JSON_EXTRACT`、`JSON_UNQUOTE`、`JSON_TYPE`、`JSON_LENGTH`、`JSON_OBJECT`、`JSON_ARRAY`、`JSON_SET`（函数名大小写不敏感），可在投影列（含算术与 `AS` 别名组合）、`WHERE` 条件、`UPDATE SET` 赋值、`CASE` 分支及函数实参嵌套位置使用，实参可为列引用、字面量与嵌套函数。路径语法为子集：根 `$`、成员 `.key`、引号成员 `$."key with space"`、数组下标 `[n]`（非负整数），**不支持**通配符 `*` / `[*]`、递归下降 `..`、过滤表达式与负数下标（`$[-1]` 报 `invalid array index`）。`JSON_SET` 为 copy-on-write 深拷贝写入，可替换既有键、追加缺失键、数组末尾追加与根替换，但中间层级已是标量时写入报错（不自动覆盖中间层），数组下标仅允许等于长度处追加（越界报 `out of range`）。非法 JSON 文本报 `invalid JSON text`、非法路径报 `invalid JSON path` 系列错误，**不静默返回 `NULL`**；`JSON_VALID` 例外——对任意（含非法）文本返回 `1` / `0`；`NULL` 实参按 SQL 语义传播为 `NULL`。结果类型仅使用既有 `TEXT` / `INT` / `NULL`：文档类结果（`JSON_EXTRACT` 的对象 / 数组 / 字符串、`JSON_OBJECT`、`JSON_ARRAY`、`JSON_SET`）为紧凑 JSON 文本（分隔符 `", "` / `": "`，字符串值带引号），数值类结果（`JSON_LENGTH`、`JSON_VALID`）为 `INT`，无 `BOOLEAN` / `DOUBLE` 结果类型——`JSON_TYPE` 以文本返回 `OBJECT` / `ARRAY` / `STRING` / `INTEGER` / `DOUBLE` / `BOOLEAN` / `NULL`。建表侧无独立 `JSON` 列类型（`CREATE TABLE ... j JSON` 报 `unsupported column type`），JSON 文档以 `TEXT` 列承载。
- **影响**：每个函数调用都在执行期对实参文本做一次**完整单遍解析**（`encoding/json` + `UseNumber`，保留数字原始字面量与对象键序，并校验值之后无多余内容），无计划期缓存、无路径预编译、无列级 JSON 索引，因此含 JSON 函数的查询开销与「行数 × 每行文档大小 × 函数调用次数」同阶；同一行内多次引用同一文档会重复解析（如 `JSON_EXTRACT` 与 `JSON_LENGTH` 并列时解析两次）。`JSON_SET` 每次写入完整重建文档并深拷贝，超大文档的更新成本随文档体积线性增长，`UPDATE` 逐行调用时按影响行数放大。`JSON_EXTRACT` 结果为文本，字符串值带引号（如 `"alice"`），与普通字符串列比较需先 `JSON_UNQUOTE`——`JSON_EXTRACT(doc, '$.name') = 'alice'` 为假，`JSON_UNQUOTE(JSON_EXTRACT(doc, '$.name')) = 'alice'` 为真；多路径并列时输出 JSON 数组（缺失路径以 `null` 占位），而非按首路径短路。文档合法性仅在实际求值时校验，`WHERE` 中未加 `JSON_VALID` 守卫时，遇到非 JSON 文本的行会直接报错中断整条查询（与 MySQL 行为一致），扫描顺序靠前的坏数据优先暴露。
- **建议**：写入时用 `JSON_VALID` 做约束性校验（或应用层前置校验）；查询时对可能含脏数据的列显式加 `JSON_VALID(doc) = 1` 谓词限定后再取值，避免单行坏数据中断整条查询。需要按 JSON 字段做等值 / 范围过滤时，先 `JSON_UNQUOTE(JSON_EXTRACT(...))` 归一为文本再比较，或将该字段在写入时冗余为独立列并建索引（当前无 JSON 函数索引，无法索引下推过滤 JSON 路径）。大文档（数百 KB 以上）避免在逐行 `UPDATE` 中反复 `JSON_SET`，改为整篇替换一次写入，或把高频访问的键拆为独立列；单行内多路径取值优先用 `JSON_EXTRACT(doc, '$.a', '$.b')` 一次调用替代多次单路径调用，减少重复解析。

### 1.8 可更新视图边界

- **限制描述**：v1.19.0 A2 在 A1 可更新视图基础上落地 `WITH CHECK OPTION`。`CREATE VIEW ... WITH [CASCADED|LOCAL] CHECK OPTION` 在建视图时把定义 `WHERE` 规范化为「基表裸列名」条件副本，随视图目录持久化（`ViewMeta` 新增 `CheckOption` / `CheckCond`，均 `omitempty`；写法省略 `CASCADED` / `LOCAL` 时按 `CASCADED` 记录）。可更新性判定与 A1 一致（仅单表 `FROM`、无 `DISTINCT` / `GROUP BY` / `HAVING` / 聚合函数 / 窗口函数 / CTE / `LIMIT` / `OFFSET`，投影列全部为可逆裸列引用，允许 `t.col` 与 `AS` 别名，`SELECT *` 按基表列序恒等展开）；不可更新视图带 `CHECK OPTION` 在建表期即报 `view <name> is not updatable: <reason>`。**`LOCAL` 与 `CASCADED` 在本版本语义等价**——可更新性判定要求基表为真实表（不支持视图套视图），不存在需要级联检查的下层视图，两者仅按写法原样记录。经带 `CHECK OPTION` 的视图写入时，对新行（`INSERT` 含视图未暴露基表列的默认取值，`UPDATE` 为 `SET` 生效后的整行）求值视图定义条件，不满足报 `CHECK OPTION failed '<view>'` 并整条语句回滚；`DELETE` 不受限、不校验。`ON DUPLICATE KEY UPDATE` 命中冲突后的更新分支与 `REPLACE` 的冲突替换分支不施加校验（仅普通插入与 `UPDATE` 产生的新行求解条件）。
- **影响**：带 `CHECK OPTION` 的视图写入失败粒度为**整条语句**——多行 `INSERT` / 多行 `UPDATE` 中任一行越界即全语句回滚（此前已处理的行同样不落盘，语句末尾统一应用写集的设计天然保证这一点）。校验复用查询侧条件求值（`matchWhereRel`），不引入额外全表扫描，仅在逐行写入路径上多一次条件求值开销；写入仍按 A1 的列映射落到基表，权限按基表判定。不带 `CHECK OPTION` 的视图沿用 A1 行为——视图「看不到」的基表行同样可能被 `UPDATE` / `DELETE` 改动（与 MySQL 无 `CHECK OPTION` 时一致）。视图列映射与校验条件均在 `CREATE VIEW` 时静态固化，`ALTER TABLE` 增删改被引用的基表列**不会**刷新既有视图，失效后写入报 `unknown column`。语句级额外开销为一次视图目录读取（每条视图 DML 一次只读事务，显式事务内复用当前事务），无计划期缓存。
- **建议**：需要严格约束「写入不得越出视图可见范围」时使用 `WITH CHECK OPTION`；批量写入前在应用层预校验可减少整条语句回滚的代价（一行越界会使同批全部回滚）。需要 `ON DUPLICATE KEY UPDATE` 的更新分支也受约束时，改为显式 `UPDATE`（走校验）或应用层比对。`LOCAL` 与 `CASCADED` 可任选其一，本版本无行为差异。基表结构变更（尤其重命名 / 删除被视图引用的列）后重建视图以刷新列映射与校验条件。高频写入路径直接写基表可省去每语句一次的视图目录读取。旧版本创建的视图如需可写或需要校验，重建一次即可补齐映射与条件。

## 2. 性能边界

### 2.1 协议/解析性能边界（v1.13.0 已落地 P1 并发处理；v1.14.0 落地 P0 只读并行 + P1 组提交定时窗口）

- **限制描述**：v1.3.0 已将全局会话锁消除为会话级细粒度锁，v1.5.0 起 Python/JDBC 驱动支持客户端 pipeline 批量流水线执行；v1.12.0 完成协议解析两项 P1 优化（① 全量对话边界 bufio 读覆层，mysqlConn 外层 bufio.Reader 减少逐字节读系统调用；② COM_QUERY 多语句在线切分执行，splitSQLStatements 安全跳过引号/注释内分号，多结果以 `serverStatusMoreResults=0x0008` 状态位串行返回，单语句行为完全兼容）；v1.13.0 完成服务端多连接并发处理：协议读侧 REPLSession 改造为 reader goroutine 预读 + 有界 channel + 主循环顺序执行，消除单连接逐命令串行读等待，每连接独立 goroutine 使多连接请求可并行进入执行层；会话级隔离保持（connSession 独立 SQL 会话），既有 server/mysqlproto 测试与单节点部署零回归。E5 脚本侧 bench_oltp.py / bench_tpcc.py 事务内语句启用客户端 pipeline 批量发送（整事务一次往返），并新增连接超时防护（--timeout 默认 120s、连接级异常自动重建并计数）。
- **影响**：E5 受控复测（干净数据重置 + 3 轮取中位数，8 连接）：OLTP read 0.40 / write 0.40 / read-write 0.40 TPS（errs=0 / reconnects=0）；TPC-C 5 仓 tpmC 78.00 / tpm_total 164.50（errs=0 / reconnects=0，较 v1.12.0 的 41.00 / 88.50 提升约 90%）。说明：OLTP read 侧仍受引擎聚合/范围查询执行开销制约（SUM/GROUP BY/ORDER BY 范围查询单条约 560ms、COUNT 全表约 580ms、点查约 3ms；SQL 执行层 v1.12→v1.13 零改动，属既有引擎特征），单连接 pipeline 与串行对照无差异证明瓶颈在服务端执行而非网络往返；v1.12.0 记录的 read 1.33 TPS 与当前受控复测不一致，以本次为准。TPC-C 高并发连接超时问题已修复：客户端默认超时提升至 120s 且连接级异常自动重建，3 轮复测 errs=0 / reconnects=0。
- **v1.14.0 更新**：落地 P0 只读 SELECT 并行（REPLSession 批内连续 autocommit 只读 SELECT 经 worker pool 并发执行、按序重组、批尾统一 Flush，开关 `ro_parallel` 默认 on 可回退）与 P1 WAL 组提交定时窗口（SyncUpTo 窗口内合并 fsync，配置 `group_commit_window_ms` 默认 0.5ms、0=关闭回退 v1.13.0 被动攒批）。E6 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read 0.43 / write 0.43 / read-write 0.43 TPS（较 v1.13.0 +7.5%），TPC-C tpmC 85.50（较 v1.13.0 +9.6%，3 轮原始 80.00 / 85.50 / 114.50，errs=0）。边界：read 模式仍受引擎聚合/范围查询执行开销制约（执行内核未并行，P3 写语句并行不在 v1.14.0 范围）；组提交窗口对延迟敏感单写事务附加 ≤0.5ms 同步等待（可配 0 关闭）；tpmC 轮 3 出现 114.50 高值，波动来源待查（候选 GC / RocksDB compaction / Python GIL 释放时序）。详见 [docs/experiments/E6_v1140_ro_parallel_wal_groupcommit.md](docs/experiments/E6_v1140_ro_parallel_wal_groupcommit.md)。
- **v1.15.0 更新**：落地 SQL 执行层四项优化——W1 Scan 快路径（txn 无未提交写时快照直透，跳过全量物化+重建排序）、W2 COUNT(*)/COUNT(1) 快速路径（无 WHERE/GROUP 等时走存储层 RangeCounter 逐 region 计数求和）、W3 SELECT 列投影下推（按需解码列，JOIN/子查询/星号保守全列回退）、W4 ORDER BY 索引顺序扫描免排序 + DISTINCT 相邻去重免排序（单列二级索引/主键 + 运行时 O(n) 已序验证）。E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read 0.38 / write 0.40 / read-write 0.40 TPS（errs=0，与 v1.14.0 持平、无回归），TPC-C tpmC 81.50（3 轮原始 63.50 / 81.50 / 102.00，errs=0，与 v1.14.0 的 85.50 持平）。单语句专项压测（PEP 249 驱动，N=7-9 取中位数，sbtest 10,000 行）：COUNT(*) 全表 **580ms → 5.18ms（约 103-112 倍）**、全表扫描多列 613.51ms（基线 780-900ms，约 1.27-1.47 倍）、全表扫描单列 532.33ms（约 1.47-1.69 倍）、点查 2.09-3.25ms（基线 ~3ms，持平）、范围聚合 525.36ms（基线 ~560ms，约 1.07 倍）；ORDER BY 主键 548.51ms 与 DISTINCT 619.88ms 均与无排序/无去重投影持平（免排序生效），ORDER BY 二级索引 764.39ms 为索引扫描+批量回表开销但免去全量 sort。边界：OLTP 事务级吞吐仍受全局会话锁与单机端侧网络往返制约（个位数 TPS 量级）；ORDER BY 二级索引快速路径在 10k 行量级显示索引+回表开销高于排序收益，仅在更大数据量/索引覆盖场景收益为正；免排序/DISTINCT 相邻去重仅限单列裸列场景，多列/表达式 ORDER BY 仍走原路径。详见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。
- **v1.17.0 更新**：落地显式事务写路径优化三项——D1.1 UPDATE 快照刷新锁外化（`snapshot_lock_external`，显式事务 UPDATE 锁内快照刷新移到锁外，对齐 C2 Begin 锁外化同型改造）、D1.2 autocommit 写 pipeline 深化（`write_pipeline` 默认 off 可热切换，写语句接入连接内 pipeline，减少端侧逐条往返）、D1.3 WAL 组提交主动定时窗口（`group_commit_active_window` 默认 off 回退 v1.16.0 C1 单等待者免窗口，写密集下主动定时刷新合并 fsync，降低 fsync 次数与写放大；不影响 WAL 重放 / 崩溃恢复 / 复制 2PC 顺序）。E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read **11.85** / write **12.03** / read-write **12.40** TPS（QPS 165.90 / 48.13 / 223.20，errs=0 / reconnects=0），较 v1.16.0（read 10.37 / write 8.33 / read-write 8.15 TPS，QPS 145.13 / 33.33 / 146.70）**write QPS +44%、read-write TPS +52%、read 无回归 +14%**；TPC-C 5 仓 tpmC **776.00** / tpm_total 1720.50（3 轮原始 810.00 / 679.50 / 776.00，errs=0，较 v1.16.0 的 254.00 / 581.00 提升约 3.0-3.1 倍）。**达成 D1 验收目标：write QPS 中位 ≥40（48.13）、TPS write ≥9（12.03）、tpmC ≥300（776.00），其余指标无回归、errs=0 / reconnects=0**。边界：write 模式已脱离吞吐下限（单轮最低 QPS 41.00 仍高于 v1.16.0 中位 33.33）；TPS/QPS 仍受全局会话锁与单机端侧往返制约，数值为单机端侧功能基准；所有优化均有独立 feature flag 可灰度回退。详见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。
- - **v1.17.0 D6 更新**：落地备库常驻读服务（`openxdb replica --mysql-port <port>`，D6.1）与只读强制（D6.2，写语句按 `isWriteStmt` 白名单拒绝）。并发模型：复制应用以 `f.mu` 串行写存储，读路径经 `ReadBarrier` 以 `AppliedLSN` 为界对齐后快照读，存储原子批次 + 快照隔离保证无半批次可见；读侧仅短临界区等待当前批次，不阻塞后续复制、无死锁。边界：备库读可见性以查询开始时的 `AppliedLSN` 为界（随后到达的复制批次不影响当前查询）；复制应用仍为单 goroutine 串行（吞吐上限与主端 binlog dump 速率相关）；备库写入被拒绝，仅支持读。专项回归 6 项验收全通过（常驻查询 / 写拒 / 滞后窗口 / 崩溃恢复二次重启 / 全仓 build+test 全绿 / 主端无回归），详见 [docs/experiments/D6_replica_resident_read.md](docs/experiments/D6_replica_resident_read.md)。
**v1.17.0 D2 回退记录**：D2 会话锁与调度精细化（D2.1 session lock granularity split + D2.2 read-parallel in write txn）因并发伸缩验收未达标（D2.3 复测 16 连接 read TPS 10.80 较 8 连接 11.85 为 -8.9%、read-write 10.28 较基线 12.40 为 -17.1%，未达"16 连接较 8 连接提升 ≥20%"验收目标）已整体回退，恢复 D1 基线行为（等价 cdd62e7）；D2.3 并发伸缩 bench 数据与实验文档（e1ba367）保留作为记录。回退后 E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read / write / read-write **12.75 / 12.63 / 10.97 TPS**（QPS 178.50 / 50.53 / 197.40，errs=0 / reconnects=0），TPC-C 5 仓 tpmC **709.00** / tpm_total 1568.00，**D1 验收指标复测仍达标（write QPS 中位 ≥40 为 50.53、TPS write ≥9 为 12.63、tpmC ≥300 为 709.00，read / read-write 无回归，errs=0 / reconnects=0）**；全局会话锁制约仍为单机端侧量级。详见 [docs/experiments/D2_3_concurrency_scaling.md](docs/experiments/D2_3_concurrency_scaling.md)。
- **v1.17.0 D5 发版最终复测**：D3 可观测性与诊断闭环（EXPLAIN 节点级耗时/行数估算 + slowlog 阈值区间 TOP N 汇总）与 D4 并发规模与稳定性验证（8 连接写密集 30min 长稳 errs=0、goroutines 恒 2、wal.log 受控 45.6MB、check COUNT 严格等价 2355864；bench 工具参数化）落地。D5 发版前 E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read / write / read-write **14.15 / 13.40 / 12.60 TPS**（QPS 198.10 / 53.60 / 226.80，errs=0 / reconnects=0），较 v1.16.0（10.37 / 8.33 / 8.15 TPS，QPS 145.13 / 33.33 / 146.70）**write QPS +61%、read-write TPS +55%、read 无回归 +36%**；TPC-C 5 仓 tpmC **774.50** / tpm_total 1746.50（较 v1.16.0 的 254.00 / 581.00 提升约 3.0 倍）。**达成 v1.17.0 验收目标：TPS read ≥11（14.15）/ write ≥9（13.40）/ rw ≥8.5（12.60），QPS read ≥145（198.10）/ write ≥40（53.60）/ rw ≥146（226.80），tpmC ≥300（774.50），errs=0 / reconnects=0**。边界：write 模式已脱离吞吐下限（QPS 中位 53.60，单轮最低 52.87 仍高于 v1.16.0 中位 33.33）；D2 回退后全局会话锁制约仍为单机端侧量级；所有优化均有独立 feature flag 可灰度回退。详见 [docs/experiments/E5_standard_bench_alignment.md](docs/experiments/E5_standard_bench_alignment.md)。
- **v1.16.0 更新**：落地 SQL 执行内核并发模型六项优化——C1 WAL 组提交增强（单等待者免窗口 + 批量刷盘，`group_commit_window_ms` 可回退）、C2 Begin 快照创建锁外化（锁内创建改为锁外，ID/序号仍单调）、C3 ro_parallel 下沉 executor + 显式事务只读并行（`ro_parallel` 默认 on 可回退）、C4 提交应用流水线化/组提交、C5 cgo 扫描拷贝优化（unsafe 边界专项）、C6 连接内语句 pipeline / 快照复用（`snapshot_share`、`pipeline_flush` 独立 flag 默认开）；另落地 **WAL 自动 checkpoint/truncate**（`wal_auto_checkpoint` 默认 on、`wal_checkpoint_interval_ms` 默认 60000、`wal_checkpoint_max_mb` 默认 64；安全位点 safe=min(synced,applied)，截断前 Flush 落 SST，只删已 fsync 且已应用记录；长期运行 wal.log 大小受控、崩溃恢复结果与截断前严格等价，详见 docs/experiments/WAL_auto_checkpoint_truncate.md）。C7 E5 受控复测（每轮 prepare 重置 + 3 轮取中位数，8 连接）：OLTP read **10.37** / write **8.33** / read-write **8.15** TPS（QPS 145.13 / 33.33 / 146.70，errs=0 / reconnects=0），较 v1.15.0（read 0.38 / write 0.40 / read-write 0.40 TPS，QPS 5.37 / 1.53 / 6.30）TPS 提升约 20-27 倍、QPS 提升约 22-27 倍，**达成 v1.16.0 验收目标（TPS≥1.5、QPS≥30）**；TPC-C 5 仓 tpmC **254.00** / tpm_total 581.00（3 轮原始 224.00 / 301.50 / 254.00，errs=0，较 v1.15.0 的 81.50 / 170.50 提升约 3.1-3.4 倍）。边界：write 模式为吞吐下限（QPS 中位 33.33，单轮波动至 29.13 仍满足目标）；所有优化均有独立 feature flag 可灰度回退；TPS/QPS 受全局会话锁与单机端侧往返制约，仍为单机端侧量级。详见 [docs/experiments/C7_e5_controlled_retest.md](docs/experiments/C7_e5_controlled_retest.md)。
- **建议**：将上述数值视为**单机端侧功能基准**，非生产级吞吐承诺；生产负载请先按真实数据量/并发做压测，评估是否需要单机优化或集群扩展。
- **v1.18.0 更新（读路径口径与 D6 结论）**：v1.16.0 SQL 执行内核并发模型优化后 OLTP 升至 read 10.37 / write 8.33 / read-write 8.15 TPS，v1.17.0 写路径优化后达 read 14.15 / write 13.40 / read-write 12.60 TPS、TPC-C tpmC 774.50（口径见 [CHANGELOG](CHANGELOG.md)）。SQL 完整路径当前约 240 QPS（read 17.12 TPS × 14 ops/事务）**不是引擎上限**：同机 RocksDB 裸读 E1 实测 71,003 QPS，热点为 `runtime.cgocall` flat 63.75%、单点 Get 固定开销与调度争用（详见 [docs/storage_engine_selection.md](docs/storage_engine_selection.md) §5/§6）。块缓存调至 256MB 实测 **-1.6% 负收益，已回退**，证明瓶颈在 cgo 往返而非缓存容量。D6.2 已把 routed JOIN 回表与 IN 子查询的逐行/逐值点查改为一次批量读（往返 N→1，N=64 实测 33.3ms → 0.52ms，见 [D6_2 报告](docs/experiments/D6_2_batch_pointget.md)），但单点 Get 固定开销与 cgo 往返成本未消除；跨 region 远端区间聚合（多 `QUERY_REQ` 合并）因需改 ScanRemote 协议，顺延 v1.19.0 与写路径 2PC 请求骨架一并处理（见 [D6_3 评估](docs/experiments/D6_3_snapshot_scan_eval.md)）。边界：以上为单机 8 连接基准口径，非生产级吞吐承诺。
- **v1.18.0 D5 更新（Linux 平台口径与跨平台可比性）**：D5 在 RHEL 9.7 目标机（2 vCPU / 2.8GB / Go 1.22.12 / RocksDB 11.8.1 CGO，源码 HEAD `dac6843`）按与 Windows **完全同口径**（同脚本、8 连接、每轮 prepare 重置、3 轮取中位数）完成受控复测：OLTP read / write / read-write **8.17 / 8.13 / 8.10 TPS**（QPS 114.33 / 32.53 / 145.80），TPC-C 5 仓 tpmC **749.50** / tpm_total 1684.50，14 组采集全部 errs=0 / reconnects=0。对比 Windows v1.17.0 基线（14.15 / 13.40 / 12.60 TPS，tpmC 774.50）：OLTP 达 57.7%–64.3%，TPC-C 达 96.8%。**边界一（平台差异由 CPU 配额解释，非代码路径）**：同机 threads=2 与 threads=8 的 OLTP read TPS 同为约 8.2，而 p50 延迟由 0.24s 升至 0.95s，说明 2 vCPU 下吞吐已饱和、8 连接为 CPU 4 倍超配口径；TPC-C 因客户端 Python 8 线程 GIL 与服务端队列共同限流，跨平台差异被掩盖仅剩 3.2%。**边界二（Linux 数值不作性能对标）**：Linux 数据仅用于同口径功能与稳定性验证，性能基线仍以 Windows 平台为准，跨平台不可直接等值比较。**边界三（复测纪律）**：不重置库表连续压测会使 TPC-C tpmC 衰减 565.00 → 230.50 → 220.00（LSM 写放大/compaction 积压，属库表状态非版本回归），"每轮 prepare 重置"为可比性必要条件；`bench_tpcc.py` 在库内已有历史订单时重跑会以固定起始订单号写入并触发 duplicate primary key: history / order_line（工具限制，重置表后 errs=0）。详见 [docs/experiments/D5_linux_e5_controlled_retest.md](docs/experiments/D5_linux_e5_controlled_retest.md)。

### 2.2 备份/恢复与大批量导入耗时

- **限制描述**：逻辑备份（BACKUP）与 mysqldump 导入为文本/逻辑路径，大表耗时与数据量线性相关；导入为文本规则解析，覆盖常见 dump 语法子集。
- **影响**：超大库备份/导入窗口较长；未知类型在严格模式报错、宽松模式跳过该表；单元格解析失败在宽松模式丢弃该行。
- **建议**：定期备份并验证恢复演练；导入使用 `IMPORT BATCH n` 控制批次与 `IGNORE ERRORS` 策略；关注日志中的跳过/失败计数。

## 3. 单机 → 分布式边界

### 3.1 跨节点写依赖 2PC

- **限制描述**：跨节点写（涉及多个 region/节点）必须走两阶段提交（2PC）协议帧；单节点部署不会进入 2PC 路径（零触发、零回归）。
- **影响**：集群模式下跨节点写有 2PC 的 PREPARE/COMMIT 往返开销；任何 PREPARE 失败或本地提交失败会整体 ABORT 语句。
- **建议**：合理设计 region 布局减少跨节点写事务；评估 2PC 延迟是否符合业务 SLA。跨节点写事务内的保存点语义见 [§1.5](#15-事务保存点savepoint语义与-2pc-边界)：本地保存点生效、远端写集按前缀丢弃（远端 `PREPARE` 发生在 `COMMIT` 阶段）。

### 3.2 单机模式集群语句受限

- **限制描述**：单机（未启用集群）部署下，集群语句如 `BALANCE` 明确报 `cluster not enabled`；region 分配、节点注册等仅在集群模式生效。
- **影响**：单机用户误用集群语句会收到明确报错；不会静默产生无效配置。
- **建议**：单机部署按单机文档使用；需要集群能力时用 `--cluster-addr` 启动并走 ADD NODE / ASSIGN REGION 流程。

### 3.3 复制模式与一致性语义

- **限制描述**：复制模式支持 async / semi-sync / sync（热配置切换）。semi-sync 在 ACK 超时/无从节点时降级为异步（记录 DegradedCount/LSN）；sync 模式无法满足 ACK（无在线从节点或超时）时拒绝伪同步（ErrNoSlave / ErrAckTimeout）。
- **影响**：全同步写入在从节点不可用时会失败，保证不产生"伪同步"假象；半同步降级期间一致性窗口扩大。
- **建议**：按 RPO 要求选择模式；监控 DegradedCount 与复制 LSN 滞后，及时恢复从节点。

### 3.4 HA 脑裂防护 quorum 开关（默认关闭）

- **限制描述**：HA 脑裂防护含 `ha.epochEnabled`（epoch fencing，v1.10.0 起默认开启）与 `ha.quorumEnabled`（多数派仲裁）两个开关。quorum 保持默认关闭：开启后升主需多数派节点确认，单节点/两节点部署无法满足多数派时将拒绝升主。
- **影响**：v1.10.0 单机/测试框架评估结论为直接全量开启会破坏单节点自动升主与两节点主从场景的主节点恢复语义（详见 docs/HA_BRAIN_SPLIT_DESIGN.md §5.5）；保持默认关闭不影响 epoch fencing 的防脑裂效果。v1.18.0 已在 VM 三节点完成 quorum 灰度全链路演练（D4.1 quorum 开启评估 / D4.2 宕机 failover / D4.3 脑裂 + epoch 拒绝 / D4.4 节点恢复回迁），结论维持 `ha.quorumEnabled=off`、`ha.epochEnabled=on`，不给全量开启建议。结构性原因：集群轨尚无一致性协议（无 raft 式日志复制与选举），写提交为归属节点本地提交 + 归属强制路由、副本补齐为异步推送，任何时刻可能存在"单副本可写窗口"与 RPO>0，与开关是否打开无关。详见 [D4.5 HA quorum 灰度报告](docs/D4_5_ha_quorum_gray_report.md) §7.1。
- **行为变化（Phase9）**：重启节点在 region 归属未经集群权威视图确认期间拒绝写入（把原"错误放行"改为"正确拒绝"），代价是秒级写不可用窗口。该门槛由 `ha_epoch_enabled`（默认 `on`）驱动、与 `ha_quorum_enabled` 无关，即默认配置下也生效；仅三种情形触发（重启后归属待权威确认 / 归属已被权威收回 / 曾获授权 `clusterOwnTerm>0`），单节点集群与从未接管 region 的普通节点恒放行，对既有单机与普通部署零回归。**集群部署的重启操作需按报告 §7「重启窗口」一行摘流。**
- **建议**：三节点及以上真实组网灰度验证 quorum 仲裁行为后再评估全量开启；单机/两节点部署维持默认关闭。升级路径：v1.19.0 M8（写路径强制路由 + 2PC 参与 + 持续复制）落地后，以三独立 IP 真机复跑 T3 真分区 fencing / NEW-1 陈旧主写拒绝 / NEW-2 重启矩阵三项，转 PASS 且 2PC 落地后再评估默认值改为 `on`。

### 3.5 跨 region 多区间读的远端往返（`remote_range_agg`，默认关闭）

- **限制描述**：v1.19.0 之前，跨 region 多区间读（多区间扫描 / 区间合并 / 分区裁剪后的合并扫描）按区间逐个发起 `QUERY_REQ`，跨节点往返次数随区间数线性增长。v1.19.0 新增 `remote_range_agg`（默认 `off`）：执行器按 `DecodeRegionKey` 归类后，把同一归属节点的多个远端区间合并为单次 `QUERY_MULTI`（服务端按 512 行 / 4MB 预算流式分块回传），本地区间保持一次合并扫描。
- **影响**：开关关闭时行为与 v1.18.0 完全一致（不发新帧，可随时灰度回退）；开启后跨节点读往返次数由「区间数」降为「归属节点数」，代价是单个远端请求数据量按节点合并——仍为分块流式回传，不改变内存峰值形态。当前粒度是「每归属节点一次往返」，未做跨节点请求并行聚合的额外优化；远端区间内的排序与 `Offset`/`Limit` 截窗仍由服务端按区间执行，跨区间全局行序由归属侧稳定排序保证（与旧路径逐行等价）。
- **建议**：跨 region 多区间读密集部署可按节点灰度开启并观察远端请求数指标（`incRemote` 按请求次数计）；单机 / 单 region 部署无收益（`clusterEnabled` 为假时直接走本地合并扫描，`beginTS` 置 0）。

## 4. 文档边界

- **限制描述**：docs/ 下设计文档（T1–T42、M 系列、E 系列）为中文开发记录，供实现追溯与深入阅读；产品手册提供中英文 11 章节面向用户文档。
- **影响**：英文用户深度查阅设计细节需依赖英文产品手册与代码注释；部分早期文档格式为中文技术笔记。
- **建议**：用户场景以 product_manual_en.md / product_manual_zh.md 为准；实现细节以 docs/ 设计文档与源码为准；文档站点（mkdocs）建立后统一导航。

## 5. 其他边界

- **鉴权兼容模式**：无 AUTH 握手时连接以默认 root 用户运行；旧客户端在 `AllowLegacy` 开启时回退受限 guest 模式，严格模式拒绝。建议生产环境配置认证并保持严格模式。
- **TLS 默认策略**：默认明文监听；TLS 经 `SetTLS` / 自签或 CA 证书装配后启用（T36 提供 gen_cert 生成工具），需明文/TLS 共存时配置 `--tls-allow-legacy`（AutoListener 同端口自动识别）；v1.11.0 起支持证书热轮换（`ReloadTLS`，AllowLegacy 模式运行中即时生效）。生产环境建议使用受信任 CA 证书并定期轮换。
- **备份格式**：BACKUP 产物为单文件逻辑备份（version + CRC32），跨版本恢复请先确认 DataFormatVersion 兼容（`openxdb version` 查看）。
- **告警通知通道**：告警引擎内置 logx 通知与 v1.11.0 新增的规则级 webhook 回调（触发/恢复异步 POST JSON 载荷）；钉钉/企业微信等原生渠道未接入，可经 webhook 网关转发。
- **测试规模**：全仓回归 694+ 测试覆盖主要路径，但非商用级验证范围；E5 真实用户验证覆盖 30+ 商业场景检查，生产使用仍需独立验收。
- **Windows 发行包运行时依赖（构建期强制）**：Windows 版 `openxdb.exe` 必须静态链接 MinGW 运行时——`scripts/build.ps1` 已固定把 `-static -static-libgcc -static-libstdc++` 追加进 `CGO_LDFLAGS`，并在构建后用 `objdump -p` 校验导入表，一旦仍含 `libwinpthread-1.dll`（或 `libgcc_s_*.dll`、`libstdc++-6.dll`）即报错退出、不出包。**影响**：若动态链接，exe 会导入 `libwinpthread-1.dll` 等 MinGW 私有 DLL；这类 DLL 不在 Windows 系统目录中，在未安装 MinGW 的机器上进程会在启动阶段直接静默退出、无任何报错输出（表现为"进程飞掉"，用户与应用日志均无痕迹可查）。**建议**：Windows 发行包一律经 `scripts/build.ps1` 构建，不要绕过其校验；手工或交叉构建 Windows 版时切勿省略上述三个链接参数，并自行用 `objdump -p bin\openxdb.exe`（查看 `DLL Name` 项）确认无 MinGW 运行时 DLL。

---

*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

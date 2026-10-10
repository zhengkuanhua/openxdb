---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_0488f390c46c11f197b3525400248c00
    ReservedCode1: h/G2iZjOiq3Hoa//j02XHk+3zeDYe/GEIYBeJEyTxB6/P2dOeMW2gCVgHkvTJOgjXU98dporM+RROajLI3w6VUBW87LNavzQOcanScHlAH0sd6pn+JxfVAdl+8j4qnsWhiiT8l7Zjb8R1CsQALiBvgOrRxW583iqVtbXLfqAezE6cVe0gCFHeGQuS+k=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_0488f390c46c11f197b3525400248c00
    ReservedCode2: h/G2iZjOiq3Hoa//j02XHk+3zeDYe/GEIYBeJEyTxB6/P2dOeMW2gCVgHkvTJOgjXU98dporM+RROajLI3w6VUBW87LNavzQOcanScHlAH0sd6pn+JxfVAdl+8j4qnsWhiiT8l7Zjb8R1CsQALiBvgOrRxW583iqVtbXLfqAezE6cVe0gCFHeGQuS+k=
---

# T19 — 客户端驱动（JDBC / Python）

实现记录：OpenXDB 客户端驱动功能簇闭环。本文档说明客户端驱动架构、线协议复用方式、JDBC / Python 驱动结构与错误码、事务语义与测试方式。

## 1. 概述

T19 为 OpenXDB 补充两类标准客户端驱动：

- **JDBC 驱动**（`drivers/jdbc`）：Java 8+，实现 `java.sql.Driver` / `Connection` / `Statement` / `PreparedStatement` / `ResultSet` 核心接口，通过 `jdbc:openxdb://host:port` URL 连接；
- **Python 驱动**（`drivers/python`）：PEP 249 风格 `openxdb` 包，提供 `connect()` / `Connection` / `Cursor` / 错误层级与协议封装。

两条驱动均**直接复用 `pkg/server` 的 TCP 线协议**（即 `cmd/openxdb` TCP server 的命令循环），不引入服务端任何新协议或鉴权握手。服务端无需改动即可被两种驱动驱动。

## 2. 客户端驱动架构

### 2.1 线协议（复用 pkg/server）

驱动与 `pkg/server` 之间是 **UTF-8 行协议**：每条命令一行、以 `\n` 结尾；服务端逐行回复。无握手、无鉴权，socket 建立后直接进入命令循环。回复形态：

| 回复形态 | 示例 | 语义 |
|---|---|---|
| 简单确认 | `OK` / `PONG` / `BYE` | `BEGIN`/`COMMIT`/`ROLLBACK`/`PING`/`QUIT` 等连接级命令 |
| 受影响行 | `(N rows affected)` | `CREATE`/`INSERT`/`UPDATE`/`DELETE`/`BACKUP`/`RESTORE` 等非查询语句 |
| 文本表 | 首行表头 + 分隔线 + 数据行 + 末行 `(N rows)` | `SELECT`/`SHOW`/`EXPLAIN` 等查询语句 |
| 错误 | `ERR <message>` | 任意失败，服务端统一以 `ERR ` 前缀返回 |

两条驱动共用同一套「帧划分 + 表格解析」规则，解析行为刻意保持一致（`drivers/jdbc` 的 `OpenXDBProtocol` 与 `drivers/python` 的 `protocol.py` 互为镜像）。

### 2.2 解析边界（必须知晓）

- **NULL 与空串**：线上空单元格同时表示 NULL 与空字符串；驱动统一把空单元格映射为 `NULL`（JDBC `getString` 返回 null / `wasNull()` 为 true；Python 返回 `None`）。
- **单元格含 `|`**：表格单元格内容可含 `|` 字符，按表头列数 N 在前 N-1 个 `" | "` 分隔处切分，最后一列保留剩余原文。
- **多行文本**：行协议不支持单元格内换行，`\n` / `\r` 出现在参数化字符串值中会被驱动端拒绝。

## 3. JDBC 驱动（drivers/jdbc）

### 3.1 模块与注册

- Maven 项目（`pom.xml`），Java 8+，JUnit 5 测试；
- 静态块 `DriverManager.registerDriver(new OpenXDBDriver())` + `META-INF/services/java.sql.Driver` 自动注册，`Class.forName("org.openxdb.jdbc.OpenXDBDriver")` 亦可。

### 3.2 类结构

| 类 | 职责 |
|---|---|
| `OpenXDBDriver` | 实现 `java.sql.Driver`；URL 前缀 `jdbc:openxdb://`（默认端口 7788），`parseHostPort` 解析 host:port；`user`/`password` 属性被接受但忽略（线上无鉴权） |
| `OpenXDBConnection` | 实现 `java.sql.Connection`；维护 TCP 会话与事务状态；`setAutoCommit(false)` 触发会话事务（首条语句前自动 `BEGIN`），`commit()`/`rollback()` 发送 `COMMIT`/`ROLLBACK`，`close()` 在事务中时先 `ROLLBACK` 再 `QUIT` |
| `OpenXDBStatement` | 实现 `java.sql.Statement`；每次执行发送一条命令并解析回复：表回复包装为 `ResultSet`（`executeQuery`/`execute` 返回 true），`(N rows affected)` 作为更新计数；批处理/命名游标/cancel 明确 `SQLFeatureNotSupportedException` |
| `OpenXDBPreparedStatement` | 继承 `OpenXDBStatement`；客户端侧 `?` 占位符替换（见 §5） |
| `OpenXDBResultSet` | 只进、只读 `ResultSet`；单元格文本解析，空单元格映射 NULL；`getString`/`getInt`/`getLong`/`getDouble`/`getBoolean`/`getBigDecimal`/`getBytes` 等；BLOB 线上为大写十六进制，按 hex 解码 |
| `OpenXDBProtocol` | 内部协议封装：socket 连接（5s 超时）、命令发送、回复解析（OK/PONG/BYE、affected、表、ERR） |

### 3.3 执行流

```
OpenXDBStatement.run(sql)
  → connection.ensureTxnStarted()   // autoCommit=false 且未 BEGIN 时发送 BEGIN
  → connection.protocol().command(sql)
  → 解析：TABLE → new OpenXDBResultSet；AFFECTED → updateCount
```

## 4. Python 驱动（drivers/python）

### 4.1 包结构

| 模块 | 职责 |
|---|---|
| `openxdb/__init__.py` | 模块级 `connect()`、`apilevel="2.0"`、`threadsafety=1`、`paramstyle="qmark"`、`__version__="0.10.0"`；导出错误类与 `DEFAULT_PORT` |
| `openxdb/connection.py` | `Connection`：TCP 会话；`cursor()` / `ping()` / `begin()` / `commit()` / `rollback()` / `close()`（`QUIT` 尽力发送）；`user`/`password` 接受并忽略 |
| `openxdb/cursor.py` | `Cursor`（PEP 249）：`execute`/`executemany`/`fetchone`/`fetchmany`/`fetchall`/迭代器；`description`/`rowcount`/`arraysize`；客户端 `?` 替换（§5） |
| `openxdb/protocol.py` | `OpenXDBProtocol`：socket 建连、`command()` 逐行读写、`_expect_ok`；`PING`→`PONG`、`BEGIN`/`COMMIT`/`ROLLBACK`→`OK`、`QUIT`→`BYE` |
| `openxdb/errors.py` | PEP 249 风格错误层级（§6） |

### 4.2 用法示例

```python
import openxdb

conn = openxdb.connect("127.0.0.1", 7788)
cur = conn.cursor()
cur.execute("SELECT id, name FROM users WHERE id > ?", (1,))
for row in cur.fetchall():
    print(row)
conn.close()
```

## 5. 参数化与客户端转义

服务端协议**没有 prepared statement / 二进制参数通道**，因此两驱动均在**客户端安全转义**：`?` 占位符被替换为安全 SQL 字面量后再发送。

| 参数类型 | 编码规则 |
|---|---|
| `bool` | `1` / `0` |
| `int` / `float` | 十进制字面量（`str` / `repr`） |
| `bytes`（BLOB） | 大写十六进制字符串，加单引号 |
| `str` | 单引号包裹，`'` 双写转义；**含 `\n` / `\r` 拒绝**（行协议限制） |
| `None` | **不支持**：协议无 NULL 字面量，抛参数错误 |
| 其他类型 | 抛不支持错误 |

边界必须写明：转义引擎保证字面量安全，但**不**支持 `LIKE` 通配符转义语义扩展、不支持 NULL 参数、不支持单元格内换行；`LIKE '%'` 等模式中的 `%`/`_` 由用户 SQL 自行控制。参数数量与占位符数量不符时抛 `ParameterError` / SQLException。

## 6. 错误码与事务语义

### 6.1 错误映射

线上只有一种错误形态：`ERR <message>`。两条驱动据此映射：

- **JDBC**：`ERR` 前缀 → `SQLException`（消息为去掉前缀的服务端错误文本）；网络失败/EOF → `SQLException("connection lost / closed by server ...")`；客户端状态误用（已关闭的连接/语句/结果集）→ `SQLException`。
- **Python**（`errors.py` 层级）：

| 异常 | 语义 |
|---|---|
| `OpenXDBError` | 所有驱动错误基类 |
| `InterfaceError` | 客户端接口/状态误用 |
| `ConnectionClosedError(InterfaceError)` | 对已关闭连接/游标操作 |
| `ProgrammingError` | 客户端 SQL/参数误用 |
| `ParameterError(ProgrammingError)` | 转义引擎非法参数 |
| `OperationalError` | 服务端 `ERR` 回复 / 网络层失败 |

服务端所有失败一律以 `OperationalError` 呈现；客户端使用错误以 `ProgrammingError` 系呈现。

### 6.2 事务语义

- 服务端为**会话级事务**：`BEGIN` / `COMMIT` / `ROLLBACK` 走 SQL 会话事务，事务状态挂在连接上。
- **JDBC**：`setAutoCommit(true)`（默认）每条语句隐式自动提交；`setAutoCommit(false)` 后首个语句前自动发 `BEGIN`，`commit()`/`rollback()` 显式结束；`autoCommit=true` 时调用 `commit()`/`rollback()` 抛异常；`close()` 时有未结束事务先 `ROLLBACK`。隔离级别仅接受 `READ_COMMITTED` / `READ_UNCOMMITTED`。
- **Python**：`Connection.begin()/commit()/rollback()` 直接透传协议命令；语句在显式事务外由服务端按隐式自动提交执行；`close()` 不做隐式回滚（尽力发 `QUIT`），测试夹具负责清理。
- 协议若在服务端不支持会话事务则明确不支持（当前服务端支持）。

## 7. 测试方式

两条驱动均为**端到端测试**：测试启动真实 `openxdb` server（临时 data-dir + 随机空闲端口），驱动直连真实服务端执行 SQL 断言。

| 驱动 | 测试入口 | 运行方式 | 覆盖 |
|---|---|---|---|
| JDBC | `drivers/jdbc/src/test/java/org/openxdb/jdbc/OpenXDBDriverTest.java` | `mvn test`（Maven，JUnit 5，7/7 通过） | 建连/PING、DDL/DML affected-rows、查询结果集、PreparedStatement 转义、事务 commit/rollback、错误映射、连接关闭语义 |
| Python | `drivers/python/tests/test_driver.py` + `tests/conftest.py` | `pytest`（session 级 fixture 起服务） | 连接/PING、auth 参数忽略、连接拒绝、DDL/DML、参数化、事务、错误映射、fetch 语义 |

测试要点：

- 服务端二进制来自 `drivers/.bin/openxdb.exe`（JDBC 测试通过 `ProcessBuilder` 定位）；`init --data-dir <临时目录>` 后 `start --data-dir <临时目录> --port <随机端口>`；
- 启动后轮询端口可连接即就绪；测试结束主动 `destroy`/`kill` 服务进程；
- 每条用例独立清理遗留表与事务（Python 侧 `clean` fixture）。

## 8. 边界与限制

- 无鉴权：`user`/`password` 属性仅兼容 API，线上不校验（后续鉴权需扩展握手）；
- 无 prepared statement / 二进制参数通道：参数化为客户端转义，NULL、含换行字符串、未绑定参数均拒绝；
- `ResultSet` 只进只读；无 `ResultSetMetaData`、命名游标、批处理、可滚动结果集、生成键；
- 行协议不支持单元格内换行；空单元格统一按 NULL 处理（空串与 NULL 不可区分）；
- 隔离级别仅 `READ_COMMITTED` / `READ_UNCOMMITTED` 名义支持，语义由服务端快照隔离实现。

## 9. 相关文件

- 驱动源码：`drivers/jdbc/`（`src/main/java/org/openxdb/jdbc/*`）、`drivers/python/openxdb/`
- 驱动 README：`drivers/jdbc/README.md`、`drivers/python/README.md`
- 服务端协议实现：`pkg/server`、`cmd/openxdb`
*（内容由AI生成，仅供参考）*

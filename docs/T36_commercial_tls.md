---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_bbbba1f8c86c11f196f9525400248c00
    ReservedCode1: 626l6o6PJvMi5/S7alC/wak2DoO0IxWW55X6s87FqbJeEDlzkrahTNwTNizZINwl5qvZwLhV+3za3+39rYKRKMc4Y6e9k98M/nrT7gM9UND+o38i+Or+Dz97SVM9JYdJpSFuHd88S/FB8zO9BapicxBw7Lg1w7pjm9EHfQgHHjGJPAtWrV6X+WQI7Vc=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_bbbba1f8c86c11f196f9525400248c00
    ReservedCode2: 626l6o6PJvMi5/S7alC/wak2DoO0IxWW55X6s87FqbJeEDlzkrahTNwTNizZINwl5qvZwLhV+3za3+39rYKRKMc4Y6e9k98M/nrT7gM9UND+o38i+Or+Dz97SVM9JYdJpSFuHd88S/FB8zO9BapicxBw7Lg1w7pjm9EHfQgHHjGJPAtWrV6X+WQI7Vc=
---

# T36 商用档：连接层 TLS 加密与证书管理

- 版本：v0.25.0-alpha（T36）
- 状态：已实现
- 关联：P0 TLS 既有基础（--tls-cert/--tls-key 监听层加密）、M11 连接鉴权（AUTH）、T19 客户端驱动（JDBC/Python）

## 1. 背景与目标

v1.0 商用档第一项：连接层 TLS 合规。目标：

1. 服务端证书/私钥配置：启动参数（--tls-cert/--tls-key，既有）与 SET PERSIST 持久化配置（restart-effective）。
2. TLS 握手流程与现有 AUTH 的配合：传输层加密先行，应用层行协议（含 AUTH）在 TLS 通道之上原样工作。
3. 纯文本降级策略（AllowLegacy 类似语义）：默认纯 TLS（安全默认，明文握手失败即断开）；显式开启 --tls-allow-legacy 时同一端口自动探测明文连接并以 legacy 会话标记。
4. 客户端驱动 TLS 连接支持：Go 客户端、JDBC、Python 驱动均可 TLS 连接，支持证书校验（verify-ca / verify-full）。
5. 证书校验与自签名证书生成工具：openxdb gen-cert 子命令生成自签名证书；启动时校验有效期。

## 2. 现状盘点（T36 前既有基础）

- pkg/server/server.go：Server.tlsConfig 字段 + SetTLS；ServeTCP 在 tlsConfig 非 nil 时使用 tls.Listen，nil 保持明文（默认兼容）。
- pkg/server/client.go：Dial / DialTimeout（tlsCfg nil=明文，非 nil=TLS）；InsecureTLSConfig（InsecureSkipVerify，内网/自签名测试）。
- cmd/openxdb/main.go：loadTLSConfig 解析 --tls-cert/--tls-key（必须同时提供）；主服务与 --replica-port 复制端口复用同一 TLS 配置；replica 子命令 --replica-tls（Insecure）。
- pkg/sql/config.go：M16-1 配置注册表（configSpecs）+ persisted.json 持久化（LoadPersistedConfig / SET PERSIST）。
- 测试：pkg/server/server_tls_test.go（TLS 握手 + 明文往返）、cmd/openxdb/main_tls_test.go（loadTLSConfig 参数解析）。
- 缺口（T36 补齐）：无证书生成工具（README 提到的 gen_cert.go 已不存在）；无有效期校验；无明文/TLS 同端口并存；驱动侧（JDBC/Python）无 TLS；无 SET PERSIST TLS 配置；客户端仅 Insecure 模式，无 CA 校验模式。

## 3. 架构设计

### 3.1 分层模型

```
客户端                         服务端
-----                         -----
TCP connect ───────────────▶ tls.Listen / AutoTLS 监听
TLS handshake（可选，先于一切）─▶ tls.Config 握手；失败即断开（默认）
行协议命令（PING/AUTH/SQL…）──▶ handleLocked（明文与 TLS 会话共用）
```

- TLS 是传输层信封：握手完成后连接即加密通道，应用层行协议（含 AUTH <user> <password>）零改动。
- AUTH 配合：TLS 只解决"传输加密"，身份鉴权仍由 M11 AUTH 握手负责；两者正交可组合（TLS + AUTH、TLS 无 AUTH、明文 + AUTH、明文无 AUTH 均可）。
- 会话安全标记：session.secure（TLS 连接=true，明文=false），供后续审计/权限增强（本期仅内部记录，不改变权限语义）。

### 3.2 服务端监听：纯 TLS 与 AutoTLS（明文降级）

| 模式 | 触发 | 行为 |
|---|---|---|
| 纯 TLS（默认，安全） | 配置证书，未开 legacy | 仅接受 TLS 握手；明文客户端握手失败断开 |
| AutoTLS（AllowLegacy 语义） | 配置证书 + --tls-allow-legacy / SET PERSIST tls_allow_legacy=on | 同一端口 peek 首字节：TLS ClientHello（0x16）走 TLS 握手；否则按明文会话处理并标记 secure=false（legacy） |
| 纯明文（向后兼容） | 未配置证书 | 原行为，secure=false |

- 实现：pkg/server 新增 AutoListener：Accept 后缓冲读取首字节判断 0x16（TLS 记录层 ContentType=handshake）→ tls.Server 包装；否则返回原始明文连接。
- 安全默认：默认纯 TLS，明文降级必须显式开启（与 AUTH AllowLegacy 一致的反默认安全取向）。
- 行协议首命令均为 ASCII 字母（PING/AUTH/HELP/SQL…），首字节不可能为 0x16，探测无歧义。

### 3.3 证书加载与校验

- loadTLSConfig（cmd/openxdb）增强：
  1. 仅提供 --tls-cert 或 --tls-key 之一 → 报错（既有）。
  2. tls.LoadX509KeyPair 加载并校验证书/私钥匹配（既有）。
  3. 新增：解析证书链首张证书，校验当前时间 ∈ [NotBefore, NotAfter]；过期或未生效 → 启动报错 "TLS certificate expired/not yet valid"，拒绝启动（商用合规：过期证书不得静默运行）。
- 证书文件读取失败/格式错误 → 启动报错（既有行为）。

### 3.4 SET PERSIST 持久化配置（restart-effective）

新增三个配置键（pkg/sql/config.go configSpecs 注册表）：

| 键 | 值 | 语义 |
|---|---|---|
| tls_cert | PEM 证书文件路径 | TLS 证书（重启生效） |
| tls_key | PEM 私钥文件路径 | TLS 私钥（重启生效） |
| tls_allow_legacy | on/off/1/0 | 同端口明文降级开关（重启生效） |

- 仅支持 SET PERSIST（TLS 监听属启动装配，无法热更新）；SET（非 PERSIST）返回明确报错："tls_* 为启动级配置，仅支持 SET PERSIST（重启生效）"。
- SHOW CONFIG 正常展示（来源 persist/default）。
- 启动装配优先级：命令行 --tls-cert/--tls-key/--tls-allow-legacy > 持久化 tls_cert/tls_key/tls_allow_legacy > 默认（无 TLS）。
- 实现：sql.Executor 新增 PersistedTLSConfig() 返回持久化意图；cmdStart 在 loadTLSConfig 之后、db.Open 之后读取持久化意图合并（命令行优先）。

### 3.5 证书生成工具：openxdb gen-cert

新增子命令（cmd/openxdb/gen_cert.go，纯 Go 标准库 crypto/x509/ecdsa，无第三方依赖）：

```
openxdb gen-cert --cert cert.pem --key key.pem [--days 365] [--cn localhost] [--out-dir]
```

- 生成 ECDSA P-256 自签名证书：Subject CN=--cn（默认 localhost），SAN=DNS:localhost, IP:127.0.0.1（商用客户端主机名校验可用）。
- --days 默认 365；有效期写入 NotBefore=now-1h（容忍时钟偏差）、NotAfter=now+days。
- 私钥 PKCS#8 PEM；证书 X.509 PEM。
- 已存在文件报错不覆盖（安全）；--force 覆盖。

### 3.6 客户端/驱动 TLS

#### Go 客户端（pkg/server/client.go）

- 既有：Dial/DialTimeout + InsecureTLSConfig。
- 新增 VerifyTLSConfig(caPath, serverName)：加载 CA PEM 构建 RootCAs，ServerName=serverName，校验链与主机名（verify-full 语义，商用默认）。
- 新增 CAFileTLSConfig(caPath)：仅校验链不校验主机名（verify-ca 语义，内网 IP 证书场景）。
- CLI replica：新增 --replica-tls-ca <path>（verify-full，配合 --master 主机名）；保留 --replica-tls（Insecure）向后兼容。

#### Python 驱动（drivers/python）

Connection 新增参数（protocol.py 用 ssl.create_default_context 包装 socket）：

| 参数 | 语义 |
|---|---|
| ssl=None/False | 明文（默认，向后兼容） |
| ssl=True | TLS 不校验（InsecureSkipVerify，内网/自签名快速联调） |
| ssl_ca=<path> | TLS 并校验 CA 链 + 主机名（verify-full，商用默认） |
| ssl_cert / ssl_key | 可选客户端证书（双向 TLS） |
| ssl_check_hostname=False | 配合 ssl_ca 时仅校验 CA 不校验主机名（verify-ca） |

#### JDBC 驱动（drivers/jdbc）

URL 形式：`jdbc:openxdb://host:port?ssl=true&sslMode=verify-full&trustStore=<pem>&hostnameInCertificate=<name>`

| 参数 | 默认 | 语义 |
|---|---|---|
| ssl | false | true 启用 TLS 连接 |
| sslMode | disable | disable / verify-ca（信任 trustStore 证书链）/ verify-full（链 + 主机名） |
| trustStore | （空） | CA PEM 文件路径（verify-ca/verify-full 必填） |
| hostnameInCertificate | 连接 host | 主机名校验用名 |

- ssl=true 且 sslMode=disable → 不校验（Insecure，等价 Go Insecure）。
- sslMode=verify-* 但无 trustStore → SQLException 报错。
- 实现：OpenXDBProtocol 构造改为接受 SslConfig；verify-full 用 SSLSocket.setEndpointIdentificationAlgorithm("HTTPS") 做主机名校验；verify-ca 只信任 trustStore 链。

## 4. 文件改动清单

| 文件 | 改动 |
|---|---|
| docs/T36_commercial_tls.md | 本设计文档（新增） |
| cmd/openxdb/main.go | 子命令分发加 gen-cert；loadTLSConfig 有效期校验；cmdStart 合并持久化 TLS 意图；usage 更新 |
| cmd/openxdb/gen_cert.go | gen-cert 子命令（新增） |
| pkg/server/listener.go | AutoListener 明文/TLS 自动探测（新增） |
| pkg/server/server.go | ServeTCP 支持 AutoListener；会话 secure 标记 |
| pkg/server/client.go | VerifyTLSConfig / CAFileTLSConfig |
| pkg/sql/config.go | tls_cert/tls_key/tls_allow_legacy 注册 + PersistedTLSConfig |
| cmd/openxdb/main_tls_test.go | 有效期校验测试；gen-cert 测试 |
| pkg/server/server_tls_test.go | AutoTLS 并存、CA 校验、过期/主机名不匹配测试 |
| pkg/sql/config_test.go | tls 键 SET PERSIST/SHOW/restart 意图测试 |
| drivers/python/openxdb/protocol.py | ssl 包装 socket |
| drivers/python/openxdb/connection.py | ssl/ssl_ca/ssl_cert/ssl_key 参数 |
| drivers/python/tests/test_driver.py | TLS 连接测试 |
| drivers/jdbc/.../OpenXDBDriver.java | URL 参数解析（ssl/sslMode/trustStore） |
| drivers/jdbc/.../OpenXDBProtocol.java | SSLSocket 创建与校验 |
| drivers/jdbc/.../OpenXDBConnection.java | 透传 SSL 配置 |
| drivers/jdbc/.../OpenXDBDriverTest.java | TLS 连接测试 |
| README.md | Status 勾选 T36、Roadmap 链接、版本 v0.25.0-alpha |

## 5. 测试计划

- 服务端：TLS 握手往返（既有保留）；纯 TLS 端口明文客户端被拒；AutoTLS 同端口明文/TLS 并存且 secure 标记正确；客户端校验：CA 链成功、Insecure 自签成功、过期证书失败、主机名不匹配失败（verify-full）。
- 证书：gen-cert 生成证书可被 loadTLSConfig 加载；过期证书启动报错。
- 配置：SET PERSIST tls_cert/tls_key/tls_allow_legacy 落盘；SET（非 PERSIST）报错；PersistedTLSConfig 意图读取；命令行优先。
- 驱动：Python pytest 连 TLS 服务（verify-full 成功 / 自签失败 / Insecure 成功）；JDBC 驱动单测连 TLS 服务（mvn 环境运行，另行说明）。
- 全量：go test ./... -count=1 -p 1 串行（并行端口竞争规避）。

## 6. 安全与兼容

- 默认安全：配置证书后即纯 TLS，明文必须显式 --tls-allow-legacy；未配置证书保持明文（向后兼容旧部署）。
- 过期证书拒绝启动；证书/私钥不匹配拒绝启动。
- 明文降级会话标记 legacy（secure=false），与 AUTH AllowLegacy 语义对齐但不降低 AUTH 强制等级。
- 驱动默认明文（兼容）；TLS 均为可选开启，开启后默认校验（verify-full），不校验需显式选择。
*（内容由AI生成，仅供参考）*

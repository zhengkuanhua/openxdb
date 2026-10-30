---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_9bb4db5cd45d11f1a0da525400393706
    ReservedCode1: f5mDOXKEjPX1lpVLaAOXevzz0c0MlO2hqEkf2b6iL76G9jcUPu420e3UnGDmb+gpPSxBaQ/b9xcV+5JS82SvbwFWEFm8dPa+sie8iR36pd7qWmw0TGwDxopi7KSKP1zXg2W/koOfhPMFL6jJ8KQkIv5oqBLx/dS8/0dLHAcisWy6Sx1WFEkWDH0vS5A=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_9bb4db5cd45d11f1a0da525400393706
    ReservedCode2: f5mDOXKEjPX1lpVLaAOXevzz0c0MlO2hqEkf2b6iL76G9jcUPu420e3UnGDmb+gpPSxBaQ/b9xcV+5JS82SvbwFWEFm8dPa+sie8iR36pd7qWmw0TGwDxopi7KSKP1zXg2W/koOfhPMFL6jJ8KQkIv5oqBLx/dS8/0dLHAcisWy6Sx1WFEkWDH0vS5A=
---





# OpenXDB 单机安装部署文档（Linux 7.x）

> 说明：本文档适用于 Linux 7.x（CentOS/RHEL 7 系）。
> 适用版本：OpenXDB v1.17.0
> 适用系统：CentOS 7.x / RHEL 7.x（x86_64）
> 面向读者：有 Linux 基础的 DBA / 运维工程师
> 来源：本文所有命令、配置项与能力描述均取自仓库（gitee：zhengkh/openxdb，github：zhengkuanhua/openxdb）的 README、`docs/product_manual_zh.md`、`scripts/build.sh`、`scripts/deploy.sh`、`Dockerfile`、`cmd/openxdb/main.go`、`KNOWN_LIMITATIONS.md` 等真实内容；凡仓库未明确定义之处均标注"以仓库实际为准"。

---

## 目录

- [1. 概述](#1-概述)
- [2. 前置检查清单](#2-前置检查清单)
  - [2.1 CentOS 7 EOL 后 yum 源修复（前置必做）](#21-centos-7-eol-后-yum-源修复前置必做)
- [3. 获取源码与构建](#3-获取源码与构建)
  - [3.1 方式 A：使用仓库构建脚本 scripts/build.sh](#31-方式-a使用仓库构建脚本-scriptsbuildsh)
  - [3.2 方式 B：手动 CGO 构建（含 RocksDB 静态链接要点）](#32-方式-b手动-cgo-构建含-rocksdb-静态链接要点)
  - [3.3 方式 C：Docker 多阶段构建](#33-方式-cdocker-多阶段构建)
- [4. 初始化与启动](#4-初始化与启动)
  - [4.1 初始化数据目录](#41-初始化数据目录)
  - [4.2 配置文件说明](#42-配置文件说明)
  - [4.3 启动服务](#43-启动服务)
  - [4.4 一键部署脚本 scripts/deploy.sh](#44-一键部署脚本-scriptsdeploysh)
  - [4.5 优雅停机](#45-优雅停机)
  - [4.6 systemd 服务化建议](#46-systemd-服务化建议)
- [5. 安装验证](#5-安装验证)
- [6. 数据目录与日志说明](#6-数据目录与日志说明)
- [7. 备份恢复与升级](#7-备份恢复与升级)
- [8. 常见问题（FAQ）](#8-常见问题faq)
- [9. 边界与已知限制](#9-边界与已知限制)
- [10. RHEL/Rocky/AlmaLinux/Oracle Linux 9.x 单机部署](#10-rhelrockyalmalinuxoracle-linux-9x-单机部署)
- [11. 参考资料](#11-参考资料)

---

## 1. 概述

OpenXDB 是一款基于 RocksDB 的国产自研数据库，具备 SQL 子集、自研二进制协议（可选 `--mysql-port` 兼容监听，非 MySQL wire protocol 全兼容）、主从复制（M2）、分片（M3）、多节点集群（M4）、分布式 2PC 写（M5）、HA（M6）、分布式事务 TSO（M8）等能力。v1.17.0 为当前版本，数据目录兼容 v1.0.0–v1.16.0 直接打开。

本单机文档只覆盖**单机部署**：构建二进制、初始化数据目录、启动、验证、日常运维。分布式与主从复制分别见《openxdb_linux_distributed.md》《openxdb_linux_replication.md》（仓库 `docs/deployment/` 下 `linux_distributed.md`、`linux_replication.md`）。

### 1.1 产物形态

- 仓库源码编译产出**单一可执行文件** `openxdb`（`go build ./cmd/openxdb`），RocksDB 以静态库方式链接（Linux 下 `#cgo` LDFLAGS 为 `-lstdc++ -lpthread`，依赖系统 C++/线程运行库，非全静态）。
- 仓库**未检出** Linux 预编译二进制发布物；官方交付路径为源码构建（`scripts/build.sh`）或 Docker 镜像构建（`Dockerfile`）。如仓库后续提供预编译产物，以仓库实际为准。

### 1.2 术语约定

- 数据目录：`--data-dir` 指定的目录，`openxdb init` 初始化后包含 `data/`（RocksDB 存储）、`openxdb.conf`（**数据目录 db conf**，init 自动生成，见 §4.2）、`slow.log`（慢查询日志）、`backups/`（默认备份目录）等。
- 服务配置：`openxdb start --config <path>` 显式加载的 INI 风格配置文件（`[server]` / `[sql]` 段），与数据目录 db conf 是**两份相互独立**的 conf 文件，路径不得重合（详见 §4.2）。
- 端口：服务端口 `7788`（默认），复制端口、指标端口、MySQL 兼容端口见配置节。

---

## 2. 前置检查清单

| # | 检查项 | 要求 | 验证命令 | 说明 |
|---|--------|------|----------|------|
| 1 | 操作系统 | CentOS 7.x / RHEL 7.x（x86_64） | `cat /etc/redhat-release` | 本文面向 7 系；其他发行版步骤相同，依赖安装命令不同 |
| 2 | glibc | ≥ 2.17（CentOS/RHEL 7 默认满足） | `ldd --version` | Go 1.22+ 官方 linux/amd64 二进制与仓库产物运行基线 |
| 3 | 磁盘空间 | 数据目录所在分区建议 ≥ 20GB（视业务量） | `df -h` | RocksDB 存储 + binlog + 备份 |
| 4 | Go 工具链 | Go 1.22+（`go.mod` 声明 `go 1.22`） | `go version` | CentOS 7 无官方仓库包，需从 go.dev 下载 tarball 安装，见 §3.0 |
| 5 | C/C++ 编译工具链 | gcc/g++（编译 RocksDB 需支持 C++17/20，见 §3.0 说明） | `gcc --version` / `g++ --version` | CentOS 7 自带 gcc 4.8.5 过旧，建议 `devtoolset` 升级，见 FAQ |
| 6 | CMake | ≥ 3.20（RocksDB v11.8.1 构建要求，以 RocksDB 官方为准） | `cmake --version` | CentOS 7 自带 cmake 2.8.12 过旧，需 `cmake3` 或 pip 安装 |
| 7 | 构建辅助工具 | make / ninja / git / pkg-config | `make --version`、`git --version` | `build.sh` 需要 git（RocksDB 源码拉取） |
| 8 | 网络 | 可访问 github.com（RocksDB 源码）或内网镜像 | `curl -I https://github.com` | 也可手动放置 RocksDB v11.8.1 源码目录 |
| 9 | 防火墙/端口 | 开放服务端口（默认 7788）、指标端口（默认值见 §4.2 冲突说明） | `firewall-cmd --list-all` | 仅外网访问需要；本机验证可不开 |
| 10 | 时区/时钟 | NTP 同步（分布式/TSO 场景必需） | `timedatectl` | 单机部署也建议同步 |

> 前置检查原则：先满足 1–8 再开始构建；构建产物拷贝到目标机运行时，目标机只需满足 1、2、3、9、10（不需要 Go 与编译工具链）。

### 2.1 CentOS 7 EOL 后 yum 源修复（前置必做）

> 来源：2026-10-30 在 CentOS 7.x（EOL）服务器上从零编译部署 OpenXDB v1.17.0 的实测记录。以下以清华镜像为例，阿里云 `mirrors.aliyun.com/centos-vault` 同样可用。

CentOS 7 已 EOL：官方 `mirrorlist.centos.org` 已不可解析，`vault.centos.org` 对 `/centos/7.9.2009` 返回 HTTPS 403。**必须先把所有源切到国内 centos-vault 归档镜像**，否则后续 yum 安装 devtoolset / cmake3 / ninja 全部失败。

```bash
# 1) base/extras/updates 三段：注释 mirrorlist，baseurl 指向国内 vault 归档
sed -i -e 's|^mirrorlist=|#mirrorlist=|' \
       -e 's|^#baseurl=http://mirror.centos.org/centos/$releasever/os/$basearch/|baseurl=https://mirrors.tuna.tsinghua.edu.cn/centos-vault/7.9.2009/os/$basearch/|' \
       -e 's|^#baseurl=http://mirror.centos.org/centos/$releasever/extras/$basearch/|baseurl=https://mirrors.tuna.tsinghua.edu.cn/centos-vault/7.9.2009/extras/$basearch/|' \
       -e 's|^#baseurl=http://mirror.centos.org/centos/$releasever/updates/$basearch/|baseurl=https://mirrors.tuna.tsinghua.edu.cn/centos-vault/7.9.2009/updates/$basearch/|' \
       /etc/yum.repos.d/CentOS-Base.repo

# 2) SCL rh 源（devtoolset 所在）：/etc/yum.repos.d/CentOS-SCLo-scl-rh.repo
sed -i 's|^#baseurl=http://mirror.centos.org/centos/7/sclo/$basearch/rh/|baseurl=https://mirrors.tuna.tsinghua.edu.cn/centos-vault/7.9.2009/sclo/$basearch/rh/|; s|^mirrorlist=|#mirrorlist=|' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo

# 3) SCL sclo 源：/etc/yum.repos.d/CentOS-SCLo-scl.repo
#    注意！此文件的注释 baseurl 写法是 '# baseurl='（# 后带空格），sed 要匹配 '# baseurl='
sed -i 's|^# baseurl=http://mirror.centos.org/centos/7/sclo/$basearch/sclo/|baseurl=https://mirrors.tuna.tsinghua.edu.cn/centos-vault/7.9.2009/sclo/$basearch/sclo/|; s|^mirrorlist=|#mirrorlist=|' /etc/yum.repos.d/CentOS-SCLo-scl.repo

# 4) EPEL 源（cmake3/ninja-build 所在）：/etc/yum.repos.d/epel.repo 用 metalink，EOL 后失效
sed -i '/^#\?metalink=/a baseurl=https://archives.fedoraproject.org/pub/archive/epel/7/$basearch/' /etc/yum.repos.d/epel.repo
sed -i 's|^metalink=|#metalink=|' /etc/yum.repos.d/epel.repo
# 备选清华：baseurl 前缀换 https://mirrors.tuna.tsinghua.edu.cn/epel-archive/7/$basearch/

# 5) 重建缓存
yum clean all && yum makecache
```

**实测要点**：
- EOL 后最常见表象是 yum 报 403 / Could not resolve host；官方 vault 直连也会 403，切国内归档镜像即可。
- SCL sclo 源的注释 baseurl 写法是 `# baseurl=`（井号后带空格），与常规 `#baseurl=` 不同，sed 模式必须匹配带空格的写法，否则替换不生效（rh 源则为常规写法）。
- 修复后验证：`yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++` 与 `yum install -y cmake3 ninja-build` 可正常安装。

---

## 3. 获取源码与构建

### 3.0 安装 Go（CentOS 7 无系统包，推荐官方 tarball）

```bash
# 以 Go 1.22+（或仓库 Dockerfile 使用的 1.27.1）为例
wget https://go.dev/dl/go1.22.12.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.22.12.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
go version
```

> CentOS 7 自带的 gcc 4.8.5 只支持到 C++11，**无法**编译 RocksDB v11.8.1（仓库 Dockerfile 使用 `CMAKE_CXX_STANDARD=20`，RocksDB 11.x 需 C++17+ 编译器）。建议安装 devtoolset（需先完成 §2.1 的 yum 源修复）：
>
> ```bash
> sudo yum install -y centos-release-scl
> sudo yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++ cmake3 ninja-build
> scl enable devtoolset-11 bash   # 或 source /opt/rh/devtoolset-11/enable
> gcc --version
> ```
>
> 实测要点：
> - `scl enable devtoolset-11 bash` 只在当前会话生效；要持久化，把 `source /opt/rh/devtoolset-11/enable` 追加进 `~/.bashrc`。
> - 系统自带 `cmake` 为 2.8.12.2（不支持 `-S/-B` 与 `--build -j` 语法），**必须使用 `cmake3` 命令**（EPEL 安装，3.17.x）；下文所有 `cmake` 命令在 CentOS 7 上均需换成 `cmake3`。
> - 验证：`gcc --version` 显示 11.2.1；`cmake3 --version` 显示 3.17.5；`ninja --version` 显示 1.10.x。
> - 具体编译器/CMake 版本要求以 RocksDB v11.8.1 官方构建文档为准（本仓库 Dockerfile 的构建参数见 §3.3）。

获取仓库源码：

```bash
git clone https://gitee.com/zhengkh/openxdb.git   # 或 https://github.com/zhengkuanhua/openxdb.git
cd openxdb
git checkout <v1.17.0 对应 tag 或 commit>          # 以仓库实际 tag 为准
```

### 3.1 方式 A：使用仓库构建脚本 `scripts/build.sh`

仓库提供可复现构建脚本（Linux/macOS）：

```bash
# 自动探测 RocksDB：优先 pkg-config，其次环境变量，最后报错提示传入参数
./scripts/build.sh
# 或显式指定 RocksDB 源码与静态库目录，并跑测试
./scripts/build.sh --rocks-src /path/to/rocksdb-11.8.1 --rocks-build /path/to/librocksdb-dir --test
# 仅设置 CGO 环境并打印，供手动 go build
./scripts/build.sh --skip-all-build
```

脚本行为（取自脚本源码）：

- 参数：`--rocks-src`（RocksDB 源码目录，需存在 `include/rocksdb/*.h`）、`--rocks-build`（含 `librocksdb*.a` 的静态库目录）、`--out-dir`、`--test`（构建后跑 `go test ./...`）、`--skip-all-build`。
- 未显式传参时：读取环境变量 `ROCKSDB_SRC` / `ROCKSDB_BUILD`；仍无则尝试 `pkg-config --exists rocksdb`。
- 命中后设置 `CGO_ENABLED=1`、`CGO_CFLAGS=-I<src>/include`、`CGO_CXXFLAGS=-I<src>/include`、`CGO_LDFLAGS=-L<build> -lrocksdb`，执行构建。

### 3.2 方式 B：手动 CGO 构建（含 RocksDB 静态链接要点）

**第一步：编译 RocksDB v11.8.1 静态库**

```bash
# 源码获取：GitHub 直连可能 TCP connection reset，可用 gitee 官方镜像（备选：ghproxy 等代理或下载 release 源码包解压）
git clone --depth 1 --branch v11.8.1 https://gitee.com/mirrors/rocksdb.git /tmp/rocksdb-src
# CentOS 7 系统自带 cmake 2.8.12 不支持 -S/-B，必须用 cmake3（3.17.5）
cmake3 -S /tmp/rocksdb-src -B /tmp/rocksdb-build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_STANDARD=20 \
    -DCMAKE_CXX_FLAGS="-Wno-error=maybe-uninitialized" \
    -DROCKSDB_BUILD_SHARED=OFF \
    -DWITH_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF -DWITH_JNI=OFF \
    -DWITH_GFLAGS=OFF -DWITH_ZLIB=OFF -DWITH_LZ4=OFF -DWITH_SNAPPY=OFF -DWITH_ZSTD=OFF -DWITH_BZ2=OFF
cmake3 --build /tmp/rocksdb-build -j $(nproc)
```

> 以上参数与仓库 `Dockerfile` 完全一致：关闭共享库与全部第三方压缩库，产出 `librocksdb.a` 静态库，产物无三方压缩库依赖。

**第二步：构建 openxdb**

```bash
export CGO_ENABLED=1
export CGO_CFLAGS="-I/tmp/rocksdb-src/include"
export CGO_CXXFLAGS="-I/tmp/rocksdb-src/include"
export CGO_LDFLAGS="-L/tmp/rocksdb-build -lrocksdb"
go build -o /usr/local/bin/openxdb ./cmd/openxdb
# 全仓编译核验
go build ./...
```

**RocksDB CGO 依赖与静态链接要点（来自仓库）**：

- 头文件必须指向 RocksDB 源码 `include/`；链接必须指向含 `librocksdb.a` 的目录，并 `-lrocksdb`。
- 无需手动追加 `-lstdc++` / `-lpthread`：仓库 Linux `#cgo` LDFLAGS 已包含（`-lstdc++ -lpthread`）。RocksDB 按仓库 Dockerfile 参数关闭了三方压缩库（`-DWITH_ZLIB=OFF` 等），也勿自行追加 `-lz` 等库，避免重复/错误链接。
- 全仓测试命令：`go test ./... -count=1 -p 1`（串行避免端口竞争，仓库测试约定）。

### 3.3 方式 C：Docker 多阶段构建

仓库 `Dockerfile` 提供官方镜像构建（golang:1.27.1-bookworm 构建阶段 + Debian slim 运行阶段）：

```bash
docker build -t openxdb:v1.17.0 .
# 运行示例（挂载数据目录）
docker run -d --name openxdb \
  -p 7788:7788 \
  -v /data/openxdb:/data \
  openxdb:v1.17.0 \
  openxdb init --data-dir /data && openxdb start --data-dir /data --port 7788
```

> Dockerfile 构建参数（Go 版本 `golang:1.27.1-bookworm`、RocksDB 静态参数）以仓库实际为准；运行阶段为 Debian slim，与 CentOS 7 场景可类比参考。

---

## 4. 初始化与启动

### 4.1 初始化数据目录

```bash
openxdb init --data-dir /data/openxdb
```

`init` 创建数据目录骨架（`data/` 存储子目录、`openxdb.conf` 数据目录 db conf 等）。数据目录格式与 v1.0.0–v1.16.0 兼容，可直接打开旧版本数据。

**init 必须先行（实测确认）**：`openxdb start` / `repl` 等子命令直接调用 `db.Open`，**不会自动 Init**；未初始化直接启动会报 `db: data directory not initialized`。任何节点首次部署都必须先执行一次 `openxdb init --data-dir <dir>`（对已初始化目录重复执行报 `data directory already initialized`，不会覆盖既有配置）。

### 4.2 配置文件说明

**双 conf 架构：OpenXDB 存在两份相互独立的 conf 文件，职责不同、路径不得重合（基于仓库真实代码 `cmd/openxdb/main.go` 的 cmdStart/loadStartConfig/cmdInit、`pkg/db/db.go` 的 Init/Open、`pkg/configfile`、`docs/openxdb.conf.example` 核实）**：

**A. 数据目录 db conf：`<data_dir>/openxdb.conf`**
- 由 `openxdb init --data-dir <dir>` 自动生成（`db.Init`），内容为简单键值（**无** `[server]` / `[sql]` 段），仓库样本见 `quickstart-data/openxdb.conf`：

```
# OpenXDB data directory config
engine = rocksdb
node_id = node-5135e5c1
created_at = 2026-10-30T09:43:32+08:00
```

- 作用是标识存储引擎与节点：`db.Open` 打开数据目录时读取该校验文件，内容必须包含 `engine = rocksdb` 字符串，否则报 `unsupported engine`；文件缺失则报 `data directory not initialized`（`pkg/db/db.go` Init/Open）。
- 它不是服务监听配置，不控制端口，也不能当作 `--config` 服务配置加载。

**B. 服务配置：`openxdb start --config <path>` 显式加载的 INI 文件**
- `main.go loadStartConfig` 仅在提供 `--config` 时解析配置文件；**不会自动读取数据目录下的 openxdb.conf**。
- INI 风格，仅接受 `[server]` / `[sql]` 两个段（未知段/未知键/非法值一律拒绝，不静默忽略；可用 `openxdb config-check --config <path>` 预检）：
  - `[server]` 合法键为白名单 `serverKeys`：`data_dir` / `port` / `replica_port` / `metrics_port` / `mysql_port` / `shutdown_timeout` / `cluster_addr` / `log_level` / `alert_enabled` / `alert_file` / `alert_webhook` / `alert_keep`。**`engine` 不是合法键**，写入即被 configfile 校验拒绝（报 `[server] unknown key "engine"`）。
  - `[sql]` 段与 `SET` / `SET PERSIST` 同键（`query_cache` / `slow_query_threshold` / `backup_*` / `log_level` / `alert_*` 等），启动时逐个装配为内存级 SET。
- 参考样本：仓库 `docs/openxdb.conf.example`（其中 `metrics_port = 0` 表示关闭，文档端口布局见下）。

合法最小示例（已剔除不在白名单的键；**保存到数据目录之外的独立路径**，如 `/etc/openxdb/openxdb.conf`）：

```ini
[server]
data_dir = /data/openxdb
port = 7788                 ; 服务端口，默认 7788
replica_port = 7789         ; 主从复制监听端口（>0 时启用复制；默认 0 不启用，常用 7789）
metrics_port = 7790         ; Prometheus 指标端口（默认 7789，0 禁用；与 replica_port=7789 冲突时让位 7790，见下方说明）
mysql_port = 3307           ; 可选：MySQL wire 兼容监听端口（默认 0 关闭；v1.1.0+ 部分兼容，非全兼容）
cluster_addr = 10.0.0.1:7791 ; 可选：集群节点链路地址（M4 集群模式，见分布式文档）
log_level = info
shutdown_timeout = 15       ; 优雅停机等待秒数（默认 30，以 main.go 实际语义为准）

[sql]
query_cache = on
slow_query_threshold = 100  ; 慢查询阈值 ms，默认 100
backup_dir = /data/openxdb/backups
```

> 注意：`node_id`、`tls_cert`、`tls_key`、`tso_node` 均**不在** `[server]` 白名单，写入会被 `config-check` / `start` 拒绝（以 main.go `serverKeys` 与 cmdStart 实际装配为准）。节点 ID 由 `init` 自动写入数据目录 db conf；TLS 走命令行 `--tls-cert` / `--tls-key` 或 `SET PERSIST tls_cert/tls_key`；中心化 TSO 走命令行 `--tso-node`。

**两份 conf 不得重合（实测踩坑）**：
- 服务配置若直接写到 `<data_dir>/openxdb.conf`（覆盖 init 生成的数据目录 db conf），`db.Open` 找不到 `engine = rocksdb` 字符串 → 启动报 `unsupported engine in <data_dir>/openxdb.conf`。
- 反之，在服务配置 `[server]` 段补 `engine = rocksdb` 行 → configfile 校验报 `[server] unknown key "engine"`。
- 结论：服务配置必须放在数据目录之外的独立路径，由 `--config` 显式加载。

- 配置加载：`openxdb start --config <服务配置路径>` 显式加载；**不会自动读取数据目录下的 openxdb.conf**。实测坑：只写好服务配置而不带 `--config` 启动时，文件内配置（含 `mysql_port`）全部不生效，必须显式 `--config` 或在命令行带对应参数。
- 命令行参数优先级高于配置文件（CLI flag 覆盖 `[server]` / `[sql]` 段同名键）。
- **端口冲突与修正方案（实测）**：`metrics_port` 仓库默认 7789（usage 口径，0 禁用）、`replica_port` 默认 0（不启用复制）。启用复制时 `replica_port` 常用 7789，此时若 `metrics_port` 保持默认 7789，两个监听端口会冲突（先启动者占用，另一监听 bind 失败）。**推荐显式让 metrics_port 让位**，形成如下端口布局：
  - `port = 7788`（服务端口）
  - `replica_port = 7789`（复制监听，>0 启用时）
  - `metrics_port = 7790`（指标端口，让位后）
  - `mysql_port = 3307`（MySQL wire 兼容端口，可选）
  - 修改示例：`sed -i 's|^metrics_port = 7789|metrics_port = 7790|' <服务配置路径>`
  - 实际启动时 metrics 端口在未显式配置时为 0（不监听），以仓库 main.go 实际行为为准。
- 在线热更新：`SET` / `SET PERSIST <key>`（持久化落盘到 `<data_dir>/persisted.json`，重启生效），如 `SET PERSIST slow_query_threshold = 50`。

### 4.3 启动服务

先执行 §4.1 的 `init`，再启动。两种方式任选：

```bash
# 方式一：--config 加载服务配置（data_dir 从 [server] 段读取）
openxdb start --config /etc/openxdb/openxdb.conf

# 方式二：命令行直接传参（优先级高于配置文件同名键）
openxdb start --data-dir /data/openxdb --port 7788
```

常用启动参数（`openxdb start` usage）：

| 参数 | 含义 | 默认 |
|------|------|------|
| `--data-dir` | 数据目录（必填） | - |
| `--port` | 服务端口 | 7788 |
| `--replica-port` | 复制监听端口，>0 启用主写复制（见复制文档） | 0（不启用） |
| `--cluster-addr` | 集群节点链路地址，非空启用 M4 集群（见分布式文档） | 空 |
| `--metrics-port` | Prometheus /metrics 端口（默认 7789，0 禁用） | 7789 |
| `--mysql-port` | MySQL wire 兼容监听端口（0 关闭） | 0 |
| `--tls-cert` / `--tls-key` | 服务 TLS 证书/私钥（同时加密复制端口） | 空 |
| `--tls-allow-legacy` | 允许兼容旧客户端 | 关 |
| `--auth` / `--auth-allow-legacy` / `--root-password` | 认证开关与 root 密码 | 以仓库实际为准 |
| `--backup-interval` / `--backup-keep` / `--backup-dir` / `--backup-remote-url` | 定时备份调度（见 §7） | 以仓库实际为准 |
| `--replication-mode` / `--replication-ack` / `--replication-timeout` | 复制模式相关（见复制文档） | async / 1 / 10000 |
| `--log-level` / `--alert-*` | 日志级别与告警配置 | info |
| `--tso-node` | 远程 TSO 发号节点（M8） | 空 |

**实测要点（CentOS 7, v1.17.0）**：
- `--mysql-port` 需显式带（或 `--config` 加载 conf 中的 `mysql_port`），否则 MySQL 兼容端口不会监听（默认 0 关闭）。
- 后台启动参考：

```bash
mkdir -p /data/openxdb /data/openxdb/backups
nohup openxdb start --data-dir /data/openxdb --port 7788 --mysql-port 3307 > /tmp/openxdb.log 2>&1 &
```

- 启动日志示例（正常启动标志）：

```
slow query log: /data/openxdb/slow.log (threshold 100ms, use SHOW SLOWQUERIES)
backup scheduler: disabled (use --backup-interval <sec> / SET PERSIST backup_interval)
alert rules engine: started (use SET alert_rules_enabled=on, SHOW ALERT RULES)
OpenXDB v1.17.0 serving on :7788 (data dir: /data/openxdb), Ctrl-C to stop.
```

### 4.4 一键部署脚本 `scripts/deploy.sh`

仓库提供 Linux 一键部署脚本（init → start(nohup) → 端口就绪等待 → doctor 自检 → 输出摘要）：

```bash
./scripts/deploy.sh -DataDir /data/openxdb [-Port 7788] [-Bin /usr/local/bin/openxdb] \
    [-ReplicaPort 7789] [-ClusterAddr 10.0.0.1:7791] [-NoStart]
```

| 参数 | 说明 |
|------|------|
| `-DataDir` | 必填，数据目录 |
| `-Port` | 服务端口，默认 7788 |
| `-Bin` | openxdb 二进制路径（不传则 PATH 查找） |
| `-ReplicaPort` | 传入则启用复制监听 |
| `-ClusterAddr` | 传入则启用集群节点链路 |
| `-NoStart` | 仅初始化不启动 |

### 4.5 优雅停机

- 前台运行：`Ctrl-C`（SIGINT）；后台运行：`kill -TERM <pid>`（SIGTERM）。
- 服务内命令：在 REPL/客户端执行 `SHUTDOWN`（触发优雅停机）。
- 优雅停机语义：停止接收新连接 → 排空进行中事务 → 关闭存储与 binlog；`shutdown_timeout` 控制等待窗口（默认 30s，以 main.go 实际语义为准）。

### 4.6 systemd 服务化建议

```ini
# /etc/systemd/system/openxdb.service
[Unit]
Description=OpenXDB Database
After=network.target

[Service]
Type=simple
User=openxdb
ExecStart=/usr/local/bin/openxdb start --data-dir /data/openxdb --port 7788
Restart=on-failure
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload && systemctl enable --now openxdb
```

---

## 5. 安装验证

按 `examples/quickstart/README.md` 的验证链执行（仅依赖 `openxdb` 二进制与 REPL stdin 管道）：

```bash
# 1) 版本
openxdb version

# 2) 启动后在线健康检查（退出码 0=通过 / 1=警告 / 2=错误）
openxdb doctor --data-dir /data/openxdb --addr 127.0.0.1:7788

# 3) 实时指标（--interval 两次采样计算 binlog LSN 增长；--json 输出结构化）
openxdb stats --addr 127.0.0.1:7788 --interval 5

# 4) 聚合状态快照（节点存活/角色、region 分布、存储用量、慢查询摘要、事务计数、连接数 QPS）
openxdb status --addr 127.0.0.1:7788 [--json]

# 5) Prometheus 指标（若启用 metrics 端口；main.go usage 默认 7789，0 禁用，按实际启动端口替换）
curl http://127.0.0.1:7789/metrics

# 6) 端口验证（服务端口 / MySQL 兼容端口 / 指标端口）
ss -lntp | grep -E '7788|3307|7789'
```

> **doctor WARN 预期（实测 v1.17.0）**：服务运行期间 `rocksdb` / `metadata` 两项会报 WARN——引擎被服务占用 LOCK、跳过本地 Open 检查，属预期；未启用复制时 `binlog` 报 WARN 也属预期。停服后再跑 `openxdb doctor --data-dir ...` 可完成本地 Open 校验。

功能验证（REPL 交互）：

```bash
openxdb repl --data-dir /data/openxdb
```

```sql
CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(64));
INSERT INTO t1 VALUES (1, 'hello');
SELECT * FROM t1;
EXPLAIN SELECT * FROM t1 WHERE id = 1;      -- 节点级耗时
SHOW SLOWQUERIES SUMMARY;                    -- 慢查询汇总
SHOW STATS;                                  -- 服务指标
SHOW CONFIG;                                 -- 配置项
```

升级前健康检查（升级发布前运行）：

```bash
openxdb preflight --data-dir /data/openxdb [--target-version v1.17.0] [--json]
```

---

## 6. 数据目录与日志说明

```
/data/openxdb/
├── openxdb.conf        # 数据目录 db conf（init 生成：engine=rocksdb/node_id/created_at，非服务配置，见 §4.2）
├── data/               # RocksDB 存储 + 元数据（m:tables / m:regions / m:nodes / m:2pc:*）
├── persisted.json      # SET PERSIST 在线配置持久化文件（存在时）
├── slow.log            # 慢查询日志（阈值默认 100ms，SET slow_query_threshold 调整，SET slow_query_log 开关）
└── backups/            # 定时备份默认目录
```

- 慢查询：默认落盘 `<数据目录>/slow.log`（由 db/cmd 装配开启），`SHOW SLOWQUERIES` 查询（新→旧）。
- 告警规则：`SET` / `SET PERSIST alert_rules*`，`SHOW ALERT RULES` / `SHOW ALERT STATUS` / `SHOW ALERTS`。
- 二进制协议客户端：可用 `openxdb repl`（本地）或 TCP 客户端连 `--port`；可选 `--mysql-port` 用 mysql 客户端连接（部分兼容）。
- **REPL 独占 data dir（实测坑）**：`openxdb repl --data-dir <dir>` 需独占该目录，服务运行期间执行会报 LOCK 占用（`Resource temporarily unavailable`）。RocksDB 为单写者模型，服务运行中不能对同一 data dir 二次打开；REPL 只能在停服后用于本地操作，运行期间请走 TCP 7788 或 MySQL 兼容端口连接。

---

## 7. 备份恢复与升级

**逻辑备份（一致性快照）**：

```sql
BACKUP TO '/data/backup/openxdb-20261030.bak';   -- 单文件，含 magic+version+meta+分段 CRC32+原子写
RESTORE FROM '/data/backup/openxdb-20261030.bak'; -- 幂等恢复（drop-then-recreate，单事务可回滚）
RESTORE FROM '...' TO LSN <n>;                    -- 恢复到指定 binlog LSN（PITR）
SHOW BACKUP STATUS;                               -- 备份任务状态
```

**远程推送/拉取**（HTTP 上传下载）：

```bash
openxdb backup push --file /data/backup/openxdb-20261030.bak --url http://backup-server/upload
openxdb backup pull --url http://backup-server/openxdb-20261030.bak --out /data/restore/openxdb.bak
```

**定时备份调度**：启动参数 `--backup-interval <sec>` / `--backup-keep <n>` / `--backup-dir <dir>`（默认 `<data>/backups`）/ `--backup-remote-url <url>`；`SET PERSIST backup_*` 持久化。

**升级**：v1.17.0 数据目录兼容 v1.0.0–v1.16.0 直接打开。升级流程建议：备份 → 替换二进制 → `preflight --data-dir ... --target-version <新版本>` → 启动 → `doctor` 验证。跨大版本注意事项以仓库 CHANGELOG / RELEASE_NOTES 为准。

---

## 8. 常见问题（FAQ）

| 问题 | 现象/原因 | 处理 |
|------|-----------|------|
| CentOS 7 自带 gcc 4.8.5 编译 RocksDB 失败 | RocksDB 11.x 需要 C++17/20 编译器 | 安装 devtoolset-11（或更高）并 `scl enable`，或改用 Dockerfile 构建 |
| cmake 版本过旧（2.8.12） | RocksDB 构建失败 | 安装 `cmake3`（EPEL）或以 `cmake3` 命令执行；版本要求以 RocksDB 官方为准 |
| 链接报错找不到 `-lrocksdb` | CGO_LDFLAGS 未指向含 `librocksdb.a` 的目录 | 确认 `-L` 目录下有 `librocksdb.a`；用 `find /tmp/rocksdb-build -name 'librocksdb*.a'` 定位 |
| 重复符号 / 链接错误 | 额外加了 `-lstdc++` / `-lz` | 删除这些追加项，仓库 `#cgo` 已处理 C++/线程库链接（`-lstdc++ -lpthread`） |
| `go build` 报 CGO 相关错误 | `CGO_ENABLED` 未置 1 或头文件路径错误 | 确认 `CGO_ENABLED=1`、`CGO_CFLAGS=-I<rocksdb>/include` |
| 端口被占用 | 默认 7788 被占用 | 换 `--port` 或停旧进程 |
| `cluster not enabled` | 执行了集群语句但未以 `--cluster-addr` 启动 | 单机部署此报错为正常（见 §9） |
| 慢查询没记录 | 阈值未达或 `slow_query_log` 关闭 | `SET slow_query_threshold = 100`、`SET slow_query_log = on` |
| mysql 客户端部分语句报 1045 not supported / unsupported statement | MySQL wire 兼容层为部分兼容（实测 v1.17.0） | `SELECT VERSION()`、`DESC`、`SHOW CREATE TABLE` 不支持；`SELECT 1`、`CREATE TABLE`、`INSERT`、`SELECT`、`SHOW TABLES/DATABASES/INDEX/SLOWQUERIES/NODES/VIEWS/USERS` 支持。完整 SQL/函数能力走 7788 自研二进制协议（Python 驱动 `openxdb.connect` 或内置 REPL） |
| `openxdb doctor` 报 rocksdb/metadata WARN | 服务运行中引擎被占用 LOCK，跳过本地 Open 检查 | 预期行为（见 §5）；停服后再跑 doctor 可验证本地 Open |
| REPL 启动报 LOCK / Resource temporarily unavailable | REPL 需独占 data dir，服务运行中已被占用 | 先停服再开 REPL；运行期间改用 TCP 7788 / MySQL 兼容端口连接 |
| 启动报 `data directory not initialized` | 未先 `openxdb init` 直接 `start`（cmdStart 直接 db.Open，不会自动 Init） | 先执行 `openxdb init --data-dir <dir>` 再启动（见 §4.1） |
| 启动报 `unsupported engine` | `<data_dir>/openxdb.conf` 被服务配置覆盖/改写，缺少 `engine = rocksdb` 行（两份 conf 重合） | 重新 `openxdb init` 恢复数据目录 db conf；服务配置另放独立路径（见 §4.2） |
| 写了 `openxdb.conf` 但 `--mysql-port` 等配置不生效 | 服务配置需 `--config` 显式加载，不会自动读取 | 启动时带 `--config <服务配置路径>`，或命令行显式带 `--mysql-port` |
| 复制端口与指标端口冲突 | `replica_port=7789` 与 `metrics_port=7789` 同时启用 | 显式错开端口，如 `--metrics-port 7790`（见 §4.2） |

---

## 9. 边界与已知限制

- **SQL 子集**：OpenXDB 支持 SQL 子集，非完整 MySQL/标准 SQL；`--mysql-port` 为部分兼容，非 MySQL wire protocol 全兼容（以产品手册第 10 章与 KNOWN_LIMITATIONS 为准）。实测（v1.17.0）：MySQL 客户端支持 `SELECT 1`、`CREATE TABLE`、`INSERT`、`SELECT`、`SHOW TABLES/DATABASES/INDEX/SLOWQUERIES/NODES/VIEWS/USERS`；不支持 `SELECT VERSION()`、`DESC`、`SHOW CREATE TABLE`（报 1045 not supported / unsupported statement）。
- **默认无密码**：未开 `--auth` 时以 root 身份运行，mysql/REPL 连接默认无密码（以仓库实际为准）。
- **性能数值**：README/E5 文档中的 QPS/tpmC 等均为**单机端侧功能基准**，不代表生产环境容量规划依据。
- **跨节点写**：分布式场景跨节点写依赖 2PC（单节点部署不触发，见分布式文档）。
- **集群语句**：单机模式（未配 `--cluster-addr`）执行 `ADD NODE` / `SHOW NODES` / `BALANCE` 等返回 `cluster not enabled`。
- **HA quorum 默认关闭**、epoch 默认开启（v1.17.0 KNOWN_LIMITATIONS）。
- 其余限制以 `KNOWN_LIMITATIONS.md` 为准。

---

## 10. RHEL/Rocky/AlmaLinux/Oracle Linux 9.x 单机部署

> 说明：本章为 9.x 系补充部署指南，对应一键脚本 `docs/deployment/install_openxdb_rhel9.sh`；前文 §1–§9 的通用概念（数据目录结构、备份恢复、FAQ、边界限制等）同样适用。
> 实测记录：已在 **Oracle Linux Server 9.7（/etc/os-release ID=ol）** 完整走通全流程（见 §10.10）。

### 10.1 支持发行版清单

- 支持发行版：RHEL 9.x / Rocky Linux 9.x / AlmaLinux 9.x / Oracle Linux 9.x（x86_64 / arm64）。
- 系统识别：读取 `/etc/os-release`，`ID` 白名单为 `rhel / rocky / almalinux / ol / oraclelinux`，`VERSION_ID` 主版本须为 9，不满足则拒绝执行。
- 前置要求：root 权限；`dnf` 可用；可访问 gitee.com / github.com / mirrors.tuna.tsinghua.edu.cn（国内网络优先）；磁盘 ≥ 10GB、内存 ≥ 4GB（RocksDB 编译较耗时）。

### 10.2 与 CentOS 7 版的关键差异

| 维度 | CentOS 7 版（install_openxdb_centos7.sh） | RHEL/Rocky/AlmaLinux/Oracle 9.x 版（install_openxdb_rhel9.sh） |
|------|------|------|
| yum/dnf 源 | 需 EOL 源修复（vault/EPEL 归档镜像多候选探测） | 无需 EOL 源修复；官方 dnf 源正常，脚本严禁修改系统 yum/dnf 源 |
| 工具链 | devtoolset-11（SCL）+ cmake3（EPEL） | `dnf` 直接安装 gcc / gcc-c++ / cmake / ninja / git / curl |
| ninja | EPEL 提供 ninja-build | 多候选降级链（见 §10.3） |
| Go | go.mod 要求 go1.22，不满足时装官方二进制 | 同左：不满足时清华镜像装官方二进制（见 §10.4） |
| RocksDB 编译参数 | 与 9.x 版一致（见 §10.5） | 与 CentOS 7 版一致 |
| 配置架构 | 脚本默认 CONF_FILE=${DATA_DIR}/openxdb.conf（未显式 init，以脚本实际为准） | 双 conf 分离：服务配置 /etc/openxdb/openxdb.conf + `openxdb init` 生成数据目录 db conf（见 §10.6） |
| 其余 | 端口布局、doctor 语义、验证链一致 | 同左 |

### 10.3 ninja 多候选降级链

Oracle Linux 9.7 默认仓库中 `ninja-build` 存在 "No match" 情况，脚本（`install_ninja()` / `enable_crb_repos()`）按以下候选链依次尝试，任一成功即结束：

a) `dnf install -y ninja-build`
b) `dnf install -y ninja`（部分发行版包名不同）
c) 启用发行版对应 CRB/Codeready Builder 仓库后重试 `dnf install -y ninja-build`：

| 发行版 | 候选仓库 ID |
|------|------|
| RHEL | `rhel-9-for-<arch>-codeready-builder-rpms` |
| Rocky Linux | `rocky9-crb` / `rocky9-codeready-builder` |
| AlmaLinux | `almalinux9-crb` |
| Oracle Linux | `ol9_codeready_builder` / `ol9_codeready_builder_archive` |

启用方式：优先 `dnf config-manager --set-enabled`（缺 `dnf-plugins-core` 时先安装），逐个候选尝试。
d) 兜底：`dnf install -y python3-pip` 后 `pip3 install ninja`。

> 实测：Oracle Linux 9.7 经候选 c（启用 `ol9_codeready_builder`）安装 ninja 成功；全部候选失败才报错退出。

### 10.4 Go 版本要求

- 仓库 `go.mod` 声明 `go 1.22`；脚本默认安装 `GO_VERSION=1.22.12`（以仓库实际为准）。
- 检测逻辑：`go version` 解析 `go1.<minor>`，命令不存在或 minor < 22 时重新安装；架构按 `uname -m` 取 amd64 / arm64。
- 安装源：优先清华镜像 `https://mirrors.tuna.tsinghua.edu.cn/golang/go1.22.12.linux-<arch>.tar.gz`，失败回退官方 `dl.google.com`；解压至 `/usr/local/go`，并写入 `/etc/profile.d/go-path.sh`（重新登录后 go 自动可用）。
- 构建时导出 `GOPROXY=https://goproxy.cn,direct`（可覆盖）。

### 10.5 RocksDB 静态编译参数（与 CentOS 7 版一致）

- 版本：`v11.8.1`；源码优先 gitee 镜像 `gitee.com/mirrors/rocksdb`（失败回退 `github.com/facebook/rocksdb`）。
- cmake 配置（-G Ninja，Release，C++20，静态库，关闭三方压缩库与 tests/tools/benchmark/jni）：

```bash
cmake -S "$ROCKSDB_SRC" -B "$ROCKSDB_BUILD" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD=20 \
  -DROCKSDB_BUILD_SHARED=OFF \
  -DWITH_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF -DWITH_JNI=OFF \
  -DWITH_GFLAGS=OFF \
  -DWITH_ZLIB=OFF -DWITH_BZ2=OFF -DWITH_LZ4=OFF -DWITH_SNAPPY=OFF -DWITH_ZSTD=OFF \
  -DFAIL_ON_WARNINGS=OFF
cmake --build "$ROCKSDB_BUILD" -j"$(nproc)"
```

- 产出静态库 `$ROCKSDB_BUILD/librocksdb.a`；编译参数与 `install_openxdb_centos7.sh` 一致。
- OpenXDB 编译（与 `scripts/build.sh` 对齐，`-l:` 强制使用静态库文件）：

```bash
export CGO_ENABLED=1
export CGO_CFLAGS="-I$ROCKSDB_SRC/include"
export CGO_CXXFLAGS="-I$ROCKSDB_SRC/include"
export CGO_LDFLAGS="-L$ROCKSDB_BUILD -l:librocksdb.a -lstdc++ -lpthread"
go build -o /usr/local/bin/openxdb ./cmd/openxdb
```

### 10.6 双 conf 架构

- 服务配置 `/etc/openxdb/openxdb.conf`：仅 `[server]` 段（data_dir / port / replica_port / metrics_port / mysql_port），**不含 engine**——`cmd/openxdb/main.go` 的 serverKeys 白名单无 engine 键，写了会报 unknown key。
- 数据目录 db conf `<data_dir>/openxdb.conf`：由 `openxdb init --data-dir <dir>` 生成，含 `engine = rocksdb` 等；db.Open 校验必须含该字符串，否则报 `unsupported engine`。
- 两份 conf 不得重合（脚本以 `readlink -f` 校验，重合则报错退出）；`cmdStart` 直接 db.Open 不自动 Init，必须先执行 init。
- 启动时服务配置必须显式加载：`openxdb start --config /etc/openxdb/openxdb.conf`（不会自动读取）。

### 10.7 一键脚本用法

```bash
sudo bash install_openxdb_rhel9.sh          # 必须用 bash，禁止用 sh
```

- 幂等：可重复执行，已完成步骤自动跳过（复用 RocksDB 源码/静态库、OpenXDB 源码/二进制）。
- 流程：系统检测 → 工具链（dnf）→ Go → RocksDB v11.8.1 静态编译 → OpenXDB 源码编译 → 双 conf 部署 → 启动 → 验证 → 汇总（9/9 步）。
- 常用可覆盖变量（环境变量传入）：`DATA_DIR`（默认 /data/openxdb）、`PORT=7788`、`REPLICA_PORT=7789`、`METRICS_PORT=7790`、`MYSQL_PORT=3307`、`CONF_FILE`（默认 /etc/openxdb/openxdb.conf）、`GO_VERSION=1.22.12`、`ROCKSDB_VERSION=v11.8.1`、`REPO_URL` 等。示例：`PORT=7799 MYSQL_PORT=3308 sudo bash install_openxdb_rhel9.sh`。
- 日志：`/var/log/openxdb/openxdb.log`；启动方式：`nohup /usr/local/bin/openxdb start --config /etc/openxdb/openxdb.conf >> /var/log/openxdb/openxdb.log 2>&1 &`。

### 10.8 端口布局

| 端口 | 用途 |
|------|------|
| 7788 | 自研二进制协议 SQL 服务（`--port`，main.go 默认 7788） |
| 7789 | 复制端口（`--replica-port`） |
| 7790 | Prometheus 指标端口（`--metrics-port`，main.go usage 默认 7789、0 禁用；脚本显式 7790 避免与 replica 冲突） |
| 3307 | MySQL wire 兼容监听（`--mysql-port`，main.go 默认 0=关闭；脚本默认 3307 开启） |

### 10.9 doctor 语义与验证命令

- `openxdb doctor --data-dir <dir> --addr <host:port>` 退出码：**0=通过 / 1=警告 / 2=错误**（`cmd/openxdb/doctor.go` summarizeExitCode：有错误=2，否则有警告=1，全通过=0）。
- 脚本判定：`DOCTOR_RC >= 2` 视为失败；1 属预期警告。
- 在线运行期间 `rocksdb` / `metadata` 项报 WARN 属预期（引擎被服务占用 LOCK，跳过本地 Open 检查）；未启用复制时 `binlog` 报 WARN 也属预期（详见 §5）。停服后再跑 doctor 可完成本地 Open 校验。
- 完整验证命令：

```bash
openxdb version
openxdb config-check --config /etc/openxdb/openxdb.conf     # 配置校验
ss -lntp | grep -E ':7788 |:3307 '                           # 端口监听
openxdb doctor --data-dir /data/openxdb --addr 127.0.0.1:7788
# MySQL wire 冒烟（部分兼容，详见 §8；不支持 VERSION()/DESC/SHOW CREATE TABLE）
mysql -h 127.0.0.1 -P 3307 -u root -e "CREATE TABLE smoke_test (id INT PRIMARY KEY, name TEXT); INSERT INTO smoke_test VALUES (1, 'hello'); SELECT * FROM smoke_test; SHOW TABLES;"
```

### 10.10 实测记录与说明

- 已在 **Oracle Linux Server 9.7（/etc/os-release ID=ol，x86_64）** 实测通过：ninja 经启用 `ol9_codeready_builder` 安装成功；gcc/gcc-c++/cmake 由 dnf 直装；Go 1.22.12 经清华镜像安装；RocksDB v11.8.1 静态编译与 OpenXDB 编译通过；init / start / 端口监听 / doctor / MySQL wire 冒烟全部通过。
- RHEL / Rocky / AlmaLinux 9.x 未逐一实测，安装逻辑基于官方 dnf 源与脚本多候选降级链，个别包名或仓库 ID 差异以仓库实际为准。

---

## 11. 参考资料

- 产品手册：`docs/product_manual_zh.md`（第 3 章部署/配置、第 7 章运维）
- 构建脚本：`scripts/build.sh`；一键部署：`scripts/deploy.sh`
- 容器构建：`Dockerfile`
- 快速验证：`examples/quickstart/README.md`
- 已知限制：`KNOWN_LIMITATIONS.md`
- 变更记录：`CHANGELOG.md` / `RELEASE_NOTES.md`
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*

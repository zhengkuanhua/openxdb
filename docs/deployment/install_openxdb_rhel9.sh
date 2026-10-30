#!/usr/bin/env bash
# =============================================================================
# install_openxdb_rhel9.sh
# OpenXDB v1.17.0 单机自动安装脚本（RHEL/Rocky/AlmaLinux/Oracle Linux 9.x）
# -----------------------------------------------------------------------------
# 用途:
#   在 RHEL/Rocky/AlmaLinux/Oracle Linux 9.x 上自动完成 OpenXDB 单机部署:
#     系统检测 -> 工具链(dnf: gcc/gcc-c++/cmake/ninja-build/git/curl + Go)
#     -> RocksDB v11.8.1 静态编译 -> OpenXDB 源码编译
#     -> 双 conf 架构部署(服务配置 /etc/openxdb/openxdb.conf +
#        openxdb init 生成数据目录 db conf) -> 启动 -> 端口/doctor/冒烟验证
#
# 用法（必须用 bash 运行，禁止用 sh）:
#   sudo bash install_openxdb_rhel9.sh
#   注意: 脚本依赖 bash 语法（数组/进程替换/管道等），使用 sh 执行会报错，务必以 bash 运行。
#
# 依赖:
#   - root 权限（dnf/系统级安装需要）
#   - 可访问 gitee.com / github.com / mirrors.tuna.tsinghua.edu.cn（国内网络优先）
#   - 磁盘 >= 10GB，内存 >= 4GB（RocksDB 编译较耗时）
#
# 注意事项:
#   * 幂等: 可重复执行; 已完成的步骤自动跳过（复用 RocksDB 源码/静态库、OpenXDB 源码/二进制）
#   * RHEL 9 官方 dnf 源正常可用，本脚本【严禁修改系统 yum/dnf 源】，不做 EOL 源修复
#   * 构建事实与仓库 scripts/build.sh、Dockerfile、go.mod、cmd/openxdb/main.go、
#     docs/deployment/linux_single_node.md 对齐:
#     - go.mod 要求 go1.22; 仓库 Dockerfile 使用 golang:1.27.1-bookworm
#     - RocksDB 静态编译参数与 centos7 版一致（-DWITH_GFLAGS=OFF -DROCKSDB_BUILD_SHARED=OFF
#       等，产出静态库 librocksdb.a），关闭三方压缩库与 tests/tools/benchmark/jni
#     - OpenXDB 编译: CGO_ENABLED=1 + CGO_LDFLAGS 强制静态链接
#       -l:librocksdb.a -lstdc++ -lpthread（-l: 强制使用静态库文件）
#     - 双 conf 架构（详见 linux_single_node.md §4.2）:
#       * 服务配置: /etc/openxdb/openxdb.conf（[server] 段，含 data_dir/port/
#         replica_port/metrics_port/mysql_port，不含 engine；main.go serverKeys
#         白名单无 engine 键，写了会报 unknown key）
#       * 数据目录 db conf: <data_dir>/openxdb.conf 由 `openxdb init --data-dir`
#         生成（含 engine = rocksdb；db.Open 校验必须含该字符串，否则 unsupported engine）
#       * 两份 conf 不得重合；cmdStart 直接 db.Open 不自动 Init，必须先 init
#   * 端口布局: port=7788 / replica_port=7789 / metrics_port=7790 / mysql_port=3307
#   * doctor exit code: 0=pass / 1=warn(预期, 详见 linux_single_node.md §5) / 2=error
#   * MySQL wire 为部分兼容（CREATE/INSERT/SELECT/SHOW TABLES 等支持,
#     不支持 VERSION()/DESC/SHOW CREATE TABLE, 详见 linux_single_node.md §8）
#
# 可覆盖变量（以环境变量方式传入, 示例）:
#   ROCKSDB_VERSION=v11.8.1  DATA_DIR=/data/openxdb  PORT=7788
#   REPLICA_PORT=7789        METRICS_PORT=7790       MYSQL_PORT=3307
#   REPO_URL=gitee 仓库地址  REPO_URL_GITHUB=GitHub 仓库地址
#   REPO_DIR=/opt/openxdb-src  ROCKSDB_SRC=/opt/rocksdb-src
#   ROCKSDB_BUILD=/opt/rocksdb-build  GO_VERSION=1.22.12
#   BIN_DIR=/usr/local/bin   CONF_FILE=/etc/openxdb/openxdb.conf
#   LOG_FILE=/var/log/openxdb/openxdb.log  GO_ARCH=amd64(自动按 uname -m)
# 示例: PORT=7799 MYSQL_PORT=3308 sudo bash install_openxdb_rhel9.sh
# =============================================================================

set -euo pipefail

# ----------------------------- 可覆盖变量 ---------------------------------
ROCKSDB_VERSION="${ROCKSDB_VERSION:-v11.8.1}"
DATA_DIR="${DATA_DIR:-/data/openxdb}"
PORT="${PORT:-7788}"
REPLICA_PORT="${REPLICA_PORT:-7789}"
METRICS_PORT="${METRICS_PORT:-7790}"
MYSQL_PORT="${MYSQL_PORT:-3307}"
REPO_URL="${REPO_URL:-https://gitee.com/zhengkh/openxdb.git}"
REPO_URL_GITHUB="${REPO_URL_GITHUB:-https://github.com/zhengkuanhua/openxdb.git}"
REPO_DIR="${REPO_DIR:-/opt/openxdb-src}"
ROCKSDB_SRC="${ROCKSDB_SRC:-/opt/rocksdb-src}"
ROCKSDB_BUILD="${ROCKSDB_BUILD:-/opt/rocksdb-build}"
GO_VERSION="${GO_VERSION:-1.22.12}"
BIN_DIR="${BIN_DIR:-/usr/local/bin}"
# 双 conf 架构: 服务配置独立于数据目录 db conf（默认 /etc/openxdb/openxdb.conf）
CONF_FILE="${CONF_FILE:-/etc/openxdb/openxdb.conf}"
LOG_FILE="${LOG_FILE:-/var/log/openxdb/openxdb.log}"
GOPROXY_URL="${GOPROXY_URL:-https://goproxy.cn,direct}"

log() { echo "[$(date '+%F %T')] $*"; }

port_listening() { # $1=port
  (ss -lnt 2>/dev/null || netstat -lnt 2>/dev/null) | grep -q ":$1 "
}

# 启用发行版对应的 CRB/Codeready Builder 类仓库（ninja-build 降级链第 3 候选用）。
# RHEL 仓库 ID 带架构: rhel-9-for-<arch>-codeready-builder-rpms；
# Rocky/AlmaLinux/Oracle 使用固定 ID（任务给定候选，逐个尝试）。
# 优先 dnf config-manager --set-enabled；缺 dnf-plugins-core 时先安装。
enable_crb_repos() {
  local repos=() r arch
  case "$OS_ID" in
    rhel)
      arch="$(uname -m)"
      repos=("rhel-9-for-${arch}-codeready-builder-rpms")
      ;;
    rocky)
      repos=("rocky9-crb" "rocky9-codeready-builder")
      ;;
    almalinux)
      repos=("almalinux9-crb")
      ;;
    ol|oraclelinux)
      repos=("ol9_codeready_builder" "ol9_codeready_builder_archive")
      ;;
    *)
      log "  CRB 启用跳过: 未知发行版 ID=${OS_ID:-空}"
      return 1
      ;;
  esac
  # dnf config-manager 由 dnf-plugins-core 提供（子命令，无法用 command -v 直接判定）
  if ! dnf config-manager --help >/dev/null 2>&1; then
    log "  安装 dnf-plugins-core（提供 dnf config-manager）..."
    dnf install -y dnf-plugins-core || { log "  安装 dnf-plugins-core 失败"; return 1; }
  fi
  for r in "${repos[@]}"; do
    log "  尝试启用仓库: $r"
    if dnf config-manager --set-enabled "$r" >/dev/null 2>&1; then
      log "  仓库已启用: $r"
      return 0
    fi
    log "  仓库 $r 启用失败（可能不存在或 ID 不符），尝试下一个"
  done
  log "  警告: 所有 CRB 候选仓库均启用失败"
  return 1
}

# ninja 安装: 多候选降级链（每个候选前先判 command -v ninja 存在则跳过）。
# 依次尝试:
#   a) dnf install -y ninja-build
#   b) dnf install -y ninja（部分发行版包名不同）
#   c) 启用 CRB/Codeready Builder 仓库后重试 ninja-build
#   d) dnf install -y python3-pip 后 pip3 install ninja 兜底
# 全部失败才报错退出，并输出最后一步的 stderr。
install_ninja() {
  local err
  if command -v ninja >/dev/null 2>&1; then
    log "  ninja 已存在，跳过安装"
    return 0
  fi
  # a) 首选包名 ninja-build
  if command -v ninja >/dev/null 2>&1; then
    log "  ninja 已存在，跳过安装"
    return 0
  fi
  if err="$(dnf install -y ninja-build 2>&1)"; then
    log "  ninja 安装成功 (dnf install ninja-build)"
    return 0
  fi
  # b) 备选包名 ninja
  if command -v ninja >/dev/null 2>&1; then
    log "  ninja 已存在，跳过安装"
    return 0
  fi
  if err="$(dnf install -y ninja 2>&1)"; then
    log "  ninja 安装成功 (dnf install ninja)"
    return 0
  fi
  # c) 启用 CRB/Codeready Builder 仓库后重试 ninja-build
  if command -v ninja >/dev/null 2>&1; then
    log "  ninja 已存在，跳过安装"
    return 0
  fi
  if enable_crb_repos; then
    if err="$(dnf install -y ninja-build 2>&1)"; then
      log "  ninja 安装成功 (启用 CRB 后 dnf install ninja-build)"
      return 0
    fi
  fi
  # d) pip3 兜底
  if command -v ninja >/dev/null 2>&1; then
    log "  ninja 已存在，跳过安装"
    return 0
  fi
  if err="$(dnf install -y python3-pip 2>&1)" && err="$(pip3 install ninja 2>&1)"; then
    if command -v ninja >/dev/null 2>&1; then
      log "  ninja 安装成功 (pip3 install ninja)"
      return 0
    fi
    log "  警告: pip3 install ninja 已执行但 ninja 不在 PATH"
    err="pip3 install ninja 成功但 ninja 不在 PATH"
  fi
  log "错误: ninja 安装失败（所有候选均不可用），最后一步 stderr:"
  printf '%s\n' "${err:-无输出}"
  return 1
}

# ----------------------------- 1. 系统检测 --------------------------------
log "[1/9] 系统检测"
if [ ! -f /etc/os-release ]; then
  log "错误: 未找到 /etc/os-release，无法识别发行版，仅支持 RHEL/Rocky/AlmaLinux/Oracle Linux 9.x"
  exit 1
fi
# shellcheck disable=SC1091
. /etc/os-release
OS_ID="${ID:-}"
OS_NAME="${NAME:-}"
OS_VERSION_ID="${VERSION_ID:-}"
OS_MAJOR="$(printf '%s' "$OS_VERSION_ID" | cut -d. -f1)"
log "  系统: ${OS_NAME} ${OS_VERSION_ID} (ID=${OS_ID})"
case "$OS_ID" in
  rhel|rocky|almalinux|ol|oraclelinux) ;;
  *)
    log "错误: 不支持的发行版 ID=${OS_ID}，仅支持 RHEL/Rocky/AlmaLinux/Oracle Linux 9.x"
    exit 1
    ;;
esac
if [ "$OS_MAJOR" != "9" ]; then
  log "错误: 需要 RHEL/Rocky/AlmaLinux/Oracle Linux 9.x，当前主版本为 ${OS_MAJOR:-空}"
  exit 1
fi
if [ "$(id -u)" != "0" ]; then
  log "错误: 需要 root 权限，请使用: sudo bash $0"
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  log "  curl 缺失，尝试用 dnf 安装（RHEL9 最小安装可能不带 curl）..."
  dnf install -y curl || { log "错误: 缺少 curl 且安装失败，请手动安装后重试"; exit 1; }
fi
log "  检测通过: $OS_NAME $OS_VERSION_ID ($(uname -m))"

# --------------------- 2. 工具链安装 (dnf: gcc/gcc-c++/cmake/ninja/git/curl) -
# RHEL 9 官方 dnf 源正常，严禁修改系统 yum/dnf 源，不做 EOL 源修复。
log "[2/9] 工具链: gcc / gcc-c++ / cmake / ninja-build / git / curl (dnf)"
NEED_TC=0
for c in gcc g++ cmake ninja git curl; do
  if ! command -v "$c" >/dev/null 2>&1; then
    log "  缺少 $c"
    NEED_TC=1
  fi
done
if [ "$NEED_TC" = "1" ]; then
  log "  安装 gcc gcc-c++ cmake git curl (dnf) ..."
  dnf install -y gcc gcc-c++ cmake git curl
  log "  安装 ninja（多候选降级链）..."
  install_ninja
else
  log "  工具链已完整，跳过"
fi
GCC_VER="$(gcc --version 2>/dev/null | head -n1 || true)"
log "  gcc: ${GCC_VER:-未知}"
log "  cmake: $(cmake --version 2>/dev/null | head -n1 || true)"
log "  ninja: $(ninja --version 2>/dev/null || true)"

# --------------------- 3. Go 工具链（go.mod 要求 go1.22） -----------------
log "[3/9] Go 工具链（go.mod 要求 go 1.22）"
GO_ARCH="${GO_ARCH:-$(uname -m)}"
case "$GO_ARCH" in
  x86_64|amd64) GO_ARCH="amd64" ;;
  aarch64|arm64) GO_ARCH="arm64" ;;
  *)
    log "错误: 不支持的架构 $GO_ARCH，仅支持 amd64/arm64"
    exit 1
    ;;
esac
export PATH="$PATH:/usr/local/go/bin:$HOME/go/bin"
GO_INSTALL=0
if ! command -v go >/dev/null 2>&1; then
  GO_INSTALL=1
else
  GO_MINOR="$(go version 2>/dev/null | grep -oE 'go1\.[0-9]+' | cut -d. -f2 || true)"
  if [ -z "$GO_MINOR" ] || [ "$GO_MINOR" -lt 22 ]; then
    log "  当前 go 版本过低: $(go version 2>/dev/null || true)"
    GO_INSTALL=1
  fi
fi
if [ "$GO_INSTALL" = "1" ]; then
  log "  安装 Go ${GO_VERSION} (linux/${GO_ARCH}) ..."
  GO_TGZ="/tmp/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz"
  if ! curl -fSL --max-time 600 -o "$GO_TGZ" "https://mirrors.tuna.tsinghua.edu.cn/golang/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz"; then
    log "  清华镜像下载失败，尝试官方源兜底..."
    curl -fSL --max-time 600 -o "$GO_TGZ" "https://dl.google.com/go/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz"
  fi
  rm -rf /usr/local/go
  tar -C /usr/local -xzf "$GO_TGZ"
  rm -f "$GO_TGZ"
  # 持久化 PATH（仅首次写入）
  if [ ! -f /etc/profile.d/go-path.sh ]; then
    printf '%s\n' 'export PATH="$PATH:/usr/local/go/bin"' > /etc/profile.d/go-path.sh
    log "  已写入 /etc/profile.d/go-path.sh（重新登录后 go 自动可用）"
  fi
fi
log "  go: $(go version)"
export GOPROXY="${GOPROXY:-$GOPROXY_URL}"

# --------------------- 4. RocksDB v11.8.1 静态编译 ------------------------
log "[4/9] RocksDB ${ROCKSDB_VERSION} 静态库"
if [ -f "$ROCKSDB_BUILD/librocksdb.a" ] && [ -d "$ROCKSDB_SRC/include/rocksdb" ] && [ -n "$(ls "$ROCKSDB_SRC/include/rocksdb"/*.h 2>/dev/null)" ]; then
  log "  复用已有 RocksDB: src=$ROCKSDB_SRC build=$ROCKSDB_BUILD"
else
  if [ -d "$ROCKSDB_SRC" ] && [ ! -d "$ROCKSDB_SRC/include/rocksdb" ]; then
    log "  清理不完整的 RocksDB 源码目录: $ROCKSDB_SRC"
    rm -rf "$ROCKSDB_SRC"
  fi
  if [ ! -d "$ROCKSDB_SRC/include/rocksdb" ]; then
    log "  克隆 RocksDB（gitee 镜像）..."
    if ! git clone --depth 1 --branch "$ROCKSDB_VERSION" https://gitee.com/mirrors/rocksdb.git "$ROCKSDB_SRC"; then
      log "  gitee 克隆失败，尝试 GitHub 兜底..."
      rm -rf "$ROCKSDB_SRC"
      git clone --depth 1 --branch "$ROCKSDB_VERSION" https://github.com/facebook/rocksdb.git "$ROCKSDB_SRC"
    fi
  else
    log "  复用已有 RocksDB 源码: $ROCKSDB_SRC"
  fi
  mkdir -p "$ROCKSDB_BUILD"
  log "  cmake 配置（Release, C++20, 静态库, 关闭 tests/tools/benchmark/jni/压缩库）..."
  cmake -S "$ROCKSDB_SRC" -B "$ROCKSDB_BUILD" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_STANDARD=20 \
    -DROCKSDB_BUILD_SHARED=OFF \
    -DWITH_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF -DWITH_JNI=OFF \
    -DWITH_GFLAGS=OFF \
    -DWITH_ZLIB=OFF -DWITH_BZ2=OFF -DWITH_LZ4=OFF -DWITH_SNAPPY=OFF -DWITH_ZSTD=OFF \
    -DFAIL_ON_WARNINGS=OFF
  log "  ninja 编译（并行 $(nproc)）..."
  cmake --build "$ROCKSDB_BUILD" -j"$(nproc)"
fi
if [ ! -f "$ROCKSDB_BUILD/librocksdb.a" ]; then
  log "错误: librocksdb.a 未生成: $ROCKSDB_BUILD"
  exit 1
fi
log "  RocksDB 静态库: $ROCKSDB_BUILD/librocksdb.a"

# --------------------- 5. OpenXDB 源码编译 --------------------------------
log "[5/9] OpenXDB 源码编译"
if [ -x "$BIN_DIR/openxdb" ]; then
  log "  复用已有二进制: $BIN_DIR/openxdb"
else
  if [ -d "$REPO_DIR" ] && [ ! -f "$REPO_DIR/go.mod" ]; then
    log "  清理不完整源码目录: $REPO_DIR"
    rm -rf "$REPO_DIR"
  fi
  if [ ! -d "$REPO_DIR" ]; then
    log "  克隆 OpenXDB 仓库: $REPO_URL"
    if ! git clone --depth 1 "$REPO_URL" "$REPO_DIR"; then
      log "  gitee 克隆失败，尝试 GitHub 兜底: $REPO_URL_GITHUB"
      rm -rf "$REPO_DIR"
      git clone --depth 1 "$REPO_URL_GITHUB" "$REPO_DIR"
    fi
  else
    log "  复用已有源码目录: $REPO_DIR"
  fi
  cd "$REPO_DIR"
  # 与 scripts/build.sh 一致: CGO_ENABLED=1, include 指向 RocksDB 源码头文件；
  # CGO_LDFLAGS 使用 -l:librocksdb.a 强制静态链接静态库文件（+ -lstdc++ -lpthread）
  export CGO_ENABLED=1
  export CGO_CFLAGS="-I$ROCKSDB_SRC/include"
  export CGO_CXXFLAGS="-I$ROCKSDB_SRC/include"
  export CGO_LDFLAGS="-L$ROCKSDB_BUILD -l:librocksdb.a -lstdc++ -lpthread"
  log "  go build ./cmd/openxdb -> $BIN_DIR/openxdb"
  go build -o "$BIN_DIR/openxdb" ./cmd/openxdb
  if [ ! -x "$BIN_DIR/openxdb" ]; then
    log "错误: openxdb 二进制未生成"
    exit 1
  fi
fi
log "  openxdb 二进制: $BIN_DIR/openxdb"

# --------------------- 6. 双 conf 架构部署 --------------------------------
log "[6/9] 双 conf 架构部署"
mkdir -p "$(dirname "$CONF_FILE")" "$DATA_DIR" "$DATA_DIR/backups" "$(dirname "$LOG_FILE")"
# 两份 conf 不得重合（服务配置必须独立于数据目录 db conf）
DB_CONF="$DATA_DIR/openxdb.conf"
if [ "$(readlink -f "$CONF_FILE" 2>/dev/null || printf '%s' "$CONF_FILE")" = "$(readlink -f "$DB_CONF" 2>/dev/null || printf '%s' "$DB_CONF")" ]; then
  log "错误: 服务配置 CONF_FILE=$CONF_FILE 与数据目录 db conf $DB_CONF 重合，请改用独立路径（默认 /etc/openxdb/openxdb.conf）"
  exit 1
fi

# 6a. 服务配置（[server] 段，不含 engine；main.go serverKeys 白名单无 engine 键）
if [ -f "$CONF_FILE" ]; then
  log "  服务配置已存在，跳过: $CONF_FILE"
else
  cat > "$CONF_FILE" <<EOF
# OpenXDB single-node service config (generated by install_openxdb_rhel9.sh)
# 双 conf 架构: 本文件仅为 [server] 段启动配置，数据目录 db conf 由 openxdb init 生成
[server]
data_dir = $DATA_DIR
port = $PORT
replica_port = $REPLICA_PORT
metrics_port = $METRICS_PORT
mysql_port = $MYSQL_PORT
EOF
  log "  已生成服务配置: $CONF_FILE"
fi
log "  端口布局: port=$PORT replica_port=$REPLICA_PORT metrics_port=$METRICS_PORT mysql_port=$MYSQL_PORT"

# 6b. 数据目录初始化（生成 db conf: engine = rocksdb；cmdStart 不自动 Init）
if [ -f "$DB_CONF" ]; then
  log "  数据目录已初始化，跳过: $DB_CONF"
else
  log "  openxdb init --data-dir $DATA_DIR"
  "$BIN_DIR/openxdb" init --data-dir "$DATA_DIR"
fi
if [ -f "$DB_CONF" ]; then
  log "  数据目录 db conf: $DB_CONF"
else
  log "  警告: 未找到 $DB_CONF（以仓库实际 init 行为为准，如路径不同请以 $DATA_DIR 下实际生成文件为准）"
fi

# --------------------- 7. 启动服务 ----------------------------------------
log "[7/9] 启动 OpenXDB"
if port_listening "$PORT"; then
  log "  端口 $PORT 已在监听，跳过启动"
else
  log "  nohup $BIN_DIR/openxdb start --config $CONF_FILE"
  nohup "$BIN_DIR/openxdb" start --config "$CONF_FILE" >> "$LOG_FILE" 2>&1 &
  OPENXDB_PID=$!
  log "  后台进程 pid=$OPENXDB_PID，日志: $LOG_FILE"
  READY=0
  for i in $(seq 1 30); do
    if port_listening "$PORT"; then
      READY=1
      log "  端口 $PORT 就绪（等待 ${i}s）"
      break
    fi
    if ! kill -0 "$OPENXDB_PID" 2>/dev/null; then
      log "错误: openxdb 进程已退出，日志尾部:"
      tail -n 20 "$LOG_FILE" 2>/dev/null || true
      exit 1
    fi
    sleep 1
  done
  if [ "$READY" != "1" ]; then
    log "错误: 启动超时(30s)，请查看日志: $LOG_FILE"
    exit 1
  fi
fi

# --------------------- 8. 验证: 配置 / 端口 / doctor / 冒烟 ---------------
log "[8/9] 验证"
if [ -x "$BIN_DIR/openxdb" ]; then
  set +e
  "$BIN_DIR/openxdb" config-check --config "$CONF_FILE"
  CONF_RC=$?
  set -e
  if [ "$CONF_RC" -ne 0 ]; then
    log "错误: 配置文件校验失败 (exit $CONF_RC): $CONF_FILE"
    exit 1
  fi
  log "  配置校验通过: $CONF_FILE"
fi
log "  监听端口:"
ss -lntp 2>/dev/null | grep -E ":$PORT |:$MYSQL_PORT " || true
set +e
"$BIN_DIR/openxdb" doctor --data-dir "$DATA_DIR" --addr "127.0.0.1:$PORT"
DOCTOR_RC=$?
set -e
if [ "$DOCTOR_RC" -ge 2 ]; then
  log "错误: doctor 健康自检失败 (exit $DOCTOR_RC)"
  exit 1
fi
log "  doctor 完成 (exit $DOCTOR_RC; 0=pass, 1=warn 属预期, 详见 linux_single_node.md §5)"
if command -v mysql >/dev/null 2>&1 && [ "$MYSQL_PORT" -gt 0 ]; then
  SMOKE_TBL="smoke_test_$$"
  log "  MySQL wire 冒烟测试 (127.0.0.1:$MYSQL_PORT, 表 $SMOKE_TBL):"
  mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root -e \
    "CREATE TABLE $SMOKE_TBL (id INT PRIMARY KEY, name TEXT); INSERT INTO $SMOKE_TBL VALUES (1, 'hello'); SELECT * FROM $SMOKE_TBL; SHOW TABLES;" \
    && log "  冒烟 SQL 通过" || log "  冒烟 SQL 未通过（MySQL wire 为部分兼容, 属预期限制, 见文档 §8）"
else
  log "  跳过 MySQL 冒烟（未安装 mysql 客户端或 MYSQL_PORT=0）"
fi

# --------------------- 9. 汇总 --------------------------------------------
log "[9/9] 安装完成"
log "============================================================="
log " OpenXDB 单机部署完成 (RHEL/Rocky/AlmaLinux/Oracle Linux 9.x)"
log "  二进制:      $BIN_DIR/openxdb"
log "  数据目录:    $DATA_DIR"
log "  服务配置:    $CONF_FILE (双 conf 架构: [server] 段, --config 显式加载)"
log "  数据目录db:  $DB_CONF (由 openxdb init 生成, engine=rocksdb)"
log "  端口:        $PORT (SQL) / $REPLICA_PORT (replica) / $METRICS_PORT (metrics) / $MYSQL_PORT (mysql wire)"
log "  日志:        $LOG_FILE"
log "  启动:        nohup $BIN_DIR/openxdb start --config $CONF_FILE >> $LOG_FILE 2>&1 &"
log "  停止:        kill \$(pgrep openxdb)（或见 openxdb 命令帮助）"
log "  doctor:      $BIN_DIR/openxdb doctor --data-dir $DATA_DIR --addr 127.0.0.1:$PORT"
log "                (exit 0=pass / 1=warn 预期 / 2=error, 详见 linux_single_node.md §5)"
log "============================================================="

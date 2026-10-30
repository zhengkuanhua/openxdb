#!/usr/bin/env bash
# =============================================================================
# install_openxdb_centos7.sh
# OpenXDB v1.17.0 单机自动安装脚本（CentOS/RHEL 7.x）
# -----------------------------------------------------------------------------
# 用途:
#   在 CentOS/RHEL 7.x 上自动完成 OpenXDB 单机部署:
#     系统检测 -> EOL 源修复 -> 工具链(devtoolset-11/cmake3/ninja/Go)
#     -> RocksDB v11.8.1 静态编译 -> OpenXDB 源码编译 -> 生成 openxdb.conf
#     -> 启动 -> 端口/doctor/冒烟验证
#
# 用法（必须用 bash 运行，禁止用 sh）:
#   sudo bash install_openxdb_centos7.sh
#   注意: 脚本依赖 bash 语法（数组/进程替换/管道等），使用 sh 执行会报错，务必以 bash 运行。
#
# 依赖:
#   - root 权限（yum/系统级安装需要）
#   - 可访问 gitee.com / github.com / 清华或阿里镜像（国内网络优先）
#   - 磁盘 >= 10GB，内存 >= 4GB（RocksDB 编译较耗时）
#
# 注意事项:
#   * 幂等: 可重复执行; 已完成的步骤自动跳过（复用 RocksDB 源码/静态库、OpenXDB 源码）
#   * 构建事实与仓库 scripts/build.sh、Dockerfile、docs/deployment/linux_single_node.md 对齐:
#     - go.mod 要求 go1.22; 仓库 Dockerfile 使用 golang:1.27.1-bookworm
#     - Linux 下 #cgo 链接为 -lstdc++ -lpthread, 只需提供 RocksDB include/lib
#   * openxdb.conf 为 INI 风格 [server] 段, 必须 `openxdb start --config` 显式加载
#     （main.go loadStartConfig 仅在提供 --config 时解析配置文件, 不会自动读取）
#   * devtoolset-11 环境写入 ~/.bashrc 持久化; 重新登录后 gcc 11 自动生效
#   * 端口布局: port=7788 / replica_port=7789 / metrics_port=7790 / mysql_port=3307
#     （metrics_port 让位 7790, 避免与 replica_port=7789 冲突）
#   * doctor exit code: 0=pass / 1=warn(预期, 详见 linux_single_node.md §5) / 2=error
#   * MySQL wire 为部分兼容（CREATE/INSERT/SELECT/SHOW TABLES 等支持,
#     不支持 VERSION()/DESC/SHOW CREATE TABLE, 详见 linux_single_node.md §8）
#
# 可覆盖变量（以环境变量方式传入, 示例）:
#   ROCKSDB_VERSION=v11.8.1  DATA_DIR=/data/openxdb  PORT=7788
#   REPLICA_PORT=7789        METRICS_PORT=7790       MYSQL_PORT=3307
#   REPO_URL=gitee 仓库地址  REPO_DIR=/opt/openxdb-src
#   ROCKSDB_SRC=/opt/rocksdb-src  ROCKSDB_BUILD=/opt/rocksdb-build
#   GO_VERSION=1.22.12       BIN_DIR=/usr/local/bin   CONF_FILE=/etc/openxdb/openxdb.conf
#   （CONF_FILE 为服务配置；数据目录 db conf 由 openxdb init 生成于 $DATA_DIR/openxdb.conf）
#   EPEL_MIRROR=指定 EPEL 归档镜像（默认多候选自动探测 repomd.xml 可用性后
#              取第一个可用源: Fedora archives / 阿里云 / 南大 / 中科大）
# 示例: PORT=7799 MYSQL_PORT=3308 sudo bash install_openxdb_centos7.sh
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
CONF_FILE="${CONF_FILE:-/etc/openxdb/openxdb.conf}"
LOG_FILE="${LOG_FILE:-/var/log/openxdb/openxdb.log}"
# yum makecache 的 stderr 单独记录，避免与 openxdb 进程日志混用
MAKECACHE_LOG="${MAKECACHE_LOG:-/var/log/openxdb/yum_makecache.log}"
VAULT_VERSION="${VAULT_VERSION:-7.9.2009}"
MIRROR_CANDIDATES=(
  "https://mirrors.tuna.tsinghua.edu.cn/centos-vault"
  "https://mirrors.aliyun.com/centos-vault"
)
# EPEL archive 镜像多候选（清华 mirrors.tuna.tsinghua.edu.cn/epel-archive
# 未保留 EL7 归档目录，实际请求 /7/x86_64 返回 404，不再作为默认值）
# 若显式设置 EPEL_MIRROR 则优先使用该镜像（单候选），否则自动探测以下候选
EPEL_MIRROR_CANDIDATES=(
  "https://archives.fedoraproject.org/pub/archive/epel"
  "https://mirrors.aliyun.com/epel-archive"
  "https://mirrors.nju.edu.cn/epel-archive"
  "https://mirrors.ustc.edu.cn/epel-archive"
)
EPEL_MIRROR="${EPEL_MIRROR:-}"
GOPROXY_URL="${GOPROXY_URL:-https://goproxy.cn,direct}"

log() { echo "[$(date '+%F %T')] $*"; }

# 探测可用的 EPEL archive 镜像并重写 /etc/yum.repos.d/epel.repo
# 优先级: 显式 EPEL_MIRROR > EPEL_MIRROR_CANDIDATES 逐个探测；
# 全部失败返回 1（不写入）。探测目标为 ${mirror}/7/x86_64/repodata/repomd.xml
# （CentOS 7 常见 x86_64；yum makecache 时按 $basearch 展开同一 repodata）
write_epel_repo() {
  local url="" m
  if [ -n "${EPEL_MIRROR:-}" ]; then
    if curl -fsS --max-time 8 -o /dev/null "${EPEL_MIRROR}/7/x86_64/repodata/repomd.xml" 2>/dev/null; then
      log "  EPEL 源探测可用: ${EPEL_MIRROR}"
      url="${EPEL_MIRROR}"
    else
      log "  警告: 用户指定 EPEL_MIRROR=${EPEL_MIRROR} 探测失败，尝试内置候选"
    fi
  fi
  if [ -z "$url" ]; then
    for m in "${EPEL_MIRROR_CANDIDATES[@]}"; do
      if curl -fsS --max-time 8 -o /dev/null "${m}/7/x86_64/repodata/repomd.xml" 2>/dev/null; then
        log "  EPEL 源探测可用: $m"
        url="$m"
        break
      fi
      log "  警告: EPEL 候选探测失败: ${m}/7/x86_64/repodata/repomd.xml"
    done
  fi
  if [ -z "$url" ]; then
    log "错误: 所有 EPEL 镜像候选均不可用，请手动配置 /etc/yum.repos.d/epel.repo"
    return 1
  fi
  cat > /etc/yum.repos.d/epel.repo <<EOF
[epel]
name=Extra Packages for Enterprise Linux 7 - \$basearch (archive)
baseurl=${url}/7/\$basearch/
enabled=1
gpgcheck=0
EOF
}

port_listening() { # $1=port
  (ss -lnt 2>/dev/null || netstat -lnt 2>/dev/null) | grep -q ":$1 "
}

# ----------------------------- 1. 系统检测 --------------------------------
log "[1/9] 系统检测"
if [ ! -f /etc/redhat-release ] && [ ! -f /etc/centos-release ]; then
  log "错误: 仅支持 CentOS/RHEL 7.x（未找到 /etc/redhat-release）"
  exit 1
fi
OS_RELEASE="$(cat /etc/redhat-release 2>/dev/null || cat /etc/centos-release 2>/dev/null)"
OS_MAJOR="$(echo "$OS_RELEASE" | grep -oE '[0-9]+' | head -n1)"
log "  系统: $OS_RELEASE"
if [ "$OS_MAJOR" != "7" ]; then
  log "错误: 需要 CentOS/RHEL 7.x，当前主版本为 $OS_MAJOR"
  exit 1
fi
if [ "$(id -u)" != "0" ]; then
  log "错误: 需要 root 权限，请使用: sudo bash $0"
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  log "错误: 缺少 curl，请先安装"
  exit 1
fi

# --------------------- 2. CentOS 7 EOL 源修复 ------------------------------
log "[2/9] 检测 yum 源状态"
NEED_FIX=0
if grep -rqE 'centos-vault|epel-archive' /etc/yum.repos.d/ 2>/dev/null; then
  log "  已使用归档/国内镜像源，跳过修复"
elif ! getent hosts mirrorlist.centos.org >/dev/null 2>&1; then
  log "  mirrorlist.centos.org 解析失败，判定 EOL 源失效，开始修复"
  NEED_FIX=1
elif ! curl -fsS --max-time 8 -o /dev/null "http://mirrorlist.centos.org/" 2>/dev/null; then
  log "  mirrorlist.centos.org 访问失败，开始修复"
  NEED_FIX=1
else
  log "  mirrorlist.centos.org 可访问，跳过修复"
fi

if [ "$NEED_FIX" = "1" ]; then
  # 确保 EPEL / SCLo repo 文件存在（cmake3/ninja 来自 EPEL，devtoolset-11 来自 SCLo）
  if [ ! -f /etc/yum.repos.d/epel.repo ]; then
    log "  创建 EPEL archive 源 (epel.repo)"
    write_epel_repo || exit 1
  fi
  if [ ! -f /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo ]; then
    log "  创建 SCLo rh 源 (CentOS-SCLo-scl-rh.repo)"
    cat > /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo <<EOF
[centos-sclo-rh]
name=CentOS-7 - SCLo rh (vault)
baseurl=${MIRROR_CANDIDATES[0]}/${VAULT_VERSION}/sclo/\$basearch/rh/
enabled=1
gpgcheck=0
EOF
  fi
  if [ ! -f /etc/yum.repos.d/CentOS-SCLo-scl.repo ]; then
    log "  创建 SCLo sclo 源 (CentOS-SCLo-scl.repo)"
    cat > /etc/yum.repos.d/CentOS-SCLo-scl.repo <<EOF
[centos-sclo-sclo]
name=CentOS-7 - SCLo sclo (vault)
baseurl=${MIRROR_CANDIDATES[0]}/${VAULT_VERSION}/sclo/\$basearch/sclo/
enabled=1
gpgcheck=0
EOF
  fi

  # 备份/还原: 每个 repo 只备份一次原始文件；切换镜像前从 .bak 还原，
  # 避免上次失败改动残留导致串镜像重试
  backup_repo_file() {
    local f="$1"
    [ -f "$f" ] || return 0
    [ -f "$f.bak" ] || cp -a "$f" "$f.bak" 2>/dev/null || true
  }
  restore_repo_backups() {
    local b
    for b in /etc/yum.repos.d/*.repo.bak; do
      [ -e "$b" ] || continue
      cp -a "$b" "${b%.bak}" 2>/dev/null || true
    done
  }

  # 修复已存在的 repo 文件: 注释 mirrorlist/metalink, baseurl 改写为归档镜像
  fix_repo_files() {
    local mirror="$1"
    # 先备份所有将被修改的 repo 文件（仅首次备份原始内容）
    backup_repo_file /etc/yum.repos.d/CentOS-Base.repo
    backup_repo_file /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo
    backup_repo_file /etc/yum.repos.d/CentOS-SCLo-scl.repo
    backup_repo_file /etc/yum.repos.d/epel.repo
    backup_repo_file /etc/yum.repos.d/CentOS-fasttrack.repo
    backup_repo_file /etc/yum.repos.d/CentOS-Debuginfo.repo
    backup_repo_file /etc/yum.repos.d/CentOS-Media.repo
    backup_repo_file /etc/yum.repos.d/CentOS-Vault.repo
    backup_repo_file /etc/yum.repos.d/CentOS-Sources.repo
    backup_repo_file /etc/yum.repos.d/CentOS-CR.repo
    # base/extras/updates（CentOS-Base.repo 原始 baseurl 为 "#baseurl=" 无空格）
    if [ -f /etc/yum.repos.d/CentOS-Base.repo ]; then
      sed -i -E 's|^mirrorlist=.*|# mirrorlist=&|; s|^#baseurl=http://mirror\.centos\.org/centos/\$releasever|baseurl='"$mirror"'/'"$VAULT_VERSION"'|' /etc/yum.repos.d/CentOS-Base.repo
    fi
    # SCLo rh/sclo: 注意原文件注释行是 "# baseurl=" （带空格），需兼容
    for f in /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo /etc/yum.repos.d/CentOS-SCLo-scl.repo; do
      if [ -f "$f" ]; then
        sed -i -E 's|^mirrorlist=.*|# mirrorlist=&|; s|^# *baseurl=http://mirror\.centos\.org/centos/7/sclo|baseurl='"$mirror"'/'"$VAULT_VERSION"'/sclo|' "$f"
      fi
    done
    # EPEL: 原始文件可能是 download.example 示例行（如
    #   #baseurl=http://download.example/pub/epel/7/$basearch）
    # 或 download.fedoraproject.org 行，sed 模式难以覆盖全部变体；
    # 直接重写整个文件为可用 archive 源，不再依赖 sed 匹配
    if [ -f /etc/yum.repos.d/epel.repo ]; then
      # 多候选探测可用镜像后重写；失败则保留现状，由 makecache 判定并
      # 进入下一个 CentOS 镜像候选（restore_repo_backups 会还原原始文件）
      write_epel_repo || true
    fi
    # 残留 repo（fasttrack/Debuginfo/Media/Vault/Sources/CR）: 注释失效
    # mirrorlist/metalink 并强制 enabled=0，避免它们干扰 makecache 判定
    for f in /etc/yum.repos.d/CentOS-fasttrack.repo /etc/yum.repos.d/CentOS-Debuginfo.repo /etc/yum.repos.d/CentOS-Media.repo /etc/yum.repos.d/CentOS-Vault.repo /etc/yum.repos.d/CentOS-Sources.repo /etc/yum.repos.d/CentOS-CR.repo; do
      if [ -f "$f" ]; then
        sed -i -E 's|^mirrorlist=.*|# mirrorlist=&|; s|^metalink=.*|# metalink=&|' "$f"
        sed -i -E 's|^enabled=1|enabled=0|' "$f"
      fi
    done
  }

  ok=0
  mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null || true
  for mirror in "${MIRROR_CANDIDATES[@]}"; do
    log "  尝试镜像源: $mirror"
    # 每次切换镜像前先从 .bak 还原 repo 文件，避免上次失败改动残留串镜像重试
    restore_repo_backups
    fix_repo_files "$mirror"
    yum clean all >/dev/null 2>&1 || true
    if yum makecache >/dev/null 2>>"$MAKECACHE_LOG"; then
      log "  EOL 源修复成功: $mirror"
      ok=1
      break
    fi
    log "  警告: $mirror makecache 失败（stderr 已写入 $MAKECACHE_LOG），尝试备选镜像"
  done
  if [ "$ok" != "1" ]; then
    log "错误: EOL 源修复失败，请手动检查 /etc/yum.repos.d/ 配置"
    exit 1
  fi
else
  # EPEL 自愈: 已使用归档源时也真实校验 epel.repo 是否可用。历史残留坏
  # 配置（metalink 被注释 + download.example 无效 baseurl，或清华
  # epel-archive 等实际 404 的镜像）仅靠字符串匹配无法识别，因此改为：
  # 取 baseurl 行 -> 替换 $basearch -> curl 探测 repodata/repomd.xml，
  # 返回 200 才视为可用，否则触发多候选重写。
  EPEL_BASEURL="$(sed -nE 's|^baseurl=(.*)$|\1|p' /etc/yum.repos.d/epel.repo 2>/dev/null | head -n1)"
  EPEL_OK=0
  if [ -n "$EPEL_BASEURL" ]; then
    ARCH_TMP="$(uname -m 2>/dev/null || echo x86_64)"
    EPEL_PROBE_URL="$(printf '%s' "$EPEL_BASEURL" | sed -e "s|\${basearch}|$ARCH_TMP|g" -e "s|\$basearch|$ARCH_TMP|g")"
    case "$EPEL_PROBE_URL" in
      */) EPEL_PROBE_URL="${EPEL_PROBE_URL}repodata/repomd.xml" ;;
      *) EPEL_PROBE_URL="${EPEL_PROBE_URL}/repodata/repomd.xml" ;;
    esac
    EPEL_CODE="$(curl -sS --max-time 8 -o /dev/null -w '%{http_code}' "$EPEL_PROBE_URL" 2>/dev/null || true)"
    if [ "$EPEL_CODE" = "200" ]; then
      EPEL_OK=1
    else
      log "  epel.repo baseurl 探测失败 (HTTP ${EPEL_CODE:-无响应}): $EPEL_PROBE_URL"
    fi
  fi
  if [ "$EPEL_OK" = "1" ]; then
    log "  epel.repo baseurl 探测通过，跳过重写"
  else
    log "  epel.repo 缺失或 baseurl 探测不可用，自动探测可用 EPEL 源并重写"
    write_epel_repo || log "  EPEL 自愈失败（所有候选不可用），可手动配置 /etc/yum.repos.d/epel.repo"
  fi
  yum clean all >/dev/null 2>&1 || true
  mkdir -p "$(dirname "$MAKECACHE_LOG")" 2>/dev/null || true
  yum makecache >/dev/null 2>>"$MAKECACHE_LOG" || log "  makecache 失败（stderr 已写入 $MAKECACHE_LOG），可手动执行 yum makecache 排查"
fi

# --------------------- 3. 工具链安装 (devtoolset-11/cmake3/ninja/Go) -------
log "[3/9] 工具链: devtoolset-11 / cmake3 / ninja / Go"
NEED_TC=0
if [ ! -f /opt/rh/devtoolset-11/enable ]; then NEED_TC=1; fi
if ! command -v cmake3 >/dev/null 2>&1; then NEED_TC=1; fi
if ! command -v ninja-build >/dev/null 2>&1; then NEED_TC=1; fi
if [ "$NEED_TC" = "1" ]; then
  log "  安装 devtoolset-11-gcc/gcc-c++ cmake3 ninja-build ..."
  yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++ cmake3 ninja-build
else
  log "  工具链已安装，跳过"
fi

# 启用 gcc 11 工具链（当前会话）并持久化到 ~/.bashrc
if [ -f /opt/rh/devtoolset-11/enable ]; then
  set +u
  # shellcheck disable=SC1091
  source /opt/rh/devtoolset-11/enable 2>/dev/null || true
  set -u
  if ! grep -q 'devtoolset-11/enable' ~/.bashrc 2>/dev/null; then
    echo 'source /opt/rh/devtoolset-11/enable' >> ~/.bashrc
    log "  已写入 ~/.bashrc（重新登录后自动启用 gcc 11）"
  fi
else
  log "警告: /opt/rh/devtoolset-11/enable 不存在，gcc 11 可能未就绪"
fi
GCC_VER="$(gcc --version 2>/dev/null | head -n1 || true)"
log "  gcc: $GCC_VER"
if echo "$GCC_VER" | grep -qv ' 11\.'; then
  log "  警告: 未检测到 gcc 11，请确认 devtoolset-11 已启用"
fi

# Go 工具链（go.mod 要求 go1.22；仓库 Dockerfile 使用 golang:1.27.1-bookworm）
export PATH="$PATH:/usr/local/go/bin:$HOME/go/bin"
GO_INSTALL=0
if ! command -v go >/dev/null 2>&1; then
  GO_INSTALL=1
else
  GO_MINOR="$(go version 2>/dev/null | grep -oE 'go1\.[0-9]+' | cut -d. -f2 || true)"
  if [ -z "$GO_MINOR" ] || [ "$GO_MINOR" -lt 22 ]; then GO_INSTALL=1; fi
fi
if [ "$GO_INSTALL" = "1" ]; then
  log "  安装 Go ${GO_VERSION} ..."
  GO_TGZ="/tmp/go${GO_VERSION}.linux-amd64.tar.gz"
  if ! curl -fSL --max-time 300 -o "$GO_TGZ" "https://mirrors.tuna.tsinghua.edu.cn/golang/go${GO_VERSION}.linux-amd64.tar.gz"; then
    curl -fSL --max-time 300 -o "$GO_TGZ" "https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz"
  fi
  rm -rf /usr/local/go
  tar -C /usr/local -xzf "$GO_TGZ"
  rm -f "$GO_TGZ"
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
  log "  cmake3 配置（Release, C++20, 关闭 tests/tools/benchmark/jni/压缩库）..."
  cmake3 -S "$ROCKSDB_SRC" -B "$ROCKSDB_BUILD" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_STANDARD=20 \
    -DWITH_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF -DWITH_JNI=OFF \
    -DWITH_GFLAGS=OFF \
    -DROCKSDB_BUILD_SHARED=OFF \
    -DWITH_ZLIB=OFF -DWITH_BZ2=OFF -DWITH_LZ4=OFF -DWITH_SNAPPY=OFF -DWITH_ZSTD=OFF \
    -DFAIL_ON_WARNINGS=OFF
  log "  ninja 编译（并行 $(nproc)）..."
  cmake3 --build "$ROCKSDB_BUILD" -j"$(nproc)"
fi
if [ ! -f "$ROCKSDB_BUILD/librocksdb.a" ]; then
  log "错误: librocksdb.a 未生成: $ROCKSDB_BUILD"
  exit 1
fi
log "  RocksDB 静态库: $ROCKSDB_BUILD/librocksdb.a"

# --------------------- 5. OpenXDB 源码编译 --------------------------------
log "[5/9] OpenXDB 源码编译"
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
# 与 scripts/build.sh 一致: CGO_ENABLED=1, CGO_CFLAGS/CGO_CXXFLAGS=-I<rocksdb>/include,
# CGO_LDFLAGS 用 -l:librocksdb.a 强制静态链接（避免优先选 librocksdb.so.11 被动态链接），
# 并保留 -lstdc++ -lpthread 满足 RocksDB 静态库对 C++ 运行时与线程库的依赖
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
log "  openxdb 二进制: $BIN_DIR/openxdb"

# --------------------- 6. 生成服务配置 + 初始化数据目录 --------------------
# 注意：OpenXDB 有两份独立 conf，路径不得重合：
#   * 服务配置（--config 加载，[server] 段合法键）：本文件 CONF_FILE
#   * 数据目录 db conf（$DATA_DIR/openxdb.conf，由 openxdb init 生成，
#     含 "engine = rocksdb" 供 pkg/db/db.go Open 校验）——engine 不能出现在
#     服务配置的 [server] 段（configfile 拒绝 unknown key），反之服务配置
#     不能占用数据目录同名文件（db.Open 读到缺 engine 报 unsupported engine）
log "[6/9] 生成服务配置 + 初始化数据目录"
mkdir -p "$(dirname "$CONF_FILE")" "$DATA_DIR" "$DATA_DIR/backups" "$(dirname "$LOG_FILE")"
cat > "$CONF_FILE" <<EOF
# OpenXDB single-node config (generated by install_openxdb_centos7.sh)
[server]
data_dir = $DATA_DIR
port = $PORT
replica_port = $REPLICA_PORT
metrics_port = $METRICS_PORT
mysql_port = $MYSQL_PORT
EOF
log "  服务配置: $CONF_FILE"
log "  端口布局: port=$PORT replica_port=$REPLICA_PORT metrics_port=$METRICS_PORT mysql_port=$MYSQL_PORT"
log "  数据目录: $DATA_DIR"
DB_CONF="$DATA_DIR/openxdb.conf"
if [ -f "$DB_CONF" ] && grep -q '^engine = rocksdb$' "$DB_CONF"; then
  log "  数据目录已初始化（$DB_CONF 含 engine = rocksdb），跳过 init"
else
  if [ -f "$DB_CONF" ]; then
    log "  $DB_CONF 非 db 格式（旧脚本残留），备份后重新初始化"
    mv "$DB_CONF" "$DB_CONF.bak.$$"
  fi
  "$BIN_DIR/openxdb" init --data-dir "$DATA_DIR"
  log "  数据目录已初始化: $DATA_DIR"
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

# --------------------- 8. 验证: 端口 / doctor / 冒烟 ----------------------
log "[8/9] 验证"
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
log " OpenXDB 单机部署完成"
log "  二进制:   $BIN_DIR/openxdb"
log "  数据目录: $DATA_DIR"
log "  配置文件: $CONF_FILE (需 --config 显式加载)"
log "  端口:     $PORT (SQL) / $REPLICA_PORT (replica) / $METRICS_PORT (metrics) / $MYSQL_PORT (mysql wire)"
log "  日志:     $LOG_FILE"
log "  启动:     nohup $BIN_DIR/openxdb start --config $CONF_FILE >> $LOG_FILE 2>&1 &"
log "  停止:     kill \$(pgrep openxdb)（或见 openxdb 命令帮助）"
log "============================================================="

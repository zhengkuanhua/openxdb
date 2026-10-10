#!/usr/bin/env bash
# deploy.sh - OpenXDB 一键部署脚本（T20 运维工具链）
#
# 用法：
#   ./deploy.sh -DataDir <dir> [-Port <n>] [-Bin <path>] [-ReplicaPort <n>] [-ClusterAddr <addr>] [-NoStart] [-Help]
#
# 功能：
#   1) 初始化数据目录（openxdb init --data-dir）
#   2) 启动服务（openxdb start --data-dir --port，端口默认 7788）
#   3) 健康自检（openxdb doctor --data-dir --addr，等待服务就绪）
#   4) 输出部署摘要（PID / 端口 / 数据目录 / binlog 位点）
#
# 示例：
#   ./deploy.sh -DataDir /data/openxdb -Port 7788
set -euo pipefail

usage() {
  cat <<'EOF'
OpenXDB 一键部署脚本 deploy.sh（T20）

用法:
  ./deploy.sh -DataDir <dir> [-Port <n>] [-Bin <path>]
              [-ReplicaPort <n>] [-ClusterAddr <addr>] [-NoStart]

参数:
  -DataDir      数据目录（必填）
  -Port         服务监听端口，默认 7788
  -Bin          openxdb 可执行文件路径（默认 PATH 中的 openxdb）
  -ReplicaPort  主节点复制端口（可选，启用 M2 复制）
  -ClusterAddr  集群节点链路地址（可选，启用 M4 节点链路）
  -NoStart      仅初始化与检查，不启动服务

输出: 部署摘要（PID / 端口 / 数据目录 / binlog 位点）
EOF
}

DataDir=""
Port=7788
Bin=""
ReplicaPort=0
ClusterAddr=""
NoStart=0

while [ $# -gt 0 ]; do
  case "$1" in
    -DataDir) DataDir="$2"; shift 2 ;;
    -Port) Port="$2"; shift 2 ;;
    -Bin) Bin="$2"; shift 2 ;;
    -ReplicaPort) ReplicaPort="$2"; shift 2 ;;
    -ClusterAddr) ClusterAddr="$2"; shift 2 ;;
    -NoStart) NoStart=1; shift ;;
    -Help|-h) usage; exit 0 ;;
    *) echo "未知参数: $1"; usage; exit 1 ;;
  esac
done

if [ -z "$DataDir" ]; then
  echo "错误: 缺少必填参数 -DataDir <dir>" >&2
  usage >&2
  exit 1
fi

EXE="${Bin:-openxdb}"

# ---------- 1) 初始化数据目录 ----------
echo "[deploy] 初始化数据目录: $DataDir"
"$EXE" init --data-dir "$DataDir"
if [ $? -ne 0 ]; then
  echo "[deploy] 初始化失败" >&2
  exit 1
fi

if [ "$NoStart" -eq 1 ]; then
  echo "[deploy] -NoStart 指定，跳过启动；数据目录已就绪: $DataDir"
  exit 0
fi

# ---------- 2) 启动服务 ----------
mkdir -p "$DataDir"
STDOUT_LOG="$DataDir/deploy.stdout.log"
STDERR_LOG="$DataDir/deploy.stderr.log"
ARGS=(start --data-dir "$DataDir" --port "$Port")
if [ "$ReplicaPort" -gt 0 ]; then ARGS+=(--replica-port "$ReplicaPort"); fi
if [ -n "$ClusterAddr" ]; then ARGS+=(--cluster-addr "$ClusterAddr"); fi

echo "[deploy] 启动服务: $EXE ${ARGS[*]}"
nohup "$EXE" "${ARGS[@]}" >"$STDOUT_LOG" 2>"$STDERR_LOG" &
SVC_PID=$!
echo "[deploy] 服务进程 PID: $SVC_PID"

# ---------- 3) 健康自检（等待就绪 + doctor） ----------
ADDR="127.0.0.1:$Port"
READY=0
for i in $(seq 1 40); do
  sleep 0.5
  if (echo > /dev/tcp/127.0.0.1/"$Port") 2>/dev/null; then
    READY=1
    break
  fi
  if ! kill -0 "$SVC_PID" 2>/dev/null; then
    break
  fi
done
if [ "$READY" -ne 1 ]; then
  echo "[deploy] 服务未在 $Port 端口就绪" >&2
  exit 1
fi
echo "[deploy] 端口就绪: $ADDR"

"$EXE" doctor --data-dir "$DataDir" --addr "$ADDR"
DOCTOR_RC=$?
if [ "$DOCTOR_RC" -ge 2 ]; then
  echo "[deploy] 健康自检失败 (doctor exit $DOCTOR_RC)" >&2
  exit 1
fi
echo "[deploy] 健康自检通过 (doctor exit $DOCTOR_RC)"

# ---------- 4) 部署摘要 ----------
echo ""
echo "=== OpenXDB 部署摘要 ==="
echo "PID:          $SVC_PID"
echo "端口:         $Port"
echo "数据目录:     $DataDir"
echo "stdout 日志:  $STDOUT_LOG"
echo "stderr 日志:  $STDERR_LOG"
echo "状态:         运行中 (doctor 通过)"
exit 0

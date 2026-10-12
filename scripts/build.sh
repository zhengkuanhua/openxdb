#!/usr/bin/env bash
# OpenXDB 可复现构建脚本（Linux/macOS，P0-1）
# 用法:
#   ./scripts/build.sh                          # 自动探测 RocksDB（pkg-config 或参数）
#   ./scripts/build.sh --rocks-src /path/to/rocksdb-11.8.1 --rocks-build /path/to/librocksdb-dir
#   ./scripts/build.sh --test                   # 构建后跑 go test ./...
#   ./scripts/build.sh --skip-all-build         # 仅设置 CGO 环境并打印，供手动 go build
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROCKS_SRC=""
ROCKS_BUILD=""
OUT_DIR=""
RUN_TESTS=0
SKIP_BUILD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rocks-src) ROCKS_SRC="$2"; shift 2 ;;
    --rocks-build) ROCKS_BUILD="$2"; shift 2 ;;
    --out-dir) OUT_DIR="$2"; shift 2 ;;
    --test) RUN_TESTS=1; shift ;;
    --skip-all-build) SKIP_BUILD=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$ROCKS_SRC" ]]; then
  if [[ -n "${ROCKSDB_SRC:-}" ]]; then
    ROCKS_SRC="$ROCKSDB_SRC"
  elif pkg-config --exists rocksdb 2>/dev/null; then
    ROCKS_SRC="$(pkg-config --variable=includedir rocksdb)/.."
  fi
fi
if [[ -z "$ROCKS_BUILD" ]]; then
  if [[ -n "${ROCKSDB_BUILD:-}" ]]; then
    ROCKS_BUILD="$ROCKSDB_BUILD"
  elif pkg-config --exists rocksdb 2>/dev/null; then
    ROCKS_BUILD="$(pkg-config --variable=libdir rocksdb)"
  fi
fi

if [[ -z "$ROCKS_SRC" || ! -d "$ROCKS_SRC/include/rocksdb" || -z "$(ls "$ROCKS_SRC/include/rocksdb"/*.h 2>/dev/null)" ]]; then
  echo "RocksDB source include not found (missing include/rocksdb/*.h); pass --rocks-src or set ROCKSDB_SRC" >&2
  exit 2
fi
if [[ -z "$ROCKS_BUILD" ]]; then
  ROCKS_BUILD="$(find "$ROCKS_SRC" -name 'librocksdb*.a' 2>/dev/null | head -n1 | xargs -r dirname)"
fi
if [[ -z "$ROCKS_BUILD" || -z "$(ls "$ROCKS_BUILD"/librocksdb*.a 2>/dev/null)" ]]; then
  echo "RocksDB static library not found; pass --rocks-build or set ROCKSDB_BUILD" >&2
  exit 2
fi

echo "[build] RocksDB src   = $ROCKS_SRC"
echo "[build] RocksDB build = $ROCKS_BUILD"
export CGO_ENABLED=1
export CGO_CFLAGS="-I$ROCKS_SRC/include"
export CGO_CXXFLAGS="-I$ROCKS_SRC/include"
export CGO_LDFLAGS="-L$ROCKS_BUILD -lrocksdb"

if [[ "$SKIP_BUILD" == "1" ]]; then
  echo "[build] CGO_CFLAGS=$CGO_CFLAGS"
  echo "[build] CGO_LDFLAGS=$CGO_LDFLAGS"
  echo "[build] environment ready (skip build)"
  exit 0
fi

if [[ -z "$OUT_DIR" ]]; then OUT_DIR="$REPO/bin"; fi
mkdir -p "$OUT_DIR"
BIN="$OUT_DIR/openxdb"
[[ "$(uname -s)" == "Darwin" ]] && BIN="$OUT_DIR/openxdb-darwin"

echo "[build] go build ./cmd/openxdb -> $BIN"
(cd "$REPO" && go build -o "$BIN" ./cmd/openxdb)
echo "[build] OK: $BIN"

echo "[build] go build ./... (verify whole repo)"
(cd "$REPO" && go build ./...)
echo "[build] OK: go build ./..."

if [[ "$RUN_TESTS" == "1" ]]; then
  echo "[build] go test ./..."
  (cd "$REPO" && go test ./...)
  echo "[build] OK: go test ./..."
fi

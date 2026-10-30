# syntax=docker/dockerfile:1
# OpenXDB v1.17.0 —— 多阶段构建镜像
# 阶段一：RocksDB 11.8.1 静态库（源码编译，参数与 CI / build.sh 一致，无三方压缩库）+ Go 工具链构建二进制
# 阶段二：Debian slim 运行时，仅含 openxdb 二进制

FROM golang:1.27.1-bookworm AS builder

# 编译工具链
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# RocksDB v11.8.1（静态库、关闭测试/工具/压缩库依赖）
RUN git clone --depth 1 --branch v11.8.1 https://github.com/facebook/rocksdb.git /tmp/rocksdb-src \
    && cmake -S /tmp/rocksdb-src -B /tmp/rocksdb-build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CXX_STANDARD=20 \
        -DCMAKE_CXX_FLAGS="-Wno-error=maybe-uninitialized" \
        -DROCKSDB_BUILD_SHARED=OFF \
        -DWITH_TESTS=OFF \
        -DWITH_TOOLS=OFF \
        -DWITH_BENCHMARK_TOOLS=OFF \
        -DWITH_JNI=OFF \
        -DWITH_GFLAGS=OFF \
        -DWITH_ZLIB=OFF \
        -DWITH_LZ4=OFF \
        -DWITH_SNAPPY=OFF \
        -DWITH_ZSTD=OFF \
        -DWITH_BZ2=OFF \
    && cmake --build /tmp/rocksdb-build -j$(nproc)

# 复制源码并构建 openxdb
WORKDIR /src
COPY . .
ENV CGO_ENABLED=1 \
    CGO_CFLAGS=-I/tmp/rocksdb-src/include \
    CGO_CXXFLAGS=-I/tmp/rocksdb-src/include \
    CGO_LDFLAGS="-L/tmp/rocksdb-build -lrocksdb"
RUN go build -o /out/openxdb ./cmd/openxdb

# 运行时镜像
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/openxdb /usr/local/bin/openxdb
WORKDIR /data
VOLUME ["/data"]
EXPOSE 7788
ENTRYPOINT ["openxdb"]
CMD ["--help"]

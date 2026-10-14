// E2 存储引擎同步写基准（fsync 版，M0-lite 实验）
// 编译: g++ -std=c++20 -O2 -static e2_fsync_bench.cpp -I<rocksdb_include> -L<rocksdb_lib> -lrocksdb -pthread -lshlwapi -lrpcrt4 -lws2_32 -o e2_fsync_bench.exe
// 运行: e2_fsync_bench.exe --keys 1000000 --dir e2_fsync_bench
//
// 与 e1_bench.cpp（E1a，无 fsync 纯引擎吞吐）同口径对照：
// 唯一差异 = WriteOptions.sync=true + WAL 开启（RocksDB 默认，不设 disableWAL），
// 每组 1000 条 WriteBatch 提交时强制 fsync WAL，模拟真实同步写持久化路径。
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

using Clock = std::chrono::steady_clock;

static std::string MakeKey(uint64_t i) {
    char buf[17];
    std::snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)i);
    return std::string(buf);
}

static std::string MakeValue(uint64_t i) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "value-%016llx-%016llx",
                  (unsigned long long)i,
                  (unsigned long long)(i * 2654435761ULL));
    return std::string(buf);
}

int main(int argc, char** argv) {
    uint64_t n = 1000000;
    int batch = 1000;
    const char* data_dir = "e2_fsync_bench";
    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "--keys") == 0 && i + 1 < argc) {
            n = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (std::strcmp(argv[i], "--batch") == 0 && i + 1 < argc) {
            batch = std::atoi(argv[++i]);
        }
    }

    rocksdb::Options opt;
    opt.create_if_missing = true;
    opt.max_background_jobs = 4;
    opt.write_buffer_size = 64 * 1024 * 1024;
    // WAL 保持 RocksDB 默认开启（与 SQL 层 db.Open / bridge.cc 路径一致），
    // 同步语义由 WriteOptions.sync 控制：sync=true 时每组 Write 提交后 fsync WAL。
    std::unique_ptr<rocksdb::DB> db;
    auto st = rocksdb::DB::Open(opt, data_dir, &db);
    if (!st.ok()) {
        std::fprintf(stderr, "open failed: %s\n", st.ToString().c_str());
        return 1;
    }

    // 1) 顺序写（同步：每批 WriteBatch 提交后 fsync WAL）
    {
        auto t0 = Clock::now();
        rocksdb::WriteOptions wo;
        wo.sync = true;
        rocksdb::WriteBatch wb;
        for (uint64_t i = 0; i < n; i++) {
            wb.Put(MakeKey(i), MakeValue(i));
            if (wb.Count() >= batch) {
                db->Write(wo, &wb);
                wb.Clear();
            }
        }
        if (wb.Count() > 0) db->Write(wo, &wb);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
        std::printf("seq_write_sync: %llu keys, %lld ms, %.0f TPS (batch=%d fsync/批)\n",
                    (unsigned long long)n, (long long)ms, n * 1000.0 / ms, batch);
    }

    // 2) 随机写（同步：每批 WriteBatch 提交后 fsync WAL）
    {
        std::mt19937_64 rng(42);
        auto t0 = Clock::now();
        rocksdb::WriteOptions wo;
        wo.sync = true;
        rocksdb::WriteBatch wb;
        for (uint64_t i = 0; i < n; i++) {
            uint64_t k = rng() % n;
            wb.Put(MakeKey(k), MakeValue(k));
            if (wb.Count() >= batch) {
                db->Write(wo, &wb);
                wb.Clear();
            }
        }
        if (wb.Count() > 0) db->Write(wo, &wb);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
        std::printf("rand_write_sync: %llu keys, %lld ms, %.0f TPS (batch=%d fsync/批)\n",
                    (unsigned long long)n, (long long)ms, n * 1000.0 / ms, batch);
    }

    // 3) 随机点查（读无 fsync 语义，与 E1a 同口径，记录 P99 延迟）
    {
        std::mt19937_64 rng(7);
        const uint64_t reads = n;
        std::vector<uint64_t> lat(reads);
        auto t0 = Clock::now();
        rocksdb::ReadOptions ro;
        for (uint64_t i = 0; i < reads; i++) {
            uint64_t k = rng() % n;
            std::string v;
            auto s1 = Clock::now();
            db->Get(ro, MakeKey(k), &v);
            auto s2 = Clock::now();
            lat[i] = std::chrono::duration_cast<std::chrono::microseconds>(s2 - s1).count();
        }
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
        std::sort(lat.begin(), lat.end());
        uint64_t p99 = lat[reads * 99 / 100];
        std::printf("rand_get: %llu reads, %lld ms, %.0f QPS, P99 %llu us\n",
                    (unsigned long long)reads, (long long)ms, reads * 1000.0 / ms,
                    (unsigned long long)p99);
    }

    db.reset();
    std::printf("E2 FSYNC BENCH DONE\n");
    return 0;
}

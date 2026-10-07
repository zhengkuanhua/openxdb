// E1 存储引擎写读基准（RocksDB 初版，M0-lite 实验）
// 编译: g++ -std=c++20 -O2 -static e1_bench.cpp -I<rocksdb_include> -L<rocksdb_lib> -lrocksdb -pthread -lshlwapi -lrpcrt4 -lws2_32 -o e1_bench.exe
// 运行: e1_bench.exe --keys 1000000 --dir e1_rocksdb_bench
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
    const char* data_dir = "e1_rocksdb_bench";
    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "--keys") == 0 && i + 1 < argc) {
            n = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        }
    }

    rocksdb::Options opt;
    opt.create_if_missing = true;
    opt.max_background_jobs = 4;
    opt.write_buffer_size = 64 * 1024 * 1024;
    std::unique_ptr<rocksdb::DB> db;
    auto st = rocksdb::DB::Open(opt, data_dir, &db);
    if (!st.ok()) {
        std::fprintf(stderr, "open failed: %s\n", st.ToString().c_str());
        return 1;
    }

    // 1) 顺序写（批量 1000，组提交）
    {
        auto t0 = Clock::now();
        rocksdb::WriteOptions wo;
        rocksdb::WriteBatch wb;
        for (uint64_t i = 0; i < n; i++) {
            wb.Put(MakeKey(i), MakeValue(i));
            if (wb.Count() >= 1000) {
                db->Write(wo, &wb);
                wb.Clear();
            }
        }
        if (wb.Count() > 0) db->Write(wo, &wb);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
        std::printf("seq_write: %llu keys, %lld ms, %.0f TPS\n",
                    (unsigned long long)n, (long long)ms, n * 1000.0 / ms);
    }

    // 2) 随机写（批量 1000）
    {
        std::mt19937_64 rng(42);
        auto t0 = Clock::now();
        rocksdb::WriteOptions wo;
        rocksdb::WriteBatch wb;
        for (uint64_t i = 0; i < n; i++) {
            uint64_t k = rng() % n;
            wb.Put(MakeKey(k), MakeValue(k));
            if (wb.Count() >= 1000) {
                db->Write(wo, &wb);
                wb.Clear();
            }
        }
        if (wb.Count() > 0) db->Write(wo, &wb);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
        std::printf("rand_write: %llu keys, %lld ms, %.0f TPS\n",
                    (unsigned long long)n, (long long)ms, n * 1000.0 / ms);
    }

    // 3) 随机点查（记录 P99 延迟）
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
    std::printf("E1A BENCH DONE\n");
    return 0;
}

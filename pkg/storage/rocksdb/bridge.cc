// bridge.cc - RocksDB C ABI 桥（M1 T1，供 Go cgo 调用）。
#include "bridge.h"

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/snapshot.h>
#include <rocksdb/write_batch.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {

inline char* dup_status(const rocksdb::Status& st) {
    std::string s = st.ToString();
    char* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (!p) return p;
    std::memcpy(p, s.c_str(), s.size());
    p[s.size()] = '\0';
    return p;
}

}  // namespace

struct xdb {
    rocksdb::DB* db;
};

// v1.17.0 D1.2：RocksDB 自身 WAL 开关（全局）。默认启用（disable_wal=false，
// 与 v1.16.0 行为一致零回归）。autocommit 写 pipeline 开启时由 Go 侧
// xdb_set_no_wal(1) 关闭——OpenXDB WAL 成为唯一持久化通道，崩溃恢复完全由
// OpenXDB WAL 重放决定，消除延迟 fsync 窗口下"RocksDB 自身 WAL 恢复出
// OpenXDB WAL 未记录事务"的幽灵数据风险。
static int g_disable_wal = 0;

// xscan：范围扫描结果。C5 优化：数据集中放在单块 arena（连续内存），
// items 各数组记录每行 key/value 在 arena 中的偏移与长度。
// 相比旧实现（std::vector<std::pair<std::string,std::string>> + xscan_item 逐行
// malloc/memcpy），消除每行 ToString 的 std::string 分配与逐行 2 次 malloc，
// 扫描结果只需一次大块分配 + 每行 2 次 memcpy（RocksDB 内部 buffer -> arena），
// 再由 Go 侧 C.GoBytes 拷贝到 Go heap，拷贝链从 3 次降为 2 次。
struct xscan {
    unsigned char* arena;  // 连续数据块：[k0][v0][k1][v1]...，malloc 分配，整体释放
    size_t arena_len;      // arena 实际使用字节数
    size_t* koffs;         // 每行 key 在 arena 中的偏移（n 项）
    size_t* klens;         // 每行 key 长度（n 项）
    size_t* voffs;         // 每行 value 在 arena 中的偏移（n 项）
    size_t* vlens;         // 每行 value 长度（n 项）
    size_t n;              // 条目数
};

struct xsnap {
    rocksdb::DB* db;
    const rocksdb::Snapshot* snap;
};

extern "C" {

xdb* xdb_open(const char* dir, int create_if_missing, char** errmsg) {
    rocksdb::Options opt;
    opt.create_if_missing = create_if_missing != 0;
    opt.max_background_jobs = 4;
    opt.write_buffer_size = 64 * 1024 * 1024;
    std::unique_ptr<rocksdb::DB> db;
    auto st = rocksdb::DB::Open(opt, dir, &db);
    if (!st.ok()) {
        if (errmsg) *errmsg = dup_status(st);
        return nullptr;
    }
    auto* h = new xdb;
    h->db = db.release();
    return h;
}

void xdb_close(xdb* h) {
    if (!h) return;
    delete h->db;
    delete h;
}

int xdb_get(xdb* h, const unsigned char* key, size_t klen,
            unsigned char** val, size_t* vlen, char** errmsg) {
    rocksdb::ReadOptions ro;
    std::string v;
    auto st = h->db->Get(ro, rocksdb::Slice(reinterpret_cast<const char*>(key), klen), &v);
    if (st.IsNotFound()) return 1;
    if (!st.ok()) {
        if (errmsg) *errmsg = dup_status(st);
        return -1;
    }
    *vlen = v.size();
    *val = static_cast<unsigned char*>(std::malloc(v.empty() ? 1 : v.size()));
    if (!v.empty()) std::memcpy(*val, v.data(), v.size());
    return 0;
}

int xdb_put(xdb* h, const unsigned char* key, size_t klen,
            const unsigned char* val, size_t vlen, char** errmsg) {
    rocksdb::WriteOptions wo;
    wo.disableWAL = g_disable_wal != 0;
    auto st = h->db->Put(wo, rocksdb::Slice(reinterpret_cast<const char*>(key), klen),
                         rocksdb::Slice(reinterpret_cast<const char*>(val), vlen));
    if (!st.ok()) {
        if (errmsg) *errmsg = dup_status(st);
        return -1;
    }
    return 0;
}

int xdb_delete(xdb* h, const unsigned char* key, size_t klen, char** errmsg) {
    rocksdb::WriteOptions wo;
    wo.disableWAL = g_disable_wal != 0;
    auto st = h->db->Delete(wo, rocksdb::Slice(reinterpret_cast<const char*>(key), klen));
    if (!st.ok()) {
        if (errmsg) *errmsg = dup_status(st);
        return -1;
    }
    return 0;
}

// xdb_set_no_wal 切换 RocksDB 自身 WAL（v1.17.0 D1.2）：v!=0 禁用（disable_wal），
// v==0 恢复默认启用。全局生效（所有连接/句柄的后续写入）。pipeline 窗口开启
// 时须先禁用，批尾 FlushDeferredSync 后恢复。
int xdb_set_no_wal(int v) {
    g_disable_wal = v != 0;
    return 0;
}

int xdb_write_batch(xdb* h,
                    const unsigned char* pairs, const size_t* put_sizes, int nputs,
                    const unsigned char* delkeys, const size_t* del_sizes, int ndeletes,
                    char** errmsg) {
    rocksdb::WriteOptions wo;
    wo.disableWAL = g_disable_wal != 0;
    rocksdb::WriteBatch wb;
    size_t off = 0;
    for (int i = 0; i < nputs; i++) {
        size_t klen = put_sizes[i * 2];
        size_t vlen = put_sizes[i * 2 + 1];
        wb.Put(rocksdb::Slice(reinterpret_cast<const char*>(pairs + off), klen),
               rocksdb::Slice(reinterpret_cast<const char*>(pairs + off + klen), vlen));
        off += klen + vlen;
    }
    off = 0;
    for (int i = 0; i < ndeletes; i++) {
        size_t klen = del_sizes[i];
        wb.Delete(rocksdb::Slice(reinterpret_cast<const char*>(delkeys + off), klen));
        off += klen;
    }
    auto st = h->db->Write(wo, &wb);
    if (!st.ok()) {
        if (errmsg) *errmsg = dup_status(st);
        return -1;
    }
    return 0;
}

// 范围扫描 [start,end)，limit<=0 不限。C5 优化：两趟迭代，第一趟只统计
// 条目数与总字节（不拷贝），第二趟一次性 memcpy 到单块 arena；
// 消除旧实现每行 k.ToString()/value().ToString() 的 std::string 分配与
// xscan_item 逐行 2 次 malloc+memcpy，拷贝链 3 次降为 2 次。
static xscan* do_scan(rocksdb::DB* db, rocksdb::ReadOptions ro,
                      const unsigned char* start, size_t slen,
                      const unsigned char* end, size_t elen,
                      int limit, char** errmsg) {
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ro));
    if (start) {
        it->Seek(rocksdb::Slice(reinterpret_cast<const char*>(start), slen));
    } else {
        it->SeekToFirst();
    }
    auto* out = new xscan;
    out->arena = nullptr;
    out->arena_len = 0;
    out->koffs = out->klens = out->voffs = out->vlens = nullptr;
    out->n = 0;

    // 第一趟：统计 n 与总字节数，不拷贝数据。
    size_t total = 0;
    size_t count = 0;
    for (; it->Valid(); it->Next()) {
        rocksdb::Slice k = it->key();
        if (end && k.compare(rocksdb::Slice(reinterpret_cast<const char*>(end), elen)) >= 0) break;
        total += k.size() + it->value().size();
        count++;
        if (limit > 0 && count >= static_cast<size_t>(limit)) break;
    }
    if (!it->status().ok()) {
        if (errmsg) *errmsg = dup_status(it->status());
        delete out;
        return nullptr;
    }

    out->n = count;
    if (count == 0) return out;

    out->arena = static_cast<unsigned char*>(std::malloc(total == 0 ? 1 : total));
    out->koffs = static_cast<size_t*>(std::malloc(count * sizeof(size_t)));
    out->klens = static_cast<size_t*>(std::malloc(count * sizeof(size_t)));
    out->voffs = static_cast<size_t*>(std::malloc(count * sizeof(size_t)));
    out->vlens = static_cast<size_t*>(std::malloc(count * sizeof(size_t)));
    if (!out->arena || !out->koffs || !out->klens || !out->voffs || !out->vlens) {
        if (errmsg) *errmsg = dup_status(rocksdb::Status::IOError("xscan arena alloc failed"));
        xscan_free(out);
        return nullptr;
    }

    // 第二趟：从起点重新迭代，直接 memcpy 到 arena。
    if (start) {
        it->Seek(rocksdb::Slice(reinterpret_cast<const char*>(start), slen));
    } else {
        it->SeekToFirst();
    }
    size_t off = 0;
    size_t i = 0;
    for (; it->Valid(); it->Next()) {
        rocksdb::Slice k = it->key();
        if (end && k.compare(rocksdb::Slice(reinterpret_cast<const char*>(end), elen)) >= 0) break;
        rocksdb::Slice v = it->value();
        out->koffs[i] = off;
        out->klens[i] = k.size();
        if (!k.empty()) std::memcpy(out->arena + off, k.data(), k.size());
        off += k.size();
        out->voffs[i] = off;
        out->vlens[i] = v.size();
        if (!v.empty()) std::memcpy(out->arena + off, v.data(), v.size());
        off += v.size();
        i++;
        if (i >= count) break;
    }
    out->arena_len = off;
    // 以第二趟实际拷贝行数为准：无快照实时视图在并发写下两趟行数可能不同，
    // 若仍用第一趟 count 会让 Go 侧读到未初始化数组项（崩溃/脏数据风险）。
    out->n = i;
    if (!it->status().ok()) {
        if (errmsg) *errmsg = dup_status(it->status());
        xscan_free(out);
        return nullptr;
    }
    return out;
}

// 范围计数：与 do_scan 同语义（[start,end)，limit<=0 不限），但只数 key，
// 不拷贝 value、不 ToString、不 malloc，避免全量物化。
static int do_count(rocksdb::DB* db, rocksdb::ReadOptions ro,
                    const unsigned char* start, size_t slen,
                    const unsigned char* end, size_t elen,
                    int limit, char** errmsg) {
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ro));
    if (start) {
        it->Seek(rocksdb::Slice(reinterpret_cast<const char*>(start), slen));
    } else {
        it->SeekToFirst();
    }
    int count = 0;
    for (; it->Valid(); it->Next()) {
        rocksdb::Slice k = it->key();
        if (end && k.compare(rocksdb::Slice(reinterpret_cast<const char*>(end), elen)) >= 0) break;
        count++;
        if (limit > 0 && count >= limit) break;
    }
    if (!it->status().ok()) {
        if (errmsg) *errmsg = dup_status(it->status());
        return -1;
    }
    return count;
}

xscan* xdb_scan(xdb* h, const unsigned char* start, size_t slen,
                const unsigned char* end, size_t elen, int limit, char** errmsg) {
    rocksdb::ReadOptions ro;
    return do_scan(h->db, ro, start, slen, end, elen, limit, errmsg);
}

int xdb_count(xdb* h, const unsigned char* start, size_t slen,
              const unsigned char* end, size_t elen, int limit, char** errmsg) {
    rocksdb::ReadOptions ro;
    return do_count(h->db, ro, start, slen, end, elen, limit, errmsg);
}

int xscan_count(xscan* s) { return static_cast<int>(s->n); }

// 返回第 i 行 key/value 在 arena 中的指针与长度（0 拷贝，不 malloc）。
int xscan_item(xscan* s, int i, unsigned char** key, size_t* klen,
               unsigned char** val, size_t* vlen) {
    if (i < 0 || i >= static_cast<int>(s->n)) return -1;
    *klen = s->klens[i];
    *vlen = s->vlens[i];
    *key = s->arena + s->koffs[i];
    *val = s->arena + s->voffs[i];
    return 0;
}

void xscan_free(xscan* s) {
    if (!s) return;
    std::free(s->arena);
    std::free(s->koffs);
    std::free(s->klens);
    std::free(s->voffs);
    std::free(s->vlens);
    delete s;
}

xsnap* xdb_snapshot_create(xdb* h) {
    auto* s = new xsnap;
    s->db = h->db;
    s->snap = h->db->GetSnapshot();
    return s;
}

int xdb_snapshot_get(xsnap* s, const unsigned char* key, size_t klen,
                     unsigned char** val, size_t* vlen, char** errmsg) {
    rocksdb::ReadOptions ro;
    ro.snapshot = s->snap;
    std::string v;
    auto st = s->db->Get(ro, rocksdb::Slice(reinterpret_cast<const char*>(key), klen), &v);
    if (st.IsNotFound()) return 1;
    if (!st.ok()) {
        if (errmsg) *errmsg = dup_status(st);
        return -1;
    }
    *vlen = v.size();
    *val = static_cast<unsigned char*>(std::malloc(v.empty() ? 1 : v.size()));
    if (!v.empty()) std::memcpy(*val, v.data(), v.size());
    return 0;
}

xscan* xdb_snapshot_scan(xsnap* s, const unsigned char* start, size_t slen,
                         const unsigned char* end, size_t elen, int limit, char** errmsg) {
    rocksdb::ReadOptions ro;
    ro.snapshot = s->snap;
    return do_scan(s->db, ro, start, slen, end, elen, limit, errmsg);
}

int xdb_snapshot_count(xsnap* s, const unsigned char* start, size_t slen,
                       const unsigned char* end, size_t elen, int limit, char** errmsg) {
    rocksdb::ReadOptions ro;
    ro.snapshot = s->snap;
    return do_count(s->db, ro, start, slen, end, elen, limit, errmsg);
}

void xdb_snapshot_release(xsnap* s) {
    if (!s) return;
    s->db->ReleaseSnapshot(s->snap);
    delete s;
}

xmget_item* xdb_snapshot_multi_get2(xsnap* s, const unsigned char* keys,
                                    const size_t* offsets, const size_t* klens,
                                    int n, char** errmsg) {
    if (!s || n <= 0 || !keys || !offsets || !klens) return nullptr;
    rocksdb::ReadOptions ro;
    ro.snapshot = s->snap;
    std::vector<rocksdb::Slice> slices;
    slices.reserve(n);
    for (int i = 0; i < n; ++i) {
        slices.emplace_back(reinterpret_cast<const char*>(keys + offsets[i]), klens[i]);
    }
    std::vector<std::string> vals(n);
    std::vector<rocksdb::Status> statuses = s->db->MultiGet(ro, slices, &vals);
    xmget_item* items = static_cast<xmget_item*>(std::calloc(n, sizeof(xmget_item)));
    if (!items) {
        if (errmsg) *errmsg = dup_status(rocksdb::Status::IOError("xmget items alloc failed"));
        return nullptr;
    }
    for (int i = 0; i < n; ++i) {
        if (statuses[i].IsNotFound()) {
            items[i].found = 0;
            continue;
        }
        if (!statuses[i].ok()) {
            if (errmsg) *errmsg = dup_status(statuses[i]);
            xmget_free(items, n);
            return nullptr;
        }
        items[i].found = 1;
        items[i].vlen = vals[i].size();
        items[i].val = static_cast<unsigned char*>(std::malloc(vals[i].empty() ? 1 : vals[i].size()));
        if (!vals[i].empty()) std::memcpy(items[i].val, vals[i].data(), vals[i].size());
    }
    return items;
}

xmget_item* xdb_snapshot_multi_get(xsnap* s, const unsigned char** keys,
                                   const size_t* klens, int n, char** errmsg) {
    if (!s || n <= 0) return nullptr;
    rocksdb::ReadOptions ro;
    ro.snapshot = s->snap;
    std::vector<rocksdb::Slice> slices;
    slices.reserve(n);
    for (int i = 0; i < n; ++i) {
        slices.emplace_back(reinterpret_cast<const char*>(keys[i]), klens[i]);
    }
    std::vector<std::string> vals(n);
    std::vector<rocksdb::Status> statuses = s->db->MultiGet(ro, slices, &vals);
    xmget_item* items = static_cast<xmget_item*>(std::calloc(n, sizeof(xmget_item)));
    if (!items) {
        if (errmsg) *errmsg = dup_status(rocksdb::Status::IOError("xmget items alloc failed"));
        return nullptr;
    }
    for (int i = 0; i < n; ++i) {
        if (statuses[i].IsNotFound()) {
            items[i].found = 0;
            continue;
        }
        if (!statuses[i].ok()) {
            if (errmsg) *errmsg = dup_status(statuses[i]);
            xmget_free(items, n);
            return nullptr;
        }
        items[i].found = 1;
        items[i].vlen = vals[i].size();
        items[i].val = static_cast<unsigned char*>(std::malloc(vals[i].empty() ? 1 : vals[i].size()));
        if (!vals[i].empty()) std::memcpy(items[i].val, vals[i].data(), vals[i].size());
    }
    return items;
}

void xmget_free(xmget_item* items, int n) {
    if (!items) return;
    for (int i = 0; i < n; ++i) std::free(items[i].val);
    std::free(items);
}

void xdb_free_str(char* p) { std::free(p); }

// ---- 内部统计 ----

namespace {

// 读取单个 RocksDB 内部属性并解析为 int64；属性不存在/解析失败时输出 0。
long long get_int_property(rocksdb::DB* db, const char* prop) {
    std::string v;
    if (!db->GetProperty(prop, &v)) return 0;
    try {
        size_t pos = 0;
        long long n = std::stoll(v, &pos);
        if (pos == 0) return 0;
        return n;
    } catch (...) {
        return 0;
    }
}

}  // namespace

// xdb_flush 强制 flush memtable 到 SST（WAL 自动 checkpoint 用）。
// wait=true：返回时本次 flush 全部完成，此前 Write 的数据均已落 SST；
// checkpoint 截断 OpenXDB WAL 前调用，保证截断位点数据不依赖被截记录。
int xdb_flush(xdb* h, char** errmsg) {
    if (!h || !h->db) {
        if (errmsg) *errmsg = dup_status(rocksdb::Status::InvalidArgument("xdb_flush: null handle"));
        return -1;
    }
    rocksdb::FlushOptions opts;
    opts.wait = true;
    rocksdb::Status st = h->db->Flush(opts);
    if (!st.ok()) {
        if (errmsg) *errmsg = dup_status(st);
        return -1;
    }
    return 0;
}

int xdb_stats_get(xdb* h, xdb_stats* out, char** errmsg) {
    if (!h || !h->db || !out) {
        if (errmsg) *errmsg = dup_status(rocksdb::Status::InvalidArgument("xdb_stats_get: null handle/out"));
        return -1;
    }
    std::memset(out, 0, sizeof(xdb_stats));
    rocksdb::DB* db = h->db;
    // RocksDB 无 "rocksdb.num-levels" 属性；实际层数通过遍历
    // "rocksdb.num-files-at-level<N>" 直到属性不存在来确定（默认 7 层 L0-L6）。
    for (long long i = 0; i < 8; i++) {
        char prop[64];
        std::snprintf(prop, sizeof(prop), "rocksdb.num-files-at-level%lld", i);
        std::string v;
        if (!db->GetProperty(prop, &v)) break;
        try {
            size_t pos = 0;
            long long n = std::stoll(v, &pos);
            if (pos == 0) break;
            out->num_files_at_level[i] = n;
        } catch (...) {
            break;
        }
        out->num_levels = i + 1;
    }
    out->cur_size_all_mem_tables = get_int_property(db, "rocksdb.cur-size-all-mem-tables");
    out->size_all_mem_tables = get_int_property(db, "rocksdb.size-all-mem-tables");
    out->compaction_pending = get_int_property(db, "rocksdb.compaction-pending");
    out->num_running_compactions = get_int_property(db, "rocksdb.num-running-compactions");
    out->num_running_flushes = get_int_property(db, "rocksdb.num-running-flushes");
    out->live_sst_files_size = get_int_property(db, "rocksdb.live-sst-files-size");
    out->num_live_versions = get_int_property(db, "rocksdb.num-live-versions");
    out->estimate_num_keys = get_int_property(db, "rocksdb.estimate-num-keys");
    return 0;
}

}  // extern "C"

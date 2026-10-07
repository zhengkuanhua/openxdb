// bridge.cc - RocksDB C ABI 桥（M1 T1，供 Go cgo 调用）。
#include "bridge.h"

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/snapshot.h>
#include <rocksdb/write_batch.h>

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

struct xscan {
    std::vector<std::pair<std::string, std::string>> items;
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
    auto st = h->db->Delete(wo, rocksdb::Slice(reinterpret_cast<const char*>(key), klen));
    if (!st.ok()) {
        if (errmsg) *errmsg = dup_status(st);
        return -1;
    }
    return 0;
}

int xdb_write_batch(xdb* h,
                    const unsigned char* pairs, const size_t* put_sizes, int nputs,
                    const unsigned char* delkeys, const size_t* del_sizes, int ndeletes,
                    char** errmsg) {
    rocksdb::WriteOptions wo;
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
    int count = 0;
    for (; it->Valid(); it->Next()) {
        rocksdb::Slice k = it->key();
        if (end && k.compare(rocksdb::Slice(reinterpret_cast<const char*>(end), elen)) >= 0) break;
        out->items.emplace_back(k.ToString(), it->value().ToString());
        count++;
        if (limit > 0 && count >= limit) break;
    }
    if (!it->status().ok()) {
        if (errmsg) *errmsg = dup_status(it->status());
        delete out;
        return nullptr;
    }
    return out;
}

xscan* xdb_scan(xdb* h, const unsigned char* start, size_t slen,
                const unsigned char* end, size_t elen, int limit, char** errmsg) {
    rocksdb::ReadOptions ro;
    return do_scan(h->db, ro, start, slen, end, elen, limit, errmsg);
}

int xscan_count(xscan* s) { return static_cast<int>(s->items.size()); }

int xscan_item(xscan* s, int i, unsigned char** key, size_t* klen,
               unsigned char** val, size_t* vlen) {
    if (i < 0 || i >= static_cast<int>(s->items.size())) return -1;
    const std::string& k = s->items[i].first;
    const std::string& v = s->items[i].second;
    *klen = k.size();
    *vlen = v.size();
    *key = static_cast<unsigned char*>(std::malloc(k.empty() ? 1 : k.size()));
    *val = static_cast<unsigned char*>(std::malloc(v.empty() ? 1 : v.size()));
    if (!k.empty()) std::memcpy(*key, k.data(), k.size());
    if (!v.empty()) std::memcpy(*val, v.data(), v.size());
    return 0;
}

void xscan_free(xscan* s) { delete s; }

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

void xdb_snapshot_release(xsnap* s) {
    if (!s) return;
    s->db->ReleaseSnapshot(s->snap);
    delete s;
}

void xdb_free_str(char* p) { std::free(p); }

}  // extern "C"

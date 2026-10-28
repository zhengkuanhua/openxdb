#ifndef OPENXDB_ROCKSDB_BRIDGE_H
#define OPENXDB_ROCKSDB_BRIDGE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct xdb xdb;
typedef struct xscan xscan;
typedef struct xsnap xsnap;

/* 打开/关闭数据库目录。create_if_missing=1 时不存在则创建。 */
xdb* xdb_open(const char* dir, int create_if_missing, char** errmsg);
void xdb_close(xdb* h);

/* 返回 0=成功, 1=NotFound, -1=错误(errmsg 置位)。val 需调用方 free。 */
int xdb_get(xdb* h, const unsigned char* key, size_t klen,
            unsigned char** val, size_t* vlen, char** errmsg);
/* 返回 0=成功, -1=错误。 */
int xdb_put(xdb* h, const unsigned char* key, size_t klen,
            const unsigned char* val, size_t vlen, char** errmsg);
int xdb_delete(xdb* h, const unsigned char* key, size_t klen, char** errmsg);

/* 批量原子写：pairs 为 nputs 个 (key,val) 连续拼接，put_sizes[i*2]=klen, [i*2+1]=vlen；
   delkeys 为 ndeletes 个 key 连续拼接，del_sizes[i]=klen。 */
int xdb_write_batch(xdb* h,
                    const unsigned char* pairs, const size_t* put_sizes, int nputs,
                    const unsigned char* delkeys, const size_t* del_sizes, int ndeletes,
                    char** errmsg);

/* 范围扫描 [start,end)，end 为 NULL 或空表示无限；limit<=0 表示不限制。
   结果数据集中在单块 arena 内，xscan_free 整体释放（key/val 指针不单独 free）。 */
xscan* xdb_scan(xdb* h, const unsigned char* start, size_t slen,
                const unsigned char* end, size_t elen, int limit, char** errmsg);
int xscan_count(xscan* s);
/* 返回第 i 行 key/value 指针与长度（指向 xscan 内部 arena，0 拷贝；整体由 xscan_free 释放）。 */
int xscan_item(xscan* s, int i, unsigned char** key, size_t* klen,
               unsigned char** val, size_t* vlen);
void xscan_free(xscan* s);

/* 范围计数 [start,end)，只数 key 不拷贝 value（迭代器 count 循环，无 ToString/malloc）。
   返回 >=0 的命中条数；-1=错误（errmsg 置位）。limit<=0 表示不限制。 */
int xdb_count(xdb* h, const unsigned char* start, size_t slen,
              const unsigned char* end, size_t elen, int limit, char** errmsg);
int xdb_snapshot_count(xsnap* s, const unsigned char* start, size_t slen,
                       const unsigned char* end, size_t elen, int limit, char** errmsg);

/* 一致性快照（备份/迁移复用）。 */
xsnap* xdb_snapshot_create(xdb* h);
int xdb_snapshot_get(xsnap* s, const unsigned char* key, size_t klen,
                     unsigned char** val, size_t* vlen, char** errmsg);
xscan* xdb_snapshot_scan(xsnap* s, const unsigned char* start, size_t slen,
                         const unsigned char* end, size_t elen, int limit, char** errmsg);
void xdb_snapshot_release(xsnap* s);

/* 快照批量读：keys[i] 长度 klens[i]（i=0..n-1），返回 n 个 item 的 malloc 数组
   （整体失败返回 NULL 并置 errmsg）。item.found: 1=命中, 0=未命中；命中时
   val 为 malloc 缓冲区，配合 xmget_free 释放。 */
typedef struct xmget_item {
    int found;
    unsigned char* val;
    size_t vlen;
} xmget_item;
xmget_item* xdb_snapshot_multi_get(xsnap* s, const unsigned char** keys,
                                   const size_t* klens, int n, char** errmsg);
/* 安全版快照批量读：keys 为 n 个 key 的连续拼接缓冲区（完全位于 C 内存，
   不包含 Go 指针），offsets[i] 为第 i 个 key 的起始偏移，klens[i] 为其长度。 */
xmget_item* xdb_snapshot_multi_get2(xsnap* s, const unsigned char* keys,
                                    const size_t* offsets, const size_t* klens,
                                    int n, char** errmsg);
void xmget_free(xmget_item* items, int n);

void xdb_free_str(char* p);

/* RocksDB 内部统计快照（监控看板 / 管理台对接）。一次调用批量采集：
   - LSM 层数（num_levels，level 0 为内存 flush 目标层）
   - 各层 SST 文件数（num_files_at_level[i]，i < num_levels）
   - MemTable 大小（cur_size_all_mem_tables 当前 / size_all_mem_tables 含 immutable）
   - Compaction 状态（compaction_pending / num_running_compactions）
   - Flush 状态（num_running_flushes）
   - 活跃 SST 文件总字节（live_sst_files_size）
   - 版本与键规模（num_live_versions / estimate_num_keys） */
typedef struct xdb_stats {
    long long num_levels;
    long long num_files_at_level[8];
    long long cur_size_all_mem_tables;
    long long size_all_mem_tables;
    long long compaction_pending;
    long long num_running_compactions;
    long long num_running_flushes;
    long long live_sst_files_size;
    long long num_live_versions;
    long long estimate_num_keys;
} xdb_stats;

/* 采集 RocksDB 内部统计。返回 0=成功；-1=失败（errmsg 置位）。 */
int xdb_stats_get(xdb* h, xdb_stats* out, char** errmsg);

/* 强制 flush memtable 到 SST（WAL 自动 checkpoint 用，保证截断位点数据落盘）。
   返回 0=成功；-1=失败（errmsg 置位）。 */
int xdb_flush(xdb* h, char** errmsg);

#ifdef __cplusplus
}
#endif

#endif /* OPENXDB_ROCKSDB_BRIDGE_H */

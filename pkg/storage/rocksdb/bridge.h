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

/* 范围扫描 [start,end)，end 为 NULL 或空表示无限；limit<=0 表示不限制。 */
xscan* xdb_scan(xdb* h, const unsigned char* start, size_t slen,
                const unsigned char* end, size_t elen, int limit, char** errmsg);
int xscan_count(xscan* s);
int xscan_item(xscan* s, int i, unsigned char** key, size_t* klen,
               unsigned char** val, size_t* vlen);
void xscan_free(xscan* s);

/* 一致性快照（备份/迁移复用）。 */
xsnap* xdb_snapshot_create(xdb* h);
int xdb_snapshot_get(xsnap* s, const unsigned char* key, size_t klen,
                     unsigned char** val, size_t* vlen, char** errmsg);
xscan* xdb_snapshot_scan(xsnap* s, const unsigned char* start, size_t slen,
                         const unsigned char* end, size_t elen, int limit, char** errmsg);
void xdb_snapshot_release(xsnap* s);

void xdb_free_str(char* p);

#ifdef __cplusplus
}
#endif

#endif /* OPENXDB_ROCKSDB_BRIDGE_H */

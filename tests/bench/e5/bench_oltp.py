# -*- coding: utf-8 -*-
"""E5 sysbench OLTP equivalent benchmark for OpenXDB (PEP 249 driver).

Aligns with sysbench oltp_common.lua default transaction template on the
sbtest(id, k, c, pad) table with 10,000 rows. Modes:
  read        -> 14 read ops per txn (10 point_selects + simple/sum/order/distinct ranges)
  write       -> 4 write ops per txn (index_update, non_index_update, delete, insert)
  read_write  -> 14 read + 4 write ops per txn (default oltp_read_write)

Each txn is wrapped in BEGIN/COMMIT (same transaction boundary as sysbench
--db-ps-mode). Deleted row is re-inserted with the same id to keep table size
stable (documented deviation, see docs/experiments/E5_standard_bench_alignment.md).

v1.13.0: 事务内全部语句经 Cursor.pipeline 一次批量发送（客户端 pipeline，
服务端 REPLSession 按行序处理），消除逐语句网络往返；连接超时提升至
--timeout（默认 120s，修复高并发长尾事务触发 30s 固定超时的问题），连接级
异常后自动重建连接继续压测。

Usage:
  python bench_oltp.py --prepare
  python bench_oltp.py --mode read_write --threads 8 --time 60
"""
import argparse
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..'))
sys.path.insert(0, os.path.join(ROOT, 'drivers', 'python'))
from openxdb import connect  # noqa: E402
from openxdb.errors import OperationalError  # noqa: E402

HOST = '127.0.0.1'
PORT = 7797
TABLE_ROWS = 10000
TEXT_CHARS = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
DEFAULT_TIMEOUT = 120.0  # v1.13.0：30s 固定超时 < 高并发长尾事务端到端延迟，已提升


class StmtError(Exception):
    """语句级错误（pipeline 中某语句返回 ERR，连接本身仍可用）。"""


def rand_text(rng, minlen=1, maxlen=10):
    n = rng.randint(minlen, maxlen)
    return ''.join(rng.choice(TEXT_CHARS) for _ in range(n))


def rand_int(rng, lo, hi):
    return rng.randint(lo, hi)


# ---------------------------------------------------------------------------
# prepare
# ---------------------------------------------------------------------------
def prepare(port):
    conn = connect(HOST, port)
    cur = conn.cursor()
    try:
        cur.execute('DROP TABLE IF EXISTS sbtest')
    except Exception:
        pass
    cur.execute('BEGIN')
    cur.execute('CREATE TABLE sbtest (id INT PRIMARY KEY, k INT, c TEXT, pad TEXT)')
    cur.execute('CREATE INDEX k_1 ON sbtest (k)')
    rng = random.Random(20261015)
    batch = []
    for i in range(1, TABLE_ROWS + 1):
        batch.append((i, rand_int(rng, 1, 2 ** 31 - 1), rand_text(rng), rand_text(rng)))
        if len(batch) >= 500:
            cur.executemany('INSERT INTO sbtest (id, k, c, pad) VALUES (?, ?, ?, ?)', batch)
            batch = []
    if batch:
        cur.executemany('INSERT INTO sbtest (id, k, c, pad) VALUES (?, ?, ?, ?)', batch)
    cur.execute('COMMIT')
    n = cur.execute('SELECT COUNT(*) AS n FROM sbtest').fetchone()[0]
    conn.close()
    print('prepare ok rows=%s' % n)


# ---------------------------------------------------------------------------
# capability probe (E5 doc: guarantee SQL used is supported by the engine)
# ---------------------------------------------------------------------------
def capability_probe(port):
    conn = connect(HOST, port)
    cur = conn.cursor()
    results = {}
    probes = [
        ('group_by_distinct', lambda: cur.execute('SELECT k FROM sbtest WHERE id BETWEEN 1 AND 100 GROUP BY k ORDER BY k').fetchall()),
        ('agg_as', lambda: cur.execute('SELECT SUM(k) AS s FROM sbtest WHERE id BETWEEN 1 AND 100').fetchall()),
        ('order_limit', lambda: cur.execute('SELECT id FROM sbtest ORDER BY id DESC LIMIT 1').fetchall()),
    ]
    for name, fn in probes:
        try:
            fn()
            results[name] = 'ok'
        except Exception as e:
            results[name] = 'FAIL: %s' % str(e)[:120]
    conn.close()
    for k, v in results.items():
        print('probe[%s] %s' % (k, v))
    failed = [k for k, v in results.items() if v != 'ok']
    if failed:
        raise RuntimeError('capability probe failed: %s' % ', '.join(failed))
    print('probe all ok')


# ---------------------------------------------------------------------------
# benchmark
# ---------------------------------------------------------------------------
READ_OPS = 10 + 1 + 1 + 1 + 1          # point_selects + simple/sum/order/distinct
WRITE_OPS = 4                          # index_update + non_index_update + delete + insert
MODE_OPS = {'read': READ_OPS, 'write': WRITE_OPS, 'read_write': READ_OPS + WRITE_OPS}


def run_txn(conn, mode, rng):
    cur = conn.cursor()
    # v1.13.0：整个事务（BEGIN + 全部操作 + COMMIT）一次 pipeline 批量发送，
    # 客户端单次 sendall + 服务端按行序逐条执行并批量读回，消除逐语句往返。
    queries = ['BEGIN']
    params = [None]
    a = rng.randint(1, TABLE_ROWS)
    b = rng.randint(1, TABLE_ROWS)
    if b < a:
        a, b = b, a
    for _ in range(10):
        queries.append('SELECT c FROM sbtest WHERE id=?')
        params.append([rng.randint(1, TABLE_ROWS)])
    queries.append('SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY id')
    params.append([a, b])
    queries.append('SELECT SUM(k) AS s FROM sbtest WHERE id BETWEEN ? AND ?')
    params.append([a, b])
    queries.append('SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY k')
    params.append([a, b])
    # DISTINCT 列级去重引擎不支持（见 E5 文档口径差异），用 GROUP BY k 等价实现 distinct_ranges
    queries.append('SELECT k FROM sbtest WHERE id BETWEEN ? AND ? GROUP BY k ORDER BY k')
    params.append([a, b])
    if mode in ('write', 'read_write'):
        did = rng.randint(1, TABLE_ROWS)
        # 引擎不支持算术表达式 k+1（见 E5 文档口径差异），index_update 等价实现为
        # 直接写随机 k 值：同为随机行上的索引列写放大，保持 4 ops 结构。
        queries.append('UPDATE sbtest SET k = ? WHERE id=?')
        params.append([rand_int(rng, 1, 2 ** 31 - 1), did])
        queries.append('UPDATE sbtest SET c=? WHERE id=?')
        params.append([rand_text(rng), did])
        queries.append('DELETE FROM sbtest WHERE id=?')
        params.append([did])
        queries.append('INSERT INTO sbtest (id, k, c, pad) VALUES (?, ?, ?, ?)')
        params.append([did, rand_int(rng, 1, 2 ** 31 - 1), rand_text(rng), rand_text(rng)])
    queries.append('COMMIT')
    params.append(None)
    results = cur.pipeline(queries, params)
    for r in results:
        if r.kind == 'error':
            raise StmtError('pipeline stmt error: %s' % (r.error,))


def worker(port, mode, duration_s, stats, stop_event, timeout):
    rng = random.Random()
    conn = connect(HOST, port, timeout=timeout)
    while not stop_event.is_set():
        t0 = time.perf_counter()
        try:
            run_txn(conn, mode, rng)
            lat = (time.perf_counter() - t0) * 1e6
            with stats['lock']:
                stats['txns'] += 1
                stats['latencies'].append(lat)
        except StmtError as e:
            # 语句级错误：连接仍可用，回滚后继续（不计数为连接问题）。
            try:
                conn.rollback()
            except Exception:
                pass
            with stats['lock']:
                stats['errs'] += 1
                if stats['errs'] <= 5:
                    stats['err_msgs'].append('stmt: %s' % str(e)[:160])
        except Exception as e:
            # 连接级异常（读超时/断线/协议错误）：关闭并重建连接，继续压测。
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            with stats['lock']:
                stats['errs'] += 1
                stats['reconnects'] += 1
                if stats['errs'] <= 5:
                    stats['err_msgs'].append('conn: %s' % str(e)[:160])
            try:
                conn = connect(HOST, port, timeout=timeout)
            except Exception as e2:
                with stats['lock']:
                    stats['errs'] += 1
                    stats['reconnects'] += 1
                    if stats['errs'] <= 5:
                        stats['err_msgs'].append('connect: %s' % str(e2)[:160])
                time.sleep(0.5)
    try:
        conn.close()
    except Exception:
        pass


def percentile(sorted_lat, p):
    if not sorted_lat:
        return 0.0
    idx = min(len(sorted_lat) - 1, int(len(sorted_lat) * p))
    return sorted_lat[idx]


def run(mode, port, threads, duration_s, timeout=DEFAULT_TIMEOUT):
    stats = {'txns': 0, 'errs': 0, 'reconnects': 0, 'latencies': [], 'err_msgs': [], 'lock': threading.Lock()}
    stop_event = threading.Event()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futs = [ex.submit(worker, port, mode, duration_s, stats, stop_event, timeout) for _ in range(threads)]
        time.sleep(duration_s)
        stop_event.set()
        for f in futs:
            f.result()
    wall = duration_s
    tps = stats['txns'] / wall
    qps = stats['txns'] * MODE_OPS[mode] / wall
    lat = sorted(stats['latencies'])
    p50 = percentile(lat, 0.50)
    p95 = percentile(lat, 0.95)
    p99 = percentile(lat, 0.99)
    p_max = lat[-1] if lat else 0.0
    print('mode=%s threads=%d time=%ds txn_ok=%d errs=%d reconnects=%d' % (mode, threads, duration_s, stats['txns'], stats['errs'], stats['reconnects']))
    print('TPS=%.2f QPS=%.2f' % (tps, qps))
    print('lat_us p50=%.0f p95=%.0f p99=%.0f max=%.0f' % (p50, p95, p99, p_max))
    for m in stats['err_msgs']:
        print('ERR:', m)
    return {'mode': mode, 'tps': tps, 'qps': qps, 'p50_us': p50, 'p95_us': p95,
            'p99_us': p99, 'max_us': p_max, 'errs': stats['errs'],
            'reconnects': stats['reconnects']}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default=HOST)
    ap.add_argument('--port', type=int, default=PORT)
    ap.add_argument('--mode', choices=['read', 'write', 'read_write'], default='read_write')
    ap.add_argument('--threads', type=int, default=8)
    ap.add_argument('--time', type=int, default=60)
    ap.add_argument('--timeout', type=float, default=DEFAULT_TIMEOUT, help='socket timeout seconds per worker connection')
    ap.add_argument('--prepare', action='store_true')
    args = ap.parse_args()
    if args.prepare:
        prepare(args.port)
        return
    capability_probe(args.port)
    run(args.mode, args.port, args.threads, args.time, args.timeout)


if __name__ == '__main__':
    main()

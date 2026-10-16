# -*- coding: utf-8 -*-
"""E5 TPC-C small-warehouse equivalent benchmark for OpenXDB (PEP 249 driver).

Implements the TPC-C 5-class transaction mix at warehouse=5 scale:
  NewOrder 45% / Payment 43% / OrderStatus 4% / Delivery 4% / StockLevel 4%
Tables: warehouse / district / customer / history / item / stock / orders /
order_line / new_order (single-column PKs, documented deviation: OpenXDB
composite PK support pending, see E5 doc).
tpmC = NewOrder transactions per minute.

Usage:
  python bench_tpcc.py --prepare
  python bench_tpcc.py --warehouses 5 --threads 8 --time 120 --warmup 10
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

HOST = '127.0.0.1'
PORT = 7797
DISTS_PER_W = 10
CUST_PER_D = 30           # 300 customers per warehouse
ORDERS_PER_D = 30         # 300 orders per warehouse
ITEMS = 10000
TEXT_CHARS = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
MIX = [('NEW_ORDER', 45), ('PAYMENT', 43), ('ORDER_STATUS', 4), ('DELIVERY', 4), ('STOCK_LEVEL', 4)]
_MIX_POOL = []
for _name, _w in MIX:
    _MIX_POOL += [_name] * _w


def rand_text(rng, minlen, maxlen):
    n = rng.randint(minlen, maxlen)
    return ''.join(rng.choice(TEXT_CHARS) for _ in range(n))


def now_str():
    return time.strftime('%Y-%m-%d %H:%M:%S')


# ---------------------------------------------------------------------------
# global PK counters (single-column PK workaround)
# ---------------------------------------------------------------------------
class Counters:
    def __init__(self):
        self.lock = threading.Lock()
        self.order = 15001          # prepare 装载 5*10*300=15000 条 orders，PK 1..15000
        self.ol = 300001            # order_line 每单 5-15 行，上限 15000*15=225000，留余量
        self.no = 1
        self.hist = 1

    def next(self, name):
        with self.lock:
            v = getattr(self, name)
            setattr(self, name, v + 1)
            return v


# ---------------------------------------------------------------------------
# prepare
# ---------------------------------------------------------------------------
def prepare(port, warehouses):
    conn = connect(HOST, port)
    cur = conn.cursor()
    for t in ('warehouse', 'district', 'customer', 'history', 'item',
              'stock', 'orders', 'order_line', 'new_order'):
        try:
            cur.execute('DROP TABLE IF EXISTS %s' % t)
        except Exception:
            pass
    cur.execute('CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_name TEXT, w_tax DECIMAL, w_ytd DECIMAL)')
    cur.execute('CREATE TABLE district (d_id INT PRIMARY KEY, d_w_id INT, d_name TEXT, d_tax DECIMAL, d_ytd DECIMAL, d_next_o_id INT)')
    cur.execute('CREATE TABLE customer (c_id INT PRIMARY KEY, c_w_id INT, c_d_id INT, c_first TEXT, c_middle TEXT, c_last TEXT, c_credit TEXT, c_discount DECIMAL, c_balance DECIMAL, c_ytd_payment DECIMAL, c_payment_cnt INT, c_delivery_cnt INT)')
    cur.execute('CREATE TABLE history (h_id INT PRIMARY KEY, h_c_id INT, h_c_d_id INT, h_c_w_id INT, h_d_id INT, h_w_id INT, h_date TEXT, h_amount DECIMAL, h_data TEXT)')
    cur.execute('CREATE TABLE item (i_id INT PRIMARY KEY, i_name TEXT, i_price DECIMAL, i_data TEXT)')
    cur.execute('CREATE TABLE stock (s_id INT PRIMARY KEY, s_w_id INT, s_i_id INT, s_quantity INT, s_ytd DECIMAL, s_order_cnt INT, s_remote_cnt INT, s_data TEXT)')
    cur.execute('CREATE TABLE orders (o_id INT PRIMARY KEY, o_w_id INT, o_d_id INT, o_no INT, o_c_id INT, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, o_entry_d TEXT)')
    cur.execute('CREATE TABLE order_line (ol_id INT PRIMARY KEY, ol_w_id INT, ol_d_id INT, ol_o_no INT, ol_i_id INT, ol_qty INT, ol_amount DECIMAL, ol_dist_info TEXT, ol_delivery_d TEXT)')
    cur.execute('CREATE TABLE new_order (no_id INT PRIMARY KEY, no_w_id INT, no_d_id INT, no_o_no INT)')

    rng = random.Random(20261015)
    # warehouse / district / customer
    wh_batch = []
    ds_batch = []
    cu_batch = []
    for w in range(1, warehouses + 1):
        wh_batch.append((w, rand_text(rng, 6, 10), round(rng.uniform(0.0, 0.2), 4), 0.0))
        for d in range(1, DISTS_PER_W + 1):
            d_id = (w - 1) * DISTS_PER_W + d
            ds_batch.append((d_id, w, rand_text(rng, 6, 10), round(rng.uniform(0.0, 0.2), 4), 0.0, 1))
            for c in range(1, CUST_PER_D + 1):
                c_id = (w - 1) * CUST_PER_D * DISTS_PER_W + (d - 1) * CUST_PER_D + c
                cu_batch.append((c_id, w, d, rand_text(rng, 5, 10), 'M', rand_text(rng, 5, 10),
                                 'GC', round(rng.uniform(0.0, 0.5), 4), 0.0, 0.0, 0, 0))
    cur.executemany('INSERT INTO warehouse (w_id, w_name, w_tax, w_ytd) VALUES (?, ?, ?, ?)', wh_batch)
    cur.executemany('INSERT INTO district (d_id, d_w_id, d_name, d_tax, d_ytd, d_next_o_id) VALUES (?, ?, ?, ?, ?, ?)', ds_batch)
    for i in range(0, len(cu_batch), 500):
        cur.executemany('INSERT INTO customer (c_id, c_w_id, c_d_id, c_first, c_middle, c_last, c_credit, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', cu_batch[i:i + 500])

    # item (global 10000)
    it_batch = []
    for i in range(1, ITEMS + 1):
        it_batch.append((i, rand_text(rng, 8, 24), round(rng.uniform(1.0, 100.0), 2), rand_text(rng, 8, 24)))
    for i in range(0, len(it_batch), 500):
        cur.executemany('INSERT INTO item (i_id, i_name, i_price, i_data) VALUES (?, ?, ?, ?)', it_batch[i:i + 500])

    # stock (per warehouse 10000; s_id = (w-1)*ITEMS + i_id)
    st_batch = []
    for w in range(1, warehouses + 1):
        for i in range(1, ITEMS + 1):
            st_batch.append(((w - 1) * ITEMS + i, w, i, rng.randint(10, 100), 0.0, 0, 0, rand_text(rng, 8, 24)))
    for i in range(0, len(st_batch), 500):
        cur.executemany('INSERT INTO stock (s_id, s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', st_batch[i:i + 500])

    # orders + order_line (300 orders per warehouse, 5-15 lines each)
    o_batch = []
    ol_batch = []
    o_pk = 1
    ol_pk = 1
    for w in range(1, warehouses + 1):
        for d in range(1, DISTS_PER_W + 1):
            for o_no in range(1, ORDERS_PER_D + 1):
                c_local = rng.randint(1, CUST_PER_D)
                c_id = (w - 1) * CUST_PER_D * DISTS_PER_W + (d - 1) * CUST_PER_D + c_local
                o_batch.append((o_pk, w, d, o_no, c_id, rng.randint(1, 10), rng.randint(5, 15), 1, now_str()))
                o_pk += 1
                n_lines = o_batch[-1][6]
                for _ in range(n_lines):
                    i_id = rng.randint(1, ITEMS)
                    ol_batch.append((ol_pk, w, d, o_no, i_id, rng.randint(1, 10), round(rng.uniform(0.01, 100.0), 2), rand_text(rng, 8, 24), ''))
                    ol_pk += 1
    for i in range(0, len(o_batch), 500):
        cur.executemany('INSERT INTO orders (o_id, o_w_id, o_d_id, o_no, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', o_batch[i:i + 500])
    for i in range(0, len(ol_batch), 500):
        cur.executemany('INSERT INTO order_line (ol_id, ol_w_id, ol_d_id, ol_o_no, ol_i_id, ol_qty, ol_amount, ol_dist_info, ol_delivery_d) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', ol_batch[i:i + 500])

    conn.close()
    print('prepare ok warehouses=%d' % warehouses)


# ---------------------------------------------------------------------------
# capability probe
# ---------------------------------------------------------------------------
def capability_probe(port):
    conn = connect(HOST, port)
    cur = conn.cursor()
    results = {}
    probes = [
        ('order_limit', lambda: cur.execute('SELECT o_no FROM orders WHERE o_w_id=1 AND o_d_id=1 ORDER BY o_no DESC LIMIT 1').fetchall()),
        ('agg_as', lambda: cur.execute('SELECT COUNT(*) AS n FROM orders').fetchall()),
        ('distinct', lambda: cur.execute('SELECT ol_i_id FROM order_line WHERE ol_w_id=1 AND ol_d_id=1 AND ol_o_no BETWEEN 1 AND 5 GROUP BY ol_i_id LIMIT 10').fetchall()),
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
# TPC-C transactions
# ---------------------------------------------------------------------------
def _txn_new_order(cur, rng, counters, warehouses):
    w = rng.randint(1, warehouses)
    d = rng.randint(1, DISTS_PER_W)
    c_local = rng.randint(1, CUST_PER_D)
    c_id = (w - 1) * CUST_PER_D * DISTS_PER_W + (d - 1) * CUST_PER_D + c_local
    d_id = (w - 1) * DISTS_PER_W + d
    n_items = rng.randint(5, 15)
    row = cur.execute('SELECT c_discount AS cd FROM customer WHERE c_id=?', [c_id]).fetchone()
    c_discount = float(row[0]) if row else 0.0
    row = cur.execute('SELECT w_tax AS wt FROM warehouse WHERE w_id=?', [w]).fetchone()
    w_tax = float(row[0]) if row else 0.0
    row = cur.execute('SELECT d_tax AS dt, d_next_o_id AS nxt FROM district WHERE d_id=?', [d_id]).fetchone()
    if not row:
        raise RuntimeError('district not found d_id=%d' % d_id)
    d_tax = float(row[0])
    next_no = int(row[1])
    # 引擎不支持算术表达式（+/-），d_next_o_id 递增改为客户端读-改-写等价实现
    cur.execute('UPDATE district SET d_next_o_id = ? WHERE d_id=?', [next_no + 1, d_id])
    o_pk = counters.next('order')
    ol_total = 0
    for _ in range(n_items):
        i_id = rng.randint(1, ITEMS)
        qty = rng.randint(1, 10)
        row = cur.execute('SELECT i_price AS ip FROM item WHERE i_id=?', [i_id]).fetchone()
        price = float(row[0]) if row else 1.0
        s_id = (w - 1) * ITEMS + i_id
        row = cur.execute('SELECT s_quantity AS sq, s_ytd AS sy, s_order_cnt AS soc FROM stock WHERE s_id=?', [s_id]).fetchone()
        s_qty = int(row[0]) if row else 0
        s_ytd = int(float(row[1])) if row else 0   # s_ytd 为 DECIMAL，返回 '0.0000'，需先转 float
        s_order_cnt = int(row[2]) if row else 0
        new_qty = s_qty - qty
        if new_qty < 10:
            new_qty += 91
        # 引擎不支持算术表达式，s_ytd/s_order_cnt 递增改为客户端读-改-写等价实现
        cur.execute('UPDATE stock SET s_quantity=?, s_ytd=?, s_order_cnt=? WHERE s_id=?',
                    [new_qty, s_ytd + qty, s_order_cnt + 1, s_id])
        amount = float(f'{qty * price * (1 + w_tax + d_tax):.2f}')  # 固定两位小数，避免 DECIMAL 精度报错
        cur.execute('INSERT INTO order_line (ol_id, ol_w_id, ol_d_id, ol_o_no, ol_i_id, ol_qty, ol_amount, ol_dist_info, ol_delivery_d) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    [counters.next('ol'), w, d, next_no, i_id, qty, amount, rand_text(rng, 8, 24), ''])
        ol_total += amount
    cur.execute('INSERT INTO orders (o_id, o_w_id, o_d_id, o_no, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [o_pk, w, d, next_no, c_id, 0, n_items, 1, now_str()])
    cur.execute('INSERT INTO new_order (no_id, no_w_id, no_d_id, no_o_no) VALUES (?, ?, ?, ?)',
                [counters.next('no'), w, d, next_no])
    return 'NEW_ORDER'


def _txn_payment(cur, rng, counters, warehouses):
    w = rng.randint(1, warehouses)
    d = rng.randint(1, DISTS_PER_W)
    c_local = rng.randint(1, CUST_PER_D)
    c_id = (w - 1) * CUST_PER_D * DISTS_PER_W + (d - 1) * CUST_PER_D + c_local
    d_id = (w - 1) * DISTS_PER_W + d
    amount = float(f'{rng.uniform(1.0, 5000.0):.2f}')  # 固定两位小数，避免 DECIMAL 精度报错
    # 引擎不支持算术表达式，w_ytd/d_ytd/c_balance 等累加改为客户端读-改-写等价实现
    row_w = cur.execute('SELECT w_ytd AS wy FROM warehouse WHERE w_id=?', [w]).fetchone()
    row_d = cur.execute('SELECT d_ytd AS dy FROM district WHERE d_id=?', [d_id]).fetchone()
    row_c = cur.execute('SELECT c_balance AS cb, c_ytd_payment AS cyp, c_payment_cnt AS cpc FROM customer WHERE c_id=?', [c_id]).fetchone()
    cur.execute('UPDATE warehouse SET w_ytd = ? WHERE w_id=?', [float(f'{float(row_w[0]) + amount:.2f}') if row_w else amount, w])
    cur.execute('UPDATE district SET d_ytd = ? WHERE d_id=?', [float(f'{float(row_d[0]) + amount:.2f}') if row_d else amount, d_id])
    cur.execute('UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_id=?',
                [float(f'{float(row_c[0]) - amount:.2f}') if row_c else -amount,
                 float(f'{float(row_c[1]) + amount:.2f}') if row_c else amount,
                 int(row_c[2]) + 1 if row_c else 1, c_id])
    cur.execute('INSERT INTO history (h_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [counters.next('hist'), c_id, d, w, d, w, now_str(), amount, rand_text(rng, 12, 24)])
    return 'PAYMENT'


def _txn_order_status(cur, rng, counters, warehouses):
    w = rng.randint(1, warehouses)
    d = rng.randint(1, DISTS_PER_W)
    c_local = rng.randint(1, CUST_PER_D)
    c_id = (w - 1) * CUST_PER_D * DISTS_PER_W + (d - 1) * CUST_PER_D + c_local
    cur.execute('SELECT c_balance AS cb FROM customer WHERE c_id=?', [c_id])
    row = cur.execute('SELECT o_no FROM orders WHERE o_w_id=? AND o_d_id=? AND o_c_id=? ORDER BY o_no DESC LIMIT 1',
                      [w, d, c_id]).fetchone()
    if row:
        cur.execute('SELECT ol_i_id, ol_qty, ol_amount FROM order_line WHERE ol_w_id=? AND ol_d_id=? AND ol_o_no=?',
                    [w, d, int(row[0])])
    return 'ORDER_STATUS'


def _txn_delivery(cur, rng, counters, warehouses):
    w = rng.randint(1, warehouses)
    carrier = rng.randint(1, 10)
    for d in range(1, DISTS_PER_W + 1):
        row = cur.execute('SELECT no_o_no FROM new_order WHERE no_w_id=? AND no_d_id=? ORDER BY no_o_no ASC LIMIT 1',
                          [w, d]).fetchone()
        if not row:
            continue
        no = int(row[0])
        c_row = cur.execute('SELECT o_c_id AS oc FROM orders WHERE o_w_id=? AND o_d_id=? AND o_no=?',
                            [w, d, no]).fetchone()
        cur.execute('UPDATE orders SET o_carrier_id=? WHERE o_w_id=? AND o_d_id=? AND o_no=?',
                    [carrier, w, d, no])
        cur.execute('UPDATE order_line SET ol_delivery_d=? WHERE ol_w_id=? AND ol_d_id=? AND ol_o_no=?',
                    [now_str(), w, d, no])
        cur.execute('DELETE FROM new_order WHERE no_w_id=? AND no_d_id=? AND no_o_no=?', [w, d, no])
        if c_row:
            cc_row = cur.execute('SELECT c_delivery_cnt AS cdc FROM customer WHERE c_id=?', [int(c_row[0])]).fetchone()
            # 引擎不支持算术表达式，c_delivery_cnt 递增改为客户端读-改-写等价实现
            cur.execute('UPDATE customer SET c_delivery_cnt = ? WHERE c_id=?', [int(cc_row[0]) + 1 if cc_row else 1, int(c_row[0])])
    return 'DELIVERY'


def _txn_stock_level(cur, rng, counters, warehouses):
    w = rng.randint(1, warehouses)
    d = rng.randint(1, DISTS_PER_W)
    threshold = rng.randint(10, 20)
    row = cur.execute('SELECT o_no FROM orders WHERE o_w_id=? AND o_d_id=? ORDER BY o_no DESC LIMIT 1',
                      [w, d]).fetchone()
    count = 0
    if row:
        max_no = int(row[0])
        lo = max(1, max_no - 19)
        rows = cur.execute('SELECT ol_i_id FROM order_line WHERE ol_w_id=? AND ol_d_id=? AND ol_o_no BETWEEN ? AND ? AND ol_qty < ? GROUP BY ol_i_id',
                           [w, d, lo, max_no, threshold]).fetchall()
        count = len(rows)
    return 'STOCK_LEVEL'


_TXN_FUNCS = {
    'NEW_ORDER': _txn_new_order,
    'PAYMENT': _txn_payment,
    'ORDER_STATUS': _txn_order_status,
    'DELIVERY': _txn_delivery,
    'STOCK_LEVEL': _txn_stock_level,
}


# ---------------------------------------------------------------------------
# benchmark
# ---------------------------------------------------------------------------
def worker(port, warehouses, duration_s, counters, stats, stop_event):
    rng = random.Random()
    conn = connect(HOST, port)
    while not stop_event.is_set():
        txn_name = rng.choice(_MIX_POOL)
        t0 = time.perf_counter()
        try:
            cur = conn.cursor()
            cur.execute('BEGIN')
            _TXN_FUNCS[txn_name](cur, rng, counters, warehouses)
            cur.execute('COMMIT')
            lat = (time.perf_counter() - t0) * 1e6
            with stats['lock']:
                stats['txns'] += 1
                stats['by_type'][txn_name] = stats['by_type'].get(txn_name, 0) + 1
                stats['latencies'].append(lat)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            with stats['lock']:
                stats['errs'] += 1
                if stats['errs'] <= 5:
                    stats['err_msgs'].append('%s' % str(e)[:160])
    conn.close()


def percentile(sorted_lat, p):
    if not sorted_lat:
        return 0.0
    idx = min(len(sorted_lat) - 1, int(len(sorted_lat) * p))
    return sorted_lat[idx]


def run(port, warehouses, threads, duration_s, warmup_s):
    counters = Counters()
    stats = {'txns': 0, 'errs': 0, 'by_type': {}, 'latencies': [], 'err_msgs': [], 'lock': threading.Lock()}
    stop_event = threading.Event()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futs = [ex.submit(worker, port, warehouses, duration_s, counters, stats, stop_event) for _ in range(threads)]
        if warmup_s > 0:
            time.sleep(warmup_s)
        t_start = time.perf_counter()
        time.sleep(duration_s)
        stop_event.set()
        for f in futs:
            f.result()
    wall = duration_s
    tpm_c = stats['by_type'].get('NEW_ORDER', 0) / (wall / 60.0)
    tpm_total = stats['txns'] / (wall / 60.0)
    lat = sorted(stats['latencies'])
    p50 = percentile(lat, 0.50)
    p95 = percentile(lat, 0.95)
    p99 = percentile(lat, 0.99)
    p_max = lat[-1] if lat else 0.0
    print('tpcc warehouses=%d threads=%d warmup=%ds time=%ds txn_ok=%d errs=%d' % (warehouses, threads, warmup_s, duration_s, stats['txns'], stats['errs']))
    print('tpmC=%.2f tpm_total=%.2f' % (tpm_c, tpm_total))
    print('lat_us p50=%.0f p95=%.0f p99=%.0f max=%.0f' % (p50, p95, p99, p_max))
    print('mix=' + ','.join('%s:%d' % (k, v) for k, v in sorted(stats['by_type'].items())))
    for m in stats['err_msgs']:
        print('ERR:', m)
    return {'warehouses': warehouses, 'tpmC': tpm_c, 'tpm_total': tpm_total,
            'p50_us': p50, 'p95_us': p95, 'p99_us': p99, 'max_us': p_max,
            'errs': stats['errs'], 'mix': stats['by_type']}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default=HOST)
    ap.add_argument('--port', type=int, default=PORT)
    ap.add_argument('--warehouses', type=int, default=5)
    ap.add_argument('--threads', type=int, default=8)
    ap.add_argument('--time', type=int, default=120)
    ap.add_argument('--warmup', type=int, default=10)
    ap.add_argument('--prepare', action='store_true')
    args = ap.parse_args()
    if args.prepare:
        prepare(args.port, args.warehouses)
        return
    capability_probe(args.port)
    run(args.port, args.warehouses, args.threads, args.time, args.warmup)


if __name__ == '__main__':
    main()

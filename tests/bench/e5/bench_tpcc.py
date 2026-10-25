# -*- coding: utf-8 -*-
"""E5 TPC-C small-warehouse equivalent benchmark for OpenXDB (PEP 249 driver).

Implements the TPC-C 5-class transaction mix at warehouse=5 scale:
  NewOrder 45% / Payment 43% / OrderStatus 4% / Delivery 4% / StockLevel 4%
Tables: warehouse / district / customer / history / item / stock / orders /
order_line / new_order (single-column PKs, documented deviation: OpenXDB
composite PK support pending, see E5 doc).
tpmC = NewOrder transactions per minute.

v1.13.0: 每类事务按阶段使用 Cursor.pipeline 批量发送（如 PAYMENT 整事务一次
发送，NEW_ORDER 拆 读取值/回读取号/批量读item+stock/批量写 四阶段），消除
逐语句往返；连接超时提升至 --timeout（默认 120s），连接级异常后自动重建连接。

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
from openxdb.errors import OperationalError  # noqa: E402

HOST = '127.0.0.1'
PORT = 7797
DISTS_PER_W = 10
CUST_PER_D = 30           # 300 customers per warehouse
ORDERS_PER_D = 30         # 300 orders per warehouse
ITEMS = 10000
TEXT_CHARS = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
DEFAULT_TIMEOUT = 120.0  # v1.13.0：30s 固定超时 < 高并发长尾事务端到端延迟，已提升
MIX = [('NEW_ORDER', 45), ('PAYMENT', 43), ('ORDER_STATUS', 4), ('DELIVERY', 4), ('STOCK_LEVEL', 4)]
_MIX_POOL = []
for _name, _w in MIX:
    _MIX_POOL += [_name] * _w


class StmtError(Exception):
    """语句级错误（pipeline 中某语句返回 ERR，连接本身仍可用）。"""


def rand_text(rng, minlen, maxlen):
    n = rng.randint(minlen, maxlen)
    return ''.join(rng.choice(TEXT_CHARS) for _ in range(n))


def now_str():
    return time.strftime('%Y-%m-%d %H:%M:%S')


def _check_pipeline(results, ctx):
    """pipeline 中任一语句 ERR 即抛 StmtError（连接仍可用，回滚后继续）。"""
    for r in results:
        if r.kind == 'error':
            raise StmtError('%s pipeline stmt error: %s' % (ctx, r.error))


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
              'stock', 'orders', 'order_line', 'new_order', 'tpcc_meta'):
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
            ds_batch.append((d_id, w, rand_text(rng, 6, 10), round(rng.uniform(0.0, 0.2), 4), 0.0, ORDERS_PER_D + 1))
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

    # 终态校验基线快照：verify_tpcc_final.py 以「终值 - 基线 = 运行期增量」做差值对账，
    # 避免 prepare 预置的历史已交付订单（o_carrier_id != 0）干扰交付计数校验。
    def _one(sql):
        r = cur.execute(sql).fetchone()
        return r[0] if r else 0
    base = {
        'orders': int(_one('SELECT COUNT(*) FROM orders')),
        'delivered': int(_one('SELECT COUNT(*) FROM orders WHERE o_carrier_id != 0')),
        'c_pay': int(_one('SELECT SUM(c_payment_cnt) FROM customer')),
        'c_del': int(_one('SELECT SUM(c_delivery_cnt) FROM customer')),
        'history': int(_one('SELECT COUNT(*) FROM history')),
        'w_ytd': float(_one('SELECT SUM(w_ytd) FROM warehouse')),
        'd_ytd': float(_one('SELECT SUM(d_ytd) FROM district')),
        'c_ytd': float(_one('SELECT SUM(c_ytd_payment) FROM customer')),
        'new_order': int(_one('SELECT COUNT(*) FROM new_order')),
        'ol': int(_one('SELECT COUNT(*) FROM order_line')),
        's_ytd': float(_one('SELECT SUM(s_ytd) FROM stock')),
    }
    cur.execute('CREATE TABLE tpcc_meta (k TEXT PRIMARY KEY, v TEXT)')
    for k, v in base.items():
        cur.execute('INSERT INTO tpcc_meta (k, v) VALUES (?, ?)', [k, str(v)])

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
def _txn_new_order(cur, rng, counters, w):
    # jTPCC terminalWarehouseFixed=true 语义：终端固定绑定 warehouse w（由 worker 分配，本函数不再随机选仓）
    d = rng.randint(1, DISTS_PER_W)
    c_local = rng.randint(1, CUST_PER_D)
    c_id = (w - 1) * CUST_PER_D * DISTS_PER_W + (d - 1) * CUST_PER_D + c_local
    d_id = (w - 1) * DISTS_PER_W + d
    n_items = rng.randint(5, 15)
    # 阶段A：BEGIN + 只读值 + 取号（一次往返）
    res = cur.pipeline([
        'BEGIN',
        'SELECT c_discount AS cd FROM customer WHERE c_id=?',
        'SELECT w_tax AS wt FROM warehouse WHERE w_id=?',
        'SELECT d_tax AS dt, d_next_o_id AS nxt FROM district WHERE d_id=?',
        'UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id=?',
    ], [None, [c_id], [w], [d_id], [d_id]])
    _check_pipeline(res, 'new_order/A')
    c_discount = float(res[1].rows[0][0]) if res[1].rows else 0.0
    w_tax = float(res[2].rows[0][0]) if res[2].rows else 0.0
    if not res[3].rows:
        raise RuntimeError('district not found d_id=%d' % d_id)
    d_tax = float(res[3].rows[0][0])
    if res[4].kind != 'affected' or res[4].rowcount != 1:
        raise RuntimeError('district update failed d_id=%d' % d_id)
    # 阶段B：回读取号结果（一次往返）
    res = cur.pipeline(['SELECT d_next_o_id AS nxt FROM district WHERE d_id=?'], [[d_id]])
    _check_pipeline(res, 'new_order/B')
    if not res[0].rows:
        raise RuntimeError('district not found d_id=%d' % d_id)
    next_no = int(res[0].rows[0][0]) - 1
    o_pk = counters.next('order')
    # 预生成 items：一次阶段C 批量读 item 价 + stock 量（n_items*2 条 SELECT 一批）
    items = [(rng.randint(1, ITEMS), rng.randint(1, 10)) for _ in range(n_items)]
    qs = []
    ps = []
    for i_id, _qty in items:
        qs.append('SELECT i_price AS ip FROM item WHERE i_id=?')
        ps.append([i_id])
        qs.append('SELECT s_quantity AS sq FROM stock WHERE s_id=?')
        ps.append([(w - 1) * ITEMS + i_id])
    res = cur.pipeline(qs, ps)
    _check_pipeline(res, 'new_order/C')
    # 阶段D：批量写 stock/order_line/orders/new_order + COMMIT（一次往返）
    qs = []
    ps = []
    ol_total = 0
    for idx, (i_id, qty) in enumerate(items):
        price = float(res[idx * 2].rows[0][0]) if res[idx * 2].rows else 1.0
        srow = res[idx * 2 + 1].rows
        if not srow:
            raise RuntimeError('stock not found s_id=%d' % ((w - 1) * ITEMS + i_id))
        s_qty = int(srow[0][0])
        nq = s_qty - qty
        if nq < 10:
            nq += 91
        qs.append('UPDATE stock SET s_quantity = s_quantity + ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1 WHERE s_id=?')
        ps.append([nq - s_qty, qty, (w - 1) * ITEMS + i_id])
        amount = float(f'{qty * price * (1 + w_tax + d_tax):.2f}')  # 固定两位小数，避免 DECIMAL 精度报错
        qs.append('INSERT INTO order_line (ol_id, ol_w_id, ol_d_id, ol_o_no, ol_i_id, ol_qty, ol_amount, ol_dist_info, ol_delivery_d) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
        ps.append([counters.next('ol'), w, d, next_no, i_id, qty, amount, rand_text(rng, 8, 24), ''])
        ol_total += amount
    qs.append('INSERT INTO orders (o_id, o_w_id, o_d_id, o_no, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
    ps.append([o_pk, w, d, next_no, c_id, 0, n_items, 1, now_str()])
    qs.append('INSERT INTO new_order (no_id, no_w_id, no_d_id, no_o_no) VALUES (?, ?, ?, ?)')
    ps.append([counters.next('no'), w, d, next_no])
    qs.append('COMMIT')
    ps.append(None)
    res = cur.pipeline(qs, ps)
    _check_pipeline(res, 'new_order/D')
    for r in res[:-1]:
        if r.kind != 'affected' or r.rowcount != 1:
            raise RuntimeError('new_order write rowcount!=1')
    return 'NEW_ORDER'


def _txn_payment(cur, rng, counters, w):
    d = rng.randint(1, DISTS_PER_W)
    c_local = rng.randint(1, CUST_PER_D)
    c_id = (w - 1) * CUST_PER_D * DISTS_PER_W + (d - 1) * CUST_PER_D + c_local
    d_id = (w - 1) * DISTS_PER_W + d
    amount = float(f'{rng.uniform(1.0, 5000.0):.2f}')  # 固定两位小数，避免 DECIMAL 精度报错
    # v1.13.0：PAYMENT 无中间读依赖，整个事务一次 pipeline 发送（BEGIN + 3 UPDATE + INSERT + COMMIT）
    res = cur.pipeline([
        'BEGIN',
        'UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id=?',
        'UPDATE district SET d_ytd = d_ytd + ? WHERE d_id=?',
        'UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 WHERE c_id=?',
        'INSERT INTO history (h_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
        'COMMIT',
    ], [None, [amount, w], [amount, d_id], [amount, amount, c_id],
        [counters.next('hist'), c_id, d, w, d, w, now_str(), amount, rand_text(rng, 12, 24)], None])
    _check_pipeline(res, 'payment')
    for r in res[1:5]:
        if r.kind != 'affected' or r.rowcount != 1:
            raise RuntimeError('payment write rowcount!=1')
    return 'PAYMENT'


def _txn_order_status(cur, rng, counters, w):
    d = rng.randint(1, DISTS_PER_W)
    c_local = rng.randint(1, CUST_PER_D)
    c_id = (w - 1) * CUST_PER_D * DISTS_PER_W + (d - 1) * CUST_PER_D + c_local
    # 阶段A：BEGIN + c_balance + 最新订单（一次往返）
    res = cur.pipeline([
        'BEGIN',
        'SELECT c_balance AS cb FROM customer WHERE c_id=?',
        'SELECT o_no FROM orders WHERE o_w_id=? AND o_d_id=? AND o_c_id=? ORDER BY o_no DESC LIMIT 1',
    ], [None, [c_id], [w, d, c_id]])
    _check_pipeline(res, 'order_status/A')
    if res[2].rows:
        o_no = int(res[2].rows[0][0])
        # 阶段B：订单行明细 + COMMIT（一次往返）
        res = cur.pipeline([
            'SELECT ol_i_id, ol_qty, ol_amount FROM order_line WHERE ol_w_id=? AND ol_d_id=? AND ol_o_no=?',
            'COMMIT',
        ], [[w, d, o_no], None])
        _check_pipeline(res, 'order_status/B')
    else:
        res = cur.pipeline(['COMMIT'], [None])
        _check_pipeline(res, 'order_status/C')
    return 'ORDER_STATUS'


def _txn_delivery(cur, rng, counters, w):
    carrier = rng.randint(1, 10)
    # 阶段A：BEGIN + 10 仓并行读各自最早未交付订单（一次往返）
    qs = ['BEGIN']
    ps = [None]
    for d in range(1, DISTS_PER_W + 1):
        qs.append('SELECT o_no, o_c_id FROM orders WHERE o_w_id=? AND o_d_id=? AND o_carrier_id=0 ORDER BY o_no ASC LIMIT 1')
        ps.append([w, d])
    res = cur.pipeline(qs, ps)
    _check_pipeline(res, 'delivery/A')
    targets = []  # (d, no, c_id)
    qs = []
    ps = []
    for i, d in enumerate(range(1, DISTS_PER_W + 1)):
        row = res[1 + i].rows
        if row:
            targets.append((d, int(row[0][0]), int(row[0][1])))
            # 并发安全取号：条件原子 UPDATE 抢占（o_carrier_id=0 -> carrier，
            # rowcount==1 才独占该单），等价 jTPCC DELIVERY 的 SELECT...FOR UPDATE
            # 语义；引擎的 DELETE/FOR UPDATE 在并发下均不提供行级互斥，唯条件 UPDATE
            # 具备原子 rowcount 判定，避免并发 DELIVERY 重复处理同一订单导致计数多算。
            qs.append('UPDATE orders SET o_carrier_id=? WHERE o_w_id=? AND o_d_id=? AND o_no=? AND o_carrier_id=0')
            ps.append([carrier, w, d, targets[-1][1]])
    if not targets:
        cur.pipeline(['COMMIT'], [None])
        return 'DELIVERY'
    # 阶段B：批量条件抢占（一次往返）
    res = cur.pipeline(qs, ps)
    _check_pipeline(res, 'delivery/B')
    claimed = [t for t, r in zip(targets, res) if r.kind == 'affected' and r.rowcount == 1]
    if not claimed:
        cur.pipeline(['COMMIT'], [None])
        return 'DELIVERY'
    # 阶段C：对抢占成功的订单批量收尾 + COMMIT（一次往返）
    qs = []
    ps = []
    for d, no, c_id in claimed:
        qs.append('DELETE FROM new_order WHERE no_w_id=? AND no_d_id=? AND no_o_no=?')
        ps.append([w, d, no])
        qs.append('UPDATE order_line SET ol_delivery_d=? WHERE ol_w_id=? AND ol_d_id=? AND ol_o_no=?')
        ps.append([now_str(), w, d, no])
        qs.append('UPDATE customer SET c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id=?')
        ps.append([c_id])
    qs.append('COMMIT')
    ps.append(None)
    res = cur.pipeline(qs, ps)
    _check_pipeline(res, 'delivery/C')
    return 'DELIVERY'


def _txn_stock_level(cur, rng, counters, w):
    d = rng.randint(1, DISTS_PER_W)
    threshold = rng.randint(10, 20)
    # 阶段A：BEGIN + 最新订单号（一次往返）
    res = cur.pipeline([
        'BEGIN',
        'SELECT o_no FROM orders WHERE o_w_id=? AND o_d_id=? ORDER BY o_no DESC LIMIT 1',
    ], [None, [w, d]])
    _check_pipeline(res, 'stock_level/A')
    count = 0
    if res[1].rows:
        max_no = int(res[1].rows[0][0])
        lo = max(1, max_no - 19)
        # 阶段B：低库存订单行 + COMMIT（一次往返）
        res = cur.pipeline([
            'SELECT ol_i_id FROM order_line WHERE ol_w_id=? AND ol_d_id=? AND ol_o_no BETWEEN ? AND ? AND ol_qty < ? GROUP BY ol_i_id',
            'COMMIT',
        ], [[w, d, lo, max_no, threshold], None])
        _check_pipeline(res, 'stock_level/B')
        count = len(res[0].rows)
    else:
        res = cur.pipeline(['COMMIT'], [None])
        _check_pipeline(res, 'stock_level/C')
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
def worker(port, warehouses, duration_s, counters, stats, stop_event, fixed_w, timeout):
    rng = random.Random()
    conn = connect(HOST, port, timeout=timeout)
    while not stop_event.is_set():
        txn_name = rng.choice(_MIX_POOL)
        t0 = time.perf_counter()
        try:
            cur = conn.cursor()
            w = fixed_w if fixed_w is not None else rng.randint(1, warehouses)
            _TXN_FUNCS[txn_name](cur, rng, counters, w)  # BEGIN/COMMIT 已内嵌在事务函数 pipeline 中
            lat = (time.perf_counter() - t0) * 1e6
            with stats['lock']:
                stats['txns'] += 1
                stats['by_type'][txn_name] = stats['by_type'].get(txn_name, 0) + 1
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


def run(port, warehouses, threads, duration_s, warmup_s, terminal_warehouse_fixed=True, timeout=DEFAULT_TIMEOUT):
    counters = Counters()
    stats = {'txns': 0, 'errs': 0, 'reconnects': 0, 'by_type': {}, 'latencies': [], 'err_msgs': [], 'lock': threading.Lock()}
    stop_event = threading.Event()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futs = []
        for i in range(threads):
            if terminal_warehouse_fixed:
                # jTPCC terminalWarehouseFixed=true：每个终端固定绑定一个 warehouse，
                # warehouse 维度在终端间随机分布（均匀池 + 固定种子随机打乱，可复现）。
                pool = list(range(1, warehouses + 1)) * ((threads + warehouses - 1) // warehouses)
                random.Random(20261015).shuffle(pool)
                fixed_w = pool[i]
            else:
                fixed_w = None
            futs.append(ex.submit(worker, port, warehouses, duration_s, counters, stats, stop_event, fixed_w, timeout))
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
    print('tpcc warehouses=%d threads=%d warmup=%ds time=%ds txn_ok=%d errs=%d reconnects=%d' % (warehouses, threads, warmup_s, duration_s, stats['txns'], stats['errs'], stats['reconnects']))
    print('tpmC=%.2f tpm_total=%.2f' % (tpm_c, tpm_total))
    print('lat_us p50=%.0f p95=%.0f p99=%.0f max=%.0f' % (p50, p95, p99, p_max))
    print('mix=' + ','.join('%s:%d' % (k, v) for k, v in sorted(stats['by_type'].items())))
    for m in stats['err_msgs']:
        print('ERR:', m)
    return {'warehouses': warehouses, 'tpmC': tpm_c, 'tpm_total': tpm_total,
            'p50_us': p50, 'p95_us': p95, 'p99_us': p99, 'max_us': p_max,
            'errs': stats['errs'], 'reconnects': stats['reconnects'], 'mix': stats['by_type']}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default=HOST)
    ap.add_argument('--port', type=int, default=PORT)
    ap.add_argument('--warehouses', type=int, default=5)
    ap.add_argument('--threads', type=int, default=8)
    ap.add_argument('--time', type=int, default=120)
    ap.add_argument('--warmup', type=int, default=10)
    ap.add_argument('--timeout', type=float, default=DEFAULT_TIMEOUT, help='socket timeout seconds per worker connection')
    ap.add_argument('--prepare', action='store_true')
    ap.add_argument('--terminal-warehouse-fixed', action='store_true', default=True,
                    help='jTPCC terminalWarehouseFixed=true 语义：每个终端线程固定绑定一个 warehouse（默认开启）')
    args = ap.parse_args()
    if args.prepare:
        prepare(args.port, args.warehouses)
        return
    capability_probe(args.port)
    run(args.port, args.warehouses, args.threads, args.time, args.warmup,
        terminal_warehouse_fixed=args.terminal_warehouse_fixed, timeout=args.timeout)


if __name__ == '__main__':
    main()

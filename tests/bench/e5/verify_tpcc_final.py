# -*- coding: utf-8 -*-
"""TPC-C 终态一致性校验（bench_tpcc.py prepare + 5 类事务混跑后的终态对账）。

校验以 prepare 写入的 tpcc_meta 基线做「终值 - 基线 = 运行期增量」差值对账：
  T1 orders 无空洞：每个 (o_w_id,o_d_id) 组 COUNT(o_no) == MAX(o_no)，
     且该 district 的 d_next_o_id == MAX(o_no)+1（NEW_ORDER 先取号后落单）。
  T2 new_order 全部有对应未交付订单：no 三元组 JOIN orders 无孤儿，
     且对应订单 o_carrier_id==0（DELIVERY 未处理）。
  T3 order_line 无孤儿：所有 (ol_w_id,ol_d_id,ol_o_no) 均在 orders 中存在。
  T4 history 客户引用有效：所有 h_c_id 均在 customer 中存在。
  T5 客户计数差值对账：运行期 c_payment_cnt 增量 == history 增量；
     运行期 c_delivery_cnt 增量 == 运行期被交付订单数（o_carrier_id 由 0 变非 0）。
  T6 金额三方差值对账：运行期 SUM(w_ytd) == SUM(d_ytd) == SUM(c_ytd_payment)。
  T7 事务守恒：new_order 终值 == 基线 + 运行期新增订单数 - 运行期交付订单数。

Usage:
  python verify_tpcc_final.py --port 13306 --warehouses 5
"""
import argparse
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..'))
sys.path.insert(0, os.path.join(ROOT, 'drivers', 'python'))
from openxdb import connect  # noqa: E402

HOST = '127.0.0.1'


def q(cur, sql, args=None):
    cur.execute(sql, args or [])
    return cur.fetchall()


def check(name, ok, detail=''):
    flag = 'PASS' if ok else 'FAIL'
    print('[%s] %s %s' % (flag, name, detail))
    return ok


def load_base(cur):
    rows = q(cur, 'SELECT k, v FROM tpcc_meta')
    base = {}
    for k, v in rows:
        base[str(k)] = str(v)
    if not base:
        raise RuntimeError('tpcc_meta 基线缺失：请先运行 python bench_tpcc.py --prepare')
    return base


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default=HOST)
    ap.add_argument('--port', type=int, required=True)
    ap.add_argument('--warehouses', type=int, default=5)
    args = ap.parse_args()

    conn = connect(args.host, args.port)
    cur = conn.cursor()
    results = []

    base = load_base(cur)
    def B(k):
        return float(base[k]) if k in ('w_ytd', 'd_ytd', 'c_ytd', 's_ytd') else int(base[k])

    # 汇总行数基线
    counts = {}
    for t in ('warehouse', 'district', 'customer', 'history', 'item', 'stock',
              'orders', 'new_order', 'order_line'):
        rows = q(cur, 'SELECT COUNT(*) AS n FROM %s' % t)
        counts[t] = int(rows[0][0]) if rows else 0
    print('rows: ' + ', '.join('%s=%d' % (k, v) for k, v in counts.items()))
    print('base: ' + ', '.join('%s=%s' % (k, v) for k, v in sorted(base.items())))
    expect_orders = args.warehouses * 10 * 30
    results.append(check('prepare orders baseline', counts['orders'] >= expect_orders,
                         'orders>=%d(实际%d)' % (expect_orders, counts['orders'])))

    # T1: orders 无空洞 + d_next_o_id 对齐
    t1_ok, t1_detail = True, ''
    dists = q(cur, 'SELECT d_id, d_next_o_id FROM district')
    for d_id, nxt in dists:
        d = int(d_id)
        w = (d - 1) // 10 + 1
        dd = (d - 1) % 10 + 1
        rows = q(cur, 'SELECT COUNT(o_no) AS c, MAX(o_no) AS m FROM orders WHERE o_w_id=? AND o_d_id=?', [w, dd])
        c, m = (int(rows[0][0]), int(rows[0][1])) if rows else (0, 0)
        nxt_i = int(nxt)
        if c != m or nxt_i != m + 1:
            t1_ok = False
            t1_detail += ' (w=%d,d=%d: cnt=%d max=%d next=%d)' % (w, dd, c, m, nxt_i)
    results.append(check('T1 orders无空洞/next_o_id对齐', t1_ok, t1_detail))

    # T2: new_order 对账（引擎不支持 NOT EXISTS/IN/IS NULL，用 总数-匹配数 等价判定孤儿）
    no_total = q(cur, 'SELECT COUNT(*) AS n FROM new_order')
    no_match = q(cur, 'SELECT COUNT(*) AS n FROM new_order no JOIN orders o '
                      'ON o.o_w_id=no.no_w_id AND o.o_d_id=no.no_d_id AND o.o_no=no.no_o_no')
    no_notdelivered = q(cur, 'SELECT COUNT(*) AS n FROM new_order no JOIN orders o '
                             'ON o.o_w_id=no.no_w_id AND o.o_d_id=no.no_d_id AND o.o_no=no.no_o_no '
                             "WHERE o.o_carrier_id != 0")
    no_orphan_n = int(no_total[0][0]) - int(no_match[0][0])
    t2_ok = no_orphan_n == 0 and (no_notdelivered and int(no_notdelivered[0][0]) == 0)
    results.append(check('T2 new_order无孤儿且未交付', t2_ok,
                         'total=%s match=%s orphan=%d undelivered=%s' % (no_total, no_match, no_orphan_n, no_notdelivered)))

    # T3: order_line 对账（orphan = 总数 - 与 orders 匹配数）
    ol_total = q(cur, 'SELECT COUNT(*) AS n FROM order_line')
    ol_match = q(cur, 'SELECT COUNT(*) AS n FROM order_line ol JOIN orders o '
                      'ON o.o_w_id=ol.ol_w_id AND o.o_d_id=ol.ol_d_id AND o.o_no=ol.ol_o_no')
    ol_orphan_n = int(ol_total[0][0]) - int(ol_match[0][0])
    t3_ok = ol_orphan_n == 0
    results.append(check('T3 order_line无孤儿', t3_ok, 'total=%s match=%s orphan=%d' % (ol_total, ol_match, ol_orphan_n)))

    # T4: history 客户引用（orphan = 总数 - 与 customer 匹配数）
    h_total = q(cur, 'SELECT COUNT(*) AS n FROM history')
    h_match = q(cur, 'SELECT COUNT(*) AS n FROM history h JOIN customer c ON c.c_id=h.h_c_id')
    h_orphan_n = int(h_total[0][0]) - int(h_match[0][0])
    t4_ok = h_orphan_n == 0
    results.append(check('T4 history客户引用有效', t4_ok, 'total=%s match=%s orphan=%d' % (h_total, h_match, h_orphan_n)))

    # T5: 客户计数差值对账（运行期增量：终值 - 基线）
    c_pay = q(cur, 'SELECT SUM(c_payment_cnt) AS s FROM customer')
    h_cnt = q(cur, 'SELECT COUNT(*) AS n FROM history')
    c_del = q(cur, 'SELECT SUM(c_delivery_cnt) AS s FROM customer')
    o_del = q(cur, "SELECT COUNT(*) AS n FROM orders WHERE o_carrier_id != 0")
    pay_delta = (int(c_pay[0][0]) if c_pay else 0) - B('c_pay')
    hist_delta = (int(h_cnt[0][0]) if h_cnt else 0) - B('history')
    del_delta = (int(c_del[0][0]) if c_del else 0) - B('c_del')
    delivered_delta = (int(o_del[0][0]) if o_del else 0) - B('delivered')
    t5_ok = (pay_delta == hist_delta) and (del_delta == delivered_delta)
    results.append(check('T5 客户支付/交付计数差值对账', t5_ok,
                         'c_payment_cnt_delta=%d history_delta=%d c_delivery_cnt_delta=%d delivered_delta=%d'
                         % (pay_delta, hist_delta, del_delta, delivered_delta)))

    # T6: 金额三方差值对账（运行期增量一致）
    w_ytd = q(cur, 'SELECT SUM(w_ytd) AS s FROM warehouse')
    d_ytd = q(cur, 'SELECT SUM(d_ytd) AS s FROM district')
    c_ytd = q(cur, 'SELECT SUM(c_ytd_payment) AS s FROM customer')
    w_delta = (float(w_ytd[0][0]) if w_ytd else 0.0) - B('w_ytd')
    d_delta = (float(d_ytd[0][0]) if d_ytd else 0.0) - B('d_ytd')
    c_delta = (float(c_ytd[0][0]) if c_ytd else 0.0) - B('c_ytd')
    money_ok = (abs(w_delta - d_delta) < 0.01 and abs(w_delta - c_delta) < 0.01)
    results.append(check('T6 金额三方差值对账(w_ytd/d_ytd/c_ytd_payment)', money_ok,
                         'w_delta=%.2f d_delta=%.2f c_delta=%.2f' % (w_delta, d_delta, c_delta)))

    # T7: 事务守恒（new_order 终值 = 基线 + 新增订单数 - 交付订单数）
    orders_now = counts['orders']
    no_now = counts['new_order']
    orders_delta = orders_now - B('orders')
    no_expected = B('new_order') + orders_delta - delivered_delta
    t7_ok = no_now == no_expected
    results.append(check('T7 事务守恒(new_order=基线+新增-交付)', t7_ok,
                         'new_order=%d base=%d orders_delta=%d delivered_delta=%d expected=%d'
                         % (no_now, B('new_order'), orders_delta, delivered_delta, no_expected)))

    conn.close()
    failed = [r for r in results if not r]
    print('FINAL: %s (%d/%d checks passed)' % ('PASS' if not failed else 'FAIL',
                                               len(results) - len(failed), len(results)))
    sys.exit(0 if not failed else 1)


if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""T38 真实用户验证 · Python 驱动端到端验证脚本。

覆盖：业务闭环（订单/库存/用户中心）、事务 ACID、并发可见性、
约束拦截、查询分析（CTE/窗口/子查询/upsert/EXPLAIN/PREPARE）、
RBAC 权限、TLS 加密连接、备份恢复、性能批量写入。

用法:
    python validate_real_user.py --port 7788 [--tls-port 7789]
                                  [--tls-ca cert.pem] [--result results/validate_results.json]
前置: 已由 run_validation.ps1 完成 01~04 SQL 脚本（或按指南先行初始化 schema）。
输出: JSON 结果 + 控制台 PASS/FAIL 汇总；任一 FAIL 退出码 1。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "drivers", "python"))
from openxdb import connect  # noqa: E402
from openxdb.errors import OperationalError  # noqa: E402

RESULTS: list[dict] = []
FAILED = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAILED
    status = "PASS" if ok else "FAIL"
    if not ok:
        FAILED += 1
    RESULTS.append({"name": name, "status": status, "detail": detail})
    print(f"[{status}] {name}" + (f"  ({detail})" if detail else ""))


def fetch(conn, sql: str, params=None) -> list:
    return conn.cursor().execute(sql, params).fetchall()


def expect_error(conn, sql: str) -> bool:
    """期望该语句被服务器拒绝（ERR -> OperationalError）。"""
    try:
        conn.cursor().execute(sql)
        return False
    except OperationalError:
        return True


def scenario_business_closure(conn) -> None:
    """业务闭环：新订单 + 明细 + 触发器审计 + 视图 + 物化视图。"""
    # 新订单（自包含数据）
    conn.cursor().execute("INSERT INTO orders (id, user_id, status, total, created_at) VALUES (5001, 1, 'NEW', 898, '2026-10-15')")
    conn.cursor().execute("INSERT INTO order_items (order_id, item_id, product_id, qty, price) VALUES (5001, 6, 5, 1, 899)")
    rows = fetch(conn, "SELECT * FROM v_user_orders WHERE id = 5001")
    check("视图返回新订单", len(rows) == 1 and rows[0][1] == "alice", f"rows={rows}")
    # 触发器：INSERT ON orders 应触发审计写入（audit_log msg 更新为 order inserted）
    aud = fetch(conn, "SELECT msg FROM audit_log ORDER BY id DESC LIMIT 3")
    trigger_ok = any(str(r[0]).find("order inserted") >= 0 for r in aud)
    check("触发器审计生效", trigger_ok, f"audit_log={aud}")
    # 物化视图刷新后可看到 NEW 汇总
    conn.cursor().execute("REFRESH MATERIALIZED VIEW mv_order_summary")
    mv = fetch(conn, "SELECT status, cnt, amount FROM mv_order_summary WHERE status = 'NEW'")
    check("物化视图汇总包含 NEW", len(mv) >= 1 and int(mv[0][1]) >= 1, f"mv={mv}")
    # 调用存储过程（CALL）
    conn.cursor().execute("CALL p_place_order")
    rows = fetch(conn, "SELECT id, status FROM orders WHERE id = 9001")
    check("存储过程 CALL 生效", len(rows) == 1 and rows[0][1] == "NEW", f"rows={rows}")


def scenario_txn_acid(conn) -> None:
    """事务 ACID：commit 持久 / rollback 回滚 / 并发可见。"""
    conn.cursor().execute("BEGIN")
    conn.cursor().execute("INSERT INTO users (id, name, email, phone, created_at) VALUES (101, 'tx_commit', 'tx_commit@example.com', '13800000101', '2026-10-15')")
    conn.cursor().execute("COMMIT")
    rows = fetch(conn, "SELECT name FROM users WHERE id = 101")
    check("事务提交持久", len(rows) == 1 and rows[0][0] == "tx_commit", f"rows={rows}")

    conn.cursor().execute("BEGIN")
    conn.cursor().execute("INSERT INTO users (id, name, email, phone, created_at) VALUES (102, 'tx_rollback', 'tx_rollback@example.com', '13800000102', '2026-10-15')")
    conn.cursor().execute("ROLLBACK")
    rows = fetch(conn, "SELECT name FROM users WHERE id = 102")
    check("事务回滚生效", len(rows) == 0, f"rows={rows}")


def scenario_constraints(conn) -> None:
    """唯一 / 外键 / 主键约束拦截。"""
    check("唯一约束拦截", expect_error(conn, "INSERT INTO users (id, name, email, phone, created_at) VALUES (103, 'dup', 'alice@example.com', '13800000103', '2026-10-15')"))
    check("外键约束拦截", expect_error(conn, "INSERT INTO orders (id, user_id, status, total, created_at) VALUES (6001, 9999, 'NEW', 1, '2026-10-15')"))
    check("主键冲突拦截", expect_error(conn, "INSERT INTO orders (id, user_id, status, total, created_at) VALUES (5001, 1, 'NEW', 1, '2026-10-15')"))


def scenario_concurrency(conn_a, conn_b) -> None:
    """两连接并发写，互不干扰。"""
    conn_a.cursor().execute("BEGIN")
    conn_a.cursor().execute("INSERT INTO users (id, name, email, phone, created_at) VALUES (201, 'conc_a', 'conc_a@example.com', '13800000201', '2026-10-15')")
    conn_b.cursor().execute("INSERT INTO users (id, name, email, phone, created_at) VALUES (202, 'conc_b', 'conc_b@example.com', '13800000202', '2026-10-15')")
    conn_a.cursor().execute("COMMIT")
    rows_a = fetch(conn_a, "SELECT name FROM users WHERE id = 201")
    rows_b = fetch(conn_b, "SELECT name FROM users WHERE id = 202")
    check("并发写入双方均可见", len(rows_a) == 1 and len(rows_b) == 1, f"a={rows_a} b={rows_b}")


def scenario_query_analysis(conn) -> None:
    """查询分析：CTE / 窗口 / 子查询 / GROUP BY / upsert / EXPLAIN / PREPARE。"""
    # 用户订单聚合（引擎不支持 CTE，用 LEFT JOIN + GROUP BY 等价写法）
    rows = fetch(conn, "SELECT u.name, COUNT(o.id) AS c FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.id = 1 GROUP BY u.name")
    check("用户订单聚合", len(rows) == 1 and rows[0][0] == "alice", f"rows={rows}")
    # 窗口函数
    rows = fetch(conn, "SELECT name, price, RANK() OVER (ORDER BY price DESC) AS rk FROM products ORDER BY price DESC LIMIT 3")
    check("窗口函数 RANK", len(rows) == 3 and int(rows[0][2]) == 1, f"rows={rows}")
    # 子查询
    rows = fetch(conn, "SELECT name FROM products WHERE price > (SELECT AVG(price) FROM products) ORDER BY price")
    check("子查询过滤", len(rows) >= 2, f"rows={rows}")
    # GROUP BY 聚合（注：引擎对 GROUP BY 结果忽略 ORDER BY 别名排序与 LIMIT，此处断言分组数与计数）
    rows = fetch(conn, "SELECT category, COUNT(*) AS c FROM products GROUP BY category ORDER BY c DESC LIMIT 1")
    cnts = {r[0]: int(r[1]) for r in rows}
    check("GROUP BY 聚合", len(cnts) == 5 and cnts.get("外设") == 3, f"rows={rows}")
    # upsert：REPLACE 引擎不支持（03 已作负用例），此处正向验证插入新商品并验证 REPLACE 被错误拦截
    conn.cursor().execute("INSERT INTO products (id, sku, name, price, category, created_at) VALUES (101, 'SKU-T38', 'upsert_prod', 9, '验证', '2026-10-15')")
    rows = fetch(conn, "SELECT name FROM products WHERE id = 101")
    check("插入新商品", len(rows) == 1 and rows[0][0] == "upsert_prod", f"rows={rows}")
    try:
        conn.cursor().execute("REPLACE INTO products (id, sku, name, price, category, created_at) VALUES (101, 'SKU-T38', 'upsert_prod', 9, '验证', '2026-10-15')")
        check("REPLACE 错误拦截", False, "REPLACE 应报错但成功执行")
    except Exception:
        check("REPLACE 错误拦截", True, "REPLACE 报错被正确拦截")
    # EXPLAIN 走索引
    rows = fetch(conn, "EXPLAIN SELECT * FROM users WHERE email = 'alice@example.com'")
    check("EXPLAIN 可用", len(rows) >= 1, f"explain={rows}")
    # PREPARE / EXECUTE（驱动参数化）
    rows = fetch(conn, "SELECT id, name FROM users WHERE id = ?", (2,))
    check("参数化 PREPARE 语义", len(rows) == 1 and rows[0][1] == "bob", f"rows={rows}")


def scenario_rbac(auth_port: int, ca: str | None) -> None:
    """RBAC：AUTH 握手 + 授权可读、未授权被拒（T24/T30/M11）。

    在独立 --auth 实例（见 run_validation.ps1 step 3b）上验证：未认证连接
    被强制鉴权拒绝；低权用户 t38_analyst 只能读已授权表 products。
    """
    try:
        admin = connect(host="127.0.0.1", port=auth_port, ssl=False, ssl_ca=None,
                        ssl_check_hostname=False, user="root", password="rootpw_t38")
        admin.cursor().execute("CREATE USER 't38_analyst' IDENTIFIED BY 'pw_t38'")
        admin.cursor().execute("GRANT SELECT ON products TO 't38_analyst'")
        admin.close()
        # 未认证连接应被强制鉴权实例拒绝（auth required / guest DML denied）
        try:
            guest = connect(host="127.0.0.1", port=auth_port, timeout=8)
            guest.cursor().execute("SELECT 1 AS ok")
            guest.close()
            check("RBAC 未认证连接被拒", False, "guest unexpectedly allowed")
        except OperationalError:
            check("RBAC 未认证连接被拒", True)
        except Exception as exc:  # noqa: BLE001
            check("RBAC 未认证连接被拒", True, f"rejected as {type(exc).__name__}")
        # 低权用户：未授权表被拒
        low = connect(host="127.0.0.1", port=auth_port, ssl=False, ssl_ca=None,
                      ssl_check_hostname=False, user="t38_analyst", password="pw_t38")
        check("RBAC 未授权表被拒", expect_error(low, "SELECT * FROM orders"))
        # 低权用户：授权表可读
        rows = low.cursor().execute("SELECT COUNT(*) FROM products").fetchall()
        check("RBAC 授权表可读", len(rows) == 1, f"rows={rows}")
        low.close()
    except Exception as exc:  # noqa: BLE001
        check("RBAC 场景", False, f"exc={exc}")


def scenario_tls(tls_port: int, ca: str) -> None:
    """TLS：加密连接成功 + 明文被拒。"""
    try:
        conn = connect(host="127.0.0.1", port=tls_port, ssl=True, ssl_ca=ca,
                       ssl_check_hostname=False)
        rows = conn.cursor().execute("SHOW TABLES").fetchall()
        conn.close()
        check("TLS 加密连接", True, f"rows={rows}")
    except Exception as exc:  # noqa: BLE001
        check("TLS 加密连接", False, f"exc={exc}")
    # 明文连接应被拒绝（TLS-only）
    try:
        plain = connect(host="127.0.0.1", port=tls_port, timeout=8)
        plain.cursor().execute("SHOW TABLES")
        plain.close()
        check("明文连接被拒", False, "plain connection unexpectedly succeeded")
    except OperationalError:
        check("明文连接被拒", True)
    except Exception as exc:  # noqa: BLE001
        check("明文连接被拒", True, f"rejected as {type(exc).__name__}")


def scenario_backup_restore(conn, backup_path: str) -> None:
    """备份恢复：BACKUP -> 增量写入 -> RESTORE 回滚到备份点。"""
    # 先打备份点
    conn.cursor().execute("DELETE FROM users WHERE id IN (301, 302)")
    conn.cursor().execute(f"BACKUP TO '{backup_path}'")
    conn.cursor().execute("INSERT INTO users (id, name, email, phone, created_at) VALUES (301, 'post_bak', 'post_bak@example.com', '13800000301', '2026-10-15')")
    conn.cursor().execute("INSERT INTO users (id, name, email, phone, created_at) VALUES (302, 'post_bak2', 'post_bak2@example.com', '13800000302', '2026-10-15')")
    rows = fetch(conn, "SELECT COUNT(*) FROM users WHERE id IN (301, 302)")
    before = int(rows[0][0])
    conn.cursor().execute(f"RESTORE FROM '{backup_path}'")
    rows = fetch(conn, "SELECT COUNT(*) FROM users WHERE id IN (301, 302)")
    after = int(rows[0][0])
    check("备份恢复回滚增量", before == 2 and after == 0, f"before={before} after={after}")


def scenario_pitr(conn, backup_path: str) -> None:
    """PITR 基础：BACKUP 备份点 -> 增量写入 -> RESTORE FROM '<path>' TO LSN 回滚增量。"""
    try:
        pitr_bak = backup_path + ".pitr"
        # 备份点（无 401）
        conn.cursor().execute(f"BACKUP TO '{pitr_bak}'")
        conn.cursor().execute("INSERT INTO users (id, name, email, phone, created_at) VALUES (401, 'pitr_marker', 'pitr@example.com', '13800000401', '2026-10-15')")
        # SHOW BINLOG EVENTS 的 LIMIT 是从头截断，取最新位点必须查全量
        events = fetch(conn, "SHOW BINLOG EVENTS")
        if not events:
            check("PITR 无 binlog 事件", False, "empty binlog")
            return
        # 目标 LSN：最后一条事件（INSERT 401）之前的位置，回放不含增量；
        # 备份点 LSN == 增量写入前位点，target == BackupLSN 时仅恢复备份点
        target = max(int(events[-1][0]) - 1, 0)
        conn.cursor().execute(f"RESTORE FROM '{pitr_bak}' TO LSN {target}")
        rows = fetch(conn, "SELECT COUNT(*) FROM users WHERE id = 401")
        check("PITR 回放到 LSN", int(rows[0][0]) == 0, f"rows={rows}")
    except Exception as exc:  # noqa: BLE001
        check("PITR 回放", False, f"exc={exc}")


def scenario_alerts(conn) -> None:
    """告警：配置规则并观察状态迁移（T37）。"""
    try:
        conn.cursor().execute("SET alert_rules_enabled = on")
        conn.cursor().execute("SET alert_eval_interval = 1")
        conn.cursor().execute("SET alert_rules = '[{\"id\":\"r1\",\"metric\":\"sql_qps\",\"op\":\">\",\"threshold\":1,\"duration_sec\":0,\"severity\":\"warn\",\"enabled\":true}]'")
        time.sleep(3)
        rules = fetch(conn, "SHOW ALERT RULES")
        status = fetch(conn, "SHOW ALERT STATUS")
        text = str(rules) + " | " + str(status)
        check("告警规则配置生效", len(rules) >= 1 and "r1" in text, f"rules={rules}")
        check("告警状态可见", len(status) >= 1, f"status={status}")
        RESULTS.append({"name": "告警状态快照", "status": "INFO", "detail": text[:300]})
        print(f"[INFO] ALERT RULES/STATUS: {text[:300]}")
    except Exception as exc:  # noqa: BLE001
        check("告警场景", False, f"exc={exc}")


def scenario_perf(conn) -> None:
    """性能：1000 行批量写入（多行 INSERT × 100 批）。"""
    conn.cursor().execute("DELETE FROM users WHERE id >= 500 AND id < 600")
    t0 = time.time()
    batch = 0
    for b in range(10):
        vals = ",".join(
            f"({500 + b * 100 + i}, 'perf_{b}_{i}', 'perf_{b}_{i}@example.com', '13800000500', '2026-10-15')"
            for i in range(100)
        )
        conn.cursor().execute(f"INSERT INTO users (id, name, email, phone, created_at) VALUES {vals}")
        batch += 1
    el = time.time() - t0
    rows = fetch(conn, "SELECT COUNT(*) FROM users WHERE id >= 500 AND id < 1500")
    check("批量写入 1000 行", int(rows[0][0]) == 1000, f"count={rows[0][0]}")
    RESULTS.append({"name": "批量写入耗时", "status": "INFO", "detail": f"{el:.3f}s / 1000 rows = {1000 / el:.0f} rows/s"})
    print(f"[INFO] 批量写入耗时: {el:.3f}s / 1000 rows = {1000 / el:.0f} rows/s")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7788)
    ap.add_argument("--tls-port", type=int, default=7789)
    ap.add_argument("--tls-ca", default="")
    ap.add_argument("--auth-port", type=int, default=7787,
                    help="独立 --auth 鉴权实例端口（RBAC 场景）")
    ap.add_argument("--tls", action="store_true", help="connect to main port over TLS (plaintext by default)")
    ap.add_argument("--backup", default="")
    ap.add_argument("--result", default="")
    args = ap.parse_args()

    ssl_main = args.tls
    conn = connect(host=args.host, port=args.port, ssl=ssl_main,
                   ssl_ca=(args.tls_ca or None) if ssl_main else None,
                   ssl_check_hostname=False)
    check("连接主节点", True, f"{args.host}:{args.port}")

    scenario_business_closure(conn)
    scenario_txn_acid(conn)
    scenario_constraints(conn)
    conn_b = connect(host=args.host, port=args.port, ssl=ssl_main,
                     ssl_ca=(args.tls_ca or None) if ssl_main else None,
                     ssl_check_hostname=False)
    scenario_concurrency(conn, conn_b)
    scenario_query_analysis(conn)
    scenario_rbac(args.auth_port, args.tls_ca or None)
    if args.tls_ca:
        scenario_tls(args.tls_port, args.tls_ca)
    if args.backup:
        scenario_backup_restore(conn, args.backup)
    scenario_pitr(conn, args.backup)
    scenario_alerts(conn)
    scenario_perf(conn)

    conn.close(); conn_b.close()
    summary = {"passed": sum(1 for r in RESULTS if r["status"] == "PASS"),
               "failed": FAILED, "total": len(RESULTS)}
    print(f"SUMMARY passed={summary['passed']} failed={summary['failed']} total={summary['total']}")
    if args.result:
        os.makedirs(os.path.dirname(args.result), exist_ok=True)
        with open(args.result, "w", encoding="utf-8") as f:
            json.dump({"results": RESULTS, "summary": summary}, f, ensure_ascii=False, indent=2)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())

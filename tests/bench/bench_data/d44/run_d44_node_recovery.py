# -*- coding: utf-8 -*-
"""D4.4 节点恢复回迁演练（可复现脚本，paramiko 驱动 VM）

用法: python run_d44_node_recovery.py [--skip-baseline] [--only-s1-s2] [--only-check]
环境: VM 192.168.100.200 Oracle Linux 9.7, root/123456
       A=node-24b6a076(7791/13306,oxd43) B=node-bdbff1cc(7792/13307,内部7789) C=node-cab8c15a(7793/13308,内部7790)
步骤: S1 杀 B -> S2 观测 failover 重指派 + down 期写 -> S3 重启 B 观测回迁/追平/路由/聚合
       -> S4 对照(优雅退出重启, --only-check 时跳过前序)
"""
import argparse, sys, time
import paramiko

HOST, USER, PWD, PORT = "192.168.100.200", "root", "123456", 22
B_DIR = "/data/d43-node2"
T = "d44_t1"
B_START = ("nohup /usr/local/bin/openxdb start --data-dir %s --port 7789 "
           "--cluster-addr 192.168.100.200:7792 --tso-node node-24b6a076 "
           "--mysql-port 13307 > %s/node-b-restart.log 2>&1 &" % (B_DIR, B_DIR))

def run(c, cmd, t=60):
    i, o, e = c.exec_command(cmd, timeout=t)
    out = o.read().decode("utf-8", "replace").strip()
    err = e.read().decode("utf-8", "replace").strip()
    return out if out else ("[stderr] " + err if err else "(empty)")

def qry(c, sp, sql, t=45):
    return run(c, "timeout %d /usr/bin/mysql -h 127.0.0.1 -P %d -u root -e \"%s\" 2>&1" % (t, sp, sql), t + 5)

def section(c, name):
    print("\n### %s" % name)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-baseline", action="store_true")
    ap.add_argument("--only-s1-s2", action="store_true")
    ap.add_argument("--only-check", action="store_true")
    args = ap.parse_args()

    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(HOST, port=PORT, username=USER, password=PWD, timeout=10)

    # 基线
    if not args.skip_baseline and not args.only_check:
        section(c, "rebuild baseline")
        for sp in (13306, 13307, 13308):
            qry(c, sp, "DROP TABLE IF EXISTS %s" % T)
        qry(c, 13306, "CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(64))" % T)
        for i in range(1, 6):
            qry(c, 13306, "INSERT INTO %s VALUES (%d,'v%d')" % (T, i, i))
        qry(c, 13306, "SPLIT REGION 9223372036854775809")
        # 再切首个 region：region id 随每次 DROP/CREATE 递增，不可硬编码（旧版固定 2，
        # 在非首次初始化时 SPLIT 失败导致基线只剩 2 个 region）
        _r = qry(c, 13306, "SHOW REGION ROUTES")
        _ids = [ln.split("\t")[0] for ln in _r.splitlines()
                if ln and not ln.startswith("region_id")]
        if _ids:
            qry(c, 13306, "SPLIT REGION %s" % _ids[0])
        routes = qry(c, 13306, "SHOW REGION ROUTES")
        rows = [ln.split("\t") for ln in routes.splitlines() if ln and not ln.startswith("region_id")]
        nodes = qry(c, 13306, "SHOW NODES")
        nrows = [ln.split("\t") for ln in nodes.splitlines() if ln.startswith("node-")]
        idB, idC = nrows[1][0], nrows[2][0]
        qry(c, 13306, "ASSIGN REGION %s TO NODE '%s'" % (rows[1][0], idB))
        qry(c, 13306, "ASSIGN REGION %s TO NODE '%s'" % (rows[2][0], idC))
        time.sleep(2)
        for i in (10, 11, 12):
            qry(c, 13307, "INSERT INTO %s VALUES (%d,'b%d')" % (T, i, i))
        for i in (20, 21, 22):
            qry(c, 13308, "INSERT INTO %s VALUES (%d,'c%d')" % (T, i, i))
        time.sleep(2)
        for sp, nm in ((13306, "A"), (13307, "B"), (13308, "C")):
            print("%s baseline: %s" % (nm, qry(c, sp, "SELECT COUNT(*), SUM(id) FROM %s" % T).replace("\n", " ")))

    # S1 杀 B
    if not args.only_check:
        section(c, "S1 kill B (SIGKILL)")
        bp = run(c, "ps -ef | grep 'd43-node2' | grep -v grep | awk '{print $2}'")
        print("B pid:", bp)
        print(run(c, "kill -9 %s 2>&1; echo RC=$?" % bp))
        time.sleep(15)
        print("A nodes:", qry(c, 13306, "SHOW NODES").replace("\n"," | ").replace("\t"," / "))
        print("A routes:", qry(c, 13306, "SHOW REGION ROUTES").replace("\n"," | ").replace("\t"," / "))
        print("A down-period write id=30:", repr(qry(c, 13306, "INSERT INTO %s VALUES (30,'pA30')" % T)))
        print("A read:", qry(c, 13306, "SELECT COUNT(*), SUM(id) FROM %s" % T).replace("\n", " "))

        # S3 重启 B
        section(c, "S3 restart B")
        run(c, B_START)
        time.sleep(18)
        print("B proc:", run(c, "ps -ef | grep d43-node2 | grep -v grep | awk '{print $2}'"))
        print("A nodes:", qry(c, 13306, "SHOW NODES").replace("\n"," | ").replace("\t"," / "))
        print("A routes:", qry(c, 13306, "SHOW REGION ROUTES").replace("\n"," | ").replace("\t"," / "))
        print("B routes:", qry(c, 13307, "SHOW REGION ROUTES").replace("\n"," | ").replace("\t"," / "))
        for sp, nm in ((13306, "A"), (13307, "B"), (13308, "C")):
            print("%s read: %s" % (nm, qry(c, sp, "SELECT COUNT(*), SUM(id) FROM %s" % T).replace("\n", " ")))
        print("B SELECT id=30 (down-period write):", repr(qry(c, 13307, "SELECT * FROM %s WHERE id=30" % T)))
        print("B write id=31:", repr(qry(c, 13307, "INSERT INTO %s VALUES (31,'pB31')" % T)))
        time.sleep(3)
        for sp, nm in ((13306, "A"), (13307, "B"), (13308, "C")):
            print("%s read after B write: %s" % (nm, qry(c, sp, "SELECT COUNT(*), SUM(id) FROM %s" % T).replace("\n", " ")))

        # 人工回迁对照
        section(c, "S3+ manual reassign r3 -> B (route convergence check)")
        print("ASSIGN rc:", repr(qry(c, 13306, "ASSIGN REGION 3 TO NODE 'node-bdbff1cc'")))
        time.sleep(5)
        print("A routes:", qry(c, 13306, "SHOW REGION ROUTES").replace("\n"," | ").replace("\t"," / "))
        print("C routes:", qry(c, 13308, "SHOW REGION ROUTES").replace("\n"," | ").replace("\t"," / "))
        for sp, nm in ((13306, "A"), (13307, "B"), (13308, "C")):
            print("%s read: %s" % (nm, qry(c, sp, "SELECT COUNT(*), SUM(id) FROM %s" % T).replace("\n", " ")))

    # S4 对照：优雅退出重启
    if not args.only_s1_s2:
        section(c, "S4 graceful-exit restart (SIGTERM) check")
        bp = run(c, "ps -ef | grep 'd43-node2' | grep -v grep | awk '{print $2}'")
        print("B pid:", bp)
        if bp:
            run(c, "kill -TERM %s" % bp)
            time.sleep(12)
            print("alive after TERM:", run(c, "ps -ef | grep d43-node2 | grep -v grep | awk '{print $2}'"))
        run(c, B_START)
        time.sleep(16)
        print("B proc:", run(c, "ps -ef | grep d43-node2 | grep -v grep | awk '{print $2}'"))
        print("A nodes:", qry(c, 13306, "SHOW NODES").replace("\n"," | ").replace("\t"," / "))
        print("A routes:", qry(c, 13306, "SHOW REGION ROUTES").replace("\n"," | ").replace("\t"," / "))
        print("B routes:", qry(c, 13307, "SHOW REGION ROUTES").replace("\n"," | ").replace("\t"," / "))
        for sp, nm in ((13306, "A"), (13307, "B"), (13308, "C")):
            print("%s read: %s" % (nm, qry(c, sp, "SELECT COUNT(*), SUM(id) FROM %s" % T).replace("\n", " ")))
    c.close()

if __name__ == "__main__":
    main()

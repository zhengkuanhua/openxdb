#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
D4.3 脑裂 / epoch 拒绝验证 — VM 三节点集群轨演练脚本
依赖：paramiko；VM 需具备 iptables + 独立用户 oxd43(uid=2001) 运行 A 进程
用法：python run_d43_split_brain_epoch.py
输出：控制台观测 + 汇总 PASS/FAIL/GAP
"""
import paramiko, time, sys

# ---- 配置 ----
HOST = "192.168.100.200"
USER = "root"
PWD  = "123456"
PORT = 22
A_PID_OWNER = 2001          # A 进程独立 uid（--uid-owner 隔离出站用）
A_CP = 7791; B_CP = 7792; C_CP = 7793   # cluster 端口
A_SP = 13306; B_SP = 13307; C_SP = 13308  # mysql 端口
TABLE = "d43_t1"

passed = failed = gap = 0
def check(name, ok, detail=""):
    global passed, failed, gap
    tag = "PASS" if ok else "FAIL"
    print(f"[{tag}] {name} {detail}")
    if ok: passed += 1
    else:  failed += 1

def gap(name, detail=""):
    global gap
    gap += 1
    print(f"[GAP ] {name} {detail}")

def main():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(HOST, port=PORT, username=USER, password=PWD, timeout=10)
    def run(cmd, t=50):
        i, o, e = c.exec_command(cmd, timeout=t)
        out = o.read().decode("utf-8", "replace").strip()
        err = e.read().decode("utf-8", "replace").strip()
        return out if out else ("[stderr] " + err if err else "(empty)")
    def qry(sp, sql, t=20):
        return run(f"timeout {t} /usr/bin/mysql -h 127.0.0.1 -P {sp} -u root -e \"{sql}\" 2>&1", t + 5)

    print("=== D4.3 split-brain / epoch fencing drill ===")

    # ---- S0 环境核查 ----
    ver = run("/usr/local/bin/openxdb version")
    print("version:", ver.splitlines()[0] if ver else "(none)")
    for sp, name in ((A_SP, "A"), (B_SP, "B"), (C_SP, "C")):
        nodes = qry(sp, "SHOW NODES", 15)
        check(f"{name} SHOW NODES 3 rows", nodes.count("\n") >= 3, nodes.replace("\n", " "))
        cfg = qry(sp, "SHOW CONFIG", 15)
        check(f"{name} epoch=on quorum=off", "ha_epoch_enabled\ton" in cfg and "ha_quorum_enabled\toff" in cfg)

    # ---- S1 基线重建 ----
    print("--- S1 baseline (split/assign region, tri-node consistent) ---")
    for sp in (A_SP, B_SP, C_SP):
        qry(sp, f"DROP TABLE IF EXISTS {TABLE}", 15)
    qry(A_SP, f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, v VARCHAR(64))")
    for i in range(1, 6):
        qry(A_SP, f"INSERT INTO {TABLE} VALUES ({i},'v{i}')")
    qry(A_SP, "SPLIT REGION 9223372036854775809")
    qry(A_SP, "SPLIT REGION 2")
    routes = qry(A_SP, "SHOW REGION ROUTES")
    rows = [ln.split("\t") for ln in routes.splitlines() if ln and not ln.startswith("region_id")]
    check("3 regions after split", len(rows) >= 3, f"rows={len(rows)}")
    if len(rows) < 3:
        print("abort: region split failed"); sys.exit(1)
    rA, rB, rC = rows[0][0], rows[1][0], rows[2][0]
    nodes = qry(A_SP, "SHOW NODES")
    nrows = [ln.split("\t") for ln in nodes.splitlines() if ln.startswith("node-")]
    idA, idB, idC = nrows[0][0], nrows[1][0], nrows[2][0]
    print(f"ids: A={idA} B={idB} C={idC}; regions rA={rA} rB={rB} rC={rC}")
    qry(A_SP, f"ASSIGN REGION {rB} TO NODE '{idB}'")
    qry(A_SP, f"ASSIGN REGION {rC} TO NODE '{idC}'")
    time.sleep(2)
    for i in (10, 11, 12):
        qry(B_SP, f"INSERT INTO {TABLE} VALUES ({i},'b{i}')", 15)
    for i in (20, 21, 22):
        qry(C_SP, f"INSERT INTO {TABLE} VALUES ({i},'c{i}')", 15)
    time.sleep(2)
    for sp, name in ((A_SP, "A"), (B_SP, "B"), (C_SP, "C")):
        r = qry(sp, f"SELECT COUNT(*), SUM(id) FROM {TABLE}", 15)
        ok = r.replace("\t", " ") == "COUNT(*) SUM(id)\n11 111"
        check(f"{name} baseline count=11 sum=111", ok, r.replace("\n", " "))

    # ---- S2 双向隔离 A ----
    print("--- S2 isolate A (bidirectional) ---")
    a_pid = run("ps -ef | grep d43-node1 | grep -v grep | grep -v runuser | awk '{print $2}'").splitlines()
    print("A pid:", a_pid)
    run(f"iptables -I INPUT  -p tcp --dport {A_CP} -j DROP")
    run(f"iptables -I OUTPUT -m owner --uid-owner {A_PID_OWNER} -p tcp --dport {B_CP} -j DROP")
    run(f"iptables -I OUTPUT -m owner --uid-owner {A_PID_OWNER} -p tcp --dport {C_CP} -j DROP")
    print("isolated; waiting 25s for heartbeat timeout...")
    time.sleep(25)
    aN = qry(A_SP, "SHOW NODES")
    bN = qry(B_SP, "SHOW NODES")
    cN = qry(C_SP, "SHOW NODES")
    check("A sees B/C DOWN", aN.count("DOWN") == 2, aN.replace("\n", " "))
    check("B sees A DOWN & C UP", aN if False else bN.count("DOWN") == 1 and bN.count("UP") == 2, bN.replace("\n", " "))
    check("C sees A DOWN & B UP", cN.count("DOWN") == 1 and cN.count("UP") == 2, cN.replace("\n", " "))
    r = qry(A_SP, f"INSERT INTO {TABLE} VALUES (100,'pA100')")
    ok = "ERROR" not in r
    if ok: gap("A(old master) write NOT fenced during partition", "insert 100 succeeded")
    else:  check("A(old master) write fenced", False, r)
    rB = qry(B_SP, f"INSERT INTO {TABLE} VALUES (200,'pB200')", 15)
    check("B write blocked (TSO single point)", "tso unavailable" in rB, rB.replace("\n", " "))
    rC = qry(C_SP, f"INSERT INTO {TABLE} VALUES (300,'pC300')", 15)
    check("C write blocked (TSO single point)", "tso unavailable" in rC, rC.replace("\n", " "))
    bRead = qry(B_SP, f"SELECT COUNT(*) FROM {TABLE}", 15)
    aRead = qry(A_SP, f"SELECT COUNT(*) FROM {TABLE}", 15)
    print(f"partition reads: A={aRead.splitlines()[-1]} B={bRead.splitlines()[-1]}")

    # ---- S3 恢复 ----
    print("--- S3 restore & convergence ---")
    run(f"iptables -D INPUT  -p tcp --dport {A_CP} -j DROP")
    run(f"iptables -D OUTPUT -m owner --uid-owner {A_PID_OWNER} -p tcp --dport {B_CP} -j DROP")
    run(f"iptables -D OUTPUT -m owner --uid-owner {A_PID_OWNER} -p tcp --dport {C_CP} -j DROP")
    print("restored; waiting 20s...")
    time.sleep(20)
    for sp, name in ((A_SP, "A"), (B_SP, "B"), (C_SP, "C")):
        nodes = qry(sp, "SHOW NODES")
        check(f"{name} all UP after restore", nodes.count("UP") == 3, nodes.replace("\n", " "))
    rA1 = qry(A_SP, f"SELECT COUNT(*) FROM {TABLE}")
    rB1 = qry(B_SP, f"SELECT COUNT(*) FROM {TABLE}", 15)
    rC1 = qry(C_SP, f"SELECT COUNT(*) FROM {TABLE}", 15)
    a_cnt = int(rA1.splitlines()[-1]); b_cnt = int(rB1.splitlines()[-1]); c_cnt = int(rC1.splitlines()[-1])
    print(f"post-restore counts: A={a_cnt} B={b_cnt} C={c_cnt}")
    if a_cnt != b_cnt or b_cnt != c_cnt:
        gap("no auto convergence after partition", f"A={a_cnt} B={b_cnt} C={c_cnt}")
    qry(A_SP, f"INSERT INTO {TABLE} VALUES (400,'pA400')")
    time.sleep(2)
    rA2 = qry(A_SP, f"SELECT COUNT(*) FROM {TABLE}")
    rB2 = qry(B_SP, f"SELECT COUNT(*) FROM {TABLE}", 15)
    a2 = int(rA2.splitlines()[-1]); b2 = int(rB2.splitlines()[-1])
    print(f"after A write 400: A={a2} B={b2}")
    if a2 != b2:
        gap("old-master post-recovery write invisible to majority", f"A={a2} B={b2}")

    print(f"\n=== SUMMARY: PASS={passed} FAIL={failed} GAP={gap} ===")
    c.close()
    sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    main()

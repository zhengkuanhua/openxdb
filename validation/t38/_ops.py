# -*- coding: utf-8 -*-
"""T38 orchestration helper: subcommand entry points for run_validation.ps1.

Usage: python _ops.py <cmd> [args...]
  wait <port> [tls] [ca]             -> print ready/no (tolerates slow startup)
  execsql <port> <sqlfile> [tls] [ca] -> print ok=N errs=M expected=X and ERR lines
  query <port> <sql> [tls] [ca]      -> print query rows or ERR

SQL scripts may mark a statement as an *expected failure* by putting a line
starting with "--@expect-error" immediately before it. Such statements count
towards `expected`; a statement marked expected that *succeeds* is reported
as an error (the assertion failed).
"""
import os
import re
import sys
import time

PYDIR = r'D:\AI项目\OpenXDB开源数据库开发\openxdb\drivers\python'
sys.path.insert(0, PYDIR)
from openxdb import connect


def run(port, sql, tls=False, ca=''):
    # OPENXDB_USER/OPENXDB_PASSWORD env vars (set by run_validation.ps1)
    # enable authenticated connections (M11 AUTH handshake) when needed.
    user = os.environ.get('OPENXDB_USER') or None
    password = os.environ.get('OPENXDB_PASSWORD') or ''
    c = connect(host='127.0.0.1', port=port, user=user, password=password,
                ssl=tls, ssl_ca=(ca or None), ssl_check_hostname=False)
    try:
        cur = c.cursor().execute(sql)
        return cur.fetchall()
    finally:
        c.close()


_CONN_ERR_HINTS = ('connect', 'refused', 'reset', 'timed out', 'timeout', 'closed')


def wait_ready(port, tls=False, ca='', tries=120, sleep_s=1.0):
    # 服务端就绪判定：连接 + SHOW TABLES 成功即就绪；仅连接类异常继续等待，
    # SQL 级错误说明实例已在服务，同样视为就绪（个别语句可能不被引擎支持）。
    for _ in range(tries):
        try:
            run(port, 'SHOW TABLES', tls, ca)
            return True
        except Exception as e:
            msg = str(e).lower()
            if not any(h in msg for h in _CONN_ERR_HINTS):
                return True
            time.sleep(sleep_s)
    return False


def _flush_ready(buf):
    """分句完成判定：普通语句以 ';' 结尾即可；
    CREATE PROCEDURE 需以 END 词收尾（函数体内含分号，不能提前切断）。"""
    if not buf.endswith(';'):
        return False
    if buf.startswith('CREATE PROCEDURE'):
        return re.search(r'\bEND\s*;?\s*$', buf) is not None
    return True


def exec_file(port, path, tls=False, ca=''):
    with open(path, encoding='utf-8') as f:
        text = f.read()
    # Keep only non-empty, non-comment lines; track expect-error markers.
    expect_next = False
    stmts = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith('--@expect-error'):
            expect_next = True
            continue
        if line.startswith('--'):
            continue
        stmts.append((line, expect_next))
        expect_next = False
    # Merge multi-line statements (accumulate until a trailing ';').
    merged = []
    buf = ''
    expect = False
    for line, exp in stmts:
        if exp:
            expect = True
        buf = (buf + ' ' + line).strip()
        if _flush_ready(buf):
            merged.append((buf.rstrip(';').strip(), expect))
            buf = ''
            expect = False
    if buf.strip():
        merged.append((buf.strip().rstrip(';').strip(), expect))
    ok = 0
    expected = 0
    errs = []
    for s, exp in merged:
        if not s:
            continue
        try:
            run(port, s, tls, ca)
            if exp:
                errs.append('EXPECTED-FAILURE-UNMET: ' + s[:80])
            else:
                ok += 1
        except Exception as e:
            if exp:
                expected += 1
            else:
                errs.append(s[:80] + ' => ' + str(e))
    return ok, expected, errs

if __name__ == '__main__':
    args = sys.argv[1:]
    cmd = args[0] if args else ''
    if cmd == 'wait':
        port = int(args[1])
        tls = args[2] == '1' if len(args) > 2 else False
        ca = args[3] if len(args) > 3 else ''
        print('ready' if wait_ready(port, tls, ca) else 'no')
    elif cmd == 'execsql':
        port = int(args[1])
        path = args[2]
        tls = args[3] == '1' if len(args) > 3 else False
        ca = args[4] if len(args) > 4 else ''
        ok, expected, errs = exec_file(port, path, tls, ca)
        print('ok=%d errs=%d expected=%d' % (ok, len(errs), expected))
        for e in errs:
            print('ERR:', e)
    elif cmd == 'query':
        port = int(args[1])
        sql = args[2]
        tls = args[3] == '1' if len(args) > 3 else False
        ca = args[4] if len(args) > 4 else ''
        try:
            print(run(port, sql, tls, ca))
        except Exception as e:
            print('ERR:', e)
    else:
        print('unknown cmd')

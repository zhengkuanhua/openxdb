# -*- coding: utf-8 -*-
"""T36 TLS driver tests (Python).

Boots a real OpenXDB server in TLS-only mode (``start --tls-cert/--tls-key``)
and exercises the driver's TLS parameters: ``ssl=True`` (no verification),
``ssl_ca`` (verify-full), ``ssl_check_hostname=False`` (verify-ca), plus the
negative paths (untrusted CA, plaintext client against TLS-only listener).

Requires the server binary at ``drivers/.bin/openxdb.exe``; tests are skipped
when it is absent (same policy as ``conftest.py``).
"""

import os
import shutil
import socket
import subprocess
import tempfile
import time

import pytest

from openxdb.connection import Connection
from openxdb.errors import OperationalError

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.path.join(REPO_ROOT, "drivers", ".bin", "openxdb.exe")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(host: str, port: int, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"server not ready on {host}:{port}")


@pytest.fixture(scope="module")
def tls_server(tmp_path_factory):
    if not os.path.exists(BIN):
        pytest.skip(f"openxdb.exe not found at {BIN}; run drivers/scripts/build_openxdb.ps1")

    base = tmp_path_factory.mktemp("openxdb-tls")
    data_dir = os.path.join(str(base), "data")
    cert_dir = os.path.join(str(base), "cert")
    os.makedirs(data_dir)
    os.makedirs(cert_dir)

    gen = subprocess.run(
        [BIN, "gen-cert", "--out-dir", cert_dir, "--cn", "openxdb-tls", "--days", "30"],
        capture_output=True, text=True, timeout=30,
    )
    if gen.returncode != 0:
        raise RuntimeError(f"openxdb gen-cert failed: {gen.stderr}")

    # A second, unrelated self-signed certificate used as an untrusted CA.
    wrong_dir = os.path.join(str(base), "wrong")
    os.makedirs(wrong_dir)
    wrong = subprocess.run(
        [BIN, "gen-cert", "--out-dir", wrong_dir, "--cn", "other-ca", "--days", "30"],
        capture_output=True, text=True, timeout=30,
    )
    if wrong.returncode != 0:
        raise RuntimeError(f"openxdb gen-cert (wrong) failed: {wrong.stderr}")

    port = _free_port()
    init = subprocess.run(
        [BIN, "init", "--data-dir", data_dir],
        capture_output=True, text=True, timeout=30,
    )
    if init.returncode != 0:
        raise RuntimeError(f"openxdb init failed: {init.stderr}")
    proc = subprocess.Popen(
        [BIN, "start", "--data-dir", data_dir, "--port", str(port),
         "--tls-cert", os.path.join(cert_dir, "tls-cert.pem"),
         "--tls-key", os.path.join(cert_dir, "tls-key.pem")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        _wait_ready("127.0.0.1", port)
        yield {
            "host": "127.0.0.1",
            "port": port,
            "ca": os.path.join(cert_dir, "tls-cert.pem"),
            "wrong_ca": os.path.join(wrong_dir, "tls-cert.pem"),
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(str(base), ignore_errors=True)


def test_tls_true_connects_without_verification(tls_server):
    conn = Connection(tls_server["host"], tls_server["port"], ssl=True, timeout=5)
    try:
        conn.ping()
    finally:
        conn.close()


def test_tls_ssl_ca_verify_full_runs_queries(tls_server):
    conn = Connection(tls_server["host"], tls_server["port"], ssl_ca=tls_server["ca"], timeout=5)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE tls_t (id INT, v TEXT, PRIMARY KEY (id))")
        cur.execute("INSERT INTO tls_t VALUES (1, 'secure'), (2, 'encrypted')")
        cur.execute("SELECT id, v FROM tls_t ORDER BY id")
        rows = cur.fetchall()
        assert [(r[0], r[1]) for r in rows] == [(1, "secure"), (2, "encrypted")]
        cur.execute("DROP TABLE tls_t")
    finally:
        conn.close()


def test_tls_verify_ca_skip_hostname(tls_server):
    conn = Connection(
        tls_server["host"], tls_server["port"],
        ssl_ca=tls_server["ca"], ssl_check_hostname=False, timeout=5,
    )
    try:
        conn.ping()
    finally:
        conn.close()


def test_tls_untrusted_ca_rejected(tls_server):
    with pytest.raises(OperationalError):
        Connection(tls_server["host"], tls_server["port"],
                   ssl_ca=tls_server["wrong_ca"], timeout=5)


def test_tls_plaintext_rejected_on_tls_only_server(tls_server):
    with pytest.raises(OperationalError):
        Connection(tls_server["host"], tls_server["port"], timeout=5).ping()

# -*- coding: utf-8 -*-
"""pytest fixtures: boot a real OpenXDB server per session.

The server binary is expected at ``drivers/.bin/openxdb.exe`` (built by
``drivers/scripts/build_openxdb.ps1``).  Each test session gets a temporary
data directory and a random free port.
"""

import os
import shutil
import socket
import subprocess
import tempfile
import time

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.path.join(REPO_ROOT, "drivers", ".bin", "openxdb.exe")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(host: str, port: int, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as exc:
            last = exc
            time.sleep(0.2)
    raise RuntimeError(f"OpenXDB server did not become ready on {host}:{port}: {last}")


@pytest.fixture(scope="session")
def server():
    """Yield (host, port, data_dir); teardown terminates the server."""
    if not os.path.exists(BIN):
        pytest.skip(f"openxdb.exe not found at {BIN}; run drivers/scripts/build_openxdb.ps1")

    data_dir = tempfile.mkdtemp(prefix="openxdb-drv-")
    port = _free_port()
    proc = None
    try:
        init = subprocess.run(
            [BIN, "init", "--data-dir", data_dir],
            capture_output=True, text=True, timeout=30,
        )
        if init.returncode != 0:
            raise RuntimeError(f"openxdb init failed: {init.stderr}")
        proc = subprocess.Popen(
            [BIN, "start", "--data-dir", data_dir, "--port", str(port)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        _wait_ready("127.0.0.1", port)
        yield ("127.0.0.1", port, data_dir)
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        shutil.rmtree(data_dir, ignore_errors=True)

# -*- coding: utf-8 -*-
"""openxdb — Python driver for the OpenXDB database.

The driver talks the native OpenXDB TCP line protocol (see
``pkg/server`` and ``docs/T19_client_drivers.md``).  There is no server
handshake or authentication; ``user`` / ``password`` are accepted for API
compatibility and ignored.

Usage::

    import openxdb

    conn = openxdb.connect("127.0.0.1", 7788)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM users WHERE id > ?", (1,))
    for row in cur.fetchall():
        print(row)
    conn.close()
"""

from .connection import Connection
from .errors import (
    ConnectionClosedError,
    InterfaceError,
    OpenXDBError,
    OperationalError,
    ParameterError,
    ProgrammingError,
)
from .protocol import DEFAULT_PORT

__version__ = "0.10.0"

__all__ = [
    "connect",
    "Connection",
    "DEFAULT_PORT",
    "OpenXDBError",
    "InterfaceError",
    "OperationalError",
    "ProgrammingError",
    "ParameterError",
    "ConnectionClosedError",
    "__version__",
]

# PEP 249 module-level aliases (driver is largely PEP 249 compatible).
apilevel = "2.0"
threadsafety = 1  # per-connection locking is not provided; share at your own risk
paramstyle = "qmark"


def connect(host: str = "127.0.0.1", port: int = DEFAULT_PORT,
            user: str | None = None, password: str | None = None,
            timeout: float = 30.0) -> Connection:
    """Connect to an OpenXDB server listening on *host*:*port*."""
    return Connection(host=host, port=port, user=user, password=password,
                      timeout=timeout)

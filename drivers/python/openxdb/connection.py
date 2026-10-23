# -*- coding: utf-8 -*-
"""Connection to an OpenXDB server (PEP 249 style)."""

from __future__ import annotations

import ssl as _ssl

from .cursor import Cursor
from .errors import ConnectionClosedError, ProgrammingError
from .protocol import DEFAULT_PORT, OpenXDBProtocol


class Connection:
    """A single TCP session to an OpenXDB server.

    The server keeps per-connection transaction state: ``BEGIN`` /
    ``COMMIT`` / ``ROLLBACK`` are forwarded verbatim as line-protocol
    commands. Statements issued outside an explicit transaction are
    executed by the server as implicit auto-commit transactions.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = DEFAULT_PORT,
                 user: str | None = None, password: str | None = None,
                 timeout: float = 30.0,
                 ssl: bool | _ssl.SSLContext | None = None,
                 ssl_ca: str | None = None, ssl_cert: str | None = None,
                 ssl_key: str | None = None, ssl_check_hostname: bool = True,
                 server_hostname: str | None = None):
        # M11: when credentials are supplied the driver performs the AUTH
        # handshake immediately; servers with --auth reject unauthenticated
        # connections otherwise. TLS (T36): ssl=True → TLS without
        # verification; ssl_ca=<path> → verify-full (CA chain + hostname);
        # ssl_cert/ssl_key → mutual TLS.
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._protocol = OpenXDBProtocol(
            host, port, timeout,
            ssl=ssl, ssl_ca=ssl_ca, ssl_cert=ssl_cert, ssl_key=ssl_key,
            ssl_check_hostname=ssl_check_hostname, server_hostname=server_hostname,
        )
        if user is not None:
            self._protocol.auth(user, password or "")
        self._closed = False

    # -- attributes ------------------------------------------------------

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def closed(self) -> bool:
        return self._closed

    # -- cursor / session ------------------------------------------------

    def cursor(self) -> Cursor:
        if self._closed:
            raise ConnectionClosedError("connection is closed")
        return Cursor(self._protocol)

    def pipeline(self, queries, params_list=None):
        """Execute several statements in one network round trip.

        Convenience wrapper around :meth:`Cursor.pipeline`: creates a cursor,
        runs the batch and closes it.  Returns the list of
        :class:`~openxdb.cursor.PipelineResult` outcomes (one per query).
        """
        if self._closed:
            raise ConnectionClosedError("connection is closed")
        cur = self.cursor()
        try:
            return cur.pipeline(queries, params_list)
        finally:
            cur.close()

    def ping(self) -> None:
        if self._closed:
            raise ConnectionClosedError("connection is closed")
        self._protocol.ping()

    def begin(self) -> None:
        """Start an explicit (session-level) transaction."""
        if self._closed:
            raise ConnectionClosedError("connection is closed")
        self._protocol.begin()

    def commit(self) -> None:
        """Commit the active session transaction."""
        if self._closed:
            raise ConnectionClosedError("connection is closed")
        self._protocol.commit()

    def rollback(self) -> None:
        """Roll back the active session transaction."""
        if self._closed:
            raise ConnectionClosedError("connection is closed")
        self._protocol.rollback()

    def close(self) -> None:
        """Send ``QUIT`` (best effort) and close the socket."""
        if self._closed:
            return
        self._closed = True
        try:
            self._protocol.quit()
        finally:
            self._protocol.close()

    # -- context managers -------------------------------------------------

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self):  # pragma: no cover - defensive
        try:
            if not self._closed:
                self._protocol.close()
        except Exception:
            pass

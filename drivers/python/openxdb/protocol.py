# -*- coding: utf-8 -*-
"""Line-protocol client for the OpenXDB TCP server (pkg/server, docs/T5).

Wire protocol (as implemented by ``pkg/server.Server.handleLocked``):

* the client sends one command per line (UTF-8, terminated by ``\\n``);
* the server replies line-by-line; there is **no handshake / authentication**:
  a socket connects directly to the command loop;
* SQL statement replies come in two shapes:
  * query (``SELECT`` / ``SHOW`` / ``EXPLAIN`` …): a text table
      ``header | ...``, ``separator | ...``, one line per row, final line
      ``(N rows)``;
  * non-query (``CREATE`` / ``INSERT`` / ``UPDATE`` / ``DELETE`` /
    ``BACKUP`` / ``RESTORE`` …): a single line ``(N rows affected)``;
* connection-level commands reply ``OK`` / ``PONG`` / ``BYE``;
* any failure replies ``ERR <message>``.

Parsing caveats (documented in docs/T19_client_drivers.md):

* NULL and the empty string are both rendered as an empty cell on the wire;
  this driver therefore maps an empty cell to ``None`` (NULL semantics);
* cell values may contain the ``|`` character, so rows are split on the
  first ``N-1`` occurrences of ``" | "`` where ``N`` is the header column
  count (the last column keeps the remainder verbatim);
* TEXT values containing newlines are **not** supported by the line
  protocol: ``\n`` / ``\r`` inside parameterised string values are rejected
  by the client-side escaping engine.
"""

from __future__ import annotations

import socket

from .errors import OperationalError, ConnectionClosedError

# Server default port (cmd/openxdb --port default).
DEFAULT_PORT = 7788
# UTF-8 so Chinese text and UTF-8 encoded identifiers survive the wire.
_ENCODING = "utf-8"


class OpenXDBProtocol:
    """Thin TCP line-protocol session to an OpenXDB server."""

    def __init__(self, host: str = "127.0.0.1", port: int = DEFAULT_PORT,
                 timeout: float = 30.0):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._sock: socket.socket | None = None
        self._reader = None
        self.connect()

    # -- connection lifecycle -------------------------------------------

    def connect(self) -> None:
        try:
            sock = socket.create_connection((self._host, self._port), timeout=self._timeout)
        except OSError as exc:  # pragma: no cover - network dependent
            raise OperationalError(
                f"cannot connect to OpenXDB server {self._host}:{self._port}: {exc}"
            ) from exc
        sock.settimeout(self._timeout)
        # BufferedReader with a large buffer keeps multi-row tables cheap.
        self._sock = sock
        self._reader = sock.makefile("r", encoding=_ENCODING, newline="\n")

    def close(self) -> None:
        sock, self._sock = self._sock, None
        self._reader = None
        if sock is not None:
            try:
                sock.close()
            except OSError:  # pragma: no cover
                pass

    @property
    def closed(self) -> bool:
        return self._sock is None

    # -- raw command / response ----------------------------------------

    def command(self, line: str) -> str:
        """Send one command line and return the raw multi-line response text.

        An ``ERR`` reply is converted into :class:`OperationalError`.
        """
        if self.closed:
            raise ConnectionClosedError("connection is closed")
        try:
            self._sock.sendall(line.encode(_ENCODING) + b"\n")
            first = self._reader.readline()
        except OSError as exc:
            self.close()
            raise OperationalError(
                f"connection lost while sending command: {exc}"
            ) from exc
        if first == "":
            self.close()
            raise OperationalError("connection closed by server (EOF)")
        first = first.rstrip("\r\n")
        if first.startswith("ERR "):
            raise OperationalError(first[4:])
        # Fast path for single-line replies (OK / PONG / BYE / rows affected).
        if _is_single_line_reply(first):
            return first
        # Otherwise this is a query table: read rows until the "(N rows)" trailer.
        lines = [first]
        try:
            while True:
                line = self._reader.readline()
                if line == "":
                    self.close()
                    raise OperationalError(
                        "connection closed by server while reading query result"
                    )
                line = line.rstrip("\r\n")
                lines.append(line)
                if _is_rows_trailer(line):
                    break
        except OperationalError:
            raise
        except OSError as exc:
            self.close()
            raise OperationalError(f"connection lost while reading result: {exc}") from exc
        return "\n".join(lines)

    # -- typed helpers --------------------------------------------------

    def ping(self) -> None:
        """Send ``PING``; raises on non-``PONG`` reply."""
        resp = self.command("PING")
        if resp != "PONG":
            raise OperationalError(f"unexpected PING reply: {resp!r}")

    def begin(self) -> None:
        self._expect_ok(self.command("BEGIN"), "BEGIN")

    def commit(self) -> None:
        self._expect_ok(self.command("COMMIT"), "COMMIT")

    def rollback(self) -> None:
        self._expect_ok(self.command("ROLLBACK"), "ROLLBACK")

    def quit(self) -> None:
        """Send ``QUIT`` and close the socket (best effort)."""
        try:
            if not self.closed:
                resp = self.command("QUIT")
                if resp != "BYE":
                    raise OperationalError(f"unexpected QUIT reply: {resp!r}")
        finally:
            self.close()

    @staticmethod
    def _expect_ok(resp: str, what: str) -> None:
        if resp != "OK":
            raise OperationalError(f"{what} failed: unexpected reply {resp!r}")


def _is_single_line_reply(line: str) -> bool:
    """OK / PONG / BYE / (N rows affected) are single-line replies."""
    if line in ("OK", "PONG", "BYE"):
        return True
    return _is_affected_trailer(line)


def _is_affected_trailer(line: str) -> bool:
    return line.startswith("(") and line.endswith(" rows affected)")


def _is_rows_trailer(line: str) -> bool:
    return line.startswith("(") and line.endswith(" rows)")

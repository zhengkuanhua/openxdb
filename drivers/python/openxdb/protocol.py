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
import ssl as _ssl

from .errors import OperationalError, ConnectionClosedError

# Server default port (cmd/openxdb --port default).
DEFAULT_PORT = 7788
# UTF-8 so Chinese text and UTF-8 encoded identifiers survive the wire.
_ENCODING = "utf-8"


class OpenXDBProtocol:
    """Thin TCP line-protocol session to an OpenXDB server (T36 TLS)."""

    def __init__(self, host: str = "127.0.0.1", port: int = DEFAULT_PORT,
                 timeout: float = 30.0, ssl: bool | _ssl.SSLContext | None = None,
                 ssl_ca: str | None = None, ssl_cert: str | None = None,
                 ssl_key: str | None = None, ssl_check_hostname: bool = True,
                 server_hostname: str | None = None):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._ssl = ssl
        self._ssl_ca = ssl_ca
        self._ssl_cert = ssl_cert
        self._ssl_key = ssl_key
        self._ssl_check_hostname = ssl_check_hostname
        self._server_hostname = server_hostname
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
        # T36: TLS envelope — wrap the raw socket before the line protocol.
        if self._ssl or self._ssl_ca is not None:
            sock = self._wrap_tls(sock)
        # BufferedReader with a large buffer keeps multi-row tables cheap.
        self._sock = sock
        self._reader = sock.makefile("r", encoding=_ENCODING, errors="replace", newline="\n")

    def _wrap_tls(self, sock: socket.socket) -> socket.socket:
        try:
            if isinstance(self._ssl, _ssl.SSLContext):
                ctx = self._ssl
            else:
                ctx = _ssl.create_default_context()
                if self._ssl_ca is not None:
                    ctx.load_verify_locations(cafile=self._ssl_ca)
                if self._ssl is True:
                    # ssl=True: TLS without verification (intranet/self-signed).
                    ctx.check_hostname = False
                    ctx.verify_mode = _ssl.CERT_NONE
                elif not self._ssl_check_hostname:
                    # ssl_ca + ssl_check_hostname=False: verify CA only (verify-ca).
                    ctx.check_hostname = False
            if self._ssl_cert:
                ctx.load_cert_chain(certfile=self._ssl_cert, keyfile=self._ssl_key or self._ssl_cert)
            server_hostname = self._server_hostname
            if server_hostname is None:
                server_hostname = self._host if ctx.check_hostname else None
            return ctx.wrap_socket(sock, server_hostname=server_hostname)
        except OSError as exc:  # pragma: no cover - network dependent
            try:
                sock.close()
            except OSError:
                pass
            raise OperationalError(
                f"TLS handshake with OpenXDB server {self._host}:{self._port} failed: {exc}"
            ) from exc

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

    def auth(self, user: str, password: str) -> None:
        """M11 connection handshake: AUTH <user> <password>.

        Server replies ``OK`` on success or ``ERR <reason>`` (which
        :meth:`command` raises as :class:`OperationalError`).
        """
        self.command(f"AUTH {user} {password}")

    def command(self, line: str) -> str:
        """Send one command and read its full reply (raises on ``ERR``)."""
        if self.closed:
            raise ConnectionClosedError("connection is closed")
        try:
            self._sock.sendall(line.encode(_ENCODING) + b"\n")
            first = self._readline()
        except OSError as exc:
            self.close()
            raise OperationalError(
                f"connection lost while sending command: {exc}"
            ) from exc
        if first.startswith("ERR "):
            raise OperationalError(first[4:])
        return self._read_reply(first)

    def pipeline(self, lines: list[str]) -> list[tuple[bool, str]]:
        """Send many commands in one batch and read their replies in order.

        All commands are written to the socket in a single ``sendall`` and the
        replies are then consumed one by one, so the server processes the whole
        batch without a round-trip per statement (the REPLSession loop already
        handles consecutive input lines in order).

        Returns a ``(is_error, reply_text)`` pair per input command:

        * ``is_error`` is ``True`` when the server replied ``ERR <message>``;
          ``reply_text`` is the message (the error does **not** abort reading
          the remaining replies);
        * otherwise ``reply_text`` is the full reply exactly as
          :meth:`command` would return it: ``OK`` / ``PONG`` / ``BYE`` /
          ``(N rows affected)`` or a text table ending with ``(N rows)``.

        Raises :class:`OperationalError` / :class:`ConnectionClosedError` if
        the connection is lost or closed mid-batch (replies are then
        unavailable, so the whole batch fails).
        """
        if not lines:
            raise ValueError("pipeline requires at least one command")
        if self.closed:
            raise ConnectionClosedError("connection is closed")
        try:
            self._sock.sendall(
                b"".join(line.encode(_ENCODING) + b"\n" for line in lines)
            )
        except OSError as exc:
            self.close()
            raise OperationalError(
                f"connection lost while sending batch: {exc}"
            ) from exc
        out: list[tuple[bool, str]] = []
        for _ in lines:
            first = self._readline()
            if first.startswith("ERR "):
                out.append((True, first[4:]))
            else:
                out.append((False, self._read_reply(first)))
        return out

    def _readline(self) -> str:
        """Read one reply line; empty line means EOF (server closed)."""
        try:
            line = self._reader.readline()
        except OSError as exc:
            self.close()
            raise OperationalError(
                f"connection lost while reading result: {exc}"
            ) from exc
        if line == "":
            self.close()
            raise OperationalError(
                "connection closed by server while reading result"
            )
        return line.rstrip("\r\n")

    def _read_reply(self, first: str) -> str:
        """Complete one reply from an already-read first line.

        Single-line replies (``OK`` / ``PONG`` / ``BYE`` / ``(N rows
        affected)``) return immediately; table replies are read until the
        ``(N rows)`` trailer.  Same boundary rules as a single
        :meth:`command`.
        """
        if _is_single_line_reply(first):
            return first
        lines = [first]
        while True:
            line = self._readline()
            lines.append(line)
            if _is_rows_trailer(line):
                break
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
    """OK / PONG / BYE / AUTH OK / (N rows affected) are single-line replies."""
    if line in ("OK", "PONG", "BYE", "AUTH OK"):
        return True
    return _is_affected_trailer(line)


def _is_affected_trailer(line: str) -> bool:
    return line.startswith("(") and line.endswith(" rows affected)")


def _is_rows_trailer(line: str) -> bool:
    return line.startswith("(") and line.endswith(" rows)")

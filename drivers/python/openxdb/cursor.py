# -*- coding: utf-8 -*-
"""PEP 249 style Cursor over the OpenXDB line protocol.

Parameterised execution is implemented **client side**: the OpenXDB server
protocol has no prepared statements / binary parameter channel, so ``?``
placeholders are substituted with safely escaped SQL literals before the
statement is sent. The exact escaping rules and their boundaries are
documented in docs/T19_client_drivers.md.
"""

from __future__ import annotations

from typing import NamedTuple

from .errors import (
    ConnectionClosedError,
    OperationalError,
    ParameterError,
    ProgrammingError,
)
from .protocol import OpenXDBProtocol

# SQL literal markers (server-side lexer accepts single or double quotes).
_QUOTE = "'"


class PipelineResult(NamedTuple):
    """Outcome of one statement inside a :meth:`Cursor.pipeline` batch.

    ``kind`` is one of:

    * ``"error"`` — the server replied ``ERR <message>``; ``error`` carries
      the :class:`OperationalError` and later statements still executed;
    * ``"affected"`` — non-query statement; ``rowcount`` is ``(N rows
      affected)``;
    * ``"ok"`` — connection-level statement (BEGIN / COMMIT / ...), no data;
    * ``"table"`` — query; ``description`` is the header list and ``rows``
      the result rows (``\\N`` cells map to ``None``, empty cells to ``""``).
    """

    kind: str
    rowcount: int = -1
    description: list | None = None
    rows: list[tuple] = ()
    error: BaseException | None = None


class Cursor:
    """Execute statements and iterate over their results."""

    def __init__(self, protocol: OpenXDBProtocol):
        self._protocol = protocol
        self._description: list | None = None
        self._rows: list[tuple] | None = None
        self._pos = 0
        self._rowcount = -1
        self._closed = False

    # -- PEP 249 attributes ---------------------------------------------

    @property
    def description(self):
        """Column name list (7-tuples PEP 249; only name is populated)."""
        if self._description is None:
            return None
        return [tuple([name] + [None] * 6) for name in self._description]

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int):
        self._arraysize = int(value)

    _arraysize = 1

    # -- execution -------------------------------------------------------

    def execute(self, query: str, params=None):
        """Execute *query* (``?`` placeholders replaced client side).

        Returns ``self`` so calls can be chained. Query results are buffered;
        use ``fetchone`` / ``fetchmany`` / ``fetchall`` to consume them.
        """
        if self._closed:
            raise ConnectionClosedError("cursor is closed")
        if not isinstance(query, str) or not query.strip():
            raise ProgrammingError("query must be a non-empty string")

        sql = self._substitute(query, params) if params is not None else query
        if params is None and "?" in query:
            raise ParameterError(
                f"query has placeholders but no parameters were given: {query!r}"
            )
        raw = self._protocol.command(sql)
        self._reset_buffers()
        if raw.startswith("(") and raw.endswith(" rows affected)"):
            # Non-query statement: only the affected-row count is returned.
            self._rowcount = _parse_affected(raw)
            self._description = None
            self._rows = []
        elif raw == "OK":
            # Connection-level statement (BEGIN / COMMIT / ROLLBACK / ...):
            # single-line OK reply, no result set, no affected-row count.
            self._rowcount = -1
            self._description = None
            self._rows = []
        else:
            header, rows = _parse_table(raw)
            self._description = header
            self._rows = rows
            self._rowcount = -1
        return self

    def executemany(self, query: str, seq_of_params):
        """Run *query* for each parameter sequence in *seq_of_params*."""
        total = 0
        for params in seq_of_params:
            self.execute(query, params)
            if self._rowcount >= 0:
                total += self._rowcount
        self._rowcount = total
        self._description = None
        self._rows = []
        return self

    def pipeline(self, queries, params_list=None):
        """Execute several statements in a single network round trip.

        All statements are sent to the server in one batch and replies are
        read in order, so there is only one write + one read burst instead of
        one round trip per statement (server REPLSession handles consecutive
        input lines in order).

        Returns a list of :class:`PipelineResult` with the same length as
        *queries*.  A statement that fails (server ``ERR``) is reported as
        ``kind == "error"`` and does **not** interrupt the remaining
        statements; every statement in the batch gets a corresponding entry.

        *queries* is a list of SQL strings; when *params_list* is given it
        must have the same length and each entry is the parameter sequence
        for the matching query (``None`` means "no parameters"; the query
        must then contain no ``?`` placeholders).  The cursor's single-
        statement state is reset after the batch — use :meth:`execute` /
        :meth:`fetchall` for sequential work.
        """
        if self._closed:
            raise ConnectionClosedError("cursor is closed")
        if not isinstance(queries, (list, tuple)) or not queries:
            raise ProgrammingError("queries must be a non-empty list of SQL strings")
        sqls = []
        if params_list is None:
            for q in queries:
                if not isinstance(q, str) or not q.strip():
                    raise ProgrammingError("query must be a non-empty string")
                if "?" in q:
                    raise ParameterError(
                        f"query has placeholders but no parameters were given: {q!r}"
                    )
                sqls.append(q)
        else:
            if len(params_list) != len(queries):
                raise ParameterError(
                    f"params_list length {len(params_list)} does not match "
                    f"queries length {len(queries)}"
                )
            for q, p in zip(queries, params_list):
                if not isinstance(q, str) or not q.strip():
                    raise ProgrammingError("query must be a non-empty string")
                if p is None:
                    if "?" in q:
                        raise ParameterError(
                            f"query has placeholders but no parameters: {q!r}"
                        )
                    sqls.append(q)
                else:
                    sqls.append(self._substitute(q, p))
        raws = self._protocol.pipeline(sqls)
        self._reset_buffers()
        out = []
        for is_err, text in raws:
            if is_err:
                out.append(PipelineResult("error", error=OperationalError(text)))
            elif text == "OK":
                out.append(PipelineResult("ok"))
            elif text.startswith("(") and text.endswith(" rows affected)"):
                out.append(
                    PipelineResult("affected", rowcount=_parse_affected(text))
                )
            else:
                header, rows = _parse_table(text)
                out.append(
                    PipelineResult("table", description=header, rows=rows)
                )
        return out

    # -- fetch -----------------------------------------------------------

    def fetchone(self):
        if self._closed:
            raise ConnectionClosedError("cursor is closed")
        if self._rows is None:
            raise ProgrammingError("no query has been executed")
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None):
        if size is None:
            size = self._arraysize
        if size <= 0:
            raise ProgrammingError("fetchmany size must be positive")
        out = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            out.append(row)
        return out

    def fetchall(self):
        if self._closed:
            raise ConnectionClosedError("cursor is closed")
        if self._rows is None:
            raise ProgrammingError("no query has been executed")
        out = self._rows[self._pos:]
        self._pos = len(self._rows)
        return out

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def close(self):
        self._closed = True
        self._rows = None
        self._description = None

    # -- internals -------------------------------------------------------

    def _reset_buffers(self):
        self._description = None
        self._rows = None
        self._pos = 0
        self._rowcount = -1

    def _substitute(self, query: str, params) -> str:
        """Replace ``?`` placeholders with escaped SQL literals."""
        if not isinstance(params, (list, tuple)):
            raise ParameterError("params must be a list or tuple")
        parts = query.split("?")
        if len(parts) - 1 != len(params):
            raise ParameterError(
                f"query has {len(parts) - 1} placeholders but {len(params)} parameters"
            )
        out = [parts[0]]
        for value, chunk in zip(params, parts[1:]):
            out.append(_literal(value))
            out.append(chunk)
        return "".join(out)


def _literal(value) -> str:
    """Convert one Python value into a safe OpenXDB SQL literal."""
    if isinstance(value, bool):
        # No BOOLEAN type on the server; map to 1 / 0 (documented boundary).
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, bytes):
        # BLOB columns are transported as upper-case hex text (docs/T9).
        return _QUOTE + value.hex().upper() + _QUOTE
    if isinstance(value, str):
        # v1.5.0 P2-12: line protocol now supports cell newlines.  String
        # parameters use the E'...' escape-string literal: backslash and
        # CR/LF are escaped (\\ / \n / \r), quotes are doubled.  The server
        # lexer decodes them back, so round-trips are lossless.
        escaped = value.replace("\\", "\\\\")
        escaped = escaped.replace("\n", "\\n")
        escaped = escaped.replace("\r", "\\r")
        escaped = escaped.replace(_QUOTE, _QUOTE * 2)
        return "E" + _QUOTE + escaped + _QUOTE
    if value is None:
        # NULL literal is accepted by the SQL engine and rendered as \N on
        # the wire; the driver decodes \N back to None on reads.
        return "NULL"
    raise ParameterError(f"unsupported parameter type: {type(value).__name__}")


def _parse_affected(raw: str) -> int:
    # "(3 rows affected)" -> 3
    inner = raw[1:].split(" rows affected")[0]
    try:
        return int(inner.strip())
    except ValueError:
        raise OperationalError(f"cannot parse affected-rows reply: {raw!r}") from None


def _parse_table(raw: str):
    """Parse a query table reply into (header, rows).

    The first line is the header, the second is the ``-`` separator line,
    the trailing ``(N rows)`` line is dropped. Rows are split on the first
    ``N-1`` occurrences of ``" | "`` so cell values may contain ``|``.

    Cell decoding (v1.5.0 P2-12): the server renders NULL as the literal
    ``\\N`` and the empty string as an empty cell, so the two are
    distinguishable on the wire.  Cell escapes ``\\\\`` / ``\\n`` / ``\\r``
    are decoded back to backslash / newline / carriage return; any other
    backslash sequence is kept verbatim.
    """
    lines = raw.split("\n")
    if len(lines) < 3:
        raise OperationalError(f"malformed query result: {raw!r}")
    header = [c.strip() for c in lines[0].split(" | ")]
    n = len(header)
    rows = []
    for line in lines[2:]:
        if line.startswith("(") and line.endswith(" rows)"):
            break
        cells = _split_row(line, n)
        vals = []
        for cell in cells:
            raw_cell = cell.strip()
            if raw_cell == _NULL_CELL:
                vals.append(None)
            else:
                vals.append(_unescape_cell(raw_cell))
        rows.append(tuple(vals))
    return header, rows


_NULL_CELL = "\\N"


def _unescape_cell(cell: str) -> str:
    """Decode server cell escapes (\\\\ -> \\, \\n -> LF, \\r -> CR)."""
    out = []
    i = 0
    while i < len(cell):
        if cell[i] == "\\" and i + 1 < len(cell):
            nxt = cell[i + 1]
            if nxt == "\\":
                out.append("\\")
                i += 2
                continue
            if nxt == "n":
                out.append("\n")
                i += 2
                continue
            if nxt == "r":
                out.append("\r")
                i += 2
                continue
        out.append(cell[i])
        i += 1
    return "".join(out)


def _split_row(line: str, n: int):
    cells = []
    rest = line
    for _ in range(n - 1):
        idx = rest.find(" | ")
        if idx < 0:
            # Fewer separators than columns: pad with empty cells.
            cells.append(rest)
            rest = ""
            break
        cells.append(rest[:idx])
        rest = rest[idx + 3:]
    cells.append(rest)
    if len(cells) < n:
        cells.extend([""] * (n - len(cells)))
    return cells[:n]
